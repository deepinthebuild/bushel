package p2p

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/membership"
	"github.com/libp2p/go-libp2p/core/network"
	"pgregory.net/rapid"
)

func waitMembershipPublication(t *testing.T, n *Node, ready func(*membership.View) bool) *membershipPublication {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		if publication := n.membership.local.Load(); publication != nil && ready(publication.view) {
			return publication
		}
		select {
		case <-deadline.C:
			t.Fatal("membership publication did not become ready")
		case <-tick.C:
		}
	}
}

func TestMembershipLoopbackSnapshotTailAndConditionalPoll(t *testing.T) {
	source, store := testNode(t)
	dest, _ := testNode(t)
	ctx := testContext(t)
	waitMembershipPublication(t, source, func(v *membership.View) bool { return v.Complete() })
	if err := dest.Connect(ctx, peerInfo(source)); err != nil {
		t.Fatal(err)
	}
	first, err := dest.membership.read(ctx, source.ID(), nil)
	if err != nil || first.view == nil || !first.view.Complete() || first.unchanged {
		t.Fatalf("initial snapshot: %+v %v", first, err)
	}
	if !dest.membership.rememberResult(source.ID(), first, time.Now()) {
		t.Fatal("could not remember initial snapshot")
	}
	known := dest.membership.known(source.ID(), time.Now())
	second, err := dest.membership.read(ctx, source.ID(), known)
	if err != nil || !second.unchanged || second.view != known.view || second.hash != known.hash {
		t.Fatalf("conditional response: %+v %v", second, err)
	}
	data := []byte("an exact addition while the baseline remains unchanged")
	d := cas.Sum(data)
	if err := store.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	waitMembershipPublication(t, source, func(v *membership.View) bool { return v.Sequence() > first.view.Sequence() && v.MayContain(d) })
	third, err := dest.membership.read(ctx, source.ID(), known)
	if err != nil || third.unchanged || third.view == nil || !third.view.MayContain(d) || third.hash == first.hash || !membership.CanReplace(first.view, third.view) {
		t.Fatalf("exact addition response: %+v %v", third, err)
	}
	if stats := source.MembershipStats(); !stats.LocalReady || !stats.LocalComplete || stats.LocalSequence == 0 || stats.BuildsStarted == 0 {
		t.Fatalf("capture telemetry missing: %+v", stats)
	}
	if err := dest.host.Network().ClosePeer(source.ID()); err != nil {
		t.Fatal(err)
	}
	if _, err := dest.membership.read(ctx, source.ID(), nil); err == nil {
		t.Fatal("membership polling dialed a disconnected peer")
	}
}

type membershipCountingStream struct {
	network.Stream
	written *atomic.Int64
}

func (s membershipCountingStream) Write(data []byte) (int, error) {
	n, err := s.Stream.Write(data)
	s.written.Add(int64(n))
	return n, err
}

func TestMembershipAutomaticPollingRefreshesWithFourByteReply(t *testing.T) {
	source, store := testNode(t)
	dest, _, _ := seedNode(t, Config{membership: membershipOptions{pollInterval: time.Second, pollTick: 100 * time.Millisecond}})
	ctx := testContext(t)
	data := []byte("automatic membership capture and conditional refresh")
	d := cas.Sum(data)
	if err := store.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	waitMembershipPublication(t, source, func(v *membership.View) bool { return v.MayContain(d) })
	unchanged := make(chan struct{}, 1)
	source.host.SetStreamHandler(MembershipProtocol, func(stream network.Stream) {
		var written atomic.Int64
		source.membership.serve(membershipCountingStream{Stream: stream, written: &written})
		if written.Load() == 4 {
			select {
			case unchanged <- struct{}{}:
			default:
			}
		}
	})
	if err := dest.Connect(ctx, peerInfo(source)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-unchanged:
	case <-ctx.Done():
		t.Fatal("background polling did not send an unchanged request")
	}
	if !dest.membership.positive(source.ID(), d, time.Now()) || dest.MembershipStats().Peers != 1 {
		t.Fatal("background worker did not retain the advertised complete blob")
	}
}

func TestMembershipRejectsMalformedResponses(t *testing.T) {
	for _, tc := range []struct {
		name string
		body []byte
	}{
		{"oversized", binary.BigEndian.AppendUint32(nil, membership.MaxWireBytes+1)},
		{"unrequested unchanged", binary.BigEndian.AppendUint32(nil, 1)},
		{"unavailable with trailing byte", append(binary.BigEndian.AppendUint32(nil, 0), 7)},
		{"malformed protobuf", append(binary.BigEndian.AppendUint32(nil, 2), 255, 255)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source, _ := testNode(t)
			dest, _ := testNode(t)
			source.host.SetStreamHandler(MembershipProtocol, func(stream network.Stream) {
				defer stream.Close()
				_, _ = io.Copy(io.Discard, io.LimitReader(stream, membership.MaxRequestBytes+2))
				_, _ = stream.Write(tc.body)
			})
			ctx := testContext(t)
			if err := dest.Connect(ctx, peerInfo(source)); err != nil {
				t.Fatal(err)
			}
			if result, err := dest.membership.read(ctx, source.ID(), nil); err == nil || result.view != nil {
				t.Fatalf("malformed response accepted: %+v %v", result, err)
			}
		})
	}
}

func TestMembershipRejectsMalformedRequests(t *testing.T) {
	source, _ := testNode(t)
	dest, _ := testNode(t)
	ctx := testContext(t)
	if err := dest.Connect(ctx, peerInfo(source)); err != nil {
		t.Fatal(err)
	}
	for _, request := range [][]byte{{35}, {1, 0}, {0, 1}, {34, 10, 32, 1}} {
		stream, err := dest.host.NewStream(ctx, source.ID(), MembershipProtocol)
		if err != nil {
			t.Fatal(err)
		}
		_ = stream.SetDeadline(time.Now().Add(2 * time.Second))
		_, _ = stream.Write(request)
		_ = stream.CloseWrite()
		var header [4]byte
		_, err = io.ReadFull(stream, header[:])
		_ = stream.Close()
		if err == nil {
			t.Fatalf("malformed request %x received a valid response", request)
		}
	}
}

func TestMembershipReadCancellationAndShutdown(t *testing.T) {
	source, _ := testNode(t)
	dest, _ := testNode(t)
	started := make(chan struct{}, 4)
	var handlers sync.WaitGroup
	source.host.SetStreamHandler(MembershipProtocol, func(stream network.Stream) {
		handlers.Add(1)
		defer handlers.Done()
		defer stream.Close()
		// Consume the request, then block until cancellation resets the stream.
		_, _ = io.Copy(io.Discard, io.LimitReader(stream, membership.MaxRequestBytes+2))
		started <- struct{}{}
		<-source.ctx.Done()
	})
	ctx := testContext(t)
	if err := dest.Connect(ctx, peerInfo(source)); err != nil {
		t.Fatal(err)
	}
	requestCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { _, err := dest.membership.read(requestCtx, source.ID(), nil); done <- err }()
	<-started
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("canceled read succeeded")
		}
	case <-ctx.Done():
		t.Fatal("cancellation did not interrupt membership read")
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	handlers.Wait()
	if err := dest.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMembershipServingRateBudget(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		var rate membershipRequestRate
		start := time.Unix(1, 0)
		now := start
		accepted := 0
		for range rapid.IntRange(1, 200).Draw(t, "requests") {
			now = now.Add(time.Duration(rapid.IntRange(0, 300).Draw(t, "elapsed milliseconds")) * time.Millisecond)
			if rate.allow(now) {
				accepted++
			}
			if accepted > 8+int(now.Sub(start)/(time.Second/4)) {
				t.Fatal("serving rate exceeded burst plus elapsed allowance")
			}
		}
		for range 8 {
			rate.allow(now)
		}
		if rate.allow(start) || rate.allow(now) {
			t.Fatal("backward time granted new serving credit")
		}
	})
}

func TestMembershipHintMissCountsOnlyNormalAbsence(t *testing.T) {
	m := testMembershipService(t)
	m.observeManifest(false, cas.ErrNotFound)
	m.observeManifest(true, context.DeadlineExceeded)
	m.observeManifest(true, errors.New("untrusted malformed manifest"))
	m.observeManifest(true, nil)
	m.observeManifest(true, cas.ErrNotFound)
	if m.misses.Load() != 1 {
		t.Fatal("hint miss metric classified an error as normal absence")
	}
}
