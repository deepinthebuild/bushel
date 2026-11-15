package publication

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/workspace"
	"github.com/libp2p/go-libp2p/core/peer"
)

type controlledSource struct {
	has    func(context.Context, cas.Digest) (bool, error)
	calls  atomic.Int32
	active atomic.Int32
}

func (s *controlledSource) Has(ctx context.Context, d cas.Digest) (bool, error) {
	s.calls.Add(1)
	s.active.Add(1)
	defer s.active.Add(-1)
	return s.has(ctx, d)
}

type controlledTransport struct {
	offer  func(context.Context, peer.ID, cas.Digest) (bool, error)
	calls  atomic.Int32
	active atomic.Int32
}

func (*controlledTransport) ID() peer.ID { return publicationPeer(0) }
func (*controlledTransport) SeedPeers() []peer.ID {
	return []peer.ID{publicationPeer(1), publicationPeer(2)}
}
func (t *controlledTransport) Offer(ctx context.Context, id peer.ID, d cas.Digest) (bool, error) {
	t.calls.Add(1)
	t.active.Add(1)
	defer t.active.Add(-1)
	return t.offer(ctx, id, d)
}

type controlledOutbox struct {
	mu      sync.Mutex
	latest  []byte
	saves   chan []byte
	failAt  int
	failure error
	failed  chan struct{}
	calls   int
	closed  atomic.Int32
}

func newControlledOutbox() *controlledOutbox {
	return &controlledOutbox{saves: make(chan []byte, 64), failed: make(chan struct{})}
}

func (b *controlledOutbox) Save(s *state) error {
	encoded, err := marshalState(s)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	if b.failAt == b.calls {
		close(b.failed)
		return b.failure
	}
	b.latest = encoded
	select {
	case b.saves <- bytes.Clone(encoded):
	default:
	}
	return nil
}

func (b *controlledOutbox) Close() error { b.closed.Add(1); return nil }
func (b *controlledOutbox) snapshot() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()
	return bytes.Clone(b.latest)
}

func startControlledPublisher(t *testing.T, s *state, source *controlledSource, transport *controlledTransport, box *controlledOutbox) *Publisher {
	t.Helper()
	ctx, cancel := context.WithCancel(t.Context())
	classifier, err := workspace.NewClassifier(transport.ID().String(), "", 32, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	p := &Publisher{ctx: ctx, cancel: cancel, source: source, transport: transport, classifier: classifier,
		options: Options{Policy: 1 << uint(workspace.Canonical), BytesPerHour: 1 << 20, Timeout: time.Minute}, box: box, state: s,
		events: make(chan observation, 256), results: make(chan result, 1), done: make(chan struct{})}
	go p.run()
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func runtimeMetadata(label string) workspace.Metadata {
	return workspace.Metadata{Valid: true, InvocationID: "invocation", ActionID: "action", TargetID: label}
}

func waitRuntimeSignal(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitSavedState(t *testing.T, box *controlledOutbox, match func(*state) bool) *state {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case encoded := <-box.saves:
			s, err := parseState(encoded)
			if err != nil {
				t.Fatal(err)
			}
			if match(s) {
				return s
			}
		case <-deadline.C:
			t.Fatal("expected durable state was never saved")
		}
	}
}

func TestPublisherCallbacksDoNotWaitForStalledOffer(t *testing.T) {
	entered := make(chan struct{})
	transport := &controlledTransport{offer: func(ctx context.Context, _ peer.ID, _ cas.Digest) (bool, error) {
		close(entered)
		<-ctx.Done()
		return false, ctx.Err()
	}}
	source := &controlledSource{has: func(context.Context, cas.Digest) (bool, error) { return true, nil }}
	box := newControlledOutbox()
	p := startControlledPublisher(t, newState(time.Now()), source, transport, box)
	d := publicationDigest(1, 1024)
	m := runtimeMetadata("@@external//pkg:t")
	p.Committed(d, m)
	waitRuntimeSignal(t, entered, "stalled offer")
	callbacksDone := make(chan struct{})
	go func() {
		for range 10000 {
			p.Observe(d, m, true)
			p.Committed(d, m)
		}
		close(callbacksDone)
	}()
	waitRuntimeSignal(t, callbacksDone, "nonblocking callbacks")
	if transport.active.Load() != 1 || len(p.events) > cap(p.events) {
		t.Fatal("callback bypassed bounded queue or ended stalled offer")
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if transport.active.Load() != 0 || source.active.Load() != 0 || box.closed.Load() != 1 {
		t.Fatal("shutdown retained a worker or failed to close persistence")
	}
	before := source.calls.Load()
	p.Committed(d, m)
	if source.calls.Load() != before {
		t.Fatal("callback after Close touched storage")
	}
}

func TestPublisherCloseCancelsAndJoinsSourceAccess(t *testing.T) {
	entered, canceled, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	releaseSource := func() { releaseOnce.Do(func() { close(release) }) }
	source := &controlledSource{has: func(ctx context.Context, _ cas.Digest) (bool, error) {
		close(entered)
		<-ctx.Done()
		close(canceled)
		<-release
		return false, ctx.Err()
	}}
	transport := &controlledTransport{offer: func(context.Context, peer.ID, cas.Digest) (bool, error) { return true, nil }}
	box := newControlledOutbox()
	p := startControlledPublisher(t, newState(time.Now()), source, transport, box)
	t.Cleanup(releaseSource)
	p.Committed(publicationDigest(1, 1024), runtimeMetadata("@@external//pkg:t"))
	waitRuntimeSignal(t, entered, "source verification")
	closed := make(chan error, 1)
	go func() { closed <- p.Close() }()
	waitRuntimeSignal(t, canceled, "source cancellation")
	select {
	case <-closed:
		t.Fatal("Close returned before source access ended")
	default:
	}
	if source.active.Load() != 1 || box.closed.Load() != 0 {
		t.Fatal("resources closed while verification was active")
	}
	releaseSource()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not join released source")
	}
	if source.active.Load() != 0 || transport.calls.Load() != 0 || box.closed.Load() != 1 {
		t.Fatal("canceled source escaped shutdown or reached transport")
	}
}

func TestPublisherWriteAheadFailurePreventsDispatch(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(map[int]string{1: "intent", 2: "charged attempt"}[failAt], func(t *testing.T) {
			failure := errors.New("outbox write failed")
			box := newControlledOutbox()
			box.failAt, box.failure = failAt, failure
			source := &controlledSource{has: func(context.Context, cas.Digest) (bool, error) { return true, nil }}
			transport := &controlledTransport{offer: func(context.Context, peer.ID, cas.Digest) (bool, error) { return true, nil }}
			p := startControlledPublisher(t, newState(time.Now()), source, transport, box)
			p.Committed(publicationDigest(1, 1024), runtimeMetadata("@@external//pkg:t"))
			waitRuntimeSignal(t, box.failed, "failed write-ahead save")
			if err := p.Close(); !errors.Is(err, failure) {
				t.Fatalf("lost persistence error: %v", err)
			}
			if source.calls.Load() != 0 || transport.calls.Load() != 0 || !p.Stats().PersistenceFailed {
				t.Fatal("failed write-ahead state dispatched work")
			}
			if failAt == 2 {
				s, err := parseState(box.snapshot())
				if err != nil {
					t.Fatal(err)
				}
				if s.Bytes != 0 || s.Offers != 0 {
					t.Fatal("failed replacement overwrote last durable state")
				}
			}
		})
	}
}

func TestPublisherSavesChargeBeforeSourceAndOffer(t *testing.T) {
	box := newControlledOutbox()
	d := publicationDigest(1, 1024)
	verified := make(chan error, 2)
	checkSaved := func(id peer.ID) error {
		s, err := parseState(box.snapshot())
		if err != nil {
			return err
		}
		j := s.Jobs[d]
		if j == nil || s.Bytes != d.Size || s.Offers != 1 || len(j.Attempts) != 1 || len(j.Completed) != 0 || id != "" && j.Attempts[0] != id {
			return errors.New("I/O began before attempt was durably charged")
		}
		return nil
	}
	source := &controlledSource{has: func(context.Context, cas.Digest) (bool, error) {
		err := checkSaved("")
		verified <- err
		return err == nil, err
	}}
	entered := make(chan struct{})
	transport := &controlledTransport{offer: func(ctx context.Context, id peer.ID, _ cas.Digest) (bool, error) {
		verified <- checkSaved(id)
		close(entered)
		<-ctx.Done()
		return false, ctx.Err()
	}}
	p := startControlledPublisher(t, newState(time.Now()), source, transport, box)
	p.Committed(d, runtimeMetadata("@@external//pkg:t"))
	waitRuntimeSignal(t, entered, "offer after durable charge")
	for range 2 {
		if err := <-verified; err != nil {
			t.Fatal(err)
		}
	}
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestPublisherMissingVerifiedSourceWaitsForExplicitCommit(t *testing.T) {
	box := newControlledOutbox()
	source := &controlledSource{has: func(context.Context, cas.Digest) (bool, error) { return false, nil }}
	transport := &controlledTransport{offer: func(context.Context, peer.ID, cas.Digest) (bool, error) { return true, nil }}
	p := startControlledPublisher(t, newState(time.Now()), source, transport, box)
	d := publicationDigest(1, 1024)
	p.Committed(d, runtimeMetadata("@@external//pkg:t"))
	restored := waitSavedState(t, box, func(s *state) bool { return s.Jobs[d] != nil && len(s.Jobs[d].Attempts) == 1 && !s.Jobs[d].Ready })
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	if source.calls.Load() != 1 || transport.calls.Load() != 0 {
		t.Fatal("missing/unverified local blob was offered")
	}
	// Advance the pure state past cooldowns without a wall-clock sleep. Missing
	// verified source data cannot become ready merely because time passes.
	restored.advance(time.Unix(restored.Clock+60, 0))
	if restored.start(15, publicationPeer(0), []peer.ID{publicationPeer(2)}, 1<<20) != nil {
		t.Fatal("missing source retried without explicit completion")
	}
	ready := &controlledSource{has: func(context.Context, cas.Digest) (bool, error) { return true, nil }}
	offered := make(chan struct{})
	transport2 := &controlledTransport{offer: func(ctx context.Context, _ peer.ID, _ cas.Digest) (bool, error) {
		close(offered)
		<-ctx.Done()
		return false, ctx.Err()
	}}
	restarted := startControlledPublisher(t, restored, ready, transport2, newControlledOutbox())
	// The upload can lack metadata: its earlier external association survives.
	restarted.Committed(d, workspace.Metadata{})
	waitRuntimeSignal(t, offered, "explicit completion after repair")
	if err := restarted.Close(); err != nil {
		t.Fatal(err)
	}
	if len(restored.Jobs[d].Attempts) != 2 || restored.Bytes != 2*d.Size {
		t.Fatal("repair reset earlier attempt accounting")
	}
}

func TestPublisherDuplicateMetadataPreservesCompletedWork(t *testing.T) {
	now := time.Now()
	s := newState(now)
	box := newControlledOutbox()
	source := &controlledSource{has: func(context.Context, cas.Digest) (bool, error) { return true, nil }}
	transport := &controlledTransport{offer: func(context.Context, peer.ID, cas.Digest) (bool, error) { return true, nil }}
	p := startControlledPublisher(t, s, source, transport, box)
	// Stop the worker before exercising explicit-time transitions on its state.
	if err := p.Close(); err != nil {
		t.Fatal(err)
	}
	d := publicationDigest(1, 1024)
	m := runtimeMetadata("@@external//pkg:t")
	p.observe(observation{digest: d, metadata: m, ready: true}, now)
	for i := 1; i <= 2; i++ {
		s.advance(now.Add(time.Duration(i) * time.Second))
		a := s.start(p.options.Policy, transport.ID(), []peer.ID{publicationPeer(i)}, 1<<20)
		if a == nil {
			t.Fatal("fixture could not complete two receipts")
		}
		s.finish(*a, true, false)
	}
	before, err := marshalState(s)
	if err != nil {
		t.Fatal(err)
	}
	for range 100 {
		if p.observe(observation{digest: d, metadata: m, ready: true}, now.Add(time.Minute)) {
			t.Fatal("identical metadata dirtied completed publication")
		}
	}
	after, err := marshalState(s)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("duplicate metadata reset prior completion")
	}
}

func TestPublisherExpiryRemainsVisibleAcrossSnapshotRestart(t *testing.T) {
	now := time.Unix(100000, 0)
	s := newState(now)
	s.observe(publicationDigest(1, 1024), association(1, workspace.Canonical), true)
	a := s.start(15, publicationPeer(0), []peer.ID{publicationPeer(1)}, 1<<20)
	s.finish(*a, true, false)
	s.advance(now.Add(time.Second))
	a = s.start(15, publicationPeer(0), []peer.ID{publicationPeer(2)}, 1<<20)
	s.finish(*a, false, true)
	s.observe(publicationDigest(2, 1024), association(2, workspace.Canonical), true)
	for i := 2; i <= 3; i++ {
		s.advance(now.Add(time.Duration(i) * time.Second))
		a = s.start(15, publicationPeer(0), []peer.ID{publicationPeer(i)}, 1<<20)
		if a == nil {
			t.Fatal("fixture could not complete second job")
		}
		s.finish(*a, true, false)
	}
	p := &Publisher{state: s, options: Options{Policy: 15}}
	p.updateStats(false)
	if stats := p.Stats(); stats.VerifiedPeers != 3 || stats.Completed != 1 || stats.Expired != 0 {
		t.Fatalf("receipt accounting: %+v", stats)
	}
	s.advance(now.Add(jobLifetime + 4*time.Second))
	s.advance(now.Add(jobLifetime + time.Hour))
	encoded, err := marshalState(s)
	if err != nil {
		t.Fatal(err)
	}
	p.state, err = parseState(encoded)
	if err != nil {
		t.Fatal(err)
	}
	p.updateStats(false)
	if stats := p.Stats(); stats.Expired != 1 || stats.Records != 0 || stats.VerifiedPeers != 0 {
		t.Fatalf("expired under-replication disappeared or counted twice: %+v", stats)
	}
}

func TestPublisherStatsExposeAgeAndUnconfirmedAttempts(t *testing.T) {
	s := newState(time.Unix(100000, 0))
	s.observe(publicationDigest(1, 1024), association(1, workspace.Canonical), true)
	a := s.start(15, publicationPeer(0), []peer.ID{publicationPeer(1)}, 1<<20)
	p := &Publisher{state: s, options: Options{Policy: 15}}
	p.updateStats(false)
	if p.Stats().UnconfirmedAttempts != 0 {
		t.Fatal("active attempt counted as failed")
	}
	s.finish(*a, false, false)
	s.advance(time.Unix(100010, 0))
	p.updateStats(false)
	if stats := p.Stats(); stats.UnconfirmedAttempts != 1 || stats.OldestPendingSeconds != 10 || stats.QueuedBytes != 1024 {
		t.Fatalf("lost under-replication age or attempt outcome: %+v", stats)
	}
}

func TestPublisherStaleMissingResultPreservesNewerCommit(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var checks atomic.Int32
		source := &controlledSource{has: func(ctx context.Context, _ cas.Digest) (bool, error) {
			if checks.Add(1) == 1 {
				// This probe observed absence before the repairing local upload.
				// Delay its result until the upload callback has been processed.
				select {
				case <-release:
					return false, nil
				case <-ctx.Done():
					return false, ctx.Err()
				}
			}
			return true, nil
		}}
		transport := &controlledTransport{offer: func(context.Context, peer.ID, cas.Digest) (bool, error) {
			return true, nil
		}}
		p := startControlledPublisher(t, newState(time.Now()), source, transport, newControlledOutbox())
		d := publicationDigest(1, 1024)
		metadata := runtimeMetadata("@@external//pkg:t")
		p.Committed(d, metadata)
		synctest.Wait()
		if source.calls.Load() != 1 || transport.calls.Load() != 0 {
			t.Fatal("first source probe did not stall before offering")
		}
		p.Committed(d, workspace.Metadata{})
		// The extra waiting job makes callback processing observable without
		// reading mutable publisher state from the test goroutine.
		p.Observe(publicationDigest(2, 1024), metadata, false)
		synctest.Wait()
		if stats := p.Stats(); stats.Records != 2 || stats.Waiting != 1 {
			t.Fatalf("newer completion was not processed before stale result: %+v", stats)
		}
		close(release)
		time.Sleep(10 * time.Second)
		synctest.Wait()
		if stats := p.Stats(); stats.Completed != 1 || stats.VerifiedPeers != ReplicaTarget || stats.Waiting != 1 {
			t.Fatalf("stale missing result erased a newer local completion: %+v", stats)
		}
	})
}

func TestPublisherEarlierLocalCommitActivatesDelayedMissingObservation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &controlledSource{has: func(context.Context, cas.Digest) (bool, error) { return true, nil }}
		transport := &controlledTransport{offer: func(context.Context, peer.ID, cas.Digest) (bool, error) {
			return true, nil
		}}
		p := startControlledPublisher(t, newState(time.Now()), source, transport, newControlledOutbox())
		d := publicationDigest(1, 1024)
		// FindMissing observed absence, but its external-association callback
		// is delayed until after a metadata-free local upload has completed.
		p.Committed(d, workspace.Metadata{})
		synctest.Wait()
		if source.calls.Load() != 0 || transport.calls.Load() != 0 {
			t.Fatal("metadata-free completion bypassed external-only policy")
		}
		p.Observe(d, runtimeMetadata("@@external//pkg:t"), false)
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if stats := p.Stats(); stats.Completed != 1 || stats.VerifiedPeers != ReplicaTarget {
			t.Fatalf("late external association lost an earlier local completion: %+v", stats)
		}
	})
}

func TestPublisherNeverCountsErroredCompletion(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &controlledSource{has: func(context.Context, cas.Digest) (bool, error) { return true, nil }}
		transport := &controlledTransport{offer: func(context.Context, peer.ID, cas.Digest) (bool, error) {
			return true, errors.New("completion response failed verification")
		}}
		p := startControlledPublisher(t, newState(time.Now()), source, transport, newControlledOutbox())
		p.Committed(publicationDigest(1, 1024), runtimeMetadata("@@external//pkg:t"))
		synctest.Wait()
		if stats := p.Stats(); stats.ChargedOffers != 1 || stats.Completed != 0 || stats.VerifiedPeers != 0 {
			t.Fatalf("errored response counted as a verified receipt: %+v", stats)
		}
	})
}

func TestPublisherMissingProbeInvalidatesCachedPositiveObservation(t *testing.T) {
	for _, duringProbe := range []bool{false, true} {
		t.Run(fmt.Sprintf("missing callback during probe=%t", duringProbe), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				var checks atomic.Int32
				source := &controlledSource{has: func(ctx context.Context, _ cas.Digest) (bool, error) {
					if checks.Add(1) == 1 {
						select {
						case <-release:
							return false, nil
						case <-ctx.Done():
							return false, ctx.Err()
						}
					}
					// A peer import may later make the blob present. That alone
					// must not revive an invalidated local publication intent.
					return true, nil
				}}
				transport := &controlledTransport{offer: func(context.Context, peer.ID, cas.Digest) (bool, error) { return true, nil }}
				p := startControlledPublisher(t, newState(time.Now()), source, transport, newControlledOutbox())
				d := publicationDigest(1, 1024)
				metadata := runtimeMetadata("@@external//pkg:t")
				p.Committed(d, metadata)
				synctest.Wait()
				if duringProbe {
					p.Observe(d, metadata, false)
					synctest.Wait()
				}
				close(release)
				synctest.Wait()
				p.Observe(d, metadata, false)
				time.Sleep(10 * time.Second)
				synctest.Wait()
				if stats := p.Stats(); source.calls.Load() != 1 || transport.calls.Load() != 0 || stats.Waiting != 1 {
					t.Fatalf("stale completion revived after missing probe: %+v", stats)
				}
				p.Committed(d, metadata)
				time.Sleep(3 * time.Second)
				synctest.Wait()
				if stats := p.Stats(); stats.Completed != 1 || stats.VerifiedPeers != ReplicaTarget {
					t.Fatalf("new explicit completion failed to reactivate: %+v", stats)
				}
			})
		})
	}
}
