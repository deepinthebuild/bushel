package p2p

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/membership"
	"github.com/deepinthebuild/bushel/internal/teststorage"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

func fallbackView(t *testing.T, keys []cas.Digest) *membership.View {
	t.Helper()
	v, err := membership.Build([16]byte{1}, 1, 0, keys)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func rememberFallbackView(t *testing.T, n *Node, id peer.ID, view *membership.View) {
	t.Helper()
	data, err := membership.Encode(view)
	if err != nil {
		t.Fatal(err)
	}
	received, err := membership.Decode(data)
	if err != nil || !n.membership.remember(id, received, len(data), time.Now()) {
		t.Fatalf("remember membership fixture: %v", err)
	}
}

func connectFallbackPeer(t *testing.T, ctx context.Context, destination, source *Node) {
	t.Helper()
	// Keep protocol support advertised, so cohort maintenance retains the
	// cached view, while preventing a live refresh from replacing this fixture.
	source.host.SetStreamHandler(MembershipProtocol, func(s network.Stream) { _ = s.Reset() })
	if err := destination.Connect(ctx, peerInfo(source)); err != nil {
		t.Fatal(err)
	}
	if err := destination.host.Peerstore().AddProtocols(source.ID(), MembershipProtocol); err != nil {
		t.Fatal(err)
	}
}

func falsePositiveFallbackView(t *testing.T) (*membership.View, []byte, [][]byte) {
	t.Helper()
	var keys []cas.Digest
	var contents [][]byte
	for i := range 64 {
		data := []byte(fmt.Sprintf("membership-baseline-%d", i))
		keys, contents = append(keys, cas.Sum(data)), append(contents, data)
	}
	view := fallbackView(t, keys)
	// Find an actual binary-fuse false positive. Distinct input domains keep
	// the searched blobs out of both the baseline and its empty exact tail.
	for i := range 1 << 16 {
		data := []byte(fmt.Sprintf("membership-absent-candidate-%d", i))
		d := cas.Sum(data)
		if !slices.Contains(keys, d) && view.MayContain(d) {
			return view, data, contents
		}
	}
	t.Fatal("bounded deterministic search found no binary-fuse false positive")
	return nil, nil, nil
}

func waitForPositiveHintMiss(t *testing.T, ctx context.Context, n *Node, want uint64) {
	t.Helper()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for n.MembershipStats().PositiveHintMisses < want {
		select {
		case <-ctx.Done():
			t.Fatalf("positive manifest miss was not observed: %v", ctx.Err())
		case <-tick.C:
		}
	}
}

func requireLocalFallbackBytes(t *testing.T, ctx context.Context, store *cas.Store, data []byte) {
	t.Helper()
	r, err := store.Open(ctx, cas.Sum(data))
	if err != nil {
		t.Fatalf("download was not retained in local CAS: %v", err)
	}
	got, readErr := io.ReadAll(r)
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(got, data) {
		t.Fatalf("local download changed after source departure: read=%v close=%v", readErr, closeErr)
	}
}

func TestMembershipFalsePositiveMissUsesHintNegativePeer(t *testing.T) {
	view, data, baseline := falsePositiveFallbackView(t)
	testMembershipMissFallback(t, view, data, baseline, false)
}

func TestMembershipEvictionMissUsesPeerWithoutHint(t *testing.T) {
	data := testData(700000)
	testMembershipMissFallback(t, fallbackView(t, []cas.Digest{cas.Sum(data)}), data, nil, true)
}

func testMembershipMissFallback(t *testing.T, view *membership.View, data []byte, baseline [][]byte, evict bool) {
	t.Helper()
	missing, missingStore, backend := seedNode(t, Config{disableRoutingRefresh: true})
	healthy, healthyStore := testNode(t)
	destination, local := testNode(t)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	d := cas.Sum(data)
	for _, baselineData := range baseline {
		if err := missingStore.Put(ctx, cas.Sum(baselineData), bytes.NewReader(baselineData)); err != nil {
			t.Fatal(err)
		}
	}
	if err := healthyStore.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if evict {
		if err := missingStore.Put(ctx, d, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, call := range backend.Calls() {
			if call.Operation == teststorage.MappingPut && call.Digest.GetHashString() == d.Hex() {
				backend.EvictMapping(call.Digest)
				found = true
			}
		}
		if !found {
			t.Fatal("eviction fixture did not publish a multi-chunk mapping")
		}
	}
	// These tests start from already received hints. Failed polling responses
	// leave the deliberately stale cached advertisement in place.
	for _, n := range []*Node{missing, healthy} {
		connectFallbackPeer(t, ctx, destination, n)
	}
	rememberFallbackView(t, destination, missing.ID(), view)
	if !evict {
		rememberFallbackView(t, destination, healthy.ID(), fallbackView(t, nil))
	}
	if !destination.membership.positive(missing.ID(), d, time.Now()) || destination.membership.positive(healthy.ID(), d, time.Now()) {
		t.Fatal("fixture did not separate positive and fallback peers")
	}
	// The same peers also appear repeatedly in routing results. Fetch must
	// attempt each direct peer once, regardless of repeated provider hints.
	destination.blobRouter = &routingSpy{providers: []peer.AddrInfo{
		peerInfo(missing), peerInfo(missing), peerInfo(healthy), peerInfo(healthy),
	}}
	var missingRequests, healthyRequests atomic.Int32
	missing.host.SetStreamHandler(ManifestProtocol, func(s network.Stream) {
		missingRequests.Add(1)
		missing.serveManifest(s)
	})
	allowHealthy := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(allowHealthy) }) }
	defer release()
	healthy.host.SetStreamHandler(ManifestProtocol, func(s network.Stream) {
		healthyRequests.Add(1)
		select {
		case <-allowHealthy:
			healthy.serveManifest(s)
		case <-ctx.Done():
			_ = s.Reset()
		}
	})
	done := make(chan error, 1)
	go func() { done <- destination.Fetch(ctx, d) }()
	waitForPositiveHintMiss(t, ctx, destination, 1)
	release()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ordinary membership miss prevented healthy fallback: %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if missingRequests.Load() != 1 || healthyRequests.Load() != 1 || destination.MembershipStats().PositiveHintMisses != 1 {
		t.Fatalf("duplicate attempts or miss accounting: missing=%d healthy=%d stats=%+v", missingRequests.Load(), healthyRequests.Load(), destination.MembershipStats())
	}
	if destination.host.Network().Connectedness(missing.ID()) != network.Connected || !destination.membership.positive(missing.ID(), d, time.Now()) {
		t.Fatal("ordinary cache miss penalized or discarded the peer's advisory view")
	}
	if err := healthy.Close(); err != nil {
		t.Fatal(err)
	}
	requireLocalFallbackBytes(t, ctx, local, data)
	// A peer that just missed remains usable, even when its old filter is
	// negative for the newly uploaded blob. There is no other live provider.
	var recovered []byte
	for i := range 1000 {
		candidate := []byte(fmt.Sprintf("new-content-after-membership-miss-%d", i))
		if !view.MayContain(cas.Sum(candidate)) {
			recovered = candidate
			break
		}
	}
	if recovered == nil {
		t.Fatal("bounded search found no negative recovery hint")
	}
	if err := missingStore.Put(ctx, cas.Sum(recovered), bytes.NewReader(recovered)); err != nil {
		t.Fatal(err)
	}
	if err := destination.Fetch(ctx, cas.Sum(recovered)); err != nil {
		t.Fatalf("previous ordinary miss made peer unusable: %v", err)
	}
	if missingRequests.Load() != 2 || destination.MembershipStats().PositiveHintMisses != 1 {
		t.Fatal("recovery repeated a peer attempt or charged another positive miss")
	}
	if err := missing.Close(); err != nil {
		t.Fatal(err)
	}
	for _, retained := range [][]byte{data, recovered} {
		if err := destination.Fetch(ctx, cas.Sum(retained)); err != nil {
			t.Fatalf("local hit depended on a departed peer: %v", err)
		}
		requireLocalFallbackBytes(t, ctx, local, retained)
	}
	if missingRequests.Load() != 2 || healthyRequests.Load() != 1 {
		t.Fatal("local cache hit opened another manifest request")
	}
}

type membershipEvictionStream struct {
	network.Stream
	beforeClose func()
}

func (s membershipEvictionStream) Close() error {
	s.beforeClose()
	return s.Stream.Close()
}

func TestMembershipChunksEvictedAfterManifestUseHealthyPeer(t *testing.T) {
	source, sourceStore, backend := seedNode(t, Config{disableRoutingRefresh: true})
	healthy, healthyStore := testNode(t)
	destination, local := testNode(t)
	ctx, cancel := context.WithTimeout(t.Context(), 8*time.Second)
	defer cancel()
	data := testData(1100000)
	d := cas.Sum(data)
	for _, store := range []*cas.Store{sourceStore, healthyStore} {
		if err := store.Put(ctx, d, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
	}
	for _, n := range []*Node{source, healthy} {
		connectFallbackPeer(t, ctx, destination, n)
	}
	rememberFallbackView(t, destination, source.ID(), fallbackView(t, []cas.Digest{d}))
	rememberFallbackView(t, destination, healthy.ID(), fallbackView(t, nil))
	healthyStarted, evicted := make(chan struct{}), make(chan struct{})
	var startOnce, evictOnce sync.Once
	var sourceRequests, healthyRequests atomic.Int32
	source.host.SetStreamHandler(ManifestProtocol, func(s network.Stream) {
		sourceRequests.Add(1)
		source.serveManifest(membershipEvictionStream{Stream: s, beforeClose: func() {
			evictOnce.Do(func() {
				select {
				case <-healthyStarted:
				case <-ctx.Done():
					return
				}
				// EOF completes the manifest response. Remove its chunks after
				// writing that response but before the requester can use it.
				for key := range backend.SnapshotChunks() {
					backend.EvictChunk(key)
				}
				close(evicted)
			})
		}})
	})
	healthy.host.SetStreamHandler(ManifestProtocol, func(s network.Stream) {
		healthyRequests.Add(1)
		startOnce.Do(func() { close(healthyStarted) })
		select {
		case <-evicted:
			healthy.serveManifest(s)
		case <-ctx.Done():
			_ = s.Reset()
		}
	})
	if err := destination.Fetch(ctx, d); err != nil {
		t.Fatalf("chunk eviction after a valid manifest prevented healthy Bitswap fallback: %v", err)
	}
	if chunks, _ := backend.Counts(); chunks != 0 {
		t.Fatal("fixture did not evict the advertised chunks")
	}
	if sourceRequests.Load() != 1 || healthyRequests.Load() != 1 || destination.MembershipStats().PositiveHintMisses != 0 {
		t.Fatal("chunk eviction repeated manifest attempts or became a manifest-miss penalty")
	}
	for _, n := range []*Node{source, healthy} {
		if err := n.Close(); err != nil {
			t.Fatal(err)
		}
	}
	requireLocalFallbackBytes(t, ctx, local, data)
}

func TestMembershipSlowPositivePeersReserveFallbackCapacity(t *testing.T) {
	type source struct {
		node  *Node
		store *cas.Store
	}
	var sources []source
	for range manifestWorkers + 1 {
		n, store := testNode(t)
		sources = append(sources, source{n, store})
	}
	slices.SortFunc(sources, func(a, b source) int {
		if a.node.ID() < b.node.ID() {
			return -1
		}
		if a.node.ID() > b.node.ID() {
			return 1
		}
		return 0
	})
	healthy := sources[len(sources)-1]
	preferred := sources[:len(sources)-1]
	destination, local, _ := seedNode(t, Config{RequestTimeout: 5 * time.Second, disableRoutingRefresh: true})
	ctx := testContext(t)
	data := testData(700000)
	d := cas.Sum(data)
	if err := healthy.store.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	for _, source := range sources {
		connectFallbackPeer(t, ctx, destination, source.node)
	}
	positive := fallbackView(t, []cas.Digest{d})
	for _, source := range preferred {
		rememberFallbackView(t, destination, source.node.ID(), positive)
	}
	rememberFallbackView(t, destination, healthy.node.ID(), fallbackView(t, nil))
	fastCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	started, releaseSlow := make(chan struct{}), make(chan struct{})
	var startedOnce, releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSlow) }) }
	defer release()
	requests := make([]atomic.Int32, len(preferred))
	var returned atomic.Int32
	for i, source := range preferred {
		source.node.host.SetStreamHandler(ManifestProtocol, func(s network.Stream) {
			requests[i].Add(1)
			startedOnce.Do(func() { close(started) })
			select {
			case <-releaseSlow:
			case <-fastCtx.Done():
			}
			_ = s.Close()
			returned.Add(1)
		})
	}
	var healthyRequests atomic.Int32
	healthy.node.host.SetStreamHandler(ManifestProtocol, func(s network.Stream) {
		healthyRequests.Add(1)
		select {
		case <-started:
			healthy.node.serveManifest(s)
		case <-fastCtx.Done():
			_ = s.Reset()
		}
	})
	if err := destination.Fetch(fastCtx, d); err != nil {
		t.Fatalf("slow positive peers consumed fallback worker capacity: %v", err)
	}
	if returned.Load() != 0 || healthyRequests.Load() != 1 || destination.MembershipStats().PositiveHintMisses != 0 {
		t.Fatal("fallback waited for slow peers or treated cancellation as a membership miss")
	}
	for i := range requests {
		if requests[i].Load() > 1 {
			t.Fatal("one slow peer occupied multiple manifest attempts")
		}
	}
	release()
	for _, source := range sources {
		if err := source.node.Close(); err != nil {
			t.Fatal(err)
		}
	}
	requireLocalFallbackBytes(t, t.Context(), local, data)
}
