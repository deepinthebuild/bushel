package p2p

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"testing"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/teststorage"
	"github.com/ipfs/go-cid"
	pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	connmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
	ma "github.com/multiformats/go-multiaddr"
	"pgregory.net/rapid"
)

func TestBlobProviderKeyGolden(t *testing.T) {
	key, err := blobProviderCID(cas.Sum([]byte("hello")))
	const want = "01551220ddc41b7cabefa4ad69ab94148c915784ba1c975e2aed4ba1712b35df9fac1884"
	if err != nil || hex.EncodeToString(key.Bytes()) != want {
		t.Fatalf("provider key: %s, %v", key, err)
	}
	if RoutingProtocol != "/bushel/routing/v1/kad/1.0.0" {
		t.Fatal("routing protocol changed")
	}
}

func TestPropertyBlobProviderIdentity(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		data := rapid.SliceOfN(rapid.Byte(), 0, 1024).Draw(t, "blob")
		d := cas.Sum(data)
		key, err := blobProviderCID(d)
		if err != nil || !key.Defined() || key.Version() != 1 || key.Type() != cid.Raw {
			t.Fatalf("invalid provider CID: %v", err)
		}
		again, _ := blobProviderCID(d)
		if !key.Equals(again) || key.Equals(routingCID(d.Hash)) || key.Equals(rendezvousCID) {
			t.Fatal("provider identity is unstable or aliases another namespace")
		}
		d.Size++
		other, err := blobProviderCID(d)
		if err != nil || other.Equals(key) {
			t.Fatal("provider key does not bind the blob size")
		}
		d.Size = -1
		if _, err := blobProviderCID(d); !errors.Is(err, cas.ErrInvalid) {
			t.Fatal("invalid digest accepted")
		}
	})
}

func TestRoutingIndexCapacityConfiguration(t *testing.T) {
	store, err := cas.NewStore(t.TempDir(), 64<<20, testBackend())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name                string
		total, perPeer      int
		wantTotal, wantPeer int // Zero total means invalid configuration.
	}{
		{"defaults", 0, 0, 32768, 16384},
		{"small total clamps default", 64, 0, 64, 64},
		{"custom budgets", 128, 96, 128, 96},
		{"too small", 63, 0, 0, 0},
		{"too large", 1<<20 + 1, 0, 0, 0},
		{"negative per peer", 64, -1, 0, 0},
		{"per peer exceeds total", 64, 65, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n, err := newNode(t.Context(), Config{Store: store, PrivateKey: key,
				Listen: []string{"/ip4/127.0.0.1/tcp/0"}, disableRoutingRefresh: true,
				RoutingRecords: tc.total, RoutingRecordsPerPeer: tc.perPeer}, loopbackRoutingAddresses)
			if err != nil {
				if tc.wantTotal != 0 {
					t.Fatal(err)
				}
				return
			}
			defer n.Close()
			if tc.wantTotal == 0 {
				t.Fatal("accepted invalid index capacity")
			}
			limits := n.providerData.limits
			if limits.MaxRecords != tc.wantTotal || limits.MaxKeysPerProvider != tc.wantPeer || limits.MaxBytes != tc.wantTotal*256 {
				t.Fatalf("routing index limits: %+v", limits)
			}
		})
	}
}

func TestRoutingConnectionSaturationAndRecovery(t *testing.T) {
	for _, limit := range []int{8, 16, 64, 128} {
		t.Run(fmt.Sprint(limit), func(t *testing.T) {
			// Drive trims explicitly, with two connections per peer, including
			// every peer in the two protected routing buckets.
			cm, err := contentConnectionManager(limit, connmgr.WithSilencePeriod(time.Hour))
			if err != nil {
				t.Fatal(err)
			}
			defer cm.Close()
			rm, err := resourceManager(limit)
			if err != nil {
				t.Fatal(err)
			}
			defer rm.Close()
			var conns []*managedTestConn
			defer func() {
				for _, c := range conns {
					_ = c.CloseWithError(0)
				}
			}()
			connect := func(i int) *managedTestConn {
				t.Helper()
				endpoint := ma.StringCast(fmt.Sprintf("/ip6/2001:db8:%x::1/tcp/1234", i/2+1))
				scope, err := rm.OpenConnection(network.DirOutbound, true, endpoint)
				if err != nil {
					t.Fatalf("connection %d: %v", i, err)
				}
				id := providerFixtureID(t, fmt.Sprintf("routing-peer-%d", i/2))
				if err := scope.SetPeer(id); err != nil {
					scope.Done()
					t.Fatal(err)
				}
				c := &managedTestConn{id: id, cm: cm, scope: scope}
				conns = append(conns, c)
				cm.Notifee().Connected(nil, c)
				return c
			}
			protectedConnections := 4 * routingBucketSize(limit)
			for i := range limit {
				c := connect(i)
				if i < protectedConnections {
					cm.Protect(c.id, "kbucket")
				}
			}
			if extra, err := rm.OpenConnection(network.DirOutbound, true, ma.StringCast("/ip6/2001:db8:ffff::1/tcp/1234")); err == nil {
				extra.Done()
				t.Fatal("hard content connection limit not enforced")
			}
			for cycle := range 2 {
				cm.TrimOpenConns(t.Context())
				remaining := cm.GetInfo().ConnCount
				if remaining > limit*3/4 {
					t.Fatalf("trim left only %d of %d slots free", limit-remaining, limit)
				}
				for _, c := range conns[:protectedConnections] {
					if c.closed.Load() {
						t.Fatal("trim closed a protected routing connection")
					}
				}
				for i := range limit - remaining {
					connect(limit*(cycle+1) + i)
				}
			}
		})
	}
}

// This policy is only reachable through the package-private test constructor.
func loopbackRoutingAddresses(addrs []ma.Multiaddr) []ma.Multiaddr {
	out := make([]ma.Multiaddr, 0, min(8, len(addrs)))
	for _, addr := range addrs {
		ps := addr.Protocols()
		ip, err := addr.ValueForProtocol(ma.P_IP4)
		if err == nil && ip == "127.0.0.1" && len(ps) == 2 && ps[1].Code == ma.P_TCP {
			out = append(out, addr)
			if len(out) == 8 {
				break
			}
		}
	}
	return out
}

func routingNodeAt(t *testing.T, root string, key crypto.PrivKey, backend cas.Backend) (*Node, *cas.Store) {
	t.Helper()
	store, err := cas.NewStore(root, 64<<20, backend)
	if err != nil {
		t.Fatal(err)
	}
	if key == nil {
		key, _, err = crypto.GenerateEd25519Key(rand.Reader)
		if err != nil {
			store.Close()
			t.Fatal(err)
		}
	}
	n, err := newNode(context.Background(), Config{
		Store: store, PrivateKey: key, Listen: []string{"/ip4/127.0.0.1/tcp/0"}, MaxPeers: 16, RequestTimeout: 3 * time.Second,
		disableRoutingRefresh: true,
		announcements: AnnouncerOptions{MinInterval: time.Millisecond, OfflinePoll: 5 * time.Millisecond,
			RetryBase: 10 * time.Millisecond, RetryMax: 50 * time.Millisecond, RecoveryInterval: 100 * time.Millisecond},
	}, loopbackRoutingAddresses)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	return n, store
}

func routingNode(t *testing.T) (*Node, *cas.Store) {
	t.Helper()
	n, store := routingNodeAt(t, t.TempDir(), nil, testBackend())
	t.Cleanup(func() { n.Close(); store.Close() })
	return n, store
}

func awaitRouting(t *testing.T, condition func() bool) {
	t.Helper()
	timer := time.NewTimer(8 * time.Second)
	defer timer.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for !condition() {
		select {
		case <-timer.C:
			t.Fatal("routing condition did not become true")
		case <-tick.C:
		}
	}
}

func bridgeHasProvider(ctx context.Context, bridge *Node, d cas.Digest, id peer.ID) bool {
	key, _ := blobProviderCID(d)
	providers, err := bridge.overlay.ProviderStore().GetProviders(ctx, key.Hash())
	return err == nil && slices.ContainsFunc(providers, func(info peer.AddrInfo) bool { return info.ID == id })
}

func TestRoutingFetchesFromInitiallyUnconnectedProvider(t *testing.T) {
	owner, store := routingNode(t)
	bridge, _ := routingNode(t)
	requester, destination := routingNode(t)
	ctx := testContext(t)
	data := testData(3 << 20)
	d := cas.Sum(data)
	// Publication happens before joining the network. The durable catalog and
	// nonblocking hint path must recover once an overlay peer becomes available.
	if err := store.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if err := owner.Connect(ctx, peerInfo(bridge)); err != nil {
		t.Fatal(err)
	}
	awaitRouting(t, func() bool { return bridgeHasProvider(ctx, bridge, d, owner.ID()) })
	if err := requester.Connect(ctx, peerInfo(bridge)); err != nil {
		t.Fatal(err)
	}
	awaitRouting(t, func() bool { return requester.overlay.RoutingTable().Size() > 0 })
	if requester.host.Network().Connectedness(owner.ID()) == network.Connected {
		t.Fatal("requester was already connected to the provider")
	}
	public := &routingSpy{}
	requester.router = public
	if err := requester.Fetch(ctx, d); err != nil {
		t.Fatal(err)
	}
	if requester.host.Network().Connectedness(owner.ID()) != network.Connected {
		t.Fatal("provider was not discovered and connected")
	}
	r, err := destination.Open(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	r.Close()
	if err != nil || !bytes.Equal(got, data) {
		t.Fatalf("routed blob: %v", err)
	}
	if public.calls() != 0 {
		t.Fatal("content lookup touched public IPFS routing")
	}
	if slices.Contains(requester.host.Mux().Protocols(), dhtPublicProtocol) {
		t.Fatal("content host serves public IPFS DHT")
	}
}

const dhtPublicProtocol = "/ipfs/kad/1.0.0"

func TestRoutingFollowsIntermediateDHTReferral(t *testing.T) {
	owner, source := routingNode(t)
	near, _ := routingNode(t)
	far, _ := routingNode(t)
	requester, destination := routingNode(t)
	ctx := testContext(t)
	data := []byte("the first routing peer does not hold this provider record")
	d := cas.Sum(data)
	if err := source.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if err := near.Connect(ctx, peerInfo(far)); err != nil {
		t.Fatal(err)
	}
	if err := requester.Connect(ctx, peerInfo(near)); err != nil {
		t.Fatal(err)
	}
	awaitRouting(t, func() bool {
		return near.overlay.RoutingTable().Find(far.ID()) == far.ID() && requester.overlay.RoutingTable().Find(near.ID()) == near.ID()
	})
	// Keep the owner disconnected and seed only the far bridge. This isolates
	// iterative lookup from announcements and directly connected manifests.
	key, _ := blobProviderCID(d)
	if err := far.overlay.ProviderStore().AddProvider(ctx, key.Hash(), peerInfo(owner)); err != nil {
		t.Fatal(err)
	}
	if bridgeHasProvider(ctx, near, d, owner.ID()) ||
		requester.host.Network().Connectedness(owner.ID()) == network.Connected ||
		requester.host.Network().Connectedness(far.ID()) == network.Connected {
		t.Fatal("test topology bypassed the intermediate routing hop")
	}
	if err := requester.Fetch(ctx, d); err != nil {
		t.Fatal(err)
	}
	if ok, err := destination.Has(ctx, d); err != nil || !ok {
		t.Fatalf("iterative provider lookup: %v", err)
	}
	if requester.host.Network().Connectedness(far.ID()) != network.Connected {
		t.Fatal("iterative lookup did not follow the routing referral")
	}
}

func TestRoutingRestartReannouncesCommittedCatalog(t *testing.T) {
	ctx := testContext(t)
	root := t.TempDir()
	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	backend := testBackend()
	owner, store := routingNodeAt(t, root, key, backend)
	data := []byte("retained across daemon restart")
	d := cas.Sum(data)
	if err := store.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	owner, store = routingNodeAt(t, root, key, backend)
	t.Cleanup(func() { owner.Close(); store.Close() })
	bridge, _ := routingNode(t)
	// The bridge is new and has no old provider records. A startup sweep,
	// without a second upload, must publish the durable manifest.
	public := &routingSpy{}
	owner.router = public
	if err := owner.Connect(ctx, peerInfo(bridge)); err != nil {
		t.Fatal(err)
	}
	awaitRouting(t, func() bool { return bridgeHasProvider(ctx, bridge, d, owner.ID()) })
	if public.calls() != 0 {
		t.Fatal("reprovide touched public IPFS routing")
	}
}

type routingSpy struct {
	mu        sync.Mutex
	keys      []cid.Cid
	providers []peer.AddrInfo
}

func (r *routingSpy) Provide(_ context.Context, key cid.Cid, _ bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys = append(r.keys, key)
	return nil
}
func (r *routingSpy) FindProvidersAsync(ctx context.Context, key cid.Cid, count int) <-chan peer.AddrInfo {
	r.mu.Lock()
	r.keys = append(r.keys, key)
	providers := slices.Clone(r.providers[:min(len(r.providers), count)])
	r.mu.Unlock()
	ch := make(chan peer.AddrInfo, len(providers))
	for _, p := range providers {
		if ctx.Err() != nil {
			break
		}
		ch <- p
	}
	close(ch)
	return ch
}
func (*routingSpy) Close() error { return nil }
func (r *routingSpy) calls() int { r.mu.Lock(); defer r.mu.Unlock(); return len(r.keys) }

func TestAnnounceRechecksBackendAvailability(t *testing.T) {
	for _, mode := range []string{"chunk eviction", "mapping eviction", "backend error", "canceled"} {
		t.Run(mode, func(t *testing.T) {
			storage := teststorage.New()
			store, err := cas.NewStore(t.TempDir(), 64<<20, cas.Backend{Chunks: storage.Chunks, Mappings: storage.Mappings})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			ctx, cancel := context.WithCancel(testContext(t))
			defer cancel()
			data := testData(700000)
			d := cas.Sum(data)
			if err := store.Put(ctx, d, bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
			manifest, err := store.Manifest(ctx, d)
			if err != nil || len(manifest.Chunks) < 2 {
				t.Fatalf("multi-chunk fixture: %v", err)
			}
			router := &routingSpy{}
			n := &Node{store: store, blobRouter: router}
			if err := n.announceBlob(ctx, d); err != nil || router.calls() != 1 {
				t.Fatalf("initial announcement: %v", err)
			}
			var wantErr error
			switch mode {
			case "chunk eviction":
				c := manifest.Chunks[0]
				storage.EvictChunk(digest.MustNewDigest("", repb.DigestFunction_SHA256, c.Hex(), c.Size))
			case "mapping eviction":
				storage.EvictMapping(digest.MustNewDigest("", repb.DigestFunction_SHA256, d.Hex(), d.Size))
			case "backend error":
				wantErr = errors.New("backend unavailable")
				storage.Fail(teststorage.ChunkFindMissing, 0, wantErr)
			case "canceled":
				cancel()
				wantErr = context.Canceled
			}
			if err := n.announceBlob(ctx, d); !errors.Is(err, wantErr) {
				t.Fatalf("availability check error: got %v, want %v", err, wantErr)
			}
			if router.calls() != 1 {
				t.Fatal("advertised after the backend became unavailable")
			}
		})
	}
}

func TestRoutingStaleProviderFallsBack(t *testing.T) {
	stale, _ := routingNode(t)
	owner, store := routingNode(t)
	requester, destination := routingNode(t)
	ctx := testContext(t)
	data := []byte("one stale hint must not hide another provider")
	d := cas.Sum(data)
	if err := store.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	requester.blobRouter = &routingSpy{providers: []peer.AddrInfo{peerInfo(stale), peerInfo(owner)}}
	if err := requester.Fetch(ctx, d); err != nil {
		t.Fatal(err)
	}
	if ok, err := destination.Has(ctx, d); err != nil || !ok {
		t.Fatalf("fallback: %v", err)
	}
}

func TestRoutingDoesNotTrustPrivateReferrals(t *testing.T) {
	owner, _ := routingNode(t)
	requester, _ := testNode(t) // production public-only policy
	requester.blobRouter = &routingSpy{providers: []peer.AddrInfo{peerInfo(owner)}}
	ctx := testContext(t)
	if err := requester.Fetch(ctx, cas.Sum([]byte("absent"))); !errors.Is(err, cas.ErrNotFound) {
		t.Fatalf("private referral: %v", err)
	}
	if requester.host.Network().Connectedness(owner.ID()) == network.Connected {
		t.Fatal("dialed a private address from an untrusted provider record")
	}
	requester.gate.mu.RLock()
	trusted := requester.gate.trusted[owner.ID()]
	requester.gate.mu.RUnlock()
	if trusted {
		t.Fatal("referral acquired explicit-peer trust")
	}
}

func TestRoutingRejectsForgedProviderIdentity(t *testing.T) {
	sender, _ := routingNode(t)
	bridge, _ := routingNode(t)
	victim, _ := routingNode(t)
	ctx := testContext(t)
	if err := sender.Connect(ctx, peerInfo(bridge)); err != nil {
		t.Fatal(err)
	}
	key, _ := blobProviderCID(cas.Sum([]byte("forged provider claim")))
	messenger, err := pb.NewProtocolMessenger(sender.overlay.MessageSender())
	if err != nil {
		t.Fatal(err)
	}
	if err := messenger.PutProviderAddrs(ctx, bridge.ID(), key.Hash(), peerInfo(victim)); err != nil {
		t.Fatal(err)
	}
	// Follow the fire-and-forget ADD_PROVIDER on the same ordered DHT stream
	// with a query, so rejection is observed after processing the forged claim.
	providers, _, err := messenger.GetProviders(ctx, bridge.ID(), key.Hash())
	if err != nil {
		t.Fatal(err)
	}
	if len(providers) != 0 {
		t.Fatal("stored a provider claim for a different authenticated peer")
	}
	if err := messenger.PutProviderAddrs(ctx, bridge.ID(), key.Hash(), peerInfo(sender)); err != nil {
		t.Fatal(err)
	}
	providers, _, err = messenger.GetProviders(ctx, bridge.ID(), key.Hash())
	if err != nil || len(providers) != 1 || providers[0].ID != sender.ID() {
		t.Fatalf("valid self-provider claim: %v %v", providers, err)
	}
}

type blockingBlobRouter struct{ started, stopped chan struct{} }

func (*blockingBlobRouter) Provide(context.Context, cid.Cid, bool) error { return nil }
func (r *blockingBlobRouter) FindProvidersAsync(ctx context.Context, _ cid.Cid, _ int) <-chan peer.AddrInfo {
	ch := make(chan peer.AddrInfo)
	close(r.started)
	go func() { <-ctx.Done(); close(ch); close(r.stopped) }()
	return ch
}

func TestRoutingLookupCancellationJoinsWorkers(t *testing.T) {
	n, _ := testNode(t)
	router := &blockingBlobRouter{started: make(chan struct{}), stopped: make(chan struct{})}
	n.blobRouter = router
	ctx, cancel := context.WithCancel(testContext(t))
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- n.Fetch(ctx, cas.Sum([]byte("cancel routing"))) }()
	select {
	case <-router.started:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("lookup workers did not stop")
	}
	select {
	case <-router.stopped:
	case <-time.After(time.Second):
		t.Fatal("router context was not canceled")
	}
}
