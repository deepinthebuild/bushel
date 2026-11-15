package p2p

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/teststorage"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	connmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
	ma "github.com/multiformats/go-multiaddr"
)

func testNode(t testing.TB) (*Node, *cas.Store) {
	t.Helper()
	n, store, err := newTestNode(t.TempDir(), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close(); store.Close() })
	return n, store
}

func newTestNode(root string, limit int64) (*Node, *cas.Store, error) {
	store, err := cas.NewStore(root, limit, testBackend())
	if err != nil {
		return nil, nil, err
	}
	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		store.Close()
		return nil, nil, err
	}
	n, err := New(context.Background(), Config{Store: store, PrivateKey: key, Listen: []string{"/ip4/127.0.0.1/tcp/0"}, MaxPeers: 8, RequestTimeout: 2 * time.Second})
	if err != nil {
		store.Close()
		return nil, nil, err
	}
	return n, store, nil
}

func testBackend() cas.Backend {
	storage := teststorage.New()
	return cas.Backend{Chunks: storage.Chunks, Mappings: storage.Mappings}
}

func peerInfo(n *Node) peer.AddrInfo {
	return peer.AddrInfo{ID: n.ID(), Addrs: n.host.Addrs()}
}

func testData(size int) []byte {
	data := make([]byte, size)
	var x uint32 = 1
	for i := range data {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		data[i] = byte(x)
	}
	return data
}

func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestBitswapMultiChunkTransfer(t *testing.T) {
	source, sourceStore := testNode(t)
	dest, destStore := testNode(t)
	ctx := testContext(t)
	data := testData(3 << 20)
	d := cas.Sum(data)
	if err := sourceStore.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	manifest, err := sourceStore.Manifest(ctx, d)
	if err != nil || len(manifest.Chunks) < 7 {
		t.Fatalf("chunking: %v %v", manifest, err)
	}
	if err := dest.Connect(ctx, peerInfo(source)); err != nil {
		t.Fatal(err)
	}
	if err := dest.Fetch(ctx, d); err != nil {
		t.Fatal(err)
	}
	r, err := destStore.Open(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	r.Close()
	if err != nil || !bytes.Equal(data, got) {
		t.Fatalf("roundtrip: %v", err)
	}
	stat, err := dest.bitswap.Stat()
	if err != nil || stat.BlocksReceived < 7 {
		t.Fatalf("did not transfer real Bitswap blocks: %v %v", stat, err)
	}
	for _, c := range manifest.Chunks {
		if c.Size >= 512<<10 {
			t.Fatal("oversize Bitswap chunk")
		}
	}
	if !slices.Contains(source.host.Mux().Protocols(), bitswapPrefix+"/ipfs/bitswap/1.2.0") {
		t.Fatal("Bitswap v1.2 protocol not registered")
	}
}

func writeManifestReply(s network.Stream, size uint32, payload []byte) {
	defer s.Close()
	_ = s.SetDeadline(time.Now().Add(2 * time.Second))
	request, err := io.ReadAll(io.LimitReader(s, 41))
	if err != nil || len(request) != 40 {
		return
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], size)
	_, _ = s.Write(header[:])
	_, _ = s.Write(payload)
}

func TestUntrustedManifestBoundsAndIdentity(t *testing.T) {
	source, _ := testNode(t)
	dest, store := testNode(t)
	ctx := testContext(t)
	d := cas.Sum([]byte("expected"))
	other := cas.Sum([]byte("different"))
	wrong, _ := (cas.Manifest{Blob: other, Chunks: []cas.Digest{other}}).MarshalBinary()
	valid, _ := (cas.Manifest{Blob: d, Chunks: []cas.Digest{d}}).MarshalBinary()
	if err := dest.Connect(ctx, peerInfo(source)); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name    string
		size    uint32
		payload []byte
	}{
		{"oversized", cas.MaxManifestSize + 1, nil},
		{"absent with trailing bytes", 0, []byte{1}},
		{"undersized", cas.ManifestHeaderSize - 1, nil},
		{"wrong digest", uint32(len(wrong)), wrong},
		{"trailing bytes", uint32(len(valid)), append(bytes.Clone(valid), 0)},
		{"invalid encoding", cas.ManifestHeaderSize, make([]byte, cas.ManifestHeaderSize)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			source.host.SetStreamHandler(ManifestProtocol, func(s network.Stream) { writeManifestReply(s, tc.size, tc.payload) })
			if err := dest.Fetch(ctx, d); !errors.Is(err, cas.ErrCorrupt) {
				t.Fatalf("accepted malicious manifest: %v", err)
			}
			if _, err := store.Manifest(ctx, d); !errors.Is(err, cas.ErrNotFound) {
				t.Fatalf("published invalid blob: %v", err)
			}
		})
	}
}

func TestFullBlobVerificationAfterBitswap(t *testing.T) {
	source, sourceStore := testNode(t)
	dest, destStore := testNode(t)
	ctx := testContext(t)
	data := testData(1 << 20)
	d := cas.Sum(data)
	if err := sourceStore.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	m, _ := sourceStore.Manifest(ctx, d)
	data[0] ^= 1
	wanted := cas.Sum(data)
	m.Blob = wanted
	wire, _ := m.MarshalBinary()
	source.host.SetStreamHandler(ManifestProtocol, func(s network.Stream) { writeManifestReply(s, uint32(len(wire)), wire) })
	if err := dest.Connect(ctx, peerInfo(source)); err != nil {
		t.Fatal(err)
	}
	if err := dest.Fetch(ctx, wanted); !errors.Is(err, cas.ErrCorrupt) {
		t.Fatalf("accepted wrong complete blob: %v", err)
	}
	if _, err := destStore.Manifest(ctx, wanted); !errors.Is(err, cas.ErrNotFound) {
		t.Fatalf("published unverified blob: %v", err)
	}
}

func TestBadPeerDoesNotPreventFallback(t *testing.T) {
	a, aStore := testNode(t)
	b, bStore := testNode(t)
	if a.ID() > b.ID() {
		a, b = b, a
		aStore, bStore = bStore, aStore
	}
	_ = aStore
	dest, _ := testNode(t)
	ctx := testContext(t)
	data := testData(700000)
	d := cas.Sum(data)
	if err := bStore.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	a.host.SetStreamHandler(ManifestProtocol, func(s network.Stream) { writeManifestReply(s, cas.MaxManifestSize+1, nil) })
	for _, n := range []*Node{a, b} {
		if err := dest.Connect(ctx, peerInfo(n)); err != nil {
			t.Fatal(err)
		}
	}
	if err := dest.Fetch(ctx, d); err != nil {
		t.Fatalf("bad first peer prevented valid fallback: %v", err)
	}
}

func TestSlowManifestPeerDoesNotBlockHealthyPeer(t *testing.T) {
	a, aStore := testNode(t)
	b, bStore := testNode(t)
	if a.ID() > b.ID() {
		a, b = b, a
		aStore, bStore = bStore, aStore
	}
	_ = aStore
	dest, _ := testNode(t)
	ctx := testContext(t)
	data := testData(700000)
	d := cas.Sum(data)
	if err := bStore.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	for _, n := range []*Node{a, b} {
		if err := dest.Connect(ctx, peerInfo(n)); err != nil {
			t.Fatal(err)
		}
	}
	release := make(chan struct{})
	defer close(release)
	a.host.SetStreamHandler(ManifestProtocol, func(s network.Stream) {
		defer s.Close()
		<-release
	})
	dest.timeout = 10 * time.Second
	fastCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := dest.Fetch(fastCtx, d); err != nil {
		t.Fatalf("silent first peer blocked healthy peer: %v", err)
	}
}

func TestFetchPreservesLocalImportErrors(t *testing.T) {
	source, sourceStore := testNode(t)
	ctx := testContext(t)
	data := testData(700000)
	d := cas.Sum(data)
	if err := sourceStore.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"quota", "filesystem"} {
		t.Run(mode, func(t *testing.T) {
			root := t.TempDir()
			limit := int64(64 << 20)
			if mode == "quota" {
				limit = 1024
			}
			dest, store, err := newTestNode(root, limit)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { dest.Close(); store.Close() }()
			if mode == "filesystem" {
				if err := os.Remove(filepath.Join(root, "tmp")); err != nil {
					t.Fatal(err)
				}
			}
			if err := dest.Connect(ctx, peerInfo(source)); err != nil {
				t.Fatal(err)
			}
			err = dest.Fetch(ctx, d)
			if mode == "quota" && !errors.Is(err, cas.ErrQuota) {
				t.Fatalf("quota error replaced: %v", err)
			}
			var pathErr *os.PathError
			if mode == "filesystem" && !errors.As(err, &pathErr) {
				t.Fatalf("filesystem error replaced: %v", err)
			}
			if errors.Is(err, cas.ErrNotFound) {
				t.Fatalf("local error became cache miss: %v", err)
			}
		})
	}
}

func TestPeerFailureDoesNotHideLocalCleanupError(t *testing.T) {
	remote := &peerTransferError{cas.ErrNotFound}
	if !peerFailure(remote) || !peerFailure(fmt.Errorf("verify: %w", cas.ErrCorrupt)) {
		t.Fatal("lost peer error classification")
	}
	for _, local := range []error{cas.ErrQuota, &os.PathError{Op: "remove", Path: "tmp", Err: os.ErrPermission}} {
		if peerFailure(errors.Join(remote, local)) || peerFailure(errors.Join(cas.ErrCorrupt, local)) {
			t.Fatal("local cleanup error was classified as peer failure")
		}
	}
}

func TestMissingCancellationAndShutdown(t *testing.T) {
	n, _ := testNode(t)
	d := cas.Sum([]byte("absent"))
	if err := n.Fetch(testContext(t), d); !errors.Is(err, cas.ErrNotFound) {
		t.Fatal(err)
	}
	for range cap(n.transfers) {
		n.transfers <- struct{}{}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := n.Fetch(ctx, d); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("transfer admission ignored cancellation: %v", err)
	}
	done := make(chan error, 1)
	go func() { done <- n.Fetch(context.Background(), d) }()
	if err := n.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown did not cancel retrieval")
	}
}

type forgedBlock struct {
	key  cid.Cid
	data []byte
}

func (b forgedBlock) Cid() cid.Cid             { return b.key }
func (b forgedBlock) RawData() []byte          { return b.data }
func (b forgedBlock) String() string           { return b.key.String() }
func (b forgedBlock) Loggable() map[string]any { return nil }

type fakeSession struct{ block blocks.Block }

func (s fakeSession) GetBlocks(context.Context, []cid.Cid) (<-chan blocks.Block, error) {
	ch := make(chan blocks.Block, 1)
	ch <- s.block
	close(ch)
	return ch, nil
}

func TestChunkDigestVerification(t *testing.T) {
	d := cas.Sum([]byte("expected"))
	key, err := sizedChunkCID(d)
	if err != nil {
		t.Fatal(err)
	}
	n := &Node{timeout: time.Second}
	err = n.receiveWindow(context.Background(), fakeSession{forgedBlock{key, []byte("corrupt!")}}, []cid.Cid{key},
		map[string]cas.Digest{key.KeyString(): d}, make(map[cas.Digest][]byte))
	if !errors.Is(err, cas.ErrCorrupt) {
		t.Fatalf("accepted bad chunk digest: %v", err)
	}
}

type recordedRendezvous struct {
	mu      sync.Mutex
	entries []peer.AddrInfo
	keys    []cid.Cid
}

func (r *recordedRendezvous) Provide(_ context.Context, key cid.Cid, _ bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys = append(r.keys, key)
	return nil
}
func (r *recordedRendezvous) FindProvidersAsync(_ context.Context, key cid.Cid, count int) <-chan peer.AddrInfo {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.keys = append(r.keys, key)
	ch := make(chan peer.AddrInfo, min(count, len(r.entries)))
	for _, info := range r.entries[:min(count, len(r.entries))] {
		ch <- info
	}
	close(ch)
	return ch
}
func (*recordedRendezvous) Close() error { return nil }

func TestDiscoveryStartupWithLocalBootstrap(t *testing.T) {
	ctx := testContext(t)
	bootstrap, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.DisableRelay())
	if err != nil {
		t.Fatal(err)
	}
	defer bootstrap.Close()
	// Empty bootstrap peers keep the fixture entirely offline. Use the real
	// public DHT protocol so its configuration validation is exercised.
	bootstrapDHT, err := dht.New(bootstrap, dht.Mode(dht.ModeServer), dht.DisableAutoRefresh(), dht.BootstrapPeers())
	if err != nil {
		t.Fatal(err)
	}
	defer bootstrapDHT.Close()
	connected := make(chan peer.ID, 1)
	bootstrap.Network().Notify(&network.NotifyBundle{ConnectedF: func(_ network.Network, c network.Conn) {
		select {
		case connected <- c.RemotePeer():
		default:
		}
	}})
	store, err := cas.NewStore(t.TempDir(), 64<<20, testBackend())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	n, err := New(ctx, Config{Store: store, PrivateKey: key, Discovery: true,
		Listen: []string{"/ip4/127.0.0.1/tcp/0"}, RequestTimeout: 2 * time.Second,
		Bootstrap: []peer.AddrInfo{{ID: bootstrap.ID(), Addrs: bootstrap.Addrs()}}})
	if err != nil {
		t.Fatalf("discovery startup: %v", err)
	}
	defer n.Close()
	router, ok := n.router.(*dht.IpfsDHT)
	if !ok || router.Mode() != dht.ModeClient {
		t.Fatal("discovery did not initialize a real DHT client")
	}
	if slices.Contains(n.discovery.Mux().Protocols(), "/ipfs/kad/1.0.0") {
		t.Fatal("discovery client serves inbound DHT requests")
	}
	select {
	case id := <-connected:
		if id != n.ID() {
			t.Fatal("bootstrap connected to an unexpected identity")
		}
	case <-ctx.Done():
		t.Fatal("discovery did not connect to its configured bootstrap")
	}
	if n.host.Network().Connectedness(bootstrap.ID()) != network.NotConnected {
		t.Fatal("bootstrap peer entered content host")
	}
}

func TestDiscoveryBridgeAndBootstrapIsolation(t *testing.T) {
	source, sourceStore := testNode(t)
	dest, _ := testNode(t)
	ctx := testContext(t)
	bootstrap, err := libp2p.New(libp2p.ListenAddrStrings("/ip4/127.0.0.1/tcp/0"), libp2p.DisableRelay())
	if err != nil {
		t.Fatal(err)
	}
	defer bootstrap.Close()
	seed := peer.AddrInfo{ID: bootstrap.ID(), Addrs: bootstrap.Addrs()}
	source.discovery, err = source.discoveryHost(Config{PrivateKey: source.host.Peerstore().PrivKey(source.ID()), Bootstrap: []peer.AddrInfo{seed}})
	if err != nil {
		t.Fatal(err)
	}
	if err := source.discovery.Connect(ctx, seed); err != nil {
		t.Fatal(err)
	}
	if source.discovery.ID() != source.ID() || len(source.discovery.Network().ListenAddresses()) != 0 {
		t.Fatal("discovery host identity or listener isolation failed")
	}
	for _, p := range source.discovery.Mux().Protocols() {
		if strings.Contains(string(p), "bitswap") || p == ManifestProtocol {
			t.Fatalf("content protocol on discovery host: %s", p)
		}
	}
	advertised := peer.AddrInfo{ID: source.discovery.ID(), Addrs: source.discovery.Addrs()}
	if !slices.EqualFunc(advertised.Addrs, source.host.Addrs(), func(a, b ma.Multiaddr) bool { return a.Equal(b) }) {
		t.Fatal("rendezvous addresses do not point to content host")
	}
	router := &recordedRendezvous{entries: []peer.AddrInfo{advertised}}
	dest.router = router
	// The injected router uses loopback fixtures. Production filtering is tested
	// independently below and never permits loopback discovery results.
	dest.gate.trust(source.ID())
	dest.findPeers(ctx, func(addrs []ma.Multiaddr) []ma.Multiaddr { return addrs })
	data := testData(700000)
	d := cas.Sum(data)
	if err := sourceStore.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if err := dest.Fetch(ctx, d); err != nil {
		t.Fatal(err)
	}
	if source.host.Network().Connectedness(bootstrap.ID()) != network.NotConnected || dest.host.Network().Connectedness(bootstrap.ID()) != network.NotConnected {
		t.Fatal("bootstrap peer entered content host")
	}
	if len(router.keys) != 1 || router.keys[0] != rendezvousCID {
		t.Fatal("content transfer used public routing")
	}
}

func TestPublicDiscoveryAddressPolicy(t *testing.T) {
	for _, value := range []string{
		"/ip4/127.0.0.1/tcp/1234", "/ip4/10.0.0.1/tcp/1234", "/ip6/::1/tcp/1234",
		"/ip6/::ffff:127.0.0.1/tcp/1234", "/dns4/example.com/tcp/1234", "/ip4/8.8.8.8/udp/1234/quic-v1",
		"/ip6/64:ff9b::7f00:1/tcp/1234", "/ip6/64:ff9b:1::7f00:1/tcp/1234",
		"/ip6/2002:7f00:1::/tcp/1234", "/ip6/2001::7f00:1/tcp/1234",
		"/ip6/::7f00:1/tcp/1234", "/ip6/::ffff:0:7f00:1/tcp/1234",
		"/ip6/2606:4700:4700::5efe:7f00:1/tcp/1234",
	} {
		if publicAddress(ma.StringCast(value)) {
			t.Fatalf("accepted unsafe discovery address: %s", value)
		}
	}
	if !publicAddress(ma.StringCast("/ip4/8.8.8.8/tcp/1234")) {
		t.Fatal("rejected public literal TCP address")
	}
	if !publicAddress(ma.StringCast("/ip6/2606:4700:4700::1111/tcp/1234")) {
		t.Fatal("rejected native public IPv6 address")
	}
	gate := &dialGate{trusted: make(map[peer.ID]bool), max: 1}
	if gate.InterceptAddrDial("untrusted", ma.StringCast("/ip4/127.0.0.1/tcp/1234")) {
		t.Fatal("identify update bypassed public dial filter")
	}
	if !gate.trust("explicit") || !gate.InterceptAddrDial("explicit", ma.StringCast("/ip4/127.0.0.1/tcp/1234")) || gate.trust("too-many") {
		t.Fatal("explicit peer policy failed")
	}
}

type managedTestConn struct {
	network.Conn
	id     peer.ID
	cm     *connmgr.BasicConnMgr
	scope  network.ConnManagementScope
	once   sync.Once
	closed atomic.Bool
}

func (c *managedTestConn) RemotePeer() peer.ID { return c.id }
func (c *managedTestConn) Stat() network.ConnStats {
	return network.ConnStats{Stats: network.Stats{Direction: network.DirOutbound}}
}
func (c *managedTestConn) CloseWithError(network.ConnErrorCode) error {
	c.once.Do(func() { c.closed.Store(true); c.scope.Done(); c.cm.Notifee().Disconnected(nil, c) })
	return nil
}

func TestDiscoveryConnectionSaturationAndRecovery(t *testing.T) {
	// Explicit trims make this test deterministic; production trims every second.
	cm, err := discoveryConnectionManager(connmgr.WithSilencePeriod(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	defer cm.Close()
	rm, err := resourceManagerWithPeerLimit(discoveryConnections, 1)
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
		endpoint := ma.StringCast(fmt.Sprintf("/ip6/2001:db8:%x::1/tcp/1234", i+1))
		scope, err := rm.OpenConnection(network.DirOutbound, true, endpoint)
		if err != nil {
			t.Fatalf("connection %d: %v", i, err)
		}
		id := peer.ID(routingCID(cas.Sum([]byte(fmt.Sprintf("fixture-%d", i))).Hash).Hash())
		if err := scope.SetPeer(id); err != nil {
			scope.Done()
			t.Fatal(err)
		}
		c := &managedTestConn{id: id, cm: cm, scope: scope}
		conns = append(conns, c)
		cm.Notifee().Connected(nil, c)
		return c
	}
	for i := range discoveryConnections {
		c := connect(i)
		if i < 40 {
			cm.Protect(c.id, "kbucket")
		}
	}
	if extra, err := rm.OpenConnection(network.DirOutbound, true, ma.StringCast("/ip6/2001:db8:ffff::1/tcp/1234")); err == nil {
		extra.Done()
		t.Fatal("hard discovery connection limit not enforced")
	}
	cm.TrimOpenConns(context.Background())
	if got := cm.GetInfo().ConnCount; got != 48 {
		t.Fatalf("pruning should retain 40 protected and 8 transient peers, got %d", got)
	}
	for _, c := range conns[:40] {
		if c.closed.Load() || !cm.IsProtected(c.id, "kbucket") {
			t.Fatal("pruning closed a protected routing connection")
		}
	}
	for i := discoveryConnections; i < discoveryConnections+8; i++ {
		connect(i)
	}
	cm.TrimOpenConns(context.Background())
	if got := cm.GetInfo().ConnCount; got != 48 {
		t.Fatalf("transient connection slots did not recover, got %d", got)
	}
	duplicate, err := rm.OpenConnection(network.DirOutbound, true, ma.StringCast("/ip6/2001:db8:fffe::1/tcp/1234"))
	if err != nil {
		t.Fatal(err)
	}
	defer duplicate.Done()
	if err := duplicate.SetPeer(conns[0].id); err == nil {
		t.Fatal("discovery allows duplicate connections to a protected peer")
	}
}

func TestWireAndCIDConstants(t *testing.T) {
	if ManifestProtocol != "/bushel/manifest/1.0.0" || bitswapPrefix+"/ipfs/bitswap/1.2.0" != "/bushel/ipfs/bitswap/1.2.0" {
		t.Fatal("wire protocol changed")
	}
	routingKey := routingCID(cas.Sum([]byte("hello")).Hash)
	if routingKey.String() != "bafkreibm6jg3ux5qumhcn2b3flc3tyu6dmlb4xa7u5bf44yegnrjhc4yeq" {
		t.Fatalf("routing CID changed: %s", routingKey)
	}
	if _, err := sizedChunkDigest(cid.NewCidV0(routingKey.Hash())); err == nil {
		t.Fatal("accepted non-v1 CID")
	}
	if rendezvousCID.String() != "bafkreihn2psfrf4egr4yjmnp4vckba2frqub2dwm4obcxmggytceetbgqq" {
		t.Fatal("rendezvous identity changed")
	}
	key, err := sizedChunkCID(cas.Sum([]byte("hello")))
	if err != nil {
		t.Fatal(err)
	}
	ch := (noProviders{}).FindProvidersAsync(context.Background(), key, 1)
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("unexpected content provider")
		}
	default:
		t.Fatal("disabled content routing did not finish")
	}
}

func BenchmarkBitswapTransfer(b *testing.B) {
	source, store := testNode(b)
	data := testData(3 << 20)
	d := cas.Sum(data)
	ctx := context.Background()
	if err := store.Put(ctx, d, bytes.NewReader(data)); err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	destRoot := filepath.Join(b.TempDir(), "destination")
	for b.Loop() {
		b.StopTimer()
		func() {
			dest, destStore, err := newTestNode(destRoot, 64<<20)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { dest.Close(); destStore.Close(); os.RemoveAll(destRoot) }()
			if err := dest.Connect(ctx, peerInfo(source)); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
			err = dest.Fetch(ctx, d)
			b.StopTimer()
			if err != nil {
				b.Fatal(err)
			}
		}()
		b.StartTimer()
	}
}
