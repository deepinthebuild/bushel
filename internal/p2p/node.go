// Package p2p bridges Bushel manifests and chunks to libp2p and Bitswap.
package p2p

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/ipfs/boxo/bitswap"
	"github.com/ipfs/boxo/bitswap/network/bsnet"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"
)

const (
	ManifestProtocol protocol.ID = "/bushel/manifest/1.0.0"
	bitswapPrefix    protocol.ID = "/bushel"
	fetchWindow                  = 8
	manifestWorkers              = 4
	maxManifestTime              = 5 * time.Second
	maxFetchTime                 = 5 * time.Minute
)

type Config struct {
	Store          *cas.Store
	PrivateKey     crypto.PrivKey
	Listen         []string
	Bootstrap      []peer.AddrInfo
	Peers          []peer.AddrInfo
	Discovery      bool
	RequestTimeout time.Duration
	// MaxPeers bounds content connections, from 8 to 128 (default 64).
	MaxPeers int
	// RoutingRecords bounds cached blob/provider pairs, from 64 to 1048576
	// (default 32768). Payload storage is additionally bounded to 256 times this.
	RoutingRecords int
	// RoutingRecordsPerPeer bounds records from one provider, from 1 to
	// RoutingRecords. Zero defaults to min(16384, RoutingRecords).
	RoutingRecordsPerPeer int
	// Tests can drive maintenance without real renewal intervals. Production
	// callers outside this package use the bounded defaults.
	announcements         AnnouncerOptions
	disableRoutingRefresh bool
}

type Node struct {
	store           *cas.Store
	host            host.Host
	discovery       host.Host
	router          rendezvousRouter
	overlay         *dht.IpfsDHT
	blobRouter      blobProviderRouter
	providerData    *routingStore
	peerstore       io.Closer
	filterAddresses func([]ma.Multiaddr) []ma.Multiaddr
	announcer       atomic.Pointer[Announcer]
	gate            *dialGate
	bitswap         *bitswap.Bitswap
	ctx             context.Context
	cancel          context.CancelFunc
	timeout         time.Duration
	maxPeers        int
	transfers       chan struct{}
	lookups         chan struct{}
	mu              sync.Mutex
	closing         bool
	wg              sync.WaitGroup
	closeOnce       sync.Once
	closeErr        error
}

func New(ctx context.Context, cfg Config) (*Node, error) {
	return newNode(ctx, cfg, publicAddresses)
}

// newNode accepts an address policy so isolated tests can use loopback peers.
// The public constructor always enforces the public-IP/TCP policy on referrals.
func newNode(ctx context.Context, cfg Config, filter func([]ma.Multiaddr) []ma.Multiaddr) (_ *Node, err error) {
	if cfg.Store == nil || cfg.PrivateKey == nil {
		return nil, errors.New("store and private key are required")
	}
	if cfg.MaxPeers == 0 {
		cfg.MaxPeers = 64
	}
	if cfg.MaxPeers < 8 || cfg.MaxPeers > 128 || len(cfg.Peers) > cfg.MaxPeers || len(cfg.Bootstrap) > 16 {
		return nil, errors.New("invalid peer limits")
	}
	if cfg.RoutingRecords == 0 {
		cfg.RoutingRecords = 32768
	}
	if cfg.RoutingRecordsPerPeer == 0 {
		cfg.RoutingRecordsPerPeer = min(16384, cfg.RoutingRecords)
	}
	if cfg.RoutingRecords < 64 || cfg.RoutingRecords > 1<<20 || cfg.RoutingRecordsPerPeer < 1 || cfg.RoutingRecordsPerPeer > cfg.RoutingRecords {
		return nil, errors.New("invalid routing index capacity")
	}
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 30 * time.Second
	}
	if cfg.RequestTimeout <= 0 || cfg.RequestTimeout > maxFetchTime {
		return nil, errors.New("request timeout must be in (0, 5m]")
	}
	if len(cfg.Listen) == 0 {
		cfg.Listen = []string{"/ip4/0.0.0.0/tcp/0", "/ip6/::/tcp/0"}
	}
	ctx, cancel := context.WithCancel(ctx)
	n := &Node{store: cfg.Store, ctx: ctx, cancel: cancel, timeout: cfg.RequestTimeout, maxPeers: cfg.MaxPeers, filterAddresses: filter,
		transfers: make(chan struct{}, 4), lookups: make(chan struct{}, 16), gate: &dialGate{trusted: make(map[peer.ID]bool), max: cfg.MaxPeers}}
	n.gate.filter = filter
	for _, info := range cfg.Peers {
		n.gate.trust(info.ID)
	}
	defer func() {
		if err != nil {
			_ = n.Close()
		}
	}()
	rm, err := resourceManager(cfg.MaxPeers)
	if err != nil {
		return nil, err
	}
	cm, err := contentConnectionManager(cfg.MaxPeers)
	if err != nil {
		_ = rm.Close()
		return nil, err
	}
	ps, err := routingPeerstore()
	if err != nil {
		_ = cm.Close()
		_ = rm.Close()
		return nil, err
	}
	n.peerstore = ps
	n.host, err = libp2p.New(libp2p.Identity(cfg.PrivateKey), libp2p.ListenAddrStrings(cfg.Listen...),
		libp2p.NoTransports, libp2p.Transport(tcp.NewTCPTransport), libp2p.DisableRelay(),
		libp2p.Peerstore(ps), libp2p.ConnectionManager(cm), libp2p.ResourceManager(rm), libp2p.ConnectionGater(n.gate), libp2p.UserAgent("bushel/1"))
	if err != nil {
		_ = cm.Close()
		_ = rm.Close()
		return nil, err
	}
	// BasicHost owns and closes the supplied peerstore after construction.
	n.peerstore = nil
	n.host.SetStreamHandler(ManifestProtocol, n.serveManifest)
	n.bitswap = bitswap.New(ctx, bsnet.NewFromIpfsHost(n.host, bsnet.Prefix(bitswapPrefix), bsnet.SupportedProtocols([]protocol.ID{bsnet.ProtocolBitswap})),
		noProviders{}, readStore{store: cfg.Store, timeout: cfg.RequestTimeout}, bitswap.EngineBlockstoreWorkerCount(4), bitswap.EngineTaskWorkerCount(4), bitswap.TaskWorkerCount(4),
		bitswap.MaxOutstandingBytesPerPeer(2<<20), bitswap.MaxQueuedWantlistEntriesPerPeer(256), bitswap.MaxCidSize(sizedChunkCIDSize),
		bitswap.WithTargetMessageSize(1<<20), bitswap.WithoutDuplicatedBlockStats(),
		bitswap.WithPeerBlockRequestFilter(func(_ peer.ID, c cid.Cid) bool { _, err := sizedChunkDigest(c); return err == nil }))
	if err := n.initRouting(cfg); err != nil {
		return nil, err
	}
	if cfg.Discovery {
		if err := n.initDiscovery(cfg); err != nil {
			return nil, err
		}
	}
	if cfg.Discovery || len(cfg.Peers) > 0 {
		n.wg.Add(1)
		go func() { defer n.wg.Done(); n.maintain(cfg) }()
	}
	if err := n.startAnnouncements(cfg.announcements); err != nil {
		return nil, err
	}
	n.wg.Add(1)
	go func() { defer n.wg.Done(); n.reportRoutingPressure() }()
	return n, nil
}

func (n *Node) ID() peer.ID { return n.host.ID() }

func (n *Node) Addrs() []string {
	addrs, err := peer.AddrInfoToP2pAddrs(&peer.AddrInfo{ID: n.ID(), Addrs: n.host.Addrs()})
	if err != nil {
		return nil
	}
	out := make([]string, len(addrs))
	for i, addr := range addrs {
		out[i] = addr.String()
	}
	return out
}

func (n *Node) start() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.closing {
		return false
	}
	n.wg.Add(1)
	return true
}

func (n *Node) Close() error {
	n.closeOnce.Do(func() {
		n.mu.Lock()
		n.closing = true
		n.cancel()
		n.mu.Unlock()
		if n.router != nil {
			n.closeErr = errors.Join(n.closeErr, n.router.Close())
		}
		if n.overlay != nil {
			n.closeErr = errors.Join(n.closeErr, n.overlay.Close())
		}
		if n.providerData != nil {
			n.closeErr = errors.Join(n.closeErr, n.providerData.Close())
		}
		if n.discovery != nil {
			n.closeErr = errors.Join(n.closeErr, n.discovery.Close())
		}
		if n.bitswap != nil {
			n.closeErr = errors.Join(n.closeErr, n.bitswap.Close())
		}
		if n.host != nil {
			n.closeErr = errors.Join(n.closeErr, n.host.Close())
		}
		n.wg.Wait()
		if n.peerstore != nil {
			n.closeErr = errors.Join(n.closeErr, n.peerstore.Close())
		}
	})
	return n.closeErr
}

// Connect is for explicitly configured peers, which may use private addresses.
// Discovered peers are filtered before connection and again at dial time.
func (n *Node) Connect(ctx context.Context, info peer.AddrInfo) error {
	if !n.gate.trust(info.ID) {
		return errors.New("too many explicit peers")
	}
	return n.connect(ctx, info)
}

func (n *Node) connect(ctx context.Context, info peer.AddrInfo) error {
	if info.ID == n.ID() {
		return nil
	}
	if err := info.ID.Validate(); err != nil || len(info.Addrs) > 8 {
		return errors.New("invalid peer address")
	}
	ctx, cancel := context.WithTimeout(ctx, n.timeout)
	defer cancel()
	stop := context.AfterFunc(n.ctx, cancel)
	defer stop()
	if err := n.host.Connect(ctx, info); err != nil {
		return err
	}
	// Negotiate Bushel before considering this connection useful for lookup.
	s, err := n.host.NewStream(ctx, info.ID, ManifestProtocol)
	if err != nil {
		_ = n.host.Network().ClosePeer(info.ID)
		return err
	}
	return s.Close()
}

func (n *Node) serveManifest(s network.Stream) {
	if !n.start() {
		_ = s.Reset()
		return
	}
	defer n.wg.Done()
	select {
	case n.lookups <- struct{}{}:
		defer func() { <-n.lookups }()
	default:
		_ = s.Reset()
		return
	}
	defer s.Close()
	ctx, cancel := context.WithTimeout(n.ctx, n.timeout)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { _ = s.Reset() })
	defer stop()
	deadline, _ := ctx.Deadline()
	_ = s.SetDeadline(deadline)
	var request [40]byte
	if _, err := io.ReadFull(s, request[:]); err != nil {
		_ = s.Reset()
		return
	}
	var extra [1]byte
	if _, err := s.Read(extra[:]); err != io.EOF {
		_ = s.Reset()
		return
	}
	d := cas.Digest{Size: int64(binary.BigEndian.Uint64(request[32:]))}
	copy(d.Hash[:], request[:32])
	if err := d.Validate(); err != nil {
		_ = s.Reset()
		return
	}
	m, err := n.store.Manifest(ctx, d)
	var payload []byte
	if err == nil {
		payload, err = m.MarshalBinary()
	}
	if err != nil && !errors.Is(err, cas.ErrNotFound) && !errors.Is(err, cas.ErrCorrupt) {
		_ = s.Reset()
		return
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
	if _, err := s.Write(header[:]); err != nil {
		return
	}
	if len(payload) != 0 {
		_, _ = s.Write(payload)
	}
}

func (n *Node) manifest(ctx context.Context, id peer.ID, d cas.Digest) (cas.Manifest, error) {
	ctx, cancel := context.WithTimeout(ctx, min(n.timeout, maxManifestTime))
	defer cancel()
	s, err := n.host.NewStream(ctx, id, ManifestProtocol)
	if err != nil {
		return cas.Manifest{}, err
	}
	defer s.Close()
	stop := context.AfterFunc(ctx, func() { _ = s.Reset() })
	defer stop()
	deadline, _ := ctx.Deadline()
	_ = s.SetDeadline(deadline)
	var request [40]byte
	copy(request[:32], d.Hash[:])
	binary.BigEndian.PutUint64(request[32:], uint64(d.Size))
	if _, err := s.Write(request[:]); err != nil {
		return cas.Manifest{}, err
	}
	if err := s.CloseWrite(); err != nil {
		return cas.Manifest{}, err
	}
	var header [4]byte
	if _, err := io.ReadFull(s, header[:]); err != nil {
		return cas.Manifest{}, err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size == 0 {
		var extra [1]byte
		if _, err := s.Read(extra[:]); err != io.EOF {
			return cas.Manifest{}, cas.ErrCorrupt
		}
		return cas.Manifest{}, cas.ErrNotFound
	}
	if size > cas.MaxManifestSize || size < cas.ManifestHeaderSize {
		return cas.Manifest{}, cas.ErrCorrupt
	}
	data := make([]byte, size)
	if _, err := io.ReadFull(s, data); err != nil {
		return cas.Manifest{}, err
	}
	var extra [1]byte
	if _, err := s.Read(extra[:]); err != io.EOF {
		return cas.Manifest{}, cas.ErrCorrupt
	}
	m, err := cas.ParseManifest(data)
	if err != nil || m.Blob != d {
		return cas.Manifest{}, cas.ErrCorrupt
	}
	return m, nil
}

type manifestResult struct {
	manifest cas.Manifest
	err      error
}

// lookupManifests prevents one silent peer from delaying every other peer.
// Both concurrent reads and buffered results are bounded by manifestWorkers.
func (n *Node) lookupManifests(ctx context.Context, ids []peer.ID, d cas.Digest) (<-chan manifestResult, func()) {
	ctx, cancel := context.WithCancel(ctx)
	jobs := make(chan peer.ID, len(ids))
	for _, id := range ids {
		jobs <- id
	}
	close(jobs)
	results := make(chan manifestResult, manifestWorkers)
	var workers sync.WaitGroup
	for range min(manifestWorkers, len(ids)) {
		workers.Go(func() {
			for id := range jobs {
				if ctx.Err() != nil {
					return
				}
				m, err := n.manifest(ctx, id, d)
				select {
				case results <- manifestResult{m, err}:
				case <-ctx.Done():
					return
				}
			}
		})
	}
	done := make(chan struct{})
	go func() {
		workers.Wait()
		close(results)
		close(done)
	}()
	return results, func() { cancel(); <-done }
}

// peerTransferError marks errors arising while obtaining bytes from peers.
// Untagged Import errors are local and must not be converted to cache misses.
type peerTransferError struct{ err error }

func (e *peerTransferError) Error() string { return "peer chunk transfer: " + e.err.Error() }
func (e *peerTransferError) Unwrap() error { return e.err }

func peerFailure(err error) bool {
	switch e := err.(type) {
	case *peerTransferError:
		return true
	case interface{ Unwrap() []error }:
		for _, child := range e.Unwrap() {
			if !peerFailure(child) {
				return false
			}
		}
		return true
	case interface{ Unwrap() error }:
		return peerFailure(e.Unwrap())
	default:
		return err == cas.ErrCorrupt
	}
}

func (n *Node) Fetch(ctx context.Context, d cas.Digest) error {
	if err := d.Validate(); err != nil {
		return err
	}
	if !n.start() {
		return context.Canceled
	}
	defer n.wg.Done()
	ctx, cancel := context.WithTimeout(ctx, maxFetchTime)
	defer cancel()
	stop := context.AfterFunc(n.ctx, cancel)
	defer stop()
	select {
	case n.transfers <- struct{}{}:
		defer func() { <-n.transfers }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if ok, err := n.store.Has(ctx, d); err != nil || ok {
		return err
	}
	ids := n.host.Network().Peers()
	slices.Sort(ids)
	if len(ids) > n.maxPeers {
		ids = ids[:n.maxPeers]
	}
	results, stopLookups := n.lookupBlobManifests(ctx, ids, d)
	defer stopLookups()
	hadCorruption := false
	for result := range results {
		if err := ctx.Err(); err != nil {
			return err
		}
		if result.err != nil {
			hadCorruption = hadCorruption || errors.Is(result.err, cas.ErrCorrupt)
			continue
		}
		err := n.importManifest(ctx, result.manifest)
		if err == nil {
			return nil
		}
		if !peerFailure(err) {
			return err
		}
		hadCorruption = hadCorruption || errors.Is(err, cas.ErrCorrupt)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if hadCorruption {
		return cas.ErrCorrupt
	}
	return fmt.Errorf("peer retrieval failed: %w", cas.ErrNotFound)
}

func (n *Node) importManifest(ctx context.Context, m cas.Manifest) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	session := n.bitswap.NewSession(ctx)
	next := 0
	var window map[cas.Digest][]byte
	return n.store.Import(ctx, m, func(ctx context.Context, d cas.Digest) ([]byte, error) {
		if next%fetchWindow == 0 {
			end := min(next+fetchWindow, len(m.Chunks))
			wanted := make(map[string]cas.Digest, end-next)
			keys := make([]cid.Cid, 0, end-next)
			window = make(map[cas.Digest][]byte, end-next)
			for _, c := range m.Chunks[next:end] {
				local, err := n.store.Block(ctx, c)
				if err != nil && !errors.Is(err, cas.ErrNotFound) && !errors.Is(err, cas.ErrCorrupt) {
					return nil, err
				}
				if err == nil && cas.Sum(local) == c {
					window[c] = local
					continue
				}
				key, err := sizedChunkCID(c)
				if err != nil {
					return nil, err
				}
				if _, ok := wanted[key.KeyString()]; !ok {
					keys = append(keys, key)
					wanted[key.KeyString()] = c
				}
			}
			if len(keys) != 0 {
				if err := n.receiveWindow(ctx, session, keys, wanted, window); err != nil {
					return nil, &peerTransferError{err}
				}
			}
		}
		next++
		data, ok := window[d]
		if !ok {
			return nil, cas.ErrCorrupt
		}
		return data, nil
	})
}

func (n *Node) receiveWindow(ctx context.Context, session interface {
	GetBlocks(context.Context, []cid.Cid) (<-chan blocks.Block, error)
}, keys []cid.Cid, wanted map[string]cas.Digest, window map[cas.Digest][]byte) error {
	ctx, cancel := context.WithTimeout(ctx, n.timeout)
	defer cancel()
	ch, err := session.GetBlocks(ctx, keys)
	if err != nil {
		return err
	}
	for len(wanted) > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case b, ok := <-ch:
			if !ok {
				return cas.ErrNotFound
			}
			d, ok := wanted[b.Cid().KeyString()]
			if !ok || cas.Sum(b.RawData()) != d {
				return cas.ErrCorrupt
			}
			window[d] = b.RawData()
			delete(wanted, b.Cid().KeyString())
		}
	}
	return nil
}

func (n *Node) maintain(cfg Config) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	var nextProvide time.Time
	for {
		for _, info := range cfg.Peers {
			if err := n.Connect(n.ctx, info); err != nil && n.ctx.Err() == nil {
				slog.Debug("Bushel peer unavailable", "peer", info.ID, "error", err)
			}
		}
		if n.router != nil {
			n.discover(cfg.Bootstrap, &nextProvide)
		}
		select {
		case <-n.ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
