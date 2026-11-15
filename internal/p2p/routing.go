package p2p

import (
	"context"
	"encoding/binary"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/ipfs/go-cid"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-kad-dht/records"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	connmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
)

const (
	routingPrefix         protocol.ID = "/bushel/routing/v1"
	RoutingProtocol       protocol.ID = routingPrefix + "/kad/1.0.0"
	providerDomain                    = "bushel:blob-provider:v1\x00"
	providerLifetime                  = 24 * time.Hour
	providerCandidates                = 16
	providerWorkers                   = 2
	providerLookupTimeout             = 30 * time.Second
)

// blobProviderRouter has no value operations and is never the public IPFS DHT.
// Its keys describe providers of blob manifests, not Bitswap chunk blocks.
type blobProviderRouter interface {
	Provide(context.Context, cid.Cid, bool) error
	FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.AddrInfo
}

func blobProviderCID(d cas.Digest) (cid.Cid, error) {
	if err := d.Validate(); err != nil {
		return cid.Undef, err
	}
	var data [len(providerDomain) + 40]byte
	copy(data[:], providerDomain)
	copy(data[len(providerDomain):], d.Hash[:])
	binary.BigEndian.PutUint64(data[len(providerDomain)+32:], uint64(d.Size))
	return chunkCID(cas.Sum(data[:]).Hash), nil
}

func routingBucketSize(maxPeers int) int { return min(8, maxPeers/8) }

func contentConnectionManager(maxPeers int, options ...connmgr.Option) (*connmgr.BasicConnMgr, error) {
	// The DHT protects at most two buckets, with up to two connections per
	// peer. The low watermark excludes protected peers; leave room for new
	// routes and concurrent transfers even at the minimum connection limit.
	low, high := max(0, maxPeers/8), max(1, maxPeers*3/4)
	opts := []connmgr.Option{connmgr.WithGracePeriod(0), connmgr.WithSilencePeriod(time.Second)}
	return connmgr.NewConnManager(low, high, append(opts, options...)...)
}

func routingPeerstore() (peerstore.Peerstore, error) {
	return pstoremem.NewPeerstore(pstoremem.WithMaxAddresses(16384),
		pstoremem.WithMaxAddressesPerPeer(8), pstoremem.WithMaxSignedPeerRecords(1024))
}

func (n *Node) initRouting(cfg Config) error {
	store, err := newRoutingStore(routingStoreLimits{
		MaxRecords: cfg.RoutingRecords, MaxBytes: cfg.RoutingRecords * 256, MaxKeyBytes: 256, MaxValueBytes: 16,
		MaxProvidersPerKey: 16, MaxKeysPerProvider: cfg.RoutingRecordsPerPeer, MaxBatchRecords: 320,
		MaxPendingRecords: 640, MaxPendingBytes: 256 << 10, MaxSnapshots: 4,
	})
	if err != nil {
		return err
	}
	n.providerData = store
	// Explicit bootstrap peers prevent fallback to the public IPFS bootstrap
	// list. Rendezvous connections also populate this independent routing table.
	opts := []dht.Option{dht.ProtocolPrefix(routingPrefix), dht.Mode(dht.ModeServer), dht.DisableValues(),
		dht.BootstrapPeers(cfg.Peers...), dht.BucketSize(routingBucketSize(n.maxPeers)), dht.Concurrency(3), dht.Resiliency(min(2, routingBucketSize(n.maxPeers))),
		dht.LookupCheckConcurrency(2), dht.RoutingTableRefreshQueryTimeout(10 * time.Second), dht.RoutingTableRefreshPeriod(5 * time.Minute),
		dht.RoutingTablePersistence(0),
		dht.AddressFilter(n.filterAddresses), dht.ProviderDatastore(store),
		dht.ProviderManagerOpts(records.Cache(noProviderCache{}), records.ProvideValidity(providerLifetime),
			records.ProviderAddrTTL(providerLifetime), records.CleanupInterval(15*time.Minute))}
	if cfg.disableRoutingRefresh {
		opts = append(opts, dht.DisableAutoRefresh())
	}
	router, err := dht.New(n.host, opts...)
	if err != nil {
		return err
	}
	n.overlay = router
	n.blobRouter = router
	return nil
}

func (n *Node) announceBlob(ctx context.Context, d cas.Digest) error {
	// A manifest can disappear after a queued hint. Stale announcements are
	// ordinary availability misses, but avoid creating one when already known.
	if _, err := n.store.Manifest(d); err != nil {
		if errors.Is(err, cas.ErrNotFound) || errors.Is(err, cas.ErrCorrupt) {
			return nil
		}
		return err
	}
	key, err := blobProviderCID(d)
	if err != nil {
		return err
	}
	return n.blobRouter.Provide(ctx, key, true)
}

func (n *Node) startAnnouncements(opts AnnouncerOptions) error {
	opts.Ready = func() bool {
		return n.overlay.RoutingTable().Size() != 0 && len(n.filterAddresses(n.host.Addrs())) != 0
	}
	a, err := NewAnnouncer(n.store, n.announceBlob, opts)
	if err != nil {
		return err
	}
	n.announcer.Store(a)
	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		for {
			if err := a.Run(n.ctx); n.ctx.Err() == nil {
				slog.Error("Blob catalog scan failed; announcement scan will restart", "error", err)
			} else {
				return
			}
			timer := time.NewTimer(min(time.Minute, a.opts.RecoveryInterval))
			select {
			case <-n.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			// Each run owns a fresh subscription, queue and catalog iterator.
			next, err := NewAnnouncer(n.store, n.announceBlob, a.opts)
			if err != nil {
				slog.Error("Cannot restart blob announcements", "error", err)
				return
			}
			a = next
			n.announcer.Store(a)
		}
	}()
	return nil
}

// RoutingStats describes the bounded, ephemeral provider index. PayloadBytes
// excludes Go map/list overhead and the separately bounded pending buffers.
type RoutingStats struct {
	Records      int
	PayloadBytes int
	Evictions    uint64
}

// RoutingStats reports index occupancy and capacity evictions since startup.
// It is safe to call during concurrent operation.
func (n *Node) RoutingStats() RoutingStats {
	s := n.providerData
	s.mu.Lock()
	defer s.mu.Unlock()
	return RoutingStats{Records: len(s.records), PayloadBytes: s.bytes, Evictions: s.evictions}
}

// AnnouncementStats describes the current announcement task. A task restarted
// after a catalog error has new counters and starts a fresh durable scan.
func (n *Node) AnnouncementStats() AnnouncerStats {
	if a := n.announcer.Load(); a != nil {
		return a.Stats()
	}
	return AnnouncerStats{}
}

func (n *Node) reportRoutingPressure() {
	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	var previous uint64
	for {
		select {
		case <-n.ctx.Done():
			return
		case <-tick.C:
			stats := n.RoutingStats()
			if stats.Evictions != previous {
				slog.Warn("Routing index capacity evicted provider hints", "records", stats.Records,
					"payload_bytes", stats.PayloadBytes, "evictions", stats.Evictions-previous)
				previous = stats.Evictions
			}
			if a := n.announcer.Load(); a != nil {
				progress := a.Stats()
				duration := progress.LastSweepDuration
				if progress.Sweeping {
					duration = a.opts.Clock.Now().Sub(progress.SweepStarted)
				}
				if duration*2+a.opts.SweepInterval >= providerLifetime {
					slog.Warn("Blob catalog renewal risks provider expiry", "sweep_duration", duration, "queued", progress.Queued)
				} else if progress.Sweeping || progress.Queued != 0 {
					slog.Info("Blob announcements progressing", "queued", progress.Queued, "sweep_duration", duration,
						"completed_attempts", progress.Announced, "failures", progress.Failures, "dropped_hint_lower_bound", progress.DroppedHints)
				}
			}
		}
	}
}

// lookupBlobManifests runs the existing direct-peer path alongside overlay
// lookup. A slow routing query cannot delay a hit from an already known peer.
func (n *Node) lookupBlobManifests(ctx context.Context, ids []peer.ID, d cas.Digest) (<-chan manifestResult, func()) {
	ctx, cancel := context.WithCancel(ctx)
	direct, stopDirect := n.lookupManifests(ctx, ids, d)
	routed, stopRouted := n.lookupRoutedManifests(ctx, ids, d)
	results := make(chan manifestResult, manifestWorkers+providerWorkers)
	var workers sync.WaitGroup
	for _, input := range []<-chan manifestResult{direct, routed} {
		workers.Go(func() {
			for result := range input {
				select {
				case results <- result:
				case <-ctx.Done():
					return
				}
			}
		})
	}
	done := make(chan struct{})
	go func() { workers.Wait(); close(results); close(done) }()
	return results, func() { cancel(); stopDirect(); stopRouted(); <-done }
}

func (n *Node) lookupRoutedManifests(ctx context.Context, known []peer.ID, d cas.Digest) (<-chan manifestResult, func()) {
	ctx, cancel := context.WithTimeout(ctx, min(n.timeout, providerLookupTimeout))
	results := make(chan manifestResult, providerWorkers)
	jobs := make(chan peer.AddrInfo, providerWorkers)
	var workers sync.WaitGroup
	workers.Go(func() {
		defer close(jobs)
		key, err := blobProviderCID(d)
		if err != nil {
			return
		}
		seen := make(map[peer.ID]bool, len(known)+providerCandidates+1)
		seen[n.ID()] = true
		for _, id := range known {
			seen[id] = true
		}
		providers := n.blobRouter.FindProvidersAsync(ctx, key, providerCandidates)
		for range providerCandidates {
			select {
			case <-ctx.Done():
				return
			case info, ok := <-providers:
				if !ok {
					return
				}
				if seen[info.ID] || info.ID.Validate() != nil {
					continue
				}
				seen[info.ID] = true
				info.Addrs = n.filterAddresses(info.Addrs)
				if len(info.Addrs) == 0 {
					continue
				}
				select {
				case jobs <- info:
				case <-ctx.Done():
					return
				}
			}
		}
	})
	for range providerWorkers {
		workers.Go(func() {
			for info := range jobs {
				attempt, stop := context.WithTimeout(ctx, min(n.timeout, maxManifestTime))
				// Referrals never acquire the trust granted to explicitly configured
				// peers. The dial gate rechecks every address, including Identify updates.
				err := n.connect(attempt, info)
				var manifest cas.Manifest
				if err == nil {
					manifest, err = n.manifest(attempt, info.ID, d)
				}
				stop()
				select {
				case results <- manifestResult{manifest, err}:
				case <-ctx.Done():
					return
				}
			}
		})
	}
	done := make(chan struct{})
	go func() { workers.Wait(); close(results); close(done) }()
	return results, func() { cancel(); <-done }
}
