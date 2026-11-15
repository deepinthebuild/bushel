package p2p

import (
	"context"
	"log/slog"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	connmgr "github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	ma "github.com/multiformats/go-multiaddr"
)

var rendezvousCID = chunkCID(cas.Sum([]byte("bushel:peer-rendezvous:v1")).Hash)

const discoveryConnections = 64

func discoveryConnectionManager(options ...connmgr.Option) (*connmgr.BasicConnMgr, error) {
	// The pinned DHT protects at most 40 peers (two 20-peer buckets). Leave
	// protected routing peers plus transient queries below the hard limit.
	// libp2p's low watermark counts unprotected candidates, so 40 + 8 <= 48.
	opts := []connmgr.Option{connmgr.WithGracePeriod(0), connmgr.WithSilencePeriod(time.Second)}
	return connmgr.NewConnManager(8, 48, append(opts, options...)...)
}

// This interface is used only for the one fixed application rendezvous CID.
// It is never passed to Bitswap.
type rendezvousRouter interface {
	Provide(context.Context, cid.Cid, bool) error
	FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.AddrInfo
	Close() error
}

func (n *Node) discoveryHost(cfg Config) (host.Host, error) {
	rm, err := resourceManagerWithPeerLimit(discoveryConnections, 1)
	if err != nil {
		return nil, err
	}
	cm, err := discoveryConnectionManager()
	if err != nil {
		_ = rm.Close()
		return nil, err
	}
	gate := &dialGate{trusted: make(map[peer.ID]bool), max: 16}
	for _, info := range cfg.Bootstrap {
		gate.trust(info.ID)
	}
	ps, err := routingPeerstore()
	if err != nil {
		_ = rm.Close()
		_ = cm.Close()
		return nil, err
	}
	h, err := libp2p.New(libp2p.Identity(cfg.PrivateKey), libp2p.NoListenAddrs,
		libp2p.NoTransports, libp2p.Transport(tcp.NewTCPTransport), libp2p.DisableRelay(),
		libp2p.Peerstore(ps), libp2p.ResourceManager(rm), libp2p.ConnectionManager(cm), libp2p.ConnectionGater(gate), libp2p.UserAgent("bushel-discovery/1"),
		// The hosts authenticate as the same peer, but provider records refer to
		// the content listener. No discovery connection carries Bitswap traffic.
		libp2p.AddrsFactory(func([]ma.Multiaddr) []ma.Multiaddr { return n.host.Addrs() }))
	if err != nil {
		_ = ps.Close()
		_ = rm.Close()
		_ = cm.Close()
	}
	return h, err
}

func (n *Node) initDiscovery(cfg Config) error {
	var err error
	n.discovery, err = n.discoveryHost(cfg)
	if err != nil {
		return err
	}
	bootstrap := cfg.Bootstrap
	if len(bootstrap) == 0 {
		bootstrap = dht.GetDefaultBootstrapPeerAddrInfos()
	}
	// The public /ipfs DHT requires its default value support and validators,
	// even in client mode. Bushel exposes only rendezvous provider operations
	// through rendezvousRouter and serves no inbound DHT requests.
	router, err := dht.New(n.discovery, dht.Mode(dht.ModeClient), dht.DisableAutoRefresh(),
		dht.BootstrapPeers(bootstrap...), dht.QueryFilter(dht.PublicQueryFilter), dht.RoutingTableFilter(dht.PublicRoutingTableFilter),
		dht.AddressFilter(publicAddresses))
	if err != nil {
		return err
	}
	n.router = router
	return nil
}

func (n *Node) discover(bootstrap []peer.AddrInfo, nextProvide *time.Time) {
	ctx, cancel := context.WithTimeout(n.ctx, n.timeout)
	defer cancel()
	if len(n.discovery.Network().Peers()) == 0 {
		if len(bootstrap) == 0 {
			bootstrap = dht.GetDefaultBootstrapPeerAddrInfos()
		}
		for _, info := range bootstrap[:min(len(bootstrap), 8)] {
			if err := n.discovery.Connect(ctx, info); err != nil && ctx.Err() == nil {
				slog.Debug("IPFS bootstrap peer unavailable", "peer", info.ID)
			}
		}
	}
	if time.Now().After(*nextProvide) && len(publicAddresses(n.host.Addrs())) != 0 {
		// Failed announcements back off for at least 15 minutes; successful
		// announcements renew every 12 hours, independently of blob activity.
		*nextProvide = time.Now().Add(15 * time.Minute)
		if err := n.router.Provide(ctx, rendezvousCID, true); err == nil {
			*nextProvide = time.Now().Add(12 * time.Hour)
		}
	}
	if len(n.host.Network().Peers()) >= n.maxPeers {
		return
	}
	n.findPeers(ctx, publicAddresses)
}

func (n *Node) findPeers(ctx context.Context, filter func([]ma.Multiaddr) []ma.Multiaddr) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	providers := n.router.FindProvidersAsync(ctx, rendezvousCID, n.maxPeers)
	for count := 0; count < n.maxPeers; count++ {
		select {
		case <-ctx.Done():
			return
		case info, ok := <-providers:
			if !ok {
				return
			}
			if info.ID == n.ID() {
				continue
			}
			info.Addrs = filter(info.Addrs)
			if len(info.Addrs) == 0 {
				continue
			}
			if err := n.connect(ctx, info); err != nil && ctx.Err() == nil {
				slog.Debug("Discovered Bushel peer unavailable", "peer", info.ID)
			}
		}
	}
}
