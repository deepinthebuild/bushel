package p2p

import (
	"net/netip"
	"sync"

	"github.com/libp2p/go-libp2p/core/control"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	rcmgr "github.com/libp2p/go-libp2p/p2p/host/resource-manager"
	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// Translation and tunneling can turn an apparently public IPv6 destination
// into a private IPv4 dial. Bushel discovery uses native public IP endpoints.
var transitionPrefixes = []netip.Prefix{
	netip.MustParsePrefix("64:ff9b::/96"),
	netip.MustParsePrefix("64:ff9b:1::/48"),
	netip.MustParsePrefix("2002::/16"),
	netip.MustParsePrefix("2001::/32"),
	netip.MustParsePrefix("::/96"),
	netip.MustParsePrefix("::ffff:0:0/96"),
	netip.MustParsePrefix("::ffff:0:0:0/96"),
}

// Public discovery accepts only literal, publicly routable TCP endpoints.
// Filtering again at dial time prevents identify updates from bypassing it.
func publicAddress(addr ma.Multiaddr) bool {
	protocols := addr.Protocols()
	if len(protocols) != 2 || (protocols[0].Code != ma.P_IP4 && protocols[0].Code != ma.P_IP6) || protocols[1].Code != ma.P_TCP {
		return false
	}
	if protocols[0].Code == ma.P_IP6 {
		literal, err := addr.ValueForProtocol(ma.P_IP6)
		if err != nil {
			return false
		}
		ip, err := netip.ParseAddr(literal)
		if err != nil {
			return false
		}
		for _, prefix := range transitionPrefixes {
			if prefix.Contains(ip) {
				return false
			}
		}
		// ISATAP embeds IPv4 in the interface identifier under an arbitrary
		// unicast prefix, so it cannot be represented by a fixed prefix above.
		b := ip.As16()
		if (b[8] == 0 || b[8] == 2) && b[9] == 0 && b[10] == 0x5e && b[11] == 0xfe {
			return false
		}
	}
	return manet.IsPublicAddr(addr)
}

func publicAddresses(addrs []ma.Multiaddr) []ma.Multiaddr {
	out := make([]ma.Multiaddr, 0, min(len(addrs), 8))
	for _, addr := range addrs {
		if publicAddress(addr) {
			out = append(out, addr)
			if len(out) == 8 {
				break
			}
		}
	}
	return out
}

type dialGate struct {
	mu      sync.RWMutex
	trusted map[peer.ID]bool
	max     int
}

func (g *dialGate) trust(id peer.ID) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.trusted[id] && len(g.trusted) >= g.max {
		return false
	}
	g.trusted[id] = true
	return true
}

func (*dialGate) InterceptPeerDial(peer.ID) bool { return true }
func (g *dialGate) InterceptAddrDial(id peer.ID, addr ma.Multiaddr) bool {
	g.mu.RLock()
	trusted := g.trusted[id]
	g.mu.RUnlock()
	return trusted || publicAddress(addr)
}
func (*dialGate) InterceptAccept(network.ConnMultiaddrs) bool { return true }
func (*dialGate) InterceptSecured(network.Direction, peer.ID, network.ConnMultiaddrs) bool {
	return true
}
func (*dialGate) InterceptUpgraded(network.Conn) (bool, control.DisconnectReason) {
	return true, 0
}

func resourceManager(peers int) (network.ResourceManager, error) {
	return resourceManagerWithPeerLimit(peers, 2)
}

func resourceManagerWithPeerLimit(peers, perPeer int) (network.ResourceManager, error) {
	limits := rcmgr.PartialLimitConfig{
		System: rcmgr.ResourceLimits{
			Conns: rcmgr.LimitVal(peers), ConnsInbound: rcmgr.LimitVal(peers), ConnsOutbound: rcmgr.LimitVal(peers),
			Streams: rcmgr.LimitVal(8 * peers), FD: rcmgr.LimitVal(peers), Memory: rcmgr.LimitVal64(64 << 20),
		},
		PeerDefault: rcmgr.ResourceLimits{Conns: rcmgr.LimitVal(perPeer), Streams: 16, Memory: rcmgr.LimitVal64(8 << 20)},
	}.Build(rcmgr.DefaultLimits.AutoScale())
	return rcmgr.NewResourceManager(rcmgr.NewFixedLimiter(limits))
}
