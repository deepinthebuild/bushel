package p2p

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"sync"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	bsnetwork "github.com/ipfs/boxo/bitswap/network"
	bsserver "github.com/ipfs/boxo/bitswap/server"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

// SeedOfferProtocol carries one digest offer and its verified-admission receipt.
const SeedOfferProtocol protocol.ID = "/bushel/seed/1.0.0"

type seedService struct {
	node                  *Node
	network               bsnetwork.BitSwapNetwork
	server                *bsserver.Server
	controls, outgoing    chan struct{}
	mu                    sync.Mutex
	receiveRate, sendRate seedRate
	admission             seedAdmission
	wireMu                sync.Mutex
	pendingMu             sync.Mutex
	pending               map[*seedWindow]struct{}
	servingMu             sync.Mutex
	requestMu             sync.Mutex
	grants                []*seedGrant
}

// SeedPeers returns connected peers explicitly advertising seed acceptance.
// It performs no dialing or discovery. Callers may apply digest-based ranking.
func (n *Node) SeedPeers() []peer.ID {
	peers := n.host.Network().Peers()
	slices.Sort(peers)
	result := make([]peer.ID, 0, min(len(peers), seedCandidates))
	for _, id := range peers {
		if id == n.ID() {
			continue
		}
		if supported, err := n.host.Peerstore().SupportsProtocols(id, SeedOfferProtocol); err == nil && len(supported) != 0 {
			result = append(result, id)
			if len(result) == seedCandidates {
				break
			}
		}
	}
	return result
}

// Offer asks a connected, willing peer to pull a blob. A true receipt reports
// verified backend and catalog publication at that instant, not retention.
// At most one offer to each recipient may be active at a time.
func (n *Node) Offer(ctx context.Context, id peer.ID, d cas.Digest) (bool, error) {
	if err := d.Validate(); err != nil {
		return false, err
	}
	if err := id.Validate(); err != nil {
		return false, err
	}
	if !n.start() {
		return false, context.Canceled
	}
	defer n.wg.Done()
	if id == n.ID() || n.host.Network().Connectedness(id) != network.Connected {
		return false, nil
	}
	if supported, err := n.host.Peerstore().SupportsProtocols(id, SeedOfferProtocol); err != nil || len(supported) == 0 {
		return false, nil
	}
	svc := n.seeds
	svc.mu.Lock()
	allowed := svc.sendRate.allow(time.Now())
	svc.mu.Unlock()
	if !allowed {
		return false, nil
	}
	select {
	case svc.outgoing <- struct{}{}:
		defer func() { <-svc.outgoing }()
	default:
		return false, nil
	}
	ctx, cancel := context.WithTimeout(ctx, maxFetchTime)
	defer cancel()
	stopNode := context.AfterFunc(n.ctx, cancel)
	defer stopNode()
	grant := svc.beginGrant(ctx, id, d)
	if grant == nil {
		return false, nil
	}
	defer svc.endGrant(grant)
	lookupCtx, lookupCancel := context.WithTimeout(ctx, n.timeout)
	manifest, err := n.store.Manifest(lookupCtx, d)
	lookupCancel()
	if err == nil {
		if err := svc.authorizeGrant(grant, manifest); err != nil {
			return false, err
		}
	} else if !errors.Is(err, cas.ErrNotFound) && !errors.Is(err, cas.ErrCorrupt) {
		return false, err
	}
	// An empty grant still permits the receiver's verified-existing fast path.
	ctx = network.WithNoDial(ctx, "seed offers use existing connections")
	s, err := n.host.NewStream(ctx, id, SeedOfferProtocol)
	if err != nil {
		return false, err
	}
	defer s.Close()
	stopStream := context.AfterFunc(ctx, func() { _ = s.Reset() })
	defer stopStream()
	deadline, _ := ctx.Deadline()
	_ = s.SetDeadline(deadline)
	var request [40]byte
	copy(request[:32], d.Hash[:])
	binary.BigEndian.PutUint64(request[32:], uint64(d.Size))
	if _, err := s.Write(request[:]); err != nil {
		return false, err
	}
	// Keep the request side open: reset/EOF cancels the receiver's pull.
	var response [1]byte
	if _, err := io.ReadFull(s, response[:]); err != nil {
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		return false, err
	}
	if response[0] > 1 {
		return false, cas.ErrCorrupt
	}
	var extra [1]byte
	if _, err := s.Read(extra[:]); err != io.EOF {
		return false, cas.ErrCorrupt
	}
	return response[0] == 1, nil
}

func parseSeedOffer(request []byte) (cas.Digest, error) {
	if len(request) != 40 {
		return cas.Digest{}, cas.ErrInvalid
	}
	d := cas.Digest{Size: int64(binary.BigEndian.Uint64(request[32:]))}
	copy(d.Hash[:], request[:32])
	return d, d.Validate()
}

func (n *Node) serveSeedOffer(s network.Stream) {
	if !n.start() {
		_ = s.Reset()
		return
	}
	defer n.wg.Done()
	defer s.Close()
	svc := n.seeds
	svc.mu.Lock()
	allowed := svc.receiveRate.allow(time.Now())
	svc.mu.Unlock()
	if !allowed {
		_ = s.Reset()
		return
	}
	select {
	case svc.controls <- struct{}{}:
		defer func() { <-svc.controls }()
	default:
		_ = s.Reset()
		return
	}
	ctx, cancel := context.WithTimeout(n.ctx, maxFetchTime)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { _ = s.Reset() })
	defer stop()
	_ = s.SetDeadline(time.Now().Add(min(n.timeout, maxManifestTime)))
	var request [40]byte
	if _, err := io.ReadFull(s, request[:]); err != nil {
		_ = s.Reset()
		return
	}
	d, err := parseSeedOffer(request[:])
	if err != nil {
		_ = s.Reset()
		return
	}
	svc.mu.Lock()
	admitted := svc.admission.admit(time.Now(), d)
	svc.mu.Unlock()
	if !admitted {
		_, _ = s.Write([]byte{0})
		return
	}
	defer func() { svc.mu.Lock(); svc.admission.finish(d); svc.mu.Unlock() }()
	deadline, _ := ctx.Deadline()
	_ = s.SetDeadline(deadline)
	watchDone := make(chan struct{})
	go func() {
		defer close(watchDone)
		var extra [1]byte
		_, _ = s.Read(extra[:])
		cancel() // Extra bytes, EOF, and reset all terminate this offer.
	}()
	defer func() {
		stop()
		_ = s.SetReadDeadline(time.Now())
		<-watchDone
	}()
	ctx = network.WithNoDial(ctx, "seed pulls use only the offering connection")
	present, err := n.store.PublishExisting(ctx, d)
	if err == nil && !present {
		var m cas.Manifest
		m, err = n.manifest(ctx, s.Conn().RemotePeer(), d)
		if err == nil {
			err = n.importManifestChunks(ctx, m, func(ctx context.Context, keys []cid.Cid, wanted map[string]cas.Digest, window map[cas.Digest][]byte) error {
				return svc.receiveWindow(ctx, s.Conn().RemotePeer(), keys, wanted, window)
			})
		}
	}
	if ctx.Err() != nil {
		return
	}
	result := byte(0)
	if err == nil {
		result = 1
	}
	_, _ = s.Write([]byte{result})
	_ = s.CloseWrite()
}
