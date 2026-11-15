package p2p

import (
	"context"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	bsmessage "github.com/ipfs/boxo/bitswap/message"
	bspb "github.com/ipfs/boxo/bitswap/message/pb"
	"github.com/ipfs/boxo/bitswap/network/bsnet"
	bsserver "github.com/ipfs/boxo/bitswap/server"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const seedBitswapPrefix protocol.ID = "/bushel/seed"
const seedBitswapProtocol protocol.ID = "/bushel/seed/ipfs/bitswap/1.2.0"
const seedWantLimit = 32

type seedWindow struct {
	peer    peer.ID
	wanted  map[string]bool
	blocks  chan blocks.Block
	missing chan struct{}
}

func (n *Node) initSeeds(cfg Config, limits seedLimits) {
	s := &seedService{node: n, controls: make(chan struct{}, seedControls), outgoing: make(chan struct{}, seedConcurrency),
		admission: seedAdmission{limits: limits}, pending: make(map[*seedWindow]struct{}, seedConcurrency)}
	s.network = bsnet.NewFromIpfsHost(n.host, bsnet.Prefix(seedBitswapPrefix), bsnet.SupportedProtocols([]protocol.ID{bsnet.ProtocolBitswap}))
	responses := &seedResponseNetwork{BitSwapNetwork: s.network, service: s, send: s.send}
	s.server = bsserver.New(n.ctx, responses, seedReadStore{readStore{store: n.store, timeout: n.timeout}},
		bsserver.EngineBlockstoreWorkerCount(2), bsserver.EngineTaskWorkerCount(2), bsserver.TaskWorkerCount(2),
		bsserver.MaxOutstandingBytesPerPeer(1<<20), bsserver.MaxQueuedWantlistEntriesPerPeer(seedWantLimit), bsserver.MaxCidSize(sizedChunkCIDSize),
		bsserver.WithTargetMessageSize(1<<20), bsserver.WithPeerBlockRequestFilter(s.allowRequest))
	s.network.Start(seedRequestReceiver{Receiver: s.server, service: s}, s)
	n.seeds = s
	if cfg.AcceptSeeds {
		n.host.SetStreamHandler(SeedOfferProtocol, n.serveSeedOffer)
	}
}

func (s *seedService) ReceiveMessage(_ context.Context, from peer.ID, message bsmessage.BitSwapMessage) {
	s.pendingMu.Lock()
	defer s.pendingMu.Unlock()
	if len(s.pending) == 0 {
		return
	}
	blocks, missing := message.Blocks(), message.DontHaves()
	for pending := range s.pending {
		if pending.peer != from {
			continue
		}
		for _, block := range blocks {
			key := block.Cid().KeyString()
			if pending.wanted[key] {
				delete(pending.wanted, key)
				pending.blocks <- block // One slot per unique requested CID.
			}
		}
		for _, key := range missing {
			if pending.wanted[key.KeyString()] {
				select {
				case pending.missing <- struct{}{}:
				default:
				}
			}
		}
	}
}
func (*seedService) ReceiveError(error)       {}
func (*seedService) PeerConnected(peer.ID)    {}
func (*seedService) PeerDisconnected(peer.ID) {}

func (s *seedService) send(ctx context.Context, id peer.ID, message bsmessage.BitSwapMessage) error {
	ctx = network.WithNoDial(ctx, "seed chunks use only the offering connection")
	stream, err := s.node.host.NewStream(ctx, id, seedBitswapProtocol)
	if err != nil {
		return err
	}
	defer stream.Close()
	stop := context.AfterFunc(ctx, func() { _ = stream.Reset() })
	defer stop()
	deadline, _ := ctx.Deadline()
	_ = stream.SetDeadline(deadline)
	return message.ToNetV1(stream)
}

func (s *seedService) receiveWindow(ctx context.Context, from peer.ID, keys []cid.Cid, wanted map[string]cas.Digest, window map[cas.Digest][]byte) error {
	if len(keys) == 0 || len(keys) > fetchWindow {
		return cas.ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, s.node.timeout)
	defer cancel()
	pending := &seedWindow{peer: from, wanted: make(map[string]bool, len(keys)), blocks: make(chan blocks.Block, len(keys)), missing: make(chan struct{}, 1)}
	request := bsmessage.New(false)
	for _, key := range keys {
		pending.wanted[key.KeyString()] = true
		request.AddEntry(key, 1, bspb.Message_Wantlist_Block, true)
	}
	// Serialize WANT/CANCEL changes with registration. Concurrent seed blobs
	// may share chunks; retiring one window must not cancel another's wants.
	s.wireMu.Lock()
	s.pendingMu.Lock()
	if len(s.pending) >= seedConcurrency {
		s.pendingMu.Unlock()
		s.wireMu.Unlock()
		return cas.ErrQuota
	}
	s.pending[pending] = struct{}{}
	s.pendingMu.Unlock()
	err := s.send(ctx, from, request)
	s.wireMu.Unlock()
	defer s.retire(pending, keys)
	if err != nil {
		return err
	}
	for len(wanted) != 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-pending.missing:
			return cas.ErrNotFound
		case block := <-pending.blocks:
			d, ok := wanted[block.Cid().KeyString()]
			if !ok || cas.Sum(block.RawData()) != d {
				return cas.ErrCorrupt
			}
			window[d] = block.RawData()
			delete(wanted, block.Cid().KeyString())
		}
	}
	return nil
}

func (s *seedService) retire(pending *seedWindow, keys []cid.Cid) {
	s.wireMu.Lock()
	defer s.wireMu.Unlock()
	s.pendingMu.Lock()
	delete(s.pending, pending)
	request := bsmessage.New(false)
	for _, key := range keys {
		shared := false
		for other := range s.pending {
			if other.peer == pending.peer && other.wanted[key.KeyString()] {
				shared = true
				break
			}
		}
		if !shared {
			request.Cancel(key)
		}
	}
	s.pendingMu.Unlock()
	if len(request.Wantlist()) != 0 {
		ctx, cancel := context.WithTimeout(s.node.ctx, min(time.Second, s.node.timeout))
		defer cancel()
		_ = s.send(ctx, pending.peer, request)
	}
}
