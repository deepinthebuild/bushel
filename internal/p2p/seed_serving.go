package p2p

import (
	"context"
	"slices"
	"sync"

	"github.com/deepinthebuild/bushel/internal/cas"
	bsmessage "github.com/ipfs/boxo/bitswap/message"
	bspb "github.com/ipfs/boxo/bitswap/message/pb"
	bsnetwork "github.com/ipfs/boxo/bitswap/network"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

// A grant lasts only for one outgoing offer. Each manifest occurrence permits
// one block or DONT_HAVE response, so duplicate chunks remain usable without
// allowing WANT/CANCEL replays to exceed the offered blob's charged size.
type seedGrant struct {
	peer      peer.ID
	blob      cas.Digest
	ctx       context.Context
	cancel    context.CancelFunc
	chunks    map[cas.Digest]int
	requested map[cas.Digest]bool
	remaining int64
	sends     sync.WaitGroup
}

// Seed requests are already authorized by a validated manifest, and only
// WANT_BLOCK is accepted. Its sized CID supplies the scheduling estimate;
// readStore.Get still verifies the actual chunk before any payload is sent.
// Avoid backend I/O in Boxo's synchronous request/ledger bookkeeping, which
// would otherwise let one slow backend lookup delay another offer's revoke.
type seedReadStore struct{ readStore }

func (s seedReadStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	d, err := sizedChunkDigest(c)
	return int(d.Size), err
}

func (s *seedService) beginGrant(ctx context.Context, id peer.ID, d cas.Digest) *seedGrant {
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	s.servingMu.Lock()
	defer s.servingMu.Unlock()
	if ctx.Err() != nil || len(s.grants) >= seedConcurrency {
		return nil
	}
	for _, grant := range s.grants {
		// Bitswap wants identify a peer and chunk, not the offer that needs
		// it. One grant per recipient makes shared-chunk charging unambiguous.
		if grant.peer == id {
			return nil
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	grant := &seedGrant{peer: id, blob: d, ctx: ctx, cancel: cancel}
	s.grants = append(s.grants, grant)
	return grant
}

func (s *seedService) authorizeGrant(grant *seedGrant, manifest cas.Manifest) error {
	if manifest.Blob != grant.blob {
		return cas.ErrCorrupt
	}
	if err := manifest.Validate(); err != nil {
		return err
	}
	s.servingMu.Lock()
	defer s.servingMu.Unlock()
	if err := grant.ctx.Err(); err != nil {
		return err
	}
	if grant.chunks != nil {
		return cas.ErrInvalid
	}
	grant.chunks = make(map[cas.Digest]int, len(manifest.Chunks))
	grant.requested = make(map[cas.Digest]bool, min(len(manifest.Chunks), seedWantLimit))
	for _, d := range manifest.Chunks {
		grant.chunks[d]++
	}
	grant.remaining = manifest.Blob.Size
	return nil
}

func (s *seedService) endGrant(grant *seedGrant) {
	// Interrupt in-flight requests before waiting for their ledger updates.
	grant.cancel()
	s.requestMu.Lock()
	s.servingMu.Lock()
	s.grants = slices.DeleteFunc(s.grants, func(g *seedGrant) bool { return g == grant })
	s.servingMu.Unlock()
	if s.server != nil {
		retired := bsmessage.New(false)
		for _, c := range s.server.WantlistForPeer(grant.peer) {
			if !s.allowRequest(grant.peer, c) {
				retired.Cancel(c)
			}
		}
		if !retired.Empty() {
			s.server.ReceiveMessage(context.Background(), grant.peer, retired)
		}
	}
	s.requestMu.Unlock()
	// Add happens under servingMu before a send starts. Once removed, no new
	// send can acquire this grant; cancellation resets existing streams.
	grant.sends.Wait()
}

func (s *seedService) allowRequest(id peer.ID, c cid.Cid) bool {
	d, err := sizedChunkDigest(c)
	if err != nil {
		return false
	}
	s.servingMu.Lock()
	defer s.servingMu.Unlock()
	return s.grantFor(id, d) != nil
}

// A response may outlive the request that queued it in Boxo. Only a WANT
// accepted for the current grant can authorize spending that grant's credit.
func (s *seedService) wantChunk(id peer.ID, d cas.Digest) bool {
	s.servingMu.Lock()
	defer s.servingMu.Unlock()
	grant := s.grantFor(id, d)
	if grant == nil {
		return false
	}
	grant.requested[d] = true
	return true
}

// grantFor is called with servingMu held. Checking the context here also
// revokes requests immediately when caller cancellation races endGrant.
func (s *seedService) grantFor(id peer.ID, d cas.Digest) *seedGrant {
	for _, grant := range s.grants {
		if grant.peer == id && grant.ctx.Err() == nil && grant.chunks[d] > 0 && d.Size <= grant.remaining {
			return grant
		}
	}
	return nil
}

type seedRequestLedger interface {
	bsnetwork.Receiver
	WantlistForPeer(peer.ID) []cid.Cid
}

type seedRequestReceiver struct {
	Receiver seedRequestLedger
	service  *seedService
}

func (r seedRequestReceiver) ReceiveError(err error)      { r.Receiver.ReceiveError(err) }
func (r seedRequestReceiver) PeerConnected(id peer.ID)    { r.Receiver.PeerConnected(id) }
func (r seedRequestReceiver) PeerDisconnected(id peer.ID) { r.Receiver.PeerDisconnected(id) }

func (r seedRequestReceiver) ReceiveMessage(ctx context.Context, id peer.ID, message bsmessage.BitSwapMessage) {
	entries := message.Wantlist()
	if (len(entries) == 0 && !message.Full()) || len(entries) > seedWantLimit {
		return
	}
	s := r.service
	s.requestMu.Lock()
	defer s.requestMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, s.node.timeout)
	defer cancel()
	filtered := bsmessage.New(false)
	stops := make(map[*seedGrant]func() bool, seedConcurrency)
	var keepRequested map[cas.Digest]bool
	if message.Full() {
		keepRequested = make(map[cas.Digest]bool, len(entries))
	}
	s.servingMu.Lock()
	for _, entry := range entries {
		d, err := sizedChunkDigest(entry.Cid)
		if err != nil {
			continue
		}
		// CANCEL cannot fetch or send bytes. Forward it after allowance is
		// exhausted too, so completed windows do not fill Boxo's want ledger.
		if entry.Cancel {
			filtered.Cancel(entry.Cid)
			for _, grant := range s.grants {
				if grant.peer == id {
					delete(grant.requested, d)
				}
			}
			continue
		}
		grant := s.grantFor(id, d)
		if entry.WantType == bspb.Message_Wantlist_Block && grant != nil {
			filtered.AddEntry(entry.Cid, entry.Priority, entry.WantType, entry.SendDontHave)
			if keepRequested != nil {
				keepRequested[d] = true
			}
			if stops[grant] == nil {
				stops[grant] = context.AfterFunc(grant.ctx, cancel)
			}
		}
	}
	if keepRequested != nil {
		for _, grant := range s.grants {
			if grant.peer == id {
				for d := range grant.requested {
					if !keepRequested[d] {
						delete(grant.requested, d)
					}
				}
			}
		}
	}
	s.servingMu.Unlock()
	defer func() {
		for _, stop := range stops {
			stop()
		}
	}()
	// Boxo dispatches multi-key GetSize calls through the same worker pool as
	// Get. Deliver single WANTs so the CID-only size estimate runs inline, and
	// enforce ledger capacity before calling Boxo to avoid its overflow path's
	// multi-key lookup under Engine.lock. These are the only request operations
	// serialized with grant retirement; they must never wait for backend I/O.
	existing := make(map[cid.Cid]bool, seedWantLimit)
	for _, c := range r.Receiver.WantlistForPeer(id) {
		existing[c] = true
	}
	retired := bsmessage.New(false)
	wants := filtered.Wantlist()
	retained := make(map[cid.Cid]bool, len(wants))
	for _, entry := range wants {
		if entry.Cancel {
			retired.Cancel(entry.Cid)
			delete(existing, entry.Cid)
		} else {
			retained[entry.Cid] = true
		}
	}
	if message.Full() {
		for c := range existing {
			if !retained[c] {
				retired.Cancel(c)
				delete(existing, c)
			}
		}
	}
	if !retired.Empty() {
		r.Receiver.ReceiveMessage(ctx, id, retired)
	}
	for _, entry := range wants {
		if entry.Cancel || (!existing[entry.Cid] && len(existing) >= seedWantLimit) {
			continue
		}
		d, err := sizedChunkDigest(entry.Cid)
		if err != nil || !s.wantChunk(id, d) {
			continue
		}
		single := bsmessage.New(false)
		single.AddEntry(entry.Cid, entry.Priority, entry.WantType, entry.SendDontHave)
		r.Receiver.ReceiveMessage(ctx, id, single)
		// Concurrent sends may remove entries, so this is a conservative count.
		existing[entry.Cid] = true
	}
}

// The server's request filter alone cannot bound sends: queued WANTs can race
// cancellation or repeat after a CANCEL. Charge and recheck at the network
// boundary, then use grant-owned contexts for all actual response streams.
type seedResponseNetwork struct {
	bsnetwork.BitSwapNetwork
	service *seedService
	send    func(context.Context, peer.ID, bsmessage.BitSwapMessage) error
}

func (n *seedResponseNetwork) SendMessage(ctx context.Context, id peer.ID, message bsmessage.BitSwapMessage) error {
	s := n.service
	s.servingMu.Lock()
	var grant *seedGrant
	response := bsmessage.New(false)
	charge := func(d cas.Digest) bool {
		current := s.grantFor(id, d)
		if current == nil || !current.requested[d] {
			return false
		}
		delete(current.requested, d)
		current.chunks[d]--
		current.remaining -= d.Size
		grant = current // At most one grant exists for this recipient.
		return true
	}
	for _, block := range message.Blocks() {
		d, err := sizedChunkDigest(block.Cid())
		if err == nil && int64(len(block.RawData())) == d.Size && cas.Sum(block.RawData()) == d {
			if charge(d) {
				response.AddBlock(block)
			}
		}
	}
	for _, c := range message.DontHaves() {
		if d, err := sizedChunkDigest(c); err == nil {
			if charge(d) {
				response.AddDontHave(c)
			}
		}
	}
	if grant != nil {
		grant.sends.Add(1)
	}
	s.servingMu.Unlock()
	if grant == nil {
		return nil
	}
	return n.sendResponse(ctx, grant, response)
}

func (n *seedResponseNetwork) sendResponse(ctx context.Context, grant *seedGrant, message bsmessage.BitSwapMessage) error {
	defer grant.sends.Done()
	ctx, cancel := context.WithTimeout(ctx, n.service.node.timeout)
	defer cancel()
	stop := context.AfterFunc(grant.ctx, cancel)
	defer stop()
	if err := grant.ctx.Err(); err != nil {
		return err
	}
	return n.send(ctx, grant.peer, message)
}

// Boxo's server currently uses SendMessage; keep the alternative interface
// under the same accounting if a future version switches to message senders.
func (n *seedResponseNetwork) NewMessageSender(ctx context.Context, id peer.ID, _ *bsnetwork.MessageSenderOpts) (bsnetwork.MessageSender, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return seedResponseSender{network: n, peer: id}, nil
}

type seedResponseSender struct {
	network *seedResponseNetwork
	peer    peer.ID
}

func (s seedResponseSender) SendMsg(ctx context.Context, message bsmessage.BitSwapMessage) error {
	return s.network.SendMessage(ctx, s.peer, message)
}
func (seedResponseSender) Reset() error       { return nil }
func (seedResponseSender) SupportsHave() bool { return false }
