package p2p

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	bbdigest "github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/teststorage"
	bsmessage "github.com/ipfs/boxo/bitswap/message"
	bspb "github.com/ipfs/boxo/bitswap/message/pb"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/peer"
)

func seedWorkerManifest(count int) cas.Manifest {
	manifest := cas.Manifest{Blob: cas.Sum([]byte("worker saturation fixture"))}
	for i := range count {
		manifest.Chunks = append(manifest.Chunks, cas.Sum(bytes.Repeat([]byte{byte(i)}, cas.MinChunkSize)))
	}
	manifest.Blob.Size = int64(count) * cas.MinChunkSize
	return manifest
}

type seedLedgerObserver struct {
	seedRequestLedger
	entered chan struct{}
}

func (r seedLedgerObserver) ReceiveMessage(ctx context.Context, id peer.ID, message bsmessage.BitSwapMessage) {
	select {
	case r.entered <- struct{}{}:
	default:
	}
	r.seedRequestLedger.ReceiveMessage(ctx, id, message)
}

func TestSeedGrantRevocationWithSaturatedBoxoWorkers(t *testing.T) {
	for _, mode := range []string{"multi WANT", "ledger overflow"} {
		t.Run(mode, func(t *testing.T) {
			source, store, backend := seedNode(t, Config{RequestTimeout: 10 * time.Second})
			receiver, _, _ := seedNode(t, Config{AcceptSeeds: true})
			independent, _, _ := seedNode(t, Config{AcceptSeeds: true})
			connectSeedNodes(t, source, receiver)
			connectSeedNodes(t, source, independent)
			manifest := seedWorkerManifest(seedWantLimit + 2)
			for i := range 2 {
				data := bytes.Repeat([]byte{byte(i)}, cas.MinChunkSize)
				if err := store.Put(t.Context(), manifest.Chunks[i], bytes.NewReader(data)); err != nil {
					t.Fatal(err)
				}
			}
			idle := cas.Sum([]byte("independent offer"))
			grant := seedTestGrant(t, source.seeds, t.Context(), independent.ID(), cas.Manifest{Blob: idle, Chunks: []cas.Digest{idle}})
			seedTestGrant(t, source.seeds, t.Context(), receiver.ID(), manifest)
			blocked := make(chan struct{}, 2)
			released := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(released) }) }
			defer release()
			backend.SetHook(func(ctx context.Context, op teststorage.Operation, _ bbdigest.Digest) error {
				if op == teststorage.ChunkGet {
					select {
					case blocked <- struct{}{}:
					default:
					}
					select {
					case <-released:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return nil
			})
			// Submit one two-block envelope directly to the actual Boxo server.
			// Both blocks fit its target message, so getBlocks dispatches them
			// through its two shared blockstore workers, which remain blocked.
			initial := bsmessage.New(false)
			for _, d := range manifest.Chunks[:2] {
				initial.AddEntry(seedCID(t, d), 1, bspb.Message_Wantlist_Block, true)
			}
			source.seeds.server.ReceiveMessage(t.Context(), receiver.ID(), initial)
			for range 2 {
				select {
				case <-blocked:
				case <-time.After(time.Second):
					t.Fatal("Boxo did not occupy both blockstore workers")
				}
			}
			if mode == "ledger overflow" {
				// Single requests bypass GetSize's pool. No block can be sent,
				// so the ledger remains full while the real workers are stalled.
				for _, d := range manifest.Chunks[2:seedWantLimit] {
					message := bsmessage.New(false)
					message.AddEntry(seedCID(t, d), 1, bspb.Message_Wantlist_Block, true)
					source.seeds.server.ReceiveMessage(t.Context(), receiver.ID(), message)
				}
				if wants := source.seeds.server.WantlistForPeer(receiver.ID()); len(wants) != seedWantLimit {
					t.Fatalf("ledger not saturated: %d", len(wants))
				}
			}
			entered := make(chan struct{}, 1)
			adapter := seedRequestReceiver{Receiver: seedLedgerObserver{source.seeds.server, entered}, service: source.seeds}
			incoming := bsmessage.New(false)
			for _, d := range manifest.Chunks[seedWantLimit:] {
				incoming.AddEntry(seedCID(t, d), 1, bspb.Message_Wantlist_Block, true)
			}
			done := make(chan struct{})
			go func() { adapter.ReceiveMessage(t.Context(), receiver.ID(), incoming); close(done) }()
			defer func() {
				release()
				select {
				case <-done:
				case <-time.After(time.Second):
					t.Error("request adapter did not shut down")
				}
			}()
			select {
			case <-entered:
			case <-done: // Full-ledger requests are dropped before Boxo.
			case <-time.After(time.Second):
				t.Fatal("request adapter stalled before dispatch")
			}
			revoked := make(chan struct{})
			go func() { source.seeds.endGrant(grant); close(revoked) }()
			select {
			case <-revoked:
			case <-time.After(time.Second):
				t.Fatal("canceling one offer waited for another offer's block workers")
			}
			select {
			case <-done:
			case <-time.After(time.Second):
				t.Fatal("request bookkeeping used the saturated worker pool")
			}
			if !source.seeds.allowRequest(receiver.ID(), seedCID(t, manifest.Chunks[seedWantLimit])) {
				t.Fatal("canceling one grant revoked the other grant")
			}
		})
	}
}

func TestSeedRequestAdapterFullReplacementAndCapacity(t *testing.T) {
	manifest := seedWorkerManifest(seedWantLimit + 2)
	svc := &seedService{node: &Node{timeout: time.Second}}
	grant := seedTestGrant(t, svc, t.Context(), "recipient", manifest)
	ledger := &seedRequestCapture{}
	adapter := seedRequestReceiver{Receiver: ledger, service: svc}
	initial := bsmessage.New(false)
	for _, d := range manifest.Chunks[:seedWantLimit] {
		initial.AddEntry(seedCID(t, d), 1, bspb.Message_Wantlist_Block, true)
	}
	adapter.ReceiveMessage(t.Context(), "recipient", initial)
	if len(ledger.wants) != seedWantLimit {
		t.Fatalf("initial ledger: %d", len(ledger.wants))
	}
	overflow := bsmessage.New(false)
	last := seedCID(t, manifest.Chunks[seedWantLimit])
	overflow.AddEntry(last, 1, bspb.Message_Wantlist_Block, true)
	adapter.ReceiveMessage(t.Context(), "recipient", overflow)
	if len(ledger.wants) != seedWantLimit || ledger.wants[last] {
		t.Fatal("overflow entered Boxo's ledger")
	}
	if grant.requested[manifest.Chunks[seedWantLimit]] {
		t.Fatal("overflow armed response credit before request admission")
	}
	replacement := bsmessage.New(true)
	replacement.AddEntry(last, 1, bspb.Message_Wantlist_Block, true)
	adapter.ReceiveMessage(t.Context(), "recipient", replacement)
	if len(ledger.wants) != 1 || !ledger.wants[last] {
		t.Fatal("Full wantlist did not replace prior wants")
	}
	if len(grant.requested) != 1 || !grant.requested[manifest.Chunks[seedWantLimit]] {
		t.Fatal("Full replacement retained stale response credit")
	}
	cancel := bsmessage.New(false)
	cancel.Cancel(last)
	adapter.ReceiveMessage(t.Context(), "recipient", cancel)
	if len(grant.requested) != 0 {
		t.Fatal("CANCEL retained response credit")
	}
	adapter.ReceiveMessage(t.Context(), "recipient", replacement)
	adapter.ReceiveMessage(t.Context(), "recipient", bsmessage.New(true))
	if len(ledger.wants) != 0 || len(grant.requested) != 0 {
		t.Fatal("empty Full wantlist did not clear prior wants")
	}
	for _, message := range ledger.requests {
		if message.Full() {
			t.Fatal("Full semantics were delegated to Boxo")
		}
		wants := 0
		for _, entry := range message.Wantlist() {
			if !entry.Cancel {
				wants++
			}
		}
		if wants > 1 {
			t.Fatal("multi-key GetSize could use Boxo's worker pool")
		}
	}
}

func TestSeedStaleEnvelopeCannotChargeSuccessorBeforeWant(t *testing.T) {
	source, store, backend := seedNode(t, Config{RequestTimeout: 10 * time.Second})
	receiver, _, _ := seedNode(t, Config{AcceptSeeds: true})
	connectSeedNodes(t, source, receiver)
	data := bytes.Repeat([]byte{1}, cas.MinChunkSize)
	d := cas.Sum(data)
	if err := store.Put(t.Context(), d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	first := seedTestGrant(t, source.seeds, t.Context(), receiver.ID(), cas.Manifest{Blob: d, Chunks: []cas.Digest{d}})
	entered := make(chan struct{}, 1)
	released := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(released) }) }
	defer release()
	backend.SetHook(func(ctx context.Context, op teststorage.Operation, _ bbdigest.Digest) error {
		if op == teststorage.ChunkGet {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-released:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	key := seedCID(t, d)
	request := bsmessage.New(false)
	request.AddEntry(key, 1, bspb.Message_Wantlist_Block, true)
	if err := receiver.seeds.send(t.Context(), source.ID(), request); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("old offer did not start its backend read")
	}
	source.seeds.endGrant(first)
	manifest := cas.Manifest{Blob: cas.Sum(bytes.Repeat(data, 2)), Chunks: []cas.Digest{d, d}}
	successor := seedTestGrant(t, source.seeds, t.Context(), receiver.ID(), manifest)
	release()
	// Boxo counts the completed envelope after the response wrapper returns,
	// including an envelope it filtered out. This waits for the old Get's
	// actual response-boundary decision without relying on a negative sleep.
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		stat, err := source.seeds.server.Stat()
		if err != nil {
			t.Fatal(err)
		}
		if stat.BlocksSent != 0 {
			break
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("old envelope did not finish")
		}
	}
	source.seeds.servingMu.Lock()
	remaining := successor.remaining
	requested := successor.requested[d]
	source.seeds.servingMu.Unlock()
	if remaining != manifest.Blob.Size || requested {
		t.Fatal("old envelope consumed successor credit before its WANT")
	}
	wanted := map[string]cas.Digest{key.KeyString(): d}
	window := make(map[cas.Digest][]byte)
	if err := receiver.seeds.receiveWindow(t.Context(), source.ID(), []cid.Cid{key}, wanted, window); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(window[d], data) {
		t.Fatal("successor's requested chunk was not delivered")
	}
	source.seeds.servingMu.Lock()
	remaining = successor.remaining
	requested = successor.requested[d]
	source.seeds.servingMu.Unlock()
	if remaining != d.Size || requested {
		t.Fatal("accepted WANT did not consume exactly one occurrence")
	}
}
