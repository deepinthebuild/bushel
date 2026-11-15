package p2p

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	bsmessage "github.com/ipfs/boxo/bitswap/message"
	bspb "github.com/ipfs/boxo/bitswap/message/pb"
	bsnetwork "github.com/ipfs/boxo/bitswap/network"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"pgregory.net/rapid"
)

func seedCID(t interface {
	Helper()
	Fatal(...any)
}, d cas.Digest) cid.Cid {
	t.Helper()
	c, err := sizedChunkCID(d)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func seedTestGrant(t testing.TB, svc *seedService, ctx context.Context, id peer.ID, manifest cas.Manifest) *seedGrant {
	t.Helper()
	grant := svc.beginGrant(ctx, id, manifest.Blob)
	if grant == nil {
		t.Fatal("grant was not reserved")
	}
	if err := svc.authorizeGrant(grant, manifest); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { svc.endGrant(grant) })
	return grant
}

func seedBlockMessage(t testing.TB, data []byte, d cas.Digest) bsmessage.BitSwapMessage {
	t.Helper()
	block, err := blocks.NewBlockWithCid(data, seedCID(t, d))
	if err != nil {
		t.Fatal(err)
	}
	message := bsmessage.New(false)
	message.AddBlock(block)
	return message
}

func TestPropertySeedServingAllowance(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		// Digest identities and occurrence counts shrink independently. No blob
		// allocation is needed to model duplicate chunks or arbitrary replays.
		occurrences := rapid.SliceOfN(rapid.IntRange(0, 3), 2, 16).Draw(t, "manifest chunks")
		chunks := make([]cas.Digest, 4)
		for i := range chunks {
			chunks[i] = cas.Sum([]byte{byte(i)})
			chunks[i].Size = cas.MinChunkSize + int64(i)
		}
		manifest := cas.Manifest{Blob: cas.Sum(nil)}
		left := make(map[cas.Digest]int)
		for _, index := range occurrences {
			d := chunks[index]
			manifest.Chunks = append(manifest.Chunks, d)
			manifest.Blob.Size += d.Size
			left[d]++
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		svc := &seedService{node: &Node{timeout: time.Second}}
		grant := svc.beginGrant(ctx, "recipient", manifest.Blob)
		if err := svc.authorizeGrant(grant, manifest); err != nil {
			t.Fatal(err)
		}
		defer svc.endGrant(grant)
		var actual []cas.Digest
		responses := &seedResponseNetwork{service: svc, send: func(_ context.Context, _ peer.ID, message bsmessage.BitSwapMessage) error {
			for _, c := range message.DontHaves() {
				d, err := sizedChunkDigest(c)
				if err != nil {
					t.Fatal(err)
				}
				actual = append(actual, d)
			}
			return nil
		}}
		active := true
		var charged int64
		for _, event := range rapid.SliceOfN(rapid.IntRange(0, 12), 1, 100).Draw(t, "requests") {
			if event == 12 {
				cancel()
				active = false
				continue
			}
			d := chunks[event%4]
			id := peer.ID("recipient")
			if event >= 8 {
				id = "unrelated peer"
			}
			requested := event < 4
			if requested {
				svc.wantChunk(id, d)
			}
			allowed := active && id == "recipient" && left[d] > 0 && requested
			before := len(actual)
			message := bsmessage.New(false)
			message.AddDontHave(seedCID(t, d))
			if err := responses.SendMessage(context.Background(), id, message); err != nil {
				t.Fatal(err)
			}
			if allowed {
				left[d]--
				charged += d.Size
				if len(actual) != before+1 || actual[before] != d {
					t.Fatal("allowed response was lost")
				}
			} else if len(actual) != before {
				t.Fatal("replayed, canceled, or unrelated response escaped")
			}
			if charged > manifest.Blob.Size || len(actual) > len(manifest.Chunks) || grant.remaining != manifest.Blob.Size-charged {
				t.Fatal("response allowance exceeded")
			}
		}
	})
}

func TestSeedServingChecksActualBytesAndDoesNotRefund(t *testing.T) {
	data := []byte("offered content")
	d := cas.Sum(data)
	svc := &seedService{node: &Node{timeout: time.Second}}
	seedTestGrant(t, svc, t.Context(), "recipient", cas.Manifest{Blob: d, Chunks: []cas.Digest{d}})
	svc.wantChunk("recipient", d)
	var sent int
	failure := errors.New("send failed")
	responses := &seedResponseNetwork{service: svc, send: func(context.Context, peer.ID, bsmessage.BitSwapMessage) error {
		sent++
		return failure
	}}
	for _, malformed := range [][]byte{[]byte("short"), bytes.Repeat([]byte{'x'}, len(data)), append(bytes.Clone(data), 0)} {
		if err := responses.SendMessage(t.Context(), "recipient", seedBlockMessage(t, malformed, d)); err != nil {
			t.Fatal(err)
		}
	}
	if sent != 0 {
		t.Fatal("mismatched payload sent")
	}
	message := seedBlockMessage(t, data, d)
	if err := responses.SendMessage(t.Context(), "recipient", message); !errors.Is(err, failure) {
		t.Fatal("send failure hidden")
	}
	for range 10 {
		svc.wantChunk("recipient", d)
		if err := responses.SendMessage(t.Context(), "recipient", message); err != nil {
			t.Fatal(err)
		}
	}
	if sent != 1 {
		t.Fatal("failed send refunded its allowance")
	}
}

func TestSeedServingCancellationJoinsSends(t *testing.T) {
	data := []byte("cancel during send")
	d := cas.Sum(data)
	svc := &seedService{node: &Node{timeout: time.Second}}
	grant := seedTestGrant(t, svc, t.Context(), "recipient", cas.Manifest{Blob: d, Chunks: []cas.Digest{d}})
	svc.wantChunk("recipient", d)
	entered := make(chan struct{}, 1)
	var calls atomic.Int32
	responses := &seedResponseNetwork{service: svc, send: func(ctx context.Context, _ peer.ID, _ bsmessage.BitSwapMessage) error {
		calls.Add(1)
		entered <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}}
	message := seedBlockMessage(t, data, d)
	results := make(chan error, 1)
	go func() { results <- responses.SendMessage(t.Context(), "recipient", message) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("send did not begin")
	}
	var replays sync.WaitGroup
	for range 32 {
		replays.Go(func() { _ = responses.SendMessage(t.Context(), "recipient", message) })
	}
	svc.endGrant(grant)
	replays.Wait()
	if err := <-results; !errors.Is(err, context.Canceled) {
		t.Fatalf("send survived revocation: %v", err)
	}
	if calls.Load() != 1 || svc.allowRequest("recipient", seedCID(t, d)) {
		t.Fatal("revoked or concurrent replay retained permission")
	}
}

func TestSeedServingSerializesRecipientAndPreservesSharedChunkCredit(t *testing.T) {
	data := make([]byte, cas.MinChunkSize)
	d := cas.Sum(data)
	manifest := cas.Manifest{Blob: cas.Sum(nil), Chunks: []cas.Digest{d, d}}
	manifest.Blob.Size = 2 * d.Size
	svc := &seedService{node: &Node{timeout: time.Second}}
	first := seedTestGrant(t, svc, t.Context(), "recipient", manifest)
	manifest.Blob.Hash[0] ^= 1
	if manifest.Blob == first.blob {
		t.Fatal("fixture requires distinct blob digests sharing chunks")
	}
	rejectedContext, rejectCancel := context.WithCancel(t.Context())
	defer rejectCancel()
	if rejected := svc.beginGrant(rejectedContext, "recipient", manifest.Blob); rejected != nil {
		svc.endGrant(rejected)
		t.Fatal("two offers to the same recipient would share chunk credit")
	}
	rejectCancel()
	second := seedTestGrant(t, svc, t.Context(), "other recipient", manifest)
	if extra := svc.beginGrant(t.Context(), "third recipient", manifest.Blob); extra != nil {
		svc.endGrant(extra)
		t.Fatal("more than two outgoing offers were admitted")
	}
	payloadBytes := make(map[peer.ID]int64)
	responses := &seedResponseNetwork{service: svc, send: func(_ context.Context, id peer.ID, message bsmessage.BitSwapMessage) error {
		for _, block := range message.Blocks() {
			payloadBytes[id] += int64(len(block.RawData()))
		}
		return nil
	}}
	message := seedBlockMessage(t, data, d)
	// The later offer requests a shared chunk first, then is canceled. The
	// earlier recipient must still retain both of its own chunk occurrences.
	svc.wantChunk("other recipient", d)
	if err := responses.SendMessage(t.Context(), "other recipient", message); err != nil {
		t.Fatal(err)
	}
	svc.endGrant(second)
	if first.remaining != manifest.Blob.Size {
		t.Fatal("another recipient or a rejected offer consumed this grant's credit")
	}
	for range 10 {
		svc.wantChunk("recipient", d)
		if err := responses.SendMessage(t.Context(), "recipient", message); err != nil {
			t.Fatal(err)
		}
	}
	if payloadBytes["recipient"] != 2*d.Size || payloadBytes["other recipient"] != d.Size || first.remaining != 0 || second.remaining != d.Size {
		t.Fatalf("shared or repeated chunk accounting: sent=%v first=%d second=%d", payloadBytes, first.remaining, second.remaining)
	}
	svc.endGrant(first)
	seedTestGrant(t, svc, t.Context(), "recipient", manifest)
}

type seedRequestCapture struct {
	bsnetwork.Receiver
	requests []bsmessage.BitSwapMessage
	wants    map[cid.Cid]bool
}

func (c *seedRequestCapture) ReceiveMessage(_ context.Context, _ peer.ID, message bsmessage.BitSwapMessage) {
	c.requests = append(c.requests, message)
	if c.wants == nil {
		c.wants = make(map[cid.Cid]bool)
	}
	for _, entry := range message.Wantlist() {
		if entry.Cancel {
			delete(c.wants, entry.Cid)
		} else {
			c.wants[entry.Cid] = true
		}
	}
}

func (c *seedRequestCapture) WantlistForPeer(peer.ID) []cid.Cid {
	var wants []cid.Cid
	for key := range c.wants {
		wants = append(wants, key)
	}
	return wants
}

func TestSeedServingFiltersRequestsBeforeBackendWork(t *testing.T) {
	d := cas.Sum([]byte("offered chunk"))
	unrelated := cas.Sum([]byte("unrelated chunk"))
	svc := &seedService{node: &Node{timeout: time.Second}}
	capture := &seedRequestCapture{}
	filter := seedRequestReceiver{Receiver: capture, service: svc}
	message := bsmessage.New(false)
	message.AddEntry(seedCID(t, d), 1, bspb.Message_Wantlist_Block, true)
	filter.ReceiveMessage(t.Context(), "recipient", message)
	if len(capture.requests) != 0 {
		t.Fatal("unsolicited request reached server")
	}
	grant := seedTestGrant(t, svc, t.Context(), "recipient", cas.Manifest{Blob: d, Chunks: []cas.Digest{d}})
	message.AddEntry(seedCID(t, unrelated), 1, bspb.Message_Wantlist_Block, true)
	filter.ReceiveMessage(t.Context(), "other peer", message)
	filter.ReceiveMessage(t.Context(), "recipient", message)
	if len(capture.requests) != 1 || len(capture.requests[0].Wantlist()) != 1 || !capture.requests[0].Wantlist()[0].Cid.Equals(seedCID(t, d)) {
		t.Fatal("request scope escaped offer manifest or authenticated peer")
	}
	message = bsmessage.New(false)
	message.AddEntry(seedCID(t, d), 1, bspb.Message_Wantlist_Have, true)
	filter.ReceiveMessage(t.Context(), "recipient", message)
	if len(capture.requests) != 1 {
		t.Fatal("HAVE polling reached server")
	}
	svc.endGrant(grant)
	message.Cancel(seedCID(t, d))
	filter.ReceiveMessage(t.Context(), "recipient", message)
	if len(capture.requests) != 2 || !capture.requests[1].Wantlist()[0].Cancel {
		t.Fatal("retirement CANCEL was discarded after revocation")
	}
}

func TestSeedServingCanceledGrantDoesNotWaitForOtherSend(t *testing.T) {
	first, second := []byte("first"), []byte("second")
	svc := &seedService{node: &Node{timeout: 10 * time.Second}}
	firstDigest, secondDigest := cas.Sum(first), cas.Sum(second)
	grant := seedTestGrant(t, svc, t.Context(), "recipient", cas.Manifest{Blob: firstDigest, Chunks: []cas.Digest{firstDigest}})
	seedTestGrant(t, svc, t.Context(), "other recipient", cas.Manifest{Blob: secondDigest, Chunks: []cas.Digest{secondDigest}})
	svc.wantChunk("recipient", firstDigest)
	svc.wantChunk("other recipient", secondDigest)
	entered := make(chan struct{}, 2)
	releaseOther := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseOther) }) }
	defer release()
	responses := &seedResponseNetwork{service: svc, send: func(ctx context.Context, _ peer.ID, message bsmessage.BitSwapMessage) error {
		entered <- struct{}{}
		if cas.Sum(message.Blocks()[0].RawData()) == firstDigest {
			<-ctx.Done()
			return ctx.Err()
		}
		select {
		case <-releaseOther:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}}
	message := seedBlockMessage(t, first, firstDigest)
	message.AddBlock(seedBlockMessage(t, second, secondDigest).Blocks()[0])
	result := make(chan error, 2)
	go func() { result <- responses.SendMessage(t.Context(), "recipient", message) }()
	go func() { result <- responses.SendMessage(t.Context(), "other recipient", message) }()
	for range 2 {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("one grant's send blocked the other")
		}
	}
	done := make(chan struct{})
	go func() { svc.endGrant(grant); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancellation waited for unrelated send")
	}
	release()
	canceled := 0
	for range 2 {
		select {
		case err := <-result:
			if errors.Is(err, context.Canceled) {
				canceled++
			} else if err != nil {
				t.Fatalf("independent send error: %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("independent sends did not join")
		}
	}
	if canceled != 1 {
		t.Fatalf("canceling one recipient affected %d sends", canceled)
	}
}

func TestSeedManyUniqueChunksRetireServerWants(t *testing.T) {
	source, store, _ := seedNode(t, Config{})
	receiver, replica, _ := seedNode(t, Config{AcceptSeeds: true})
	connectSeedNodes(t, source, receiver)
	data := testData(16 << 20)
	d := cas.Sum(data)
	if err := store.Put(t.Context(), d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	manifest, err := store.Manifest(t.Context(), d)
	if err != nil {
		t.Fatal(err)
	}
	unique := make(map[cas.Digest]bool)
	for _, d := range manifest.Chunks {
		unique[d] = true
	}
	if len(unique) <= 32 {
		t.Fatal("fixture must exceed the server want ledger")
	}
	if complete, err := source.Offer(t.Context(), receiver.ID(), d); err != nil || !complete {
		t.Fatalf("many-window transfer: %t %v", complete, err)
	}
	if wants := source.seeds.server.WantlistForPeer(receiver.ID()); len(wants) != 0 {
		t.Fatalf("completed offer retained %d server wants", len(wants))
	}
	if complete, err := replica.Has(t.Context(), d); err != nil || !complete {
		t.Fatalf("many-window replica: %t %v", complete, err)
	}
}

func TestSeedServingRejectsUnsolicitedReplayAndPostCancelPulls(t *testing.T) {
	source, store, _ := seedNode(t, Config{})
	receiver, _, _ := seedNode(t, Config{AcceptSeeds: true})
	connectSeedNodes(t, source, receiver)
	data := testData(900000)
	d := cas.Sum(data)
	if err := store.Put(t.Context(), d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	manifest, err := store.Manifest(t.Context(), d)
	if err != nil || len(manifest.Chunks) < 2 {
		t.Fatalf("multi-chunk fixture: %v", err)
	}
	unrelated := cas.Sum([]byte("unrelated local content"))
	if err := store.Put(t.Context(), unrelated, bytes.NewReader([]byte("unrelated local content"))); err != nil {
		t.Fatal(err)
	}
	responses := make(chan bsmessage.BitSwapMessage, 32)
	receiver.host.SetStreamHandler(seedBitswapProtocol, func(stream network.Stream) {
		defer stream.Close()
		_ = stream.SetDeadline(time.Now().Add(time.Second))
		message, _, err := bsmessage.FromNet(stream)
		if err == nil {
			responses <- message
		}
	})
	want := func(d cas.Digest, cancel bool) {
		t.Helper()
		message := bsmessage.New(false)
		if cancel {
			message.Cancel(seedCID(t, d))
		} else {
			message.AddEntry(seedCID(t, d), 1, bspb.Message_Wantlist_Block, true)
		}
		if err := receiver.seeds.send(t.Context(), source.ID(), message); err != nil {
			t.Fatal(err)
		}
	}
	assertSilent := func() {
		t.Helper()
		select {
		case response := <-responses:
			t.Fatalf("unauthorized response: %d blocks, %d presences", len(response.Blocks()), len(response.BlockPresences()))
		case <-time.After(100 * time.Millisecond):
		}
	}
	want(manifest.Chunks[0], false)
	assertSilent()
	offers := make(chan network.Stream, 1)
	receiver.host.SetStreamHandler(SeedOfferProtocol, func(stream network.Stream) {
		var request [40]byte
		if _, err := io.ReadFull(stream, request[:]); err != nil {
			_ = stream.Reset()
			return
		}
		offers <- stream // The test owns this stream until cancellation below.
	})
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	result := make(chan seedOfferResult, 1)
	go func() { ok, err := source.Offer(ctx, receiver.ID(), d); result <- seedOfferResult{ok, err} }()
	select {
	case stream := <-offers:
		defer stream.Reset()
	case <-time.After(3 * time.Second):
		t.Fatal("offer did not start")
	}
	want(manifest.Chunks[0], false)
	select {
	case message := <-responses:
		if len(message.Blocks()) != 1 || cas.Sum(message.Blocks()[0].RawData()) != manifest.Chunks[0] {
			t.Fatal("authorized chunk did not arrive intact")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("authorized chunk was not served")
	}
	for range 10 {
		want(manifest.Chunks[0], true)
		want(manifest.Chunks[0], false)
		want(unrelated, false)
	}
	assertSilent()
	cancel()
	if received := waitSeedResult(t, result); received.complete || !errors.Is(received.err, context.Canceled) {
		t.Fatalf("offer cancellation: %+v", received)
	}
	want(manifest.Chunks[1], false) // This occurrence still had allowance.
	assertSilent()
	// Revoking seed permission does not affect ordinary read-through fetching.
	if err := receiver.Fetch(t.Context(), d); err != nil {
		t.Fatalf("ordinary fetch changed: %v", err)
	}
}

func TestSeedDuplicateChunksAcrossFetchWindows(t *testing.T) {
	source, store, _ := seedNode(t, Config{})
	receiver, replica, _ := seedNode(t, Config{AcceptSeeds: true})
	connectSeedNodes(t, source, receiver)
	data := make([]byte, 8<<20)
	d := cas.Sum(data)
	if err := store.Put(t.Context(), d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	manifest, err := store.Manifest(t.Context(), d)
	if err != nil {
		t.Fatal(err)
	}
	counts := make(map[cas.Digest]int)
	for _, chunk := range manifest.Chunks {
		counts[chunk]++
	}
	duplicates := false
	for _, count := range counts {
		duplicates = duplicates || count > fetchWindow
	}
	if !duplicates {
		t.Fatal("fixture needs identical chunks across fetch windows")
	}
	if complete, err := source.Offer(t.Context(), receiver.ID(), d); err != nil || !complete {
		t.Fatalf("duplicate chunk transfer: %t %v", complete, err)
	}
	source.seeds.servingMu.Lock()
	grants := len(source.seeds.grants)
	source.seeds.servingMu.Unlock()
	if grants != 0 {
		t.Fatal("completed offer retained serving permission")
	}
	if complete, err := replica.Has(t.Context(), d); err != nil || !complete {
		t.Fatalf("duplicate chunk replica: %t %v", complete, err)
	}
}

func BenchmarkSeedResponseAuthorization(b *testing.B) {
	for _, size := range []int{256 << 10, cas.MaxChunkSize} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			data := make([]byte, size)
			d := cas.Sum(data)
			manifest := cas.Manifest{Blob: d, Chunks: []cas.Digest{d}}
			svc := &seedService{node: &Node{timeout: time.Second}}
			responses := &seedResponseNetwork{service: svc, send: func(context.Context, peer.ID, bsmessage.BitSwapMessage) error { return nil }}
			message := seedBlockMessage(b, data, d)
			b.SetBytes(int64(size))
			b.ReportAllocs()
			for b.Loop() {
				grant := svc.beginGrant(b.Context(), "recipient", d)
				if err := svc.authorizeGrant(grant, manifest); err != nil {
					b.Fatal(err)
				}
				svc.wantChunk("recipient", d)
				if err := responses.SendMessage(b.Context(), "recipient", message); err != nil {
					b.Fatal(err)
				}
				svc.endGrant(grant)
			}
		})
	}
}
