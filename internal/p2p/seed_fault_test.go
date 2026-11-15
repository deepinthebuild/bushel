package p2p

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"sync/atomic"
	"testing"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	bbdigest "github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/teststorage"
	"github.com/libp2p/go-libp2p/core/network"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type seedOfferResult struct {
	complete bool
	err      error
}

func waitSeedResult(t *testing.T, results <-chan seedOfferResult) seedOfferResult {
	t.Helper()
	select {
	case result := <-results:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("offer did not finish promptly")
		return seedOfferResult{}
	}
}

func waitSeedIdle(t *testing.T, n *Node) {
	t.Helper()
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		n.seeds.mu.Lock()
		active := len(n.seeds.admission.active)
		n.seeds.mu.Unlock()
		n.seeds.pendingMu.Lock()
		pending := len(n.seeds.pending)
		n.seeds.pendingMu.Unlock()
		if active == 0 && pending == 0 {
			return
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("seed admission or window leaked")
		}
	}
}

func TestSeedDuplicateAdmissionAndCancellation(t *testing.T) {
	for _, mode := range []string{"cancel offer", "close receiver"} {
		t.Run(mode, func(t *testing.T) {
			source, sourceStore, _ := seedNode(t, Config{})
			receiver, replicaStore, receiverBackend := seedNode(t, Config{AcceptSeeds: true})
			connectSeedNodes(t, source, receiver)
			data := testData(700000)
			d := cas.Sum(data)
			if err := sourceStore.Put(t.Context(), d, bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
			entered := make(chan struct{}, 1)
			var probes atomic.Int32
			receiverBackend.SetHook(func(ctx context.Context, op teststorage.Operation, _ bbdigest.Digest) error {
				if op == teststorage.MappingGet {
					probes.Add(1)
					select {
					case entered <- struct{}{}:
					default:
					}
					<-ctx.Done()
					return ctx.Err()
				}
				return nil
			})
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			results := make(chan seedOfferResult, 1)
			go func() { ok, err := source.Offer(ctx, receiver.ID(), d); results <- seedOfferResult{ok, err} }()
			select {
			case <-entered:
			case <-time.After(3 * time.Second):
				t.Fatal("receiver did not start admission")
			}
			if ok, err := source.Offer(t.Context(), receiver.ID(), d); err != nil || ok {
				t.Fatalf("duplicate admission: %t %v", ok, err)
			}
			if probes.Load() != 1 {
				t.Fatal("duplicate offer triggered another pull")
			}
			if mode == "cancel offer" {
				cancel()
			} else if err := receiver.Close(); err != nil {
				t.Fatal(err)
			}
			result := waitSeedResult(t, results)
			if result.complete || result.err == nil {
				t.Fatalf("canceled offer acknowledged: %+v", result)
			}
			if mode == "cancel offer" && !errors.Is(result.err, context.Canceled) {
				t.Fatalf("caller cancellation hidden: %v", result.err)
			}
			waitSeedIdle(t, receiver)
			receiverBackend.SetHook(nil)
			if present, err := replicaStore.Committed(t.Context(), d); err != nil || present {
				t.Fatalf("partial seed published: %t %v", present, err)
			}
			if err := source.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func installSeedManifest(t *testing.T, n *Node, manifest cas.Manifest) {
	t.Helper()
	payload, err := manifest.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	n.host.SetStreamHandler(ManifestProtocol, func(stream network.Stream) {
		defer stream.Close()
		_ = stream.SetDeadline(time.Now().Add(2 * time.Second))
		var request [40]byte
		if _, err := io.ReadFull(stream, request[:]); err != nil {
			return
		}
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
		_, _ = stream.Write(header[:])
		_, _ = stream.Write(payload)
	})
}

func seedSetMapping(t *testing.T, backend *teststorage.Store, manifest cas.Manifest) {
	t.Helper()
	function := bbdigest.MustNewFunction("", repb.DigestFunction_SHA256)
	keys := make([]bbdigest.Digest, len(manifest.Chunks))
	for i, d := range manifest.Chunks {
		var err error
		keys[i], err = function.NewDigest(d.Hex(), d.Size)
		if err != nil {
			t.Fatal(err)
		}
	}
	mapping, err := chunk.NewMappingFromDigests(keys, uint64(manifest.Blob.Size), true)
	if err != nil {
		t.Fatal(err)
	}
	key, err := function.NewDigest(manifest.Blob.Hex(), manifest.Blob.Size)
	if err != nil {
		t.Fatal(err)
	}
	backend.SetMapping(key, mapping)
}

func TestSeedRejectsMaliciousComposition(t *testing.T) {
	for _, mode := range []string{"full digest", "noncanonical cuts"} {
		t.Run(mode, func(t *testing.T) {
			source, sourceStore, sourceBackend := seedNode(t, Config{})
			receiver, replicaStore, _ := seedNode(t, Config{AcceptSeeds: true})
			connectSeedNodes(t, source, receiver)
			data := testData(700000)
			d := cas.Sum(data)
			if err := sourceStore.Put(t.Context(), d, bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
			manifest, err := sourceStore.Manifest(t.Context(), d)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "full digest" {
				modified := bytes.Clone(data)
				modified[0] ^= 1
				d = cas.Sum(modified)
				manifest.Blob = d
			} else {
				cut := cas.MinChunkSize
				if manifest.Chunks[0].Size == int64(cut) {
					cut++
				}
				manifest.Chunks = nil
				for _, chunk := range [][]byte{data[:cut], data[cut:]} {
					c := cas.Sum(chunk)
					if err := sourceStore.Put(t.Context(), c, bytes.NewReader(chunk)); err != nil {
						t.Fatal(err)
					}
					manifest.Chunks = append(manifest.Chunks, c)
				}
			}
			// A hostile source may advertise a structurally valid mapping whose
			// verified chunks do not reproduce the blob or canonical CDC cuts.
			seedSetMapping(t, sourceBackend, manifest)
			installSeedManifest(t, source, manifest)
			if ok, err := source.Offer(t.Context(), receiver.ID(), d); err != nil || ok {
				t.Fatalf("malicious composition accepted: %t %v", ok, err)
			}
			if present, err := replicaStore.Committed(t.Context(), d); err != nil || present {
				t.Fatalf("malicious composition published: %t %v", present, err)
			}
			waitSeedIdle(t, receiver)
		})
	}
}

func TestSeedVerifiedExistingDoesNotNeedSource(t *testing.T) {
	for _, mode := range []string{"verified", "corrupt", "backend error"} {
		t.Run(mode, func(t *testing.T) {
			source, _, _ := seedNode(t, Config{})
			receiver, store, backend := seedNode(t, Config{AcceptSeeds: true})
			connectSeedNodes(t, source, receiver)
			data := testData(700000)
			d := cas.Sum(data)
			if err := store.Put(t.Context(), d, bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
			var pulls atomic.Int32
			source.host.SetStreamHandler(ManifestProtocol, func(stream network.Stream) { pulls.Add(1); source.serveManifest(stream) })
			if mode == "corrupt" {
				for key := range backend.SnapshotChunks() {
					backend.CorruptChunk(key, []byte("corrupt"))
					break
				}
			} else if mode == "backend error" {
				backend.Fail(teststorage.ChunkGet, 0, status.Error(codes.Unavailable, "backend unavailable"))
			}
			backend.ResetCalls()
			ok, err := source.Offer(t.Context(), receiver.ID(), d)
			if err != nil || ok != (mode == "verified") {
				t.Fatalf("existing verification: complete=%t error=%v", ok, err)
			}
			verifiedRead := false
			for _, call := range backend.Calls() {
				if call.Operation == teststorage.ChunkGet {
					verifiedRead = true
				}
				if call.Operation == teststorage.ChunkPut || call.Operation == teststorage.MappingPut {
					t.Fatal("existing verification rewrote backend data")
				}
			}
			if !verifiedRead {
				t.Fatal("receipt trusted presence without reading bytes")
			}
			if mode != "corrupt" && pulls.Load() != 0 {
				t.Fatal("existing data or a backend failure triggered source pull")
			}
		})
	}
}

func TestSeedConcurrentSourcesWithSharedChunks(t *testing.T) {
	firstSource, firstStore, _ := seedNode(t, Config{})
	secondSource, secondStore, _ := seedNode(t, Config{})
	receiver, replicaStore, backend := seedNode(t, Config{AcceptSeeds: true})
	connectSeedNodes(t, firstSource, receiver)
	connectSeedNodes(t, secondSource, receiver)
	first := testData(3 << 20)
	second := bytes.Clone(first)
	second[len(second)-1] ^= 1
	digests := []cas.Digest{cas.Sum(first), cas.Sum(second)}
	stores := []*cas.Store{firstStore, secondStore}
	for i, data := range [][]byte{first, second} {
		if err := stores[i].Put(t.Context(), digests[i], bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
	}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	backend.SetHook(func(ctx context.Context, op teststorage.Operation, _ bbdigest.Digest) error {
		if op == teststorage.MappingGet {
			select {
			case entered <- struct{}{}:
			default:
			}
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	results := make(chan seedOfferResult, 2)
	sources := []*Node{firstSource, secondSource}
	for i, d := range digests {
		source := sources[i]
		go func() { ok, err := source.Offer(t.Context(), receiver.ID(), d); results <- seedOfferResult{ok, err} }()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("concurrent offers did not reserve two slots")
		}
	}
	close(release)
	for range 2 {
		if result := waitSeedResult(t, results); result.err != nil || !result.complete {
			t.Fatalf("shared chunk transfer: %+v", result)
		}
	}
	for _, d := range digests {
		if ok, err := replicaStore.Has(t.Context(), d); err != nil || !ok {
			t.Fatalf("shared replica missing: %t %v", ok, err)
		}
	}
	waitSeedIdle(t, receiver)
}

func TestSeedConcurrentDistinctRecipients(t *testing.T) {
	source, store, _ := seedNode(t, Config{})
	data := testData(700000)
	d := cas.Sum(data)
	if err := store.Put(t.Context(), d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	results := make(chan seedOfferResult, 2)
	var replicas []*cas.Store
	for range 2 {
		receiver, replica, backend := seedNode(t, Config{AcceptSeeds: true})
		connectSeedNodes(t, source, receiver)
		replicas = append(replicas, replica)
		backend.SetHook(func(ctx context.Context, op teststorage.Operation, _ bbdigest.Digest) error {
			if op == teststorage.MappingGet {
				select {
				case entered <- struct{}{}:
				default:
				}
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return nil
		})
		go func() { ok, err := source.Offer(t.Context(), receiver.ID(), d); results <- seedOfferResult{ok, err} }()
	}
	for range 2 {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			close(release)
			t.Fatal("outgoing offers to different recipients did not overlap")
		}
	}
	close(release)
	for range 2 {
		if result := waitSeedResult(t, results); result.err != nil || !result.complete {
			t.Fatalf("concurrent recipients: %+v", result)
		}
	}
	for _, replica := range replicas {
		if present, err := replica.Has(t.Context(), d); err != nil || !present {
			t.Fatalf("recipient replica missing: %t %v", present, err)
		}
	}
}
