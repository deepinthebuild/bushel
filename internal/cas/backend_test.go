package cas

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	bbdigest "github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/deepinthebuild/bushel/internal/teststorage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"pgregory.net/rapid"
)

func TestPropertyBackendFailureLeavesOnlyVerifiedOrphans(t *testing.T) {
	parent := t.TempDir()
	rapid.Check(t, func(t *rapid.T) {
		f := newPropertyStore(t, parent, 4<<20)
		data := propertyData(rapid.IntRange(2*MinChunkSize+1, 3*MinChunkSize-1).Draw(t, "size"), rapid.Uint64().Draw(t, "seed"), 0)
		d := Sum(data)
		m, err := Chunk(context.Background(), d, bytes.NewReader(data), func(Digest, []byte) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		failAt := rapid.IntRange(0, len(m.Chunks)).Draw(t, "failure position")
		failure := status.Error(codes.Unavailable, "backend write unavailable")
		if failAt == len(m.Chunks) {
			f.backend.Fail(teststorage.MappingPut, 0, failure)
		} else {
			f.backend.Fail(teststorage.ChunkPut, failAt, failure)
		}
		hints, _, unsubscribe := f.store.SubscribeCommits(1)
		defer unsubscribe()
		notified, cancel := f.store.WatchCommit(d)
		defer cancel()
		if err := f.store.Put(context.Background(), d, bytes.NewReader(data)); !errors.Is(err, failure) {
			t.Fatalf("backend failure lost: %v", err)
		}
		chunks, mappings := f.backend.Counts()
		if chunks != failAt || mappings != 0 {
			t.Fatalf("partial writes: chunks %d, mappings %d, expected chunks %d", chunks, mappings, failAt)
		}
		for key, stored := range f.backend.SnapshotChunks() {
			got := Sum(stored)
			if got.Hex() != key.GetHashString() || got.Size != key.GetSizeBytes() {
				t.Fatal("failed upload left an unverified orphan")
			}
		}
		if _, err := os.Stat(f.store.catalogPath(d)); !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("failed backend write published a catalog record: %v", err)
		}
		if len(hints) != 0 {
			t.Fatal("failed write emitted a publication hint")
		}
		select {
		case <-notified:
			t.Fatal("failed write notified an upload waiter")
		default:
		}
		if err := f.store.Put(context.Background(), d, bytes.NewReader(data)); err != nil {
			t.Fatalf("retry failed: %v", err)
		}
		calls := f.backend.Calls()
		var last teststorage.Call
		for _, call := range calls {
			if call.Operation == teststorage.ChunkPut || call.Operation == teststorage.MappingPut {
				last = call
			}
		}
		if last.Operation != teststorage.MappingPut || last.Digest != backendDigest(d) {
			t.Fatal("complete blob mapping was not the last backend write")
		}
		checkStoreModel(t, f.store, []modelBlob{{data: data, digest: d, published: true}})
		if len(hints) != 1 {
			t.Fatal("successful retry did not publish")
		}
	})
}

func TestAmbiguousBackendWriteDoesNotPublishCatalog(t *testing.T) {
	s, backend := testStoreWithBackend(t)
	data := fixture(2*MinChunkSize + 1)
	d := Sum(data)
	failure := status.Error(codes.Unavailable, "response lost after storage committed")
	backend.FailAfterWrite(teststorage.MappingPut, 0, failure)
	hints, _, cancel := s.SubscribeCommits(1)
	defer cancel()
	if err := s.Put(context.Background(), d, bytes.NewReader(data)); !errors.Is(err, failure) {
		t.Fatal(err)
	}
	// Backend presence and Bushel's durable publication catalog are distinct.
	if present, err := s.Committed(context.Background(), d); err != nil || !present {
		t.Fatalf("ambiguous backend commit was not present: %v", err)
	}
	if len(hints) != 0 {
		t.Fatal("ambiguous failure emitted a success hint")
	}
	if err := s.WalkCommitted(context.Background(), func(Digest) error {
		t.Error("failed request entered the announcement catalog")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if err := s.Put(context.Background(), d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	if len(hints) != 1 {
		t.Fatal("retry failed to publish existing backend bytes")
	}
}

func TestPropertyBackendEvictionCorruptionAndRepair(t *testing.T) {
	parent := t.TempDir()
	rapid.Check(t, func(t *rapid.T) {
		f := newPropertyStore(t, parent, 4<<20)
		data := propertyData(rapid.IntRange(2*MinChunkSize+1, 3*MinChunkSize-1).Draw(t, "size"), rapid.Uint64().Draw(t, "seed"), 0)
		d := Sum(data)
		if err := f.store.Put(context.Background(), d, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		m, err := f.store.Manifest(context.Background(), d)
		if err != nil {
			t.Fatal(err)
		}
		wantError := ErrNotFound
		presenceHint := false
		switch rapid.IntRange(0, 4).Draw(t, "backend fault") {
		case 0:
			f.backend.EvictChunk(backendDigest(m.Chunks[0]))
		case 1:
			f.backend.EvictMapping(backendDigest(d))
		case 2:
			bad := bytes.Clone(data[:m.Chunks[0].Size])
			bad[rapid.IntRange(0, len(bad)-1).Draw(t, "corrupt offset")] ^= 1
			f.backend.CorruptChunk(backendDigest(m.Chunks[0]), bad)
			wantError, presenceHint = ErrCorrupt, true
		case 3:
			// Individually valid chunks must not authenticate the wrong blob.
			bad := bytes.Clone(data[:m.Chunks[0].Size])
			bad[0] ^= 1
			changed := Sum(bad)
			f.backend.CorruptChunk(backendDigest(changed), bad)
			keys := []bbdigest.Digest{backendDigest(changed), backendDigest(m.Chunks[1])}
			mapping, err := chunk.NewMappingFromDigests(keys, uint64(d.Size), true)
			if err != nil {
				t.Fatal(err)
			}
			f.backend.SetMapping(backendDigest(d), mapping)
			wantError, presenceHint = ErrCorrupt, true
		case 4:
			f.backend.EvictChunk(backendDigest(m.Chunks[0]))
			f.backend.SetStalePresence(true)
			presenceHint = true
		}
		if present, err := f.store.Committed(context.Background(), d); err != nil || present != presenceHint {
			t.Fatalf("presence hint: got %v, want %v, err %v", present, presenceHint, err)
		}
		if has, err := f.store.Has(context.Background(), d); err != nil || has {
			t.Fatalf("unavailable or corrupt blob passed verification: %v", err)
		}
		r, err := f.store.Open(context.Background(), d)
		if r != nil {
			_ = r.Close()
			t.Fatal("unverified bytes escaped through a reader")
		}
		if !errors.Is(err, wantError) {
			t.Fatalf("read failure: got %v, want %v", err, wantError)
		}
		if entries, err := os.ReadDir(filepath.Join(f.root, "tmp")); err != nil || len(entries) != 0 {
			t.Fatalf("failed read retained scratch bytes: %v", err)
		}
		var announced int
		if err := f.store.WalkCommitted(context.Background(), func(Digest) error { announced++; return nil }); err != nil {
			t.Fatal(err)
		}
		if (announced == 1) != presenceHint {
			t.Fatal("catalog sweep disagreed with backend presence observation")
		}
		if !presenceHint {
			if _, err := os.Stat(f.store.catalogPath(d)); !errors.Is(err, os.ErrNotExist) || f.store.used != 0 {
				t.Fatalf("missing backend data retained catalog quota: %v", err)
			}
		}
		// A stale positive must never cause a verified re-upload to skip repair.
		if err := f.store.Put(context.Background(), d, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		f.backend.SetStalePresence(false)
		f.reopen(t)
		checkStoreModel(t, f.store, []modelBlob{{data: data, digest: d, published: true}})
	})
}

func TestBackendObjectsDoNotRequireCatalogMembership(t *testing.T) {
	s, backend := testStoreWithBackend(t)
	data := []byte("populated outside Bushel")
	d := Sum(data)
	backend.CorruptChunk(backendDigest(d), data)
	if present, err := s.Committed(context.Background(), d); err != nil || !present {
		t.Fatalf("external object missing: %v", err)
	}
	r, err := s.Open(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	got, readErr := io.ReadAll(r)
	closeErr := r.Close()
	if readErr != nil || closeErr != nil || !bytes.Equal(got, data) {
		t.Fatalf("external object read: %v %v", readErr, closeErr)
	}
	if err := s.WalkCommitted(context.Background(), func(Digest) error {
		t.Error("backend contents were silently adopted into the catalog")
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

type borrowedChunkReader struct{ data []byte }

func (r borrowedChunkReader) Read(context.Context, bbdigest.Digest) ([]byte, error) {
	return r.data, nil
}

func TestBlockVerifiesConfiguredCacheAndReturnsOwnedBytes(t *testing.T) {
	data := []byte("cached bytes")
	backend := testBackend(teststorage.New())
	backend.ChunkReader = borrowedChunkReader{data}
	s, err := NewStore(t.TempDir(), 1<<20, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	d := Sum(data)
	got, err := s.Block(context.Background(), d)
	if err != nil {
		t.Fatal(err)
	}
	got[0] ^= 1
	if Sum(data) != d {
		t.Fatal("caller mutated a shared Buildbarn cache buffer")
	}
	data[0] ^= 1
	if got, err := s.Block(context.Background(), d); got != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt cached bytes escaped: %v", err)
	}
}

func TestBackendInstanceAndCancellation(t *testing.T) {
	fake := teststorage.New()
	instance, err := bbdigest.NewInstanceName("team/cache")
	if err != nil {
		t.Fatal(err)
	}
	backend := testBackend(fake)
	backend.InstanceName = instance
	s, err := NewStore(t.TempDir(), 1<<20, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	data := []byte("instance scoped")
	d := Sum(data)
	if err := s.Put(context.Background(), d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	function := bbdigest.MustNewFunction("team/cache", repb.DigestFunction_SHA256)
	key, err := function.NewDigest(d.Hex(), d.Size)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fake.Chunks.Get(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if _, err := fake.Chunks.Get(context.Background(), backendDigest(d)); status.Code(err) != codes.NotFound {
		t.Fatal("backend write leaked into another instance")
	}
	fake.ResetCalls()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.Put(ctx, d, bytes.NewReader(data)); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.Manifest(ctx, d); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.Committed(ctx, d); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := s.Block(ctx, d); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if r, err := s.Open(ctx, d); r != nil || !errors.Is(err, context.Canceled) {
		if r != nil {
			_ = r.Close()
		}
		t.Fatal(err)
	}
	if len(fake.Calls()) != 0 {
		t.Fatal("pre-canceled operations reached the backend")
	}
}

type fixedMappingReader struct{ mapping chunk.Mapping }

func (r fixedMappingReader) FetchChunkMapping(context.Context, bbdigest.Digest) (chunk.Mapping, error) {
	return r.mapping, nil
}

func TestManifestValidatesConfiguredMappingCache(t *testing.T) {
	d := Sum(fixture(2*MinChunkSize + 1))
	function := bbdigest.MustNewFunction("", repb.DigestFunction_BLAKE3)
	first, err := function.NewDigest(Sum([]byte("first")).Hex(), MinChunkSize)
	if err != nil {
		t.Fatal(err)
	}
	second, err := function.NewDigest(Sum([]byte("second")).Hex(), MinChunkSize+1)
	if err != nil {
		t.Fatal(err)
	}
	wrongAlgorithm, err := chunk.NewMappingFromDigests([]bbdigest.Digest{first, second}, uint64(d.Size), true)
	if err != nil {
		t.Fatal(err)
	}
	badBounds, err := chunk.NewMappingFromDigests([]bbdigest.Digest{
		backendDigest(Digest{Hash: d.Hash, Size: 1}),
		backendDigest(Digest{Hash: d.Hash, Size: d.Size - 1}),
	}, uint64(d.Size), true)
	if err != nil {
		t.Fatal(err)
	}
	for name, mapping := range map[string]chunk.Mapping{"empty": {}, "wrong algorithm": wrongAlgorithm, "invalid chunk bounds": badBounds} {
		t.Run(name, func(t *testing.T) {
			backend := testBackend(teststorage.New())
			backend.MappingReader = fixedMappingReader{mapping}
			s, err := NewStore(t.TempDir(), 1<<20, backend)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			if _, err := s.Manifest(context.Background(), d); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("trusted malformed cache mapping: %v", err)
			}
		})
	}
}

func TestPropertyBlockRejectsWrongSizeAndDigest(t *testing.T) {
	parent := t.TempDir()
	rapid.Check(t, func(t *rapid.T) {
		f := newPropertyStore(t, parent, 1<<20)
		data := propertyData(rapid.IntRange(1, MaxChunkSize).Draw(t, "chunk size"), rapid.Uint64().Draw(t, "seed"), 0)
		d := Sum(data)
		bad := bytes.Clone(data)
		switch rapid.IntRange(0, 2).Draw(t, "response corruption") {
		case 0:
			bad = bad[:len(bad)-1]
		case 1:
			bad = append(bad, 0)
		case 2:
			bad[rapid.IntRange(0, len(bad)-1).Draw(t, "byte")] ^= 1
		}
		f.backend.CorruptChunk(backendDigest(d), bad)
		if got, err := f.store.Block(context.Background(), d); got != nil || !errors.Is(err, ErrCorrupt) {
			t.Fatalf("unverified backend response escaped: %v", err)
		}
	})
}

func TestBackendReadRejectsNoncanonicalMapping(t *testing.T) {
	s, backend := testStoreWithBackend(t)
	data := fixture(2*MinChunkSize + 1)
	d := Sum(data)
	canonical, err := Chunk(context.Background(), d, bytes.NewReader(data), func(Digest, []byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	cut := MinChunkSize
	if canonical.Chunks[0].Size == int64(cut) {
		cut++
	}
	keys := make([]bbdigest.Digest, 0, 2)
	for _, part := range [][]byte{data[:cut], data[cut:]} {
		partDigest := Sum(part)
		key := backendDigest(partDigest)
		backend.CorruptChunk(key, part)
		keys = append(keys, key)
		if _, err := s.Block(context.Background(), partDigest); err != nil {
			t.Fatalf("fixture chunk must have a valid digest: %v", err)
		}
	}
	mapping, err := chunk.NewMappingFromDigests(keys, uint64(d.Size), true)
	if err != nil {
		t.Fatal(err)
	}
	backend.SetMapping(backendDigest(d), mapping)
	if has, err := s.Has(context.Background(), d); err != nil || has {
		t.Fatalf("noncanonical external mapping passed verification: %v", err)
	}
	if r, err := s.Open(context.Background(), d); r != nil || !errors.Is(err, ErrCorrupt) {
		if r != nil {
			_ = r.Close()
		}
		t.Fatalf("noncanonical external mapping exposed a reader: %v", err)
	}
}

func TestPropertyRepeatedChunksRepairAfterRestart(t *testing.T) {
	parent := t.TempDir()
	rapid.Check(t, func(t *rapid.T) {
		f := newPropertyStore(t, parent, 8<<20)
		size := rapid.IntRange(3*MaxChunkSize, 4*MaxChunkSize).Draw(t, "size")
		data := bytes.Repeat([]byte{rapid.Byte().Draw(t, "byte")}, size)
		d := Sum(data)
		m, err := Chunk(context.Background(), d, bytes.NewReader(data), func(Digest, []byte) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		unique := map[Digest]bool{}
		for _, c := range m.Chunks {
			unique[c] = true
		}
		if len(unique) == len(m.Chunks) {
			t.Fatal("fixture did not generate repeated chunks")
		}
		for step := 0; step < 3; step++ {
			if step == 1 {
				f.reopen(t)
				f.backend.SetStalePresence(true)
				for c := range unique {
					f.backend.CorruptChunk(backendDigest(c), []byte("corrupt"))
				}
			}
			f.backend.ResetCalls()
			if err := f.store.Put(context.Background(), d, bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
			written := map[bbdigest.Digest]int{}
			for _, call := range f.backend.Calls() {
				if call.Operation == teststorage.ChunkPut {
					written[call.Digest]++
				}
			}
			if len(written) != len(unique) {
				t.Fatalf("wrote %d distinct chunks, want %d", len(written), len(unique))
			}
			for _, count := range written {
				if count != 1 {
					t.Fatalf("wrote a chunk %d times", count)
				}
			}
			checkStoreModel(t, f.store, []modelBlob{{data: data, digest: d, published: true}})
		}
	})
}
