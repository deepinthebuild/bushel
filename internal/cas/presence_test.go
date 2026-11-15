package cas

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	bbdigest "github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/deepinthebuild/bushel/internal/teststorage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"pgregory.net/rapid"
)

// Presence fixtures intentionally contain no authenticated bytes: existence
// probes must neither fetch chunk contents nor claim to verify their integrity.
func presenceDigest(id uint64, size int64) Digest {
	if size == 0 {
		return Sum(nil)
	}
	var data [8]byte
	binary.LittleEndian.PutUint64(data[:], id)
	d := Sum(data[:])
	d.Size = size
	return d
}

func presenceMapping(t testing.TB, chunks []Digest) chunk.Mapping {
	t.Helper()
	keys := make([]bbdigest.Digest, len(chunks))
	var size uint64
	for i, d := range chunks {
		keys[i] = backendDigest(d)
		size += uint64(d.Size)
	}
	mapping, err := chunk.NewMappingFromDigests(keys, size, true)
	if err != nil {
		t.Fatal(err)
	}
	return mapping
}

func countPresenceCalls(calls []teststorage.Call, operation teststorage.Operation) int {
	var count int
	for _, call := range calls {
		if call.Operation == operation {
			count++
		}
	}
	return count
}

func TestFindMissingSmallBatchUsesOneBackendCall(t *testing.T) {
	s, backend := testStoreWithBackend(t)
	blobs := make([]Digest, MaxPresenceBatch)
	var want []Digest
	for i := range blobs {
		blobs[i] = presenceDigest(uint64(i), int64(i+1))
		if i%3 == 0 {
			want = append(want, blobs[i])
		} else {
			backend.CorruptChunk(backendDigest(blobs[i]), nil)
		}
	}
	got, err := s.FindMissing(context.Background(), blobs)
	if err != nil || !slices.Equal(got, want) {
		t.Fatalf("missing blobs: got %d, want %d, error %v", len(got), len(want), err)
	}
	if calls := backend.Calls(); len(calls) != 1 || calls[0].Operation != teststorage.ChunkFindMissing {
		t.Fatalf("1024 small blobs required more than one presence request: %v", calls)
	}
	backend.ResetCalls()
	if present, err := s.Committed(context.Background(), blobs[1]); !present || err != nil {
		t.Fatalf("singleton presence: %v, %v", present, err)
	}
	if calls := backend.Calls(); len(calls) != 1 || calls[0].Operation != teststorage.ChunkFindMissing {
		t.Fatalf("singleton duplicated its presence request: %v", calls)
	}
}

func TestPropertyFindMissingMatchesPresenceModel(t *testing.T) {
	parent := t.TempDir()
	rapid.Check(t, func(t *rapid.T) {
		f := newPropertyStore(t, parent, 1<<20)
		stale := rapid.Bool().Draw(t, "stale presence")
		f.backend.SetStalePresence(stale)
		pool := make([]Digest, 16)
		present := make(map[Digest]bool)
		for i := range pool {
			pool[i] = presenceDigest(uint64(i), rapid.Int64Range(MinChunkSize, MaxChunkSize).Draw(t, "chunk size"))
			present[pool[i]] = rapid.Bool().Draw(t, "chunk present")
			if present[pool[i]] {
				f.backend.CorruptChunk(backendDigest(pool[i]), nil)
			}
		}
		choices := []Digest{Sum(nil)}
		model := map[Digest]bool{Sum(nil): true}
		chunkCounts := make(map[Digest]int)
		for i := range rapid.IntRange(1, 32).Draw(t, "blobs") {
			if rapid.Bool().Draw(t, "small") {
				d := presenceDigest(uint64(i+100), rapid.Int64Range(1, MaxChunkSize).Draw(t, "blob size"))
				exists := rapid.Bool().Draw(t, "small present")
				if exists {
					f.backend.CorruptChunk(backendDigest(d), nil)
				}
				choices = append(choices, d)
				model[d] = exists || stale
				chunkCounts[d] = 1
				continue
			}
			chunks := make([]Digest, rapid.IntRange(2, 8).Draw(t, "chunks"))
			var size int64
			exists := true
			keys := make([]bbdigest.Digest, len(chunks))
			for j := range chunks {
				chunks[j] = pool[rapid.IntRange(0, len(pool)-1).Draw(t, "shared chunk")]
				size += chunks[j].Size
				exists = exists && (present[chunks[j]] || stale)
				keys[j] = backendDigest(chunks[j])
			}
			d := presenceDigest(uint64(i+100), size)
			switch rapid.IntRange(0, 2).Draw(t, "mapping state") {
			case 0:
				exists = false
			case 1:
				f.backend.SetMapping(backendDigest(d), chunk.Mapping{})
				exists = false
			case 2:
				mapping, err := chunk.NewMappingFromDigests(keys, uint64(size), true)
				if err != nil {
					t.Fatal(err)
				}
				f.backend.SetMapping(backendDigest(d), mapping)
				chunkCounts[d] = len(chunks)
			}
			choices = append(choices, d)
			model[d] = exists
		}
		request := make([]Digest, rapid.IntRange(0, 96).Draw(t, "request size"))
		var want []Digest
		unique := make(map[Digest]bool)
		var mappings, references int
		for i := range request {
			d := choices[rapid.IntRange(0, len(choices)-1).Draw(t, "requested blob")]
			request[i] = d
			if !model[d] {
				want = append(want, d)
			}
			if !unique[d] {
				unique[d] = true
				references += chunkCounts[d]
				if d.Size > MaxChunkSize {
					mappings++
				}
			}
		}
		got, err := f.store.FindMissing(context.Background(), request)
		if err != nil || !slices.Equal(got, want) {
			t.Fatalf("presence differs from model: got %v, want %v, error %v", got, want, err)
		}
		calls := f.backend.Calls()
		if countPresenceCalls(calls, teststorage.MappingGet) != mappings {
			t.Fatal("duplicate requests repeated mapping fetches")
		}
		if countPresenceCalls(calls, teststorage.ChunkFindMissing) > (references+presenceWindow-1)/presenceWindow {
			t.Fatal("chunk presence requests were not aggregated")
		}
		if countPresenceCalls(calls, teststorage.ChunkGet) != 0 {
			t.Fatal("presence fetched chunk bytes")
		}
	})
}

type recordingPresenceAccess struct {
	blobstore.BlobAccess[*chunk.Chunk]
	mu      sync.Mutex
	windows []int
}

func (a *recordingPresenceAccess) FindMissing(ctx context.Context, keys bbdigest.Set) (bbdigest.Set, error) {
	a.mu.Lock()
	a.windows = append(a.windows, keys.Length())
	a.mu.Unlock()
	return a.BlobAccess.FindMissing(ctx, keys)
}

func TestFindMissingBoundsMappingConcurrencyAndAggregatesAcrossWaves(t *testing.T) {
	s, backend := testStoreWithBackend(t)
	chunks := []Digest{presenceDigest(1, MinChunkSize), presenceDigest(2, MinChunkSize)}
	for _, d := range chunks {
		backend.CorruptChunk(backendDigest(d), nil)
	}
	mapping := presenceMapping(t, chunks)
	blobs := make([]Digest, MaxPresenceBatch)
	for i := range blobs {
		blobs[i] = presenceDigest(uint64(i+100), 2*MinChunkSize)
		backend.SetMapping(backendDigest(blobs[i]), mapping)
	}
	started := make(chan struct{}, presenceConcurrency)
	release := make(chan struct{})
	var active, peak atomic.Int32
	backend.SetHook(func(ctx context.Context, op teststorage.Operation, _ bbdigest.Digest) error {
		if op != teststorage.MappingGet {
			return nil
		}
		n := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); n > previous && !peak.CompareAndSwap(previous, n); previous = peak.Load() {
		}
		select {
		case started <- struct{}{}:
		default:
		}
		select {
		case <-release:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		missing, err := s.FindMissing(ctx, blobs)
		if err == nil && len(missing) != 0 {
			err = fmt.Errorf("unexpected missing blobs: %d", len(missing))
		}
		done <- err
	}()
	for range presenceConcurrency {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("mapping fetches were not concurrent")
		}
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if active.Load() != 0 || peak.Load() != presenceConcurrency {
		t.Fatalf("mapping worker bounds: active %d, peak %d", active.Load(), peak.Load())
	}
	if count := countPresenceCalls(backend.Calls(), teststorage.ChunkFindMissing); count != 2 {
		t.Fatalf("1024 two-chunk mappings required %d CS requests, want 2", count)
	}
}

func TestFindMissingBoundsChunkWindows(t *testing.T) {
	fake := teststorage.New()
	fake.SetStalePresence(true)
	recorder := &recordingPresenceAccess{BlobAccess: fake.Chunks}
	backend := testBackend(fake)
	backend.Chunks = recorder
	s, err := NewStore(t.TempDir(), 1<<20, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	chunks := make([]Digest, MaxChunks)
	for i := range chunks {
		chunks[i] = presenceDigest(uint64(i), MinChunkSize)
	}
	d := presenceDigest(100000, int64(len(chunks))*MinChunkSize)
	fake.SetMapping(backendDigest(d), presenceMapping(t, chunks))
	if missing, err := s.FindMissing(context.Background(), []Digest{d}); err != nil || len(missing) != 0 {
		t.Fatalf("maximum mapping presence: %v, %v", missing, err)
	}
	if len(recorder.windows) != MaxChunks/presenceWindow {
		t.Fatalf("maximum mapping required %d CS requests", len(recorder.windows))
	}
	for _, size := range recorder.windows {
		if size != presenceWindow {
			t.Fatalf("unbounded or fragmented CS request: %d keys", size)
		}
	}
}

func TestFindMissingClassifiesBackendErrors(t *testing.T) {
	for _, op := range []teststorage.Operation{teststorage.ChunkFindMissing, teststorage.MappingGet} {
		for _, code := range []codes.Code{codes.NotFound, codes.DataLoss, codes.Unavailable} {
			t.Run(fmt.Sprintf("%s/%s", op, code), func(t *testing.T) {
				s, backend := testStoreWithBackend(t)
				size := int64(1)
				if op == teststorage.MappingGet {
					size = 2 * MinChunkSize
				}
				d := presenceDigest(1, size)
				failure := status.Error(code, "injected presence failure")
				backend.Fail(op, 0, failure)
				got, err := s.FindMissing(context.Background(), []Digest{d, Sum(nil), d})
				if code == codes.Unavailable {
					if got != nil || !errors.Is(err, failure) {
						t.Fatalf("transient error was hidden: %v, %v", got, err)
					}
				} else if err != nil || !slices.Equal(got, []Digest{d, d}) {
					t.Fatalf("missing/corrupt presence: %v, %v", got, err)
				}
			})
		}
	}
}

func TestFindMissingRejectsInvalidBatchBeforeBackendCalls(t *testing.T) {
	s, backend := testStoreWithBackend(t)
	valid := presenceDigest(1, 1)
	for _, request := range [][]Digest{
		{valid, {Size: -1}},
		{valid, {Size: MaxBlobSize + 1}},
		{valid, {}},
		make([]Digest, MaxPresenceBatch+1),
	} {
		if missing, err := s.FindMissing(context.Background(), request); missing != nil || !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid batch accepted: %v, %v", missing, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if missing, err := s.FindMissing(ctx, []Digest{valid}); missing != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("pre-canceled batch accepted: %v, %v", missing, err)
	}
	if len(backend.Calls()) != 0 {
		t.Fatal("invalid or pre-canceled request reached backend")
	}
	if missing, err := s.FindMissing(context.Background(), []Digest{Sum(nil), Sum(nil)}); err != nil || len(missing) != 0 || len(backend.Calls()) != 0 {
		t.Fatalf("empty blobs required backend access: %v, %v", missing, err)
	}
}

func TestFindMissingCancelsAndJoinsMappingWorkers(t *testing.T) {
	s, backend := testStoreWithBackend(t)
	started := make(chan struct{}, presenceConcurrency)
	var active atomic.Int32
	backend.SetHook(func(ctx context.Context, op teststorage.Operation, _ bbdigest.Digest) error {
		if op != teststorage.MappingGet {
			return nil
		}
		active.Add(1)
		defer active.Add(-1)
		select {
		case started <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}
		<-ctx.Done()
		return ctx.Err()
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	blobs := make([]Digest, presenceConcurrency*2)
	for i := range blobs {
		blobs[i] = presenceDigest(uint64(i), 2*MinChunkSize)
	}
	done := make(chan error, 1)
	go func() {
		_, err := s.FindMissing(ctx, blobs)
		done <- err
	}()
	for range presenceConcurrency {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("mapping workers did not start")
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	if active.Load() != 0 || len(backend.Calls()) != presenceConcurrency {
		t.Fatal("canceled batch retained workers or started another wave")
	}
}

func BenchmarkFindMissingMemoryBackendSmallBatch(b *testing.B) {
	for _, size := range []int{1, MaxPresenceBatch} {
		b.Run(fmt.Sprint(size), func(b *testing.B) {
			backend := teststorage.New()
			s, err := NewStore(b.TempDir(), 1<<20, testBackend(backend))
			if err != nil {
				b.Fatal(err)
			}
			defer s.Close()
			blobs := make([]Digest, size)
			for i := range blobs {
				blobs[i] = presenceDigest(uint64(i), 1)
				backend.CorruptChunk(backendDigest(blobs[i]), nil)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if missing, err := s.FindMissing(context.Background(), blobs); err != nil || len(missing) != 0 {
					b.Fatal(err)
				}
			}
		})
	}
}
