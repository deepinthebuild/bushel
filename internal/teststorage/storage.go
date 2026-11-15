// Package teststorage provides an explicitly controlled, in-memory Buildbarn
// backend for tests. It deliberately permits corrupt responses and stale
// existence results so adapters can be exercised at their trust boundary.
package teststorage

import (
	"bytes"
	"context"
	"slices"
	"sync"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Operation string

const (
	ChunkGet           Operation = "chunk.get"
	ChunkPut           Operation = "chunk.put"
	ChunkFindMissing   Operation = "chunk.find_missing"
	MappingGet         Operation = "mapping.get"
	MappingPut         Operation = "mapping.put"
	MappingFindMissing Operation = "mapping.find_missing"
)

type Call struct {
	Operation Operation
	Digest    digest.Digest
}

type fault struct {
	after       int
	err         error
	commitFirst bool
}

// Store owns all fake state. Methods and backend operations are safe to use
// concurrently. Hooks run outside its mutex and may coordinate interleavings.
type Store struct {
	Chunks   blobstore.BlobAccess[*chunk.Chunk]
	Mappings blobstore.BlobAccess[chunk.Mapping]

	mu       sync.Mutex
	chunks   map[digest.Digest][]byte
	mappings map[digest.Digest]chunk.Mapping
	faults   map[Operation]fault
	hook     func(context.Context, Operation, digest.Digest) error
	stale    bool
	calls    []Call
}

func New() *Store {
	s := &Store{
		chunks: make(map[digest.Digest][]byte), mappings: make(map[digest.Digest]chunk.Mapping),
		faults: make(map[Operation]fault),
	}
	s.Chunks = chunkAccess{s}
	s.Mappings = mappingAccess{s}
	return s
}

// Fail injects one error after the specified number of calls to an operation.
// An after value of zero fails the next call. A nil error clears that fault.
func (s *Store) Fail(op Operation, after int, err error) {
	s.setFault(op, after, err, false)
}

// FailAfterWrite models an ambiguous backend response: the selected Put writes
// its value, then returns an error. Other operations are rejected.
func (s *Store) FailAfterWrite(op Operation, after int, err error) {
	if op != ChunkPut && op != MappingPut {
		panic("FailAfterWrite requires a Put operation")
	}
	s.setFault(op, after, err, true)
}

func (s *Store) setFault(op Operation, after int, err error, commitFirst bool) {
	if after < 0 {
		panic("fault delay must be nonnegative")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err == nil {
		delete(s.faults, op)
	} else {
		s.faults[op] = fault{after: after, err: err, commitFirst: commitFirst}
	}
}

func (s *Store) SetHook(hook func(context.Context, Operation, digest.Digest) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.hook = hook
}

// SetStalePresence makes both FindMissing methods claim every requested object
// exists, including objects that Get will report as missing or corrupt.
func (s *Store) SetStalePresence(stale bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stale = stale
}

func (s *Store) EvictChunk(d digest.Digest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.chunks, d)
}

func (s *Store) EvictMapping(d digest.Digest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.mappings, d)
}

// CorruptChunk installs arbitrary bytes under an existing or absent digest.
func (s *Store) CorruptChunk(d digest.Digest, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.chunks[d] = bytes.Clone(data)
}

func (s *Store) SetMapping(d digest.Digest, mapping chunk.Mapping) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mappings[d] = mapping
}

func (s *Store) Counts() (chunks, mappings int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.chunks), len(s.mappings)
}

func (s *Store) Calls() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.calls)
}

func (s *Store) ResetCalls() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = nil
}

func (s *Store) SnapshotChunks() map[digest.Digest][]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make(map[digest.Digest][]byte, len(s.chunks))
	for d, data := range s.chunks {
		result[d] = bytes.Clone(data)
	}
	return result
}

func (s *Store) before(ctx context.Context, op Operation, d digest.Digest) (error, bool) {
	if err := ctx.Err(); err != nil {
		return err, false
	}
	s.mu.Lock()
	// Keep diagnostic recording bounded even in long-running benchmarks.
	if len(s.calls) < 4096 {
		s.calls = append(s.calls, Call{Operation: op, Digest: d})
	}
	hook := s.hook
	var err error
	var commitFirst bool
	if f, exists := s.faults[op]; exists {
		if f.after == 0 {
			err, commitFirst = f.err, f.commitFirst
			delete(s.faults, op)
		} else {
			f.after--
			s.faults[op] = f
		}
	}
	s.mu.Unlock()
	if hook != nil {
		if hookErr := hook(ctx, op, d); hookErr != nil {
			return hookErr, false
		}
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr, false
	}
	return err, commitFirst
}

func capabilities(ctx context.Context) (*repb.ServerCapabilities, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return &repb.ServerCapabilities{CacheCapabilities: &repb.CacheCapabilities{
		DigestFunctions:  []repb.DigestFunction_Value{repb.DigestFunction_SHA256},
		SplitBlobSupport: true, SpliceBlobSupport: true,
		RepMaxCdcParams: &repb.RepMaxCdcParams{MinChunkSizeBytes: 256 << 10, HorizonSizeBytes: 1 << 20},
	}}, nil
}

type chunkAccess struct{ store *Store }

func (a chunkAccess) GetCapabilities(ctx context.Context, _ digest.InstanceName) (*repb.ServerCapabilities, error) {
	return capabilities(ctx)
}

func (a chunkAccess) Get(ctx context.Context, d digest.Digest) (*chunk.Chunk, error) {
	if err, _ := a.store.before(ctx, ChunkGet, d); err != nil {
		return nil, err
	}
	a.store.mu.Lock()
	defer a.store.mu.Unlock()
	data, exists := a.store.chunks[d]
	if !exists {
		return nil, status.Error(codes.NotFound, "chunk absent")
	}
	return chunk.NewChunk(bytes.Clone(data)), nil
}

func (a chunkAccess) Put(ctx context.Context, d digest.Digest, value *chunk.Chunk) error {
	err, commitFirst := a.store.before(ctx, ChunkPut, d)
	if err != nil && !commitFirst {
		return err
	}
	a.store.mu.Lock()
	a.store.chunks[d] = bytes.Clone(value.GetBytes())
	a.store.mu.Unlock()
	return err
}

func (a chunkAccess) FindMissing(ctx context.Context, ds digest.Set) (digest.Set, error) {
	if err, _ := a.store.before(ctx, ChunkFindMissing, digest.BadDigest); err != nil {
		return digest.EmptySet, err
	}
	a.store.mu.Lock()
	defer a.store.mu.Unlock()
	missing := digest.NewSetBuilder(ds.Length())
	for _, d := range ds.Items() {
		if _, exists := a.store.chunks[d]; !exists && !a.store.stale {
			missing.Add(d)
		}
	}
	return missing.Build(), nil
}

type mappingAccess struct{ store *Store }

func (a mappingAccess) GetCapabilities(ctx context.Context, _ digest.InstanceName) (*repb.ServerCapabilities, error) {
	return capabilities(ctx)
}

func (a mappingAccess) Get(ctx context.Context, d digest.Digest) (chunk.Mapping, error) {
	if err, _ := a.store.before(ctx, MappingGet, d); err != nil {
		return chunk.Mapping{}, err
	}
	a.store.mu.Lock()
	defer a.store.mu.Unlock()
	value, exists := a.store.mappings[d]
	if !exists {
		return chunk.Mapping{}, status.Error(codes.NotFound, "mapping absent")
	}
	return value, nil
}

func (a mappingAccess) Put(ctx context.Context, d digest.Digest, value chunk.Mapping) error {
	err, commitFirst := a.store.before(ctx, MappingPut, d)
	if err != nil && !commitFirst {
		return err
	}
	a.store.mu.Lock()
	a.store.mappings[d] = value
	a.store.mu.Unlock()
	return err
}

func (a mappingAccess) FindMissing(ctx context.Context, ds digest.Set) (digest.Set, error) {
	if err, _ := a.store.before(ctx, MappingFindMissing, digest.BadDigest); err != nil {
		return digest.EmptySet, err
	}
	a.store.mu.Lock()
	defer a.store.mu.Unlock()
	missing := digest.NewSetBuilder(ds.Length())
	for _, d := range ds.Items() {
		if _, exists := a.store.mappings[d]; !exists && !a.store.stale {
			missing.Add(d)
		}
	}
	return missing.Build(), nil
}
