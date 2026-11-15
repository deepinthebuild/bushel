package cas

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	bbcas "github.com/buildbarn/bb-storage/pkg/cas"
	"github.com/buildbarn/bb-storage/pkg/cas/reader"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Backend is the Buildbarn CS/CMS pair and its optional configured read caches.
// Its owner must keep it alive until the Store and all its readers have stopped.
type Backend struct {
	Chunks        blobstore.BlobAccess[*chunk.Chunk]
	Mappings      blobstore.BlobAccess[chunk.Mapping]
	InstanceName  digest.InstanceName
	ChunkReader   reader.Reader[[]byte]
	MappingReader chunk.MappingFetcher
}

func (b *Backend) initialize() error {
	if b.Chunks == nil || b.Mappings == nil {
		return fmt.Errorf("%w: both Buildbarn CS and CMS are required", ErrInvalid)
	}
	if b.ChunkReader == nil {
		b.ChunkReader = bbcas.NewChunkBytesReader(b.Chunks)
	}
	if b.MappingReader == nil {
		b.MappingReader = blobstore.NewBlobAccessMappingFetcher(b.Mappings)
	}
	return nil
}

func (b Backend) digest(d Digest) (digest.Digest, error) {
	if err := d.Validate(); err != nil {
		return digest.BadDigest, err
	}
	f, err := b.InstanceName.GetDigestFunction(repb.DigestFunction_SHA256, 0)
	if err != nil {
		return digest.BadDigest, err
	}
	return f.NewDigest(d.Hex(), d.Size)
}

func backendError(err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		return errors.Join(ErrNotFound, err)
	case codes.DataLoss:
		return errors.Join(ErrCorrupt, err)
	case codes.Canceled:
		return errors.Join(context.Canceled, err)
	case codes.DeadlineExceeded:
		return errors.Join(context.DeadlineExceeded, err)
	default:
		return err
	}
}

// Manifest resolves through Buildbarn, including blobs populated outside Bushel.
// A mapping is an untrusted hint: Open/Import verify the complete composition.
func (s *Store) Manifest(ctx context.Context, d Digest) (Manifest, error) {
	if err := ctx.Err(); err != nil {
		return Manifest{}, err
	}
	key, err := s.backend.digest(d)
	if err != nil {
		return Manifest{}, err
	}
	m := Manifest{Blob: d}
	if d.Size == 0 {
		return m, nil
	}
	if d.Size <= MaxChunkSize {
		missing, err := s.backend.Chunks.FindMissing(ctx, key.ToSingletonSet())
		if err != nil {
			return Manifest{}, backendError(err)
		}
		if !missing.Empty() {
			return Manifest{}, ErrNotFound
		}
		m.Chunks = []Digest{d}
		return m, nil
	}
	mapping, err := s.backend.MappingReader.FetchChunkMapping(ctx, key)
	if err != nil {
		return Manifest{}, backendError(err)
	}
	return manifestFromMapping(d, mapping, s.backend.InstanceName)
}

func manifestFromMapping(d Digest, mapping chunk.Mapping, instance digest.InstanceName) (Manifest, error) {
	if mapping.Length() < 2 || mapping.Length() > MaxChunks || mapping.GetSizeBytes() != d.Size {
		return Manifest{}, ErrCorrupt
	}
	m := Manifest{Blob: d, Chunks: make([]Digest, mapping.Length())}
	for i := range m.Chunks {
		key := mapping.GetDigestAtIndex(instance, i)
		if key.GetDigestFunction().GetEnumValue() != repb.DigestFunction_SHA256 {
			return Manifest{}, ErrCorrupt
		}
		var err error
		m.Chunks[i], err = ParseDigest(key.GetHashString(), key.GetSizeBytes())
		if err != nil {
			return Manifest{}, ErrCorrupt
		}
	}
	if err := m.Validate(); err != nil {
		return Manifest{}, errors.Join(ErrCorrupt, err)
	}
	return m, nil
}

// Committed is a presence hint, subject to backend eviction and existence caches.
// Bytes must still pass Open or Block before being served.
func (s *Store) Committed(ctx context.Context, d Digest) (bool, error) {
	missing, err := s.FindMissing(ctx, []Digest{d})
	return len(missing) == 0 && err == nil, err
}

const (
	// MaxPresenceBatch bounds a FindMissing request before any backend calls.
	MaxPresenceBatch    = 1024
	presenceConcurrency = 4
	presenceWindow      = 1024
)

type presenceChunk struct {
	key  digest.Digest
	blob Digest
}

// FindMissing returns presence hints in input order, including duplicates.
// It accepts at most MaxPresenceBatch digests. Backend keys are deduplicated in
// bounded windows; at most four untrusted mappings are retained concurrently.
// A presence hint does not authenticate bytes or guarantee they survive eviction.
func (s *Store) FindMissing(ctx context.Context, blobs []Digest) ([]Digest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(blobs) > MaxPresenceBatch {
		return nil, fmt.Errorf("%w: too many presence digests", ErrInvalid)
	}
	seen := make(map[Digest]bool, len(blobs))
	window := make([]presenceChunk, 0, len(blobs))
	var large []Digest
	for _, d := range blobs {
		if seen[d] {
			continue
		}
		key, err := s.backend.digest(d)
		if err != nil {
			return nil, err
		}
		seen[d] = true
		switch {
		case d.Size == 0:
		case d.Size <= MaxChunkSize:
			window = append(window, presenceChunk{key: key, blob: d})
		default:
			large = append(large, d)
		}
	}
	missing := make(map[Digest]bool)
	flush := func() error {
		if len(window) == 0 {
			return ctx.Err()
		}
		if err := s.missingChunks(ctx, window, missing); err != nil {
			return err
		}
		window = window[:0]
		return nil
	}
	for start := 0; start < len(large); start += presenceConcurrency {
		batch := large[start:min(start+presenceConcurrency, len(large))]
		mappings, err := s.presenceMappings(ctx, batch)
		if err != nil {
			return nil, err
		}
		for i, m := range mappings {
			if len(m.Chunks) == 0 {
				missing[batch[i]] = true
				continue
			}
			for _, c := range m.Chunks {
				if len(window) == presenceWindow {
					if err := flush(); err != nil {
						return nil, err
					}
				}
				if missing[batch[i]] {
					break
				}
				key, err := s.backend.digest(c)
				if err != nil {
					return nil, err
				}
				window = append(window, presenceChunk{key: key, blob: batch[i]})
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	var result []Digest
	for _, d := range blobs {
		if missing[d] {
			result = append(result, d)
		}
	}
	return result, nil
}

// presenceMappings fetches one bounded wave. Cancellation joins every worker
// before returning, and a transient failure cancels the other outstanding reads.
func (s *Store) presenceMappings(ctx context.Context, blobs []Digest) ([]Manifest, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	result := make([]Manifest, len(blobs))
	var wg sync.WaitGroup
	var failed sync.Once
	var failure error
	for i, d := range blobs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m, err := s.Manifest(ctx, d)
			if errors.Is(err, ErrNotFound) || errors.Is(err, ErrCorrupt) {
				return
			}
			if err != nil {
				failed.Do(func() { failure = err; cancel() })
				return
			}
			result[i] = m
		}()
	}
	wg.Wait()
	return result, failure
}

func (s *Store) missingChunks(ctx context.Context, window []presenceChunk, missing map[Digest]bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	keys := digest.NewSetBuilder(len(window))
	for _, c := range window {
		keys.Add(c.key)
	}
	absent, err := s.backend.Chunks.FindMissing(ctx, keys.Build())
	err = backendError(err)
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrCorrupt) {
		for _, c := range window {
			missing[c.blob] = true
		}
		return ctx.Err()
	}
	if err != nil {
		return err
	}
	if absent.Empty() {
		return ctx.Err()
	}
	missingKeys := make(map[digest.Digest]bool, len(window))
	for _, c := range window {
		missingKeys[c.key] = false
	}
	for _, key := range absent.Items() {
		if _, requested := missingKeys[key]; requested {
			missingKeys[key] = true
		}
	}
	for _, c := range window {
		if missingKeys[c.key] {
			missing[c.blob] = true
		}
	}
	return ctx.Err()
}

// Block checks both fields on every read, even when a configured cache hits.
// Return an owned copy: Buildbarn chunks/cached byte slices are immutable.
func (s *Store) Block(ctx context.Context, d Digest) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if d.Size <= 0 || d.Size > MaxChunkSize {
		return nil, ErrInvalid
	}
	key, err := s.backend.digest(d)
	if err != nil {
		return nil, err
	}
	b, err := s.backend.ChunkReader.Read(ctx, key)
	if err != nil {
		return nil, backendError(err)
	}
	if int64(len(b)) != d.Size || Sum(b) != d {
		return nil, ErrCorrupt
	}
	return slices.Clone(b), ctx.Err()
}

// putBackend runs only after upload staging passed full digest and CDC checks.
// Verified orphan chunks may survive a later failure. CMS publication is last.
func (s *Store) putBackend(ctx context.Context, m Manifest, stage string) error {
	keys := make([]digest.Digest, 0, len(m.Chunks))
	written := make(map[Digest]struct{}, len(m.Chunks))
	for _, c := range m.Chunks {
		if err := ctx.Err(); err != nil {
			return err
		}
		key, err := s.backend.digest(c)
		if err != nil {
			return err
		}
		keys = append(keys, key)
		if _, ok := written[c]; ok {
			continue
		}
		b, err := readBounded(stageChunkPath(stage, c), MaxChunkSize)
		if err != nil {
			return err
		}
		if Sum(b) != c {
			return ErrCorrupt
		}
		// Never skip writes on an existence-cache hit: this must repair corruption.
		if err := s.backend.Chunks.Put(ctx, key, chunk.NewChunk(b)); err != nil {
			return backendError(err)
		}
		written[c] = struct{}{}
	}
	if len(keys) < 2 {
		return nil
	}
	mapping, err := chunk.NewMappingFromDigests(keys, uint64(m.Blob.Size), true)
	if err != nil {
		return err
	}
	key, err := s.backend.digest(m.Blob)
	if err != nil {
		return err
	}
	return backendError(s.backend.Mappings.Put(ctx, key, mapping))
}
