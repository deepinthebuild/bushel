package cas

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

const catalogBatchSize = 64
const maxPublicationHints = 4096
const catalogRecordSize = 56
const catalogMagic = "BSHLC001"

type catalogRecord struct {
	digest     Digest
	generation uint64
}

func (r catalogRecord) marshal() []byte {
	b := make([]byte, catalogRecordSize)
	copy(b, catalogMagic)
	copy(b[8:40], r.digest.Hash[:])
	binary.BigEndian.PutUint64(b[40:48], uint64(r.digest.Size))
	binary.BigEndian.PutUint64(b[48:56], r.generation)
	return b
}

func parseCatalogRecord(b []byte) (catalogRecord, error) {
	var r catalogRecord
	if len(b) != catalogRecordSize || !bytes.Equal(b[:8], []byte(catalogMagic)) {
		return r, ErrCorrupt
	}
	copy(r.digest.Hash[:], b[8:40])
	r.digest.Size = int64(binary.BigEndian.Uint64(b[40:48]))
	r.generation = binary.BigEndian.Uint64(b[48:56])
	if err := r.digest.Validate(); err != nil || r.generation == 0 {
		return catalogRecord{}, ErrCorrupt
	}
	return r, nil
}

func readCatalogRecord(path string) (catalogRecord, error) {
	b, err := readBounded(path, catalogRecordSize)
	if err != nil {
		return catalogRecord{}, err
	}
	return parseCatalogRecord(b)
}

// CatalogProbeError reports a completed walk that could not check some backend
// entries. Healthy entries were yielded and failed entries remain retryable.
type CatalogProbeError struct {
	Failed int
	Err    error
}

func (e *CatalogProbeError) Error() string {
	return fmt.Sprintf("backend probes failed for %d catalog entries: %v", e.Failed, e.Err)
}

func (e *CatalogProbeError) Unwrap() error { return e.Err }

// WalkCommitted streams blobs observed by this daemon, probing backend presence
// before yielding. Evicted entries are pruned; transient failures remain retryable.
// Concurrent publications need not appear: subscribe first, then repeat walks.
func (s *Store) WalkCommitted(ctx context.Context, yield func(Digest) error) error {
	var failed CatalogProbeError
	err := walkDirectoryEntries(ctx, filepath.Join(s.root, "catalog"), func(entry os.DirEntry) error {
		d, err := catalogDigest(entry.Name())
		if err != nil || !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() != catalogRecordSize {
			return nil
		}
		record, err := readCatalogRecord(s.catalogPath(d))
		if errors.Is(err, ErrNotFound) || errors.Is(err, ErrCorrupt) {
			return nil
		}
		if err != nil {
			return err
		}
		if record.digest != d {
			return nil
		}
		probeCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		present, err := s.Committed(probeCtx, d)
		cancel()
		if err != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
			failed.Failed++
			if failed.Err == nil {
				failed.Err = err
			}
			return nil
		}
		if !present {
			return s.pruneCatalog(record)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return yield(d)
	})
	if err != nil {
		return err
	}
	if failed.Failed != 0 {
		return &failed
	}
	return nil
}

// A failed probe must not remove a publication repaired while it was in flight.
func (s *Store) pruneCatalog(observed catalogRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	path := s.catalogPath(observed.digest)
	current, err := readCatalogRecord(path)
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if current != observed {
		return nil
	}
	if err := os.Remove(path); err != nil {
		return err
	}
	s.used -= catalogRecordSize
	return nil
}

// invalidateCatalog removes only publications preceding a failed byte read.
// Snapshotting the generation avoids a metadata file read on every healthy read.
func (s *Store) invalidateCatalog(d Digest, generation uint64) error {
	s.mu.Lock()
	observed, err := readCatalogRecord(s.catalogPath(d))
	s.mu.Unlock()
	if errors.Is(err, ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if observed.digest != d || observed.generation > generation {
		return nil
	}
	return s.pruneCatalog(observed)
}

// Both startup accounting and catalog walks retain only one directory batch.
func walkDirectoryEntries(ctx context.Context, path string, visit func(os.DirEntry) error) (retErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("%w: CAS directory must be a directory, not a link", ErrInvalid)
	}
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, dir.Close()) }()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, readErr := dir.ReadDir(catalogBatchSize)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := visit(entry); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func catalogDigest(name string) (Digest, error) {
	if len(name) < 66 || name[64] != '-' {
		return Digest{}, ErrInvalid
	}
	size, err := strconv.ParseInt(name[65:], 10, 64)
	if err != nil {
		return Digest{}, ErrInvalid
	}
	d, err := ParseDigest(name[:64], size)
	if err != nil || d.Key() != name {
		return Digest{}, fmt.Errorf("%w: noncanonical catalog name", ErrInvalid)
	}
	return d, nil
}

type publicationSubscription struct {
	hints    chan Digest
	overflow chan struct{}
}

// SubscribeCommits supplies lossy, bounded hints after backend publication.
// A capacity-one overflow signal requests a later catalog walk. Uploads never
// wait for either channel. cancel is idempotent and closes both channels after
// unregistering them; callers must cancel before closing the Store.
func (s *Store) SubscribeCommits(buffer int) (<-chan Digest, <-chan struct{}, func()) {
	sub := &publicationSubscription{
		hints:    make(chan Digest, min(max(buffer, 1), maxPublicationHints)),
		overflow: make(chan struct{}, 1),
	}
	s.mu.Lock()
	if s.publications == nil {
		s.publications = make(map[*publicationSubscription]struct{})
	}
	s.publications[sub] = struct{}{}
	s.mu.Unlock()
	return sub.hints, sub.overflow, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, exists := s.publications[sub]; exists {
			delete(s.publications, sub)
			close(sub.hints)
			close(sub.overflow)
		}
	}
}

// publishLocked runs after catalog publication, while Store.mu is held.
func (s *Store) publishLocked(d Digest) {
	for sub := range s.publications {
		select {
		case sub.hints <- d:
		default:
			select {
			case sub.overflow <- struct{}{}:
			default:
			}
		}
	}
}
