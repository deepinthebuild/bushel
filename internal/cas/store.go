package cas

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/gofrs/flock"
)

var ErrQuota = errors.New("CAS staging or catalog quota exceeded")

// Store owns private staging and announcement metadata. Buildbarn owns blob
// storage and eviction. Close only after all operations and Readers finish;
// it releases the Bushel lock, not the separately owned Buildbarn backend.
// The limit bounds catalog bytes plus upload staging. Reads have an equal budget.
type Store struct {
	root                  string
	backend               Backend
	lock                  *flock.Flock
	mu                    sync.Mutex
	used, reserved, limit int64
	readReserved          int64
	operations            chan struct{}
	watchers              map[Digest]map[chan struct{}]struct{}
	publications          map[*publicationSubscription]struct{}
	generation            uint64
}

func NewStore(root string, limit int64, backend Backend) (_ *Store, err error) {
	if limit < catalogRecordSize {
		return nil, fmt.Errorf("%w: disk limit too small", ErrInvalid)
	}
	if err := backend.initialize(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(root)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: state directory must not be a link", ErrInvalid)
	}
	s := &Store{root: root, backend: backend, limit: limit, operations: make(chan struct{}, 8), lock: flock.New(filepath.Join(root, "store.lock"))}
	locked, err := s.lock.TryLock()
	if err != nil {
		return nil, err
	}
	if !locked {
		return nil, errors.New("CAS directory already in use")
	}
	defer func() {
		if err != nil {
			_ = s.lock.Unlock()
		}
	}()
	const format = "bushel-state-v1\n"
	marker, markerErr := readBounded(filepath.Join(root, "format"), 32)
	if errors.Is(markerErr, ErrNotFound) {
		if e := walkDirectoryEntries(context.Background(), root, func(entry os.DirEntry) error {
			if entry.Name() != "store.lock" {
				return fmt.Errorf("%w: state directory is not an empty Bushel store", ErrInvalid)
			}
			return nil
		}); e != nil {
			return nil, e
		}
		if e := writeSynced(filepath.Join(root, "format"), []byte(format)); e != nil {
			return nil, e
		}
	} else if markerErr != nil || string(marker) != format {
		return nil, fmt.Errorf("%w: unrecognized store format", ErrInvalid)
	}
	// The process lock makes stale staging directories safe to remove on restart.
	if err = os.RemoveAll(filepath.Join(root, "tmp")); err != nil {
		return nil, err
	}
	for _, dir := range []string{"catalog", "tmp"} {
		if err = os.MkdirAll(filepath.Join(root, dir), 0700); err != nil {
			return nil, err
		}
	}
	for _, dir := range []string{"catalog"} {
		err = walkDirectoryEntries(context.Background(), filepath.Join(root, dir), func(e os.DirEntry) error {
			if !e.Type().IsRegular() {
				return errors.New("non-regular entry in flat CAS directory")
			}
			info, eErr := e.Info()
			if eErr != nil {
				return eErr
			}
			if !info.Mode().IsRegular() {
				return errors.New("non-regular entry in flat CAS directory")
			}
			s.used += info.Size()
			if record, e := readCatalogRecord(filepath.Join(root, dir, e.Name())); e == nil {
				s.generation = max(s.generation, record.generation)
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *Store) Close() error { return s.lock.Unlock() }

func (s *Store) acquire(ctx context.Context) error {
	select {
	case s.operations <- struct{}{}:
		if err := ctx.Err(); err != nil {
			s.release()
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *Store) release() { <-s.operations }

func (s *Store) reserve(n int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n < 0 || n > s.limit-s.used-s.reserved {
		return ErrQuota
	}
	s.reserved += n
	return nil
}
func (s *Store) unreserve(n int64) { s.mu.Lock(); s.reserved -= n; s.mu.Unlock() }

func (s *Store) reserveRead(n int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n > s.limit-s.readReserved {
		return ErrQuota
	}
	s.readReserved += n
	return nil
}
func (s *Store) unreserveRead(n int64) { s.mu.Lock(); s.readReserved -= n; s.mu.Unlock() }

func (s *Store) catalogPath(d Digest) string       { return filepath.Join(s.root, "catalog", d.Key()) }
func stageChunkPath(stage string, d Digest) string { return filepath.Join(stage, d.Key()) }

func readBounded(path string, max int64) ([]byte, error) {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(b)) > max {
		return nil, ErrCorrupt
	}
	return b, nil
}

func (s *Store) Put(ctx context.Context, d Digest, r io.Reader) error { return s.put(ctx, d, r, nil) }

func (s *Store) put(ctx context.Context, d Digest, r io.Reader, expected *Manifest) (retErr error) {
	if err := d.Validate(); err != nil {
		return err
	}
	if err := s.acquire(ctx); err != nil {
		return err
	}
	defer s.release()
	reservation := d.Size + catalogRecordSize
	if err := s.reserve(reservation); err != nil {
		return err
	}
	releaseReservation := true
	defer func() {
		if releaseReservation {
			s.unreserve(reservation)
		}
	}()
	stage, err := os.MkdirTemp(filepath.Join(s.root, "tmp"), "put-")
	if err != nil {
		return err
	}
	defer func() {
		if err := os.RemoveAll(stage); err != nil {
			// Conservatively retain the reservation until restart reconciliation.
			releaseReservation = false
			retErr = errors.Join(retErr, err)
		}
	}()
	m, err := Chunk(ctx, d, r, func(c Digest, b []byte) error { return os.WriteFile(stageChunkPath(stage, c), b, 0600) })
	if err != nil {
		return err
	}
	if expected != nil && !slices.Equal(m.Chunks, expected.Chunks) {
		return fmt.Errorf("%w: noncanonical chunks", ErrCorrupt)
	}
	if err := s.putBackend(ctx, m, stage); err != nil {
		return err
	}
	return s.commitCatalog(ctx, d, filepath.Join(stage, "catalog"))
}

// PublishExisting confirms a complete backend blob and records its availability.
// Retry/status paths use it to recover ambiguous writes that completed in the
// backend before the original caller could publish local announcement metadata.
func (s *Store) PublishExisting(ctx context.Context, d Digest) (present bool, retErr error) {
	if err := s.acquire(ctx); err != nil {
		return false, err
	}
	defer s.release()
	err := s.verifyTo(ctx, d, io.Discard)
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrCorrupt) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if err := s.reserve(catalogRecordSize); err != nil {
		return false, err
	}
	stage, err := os.MkdirTemp(filepath.Join(s.root, "tmp"), "publish-")
	if err != nil {
		s.unreserve(catalogRecordSize)
		return false, err
	}
	defer func() {
		if err := os.RemoveAll(stage); err != nil {
			retErr = errors.Join(retErr, err)
			present = false
		} else {
			s.unreserve(catalogRecordSize)
		}
	}()
	if err := s.commitCatalog(ctx, d, filepath.Join(stage, "catalog")); err != nil {
		return false, err
	}
	return true, nil
}

func (s *Store) commitCatalog(ctx context.Context, d Digest, catalogTemp string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.generation == ^uint64(0) {
		return errors.New("publication generation exhausted")
	}
	s.generation++
	record := catalogRecord{digest: d, generation: s.generation}
	if err := writeSynced(catalogTemp, record.marshal()); err != nil {
		return err
	}
	if err := s.install(catalogTemp, s.catalogPath(d)); err != nil {
		return err
	}
	for ch := range s.watchers[d] {
		close(ch)
	}
	delete(s.watchers, d)
	s.publishLocked(d)
	return nil
}

// WatchCommit subscribes to the next successful publication. Register before
// checking Has to avoid missing a concurrent commit, and always call cancel.
func (s *Store) WatchCommit(d Digest) (<-chan struct{}, func()) {
	ch := make(chan struct{})
	s.mu.Lock()
	if s.watchers == nil {
		s.watchers = make(map[Digest]map[chan struct{}]struct{})
	}
	if s.watchers[d] == nil {
		s.watchers[d] = make(map[chan struct{}]struct{})
	}
	s.watchers[d][ch] = struct{}{}
	s.mu.Unlock()
	return ch, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		delete(s.watchers[d], ch)
		if len(s.watchers[d]) == 0 {
			delete(s.watchers, d)
		}
	}
}

func writeSynced(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(b)
	if err == nil {
		err = f.Sync()
	}
	return errors.Join(err, f.Close())
}

// install is called under mu and accounts only Bushel catalog metadata.
func (s *Store) install(from, to string) error {
	info, err := os.Stat(from)
	if err != nil {
		return err
	}
	var oldSize int64
	if old, e := os.Stat(to); e == nil {
		oldSize = old.Size()
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	}
	if err := os.Rename(from, to); err != nil {
		return err
	}
	s.used += info.Size() - oldSize
	return nil
}

type FetchChunk func(context.Context, Digest) ([]byte, error)

// Import rejects malformed, corrupt, and noncanonical manifests before publishing.
func (s *Store) Import(ctx context.Context, m Manifest, fetch FetchChunk) error {
	if err := m.Validate(); err != nil {
		return err
	}
	r := &chunkSource{ctx: ctx, chunks: m.Chunks, fetch: fetch}
	return s.put(ctx, m.Blob, r, &m)
}

type chunkSource struct {
	ctx    context.Context
	chunks []Digest
	fetch  FetchChunk
	buf    *bytes.Reader
}

func (r *chunkSource) Read(p []byte) (int, error) {
	for r.buf == nil || r.buf.Len() == 0 {
		if len(r.chunks) == 0 {
			return 0, io.EOF
		}
		if err := r.ctx.Err(); err != nil {
			return 0, err
		}
		d := r.chunks[0]
		b, err := r.fetch(r.ctx, d)
		if err != nil {
			return 0, err
		}
		if Sum(b) != d {
			return 0, ErrCorrupt
		}
		r.chunks = r.chunks[1:]
		r.buf = bytes.NewReader(b)
	}
	return r.buf.Read(p)
}

func (s *Store) verifyTo(ctx context.Context, d Digest, w io.Writer) error {
	m, err := s.Manifest(ctx, d)
	if err != nil {
		return err
	}
	r := &chunkSource{ctx: ctx, chunks: m.Chunks, fetch: s.Block}
	actual, err := Chunk(ctx, d, r, func(_ Digest, b []byte) error {
		n, err := w.Write(b)
		if err == nil && n != len(b) {
			return io.ErrShortWrite
		}
		return err
	})
	if err != nil {
		return err
	}
	if !slices.Equal(actual.Chunks, m.Chunks) {
		return fmt.Errorf("%w: noncanonical backend mapping", ErrCorrupt)
	}
	return nil
}

func (s *Store) Has(ctx context.Context, d Digest) (bool, error) {
	if err := s.acquire(ctx); err != nil {
		return false, err
	}
	defer s.release()
	err := s.verifyTo(ctx, d, io.Discard)
	if errors.Is(err, ErrNotFound) || errors.Is(err, ErrCorrupt) {
		return false, nil
	}
	return err == nil, err
}

// Reader contains a completely verified blob. Close releases its disk reservation.
type Reader struct {
	*os.File
	store *Store
	size  int64
	once  sync.Once
	err   error
}

func (r *Reader) Close() error {
	r.once.Do(func() {
		closeErr := r.File.Close()
		removeErr := os.Remove(r.File.Name())
		if removeErr == nil || errors.Is(removeErr, os.ErrNotExist) {
			r.store.unreserveRead(r.size)
		}
		r.err = errors.Join(closeErr, removeErr)
		r.store.release()
	})
	return r.err
}

func (s *Store) Open(ctx context.Context, d Digest) (*Reader, error) {
	if err := d.Validate(); err != nil {
		return nil, err
	}
	if err := s.acquire(ctx); err != nil {
		return nil, err
	}
	if err := s.reserveRead(d.Size); err != nil {
		s.release()
		return nil, err
	}
	f, err := os.CreateTemp(filepath.Join(s.root, "tmp"), "read-")
	if err != nil {
		s.unreserveRead(d.Size)
		s.release()
		return nil, err
	}
	r := &Reader{File: f, store: s, size: d.Size}
	if err = s.verifyTo(ctx, d, f); err == nil {
		_, err = f.Seek(0, io.SeekStart)
	}
	if err != nil {
		_ = r.Close()
		return nil, err
	}
	return r, nil
}
