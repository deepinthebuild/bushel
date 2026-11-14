package cas

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/gofrs/flock"
)

var ErrQuota = errors.New("CAS disk quota exceeded")

// Store owns a private directory. Close only after all operations and Readers
// have finished. The limit bounds storage plus upload staging. Verified read
// spools have a separate equal budget so a full store can still serve cache hits.
type Store struct {
	root                  string
	lock                  *flock.Flock
	mu                    sync.Mutex
	used, reserved, limit int64
	readReserved          int64
	operations            chan struct{}
	watchers              map[Digest]map[chan struct{}]struct{}
}

func NewStore(root string, limit int64) (_ *Store, err error) {
	if limit < ManifestHeaderSize {
		return nil, fmt.Errorf("%w: disk limit too small", ErrInvalid)
	}
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	s := &Store{root: root, limit: limit, operations: make(chan struct{}, 8), lock: flock.New(filepath.Join(root, "store.lock"))}
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
	// The process lock makes stale staging directories safe to remove on restart.
	if err = os.RemoveAll(filepath.Join(root, "tmp")); err != nil {
		return nil, err
	}
	for _, dir := range []string{"chunks", "manifests", "tmp"} {
		if err = os.MkdirAll(filepath.Join(root, dir), 0700); err != nil {
			return nil, err
		}
	}
	for _, dir := range []string{"chunks", "manifests"} {
		err = filepath.WalkDir(filepath.Join(root, dir), func(path string, e fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if e.IsDir() {
				return nil
			}
			if !e.Type().IsRegular() {
				return errors.New("non-regular file in CAS")
			}
			info, eErr := e.Info()
			if eErr != nil {
				return eErr
			}
			s.used += info.Size()
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

func (s *Store) manifestPath(d Digest) string { return filepath.Join(s.root, "manifests", d.Key()) }
func (s *Store) chunkPath(hash [32]byte) string {
	return filepath.Join(s.root, "chunks", fmt.Sprintf("%x", hash))
}

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

func (s *Store) Manifest(d Digest) (Manifest, error) {
	if err := d.Validate(); err != nil {
		return Manifest{}, err
	}
	if d.Size == 0 {
		return Manifest{Blob: d}, nil
	}
	b, err := readBounded(s.manifestPath(d), MaxManifestSize)
	if err != nil {
		return Manifest{}, err
	}
	m, err := ParseManifest(b)
	if err != nil || m.Blob != d {
		return Manifest{}, ErrCorrupt
	}
	return m, nil
}

// Committed checks the publication marker without reading blob data. It is for
// presence queries; callers must still use Open or Block before serving bytes.
func (s *Store) Committed(d Digest) (bool, error) {
	if err := d.Validate(); err != nil {
		return false, err
	}
	if d.Size == 0 {
		return true, nil
	}
	info, err := os.Stat(s.manifestPath(d))
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return info.Mode().IsRegular() && info.Size() >= ManifestHeaderSize && info.Size() <= MaxManifestSize && (info.Size()-ManifestHeaderSize)%36 == 0, nil
}

// Block validates stored bytes every time they cross the network boundary.
func (s *Store) Block(hash [32]byte) ([]byte, error) {
	b, err := readBounded(s.chunkPath(hash), MaxChunkSize)
	if err != nil {
		return nil, err
	}
	if len(b) == 0 || sha256.Sum256(b) != hash {
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
	maxChunks := max(int64(1), d.Size/MinChunkSize)
	reservation := d.Size + ManifestHeaderSize + 36*maxChunks
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
	m, err := Chunk(ctx, d, r, func(c Digest, b []byte) error { return writeSynced(filepath.Join(stage, c.Hex()), b) })
	if err != nil {
		return err
	}
	if expected != nil && !slices.Equal(m.Chunks, expected.Chunks) {
		return fmt.Errorf("%w: noncanonical chunks", ErrCorrupt)
	}
	b, err := m.MarshalBinary()
	if err != nil {
		return err
	}
	manifestTemp := filepath.Join(stage, "manifest")
	if err := writeSynced(manifestTemp, b); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range m.Chunks {
		if existing, e := s.Block(c.Hash); e == nil && int64(len(existing)) == c.Size {
			continue
		}
		if err := s.install(filepath.Join(stage, c.Hex()), s.chunkPath(c.Hash)); err != nil {
			return err
		}
	}
	// The manifest is the commit marker. A partial install exposes no new blob.
	if err := s.install(manifestTemp, s.manifestPath(d)); err != nil {
		return err
	}
	for ch := range s.watchers[d] {
		close(ch)
	}
	delete(s.watchers, d)
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

// install is called under mu. Already installed chunks survive failed commits
// and count against the quota; they can be reused by a subsequent upload.
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
	m, err := s.Manifest(d)
	if err != nil {
		return err
	}
	h := sha256.New()
	var size int64
	for _, c := range m.Chunks {
		if err := ctx.Err(); err != nil {
			return err
		}
		b, err := s.Block(c.Hash)
		if err != nil {
			return err
		}
		if int64(len(b)) != c.Size {
			return ErrCorrupt
		}
		_, _ = h.Write(b)
		n, err := w.Write(b)
		size += int64(n)
		if err != nil {
			return err
		}
		if n != len(b) {
			return io.ErrShortWrite
		}
	}
	if size != d.Size || [32]byte(h.Sum(nil)) != d.Hash {
		return ErrCorrupt
	}
	return ctx.Err()
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
