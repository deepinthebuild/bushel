package cas

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"

	"pgregory.net/rapid"
)

type propertyStoreFixture struct {
	store *Store
	root  string
	limit int64
}

// Each generated case owns an explicit, fresh directory, including during
// shrinking. Cleanup runs after that case, rather than retaining every store.
func newPropertyStore(t *rapid.T, parent string, limit int64) *propertyStoreFixture {
	root, err := os.MkdirTemp(parent, "case-")
	if err != nil {
		t.Fatal(err)
	}
	f := &propertyStoreFixture{root: root, limit: limit}
	t.Cleanup(func() {
		if f.store != nil {
			if err := f.store.Close(); err != nil {
				t.Error(err)
			}
		}
		if err := os.RemoveAll(root); err != nil {
			t.Error(err)
		}
	})
	f.store, err = NewStore(root, limit)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *propertyStoreFixture) reopen(t *rapid.T) {
	if err := f.store.Close(); err != nil {
		t.Fatal(err)
	}
	f.store = nil
	var err error
	f.store, err = NewStore(f.root, f.limit)
	if err != nil {
		t.Fatal(err)
	}
}

type modelBlob struct {
	data      []byte
	digest    Digest
	published bool
	corrupt   bool
}

func checkStoreModel(t *rapid.T, s *Store, blobs []modelBlob) {
	for _, b := range blobs {
		committed, err := s.Committed(b.digest)
		if err != nil || committed != b.published {
			t.Fatalf("publication differs from model: got %v, want %v, err %v", committed, b.published, err)
		}
		available := b.published && !b.corrupt
		has, err := s.Has(context.Background(), b.digest)
		if err != nil || has != available {
			t.Fatalf("verified presence differs from model: got %v, want %v, err %v", has, available, err)
		}
		r, err := s.Open(context.Background(), b.digest)
		if !available {
			want := ErrNotFound
			if b.corrupt {
				want = ErrCorrupt
			}
			if r != nil {
				_ = r.Close()
				t.Fatal("unavailable blob exposed a reader")
			}
			if !errors.Is(err, want) {
				t.Fatalf("unavailable blob: got %v, want %v", err, want)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		data, readErr := io.ReadAll(r)
		closeErr := r.Close()
		if readErr != nil || closeErr != nil || !bytes.Equal(data, b.data) {
			t.Fatalf("read differs from model: read %v, close %v", readErr, closeErr)
		}
	}
}

func TestPropertyStoreOperationSequences(t *testing.T) {
	parent := t.TempDir()
	rapid.Check(t, func(t *rapid.T) {
		f := newPropertyStore(t, parent, 4<<20)
		blobs := []modelBlob{{digest: Sum(nil), published: true}}
		seed := rapid.Uint64().Draw(t, "data seed")
		for i := 1; i < 4; i++ {
			n := rapid.IntRange(1, 2048).Draw(t, "blob size")
			data := append([]byte{byte(i)}, propertyData(n, seed, 0)...)
			blobs = append(blobs, modelBlob{data: data, digest: Sum(data)})
		}
		// The action list itself shrinks, so a failed history reduces to the
		// smallest sequence of puts, faults, reads, and restarts that exposes it.
		actions := rapid.SliceOfN(rapid.IntRange(0, 31), 1, 16).Draw(t, "actions")
		for _, action := range actions {
			b := &blobs[action%len(blobs)]
			s := f.store
			switch action / len(blobs) {
			case 0: // Publication also repairs previously damaged local bytes.
				notified, cancel := s.WatchCommit(b.digest)
				err := s.Put(context.Background(), b.digest, bytes.NewReader(b.data))
				cancel()
				if err != nil {
					t.Fatal(err)
				}
				select {
				case <-notified:
				default:
					t.Fatal("successful publication did not notify its watcher")
				}
				b.published, b.corrupt = true, false
			case 1:
				notified, cancel := s.WatchCommit(b.digest)
				bad := append(bytes.Clone(b.data), 0)
				err := s.Put(context.Background(), b.digest, bytes.NewReader(bad))
				cancel()
				if !errors.Is(err, ErrCorrupt) {
					t.Fatalf("bad put: %v", err)
				}
				select {
				case <-notified:
					t.Fatal("failed publication notified its watcher")
				default:
				}
			case 2: // Reads are checked against the model after every operation.
			case 3:
				f.reopen(t)
			case 4:
				if b.published && b.digest.Size > 0 {
					bad := bytes.Clone(b.data)
					bad[0] ^= 1
					if err := os.WriteFile(s.chunkPath(b.digest.Hash), bad, 0600); err != nil {
						t.Fatal(err)
					}
					if _, err := s.Block(b.digest.Hash); !errors.Is(err, ErrCorrupt) {
						t.Fatalf("corrupted block crossed serving boundary: %v", err)
					}
					b.corrupt = true
				}
			case 5:
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if err := s.Put(ctx, b.digest, bytes.NewReader(b.data)); !errors.Is(err, context.Canceled) {
					t.Fatalf("canceled put: %v", err)
				}
			case 6:
				m := Manifest{Blob: b.digest}
				if b.digest.Size > 0 {
					m.Chunks = []Digest{b.digest}
				}
				if err := s.Import(context.Background(), m, func(context.Context, Digest) ([]byte, error) { return bytes.Clone(b.data), nil }); err != nil {
					t.Fatal(err)
				}
				b.published, b.corrupt = true, false
			case 7:
				if b.digest.Size > 0 {
					m := Manifest{Blob: b.digest, Chunks: []Digest{b.digest}}
					err := s.Import(context.Background(), m, func(context.Context, Digest) ([]byte, error) { return nil, io.ErrUnexpectedEOF })
					if !errors.Is(err, io.ErrUnexpectedEOF) {
						t.Fatalf("failed fetch: %v", err)
					}
				}
			}
			checkStoreModel(t, f.store, blobs)
			entries, err := os.ReadDir(filepath.Join(f.root, "tmp"))
			if err != nil || len(entries) != 0 {
				t.Fatalf("operation leaked staging or read spools: %v", err)
			}
		}
		f.reopen(t)
		checkStoreModel(t, f.store, blobs)
	})
}

func TestPropertyImportRejectsUntrustedContent(t *testing.T) {
	parent := t.TempDir()
	rapid.Check(t, func(t *rapid.T) {
		f := newPropertyStore(t, parent, 8<<20)
		n := rapid.IntRange(2*MinChunkSize+1, 3*MinChunkSize-1).Draw(t, "blob size")
		data := propertyData(n, rapid.Uint64().Draw(t, "data seed"), rapid.IntRange(0, 3).Draw(t, "pattern"))
		parts := make(map[Digest][]byte)
		m, err := Chunk(context.Background(), Sum(data), bytes.NewReader(data), func(d Digest, p []byte) error {
			parts[d] = bytes.Clone(p)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		bad := Manifest{Blob: m.Blob, Chunks: slices.Clone(m.Chunks)}
		badParts := make(map[Digest][]byte)
		for d, p := range parts {
			badParts[d] = p
		}
		switch rapid.IntRange(0, 2).Draw(t, "attack") {
		case 0: // The chunk hash must reject changed network bytes.
			p := bytes.Clone(parts[bad.Chunks[0]])
			p[0] ^= 1
			badParts[bad.Chunks[0]] = p
		case 1: // Self-consistent chunk hashes cannot override the blob hash.
			p := bytes.Clone(parts[bad.Chunks[0]])
			p[0] ^= 1
			bad.Chunks[0] = Sum(p)
			badParts[bad.Chunks[0]] = p
		case 2: // Even the right bytes must have canonical RepMax boundaries.
			cut := MinChunkSize
			if m.Chunks[0].Size == int64(cut) {
				cut++
			}
			bad.Chunks = []Digest{Sum(data[:cut]), Sum(data[cut:])}
			badParts = map[Digest][]byte{bad.Chunks[0]: data[:cut], bad.Chunks[1]: data[cut:]}
		}
		if err := bad.Validate(); err != nil {
			t.Fatalf("attack must pass structural validation: %v", err)
		}
		err = f.store.Import(context.Background(), bad, func(_ context.Context, d Digest) ([]byte, error) { return badParts[d], nil })
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("untrusted import: %v", err)
		}
		if committed, err := f.store.Committed(m.Blob); err != nil || committed {
			t.Fatalf("failed import published: %v", err)
		}
		for _, dir := range []string{"chunks", "manifests", "tmp"} {
			entries, err := os.ReadDir(filepath.Join(f.root, dir))
			if err != nil || len(entries) != 0 {
				t.Fatalf("failed import leaked %s: %v", dir, err)
			}
		}
		if err := f.store.Import(context.Background(), m, func(_ context.Context, d Digest) ([]byte, error) { return parts[d], nil }); err != nil {
			t.Fatalf("valid retry: %v", err)
		}
		checkStoreModel(t, f.store, []modelBlob{{data: data, digest: m.Blob, published: true}})
	})
}

func TestPropertyStoreConcurrentPublication(t *testing.T) {
	parent := t.TempDir()
	rapid.Check(t, func(t *rapid.T) {
		f := newPropertyStore(t, parent, 4<<20)
		data := propertyData(rapid.IntRange(1, 4096).Draw(t, "size"), rapid.Uint64().Draw(t, "seed"), 0)
		d := Sum(data)
		writers := rapid.IntRange(2, 6).Draw(t, "writers")
		notified, cancel := f.store.WatchCommit(d)
		defer cancel()
		var wg sync.WaitGroup
		errorsCh := make(chan error, writers)
		start := make(chan struct{})
		for range writers {
			wg.Go(func() {
				<-start
				errorsCh <- f.store.Put(context.Background(), d, bytes.NewReader(data))
			})
		}
		close(start)
		wg.Wait()
		close(errorsCh)
		for err := range errorsCh {
			if err != nil {
				t.Fatal(err)
			}
		}
		select {
		case <-notified:
		default:
			t.Fatal("concurrent publication lost notification")
		}
		f.reopen(t)
		checkStoreModel(t, f.store, []modelBlob{{data: data, digest: d, published: true}})
	})
}

func TestPropertyStoreQuotaFailureDoesNotPublish(t *testing.T) {
	parent := t.TempDir()
	rapid.Check(t, func(t *rapid.T) {
		size := rapid.IntRange(256, 4096).Draw(t, "blob size")
		limit := rapid.Int64Range(128, int64(size)-1).Draw(t, "quota")
		f := newPropertyStore(t, parent, limit)
		data := propertyData(size, rapid.Uint64().Draw(t, "seed"), 0)
		if err := f.store.Put(context.Background(), Sum(data), bytes.NewReader(data)); !errors.Is(err, ErrQuota) {
			t.Fatalf("quota overflow: %v", err)
		}
		d := Sum(data)
		if committed, err := f.store.Committed(d); err != nil || committed {
			t.Fatalf("quota rejection published a blob: %v", err)
		}
		if _, err := f.store.Manifest(d); !errors.Is(err, ErrNotFound) {
			t.Fatalf("quota rejection installed a manifest: %v", err)
		}
		if has, err := f.store.Has(context.Background(), d); err != nil || has {
			t.Fatalf("quota rejection made bytes available: %v", err)
		}
		// A failed reservation must leave room for a later operation that fits.
		small := []byte{1}
		if err := f.store.Put(context.Background(), Sum(small), bytes.NewReader(small)); err != nil {
			t.Fatalf("quota leaked after rejection: %v", err)
		}
		checkStoreModel(t, f.store, []modelBlob{{data: small, digest: Sum(small), published: true}})
	})
}
