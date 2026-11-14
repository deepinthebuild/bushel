package cas

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func testStore(t *testing.T) *Store {
	t.Helper()
	s, err := NewStore(t.TempDir(), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s
}

func TestStoreRoundTripRestartAndLock(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	s, err := NewStore(root, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := NewStore(root, 64<<20); err == nil {
		other.Close()
		t.Fatal("second writer acquired lock")
	}
	b := fixture(3 << 20)
	d := Sum(b)
	if err := s.Put(ctx, d, bytes.NewReader(b)); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewStore(root, 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	r, err := s.Open(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
	if !bytes.Equal(got, b) {
		t.Fatal("roundtrip mismatch")
	}
	if ok, err := s.Has(ctx, Sum(nil)); err != nil || !ok {
		t.Fatalf("empty: %v %v", ok, err)
	}
}

func TestStoreRejectsCorruptUploadWithoutPublication(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	b := fixture(1 << 20)
	d := Sum(b)
	b[0] ^= 1
	if err := s.Put(ctx, d, bytes.NewReader(b)); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if _, err := s.Manifest(d); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	for _, dir := range []string{"chunks", "tmp"} {
		entries, err := os.ReadDir(filepath.Join(s.root, dir))
		if err != nil || len(entries) != 0 {
			t.Fatalf("leaked %s", dir)
		}
	}
}

func TestStoreCorruptionAndRepair(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	b := fixture(1 << 20)
	d := Sum(b)
	if err := s.Put(ctx, d, bytes.NewReader(b)); err != nil {
		t.Fatal(err)
	}
	m, _ := s.Manifest(d)
	path := s.chunkPath(m.Chunks[0].Hash)
	f, err := os.OpenFile(path, os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, err = f.WriteAt([]byte{b[0] ^ 1}, 0)
	f.Close()
	if err != nil {
		t.Fatal(err)
	}
	if r, err := s.Open(ctx, d); !errors.Is(err, ErrCorrupt) {
		if r != nil {
			r.Close()
		}
		t.Fatal(err)
	}
	if err := s.Put(ctx, d, bytes.NewReader(b)); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.Has(ctx, d); !ok || err != nil {
		t.Fatalf("repair: %v %v", ok, err)
	}
}

func TestStoreImportVerifiesChunksAndCanonicalCuts(t *testing.T) {
	a, b := testStore(t), testStore(t)
	ctx := context.Background()
	data := fixture(2 << 20)
	d := Sum(data)
	if err := a.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	m, _ := a.Manifest(d)
	if err := b.Import(ctx, m, func(_ context.Context, c Digest) ([]byte, error) {
		p, err := a.Block(c.Hash)
		if len(p) > 0 {
			p[0] ^= 1
		}
		return p, err
	}); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if _, err := b.Manifest(d); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := b.Import(ctx, m, func(_ context.Context, c Digest) ([]byte, error) { return a.Block(c.Hash) }); err != nil {
		t.Fatal(err)
	}
	other := fixture(3 * MinChunkSize)
	od := Sum(other)
	noncanonical := Manifest{Blob: od, Chunks: []Digest{Sum(other[:MinChunkSize]), Sum(other[MinChunkSize : 2*MinChunkSize]), Sum(other[2*MinChunkSize:])}}
	parts := map[[32]byte][]byte{}
	for i, c := range noncanonical.Chunks {
		parts[c.Hash] = other[i*MinChunkSize : (i+1)*MinChunkSize]
	}
	if err := b.Import(ctx, noncanonical, func(_ context.Context, c Digest) ([]byte, error) { return parts[c.Hash], nil }); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("canonical check: %v", err)
	}
}

func TestStoreQuotaAndConcurrentPublication(t *testing.T) {
	s := testStore(t)
	ctx := context.Background()
	data := fixture(700000)
	d := Sum(data)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			if err := s.Put(ctx, d, bytes.NewReader(data)); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if ok, err := s.Has(ctx, d); err != nil || !ok {
		t.Fatalf("has %v %v", ok, err)
	}
	limited, err := NewStore(t.TempDir(), 128)
	if err != nil {
		t.Fatal(err)
	}
	defer limited.Close()
	if err := limited.Put(ctx, d, bytes.NewReader(data)); !errors.Is(err, ErrQuota) {
		t.Fatal(err)
	}
}

func BenchmarkStoreDuplicateRoundTrip(b *testing.B) {
	s, err := NewStore(b.TempDir(), 256<<20)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	data := fixture(4 << 20)
	d := Sum(data)
	ctx := context.Background()
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := s.Put(ctx, d, bytes.NewReader(data)); err != nil {
			b.Fatal(err)
		}
		r, err := s.Open(ctx, d)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := io.Copy(io.Discard, r); err != nil {
			b.Fatal(err)
		}
		if err := r.Close(); err != nil {
			b.Fatal(err)
		}
	}
}

func TestOpenReaderAdmissionAndCancellation(t *testing.T) {
	s := testStore(t)
	readers := make([]*Reader, 0, cap(s.operations))
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()
	for range cap(s.operations) {
		r, err := s.Open(context.Background(), Sum(nil))
		if err != nil {
			t.Fatal(err)
		}
		readers = append(readers, r)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := s.Open(ctx, Sum(nil)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("unbounded readers: %v", err)
	}
	if err := readers[0].Close(); err != nil {
		t.Fatal(err)
	}
	r, err := s.Open(context.Background(), Sum(nil))
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
}

func TestFullStoreCanReadAndScratchIsBounded(t *testing.T) {
	s, err := NewStore(t.TempDir(), 1<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	data := fixture(700000)
	d := Sum(data)
	ctx := context.Background()
	if err := s.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	r, err := s.Open(ctx, d)
	if err != nil {
		t.Fatalf("full store cache hit: %v", err)
	}
	if extra, err := s.Open(ctx, d); !errors.Is(err, ErrQuota) {
		if extra != nil {
			extra.Close()
		}
		t.Fatalf("scratch quota: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	r, err = s.Open(ctx, d)
	if err != nil {
		t.Fatal(err)
	}
	r.Close()
}

func BenchmarkCommittedPresence(b *testing.B) {
	s, err := NewStore(b.TempDir(), 64<<20)
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close()
	data := fixture(8 << 20)
	d := Sum(data)
	if err := s.Put(context.Background(), d, bytes.NewReader(data)); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		ok, err := s.Committed(d)
		if err != nil || !ok {
			b.Fatalf("%v %v", ok, err)
		}
	}
}
