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

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bbdigest "github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/deepinthebuild/bushel/internal/teststorage"
)

func testBackend(f *teststorage.Store) Backend {
	return Backend{Chunks: f.Chunks, Mappings: f.Mappings}
}

func backendDigest(d Digest) bbdigest.Digest {
	parsed, err := bbdigest.MustNewFunction("", repb.DigestFunction_SHA256).NewDigest(d.Hex(), d.Size)
	if err != nil {
		panic(err)
	}
	return parsed
}

func testStore(t *testing.T) *Store {
	t.Helper()
	s, _ := testStoreWithBackend(t)
	return s
}

func testStoreWithBackend(t *testing.T) (*Store, *teststorage.Store) {
	t.Helper()
	f := teststorage.New()
	s, err := NewStore(t.TempDir(), 64<<20, testBackend(f))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := s.Close(); err != nil {
			t.Error(err)
		}
	})
	return s, f
}

func TestStoreRoundTripRestartAndLock(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	f := teststorage.New()
	s, err := NewStore(root, 64<<20, testBackend(f))
	if err != nil {
		t.Fatal(err)
	}
	if other, err := NewStore(root, 64<<20, testBackend(f)); err == nil {
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
	s, err = NewStore(root, 64<<20, testBackend(f))
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
	s, backend := testStoreWithBackend(t)
	ctx := context.Background()
	b := fixture(1 << 20)
	d := Sum(b)
	b[0] ^= 1
	if err := s.Put(ctx, d, bytes.NewReader(b)); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if _, err := s.Manifest(context.Background(), d); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if chunks, mappings := backend.Counts(); chunks != 0 || mappings != 0 {
		t.Fatal("unverified upload wrote to the backend")
	}
	for _, dir := range []string{"catalog", "tmp"} {
		entries, err := os.ReadDir(filepath.Join(s.root, dir))
		if err != nil || len(entries) != 0 {
			t.Fatalf("leaked %s", dir)
		}
	}
}

func TestStoreCorruptionAndRepair(t *testing.T) {
	s, backend := testStoreWithBackend(t)
	ctx := context.Background()
	b := fixture(1 << 20)
	d := Sum(b)
	if err := s.Put(ctx, d, bytes.NewReader(b)); err != nil {
		t.Fatal(err)
	}
	m, _ := s.Manifest(context.Background(), d)
	bad := bytes.Clone(b[:m.Chunks[0].Size])
	bad[0] ^= 1
	backend.CorruptChunk(backendDigest(m.Chunks[0]), bad)
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
	m, _ := a.Manifest(context.Background(), d)
	if err := b.Import(ctx, m, func(_ context.Context, c Digest) ([]byte, error) {
		p, err := a.Block(context.Background(), c)
		if len(p) > 0 {
			p[0] ^= 1
		}
		return p, err
	}); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	if _, err := b.Manifest(context.Background(), d); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	if err := b.Import(ctx, m, func(_ context.Context, c Digest) ([]byte, error) { return a.Block(context.Background(), c) }); err != nil {
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
	limited, err := NewStore(t.TempDir(), 128, testBackend(teststorage.New()))
	if err != nil {
		t.Fatal(err)
	}
	defer limited.Close()
	if err := limited.Put(ctx, d, bytes.NewReader(data)); !errors.Is(err, ErrQuota) {
		t.Fatal(err)
	}
}

// Measures Bushel staging and verification with an injected memory backend.
func BenchmarkStoreMemoryBackendRoundTrip(b *testing.B) {
	s, err := NewStore(b.TempDir(), 256<<20, testBackend(teststorage.New()))
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
	s, err := NewStore(t.TempDir(), 1<<20, testBackend(teststorage.New()))
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
	s, err := NewStore(b.TempDir(), 64<<20, testBackend(teststorage.New()))
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
		ok, err := s.Committed(context.Background(), d)
		if err != nil || !ok {
			b.Fatalf("%v %v", ok, err)
		}
	}
}

func TestStoreRefusesUnownedDirectory(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tmp", "keep")
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if s, err := NewStore(dir, 1<<20, testBackend(teststorage.New())); err == nil {
		s.Close()
		t.Fatal("accepted nonempty unowned directory")
	}
	data, err := os.ReadFile(path)
	if err != nil || string(data) != "keep" {
		t.Fatalf("existing files changed: %v", err)
	}
}
