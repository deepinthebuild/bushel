package cas

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	bbdigest "github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/deepinthebuild/bushel/internal/teststorage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"pgregory.net/rapid"
)

func TestCatalogValidatesMetadataAcrossBatches(t *testing.T) {
	s := testStore(t)
	want := make(map[Digest]bool)
	for i := range 2*catalogBatchSize + 3 {
		data := []byte(fmt.Sprintf("catalog-%d", i))
		d := Sum(data)
		if err := s.Put(context.Background(), d, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		want[d] = true
	}
	wrong := Sum([]byte("wrong identity"))
	other := Sum([]byte("another identity"))
	otherWire := (catalogRecord{digest: other, generation: 1}).marshal()
	for name, data := range map[string][]byte{
		"malformed": otherWire,
		other.Hex() + "-0" + strconv.FormatInt(other.Size, 10): otherWire,
		wrong.Key():                    otherWire,
		Sum(nil).Key():                 []byte("truncated"),
		Sum([]byte("truncated")).Key(): otherWire[:len(otherWire)-1],
	} {
		if err := os.WriteFile(filepath.Join(s.root, "catalog", name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Mkdir(s.catalogPath(Sum([]byte("directory"))), 0700); err != nil {
		t.Fatal(err)
	}
	got := make(map[Digest]bool)
	if err := s.WalkCommitted(context.Background(), func(d Digest) error {
		if got[d] {
			t.Fatal("catalog yielded a duplicate")
		}
		got[d] = true
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != len(want) {
		t.Fatalf("catalog yielded %d entries, want %d", len(got), len(want))
	}
	for d := range want {
		if !got[d] {
			t.Fatal("catalog omitted a valid manifest")
		}
	}
}

func TestCatalogCancellationAndErrors(t *testing.T) {
	s := testStore(t)
	for _, data := range [][]byte{[]byte("a"), []byte("b")} {
		if err := s.Put(context.Background(), Sum(data), bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	calls := 0
	err := s.WalkCommitted(ctx, func(Digest) error {
		calls++
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) || calls != 1 {
		t.Fatalf("cancellation: calls %d, error %v", calls, err)
	}
	want := errors.New("consumer stopped")
	if err := s.WalkCommitted(context.Background(), func(Digest) error { return want }); !errors.Is(err, want) {
		t.Fatalf("callback error lost: %v", err)
	}
	empty := testStore(t)
	if err := os.Remove(filepath.Join(empty.root, "catalog")); err != nil {
		t.Fatal(err)
	}
	if err := empty.WalkCommitted(context.Background(), func(Digest) error { return nil }); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("directory error lost: %v", err)
	}
}

func TestPropertyCatalogNames(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		data := rapid.SliceOfN(rapid.Byte(), 0, 256).Draw(t, "data")
		d := Sum(data)
		if got, err := catalogDigest(d.Key()); err != nil || got != d {
			t.Fatalf("catalog name round trip: %v", err)
		}
		for _, name := range []string{"../" + d.Key(), d.Hex() + "-+" + strconv.FormatInt(d.Size, 10), d.Hex() + "-0" + strconv.FormatInt(d.Size, 10), d.Hex() + "--1", d.Hex() + "-9223372036854775808"} {
			if _, err := catalogDigest(name); err == nil {
				t.Fatalf("accepted noncanonical catalog name %q", name)
			}
		}
		if upper := strings.ToUpper(d.Key()); upper != d.Key() {
			if _, err := catalogDigest(upper); err == nil {
				t.Fatal("accepted uppercase catalog name")
			}
		}
	})
}

func FuzzCatalogDigest(f *testing.F) {
	f.Add(Sum(nil).Key())
	f.Add(Sum([]byte("catalog seed")).Key())
	f.Add("../invalid-9223372036854775808")
	f.Fuzz(func(t *testing.T, name string) {
		d, err := catalogDigest(name)
		if err == nil && (d.Validate() != nil || d.Key() != name) {
			t.Fatal("accepted a noncanonical catalog identity")
		}
	})
}

func TestPropertyCatalogRecordRoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		data := rapid.SliceOfN(rapid.Byte(), 0, 1024).Draw(t, "data")
		r := catalogRecord{digest: Sum(data), generation: rapid.Uint64Range(1, math.MaxUint64).Draw(t, "generation")}
		wire := r.marshal()
		got, err := parseCatalogRecord(wire)
		if err != nil || got != r || len(wire) != 56 || string(wire[:8]) != "BSHLC001" {
			t.Fatalf("catalog record round trip: %v", err)
		}
		badGeneration := bytes.Clone(wire)
		binary.BigEndian.PutUint64(badGeneration[48:56], 0)
		badSize := bytes.Clone(wire)
		binary.BigEndian.PutUint64(badSize[40:48], uint64(MaxBlobSize)+1)
		for _, bad := range [][]byte{wire[:55], append(bytes.Clone(wire), 0), badGeneration, badSize} {
			if _, err := parseCatalogRecord(bad); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("accepted malformed catalog record: %v", err)
			}
		}
	})
}

func FuzzCatalogRecord(f *testing.F) {
	f.Add((catalogRecord{digest: Sum(nil), generation: 1}).marshal())
	f.Add((catalogRecord{digest: Sum([]byte("seed")), generation: math.MaxUint64}).marshal())
	f.Add([]byte("BSHLC001"))
	f.Fuzz(func(t *testing.T, wire []byte) {
		r, err := parseCatalogRecord(wire)
		if err == nil && (r.digest.Validate() != nil || r.generation == 0 || !bytes.Equal(r.marshal(), wire)) {
			t.Fatal("accepted a noncanonical catalog record")
		}
	})
}

func TestCatalogTransientProbeRetainsRecord(t *testing.T) {
	s, backend := testStoreWithBackend(t)
	data := []byte("retry after backend outage")
	d := Sum(data)
	if err := s.Put(context.Background(), d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(s.catalogPath(d))
	if err != nil {
		t.Fatal(err)
	}
	failure := status.Error(codes.Unavailable, "temporary backend outage")
	backend.Fail(teststorage.ChunkFindMissing, 0, failure)
	if err := s.WalkCommitted(context.Background(), func(Digest) error { return nil }); !errors.Is(err, failure) {
		t.Fatalf("transient failure was hidden: %v", err)
	}
	after, err := os.ReadFile(s.catalogPath(d))
	if err != nil || !bytes.Equal(after, before) {
		t.Fatalf("transient error discarded or changed catalog record: %v", err)
	}
	var count int
	if err := s.WalkCommitted(context.Background(), func(got Digest) error {
		if got != d {
			t.Error("unexpected catalog entry")
		}
		count++
		return nil
	}); err != nil || count != 1 {
		t.Fatalf("recovery: count %d, error %v", count, err)
	}
}

func TestCatalogStaleNegativeCannotPruneConcurrentRepair(t *testing.T) {
	s, backend := testStoreWithBackend(t)
	data := fixture(2*MinChunkSize + 1)
	d := Sum(data)
	if err := s.Put(context.Background(), d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	old, err := readCatalogRecord(s.catalogPath(d))
	if err != nil {
		t.Fatal(err)
	}
	backend.EvictMapping(backendDigest(d))
	entered, release := make(chan struct{}), make(chan struct{})
	var intercepted atomic.Bool
	backend.SetHook(func(ctx context.Context, op teststorage.Operation, _ bbdigest.Digest) error {
		if op == teststorage.MappingGet && intercepted.CompareAndSwap(false, true) {
			close(entered)
			select {
			case <-release:
				return status.Error(codes.NotFound, "mapping was missing when probe began")
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	done := make(chan error, 1)
	go func() { done <- s.WalkCommitted(ctx, func(Digest) error { return nil }) }()
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-done
		}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("catalog did not probe the missing mapping")
	}
	if err := s.Put(ctx, d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	close(release)
	err = <-done
	joined = true
	if err != nil {
		t.Fatal(err)
	}
	current, err := readCatalogRecord(s.catalogPath(d))
	if err != nil || current.digest != d || current.generation <= old.generation {
		t.Fatalf("stale probe removed a newer publication: %v", err)
	}
	if s.used != catalogRecordSize {
		t.Fatalf("stale prune damaged accounting: %d", s.used)
	}
	var count int
	if err := s.WalkCommitted(context.Background(), func(Digest) error { count++; return nil }); err != nil || count != 1 {
		t.Fatalf("repaired publication was not discoverable: %v", err)
	}
}

func TestPropertyPublicationOverflowRecoversFromCatalog(t *testing.T) {
	parent := t.TempDir()
	rapid.Check(t, func(t *rapid.T) {
		f := newPropertyStore(t, parent, 1<<20)
		buffer := rapid.IntRange(1, 4).Draw(t, "hint buffer")
		count := rapid.IntRange(1, 8).Draw(t, "publications")
		hints, overflow, cancel := f.store.SubscribeCommits(buffer)
		defer cancel()
		want := make(map[Digest]bool)
		for i := range count {
			data := []byte{byte(i), 1}
			d := Sum(data)
			if err := f.store.Put(context.Background(), d, bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
			want[d] = true
		}
		if len(hints) != min(count, buffer) || len(overflow) != min(max(count-buffer, 0), 1) {
			t.Fatal("publication hint or overflow bounds failed")
		}
		for len(hints) > 0 {
			d := <-hints
			if !want[d] {
				t.Fatal("hint preceded a valid publication")
			}
		}
		for len(overflow) > 0 {
			<-overflow
		}
		bad := Sum([]byte("expected"))
		if err := f.store.Put(context.Background(), bad, bytes.NewReader([]byte("wrong"))); !errors.Is(err, ErrCorrupt) {
			t.Fatal(err)
		}
		if len(hints) != 0 || len(overflow) != 0 {
			t.Fatal("failed publication generated a hint")
		}
		cancel()
		cancel()
		f.reopen(t)
		got := make(map[Digest]bool)
		if err := f.store.WalkCommitted(context.Background(), func(d Digest) error { got[d] = true; return nil }); err != nil {
			t.Fatal(err)
		}
		if len(got) != len(want) {
			t.Fatal("durable catalog lost dropped hints after restart")
		}
		for d := range want {
			if !got[d] {
				t.Fatal("durable catalog omitted publication")
			}
		}
	})
}

func TestPublicationUnsubscribeDuringUploads(t *testing.T) {
	s := testStore(t)
	hints, overflow, cancel := s.SubscribeCommits(1)
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() { cancel(); cancel() })
	}
	wg.Go(func() {
		for i := range 16 {
			data := []byte{byte(i)}
			if err := s.Put(context.Background(), Sum(data), bytes.NewReader(data)); err != nil {
				t.Error(err)
			}
		}
	})
	wg.Wait()
	for range hints {
	}
	for range overflow {
	}
	if len(s.publications) != 0 {
		t.Fatal("canceled subscriber remains registered")
	}
}

func TestStoreReopenAccountsMultipleDirectoryBatches(t *testing.T) {
	root := t.TempDir()
	fake := teststorage.New()
	s, err := NewStore(root, 1<<20, testBackend(fake))
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if s != nil {
			_ = s.Close()
		}
	}()
	var used int64
	for i := range 2*catalogBatchSize + 3 {
		data := []byte(fmt.Sprintf("quota-%03d", i))
		if err := s.Put(context.Background(), Sum(data), bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		used += catalogRecordSize
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewStore(root, used, testBackend(fake))
	if err != nil {
		t.Fatal(err)
	}
	if s.used != used {
		t.Fatalf("startup accounted %d bytes, want %d", s.used, used)
	}
	data := []byte{1}
	if err := s.Put(context.Background(), Sum(data), bytes.NewReader(data)); !errors.Is(err, ErrQuota) {
		t.Fatalf("reopen lost quota accounting: %v", err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = NewStore(root, used+catalogRecordSize+1, testBackend(fake))
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Put(context.Background(), Sum(data), bytes.NewReader(data)); err != nil {
		t.Fatalf("exact remaining quota was unavailable: %v", err)
	}
}

func TestStoreRejectsUnexpectedNestedDirectories(t *testing.T) {
	for _, directory := range []string{"catalog"} {
		t.Run(directory, func(t *testing.T) {
			root := t.TempDir()
			s, err := NewStore(root, 1<<20, testBackend(teststorage.New()))
			if err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			unexpected := filepath.Join(root, directory, "unexpected")
			if err := os.Mkdir(unexpected, 0700); err != nil {
				t.Fatal(err)
			}
			if reopened, err := NewStore(root, 1<<20, testBackend(teststorage.New())); err == nil {
				_ = reopened.Close()
				t.Fatal("accepted directory in a flat CAS layout")
			}
			if err := os.Remove(unexpected); err != nil {
				t.Fatal(err)
			}
			s, err = NewStore(root, 1<<20, testBackend(teststorage.New()))
			if err != nil {
				t.Fatalf("failed accounting leaked a lock or directory handle: %v", err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStoreRejectsSymlinkedReservedDirectories(t *testing.T) {
	for _, directory := range []string{"catalog"} {
		t.Run(directory, func(t *testing.T) {
			root, outside := t.TempDir(), t.TempDir()
			s, err := NewStore(root, 1<<20, testBackend(teststorage.New()))
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = s.Close() }()
			d := Sum([]byte("outside fixture"))
			wire := (catalogRecord{digest: d, generation: 1}).marshal()
			want := map[string][]byte{"keep": []byte("keep"), d.Key(): wire}
			for name, data := range want {
				if err := os.WriteFile(filepath.Join(outside, name), data, 0600); err != nil {
					t.Fatal(err)
				}
			}
			reserved := filepath.Join(root, directory)
			if err := os.Remove(reserved); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, reserved); err != nil {
				// Windows may require developer mode or the symlink privilege.
				if runtime.GOOS == "windows" && (errors.Is(err, os.ErrPermission) || errors.Is(err, syscall.Errno(1314)) || errors.Is(err, syscall.Errno(50))) {
					t.Skipf("symbolic links unavailable: %v", err)
				}
				t.Fatal(err)
			}
			if directory == "catalog" {
				err := s.WalkCommitted(context.Background(), func(Digest) error {
					t.Error("catalog visited a manifest outside the store")
					return nil
				})
				if !errors.Is(err, ErrInvalid) {
					t.Fatalf("catalog followed reserved directory link: %v", err)
				}
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			if reopened, err := NewStore(root, 1<<20, testBackend(teststorage.New())); !errors.Is(err, ErrInvalid) {
				if reopened != nil {
					_ = reopened.Close()
				}
				t.Fatalf("startup followed reserved directory link: %v", err)
			}
			checkOutside := func() {
				entries, err := os.ReadDir(outside)
				if err != nil || len(entries) != len(want) {
					t.Fatalf("outside directory changed: %v", err)
				}
				for name, data := range want {
					got, err := os.ReadFile(filepath.Join(outside, name))
					if err != nil || !bytes.Equal(got, data) {
						t.Fatalf("outside file changed: %v", err)
					}
				}
			}
			checkOutside()
			if err := os.Remove(reserved); err != nil {
				t.Fatal(err)
			}
			if err := os.Mkdir(reserved, 0700); err != nil {
				t.Fatal(err)
			}
			reopened, err := NewStore(root, 1<<20, testBackend(teststorage.New()))
			if err != nil {
				t.Fatalf("rejected link leaked the process lock: %v", err)
			}
			defer func() { _ = reopened.Close() }()
			data := []byte("inside fixture")
			if err := reopened.Put(context.Background(), Sum(data), bytes.NewReader(data)); err != nil {
				t.Fatal(err)
			}
			checkOutside()
		})
	}
}
