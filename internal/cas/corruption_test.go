package cas

import (
	"bytes"
	"context"
	"errors"
	"os"
	"testing"

	bbdigest "github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/deepinthebuild/bushel/internal/teststorage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"pgregory.net/rapid"
)

func TestPropertyCorruptionInvalidatesObservedPublication(t *testing.T) {
	parent := t.TempDir()
	rapid.Check(t, func(t *rapid.T) {
		f := newPropertyStore(t, parent, 1<<20)
		data := []byte("verified publication")
		d := Sum(data)
		marked, healthy := false, false
		for range rapid.IntRange(1, 50).Draw(t, "steps") {
			switch rapid.IntRange(0, 5).Draw(t, "action") {
			case 0: // repair and publish
				if err := f.store.Put(context.Background(), d, bytes.NewReader(data)); err != nil {
					t.Fatal(err)
				}
				marked, healthy = true, true
			case 1: // corrupt local bytes; existence alone cannot discover it
				f.backend.CorruptChunk(backendDigest(d), []byte("corrupt"))
				healthy = false
			case 2: // verify before serving; no corrupt bytes escape
				r, err := f.store.Open(context.Background(), d)
				if healthy {
					if err != nil {
						t.Fatal(err)
					}
					if err := r.Close(); err != nil {
						t.Fatal(err)
					}
				} else {
					if r != nil || (!errors.Is(err, ErrCorrupt) && !errors.Is(err, ErrNotFound)) {
						t.Fatalf("corrupt read: %v", err)
					}
					marked = false
				}
			case 3: // backend failure does not invalidate a marker
				failure := status.Error(codes.Unavailable, "injected")
				f.backend.Fail(teststorage.ChunkFindMissing, 0, failure)
				r, err := f.store.Open(context.Background(), d)
				if r != nil || !errors.Is(err, failure) {
					t.Fatalf("transient read: %v", err)
				}
			case 4:
				f.reopen(t)
			case 5:
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				if _, err := f.store.Open(ctx, d); !errors.Is(err, context.Canceled) {
					t.Fatal(err)
				}
			}
			_, err := os.Stat(f.store.catalogPath(d))
			if (err == nil) != marked {
				t.Fatalf("marker state: marked=%v, stat=%v", marked, err)
			}
		}
	})
}

func TestFailedVerificationDoesNotPruneConcurrentRepair(t *testing.T) {
	s, backend := testStoreWithBackend(t)
	data := []byte("repair while verification is in flight")
	d := Sum(data)
	if err := s.Put(t.Context(), d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	old, err := readCatalogRecord(s.catalogPath(d))
	if err != nil {
		t.Fatal(err)
	}
	backend.SetHook(func(ctx context.Context, op teststorage.Operation, _ bbdigest.Digest) error {
		if op != teststorage.ChunkGet {
			return nil
		}
		backend.SetHook(nil)
		if err := s.Put(ctx, d, bytes.NewReader(data)); err != nil {
			return err
		}
		return status.Error(codes.DataLoss, "old read failed after repair")
	})
	if _, err := s.Open(t.Context(), d); !errors.Is(err, ErrCorrupt) {
		t.Fatal(err)
	}
	current, err := readCatalogRecord(s.catalogPath(d))
	if err != nil || current.generation <= old.generation {
		t.Fatalf("repair pruned: %v, %v", current, err)
	}
}
