package cas

import (
	"bytes"
	"context"
	"errors"
	"os"
	"slices"
	"testing"

	bbdigest "github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/deepinthebuild/bushel/internal/teststorage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func catalogProbeFixture(t *testing.T) (*Store, *teststorage.Store, []Digest) {
	t.Helper()
	store, backend := testStoreWithBackend(t)
	for i := range 3 {
		data := fixture(700000)
		data[0] ^= byte(i)
		if err := store.Put(t.Context(), Sum(data), bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
	}
	var order []Digest
	if err := store.WalkCommitted(t.Context(), func(d Digest) error { order = append(order, d); return nil }); err != nil || len(order) != 3 {
		t.Fatalf("catalog fixture: %d entries, error=%v", len(order), err)
	}
	return store, backend, order
}

func TestCatalogProbeFailuresDoNotStarveLaterEntries(t *testing.T) {
	store, backend, order := catalogProbeFixture(t)
	before := make(map[Digest][]byte)
	for _, d := range order {
		record, err := os.ReadFile(store.catalogPath(d))
		if err != nil {
			t.Fatal(err)
		}
		before[d] = record
	}
	failure := status.Error(codes.Unavailable, "first backend partition unavailable")
	second := status.Error(codes.Unavailable, "second backend partition unavailable")
	backend.SetHook(func(_ context.Context, op teststorage.Operation, d bbdigest.Digest) error {
		if op == teststorage.MappingGet {
			switch d {
			case backendDigest(order[0]):
				return failure
			case backendDigest(order[1]):
				return second
			}
		}
		return nil
	})
	for range 2 {
		var got []Digest
		err := store.WalkCommitted(t.Context(), func(d Digest) error { got = append(got, d); return nil })
		var partial *CatalogProbeError
		if !errors.As(err, &partial) || partial.Failed != 2 || !errors.Is(err, failure) {
			t.Fatalf("partial scan result: %v", err)
		}
		if !slices.Equal(got, order[2:]) {
			t.Fatalf("healthy later entry was starved: %v", got)
		}
	}
	for _, d := range order {
		after, err := os.ReadFile(store.catalogPath(d))
		if err != nil || !bytes.Equal(after, before[d]) {
			t.Fatalf("probe changed a retryable catalog record: %v", err)
		}
	}
	backend.SetHook(nil)
	var recovered []Digest
	if err := store.WalkCommitted(t.Context(), func(d Digest) error { recovered = append(recovered, d); return nil }); err != nil || !slices.Equal(recovered, order) {
		t.Fatalf("backend recovery lost entries: %v", err)
	}
}

func TestCatalogProbeFailuresDoNotHideFatalErrors(t *testing.T) {
	for _, mode := range []string{"yield", "parent cancellation"} {
		t.Run(mode, func(t *testing.T) {
			store, backend, order := catalogProbeFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			backend.SetHook(func(_ context.Context, op teststorage.Operation, d bbdigest.Digest) error {
				if op != teststorage.MappingGet {
					return nil
				}
				if d == backendDigest(order[0]) {
					return status.Error(codes.Unavailable, "retryable probe")
				}
				if mode == "parent cancellation" {
					cancel()
					return context.Canceled
				}
				return nil
			})
			want := errors.New("consumer stopped")
			if mode == "parent cancellation" {
				want = context.Canceled
			}
			err := store.WalkCommitted(ctx, func(Digest) error { return want })
			var partial *CatalogProbeError
			if !errors.Is(err, want) || errors.As(err, &partial) {
				t.Fatalf("fatal error became a recoverable probe failure: %v", err)
			}
		})
	}
}
