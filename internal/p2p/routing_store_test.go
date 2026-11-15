package p2p

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
	"github.com/libp2p/go-libp2p-kad-dht/records"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/host/peerstore/pstoremem"
	"github.com/multiformats/go-base32"
	mh "github.com/multiformats/go-multihash"
	"pgregory.net/rapid"
)

func testRoutingLimits() routingStoreLimits {
	return routingStoreLimits{MaxRecords: 32768, MaxBytes: 8 << 20, MaxKeyBytes: 256, MaxValueBytes: 16,
		MaxProvidersPerKey: 16, MaxKeysPerProvider: 4096, MaxBatchRecords: 320,
		MaxPendingRecords: 640, MaxPendingBytes: 256 << 10, MaxSnapshots: 4}
}

func testRoutingStore(t testing.TB, limits routingStoreLimits) *routingStore {
	t.Helper()
	s, err := newRoutingStore(limits)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func routingEntries(t testing.TB, s *routingStore, q dsq.Query) []dsq.Entry {
	t.Helper()
	result, err := s.Query(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	entries, err := result.Rest()
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func TestRoutingStoreOwnershipAndSnapshot(t *testing.T) {
	s := testRoutingStore(t, testRoutingLimits())
	ctx := t.Context()
	key := ds.NewKey("/providers/A/one")
	value := []byte{1, 2, 3}
	if err := s.Put(ctx, key, value); err != nil {
		t.Fatal(err)
	}
	value[0] = 99
	got, err := s.Get(ctx, key)
	if err != nil || !bytes.Equal(got, []byte{1, 2, 3}) {
		t.Fatalf("input slice was retained: %v, %v", got, err)
	}
	got[0] = 99
	if size, err := s.GetSize(ctx, key); err != nil || size != 3 {
		t.Fatalf("size: %d, %v", size, err)
	}
	snapshot, err := s.Query(ctx, dsq.Query{Prefix: "/providers/A"})
	if err != nil {
		t.Fatal(err)
	}
	defer snapshot.Close()
	if err := s.Put(ctx, key, []byte{4}); err != nil {
		t.Fatal(err)
	}
	entry, ok := snapshot.NextSync()
	if !ok || entry.Error != nil || !bytes.Equal(entry.Value, []byte{1, 2, 3}) {
		t.Fatalf("snapshot changed after update: %+v", entry)
	}
	entry.Value[0] = 99
	got, err = s.Get(ctx, key)
	if err != nil || !bytes.Equal(got, []byte{4}) {
		t.Fatalf("reader changed datastore: %v, %v", got, err)
	}
	if err := s.Delete(ctx, key); err != nil {
		t.Fatal(err)
	}
	if err := s.Delete(ctx, key); err != nil {
		t.Fatalf("missing delete is not idempotent: %v", err)
	}
	if found, err := s.Has(ctx, key); err != nil || found {
		t.Fatalf("deleted key remains: %v, %v", found, err)
	}
	if _, err := s.Get(ctx, key); !errors.Is(err, ds.ErrNotFound) {
		t.Fatal(err)
	}
	if size, err := s.GetSize(ctx, key); !errors.Is(err, ds.ErrNotFound) || size != -1 {
		t.Fatalf("missing size: %d, %v", size, err)
	}
}

func TestRoutingStoreQuerySemantics(t *testing.T) {
	s := testRoutingStore(t, testRoutingLimits())
	ctx := t.Context()
	for i, key := range []string{"/providers/A/one", "/providers/A/two", "/providers/A/three", "/providers/AB/one", "/providers/B/one"} {
		if err := s.Put(ctx, ds.NewKey(key), []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	for _, prefix := range []string{"/providers/A", "providers/A/", "/providers/A/ignored/.."} {
		entries := routingEntries(t, s, dsq.Query{Prefix: prefix})
		if len(entries) != 3 {
			t.Fatalf("namespace prefix %q returned %d entries", prefix, len(entries))
		}
	}
	for _, prefix := range []string{"/provider", "/providers/missing", "/providers/A/one", "/providers/A/one/child"} {
		if got := routingEntries(t, s, dsq.Query{Prefix: prefix}); len(got) != 0 {
			t.Fatalf("non-parent prefix %q returned %+v", prefix, got)
		}
	}
	for _, prefix := range []string{"", "/", "/providers/"} {
		if got := routingEntries(t, s, dsq.Query{Prefix: prefix}); len(got) != 5 {
			t.Fatalf("full sweep %q returned %d", prefix, len(got))
		}
	}
	query := dsq.Query{Prefix: "/providers/A", KeysOnly: true, ReturnsSizes: true,
		Filters: []dsq.Filter{dsq.FilterValueCompare{Op: dsq.GreaterThanOrEqual, Value: []byte{1}}},
		Orders:  []dsq.Order{dsq.OrderByValueDescending{}}, Offset: 1, Limit: 1}
	got := routingEntries(t, s, query)
	if len(got) != 1 || got[0].Key != "/providers/A/two" || got[0].Value != nil || got[0].Size != 1 {
		t.Fatalf("filter/order/offset/limit/keys-only: %+v", got)
	}
	// Query callbacks may inspect the store; they must not run under its lock.
	query = dsq.Query{Orders: []dsq.Order{dsq.OrderByFunction(func(a, b dsq.Entry) int {
		if _, err := s.Has(ctx, ds.NewKey(a.Key)); err != nil {
			t.Error(err)
		}
		return strings.Compare(a.Key, b.Key)
	})}}
	if got := routingEntries(t, s, query); len(got) != 5 {
		t.Fatal(got)
	}
}

func TestRoutingStoreScopedAndGlobalEviction(t *testing.T) {
	cases := []struct {
		name  string
		limit func(*routingStoreLimits)
		keys  []string
		want  []string
	}{
		{"group", func(l *routingStoreLimits) { l.MaxProvidersPerKey = 2 },
			[]string{"/providers/B/one", "/providers/A/one", "/providers/A/two", "/providers/A/three"},
			[]string{"/providers/B/one", "/providers/A/two", "/providers/A/three"}},
		{"provider", func(l *routingStoreLimits) { l.MaxKeysPerProvider = 2 },
			[]string{"/providers/A/other", "/providers/A/one", "/providers/B/one", "/providers/C/one"},
			[]string{"/providers/A/other", "/providers/B/one", "/providers/C/one"}},
		{"count", func(l *routingStoreLimits) { l.MaxRecords = 2 },
			[]string{"/providers/A/one", "/providers/B/two", "/providers/C/three"},
			[]string{"/providers/B/two", "/providers/C/three"}},
		{"bytes", func(l *routingStoreLimits) { l.MaxKeyBytes = 20; l.MaxValueBytes = 4; l.MaxBytes = 36 },
			[]string{"/providers/A/one", "/providers/B/two", "/providers/C/three"},
			[]string{"/providers/B/two", "/providers/C/three"}},
		{"refresh", func(l *routingStoreLimits) { l.MaxRecords = 2 },
			[]string{"/providers/A/one", "/providers/B/two", "/providers/A/one", "/providers/C/three"},
			[]string{"/providers/A/one", "/providers/C/three"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			limits := testRoutingLimits()
			tc.limit(&limits)
			s := testRoutingStore(t, limits)
			for _, key := range tc.keys {
				if err := s.Put(t.Context(), ds.NewKey(key), []byte{1}); err != nil {
					t.Fatal(err)
				}
			}
			entries := routingEntries(t, s, dsq.Query{})
			var got []string
			for _, e := range entries {
				got = append(got, e.Key)
			}
			if !slices.Equal(got, tc.want) {
				t.Fatalf("retained %v, want %v", got, tc.want)
			}
		})
	}
}

func TestRoutingStoreRejectsInvalidAndOversizedRecords(t *testing.T) {
	s := testRoutingStore(t, testRoutingLimits())
	for _, key := range []string{"/ipns/key", "/providers", "/providers/A", "/providers//peer", "/providers/A/peer/extra", "/providers/../peer"} {
		if err := s.Put(t.Context(), ds.RawKey(key), []byte{1}); !errors.Is(err, errRoutingInvalid) {
			t.Fatalf("accepted malformed provider key %q: %v", key, err)
		}
	}
	if err := s.Put(t.Context(), ds.NewKey("/providers/A/"+strings.Repeat("P", 256)), nil); !errors.Is(err, errRoutingLimit) {
		t.Fatal(err)
	}
	if err := s.Put(t.Context(), ds.NewKey("/providers/A/peer"), make([]byte, 17)); !errors.Is(err, errRoutingLimit) {
		t.Fatal(err)
	}
	for _, q := range []dsq.Query{{Limit: -1}, {Offset: -1}, {Prefix: strings.Repeat("x", 257)}} {
		if _, err := s.Query(t.Context(), q); !errors.Is(err, errRoutingInvalid) {
			t.Fatal(err)
		}
	}
	if len(s.records) != 0 || s.bytes != 0 {
		t.Fatal("rejected records changed the datastore")
	}
}

func TestRoutingStoreRejectsInvalidLimits(t *testing.T) {
	for _, field := range []string{"MaxRecords", "MaxBytes", "MaxKeyBytes", "MaxValueBytes", "MaxProvidersPerKey", "MaxKeysPerProvider",
		"MaxBatchRecords", "MaxPendingRecords", "MaxPendingBytes", "MaxSnapshots"} {
		limits := testRoutingLimits()
		reflect.ValueOf(&limits).Elem().FieldByName(field).SetInt(0)
		if _, err := newRoutingStore(limits); !errors.Is(err, errRoutingInvalid) {
			t.Fatalf("accepted zero %s: %v", field, err)
		}
	}
	for _, mutate := range []func(*routingStoreLimits){
		func(l *routingStoreLimits) { l.MaxPendingRecords = l.MaxBatchRecords - 1 },
		func(l *routingStoreLimits) { l.MaxBytes = l.MaxKeyBytes + l.MaxValueBytes - 1 },
	} {
		limits := testRoutingLimits()
		mutate(&limits)
		if _, err := newRoutingStore(limits); !errors.Is(err, errRoutingInvalid) {
			t.Fatalf("accepted inconsistent limits: %v", err)
		}
	}
}

func TestRoutingStoreBatchBoundsAndAbort(t *testing.T) {
	limits := testRoutingLimits()
	limits.MaxBatchRecords, limits.MaxPendingRecords = 2, 3
	s := testRoutingStore(t, limits)
	ctx := t.Context()
	a, err := s.Batch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	b, err := s.Batch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	key := ds.NewKey("/providers/A/one")
	value := []byte{1}
	if err := a.Put(ctx, key, value); err != nil {
		t.Fatal(err)
	}
	value[0] = 9
	if err := a.Put(ctx, ds.NewKey("/providers/A/two"), []byte{2}); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(ctx, ds.NewKey("/providers/B/one"), []byte{3}); err != nil {
		t.Fatal(err)
	}
	if has, _ := s.Has(ctx, key); has {
		t.Fatal("uncommitted batch was visible")
	}
	if err := b.Delete(ctx, key); !errors.Is(err, errRoutingLimit) {
		t.Fatalf("aggregate staging cap: %v", err)
	}
	if s.pendingCount != 2 || len(s.batches) != 1 {
		t.Fatal("failed batch did not release its reservations")
	}
	if err := b.Commit(ctx); !errors.Is(err, errRoutingClosed) {
		t.Fatal(err)
	}
	if err := a.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	got, err := s.Get(ctx, key)
	if err != nil || !bytes.Equal(got, []byte{1}) {
		t.Fatalf("batch input was retained: %v, %v", got, err)
	}
	if s.pendingCount != 0 || s.pendingBytes != 0 || len(s.batches) != 0 {
		t.Fatal("commit retained staging memory")
	}
	for _, fault := range []string{"count", "value", "key", "cancel"} {
		t.Run(fault, func(t *testing.T) {
			batch, err := s.Batch(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := batch.Delete(ctx, key); err != nil {
				t.Fatal(err)
			}
			switch fault {
			case "count":
				if err := batch.Delete(ctx, key); err != nil {
					t.Fatal(err)
				}
				err = batch.Delete(ctx, key)
			case "value":
				err = batch.Put(ctx, key, make([]byte, 17))
			case "key":
				err = batch.Put(ctx, ds.NewKey("/outside/providers"), nil)
			case "cancel":
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				err = batch.Commit(cancelled)
			}
			if err == nil {
				t.Fatal("invalid batch succeeded")
			}
			if _, err := s.Get(ctx, key); err != nil {
				t.Fatalf("failed batch partially applied deletes: %v", err)
			}
			if s.pendingCount != 0 || s.pendingBytes != 0 {
				t.Fatal("aborted batch retained staging memory")
			}
		})
	}
}

func TestRoutingStorePendingBytesAndHandles(t *testing.T) {
	limits := testRoutingLimits()
	limits.MaxPendingBytes = len("/providers/A/one") + 1
	limits.MaxBatchRecords, limits.MaxPendingRecords = 2, 2
	s := testRoutingStore(t, limits)
	ctx := t.Context()
	a, _ := s.Batch(ctx)
	b, _ := s.Batch(ctx)
	for range limits.MaxPendingRecords + 1 {
		empty, err := s.Batch(ctx)
		if err != nil {
			t.Fatalf("empty handles consumed admission: %v", err)
		}
		if empty.(*routingBatch).stop != nil || len(s.batches) != 0 {
			t.Fatal("empty batch retained by store or context")
		}
	}
	if err := a.Put(ctx, ds.NewKey("/providers/A/one"), []byte{1}); err != nil {
		t.Fatal(err)
	}
	if err := b.Put(ctx, ds.NewKey("/providers/B/one"), nil); !errors.Is(err, errRoutingLimit) {
		t.Fatalf("aggregate staging bytes are not bounded: %v", err)
	}
	if err := a.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if s.pendingBytes != 0 || s.pendingCount != 0 || len(s.batches) != 0 {
		t.Fatal("pending reservations leaked")
	}
}

func TestRoutingStoreEmptyBatchLifecycle(t *testing.T) {
	s := testRoutingStore(t, testRoutingLimits())
	ctx := t.Context()
	key := ds.NewKey("/providers/A/one")
	committed, _ := s.Batch(ctx)
	if err := committed.Commit(ctx); err != nil {
		t.Fatalf("empty commit failed: %v", err)
	}
	if err := committed.Put(ctx, key, []byte{1}); !errors.Is(err, errRoutingClosed) {
		t.Fatalf("committed empty batch remains usable: %v", err)
	}
	cancelledCtx, cancel := context.WithCancel(ctx)
	cancelled, _ := s.Batch(cancelledCtx)
	cancel()
	if err := cancelled.Commit(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("empty batch lost creation-context cancellation: %v", err)
	}
	closed, _ := s.Batch(ctx)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := closed.Commit(ctx); !errors.Is(err, errRoutingClosed) {
		t.Fatalf("empty batch committed after datastore close: %v", err)
	}
	if len(s.batches) != 0 || s.pendingCount != 0 || s.pendingBytes != 0 {
		t.Fatal("empty batch lifecycle retained reservations")
	}
}

func TestRoutingStoreCancellationAndCloseReleaseResources(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := testRoutingStore(t, testRoutingLimits())
		ctx, cancel := context.WithCancel(context.Background())
		batch, _ := s.Batch(ctx)
		key := ds.NewKey("/providers/A/one")
		if err := batch.Put(ctx, key, []byte{1}); err != nil {
			t.Fatal(err)
		}
		query, err := s.Query(ctx, dsq.Query{})
		if err != nil {
			t.Fatal(err)
		}
		cancel()
		synctest.Wait()
		if s.pendingCount != 0 || s.pendingBytes != 0 || len(s.batches) != 0 || len(s.snapshots) != 0 {
			t.Fatal("cancellation retained batch or snapshot memory")
		}
		if err := batch.Commit(context.Background()); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		_ = query.Close()
		if _, err := s.Get(ctx, key); !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
		ctx = context.Background()
		batch, _ = s.Batch(ctx)
		if err := batch.Put(ctx, key, []byte{1}); err != nil {
			t.Fatal(err)
		}
		query, err = s.Query(ctx, dsq.Query{})
		if err != nil {
			t.Fatal(err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if s.pendingCount != 0 || s.pendingBytes != 0 || len(s.batches) != 0 || len(s.snapshots) != 0 {
			t.Fatal("close retained batch or snapshot memory")
		}
		if _, err := s.Query(ctx, dsq.Query{}); !errors.Is(err, errRoutingClosed) {
			t.Fatal(err)
		}
		if err := batch.Commit(ctx); !errors.Is(err, errRoutingClosed) {
			t.Fatal(err)
		}
		if _, ok := query.NextSync(); ok {
			t.Fatal("closed store exposed a live snapshot")
		}
		if err := s.Sync(ctx, ds.NewKey("/")); !errors.Is(err, errRoutingClosed) {
			t.Fatal(err)
		}
	})
}

func TestRoutingStoreSnapshotLimit(t *testing.T) {
	limits := testRoutingLimits()
	limits.MaxSnapshots = 2
	s := testRoutingStore(t, limits)
	a, _ := s.Query(t.Context(), dsq.Query{})
	b, _ := s.Query(t.Context(), dsq.Query{})
	if _, err := s.Query(t.Context(), dsq.Query{}); !errors.Is(err, errRoutingLimit) {
		t.Fatalf("snapshot admission was not bounded: %v", err)
	}
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	if _, ok := b.NextSync(); ok {
		t.Fatal("empty store returned a record")
	}
	if len(s.snapshots) != 0 {
		t.Fatal("closed/drained snapshots leaked admission")
	}
	q, err := s.Query(t.Context(), dsq.Query{})
	if err != nil {
		t.Fatal(err)
	}
	_ = q.Close()
}

func TestRoutingStoreAtomicConcurrentBatches(t *testing.T) {
	s := testRoutingStore(t, testRoutingLimits())
	ctx := t.Context()
	write := func(value byte) error {
		b, err := s.Batch(ctx)
		if err != nil {
			return err
		}
		for _, key := range []string{"/providers/A/one", "/providers/A/two"} {
			if err := b.Put(ctx, ds.NewKey(key), []byte{value}); err != nil {
				return err
			}
		}
		return b.Commit(ctx)
	}
	if err := write(0); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	wg.Go(func() {
		for i := range 1000 {
			if err := write(byte(i)); err != nil {
				t.Error(err)
				return
			}
		}
	})
	defer wg.Wait()
	for range 1000 {
		entries := routingEntries(t, s, dsq.Query{Prefix: "/providers/A"})
		if len(entries) != 2 || !bytes.Equal(entries[0].Value, entries[1].Value) {
			t.Fatalf("query observed a partial batch: %+v", entries)
		}
	}
	wg.Wait()
}

type routingModelRecord struct {
	key, group, provider string
	value                []byte
}

// The model uses a small ordered slice and direct counting, independently of
// the implementation's maps and three linked-list indexes.
func modelRoutingPut(model []routingModelRecord, key string, value []byte, limits routingStoreLimits) []routingModelRecord {
	i := strings.LastIndexByte(key, '/')
	group, provider := key[:i], key[i+1:]
	model = slices.DeleteFunc(model, func(r routingModelRecord) bool { return r.key == key })
	for _, bound := range []struct {
		limit int
		match func(routingModelRecord) bool
	}{
		{limits.MaxProvidersPerKey, func(r routingModelRecord) bool { return r.group == group }},
		{limits.MaxKeysPerProvider, func(r routingModelRecord) bool { return r.provider == provider }},
	} {
		count, first := 0, -1
		for j, record := range model {
			if bound.match(record) {
				count++
				if first == -1 {
					first = j
				}
			}
		}
		if count >= bound.limit {
			model = slices.Delete(model, first, first+1)
		}
	}
	for {
		size := len(key) + len(value)
		for _, record := range model {
			size += len(record.key) + len(record.value)
		}
		if len(model) < limits.MaxRecords && size <= limits.MaxBytes {
			break
		}
		model = model[1:]
	}
	return append(model, routingModelRecord{key: key, group: group, provider: provider, value: bytes.Clone(value)})
}

func checkRoutingModel(t *rapid.T, s *routingStore, model []routingModelRecord) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.records) != len(model) || s.order.Len() != len(model) || len(s.records) > s.limits.MaxRecords ||
		s.pendingCount < 0 || s.pendingCount > s.limits.MaxPendingRecords || s.pendingBytes < 0 ||
		s.pendingBytes > s.limits.MaxPendingBytes || len(s.snapshots) > s.limits.MaxSnapshots {
		t.Fatalf("record/admission bounds or count differ from model: %d vs %d", len(s.records), len(model))
	}
	total := 0
	for _, want := range model {
		got := s.records[want.key]
		if got == nil || !bytes.Equal(got.value, want.value) {
			t.Fatalf("record %s differs: %+v, want %+v", want.key, got, want)
		}
		total += len(want.key) + len(want.value)
	}
	if total != s.bytes || total > s.limits.MaxBytes {
		t.Fatalf("payload accounting %d, want %d, cap %d", s.bytes, total, s.limits.MaxBytes)
	}
	groupCount, providerCount := 0, 0
	for group, entries := range s.groups {
		if entries.Len() == 0 || entries.Len() > s.limits.MaxProvidersPerKey {
			t.Fatal("group index violates cap")
		}
		for e := entries.Front(); e != nil; e = e.Next() {
			r := e.Value.(*routingRecord)
			if r.group != group || r.byGroup != e || s.records[r.key] != r {
				t.Fatal("stale group index")
			}
			groupCount++
		}
	}
	for provider, entries := range s.providers {
		if entries.Len() == 0 || entries.Len() > s.limits.MaxKeysPerProvider {
			t.Fatal("provider index violates cap")
		}
		for e := entries.Front(); e != nil; e = e.Next() {
			r := e.Value.(*routingRecord)
			if r.provider != provider || r.byProvider != e || s.records[r.key] != r {
				t.Fatal("stale provider index")
			}
			providerCount++
		}
	}
	if groupCount != len(model) || providerCount != len(model) {
		t.Fatal("index cardinality differs from model")
	}
}

func TestPropertyRoutingStoreHistories(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		limits := testRoutingLimits()
		limits.MaxRecords = rapid.IntRange(1, 8).Draw(t, "record cap")
		limits.MaxKeyBytes, limits.MaxValueBytes = 32, 4
		limits.MaxBytes = rapid.IntRange(36, 160).Draw(t, "byte cap")
		limits.MaxProvidersPerKey = rapid.IntRange(1, 4).Draw(t, "group cap")
		limits.MaxKeysPerProvider = rapid.IntRange(1, 4).Draw(t, "provider cap")
		s, err := newRoutingStore(limits)
		if err != nil {
			t.Fatal(err)
		}
		defer s.Close()
		ctx := context.Background()
		var model []routingModelRecord
		// Both the history length and every action shrink to a minimal failing
		// sequence; small generated limits force collisions and evictions.
		actions := rapid.SliceOfN(rapid.IntRange(0, 2047), 1, 100).Draw(t, "actions")
		for _, action := range actions {
			key := fmt.Sprintf("/providers/G%d/P%d", (action/8)%6, (action/48)%6)
			value := bytes.Repeat([]byte{byte(action)}, (action/288)%5)
			switch action % 8 {
			case 0, 1:
				err = s.Put(ctx, ds.NewKey(key), value)
				model = modelRoutingPut(model, key, value, limits)
			case 2:
				err = s.Delete(ctx, ds.NewKey(key))
				model = slices.DeleteFunc(model, func(r routingModelRecord) bool { return r.key == key })
			case 3:
				batch, batchErr := s.Batch(ctx)
				if batchErr != nil {
					t.Fatal(batchErr)
				}
				other := fmt.Sprintf("/providers/G%d/P%d", (action/8+1)%6, (action/48+1)%6)
				if err = batch.Put(ctx, ds.NewKey(key), value); err != nil {
					t.Fatal(err)
				}
				if err = batch.Delete(ctx, ds.NewKey(other)); err != nil {
					t.Fatal(err)
				}
				checkRoutingModel(t, s, model)
				err = batch.Commit(ctx)
				model = modelRoutingPut(model, key, value, limits)
				model = slices.DeleteFunc(model, func(r routingModelRecord) bool { return r.key == other })
			case 4:
				batch, batchErr := s.Batch(ctx)
				if batchErr != nil {
					t.Fatal(batchErr)
				}
				if err := batch.Delete(ctx, ds.NewKey(key)); err != nil {
					t.Fatal(err)
				}
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				if err := batch.Commit(cancelled); !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled commit: %v", err)
				}
			case 5:
				batch, batchErr := s.Batch(ctx)
				if batchErr != nil {
					t.Fatal(batchErr)
				}
				if err := batch.Delete(ctx, ds.NewKey(key)); err != nil {
					t.Fatal(err)
				}
				if err := batch.Put(ctx, ds.NewKey(key), make([]byte, 5)); !errors.Is(err, errRoutingLimit) {
					t.Fatalf("oversized batch write: %v", err)
				}
			case 6:
				prefix := fmt.Sprintf("/providers/G%d", (action/8)%6)
				result, queryErr := s.Query(ctx, dsq.Query{Prefix: prefix})
				if queryErr != nil {
					t.Fatal(queryErr)
				}
				entries, queryErr := result.Rest()
				_ = result.Close()
				if queryErr != nil {
					t.Fatal(queryErr)
				}
				var want []dsq.Entry
				for _, record := range model {
					if record.group == prefix {
						want = append(want, dsq.Entry{Key: record.key, Value: record.value, Size: len(record.value)})
					}
				}
				if !reflect.DeepEqual(entries, want) {
					t.Fatalf("prefix result differs from model: %+v vs %+v", entries, want)
				}
			case 7:
				got, getErr := s.Get(ctx, ds.NewKey(key))
				found := false
				for _, record := range model {
					if record.key == key {
						found = true
						if getErr != nil || !bytes.Equal(got, record.value) {
							t.Fatalf("get differs from model: %v vs %v (%v)", got, record.value, getErr)
						}
					}
				}
				if !found && !errors.Is(getErr, ds.ErrNotFound) {
					t.Fatalf("missing record: %v", getErr)
				}
				clear(got)
			}
			if err != nil {
				t.Fatal(err)
			}
			checkRoutingModel(t, s, model)
		}
	})
}

func providerFixtureID(t testing.TB, value string) peer.ID {
	t.Helper()
	hash, err := mh.Sum([]byte(value), mh.IDENTITY, -1)
	if err != nil {
		t.Fatal(err)
	}
	return peer.ID(hash)
}

func TestRoutingStoreProviderManagerFlushAndExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := testRoutingStore(t, testRoutingLimits())
		ps, err := pstoremem.NewPeerstore()
		if err != nil {
			t.Fatal(err)
		}
		defer ps.Close()
		manager, err := records.NewProviderManager(providerFixtureID(t, "self"), ps, s,
			records.Cache(noProviderCache{}), records.ProvideValidity(2*time.Second), records.CleanupInterval(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		defer manager.Close()
		key := []byte("same content key")
		for i := range 300 {
			id := providerFixtureID(t, fmt.Sprintf("provider-%d", i))
			if err := manager.AddProvider(t.Context(), key, peer.AddrInfo{ID: id}); err != nil {
				t.Fatal(err)
			}
		}
		providers, err := manager.GetProviders(t.Context(), key)
		if err != nil || len(providers) != 16+44 {
			t.Fatalf("persisted cap plus manager pending overlay: %d, %v", len(providers), err)
		}
		if got := routingEntries(t, s, dsq.Query{Prefix: records.ProvidersKeyPrefix}); len(got) != 16 {
			t.Fatalf("provider flush ignored group cap: %d", len(got))
		}
		time.Sleep(3 * time.Second)
		synctest.Wait()
		if got := routingEntries(t, s, dsq.Query{Prefix: records.ProvidersKeyPrefix}); len(got) != 0 {
			t.Fatalf("provider GC failed to remove expired records: %d", len(got))
		}
		providers, err = manager.GetProviders(t.Context(), key)
		if err != nil || len(providers) != 0 {
			t.Fatalf("manager served expired pending records: %d, %v", len(providers), err)
		}
		if err := manager.Close(); err != nil {
			t.Fatal(err)
		}
		// Close flushes the bounded pending buffer, including expired values.
		// A fresh manager must query and delete them without serving them.
		manager, err = records.NewProviderManager(providerFixtureID(t, "self"), ps, s,
			records.Cache(noProviderCache{}), records.ProvideValidity(2*time.Second), records.CleanupInterval(0))
		if err != nil {
			t.Fatal(err)
		}
		defer manager.Close()
		providers, err = manager.GetProviders(t.Context(), key)
		if err != nil || len(providers) != 0 {
			t.Fatalf("reload served expired records: %d, %v", len(providers), err)
		}
		if got := routingEntries(t, s, dsq.Query{}); len(got) != 0 {
			t.Fatal("expired reload did not remove stale datastore records")
		}
	})
}

func TestRoutingStoreProviderManagerTimestampEncoding(t *testing.T) {
	s := testRoutingStore(t, testRoutingLimits())
	ps, err := pstoremem.NewPeerstore()
	if err != nil {
		t.Fatal(err)
	}
	defer ps.Close()
	manager, err := records.NewProviderManager(providerFixtureID(t, "self"), ps, s, records.Cache(noProviderCache{}), records.CleanupInterval(0))
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	key, provider := []byte("content"), providerFixtureID(t, "remote")
	dsk := ds.NewKey(records.ProvidersKeyPrefix + base32.RawStdEncoding.EncodeToString(key) + "/" + base32.RawStdEncoding.EncodeToString([]byte(provider)))
	var value [binary.MaxVarintLen64]byte
	n := binary.PutVarint(value[:], time.Now().UnixNano())
	if err := s.Put(t.Context(), dsk, value[:n]); err != nil {
		t.Fatal(err)
	}
	got, err := manager.GetProviders(t.Context(), key)
	if err != nil || len(got) != 1 || got[0].ID != provider {
		t.Fatalf("manager cannot read provider record: %+v, %v", got, err)
	}
}

func TestRoutingStoreProviderManagerIdleGCKeepsWriteCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		limits := testRoutingLimits()
		s := testRoutingStore(t, limits)
		ps, err := pstoremem.NewPeerstore()
		if err != nil {
			t.Fatal(err)
		}
		defer ps.Close()
		manager, err := records.NewProviderManager(providerFixtureID(t, "self"), ps, s,
			records.Cache(noProviderCache{}), records.CleanupInterval(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		defer manager.Close()
		synctest.Wait()
		// Every idle sweep abandons an empty batch with the manager's lifetime
		// context. Advancing each tick separately guarantees each sweep runs.
		for range limits.MaxPendingRecords + 1 {
			time.Sleep(time.Second)
			synctest.Wait()
		}
		provider := providerFixtureID(t, "remote")
		for i := range 256 {
			key := []byte(fmt.Sprintf("fresh-content-%d", i))
			if err := manager.AddProvider(t.Context(), key, peer.AddrInfo{ID: provider}); err != nil {
				t.Fatal(err)
			}
		}
		if got := routingEntries(t, s, dsq.Query{Prefix: records.ProvidersKeyPrefix}); len(got) != 256 {
			t.Fatalf("idle GC exhausted fresh-flush capacity: stored %d records, want 256", len(got))
		}
		if len(s.batches) != 0 || s.pendingCount != 0 || s.pendingBytes != 0 {
			t.Fatal("idle GC or completed flush retained batch reservations")
		}
	})
}

func TestNoProviderCache(t *testing.T) {
	cache := noProviderCache{}
	if cache.Add("key", []byte("value")) || cache.Contains("key") || cache.Remove("key") || cache.Len() != 0 || len(cache.Keys()) != 0 || cache.Resize(100) != 0 {
		t.Fatal("disabled cache retained state")
	}
	if value, ok := cache.Get("key"); value != nil || ok {
		t.Fatal("disabled Get returned a value")
	}
	if value, ok := cache.Peek("key"); value != nil || ok {
		t.Fatal("disabled Peek returned a value")
	}
	if key, value, ok := cache.GetOldest(); key != nil || value != nil || ok {
		t.Fatal("disabled GetOldest returned a value")
	}
	if key, value, ok := cache.RemoveOldest(); key != nil || value != nil || ok {
		t.Fatal("disabled RemoveOldest returned a value")
	}
	cache.Purge()
}

func fillRoutingBenchmark(b *testing.B, count int) (*routingStore, []ds.Key) {
	b.Helper()
	s := testRoutingStore(b, testRoutingLimits())
	keys := make([]ds.Key, count)
	for i := range keys {
		keys[i] = ds.NewKey(fmt.Sprintf("/providers/K%06d/P%02d", i/16, i%16))
		if err := s.Put(b.Context(), keys[i], []byte("timestamp")); err != nil {
			b.Fatal(err)
		}
	}
	return s, keys
}

func BenchmarkRoutingStorePrefix(b *testing.B) {
	for _, count := range []int{256, 32768} {
		b.Run(fmt.Sprint(count), func(b *testing.B) {
			s, _ := fillRoutingBenchmark(b, count)
			b.ReportAllocs()
			for b.Loop() {
				result, err := s.Query(b.Context(), dsq.Query{Prefix: "/providers/K000000"})
				if err != nil {
					b.Fatal(err)
				}
				n := 0
				for {
					e, ok := result.NextSync()
					if !ok {
						break
					}
					if e.Error != nil {
						b.Fatal(e.Error)
					}
					n++
				}
				_ = result.Close()
				if n != 16 {
					b.Fatalf("prefix returned %d providers", n)
				}
			}
		})
	}
}

func BenchmarkRoutingStoreBatch(b *testing.B) {
	s, keys := fillRoutingBenchmark(b, 32768)
	value := []byte("timestamp")
	ctx := b.Context()
	index := 0
	b.ReportAllocs()
	for b.Loop() {
		batch, err := s.Batch(ctx)
		if err != nil {
			b.Fatal(err)
		}
		for range 256 {
			if err := batch.Put(ctx, keys[index%len(keys)], value); err != nil {
				b.Fatal(err)
			}
			index++
		}
		if err := batch.Commit(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRoutingStoreSweep(b *testing.B) {
	s, _ := fillRoutingBenchmark(b, 32768)
	b.ReportAllocs()
	for b.Loop() {
		result, err := s.Query(b.Context(), dsq.Query{Prefix: records.ProvidersKeyPrefix})
		if err != nil {
			b.Fatal(err)
		}
		for {
			e, ok := result.NextSync()
			if !ok {
				break
			}
			if e.Error != nil {
				b.Fatal(e.Error)
			}
		}
		_ = result.Close()
	}
}
