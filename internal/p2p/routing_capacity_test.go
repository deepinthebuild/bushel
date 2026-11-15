package p2p

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"runtime"
	"slices"
	"testing"

	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

func FuzzRoutingCapacityHistory(f *testing.F) {
	f.Add([]byte{0, 1, 2, 3, 4, 5, 0, 8, 16, 24, 32, 40, 48})
	f.Fuzz(func(t *testing.T, actions []byte) {
		if len(actions) > 1024 {
			t.Skip()
		}
		limits := testRoutingLimits()
		limits.MaxRecords, limits.MaxBytes = 8, 256
		limits.MaxKeyBytes, limits.MaxValueBytes = 32, 4
		limits.MaxProvidersPerKey, limits.MaxKeysPerProvider = 3, 4
		s := testRoutingStore(t, limits)
		var model []routingModelRecord
		for _, action := range actions {
			key := fmt.Sprintf("/providers/G%d/P%d", (action/4)%8, (action/32)%8)
			if action%4 == 0 {
				if err := s.Delete(t.Context(), ds.NewKey(key)); err != nil {
					t.Fatal(err)
				}
				model = slices.DeleteFunc(model, func(r routingModelRecord) bool { return r.key == key })
			} else {
				value := bytes.Repeat([]byte{action}, int(action%5))
				if err := s.Put(t.Context(), ds.NewKey(key), value); err != nil {
					t.Fatal(err)
				}
				model = modelRoutingPut(model, key, value, limits)
			}
			entries := routingEntries(t, s, dsq.Query{})
			if len(entries) != len(model) {
				t.Fatalf("got %d records, want %d", len(entries), len(model))
			}
			for i, entry := range entries {
				if entry.Key != model[i].key || !bytes.Equal(entry.Value, model[i].value) {
					t.Fatalf("record %d: got %+v, want %+v", i, entry, model[i])
				}
			}
		}
	})
}

// These synthetic keys have the same approximate lengths as encoded content
// and peer IDs. They measure the local datastore, not network retention.
func capacityKey(blob, provider int) ds.Key {
	return ds.NewKey(fmt.Sprintf("/providers/CIQ%048d/12D3KooW%044d", blob, provider))
}

func BenchmarkRoutingRetainedHeap(b *testing.B) {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	limits := testRoutingLimits()
	limits.MaxKeysPerProvider = limits.MaxRecords
	s := testRoutingStore(b, limits)
	for blob := range limits.MaxRecords {
		capacityPut(b, s, blob, 0, 0)
	}
	runtime.GC()
	runtime.ReadMemStats(&after)
	for b.Loop() {
		runtime.KeepAlive(s)
	}
	if after.HeapAlloc > before.HeapAlloc {
		b.ReportMetric(float64(after.HeapAlloc-before.HeapAlloc)/float64(limits.MaxRecords), "retained-B/record")
	}
}

func capacityPut(t testing.TB, s *routingStore, blob, provider, hour int) {
	t.Helper()
	var value [8]byte
	binary.BigEndian.PutUint64(value[:], uint64(hour))
	if err := s.Put(context.Background(), capacityKey(blob, provider), value[:]); err != nil {
		t.Fatal(err)
	}
}

// Expiry is applied to actual snapshots and deletes; it mirrors the manager's
// validity rule without asserting anything about remote DHT replicas.
func capacitySweep(t testing.TB, s *routingStore, hour int) {
	t.Helper()
	for _, entry := range routingEntries(t, s, dsq.Query{Prefix: "/providers"}) {
		if hour-int(binary.BigEndian.Uint64(entry.Value)) > 24 {
			if err := s.Delete(context.Background(), ds.NewKey(entry.Key)); err != nil {
				t.Fatal(err)
			}
		}
	}
}

func capacityCoverage(t testing.TB, s *routingStore, blobs, provider int) int {
	t.Helper()
	found := 0
	for blob := range blobs {
		ok, err := s.Has(context.Background(), capacityKey(blob, provider))
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			found++
		}
	}
	return found
}

func TestRoutingGeneratedCatalogHistory(t *testing.T) {
	limits := testRoutingLimits()
	limits.MaxRecords = 120000
	limits.MaxBytes = 120000 * 256
	limits.MaxKeysPerProvider = 80000
	s := testRoutingStore(t, limits)
	const catalog = 100000
	for blob := range catalog {
		capacityPut(t, s, blob, 0, 0)
		// Repeated announcements refresh a record rather than consuming slots.
		capacityPut(t, s, blob, 0, 0)
	}
	if got := capacityCoverage(t, s, catalog, 0); got != 80000 {
		t.Fatalf("per-provider capacity: got %d, want 80000", got)
	}
	// A second provider churns through a larger catalog; the global limit
	// causes older records to disappear before their 24-hour expiry.
	for blob := range catalog {
		capacityPut(t, s, blob, 1, 1)
	}
	if got := capacityCoverage(t, s, catalog, 0); got != 40000 {
		t.Fatalf("overload coverage: got %d, want 40000", got)
	}
	// A slow renewal updates only half the catalog before expiration.
	for blob := catalog / 2; blob < catalog; blob++ {
		capacityPut(t, s, blob, 0, 23)
	}
	capacitySweep(t, s, 25)
	if got := capacityCoverage(t, s, catalog, 0); got != 50000 {
		t.Fatalf("post-expiry coverage: got %d, want 50000", got)
	}
	if got := capacityCoverage(t, s, catalog, 1); got != 70000 {
		t.Fatalf("coverage at validity boundary: got %d, want 70000", got)
	}
	capacitySweep(t, s, 26)
	if got := capacityCoverage(t, s, catalog, 1); got != 0 {
		t.Fatalf("expired provider coverage: got %d, want 0", got)
	}
	// The current store is ephemeral: restart loses even fresh records.
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	restarted := testRoutingStore(t, limits)
	if got := capacityCoverage(t, restarted, catalog, 0); got != 0 {
		t.Fatalf("ephemeral restart retained %d records", got)
	}
}

func BenchmarkRoutingIndex(b *testing.B) {
	limits := testRoutingLimits()
	limits.MaxKeysPerProvider = limits.MaxRecords
	s := testRoutingStore(b, limits)
	for blob := range limits.MaxRecords {
		capacityPut(b, s, blob, 0, 0)
	}
	b.Run("Lookup", func(b *testing.B) {
		key := capacityKey(limits.MaxRecords/2, 0)
		b.ReportAllocs()
		for b.Loop() {
			if _, err := s.Get(context.Background(), key); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("FullSnapshot", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			result, err := s.Query(context.Background(), dsq.Query{Prefix: "/providers"})
			if err != nil {
				b.Fatal(err)
			}
			if _, err := result.Rest(); err != nil {
				b.Fatal(err)
			}
			if err := result.Close(); err != nil {
				b.Fatal(err)
			}
		}
	})
	b.Run("SyntheticExpirySweep", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			b.StopTimer()
			for blob := range limits.MaxRecords {
				capacityPut(b, s, blob, 0, 0)
			}
			b.StartTimer()
			capacitySweep(b, s, 25)
		}
	})
}
