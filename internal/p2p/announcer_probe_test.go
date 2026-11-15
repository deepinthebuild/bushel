package p2p

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	bbdigest "github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/teststorage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAnnouncerPartialCatalogRetainsWorkAndRetries(t *testing.T) {
	backend := teststorage.New()
	store, err := cas.NewStore(t.TempDir(), 64<<20, cas.Backend{Chunks: backend.Chunks, Mappings: backend.Mappings})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i := range 3 {
		data := testData(700000)
		data[0] ^= byte(i)
		if err := store.Put(t.Context(), cas.Sum(data), bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
	}
	// Use the actual directory walk order, which differs between filesystems.
	var order []cas.Digest
	if err := store.WalkCommitted(t.Context(), func(d cas.Digest) error { order = append(order, d); return nil }); err != nil || len(order) != 3 {
		t.Fatalf("catalog fixture: %d entries, error=%v", len(order), err)
	}
	var recovered atomic.Bool
	var failedProbes atomic.Int32
	backend.SetHook(func(_ context.Context, op teststorage.Operation, d bbdigest.Digest) error {
		if op == teststorage.MappingGet && d.GetHashString() == order[0].Hex() && !recovered.Load() {
			failedProbes.Add(1)
			return status.Error(codes.Unavailable, "one backend partition is unavailable")
		}
		return nil
	})
	synctest.Test(t, func(t *testing.T) {
		opts := testAnnouncerOptions()
		opts.QueueSize = 1
		recorder := &announcementRecorder{}
		release := make(chan struct{})
		var first atomic.Bool
		announce := func(ctx context.Context, d cas.Digest) error {
			if first.CompareAndSwap(false, true) {
				select {
				case <-release:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
			return recorder.announce(ctx, d)
		}
		a, cancel, done := startTestAnnouncerInstance(t, store, announce, opts)
		defer stopTestAnnouncer(t, cancel, done)
		// With one inflight job and one buffered final scan item, the partial
		// error arrives first. Neither piece of work may be discarded.
		stats := a.Stats()
		if stats.ProbeFailures != 1 || stats.Queued != 1 || !stats.Sweeping || !first.Load() {
			t.Fatalf("partial scan discarded active work: %+v", stats)
		}
		close(release)
		time.Sleep(2 * opts.MinInterval)
		synctest.Wait()
		if got := recorder.snapshot(); len(got) != 2 || got[0].digest != order[1] || got[1].digest != order[2] {
			t.Fatalf("healthy entries did not survive partial completion: %v", got)
		}
		for sweep := 2; sweep <= 3; sweep++ {
			time.Sleep(opts.RecoveryInterval)
			synctest.Wait()
			stats := a.Stats()
			if failedProbes.Load() != int32(sweep) || stats.ProbeFailures != uint64(sweep) || stats.Announced != uint64(2*sweep) {
				t.Fatalf("partial recovery sweep %d: failed probes=%d stats=%+v", sweep, failedProbes.Load(), stats)
			}
		}
		recovered.Store(true)
		time.Sleep(opts.RecoveryInterval + 3*opts.MinInterval)
		synctest.Wait()
		counts := make(map[cas.Digest]int)
		for _, call := range recorder.snapshot() {
			counts[call.digest]++
		}
		if counts[order[0]] != 1 || counts[order[1]] != 4 || counts[order[2]] != 4 || a.Stats().ProbeFailures != 3 {
			t.Fatalf("retained entry did not recover: counts=%v stats=%+v", counts, a.Stats())
		}
	})
}

func TestAnnouncerEmptyPartialScansRetryWithoutStopping(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failure := errors.New("backend unavailable")
		source := &testAnnouncementSource{walkErr: &cas.CatalogProbeError{Failed: 2, Err: failure}}
		opts := testAnnouncerOptions()
		a, cancel, done := startTestAnnouncerInstance(t, source, func(context.Context, cas.Digest) error {
			t.Fatal("empty scan announced an entry")
			return nil
		}, opts)
		defer stopTestAnnouncer(t, cancel, done)
		for sweep := 1; sweep <= 3; sweep++ {
			if sweep > 1 {
				time.Sleep(opts.RecoveryInterval)
				synctest.Wait()
			}
			stats := a.Stats()
			if stats.ProbeFailures != uint64(2*sweep) || stats.Sweeping || stats.Queued != 0 {
				t.Fatalf("empty partial sweep %d: %+v", sweep, stats)
			}
		}
	})
}
