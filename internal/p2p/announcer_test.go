package p2p

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"pgregory.net/rapid"
)

func testAnnouncerOptions() AnnouncerOptions {
	return AnnouncerOptions{
		QueueSize: 2, HintBuffer: 1, MaxAttempts: 2,
		MinInterval: time.Second, Timeout: 2 * time.Second,
		SweepInterval: time.Hour, RecoveryInterval: 20 * time.Second,
		RetryBase: 2 * time.Second, RetryMax: 4 * time.Second,
		OfflinePoll: 3 * time.Second, JitterSeed: 1,
		Clock: systemAnnouncerClock{},
	}
}

type testAnnouncementSource struct {
	mu          sync.Mutex
	catalog     []cas.Digest
	hints       chan cas.Digest
	overflow    chan struct{}
	subscribed  bool
	canceled    int
	walks       int
	activeWalks int
	walkErr     error
}

func (s *testAnnouncementSource) SubscribeCommits(n int) (<-chan cas.Digest, <-chan struct{}, func()) {
	s.mu.Lock()
	s.hints = make(chan cas.Digest, n)
	s.overflow = make(chan struct{}, 1)
	s.subscribed = true
	hints, overflow := s.hints, s.overflow
	s.mu.Unlock()
	var once sync.Once
	return hints, overflow, func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.subscribed = false
			s.canceled++
			close(hints)
			close(overflow)
		})
	}
}

func (s *testAnnouncementSource) WalkCommitted(ctx context.Context, yield func(cas.Digest) error) error {
	s.mu.Lock()
	if !s.subscribed {
		s.mu.Unlock()
		return errors.New("walk preceded subscription")
	}
	s.walks++
	s.activeWalks++
	catalog := slices.Clone(s.catalog)
	err := s.walkErr
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		s.activeWalks--
		s.mu.Unlock()
	}()
	if err != nil {
		return err
	}
	for _, d := range catalog {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := yield(d); err != nil {
			return err
		}
	}
	return nil
}

func (s *testAnnouncementSource) publish(d cas.Digest) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !slices.Contains(s.catalog, d) {
		s.catalog = append(s.catalog, d)
	}
	if !s.subscribed {
		return
	}
	select {
	case s.hints <- d:
	default:
		select {
		case s.overflow <- struct{}{}:
		default:
		}
	}
}

type announcementCall struct {
	digest cas.Digest
	at     time.Time
}

type announcementRecorder struct {
	mu    sync.Mutex
	calls []announcementCall
}

func (r *announcementRecorder) announce(_ context.Context, d cas.Digest) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, announcementCall{d, time.Now()})
	return nil
}

func (r *announcementRecorder) snapshot() []announcementCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.calls)
}

func startTestAnnouncer(t *testing.T, source AnnouncementSource, fn func(context.Context, cas.Digest) error, opts AnnouncerOptions) (context.CancelFunc, <-chan error) {
	t.Helper()
	_, cancel, done := startTestAnnouncerInstance(t, source, fn, opts)
	return cancel, done
}

func startTestAnnouncerInstance(t *testing.T, source AnnouncementSource, fn func(context.Context, cas.Digest) error, opts AnnouncerOptions) (*Announcer, context.CancelFunc, <-chan error) {
	t.Helper()
	a, err := NewAnnouncer(source, fn, opts)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	synctest.Wait()
	return a, cancel, done
}

func stopTestAnnouncer(t *testing.T, cancel context.CancelFunc, done <-chan error) {
	t.Helper()
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("announcer cancellation: %v", err)
	}
}

func TestPropertyAnnouncementQueueBoundsAndRetryModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		limit := rapid.IntRange(1, 12).Draw(t, "queue limit")
		opts := testAnnouncerOptions()
		opts.MaxAttempts = rapid.IntRange(1, 5).Draw(t, "maximum attempts")
		opts.JitterSeed = rapid.Uint64().Draw(t, "jitter seed")
		q := newAnnouncementQueue(limit)
		model := make(map[cas.Digest]int)
		var active *cas.Digest
		var lastStart time.Time
		now := time.Unix(0, 0)
		actions := rapid.SliceOfN(rapid.IntRange(0, 31), 1, 200).Draw(t, "events")
		for _, action := range actions {
			switch action % 4 {
			case 0:
				d := cas.Sum([]byte{byte(action / 4)})
				_, existing := model[d]
				want := existing || len(model) < limit
				if accepted := q.offer(d, now); accepted != want {
					t.Fatal("admission differs from bounded deduplication model")
				}
				if want && !existing {
					model[d] = 0
				}
			case 1:
				d, started := q.start(now, opts.MinInterval)
				if started {
					if active != nil || (!lastStart.IsZero() && now.Sub(lastStart) < opts.MinInterval) {
						t.Fatal("concurrency or rate bound exceeded")
					}
					if _, exists := model[d]; !exists {
						t.Fatal("started a digest outside the model")
					}
					model[d]++
					if model[d] > opts.MaxAttempts {
						t.Fatal("attempt bound exceeded")
					}
					active, lastStart = &d, now
				}
			case 2:
				if active != nil {
					var failure error
					if rapid.Bool().Draw(t, "announcement fails") {
						failure = errors.New("unavailable")
					}
					exhausted := failure != nil && model[*active] == opts.MaxAttempts
					if q.finish(now, failure, opts) != exhausted {
						t.Fatal("retry exhaustion differs from model")
					}
					if failure == nil || exhausted {
						delete(model, *active)
					}
					active = nil
				}
			case 3:
				now = now.Add(time.Duration(action/4+1) * time.Second)
			}
			if len(q.jobs) != len(model) || len(q.jobs) > limit {
				t.Fatal("queued plus inflight bound or model membership failed")
			}
			seen := make(map[cas.Digest]bool)
			for _, d := range q.order {
				if seen[d] || (active != nil && d == *active) {
					t.Fatal("duplicate queued or inflight digest")
				}
				seen[d] = true
			}
			if active != nil {
				seen[*active] = true
			}
			for d, attempts := range model {
				if !seen[d] || q.jobs[d].attempts != attempts {
					t.Fatal("queue lost a modeled attempt")
				}
			}
		}
	})
}

func TestPropertyAnnouncementBackoffBounds(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		opts := testAnnouncerOptions()
		opts.RetryBase = time.Duration(rapid.Int64Range(1, int64(time.Hour)).Draw(t, "base"))
		opts.RetryMax = time.Duration(rapid.Int64Range(int64(opts.RetryBase), int64(24*time.Hour)).Draw(t, "maximum"))
		opts.JitterSeed = rapid.Uint64().Draw(t, "seed")
		d := cas.Sum(rapid.SliceOfN(rapid.Byte(), 0, 64).Draw(t, "digest bytes"))
		attempt := rapid.IntRange(1, 16).Draw(t, "attempt")
		delay := announcementBackoff(d, attempt, opts)
		if delay < 1 || delay > opts.RetryMax || delay != announcementBackoff(d, attempt, opts) {
			t.Fatal("backoff is unbounded or nondeterministic")
		}
	})
}

func TestAnnouncerOverflowRecoveryAndRestart(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &testAnnouncementSource{}
		var first announcementRecorder
		cancel, done := startTestAnnouncer(t, source, first.announce, testAnnouncerOptions())
		for i := range 8 {
			source.publish(cas.Sum([]byte{byte(i)}))
		}
		time.Sleep(40 * time.Second)
		synctest.Wait()
		seen := make(map[cas.Digest]bool)
		for _, call := range first.snapshot() {
			seen[call.digest] = true
		}
		if len(seen) != 8 || source.walks < 2 {
			t.Fatalf("dropped hints did not recover: digests %d, walks %d", len(seen), source.walks)
		}
		stopTestAnnouncer(t, cancel, done)
		if source.activeWalks != 0 || source.subscribed || source.canceled != 1 {
			t.Fatal("shutdown did not join the producer and unsubscribe")
		}
		var second announcementRecorder
		cancel, done = startTestAnnouncer(t, source, second.announce, testAnnouncerOptions())
		time.Sleep(10 * time.Second)
		synctest.Wait()
		if len(second.snapshot()) != 8 {
			t.Fatal("restart did not replay the durable backlog")
		}
		stopTestAnnouncer(t, cancel, done)
	})
}

func TestPropertyAnnouncerSweepFairnessUnderHints(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		count := rapid.IntRange(1, 12).Draw(rt, "catalog size")
		opts := testAnnouncerOptions()
		opts.QueueSize = rapid.IntRange(1, 4).Draw(rt, "queue size")
		var failure string
		// Rapid uses the real test deadline. Each generated history gets its
		// own virtual-time bubble, and reports invariant failures outside it.
		synctest.Test(t, func(t *testing.T) {
			source := &testAnnouncementSource{}
			for i := range count {
				source.publish(cas.Sum([]byte(fmt.Sprintf("old-%d", i))))
			}
			original := slices.Clone(source.catalog)
			var recorder announcementRecorder
			cancel, done := startTestAnnouncer(t, source, recorder.announce, opts)
			defer stopTestAnnouncer(t, cancel, done)
			for i := range 3*count + 2*opts.QueueSize + 5 {
				source.publish(cas.Sum([]byte(fmt.Sprintf("new-%d", i))))
				time.Sleep(time.Second)
				synctest.Wait()
			}
			seen := make(map[cas.Digest]bool)
			calls := recorder.snapshot()
			for i, call := range calls {
				seen[call.digest] = true
				if i > 0 && call.at.Sub(calls[i-1].at) < opts.MinInterval {
					failure = "hint flood bypassed the announcement rate"
				}
			}
			for _, d := range original {
				if !seen[d] {
					failure = "continuous hints starved a durable catalog entry"
				}
			}
		})
		if failure != "" {
			rt.Fatal(failure)
		}
	})
}

func TestAnnouncerOfflinePauseAndCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &testAnnouncementSource{}
		for i := range 100 {
			source.publish(cas.Sum([]byte{byte(i)}))
		}
		opts := testAnnouncerOptions()
		var ready atomic.Bool
		var polls atomic.Int64
		opts.Ready = func() bool { polls.Add(1); return ready.Load() }
		var recorder announcementRecorder
		cancel, done := startTestAnnouncer(t, source, recorder.announce, opts)
		time.Sleep(time.Minute)
		synctest.Wait()
		if len(recorder.snapshot()) != 0 || polls.Load() > 21 {
			t.Fatalf("offline hot loop: calls %d, readiness polls %d", len(recorder.snapshot()), polls.Load())
		}
		ready.Store(true)
		time.Sleep(4 * time.Second)
		synctest.Wait()
		if len(recorder.snapshot()) == 0 {
			t.Fatal("reconnection did not resume publication")
		}
		// The catalog is much larger than the queue, so its producer is blocked.
		stopTestAnnouncer(t, cancel, done)
		if source.activeWalks != 0 || source.subscribed {
			t.Fatal("cancellation leaked a blocked catalog producer")
		}
	})
}

func TestAnnouncerDeadlineAndBoundedRetries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &testAnnouncementSource{catalog: []cas.Digest{cas.Sum([]byte("deadline"))}}
		var attempts atomic.Int64
		var deadlineErrors atomic.Int64
		fn := func(ctx context.Context, _ cas.Digest) error {
			attempts.Add(1)
			<-ctx.Done()
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				deadlineErrors.Add(1)
			}
			return ctx.Err()
		}
		cancel, done := startTestAnnouncer(t, source, fn, testAnnouncerOptions())
		time.Sleep(15 * time.Second)
		synctest.Wait()
		if attempts.Load() != 2 || deadlineErrors.Load() != 2 {
			t.Fatalf("deadlines or retry bound failed: attempts %d, deadlines %d", attempts.Load(), deadlineErrors.Load())
		}
		time.Sleep(20 * time.Second)
		synctest.Wait()
		if attempts.Load() < 3 {
			t.Fatal("exhausted retries did not trigger recovery")
		}
		stopTestAnnouncer(t, cancel, done)
	})
}

func TestAnnouncerReturnsCatalogFailure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		failure := errors.New("catalog unavailable")
		source := &testAnnouncementSource{walkErr: failure}
		a, err := NewAnnouncer(source, func(context.Context, cas.Digest) error { return nil }, testAnnouncerOptions())
		if err != nil {
			t.Fatal(err)
		}
		if err := a.Run(context.Background()); !errors.Is(err, failure) || !strings.Contains(err.Error(), "scan committed manifests") {
			t.Fatalf("catalog failure was hidden or misclassified: %v", err)
		}
		if source.subscribed || source.activeWalks != 0 {
			t.Fatal("scan failure leaked producer or subscription")
		}
	})
}

func TestPropertyAnnouncerRecoversFromFiniteFailures(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		failures := rapid.SliceOfN(rapid.IntRange(0, 4), 1, 6).Draw(rt, "failures per digest")
		opts := testAnnouncerOptions()
		opts.QueueSize = rapid.IntRange(1, 4).Draw(rt, "queue size")
		var missing bool
		synctest.Test(t, func(t *testing.T) {
			source := &testAnnouncementSource{}
			remaining := make(map[cas.Digest]int)
			for i, count := range failures {
				d := cas.Sum([]byte{byte(i)})
				source.publish(d)
				remaining[d] = count
			}
			seen := make(map[cas.Digest]bool)
			fn := func(_ context.Context, d cas.Digest) error {
				if remaining[d] > 0 {
					remaining[d]--
					return errors.New("transient announcement failure")
				}
				seen[d] = true
				return nil
			}
			cancel, done := startTestAnnouncer(t, source, fn, opts)
			time.Sleep(2 * time.Minute)
			synctest.Wait()
			missing = len(seen) != len(failures)
			stopTestAnnouncer(t, cancel, done)
		})
		if missing {
			rt.Fatal("bounded retries and recovery sweeps lost a recoverable publication")
		}
	})
}

func TestAnnouncerStatsExposeBacklogAndRecovery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		source := &testAnnouncementSource{}
		for i := range 6 {
			source.publish(cas.Sum([]byte{byte(i)}))
		}
		opts := testAnnouncerOptions()
		opts.QueueSize = 1
		var ready atomic.Bool
		opts.Ready = ready.Load
		failed := false
		fn := func(context.Context, cas.Digest) error {
			if !failed {
				failed = true
				return errors.New("one failed attempt")
			}
			return nil
		}
		a, cancel, done := startTestAnnouncerInstance(t, source, fn, opts)
		started := a.Stats()
		if started.Queued != 1 || !started.Sweeping || started.SweepStarted.IsZero() {
			t.Fatalf("offline backlog is not observable: %+v", started)
		}
		for i := 6; i < 9; i++ {
			source.publish(cas.Sum([]byte{byte(i)}))
			// Read concurrently with hint admission, before waiting for quiescence.
			if stats := a.Stats(); stats.Queued < 0 || stats.Queued > opts.QueueSize {
				t.Fatalf("snapshot exceeded queue bound: %+v", stats)
			}
		}
		synctest.Wait()
		if a.Stats().DroppedHints == 0 {
			t.Fatal("overflow is not observable")
		}
		ready.Store(true)
		time.Sleep(40 * time.Second)
		synctest.Wait()
		stats := a.Stats()
		if stats.Failures != 1 || stats.Announced < 9 || stats.LastSweepDuration <= 0 {
			t.Fatalf("completed attempts and sweep duration are not observable: %+v", stats)
		}
		stopTestAnnouncer(t, cancel, done)
		if stats := a.Stats(); stats.Queued != 0 || stats.Sweeping {
			t.Fatalf("shutdown retained active backlog: %+v", stats)
		}
	})
}

type providerLeaseModel struct {
	mu         sync.Mutex
	now        func() time.Time
	ttl        time.Duration
	expires    map[cas.Digest]time.Time
	renewals   map[cas.Digest]int
	gaps       int
	longestGap time.Duration
}

type providerLeaseSnapshot struct {
	expires    map[cas.Digest]time.Time
	renewals   map[cas.Digest]int
	gaps       int
	longestGap time.Duration
}

func newProviderLeaseModel(now func() time.Time, ttl time.Duration) *providerLeaseModel {
	return &providerLeaseModel{now: now, ttl: ttl, expires: make(map[cas.Digest]time.Time), renewals: make(map[cas.Digest]int)}
}

func (m *providerLeaseModel) announce(_ context.Context, d cas.Digest) error {
	// The provider-side model knows only a successful announcement and its TTL,
	// not the scheduler's queue, sweep, or retry implementation.
	now := m.now()
	m.mu.Lock()
	defer m.mu.Unlock()
	if expires, exists := m.expires[d]; exists && now.After(expires) {
		m.gaps++
		m.longestGap = max(m.longestGap, now.Sub(expires))
	}
	m.expires[d] = now.Add(m.ttl)
	m.renewals[d]++
	return nil
}

func (m *providerLeaseModel) snapshot() providerLeaseSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	return providerLeaseSnapshot{maps.Clone(m.expires), maps.Clone(m.renewals), m.gaps, m.longestGap}
}

func TestPropertyAnnouncerRenewsLeasesUnderSustainableLoad(t *testing.T) {
	rapid.Check(t, func(rt *rapid.T) {
		count := rapid.IntRange(1, 8).Draw(rt, "catalog size")
		opts := testAnnouncerOptions()
		opts.QueueSize = rapid.IntRange(1, 4).Draw(rt, "queue size")
		opts.MinInterval = rapid.SampledFrom([]time.Duration{15 * time.Minute, 30 * time.Minute, 45 * time.Minute, time.Hour}).Draw(rt, "rate interval")
		opts.SweepInterval = 6 * time.Hour
		opts.RecoveryInterval = 15 * time.Minute
		flood := rapid.Bool().Draw(rt, "continuous duplicate hints")
		const ttl = 24 * time.Hour
		var failure string
		synctest.Test(t, func(t *testing.T) {
			source := &testAnnouncementSource{}
			for i := range count {
				source.publish(cas.Sum([]byte{byte(i)}))
			}
			original := slices.Clone(source.catalog)
			leases := newProviderLeaseModel(opts.Clock.Now, ttl)
			a, cancel, done := startTestAnnouncerInstance(t, source, leases.announce, opts)
			defer stopTestAnnouncer(t, cancel, done)
			// Even with half the starts spent on hints, this bounded catalog has
			// room for a full renewal and six-hour sweep delay within the TTL.
			until := opts.Clock.Now().Add(3 * ttl)
			for opts.Clock.Now().Before(until) {
				if flood {
					// Duplicate uploads keep a constant durable catalog while applying
					// continuous pressure to the fast announcement path.
					source.publish(original[0])
				}
				time.Sleep(opts.MinInterval / 4)
				synctest.Wait()
				if stats := a.Stats(); stats.Queued > opts.QueueSize {
					failure = "sustained load exceeded the queue bound"
				}
			}
			got := leases.snapshot()
			if got.gaps != 0 {
				failure = "sustainable catalog or hint pressure allowed a provider lease to expire"
			}
			for _, d := range original {
				if got.renewals[d] < 2 || !opts.Clock.Now().Before(got.expires[d]) {
					failure = "durable entry was not renewed before expiry"
				}
			}
		})
		if failure != "" {
			rt.Fatal(failure)
		}
	})
}

func TestAnnouncerInsufficientCapacityExposesLeaseGaps(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		const ttl = 24 * time.Hour
		source := &testAnnouncementSource{}
		for i := range 4 {
			source.publish(cas.Sum([]byte{byte(i)}))
		}
		opts := testAnnouncerOptions()
		opts.QueueSize = 1
		opts.MinInterval = 8 * time.Hour // Four keys require 32 hours of capacity.
		opts.SweepInterval = 6 * time.Hour
		opts.RecoveryInterval = 15 * time.Minute
		leases := newProviderLeaseModel(opts.Clock.Now, ttl)
		a, cancel, done := startTestAnnouncerInstance(t, source, leases.announce, opts)
		time.Sleep(3 * ttl)
		synctest.Wait()
		got, stats := leases.snapshot(), a.Stats()
		if len(got.renewals) != 4 || got.gaps == 0 || got.longestGap < 8*time.Hour {
			t.Fatalf("overload failed to expose the expected expiry gap: %+v", got)
		}
		if stats.LastSweepDuration < ttl || stats.Queued != 1 || !stats.Sweeping {
			t.Fatalf("overload backlog and slow renewal are not observable: %+v", stats)
		}
		stopTestAnnouncer(t, cancel, done)
	})
}

func BenchmarkAnnouncerStats(b *testing.B) {
	var a Announcer
	b.ReportAllocs()
	for b.Loop() {
		_ = a.Stats()
	}
}
