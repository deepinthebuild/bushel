package p2p

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
)

// AnnouncementSource is a durable catalog plus bounded, lossy fast-path hints.
// WalkCommitted must honor cancellation, including while its callback blocks.
type AnnouncementSource interface {
	WalkCommitted(context.Context, func(cas.Digest) error) error
	SubscribeCommits(int) (<-chan cas.Digest, <-chan struct{}, func())
}

// AnnouncerClock makes scheduling and timeout progress explicit in tests.
type AnnouncerClock interface {
	Now() time.Time
	NewTimer(time.Duration) AnnouncerTimer
	WithTimeout(context.Context, time.Duration) (context.Context, context.CancelFunc)
}

type AnnouncerTimer interface {
	C() <-chan time.Time
	Stop() bool
	Reset(time.Duration) bool
}

type AnnouncerOptions struct {
	QueueSize        int
	HintBuffer       int
	MaxAttempts      int
	MinInterval      time.Duration
	Timeout          time.Duration
	SweepInterval    time.Duration
	RecoveryInterval time.Duration
	RetryBase        time.Duration
	RetryMax         time.Duration
	OfflinePoll      time.Duration
	Ready            func() bool
	Clock            AnnouncerClock
	JitterSeed       uint64
}

type Announcer struct {
	source   AnnouncementSource
	announce func(context.Context, cas.Digest) error
	opts     AnnouncerOptions
	statsMu  sync.RWMutex
	stats    AnnouncerStats
}

// AnnouncerStats is an approximate lifetime snapshot. Queued includes inflight
// work; Announced and Failures count completed announcement attempts observed
// by Run. DroppedHints is a lower bound: source overflow signals can coalesce
// multiple losses. ProbeFailures counts backend entries skipped during partial
// catalog scans. Sweep timing covers catalog admission, including queue
// backpressure; remaining announcements are represented by Queued.
type AnnouncerStats struct {
	Queued            int
	Announced         uint64
	Failures          uint64
	ProbeFailures     uint64
	DroppedHints      uint64
	Sweeping          bool
	SweepStarted      time.Time
	LastSweepDuration time.Duration
}

// Stats is safe to call concurrently with Run. SweepStarted remains the start
// time of the latest sweep; LastSweepDuration is the latest completed sweep.
func (a *Announcer) Stats() AnnouncerStats {
	a.statsMu.RLock()
	defer a.statsMu.RUnlock()
	return a.stats
}

// NewAnnouncer defaults to one worker, 1024 queued/inflight digests, 10 starts
// per second, and a 15-second announcement deadline. A six-hour renewal sweep
// leaves margin within the 24-hour provider TTL. Overflow and exhausted retries
// trigger a separate 15-minute recovery sweep. A large or unavailable catalog
// can exceed this throughput; these bounds do not guarantee global visibility.
// announce must return when its context is canceled. Run may be called once.
func NewAnnouncer(source AnnouncementSource, announce func(context.Context, cas.Digest) error, opts AnnouncerOptions) (*Announcer, error) {
	if source == nil || announce == nil {
		return nil, errors.New("announcement source and function are required")
	}
	if opts.QueueSize == 0 {
		opts.QueueSize = 1024
	}
	if opts.HintBuffer == 0 {
		opts.HintBuffer = 256
	}
	if opts.MaxAttempts == 0 {
		opts.MaxAttempts = 3
	}
	defaults := []struct {
		value *time.Duration
		def   time.Duration
	}{
		{&opts.MinInterval, 100 * time.Millisecond}, {&opts.Timeout, 15 * time.Second},
		{&opts.SweepInterval, 6 * time.Hour}, {&opts.RecoveryInterval, 15 * time.Minute},
		{&opts.RetryBase, 5 * time.Second}, {&opts.RetryMax, time.Minute},
		{&opts.OfflinePoll, 15 * time.Second},
	}
	for _, d := range defaults {
		if *d.value == 0 {
			*d.value = d.def
		}
		if *d.value < 0 || *d.value > 24*time.Hour {
			return nil, errors.New("announcement durations must be positive and at most 24 hours")
		}
	}
	if opts.QueueSize < 1 || opts.QueueSize > 65536 || opts.HintBuffer < 1 || opts.HintBuffer > 4096 || opts.MaxAttempts < 1 || opts.MaxAttempts > 16 {
		return nil, errors.New("announcement queue, hint, or attempt bound is invalid")
	}
	if opts.RetryMax < opts.RetryBase || opts.RecoveryInterval > opts.SweepInterval || opts.SweepInterval >= 24*time.Hour {
		return nil, errors.New("announcement retry and renewal intervals are inconsistent")
	}
	if opts.Clock == nil {
		opts.Clock = systemAnnouncerClock{}
	}
	if opts.Ready == nil {
		opts.Ready = func() bool { return true }
	}
	if opts.JitterSeed == 0 {
		var seed [8]byte
		if _, err := rand.Read(seed[:]); err != nil {
			return nil, err
		}
		opts.JitterSeed = binary.LittleEndian.Uint64(seed[:])
	}
	return &Announcer{source: source, announce: announce, opts: opts}, nil
}

type announcementJob struct {
	digest   cas.Digest
	attempts int
	due      time.Time
}

// announcementQueue owns only explicit protocol state. The methods perform no
// I/O and receive time as a parameter, allowing generated event histories.
type announcementQueue struct {
	limit     int
	jobs      map[cas.Digest]*announcementJob
	order     []cas.Digest
	inflight  *announcementJob
	nextStart time.Time
}

func newAnnouncementQueue(limit int) *announcementQueue {
	return &announcementQueue{limit: limit, jobs: make(map[cas.Digest]*announcementJob)}
}

func (q *announcementQueue) offer(d cas.Digest, now time.Time) bool {
	if d.Validate() != nil {
		return false
	}
	if _, exists := q.jobs[d]; exists {
		return true
	}
	if len(q.jobs) == q.limit {
		return false
	}
	q.jobs[d] = &announcementJob{digest: d, due: now}
	q.order = append(q.order, d)
	return true
}

func (q *announcementQueue) due() (time.Time, bool) {
	if q.inflight != nil || len(q.order) == 0 {
		return time.Time{}, false
	}
	due := q.jobs[q.order[0]].due
	for _, d := range q.order {
		if candidate := q.jobs[d].due; candidate.Before(due) {
			due = candidate
		}
	}
	if due.Before(q.nextStart) {
		return q.nextStart, true
	}
	return due, true
}

func (q *announcementQueue) start(now time.Time, interval time.Duration) (cas.Digest, bool) {
	if q.inflight != nil || now.Before(q.nextStart) {
		return cas.Digest{}, false
	}
	for i, d := range q.order {
		job := q.jobs[d]
		if now.Before(job.due) {
			continue
		}
		q.order = append(q.order[:i], q.order[i+1:]...)
		job.attempts++
		q.inflight = job
		q.nextStart = now.Add(interval)
		return d, true
	}
	return cas.Digest{}, false
}

// finish returns true when bounded retries are exhausted and a recovery sweep
// is needed. Failures keep their queue slot until success or exhaustion.
func (q *announcementQueue) finish(now time.Time, err error, opts AnnouncerOptions) bool {
	job := q.inflight
	q.inflight = nil
	if err == nil || job.attempts >= opts.MaxAttempts {
		delete(q.jobs, job.digest)
		return err != nil
	}
	job.due = now.Add(announcementBackoff(job.digest, job.attempts, opts))
	q.order = append(q.order, job.digest)
	return false
}

func announcementBackoff(d cas.Digest, attempt int, opts AnnouncerOptions) time.Duration {
	delay := opts.RetryBase
	for i := 1; i < attempt && delay < opts.RetryMax; i++ {
		delay = min(delay*2, opts.RetryMax)
	}
	// Stable for a given injected seed, but independent between running nodes.
	x := binary.LittleEndian.Uint64(d.Hash[:8]) ^ opts.JitterSeed ^ uint64(attempt)
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	jittered := delay*3/4 + time.Duration(x%uint64(delay/2+1))
	return min(max(jittered, time.Nanosecond), opts.RetryMax)
}

func earlier(a, b time.Time) time.Time {
	if a.IsZero() || (!b.IsZero() && b.Before(a)) {
		return b
	}
	return a
}

type announcementScan struct {
	items <-chan cas.Digest
	err   <-chan error
}

// Run subscribes before starting its first catalog walk. Catalog production is
// backpressured rather than discarded; alternating admissions prevent a steady
// stream of new hints from starving a sweep. Partial backend probe failures and
// announcement errors are retried internally; other scan errors stop Run.
// Cancellation joins both workers.
func (a *Announcer) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	hints, overflow, unsubscribe := a.source.SubscribeCommits(a.opts.HintBuffer)
	var workers sync.WaitGroup
	defer func() {
		cancel()
		workers.Wait()
		unsubscribe()
		a.statsMu.Lock()
		a.stats.Queued = 0
		a.stats.Sweeping = false
		a.statsMu.Unlock()
	}()
	q := newAnnouncementQueue(a.opts.QueueSize)
	clock := a.opts.Clock
	timer := clock.NewTimer(time.Hour)
	timer.Stop()
	defer timer.Stop()
	results := make(chan error, 1)
	var scan *announcementScan
	nextSweep := clock.Now()
	var recovery, offlineUntil time.Time
	preferSweep := true
	markRecovery := func(now time.Time) { recovery = earlier(recovery, now.Add(a.opts.RecoveryInterval)) }
	acceptScanError := func(now time.Time, err error) error {
		if err == nil {
			return nil
		}
		var partial *cas.CatalogProbeError
		if !errors.As(err, &partial) {
			return fmt.Errorf("scan committed manifests: %w", err)
		}
		markRecovery(now)
		a.statsMu.Lock()
		a.stats.ProbeFailures += uint64(max(partial.Failed, 0))
		a.statsMu.Unlock()
		return nil
	}
	finish := func(now time.Time, err error) {
		if q.finish(now, err, a.opts) {
			markRecovery(now)
		}
		a.statsMu.Lock()
		a.stats.Queued = len(q.jobs)
		if err == nil {
			a.stats.Announced++
		} else {
			a.stats.Failures++
		}
		a.statsMu.Unlock()
	}
	acceptHint := func(d cas.Digest, ok bool, now time.Time) {
		if !ok {
			hints = nil
			return
		}
		accepted := q.offer(d, now)
		if !accepted {
			markRecovery(now)
		}
		a.statsMu.Lock()
		a.stats.Queued = len(q.jobs)
		if !accepted {
			a.stats.DroppedHints++
		}
		a.statsMu.Unlock()
		preferSweep = true
	}
	acceptSweep := func(d cas.Digest, ok bool, now time.Time) error {
		if !ok {
			if scan.err != nil {
				if err := acceptScanError(now, <-scan.err); err != nil {
					return err
				}
			}
			scan = nil
			nextSweep = now.Add(a.opts.SweepInterval)
			a.statsMu.Lock()
			a.stats.LastSweepDuration = now.Sub(a.stats.SweepStarted)
			a.stats.Sweeping = false
			a.statsMu.Unlock()
			return nil
		}
		q.offer(d, now) // Called only with a free slot; duplicates consume none.
		a.statsMu.Lock()
		a.stats.Queued = len(q.jobs)
		a.statsMu.Unlock()
		preferSweep = false
		return nil
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		now := clock.Now()
		// Completion is checked before accepting more input, even under floods.
		select {
		case err := <-results:
			finish(now, err)
		default:
		}
		if scan == nil && !now.Before(earlier(nextSweep, recovery)) {
			items := make(chan cas.Digest, 1)
			done := make(chan error, 1)
			scan = &announcementScan{items: items, err: done}
			recovery = time.Time{}
			a.statsMu.Lock()
			a.stats.Sweeping = true
			a.stats.SweepStarted = now
			a.statsMu.Unlock()
			workers.Go(func() {
				err := a.source.WalkCommitted(ctx, func(d cas.Digest) error {
					select {
					case items <- d:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				})
				close(items)
				done <- err
			})
		}
		if due, ok := q.due(); ok && !now.Before(due) && !now.Before(offlineUntil) {
			if a.opts.Ready() {
				if d, ok := q.start(now, a.opts.MinInterval); ok {
					workers.Go(func() {
						callCtx, callCancel := clock.WithTimeout(ctx, a.opts.Timeout)
						err := a.announce(callCtx, d)
						callCancel()
						results <- err
					})
				}
			} else {
				offlineUntil = now.Add(a.opts.OfflinePoll)
			}
		}
		var sweepItems <-chan cas.Digest
		var scanErrors <-chan error
		if scan != nil {
			scanErrors = scan.err
			if len(q.jobs) < q.limit {
				sweepItems = scan.items
			}
		}
		// A preferred ready input gets its turn before the general select.
		if len(q.jobs) < q.limit {
			if preferSweep {
				select {
				case d, ok := <-sweepItems:
					if err := acceptSweep(d, ok, now); err != nil {
						return err
					}
					continue
				default:
				}
			} else {
				select {
				case d, ok := <-hints:
					acceptHint(d, ok, now)
					continue
				default:
				}
			}
		}
		var wake time.Time
		if scan == nil {
			wake = earlier(nextSweep, recovery)
		}
		if due, ok := q.due(); ok {
			if due.Before(offlineUntil) {
				due = offlineUntil
			}
			wake = earlier(wake, due)
		}
		if !timer.Stop() {
			select {
			case <-timer.C():
			default:
			}
		}
		var tick <-chan time.Time
		if !wake.IsZero() {
			timer.Reset(max(wake.Sub(now), 0))
			tick = timer.C()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-results:
			finish(clock.Now(), err)
		case <-tick:
		case err := <-scanErrors:
			if err := acceptScanError(clock.Now(), err); err != nil {
				return err
			}
			scan.err = nil
		case d, ok := <-sweepItems:
			if err := acceptSweep(d, ok, clock.Now()); err != nil {
				return err
			}
		case d, ok := <-hints:
			acceptHint(d, ok, clock.Now())
		case _, ok := <-overflow:
			if ok {
				markRecovery(clock.Now())
				a.statsMu.Lock()
				a.stats.DroppedHints++
				a.statsMu.Unlock()
			} else {
				overflow = nil
			}
		}
	}
}

type systemAnnouncerClock struct{}

func (systemAnnouncerClock) Now() time.Time { return time.Now() }
func (systemAnnouncerClock) NewTimer(d time.Duration) AnnouncerTimer {
	return systemAnnouncerTimer{time.NewTimer(d)}
}
func (systemAnnouncerClock) WithTimeout(ctx context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, d)
}

type systemAnnouncerTimer struct{ timer *time.Timer }

func (t systemAnnouncerTimer) C() <-chan time.Time        { return t.timer.C }
func (t systemAnnouncerTimer) Stop() bool                 { return t.timer.Stop() }
func (t systemAnnouncerTimer) Reset(d time.Duration) bool { return t.timer.Reset(d) }
