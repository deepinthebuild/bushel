package p2p

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/membership"
	"pgregory.net/rapid"
)

func TestMembershipCaptureHistory(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		state := membershipCapture{maxTail: rapid.IntRange(1, 4).Draw(t, "tail limit")}
		catalog := make(map[cas.Digest]bool)
		var scan []cas.Digest
		var previous *membership.View
		finish := func(ok bool) {
			build := state.building
			base, err := membership.Build([16]byte{1}, build.generation, build.watermark, scan)
			if err != nil {
				t.Fatal(err)
			}
			state.finish(base, ok)
		}
		for step := range rapid.IntRange(1, 100).Draw(t, "steps") {
			d := cas.Sum([]byte{byte(rapid.IntRange(0, 15).Draw(t, "digest"))})
			switch rapid.IntRange(0, 4).Draw(t, "event") {
			case 0, 1:
				catalog[d] = true
				if err := state.add(d); err != nil {
					t.Fatal(err)
				}
			case 2:
				catalog[d] = true // The commit channel lost this observation.
				state.invalidate()
			case 3:
				if state.building == nil {
					if _, err := state.begin(); err != nil {
						t.Fatal(err)
					}
					scan = slices.Collect(maps.Keys(catalog))
				}
			case 4:
				if state.building != nil {
					finish(rapid.Bool().Draw(t, "scan complete"))
				}
			}
			view, err := state.view()
			if err != nil || len(state.tail) > state.maxTail || (state.building != nil && len(state.building.additions) > state.maxTail) {
				t.Fatalf("step %d: unbounded or invalid capture: %v", step, err)
			}
			if view != nil {
				if !membership.CanReplace(previous, view) {
					t.Fatalf("step %d: capture regressed", step)
				}
				if view.Complete() {
					for d := range catalog {
						if !view.MayContain(d) {
							t.Fatalf("step %d: complete capture omitted a committed digest", step)
						}
					}
				}
				previous = view
			}
		}
		if state.building != nil {
			finish(false)
		}
		if _, err := state.begin(); err != nil {
			t.Fatal(err)
		}
		scan = slices.Collect(maps.Keys(catalog))
		finish(true)
		view, err := state.view()
		if err != nil || !view.Complete() || state.dirty {
			t.Fatalf("successful recovery remained incomplete: %v", err)
		}
		for d := range catalog {
			if !view.MayContain(d) {
				t.Fatal("recovery omitted committed digest")
			}
		}
	})
}

// Freeze each scan after taking its catalog snapshot. Additions and lost hints
// can then be placed on either side of an actual asynchronous scan boundary.
type membershipBlockedSource struct {
	testAnnouncementSource
	started chan struct{}
	release chan struct{}
}

func (s *membershipBlockedSource) WalkCommitted(ctx context.Context, yield func(cas.Digest) error) error {
	s.mu.Lock()
	if !s.subscribed {
		s.mu.Unlock()
		return errors.New("scan preceded subscription")
	}
	s.walks++
	s.activeWalks++
	catalog := slices.Clone(s.catalog)
	s.mu.Unlock()
	defer func() { s.mu.Lock(); s.activeWalks--; s.mu.Unlock() }()
	select {
	case s.started <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-s.release:
	case <-ctx.Done():
		return ctx.Err()
	}
	for _, d := range catalog {
		if err := yield(d); err != nil {
			return err
		}
	}
	return nil
}

type membershipTestReporter interface {
	Helper()
	Fatal(...any)
}

func testMembershipService(t membershipTestReporter) *membershipService {
	t.Helper()
	opts, err := (membershipOptions{}).withDefaults()
	if err != nil {
		t.Fatal(err)
	}
	return &membershipService{opts: opts, session: [16]byte{1}}
}

func TestMembershipCaptureOverflowRecoveryAndIdlePublication(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := testMembershipService(t)
		source := &membershipBlockedSource{started: make(chan struct{}), release: make(chan struct{})}
		hints, overflow, unsubscribe := source.SubscribeCommits(1)
		before := cas.Sum([]byte("before scan"))
		lost := cas.Sum([]byte("lost before scan"))
		source.publish(before)
		source.publish(lost) // Overflow is already pending when capture starts.
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); m.capture(ctx, source, hints, overflow, unsubscribe) }()
		<-source.started
		during := cas.Sum([]byte("during frozen scan"))
		source.publish(during)
		synctest.Wait()
		source.release <- struct{}{}
		synctest.Wait()
		first := m.local.Load()
		if first == nil || first.view.Complete() || !first.view.MayContain(before) {
			t.Fatal("overflow did not invalidate initial capture")
		}
		time.Sleep(m.opts.recoveryInterval)
		<-source.started
		// Lose an observation during the recovery scan as well.
		source.mu.Lock()
		lostDuring := cas.Sum([]byte("lost during scan"))
		source.catalog = append(source.catalog, lostDuring)
		source.overflow <- struct{}{}
		source.mu.Unlock()
		synctest.Wait()
		source.release <- struct{}{}
		synctest.Wait()
		if m.local.Load().view.Complete() {
			t.Fatal("loss during scan was incorrectly repaired by that scan")
		}
		time.Sleep(m.opts.recoveryInterval)
		<-source.started
		source.release <- struct{}{}
		synctest.Wait()
		recovered := m.local.Load()
		if !recovered.view.Complete() {
			t.Fatal("quiet complete scan did not recover")
		}
		for _, d := range []cas.Digest{before, lost, during, lostDuring} {
			if !recovered.view.MayContain(d) {
				t.Fatal("recovery lost a catalog entry")
			}
		}
		time.Sleep(m.opts.rebuildInterval)
		<-source.started
		source.release <- struct{}{}
		synctest.Wait()
		if m.local.Load() != recovered {
			t.Fatal("idle rebuild replaced encoded publication and conditional token")
		}
		cancel()
		<-done
		source.mu.Lock()
		defer source.mu.Unlock()
		if source.canceled != 1 || source.activeWalks != 0 || m.building.Load() {
			t.Fatal("capture did not release its subscription and worker")
		}
	})
}

func TestMembershipCaptureCancellationAndCapacity(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := testMembershipService(t)
		source := &membershipBlockedSource{started: make(chan struct{}), release: make(chan struct{})}
		hints, overflow, unsubscribe := source.SubscribeCommits(1)
		ctx, cancel := context.WithCancel(context.Background())
		var done sync.WaitGroup
		done.Go(func() { m.capture(ctx, source, hints, overflow, unsubscribe) })
		<-source.started
		cancel()
		done.Wait()
		if source.activeWalks != 0 || source.canceled != 1 {
			t.Fatal("blocked scan outlived capture shutdown")
		}
	})
	m := testMembershipService(t)
	m.opts.maxKeys = 1
	source := &testAnnouncementSource{catalog: []cas.Digest{cas.Sum([]byte("a")), cas.Sum([]byte("b"))}}
	_, _, unsubscribe := source.SubscribeCommits(1)
	defer unsubscribe()
	result := m.build(context.Background(), source, 1, 0)
	if result.complete || !errors.Is(result.err, errMembershipScanLimit) || result.view == nil || !result.view.MayContain(source.catalog[0]) {
		t.Fatalf("bounded catalog did not retain partial positive hints: %+v", result)
	}
}

func TestMembershipCaptureTimeoutKeepsRegularScanCadence(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		m := testMembershipService(t)
		source := &membershipBlockedSource{started: make(chan struct{}, 1), release: make(chan struct{})}
		hints, overflow, unsubscribe := source.SubscribeCommits(1)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); m.capture(ctx, source, hints, overflow, unsubscribe) }()
		defer func() { cancel(); <-done }()
		<-source.started
		// The two-minute deadline is longer than the loss-recovery interval.
		// A timeout alone must not immediately start the same expensive scan.
		time.Sleep(m.opts.buildTimeout)
		synctest.Wait()
		if publication := m.local.Load(); publication == nil || publication.view.Complete() || m.building.Load() {
			t.Fatal("timed-out scan did not publish an incomplete idle view")
		}
		time.Sleep(m.opts.rebuildInterval - m.opts.buildTimeout - time.Second)
		synctest.Wait()
		if m.builds.Load() != 1 || m.building.Load() {
			t.Fatal("persistent scan timeout used the capture-loss recovery cadence")
		}
		time.Sleep(time.Second)
		<-source.started
		synctest.Wait()
		if m.builds.Load() != 2 || !m.building.Load() {
			t.Fatal("normal five-minute rebuild did not resume after timeout")
		}
	})
}

func TestMembershipCaptureFailureDistinguishesLostObservations(t *testing.T) {
	for _, before := range []bool{false, true} {
		for _, during := range []bool{false, true} {
			for _, hasBaseline := range []bool{false, true} {
				state := membershipCapture{maxTail: 1}
				if before {
					state.invalidate()
				}
				build, err := state.begin()
				if err != nil {
					t.Fatal(err)
				}
				if during {
					state.invalidate()
				}
				var baseline *membership.View
				if hasBaseline {
					baseline, err = membership.Build([16]byte{1}, build.generation, build.watermark, nil)
					if err != nil {
						t.Fatal(err)
					}
				}
				state.finish(baseline, false)
				wantRecovery := during || (before && !hasBaseline)
				if state.dirty != wantRecovery || state.complete {
					t.Fatalf("loss before=%v during=%v baseline=%v: recovery=%v complete=%v", before, during, hasBaseline, state.dirty, state.complete)
				}
			}
		}
	}
}
