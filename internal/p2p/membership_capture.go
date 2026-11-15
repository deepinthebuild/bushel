package p2p

import (
	"context"
	"crypto/sha256"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/membership"
)

var errMembershipScanLimit = errors.New("membership catalog limit reached")

type membershipBuild struct {
	generation    uint64
	watermark     uint64
	additions     map[cas.Digest]bool
	complete      bool
	repairingLoss bool
}

// membershipCapture owns ordering and bounded exact additions without I/O.
// The current baseline and an in-flight rebuild need separate tails: filling
// the old tail must not discard additions needed by the new baseline.
type membershipCapture struct {
	sequence   uint64
	generation uint64
	maxTail    int
	base       *membership.View
	tail       map[cas.Digest]bool
	complete   bool
	dirty      bool
	building   *membershipBuild
}

func (c *membershipCapture) add(d cas.Digest) error {
	if d.Validate() != nil {
		return membership.ErrInvalid
	}
	if c.sequence == ^uint64(0) {
		return membership.ErrInvalid
	}
	c.sequence++
	if c.base != nil && !c.append(c.tail, d) {
		c.complete, c.dirty = false, true
	}
	if c.building != nil && !c.append(c.building.additions, d) {
		c.building.complete, c.dirty = false, true
	}
	return nil
}

func (c *membershipCapture) append(tail map[cas.Digest]bool, d cas.Digest) bool {
	if tail[d] {
		return true
	}
	if len(tail) == c.maxTail {
		return false
	}
	tail[d] = true
	return true
}

func (c *membershipCapture) invalidate() {
	c.complete, c.dirty = false, true
	if c.building != nil {
		c.building.complete = false
	}
}

func (c *membershipCapture) begin() (*membershipBuild, error) {
	if c.building != nil || c.generation == ^uint64(0) {
		return nil, membership.ErrInvalid
	}
	c.generation++
	c.building = &membershipBuild{generation: c.generation, watermark: c.sequence,
		additions: make(map[cas.Digest]bool), complete: true, repairingLoss: c.dirty}
	c.dirty = false
	return c.building, nil
}

func (c *membershipCapture) finish(base *membership.View, complete bool) {
	if base == nil {
		// No baseline replaced the old capture. Preserve genuine loss from
		// before or during this attempt, without making constructor errors
		// alone request the shorter recovery interval.
		c.dirty = c.dirty || c.building.repairingLoss || !c.building.complete
		c.building = nil
		return
	}
	c.base, c.tail = base, c.building.additions
	c.complete = complete && c.building.complete
	// Scan capacity, backend errors and deadlines can persist. Only lost
	// additions request early recovery; partial scans keep the normal cadence.
	c.dirty = !c.building.complete
	c.building = nil
}

func (c *membershipCapture) view() (*membership.View, error) {
	if c.base == nil {
		return nil, nil
	}
	tail := make([]cas.Digest, 0, len(c.tail))
	for d := range c.tail {
		tail = append(tail, d)
	}
	return membership.WithTail(c.base, c.sequence, c.complete, tail)
}

type membershipBuildResult struct {
	view     *membership.View
	complete bool
	err      error
}

func (m *membershipService) capture(ctx context.Context, source AnnouncementSource, hints <-chan cas.Digest, overflow <-chan struct{}, unsubscribe func()) {
	defer unsubscribe()
	defer m.building.Store(false)
	ctx, cancel := context.WithCancel(ctx)
	var worker sync.WaitGroup
	defer func() { cancel(); worker.Wait() }()
	state := membershipCapture{maxTail: m.opts.maxTail}
	finished := make(chan membershipBuildResult, 1)
	var lastBuild time.Time
	pending := false
	start := func() error {
		build, err := state.begin()
		if err != nil {
			return err
		}
		generation, watermark := build.generation, build.watermark
		lastBuild = time.Now()
		m.building.Store(true)
		m.builds.Add(1)
		worker.Go(func() {
			result := m.build(ctx, source, generation, watermark)
			select {
			case finished <- result:
			case <-ctx.Done():
			}
		})
		return nil
	}
	publish := func() error {
		view, err := state.view()
		if err != nil || view == nil {
			return err
		}
		if previous := m.local.Load(); previous != nil && membership.SameContents(previous.view, view) {
			pending = false
			return nil
		}
		data, err := membership.Encode(view)
		if err != nil {
			return err
		}
		m.local.Store(&membershipPublication{view: view, data: data, hash: sha256.Sum256(data)})
		pending = false
		return nil
	}
	if err := start(); err != nil {
		slog.Error("Cannot start membership capture", "error", err)
		return
	}
	ticker := time.NewTicker(m.opts.publishInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case d, ok := <-hints:
			if !ok {
				return
			}
			if err := state.add(d); err != nil {
				slog.Error("Membership capture ordering failed", "error", err)
				return
			}
			pending = true
		case _, ok := <-overflow:
			if !ok {
				return
			}
			state.invalidate()
			pending = true
		case result := <-finished:
			m.building.Store(false)
			// Consume a pending loss signal before advertising a complete build.
			select {
			case _, ok := <-overflow:
				if !ok {
					return
				}
				state.invalidate()
			default:
			}
			state.finish(result.view, result.complete)
			if !state.complete {
				m.incomplete.Add(1)
			}
			if result.err != nil && ctx.Err() == nil && !errors.Is(result.err, errMembershipScanLimit) {
				slog.Warn("Membership catalog scan is incomplete", "error", result.err)
			}
			if err := publish(); err != nil {
				slog.Error("Cannot publish membership capture", "error", err)
				return
			}
		case now := <-ticker.C:
			if pending {
				if err := publish(); err != nil {
					slog.Error("Cannot publish membership additions", "error", err)
					return
				}
			}
			interval := m.opts.rebuildInterval
			if state.dirty {
				interval = m.opts.recoveryInterval
			}
			if state.building == nil && !now.Before(lastBuild.Add(interval)) {
				if err := start(); err != nil {
					slog.Error("Cannot rebuild membership capture", "error", err)
					return
				}
			}
		}
	}
}

func (m *membershipService) build(ctx context.Context, source AnnouncementSource, generation, watermark uint64) membershipBuildResult {
	scanCtx, cancel := context.WithTimeout(ctx, m.opts.buildTimeout)
	defer cancel()
	digests := make([]cas.Digest, 0, min(m.opts.maxKeys, 4096))
	scanErr := source.WalkCommitted(scanCtx, func(d cas.Digest) error {
		if len(digests) == m.opts.maxKeys {
			return errMembershipScanLimit
		}
		digests = append(digests, d)
		return nil
	})
	if ctx.Err() != nil {
		return membershipBuildResult{err: ctx.Err()}
	}
	view, err := membership.Build(m.session, generation, watermark, digests)
	if err != nil {
		return membershipBuildResult{err: err}
	}
	return membershipBuildResult{view: view, complete: scanErr == nil, err: scanErr}
}
