package p2p

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/deepinthebuild/bushel/internal/membership"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

// A stable bounded cohort keeps idle conditional polling cheap. Active reads
// permit periodic rotation; peers outside the cohort remain ordinary fallback.
type membershipPollSchedule struct {
	next            map[peer.ID]time.Time
	inflight        map[peer.ID]bool
	cursor          peer.ID
	selected        map[peer.ID]bool
	selectionCursor peer.ID
	rotated         time.Time
	started         bool
	budget          membershipPollBudget
}

const membershipPollBurst = 4

// The aggregate budget survives disconnects, protocol changes, and cohort
// rotation. Forgetting a per-peer cooldown must not create new download credit.
// Four burst tokens accommodate the two workers and five-second scheduler tick;
// sustained starts are bounded to one cohort per polling interval.
type membershipPollBudget struct {
	at      time.Time
	tokens  int
	started bool
}

func (b *membershipPollBudget) allow(now time.Time, interval time.Duration) bool {
	step := max(time.Nanosecond, interval/membershipPeerLimit)
	if !b.started {
		b.at, b.tokens, b.started = now, membershipPollBurst, true
	}
	if now.After(b.at) {
		if refill := int(now.Sub(b.at) / step); refill > 0 {
			b.tokens = min(membershipPollBurst, b.tokens+refill)
			b.at = b.at.Add(time.Duration(refill) * step)
		}
	}
	if b.tokens == 0 {
		return false
	}
	b.tokens--
	return true
}

func (s *membershipPollSchedule) reconcile(peers []peer.ID, now time.Time, rotate bool) []peer.ID {
	if s.selected == nil {
		s.selected = make(map[peer.ID]bool)
		s.next = make(map[peer.ID]time.Time)
		s.inflight = make(map[peer.ID]bool)
	}
	if !s.started || rotate {
		s.rotated, s.started = now, true
		clear(s.selected)
	}
	connected := make(map[peer.ID]bool, len(peers))
	for _, id := range peers {
		connected[id] = true
	}
	for id := range s.selected {
		if !connected[id] {
			delete(s.selected, id)
		}
	}
	start, _ := slices.BinarySearch(peers, s.selectionCursor)
	for start < len(peers) && peers[start] <= s.selectionCursor {
		start++
	}
	for offset := 0; offset < len(peers) && len(s.selected) < membershipPeerLimit; offset++ {
		id := peers[(start+offset)%len(peers)]
		if !s.selected[id] {
			s.selected[id], s.selectionCursor = true, id
		}
	}
	selected := make([]peer.ID, 0, len(s.selected))
	for id := range s.selected {
		selected = append(selected, id)
	}
	slices.Sort(selected)
	for id := range s.next {
		if !s.selected[id] {
			delete(s.next, id)
		}
	}
	return selected
}

func (s *membershipPollSchedule) take(peers []peer.ID, now time.Time, interval time.Duration) (peer.ID, bool) {
	if len(peers) == 0 || len(s.inflight) == membershipWorkers {
		return "", false
	}
	if s.next == nil {
		s.next = make(map[peer.ID]time.Time)
		s.inflight = make(map[peer.ID]bool)
	}
	start, _ := slices.BinarySearch(peers, s.cursor)
	for start < len(peers) && peers[start] <= s.cursor {
		start++
	}
	for offset := range len(peers) {
		id := peers[(start+offset)%len(peers)]
		if s.inflight[id] || now.Before(s.next[id]) {
			continue
		}
		if !s.budget.allow(now, interval) {
			return "", false
		}
		s.next[id], s.inflight[id], s.cursor = now.Add(interval), true, id
		return id, true
	}
	return "", false
}

type membershipPollResult struct {
	id     peer.ID
	result membershipReadResult
	err    error
}

type membershipPollJob struct {
	id    peer.ID
	known *membershipPeerView
}

func (m *membershipService) poll(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	var workers sync.WaitGroup
	defer func() { cancel(); workers.Wait() }()
	jobs := make(chan membershipPollJob, membershipWorkers)
	results := make(chan membershipPollResult, membershipWorkers)
	for range membershipWorkers {
		workers.Go(func() {
			for {
				select {
				case <-ctx.Done():
					return
				case job := <-jobs:
					result, err := m.read(ctx, job.id, job.known)
					select {
					case results <- membershipPollResult{id: job.id, result: result, err: err}:
					case <-ctx.Done():
						return
					}
				}
			}
		})
	}
	var schedule membershipPollSchedule
	ticker := time.NewTicker(m.opts.pollTick)
	defer ticker.Stop()
	for {
		now := time.Now()
		peers := m.node.host.Network().Peers()
		slices.Sort(peers)
		if len(peers) > m.node.maxPeers {
			peers = peers[:m.node.maxPeers]
		}
		eligible := make([]peer.ID, 0, len(peers))
		for _, id := range peers {
			if supported, err := m.node.host.Peerstore().SupportsProtocols(id, MembershipProtocol); err == nil && len(supported) != 0 {
				eligible = append(eligible, id)
			}
		}
		selected := m.selectPeers(&schedule, eligible, now)
		m.expire(selected, now)
		for {
			id, ok := schedule.take(selected, now, m.opts.pollInterval)
			if !ok {
				break
			}
			jobs <- membershipPollJob{id: id, known: m.known(id, now)} // Queued plus running jobs never exceed membershipWorkers.
		}
		select {
		case <-ctx.Done():
			return
		case <-m.wake:
		case <-ticker.C:
		case result := <-results:
			delete(schedule.inflight, result.id)
			if result.err == nil && result.result.view != nil && schedule.selected[result.id] && m.node.host.Network().Connectedness(result.id) == network.Connected {
				if result.result.unchanged {
					m.refresh(result.id, result.result.hash, time.Now())
				} else {
					m.rememberResult(result.id, result.result, time.Now())
				}
			}
		}
	}
}

func (m *membershipService) selectPeers(schedule *membershipPollSchedule, eligible []peer.ID, now time.Time) []peer.ID {
	rotate := schedule.started && len(eligible) > membershipPeerLimit && !now.Before(schedule.rotated.Add(m.opts.rotationInterval)) && m.used.Swap(false)
	return schedule.reconcile(eligible, now, rotate)
}

func (m *membershipService) expire(connected []peer.ID, now time.Time) {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()
	for id, record := range m.cache {
		if _, ok := slices.BinarySearch(connected, id); !ok || !now.Before(record.updated.Add(m.opts.ttl)) {
			m.bytes -= record.bytes
			delete(m.cache, id)
		}
	}
}

func (m *membershipService) known(id peer.ID, now time.Time) *membershipPeerView {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()
	record, ok := m.cache[id]
	if !ok || !now.Before(record.updated.Add(m.opts.ttl)) {
		return nil
	}
	return &record
}

// A conditional reply can refresh only the exact, still-live cached view that
// was sent with its request. It cannot resurrect an expired or evicted entry.
func (m *membershipService) refresh(id peer.ID, hash [32]byte, now time.Time) bool {
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()
	record, ok := m.cache[id]
	if !ok || record.hash != hash || !now.Before(record.updated.Add(m.opts.ttl)) {
		return false
	}
	record.updated = now
	m.cache[id] = record
	return true
}

func (m *membershipService) rememberResult(id peer.ID, result membershipReadResult, now time.Time) bool {
	view, size := result.view, result.bytes
	if view == nil || size <= 0 || size > membership.MaxWireBytes {
		return false
	}
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()
	previous := m.cache[id]
	if !membership.CanReplace(previous.view, view) {
		return false
	}
	if m.cache == nil {
		m.cache = make(map[peer.ID]membershipPeerView)
	}
	if previous.view != nil {
		m.bytes -= previous.bytes
		delete(m.cache, id)
	}
	for len(m.cache) >= membershipPeerLimit || m.bytes+size > membershipByteLimit {
		var oldest peer.ID
		var updated time.Time
		for candidate, record := range m.cache {
			if oldest == "" || record.updated.Before(updated) || (record.updated.Equal(updated) && candidate < oldest) {
				oldest, updated = candidate, record.updated
			}
		}
		m.bytes -= m.cache[oldest].bytes
		delete(m.cache, oldest)
	}
	m.cache[id] = membershipPeerView{view: view, bytes: size, updated: now, hash: result.hash}
	m.bytes += size
	return true
}
