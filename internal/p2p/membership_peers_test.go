package p2p

import (
	"crypto/sha256"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/membership"
	"github.com/libp2p/go-libp2p/core/peer"
	"pgregory.net/rapid"
)

func (m *membershipService) remember(id peer.ID, view *membership.View, size int, now time.Time) bool {
	data, err := membership.Encode(view)
	if err != nil {
		return false
	}
	return m.rememberResult(id, membershipReadResult{view: view, bytes: size, hash: sha256.Sum256(data)}, now)
}

func membershipTestView(t membershipTestReporter, session byte, generation, sequence uint64, keys ...cas.Digest) *membership.View {
	t.Helper()
	v, err := membership.Build([16]byte{session}, generation, sequence, keys)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestMembershipCohortIdleStabilityAndActiveFairness(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		count := rapid.IntRange(33, 128).Draw(t, "connected peers")
		peers := make([]peer.ID, count)
		for i := range peers {
			peers[i] = peer.ID(fmt.Sprintf("peer%03d", i))
		}
		m := testMembershipService(t)
		var schedule membershipPollSchedule
		now := time.Unix(1, 0)
		initial := m.selectPeers(&schedule, peers, now)
		for range 48 {
			now = now.Add(m.opts.rotationInterval)
			if current := m.selectPeers(&schedule, peers, now); !slices.Equal(current, initial) {
				t.Fatal("idle cohort downloaded a new set of snapshots")
			}
		}
		seen := make(map[peer.ID]bool)
		for range (count+membershipPeerLimit-1)/membershipPeerLimit + 1 {
			m.used.Store(true)
			now = now.Add(m.opts.rotationInterval)
			cohort := m.selectPeers(&schedule, peers, now)
			if len(cohort) != membershipPeerLimit || m.used.Load() {
				t.Fatal("active rotation exceeded cohort bound or retained consumed activity")
			}
			for _, id := range cohort {
				seen[id] = true
			}
			// Every selected peer receives one slot before any can repeat. A
			// completion frees capacity; evicting a cache entry cannot reset due.
			polled := make(map[peer.ID]bool)
			for i := range len(cohort) {
				if i != 0 {
					now = now.Add(m.opts.pollInterval / membershipPeerLimit)
				}
				id, ok := schedule.take(cohort, now, m.opts.pollInterval)
				if !ok || polled[id] {
					t.Fatal("polling starved or duplicated a cohort member")
				}
				polled[id] = true
				delete(schedule.inflight, id)
			}
			if _, ok := schedule.take(cohort, now, m.opts.pollInterval); ok {
				t.Fatal("same peer polled before its interval")
			}
		}
		if len(seen) != len(peers) {
			t.Fatalf("active cohorts starved %d peers", len(peers)-len(seen))
		}
		removed := schedule.selectionCursor
		peers = slices.DeleteFunc(peers, func(id peer.ID) bool { return id == removed })
		cohort := m.selectPeers(&schedule, peers, now)
		if len(cohort) != membershipPeerLimit || slices.Contains(cohort, removed) {
			t.Fatal("disconnection did not immediately fill cohort vacancy")
		}
	})
}

func TestMembershipReconnectCannotResetAggregatePollBudget(t *testing.T) {
	var schedule membershipPollSchedule
	now := time.Unix(1, 0)
	for attempt := range membershipPollBurst + 10 {
		// Both reconnecting an identity and replacing it with a new identity
		// discard its per-peer cooldown, but neither replenishes shared credit.
		schedule.reconcile(nil, now, false)
		id := peer.ID(fmt.Sprintf("peer%d", attempt/2))
		peers := schedule.reconcile([]peer.ID{id}, now, false)
		started, ok := schedule.take(peers, now, time.Minute)
		if ok != (attempt < membershipPollBurst) {
			t.Fatalf("attempt %d: reconnect reset poll budget: started=%v", attempt, ok)
		}
		delete(schedule.inflight, started)
	}
	peers := schedule.reconcile([]peer.ID{"fresh"}, now, true)
	if _, ok := schedule.take(peers, now.Add(-time.Hour), time.Minute); ok {
		t.Fatal("backward time or cohort rotation granted poll credit")
	}
	refill := now.Add(time.Minute / membershipPeerLimit)
	if id, ok := schedule.take(peers, refill, time.Minute); !ok || id != "fresh" {
		t.Fatal("elapsed refill did not restore one poll opportunity")
	}
}

func TestMembershipPollBudgetBoundsChurnHistory(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		var schedule membershipPollSchedule
		start := time.Unix(1, 0)
		now, highWater := start, start
		started := 0
		for step := range rapid.IntRange(1, 200).Draw(t, "events") {
			now = now.Add(time.Duration(rapid.IntRange(-2000, 4000).Draw(t, "clock delta milliseconds")) * time.Millisecond)
			// The first request starts the budget clock, so keep that request
			// fixed and explore arbitrary forward/backward steps afterward.
			if step == 0 {
				now = start
			}
			if now.After(highWater) {
				highWater = now
			}
			schedule.reconcile(nil, now, false)
			id := peer.ID(fmt.Sprintf("peer%d", rapid.IntRange(0, 16).Draw(t, "identity")))
			peers := schedule.reconcile([]peer.ID{id}, now, rapid.Bool().Draw(t, "cohort rotation"))
			if id, ok := schedule.take(peers, now, time.Minute); ok {
				started++
				delete(schedule.inflight, id)
			}
			if started > membershipPollBurst+int(highWater.Sub(start)/(time.Minute/membershipPeerLimit)) {
				t.Fatal("identity/protocol churn or backward time exceeded aggregate starts")
			}
			if len(schedule.next) > 1 || len(schedule.selected) > 1 || len(schedule.inflight) != 0 {
				t.Fatal("departed identities left unbounded scheduler state")
			}
		}
	})
}

func TestMembershipPollBudgetRefreshesStableCohortBeforeTTL(t *testing.T) {
	m := testMembershipService(t)
	var schedule membershipPollSchedule
	peers := make([]peer.ID, membershipPeerLimit)
	for i := range peers {
		peers[i] = peer.ID(fmt.Sprintf("peer%03d", i))
	}
	start := time.Unix(1, 0)
	last := make(map[peer.ID]time.Time)
	for now := start; now.Before(start.Add(10 * time.Minute)); now = now.Add(m.opts.pollTick) {
		selected := m.selectPeers(&schedule, peers, now)
		for {
			id, ok := schedule.take(selected, now, m.opts.pollInterval)
			if !ok {
				break
			}
			last[id] = now
			delete(schedule.inflight, id) // Fast unchanged reply; no credit refund.
		}
		if !now.Before(start.Add(time.Minute)) && len(last) != membershipPeerLimit {
			t.Fatal("aggregate budget prevented initial cohort fill within a minute")
		}
		for _, polled := range last {
			if now.Sub(polled) > m.opts.pollInterval+m.opts.pollTick {
				t.Fatal("five-second tick and budget starved a stable peer")
			}
			if !now.Before(polled.Add(m.opts.ttl)) {
				t.Fatal("unchanged stable cohort expired due to aggregate budget")
			}
		}
	}
}

func TestMembershipPollSingleFlightAndRestart(t *testing.T) {
	m := testMembershipService(t)
	now := time.Unix(1, 0)
	var schedule membershipPollSchedule
	peers := m.selectPeers(&schedule, []peer.ID{"a", "b", "c"}, now)
	a, ok := schedule.take(peers, now, time.Minute)
	if !ok {
		t.Fatal("missing first worker")
	}
	b, ok := schedule.take(peers, now, time.Minute)
	if !ok || a == b {
		t.Fatal("missing second independent worker")
	}
	if _, ok := schedule.take(peers, now.Add(time.Hour), time.Minute); ok {
		t.Fatal("more than two simultaneous polls")
	}
	delete(schedule.inflight, a)
	c, ok := schedule.take(peers, now, time.Minute)
	if !ok || c == a || c == b {
		t.Fatal("pending peer was starved")
	}
	d := cas.Sum([]byte("hint"))
	old := membershipTestView(t, 1, 5, 9, d)
	restarted := membershipTestView(t, 2, 1, 0, d)
	if !m.remember("a", old, 100, now) || !m.remember("a", restarted, 100, now.Add(time.Second)) {
		t.Fatal("serialized poll did not accept restarted session")
	}
	regressed := membershipTestView(t, 2, 1, 0)
	if m.remember("a", regressed, 100, now.Add(2*time.Second)) {
		t.Fatal("same-session conflicting baseline replaced cached view")
	}
}

func TestMembershipConditionalReplyCannotResurrectCache(t *testing.T) {
	m := testMembershipService(t)
	now := time.Unix(1, 0)
	v := membershipTestView(t, 1, 1, 0, cas.Sum([]byte("hint")))
	if !m.remember("peer", v, 100, now) {
		t.Fatal("cache insert failed")
	}
	known := m.known("peer", now)
	if !m.refresh("peer", known.hash, now.Add(time.Second)) {
		t.Fatal("matching unchanged response did not refresh")
	}
	wrong := known.hash
	wrong[0] ^= 1
	if m.refresh("peer", wrong, now.Add(2*time.Second)) {
		t.Fatal("unrelated conditional token refreshed view")
	}
	if m.refresh("peer", known.hash, now.Add(m.opts.ttl+time.Second)) {
		t.Fatal("late unchanged response revived expired view")
	}
	m.expire([]peer.ID{"peer"}, now.Add(m.opts.ttl+time.Second))
	if m.refresh("peer", known.hash, now.Add(m.opts.ttl+2*time.Second)) || len(m.cache) != 0 || m.bytes != 0 {
		t.Fatal("unchanged response recreated removed cache entry")
	}
	if !m.remember("peer", v, 100, now) {
		t.Fatal("cache insert failed")
	}
	m.expire(nil, now)
	if m.refresh("peer", known.hash, now) {
		t.Fatal("disconnected or rotated peer recreated cached view")
	}
}

func TestMembershipCacheBoundsAndTTL(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		m := testMembershipService(t)
		v := membershipTestView(t, 1, 1, 0, cas.Sum([]byte("hint")))
		now := time.Unix(1, 0)
		for range rapid.IntRange(33, 100).Draw(t, "updates") {
			id := peer.ID(fmt.Sprintf("peer%03d", rapid.IntRange(0, 63).Draw(t, "peer")))
			size := rapid.IntRange(1, membership.MaxWireBytes).Draw(t, "encoded bytes")
			if !m.remember(id, v, size, now) {
				t.Fatal("valid cache update rejected")
			}
			now = now.Add(time.Millisecond)
			if len(m.cache) > membershipPeerLimit || m.bytes > membershipByteLimit {
				t.Fatal("cache resource bound exceeded")
			}
			bytes := 0
			for _, record := range m.cache {
				bytes += record.bytes
			}
			if bytes != m.bytes {
				t.Fatal("cache byte accounting drifted")
			}
		}
		var peers []peer.ID
		for id := range m.cache {
			peers = append(peers, id)
		}
		slices.Sort(peers)
		m.expire(peers, now.Add(m.opts.ttl))
		if len(m.cache) != 0 || m.bytes != 0 {
			t.Fatal("TTL left stale views")
		}
	})
}
