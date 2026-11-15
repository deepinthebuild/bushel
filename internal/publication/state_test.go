package publication

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"math"
	"slices"
	"testing"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/workspace"
	"github.com/libp2p/go-libp2p/core/peer"
	mh "github.com/multiformats/go-multihash"
	"pgregory.net/rapid"
)

func publicationDigest(id int, size int64) cas.Digest {
	if size == 0 {
		return cas.Sum(nil)
	}
	d := cas.Sum([]byte(fmt.Sprintf("publication-fixture-%d", id)))
	d.Size = size
	return d
}

// Public multihashes produce valid peer IDs without generating or persisting
// any private key material in fixtures.
func publicationPeer(id int) peer.ID {
	hash := sha256.Sum256([]byte(fmt.Sprintf("publication-peer-%d", id)))
	encoded, err := mh.Encode(hash[:], mh.SHA2_256)
	if err != nil {
		panic(err)
	}
	return peer.ID(encoded)
}

func association(partition byte, kind workspace.Kind) workspace.Association {
	return workspace.Association{Kind: kind, Partition: [32]byte{partition}}
}

func assertStateBounds(t testing.TB, s *state, self peer.ID) {
	t.Helper()
	count, size := s.pending()
	if len(s.Jobs) > maxRecords || count > maxPending || size > maxQueuedBytes || s.Offers > maxOffersPerHour || s.Bytes < 0 {
		t.Fatal("publication state exceeded its bounds")
	}
	for _, j := range s.Jobs {
		if len(j.Attempts) > maxAttempts || len(j.Completed) > ReplicaTarget {
			t.Fatal("attempt/receipt bound exceeded")
		}
		tries := make(map[peer.ID]int)
		for _, id := range j.Attempts {
			tries[id]++
			if id == self || tries[id] > maxPeerAttempts {
				t.Fatal("self or excessive per-peer attempts")
			}
		}
		seen := make(map[peer.ID]bool)
		for _, id := range j.Completed {
			if id == self || tries[id] == 0 || seen[id] {
				t.Fatal("receipt is not from a distinct attempted remote peer")
			}
			seen[id] = true
		}
	}
}

func TestPropertyPublicationHistoriesPreserveBudgetsAndReceipts(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		s := newState(time.Unix(100000, 0))
		self := publicationPeer(0)
		candidates := []peer.ID{self, publicationPeer(1), publicationPeer(1), publicationPeer(2), publicationPeer(3), publicationPeer(4)}
		digests := make([]cas.Digest, 8)
		for i := range digests {
			digests[i] = publicationDigest(i, int64(i+1)<<16)
		}
		for range rapid.IntRange(20, 100).Draw(t, "steps") {
			d := digests[rapid.IntRange(0, len(digests)-1).Draw(t, "digest")]
			switch rapid.IntRange(0, 4).Draw(t, "operation") {
			case 0:
				before, _ := marshalState(s)
				old := s.Jobs[d]
				var created, due int64
				var attempts, completed []peer.ID
				var ready bool
				if old != nil {
					created, due, attempts, completed, ready = old.Created, old.Due, slices.Clone(old.Attempts), slices.Clone(old.Completed), old.Ready
				}
				previousBytes, previousOffers := s.Bytes, s.Offers
				if !s.observe(d, association(byte(rapid.IntRange(0, 7).Draw(t, "partition")), workspace.Kind(rapid.IntRange(0, 3).Draw(t, "kind"))), rapid.Bool().Draw(t, "ready")) {
					after, _ := marshalState(s)
					if !bytes.Equal(before, after) {
						t.Fatal("rejected observation changed state")
					}
				}
				if previousBytes != s.Bytes || previousOffers != s.Offers {
					t.Fatal("observation reset charged budgets")
				}
				if old != nil && (old.Created != created || old.Due != due || !slices.Equal(old.Attempts, attempts) || !slices.Equal(old.Completed, completed) || ready && !old.Ready) {
					t.Fatal("duplicate observation reset publication progress or deadline")
				}
			case 1:
				previousBytes, previousOffers := s.Bytes, s.Offers
				active := s.running
				a := s.start(15, self, candidates, 8<<20)
				if a == nil {
					if s.Bytes != previousBytes || s.Offers != previousOffers {
						t.Fatal("idle start charged an attempt")
					}
				} else if active != nil || a.Peer == self || !slices.Contains(candidates, a.Peer) || s.Bytes != previousBytes+a.Digest.Size || s.Offers != previousOffers+1 || s.Bytes > 8<<20 {
					t.Fatal("dispatch did not charge exactly one bounded remote attempt")
				}
			case 2:
				if s.running != nil {
					a := *s.running
					complete := rapid.Bool().Draw(t, "verified receipt")
					s.finish(a, complete, !complete && rapid.Bool().Draw(t, "local eviction"))
					before, _ := marshalState(s)
					s.finish(a, true, false)
					after, _ := marshalState(s)
					if !bytes.Equal(before, after) {
						t.Fatal("duplicate completion changed progress")
					}
				}
			case 3:
				clock, chargedBytes, offers := s.Clock, s.Bytes, s.Offers
				delta := rapid.SampledFrom([]int64{-100, -1, 0, 1, 5, 30, 3600, 86400}).Draw(t, "clock movement")
				s.advance(time.Unix(clock+delta, 0))
				if s.Clock < clock || delta <= 0 && (s.Bytes != chargedBytes || s.Offers != offers) {
					t.Fatal("clock rollback refunded budget")
				}
			case 4:
				encoded, err := marshalState(s)
				if err != nil {
					t.Fatal(err)
				}
				restored, err := parseState(encoded)
				if err != nil {
					t.Fatal(err)
				}
				again, err := marshalState(restored)
				if err != nil || !bytes.Equal(encoded, again) || restored.running != nil {
					t.Fatal("restart changed persisted charge or progress")
				}
				s = restored
			}
			// These assertions do not select the next job: they constrain every
			// possible history, regardless of the scheduler's tie-breaking.
			count, size := s.pending()
			if count > maxPending || size > maxQueuedBytes || s.Offers > maxOffersPerHour {
				t.Fatal("queue/budget limit exceeded")
			}
			for _, j := range s.Jobs {
				counts := make(map[peer.ID]int)
				for _, id := range j.Attempts {
					counts[id]++
					if id == self || counts[id] > maxPeerAttempts {
						t.Fatal("invalid attempt history")
					}
				}
				seen := make(map[peer.ID]bool)
				for _, id := range j.Completed {
					if seen[id] || counts[id] == 0 || id == self {
						t.Fatal("invalid receipt history")
					}
					seen[id] = true
				}
				if len(j.Attempts) > maxAttempts || len(j.Completed) > ReplicaTarget {
					t.Fatal("history bounds exceeded")
				}
			}
		}
	})
}

func TestTwoDistinctRemoteReceiptsCompleteOneJob(t *testing.T) {
	s := newState(time.Unix(100000, 0))
	d, self, remote := publicationDigest(1, 128), publicationPeer(0), publicationPeer(1)
	s.observe(d, association(1, workspace.Canonical), true)
	if s.start(15, self, []peer.ID{self, self}, 1024) != nil {
		t.Fatal("self counted as a replica")
	}
	a := s.start(15, self, []peer.ID{remote, remote, self}, 1024)
	if a == nil {
		t.Fatal("first remote was not offered")
	}
	s.finish(*a, true, false)
	s.finish(*a, true, false)
	s.advance(time.Unix(s.Clock+10, 0))
	if s.start(15, self, []peer.ID{remote, remote}, 1024) != nil || len(s.Jobs[d].Completed) != 1 {
		t.Fatal("duplicate remote completed the target")
	}
	a = s.start(15, self, []peer.ID{remote, publicationPeer(2)}, 1024)
	if a == nil {
		t.Fatal("second distinct remote was not offered")
	}
	s.finish(*a, true, false)
	s.advance(time.Unix(s.Clock+10, 0))
	if s.start(15, self, []peer.ID{publicationPeer(3)}, 1024) != nil || !s.Jobs[d].done(s.Clock) {
		t.Fatal("completed job was dispatched again")
	}
	assertStateBounds(t, s, self)
}

func TestCandidatePermutationDoesNotChangePlacement(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		j := &job{Digest: publicationDigest(1, 1)}
		candidates := []peer.ID{publicationPeer(0), publicationPeer(1), publicationPeer(2), publicationPeer(3)}
		want := choosePeer(j, candidates[0], candidates)
		for i := range candidates {
			other := rapid.IntRange(0, len(candidates)-1).Draw(t, "swap")
			candidates[i], candidates[other] = candidates[other], candidates[i]
		}
		candidates = append(candidates, candidates...)
		if got := choosePeer(j, publicationPeer(0), candidates); got != want {
			t.Fatal("candidate ordering/duplicates changed stable placement")
		}
	})
}

func TestByteFairnessLetsLargePartitionMakeProgress(t *testing.T) {
	s := newState(time.Unix(100000, 0))
	self := publicationPeer(0)
	peers := []peer.ID{publicationPeer(1), publicationPeer(2)}
	large := publicationDigest(999, 1<<20)
	s.observe(large, association(2, workspace.Canonical), true)
	for i := range 64 {
		s.observe(publicationDigest(i, 1<<16), association(1, workspace.Canonical), true)
	}
	var smallBytes int64
	for range 40 {
		a := s.start(15, self, peers, maxQueuedBytes)
		if a == nil {
			t.Fatal("fairness fixture unexpectedly idle")
		}
		if a.Digest == large {
			if smallBytes < large.Size-(1<<16) || smallBytes > large.Size+(1<<16) {
				t.Fatalf("byte fairness: small partition served %d before large %d", smallBytes, large.Size)
			}
			return
		}
		smallBytes += a.Digest.Size
		s.finish(*a, true, false)
		s.advance(time.Unix(s.Clock+5, 0))
		// Continually arriving small work cannot starve an eligible large blob.
		s.observe(publicationDigest(1000+int(s.Clock), 1<<16), association(1, workspace.Canonical), true)
	}
	t.Fatal("continuous small work starved the large partition")
}

func TestPublicationCapacityAndFixedLifetime(t *testing.T) {
	now := time.Unix(100000, 0)
	s := newState(now)
	for i := range maxPending {
		if !s.observe(publicationDigest(i, 1), association(1, workspace.Canonical), false) {
			t.Fatal("pending bound rejected too early")
		}
	}
	if s.observe(publicationDigest(maxPending, 1), association(1, workspace.Canonical), true) {
		t.Fatal("pending bound exceeded")
	}
	first := publicationDigest(0, 1)
	created := s.Jobs[first].Created
	s.advance(now.Add(jobLifetime - time.Second))
	for range 20 {
		s.observe(first, association(2, workspace.Main), true)
	}
	if s.Jobs[first].Created != created {
		t.Fatal("duplicate observation extended lifetime")
	}
	s.advance(now.Add(jobLifetime))
	if len(s.Jobs) != 0 {
		t.Fatal("fixed lifetime did not expire idle records")
	}
	for i, size := range []int64{cas.MaxBlobSize, cas.MaxBlobSize, maxQueuedBytes - 2*cas.MaxBlobSize} {
		if !s.observe(publicationDigest(i, size), association(1, workspace.Canonical), false) {
			t.Fatal("byte bound rejected too early")
		}
	}
	if s.observe(publicationDigest(999, 1), association(1, workspace.Canonical), true) {
		t.Fatal("queued byte bound exceeded")
	}
	if count, size := s.pending(); count != 3 || size != maxQueuedBytes {
		t.Fatalf("queue accounting: %d %d", count, size)
	}
}

func TestInterruptedAttemptStaysChargedAfterRestartAndRollback(t *testing.T) {
	s := newState(time.Unix(100000, 0))
	d := publicationDigest(1, 1024)
	s.observe(d, association(1, workspace.Canonical), true)
	a := s.start(15, publicationPeer(0), []peer.ID{publicationPeer(1)}, 1024)
	if a == nil {
		t.Fatal("fixture did not dispatch")
	}
	b, err := marshalState(s)
	if err != nil {
		t.Fatal(err)
	}
	s, err = parseState(b)
	if err != nil {
		t.Fatal(err)
	}
	s.advance(time.Unix(1, 0))
	if s.Bytes != 1024 || s.Offers != 1 || len(s.Jobs[d].Attempts) != 1 || s.running != nil {
		t.Fatal("restart or rollback refunded interrupted attempt")
	}
	s.advance(time.Unix(s.Clock+10, 0))
	if s.start(15, publicationPeer(0), []peer.ID{publicationPeer(2)}, 1024) != nil {
		t.Fatal("spent byte budget was reused")
	}
	s.advance(time.Unix(s.Window+3600, 0))
	if s.start(15, publicationPeer(0), []peer.ID{publicationPeer(2)}, 1024) == nil {
		t.Fatal("new hourly window did not restore its budget")
	}
}

func TestPersistedFairnessCounterCannotWrapPriority(t *testing.T) {
	s := newState(time.Unix(100000, 0))
	overflow := publicationDigest(1, 1)
	other := publicationDigest(2, 1)
	s.observe(overflow, association(1, workspace.Canonical), true)
	s.observe(other, association(2, workspace.Canonical), true)
	s.Served[1] = math.MaxUint64
	encoded, err := marshalState(s)
	if err != nil {
		t.Fatal(err)
	}
	s, err = parseState(encoded)
	if err != nil {
		t.Fatal(err)
	}
	a := s.start(15, publicationPeer(0), []peer.ID{publicationPeer(1)}, 1024)
	if a == nil || a.Digest != other || s.Served[1] != math.MaxUint64 {
		t.Fatal("persisted counter overflow granted priority to an overserved partition")
	}
}

func TestAgingDispatchesLargeBlobUnderDecliningSmallJobLoad(t *testing.T) {
	now := time.Unix(100000, 0)
	s := newState(now)
	self := publicationPeer(0)
	peers := []peer.ID{publicationPeer(1), publicationPeer(2), publicationPeer(3), publicationPeer(4)}
	large := publicationDigest(-1, 256<<20)
	if !s.observe(large, association(255, workspace.Canonical), true) {
		t.Fatal("large fixture rejected")
	}
	inserted := 0
	refill := func() {
		count, _ := s.pending()
		for count < maxPending && len(s.Jobs) < maxRecords {
			d := publicationDigest(inserted, 1)
			if !s.observe(d, association(byte(inserted%32), workspace.Canonical), true) {
				t.Fatal("small fixture exceeded configured capacity")
			}
			inserted++
			count++
		}
	}
	refill()
	if count, _ := s.pending(); count != maxPending {
		t.Fatal("fixture did not fill real pending bound")
	}
	// At the first hourly reset, an aged blob has at most 1,023 other jobs
	// ahead of it. Each of their declined retries must yield its aged priority.
	deadline := now.Add(maxFairWait + time.Duration(maxPending)*time.Second)
	for s.Clock <= deadline.Unix() {
		a := s.start(15, self, peers, 1<<30)
		if a == nil {
			if s.Offers == maxOffersPerHour {
				s.advance(time.Unix(s.Window+3600, 0))
			} else {
				s.advance(time.Unix(s.Clock+1, 0))
			}
			continue
		}
		if a.Digest == large {
			if s.Clock < now.Add(maxFairWait).Unix() {
				t.Fatal("fixture did not require aging to overcome byte score")
			}
			if s.Bytes > 1<<30 || s.Offers > maxOffersPerHour || len(s.Jobs) > maxRecords {
				t.Fatal("aging bypassed resource limits")
			}
			if len(s.Jobs[large].Attempts) != 1 || s.Expired != 0 {
				t.Fatal("large blob expired or consumed phantom attempts")
			}
			t.Logf("256 MiB blob dispatched after %s with %d retained records and %d small arrivals", time.Duration(s.Clock-now.Unix())*time.Second, len(s.Jobs), inserted)
			return
		}
		s.finish(*a, false, false)
		s.advance(time.Unix(s.Clock+1, 0))
		refill()
	}
	t.Fatal("declining small jobs starved large blob past bounded aged turns")
}

func TestPropertyAgedJobsGetATurnBeforeFreshRetries(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		s := newState(time.Unix(100000, 0))
		count := rapid.IntRange(2, 48).Draw(t, "jobs")
		for i := range count {
			size := rapid.Int64Range(1, 8<<20).Draw(t, "size")
			s.observe(publicationDigest(i, size), association(byte(rapid.IntRange(0, 31).Draw(t, "bucket")), workspace.Canonical), true)
		}
		s.advance(time.Unix(s.Clock+int64(maxFairWait/time.Second), 0))
		seen := make(map[cas.Digest]bool)
		for range count {
			a := s.start(15, publicationPeer(0), []peer.ID{publicationPeer(1), publicationPeer(2)}, maxQueuedBytes)
			if a == nil {
				t.Fatal("aged eligible work was not dispatched")
			}
			if seen[a.Digest] {
				t.Fatal("declined retry overtook a job still waiting for its first aged turn")
			}
			seen[a.Digest] = true
			s.finish(*a, false, false)
			s.advance(time.Unix(s.Clock+1, 0))
		}
	})
}

func TestRetryAgingUsesPersistedDueInsteadOfOriginalCreation(t *testing.T) {
	now := time.Unix(100000, 0)
	s := newState(now)
	small, large := publicationDigest(1, 1), publicationDigest(2, 256<<20)
	s.observe(small, association(1, workspace.Canonical), true)
	s.advance(now.Add(maxFairWait))
	s.observe(large, association(2, workspace.Canonical), true)
	s.advance(now.Add(2 * maxFairWait))
	peers := []peer.ID{publicationPeer(1), publicationPeer(2)}
	a := s.start(15, publicationPeer(0), peers, 1<<30)
	if a == nil || a.Digest != small {
		t.Fatal("oldest eligible job lost its aged turn")
	}
	s.finish(*a, false, false)
	s.advance(time.Unix(s.Jobs[small].Due, 0))
	// Both are eligible again. The small retry retains older Created but its
	// newly persisted Due yields priority to the large job that kept waiting.
	a = s.start(15, publicationPeer(0), peers, 1<<30)
	if a == nil || a.Digest != large {
		t.Fatal("retry retained creation-time priority over aged work")
	}
}

func TestNewPositiveObservationSupersedesRunningMissingProbe(t *testing.T) {
	s := newState(time.Unix(100000, 0))
	d := publicationDigest(1, 1024)
	label := association(1, workspace.Canonical)
	s.observe(d, label, true)
	peers := []peer.ID{publicationPeer(1), publicationPeer(2)}
	a := s.start(15, publicationPeer(0), peers, 1<<20)
	if a == nil {
		t.Fatal("fixture did not dispatch")
	}
	// This is intentionally identical metadata and an already-true Ready bit.
	// A newer explicit commit still supersedes the older source probe result.
	s.observe(d, label, true)
	s.finish(*a, false, true)
	if !s.Jobs[d].Ready || s.Bytes != d.Size || s.Offers != 1 {
		t.Fatal("stale negative cleared newer intent or refunded the attempt")
	}
	s.advance(time.Unix(s.Jobs[d].Due, 0))
	a = s.start(15, publicationPeer(0), peers, 1<<20)
	if a == nil {
		t.Fatal("newer commit did not allow retry")
	}
	s.finish(*a, false, true)
	if s.Jobs[d].Ready {
		t.Fatal("refresh from previous attempt protected a newer missing probe")
	}
}

func TestPropertyPositiveObservationSurvivesProbeCompletionOrdering(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		s := newState(time.Unix(100000, 0))
		d, other := publicationDigest(1, 1024), publicationDigest(2, 1024)
		aLabel := association(1, workspace.Canonical)
		s.observe(d, aLabel, true)
		a := s.start(15, publicationPeer(0), []peer.ID{publicationPeer(1)}, 1<<20)
		count := rapid.IntRange(0, 24).Draw(t, "observations")
		finishAt := rapid.IntRange(0, count).Draw(t, "probe completion position")
		positive := false
		for i := 0; i <= count; i++ {
			if i == finishAt {
				s.finish(*a, false, true)
			}
			if i == count {
				break
			}
			switch rapid.IntRange(0, 2).Draw(t, "observation kind") {
			case 0:
				s.observe(d, aLabel, true)
				positive = true
			case 1:
				s.observe(d, aLabel, false)
			case 2:
				s.observe(other, aLabel, true)
			}
		}
		if s.Jobs[d].Ready != positive {
			t.Fatal("completion ordering lost positive intent or unrelated metadata revived a missing blob")
		}
		if s.Bytes != d.Size || s.Offers != 1 || len(s.Jobs[d].Attempts) != 1 {
			t.Fatal("observation race reset the charged attempt")
		}
	})
}
