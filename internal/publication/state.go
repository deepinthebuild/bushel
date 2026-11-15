// Package publication schedules optional, bounded replication of local uploads.
package publication

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"math"
	"slices"
	"strings"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/workspace"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	ReplicaTarget          = 2
	maxAttempts            = 8
	maxPeerAttempts        = 2
	maxRecords             = 8192
	maxPending             = 1024
	maxQueuedBytes   int64 = 20 << 30
	maxOffersPerHour       = 2048
	jobLifetime            = 24 * time.Hour
	maxFairWait            = time.Hour
	fairBuckets            = 256
)

// Policy selects local metadata classes. Content identity is independent of it.
type Policy uint8

func ParsePolicy(value string) (Policy, error) {
	if value == "off" {
		return 0, nil
	}
	if value == "all" {
		return 15, nil
	}
	var p Policy
	for _, value := range strings.Split(value, ",") {
		var kind workspace.Kind
		switch value {
		case "external":
			kind = workspace.Canonical
		case "main":
			kind = workspace.Main
		case "apparent":
			kind = workspace.Apparent
		case "unknown":
			kind = workspace.Unknown
		default:
			return 0, errors.New("publication policy must be off, all, or comma-separated external,main,apparent,unknown")
		}
		p |= 1 << uint(kind)
	}
	return p, nil
}

type job struct {
	Digest     cas.Digest
	Created    int64
	Due        int64
	Ready      bool
	Kinds      uint8
	Partitions [4][32]byte
	Attempts   []peer.ID
	Completed  []peer.ID
}

func (j *job) done(now int64) bool {
	return len(j.Completed) == ReplicaTarget || len(j.Attempts) == maxAttempts || now >= j.Created+int64(jobLifetime/time.Second)
}

func (j *job) partition(policy Policy) ([32]byte, bool) {
	var best [32]byte
	found := false
	for kind, partition := range j.Partitions {
		if j.Kinds&(1<<uint(kind)) != 0 && uint8(policy)&(1<<uint(kind)) != 0 && (!found || bytes.Compare(partition[:], best[:]) < 0) {
			best, found = partition, true
		}
	}
	return best, found
}

// state owns protocol decisions only. Time, peer candidates and observations
// are explicit inputs; disk and network operations are performed by Publisher.
type state struct {
	Expired          uint64
	Clock            int64
	Window           int64
	Bytes            int64
	Offers           int
	NextStart        int64
	Served           [fairBuckets]uint64
	Jobs             map[cas.Digest]*job
	running          *attempt
	runningRefreshed bool
}

type attempt struct {
	Digest cas.Digest
	Peer   peer.ID
}

func newState(now time.Time) *state {
	n := min(max(now.Unix(), 0), int64(253402214399))
	return &state{Clock: n, Window: n, Jobs: make(map[cas.Digest]*job)}
}

func (s *state) advance(now time.Time) {
	// A clock rollback cannot refund a budget or extend an observation's life.
	s.Clock = max(s.Clock, min(now.Unix(), int64(253402214399)))
	if s.Clock-s.Window >= int64(time.Hour/time.Second) {
		s.Window, s.Bytes, s.Offers = s.Clock, 0, 0
	}
	for d, j := range s.Jobs {
		if s.Clock >= j.Created+int64(jobLifetime/time.Second) && (s.running == nil || s.running.Digest != d) {
			if len(j.Completed) < ReplicaTarget && s.Expired < math.MaxUint64 {
				s.Expired++
			}
			delete(s.Jobs, d)
		}
	}
}

func (s *state) pending() (int, int64) {
	var count int
	var size int64
	for _, j := range s.Jobs {
		if !j.done(s.Clock) || (s.running != nil && s.running.Digest == j.Digest) {
			count++
			size += j.Digest.Size
		}
	}
	return count, size
}

// observe monotonically combines associations. Ready is set only by an
// explicit local completion or a positive local REAPI presence observation.
func (s *state) observe(d cas.Digest, a workspace.Association, ready bool) bool {
	if d.Validate() != nil || a.Kind > workspace.Apparent {
		return false
	}
	j := s.Jobs[d]
	if j == nil {
		count, size := s.pending()
		if len(s.Jobs) >= maxRecords || count >= maxPending || d.Size > maxQueuedBytes-size {
			return false
		}
		j = &job{Digest: d, Created: s.Clock, Due: s.Clock}
		s.Jobs[d] = j
	}
	bit := uint8(1 << uint(a.Kind))
	if j.Kinds&bit == 0 || bytes.Compare(a.Partition[:], j.Partitions[a.Kind][:]) < 0 {
		j.Partitions[a.Kind] = a.Partition
	}
	j.Kinds |= bit
	j.Ready = j.Ready || ready
	if ready && s.running != nil && s.running.Digest == d {
		// A later positive local observation supersedes the current source
		// probe if that probe eventually reports older missing content.
		s.runningRefreshed = true
	}
	return true
}

func peerRank(d cas.Digest, id peer.ID) [32]byte {
	h := sha256.New()
	h.Write([]byte("bushel-publication-peer\x00"))
	h.Write([]byte(d.Key()))
	h.Write([]byte{0})
	h.Write([]byte(id))
	var result [32]byte
	copy(result[:], h.Sum(nil))
	return result
}

func choosePeer(j *job, self peer.ID, candidates []peer.ID) peer.ID {
	var best peer.ID
	var rank [32]byte
	bestTries := maxPeerAttempts
	for _, id := range candidates {
		if id == "" || id == self || len(id) > 128 || slices.Contains(j.Completed, id) {
			continue
		}
		tries := 0
		for _, tried := range j.Attempts {
			if tried == id {
				tries++
			}
		}
		if tries >= maxPeerAttempts || tries > bestTries {
			continue
		}
		r := peerRank(j.Digest, id)
		if best == "" || tries < bestTries || bytes.Compare(r[:], rank[:]) < 0 {
			best, bestTries, rank = id, tries, r
		}
	}
	return best
}

// Eligible jobs waiting an hour enter oldest-due order before byte fairness.
// Due is persisted and moves on every dispatch, so declining retries cannot
// retain their original creation-time priority over work still awaiting a turn.
// This bounds competing turns, not wall time: unavailable peers and exhausted
// transfer/rate budgets still prevent dispatch.
func preferJob(candidate *job, score uint64, selected *job, selectedScore uint64, now int64) bool {
	if selected == nil {
		return true
	}
	aged := now-candidate.Due >= int64(maxFairWait/time.Second)
	selectedAged := now-selected.Due >= int64(maxFairWait/time.Second)
	if aged != selectedAged {
		return aged
	}
	if aged && candidate.Due != selected.Due {
		return candidate.Due < selected.Due
	}
	return score < selectedScore || score == selectedScore &&
		(candidate.Created < selected.Created || candidate.Created == selected.Created && candidate.Digest.Key() < selected.Digest.Key())
}

// start charges the complete possible transfer before dispatch. Persist this
// state before performing I/O. Failed/declined/interrupted attempts stay charged.
func (s *state) start(policy Policy, self peer.ID, candidates []peer.ID, hourlyBytes int64) *attempt {
	if s.running != nil || s.Clock < s.NextStart || s.Offers >= maxOffersPerHour {
		return nil
	}
	var selected *job
	var selectedPeer peer.ID
	var selectedBucket byte
	bestScore := uint64(math.MaxUint64)
	for _, j := range s.Jobs {
		partition, eligible := j.partition(policy)
		if !eligible || !j.Ready || j.done(s.Clock) || s.Clock < j.Due || j.Digest.Size > hourlyBytes-s.Bytes {
			continue
		}
		bucket := partition[0]
		cost := uint64(max(j.Digest.Size, 64<<10))
		score := uint64(math.MaxUint64)
		if cost <= math.MaxUint64-s.Served[bucket] {
			score = s.Served[bucket] + cost
		}
		if !preferJob(j, score, selected, bestScore, s.Clock) {
			continue
		}
		// Peer ranking is needed only when this job can improve the choice.
		id := choosePeer(j, self, candidates)
		if id == "" {
			continue
		}
		selected, selectedPeer, selectedBucket, bestScore = j, id, bucket, score
	}
	if selected == nil {
		return nil
	}
	selected.Attempts = append(selected.Attempts, selectedPeer)
	selected.Due = s.Clock + int64(5*(1<<min(len(selected.Attempts)-1, 6)))
	s.Bytes += selected.Digest.Size
	s.Offers++
	s.NextStart = s.Clock + 1
	s.Served[selectedBucket] = bestScore
	s.running = &attempt{Digest: selected.Digest, Peer: selectedPeer}
	s.runningRefreshed = false
	return s.running
}

func (s *state) finish(a attempt, complete, localMissing bool) {
	if s.running == nil || *s.running != a {
		return
	}
	j := s.Jobs[a.Digest]
	if complete && !slices.Contains(j.Completed, a.Peer) {
		j.Completed = append(j.Completed, a.Peer)
		j.Due = s.Clock
	}
	if localMissing && !s.runningRefreshed {
		j.Ready = false
	}
	s.running = nil
	s.runningRefreshed = false
}
