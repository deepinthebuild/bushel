package publication

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"slices"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/libp2p/go-libp2p/core/peer"
)

const maxSnapshotBytes = 16 << 20

type snapshot struct {
	Expired   uint64              `json:"expired"`
	Version   int                 `json:"version"`
	Clock     int64               `json:"clock"`
	Window    int64               `json:"window"`
	Bytes     int64               `json:"bytes"`
	Offers    int                 `json:"offers"`
	NextStart int64               `json:"next_start"`
	Served    [fairBuckets]uint64 `json:"served"`
	Jobs      []savedJob          `json:"jobs"`
}

type savedJob struct {
	Hash       string    `json:"hash"`
	Size       int64     `json:"size"`
	Created    int64     `json:"created"`
	Due        int64     `json:"due"`
	Ready      bool      `json:"ready"`
	Kinds      uint8     `json:"kinds"`
	Partitions [4]string `json:"partitions"`
	Attempts   []string  `json:"attempts"`
	Completed  []string  `json:"completed"`
}

func marshalState(s *state) ([]byte, error) {
	v := snapshot{Expired: s.Expired, Version: 1, Clock: s.Clock, Window: s.Window, Bytes: s.Bytes, Offers: s.Offers, NextStart: s.NextStart, Served: s.Served}
	for _, j := range s.Jobs {
		r := savedJob{Hash: j.Digest.Hex(), Size: j.Digest.Size, Created: j.Created, Due: j.Due, Ready: j.Ready, Kinds: j.Kinds}
		for kind, partition := range j.Partitions {
			r.Partitions[kind] = hex.EncodeToString(partition[:])
		}
		for _, id := range j.Attempts {
			r.Attempts = append(r.Attempts, id.String())
		}
		for _, id := range j.Completed {
			r.Completed = append(r.Completed, id.String())
		}
		v.Jobs = append(v.Jobs, r)
	}
	slices.SortFunc(v.Jobs, func(a, b savedJob) int {
		if a.Hash < b.Hash {
			return -1
		}
		if a.Hash > b.Hash {
			return 1
		}
		if a.Size < b.Size {
			return -1
		}
		if a.Size > b.Size {
			return 1
		}
		return 0
	})
	b, err := json.Marshal(v)
	if len(b) > maxSnapshotBytes {
		return nil, errors.New("publication snapshot exceeds limit")
	}
	return b, err
}

func parseState(b []byte) (*state, error) {
	if len(b) > maxSnapshotBytes {
		return nil, errors.New("publication snapshot exceeds limit")
	}
	if err := checkJSONBounds(b); err != nil {
		return nil, err
	}
	var v snapshot
	decoder := json.NewDecoder(bytes.NewReader(b))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&v); err != nil {
		return nil, err
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return nil, errors.New("trailing publication snapshot data")
	}
	// Bound counters before any additions or conversions; clock rollback is
	// handled conservatively after restoration, never by discarding the file.
	if v.Version != 1 || v.Clock < 0 || v.Clock > 253402214399 || v.Window < 0 || v.Window > v.Clock || v.Clock-v.Window >= 3600 || v.Bytes < 0 || v.Bytes > cas.MaxBlobSize*int64(v.Offers) || v.Offers < 0 || v.Offers > maxOffersPerHour || v.NextStart < 0 || v.NextStart > v.Clock+1 || len(v.Jobs) > maxRecords {
		return nil, errors.New("invalid publication snapshot header")
	}
	s := &state{Expired: v.Expired, Clock: v.Clock, Window: v.Window, Bytes: v.Bytes, Offers: v.Offers, NextStart: v.NextStart, Served: v.Served, Jobs: make(map[cas.Digest]*job, len(v.Jobs))}
	for _, r := range v.Jobs {
		d, err := cas.ParseDigest(r.Hash, r.Size)
		if err != nil || s.Jobs[d] != nil || r.Created < 0 || r.Created > v.Clock || r.Due < r.Created || r.Due > v.Clock+3600 || r.Kinds == 0 || r.Kinds > 15 || len(r.Attempts) > maxAttempts || len(r.Completed) > ReplicaTarget {
			return nil, errors.New("invalid publication job")
		}
		j := &job{Digest: d, Created: r.Created, Due: r.Due, Ready: r.Ready, Kinds: r.Kinds}
		for kind, encoded := range r.Partitions {
			if len(encoded) != 64 {
				return nil, errors.New("invalid workspace partition")
			}
			p, err := hex.DecodeString(encoded)
			if err != nil || hex.EncodeToString(p) != encoded {
				return nil, errors.New("invalid workspace partition")
			}
			copy(j.Partitions[kind][:], p)
			if r.Kinds&(1<<uint(kind)) == 0 && j.Partitions[kind] != ([32]byte{}) {
				return nil, errors.New("unexpected workspace partition")
			}
		}
		counts := make(map[peer.ID]int)
		for _, encoded := range r.Attempts {
			if len(encoded) > 128 {
				return nil, errors.New("invalid publication peer")
			}
			id, err := peer.Decode(encoded)
			if err != nil || id.String() != encoded {
				return nil, errors.New("invalid publication peer")
			}
			counts[id]++
			if counts[id] > maxPeerAttempts {
				return nil, errors.New("too many attempts for peer")
			}
			j.Attempts = append(j.Attempts, id)
		}
		for _, encoded := range r.Completed {
			if len(encoded) > 128 {
				return nil, errors.New("invalid completed peer")
			}
			id, err := peer.Decode(encoded)
			if err != nil || counts[id] == 0 || slices.Contains(j.Completed, id) {
				return nil, errors.New("invalid completed peer")
			}
			j.Completed = append(j.Completed, id)
		}
		s.Jobs[d] = j
	}
	count, size := s.pending()
	if count > maxPending || size > maxQueuedBytes {
		return nil, errors.New("publication queue exceeds limits")
	}
	canonical, err := marshalState(s)
	if err != nil || !bytes.Equal(canonical, b) {
		return nil, errors.New("noncanonical publication snapshot")
	}
	return s, nil
}

// Validate collection sizes before decoding a slice of job structs. A small
// JSON array of empty objects must not cause allocation for millions of jobs.
func checkJSONBounds(b []byte) error {
	type level struct {
		array bool
		count int
	}
	var stack []level
	d := json.NewDecoder(bytes.NewReader(b))
	for {
		token, err := d.Token()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if delim, ok := token.(json.Delim); ok && (delim == ']' || delim == '}') {
			if len(stack) == 0 {
				return errors.New("invalid snapshot nesting")
			}
			stack = stack[:len(stack)-1]
			continue
		}
		if len(stack) > 0 {
			level := &stack[len(stack)-1]
			level.count++
			if (level.array && level.count > maxRecords) || (!level.array && level.count > 64) {
				return errors.New("snapshot collection exceeds limit")
			}
		}
		if value, ok := token.(string); ok && len(value) > 512 {
			return errors.New("snapshot string exceeds limit")
		}
		if delim, ok := token.(json.Delim); ok {
			if len(stack) >= 5 {
				return errors.New("snapshot nesting exceeds limit")
			}
			stack = append(stack, level{array: delim == '['})
		}
	}
}
