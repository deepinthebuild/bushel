package publication

import (
	"bytes"
	"cmp"
	"errors"
	"slices"

	"github.com/deepinthebuild/bushel/internal/cas"
	publicationpb "github.com/deepinthebuild/bushel/internal/publication/pb"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maxSnapshotBytes = 16 << 20

// The worker owns s throughout encoding, so protobuf byte fields can borrow
// its fixed-size hashes without copying. No generated message escapes encoding.
func snapshotMessage(s *state) (*publicationpb.Snapshot, error) {
	if len(s.Jobs) > maxRecords || s.Clock < 0 || s.Window < 0 || s.Bytes < 0 || s.Offers < 0 || s.NextStart < 0 {
		return nil, errors.New("invalid publication state for encoding")
	}
	v := &publicationpb.Snapshot{Version: 1, Clock: &timestamppb.Timestamp{Seconds: s.Clock}, Window: &timestamppb.Timestamp{Seconds: s.Window},
		ChargedBytes: uint64(s.Bytes), ChargedOffers: uint32(s.Offers), NextStart: &timestamppb.Timestamp{Seconds: s.NextStart},
		Expired: s.Expired, Jobs: make([]*publicationpb.Job, 0, len(s.Jobs))}
	for bucket, charged := range s.Served {
		if charged != 0 {
			v.Served = append(v.Served, &publicationpb.ServiceCredit{Bucket: uint32(bucket), ChargedBytes: charged})
		}
	}
	for _, j := range s.Jobs {
		r := &publicationpb.Job{Digest: digestMessage(j.Digest), Created: &timestamppb.Timestamp{Seconds: j.Created}, Due: &timestamppb.Timestamp{Seconds: j.Due}, Ready: j.Ready}
		for kind := range j.Partitions {
			if j.Kinds&(1<<uint(kind)) != 0 {
				r.Associations = append(r.Associations, &publicationpb.Association{Kind: publicationpb.WorkspaceKind(kind), PartitionSha256: j.Partitions[kind][:]})
			}
		}
		for _, id := range j.Attempts {
			r.AttemptedPeers = append(r.AttemptedPeers, &publicationpb.PeerID{Multihash: []byte(id)})
		}
		for _, id := range j.Completed {
			r.CompletedPeers = append(r.CompletedPeers, &publicationpb.PeerID{Multihash: []byte(id)})
		}
		v.Jobs = append(v.Jobs, r)
	}
	slices.SortFunc(v.Jobs, func(a, b *publicationpb.Job) int { return compareDigestMessages(a.Digest, b.Digest) })
	return v, nil
}

func digestMessage(d cas.Digest) *publicationpb.Digest {
	return &publicationpb.Digest{Sha256: d.Hash[:], SizeBytes: uint64(d.Size)}
}

func compareDigestMessages(a, b *publicationpb.Digest) int {
	if c := bytes.Compare(a.Sha256, b.Sha256); c != 0 {
		return c
	}
	return cmp.Compare(a.SizeBytes, b.SizeBytes)
}

func compareDigests(a, b cas.Digest) int {
	if c := bytes.Compare(a.Hash[:], b.Hash[:]); c != 0 {
		return c
	}
	return cmp.Compare(a.Size, b.Size)
}

func marshalState(s *state) ([]byte, error) {
	v, err := snapshotMessage(s)
	if err != nil {
		return nil, err
	}
	if proto.Size(v) > maxSnapshotBytes {
		return nil, errors.New("publication snapshot exceeds limit")
	}
	return (proto.MarshalOptions{Deterministic: true}).Marshal(v)
}

func parseState(b []byte) (*state, error) {
	if len(b) > maxSnapshotBytes {
		return nil, errors.New("publication snapshot exceeds limit")
	}
	if err := checkProtoBounds(b, snapshotWire); err != nil {
		return nil, err
	}
	var v publicationpb.Snapshot
	if err := (proto.UnmarshalOptions{RecursionLimit: 8}).Unmarshal(b, &v); err != nil {
		return nil, err
	}
	return restoreSnapshot(&v)
}

func restoreDigest(v *publicationpb.Digest) (cas.Digest, error) {
	var d cas.Digest
	if v == nil || len(v.Sha256) != len(d.Hash) || v.SizeBytes > uint64(cas.MaxBlobSize) {
		return d, errors.New("invalid publication digest")
	}
	copy(d.Hash[:], v.Sha256)
	d.Size = int64(v.SizeBytes)
	return d, d.Validate()
}

func validTimestamp(v *timestamppb.Timestamp) bool {
	return v != nil && v.Seconds >= 0 && v.Seconds <= 253402300799 && v.Nanos == 0
}

func restoreSnapshot(v *publicationpb.Snapshot) (*state, error) {
	if v == nil || v.Version != 1 || !validTimestamp(v.Clock) || !validTimestamp(v.Window) || !validTimestamp(v.NextStart) ||
		v.Clock.Seconds > 253402214399 || v.Window.Seconds > v.Clock.Seconds || v.Clock.Seconds-v.Window.Seconds >= 3600 ||
		v.ChargedOffers > maxOffersPerHour || v.ChargedBytes > uint64(cas.MaxBlobSize)*uint64(v.ChargedOffers) ||
		v.NextStart.Seconds > v.Clock.Seconds+1 || len(v.Served) > fairBuckets || len(v.Jobs) > maxRecords {
		return nil, errors.New("invalid publication snapshot header")
	}
	s := &state{Expired: v.Expired, Clock: v.Clock.Seconds, Window: v.Window.Seconds, Bytes: int64(v.ChargedBytes),
		Offers: int(v.ChargedOffers), NextStart: v.NextStart.Seconds, Jobs: make(map[cas.Digest]*job, len(v.Jobs))}
	var previousBucket uint32
	for i, credit := range v.Served {
		if credit == nil || credit.Bucket >= fairBuckets || credit.ChargedBytes == 0 || i > 0 && credit.Bucket <= previousBucket {
			return nil, errors.New("invalid publication service credit")
		}
		s.Served[credit.Bucket] = credit.ChargedBytes
		previousBucket = credit.Bucket
	}
	var previous cas.Digest
	for i, r := range v.Jobs {
		if r == nil {
			return nil, errors.New("missing publication job")
		}
		d, err := restoreDigest(r.Digest)
		if err != nil || i > 0 && compareDigests(previous, d) >= 0 || !validTimestamp(r.Created) || !validTimestamp(r.Due) ||
			r.Created.Seconds > s.Clock || r.Due.Seconds < r.Created.Seconds || r.Due.Seconds > s.Clock+3600 ||
			len(r.Associations) == 0 || len(r.Associations) > 4 || len(r.AttemptedPeers) > maxAttempts || len(r.CompletedPeers) > ReplicaTarget {
			return nil, errors.New("invalid publication job")
		}
		previous = d
		j := &job{Digest: d, Created: r.Created.Seconds, Due: r.Due.Seconds, Ready: r.Ready}
		var previousKind publicationpb.WorkspaceKind
		for i, a := range r.Associations {
			if a == nil || a.Kind < 0 || a.Kind >= publicationpb.WorkspaceKind(len(j.Partitions)) || i > 0 && a.Kind <= previousKind || len(a.PartitionSha256) != 32 {
				return nil, errors.New("invalid workspace association")
			}
			copy(j.Partitions[a.Kind][:], a.PartitionSha256)
			j.Kinds |= 1 << a.Kind
			previousKind = a.Kind
		}
		for _, encoded := range r.AttemptedPeers {
			if encoded == nil || len(encoded.Multihash) > 128 {
				return nil, errors.New("invalid publication peer")
			}
			id, err := peer.IDFromBytes(encoded.Multihash)
			if err != nil {
				return nil, errors.New("invalid publication peer")
			}
			count := 0
			for _, tried := range j.Attempts {
				if tried == id {
					count++
				}
			}
			if count >= maxPeerAttempts {
				return nil, errors.New("too many attempts for peer")
			}
			j.Attempts = append(j.Attempts, id)
		}
		for _, encoded := range r.CompletedPeers {
			// Receipts refer to validated attempts. Reuse their IDs instead of
			// allocating and decoding the same peer a second time.
			var id peer.ID
			if encoded == nil {
				return nil, errors.New("invalid completed peer")
			}
			for _, tried := range j.Attempts {
				if string(tried) == string(encoded.Multihash) {
					id = tried
					break
				}
			}
			if id == "" || slices.Contains(j.Completed, id) {
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
	return s, nil
}
