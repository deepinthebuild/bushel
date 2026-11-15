package membership

import (
	"bytes"
	"slices"

	"github.com/FastFilter/xorfilter"
	"github.com/deepinthebuild/bushel/internal/cas"
	membershippb "github.com/deepinthebuild/bushel/internal/membership/pb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

const MaxRequestBytes = 34

// EncodeRequest sends the hash of a complete cached payload, rather than a
// generation alone: completeness or a tail can change within a generation.
func EncodeRequest(known *[32]byte) []byte {
	if known == nil {
		return nil
	}
	// This is the canonical encoding of Request. Keeping the fixed shape
	// explicit makes its allocation bound independent of protobuf machinery.
	return protowire.AppendBytes(protowire.AppendTag(nil, 1, protowire.BytesType), known[:])
}

func DecodeRequest(data []byte) (*[32]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}
	if len(data) != MaxRequestBytes || data[0] != 0x0a || data[1] != 32 {
		return nil, ErrInvalid
	}
	var p membershippb.Request
	if err := proto.Unmarshal(data, &p); err != nil || len(p.KnownSnapshotSha256) != 32 {
		return nil, ErrInvalid
	}
	known := [32]byte(p.KnownSnapshotSha256)
	return &known, nil
}

// Encode returns the deterministic protobuf payload. Stream framing belongs to
// the transport; it must reject lengths above MaxWireBytes before allocation.
func Encode(v *View) ([]byte, error) {
	if v == nil {
		return nil, ErrInvalid
	}
	p := &membershippb.Snapshot{
		SessionId: v.session[:], Generation: v.generation,
		SnapshotSequence: v.snapshotSequence, Sequence: v.sequence,
		Complete: v.complete, KeyAlgorithm: membershippb.KeyAlgorithm_KEY_ALGORITHM_SHA256_SIZE64BE_PREFIX64,
		KeyCount: v.keyCount,
	}
	if v.filter != nil {
		p.Filter = &membershippb.BinaryFuse8{Seed: v.filter.Seed,
			SegmentLength: v.filter.SegmentLength, SegmentCount: v.filter.SegmentCount,
			Fingerprints: v.filter.Fingerprints}
	}
	for _, d := range v.tail {
		p.Additions = append(p.Additions, &membershippb.Digest{Sha256: slices.Clone(d.Hash[:]), SizeBytes: uint64(d.Size)})
	}
	if proto.Size(p) > MaxWireBytes {
		return nil, ErrInvalid
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(p)
}

// Decode rejects unknown fields, duplicates, oversized collections and invalid
// filter geometry before exposing a view. The preflight limits allocations made
// by generated protobuf unmarshalling, including tiny repeated submessages.
func Decode(data []byte) (*View, error) {
	if len(data) > MaxWireBytes || !preflight(data, snapshotShape) {
		return nil, ErrInvalid
	}
	var p membershippb.Snapshot
	if err := (proto.UnmarshalOptions{RecursionLimit: 4}).Unmarshal(data, &p); err != nil {
		return nil, ErrInvalid
	}
	if len(p.SessionId) != 16 || p.Generation == 0 || p.SnapshotSequence > p.Sequence || p.KeyAlgorithm != membershippb.KeyAlgorithm_KEY_ALGORITHM_SHA256_SIZE64BE_PREFIX64 || uint64(len(p.Additions)) > p.Sequence-p.SnapshotSequence {
		return nil, ErrInvalid
	}
	v := &View{session: [16]byte(p.SessionId), generation: p.Generation, snapshotSequence: p.SnapshotSequence,
		sequence: p.Sequence, complete: p.Complete, keyCount: p.KeyCount}
	if v.session == ([16]byte{}) {
		return nil, ErrInvalid
	}
	if f := p.Filter; f != nil {
		// Validate the multiplication before narrowing it to the library field.
		countLength := uint64(f.SegmentCount) * uint64(f.SegmentLength)
		if countLength > maxFingerprints {
			return nil, ErrInvalid
		}
		v.filter = &xorfilter.BinaryFuse[uint8]{Seed: f.Seed, SegmentLength: f.SegmentLength,
			SegmentLengthMask: f.SegmentLength - 1, SegmentCount: f.SegmentCount,
			SegmentCountLength: uint32(countLength), Fingerprints: f.Fingerprints}
	}
	if !validFilter(v.keyCount, v.filter) {
		return nil, ErrInvalid
	}
	for _, d := range p.Additions {
		if len(d.Sha256) != 32 || d.SizeBytes > uint64(cas.MaxBlobSize) {
			return nil, ErrInvalid
		}
		digest := cas.Digest{Hash: [32]byte(d.Sha256), Size: int64(d.SizeBytes)}
		if digest.Validate() != nil || (len(v.tail) != 0 && compareDigest(v.tail[len(v.tail)-1], digest) >= 0) {
			return nil, ErrInvalid
		}
		v.tail = append(v.tail, digest)
	}
	// One canonical representation keeps duplicate/default/overlong encodings
	// out of golden vectors and prevents ambiguous snapshot equality.
	canonical, err := Encode(v)
	if err != nil || !bytes.Equal(canonical, data) {
		return nil, ErrInvalid
	}
	return v, nil
}

type shape uint8

const (
	snapshotShape shape = iota
	filterShape
	digestShape
)

func preflight(data []byte, message shape) bool {
	var seen uint16
	var additions int
	for len(data) != 0 {
		number, kind, n := protowire.ConsumeTag(data)
		if n < 0 || number < 1 || number > 9 {
			return false
		}
		data = data[n:]
		repeated := message == snapshotShape && number == 9
		if !repeated && seen&(1<<number) != 0 {
			return false
		}
		seen |= 1 << number
		expected := protowire.VarintType
		limit := 0
		var nested shape
		hasNested := false
		switch message {
		case snapshotShape:
			switch number {
			case 1:
				expected, limit = protowire.BytesType, 16
			case 8:
				expected, limit, nested, hasNested = protowire.BytesType, maxFingerprints+32, filterShape, true
			case 9:
				additions++
				if additions > MaxTail {
					return false
				}
				expected, limit, nested, hasNested = protowire.BytesType, 48, digestShape, true
			}
		case filterShape:
			switch number {
			case 1:
				expected = protowire.Fixed64Type
			case 2, 3:
			case 4:
				expected, limit = protowire.BytesType, maxFingerprints
			default:
				return false
			}
		case digestShape:
			switch number {
			case 1:
				expected, limit = protowire.BytesType, 32
			case 2:
			default:
				return false
			}
		}
		if kind != expected {
			return false
		}
		if kind == protowire.BytesType {
			value, consumed := protowire.ConsumeBytes(data)
			if consumed < 0 || len(value) > limit || (hasNested && !preflight(value, nested)) {
				return false
			}
			n = consumed
		} else {
			n = protowire.ConsumeFieldValue(number, kind, data)
			if n < 0 {
				return false
			}
		}
		data = data[n:]
	}
	return true
}
