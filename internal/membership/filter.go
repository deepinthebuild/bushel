// Package membership provides bounded, immutable hints about complete CAS blobs.
// Hints never establish availability or justify skipping another peer.
package membership

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"slices"

	"github.com/FastFilter/xorfilter"
	"github.com/deepinthebuild/bushel/internal/cas"
)

const (
	MaxKeys      = 262144
	MaxTail      = 1024
	MaxWireBytes = 512 << 10
	// A full filter at MaxKeys occupies about 300 KiB. Leave room for its
	// exact addition tail and protobuf framing within MaxWireBytes.
	maxFingerprints = 400 << 10
)

var ErrInvalid = errors.New("invalid membership hint")

// View owns its filter and tail. All operations leave existing views unchanged,
// so readers may query a published view without synchronization.
type View struct {
	session          [16]byte
	generation       uint64
	snapshotSequence uint64
	sequence         uint64
	complete         bool
	keyCount         uint32
	filter           *xorfilter.BinaryFuse[uint8]
	tail             []cas.Digest
}

// Key includes both parts of the REAPI identity. The domain and byte order are
// wire-format constants; the first eight hash bytes are interpreted big-endian.
func Key(d cas.Digest) uint64 {
	var input [len("bushel:blob-have:") + 32 + 8]byte
	n := copy(input[:], "bushel:blob-have:")
	n += copy(input[n:], d.Hash[:])
	binary.BigEndian.PutUint64(input[n:], uint64(d.Size))
	sum := sha256.Sum256(input[:])
	return binary.BigEndian.Uint64(sum[:8])
}

// Build constructs a baseline from at most MaxKeys catalog observations.
// The caller captures additions after snapshotSequence separately, then attaches
// them with WithTail before publishing. No caller-owned memory is retained.
func Build(session [16]byte, generation, snapshotSequence uint64, digests []cas.Digest) (*View, error) {
	if session == ([16]byte{}) || generation == 0 || len(digests) > MaxKeys {
		return nil, ErrInvalid
	}
	keys := make([]uint64, len(digests))
	for i, d := range digests {
		if d.Validate() != nil {
			return nil, ErrInvalid
		}
		keys[i] = Key(d)
	}
	slices.Sort(keys)
	keys = slices.Compact(keys)
	v := &View{session: session, generation: generation, snapshotSequence: snapshotSequence,
		sequence: snapshotSequence, complete: true, keyCount: uint32(len(keys))}
	if len(keys) != 0 {
		var err error
		// Use a fresh builder: its fingerprints remain owned by this view.
		v.filter, err = xorfilter.NewBinaryFuse[uint8](keys)
		if err != nil {
			return nil, err
		}
	}
	if !validFilter(v.keyCount, v.filter) {
		return nil, ErrInvalid
	}
	return v, nil
}

// WithTail attaches all distinct observations after the baseline's watermark.
// complete must be false if observations were lost or the scan was truncated.
// Even a complete view is a hint: backend eviction can happen at any instant.
func WithTail(base *View, sequence uint64, complete bool, tail []cas.Digest) (*View, error) {
	if base == nil || sequence < base.snapshotSequence || len(tail) > MaxTail || uint64(len(tail)) > sequence-base.snapshotSequence {
		return nil, ErrInvalid
	}
	for _, d := range tail {
		if d.Validate() != nil {
			return nil, ErrInvalid
		}
	}
	v := *base
	v.sequence, v.complete = sequence, complete
	v.tail = slices.Clone(tail)
	slices.SortFunc(v.tail, compareDigest)
	v.tail = slices.Compact(v.tail)
	return &v, nil
}

func compareDigest(a, b cas.Digest) int {
	if c := bytes.Compare(a.Hash[:], b.Hash[:]); c != 0 {
		return c
	}
	if a.Size < b.Size {
		return -1
	}
	if a.Size > b.Size {
		return 1
	}
	return 0
}

func (v *View) Session() [16]byte        { return v.session }
func (v *View) Generation() uint64       { return v.generation }
func (v *View) Sequence() uint64         { return v.sequence }
func (v *View) SnapshotSequence() uint64 { return v.snapshotSequence }
func (v *View) Complete() bool           { return v.complete }

// MayContain returns an untrusted positive hint. False must never suppress
// fallback: a peer may have newer blobs, a partial catalog, or a stale snapshot.
func (v *View) MayContain(d cas.Digest) bool {
	if v == nil || d.Validate() != nil {
		return false
	}
	if _, ok := slices.BinarySearchFunc(v.tail, d, compareDigest); ok {
		return true
	}
	return v.filter != nil && v.filter.Contains(Key(d))
}

// CanReplace rejects reordered observations within a session. Callers serialize
// polling per peer, so a changed session comes from a later authenticated stream
// after a restart, never from an older request still in flight.
func CanReplace(previous, next *View) bool {
	if next == nil {
		return false
	}
	if previous == nil || previous.session != next.session {
		return true
	}
	if next.generation < previous.generation || next.sequence < previous.sequence {
		return false
	}
	if next.generation > previous.generation {
		return next.snapshotSequence >= previous.snapshotSequence
	}
	if next.snapshotSequence != previous.snapshotSequence || next.keyCount != previous.keyCount || !sameFilter(next.filter, previous.filter) {
		return false
	}
	if !previous.complete && next.complete {
		return false
	}
	// A full-tail update cannot remove previously advertised additions.
	for _, d := range previous.tail {
		if _, ok := slices.BinarySearchFunc(next.tail, d, compareDigest); !ok {
			return false
		}
	}
	return next.sequence != previous.sequence || slices.Equal(next.tail, previous.tail)
}

func sameFilter(a, b *xorfilter.BinaryFuse[uint8]) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.Seed == b.Seed && a.SegmentLength == b.SegmentLength && a.SegmentCount == b.SegmentCount && bytes.Equal(a.Fingerprints, b.Fingerprints)
}

// SameContents lets a publisher keep an unchanged view after a periodic scan.
// Generations and baseline watermarks alone do not justify retransmitting an
// identical summary. Observation sequence and completeness must still match.
func SameContents(a, b *View) bool {
	return a != nil && b != nil && a.session == b.session && a.sequence == b.sequence &&
		a.complete == b.complete && a.keyCount == b.keyCount &&
		sameFilter(a.filter, b.filter) && slices.Equal(a.tail, b.tail)
}

// Validate every value used by Contains before constructing a filter from wire
// data. Calculate in uint64 so hostile uint32 multiplication cannot wrap.
func validFilter(count uint32, f *xorfilter.BinaryFuse[uint8]) bool {
	if count == 0 {
		return f == nil
	}
	if count > MaxKeys || f == nil || f.SegmentLength < 4 || f.SegmentLength > 262144 || f.SegmentLength&(f.SegmentLength-1) != 0 || f.SegmentCount == 0 {
		return false
	}
	length := uint64(f.SegmentLength)
	countLength := length * uint64(f.SegmentCount)
	fingerprints := countLength + 2*length
	return fingerprints <= maxFingerprints && fingerprints == uint64(len(f.Fingerprints)) &&
		f.SegmentLengthMask == f.SegmentLength-1 && uint64(f.SegmentCountLength) == countLength
}
