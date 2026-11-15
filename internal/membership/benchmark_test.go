package membership

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/deepinthebuild/bushel/internal/cas"
)

// Synthetic identities represent small, typical-chunk, and mapped blobs. No
// payload, backend, or network I/O is included. Fixtures are built before timing.
func benchmarkMembershipDigests(first uint64, count int) []cas.Digest {
	digests := make([]cas.Digest, count)
	sizes := [...]int64{8 << 10, 384 << 10, 3 << 20}
	for i := range digests {
		var input [16]byte
		binary.BigEndian.PutUint64(input[:8], first+uint64(i))
		copy(input[8:], "member-b")
		digests[i] = cas.Digest{Hash: sha256.Sum256(input[:]), Size: sizes[i%len(sizes)]}
	}
	return digests
}

func benchmarkMembershipCases(b *testing.B, run func(*testing.B, int, int)) {
	for _, count := range []int{1024, 32768, MaxKeys} {
		for _, tail := range []int{0, MaxTail} {
			b.Run(fmt.Sprintf("keys=%d/tail=%d", count, tail), func(b *testing.B) {
				run(b, count, tail)
			})
		}
	}
}

func benchmarkMembershipView(b *testing.B, digests, tail []cas.Digest) *View {
	v, err := Build([16]byte{1}, 1, 1, digests)
	if err != nil {
		b.Fatal(err)
	}
	v, err = WithTail(v, 1+uint64(len(tail)), true, tail)
	if err != nil {
		b.Fatal(err)
	}
	return v
}

func benchmarkMembershipEncode(b *testing.B, v *View) []byte {
	data, err := Encode(v)
	if err != nil || len(data) > MaxWireBytes {
		b.Fatalf("encode bounded fixture: %v", err)
	}
	return data
}

// Build includes digest validation, domain-separated hashing, sorting/dedup,
// fresh filter construction, immutable tail copying, and protobuf encoding.
// It measures the current public API, without assuming builder-buffer reuse.
func BenchmarkMembershipBuild(b *testing.B) {
	benchmarkMembershipCases(b, func(b *testing.B, count, tailCount int) {
		digests := benchmarkMembershipDigests(0, count)
		tail := benchmarkMembershipDigests(1<<32, tailCount)
		var last *View
		var encoded []byte
		b.ReportAllocs()
		for b.Loop() {
			last = benchmarkMembershipView(b, digests, tail)
			encoded = benchmarkMembershipEncode(b, last)
		}
		b.ReportMetric(float64(len(encoded)), "encoded-B/op")
		b.ReportMetric(float64(len(last.filter.Fingerprints)), "fingerprint-B")
		// Retained payload backing arrays only; excludes headers and allocator
		// overhead, and is deliberately distinct from cumulative B/op.
		b.ReportMetric(float64(len(last.filter.Fingerprints)+40*len(last.tail)), "retained-payload-B")
	})
}

func BenchmarkMembershipEncode(b *testing.B) {
	benchmarkMembershipCases(b, func(b *testing.B, count, tailCount int) {
		v := benchmarkMembershipView(b, benchmarkMembershipDigests(0, count), benchmarkMembershipDigests(1<<32, tailCount))
		size := len(benchmarkMembershipEncode(b, v))
		b.ReportAllocs()
		b.SetBytes(int64(size))
		for b.Loop() {
			if got := benchmarkMembershipEncode(b, v); len(got) != size {
				b.Fatal("encoding changed size")
			}
		}
		b.ReportMetric(float64(size), "encoded-B/op")
	})
}

func BenchmarkMembershipDecode(b *testing.B) {
	benchmarkMembershipCases(b, func(b *testing.B, count, tailCount int) {
		v := benchmarkMembershipView(b, benchmarkMembershipDigests(0, count), benchmarkMembershipDigests(1<<32, tailCount))
		data := benchmarkMembershipEncode(b, v)
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		for b.Loop() {
			decoded, err := Decode(data)
			if err != nil || decoded.keyCount != uint32(count) || len(decoded.tail) != tailCount {
				b.Fatalf("decode lost fixture entries: %v", err)
			}
		}
		b.ReportMetric(float64(len(data)), "encoded-B/op")
	})
}

// Queries include the public method's digest validation and SHA-256 key hash.
// An absent query may return true: false positives are normal routing hints.
func BenchmarkMembershipQuery(b *testing.B) {
	benchmarkMembershipCases(b, func(b *testing.B, count, tailCount int) {
		digests := benchmarkMembershipDigests(0, count)
		tail := benchmarkMembershipDigests(1<<32, tailCount)
		v := benchmarkMembershipView(b, digests, tail)
		for _, tc := range []struct {
			name     string
			queries  []cas.Digest
			allMatch bool
		}{
			{"baseline_hit", digests[:1024], true},
			{"tail_hit", tail, true},
			{"absent", benchmarkMembershipDigests(1<<33, 1024), false},
		} {
			if len(tc.queries) == 0 {
				continue
			}
			b.Run(tc.name, func(b *testing.B) {
				var index, matches int
				b.ReportAllocs()
				for b.Loop() {
					if v.MayContain(tc.queries[index]) {
						matches++
					}
					index = (index + 1) % len(tc.queries)
				}
				if tc.allMatch && matches != b.N {
					b.Fatal("filter missed an inserted identity")
				}
				b.ReportMetric(100*float64(matches)/float64(b.N), "positive-%")
			})
		}
	})
}
