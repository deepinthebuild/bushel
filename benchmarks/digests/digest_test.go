package digests

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"fmt"
	"hash"
	"io"
	"testing"

	cdc "github.com/buildbarn/go-cdc"
	"github.com/zeebo/blake3"
)

var result []byte

func BenchmarkDigest(b *testing.B) {
	for _, size := range []int{256 << 10, 384 << 10, 512 << 10} {
		data := make([]byte, size)
		for i := range data {
			data[i] = byte(i)
		}
		for _, algorithm := range []struct {
			name string
			new  func() hash.Hash
		}{
			{"SHA256", sha256.New}, {"BLAKE3", func() hash.Hash { return blake3.New() }},
		} {
			b.Run(fmt.Sprintf("%s/%d", algorithm.name, size), func(b *testing.B) {
				h := algorithm.new()
				sum := make([]byte, 0, 32)
				b.SetBytes(int64(size))
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					h.Reset()
					_, _ = h.Write(data)
					result = h.Sum(sum[:0])
				}
			})
		}
	}
}

// BenchmarkPipeline includes the blob hash, RepMaxCDC, and each chunk hash.
// Manifest assembly and storage I/O are excluded equally from both algorithms.
func BenchmarkPipeline(b *testing.B) {
	data := make([]byte, 8<<20)
	var x uint32 = 1
	for i := range data {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		data[i] = byte(x)
	}
	chunker := cdc.NewRepMaxContentDefinedChunker(&cdc.FastContentDefinedChunkerGearTable, 256<<10, 1<<20)
	for _, algorithm := range []struct {
		name string
		new  func() hash.Hash
		sum  func([]byte) [32]byte
	}{
		{"SHA256", sha256.New, sha256.Sum256},
		{"BLAKE3", func() hash.Hash { return blake3.New() }, blake3.Sum256},
	} {
		b.Run(algorithm.name, func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			b.ReportAllocs()
			for b.Loop() {
				h := algorithm.new()
				r := chunker.NewChunkReader(bufio.NewReaderSize(io.TeeReader(bytes.NewReader(data), h), chunker.GetMaximumPeekSizeBytes()))
				for {
					chunk, err := r.ReadNextChunk()
					if err == io.EOF {
						break
					}
					if err != nil {
						b.Fatal(err)
					}
					pipelineSum = algorithm.sum(chunk)
				}
				result = h.Sum(nil)
			}
		})
	}
}

var pipelineSum [32]byte
