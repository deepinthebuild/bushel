package cas

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"testing"

	cdc "github.com/buildbarn/go-cdc"
	"pgregory.net/rapid"
)

// Generating a seed, length, and pattern keeps large cases cheap to shrink.
func propertyData(n int, seed uint64, pattern int) []byte {
	b := make([]byte, n)
	x := seed | 1
	for i := range b {
		switch pattern {
		case 0:
			x ^= x << 13
			x ^= x >> 7
			x ^= x << 17
			b[i] = byte(x)
		case 1:
			b[i] = byte(seed)
		case 2:
			b[i] = byte(uint64(i)%257 + seed)
		}
	}
	return b
}

type segmentedReader struct {
	io.Reader
	maxRead int
}

func (r segmentedReader) Read(p []byte) (int, error) {
	return r.Reader.Read(p[:min(len(p), r.maxRead)])
}

func chunkWithSegments(data []byte, maxRead int) (Manifest, []byte, error) {
	var out bytes.Buffer
	m, err := Chunk(context.Background(), Sum(data), segmentedReader{bytes.NewReader(data), maxRead}, func(d Digest, p []byte) error {
		if Sum(p) != d || len(p) == 0 || len(p) > MaxChunkSize {
			return fmt.Errorf("invalid emitted chunk: size %d", len(p))
		}
		_, err := out.Write(p)
		return err
	})
	return m, out.Bytes(), err
}

func referenceChunks(data []byte) ([]Digest, error) {
	// Buildbarn supplies an independent, simpler RepMax implementation as an
	// oracle for its optimized implementation. Do not derive expected cuts
	// by calling Chunk a second time.
	c := cdc.NewSimpleRepMaxContentDefinedChunker(&cdc.FastContentDefinedChunkerGearTable, MinChunkSize, HorizonSize)
	r := c.NewChunkReader(bufio.NewReaderSize(bytes.NewReader(data), c.GetMaximumPeekSizeBytes()))
	var chunks []Digest
	for {
		p, err := r.ReadNextChunk()
		if err == io.EOF {
			return chunks, nil
		}
		if err != nil {
			return nil, err
		}
		chunks = append(chunks, Sum(p))
	}
}

func TestPropertyDigestRoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		data := rapid.SliceOfN(rapid.Byte(), 0, 4096).Draw(t, "data")
		d := Sum(data)
		got, err := ParseDigest(d.Hex(), d.Size)
		if err != nil || got != d {
			t.Fatalf("digest round trip: %v %v", got, err)
		}
		for _, hash := range []string{d.Hex()[1:], d.Hex() + "0", "g" + d.Hex()[1:]} {
			if _, err := ParseDigest(hash, d.Size); !errors.Is(err, ErrInvalid) {
				t.Fatalf("accepted malformed digest %q: %v", hash, err)
			}
		}
		upper := strings.ToUpper(d.Hex())
		if upper != d.Hex() {
			if _, err := ParseDigest(upper, d.Size); !errors.Is(err, ErrInvalid) {
				t.Fatal("accepted uppercase digest")
			}
		}
		for _, size := range []int64{-1, MaxBlobSize + 1, math.MaxInt64, math.MinInt64} {
			if _, err := ParseDigest(d.Hex(), size); !errors.Is(err, ErrInvalid) {
				t.Fatalf("accepted size %d", size)
			}
		}
	})
}

func TestPropertyManifestRoundTripAndStructure(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		count := rapid.IntRange(0, 32).Draw(t, "chunk count")
		m := Manifest{Blob: Sum(nil)}
		for i := range count {
			lower := int64(MinChunkSize)
			if count == 1 {
				lower = 1
			}
			size := rapid.Int64Range(lower, MaxChunkSize).Draw(t, "chunk size")
			hash := Sum([]byte{byte(i), rapid.Byte().Draw(t, "hash seed")}).Hash
			m.Chunks = append(m.Chunks, Digest{Hash: hash, Size: size})
			m.Blob.Size += size
		}
		encoded, err := m.MarshalBinary()
		if err != nil || len(encoded) != ManifestHeaderSize+36*count {
			t.Fatalf("encoding: %v", err)
		}
		decoded, err := ParseManifest(encoded)
		if err != nil || decoded.Blob != m.Blob || !slices.Equal(decoded.Chunks, m.Chunks) {
			t.Fatalf("manifest round trip: %v", err)
		}
		badCount := bytes.Clone(encoded)
		binary.BigEndian.PutUint32(badCount[48:52], uint32(count+1))
		badSize := bytes.Clone(encoded)
		binary.BigEndian.PutUint64(badSize[40:48], uint64(m.Blob.Size+1))
		for _, bad := range [][]byte{encoded[:len(encoded)-1], append(bytes.Clone(encoded), 0), badCount, badSize} {
			if _, err := ParseManifest(bad); !errors.Is(err, ErrInvalid) {
				t.Fatalf("accepted inconsistent manifest: %v", err)
			}
		}
		if count > 0 {
			badChunk := bytes.Clone(encoded)
			binary.BigEndian.PutUint32(badChunk[ManifestHeaderSize+32:], 0)
			if _, err := ParseManifest(badChunk); !errors.Is(err, ErrInvalid) {
				t.Fatal("accepted empty chunk")
			}
		}
	})
}

func TestPropertyChunkingMatchesReferenceAcrossReadBoundaries(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		boundary := rapid.SampledFrom([]int{0, MinChunkSize, MaxChunkSize, HorizonSize, HorizonSize + 2*MinChunkSize}).Draw(t, "size boundary")
		n := max(0, boundary+rapid.IntRange(-64, 64).Draw(t, "size delta"))
		data := propertyData(n, rapid.Uint64().Draw(t, "data seed"), rapid.IntRange(0, 3).Draw(t, "pattern"))
		readSize := rapid.IntRange(1, 32<<10).Draw(t, "read size")
		m, out, err := chunkWithSegments(data, readSize)
		if err != nil || !bytes.Equal(out, data) || m.Blob != Sum(data) || m.Validate() != nil {
			t.Fatalf("chunk reconstruction: %v", err)
		}
		want, err := referenceChunks(data)
		if err != nil || !slices.Equal(m.Chunks, want) {
			t.Fatalf("read segmentation changed reference boundaries: %v", err)
		}
	})
}

func TestPropertyChunkRejectsChangedContent(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		n := rapid.IntRange(1, 2*MinChunkSize+1).Draw(t, "size")
		data := propertyData(n, rapid.Uint64().Draw(t, "seed"), 0)
		d := Sum(data)
		bad := bytes.Clone(data)
		switch rapid.IntRange(0, 2).Draw(t, "corruption") {
		case 0:
			bad[rapid.IntRange(0, n-1).Draw(t, "byte")] ^= 1
		case 1:
			bad = bad[:len(bad)-1]
		case 2:
			bad = append(bad, 0)
		}
		if _, err := Chunk(context.Background(), d, bytes.NewReader(bad), func(Digest, []byte) error { return nil }); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("changed input accepted: %v", err)
		}
	})
}

func FuzzParseDigest(f *testing.F) {
	f.Add(Sum(nil).Hex(), int64(0))
	f.Add(Sum([]byte("seed")).Hex(), MaxBlobSize)
	f.Add(strings.Repeat("f", 64), int64(math.MaxInt64))
	f.Add("ABC", int64(-1))
	f.Fuzz(func(t *testing.T, hash string, size int64) {
		d, err := ParseDigest(hash, size)
		if err != nil {
			return
		}
		if d.Hex() != hash || d.Size != size || d.Validate() != nil {
			t.Fatal("accepted noncanonical digest")
		}
	})
}

func FuzzChunkSegmentation(f *testing.F) {
	for _, n := range []int{0, 1, MinChunkSize - 1, 2 * MinChunkSize, 3*MinChunkSize + 7} {
		f.Add(propertyData(n, 1, 0), uint16(4096))
	}
	f.Fuzz(func(t *testing.T, data []byte, segment uint16) {
		if len(data) > 2<<20 {
			t.Skip()
		}
		m, out, err := chunkWithSegments(data, int(segment)+1)
		if err != nil || !bytes.Equal(out, data) || m.Validate() != nil {
			t.Fatalf("invalid chunking: %v", err)
		}
		want, err := referenceChunks(data)
		if err != nil || !slices.Equal(m.Chunks, want) {
			t.Fatalf("chunking differs from reference: %v", err)
		}
	})
}
