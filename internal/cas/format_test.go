package cas

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"testing"
)

func fixture(n int) []byte {
	b := make([]byte, n)
	var x uint32 = 1
	for i := range b {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		b[i] = byte(x)
	}
	return b
}

func TestManifestGolden(t *testing.T) {
	d := Sum([]byte("hello"))
	m := Manifest{Blob: d, Chunks: []Digest{d}}
	b, err := m.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	want := "4253484c4d3030312cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b98240000000000000005000000012cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b982400000005"
	if hex.EncodeToString(b) != want {
		t.Fatalf("wire change: %x", b)
	}
	got, err := ParseManifest(b)
	if err != nil || got.Blob != d || got.Chunks[0] != d {
		t.Fatalf("roundtrip: %v %v", got, err)
	}
	for _, bad := range [][]byte{b[:51], append(bytes.Clone(b), 0), bytes.Repeat([]byte{255}, 52)} {
		if _, err := ParseManifest(bad); err == nil {
			t.Fatal("accepted malformed manifest")
		}
	}
}

func TestChunkBoundsAndReconstruction(t *testing.T) {
	for _, n := range []int{0, 1, MinChunkSize - 1, MinChunkSize, MaxChunkSize, MaxChunkSize + 1, 4 << 20} {
		b := fixture(n)
		var out bytes.Buffer
		m, err := Chunk(context.Background(), Sum(b), bytes.NewReader(b), func(d Digest, p []byte) error {
			if Sum(p) != d || len(p) > MaxChunkSize {
				t.Fatal("invalid chunk")
			}
			_, e := out.Write(p)
			return e
		})
		if err != nil || !bytes.Equal(out.Bytes(), b) {
			t.Fatalf("size %d: %v", n, err)
		}
		if err := m.Validate(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestChunkRejectsCorruptionAndCancellation(t *testing.T) {
	d := Sum([]byte("hello"))
	for _, b := range [][]byte{[]byte("jello"), []byte("hell"), []byte("helloo")} {
		_, err := Chunk(context.Background(), d, bytes.NewReader(b), func(Digest, []byte) error { return nil })
		if !errors.Is(err, ErrCorrupt) {
			t.Fatalf("got %v", err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Chunk(ctx, d, bytes.NewReader([]byte("hello")), func(Digest, []byte) error { return nil }); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestChunkGolden(t *testing.T) {
	b := fixture(2 << 20)
	m, err := Chunk(context.Background(), Sum(b), bytes.NewReader(b), func(Digest, []byte) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	wire, _ := m.MarshalBinary()
	// This digest freezes chunk boundaries, ordering, hashes, and manifest encoding.
	const want = "de330017464a25c86733c4a212a9a829d76dfcdb8e9aeecc995bfd8462cfa690"
	if got := Sum(wire).Hex(); got != want {
		t.Fatalf("manifest hash = %s", got)
	}
}

func FuzzParseManifest(f *testing.F) {
	b, _ := (Manifest{Blob: Sum(nil)}).MarshalBinary()
	f.Add(b)
	f.Fuzz(func(t *testing.T, b []byte) {
		m, err := ParseManifest(b)
		if err != nil {
			return
		}
		out, err := m.MarshalBinary()
		if err != nil || !bytes.Equal(b, out) {
			t.Fatal("noncanonical manifest")
		}
	})
}

func BenchmarkChunk(b *testing.B) {
	data := fixture(8 << 20)
	d := Sum(data)
	b.SetBytes(int64(len(data)))
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		_, err := Chunk(context.Background(), d, bytes.NewReader(data), func(_ Digest, p []byte) error { _, e := io.Discard.Write(p); return e })
		if err != nil {
			b.Fatal(err)
		}
	}
}
