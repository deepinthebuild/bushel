package p2p

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/deepinthebuild/bushel/internal/cas"
	bsmessage "github.com/ipfs/boxo/bitswap/message"
	bspb "github.com/ipfs/boxo/bitswap/message/pb"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	mhcore "github.com/multiformats/go-multihash/core"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

func sizedCIDPayload(data []byte) []byte {
	sum := sha256.Sum256(data)
	return binary.BigEndian.AppendUint64(sum[:], uint64(len(data)))
}

func TestSizedCIDGolden(t *testing.T) {
	key, err := sizedChunkCID(cas.Sum([]byte("hello")))
	if err != nil {
		t.Fatal(err)
	}
	// CIDv1/raw, private multihash code, length40, SHA256("hello"), size5.
	const wire = "01558180c00128" + "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824" + "0000000000000005"
	if got := hex.EncodeToString(key.Bytes()); got != wire {
		t.Fatalf("sized CID wire changed: %s", got)
	}
	if key.ByteLen() != sizedChunkCIDSize || sizedChunkCIDSize != 47 {
		t.Fatal("sized CID byte length changed")
	}
	parsed, err := cid.Decode(key.String())
	if err != nil || !parsed.Equals(key) {
		t.Fatalf("CID text roundtrip: %v", err)
	}
	got, err := sizedChunkDigest(parsed)
	if err != nil || got != cas.Sum([]byte("hello")) {
		t.Fatalf("REAPI tuple roundtrip: %+v, %v", got, err)
	}
}

func TestPropertySizedCIDRoundTripAndSegmentedHash(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		data := rapid.SliceOfN(rapid.Byte(), 1, 4096).Draw(t, "data")
		d := cas.Sum(data)
		key, err := sizedChunkCID(d)
		if err != nil {
			t.Fatal(err)
		}
		got, err := sizedChunkDigest(key)
		if err != nil || got != d {
			t.Fatalf("digest roundtrip: %+v vs %+v, %v", got, d, err)
		}
		rebuilt, err := key.Prefix().Sum(data)
		if err != nil || !rebuilt.Equals(key) {
			t.Fatalf("prefix reconstruction differs: %s, %v", rebuilt, err)
		}
		h, err := mhcore.GetHasher(sizedChunkMultihashCode)
		if err != nil {
			t.Fatal(err)
		}
		offset := 0
		segments := rapid.SliceOfN(rapid.IntRange(0, len(data)), 0, 16).Draw(t, "segments")
		for _, size := range append(segments, len(data)) {
			end := min(offset+size, len(data))
			n, err := h.Write(data[offset:end])
			if err != nil || n != end-offset {
				t.Fatalf("hash write: %d, %v", n, err)
			}
			offset = end
			// Sum must preserve state and append without altering its prefix.
			prefix := []byte{0x91, 0x82, 0x73}
			sum := h.Sum(bytes.Clone(prefix))
			if !bytes.Equal(sum[:len(prefix)], prefix) || !bytes.Equal(sum[len(prefix):], sizedCIDPayload(data[:offset])) {
				t.Fatal("segmented Sum changed the hash, length, or prefix")
			}
		}
		if h.Size() != 40 || h.BlockSize() != sha256.BlockSize || !bytes.Equal(h.Sum(nil), sizedCIDPayload(data)) {
			t.Fatal("segmented hash differs from one-shot SHA256 and byte count")
		}
		h.Reset()
		if !bytes.Equal(h.Sum(nil), sizedCIDPayload(nil)) {
			t.Fatal("Reset retained digest state or byte count")
		}
		if _, err := h.Write(data); err != nil || !bytes.Equal(h.Sum(nil), sizedCIDPayload(data)) {
			t.Fatalf("hash reuse: %v", err)
		}
	})
}

func TestSizedCIDHasherRejectsTruncation(t *testing.T) {
	data := []byte("complete tuple required")
	key, err := sizedChunkCID(cas.Sum(data))
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{-1, 40} {
		mh, err := multihash.Sum(data, sizedChunkMultihashCode, size)
		if err != nil || !bytes.Equal(mh, key.Hash()) {
			t.Fatalf("supported hash size %d: %v", size, err)
		}
		streamed, err := multihash.SumStream(bytes.NewReader(data), sizedChunkMultihashCode, size)
		if err != nil || !bytes.Equal(streamed, key.Hash()) {
			t.Fatalf("streamed hash size %d: %v", size, err)
		}
	}
	for _, size := range []int{-2, 0, 1, 31, 32, 39, 41, 64, math.MaxInt} {
		if _, err := multihash.Sum(data, sizedChunkMultihashCode, size); err == nil {
			t.Fatalf("accepted unsupported digest size %d", size)
		}
	}
}

func TestSizedCIDRejectsMalformedTuples(t *testing.T) {
	payload := sizedCIDPayload([]byte("hello"))
	makeCID := func(codec, code uint64, payload []byte) cid.Cid {
		t.Helper()
		mh, err := multihash.Encode(payload, code)
		if err != nil {
			t.Fatal(err)
		}
		return cid.NewCidV1(codec, mh)
	}
	standard, err := multihash.Sum([]byte("hello"), multihash.SHA2_256, -1)
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]cid.Cid{
		"undefined":       cid.Undef,
		"v0":              cid.NewCidV0(standard),
		"standard SHA256": cid.NewCidV1(cid.Raw, standard),
		"wrong codec":     makeCID(cid.DagProtobuf, sizedChunkMultihashCode, payload),
		"wrong code":      makeCID(cid.Raw, sizedChunkMultihashCode+1, payload),
		"short tuple":     makeCID(cid.Raw, sizedChunkMultihashCode, payload[:39]),
		"long tuple":      makeCID(cid.Raw, sizedChunkMultihashCode, append(bytes.Clone(payload), 0)),
	}
	for _, size := range []uint64{0, cas.MaxChunkSize + 1, 1 << 63, math.MaxUint64} {
		invalid := bytes.Clone(payload)
		binary.BigEndian.PutUint64(invalid[32:], size)
		cases[fmt.Sprintf("size%d", size)] = makeCID(cid.Raw, sizedChunkMultihashCode, invalid)
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := sizedChunkDigest(c); !errors.Is(err, cas.ErrInvalid) {
				t.Fatalf("accepted malformed CID: %v", err)
			}
		})
	}
	for _, size := range []int64{-1, 0, cas.MaxChunkSize + 1, math.MaxInt64} {
		if _, err := sizedChunkCID(cas.Digest{Size: size}); !errors.Is(err, cas.ErrInvalid) {
			t.Fatalf("encoded invalid chunk size %d: %v", size, err)
		}
	}
}

func TestSizedCIDBoxoMessageRoundTrip(t *testing.T) {
	for _, size := range []int{1, cas.MinChunkSize, cas.MaxChunkSize} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			data := make([]byte, size)
			for i := range data {
				data[i] = byte(i*31 + i/256)
			}
			d := cas.Sum(data)
			key, err := sizedChunkCID(d)
			if err != nil {
				t.Fatal(err)
			}
			block, err := blocks.NewBlockWithCid(data, key)
			if err != nil {
				t.Fatal(err)
			}
			message := bsmessage.New(true)
			message.AddBlock(block)
			message.AddEntry(key, 1, bspb.Message_Wantlist_Block, true)
			haveKey, err := sizedChunkCID(cas.Sum([]byte("have-only fixture")))
			if err != nil {
				t.Fatal(err)
			}
			message.AddHave(haveKey)
			var wire bytes.Buffer
			if err := message.ToNetV1(&wire); err != nil {
				t.Fatal(err)
			}
			decoded, _, err := bsmessage.FromNet(&wire)
			if err != nil {
				t.Fatal(err)
			}
			gotBlocks := decoded.Blocks()
			if len(gotBlocks) != 1 || !gotBlocks[0].Cid().Equals(key) || !bytes.Equal(gotBlocks[0].RawData(), data) {
				t.Fatal("Boxo did not reconstruct the sized CID from prefix and bytes")
			}
			got, err := sizedChunkDigest(gotBlocks[0].Cid())
			if err != nil || got != d {
				t.Fatalf("Boxo reconstructed a different REAPI tuple: %+v, %v", got, err)
			}
			if wants := decoded.Wantlist(); len(wants) != 1 || !wants[0].Cid.Equals(key) {
				t.Fatal("sized want-list CID changed on the wire")
			}
			if haves := decoded.Haves(); len(haves) != 1 || !haves[0].Equals(haveKey) {
				t.Fatal("sized HAVE CID changed on the wire")
			}
		})
	}
}

func TestSizedCIDBoxoRejectsMismatchedContentAndSize(t *testing.T) {
	data := []byte("hello")
	d := cas.Sum(data)
	key, err := sizedChunkCID(d)
	if err != nil {
		t.Fatal(err)
	}
	for _, wrongData := range [][]byte{[]byte("jello"), []byte("hello!"), nil} {
		if _, err := bsmessage.NewWantlistBlock(wrongData, key, key.Prefix()); !errors.Is(err, blocks.ErrWrongHash) {
			t.Fatalf("Boxo accepted bytes outside the requested tuple: %v", err)
		}
	}
	d.Size++
	wrongSize, err := sizedChunkCID(d)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := bsmessage.NewWantlistBlock(data, wrongSize, wrongSize.Prefix()); !errors.Is(err, blocks.ErrWrongHash) {
		t.Fatalf("Boxo accepted an incorrect length with the correct SHA256: %v", err)
	}
	for _, length := range []int{0, 32, 39, 41} {
		prefix := key.Prefix()
		prefix.MhLength = length
		if _, err := bsmessage.NewWantlistBlock(data, cid.Undef, prefix); err == nil {
			t.Fatalf("Boxo accepted a truncated or extended tuple length %d", length)
		}
	}
}

func TestSizedCIDForeignBlake3PrefixLengthIsBounded(t *testing.T) {
	for _, length := range []int{128, 129, 1 << 30, math.MaxInt} {
		t.Run(fmt.Sprint(length), func(t *testing.T) {
			prefix := cid.Prefix{Version: 1, Codec: cid.Raw, MhType: multihash.BLAKE3, MhLength: length}
			payload, err := proto.Marshal(&bspb.Message{Payload: []*bspb.Message_Block{{Prefix: prefix.Bytes(), Data: []byte("tiny payload")}}})
			if err != nil {
				t.Fatal(err)
			}
			wire := binary.AppendUvarint(nil, uint64(len(payload)))
			wire = append(wire, payload...)
			decoded, _, err := bsmessage.FromNet(bytes.NewReader(wire))
			if length > 128 {
				if err == nil {
					t.Fatal("Boxo accepted a foreign extendable-output hash above the registry limit")
				}
				return
			}
			// The pinned registry permits at most 128 output bytes. This is
			// still not a Bushel content identity and is rejected on admission.
			if err != nil || len(decoded.Blocks()) != 1 {
				t.Fatalf("bounded foreign hash fixture: %v", err)
			}
			if _, err := sizedChunkDigest(decoded.Blocks()[0].Cid()); !errors.Is(err, cas.ErrInvalid) {
				t.Fatalf("foreign digest accepted as a Bushel chunk: %v", err)
			}
		})
	}
}

func FuzzSizedCIDCanonicalRoundTrip(f *testing.F) {
	for _, size := range []int{1, cas.MinChunkSize, cas.MaxChunkSize} {
		key, err := sizedChunkCID(cas.Sum(testData(size)))
		if err != nil {
			f.Fatal(err)
		}
		f.Add(key.Bytes())
	}
	f.Add(routingCID(cas.Sum([]byte("routing key")).Hash).Bytes())
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 4096 {
			return
		}
		key, err := cid.Cast(data)
		if err != nil {
			return
		}
		d, err := sizedChunkDigest(key)
		if err != nil {
			return
		}
		canonical, err := sizedChunkCID(d)
		if err != nil || !bytes.Equal(canonical.Bytes(), data) {
			t.Fatalf("accepted a noncanonical tuple: %x, %v", data, err)
		}
	})
}

func BenchmarkCIDPrefixSum(b *testing.B) {
	for _, size := range []int{cas.MinChunkSize, cas.MaxChunkSize} {
		data := testData(size)
		d := cas.Sum(data)
		sized, err := sizedChunkCID(d)
		if err != nil {
			b.Fatal(err)
		}
		for _, tc := range []struct {
			name string
			key  cid.Cid
		}{
			{"sha256", routingCID(d.Hash)},
			{"sha256_size", sized},
		} {
			b.Run(fmt.Sprintf("%s/%d", tc.name, size), func(b *testing.B) {
				prefix := tc.key.Prefix()
				b.SetBytes(int64(size))
				b.ReportAllocs()
				for b.Loop() {
					got, err := prefix.Sum(data)
					if err != nil || !got.Equals(tc.key) {
						b.Fatalf("CID reconstruction: %v", err)
					}
				}
			})
		}
	}
}
