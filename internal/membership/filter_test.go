package membership

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"slices"
	"testing"

	"github.com/deepinthebuild/bushel/internal/cas"
	membershippb "github.com/deepinthebuild/bushel/internal/membership/pb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

func numberedDigest(n uint64) cas.Digest {
	var data [8]byte
	binary.BigEndian.PutUint64(data[:], n)
	return cas.Sum(data[:])
}

func mustBuild(t testing.TB, keys []cas.Digest) *View {
	t.Helper()
	v, err := Build([16]byte{1}, 1, 0, keys)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestRoundTripNeverLosesObservedContent(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		keys := rapid.SliceOfN(rapid.Uint64Range(0, 1000), 0, 300).Draw(t, "keys")
		added := rapid.SliceOfN(rapid.Uint64Range(0, 1000), 0, 50).Draw(t, "additions")
		original := make([]cas.Digest, len(keys))
		for i, key := range keys {
			original[i] = numberedDigest(key)
		}
		source := slices.Clone(original)
		v, err := Build([16]byte{1}, 1, 25, source)
		if err != nil {
			t.Fatal(err)
		}
		if !slices.Equal(source, original) {
			t.Fatal("build changed caller data")
		}
		tail := make([]cas.Digest, len(added))
		for i, key := range added {
			tail[i] = numberedDigest(key)
		}
		withTail, err := WithTail(v, 25+uint64(len(tail)), rapid.Bool().Draw(t, "complete"), tail)
		if err != nil {
			t.Fatal(err)
		}
		data, err := Encode(withTail)
		if err != nil {
			t.Fatal(err)
		}
		received, err := Decode(data)
		if err != nil {
			t.Fatal(err)
		}
		for _, d := range append(original, tail...) {
			if !withTail.MayContain(d) || !received.MayContain(d) {
				t.Fatalf("false negative for %v", d)
			}
		}
		clear(source)
		clear(tail)
		clear(data)
		for _, d := range original {
			if !v.MayContain(d) || !received.MayContain(d) {
				t.Fatal("caller buffer mutation changed a published view")
			}
		}
		if v.sequence != 25 || len(v.tail) != 0 || !v.complete {
			t.Fatal("attaching a tail modified the baseline")
		}
	})
}

func TestKeyIncludesSize(t *testing.T) {
	d := numberedDigest(42)
	other := d
	other.Size++
	if Key(d) == Key(other) {
		t.Fatal("size did not distinguish blob identities")
	}
	// This vector freezes domain separation, hash input and byte order.
	const want = "190511d90ad462cc"
	var encoded [8]byte
	binary.BigEndian.PutUint64(encoded[:], Key(d))
	if got := hex.EncodeToString(encoded[:]); got != want {
		t.Fatalf("key vector: got %s", got)
	}
}

func TestWireGolden(t *testing.T) {
	v := mustBuild(t, []cas.Digest{cas.Sum([]byte("first")), cas.Sum([]byte("second"))})
	v, err := WithTail(v, 1, true, []cas.Digest{cas.Sum([]byte("third"))})
	if err != nil {
		t.Fatal(err)
	}
	data, err := Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	const want = "0a100100000000000000000000000000000010012001280130013802421b09c15c0289ec2d0a9110041801220c00000000e00000000000007d4a240a20b1e99324505bd32da0e1f85dcf5e19a09db0481e8a15f62c41eb320304a8e9271005"
	if got := hex.EncodeToString(data); got != want {
		t.Fatalf("wire vector: got %s", got)
	}
	if _, err := Decode(data); err != nil {
		t.Fatal(err)
	}
}

func TestReorderedViewsAndRestart(t *testing.T) {
	base := mustBuild(t, []cas.Digest{numberedDigest(1)})
	tail := []cas.Digest{numberedDigest(2)}
	newer, _ := WithTail(base, 1, true, tail)
	if !CanReplace(base, newer) || CanReplace(newer, base) || !CanReplace(newer, newer) {
		t.Fatal("same-generation ordering")
	}
	incomplete, _ := WithTail(newer, 1, false, tail)
	if !CanReplace(newer, incomplete) || CanReplace(incomplete, newer) {
		t.Fatal("lost observations must not restore completeness within a generation")
	}
	dropped, _ := WithTail(base, 2, true, nil)
	if CanReplace(newer, dropped) {
		t.Fatal("accepted a gapped full tail")
	}
	changed, _ := Build(base.Session(), 1, 0, []cas.Digest{numberedDigest(3)})
	if CanReplace(base, changed) {
		t.Fatal("same-generation baseline changed")
	}
	rebuilt, _ := Build(base.Session(), 2, 1, tail)
	if !CanReplace(incomplete, rebuilt) || CanReplace(rebuilt, newer) {
		t.Fatal("new snapshot ordering")
	}
	restarted, _ := Build([16]byte{2}, 1, 0, nil)
	if !CanReplace(rebuilt, restarted) {
		t.Fatal("new session rejected")
	}
}

func TestBuilderLimits(t *testing.T) {
	for _, test := range []struct {
		session [16]byte
		gen     uint64
		keys    []cas.Digest
	}{
		{},
		{session: [16]byte{1}},
		{session: [16]byte{1}, gen: 1, keys: []cas.Digest{{Size: -1}}},
		{session: [16]byte{1}, gen: 1, keys: make([]cas.Digest, MaxKeys+1)},
	} {
		if _, err := Build(test.session, test.gen, 0, test.keys); err == nil {
			t.Fatal("accepted invalid builder input")
		}
	}
	base := mustBuild(t, nil)
	if _, err := WithTail(base, 0, true, []cas.Digest{numberedDigest(1)}); err == nil {
		t.Fatal("tail exceeded sequence interval")
	}
	if _, err := WithTail(base, MaxTail+1, true, make([]cas.Digest, MaxTail+1)); err == nil {
		t.Fatal("oversized tail accepted")
	}
	if base.MayContain(numberedDigest(1)) {
		t.Fatal("empty inventory reported present")
	}
}

func TestMaximumViewAndFalsePositives(t *testing.T) {
	keys := make([]cas.Digest, MaxKeys)
	for i := range keys {
		keys[i] = numberedDigest(uint64(i))
	}
	v := mustBuild(t, keys)
	tail := make([]cas.Digest, MaxTail)
	for i := range tail {
		tail[i] = numberedDigest(uint64(MaxKeys + i))
	}
	v, err := WithTail(v, MaxTail, true, tail)
	if err != nil {
		t.Fatal(err)
	}
	data, err := Encode(v)
	if err != nil || len(data) > MaxWireBytes {
		t.Fatalf("maximum view did not fit wire budget: %d bytes, %v", len(data), err)
	}
	received, err := Decode(data)
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range keys {
		if !received.MayContain(d) {
			t.Fatal("false negative at maximum cardinality")
		}
	}
	for _, d := range tail {
		if !received.MayContain(d) {
			t.Fatal("tail false negative")
		}
	}
	positives := 0
	for i := range 100000 {
		if received.MayContain(numberedDigest(uint64(MaxKeys + MaxTail + i))) {
			positives++
		}
	}
	// This deterministic corpus should exhibit ordinary false positives. A
	// generous bound detects a broken always-positive query without flakiness.
	if positives == 0 || positives > 1000 {
		t.Fatalf("unexpected false positives: %d/100000", positives)
	}
}

func TestDecodeRejectsHostileShapes(t *testing.T) {
	v := mustBuild(t, []cas.Digest{numberedDigest(1)})
	valid, err := Encode(v)
	if err != nil {
		t.Fatal(err)
	}
	mutations := []func(*membershippb.Snapshot){
		func(p *membershippb.Snapshot) { p.SessionId = nil },
		func(p *membershippb.Snapshot) { p.SessionId = make([]byte, 16) },
		func(p *membershippb.Snapshot) { p.Generation = 0 },
		func(p *membershippb.Snapshot) { p.SnapshotSequence = 1 },
		func(p *membershippb.Snapshot) { p.KeyAlgorithm = 33 },
		func(p *membershippb.Snapshot) { p.KeyCount = MaxKeys + 1 },
		func(p *membershippb.Snapshot) { p.KeyCount = 0 },
		func(p *membershippb.Snapshot) { p.Filter = nil },
		func(p *membershippb.Snapshot) { p.Filter.SegmentLength = 0 },
		func(p *membershippb.Snapshot) { p.Filter.SegmentLength = 3 },
		func(p *membershippb.Snapshot) { p.Filter.SegmentLength = 1 << 31 },
		func(p *membershippb.Snapshot) { p.Filter.SegmentCount = ^uint32(0) },
		func(p *membershippb.Snapshot) { p.Filter.Fingerprints = nil },
		func(p *membershippb.Snapshot) { p.Filter.Fingerprints = make([]byte, maxFingerprints+1) },
		func(p *membershippb.Snapshot) {
			p.Additions = []*membershippb.Digest{{Sha256: make([]byte, 32), SizeBytes: 1}}
		},
		func(p *membershippb.Snapshot) {
			p.Sequence = 1
			p.Additions = []*membershippb.Digest{{Sha256: make([]byte, 31), SizeBytes: 1}}
		},
		func(p *membershippb.Snapshot) {
			p.Sequence = 1
			p.Additions = []*membershippb.Digest{{Sha256: make([]byte, 32)}}
		},
		func(p *membershippb.Snapshot) {
			p.Sequence = 1
			p.Additions = []*membershippb.Digest{{Sha256: make([]byte, 32), SizeBytes: uint64(cas.MaxBlobSize) + 1}}
		},
		func(p *membershippb.Snapshot) {
			p.Sequence = MaxTail + 1
			for range MaxTail + 1 {
				p.Additions = append(p.Additions, &membershippb.Digest{})
			}
		},
	}
	for i, mutate := range mutations {
		var p membershippb.Snapshot
		if err := proto.Unmarshal(valid, &p); err != nil {
			t.Fatal(err)
		}
		mutate(&p)
		data, err := proto.Marshal(&p)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := Decode(data); err == nil {
			t.Errorf("mutation %d accepted", i)
		}
	}
	for _, data := range [][]byte{
		bytes.Repeat([]byte{0}, MaxWireBytes+1),
		append(slices.Clone(valid), protowire.AppendVarint(protowire.AppendTag(nil, 2, protowire.VarintType), 1)...),
		append(slices.Clone(valid), protowire.AppendVarint(protowire.AppendTag(nil, 10, protowire.VarintType), 1)...),
		append(slices.Clone(valid), 0x18, 0), // Explicit default snapshot sequence is not canonical.
		{0x0a, 0xff, 0xff, 0xff, 0xff, 0x7f},
	} {
		if _, err := Decode(data); err == nil {
			t.Fatal("hostile framing accepted")
		}
	}
}

func FuzzMembershipSnapshot(f *testing.F) {
	v := mustBuild(f, []cas.Digest{numberedDigest(1), numberedDigest(2)})
	seed, err := Encode(v)
	if err != nil {
		f.Fatal(err)
	}
	f.Add(seed)
	f.Add([]byte{})
	f.Add([]byte{0x0a, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		v, err := Decode(data)
		if err != nil {
			return
		}
		encoded, err := Encode(v)
		if err != nil || !bytes.Equal(encoded, data) {
			t.Fatal("noncanonical accepted snapshot")
		}
		// Exercise every hostile-but-valid geometry with independent keys.
		for i := range 100 {
			v.MayContain(numberedDigest(uint64(i)))
		}
	})
}

func TestEquivalentRebuildDoesNotChangePublishedContents(t *testing.T) {
	keys := []cas.Digest{numberedDigest(1), numberedDigest(2)}
	old, err := Build([16]byte{1}, 1, 10, keys)
	if err != nil {
		t.Fatal(err)
	}
	rebuilt, err := Build([16]byte{1}, 2, 10, keys)
	if err != nil || !SameContents(old, rebuilt) {
		t.Fatalf("equivalent rebuild: %v", err)
	}
	newSequence, _ := WithTail(rebuilt, 11, true, nil)
	incomplete, _ := WithTail(rebuilt, 10, false, nil)
	different, _ := Build([16]byte{1}, 3, 10, []cas.Digest{numberedDigest(3)})
	restarted, _ := Build([16]byte{2}, 1, 10, keys)
	for _, next := range []*View{newSequence, incomplete, different, restarted, nil} {
		if SameContents(old, next) {
			t.Fatal("changed observation was suppressed")
		}
	}
}

func TestConditionalRequest(t *testing.T) {
	known := [32]byte{1, 2, 3}
	data := EncodeRequest(&known)
	want, err := proto.Marshal(&membershippb.Request{KnownSnapshotSha256: known[:]})
	if err != nil || !bytes.Equal(data, want) {
		t.Fatalf("request differs from generated schema: %v", err)
	}
	decoded, err := DecodeRequest(data)
	if err != nil || decoded == nil || *decoded != known {
		t.Fatalf("request roundtrip: %v", err)
	}
	clear(data)
	if *decoded != known {
		t.Fatal("request retained caller bytes")
	}
	if decoded, err := DecodeRequest(EncodeRequest(nil)); err != nil || decoded != nil {
		t.Fatal("empty request rejected")
	}
	for _, invalid := range [][]byte{{0x0a, 0}, {0x0a, 33}, make([]byte, MaxRequestBytes+1), append(want, want...)} {
		if _, err := DecodeRequest(invalid); err == nil {
			t.Fatal("invalid conditional request accepted")
		}
	}
}

func FuzzMembershipRequest(f *testing.F) {
	f.Add(EncodeRequest(&[32]byte{1}))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		known, err := DecodeRequest(data)
		if err == nil && !bytes.Equal(data, EncodeRequest(known)) {
			t.Fatal("accepted noncanonical request")
		}
	})
}
