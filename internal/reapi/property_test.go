package reapi

import (
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/deepinthebuild/bushel/internal/cas"
	"pgregory.net/rapid"
)

func resourceName(instance string, d cas.Digest, write, explicitHash bool) string {
	var b strings.Builder
	if instance != "" {
		b.WriteString(instance)
		b.WriteByte('/')
	}
	if write {
		b.WriteString("uploads/x/")
	}
	b.WriteString("blobs/")
	if explicitHash {
		b.WriteString("sha256/")
	}
	b.WriteString(d.Hex())
	b.WriteByte('/')
	b.WriteString(strconv.FormatInt(d.Size, 10))
	return b.String()
}

func TestPropertyResourceRoundTrip(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		data := rapid.SliceOfN(rapid.Byte(), 0, 1024).Draw(t, "data")
		d := cas.Sum(data)
		instance := rapid.SampledFrom([]string{"", "build", "team/cache"}).Draw(t, "instance")
		write := rapid.Bool().Draw(t, "write")
		name := resourceName(instance, d, write, rapid.Bool().Draw(t, "explicit SHA-256"))
		if write && rapid.Bool().Draw(t, "metadata") {
			name += "/filename/metadata"
		}
		parsed, err := parseResource(instance, name, write)
		if err != nil || parsed != d {
			t.Fatalf("resource round trip: %v %v", parsed, err)
		}
		if _, err := parseResource("other/"+instance, name, write); err == nil {
			t.Fatal("accepted resource for a different instance")
		}
		if _, err := parseResource(instance, name+"/..", write); err == nil {
			t.Fatal("accepted traversal component")
		}
		if _, err := parseResource(instance, name+"\x00", write); err == nil {
			t.Fatal("accepted NUL")
		}
		if !write {
			if _, err := parseResource(instance, name+"/extra", false); err == nil {
				t.Fatal("accepted read metadata")
			}
		}
	})
}

func TestPropertyResourceSizeEncodings(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		size := rapid.Int64Range(1, cas.MaxBlobSize).Draw(t, "size")
		d := cas.Digest{Hash: cas.Sum([]byte("digest")).Hash, Size: size}
		base := "blobs/" + d.Hex() + "/"
		for _, suffix := range []string{"+" + strconv.FormatInt(size, 10), "0" + strconv.FormatInt(size, 10), "-1", "9223372036854775808"} {
			if _, err := parseResource("", base+suffix, false); err == nil {
				t.Fatalf("accepted noncanonical size %q", suffix)
			}
		}
	})
}

func TestPropertyUploadPartitionsAndRejectedSteps(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		parts := rapid.SliceOfN(rapid.Int64Range(0, 64<<10), 0, 24).Draw(t, "part sizes")
		var total int64
		for _, n := range parts {
			total += n
		}
		name := resourceName("", cas.Digest{Hash: cas.Sum(nil).Hash, Size: total}, true, false)
		state := uploadState{resource: name, size: total}
		var accepted int64
		for _, n := range parts {
			bad := uploadPart{resource: name, offset: accepted + 1, dataSize: n}
			if got, err := advanceUpload(state, bad); err == nil || got != state {
				t.Fatal("nonsequential request changed state")
			}
			bad = uploadPart{resource: name, offset: accepted, dataSize: total - accepted + 1}
			if got, err := advanceUpload(state, bad); err == nil || got != state {
				t.Fatal("oversize request changed state")
			}
			part := uploadPart{offset: accepted, dataSize: n}
			if rapid.Bool().Draw(t, "repeat resource") {
				part.resource = name
			}
			next, err := advanceUpload(state, part)
			if err != nil {
				t.Fatal(err)
			}
			accepted += n
			if next.offset != accepted || next.size != total || next.resource != name || next.finished {
				t.Fatal("accepted prefix invariant failed")
			}
			state = next
		}
		next, err := advanceUpload(state, uploadPart{offset: accepted, finish: true})
		if err != nil || !next.finished || next.offset != total {
			t.Fatalf("completion: %v %v", next, err)
		}
		if got, err := advanceUpload(next, uploadPart{offset: total, finish: true}); err == nil || got != next {
			t.Fatal("completed state accepted another part")
		}
	})
}

func TestPropertyUploadPrematureFinishIsTransactional(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		size := rapid.Int64Range(1, cas.MaxBlobSize).Draw(t, "size")
		offset := rapid.Int64Range(0, size-1).Draw(t, "offset")
		partSize := rapid.Int64Range(0, size-offset-1).Draw(t, "part size")
		state := uploadState{resource: "upload", size: size, offset: offset}
		if next, err := advanceUpload(state, uploadPart{offset: offset, dataSize: partSize, finish: true}); err == nil || next != state {
			t.Fatal("premature finish changed state")
		}
		if next, err := advanceUpload(state, uploadPart{resource: "another upload", offset: offset, dataSize: partSize}); err == nil || next != state {
			t.Fatal("different resource changed state")
		}
	})
}

func FuzzParseResource(f *testing.F) {
	empty := cas.Sum(nil)
	f.Add("", resourceName("", empty, false, false), false)
	f.Add("team/cache", resourceName("team/cache", cas.Sum([]byte("seed")), true, true), true)
	f.Add("", "blobs/../-1", false)
	f.Add("", "uploads/id/blobs/sha256/hash/9223372036854775808", true)
	f.Fuzz(func(t *testing.T, instance, name string, write bool) {
		d, err := parseResource(instance, name, write)
		if err != nil {
			return
		}
		if len(name) > 4096 || d.Validate() != nil {
			t.Fatal("accepted invalid resource")
		}
		canonical := resourceName(instance, d, write, false)
		got, err := parseResource(instance, canonical, write)
		if err != nil || got != d {
			t.Fatalf("accepted resource cannot be represented canonically: %v", err)
		}
	})
}

func FuzzUploadTransition(f *testing.F) {
	f.Add(int64(3), int64(0), false, "", int64(0), int64(3), true)
	f.Add(int64(0), int64(0), false, "upload", int64(0), int64(0), true)
	f.Add(cas.MaxBlobSize, cas.MaxBlobSize, false, "", cas.MaxBlobSize, int64(0), true)
	f.Add(int64(math.MaxInt64), int64(-1), true, "other", int64(math.MinInt64), int64(math.MaxInt64), false)
	f.Fuzz(func(t *testing.T, size, offset int64, finished bool, resource string, partOffset, partSize int64, finish bool) {
		state := uploadState{resource: "upload", size: size, offset: offset, finished: finished}
		next, err := advanceUpload(state, uploadPart{resource: resource, offset: partOffset, dataSize: partSize, finish: finish})
		if err != nil {
			if next != state {
				t.Fatal("rejection mutated state")
			}
			return
		}
		if state.finished || size < 0 || size > cas.MaxBlobSize || offset < 0 || offset > size || partSize < 0 || partOffset != offset {
			t.Fatal("accepted invalid transition")
		}
		if next.resource != state.resource || next.size != size || next.offset < offset || next.offset > size || next.offset-offset != partSize || next.finished != finish {
			t.Fatal("successful transition broke prefix invariants")
		}
		if next.finished && next.offset != size {
			t.Fatal("finished an incomplete upload")
		}
		if resource != "" && resource != state.resource {
			t.Fatal("changed upload identity")
		}
	})
}
