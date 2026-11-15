package workspace

import (
	"fmt"
	"strings"
	"testing"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

func classifier(t testing.TB, capacity int, ttl time.Duration) *Classifier {
	t.Helper()
	c, err := NewClassifier("producer", "instance", capacity, ttl)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestLabelSpellings(t *testing.T) {
	for _, tc := range []struct {
		text string
		kind Kind
		repo string
	}{
		{"//pkg:target", Main, ""}, {"@//pkg:target", Main, ""}, {"@@//pkg:target", Main, ""},
		{"//:target", Main, ""}, {"//pkg", Main, ""},
		{"@@_main//pkg:target", Canonical, "_main"},
		{"@@rules_go++go_deps+com_example~1.2//pkg:target", Canonical, "rules_go++go_deps+com_example~1.2"},
		{"@repo//pkg:target", Apparent, "repo"},
	} {
		label, ok := ParseLabel(tc.text)
		if !ok || label.Kind != tc.kind || label.Repository != tc.repo {
			t.Fatalf("%q: %+v %t", tc.text, label, ok)
		}
	}
	for _, bad := range []string{"", "pkg:target", "//", "//:", "@repo", "@@@repo//x", "@@a/b//x", "@@.//p:t", "@@..//p:t", "//a//b:t", "//../x:t", "//p:t:u", "//p:\\t", "//p:t\n", "//p: t", string([]byte{255})} {
		if _, ok := ParseLabel(bad); ok {
			t.Fatalf("accepted malformed label %q", bad)
		}
	}
}

func TestMetadataBoundsAndConflicts(t *testing.T) {
	encode := func(m *repb.RequestMetadata) string {
		b, err := proto.Marshal(m)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	valid := encode(&repb.RequestMetadata{ToolInvocationId: "invocation", ActionId: "action", TargetId: "@@repo//pkg:target"})
	if m := ParseMetadata([]string{valid, valid}); !m.Valid || m.TargetID != "@@repo//pkg:target" {
		t.Fatal("equal metadata did not coalesce")
	}
	wrongType := append([]byte(valid), protowire.AppendTag(nil, 6, protowire.VarintType)...)
	wrongType = protowire.AppendVarint(wrongType, 1)
	conflict := append([]byte(valid), protowire.AppendTag(nil, 6, protowire.BytesType)...)
	conflict = protowire.AppendString(conflict, "//different:t")
	for _, values := range [][]string{
		nil, {valid, encode(&repb.RequestMetadata{TargetId: "//other:t"})}, {"\xff"},
		{string(wrongType)}, {string(conflict)}, {strings.Repeat("x", MaxMetadataBytes+1)},
		{encode(&repb.RequestMetadata{TargetId: strings.Repeat("x", MaxTargetBytes+1)})},
		{encode(&repb.RequestMetadata{ActionId: strings.Repeat("x", MaxIDBytes+1)})},
		{encode(&repb.RequestMetadata{ToolInvocationId: strings.Repeat("x", MaxIDBytes+1)})},
		{encode(&repb.RequestMetadata{ConfigurationId: strings.Repeat("x", MaxIDBytes+1)})},
		make([]string, MaxMetadataBytes/len(MetadataHeader)+1),
	} {
		if got := ParseMetadata(values); got != (Metadata{}) {
			t.Fatal("invalid metadata did not become unknown")
		}
	}
}

func TestClassifierIsolationAndFixedExpiry(t *testing.T) {
	now := time.Unix(100, 0)
	c := classifier(t, 2, time.Minute)
	m := Metadata{Valid: true, InvocationID: "invocation", ActionID: "action", TargetID: "@@repo//pkg:t"}
	direct := c.Classify(m, now)
	m.TargetID = ""
	if got := c.Classify(m, now.Add(59*time.Second)); got != direct {
		t.Fatal("missing target did not infer its action association")
	}
	if got := c.Classify(m, now.Add(time.Minute)); got.Kind != Unknown || c.Len() != 0 {
		t.Fatal("observation extended TTL")
	}
	m.TargetID = "@@repo//pkg:t"
	a := c.Classify(m, now.Add(time.Minute))
	m.CorrelatedInvocationsID = "accounting-group"
	if got := c.Classify(m, now.Add(time.Minute)); got != a {
		t.Fatal("accounting group changed repository identity")
	}
	m.InvocationID = "other-invocation"
	if got := c.Classify(m, now.Add(time.Minute)); got.Partition == a.Partition {
		t.Fatal("invocations merged")
	}
	m.InvocationID = ""
	if got := c.Classify(m, now.Add(time.Minute)); got.Partition == a.Partition || got.Kind != Canonical {
		t.Fatal("unscoped bucket collided or lost direct label")
	}
	for _, scope := range [][2]string{{"other-producer", "instance"}, {"producer", "other-instance"}} {
		other, _ := NewClassifier(scope[0], scope[1], 2, time.Minute)
		m.InvocationID = "invocation"
		if got := other.Classify(m, now); got.Partition == a.Partition {
			t.Fatal("producer/instance scopes merged")
		}
	}
}

func TestPropertyConflictingActionsNeverInfer(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		c, err := NewClassifier("producer", "instance", 4, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		labels := []string{"//main:t", "@@repo+v1//pkg:a", "@apparent//pkg:t", "@@repo+v1//pkg:b"}
		first := rapid.IntRange(0, len(labels)-1).Draw(t, "first")
		second := (first + rapid.IntRange(1, len(labels)-1).Draw(t, "different")) % len(labels)
		if rapid.Bool().Draw(t, "reverse") {
			first, second = second, first
		}
		now := time.Unix(100, 0)
		m := Metadata{Valid: true, InvocationID: "invocation", ActionID: "action"}
		for _, index := range []int{first, second} {
			m.TargetID = labels[index]
			want, _ := ParseLabel(m.TargetID)
			if got := c.Classify(m, now); got.Kind != want.Kind {
				t.Fatal("conflict hid a directly supplied valid label")
			}
		}
		for range rapid.IntRange(1, 20).Draw(t, "later observations") {
			m.TargetID = ""
			if got := c.Classify(m, now); got.Kind != Unknown {
				t.Fatal("conflicting action inferred a workspace")
			}
			m.TargetID = labels[first]
			c.Classify(m, now)
		}
	})
}

func TestPropertyCorrelationMatchesBoundedModel(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		capacity := rapid.IntRange(1, 8).Draw(t, "capacity")
		c, _ := NewClassifier("producer", "instance", capacity, 10*time.Second)
		type entry struct {
			id, expires int
			label       string
			conflict    bool
		}
		var model []entry
		var tick int
		for range rapid.IntRange(1, 100).Draw(t, "steps") {
			tick += rapid.IntRange(0, 3).Draw(t, "elapsed")
			for len(model) > 0 && model[0].expires <= tick {
				model = model[1:]
			}
			id := rapid.IntRange(0, 12).Draw(t, "action")
			label := rapid.SampledFrom([]string{"", "//main:t", "@@repo//pkg:t", "malformed"}).Draw(t, "label")
			index := -1
			for i, e := range model {
				if e.id == id {
					index = i
				}
			}
			want := Unknown
			if label == "" && index >= 0 && !model[index].conflict {
				if model[index].label == "//main:t" {
					want = Main
				} else {
					want = Canonical
				}
			} else if label == "//main:t" || label == "@@repo//pkg:t" {
				if label == "//main:t" {
					want = Main
				} else {
					want = Canonical
				}
				if index >= 0 {
					model[index].conflict = model[index].conflict || model[index].label != label
				} else {
					if len(model) == capacity {
						model = model[1:]
					}
					model = append(model, entry{id: id, expires: tick + 10, label: label})
				}
			}
			m := Metadata{Valid: true, InvocationID: "invocation", ActionID: fmt.Sprint(id), TargetID: label}
			if got := c.Classify(m, time.Unix(int64(tick), 0)); got.Kind != want || c.Len() != len(model) || c.Len() > capacity {
				t.Fatalf("bounded model mismatch: kind %v/%v, entries %d/%d", got.Kind, want, c.Len(), len(model))
			}
		}
	})
}

func FuzzMetadataAndLabel(f *testing.F) {
	for _, seed := range []string{"//pkg:t", "@@repo++extension+name//pkg:t", "@repo//:t", "@@//pkg", "\xff"} {
		f.Add([]byte(seed))
	}
	valid, err := proto.Marshal(&repb.RequestMetadata{ToolInvocationId: "invocation", ActionId: "action", TargetId: "@@repo++extension+name//pkg:t"})
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add(append(append([]byte{}, valid...), valid...))
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) > MaxMetadataBytes+1 {
			return
		}
		m := ParseMetadata([]string{string(raw)})
		if m.Valid && !m.validFields() {
			t.Fatal("accepted unbounded metadata")
		}
		if label, ok := ParseLabel(string(raw)); ok {
			prefix := "//"
			if label.Kind == Canonical {
				prefix = "@@" + label.Repository + "//"
			}
			if label.Kind == Apparent {
				prefix = "@" + label.Repository + "//"
			}
			if again, valid := ParseLabel(prefix + label.Target); !valid || again != label {
				t.Fatal("label normalization changed classification")
			}
		}
	})
}
