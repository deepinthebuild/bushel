package membership

import (
	"bytes"
	"math"
	"slices"
	"testing"

	"github.com/deepinthebuild/bushel/internal/cas"
	"pgregory.net/rapid"
)

type membershipReporter interface {
	Helper()
	Fatalf(string, ...any)
}

func encodeView(t membershipReporter, v *View) []byte {
	t.Helper()
	data, err := Encode(v)
	if err != nil {
		t.Fatalf("encode view: %v", err)
	}
	return data
}

func buildView(t membershipReporter, session [16]byte, generation, sequence uint64, keys []cas.Digest) *View {
	t.Helper()
	v, err := Build(session, generation, sequence, keys)
	if err != nil {
		t.Fatalf("build view: %v", err)
	}
	return v
}

func attachTail(t membershipReporter, base *View, sequence uint64, complete bool, tail []cas.Digest) *View {
	t.Helper()
	v, err := WithTail(base, sequence, complete, tail)
	if err != nil {
		t.Fatalf("attach tail: %v", err)
	}
	return v
}

// Nearby identities deliberately share a hash but have different sizes. Zero
// is the canonical empty blob, which has its own digest validation rule.
func stateDigest(id int) cas.Digest {
	if id == 0 {
		return cas.Sum(nil)
	}
	d := numberedDigest(uint64(id / 3))
	d.Size = int64(1 + id%3)
	return d
}

func TestPropertySnapshotDeliveryFollowsSourceHistory(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		type observation struct {
			ordinal int
			view    *View
			wire    []byte
			known   []cas.Digest
		}
		var epoch byte = 1
		session := [16]byte{epoch}
		generation, sequence := uint64(1), uint64(0)
		inventory := make(map[cas.Digest]bool)
		var baseline, tail []cas.Digest
		base := buildView(t, session, generation, sequence, nil)
		complete := true
		var history []observation
		var retained []observation
		var accepted *View
		acceptedOrdinal := -1
		var acceptedKnown []cas.Digest
		emit := func() {
			callerTail := slices.Clone(tail)
			v := attachTail(t, base, sequence, complete, callerTail)
			clear(callerTail)
			history = append(history, observation{
				ordinal: len(history), view: v, wire: encodeView(t, v),
				known: append(slices.Clone(baseline), tail...),
			})
			retained = append(retained, history[len(history)-1])
		}
		rebuild := func() {
			baseline = nil
			for id := 0; id < 32; id++ {
				d := stateDigest(id)
				if inventory[d] {
					baseline = append(baseline, d)
				}
			}
			callerKeys := slices.Clone(baseline)
			base = buildView(t, session, generation, sequence, callerKeys)
			clear(callerKeys)
			tail, complete = nil, true
			emit()
		}
		emit()
		for step := 0; step < 40; step++ {
			switch rapid.IntRange(0, 5).Draw(t, "source action") {
			case 0: // Duplicate publications still advance the observation sequence.
				d := stateDigest(rapid.IntRange(0, 31).Draw(t, "added identity"))
				inventory[d] = true
				sequence++
				if rapid.Bool().Draw(t, "capture addition") {
					tail = append(tail, d)
				} else {
					complete = false
				}
				emit()
			case 1:
				generation++
				rebuild()
			case 2: // Lost observations cannot restore completeness in this generation.
				if complete {
					complete = false
					emit()
				}
			case 3: // Eviction changes the next scan, not an already published hint.
				delete(inventory, stateDigest(rapid.IntRange(0, 31).Draw(t, "evicted identity")))
			case 4:
				epoch++
				session, generation, sequence = [16]byte{epoch}, 1, 0
				history = nil
				if rapid.Bool().Draw(t, "empty restart") {
					clear(inventory)
				}
				rebuild()
			case 5: // Deliver another prior poll without changing the source.
			}
			candidate := history[rapid.IntRange(0, len(history)-1).Draw(t, "delivered poll")]
			input := bytes.Clone(candidate.wire)
			received, err := Decode(input)
			if err != nil {
				t.Fatalf("decode emitted observation: %v", err)
			}
			clear(input)
			// Only emission order is modeled here. The source model creates no
			// backwards transitions, and old sessions have no in-flight polls.
			want := accepted == nil || accepted.Session() != received.Session() || candidate.ordinal >= acceptedOrdinal
			if got := CanReplace(accepted, received); got != want {
				t.Fatalf("replacement disagrees with source order at step %d: got %v, want %v", step, got, want)
			}
			if want {
				accepted, acceptedOrdinal, acceptedKnown = received, candidate.ordinal, candidate.known
			}
			for _, d := range acceptedKnown {
				if !accepted.MayContain(d) {
					t.Fatalf("accepted view lost represented content %v", d)
				}
			}
			if !bytes.Equal(encodeView(t, received), candidate.wire) {
				t.Fatalf("decoder retained caller-owned wire bytes")
			}
			for _, old := range retained {
				if !bytes.Equal(encodeView(t, old.view), old.wire) {
					t.Fatalf("later source activity mutated an earlier view")
				}
			}
		}
	})
}

func TestPropertyEmptyBaselineHasExactTailMembership(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		ids := rapid.SliceOfN(rapid.IntRange(0, 63), 0, 128).Draw(t, "tail identities")
		tail := make([]cas.Digest, len(ids))
		expected := make(map[cas.Digest]bool)
		for i, id := range ids {
			tail[i] = stateDigest(id)
			expected[tail[i]] = true
		}
		original := slices.Clone(tail)
		base := buildView(t, [16]byte{1}, 7, 19, nil)
		view := attachTail(t, base, 19+uint64(len(tail)), rapid.Bool().Draw(t, "complete"), tail)
		if !slices.Equal(tail, original) {
			t.Fatalf("tail canonicalization reordered caller data")
		}
		wire := encodeView(t, view)
		received, err := Decode(wire)
		if err != nil {
			t.Fatalf("decode exact tail: %v", err)
		}
		clear(tail)
		clear(wire)
		for id := 0; id < 96; id++ {
			d := stateDigest(id)
			if view.MayContain(d) != expected[d] || received.MayContain(d) != expected[d] {
				t.Fatalf("exact tail confused hash/size identity %d", id)
			}
			if base.MayContain(d) {
				t.Fatalf("attaching a tail changed the empty baseline")
			}
		}
		// A full replacement is independent of the input order and duplicates.
		slices.Reverse(original)
		permuted := attachTail(t, base, view.Sequence(), view.Complete(), original)
		if !CanReplace(view, permuted) || !bytes.Equal(encodeView(t, view), encodeView(t, permuted)) {
			t.Fatalf("equivalent full-tail observations have different canonical views")
		}
	})
}

func TestFullTailReplacementRequiresMonotonicEvidence(t *testing.T) {
	base := buildView(t, [16]byte{1}, 5, 3, nil)
	first, second, third := stateDigest(1), stateDigest(2), stateDigest(3)
	previous := attachTail(t, base, 8, true, []cas.Digest{first, second})
	for name, next := range map[string]*View{
		"same sequence adds evidence":              attachTail(t, base, 8, true, []cas.Digest{first, second, third}),
		"higher sequence drops evidence":           attachTail(t, base, 9, true, []cas.Digest{first, third}),
		"same generation moves watermark":          buildView(t, base.Session(), 5, 9, []cas.Digest{first, second}),
		"new generation moves watermark backwards": attachTail(t, buildView(t, base.Session(), 6, 2, nil), 9, true, []cas.Digest{first, second}),
		"new generation moves sequence backwards":  buildView(t, base.Session(), 6, 7, []cas.Digest{first, second}),
	} {
		t.Run(name, func(t *testing.T) {
			before, nextBefore := encodeView(t, previous), encodeView(t, next)
			if CanReplace(previous, next) {
				t.Fatal("accepted a replacement that rewrites observed history")
			}
			if !bytes.Equal(before, encodeView(t, previous)) || !bytes.Equal(nextBefore, encodeView(t, next)) {
				t.Fatal("rejected replacement mutated a view")
			}
		})
	}
	// Rebuilding may lose evicted content. Monotonic metadata, not old positive
	// hints, controls replacement across generations.
	rebuilt := buildView(t, base.Session(), 6, previous.Sequence(), nil)
	if !CanReplace(previous, rebuilt) || rebuilt.MayContain(first) {
		t.Fatal("new generation could not reflect an evicted inventory")
	}
}

func TestWatermarksAtUint64Boundary(t *testing.T) {
	base := buildView(t, [16]byte{1}, math.MaxUint64-1, math.MaxUint64-1, nil)
	tail := []cas.Digest{stateDigest(1)}
	last := attachTail(t, base, math.MaxUint64, true, tail)
	if !CanReplace(base, last) || CanReplace(last, base) || !last.MayContain(tail[0]) {
		t.Fatal("boundary tail ordering wrapped")
	}
	if _, err := WithTail(last, 0, true, nil); err == nil {
		t.Fatal("wrapped sequence accepted as a later observation")
	}
	rebuilt := buildView(t, base.Session(), math.MaxUint64, math.MaxUint64, tail)
	if !CanReplace(last, rebuilt) || CanReplace(rebuilt, last) {
		t.Fatal("boundary generation ordering wrapped")
	}
	restarted := buildView(t, [16]byte{2}, 1, 0, nil)
	if !CanReplace(rebuilt, restarted) || CanReplace(restarted, nil) || !CanReplace(nil, restarted) {
		t.Fatal("restart or absent-view handling depends on wrapped counters")
	}
	for _, view := range []*View{last, rebuilt, restarted} {
		data := encodeView(t, view)
		received, err := Decode(data)
		if err != nil || !CanReplace(view, received) || !CanReplace(received, view) {
			t.Fatal("boundary counters changed during wire round trip")
		}
	}
}
