package publication

import (
	"testing"

	"github.com/deepinthebuild/bushel/internal/cas"
	"pgregory.net/rapid"
)

func TestRecentReadyExpiryAndCapacity(t *testing.T) {
	var c readyCache
	d := publicationDigest(0, 1)
	c.remember(d, 1)
	c.remember(d, 3599)
	if !c.contains(d, 3600) || c.contains(d, 3601) {
		t.Fatal("duplicate extended fixed completion lifetime")
	}
	c.remember(d, 3601)
	if !c.contains(d, 3601) || len(c.slots) != 1 {
		t.Fatal("expired key created duplicate ring entries")
	}
	for i := 1; i <= maxRecentReady; i++ {
		c.remember(publicationDigest(i, 1), 3601)
	}
	if c.contains(d, 3601) || len(c.entries) != maxRecentReady || len(c.slots) != maxRecentReady {
		t.Fatal("recent completion cache exceeded capacity")
	}
}

func TestRecentReadyForgetDoesNotEraseNewProofAtOldSlot(t *testing.T) {
	var c readyCache
	d := publicationDigest(0, 1)
	c.remember(d, 1)
	c.forget(d)
	if c.contains(d, 1) {
		t.Fatal("forgotten proof remains usable")
	}
	c.remember(d, 2)
	for i := 1; i < maxRecentReady; i++ {
		c.remember(publicationDigest(i, 1), 2)
	}
	if !c.contains(d, 2) {
		t.Fatal("recycling old slot erased newer positive observation")
	}
}

func TestPropertyRecentReadyRequiresUnexpiredExplicitObservation(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		var c readyCache
		seen := make(map[cas.Digest]int64)
		var now int64
		for step := 0; step < 100; step++ {
			now += int64(rapid.IntRange(0, 3600).Draw(t, "elapsed"))
			d := publicationDigest(rapid.IntRange(0, 15).Draw(t, "digest"), 1)
			if rapid.Bool().Draw(t, "positive") {
				c.remember(d, now)
				if until, ok := seen[d]; !ok || now >= until {
					seen[d] = now + 3600
				}
			}
			until, ok := seen[d]
			if c.contains(d, now) != (ok && now < until) {
				t.Fatal("cache invented or lost a positive observation")
			}
		}
	})
}
