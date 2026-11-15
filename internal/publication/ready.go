package publication

import (
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
)

const maxRecentReady = 4096

// A bounded, process-local join for reordered REAPI callbacks. Only explicit
// positive observations enter this cache, never generic imports or CAS reads.
// Repeated hints do not extend a slot's fixed lifetime.
type readyCache struct {
	entries map[cas.Digest]readyMark
	slots   []cas.Digest
	next    int
}

type readyMark struct {
	expires int64
	slot    int
}

func (c *readyCache) remember(d cas.Digest, now int64) {
	if c.contains(d, now) {
		return
	}
	if c.entries == nil {
		c.entries = make(map[cas.Digest]readyMark)
	}
	if mark, ok := c.entries[d]; ok {
		mark.expires = now + int64(time.Hour/time.Second)
		c.entries[d] = mark
		return
	}
	var slot int
	if len(c.slots) < maxRecentReady {
		slot = len(c.slots)
		c.slots = append(c.slots, d)
	} else {
		slot = c.next
		if mark, ok := c.entries[c.slots[slot]]; ok && mark.slot == slot {
			delete(c.entries, c.slots[slot])
		}
		c.slots[c.next] = d
		c.next = (c.next + 1) % maxRecentReady
	}
	c.entries[d] = readyMark{expires: now + int64(time.Hour/time.Second), slot: slot}
}

func (c *readyCache) contains(d cas.Digest, now int64) bool {
	mark, ok := c.entries[d]
	return ok && now < mark.expires
}

func (c *readyCache) forget(d cas.Digest) { delete(c.entries, d) }
