package workspace

import (
	"container/list"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"hash"
	"strings"
	"sync"
	"time"
	"unicode"
)

type Kind uint8

const (
	Unknown Kind = iota
	Main
	Canonical
	Apparent
)

type Association struct {
	Partition [32]byte
	Kind      Kind
}

// Label preserves canonical repository suffixes and keeps apparent names
// unresolved. Target is the portion following //, used to detect conflicting
// action labels; it does not participate in workspace partition identity.
type Label struct {
	Kind       Kind
	Repository string
	Target     string
}

func ParseLabel(target string) (Label, bool) {
	if target == "" || !boundedText(target, MaxTargetBytes) {
		return Label{}, false
	}
	for _, r := range target {
		if unicode.IsSpace(r) || r == '\\' {
			return Label{}, false
		}
	}
	label := Label{Kind: Main}
	switch {
	case strings.HasPrefix(target, "@@"):
		label.Kind = Canonical
		target = target[2:]
	case strings.HasPrefix(target, "@"):
		label.Kind = Apparent
		target = target[1:]
	}
	repository, suffix, found := strings.Cut(target, "//")
	if !found || (label.Kind == Main && repository != "") {
		return Label{}, false
	}
	if repository == "" {
		label.Kind = Main
	}
	if repository == "." || repository == ".." {
		return Label{}, false
	}
	for _, r := range repository {
		if !(r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || strings.ContainsRune("._-+~", r)) {
			return Label{}, false
		}
	}
	packageName, name, explicit := strings.Cut(suffix, ":")
	if strings.Contains(name, ":") || !labelPath(packageName, explicit) || (explicit && !labelPath(name, false)) {
		return Label{}, false
	}
	label.Repository, label.Target = repository, suffix
	return label, true
}

func labelPath(path string, allowEmpty bool) bool {
	if path == "" {
		return allowEmpty
	}
	for _, part := range strings.Split(path, "/") {
		if part == "" || part == "." || part == ".." {
			return false
		}
	}
	return true
}

type actionKey struct{ invocation, action string }
type correlation struct {
	key      actionKey
	label    Label
	conflict bool
	expires  time.Time
}

// Classifier owns a bounded FIFO correlation cache. The caller supplies time;
// observations never extend an entry's original TTL. Calls are concurrency-safe.
type Classifier struct {
	mu                 sync.Mutex
	producer, instance string
	capacity           int
	ttl                time.Duration
	lastTime           time.Time
	entries            map[actionKey]*list.Element
	order              list.List
}

func NewClassifier(producer, instance string, capacity int, ttl time.Duration) (*Classifier, error) {
	if capacity < 1 || capacity > 1<<20 || ttl <= 0 {
		return nil, errors.New("invalid workspace correlation limits")
	}
	return &Classifier{producer: producer, instance: instance, capacity: capacity, ttl: ttl,
		entries: make(map[actionKey]*list.Element)}, nil
}

func (c *Classifier) Classify(metadata Metadata, now time.Time) Association {
	c.mu.Lock()
	defer c.mu.Unlock()
	// A wall-clock correction cannot resurrect expired associations or reorder
	// expiration. Tests can advance time without relying on timers or sleeps.
	if now.Before(c.lastTime) {
		now = c.lastTime
	}
	c.lastTime = now
	for first := c.order.Front(); first != nil && !now.Before(first.Value.(*correlation).expires); first = c.order.Front() {
		c.remove(first)
	}
	if !metadata.validFields() {
		return c.association("", Label{})
	}
	key := actionKey{metadata.InvocationID, metadata.ActionID}
	cacheable := key.invocation != "" && key.action != ""
	if metadata.TargetID == "" {
		if entry := c.entries[key]; cacheable && entry != nil {
			value := entry.Value.(*correlation)
			if !value.conflict {
				return c.association(metadata.InvocationID, value.label)
			}
		}
		return c.association(metadata.InvocationID, Label{})
	}
	label, ok := ParseLabel(metadata.TargetID)
	if !ok {
		return c.association(metadata.InvocationID, Label{})
	}
	if cacheable {
		if entry := c.entries[key]; entry != nil {
			value := entry.Value.(*correlation)
			value.conflict = value.conflict || value.label != label
		} else {
			if len(c.entries) == c.capacity {
				c.remove(c.order.Front())
			}
			c.entries[key] = c.order.PushBack(&correlation{key: key, label: label, expires: now.Add(c.ttl)})
		}
	}
	return c.association(metadata.InvocationID, label)
}

func (c *Classifier) remove(element *list.Element) {
	delete(c.entries, element.Value.(*correlation).key)
	c.order.Remove(element)
}

// Len reports retained correlations; expired entries are removed on Classify.
func (c *Classifier) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.entries)
}

func (c *Classifier) association(invocation string, label Label) Association {
	h := sha256.New()
	for _, part := range []string{"bushel-workspace-v1", c.producer, c.instance, invocation, string([]byte{byte(label.Kind)}), label.Repository} {
		hashField(h, part)
	}
	var partition [32]byte
	copy(partition[:], h.Sum(nil))
	return Association{Partition: partition, Kind: label.Kind}
}

func hashField(h hash.Hash, value string) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	_, _ = h.Write(length[:])
	_, _ = h.Write([]byte(value))
}
