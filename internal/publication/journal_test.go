package publication

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	publicationpb "github.com/deepinthebuild/bushel/internal/publication/pb"
	"github.com/deepinthebuild/bushel/internal/workspace"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

func encodedJournalFixture(t testing.TB) []byte {
	t.Helper()
	f, err := encodeEntry(snapshotFixture(), true, nil)
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte(journalMagic), f...)
}

func assertSameState(t testing.TB, want, got *state) {
	t.Helper()
	a, err := marshalState(want)
	if err != nil {
		t.Fatal(err)
	}
	b, err := marshalState(got)
	if err != nil || !bytes.Equal(a, b) {
		t.Fatalf("journal changed state: %v", err)
	}
}

func journalProtoFixture(t testing.TB) *publicationpb.JournalEntry {
	t.Helper()
	frame, err := encodeEntry(snapshotFixture(), false, nil)
	if err != nil {
		t.Fatal(err)
	}
	entry := new(publicationpb.JournalEntry)
	if err := proto.Unmarshal(frame[frameHeaderBytes:], entry); err != nil {
		t.Fatal(err)
	}
	return entry
}

func protoDigest(d cas.Digest) *publicationpb.Digest {
	return &publicationpb.Digest{Sha256: bytes.Clone(d.Hash[:]), SizeBytes: uint64(d.Size)}
}

// Give deliberately invalid protobuf a valid frame checksum. Corrupt semantic
// state must still fail closed after the framing layer accepts its bytes.
func checkedJournalFrame(payload []byte) []byte {
	frame := make([]byte, frameHeaderBytes+len(payload))
	binary.BigEndian.PutUint32(frame, uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:], ^uint32(len(payload)))
	sum := sha256.Sum256(payload)
	copy(frame[8:], sum[:])
	copy(frame[frameHeaderBytes:], payload)
	return frame
}

func TestJournalEntryRejectsInvalidData(t *testing.T) {
	first, second := protoDigest(publicationDigest(90, 128)), protoDigest(publicationDigest(91, 128))
	if bytes.Compare(first.Sha256, second.Sha256) > 0 {
		first, second = second, first
	}
	mutations := map[string]func(*publicationpb.JournalEntry){
		"unspecified kind":     func(e *publicationpb.JournalEntry) { e.Kind = publicationpb.RecordKind_RECORD_KIND_UNSPECIFIED },
		"unknown kind":         func(e *publicationpb.JournalEntry) { e.Kind = 3 },
		"negative kind":        func(e *publicationpb.JournalEntry) { e.Kind = -1 },
		"missing state":        func(e *publicationpb.JournalEntry) { e.State = nil },
		"invalid nested state": func(e *publicationpb.JournalEntry) { e.State.Clock = nil },
		"checkpoint deletes": func(e *publicationpb.JournalEntry) {
			e.Kind = publicationpb.RecordKind_RECORD_KIND_CHECKPOINT
			e.Deleted = []*publicationpb.Digest{first}
		},
		"duplicate deletion":  func(e *publicationpb.JournalEntry) { e.Deleted = []*publicationpb.Digest{first, first} },
		"reordered deletions": func(e *publicationpb.JournalEntry) { e.Deleted = []*publicationpb.Digest{second, first} },
		"delete updated job":  func(e *publicationpb.JournalEntry) { e.Deleted = []*publicationpb.Digest{e.State.Jobs[0].Digest} },
		"short deletion hash": func(e *publicationpb.JournalEntry) {
			e.Deleted = []*publicationpb.Digest{{Sha256: make([]byte, 31), SizeBytes: 1}}
		},
		"long deletion hash": func(e *publicationpb.JournalEntry) {
			e.Deleted = []*publicationpb.Digest{{Sha256: make([]byte, 33), SizeBytes: 1}}
		},
		"deletion size overflow": func(e *publicationpb.JournalEntry) {
			e.Deleted = []*publicationpb.Digest{{Sha256: make([]byte, 32), SizeBytes: math.MaxUint64}}
		},
		"wrong empty deletion hash": func(e *publicationpb.JournalEntry) {
			e.Deleted = []*publicationpb.Digest{{Sha256: make([]byte, 32)}}
		},
		"too many deletions": func(e *publicationpb.JournalEntry) {
			e.Deleted = make([]*publicationpb.Digest, maxRecords+1)
			for i := range e.Deleted {
				e.Deleted[i] = first
			}
		},
		"unknown nested field": func(e *publicationpb.JournalEntry) {
			e.State.ProtoReflect().SetUnknown(protoVarintField(99, 1))
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			entry := journalProtoFixture(t)
			mutate(entry)
			if _, _, _, err := decodeEntry(marshalProto(t, entry)); err == nil {
				t.Fatal("accepted invalid journal entry")
			}
		})
	}
}

func TestJournalEntryRejectsNoncanonicalWire(t *testing.T) {
	entry := journalProtoFixture(t)
	payload := marshalProto(t, entry)
	stateBytes := marshalProto(t, entry.State)
	for name, malformed := range map[string][]byte{
		"explicit unspecified kind": replaceProtoField(t, payload, 1, protoVarintField(1, 0)),
		"unknown record kind":       replaceProtoField(t, payload, 1, protoVarintField(1, 3)),
		"duplicate kind":            append(bytes.Clone(payload), protoVarintField(1, 2)...),
		"duplicate state":           append(bytes.Clone(payload), protoBytesField(2, stateBytes)...),
		"wrong state type":          protoVarintField(2, 1),
		"unknown field":             append(bytes.Clone(payload), protoVarintField(4, 1)...),
		"nested duplicate version":  protoBytesField(2, append(bytes.Clone(stateBytes), protoVarintField(1, 1)...)),
		"nested unknown field":      protoBytesField(2, append(bytes.Clone(stateBytes), protoVarintField(99, 1)...)),
		"nested explicit default":   protoBytesField(2, append(bytes.Clone(stateBytes), protoVarintField(8, 0)...)),
		"truncated state":           payload[:len(payload)-1],
		"overflow state length":     append([]byte{0x12}, bytes.Repeat([]byte{0xff}, 11)...),
		"nested group":              protoBytesField(2, append(bytes.Clone(stateBytes), 0x0b, 0x0c)),
		"JSON payload":              []byte(`{"reset":true,"state":{},"deleted":null}`),
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, _, err := decodeEntry(malformed); err == nil {
				t.Fatal("accepted noncanonical journal protobuf")
			}
		})
	}
	entry.Kind = publicationpb.RecordKind_RECORD_KIND_CHECKPOINT
	entry.State.Clock.Seconds = math.MaxInt64
	badLog := append([]byte(journalMagic), checkedJournalFrame(marshalProto(t, entry))...)
	if _, _, _, err := parseJournal(badLog); err == nil {
		t.Fatal("valid checksum concealed invalid protobuf state")
	}
}

func TestJournalRequiresCompleteInitialCheckpointAndMagic(t *testing.T) {
	initial := encodedJournalFixture(t)
	if !bytes.HasPrefix(initial, []byte("BUSHELJ1")) {
		t.Fatal("pre-stable schema change modified the journal marker")
	}
	for end := len(journalMagic); end < len(initial); end++ {
		if _, _, _, err := parseJournal(initial[:end]); err == nil {
			t.Fatalf("accepted incomplete initial checkpoint at %d", end)
		}
	}
	for name, data := range map[string][]byte{
		"wrong magic":   append([]byte("NOTAJRNL"), initial[len(journalMagic):]...),
		"JSON payload":  append([]byte(journalMagic), checkedJournalFrame([]byte(`{"reset":true,"state":{},"deleted":null}`))...),
		"missing magic": initial[len(journalMagic):],
	} {
		t.Run(name, func(t *testing.T) {
			if _, _, _, err := parseJournal(data); err == nil {
				t.Fatal("accepted invalid journal format")
			}
		})
	}
	frame, err := encodeEntry(snapshotFixture(), false, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := parseJournal(append([]byte(journalMagic), frame...)); err == nil {
		t.Fatal("accepted delta before a checkpoint")
	}
	if _, _, _, err := parseJournal(append(bytes.Clone(initial), initial[len(journalMagic):]...)); err == nil {
		t.Fatal("accepted checkpoint in append history")
	}
}

func TestJournalPartialTailPreservesPriorCharges(t *testing.T) {
	initial := encodedJournalFixture(t)
	s := snapshotFixture()
	s.advance(time.Unix(s.Clock+1, 0))
	a := s.start(15, publicationPeer(0), []peer.ID{publicationPeer(2)}, 4096)
	if a == nil {
		t.Fatal("missing second attempt")
	}
	f, err := encodeEntry(s, false, nil)
	if err != nil {
		t.Fatal(err)
	}
	for n := 0; n < len(f); n++ {
		b := append(bytes.Clone(initial), f[:n]...)
		restored, size, frames, err := parseJournal(b)
		if err != nil || size != len(initial) || frames != 1 {
			t.Fatalf("partial tail %d: %v", n, err)
		}
		assertSameState(t, snapshotFixture(), restored)
	}
	complete := append(bytes.Clone(initial), f...)
	restored, size, frames, err := parseJournal(complete)
	if err != nil || size != len(complete) || frames != 2 {
		t.Fatal(err)
	}
	assertSameState(t, s, restored)
	for _, n := range []int{len(initial), len(initial) + 4, len(initial) + 8, len(complete) - 1} {
		bad := bytes.Clone(complete)
		bad[n] ^= 1
		if _, _, _, err := parseJournal(bad); err == nil {
			t.Fatalf("accepted damaged complete frame at %d", n)
		}
	}
}

func TestJournalReopenTruncatesPartialAppendAndCompacts(t *testing.T) {
	root := t.TempDir()
	o, s, err := openOutbox(root, time.Unix(100000, 0))
	if err != nil {
		t.Fatal(err)
	}
	s.observe(publicationDigest(1, 32), association(1, workspace.Canonical), true)
	if err := o.Save(s); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < maxJournalFrames+2; i++ {
		s.advance(time.Unix(s.Clock+1, 0))
		if err := o.Save(s); err != nil {
			t.Fatal(err)
		}
	}
	if o.frames != 3 {
		t.Fatalf("checkpoint was not compacted: %d frames", o.frames)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(root, "state.log")
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte("torn")); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, restored, err := openOutbox(root, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	assertSameState(t, s, restored)
	info, err := os.Stat(path)
	if err != nil || info.Size() != int64(reopened.size) {
		t.Fatal("partial append not truncated")
	}
	restored.advance(time.Unix(restored.Clock+1, 0))
	if err := reopened.Save(restored); err != nil {
		t.Fatal(err)
	}
}

func TestPropertyJournalReplayMatchesState(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		s := newState(time.Unix(100000, 0))
		checkpoint, err := encodeEntry(s, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		log := append([]byte(journalMagic), checkpoint...)
		known := make(map[cas.Digest]jobStamp)
		for step := 0; step < 30; step++ {
			s.advance(time.Unix(s.Clock+int64(rapid.IntRange(0, 7200).Draw(t, "seconds")), 0))
			d := publicationDigest(rapid.IntRange(0, 15).Draw(t, "blob"), 256)
			s.observe(d, association(byte(step), workspace.Canonical), rapid.Bool().Draw(t, "ready"))
			if a := s.start(15, publicationPeer(0), []peer.ID{publicationPeer(1), publicationPeer(2)}, 1<<20); a != nil {
				s.finish(*a, rapid.Bool().Draw(t, "complete"), false)
			}
			delta := *s
			delta.Jobs = make(map[cas.Digest]*job)
			var deleted []cas.Digest
			for d, j := range s.Jobs {
				if old, ok := known[d]; !ok || old != stamp(j) {
					delta.Jobs[d] = j
					known[d] = stamp(j)
				}
			}
			for d := range known {
				if s.Jobs[d] == nil {
					deleted = append(deleted, d)
					delete(known, d)
				}
			}
			frame, err := encodeEntry(&delta, false, deleted)
			if err != nil {
				t.Fatal(err)
			}
			log = append(log, frame...)
			restored, size, frames, err := parseJournal(log)
			if err != nil || size != len(log) || frames != step+2 {
				t.Fatalf("replay failed: %v", err)
			}
			a, _ := marshalState(s)
			b, _ := marshalState(restored)
			if !bytes.Equal(a, b) {
				t.Fatal("journal did not replay state")
			}
		}
	})
}

func TestJournalRejectsLiveJobAndBudgetRefunds(t *testing.T) {
	initial := encodedJournalFixture(t)
	d := publicationDigest(1, 1024)
	for _, name := range []string{"early window rollover", "attempt reset", "receipt loss", "peer replacement", "creation reset", "early deletion"} {
		t.Run(name, func(t *testing.T) {
			next := snapshotFixture()
			var deleted []cas.Digest
			switch name {
			case "early window rollover":
				next.Clock++
				next.Window++
				next.Bytes = 0
				next.Offers = 0
			case "attempt reset":
				next.Jobs[d].Attempts = nil
				next.Jobs[d].Completed = nil
			case "receipt loss":
				next.Jobs[d].Completed = nil
			case "peer replacement":
				next.Jobs[d].Attempts = []peer.ID{publicationPeer(9)}
				next.Jobs[d].Completed = []peer.ID{publicationPeer(9)}
			case "creation reset":
				next.Clock++
				next.Jobs[d].Created++
				next.Jobs[d].Due = next.Clock
			case "early deletion":
				delete(next.Jobs, d)
				deleted = []cas.Digest{d}
			}
			frame, err := encodeEntry(next, false, deleted)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := parseJournal(append(bytes.Clone(initial), frame...)); err == nil {
				t.Fatal("accepted a valid-checksum journal entry that refunds live accounting")
			}
		})
	}
}

func TestJournalAllowsFreshIntentAfterOriginalLifetime(t *testing.T) {
	initial := encodedJournalFixture(t)
	next := snapshotFixture()
	next.advance(time.Unix(next.Clock+int64(jobLifetime/time.Second), 0))
	recreated := publicationDigest(1, 1024)
	if !next.observe(recreated, association(4, workspace.Canonical), true) {
		t.Fatal("new post-expiry intent rejected")
	}
	frame, err := encodeEntry(next, false, []cas.Digest{publicationDigest(2, 2048)})
	if err != nil {
		t.Fatal(err)
	}
	got, _, _, err := parseJournal(append(bytes.Clone(initial), frame...))
	if err != nil {
		t.Fatal(err)
	}
	assertSameState(t, next, got)
}

func TestJournalBoundsCompleteFrameCount(t *testing.T) {
	initial := encodedJournalFixture(t)
	frame, err := encodeEntry(snapshotFixture(), false, nil)
	if err != nil {
		t.Fatal(err)
	}
	log := bytes.Clone(initial)
	for range maxJournalFrames {
		log = append(log, frame...)
	}
	if _, _, _, err := parseJournal(log); err == nil {
		t.Fatal("journal accepted too many complete frames")
	}
}

func TestJournalReplayEnforcesMergedQueueBounds(t *testing.T) {
	for _, tc := range []struct {
		name      string
		count     int
		size      int64
		completed bool
	}{
		{"pending jobs", maxPending, 1, false},
		{"queued bytes", 2, 8 << 30, false},
		{"retained records", maxRecords, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newState(time.Unix(100000, 0))
			for i := range tc.count {
				d := publicationDigest(i, tc.size)
				j := &job{Digest: d, Created: s.Clock, Due: s.Clock, Kinds: 1}
				if tc.completed {
					j.Attempts = []peer.ID{publicationPeer(1), publicationPeer(2)}
					j.Completed = []peer.ID{publicationPeer(1), publicationPeer(2)}
				}
				s.Jobs[d] = j
			}
			checkpoint, err := encodeEntry(s, true, nil)
			if err != nil {
				t.Fatal(err)
			}
			log := append([]byte(journalMagic), checkpoint...)
			if _, _, _, err := parseJournal(log); err != nil {
				t.Fatalf("valid boundary checkpoint: %v", err)
			}
			delta := newState(time.Unix(s.Clock, 0))
			if !delta.observe(publicationDigest(tc.count, tc.size), association(1, workspace.Canonical), true) {
				t.Fatal("isolated delta unexpectedly exceeds bounds")
			}
			frame, err := encodeEntry(delta, false, nil)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, _, err := decodeEntry(frame[frameHeaderBytes:]); err != nil {
				t.Fatalf("valid isolated delta: %v", err)
			}
			if _, _, _, err := parseJournal(append(log, frame...)); err == nil {
				t.Fatal("individually bounded delta exceeded merged state limit")
			}
		})
	}
}

func TestJournalRejectsInvalidCompleteFrameLengths(t *testing.T) {
	initial := encodedJournalFixture(t)
	for _, size := range []uint32{0, maxSnapshotBytes + 1, math.MaxUint32} {
		frame := make([]byte, frameHeaderBytes)
		binary.BigEndian.PutUint32(frame, size)
		binary.BigEndian.PutUint32(frame[4:], ^size)
		if _, _, _, err := parseJournal(append(bytes.Clone(initial), frame...)); err == nil {
			t.Fatalf("accepted invalid declared length %d as a recoverable partial append", size)
		}
	}
	if _, _, _, err := parseJournal(make([]byte, maxJournalBytes+1)); err == nil {
		t.Fatal("accepted journal larger than its byte limit")
	}
}

func TestOutboxRejectsOversizedFileWithoutReset(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "state.log")
	initial := encodedJournalFixture(t)
	if err := os.WriteFile(path, initial, 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, maxJournalBytes+1); err != nil {
		t.Fatal(err)
	}
	if o, _, err := openOutbox(root, time.Unix(100000, 0)); err == nil {
		_ = o.Close()
		t.Fatal("accepted oversized on-disk journal")
	}
	if info, err := os.Stat(path); err != nil || info.Size() != maxJournalBytes+1 {
		t.Fatal("failed open modified oversized journal")
	}
	if err := os.Truncate(path, int64(len(initial))); err != nil {
		t.Fatal(err)
	}
	o, restored, err := openOutbox(root, time.Unix(100000, 0))
	if err != nil {
		t.Fatalf("size rejection leaked outbox lock: %v", err)
	}
	defer o.Close()
	assertSameState(t, snapshotFixture(), restored)
}

func FuzzPublicationJournal(f *testing.F) {
	f.Add(encodedJournalFixture(f))
	f.Add([]byte(journalMagic))
	f.Add(append([]byte(journalMagic), checkedJournalFrame([]byte(`{"reset":true}`))...))
	entry := journalProtoFixture(f)
	entry.Kind = publicationpb.RecordKind_RECORD_KIND_CHECKPOINT
	payload := marshalProto(f, entry)
	f.Add(payload)
	validFrame := checkedJournalFrame(payload)
	f.Add(append([]byte(journalMagic), validFrame...))
	entry.State.Clock = nil
	f.Add(append([]byte(journalMagic), checkedJournalFrame(marshalProto(f, entry))...))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 64<<10 {
			return
		}
		// Raw entry seeds exercise protobuf validation even when a mutation
		// cannot preserve a complete journal frame's checksum.
		if s, checkpoint, deleted, err := decodeEntry(b); err == nil {
			frame, err := encodeEntry(s, checkpoint, deleted)
			if err != nil || !bytes.Equal(frame[frameHeaderBytes:], b) {
				t.Fatal("entry decoder accepted noncanonical protobuf")
			}
			assertStateBounds(t, s, "")
		}
		s, offset, frames, err := parseJournal(b)
		if err != nil {
			return
		}
		if offset > len(b) || frames < 1 {
			t.Fatal("invalid replay bounds")
		}
		assertStateBounds(t, s, "")
		full, err := encodeEntry(s, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		again, _, _, err := parseJournal(append([]byte(journalMagic), full...))
		if err != nil {
			t.Fatal(err)
		}
		assertSameState(t, s, again)
	})
}
