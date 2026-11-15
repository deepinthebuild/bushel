package publication

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/workspace"
	"github.com/libp2p/go-libp2p/core/peer"
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
			s.advance(time.Unix(s.Clock+int64(rapid.IntRange(0, 3600).Draw(t, "seconds")), 0))
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

func FuzzPublicationJournal(f *testing.F) {
	f.Add(encodedJournalFixture(f))
	f.Add([]byte(journalMagic))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 64<<10 {
			return
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
