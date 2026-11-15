package publication

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/workspace"
	"github.com/libp2p/go-libp2p/core/peer"
)

// Fixtures use valid public peer IDs and synthetic digests, without reading
// blobs or generating private keys. All fixture construction is outside timing.
func benchmarkPublicationState(records int, completed bool) *state {
	s := newState(time.Unix(1700000000, 0))
	peers := []peer.ID{publicationPeer(1), publicationPeer(2)}
	for i := range records {
		d := publicationDigest(i, cas.MinChunkSize)
		j := &job{Digest: d, Created: s.Clock, Due: s.Clock, Ready: true,
			Kinds: 1 << uint(workspace.Canonical), Attempts: make([]peer.ID, 0, maxAttempts)}
		j.Partitions[workspace.Canonical][0] = byte(i % fairBuckets)
		if completed {
			j.Attempts = append(j.Attempts, peers...)
			j.Completed = append(j.Completed, peers...)
		}
		s.Jobs[d] = j
	}
	return s
}

func benchmarkJournalState() (*state, *job) {
	s := benchmarkPublicationState(maxRecords, true)
	j := s.Jobs[publicationDigest(maxRecords-1, cas.MinChunkSize)]
	j.Attempts, j.Completed = nil, nil
	return s, j
}

func benchmarkOutbox(b *testing.B, s *state) *diskOutbox {
	b.Helper()
	o, _, err := openOutbox(b.TempDir(), time.Unix(s.Clock, 0))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := o.Close(); err != nil {
			b.Error(err)
		}
	})
	return o
}

func checkBenchmarkJournal(b *testing.B, o *diskOutbox) {
	b.Helper()
	if o.size > maxJournalBytes || o.frames < 1 || o.frames > maxJournalFrames {
		b.Fatalf("journal exceeded bounds: %d bytes, %d frames", o.size, o.frames)
	}
}

func BenchmarkPublicationSnapshot8192(b *testing.B) {
	s := benchmarkPublicationState(maxRecords, true)
	encoded, err := marshalState(s)
	if err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		got, err := marshalState(s)
		if err != nil || len(got) != len(encoded) {
			b.Fatalf("snapshot marshal: %v", err)
		}
	}
	b.ReportMetric(float64(len(encoded)), "encoded-B/op")
}

// Only Save is timed; directory creation, locking and resetting the fixture
// are excluded. Save includes the complete checkpoint write and fsync.
func BenchmarkPublicationInitialSave8192(b *testing.B) {
	s := benchmarkPublicationState(maxRecords, true)
	o := benchmarkOutbox(b, s)
	var written int
	b.ReportAllocs()
	for b.Loop() {
		if err := o.Save(s); err != nil {
			b.Fatal(err)
		}
		checkBenchmarkJournal(b, o)
		written = o.size
		b.StopTimer()
		if err := os.Remove(filepath.Join(o.root, "state.log")); err != nil {
			b.Fatal(err)
		}
		o.size, o.frames = 0, 0
		o.previous = make(map[cas.Digest]jobStamp)
		b.StartTimer()
	}
	b.ReportMetric(float64(written), "written-B/op")
}

// One pending job changes readiness among 8,191 retained completed jobs.
// This includes comparison against every retained job and an append plus fsync.
// Checkpoint resets are excluded here and included in the amortized benchmark.
func BenchmarkPublicationDeltaSave8192(b *testing.B) {
	s, changed := benchmarkJournalState()
	o := benchmarkOutbox(b, s)
	if err := o.Save(s); err != nil {
		b.Fatal(err)
	}
	var written int64
	b.ReportAllocs()
	for b.Loop() {
		if o.frames == maxJournalFrames {
			b.StopTimer()
			o.frames = 0
			if err := o.Save(s); err != nil {
				b.Fatal(err)
			}
			b.StartTimer()
		}
		changed.Ready = !changed.Ready
		before := o.size
		if err := o.Save(s); err != nil {
			b.Fatal(err)
		}
		checkBenchmarkJournal(b, o)
		written += int64(o.size - before)
	}
	b.ReportMetric(float64(written)/float64(b.N), "written-B/op")
}

// Each operation is a complete 1,024-save cycle, guaranteeing one timed
// compaction even for -benchtime=1x. Custom metrics normalize cost per Save.
func BenchmarkPublicationJournalSaveAmortized8192(b *testing.B) {
	s, changed := benchmarkJournalState()
	o := benchmarkOutbox(b, s)
	if err := o.Save(s); err != nil {
		b.Fatal(err)
	}
	var written int64
	var compactions, peakBytes int
	b.ReportAllocs()
	for b.Loop() {
		for range maxJournalFrames {
			changed.Ready = !changed.Ready
			before := o.size
			if err := o.Save(s); err != nil {
				b.Fatal(err)
			}
			checkBenchmarkJournal(b, o)
			if o.frames == 1 {
				written += int64(o.size)
				compactions++
			} else {
				written += int64(o.size - before)
			}
			peakBytes = max(peakBytes, o.size)
		}
	}
	if compactions != b.N {
		b.Fatalf("got %d compactions for %d complete cycles", compactions, b.N)
	}
	saves := float64(b.N) * maxJournalFrames
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/saves, "ns/save")
	b.ReportMetric(float64(written)/saves, "written-B/save")
	b.ReportMetric(float64(peakBytes), "peak-journal-B")
	b.ReportMetric(float64(compactions)/float64(b.N), "compactions/op")
}

func benchmarkJournalBytes(b *testing.B, frameLimit, changedCount int) ([]byte, int) {
	b.Helper()
	s := benchmarkPublicationState(maxRecords, true)
	delta := *s
	delta.Jobs = make(map[cas.Digest]*job, changedCount)
	for i := range changedCount {
		j := s.Jobs[publicationDigest(maxRecords-1-i, cas.MinChunkSize)]
		j.Attempts, j.Completed = nil, nil
		delta.Jobs[j.Digest] = j
	}
	checkpoint, err := encodeEntry(s, true, nil)
	if err != nil {
		b.Fatal(err)
	}
	data := append([]byte(journalMagic), checkpoint...)
	frames := 1
	for frames < frameLimit {
		for _, j := range delta.Jobs {
			j.Ready = !j.Ready
		}
		frame, err := encodeEntry(&delta, false, nil)
		if err != nil {
			b.Fatal(err)
		}
		if len(frame) > maxJournalBytes-len(data) {
			break
		}
		data = append(data, frame...)
		frames++
	}
	if len(data) > maxJournalBytes {
		b.Fatal("recovery fixture exceeds journal byte limit")
	}
	return data, frames
}

// Recovery includes file reads, validation, replay, locking, and rebuilding the
// comparison cache. Repeated reads use a warm filesystem cache, not cold media.
func BenchmarkPublicationJournalRecovery8192(b *testing.B) {
	for _, tc := range []struct {
		name            string
		frames, changed int
	}{{"checkpoint", 1, 1}, {"1024_frames", maxJournalFrames, 1}, {"near_byte_limit", maxJournalFrames, 64}} {
		b.Run(tc.name, func(b *testing.B) {
			data, frames := benchmarkJournalBytes(b, tc.frames, tc.changed)
			root := b.TempDir()
			if err := os.WriteFile(filepath.Join(root, "state.log"), data, 0600); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for b.Loop() {
				o, s, err := openOutbox(root, time.Unix(1700000000, 0))
				if err != nil {
					b.Fatal(err)
				}
				if len(s.Jobs) != maxRecords || o.frames != frames {
					_ = o.Close()
					b.Fatal("recovery lost records or frames")
				}
				if err := o.Close(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(len(data)), "journal-B")
			b.ReportMetric(float64(frames), "frames")
		})
	}
}

func BenchmarkPublicationSelect1024Jobs16Peers(b *testing.B) {
	s := benchmarkPublicationState(maxPending, false)
	peers := make([]peer.ID, 16)
	for i := range peers {
		peers[i] = publicationPeer(i + 1)
	}
	self := publicationPeer(0)
	b.ReportAllocs()
	for b.Loop() {
		a := s.start(15, self, peers, 1<<30)
		if a == nil {
			b.Fatal("eligible scheduler fixture did not dispatch")
		}
		// Restore the selected job and budgets, retaining allocated capacity.
		// Resetting avoids measuring fixture construction or progressively idle work.
		j := s.Jobs[a.Digest]
		j.Attempts, j.Due = j.Attempts[:0], s.Clock
		s.Served[j.Partitions[workspace.Canonical][0]] = 0
		s.running = nil
		s.Bytes, s.Offers, s.NextStart = 0, 0, 0
	}
}
