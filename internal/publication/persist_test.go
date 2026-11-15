package publication

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/workspace"
	"github.com/libp2p/go-libp2p/core/peer"
	"pgregory.net/rapid"
)

func snapshotFixture() *state {
	s := newState(time.Unix(100000, 0))
	s.observe(publicationDigest(1, 1024), association(1, workspace.Canonical), true)
	s.observe(publicationDigest(2, 2048), association(2, workspace.Main), false)
	a := s.start(15, publicationPeer(0), []peer.ID{publicationPeer(1)}, 4096)
	s.finish(*a, true, false)
	return s
}

func encodedFixture(t testing.TB) []byte {
	t.Helper()
	b, err := marshalState(snapshotFixture())
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestSnapshotRejectsInvalidAndNoncanonicalData(t *testing.T) {
	fixture := encodedFixture(t)
	mutations := map[string]func(*snapshot){
		"version":                func(s *snapshot) { s.Version++ },
		"negative clock":         func(s *snapshot) { s.Clock = -1 },
		"clock overflow":         func(s *snapshot) { s.Clock = math.MaxInt64 },
		"future budget window":   func(s *snapshot) { s.Window = s.Clock + 1 },
		"negative bytes":         func(s *snapshot) { s.Bytes = -1 },
		"too many offers":        func(s *snapshot) { s.Offers = maxOffersPerHour + 1 },
		"unbounded cooldown":     func(s *snapshot) { s.NextStart = s.Clock + 2 },
		"duplicate job":          func(s *snapshot) { s.Jobs = append(s.Jobs, s.Jobs[0]) },
		"uppercase hash":         func(s *snapshot) { s.Jobs[0].Hash = strings.ToUpper(s.Jobs[0].Hash) },
		"future creation":        func(s *snapshot) { s.Jobs[0].Created = s.Clock + 1 },
		"past due":               func(s *snapshot) { s.Jobs[0].Due = s.Jobs[0].Created - 1 },
		"unbounded due":          func(s *snapshot) { s.Jobs[0].Due = s.Clock + 3601 },
		"no association":         func(s *snapshot) { s.Jobs[0].Kinds = 0 },
		"unknown class bits":     func(s *snapshot) { s.Jobs[0].Kinds = 16 },
		"bad partition":          func(s *snapshot) { s.Jobs[0].Partitions[0] = "bad" },
		"partition without kind": func(s *snapshot) { s.Jobs[0].Partitions[3] = strings.Repeat("01", 32) },
		"invalid peer":           func(s *snapshot) { s.Jobs[0].Attempts = []string{"not-a-peer"} },
		"per peer attempt limit": func(s *snapshot) {
			s.Jobs[0].Attempts = []string{publicationPeer(1).String(), publicationPeer(1).String(), publicationPeer(1).String()}
		},
		"total attempt limit": func(s *snapshot) {
			s.Jobs[0].Attempts = nil
			for i := range maxAttempts + 1 {
				s.Jobs[0].Attempts = append(s.Jobs[0].Attempts, publicationPeer(i+1).String())
			}
		},
		"unattempted receipt": func(s *snapshot) { s.Jobs[0].Completed = []string{publicationPeer(99).String()} },
		"duplicate receipt": func(s *snapshot) {
			s.Jobs[0].Attempts = []string{publicationPeer(1).String()}
			s.Jobs[0].Completed = []string{publicationPeer(1).String(), publicationPeer(1).String()}
		},
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			var value snapshot
			if err := json.Unmarshal(fixture, &value); err != nil {
				t.Fatal(err)
			}
			mutate(&value)
			bad, err := json.Marshal(value)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := parseState(bad); err == nil {
				t.Fatal("accepted invalid snapshot")
			}
		})
	}
	var reordered snapshot
	if err := json.Unmarshal(fixture, &reordered); err != nil {
		t.Fatal(err)
	}
	reordered.Jobs[0], reordered.Jobs[1] = reordered.Jobs[1], reordered.Jobs[0]
	reorderedBytes, err := json.Marshal(reordered)
	if err != nil {
		t.Fatal(err)
	}
	for name, bad := range map[string][]byte{
		"duplicate field":    bytes.Replace(fixture, []byte(`"version":1`), []byte(`"version":1,"version":1`), 1),
		"missing field":      bytes.Replace(fixture, []byte(`"version":1,`), nil, 1),
		"missing zero field": bytes.Replace(fixture, []byte(`"ready":false,`), nil, 1),
		"unknown field":      bytes.Replace(fixture, []byte(`"version":1`), []byte(`"version":1,"extra":0`), 1),
		"whitespace":         append([]byte(" "), fixture...),
		"trailing value":     append(append([]byte{}, fixture...), []byte("{}")...),
		"reordered jobs":     reorderedBytes,
		"truncated":          fixture[:len(fixture)-1],
		"deep arrays":        []byte(strings.Repeat("[", 64) + strings.Repeat("]", 64)),
		"overlong string":    bytes.Replace(fixture, []byte(`"hash":"`), []byte(`"hash":"`+strings.Repeat("a", 4096)), 1),
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseState(bad); err == nil {
				t.Fatal("accepted noncanonical snapshot")
			}
		})
	}
}

func TestSnapshotQueueAndRecordLimits(t *testing.T) {
	for _, tc := range []struct {
		name  string
		count int
		size  int64
	}{
		{"pending", maxPending + 1, 1}, {"queued bytes", 3, 8 << 30}, {"records", maxRecords + 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newState(time.Unix(100000, 0))
			for i := range tc.count {
				d := publicationDigest(i, tc.size)
				s.Jobs[d] = &job{Digest: d, Created: s.Clock, Due: s.Clock, Kinds: 1}
				if tc.name == "records" {
					s.Jobs[d].Attempts = []peer.ID{publicationPeer(1), publicationPeer(2)}
					s.Jobs[d].Completed = []peer.ID{publicationPeer(1), publicationPeer(2)}
				}
			}
			encoded, err := marshalState(s)
			if err == nil {
				_, err = parseState(encoded)
			}
			if err == nil {
				t.Fatal("accepted state exceeding declared bounds")
			}
		})
	}
	if _, err := parseState(bytes.Repeat([]byte(" "), maxSnapshotBytes+1)); err == nil {
		t.Fatal("accepted oversized snapshot")
	}
}

func TestPropertySnapshotEncodingIgnoresMapInsertionOrder(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		first, second := newState(time.Unix(100000, 0)), newState(time.Unix(100000, 0))
		count := rapid.IntRange(1, 24).Draw(t, "jobs")
		order := make([]int, count)
		for i := range order {
			order[i] = i
			first.observe(publicationDigest(i, int64(i+1)), association(byte(i), workspace.Canonical), i%2 == 0)
		}
		for i := range order {
			other := rapid.IntRange(0, count-1).Draw(t, "swap")
			order[i], order[other] = order[other], order[i]
		}
		for _, i := range order {
			second.observe(publicationDigest(i, int64(i+1)), association(byte(i), workspace.Canonical), i%2 == 0)
		}
		a, err := marshalState(first)
		if err != nil {
			t.Fatal(err)
		}
		b, err := marshalState(second)
		if err != nil || !bytes.Equal(a, b) {
			t.Fatal("map insertion order changed snapshot")
		}
		parsed, err := parseState(a)
		if err != nil {
			t.Fatal(err)
		}
		b, err = marshalState(parsed)
		if err != nil || !bytes.Equal(a, b) {
			t.Fatal("canonical snapshot did not round trip")
		}
	})
}

func TestOutboxLockAndInterruptedAttemptReopen(t *testing.T) {
	root := t.TempDir()
	now := time.Unix(100000, 0)
	o, s, err := openOutbox(root, now)
	if err != nil {
		t.Fatal(err)
	}
	defer o.Close()
	if other, _, err := openOutbox(root, now); err == nil {
		_ = other.Close()
		t.Fatal("second writer obtained outbox lock")
	}
	d := publicationDigest(1, 1024)
	s.observe(d, association(1, workspace.Canonical), true)
	if s.start(15, publicationPeer(0), []peer.ID{publicationPeer(1)}, 4096) == nil {
		t.Fatal("missing fixture attempt")
	}
	if err := o.Save(s); err != nil {
		t.Fatal(err)
	}
	if err := o.Close(); err != nil {
		t.Fatal(err)
	}
	// A newer uninstalled temporary snapshot cannot replace committed state.
	if err := os.WriteFile(filepath.Join(root, "state.tmp"), []byte("interrupted replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	reopened, restored, err := openOutbox(root, time.Unix(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if restored.Bytes != 1024 || restored.Offers != 1 || len(restored.Jobs[d].Attempts) != 1 || restored.running != nil {
		t.Fatal("reopen lost interrupted attempt charge")
	}
	if _, err := os.Stat(filepath.Join(root, "state.tmp")); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("orphan temporary snapshot was retained")
	}
	restored.advance(time.Unix(1, 0))
	if restored.Bytes != s.Bytes || restored.Clock != s.Clock {
		t.Fatal("restart wall-clock rollback reset state")
	}
	if err := reopened.Save(restored); err != nil {
		t.Fatal(err)
	}
	assertStateBounds(t, restored, publicationPeer(0))
}

func TestOutboxRejectsCorruptionWithoutResetAndReleasesLock(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "state.log")
	bad := []byte(`{"version":1,"jobs":"corrupted"}`)
	if err := os.WriteFile(path, bad, 0600); err != nil {
		t.Fatal(err)
	}
	if o, _, err := openOutbox(root, time.Unix(100000, 0)); err == nil {
		_ = o.Close()
		t.Fatal("corrupt snapshot silently reset")
	}
	if got, err := os.ReadFile(path); err != nil || !bytes.Equal(got, bad) {
		t.Fatal("corrupt snapshot was overwritten")
	}
	if err := os.WriteFile(path, encodedJournalFixture(t), 0600); err != nil {
		t.Fatal(err)
	}
	o, _, err := openOutbox(root, time.Unix(100000, 0))
	if err != nil {
		t.Fatalf("error leaked outbox lock: %v", err)
	}
	_ = o.Close()
}

type restoredIdentitySource struct{}

func (restoredIdentitySource) Has(context.Context, cas.Digest) (bool, error) { return true, nil }

type restoredIdentityTransport struct{ id peer.ID }

func (t restoredIdentityTransport) ID() peer.ID        { return t.id }
func (restoredIdentityTransport) SeedPeers() []peer.ID { return nil }
func (restoredIdentityTransport) Offer(context.Context, peer.ID, cas.Digest) (bool, error) {
	return false, nil
}

func TestRestoredReceiptsCannotCountCurrentProducer(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "state.log"), encodedJournalFixture(t), 0600); err != nil {
		t.Fatal(err)
	}
	p, err := New(t.Context(), root, restoredIdentitySource{}, restoredIdentityTransport{id: publicationPeer(1)}, Options{Policy: 15})
	if err == nil {
		_ = p.Close()
		t.Fatal("restored producer identity counted as a remote receipt")
	}
	o, _, err := openOutbox(root, time.Unix(100000, 0))
	if err != nil {
		t.Fatalf("identity rejection leaked snapshot lock: %v", err)
	}
	_ = o.Close()
}

func TestOutboxRejectsLinksAndUnexpectedEntries(t *testing.T) {
	for _, name := range []string{"root", "outbox.lock", "state.log", "state.tmp"} {
		t.Run(name, func(t *testing.T) {
			parent := t.TempDir()
			root, outside := filepath.Join(parent, "outbox"), filepath.Join(parent, "outside")
			if err := os.Mkdir(outside, 0700); err != nil {
				t.Fatal(err)
			}
			marker := filepath.Join(outside, "keep")
			contents := []byte("outside state must remain unchanged")
			if err := os.WriteFile(marker, contents, 0600); err != nil {
				t.Fatal(err)
			}
			link, target := root, outside
			if name != "root" {
				if err := os.Mkdir(root, 0700); err != nil {
					t.Fatal(err)
				}
				link, target = filepath.Join(root, name), marker
			}
			if err := os.Symlink(target, link); err != nil {
				if runtime.GOOS == "windows" {
					t.Skipf("symlinks unavailable: %v", err)
				}
				t.Fatal(err)
			}
			if o, _, err := openOutbox(root, time.Unix(100000, 0)); err == nil {
				_ = o.Close()
				t.Fatal("followed a reserved symlink")
			}
			if got, err := os.ReadFile(marker); err != nil || !bytes.Equal(got, contents) {
				t.Fatal("wrote through reserved symlink")
			}
			if entries, err := os.ReadDir(outside); err != nil || len(entries) != 1 {
				t.Fatal("created files outside outbox")
			}
			if err := os.Remove(link); err != nil {
				t.Fatal(err)
			}
			o, _, err := openOutbox(root, time.Unix(100000, 0))
			if err != nil {
				t.Fatalf("link rejection leaked lock: %v", err)
			}
			_ = o.Close()
		})
	}
	root := t.TempDir()
	foreign := filepath.Join(root, "unrelated")
	if err := os.WriteFile(foreign, []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if o, _, err := openOutbox(root, time.Unix(100000, 0)); err == nil {
		_ = o.Close()
		t.Fatal("accepted unrelated files")
	}
	if err := os.Remove(foreign); err != nil {
		t.Fatal(err)
	}
	o, _, err := openOutbox(root, time.Unix(100000, 0))
	if err != nil {
		t.Fatal("unrelated file error leaked lock")
	}
	_ = o.Close()
}

func TestOutboxSaveRejectsReservedSymlinks(t *testing.T) {
	for _, name := range []string{"state.log", "state.tmp"} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			o, s, err := openOutbox(root, time.Unix(100000, 0))
			if err != nil {
				t.Fatal(err)
			}
			defer o.Close()
			outside := filepath.Join(t.TempDir(), "keep")
			original := []byte("outside bytes")
			if err := os.WriteFile(outside, original, 0600); err != nil {
				t.Fatal(err)
			}
			if err := os.Symlink(outside, filepath.Join(root, name)); err != nil {
				if runtime.GOOS == "windows" {
					t.Skipf("symlinks unavailable: %v", err)
				}
				t.Fatal(err)
			}
			if err := o.Save(s); err == nil {
				t.Fatal("save accepted reserved symlink")
			}
			if got, err := os.ReadFile(outside); err != nil || !bytes.Equal(got, original) {
				t.Fatal("save wrote through a symlink")
			}
		})
	}
}

func FuzzPublicationSnapshot(f *testing.F) {
	valid, err := marshalState(snapshotFixture())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(valid)
	f.Add([]byte(`{"version":1}`))
	f.Add(valid[:len(valid)/2])
	f.Add(append(append([]byte{}, valid...), valid...))
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 64<<10 {
			return
		}
		s, err := parseState(b)
		if err != nil {
			return
		}
		canonical, err := marshalState(s)
		if err != nil || !bytes.Equal(canonical, b) {
			t.Fatal("decoder accepted noncanonical state")
		}
		assertStateBounds(t, s, "")
	})
}
