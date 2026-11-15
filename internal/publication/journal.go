package publication

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	publicationpb "github.com/deepinthebuild/bushel/internal/publication/pb"
	"github.com/gofrs/flock"
	"github.com/libp2p/go-libp2p/core/peer"
	"google.golang.org/protobuf/proto"
)

const (
	journalMagic     = "BUSHELJ1"
	frameHeaderBytes = 8 + sha256.Size
	maxJournalBytes  = 32 << 20
	maxJournalFrames = 1024
)

type jobStamp struct {
	Created, Due int64
	Ready        bool
	Kinds        uint8
	Partitions   [4][32]byte
	Attempts     [maxAttempts]peer.ID
	Completed    [ReplicaTarget]peer.ID
}

func stamp(j *job) jobStamp {
	v := jobStamp{Created: j.Created, Due: j.Due, Ready: j.Ready, Kinds: j.Kinds, Partitions: j.Partitions}
	copy(v.Attempts[:], j.Attempts)
	copy(v.Completed[:], j.Completed)
	return v
}

func encodeEntry(s *state, reset bool, deleted []cas.Digest) ([]byte, error) {
	v, err := snapshotMessage(s)
	if err != nil {
		return nil, err
	}
	if len(deleted) > maxRecords {
		return nil, errors.New("too many publication journal deletions")
	}
	e := &publicationpb.JournalEntry{Kind: publicationpb.RecordKind_RECORD_KIND_DELTA, State: v}
	if reset {
		e.Kind = publicationpb.RecordKind_RECORD_KIND_CHECKPOINT
	}
	for _, d := range deleted {
		e.Deleted = append(e.Deleted, digestMessage(d))
	}
	slices.SortFunc(e.Deleted, compareDigestMessages)
	size := proto.Size(e)
	if size > maxSnapshotBytes {
		return nil, errors.New("publication journal entry exceeds limit")
	}
	frame, err := (proto.MarshalOptions{Deterministic: true}).MarshalAppend(make([]byte, frameHeaderBytes, frameHeaderBytes+size), e)
	if err != nil {
		return nil, err
	}
	payload := frame[frameHeaderBytes:]
	binary.BigEndian.PutUint32(frame, uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:], ^uint32(len(payload)))
	sum := sha256.Sum256(payload)
	copy(frame[8:], sum[:])
	return frame, nil
}

func decodeEntry(payload []byte) (*state, bool, []cas.Digest, error) {
	if len(payload) > maxSnapshotBytes {
		return nil, false, nil, errors.New("publication journal entry exceeds limit")
	}
	if err := checkProtoBounds(payload, entryWire); err != nil {
		return nil, false, nil, err
	}
	var e publicationpb.JournalEntry
	if err := (proto.UnmarshalOptions{RecursionLimit: 8}).Unmarshal(payload, &e); err != nil {
		return nil, false, nil, err
	}
	if e.Kind != publicationpb.RecordKind_RECORD_KIND_CHECKPOINT && e.Kind != publicationpb.RecordKind_RECORD_KIND_DELTA {
		return nil, false, nil, errors.New("invalid publication journal record kind")
	}
	s, err := restoreSnapshot(e.State)
	if err != nil {
		return nil, false, nil, err
	}
	var deleted []cas.Digest
	for _, r := range e.Deleted {
		d, err := restoreDigest(r)
		if err != nil || s.Jobs[d] != nil || len(deleted) > 0 && compareDigests(deleted[len(deleted)-1], d) >= 0 {
			return nil, false, nil, errors.New("invalid journal deletion")
		}
		deleted = append(deleted, d)
	}
	checkpoint := e.Kind == publicationpb.RecordKind_RECORD_KIND_CHECKPOINT
	if checkpoint && len(deleted) != 0 {
		return nil, false, nil, errors.New("publication checkpoint contains deletions")
	}
	return s, checkpoint, deleted, nil
}

// The returned offset is the end of the last complete durable frame. A partial
// final frame was never eligible for dispatch: Save must finish before Offer.
// A complete frame with a bad checksum or invalid state fails closed.
func parseJournal(b []byte) (*state, int, int, error) {
	if len(b) > maxJournalBytes || !bytes.HasPrefix(b, []byte(journalMagic)) {
		return nil, 0, 0, errors.New("invalid publication journal header")
	}
	var s *state
	offset, frames := len(journalMagic), 0
	for offset < len(b) {
		remaining := b[offset:]
		if len(remaining) < frameHeaderBytes {
			break
		}
		size := int64(binary.BigEndian.Uint32(remaining))
		if uint32(size) != ^binary.BigEndian.Uint32(remaining[4:]) || size == 0 || size > maxSnapshotBytes {
			return nil, 0, 0, errors.New("invalid publication frame length")
		}
		if size > int64(len(remaining)-frameHeaderBytes) {
			break
		}
		payload := remaining[frameHeaderBytes : frameHeaderBytes+int(size)]
		sum := sha256.Sum256(payload)
		if !bytes.Equal(sum[:], remaining[8:frameHeaderBytes]) {
			return nil, 0, 0, errors.New("publication journal checksum mismatch")
		}
		next, reset, deleted, err := decodeEntry(payload)
		if err != nil {
			return nil, 0, 0, err
		}
		if reset != (s == nil) {
			return nil, 0, 0, errors.New("invalid publication checkpoint position")
		}
		if s != nil {
			if next.Clock < s.Clock || next.Window < s.Window || next.Expired < s.Expired || next.NextStart < s.NextStart || (next.Window == s.Window && (next.Bytes < s.Bytes || next.Offers < s.Offers)) {
				return nil, 0, 0, errors.New("publication journal refunds accounting")
			}
			if next.Window != s.Window && next.Window-s.Window < int64(time.Hour/time.Second) {
				return nil, 0, 0, errors.New("publication journal resets budget before hour elapsed")
			}
			for i, credit := range next.Served {
				if credit < s.Served[i] {
					return nil, 0, 0, errors.New("publication journal refunds service credit")
				}
			}
			jobs := s.Jobs
			for _, d := range deleted {
				if jobs[d] == nil {
					return nil, 0, 0, errors.New("publication journal deletes absent job")
				}
				if next.Clock < jobs[d].Created+int64(jobLifetime/time.Second) {
					return nil, 0, 0, errors.New("publication journal deletes retained history")
				}
				delete(jobs, d)
			}
			for d, j := range next.Jobs {
				if old := jobs[d]; old != nil && !validJobUpdate(old, j, next.Clock) {
					return nil, 0, 0, errors.New("publication journal rewrites retained history")
				}
				jobs[d] = j
			}
			next.Jobs = jobs
		}
		s = next
		count, sizePending := s.pending()
		if len(s.Jobs) > maxRecords || count > maxPending || sizePending > maxQueuedBytes {
			return nil, 0, 0, errors.New("publication journal exceeds queue limits")
		}
		offset += frameHeaderBytes + int(size)
		frames++
		if frames > maxJournalFrames {
			return nil, 0, 0, errors.New("too many publication journal frames")
		}
	}
	if s == nil {
		return nil, 0, 0, errors.New("missing complete publication checkpoint")
	}
	return s, offset, frames, nil
}

func validJobUpdate(old, next *job, now int64) bool {
	if next.Created != old.Created {
		return now >= old.Created+int64(jobLifetime/time.Second) && next.Created >= old.Created+int64(jobLifetime/time.Second)
	}
	if len(next.Attempts) < len(old.Attempts) || !slices.Equal(old.Attempts, next.Attempts[:len(old.Attempts)]) ||
		len(next.Completed) < len(old.Completed) || !slices.Equal(old.Completed, next.Completed[:len(old.Completed)]) || next.Kinds&old.Kinds != old.Kinds {
		return false
	}
	for kind, partition := range old.Partitions {
		if old.Kinds&(1<<uint(kind)) != 0 && bytes.Compare(next.Partitions[kind][:], partition[:]) > 0 {
			return false
		}
	}
	return true
}

type diskOutbox struct {
	root         string
	lock         *flock.Flock
	size, frames int
	previous     map[cas.Digest]jobStamp
}

func regularFile(path string) (bool, error) {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if !info.Mode().IsRegular() {
		return false, errors.New("publication state must be a regular file")
	}
	return true, nil
}

func openOutbox(root string, now time.Time) (_ *diskOutbox, _ *state, retErr error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, nil, err
	}
	info, err := os.Lstat(root)
	if err != nil {
		return nil, nil, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, nil, errors.New("publication directory must not be a link")
	}
	for _, name := range []string{"outbox.lock", "state.log", "state.tmp"} {
		if _, err := regularFile(filepath.Join(root, name)); err != nil {
			return nil, nil, err
		}
	}
	o := &diskOutbox{root: root, lock: flock.New(filepath.Join(root, "outbox.lock")), previous: make(map[cas.Digest]jobStamp)}
	locked, err := o.lock.TryLock()
	if err != nil {
		return nil, nil, err
	}
	if !locked {
		return nil, nil, errors.New("publication directory is already in use")
	}
	defer func() {
		if retErr != nil {
			_ = o.Close()
		}
	}()
	dir, err := os.Open(root)
	if err != nil {
		return nil, nil, err
	}
	entries, readErr := dir.ReadDir(4)
	closeErr := dir.Close()
	if (readErr != nil && readErr != io.EOF) || closeErr != nil {
		return nil, nil, errors.Join(readErr, closeErr)
	}
	for _, e := range entries {
		if e.Name() != "outbox.lock" && e.Name() != "state.log" && e.Name() != "state.tmp" {
			return nil, nil, errors.New("unrelated entry in publication directory")
		}
	}
	path := filepath.Join(root, "state.log")
	f, err := os.OpenFile(path, os.O_RDWR, 0600)
	var s *state
	if errors.Is(err, os.ErrNotExist) {
		s = newState(now)
	} else if err != nil {
		return nil, nil, err
	} else {
		b, readErr := readJournal(f)
		if readErr != nil {
			_ = f.Close()
			return nil, nil, readErr
		}
		s, o.size, o.frames, err = parseJournal(b)
		if err != nil {
			_ = f.Close()
			return nil, nil, fmt.Errorf("invalid publication state: %w", err)
		}
		if o.size != len(b) {
			err = f.Truncate(int64(o.size))
			if err == nil {
				err = f.Sync()
			}
		}
		if err = errors.Join(err, f.Close()); err != nil {
			return nil, nil, err
		}
		for d, j := range s.Jobs {
			o.previous[d] = stamp(j)
		}
	}
	if exists, err := regularFile(filepath.Join(root, "state.tmp")); err != nil {
		return nil, nil, err
	} else if exists {
		if err := os.Remove(filepath.Join(root, "state.tmp")); err != nil {
			return nil, nil, err
		}
	}
	return o, s, nil
}

// The journal is locked and bounded. Allocate its known length once instead of
// growing a buffer during recovery, and reject a concurrent size change.
func readJournal(f *os.File) ([]byte, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > maxJournalBytes {
		return nil, errors.New("invalid publication journal file size")
	}
	b := make([]byte, int(info.Size()))
	if _, err := io.ReadFull(f, b); err != nil {
		return nil, err
	}
	var extra [1]byte
	if n, err := f.Read(extra[:]); n != 0 || err != io.EOF {
		return nil, errors.New("publication journal changed during recovery")
	}
	return b, nil
}

func (o *diskOutbox) Save(s *state) error {
	info, err := os.Lstat(o.root)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("publication directory must not be a link")
	}
	for _, name := range []string{"state.log", "state.tmp"} {
		if _, err := regularFile(filepath.Join(o.root, name)); err != nil {
			return err
		}
	}
	checkpoint := o.frames == 0 || o.frames >= maxJournalFrames
	changed := s.Jobs
	var deleted []cas.Digest
	var frame []byte
	if !checkpoint {
		changed = make(map[cas.Digest]*job)
		for d, j := range s.Jobs {
			if old, ok := o.previous[d]; !ok || old != stamp(j) {
				changed[d] = j
			}
		}
		for d := range o.previous {
			if s.Jobs[d] == nil {
				deleted = append(deleted, d)
			}
		}
		delta := *s
		delta.Jobs = changed
		frame, err = encodeEntry(&delta, false, deleted)
		if err != nil {
			return err
		}
		checkpoint = len(frame) > maxJournalBytes-o.size
	}
	if checkpoint {
		frame, err = encodeEntry(s, true, nil)
		if err == nil {
			err = o.replace(frame)
		}
		if err != nil {
			return err
		}
		o.size, o.frames = len(journalMagic)+len(frame), 1
	} else {
		f, err := os.OpenFile(filepath.Join(o.root, "state.log"), os.O_WRONLY|os.O_APPEND, 0600)
		if err != nil {
			return err
		}
		info, err := f.Stat()
		if err == nil && (!info.Mode().IsRegular() || info.Size() != int64(o.size)) {
			err = errors.New("publication journal changed outside writer")
		}
		if err == nil {
			_, err = f.Write(frame)
		}
		if err == nil {
			err = f.Sync()
		}
		if err = errors.Join(err, f.Close()); err != nil {
			return err
		}
		o.size += len(frame)
		o.frames++
	}
	// Advance the comparison cache only after the durable write succeeds.
	if checkpoint {
		clear(o.previous)
		changed = s.Jobs
	}
	for d, j := range changed {
		o.previous[d] = stamp(j)
	}
	for _, d := range deleted {
		delete(o.previous, d)
	}
	return nil
}

func (o *diskOutbox) replace(frame []byte) error {
	tmp := filepath.Join(o.root, "state.tmp")
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return err
	}
	_, err = f.WriteString(journalMagic)
	if err == nil {
		_, err = f.Write(frame)
	}
	if err == nil {
		err = f.Sync()
	}
	if err = errors.Join(err, f.Close()); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if err = os.Rename(tmp, filepath.Join(o.root, "state.log")); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	if runtime.GOOS != "windows" {
		dir, err := os.Open(o.root)
		if err != nil {
			return err
		}
		return errors.Join(dir.Sync(), dir.Close())
	}
	return nil
}

func (o *diskOutbox) Close() error { return o.lock.Unlock() }
