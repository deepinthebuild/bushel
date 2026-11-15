package cas

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
)

const catalogBatchSize = 64
const maxPublicationHints = 4096

// WalkCommitted streams the durable publication catalog. It validates each
// filename and manifest without rehashing blob data, which was verified before
// publication. Concurrent publications need not appear in the current walk;
// subscribe before walking, then repeat walks to recover lost hints.
func (s *Store) WalkCommitted(ctx context.Context, yield func(Digest) error) error {
	return walkDirectoryEntries(ctx, filepath.Join(s.root, "manifests"), func(entry os.DirEntry) error {
		d, err := catalogDigest(entry.Name())
		if err != nil || !entry.Type().IsRegular() {
			return nil
		}
		info, err := entry.Info()
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() < ManifestHeaderSize || info.Size() > MaxManifestSize {
			return nil
		}
		b, err := readBounded(filepath.Join(s.root, "manifests", entry.Name()), MaxManifestSize)
		if errors.Is(err, ErrNotFound) || errors.Is(err, ErrCorrupt) {
			return nil
		}
		if err != nil {
			return err
		}
		m, err := ParseManifest(b)
		if err != nil || m.Blob != d {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		return yield(d)
	})
}

// Both startup accounting and catalog walks retain only one directory batch.
func walkDirectoryEntries(ctx context.Context, path string, visit func(os.DirEntry) error) (retErr error) {
	if err := ctx.Err(); err != nil {
		return err
	}
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("%w: CAS directory must be a directory, not a link", ErrInvalid)
	}
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, dir.Close()) }()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		entries, readErr := dir.ReadDir(catalogBatchSize)
		for _, entry := range entries {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := visit(entry); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func catalogDigest(name string) (Digest, error) {
	if len(name) < 66 || name[64] != '-' {
		return Digest{}, ErrInvalid
	}
	size, err := strconv.ParseInt(name[65:], 10, 64)
	if err != nil {
		return Digest{}, ErrInvalid
	}
	d, err := ParseDigest(name[:64], size)
	if err != nil || d.Key() != name {
		return Digest{}, fmt.Errorf("%w: noncanonical catalog name", ErrInvalid)
	}
	return d, nil
}

type publicationSubscription struct {
	hints    chan Digest
	overflow chan struct{}
}

// SubscribeCommits supplies lossy, bounded hints after manifest publication.
// A capacity-one overflow signal requests a later catalog walk. Uploads never
// wait for either channel. cancel is idempotent and closes both channels after
// unregistering them; callers must cancel before closing the Store.
func (s *Store) SubscribeCommits(buffer int) (<-chan Digest, <-chan struct{}, func()) {
	sub := &publicationSubscription{
		hints:    make(chan Digest, min(max(buffer, 1), maxPublicationHints)),
		overflow: make(chan struct{}, 1),
	}
	s.mu.Lock()
	if s.publications == nil {
		s.publications = make(map[*publicationSubscription]struct{})
	}
	s.publications[sub] = struct{}{}
	s.mu.Unlock()
	return sub.hints, sub.overflow, func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if _, exists := s.publications[sub]; exists {
			delete(s.publications, sub)
			close(sub.hints)
			close(sub.overflow)
		}
	}
}

// publishLocked runs after the manifest is installed, while Store.mu is held.
func (s *Store) publishLocked(d Digest) {
	for sub := range s.publications {
		select {
		case sub.hints <- d:
		default:
			select {
			case sub.overflow <- struct{}{}:
			default:
			}
		}
	}
}
