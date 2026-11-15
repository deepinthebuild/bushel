package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	storagepb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	"github.com/deepinthebuild/bushel/internal/storage"
	"github.com/gofrs/flock"
)

func storageConfiguration(c config) (*storagepb.ContentAddressableStorageConfiguration, error) {
	if c.storageConfig != "" {
		return storage.Read(c.storageConfig)
	}
	return storage.Default(filepath.Join(c.data, "storage"), c.localStorage)
}

// Hold this lock through Buildbarn's shutdown flush, including failed startup.
func lockDirectory(root string) (_ *flock.Flock, retErr error) {
	if err := os.MkdirAll(root, 0700); err != nil {
		return nil, err
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.Join(errors.New("state directory must be a directory, not a link"), err)
	}
	lock := flock.New(filepath.Join(root, "daemon.lock"))
	locked, err := lock.TryLock()
	if err != nil {
		return nil, err
	}
	if !locked {
		return nil, errors.New("daemon directory already in use")
	}
	defer func() {
		if retErr != nil {
			_ = lock.Unlock()
		}
	}()
	const format = "bushel-daemon-v1\n"
	path := filepath.Join(root, "format")
	f, err := os.Open(path)
	if err == nil {
		b, readErr := io.ReadAll(io.LimitReader(f, 32))
		if err := errors.Join(readErr, f.Close()); err != nil {
			return nil, err
		}
		if string(b) != format {
			return nil, fmt.Errorf("unrecognized daemon state format")
		}
		return lock, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	dir, err := os.Open(root)
	if err != nil {
		return nil, err
	}
	entries, readErr := dir.ReadDir(2)
	closeErr := dir.Close()
	if readErr != nil && readErr != io.EOF {
		return nil, errors.Join(readErr, closeErr)
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if len(entries) != 1 || entries[0].Name() != "daemon.lock" {
		return nil, errors.New("new daemon requires an empty state directory")
	}
	f, err = os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	_, writeErr := f.WriteString(format)
	return lock, errors.Join(writeErr, f.Sync(), f.Close())
}
