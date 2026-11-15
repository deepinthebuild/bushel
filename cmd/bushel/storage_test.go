package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

// Buildbarn's local mappings are process-lifetime resources. Exercise daemon
// shutdown in a child and let it exit before the parent removes persistent files.
func daemonSubprocess(t *testing.T) (string, bool) {
	t.Helper()
	if root := os.Getenv("BUSHEL_TEST_DAEMON_ROOT"); root != "" {
		return root, true
	}
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, exe, "-test.run=^TestDaemonStartsOfflineAndStops$")
	cmd.Env = append(os.Environ(), "BUSHEL_TEST_DAEMON_ROOT="+t.TempDir())
	cmd.WaitDelay = 5 * time.Second
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("daemon subprocess: %v\n%s", err, out)
	}
	return "", false
}

func TestDaemonDirectoryOwnership(t *testing.T) {
	root := t.TempDir()
	lock, err := lockDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	if other, err := lockDirectory(root); err == nil {
		_ = other.Unlock()
		t.Fatal("accepted second daemon")
	}
	if err := lock.Unlock(); err != nil {
		t.Fatal(err)
	}
	lock, err = lockDirectory(root)
	if err != nil {
		t.Fatal(err)
	}
	_ = lock.Unlock()
	foreign := t.TempDir()
	if err := os.WriteFile(filepath.Join(foreign, "keep"), []byte("keep"), 0600); err != nil {
		t.Fatal(err)
	}
	if lock, err := lockDirectory(foreign); err == nil {
		_ = lock.Unlock()
		t.Fatal("accepted unowned state directory")
	}
}
