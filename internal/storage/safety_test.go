package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultRejectsRedirectedPaths(t *testing.T) {
	for _, part := range []string{"root", "cs", "cms", "cs/keys", "cs/blocks", "cs/state", "cms/keys", "cms/blocks", "cms/state"} {
		t.Run(part, func(t *testing.T) {
			parent, outside := t.TempDir(), t.TempDir()
			root := filepath.Join(parent, "backend")
			path := root
			target := outside
			file := part != "root" && part != "cs" && part != "cms"
			if part != "root" {
				path = filepath.Join(root, filepath.FromSlash(part))
				if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
					t.Fatal(err)
				}
			}
			if file {
				target = filepath.Join(outside, "keep")
				if err := os.WriteFile(target, []byte("keep"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if err := os.Symlink(target, path); err != nil {
				t.Skipf("symlink unavailable: %v", err)
			}
			if _, err := Default(root, 32<<20); err == nil {
				t.Fatal("accepted redirected default storage")
			}
			entries, err := os.ReadDir(outside)
			if err != nil {
				t.Fatal(err)
			}
			if file {
				b, err := os.ReadFile(target)
				if err != nil || string(b) != "keep" || len(entries) != 1 {
					t.Fatalf("outside file changed: %v", err)
				}
			} else if len(entries) != 0 {
				t.Fatal("created files outside owned state")
			}
		})
	}
}
