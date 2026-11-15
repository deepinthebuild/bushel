//go:build bazel

package publicationpb

import (
	"bytes"
	_ "embed"
	"testing"
)

// Bazel supplies both inputs at compile time, including on Windows where
// native file-comparison commands cannot handle long output paths.
//
//go:embed journal.pb.go
var checkedIn []byte

//go:embed generated/journal.pb.go
var regenerated []byte

func TestGeneratedJournalIsCurrent(t *testing.T) {
	if !bytes.Equal(checkedIn, regenerated) {
		t.Fatal("journal.pb.go is stale; run bazel run //internal/publication/pb:update")
	}
}
