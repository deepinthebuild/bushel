//go:build bazel

package membershippb

import (
	"bytes"
	_ "embed"
	"testing"
)

// Bazel supplies both inputs at compile time, including on Windows where
// native file-comparison commands cannot handle long output paths.
//
//go:embed membership.pb.go
var checkedIn []byte

//go:embed generated/membership.pb.go
var regenerated []byte

func TestGeneratedMembershipIsCurrent(t *testing.T) {
	if !bytes.Equal(checkedIn, regenerated) {
		t.Fatal("membership.pb.go is stale; run bazel run //internal/membership/pb:update")
	}
}
