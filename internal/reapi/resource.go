package reapi

import (
	"strconv"
	"strings"

	"github.com/deepinthebuild/bushel/internal/cas"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// parseResource depends only on its explicit inputs; names never become paths.
func parseResource(instance, name string, write bool) (cas.Digest, error) {
	bad := status.Error(codes.InvalidArgument, "invalid ByteStream resource name")
	if len(name) > 4096 {
		return cas.Digest{}, bad
	}
	prefix := ""
	if instance != "" {
		prefix = instance + "/"
	}
	if !strings.HasPrefix(name, prefix) {
		return cas.Digest{}, bad
	}
	parts := strings.Split(strings.TrimPrefix(name, prefix), "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." || strings.ContainsAny(part, "\\\x00\r\n") {
			return cas.Digest{}, bad
		}
	}
	if write {
		if len(parts) < 5 || parts[0] != "uploads" {
			return cas.Digest{}, bad
		}
		parts = parts[2:]
	}
	if len(parts) < 3 || parts[0] != "blobs" {
		return cas.Digest{}, bad
	}
	parts = parts[1:]
	if parts[0] == "sha256" {
		parts = parts[1:]
	}
	if len(parts) < 2 || (!write && len(parts) != 2) {
		return cas.Digest{}, bad
	}
	size, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || strconv.FormatInt(size, 10) != parts[1] {
		return cas.Digest{}, bad
	}
	d, err := cas.ParseDigest(parts[0], size)
	return d, rpcError(err)
}
