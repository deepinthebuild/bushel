package reapi

import (
	"context"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/workspace"
)

// PublicationObserver receives explicit local-client intent, never cache reads
// or generic storage commits. Implementations must be concurrency-safe and
// nonblocking: enqueue bounded work without waiting for disk or network access.
type PublicationObserver interface {
	Observe(d cas.Digest, metadata workspace.Metadata, present bool)
	Committed(d cas.Digest, metadata workspace.Metadata)
}

// WithPublicationObserver configures a server before it starts serving requests.
func (s *Server) WithPublicationObserver(observer PublicationObserver) *Server {
	s.observer = observer
	return s
}

func (s *Server) observePresence(ctx context.Context, digests, missing []cas.Digest) {
	if s.observer == nil {
		return
	}
	metadata := workspace.FromContext(ctx)
	absent := make(map[cas.Digest]bool, len(missing))
	for _, d := range missing {
		absent[d] = true
	}
	seen := make(map[cas.Digest]bool, len(digests))
	for _, d := range digests {
		if !seen[d] {
			s.observer.Observe(d, metadata, !absent[d])
			seen[d] = true
		}
	}
}

func (s *Server) observeCommit(ctx context.Context, d cas.Digest) {
	if s.observer != nil {
		s.observer.Committed(d, workspace.FromContext(ctx))
	}
}
