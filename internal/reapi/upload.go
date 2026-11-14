package reapi

import (
	"errors"

	"github.com/deepinthebuild/bushel/internal/cas"
)

// uploadState is the immutable protocol state, separate from stream I/O and
// borrowed payload buffers. A rejected part leaves the state unchanged.
type uploadState struct {
	resource string
	size     int64
	offset   int64
	finished bool
}

type uploadPart struct {
	resource string
	offset   int64
	dataSize int64
	finish   bool
}

func advanceUpload(state uploadState, part uploadPart) (uploadState, error) {
	if state.resource == "" || state.size < 0 || state.size > cas.MaxBlobSize || state.offset < 0 || state.offset > state.size {
		return state, errors.New("invalid upload state")
	}
	if state.finished {
		return state, errors.New("upload already finished")
	}
	if (part.resource != "" && part.resource != state.resource) || part.offset != state.offset {
		return state, errors.New("upload resource or offset changed")
	}
	if part.dataSize < 0 || part.dataSize > state.size-state.offset {
		return state, errors.New("upload exceeds declared size")
	}
	next := state
	next.offset += part.dataSize
	next.finished = part.finish
	if next.finished && next.offset != next.size {
		return state, errors.New("premature finish_write")
	}
	return next, nil
}
