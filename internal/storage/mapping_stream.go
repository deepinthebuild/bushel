package storage

import (
	"context"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/deepinthebuild/bushel/internal/cas"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// Buildbarn collects GetChunkMapping responses until EOF. A per-message gRPC
// limit cannot bound that accumulation, so enforce the complete stream budget
// before returning each response to its collector.
func boundMappingStream(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if method != repb.ContentAddressableStorage_GetChunkMapping_FullMethodName {
		return streamer(ctx, desc, cc, method, opts...)
	}
	ctx, cancel := context.WithCancel(ctx)
	stream, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		cancel()
		return nil, err
	}
	return &mappingStream{ClientStream: stream, cancel: cancel}, nil
}

type mappingBudget struct {
	chunks, bytes, messages int
}

func (b mappingBudget) add(r *repb.GetChunkMappingResponse) (mappingBudget, error) {
	const maxBytes = 4 << 20
	if b.messages == 0 && r.ChunkingFunction != repb.ChunkingFunction_REP_MAX_CDC {
		return b, status.Error(codes.DataLoss, "backend mapping must use RepMaxCDC")
	}
	size := proto.Size(r)
	if len(r.ChunkDigests) > cas.MaxChunks-b.chunks || size > maxBytes-b.bytes || b.messages >= cas.MaxChunks+1 {
		return b, status.Error(codes.ResourceExhausted, "backend chunk mapping exceeds stream budget")
	}
	// Nested reference-expanding backends may use different digest functions
	// or cuts. Validate Bushel's own mappings in cas.manifestFromMapping.
	return mappingBudget{chunks: b.chunks + len(r.ChunkDigests), bytes: b.bytes + size, messages: b.messages + 1}, nil
}

type mappingStream struct {
	grpc.ClientStream
	cancel context.CancelFunc
	budget mappingBudget
}

func (s *mappingStream) RecvMsg(value any) error {
	if err := s.ClientStream.RecvMsg(value); err != nil {
		s.cancel()
		return err
	}
	r, ok := value.(*repb.GetChunkMappingResponse)
	if !ok {
		s.cancel()
		return status.Error(codes.Internal, "unexpected backend mapping response")
	}
	next, err := s.budget.add(r)
	if err != nil {
		s.cancel()
		return err
	}
	s.budget = next
	return nil
}

func (s *mappingStream) SendMsg(value any) error {
	err := s.ClientStream.SendMsg(value)
	if err != nil {
		s.cancel()
	}
	return err
}

func (s *mappingStream) CloseSend() error {
	err := s.ClientStream.CloseSend()
	if err != nil {
		s.cancel()
	}
	return err
}
