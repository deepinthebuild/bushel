package storage

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/program"
	storagepb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	grpcpb "github.com/buildbarn/bb-storage/pkg/proto/configuration/grpc"
	"github.com/deepinthebuild/bushel/internal/cas"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/stats"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"pgregory.net/rapid"
)

func mappingResponse(chunks, padding int) *repb.GetChunkMappingResponse {
	r := &repb.GetChunkMappingResponse{
		ChunkingFunction: repb.ChunkingFunction_REP_MAX_CDC,
		ChunkDigests:     make([]*repb.Digest, chunks),
	}
	for i := range r.ChunkDigests {
		r.ChunkDigests[i] = &repb.Digest{Hash: strings.Repeat("a", 64), SizeBytes: cas.MinChunkSize}
	}
	if padding != 0 {
		unknown := protowire.AppendTag(nil, 100, protowire.BytesType)
		r.ProtoReflect().SetUnknown(protowire.AppendBytes(unknown, make([]byte, padding)))
	}
	return r
}

func TestPropertyMappingBudgetConservation(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		nearLimit := func(limit, spread int, name string) int {
			if rapid.Bool().Draw(t, name+".empty") {
				return 0
			}
			return limit - rapid.IntRange(0, spread).Draw(t, name+".remaining")
		}
		before := mappingBudget{
			chunks: nearLimit(cas.MaxChunks, 16, "chunks"), bytes: nearLimit(4<<20, 2048, "bytes"),
			messages: nearLimit(cas.MaxChunks+1, 3, "messages"),
		}
		r := mappingResponse(rapid.IntRange(0, 16).Draw(t, "response.chunks"), rapid.IntRange(0, 256).Draw(t, "response.padding"))
		if rapid.Bool().Draw(t, "response.unknown_chunking") {
			r.ChunkingFunction = repb.ChunkingFunction_UNKNOWN
		}
		next, err := before.add(r)
		protocolValid := before.messages != 0 || r.ChunkingFunction == repb.ChunkingFunction_REP_MAX_CDC
		fits := before.chunks+len(r.ChunkDigests) <= cas.MaxChunks && before.bytes+proto.Size(r) <= 4<<20 && before.messages+1 <= cas.MaxChunks+1
		if protocolValid && fits {
			want := mappingBudget{chunks: before.chunks + len(r.ChunkDigests), bytes: before.bytes + proto.Size(r), messages: before.messages + 1}
			if err != nil || next != want {
				t.Fatalf("accepted response changed accounting: before=%+v next=%+v want=%+v error=%v", before, next, want, err)
			}
		} else {
			if err == nil || next != before {
				t.Fatalf("rejected response changed state: before=%+v next=%+v error=%v", before, next, err)
			}
			wantCode := codes.ResourceExhausted
			if !protocolValid {
				wantCode = codes.DataLoss
			}
			if status.Code(err) != wantCode {
				t.Fatalf("rejection code: got %v, want %v", status.Code(err), wantCode)
			}
		}
	})
}

type mappingTestServer struct {
	repb.UnimplementedContentAddressableStorageServer
	repb.UnimplementedCapabilitiesServer
	response   *repb.GetChunkMappingResponse
	messages   int
	waitCancel bool
	canceled   chan struct{}
}

func (*mappingTestServer) GetCapabilities(context.Context, *repb.GetCapabilitiesRequest) (*repb.ServerCapabilities, error) {
	return &repb.ServerCapabilities{CacheCapabilities: &repb.CacheCapabilities{
		DigestFunctions:  []repb.DigestFunction_Value{repb.DigestFunction_SHA256},
		RepMaxCdcParams:  &repb.RepMaxCdcParams{MinChunkSizeBytes: cas.MinChunkSize, HorizonSizeBytes: cas.HorizonSize},
		SplitBlobSupport: true, SpliceBlobSupport: true,
	}}, nil
}

func (s *mappingTestServer) GetChunkMapping(_ *repb.GetChunkMappingRequest, stream grpc.ServerStreamingServer[repb.GetChunkMappingResponse]) error {
	for range s.messages {
		if err := stream.Send(s.response); err != nil {
			if s.waitCancel {
				<-stream.Context().Done()
				close(s.canceled)
			}
			return err
		}
	}
	if s.waitCancel {
		<-stream.Context().Done()
		close(s.canceled)
		return stream.Context().Err()
	}
	return nil
}

type mappingConnectionStats struct {
	opened, active atomic.Int32
	changed        chan struct{}
}

func (*mappingConnectionStats) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}
func (*mappingConnectionStats) HandleRPC(context.Context, stats.RPCStats) {}
func (*mappingConnectionStats) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (s *mappingConnectionStats) HandleConn(_ context.Context, event stats.ConnStats) {
	switch event.(type) {
	case *stats.ConnBegin:
		s.opened.Add(1)
		s.active.Add(1)
	case *stats.ConnEnd:
		s.active.Add(-1)
	}
	select {
	case s.changed <- struct{}{}:
	default:
	}
}

func TestMappingStreamLimitsThroughConfiguredGRPCBackend(t *testing.T) {
	invalidFunction := mappingResponse(2, 0)
	invalidFunction.ChunkingFunction = repb.ChunkingFunction_UNKNOWN
	for _, tc := range []struct {
		name       string
		response   *repb.GetChunkMappingResponse
		messages   int
		wantCode   codes.Code
		wantChunks int
	}{
		{"chunk count", mappingResponse(1024, 0), cas.MaxChunks/1024 + 1, codes.ResourceExhausted, 0},
		{"aggregate bytes", mappingResponse(0, 64<<10), 65, codes.ResourceExhausted, 0},
		{"empty message count", mappingResponse(0, 0), cas.MaxChunks + 2, codes.ResourceExhausted, 0},
		{"invalid first chunking function", invalidFunction, 1, codes.DataLoss, 0},
		{"maximum mapping", mappingResponse(1024, 0), cas.MaxChunks / 1024, codes.OK, cas.MaxChunks},
		{"normal EOF", mappingResponse(1, 0), 2, codes.OK, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if size := proto.Size(tc.response); size >= 4<<20 {
				t.Fatalf("fixture must be below the per-message limit: %d", size)
			}
			listener, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			connections := &mappingConnectionStats{changed: make(chan struct{}, 1)}
			server := grpc.NewServer(grpc.StatsHandler(connections))
			malicious := &mappingTestServer{response: tc.response, messages: tc.messages, waitCancel: tc.wantCode != codes.OK, canceled: make(chan struct{})}
			repb.RegisterContentAddressableStorageServer(server, malicious)
			repb.RegisterCapabilitiesServer(server, malicious)
			stopped := make(chan struct{})
			go func() { defer close(stopped); _ = server.Serve(listener) }()
			t.Cleanup(func() { server.Stop(); <-stopped })
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			remote := &storagepb.BlobAccessConfiguration{Backend: &storagepb.BlobAccessConfiguration_Grpc{Grpc: &storagepb.GrpcBlobAccessConfiguration{
				Client: &grpcpb.ClientConfiguration{Address: listener.Addr().String()},
			}}}
			err = program.RunLocal(ctx, func(ctx context.Context, _, dependencies program.Group) error {
				backend, err := New(ctx, dependencies, &storagepb.ContentAddressableStorageConfiguration{ChunkStorage: remote, ChunkMappingStorage: remote}, "")
				if err != nil {
					return fmt.Errorf("construct remote backend: %w", err)
				}
				chunkCount := tc.wantChunks
				if chunkCount == 0 {
					chunkCount = cas.MaxChunks
				}
				key := digest.MustNewDigest("", repb.DigestFunction_SHA256, strings.Repeat("b", 64), int64(chunkCount)*cas.MinChunkSize)
				mapping, err := backend.Mappings.Get(ctx, key)
				if status.Code(err) != tc.wantCode {
					return fmt.Errorf("mapping result: got %v, want %v", err, tc.wantCode)
				}
				if tc.wantCode == codes.OK {
					if mapping.Length() != tc.wantChunks || mapping.GetSizeBytes() != key.GetSizeBytes() {
						return fmt.Errorf("mapping changed: chunks=%d size=%d", mapping.Length(), mapping.GetSizeBytes())
					}
					return nil
				}
				// Observe stream cancellation before RunLocal closes the connection:
				// a caller must not leak a backend stream after rejecting its prefix.
				select {
				case <-malicious.canceled:
					return nil
				case <-time.After(2 * time.Second):
					return fmt.Errorf("rejected backend stream was not canceled promptly")
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			deadline := time.NewTimer(2 * time.Second)
			defer deadline.Stop()
			for connections.active.Load() != 0 {
				select {
				case <-connections.changed:
				case <-deadline.C:
					t.Fatal("backend connection survived dependency shutdown")
				}
			}
			if connections.opened.Load() == 0 {
				t.Fatal("test did not open a real backend connection")
			}
		})
	}
}
