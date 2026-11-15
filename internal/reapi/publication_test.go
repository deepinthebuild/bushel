package reapi

import (
	"bytes"
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/teststorage"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

func TestByteStreamRetryRecoversAmbiguousPublication(t *testing.T) {
	for _, operation := range []string{"write", "status"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			backend := teststorage.New()
			store, err := cas.NewStore(t.TempDir(), 16<<20, cas.Backend{Chunks: backend.Chunks, Mappings: backend.Mappings})
			if err != nil {
				t.Fatal(err)
			}
			defer store.Close()
			hints, _, unsubscribe := store.SubscribeCommits(4)
			defer unsubscribe()
			data := bytes.Repeat([]byte("ambiguous backend completion"), 24000)
			d := cas.Sum(data)
			ambiguous := status.Error(codes.Unavailable, "response lost after write")
			backend.FailAfterWrite(teststorage.MappingPut, 0, ambiguous)
			if err := store.Put(ctx, d, bytes.NewReader(data)); !errors.Is(err, ambiguous) {
				t.Fatalf("initial write: %v", err)
			}
			count := 0
			if err := store.WalkCommitted(ctx, func(cas.Digest) error { count++; return nil }); err != nil || count != 0 {
				t.Fatalf("failed write entered catalog: %d %v", count, err)
			}
			select {
			case <-hints:
				t.Fatal("failed write published a hint")
			default:
			}
			server := New(store, nil, "").GRPC()
			listener := bufconn.Listen(1 << 20)
			done := make(chan error, 1)
			go func() { done <- server.Serve(listener) }()
			conn, err := grpc.NewClient("passthrough:///retry", grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
			if err != nil {
				server.Stop()
				listener.Close()
				t.Fatal(err)
			}
			defer func() { conn.Close(); server.Stop(); listener.Close(); <-done }()
			client := bspb.NewByteStreamClient(conn)
			name := "uploads/retry/blobs/" + d.Hex() + "/" + strconv.FormatInt(d.Size, 10)
			if operation == "write" {
				stream, err := client.Write(ctx)
				if err != nil {
					t.Fatal(err)
				}
				if err := stream.Send(&bspb.WriteRequest{ResourceName: name, FinishWrite: true}); err != nil {
					t.Fatal(err)
				}
				response, err := stream.CloseAndRecv()
				if err != nil || response.GetCommittedSize() != d.Size {
					t.Fatalf("duplicate retry: %v %v", response, err)
				}
			} else {
				response, err := client.QueryWriteStatus(ctx, &bspb.QueryWriteStatusRequest{ResourceName: name})
				if err != nil || !response.GetComplete() || response.GetCommittedSize() != d.Size {
					t.Fatalf("status retry: %v %v", response, err)
				}
			}
			select {
			case got := <-hints:
				if got != d {
					t.Fatal("wrong repaired publication")
				}
			case <-ctx.Done():
				t.Fatal("retry did not restore announcement hint")
			}
			if err := store.WalkCommitted(ctx, func(got cas.Digest) error {
				if got != d {
					t.Fatal("wrong catalog digest")
				}
				count++
				return nil
			}); err != nil || count != 1 {
				t.Fatalf("retry did not restore catalog: %d %v", count, err)
			}
		})
	}
}
