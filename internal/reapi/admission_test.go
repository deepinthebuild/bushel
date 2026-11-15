package reapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	bbdigest "github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/teststorage"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"pgregory.net/rapid"
)

func admissionClients(t *testing.T, store *cas.Store, fetcher Fetcher) (*grpc.Server, []*grpc.ClientConn) {
	t.Helper()
	server := New(store, fetcher, "").GRPC()
	listener := bufconn.Listen(64 << 10)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); listener.Close() })
	var clients []*grpc.ClientConn
	for range MaxConnections {
		c, err := grpc.NewClient("passthrough:///bushel", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { c.Close() })
		clients = append(clients, c)
	}
	return server, clients
}

type malformedCodec struct{}

func (malformedCodec) Name() string                { return "proto" }
func (malformedCodec) Marshal(any) ([]byte, error) { return []byte{0xff}, nil }
func (malformedCodec) Unmarshal([]byte, any) error { return nil }

func TestAdmissionPrecedesUnaryDecodeAcrossConnections(t *testing.T) {
	backend := teststorage.New()
	store, err := cas.NewStore(t.TempDir(), 64<<20, cas.Backend{Chunks: backend.Chunks, Mappings: backend.Mappings})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	f := blockingFetcher{make(chan struct{}, maxActiveRPCs), make(chan struct{})}
	server, clients := admissionClients(t, store, f)
	defer server.Stop()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	// One saturated connection cannot occupy every global slot. All sixteen
	// independent clients can make progress, including capability requests.
	for i := range maxActiveRPCs {
		c := repb.NewContentAddressableStorageClient(clients[i%len(clients)])
		wg.Go(func() {
			_, _ = c.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{Digests: []*repb.Digest{pd([]byte("absent"))}})
		})
		select {
		case <-f.entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		if i == maxStreamsPerConnection-1 {
			if _, err := repb.NewCapabilitiesClient(clients[len(clients)-1]).GetCapabilities(ctx, &repb.GetCapabilitiesRequest{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	// Invalid protobuf must be rejected by admission, before unmarshaling.
	// A unary interceptor alone returns Internal here instead.
	for service, info := range server.GetServiceInfo() {
		for _, method := range info.Methods {
			if method.IsClientStream || method.IsServerStream {
				continue
			}
			path := "/" + service + "/" + method.Name
			err = clients[0].Invoke(ctx, path, &repb.GetCapabilitiesRequest{}, &repb.ServerCapabilities{}, grpc.ForceCodec(malformedCodec{}))
			if status.Code(err) != codes.ResourceExhausted {
				t.Fatalf("decoded saturated %s request: %v", path, err)
			}
		}
	}
	close(f.release)
	wg.Wait()
	if _, err := repb.NewCapabilitiesClient(clients[0]).GetCapabilities(ctx, &repb.GetCapabilitiesRequest{}); err != nil {
		t.Fatal(err)
	}
	err = clients[0].Invoke(ctx, "/build.bazel.remote.execution.v2.Capabilities/GetCapabilities", &repb.GetCapabilitiesRequest{}, &repb.ServerCapabilities{}, grpc.ForceCodec(malformedCodec{}))
	if status.Code(err) != codes.Internal {
		t.Fatalf("malformed request did not reach decoder after recovery: %v", err)
	}
}

// Generated cancellation/release histories exercise the actual nonblocking
// admission primitive. Cancellation never releases an in-flight decoder.
func TestPropertyAdmissionHistories(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		limit := rapid.IntRange(1, 32).Draw(t, "limit")
		a := make(rpcAdmission, limit)
		var cancelers []context.CancelFunc
		for range rapid.IntRange(1, 200).Draw(t, "steps") {
			switch rapid.IntRange(0, 3).Draw(t, "action") {
			case 0, 1:
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				canceled := rapid.Bool().Draw(t, "canceled before admission")
				if canceled {
					cancel()
				}
				err := a.enter(ctx)
				if canceled {
					if status.Code(err) != codes.Canceled {
						t.Fatal(err)
					}
				} else if len(cancelers) == limit {
					if status.Code(err) != codes.ResourceExhausted {
						t.Fatal(err)
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					cancelers = append(cancelers, cancel)
				}
				if err != nil {
					cancel()
				}
			case 2:
				if len(cancelers) > 0 {
					cancelers[rapid.IntRange(0, len(cancelers)-1).Draw(t, "cancel index")]()
				}
			case 3:
				if len(cancelers) > 0 {
					cancelers[0]()
					cancelers = cancelers[1:]
					a.leave()
				}
			}
			if len(a) != len(cancelers) || len(a) > limit {
				t.Fatal("admission ownership diverged")
			}
		}
		for _, cancel := range cancelers {
			cancel()
			a.leave()
		}
		if err := a.enter(context.Background()); err != nil {
			t.Fatal(err)
		}
		a.leave()
	})
}

func TestStalledUploadsStopBeforeStoreReopen(t *testing.T) {
	root := t.TempDir()
	backend := teststorage.New()
	config := cas.Backend{Chunks: backend.Chunks, Mappings: backend.Mappings}
	store, err := cas.NewStore(root, 64<<20, config)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = store.Close() }()
	server, clients := admissionClients(t, store, nil)
	defer server.Stop()
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	observed := make(chan struct{}, maxStreamsPerConnection)
	backend.SetHook(func(_ context.Context, op teststorage.Operation, _ bbdigest.Digest) error {
		if op == teststorage.MappingGet {
			observed <- struct{}{}
		}
		return nil
	})
	var cancelers []context.CancelFunc
	for i := range maxStreamsPerConnection {
		data := bytes.Repeat([]byte{byte(i)}, 1<<20)
		uploadCtx, cancelUpload := context.WithCancel(ctx)
		cancelers = append(cancelers, cancelUpload)
		defer cancelUpload()
		w, err := bspb.NewByteStreamClient(clients[0]).Write(uploadCtx)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Send(&bspb.WriteRequest{ResourceName: fmt.Sprintf("uploads/%d/", i) + resource(data), Data: data[:1]}); err != nil {
			t.Fatal(err)
		}
		select {
		case <-observed:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	backend.SetHook(nil)
	// All stream capacity on one connection is held by stalled writers; an
	// independent Bazel client can still query capabilities, upload, and read.
	if _, err := repb.NewCapabilitiesClient(clients[1]).GetCapabilities(ctx, &repb.GetCapabilitiesRequest{}); err != nil {
		t.Fatal(err)
	}
	healthy := []byte("another client progresses while uploads stall")
	casClient := repb.NewContentAddressableStorageClient(clients[1])
	updated, err := casClient.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: pd(healthy), Data: healthy}}})
	if err != nil || len(updated.GetResponses()) != 1 || updated.Responses[0].Status.Code != 0 {
		t.Fatalf("another client's upload stalled: %v %v", updated, err)
	}
	read, err := casClient.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{Digests: []*repb.Digest{pd(healthy)}})
	if err != nil || len(read.GetResponses()) != 1 || read.Responses[0].Status.Code != 0 || !bytes.Equal(read.Responses[0].Data, healthy) {
		t.Fatalf("another client's verified read stalled: %v %v", read, err)
	}
	if other, err := cas.NewStore(root, 64<<20, config); err == nil {
		other.Close()
		t.Fatal("released store lock with active uploads")
	}
	for i, cancel := range cancelers {
		if i%2 == 0 {
			cancel()
		}
	}
	server.Stop() // WaitForHandlers joins canceled uploads and their cleanup.
	entries, err := os.ReadDir(filepath.Join(root, "tmp"))
	if err != nil || len(entries) != 0 {
		t.Fatalf("shutdown retained scratch: %v %v", entries, err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store, err = cas.NewStore(root, 64<<20, config)
	if err != nil {
		t.Fatal(err)
	}
	for i := range maxStreamsPerConnection {
		d := cas.Sum(bytes.Repeat([]byte{byte(i)}, 1<<20))
		if r, err := store.Open(ctx, d); !errors.Is(err, cas.ErrNotFound) {
			if r != nil {
				r.Close()
			}
			t.Fatalf("partial upload survived reopen: %v", err)
		}
	}
	data := []byte("write after shutdown and reopen")
	if err := store.Put(ctx, cas.Sum(data), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
}

type captureRegistrar struct{ descriptor *grpc.ServiceDesc }

func (r *captureRegistrar) RegisterService(d *grpc.ServiceDesc, _ any) { r.descriptor = d }

func FuzzUnaryAdmission(f *testing.F) {
	f.Add(uint8(1), uint8(1), false, false)
	f.Add(uint8(32), uint8(0), true, true)
	f.Add(uint8(32), uint8(31), false, true)
	f.Fuzz(func(t *testing.T, limitByte, occupiedByte uint8, canceled, decoderFails bool) {
		limit := 1 + int(limitByte)%maxActiveRPCs
		occupied := int(occupiedByte) % (limit + 1)
		a := make(rpcAdmission, limit)
		for range occupied {
			if err := a.enter(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		decoderError := errors.New("injected decode error")
		original := grpc.ServiceDesc{Methods: []grpc.MethodDesc{{MethodName: "test", Handler: func(_ any, _ context.Context, dec func(any) error, _ grpc.UnaryServerInterceptor) (any, error) {
			return nil, dec(nil)
		}}}}
		captured := &captureRegistrar{}
		admittedRegistrar{server: captured, admission: a}.RegisterService(&original, nil)
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		if canceled {
			cancel()
		}
		decoded := false
		_, err := captured.descriptor.Methods[0].Handler(nil, ctx, func(any) error {
			decoded = true
			// Cancellation during decode cannot release the owned slot.
			cancel()
			if len(a) != occupied+1 {
				t.Fatal("lost decoder ownership")
			}
			if decoderFails {
				return decoderError
			}
			return nil
		}, nil)
		switch {
		case canceled:
			if decoded || status.Code(err) != codes.Canceled {
				t.Fatal("decoded canceled request")
			}
		case occupied == limit:
			if decoded || status.Code(err) != codes.ResourceExhausted {
				t.Fatal("decoded without capacity")
			}
		default:
			if !decoded || (decoderFails && !errors.Is(err, decoderError)) || (!decoderFails && err != nil) {
				t.Fatal("decoder result lost")
			}
		}
		if len(a) != occupied {
			t.Fatal("admission leaked after decoder return")
		}
		// Registration never modifies the globally shared generated descriptor.
		called := false
		_, err = original.Methods[0].Handler(nil, ctx, func(any) error { called = true; return nil }, nil)
		if !called || err != nil {
			t.Fatal("mutated original descriptor")
		}
	})
}

func BenchmarkRPCAdmission(b *testing.B) {
	a := make(rpcAdmission, maxActiveRPCs)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		if err := a.enter(ctx); err != nil {
			b.Fatal(err)
		}
		a.leave()
	}
}

func TestPropertyReceiveLeaseSurvivesHandlerReturn(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		a := make(rpcAdmission, 1)
		if err := a.enter(context.Background()); err != nil {
			t.Fatal(err)
		}
		lease := &rpcLease{admission: a}
		lease.owners.Store(1)
		ctx, cancel := context.WithCancel(context.WithValue(context.Background(), rpcLeaseKey{}, lease))
		releaseReceiver := retainReceiver(ctx)
		cancel()
		if rapid.Bool().Draw(t, "receiver returns first") {
			releaseReceiver()
			if err := a.enter(context.Background()); status.Code(err) != codes.ResourceExhausted {
				t.Fatal("released active handler")
			}
			lease.release()
		} else {
			lease.release()
			if err := a.enter(context.Background()); status.Code(err) != codes.ResourceExhausted {
				t.Fatal("released active decoder")
			}
			releaseReceiver()
		}
		if err := a.enter(context.Background()); err != nil {
			t.Fatal(err)
		}
		a.leave()
	})
}
