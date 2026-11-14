package reapi

import (
	"bytes"
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/deepinthebuild/bushel/internal/cas"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

func clients(t *testing.T, fetchers ...Fetcher) (context.Context, repb.ContentAddressableStorageClient, bspb.ByteStreamClient, repb.CapabilitiesClient) {
	t.Helper()
	store, err := cas.NewStore(t.TempDir(), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	var fetcher Fetcher
	if len(fetchers) > 0 {
		fetcher = fetchers[0]
	}
	s := New(store, fetcher, "").GRPC()
	listener := bufconn.Listen(1 << 20)
	go func() { _ = s.Serve(listener) }()
	c, err := grpc.NewClient("passthrough:///bushel", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	capsConnection, err := grpc.NewClient("passthrough:///bushel", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(func() {
		cancel()
		c.Close()
		capsConnection.Close()
		s.Stop()
		listener.Close()
		store.Close()
	})
	return ctx, repb.NewContentAddressableStorageClient(c), bspb.NewByteStreamClient(c), repb.NewCapabilitiesClient(capsConnection)
}
func pd(b []byte) *repb.Digest {
	d := cas.Sum(b)
	return &repb.Digest{Hash: d.Hex(), SizeBytes: d.Size}
}
func resource(b []byte) string {
	d := cas.Sum(b)
	return "blobs/" + d.Hex() + "/" + strconv.FormatInt(d.Size, 10)
}

func TestBatchCapabilitiesAndPerItemErrors(t *testing.T) {
	ctx, c, _, caps := clients(t)
	capabilities, err := caps.GetCapabilities(ctx, &repb.GetCapabilitiesRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if capabilities.CacheCapabilities.DigestFunctions[0] != repb.DigestFunction_SHA256 || capabilities.CacheCapabilities.MaxBatchTotalSizeBytes != MaxBatchBytes {
		t.Fatal(capabilities)
	}
	b := []byte("hello")
	update, err := c.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: pd(b), Data: b}, {Digest: pd([]byte("wrong")), Data: b}}})
	if err != nil {
		t.Fatal(err)
	}
	if update.Responses[0].Status.Code != 0 || update.Responses[1].Status.Code != int32(codes.InvalidArgument) {
		t.Fatal(update)
	}
	missing, err := c.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{pd(b), pd(nil), pd([]byte("absent"))}})
	if err != nil || len(missing.GetMissingBlobDigests()) != 1 {
		t.Fatalf("%v %v", missing, err)
	}
	read, err := c.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{Digests: []*repb.Digest{pd(b), pd([]byte("absent"))}})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(read.Responses[0].Data, b) || read.Responses[1].Status.Code != int32(codes.NotFound) {
		t.Fatal(read)
	}
	_, err = c.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{DigestFunction: repb.DigestFunction_BLAKE3})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatal(err)
	}
}

func TestByteStreamRoundTripRangeAndStatus(t *testing.T) {
	ctx, _, c, _ := clients(t)
	data := bytes.Repeat([]byte("bushel"), 150000)
	name := "uploads/test/" + resource(data)
	w, err := c.Write(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for off := 0; off < len(data); {
		end := min(off+64000, len(data))
		req := &bspb.WriteRequest{WriteOffset: int64(off), Data: data[off:end], FinishWrite: end == len(data)}
		if off == 0 {
			req.ResourceName = name
		}
		if err := w.Send(req); err != nil {
			t.Fatal(err)
		}
		off = end
	}
	resp, err := w.CloseAndRecv()
	if err != nil || resp.GetCommittedSize() != int64(len(data)) {
		t.Fatalf("%v %v", resp, err)
	}
	q, err := c.QueryWriteStatus(ctx, &bspb.QueryWriteStatusRequest{ResourceName: name})
	if err != nil || !q.Complete {
		t.Fatalf("%v %v", q, err)
	}
	r, err := c.Read(ctx, &bspb.ReadRequest{ResourceName: resource(data), ReadOffset: 17, ReadLimit: 100003})
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	for {
		p, err := r.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		out.Write(p.Data)
	}
	if !bytes.Equal(out.Bytes(), data[17:100020]) {
		t.Fatal("range mismatch")
	}
}

func TestByteStreamRejectsInterruptedAndInvalidUploads(t *testing.T) {
	ctx, casClient, c, _ := clients(t)
	data := []byte("hello")
	name := "uploads/test/" + resource(data)
	for _, req := range []*bspb.WriteRequest{
		{ResourceName: name, WriteOffset: 1, Data: data, FinishWrite: true},
		{ResourceName: name, Data: data},
		{ResourceName: name, Data: []byte("jello"), FinishWrite: true},
		{ResourceName: name, Data: data[:2], FinishWrite: true},
	} {
		w, err := c.Write(ctx)
		if err != nil {
			t.Fatal(err)
		}
		_ = w.Send(req)
		_, err = w.CloseAndRecv()
		if status.Code(err) != codes.InvalidArgument {
			t.Fatalf("got %v", err)
		}
	}
	m, err := casClient.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{pd(data)}})
	if err != nil || len(m.MissingBlobDigests) != 1 {
		t.Fatalf("published incomplete upload: %v %v", m, err)
	}
	_, err = c.QueryWriteStatus(ctx, &bspb.QueryWriteStatusRequest{ResourceName: name})
	if status.Code(err) != codes.NotFound {
		t.Fatal(err)
	}
}

func TestResourcesAndTree(t *testing.T) {
	s := New(nil, nil, "")
	for _, name := range []string{"../blobs/a/1", "blobs/" + strings.Repeat("a", 64) + "/-1", "compressed-blobs/zstd/a/1", "blobs/a/1", "blobs/" + strings.Repeat("a", 64) + "/01"} {
		if _, err := s.resource(name, false); err == nil {
			t.Fatal(name)
		}
	}
	ctx, c, _, _ := clients(t)
	leaf, _ := proto.Marshal(&repb.Directory{})
	root, _ := proto.Marshal(&repb.Directory{Directories: []*repb.DirectoryNode{{Name: "missing", Digest: pd([]byte("absent"))}, {Name: "child", Digest: pd(leaf)}}})
	_, err := c.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: pd(leaf), Data: leaf}, {Digest: pd(root), Data: root}}})
	if err != nil {
		t.Fatal(err)
	}
	tree, err := c.GetTree(ctx, &repb.GetTreeRequest{RootDigest: pd(root)})
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for {
		p, err := tree.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		count += len(p.Directories)
	}
	if count != 2 {
		t.Fatal(count)
	}
}

func TestDuplicateWriteCompletesWhileSenderIsStalled(t *testing.T) {
	ctx, c, bs, _ := clients(t)
	data := []byte("duplicate upload")
	name := "uploads/first/" + resource(data)
	w, err := bs.Write(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.Send(&bspb.WriteRequest{ResourceName: name, Data: data[:1]}); err != nil {
		t.Fatal(err)
	}
	update, err := c.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: pd(data), Data: data}}})
	if err != nil || update.Responses[0].Status.Code != 0 {
		t.Fatalf("%v %v", update, err)
	}
	var response bspb.WriteResponse
	if err := w.RecvMsg(&response); err != nil || response.CommittedSize != int64(len(data)) {
		t.Fatalf("stalled duplicate: %v %v", response.CommittedSize, err)
	}
	for _, b := range [][]byte{data, nil} {
		w, err := bs.Write(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if err := w.Send(&bspb.WriteRequest{ResourceName: "uploads/retry/" + resource(b), WriteOffset: int64(len(b)), FinishWrite: true}); err != nil {
			t.Fatal(err)
		}
		r, err := w.CloseAndRecv()
		if err != nil || r.CommittedSize != int64(len(b)) {
			t.Fatalf("completed retry: %v %v", r, err)
		}
	}
}

type blockingFetcher struct {
	entered chan struct{}
	release chan struct{}
}

func (f blockingFetcher) Fetch(ctx context.Context, _ cas.Digest) error {
	f.entered <- struct{}{}
	select {
	case <-f.release:
		return cas.ErrNotFound
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestRPCSaturationFailsPromptlyAndRecovers(t *testing.T) {
	f := blockingFetcher{make(chan struct{}, 32), make(chan struct{})}
	ctx, c, _, caps := clients(t, f)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			_, _ = c.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{Digests: []*repb.Digest{pd([]byte("absent"))}})
		})
	}
	for range 32 {
		select {
		case <-f.entered:
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	// A separate connection avoids the per-connection HTTP/2 stream limit.
	_, err := caps.GetCapabilities(ctx, &repb.GetCapabilitiesRequest{})
	close(f.release)
	wg.Wait()
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("saturation: %v", err)
	}
	if _, err := caps.GetCapabilities(ctx, &repb.GetCapabilitiesRequest{}); err != nil {
		t.Fatal(err)
	}
}

type stalledUpload struct {
	grpc.ServerStream
	ctx      context.Context
	first    *bspb.WriteRequest
	waiting  chan struct{}
	response *bspb.WriteResponse
}

func (s *stalledUpload) Context() context.Context { return s.ctx }
func (s *stalledUpload) Recv() (*bspb.WriteRequest, error) {
	if s.first != nil {
		r := s.first
		s.first = nil
		return r, nil
	}
	close(s.waiting)
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}
func (s *stalledUpload) SendAndClose(r *bspb.WriteResponse) error { s.response = r; return nil }

func TestConcurrentCommitNotificationFinishesUpload(t *testing.T) {
	store, err := cas.NewStore(t.TempDir(), 64<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	data := []byte("concurrent publication")
	stream := &stalledUpload{ctx: ctx, first: &bspb.WriteRequest{ResourceName: "uploads/first/" + resource(data), Data: data[:1]}, waiting: make(chan struct{})}
	result := make(chan error, 1)
	go func() { result <- New(store, nil, "").Write(stream) }()
	select {
	case <-stream.waiting:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if err := store.Put(ctx, cas.Sum(data), bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil || stream.response == nil || stream.response.CommittedSize != int64(len(data)) {
			t.Fatalf("notification: %v %v", stream.response, err)
		}
	case <-ctx.Done():
		t.Fatal("stalled upload ignored concurrent commit")
	}
}
