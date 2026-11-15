package reapi

import (
	"bytes"
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/teststorage"
	"github.com/deepinthebuild/bushel/internal/workspace"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type publicationEvent struct {
	digest    cas.Digest
	metadata  workspace.Metadata
	present   bool
	committed bool
}

type recordingObserver struct {
	mu     sync.Mutex
	events []publicationEvent
}

func (o *recordingObserver) Observe(d cas.Digest, m workspace.Metadata, present bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, publicationEvent{digest: d, metadata: m, present: present})
}

func (o *recordingObserver) Committed(d cas.Digest, m workspace.Metadata) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.events = append(o.events, publicationEvent{digest: d, metadata: m, committed: true})
}

func (o *recordingObserver) take() []publicationEvent {
	o.mu.Lock()
	defer o.mu.Unlock()
	events := o.events
	o.events = nil
	return events
}

func observedServer(t *testing.T) (*Server, *recordingObserver, *teststorage.Store) {
	t.Helper()
	backend := teststorage.New()
	store, err := cas.NewStore(t.TempDir(), 16<<20, cas.Backend{Chunks: backend.Chunks, Mappings: backend.Mappings})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	observer := &recordingObserver{}
	return New(store, nil, "").WithPublicationObserver(observer), observer, backend
}

func observationContext(t *testing.T, label string) context.Context {
	t.Helper()
	raw, err := proto.Marshal(&repb.RequestMetadata{ToolInvocationId: "invocation", ActionId: "action", TargetId: label})
	if err != nil {
		t.Fatal(err)
	}
	// Multiple equal metadata values are permitted and coalesce.
	return metadata.NewIncomingContext(t.Context(), metadata.Pairs(workspace.MetadataHeader, string(raw), workspace.MetadataHeader, string(raw)))
}

func TestPublicationObserverPresenceAndBatchSuccessOnly(t *testing.T) {
	s, observer, backend := observedServer(t)
	ctx := observationContext(t, "@@repository+suffix//pkg:t")
	stored, absent, uploaded := []byte("stored"), []byte("absent"), []byte("uploaded")
	if err := s.store.Put(ctx, cas.Sum(stored), bytes.NewReader(stored)); err != nil {
		t.Fatal(err)
	}
	if len(observer.take()) != 0 {
		t.Fatal("generic storage commit became local intent")
	}
	if _, err := s.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{pd(stored), pd(absent), pd(stored)}}); err != nil {
		t.Fatal(err)
	}
	events := observer.take()
	if len(events) != 2 || !events[0].present || events[1].present || events[0].digest != cas.Sum(stored) || events[1].digest != cas.Sum(absent) {
		t.Fatalf("presence observations: %+v", events)
	}
	for _, event := range events {
		if event.committed || !event.metadata.Valid || event.metadata.TargetID != "@@repository+suffix//pkg:t" {
			t.Fatal("lost metadata")
		}
	}
	if _, err := s.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{pd(stored), nil}}); err == nil {
		t.Fatal("invalid request accepted")
	}
	backend.Fail(teststorage.ChunkFindMissing, 0, status.Error(codes.Unavailable, "backend unavailable"))
	if _, err := s.FindMissingBlobs(ctx, &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{pd(stored)}}); err == nil {
		t.Fatal("failed presence accepted")
	}
	if len(observer.take()) != 0 {
		t.Fatal("failed request generated observations")
	}
	response, err := s.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{Requests: []*repb.BatchUpdateBlobsRequest_Request{
		{Digest: pd(uploaded), Data: uploaded}, {Digest: pd(absent), Data: []byte("wrong!")},
	}})
	if err != nil || response.Responses[0].Status.Code != 0 || response.Responses[1].Status.Code == 0 {
		t.Fatalf("batch: %v %v", response, err)
	}
	events = observer.take()
	if len(events) != 1 || !events[0].committed || events[0].digest != cas.Sum(uploaded) {
		t.Fatalf("commit observations: %+v", events)
	}
	if _, err := s.BatchReadBlobs(ctx, &repb.BatchReadBlobsRequest{Digests: []*repb.Digest{pd(uploaded)}}); err != nil {
		t.Fatal(err)
	}
	read := &observedRead{ctx: ctx}
	if err := s.Read(&bspb.ReadRequest{ResourceName: resource(uploaded)}, read); err != nil {
		t.Fatal(err)
	}
	if len(observer.take()) != 0 {
		t.Fatal("reads created publication intent")
	}
}

type observedRead struct {
	grpc.ServerStream
	ctx context.Context
}

func (s *observedRead) Context() context.Context      { return s.ctx }
func (s *observedRead) Send(*bspb.ReadResponse) error { return nil }

type importingFetcher struct {
	store *cas.Store
	data  []byte
}

func (f importingFetcher) Fetch(ctx context.Context, d cas.Digest) error {
	return f.store.Put(ctx, d, bytes.NewReader(f.data))
}

func TestPeerReadImportDoesNotCreatePublicationIntent(t *testing.T) {
	s, observer, _ := observedServer(t)
	data := []byte("imported peer data")
	s.fetcher = importingFetcher{store: s.store, data: data}
	ctx := observationContext(t, "@@external//pkg:t")
	if err := s.Read(&bspb.ReadRequest{ResourceName: resource(data)}, &observedRead{ctx: ctx}); err != nil {
		t.Fatal(err)
	}
	if present, err := s.store.Has(ctx, cas.Sum(data)); err != nil || !present {
		t.Fatalf("peer read did not populate cache: %t %v", present, err)
	}
	if len(observer.take()) != 0 {
		t.Fatal("peer read created forwarding intent")
	}
}

type observedUpload struct {
	grpc.ServerStream
	ctx       context.Context
	requests  []*bspb.WriteRequest
	sendError error
}

func (s *observedUpload) Context() context.Context { return s.ctx }
func (s *observedUpload) Recv() (*bspb.WriteRequest, error) {
	if len(s.requests) == 0 {
		return nil, io.EOF
	}
	request := s.requests[0]
	s.requests = s.requests[1:]
	return request, nil
}
func (s *observedUpload) SendAndClose(*bspb.WriteResponse) error { return s.sendError }

func TestPublicationObserverWriteStatusAndRecovery(t *testing.T) {
	for _, mode := range []string{"new", "duplicate", "ambiguous", "status", "corrupt", "send failure"} {
		t.Run(mode, func(t *testing.T) {
			s, observer, backend := observedServer(t)
			ctx := observationContext(t, "//pkg:target")
			data := bytes.Repeat([]byte("test payload"), 48000)
			d := cas.Sum(data)
			if mode == "duplicate" || mode == "status" || mode == "ambiguous" {
				if mode == "ambiguous" {
					backend.FailAfterWrite(teststorage.MappingPut, 0, status.Error(codes.Unavailable, "ambiguous write"))
				}
				err := s.store.Put(ctx, d, bytes.NewReader(data))
				if (err != nil) != (mode == "ambiguous") {
					t.Fatal(err)
				}
			}
			name := "uploads/observation/" + resource(data)
			var err error
			if mode == "status" {
				_, err = s.QueryWriteStatus(ctx, &bspb.QueryWriteStatusRequest{ResourceName: name})
			} else {
				body := bytes.Clone(data)
				if mode == "corrupt" {
					body[0] ^= 1
				}
				stream := &observedUpload{ctx: ctx, requests: []*bspb.WriteRequest{{ResourceName: name, Data: body, FinishWrite: true}}}
				if mode == "send failure" {
					stream.sendError = errors.New("response transport failed")
				}
				err = s.Write(stream)
			}
			failure := mode == "corrupt" || mode == "send failure"
			if (err != nil) != failure {
				t.Fatalf("operation outcome: %v", err)
			}
			events := observer.take()
			if failure {
				if len(events) != 0 {
					t.Fatal("failed upload observed as committed")
				}
			} else if len(events) != 1 || !events[0].committed || events[0].digest != d || events[0].metadata.TargetID != "//pkg:target" {
				t.Fatalf("successful operation lost observation: %+v", events)
			}
		})
	}
}

func TestPublicationObserverCoalescedUploadsKeepBothMetadata(t *testing.T) {
	s, observer, _ := observedServer(t)
	ctx, cancel := context.WithTimeout(observationContext(t, "@@one//pkg:t"), 5*time.Second)
	defer cancel()
	data := []byte("shared upload")
	stream := &stalledUpload{ctx: ctx, first: &bspb.WriteRequest{ResourceName: "uploads/first/" + resource(data), Data: data[:1]}, waiting: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- s.Write(stream) }()
	select {
	case <-stream.waiting:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if _, err := s.BatchUpdateBlobs(observationContext(t, "@@two//pkg:t"), &repb.BatchUpdateBlobsRequest{Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: pd(data), Data: data}}}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	events := observer.take()
	labels := make(map[string]bool)
	for _, event := range events {
		if !event.committed || event.digest != cas.Sum(data) {
			t.Fatal("coalesced observation lost digest")
		}
		labels[event.metadata.TargetID] = true
	}
	if len(events) != 2 || !labels["@@one//pkg:t"] || !labels["@@two//pkg:t"] {
		t.Fatalf("coalescing lost workspace metadata: %+v", events)
	}
}

func TestPublicationObserverUnknownMetadataDoesNotBreakCAS(t *testing.T) {
	s, observer, _ := observedServer(t)
	for _, values := range [][]string{nil, {"\xff"}, {"bad", "other"}} {
		ctx := metadata.NewIncomingContext(t.Context(), metadata.MD{workspace.MetadataHeader: values})
		data := []byte("unknown workspace")
		if _, err := s.BatchUpdateBlobs(ctx, &repb.BatchUpdateBlobsRequest{Requests: []*repb.BatchUpdateBlobsRequest_Request{{Digest: pd(data), Data: data}}}); err != nil {
			t.Fatal(err)
		}
		events := observer.take()
		if len(events) != 1 || events[0].metadata.Valid || !events[0].committed {
			t.Fatalf("unknown metadata lost successful write: %+v", events)
		}
	}
	if _, err := s.QueryWriteStatus(t.Context(), &bspb.QueryWriteStatusRequest{ResourceName: "uploads/missing/" + resource([]byte("missing"))}); status.Code(err) != codes.NotFound {
		t.Fatal(err)
	}
	if len(observer.take()) != 0 {
		t.Fatal("missing status created intent")
	}
}
