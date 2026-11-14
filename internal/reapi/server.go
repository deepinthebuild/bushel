// Package reapi exposes Bushel's CAS through Bazel's Remote Execution API.
package reapi

import (
	"bytes"
	"context"
	"errors"
	"io"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	semver "github.com/bazelbuild/remote-apis/build/bazel/semver"
	"github.com/deepinthebuild/bushel/internal/cas"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	statuspb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	MaxBatchBytes      = 4 << 20
	MaxBatchItems      = 1024
	maxTreeDirectories = 4096
)

type Fetcher interface {
	Fetch(context.Context, cas.Digest) error
}

type Server struct {
	repb.UnimplementedContentAddressableStorageServer
	repb.UnimplementedCapabilitiesServer
	bspb.UnimplementedByteStreamServer
	store    *cas.Store
	fetcher  Fetcher
	instance string
}

func New(store *cas.Store, fetcher Fetcher, instance string) *Server {
	return &Server{store: store, fetcher: fetcher, instance: instance}
}

// GRPC bounds each message and admits at most 32 active RPCs across connections.
func (s *Server) GRPC() *grpc.Server {
	slots := make(chan struct{}, 32)
	enter := func(ctx context.Context) error {
		select {
		case slots <- struct{}{}:
			return nil
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		default:
			return status.Error(codes.ResourceExhausted, "too many active RPCs")
		}
	}
	g := grpc.NewServer(grpc.WaitForHandlers(true), grpc.MaxRecvMsgSize(MaxBatchBytes+(256<<10)), grpc.MaxSendMsgSize(MaxBatchBytes+(256<<10)), grpc.MaxConcurrentStreams(32),
		grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			if err := enter(ctx); err != nil {
				return nil, err
			}
			defer func() { <-slots }()
			return handler(ctx, req)
		}),
		grpc.StreamInterceptor(func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
			if err := enter(stream.Context()); err != nil {
				return err
			}
			defer func() { <-slots }()
			return handler(srv, stream)
		}))
	repb.RegisterContentAddressableStorageServer(g, s)
	repb.RegisterCapabilitiesServer(g, s)
	bspb.RegisterByteStreamServer(g, s)
	return g
}

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, cas.ErrInvalid):
		return status.Error(codes.InvalidArgument, "invalid digest or manifest")
	case errors.Is(err, cas.ErrNotFound):
		return status.Error(codes.NotFound, "blob not found")
	case errors.Is(err, cas.ErrCorrupt):
		return status.Error(codes.DataLoss, "content integrity check failed")
	case errors.Is(err, cas.ErrQuota):
		return status.Error(codes.ResourceExhausted, "CAS disk budget exhausted")
	default:
		if _, ok := status.FromError(err); ok {
			return err
		}
		return status.Error(codes.Internal, "CAS operation failed")
	}
}

func itemStatus(err error) *statuspb.Status {
	if err == nil {
		return &statuspb.Status{}
	}
	return status.Convert(rpcError(err)).Proto()
}

func (s *Server) check(instance string, function repb.DigestFunction_Value) error {
	if instance != s.instance {
		return status.Error(codes.NotFound, "unknown instance")
	}
	if function != repb.DigestFunction_UNKNOWN && function != repb.DigestFunction_SHA256 {
		return status.Error(codes.InvalidArgument, "only SHA-256 is supported")
	}
	return nil
}

func digest(d *repb.Digest) (cas.Digest, error) {
	if d == nil {
		return cas.Digest{}, status.Error(codes.InvalidArgument, "missing digest")
	}
	v, err := cas.ParseDigest(d.Hash, d.SizeBytes)
	return v, rpcError(err)
}

func (s *Server) open(ctx context.Context, d cas.Digest) (*cas.Reader, error) {
	r, err := s.store.Open(ctx, d)
	if (errors.Is(err, cas.ErrNotFound) || errors.Is(err, cas.ErrCorrupt)) && s.fetcher != nil {
		if err = s.fetcher.Fetch(ctx, d); err != nil {
			return nil, rpcError(err)
		}
		r, err = s.store.Open(ctx, d)
	}
	return r, rpcError(err)
}

func (s *Server) GetCapabilities(_ context.Context, req *repb.GetCapabilitiesRequest) (*repb.ServerCapabilities, error) {
	if err := s.check(req.InstanceName, repb.DigestFunction_SHA256); err != nil {
		return nil, err
	}
	return &repb.ServerCapabilities{LowApiVersion: &semver.SemVer{Major: 2}, HighApiVersion: &semver.SemVer{Major: 2, Minor: 13}, CacheCapabilities: &repb.CacheCapabilities{
		DigestFunctions: []repb.DigestFunction_Value{repb.DigestFunction_SHA256}, MaxBatchTotalSizeBytes: MaxBatchBytes, MaxCasBlobSizeBytes: cas.MaxBlobSize,
		ActionCacheUpdateCapabilities: &repb.ActionCacheUpdateCapabilities{UpdateEnabled: false},
	}}, nil
}

func (s *Server) FindMissingBlobs(ctx context.Context, req *repb.FindMissingBlobsRequest) (*repb.FindMissingBlobsResponse, error) {
	if err := s.check(req.InstanceName, req.DigestFunction); err != nil {
		return nil, err
	}
	if len(req.BlobDigests) > MaxBatchItems {
		return nil, status.Error(codes.ResourceExhausted, "too many digests")
	}
	resp := &repb.FindMissingBlobsResponse{}
	for _, p := range req.BlobDigests {
		d, err := digest(p)
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, rpcError(err)
		}
		ok, err := s.store.Committed(d)
		if err != nil {
			return nil, rpcError(err)
		}
		if !ok {
			resp.MissingBlobDigests = append(resp.MissingBlobDigests, p)
		}
	}
	return resp, nil
}

func (s *Server) BatchUpdateBlobs(ctx context.Context, req *repb.BatchUpdateBlobsRequest) (*repb.BatchUpdateBlobsResponse, error) {
	if err := s.check(req.InstanceName, req.DigestFunction); err != nil {
		return nil, err
	}
	if len(req.Requests) > MaxBatchItems {
		return nil, status.Error(codes.ResourceExhausted, "too many blobs")
	}
	var total int64
	for _, r := range req.Requests {
		total += int64(len(r.GetData()))
	}
	if total > MaxBatchBytes {
		return nil, status.Error(codes.InvalidArgument, "batch too large")
	}
	resp := &repb.BatchUpdateBlobsResponse{}
	for _, r := range req.Requests {
		d, err := digest(r.GetDigest())
		if err == nil && r.GetCompressor() != repb.Compressor_IDENTITY {
			err = status.Error(codes.InvalidArgument, "compression unsupported")
		}
		if err == nil && int64(len(r.GetData())) != d.Size {
			err = status.Error(codes.InvalidArgument, "upload size mismatch")
		}
		if err == nil {
			err = s.store.Put(ctx, d, bytes.NewReader(r.GetData()))
			if errors.Is(err, cas.ErrCorrupt) {
				err = status.Error(codes.InvalidArgument, "upload digest mismatch")
			}
		}
		resp.Responses = append(resp.Responses, &repb.BatchUpdateBlobsResponse_Response{Digest: r.GetDigest(), Status: itemStatus(err)})
	}
	return resp, nil
}

func (s *Server) BatchReadBlobs(ctx context.Context, req *repb.BatchReadBlobsRequest) (*repb.BatchReadBlobsResponse, error) {
	if err := s.check(req.InstanceName, req.DigestFunction); err != nil {
		return nil, err
	}
	if len(req.Digests) > MaxBatchItems {
		return nil, status.Error(codes.ResourceExhausted, "too many blobs")
	}
	var total int64
	for _, p := range req.Digests {
		d, err := digest(p)
		if err == nil {
			if d.Size > MaxBatchBytes-total {
				return nil, status.Error(codes.InvalidArgument, "batch too large")
			}
			total += d.Size
		}
	}
	resp := &repb.BatchReadBlobsResponse{}
	for _, p := range req.Digests {
		out := &repb.BatchReadBlobsResponse_Response{Digest: p, Compressor: repb.Compressor_IDENTITY}
		d, err := digest(p)
		if err == nil {
			r, e := s.open(ctx, d)
			err = e
			if err == nil {
				out.Data, err = io.ReadAll(r)
				err = errors.Join(err, r.Close())
			}
		}
		out.Status = itemStatus(err)
		resp.Responses = append(resp.Responses, out)
	}
	return resp, nil
}

func (s *Server) resource(name string, write bool) (cas.Digest, error) {
	return parseResource(s.instance, name, write)
}

func (s *Server) Read(req *bspb.ReadRequest, stream bspb.ByteStream_ReadServer) error {
	d, err := s.resource(req.ResourceName, false)
	if err != nil {
		return err
	}
	if req.ReadOffset < 0 || req.ReadOffset > d.Size || req.ReadLimit < 0 {
		return status.Error(codes.OutOfRange, "invalid read range")
	}
	r, err := s.open(stream.Context(), d)
	if err != nil {
		return err
	}
	defer r.Close()
	if _, err := r.Seek(req.ReadOffset, io.SeekStart); err != nil {
		return rpcError(err)
	}
	remaining := d.Size - req.ReadOffset
	if req.ReadLimit > 0 && req.ReadLimit < remaining {
		remaining = req.ReadLimit
	}
	buf := make([]byte, 64<<10)
	for remaining > 0 {
		n, err := r.Read(buf[:min(int64(len(buf)), remaining)])
		if err != nil {
			return rpcError(err)
		}
		if err := stream.Send(&bspb.ReadResponse{Data: bytes.Clone(buf[:n])}); err != nil {
			return err
		}
		remaining -= int64(n)
	}
	return nil
}

// Uploads are sequential and restart from zero after interruption. QueryWriteStatus
// returns NotFound for discarded partial uploads, allowing standards-based retries.
func (s *Server) Write(stream bspb.ByteStream_WriteServer) error {
	first, err := stream.Recv()
	if err != nil {
		if err == io.EOF {
			return status.Error(codes.InvalidArgument, "missing upload request")
		}
		return rpcError(err)
	}
	d, err := s.resource(first.ResourceName, true)
	if err != nil {
		return err
	}
	committed, unwatch := s.store.WatchCommit(d)
	defer unwatch()
	if ok, err := s.store.Has(stream.Context(), d); err != nil {
		return rpcError(err)
	} else if ok {
		return stream.SendAndClose(&bspb.WriteResponse{CommittedSize: d.Size})
	}
	requests := make(chan uploadMessage, 1)
	stop := make(chan struct{})
	defer close(stop)
	// One receive pump permits a concurrent commit to finish a stalled upload.
	// Returning from the handler cancels any pending transport Recv.
	go func() {
		for {
			req, err := stream.Recv()
			select {
			case requests <- uploadMessage{req, err}:
			case <-stop:
				return
			}
			if err != nil {
				return
			}
		}
	}()
	r := &uploadReader{ctx: stream.Context(), requests: requests, committed: committed, current: first,
		state: uploadState{resource: first.ResourceName, size: d.Size}}
	if err := s.store.Put(stream.Context(), d, r); err != nil {
		if errors.Is(err, errCommitted) {
			return stream.SendAndClose(&bspb.WriteResponse{CommittedSize: d.Size})
		}
		if ok, _ := s.store.Has(stream.Context(), d); ok {
			return stream.SendAndClose(&bspb.WriteResponse{CommittedSize: d.Size})
		}
		if errors.Is(err, cas.ErrCorrupt) {
			return status.Error(codes.InvalidArgument, "upload digest or size mismatch")
		}
		return rpcError(err)
	}
	return stream.SendAndClose(&bspb.WriteResponse{CommittedSize: d.Size})
}

type uploadReader struct {
	ctx       context.Context
	requests  <-chan uploadMessage
	committed <-chan struct{}
	current   *bspb.WriteRequest
	state     uploadState
	data      []byte
}

var errCommitted = errors.New("blob completed by another writer")

type uploadMessage struct {
	request *bspb.WriteRequest
	err     error
}

func (r *uploadReader) Read(p []byte) (int, error) {
	for len(r.data) == 0 {
		if r.state.finished {
			return 0, io.EOF
		}
		req := r.current
		r.current = nil
		if req == nil {
			var err error
			select {
			case <-r.ctx.Done():
				return 0, r.ctx.Err()
			case <-r.committed:
				return 0, errCommitted
			case msg := <-r.requests:
				req, err = msg.request, msg.err
			}
			if err == io.EOF {
				return 0, status.Error(codes.InvalidArgument, "upload ended without finish_write")
			}
			if err != nil {
				return 0, err
			}
		}
		next, err := advanceUpload(r.state, uploadPart{resource: req.ResourceName, offset: req.WriteOffset,
			dataSize: int64(len(req.Data)), finish: req.FinishWrite})
		if err != nil {
			return 0, status.Error(codes.InvalidArgument, err.Error())
		}
		r.state = next
		r.data = req.Data
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

func (s *Server) QueryWriteStatus(ctx context.Context, req *bspb.QueryWriteStatusRequest) (*bspb.QueryWriteStatusResponse, error) {
	d, err := s.resource(req.ResourceName, true)
	if err != nil {
		return nil, err
	}
	ok, err := s.store.Has(ctx, d)
	if err != nil {
		return nil, rpcError(err)
	}
	if !ok {
		return nil, status.Error(codes.NotFound, "upload not committed; restart at zero")
	}
	return &bspb.QueryWriteStatusResponse{CommittedSize: d.Size, Complete: true}, nil
}

func (s *Server) GetTree(req *repb.GetTreeRequest, stream grpc.ServerStreamingServer[repb.GetTreeResponse]) error {
	if err := s.check(req.InstanceName, req.DigestFunction); err != nil {
		return err
	}
	if req.PageToken != "" || req.PageSize < 0 {
		return status.Error(codes.InvalidArgument, "invalid tree pagination")
	}
	root, err := digest(req.RootDigest)
	if err != nil {
		return err
	}
	queue := []cas.Digest{root}
	seen := map[cas.Digest]bool{root: true}
	for len(queue) > 0 {
		d := queue[0]
		queue = queue[1:]
		if d.Size > MaxBatchBytes {
			return status.Error(codes.ResourceExhausted, "directory too large")
		}
		r, err := s.open(stream.Context(), d)
		if err != nil {
			if d != root && status.Code(err) == codes.NotFound {
				continue
			}
			return err
		}
		data, err := io.ReadAll(r)
		err = errors.Join(err, r.Close())
		if err != nil {
			return rpcError(err)
		}
		dir := &repb.Directory{}
		if err := proto.Unmarshal(data, dir); err != nil {
			return status.Error(codes.DataLoss, "invalid Directory blob")
		}
		for _, child := range dir.Directories {
			cd, err := digest(child.GetDigest())
			if err != nil {
				return status.Error(codes.DataLoss, "invalid child directory digest")
			}
			if !seen[cd] {
				if len(seen) >= maxTreeDirectories {
					return status.Error(codes.ResourceExhausted, "tree too large")
				}
				seen[cd] = true
				queue = append(queue, cd)
			}
		}
		if err := stream.Send(&repb.GetTreeResponse{Directories: []*repb.Directory{dir}}); err != nil {
			return err
		}
	}
	return nil
}
