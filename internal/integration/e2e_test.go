package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"net"
	"strconv"
	"testing"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/p2p"
	"github.com/deepinthebuild/bushel/internal/reapi"
	"github.com/deepinthebuild/bushel/internal/teststorage"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func node(t *testing.T, ctx context.Context) (*p2p.Node, *grpc.ClientConn) {
	t.Helper()
	backend := teststorage.New()
	store, err := cas.NewStore(t.TempDir(), 64<<20, cas.Backend{Chunks: backend.Chunks, Mappings: backend.Mappings})
	if err != nil {
		t.Fatal(err)
	}
	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	n, err := p2p.New(ctx, p2p.Config{Store: store, PrivateKey: key, Listen: []string{"/ip4/127.0.0.1/tcp/0"}, RequestTimeout: 5 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	g := reapi.New(store, n, "").GRPC()
	go func() { _ = g.Serve(l) }()
	c, err := grpc.NewClient(l.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close(); g.Stop(); l.Close(); n.Close(); store.Close() })
	return n, c
}

func TestGRPCUploadToPeerBackedGRPCRead(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	a, ac := node(t, ctx)
	b, bc := node(t, ctx)
	addr, err := ma.NewMultiaddr(a.Addrs()[0])
	if err != nil {
		t.Fatal(err)
	}
	info, err := peer.AddrInfoFromP2pAddr(addr)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Connect(ctx, *info); err != nil {
		t.Fatal(err)
	}
	data := make([]byte, 3<<20)
	var x uint32 = 7
	for i := range data {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		data[i] = byte(x)
	}
	d := cas.Sum(data)
	resource := "blobs/" + d.Hex() + "/" + strconv.FormatInt(d.Size, 10)
	upload, err := bspb.NewByteStreamClient(ac).Write(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for off := 0; off < len(data); {
		end := min(off+(64<<10), len(data))
		req := &bspb.WriteRequest{WriteOffset: int64(off), Data: data[off:end], FinishWrite: end == len(data)}
		if off == 0 {
			req.ResourceName = "uploads/e2e/" + resource
		}
		if err := upload.Send(req); err != nil {
			t.Fatal(err)
		}
		off = end
	}
	if _, err := upload.CloseAndRecv(); err != nil {
		t.Fatal(err)
	}
	casClient := repb.NewContentAddressableStorageClient(bc)
	query := &repb.FindMissingBlobsRequest{BlobDigests: []*repb.Digest{{Hash: d.Hex(), SizeBytes: d.Size}}}
	before, err := casClient.FindMissingBlobs(ctx, query)
	if err != nil || len(before.GetMissingBlobDigests()) != 1 {
		t.Fatalf("receiver should start empty: %v %v", before, err)
	}
	read, err := bspb.NewByteStreamClient(bc).Read(ctx, &bspb.ReadRequest{ResourceName: resource})
	if err != nil {
		t.Fatal(err)
	}
	var result bytes.Buffer
	for {
		part, err := read.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		result.Write(part.Data)
	}
	if !bytes.Equal(result.Bytes(), data) || cas.Sum(result.Bytes()) != d {
		t.Fatal("peer-backed read differs from upload")
	}
	after, err := casClient.FindMissingBlobs(ctx, query)
	if err != nil || len(after.GetMissingBlobDigests()) != 0 {
		t.Fatalf("receiver did not commit verified blob: %v %v", after, err)
	}
}
