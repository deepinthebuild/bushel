package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/chunk"
	"github.com/buildbarn/bb-storage/pkg/digest"
	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/p2p"
	"github.com/deepinthebuild/bushel/internal/reapi"
	"github.com/deepinthebuild/bushel/internal/storage"
	"github.com/deepinthebuild/bushel/internal/teststorage"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
	bspb "google.golang.org/genproto/googleapis/bytestream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type cachePayloadCase struct {
	name string
	size int
	seed uint32
}

// Exercise tiny blobs, both sides of the single-chunk boundary, a transfer
// spanning several Bitswap windows, and repeated chunks. Regenerating bytes
// in the second process avoids accidentally retaining downloaded buffers.
var cachePayloadCases = []cachePayloadCase{
	{"tiny", 1, 7},
	{"below minimum", cas.MinChunkSize - 1, 23},
	{"largest single chunk", cas.MaxChunkSize, 31},
	{"smallest mapping", cas.MaxChunkSize + 1, 47},
	{"multiple windows", 5<<20 + 117, 59},
	{"repeated chunks", 2<<20 + 73, 0},
}

func cachePayload(c cachePayloadCase) []byte {
	data := make([]byte, c.size)
	x := c.seed
	for i := range data {
		x ^= x << 13
		x ^= x >> 17
		x ^= x << 5
		data[i] = byte(x)
	}
	return data
}

func TestPeerReadCachesAcrossDisconnectAndRestart(t *testing.T) {
	root := t.TempDir()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	// Buildbarn owns process-lifetime mappings. Exit and flush the receiver
	// before reopening its backend, including on Windows.
	for _, mode := range []string{"fetch", "offline"} {
		ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
		cmd := exec.CommandContext(ctx, executable, "-test.run=^TestPeerCacheProcess$", "-test.timeout=45s")
		cmd.Env = append(os.Environ(), "BUSHEL_PEER_CACHE_TEST_MODE="+mode, "BUSHEL_PEER_CACHE_TEST_ROOT="+root)
		cmd.WaitDelay = 5 * time.Second
		output, err := cmd.CombinedOutput()
		cancel()
		if err != nil {
			t.Fatalf("peer cache %s process: %v\n%s", mode, err, output)
		}
	}
}

func TestPeerCacheProcess(t *testing.T) {
	mode := os.Getenv("BUSHEL_PEER_CACHE_TEST_MODE")
	if mode == "" {
		t.Skip("subprocess helper")
	}
	if mode != "fetch" && mode != "offline" {
		t.Fatalf("unknown peer cache test mode %q", mode)
	}
	root := os.Getenv("BUSHEL_PEER_CACHE_TEST_ROOT")
	if root == "" {
		t.Fatal("missing peer cache state directory")
	}
	configuration, err := storage.Default(filepath.Join(root, "backend"), 32<<20)
	if err != nil {
		t.Fatal(err)
	}
	err = program.RunLocal(t.Context(), func(ctx context.Context, _, dependencies program.Group) (retErr error) {
		backend, err := storage.New(ctx, dependencies, configuration, "")
		if err != nil {
			return err
		}
		store, err := cas.NewStore(filepath.Join(root, "receiver"), 64<<20, backend)
		if err != nil {
			return err
		}
		defer func() { retErr = errors.Join(retErr, store.Close()) }()
		if mode == "fetch" {
			return fetchAndDisconnect(ctx, root, store, backend)
		}
		// No peer node or fetcher exists in this process. Every byte must come
		// from the reopened receiver's persistent Buildbarn backend.
		client, closeRPC, err := cacheRPC(store, nil)
		if err != nil {
			return err
		}
		defer closeRPC()
		return verifyCachedPayloads(ctx, store, client)
	})
	if err != nil {
		t.Fatal(err)
	}
}

func fetchAndDisconnect(ctx context.Context, root string, receiver *cas.Store, receiverBackend cas.Backend) (retErr error) {
	sourceBackend := teststorage.New()
	source, err := cas.NewStore(filepath.Join(root, "source"), 64<<20, cas.Backend{
		Chunks: sourceBackend.Chunks, Mappings: sourceBackend.Mappings,
	})
	if err != nil {
		return err
	}
	defer func() { retErr = errors.Join(retErr, source.Close()) }()
	a, ac, closeSource, err := cachePeer(ctx, source)
	if err != nil {
		return err
	}
	defer closeSource()
	b, bc, closeReceiver, err := cachePeer(ctx, receiver)
	if err != nil {
		return err
	}
	defer closeReceiver()
	address, err := ma.NewMultiaddr(a.Addrs()[0])
	if err != nil {
		return err
	}
	info, err := peer.AddrInfoFromP2pAddr(address)
	if err != nil {
		return err
	}
	if err := b.Connect(ctx, *info); err != nil {
		return err
	}
	for _, c := range cachePayloadCases {
		if err := uploadCachedPayload(ctx, ac, cachePayload(c)); err != nil {
			return fmt.Errorf("upload %s: %w", c.name, err)
		}
	}
	if err := rejectFalseBlobDigest(ctx, source, sourceBackend, receiver, receiverBackend, bc); err != nil {
		return err
	}
	for _, c := range cachePayloadCases {
		data := cachePayload(c)
		d := cas.Sum(data)
		if present, err := receiver.Committed(ctx, d); err != nil || present {
			return fmt.Errorf("%s unexpectedly cached before peer read: %t, %v", c.name, present, err)
		}
		if err := readCachedPayload(ctx, bc, data); err != nil {
			return fmt.Errorf("fetch %s: %w", c.name, err)
		}
	}
	// Close the source transport and its REAPI server before reading again.
	// No remote source can satisfy the receiver's subsequent requests.
	closeSource()
	return verifyCachedPayloads(ctx, receiver, bc)
}

func rejectFalseBlobDigest(ctx context.Context, source *cas.Store, sourceBackend *teststorage.Store,
	receiver *cas.Store, receiverBackend cas.Backend, client *grpc.ClientConn,
) error {
	data := cachePayload(cachePayloadCases[len(cachePayloadCases)-1])
	d := cas.Sum(data)
	m, err := source.Manifest(ctx, d)
	if err != nil {
		return err
	}
	function := digest.MustNewFunction("", repb.DigestFunction_SHA256)
	keys := make([]digest.Digest, len(m.Chunks))
	keySet := digest.NewSetBuilder(len(keys))
	for i, c := range m.Chunks {
		keys[i], err = function.NewDigest(c.Hex(), c.Size)
		if err != nil {
			return err
		}
		keySet.Add(keys[i])
	}
	mapping, err := chunk.NewMappingFromDigests(keys, uint64(d.Size), true)
	if err != nil {
		return err
	}
	wrong := d
	wrong.Hash[0] ^= 1
	wrongKey, err := function.NewDigest(wrong.Hex(), wrong.Size)
	if err != nil {
		return err
	}
	// Each supplied chunk has a valid digest, but their composition does not
	// match the advertised blob. A peer-controlled mapping is not authority.
	sourceBackend.SetMapping(wrongKey, mapping)
	stream, err := bspb.NewByteStreamClient(client).Read(ctx, &bspb.ReadRequest{ResourceName: cacheResource(wrong)})
	if err != nil {
		return err
	}
	response, err := stream.Recv()
	if status.Code(err) != codes.DataLoss || len(response.GetData()) != 0 {
		return fmt.Errorf("false blob digest exposed bytes or was accepted: %v", err)
	}
	keysToCheck := keySet.Build()
	missing, err := receiverBackend.Chunks.FindMissing(ctx, keysToCheck)
	if err != nil || missing.Length() != keysToCheck.Length() {
		return fmt.Errorf("unverified peer download wrote local chunks: %v", err)
	}
	if present, err := receiver.Committed(ctx, wrong); err != nil || present {
		return fmt.Errorf("false peer blob was published: %t, %v", present, err)
	}
	return receiver.WalkCommitted(ctx, func(cas.Digest) error {
		return errors.New("failed peer verification published a catalog entry")
	})
}

func verifyCachedPayloads(ctx context.Context, store *cas.Store, client *grpc.ClientConn) error {
	want := make(map[cas.Digest]bool, len(cachePayloadCases))
	for _, c := range cachePayloadCases {
		data := cachePayload(c)
		d := cas.Sum(data)
		want[d] = true
		if err := readCachedPayload(ctx, client, data); err != nil {
			return fmt.Errorf("offline read %s: %w", c.name, err)
		}
		if present, err := store.Committed(ctx, d); err != nil || !present {
			return fmt.Errorf("cached %s lost backend presence: %t, %v", c.name, present, err)
		}
	}
	if err := store.WalkCommitted(ctx, func(d cas.Digest) error {
		if !want[d] {
			return errors.New("unexpected or duplicate announcement catalog entry")
		}
		delete(want, d)
		return nil
	}); err != nil {
		return err
	}
	if len(want) != 0 {
		return errors.New("fetched blobs did not survive in the announcement catalog")
	}
	return nil
}

func cacheResource(d cas.Digest) string {
	return "blobs/" + d.Hex() + "/" + strconv.FormatInt(d.Size, 10)
}

func uploadCachedPayload(ctx context.Context, client *grpc.ClientConn, data []byte) error {
	d := cas.Sum(data)
	stream, err := bspb.NewByteStreamClient(client).Write(ctx)
	if err != nil {
		return err
	}
	for offset := 0; offset < len(data); {
		end := min(offset+(64<<10), len(data))
		req := &bspb.WriteRequest{WriteOffset: int64(offset), Data: data[offset:end], FinishWrite: end == len(data)}
		if offset == 0 {
			req.ResourceName = "uploads/cache-regression/" + cacheResource(d)
		}
		if err := stream.Send(req); err != nil {
			return err
		}
		offset = end
	}
	response, err := stream.CloseAndRecv()
	if err == nil && response.GetCommittedSize() != d.Size {
		return errors.New("source upload reported the wrong committed size")
	}
	return err
}

func readCachedPayload(ctx context.Context, client *grpc.ClientConn, data []byte) error {
	d := cas.Sum(data)
	stream, err := bspb.NewByteStreamClient(client).Read(ctx, &bspb.ReadRequest{ResourceName: cacheResource(d)})
	if err != nil {
		return err
	}
	var got bytes.Buffer
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		got.Write(response.Data)
	}
	if !bytes.Equal(got.Bytes(), data) || cas.Sum(got.Bytes()) != d {
		return errors.New("cached read differs from generated payload")
	}
	return nil
}

func cachePeer(ctx context.Context, store *cas.Store) (*p2p.Node, *grpc.ClientConn, func(), error) {
	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}
	n, err := p2p.New(ctx, p2p.Config{Store: store, PrivateKey: key,
		Listen: []string{"/ip4/127.0.0.1/tcp/0"}, RequestTimeout: 5 * time.Second})
	if err != nil {
		return nil, nil, nil, err
	}
	client, closeRPC, err := cacheRPC(store, n)
	if err != nil {
		_ = n.Close()
		return nil, nil, nil, err
	}
	close := sync.OnceFunc(func() { closeRPC(); _ = n.Close() })
	return n, client, close, nil
}

func cacheRPC(store *cas.Store, fetcher reapi.Fetcher) (*grpc.ClientConn, func(), error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, nil, err
	}
	server := reapi.New(store, fetcher, "").GRPC()
	go func() { _ = server.Serve(listener) }()
	client, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		server.Stop()
		_ = listener.Close()
		return nil, nil, err
	}
	close := sync.OnceFunc(func() { _ = client.Close(); server.Stop(); _ = listener.Close() })
	return client, close, nil
}
