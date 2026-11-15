package p2p

import (
	"bytes"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/teststorage"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
)

func seedNode(t *testing.T, cfg Config) (*Node, *cas.Store, *teststorage.Store) {
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
	cfg.Store, cfg.PrivateKey, cfg.Listen, cfg.MaxPeers = store, key, []string{"/ip4/127.0.0.1/tcp/0"}, 8
	if cfg.RequestTimeout == 0 {
		cfg.RequestTimeout = 2 * time.Second
	}
	n, err := New(t.Context(), cfg)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { n.Close(); store.Close() })
	return n, store, backend
}

func connectSeedNodes(t *testing.T, source, receiver *Node) {
	t.Helper()
	if err := source.Connect(t.Context(), peerInfo(receiver)); err != nil {
		t.Fatal(err)
	}
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !slices.Contains(source.SeedPeers(), receiver.ID()) {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("receiver did not advertise seed acceptance")
		}
	}
}

func TestSeedOfferVerifiedCompletionAndSourceDeparture(t *testing.T) {
	source, sourceStore, _ := seedNode(t, Config{})
	receiver, replicaStore, _ := seedNode(t, Config{AcceptSeeds: true})
	connectSeedNodes(t, source, receiver)
	data := testData(3<<20 + 17)
	d := cas.Sum(data)
	if err := sourceStore.Put(t.Context(), d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	ok, err := source.Offer(t.Context(), receiver.ID(), d)
	if err != nil || !ok {
		t.Fatalf("verified offer: %t, %v", ok, err)
	}
	if err := source.Close(); err != nil {
		t.Fatal(err)
	}
	r, err := replicaStore.Open(t.Context(), d)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(r)
	err = errors.Join(err, r.Close())
	if err != nil || !bytes.Equal(data, got) {
		t.Fatalf("replica after source departure: %v", err)
	}
	var catalog []cas.Digest
	if err := replicaStore.WalkCommitted(t.Context(), func(d cas.Digest) error { catalog = append(catalog, d); return nil }); err != nil || !slices.Contains(catalog, d) {
		t.Fatalf("replica catalog: %v", err)
	}
}

func TestSeedOfferOptOutAndLimits(t *testing.T) {
	source, sourceStore, _ := seedNode(t, Config{})
	optOut, _, _ := seedNode(t, Config{})
	if err := source.Connect(t.Context(), peerInfo(optOut)); err != nil {
		t.Fatal(err)
	}
	d := cas.Sum([]byte("small blob"))
	if ok, err := source.Offer(t.Context(), optOut.ID(), d); err != nil || ok {
		t.Fatalf("opt-out: %t %v", ok, err)
	}
	if slices.Contains(source.SeedPeers(), optOut.ID()) {
		t.Fatal("opt-out peer was selected")
	}
	if ok, err := source.Offer(t.Context(), source.ID(), d); err != nil || ok {
		t.Fatalf("self offer: %t %v", ok, err)
	}
	receiver, replicaStore, _ := seedNode(t, Config{AcceptSeeds: true, SeedMaxBlobBytes: 5, SeedBytesPerHour: 5})
	connectSeedNodes(t, source, receiver)
	if ok, err := source.Offer(t.Context(), receiver.ID(), d); err != nil || ok {
		t.Fatalf("oversized offer: %t %v", ok, err)
	}
	for _, data := range [][]byte{[]byte("1234"), []byte("ab"), nil} {
		d := cas.Sum(data)
		if err := sourceStore.Put(t.Context(), d, bytes.NewReader(data)); err != nil {
			t.Fatal(err)
		}
		ok, err := source.Offer(t.Context(), receiver.ID(), d)
		if err != nil || ok != (len(data) == 4 || len(data) == 0) {
			t.Fatalf("hourly allowance: %t %v", ok, err)
		}
		if present, err := replicaStore.Committed(t.Context(), d); err != nil || present != ok {
			t.Fatalf("declined blob published: %t %v", present, err)
		}
	}
}

func TestSeedOfferPullsOnlyFromAuthenticatedSource(t *testing.T) {
	source, _, sourceBackend := seedNode(t, Config{})
	receiver, replicaStore, _ := seedNode(t, Config{AcceptSeeds: true})
	other, otherStore, _ := seedNode(t, Config{})
	connectSeedNodes(t, source, receiver)
	if err := receiver.Connect(t.Context(), peerInfo(other)); err != nil {
		t.Fatal(err)
	}
	data := testData(700000)
	d := cas.Sum(data)
	if err := otherStore.Put(t.Context(), d, bytes.NewReader(data)); err != nil {
		t.Fatal(err)
	}
	manifest, err := otherStore.Manifest(t.Context(), d)
	if err != nil {
		t.Fatal(err)
	}
	seedSetMapping(t, sourceBackend, manifest)
	// The offering peer supplies a correct manifest but owns no chunks. A
	// connected third peer has every chunk; seeding must not ask that peer.
	source.host.SetStreamHandler(ManifestProtocol, func(stream network.Stream) {
		defer stream.Close()
		var request [40]byte
		if _, err := io.ReadFull(stream, request[:]); err != nil {
			return
		}
		payload, _ := manifest.MarshalBinary()
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], uint32(len(payload)))
		_, _ = stream.Write(header[:])
		_, _ = stream.Write(payload)
	})
	public, overlay := &routingSpy{}, &routingSpy{}
	receiver.router, receiver.blobRouter = public, overlay
	before := other.seeds.network.Stats().MessagesRecvd
	ok, err := source.Offer(t.Context(), receiver.ID(), d)
	if err != nil || ok {
		t.Fatalf("missing source chunks: %t %v", ok, err)
	}
	if other.seeds.network.Stats().MessagesRecvd != before || public.calls() != 0 || overlay.calls() != 0 {
		t.Fatal("seed pull contacted another peer or router")
	}
	if present, err := replicaStore.Committed(t.Context(), d); err != nil || present {
		t.Fatalf("unavailable seed published: %t %v", present, err)
	}
}
