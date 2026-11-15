package main

import (
	"context"
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/buildbarn/bb-storage/pkg/program"
	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/p2p"
	"github.com/deepinthebuild/bushel/internal/publication"
	"github.com/deepinthebuild/bushel/internal/reapi"
	"github.com/deepinthebuild/bushel/internal/storage"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	ma "github.com/multiformats/go-multiaddr"
)

type values []string

func (v *values) String() string     { return strings.Join(*v, ",") }
func (v *values) Set(s string) error { *v = append(*v, s); return nil }

type config struct {
	data, grpc, instance                 string
	storageConfig                        string
	localStorage                         int64
	listen, peers, bootstrap             values
	discovery                            bool
	disk                                 int64
	timeout                              time.Duration
	routingRecords, routingPerPeer       int
	publicationPolicy                    publication.Policy
	acceptSeeds                          bool
	sendBytes, receiveBytes, seedMaxBlob int64
}

func parse(args []string) (config, error) {
	var c config
	fs := flag.NewFlagSet("bushel", flag.ContinueOnError)
	fs.StringVar(&c.data, "data-dir", "data", "private state directory")
	fs.StringVar(&c.grpc, "grpc-listen", "127.0.0.1:8980", "loopback REAPI address")
	fs.StringVar(&c.instance, "instance", "", "REAPI instance name")
	fs.Var(&c.listen, "p2p-listen", "content listener multiaddress (repeatable)")
	fs.Var(&c.peers, "peer", "explicit Bushel peer multiaddress including /p2p ID (repeatable)")
	fs.Var(&c.bootstrap, "bootstrap", "IPFS bootstrap peer multiaddress (repeatable; defaults to library list)")
	fs.BoolVar(&c.discovery, "discovery", true, "discover peers using the public IPFS rendezvous")
	fs.Int64Var(&c.disk, "staging-bytes", 20<<30, "upload staging/catalog budget; an equal budget is reserved for verified read scratch")
	fs.StringVar(&c.storageConfig, "storage-config", "", "Buildbarn ContentAddressableStorageConfiguration JSON/Jsonnet (default: local persistent files)")
	fs.Int64Var(&c.localStorage, "local-storage-bytes", 20<<30, "chunk file capacity for the default Buildbarn backend")
	fs.DurationVar(&c.timeout, "request-timeout", 30*time.Second, "maximum peer request duration")
	fs.IntVar(&c.routingRecords, "routing-records", 32768, "maximum cached blob-provider pairs")
	fs.IntVar(&c.routingPerPeer, "routing-records-per-peer", 0, "maximum cached pairs per provider (0: up to 16384 within total limit)")
	var policy string
	fs.StringVar(&policy, "federation-publish", "off", "publication classes: off, external, all, or comma-separated external,main,apparent,unknown")
	fs.BoolVar(&c.acceptSeeds, "federation-accept", false, "accept bounded unsolicited blob offers from Bushel peers")
	fs.Int64Var(&c.sendBytes, "federation-send-bytes-per-hour", 1<<30, "maximum charged outgoing publication bytes per hour")
	fs.Int64Var(&c.receiveBytes, "federation-receive-bytes-per-hour", 1<<30, "maximum admitted incoming publication bytes per hour")
	fs.Int64Var(&c.seedMaxBlob, "federation-max-blob-bytes", 256<<20, "largest unsolicited blob to accept")
	if err := fs.Parse(args); err != nil {
		return c, err
	}
	if fs.NArg() != 0 {
		return c, errors.New("unexpected positional arguments")
	}
	var policyErr error
	c.publicationPolicy, policyErr = publication.ParsePolicy(policy)
	if policyErr != nil {
		return c, policyErr
	}
	if c.sendBytes < 1 || c.sendBytes > 1<<40 || c.receiveBytes < 1 || c.receiveBytes > 1<<40 || c.seedMaxBlob < 1 || c.seedMaxBlob > cas.MaxBlobSize {
		return c, errors.New("invalid federation byte limits")
	}
	if c.data == "" || c.disk < 1<<20 || c.timeout <= 0 {
		return c, errors.New("invalid state directory, disk budget, or timeout")
	}
	if c.storageConfig == "" && c.localStorage < 16<<20 {
		return c, errors.New("default local storage needs at least 16 MiB")
	}
	if c.routingRecords < 64 || c.routingRecords > 1<<20 || c.routingPerPeer < 0 || c.routingPerPeer > c.routingRecords {
		return c, errors.New("invalid routing index capacity")
	}
	if len(c.listen) == 0 {
		c.listen = values{"/ip4/0.0.0.0/tcp/8981"}
	}
	if _, err := loopbackAddress(c.grpc); err != nil {
		return c, err
	}
	return c, nil
}

// Require a literal loopback IP. DNS changes cannot expose plaintext gRPC.
func loopbackAddress(address string) (string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", fmt.Errorf("grpc-listen: %w", err)
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return "", errors.New("grpc-listen must use a literal loopback IP")
	}
	if port == "" {
		return "", errors.New("grpc-listen needs a port")
	}
	return net.JoinHostPort(ip.String(), port), nil
}

func peerAddresses(values []string) ([]peer.AddrInfo, error) {
	peers := make([]peer.AddrInfo, 0, len(values))
	for _, value := range values {
		address, err := ma.NewMultiaddr(value)
		if err != nil {
			return nil, fmt.Errorf("invalid peer multiaddress: %w", err)
		}
		info, err := peer.AddrInfoFromP2pAddr(address)
		if err != nil {
			return nil, fmt.Errorf("peer address needs a /p2p identity: %w", err)
		}
		peers = append(peers, *info)
	}
	return peers, nil
}

// Called while holding the store process lock. Keys are generated at runtime and
// written atomically in the private state directory, never in source artifacts.
func identity(dir string) (crypto.PrivKey, error) {
	path := filepath.Join(dir, "identity.key")
	f, err := os.Open(path)
	if err == nil {
		data, readErr := io.ReadAll(io.LimitReader(f, 4097))
		closeErr := f.Close()
		if readErr != nil || closeErr != nil {
			return nil, errors.Join(readErr, closeErr)
		}
		if len(data) > 4096 {
			return nil, errors.New("identity file too large")
		}
		key, err := crypto.UnmarshalPrivateKey(data)
		if err != nil {
			return nil, errors.New("invalid identity file")
		}
		return key, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	key, _, err := crypto.GenerateEd25519Key(rand.Reader)
	if err != nil {
		return nil, err
	}
	data, err := crypto.MarshalPrivateKey(key)
	if err != nil {
		return nil, err
	}
	temp, err := os.CreateTemp(dir, "identity-")
	if err != nil {
		return nil, err
	}
	defer os.Remove(temp.Name())
	if err := temp.Chmod(0600); err != nil {
		temp.Close()
		return nil, err
	}
	if _, err := temp.Write(data); err != nil {
		temp.Close()
		return nil, err
	}
	if err := temp.Sync(); err != nil {
		temp.Close()
		return nil, err
	}
	if err := temp.Close(); err != nil {
		return nil, err
	}
	if err := os.Rename(temp.Name(), path); err != nil {
		return nil, err
	}
	return key, nil
}

// boundedListener caps transport connections in addition to per-RPC admission.
type boundedListener struct {
	net.Listener
	slots chan struct{}
	done  chan struct{}
	once  sync.Once
}

func newBoundedListener(l net.Listener, n int) *boundedListener {
	return &boundedListener{Listener: l, slots: make(chan struct{}, n), done: make(chan struct{})}
}
func (l *boundedListener) Accept() (net.Conn, error) {
	select {
	case l.slots <- struct{}{}:
	case <-l.done:
		return nil, net.ErrClosed
	}
	c, err := l.Listener.Accept()
	if err != nil {
		<-l.slots
		return nil, err
	}
	return &boundedConn{Conn: c, release: func() { <-l.slots }}, nil
}
func (l *boundedListener) Close() error {
	var err error
	l.once.Do(func() { close(l.done); err = l.Listener.Close() })
	return err
}

type boundedConn struct {
	net.Conn
	release func()
	once    sync.Once
}

func (c *boundedConn) Close() error { err := c.Conn.Close(); c.once.Do(c.release); return err }

func run(ctx context.Context, c config) error {
	lock, err := lockDirectory(c.data)
	if err != nil {
		return err
	}
	defer lock.Unlock()
	return program.RunLocal(ctx, func(ctx context.Context, _, dependencies program.Group) error {
		var backend cas.Backend
		cfg, err := storageConfiguration(c)
		if err != nil {
			return err
		}
		startupCtx, cancel := context.WithTimeout(ctx, c.timeout)
		backend, err = storage.New(startupCtx, dependencies, cfg, c.instance)
		cancel()
		if err != nil {
			return err
		}
		store, err := cas.NewStore(filepath.Join(c.data, "state"), c.disk, backend)
		if err != nil {
			return err
		}
		defer store.Close()
		return serve(ctx, c, store)
	})
}

func serve(ctx context.Context, c config, store *cas.Store) error {
	key, err := identity(c.data)
	if err != nil {
		return err
	}
	peers, err := peerAddresses(c.peers)
	if err != nil {
		return err
	}
	bootstrap, err := peerAddresses(c.bootstrap)
	if err != nil {
		return err
	}
	network, err := p2p.New(ctx, p2p.Config{Store: store, PrivateKey: key, Listen: c.listen, Peers: peers, Bootstrap: bootstrap, Discovery: c.discovery, RequestTimeout: c.timeout, MaxPeers: 64,
		RoutingRecords: c.routingRecords, RoutingRecordsPerPeer: c.routingPerPeer,
		AcceptSeeds: c.acceptSeeds, SeedMaxBlobBytes: c.seedMaxBlob, SeedBytesPerHour: c.receiveBytes})
	if err != nil {
		return err
	}
	defer network.Close()
	var publisher *publication.Publisher
	if c.publicationPolicy != 0 {
		publisher, err = publication.New(ctx, filepath.Join(c.data, "publication"), store, network, publication.Options{Policy: c.publicationPolicy, Instance: c.instance, BytesPerHour: c.sendBytes})
		if err != nil {
			return err
		}
		defer func() {
			if err := publisher.Close(); err != nil {
				slog.Error("federated publication stopped with state error", "error", err)
			}
		}()
	}
	address, err := loopbackAddress(c.grpc)
	if err != nil {
		return err
	}
	l, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	listener := newBoundedListener(l, reapi.MaxConnections)
	defer listener.Close()
	api := reapi.New(store, network, c.instance)
	if publisher != nil {
		api.WithPublicationObserver(publisher)
	}
	server := api.GRPC()
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	slog.Info("bushel ready", "grpc", listener.Addr().String(), "peer", network.ID().String(), "addresses", network.Addrs(), "discovery", c.discovery)
	var report <-chan time.Time
	if publisher != nil {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		report = ticker.C
	}
serving:
	for {
		select {
		case err := <-serveErr:
			server.Stop()
			return err
		case <-ctx.Done():
			break serving
		case <-report:
			s := publisher.Stats()
			slog.Info("federated publication progress", "queued", s.Queued, "queued_bytes", s.QueuedBytes, "oldest_pending_seconds", s.OldestPendingSeconds, "unconfirmed_attempts", s.UnconfirmedAttempts, "waiting", s.Waiting, "completed", s.Completed, "verified_peers", s.VerifiedPeers, "exhausted", s.Exhausted, "expired", s.Expired, "dropped", s.Dropped, "charged_bytes", s.ChargedBytes, "charged_offers", s.ChargedOffers, "state_failed", s.PersistenceFailed)
		}
	}
	done := make(chan struct{})
	go func() { server.GracefulStop(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		server.Stop()
		<-done
	}
	return nil
}

func main() {
	c, err := parse(os.Args[1:])
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		slog.Error("configuration failed", "error", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, c); err != nil {
		slog.Error("bushel stopped", "error", err)
		os.Exit(1)
	}
}
