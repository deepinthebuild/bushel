package main

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
)

func TestLoopbackOnly(t *testing.T) {
	for _, address := range []string{"127.0.0.1:8980", "[::1]:8980"} {
		if _, err := loopbackAddress(address); err != nil {
			t.Fatal(err)
		}
	}
	for _, address := range []string{"0.0.0.0:8980", "[::]:8980", "localhost:8980", ":8980", "192.168.1.1:8980"} {
		if _, err := loopbackAddress(address); err == nil {
			t.Fatal("accepted " + address)
		}
	}
}

func TestIdentityPersistenceAndInvalidFile(t *testing.T) {
	dir := t.TempDir()
	a, err := identity(dir)
	if err != nil {
		t.Fatal(err)
	}
	b, err := identity(dir)
	if err != nil {
		t.Fatal(err)
	}
	ai, _ := peer.IDFromPrivateKey(a)
	bi, _ := peer.IDFromPrivateKey(b)
	if ai != bi {
		t.Fatal("identity changed")
	}
	if err := os.WriteFile(filepath.Join(dir, "identity.key"), []byte("invalid"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := identity(dir); err == nil {
		t.Fatal("silently replaced invalid identity")
	}
}

func TestListenerLimitAndShutdown(t *testing.T) {
	raw, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := newBoundedListener(raw, 1)
	defer l.Close()
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	accepted, err := l.Accept()
	if err != nil {
		t.Fatal(err)
	}
	defer accepted.Close()
	result := make(chan error, 1)
	go func() { _, err := l.Accept(); result <- err }()
	l.Close()
	select {
	case err := <-result:
		if !errors.Is(err, net.ErrClosed) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not unblock Accept")
	}
}

func TestDaemonStartsOfflineAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := config{data: t.TempDir(), grpc: "127.0.0.1:0", listen: values{"/ip4/127.0.0.1/tcp/0"}, discovery: false, disk: 64 << 20, timeout: time.Second}
	result := make(chan error, 1)
	go func() { result <- run(ctx, c) }()
	// The state lock and identity file establish that startup reached initialization.
	deadline := time.After(5 * time.Second)
	for {
		if _, err := os.Stat(filepath.Join(c.data, "identity.key")); err == nil {
			break
		}
		select {
		case err := <-result:
			t.Fatalf("startup: %v", err)
		case <-deadline:
			t.Fatal("startup timeout")
		case <-time.After(10 * time.Millisecond):
		}
	}
	cancel()
	select {
	case err := <-result:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("shutdown timeout")
	}
}
