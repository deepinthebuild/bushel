package p2p

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/membership"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const MembershipProtocol protocol.ID = "/bushel/membership/1.0.0"

const (
	membershipWorkers   = 2
	membershipPeerLimit = 32
	membershipByteLimit = 16 << 20
)

type membershipOptions struct {
	maxKeys, maxTail, hintBuffer                int
	rebuildInterval, recoveryInterval           time.Duration
	buildTimeout, publishInterval               time.Duration
	pollInterval, pollTick, requestTimeout, ttl time.Duration
	rotationInterval                            time.Duration
}

func (o membershipOptions) withDefaults() (membershipOptions, error) {
	if o.maxKeys == 0 {
		o.maxKeys = membership.MaxKeys
	}
	if o.maxTail == 0 {
		o.maxTail = membership.MaxTail
	}
	if o.hintBuffer == 0 {
		o.hintBuffer = 4096
	}
	if o.maxKeys < 1 || o.maxKeys > membership.MaxKeys || o.maxTail < 1 || o.maxTail > membership.MaxTail || o.hintBuffer < 1 || o.hintBuffer > 4096 {
		return o, membership.ErrInvalid
	}
	defaults := []struct {
		value *time.Duration
		def   time.Duration
	}{
		{&o.rebuildInterval, 5 * time.Minute}, {&o.recoveryInterval, 30 * time.Second},
		{&o.buildTimeout, 2 * time.Minute}, {&o.publishInterval, time.Second},
		{&o.pollInterval, time.Minute}, {&o.pollTick, 5 * time.Second},
		{&o.requestTimeout, 5 * time.Second}, {&o.ttl, 3 * time.Minute},
		{&o.rotationInterval, 30 * time.Minute},
	}
	for _, d := range defaults {
		if *d.value == 0 {
			*d.value = d.def
		}
		if *d.value <= 0 || *d.value > d.def {
			return o, membership.ErrInvalid
		}
	}
	if o.recoveryInterval > o.rebuildInterval || o.publishInterval > o.recoveryInterval || o.pollTick > o.pollInterval {
		return o, membership.ErrInvalid
	}
	return o, nil
}

type membershipPublication struct {
	view *membership.View
	data []byte
	hash [32]byte
}

type membershipPeerView struct {
	view    *membership.View
	bytes   int
	updated time.Time
	hash    [32]byte
}

type membershipService struct {
	node       *Node
	opts       membershipOptions
	session    [16]byte
	local      atomic.Pointer[membershipPublication]
	serving    chan struct{}
	wake       chan struct{}
	notify     *network.NotifyBundle
	rateMu     sync.Mutex
	rate       membershipRequestRate
	cacheMu    sync.Mutex
	cache      map[peer.ID]membershipPeerView
	bytes      int
	misses     atomic.Uint64
	used       atomic.Bool
	building   atomic.Bool
	builds     atomic.Uint64
	incomplete atomic.Uint64
}

// MembershipStats reports advisory state only. PositiveHintMisses counts normal
// manifest absence after a positive hint, including false positives and eviction.
type MembershipStats struct {
	Peers              int
	EncodedBytes       int
	PositiveHintMisses uint64
	LocalReady         bool
	LocalComplete      bool
	LocalBuilding      bool
	LocalGeneration    uint64
	LocalSequence      uint64
	BuildsStarted      uint64
	IncompleteBuilds   uint64
}

func (n *Node) MembershipStats() MembershipStats {
	if n.membership == nil {
		return MembershipStats{}
	}
	m := n.membership
	m.cacheMu.Lock()
	defer m.cacheMu.Unlock()
	stats := MembershipStats{Peers: len(m.cache), EncodedBytes: m.bytes, PositiveHintMisses: m.misses.Load(),
		LocalBuilding: m.building.Load(), BuildsStarted: m.builds.Load(), IncompleteBuilds: m.incomplete.Load()}
	if local := m.local.Load(); local != nil {
		stats.LocalReady, stats.LocalComplete = true, local.view.Complete()
		stats.LocalGeneration, stats.LocalSequence = local.view.Generation(), local.view.Sequence()
	}
	return stats
}

func (n *Node) initMembership(options membershipOptions) error {
	opts, err := options.withDefaults()
	if err != nil {
		return err
	}
	opts.requestTimeout = min(n.timeout, opts.requestTimeout)
	m := &membershipService{node: n, opts: opts, serving: make(chan struct{}, 2), wake: make(chan struct{}, 1),
		cache: make(map[peer.ID]membershipPeerView)}
	if _, err := rand.Read(m.session[:]); err != nil {
		return err
	}
	// Register synchronously before any walk can begin.
	hints, overflow, unsubscribe := n.store.SubscribeCommits(opts.hintBuffer)
	n.membership = m
	n.host.SetStreamHandler(MembershipProtocol, m.serve)
	m.notify = &network.NotifyBundle{
		ConnectedF:    func(network.Network, network.Conn) { m.signal() },
		DisconnectedF: func(network.Network, network.Conn) { m.signal() },
	}
	n.host.Network().Notify(m.notify)
	n.wg.Go(func() { m.capture(n.ctx, n.store, hints, overflow, unsubscribe) })
	n.wg.Go(func() {
		defer n.host.Network().StopNotify(m.notify)
		m.poll(n.ctx)
	})
	return nil
}

func (m *membershipService) signal() {
	select {
	case m.wake <- struct{}{}:
	default:
	}
}

// Four requests per second with an eight-request burst bounds serialization-free
// serving even when an authenticated peer repeatedly polls the same cached view.
type membershipRequestRate struct {
	at      time.Time
	tokens  int
	started bool
}

func (r *membershipRequestRate) allow(now time.Time) bool {
	const interval = time.Second / 4
	if !r.started {
		r.at, r.tokens, r.started = now, 8, true
	}
	if now.After(r.at) {
		refill := int(now.Sub(r.at) / interval)
		if refill > 0 {
			r.tokens = min(8, r.tokens+refill)
			r.at = r.at.Add(time.Duration(refill) * interval)
		}
	}
	if r.tokens == 0 {
		return false
	}
	r.tokens--
	return true
}

func (m *membershipService) serve(stream network.Stream) {
	if !m.node.start() {
		_ = stream.Reset()
		return
	}
	defer m.node.wg.Done()
	defer stream.Close()
	m.rateMu.Lock()
	allowed := m.rate.allow(time.Now())
	m.rateMu.Unlock()
	if !allowed {
		_ = stream.Reset()
		return
	}
	select {
	case m.serving <- struct{}{}:
		defer func() { <-m.serving }()
	default:
		_ = stream.Reset()
		return
	}
	ctx, cancel := context.WithTimeout(m.node.ctx, m.opts.requestTimeout)
	defer cancel()
	stop := context.AfterFunc(ctx, func() { _ = stream.Reset() })
	defer stop()
	deadline, _ := ctx.Deadline()
	_ = stream.SetDeadline(deadline)
	var request [1]byte
	if _, err := io.ReadFull(stream, request[:]); err != nil || request[0] > membership.MaxRequestBytes {
		_ = stream.Reset()
		return
	}
	requestData := make([]byte, int(request[0]))
	if _, err := io.ReadFull(stream, requestData); err != nil {
		_ = stream.Reset()
		return
	}
	known, err := membership.DecodeRequest(requestData)
	if err != nil {
		_ = stream.Reset()
		return
	}
	var extra [1]byte
	if _, err := stream.Read(extra[:]); err != io.EOF {
		_ = stream.Reset()
		return
	}
	publication := m.local.Load()
	var data []byte
	if publication != nil {
		data = publication.data
	}
	var header [4]byte
	size := uint32(len(data))
	if publication != nil && known != nil && *known == publication.hash {
		size, data = 1, nil
	}
	binary.BigEndian.PutUint32(header[:], size)
	if _, err := stream.Write(header[:]); err == nil && len(data) != 0 {
		_, _ = stream.Write(data)
	}
}

type membershipReadResult struct {
	view      *membership.View
	bytes     int
	hash      [32]byte
	unchanged bool
}

func (m *membershipService) read(ctx context.Context, id peer.ID, known *membershipPeerView) (membershipReadResult, error) {
	ctx, cancel := context.WithTimeout(ctx, m.opts.requestTimeout)
	defer cancel()
	ctx = network.WithNoDial(ctx, "membership uses existing content connections")
	stream, err := m.node.host.NewStream(ctx, id, MembershipProtocol)
	if err != nil {
		return membershipReadResult{}, err
	}
	defer stream.Close()
	stop := context.AfterFunc(ctx, func() { _ = stream.Reset() })
	defer stop()
	deadline, _ := ctx.Deadline()
	_ = stream.SetDeadline(deadline)
	var hash *[32]byte
	if known != nil {
		hash = &known.hash
	}
	request := membership.EncodeRequest(hash)
	if _, err := stream.Write(append([]byte{byte(len(request))}, request...)); err != nil {
		return membershipReadResult{}, err
	}
	if err := stream.CloseWrite(); err != nil {
		return membershipReadResult{}, err
	}
	var header [4]byte
	if _, err := io.ReadFull(stream, header[:]); err != nil {
		return membershipReadResult{}, err
	}
	size := binary.BigEndian.Uint32(header[:])
	if size > membership.MaxWireBytes {
		return membershipReadResult{}, membership.ErrInvalid
	}
	var data []byte
	if size > 1 {
		data = make([]byte, size)
	}
	if _, err := io.ReadFull(stream, data); err != nil {
		return membershipReadResult{}, err
	}
	var extra [1]byte
	if _, err := stream.Read(extra[:]); err != io.EOF {
		return membershipReadResult{}, membership.ErrInvalid
	}
	if size == 0 {
		return membershipReadResult{}, nil
	}
	if size == 1 {
		if known == nil {
			return membershipReadResult{}, membership.ErrInvalid
		}
		return membershipReadResult{view: known.view, bytes: known.bytes, hash: known.hash, unchanged: true}, nil
	}
	view, err := membership.Decode(data)
	return membershipReadResult{view: view, bytes: len(data), hash: sha256.Sum256(data)}, err
}

func (m *membershipService) positive(id peer.ID, d cas.Digest, now time.Time) bool {
	m.cacheMu.Lock()
	record, ok := m.cache[id]
	m.cacheMu.Unlock()
	return ok && now.Before(record.updated.Add(m.opts.ttl)) && record.view.MayContain(d)
}

func (m *membershipService) observeManifest(positive bool, err error) {
	if positive && errors.Is(err, cas.ErrNotFound) {
		m.misses.Add(1)
	}
}
