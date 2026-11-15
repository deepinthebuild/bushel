package publication

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/deepinthebuild/bushel/internal/workspace"
	"github.com/libp2p/go-libp2p/core/peer"
)

type Source interface {
	Has(context.Context, cas.Digest) (bool, error)
}

type Transport interface {
	ID() peer.ID
	SeedPeers() []peer.ID
	Offer(context.Context, peer.ID, cas.Digest) (bool, error)
}

type Options struct {
	Policy       Policy
	Instance     string
	BytesPerHour int64
	Timeout      time.Duration
}

type observation struct {
	digest   cas.Digest
	metadata workspace.Metadata
	ready    bool
}
type result struct {
	attempt           attempt
	complete, missing bool
}
type outbox interface {
	Save(*state) error
	Close() error
}

type Stats struct {
	UnconfirmedAttempts                            int
	OldestPendingSeconds                           int64
	Expired                                        uint64
	VerifiedPeers                                  int
	Queued, Waiting, Completed, Exhausted, Records int
	QueuedBytes, ChargedBytes                      int64
	ChargedOffers                                  int
	Dropped                                        uint64
	PersistenceFailed                              bool
}

type Publisher struct {
	ctx         context.Context
	cancel      context.CancelFunc
	source      Source
	transport   Transport
	classifier  *workspace.Classifier
	options     Options
	box         outbox
	state       *state
	recentReady readyCache
	events      chan observation
	results     chan result
	done        chan struct{}
	dropped     atomic.Uint64
	mu          sync.RWMutex
	stats       Stats
	err         error
}

// New starts a best-effort background publisher. RPC callbacks only enqueue
// bounded hints. Work is recoverable once the worker saves it; a crash before
// that save can lose a hint without invalidating the successful local write.
func New(ctx context.Context, root string, source Source, transport Transport, options Options) (*Publisher, error) {
	if source == nil || transport == nil || options.Policy == 0 || options.Policy > 15 {
		return nil, errors.New("invalid publication source, transport, or policy")
	}
	if options.BytesPerHour == 0 {
		options.BytesPerHour = 1 << 30
	}
	if options.Timeout == 0 {
		options.Timeout = 5 * time.Minute
	}
	if options.BytesPerHour < 1 || options.BytesPerHour > 1<<40 || options.Timeout <= 0 || options.Timeout > 5*time.Minute {
		return nil, errors.New("invalid publication byte budget or timeout")
	}
	classifier, err := workspace.NewClassifier(transport.ID().String(), options.Instance, 4096, time.Hour)
	if err != nil {
		return nil, err
	}
	box, s, err := openOutbox(root, time.Now())
	if err != nil {
		return nil, err
	}
	for _, j := range s.Jobs {
		for _, id := range j.Completed {
			if id == transport.ID() {
				_ = box.Close()
				return nil, errors.New("publication state counts this node as a remote replica")
			}
		}
	}
	ctx, cancel := context.WithCancel(ctx)
	p := &Publisher{ctx: ctx, cancel: cancel, source: source, transport: transport, classifier: classifier, options: options, box: box, state: s,
		events: make(chan observation, 256), results: make(chan result, 1), done: make(chan struct{})}
	go p.run()
	return p, nil
}

// Observe records client-side upload context. Missing presence observations
// cannot themselves activate a transfer; a later local commit can do so.
func (p *Publisher) Observe(d cas.Digest, metadata workspace.Metadata, present bool) {
	p.enqueue(observation{d, metadata, present})
}
func (p *Publisher) Committed(d cas.Digest, metadata workspace.Metadata) {
	p.enqueue(observation{d, metadata, true})
}

func (p *Publisher) enqueue(event observation) {
	if p.ctx.Err() != nil {
		return
	}
	select {
	case p.events <- event:
	default:
		p.dropped.Add(1)
	}
}

func (p *Publisher) Stats() Stats {
	p.mu.RLock()
	s := p.stats
	p.mu.RUnlock()
	s.Dropped = p.dropped.Load()
	return s
}

func (p *Publisher) Close() error {
	p.cancel()
	<-p.done
	return p.err
}

func (p *Publisher) updateStats(failed bool) {
	s := Stats{Expired: p.state.Expired, Records: len(p.state.Jobs), ChargedBytes: p.state.Bytes, ChargedOffers: p.state.Offers, PersistenceFailed: failed}
	for _, j := range p.state.Jobs {
		s.VerifiedPeers += len(j.Completed)
		s.UnconfirmedAttempts += len(j.Attempts) - len(j.Completed)
		if !j.done(p.state.Clock) || (p.state.running != nil && p.state.running.Digest == j.Digest) {
			s.OldestPendingSeconds = max(s.OldestPendingSeconds, p.state.Clock-j.Created)
		}
		_, eligible := j.partition(p.options.Policy)
		switch {
		case len(j.Completed) == ReplicaTarget:
			s.Completed++
		case j.done(p.state.Clock) && (p.state.running == nil || p.state.running.Digest != j.Digest):
			s.Exhausted++
		case !j.Ready || !eligible:
			s.Waiting++
		default:
			s.Queued++
			s.QueuedBytes += j.Digest.Size
		}
	}
	if p.state.running != nil {
		s.UnconfirmedAttempts-- // An active attempt has no outcome yet.
	}
	p.mu.Lock()
	p.stats = s
	p.mu.Unlock()
}

func (p *Publisher) observe(event observation, now time.Time) bool {
	if event.digest.Validate() != nil {
		return false
	}
	if event.ready {
		p.recentReady.remember(event.digest, p.state.Clock)
	}
	a := p.classifier.Classify(event.metadata, now)
	// Ineligible traffic must not fill the outbox ahead of eligible work.
	// An earlier eligible FindMissing observation still survives a subsequent
	// metadata-free upload, because its existing job is updated here.
	previous := p.state.Jobs[event.digest]
	if uint8(p.options.Policy)&(1<<uint(a.Kind)) == 0 && previous == nil {
		return false
	}
	var before job
	if previous != nil {
		before = *previous
	}
	if !p.state.observe(event.digest, a, event.ready) {
		p.dropped.Add(1)
		return false
	}
	after := p.state.Jobs[event.digest]
	// Joining an older completion is not a new positive observation. It must
	// not override a missing-source probe that was started after that proof.
	if p.recentReady.contains(event.digest, p.state.Clock) {
		after.Ready = true
	}
	return previous == nil || before.Ready != after.Ready || before.Kinds != after.Kinds || before.Partitions != after.Partitions
}

func (p *Publisher) finish(r result) {
	if r.missing && p.state.running != nil && *p.state.running == r.attempt && !p.state.runningRefreshed {
		p.recentReady.forget(r.attempt.Digest)
	}
	p.state.finish(r.attempt, r.complete, r.missing)
}

func (p *Publisher) transfer(a attempt) {
	ctx, cancel := context.WithTimeout(p.ctx, p.options.Timeout)
	defer cancel()
	present, err := p.source.Has(ctx, a.Digest)
	r := result{attempt: a, missing: !present && err == nil}
	if present && err == nil {
		complete, err := p.transport.Offer(ctx, a.Peer, a.Digest)
		r.complete = complete && err == nil
	}
	// A single active attempt and a one-slot result channel let shutdown wait
	// for cancellation without abandoning a goroutine or its storage accesses.
	p.results <- r
}

func (p *Publisher) advance(now time.Time) bool {
	count, window, charged := len(p.state.Jobs), p.state.Window, p.state.Offers
	p.state.advance(now)
	return count != len(p.state.Jobs) || (charged != 0 && window != p.state.Window)
}

func (p *Publisher) run() {
	defer close(p.done)
	defer func() { p.err = errors.Join(p.err, p.box.Close()) }()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	dirty := false
	lastSave := time.Time{}
	failed := false
	save := func() bool {
		if err := p.box.Save(p.state); err != nil {
			failed = true
			p.err = errors.Join(p.err, err)
			slog.Error("federated publication disabled after state write failure", "error", err)
			return false
		}
		dirty = false
		lastSave = time.Now()
		return true
	}
	for {
		var finished bool
		select {
		case <-p.ctx.Done():
			if p.state.running != nil {
				r := <-p.results
				p.finish(r)
				dirty = true
			}
			// No transfer can follow this final save. Queued callbacks remain
			// best-effort hints and are intentionally not turned into new jobs.
			if dirty && !failed {
				save()
			}
			p.updateStats(failed)
			return
		case event := <-p.events:
			if !failed {
				now := time.Now()
				dirty = p.advance(now) || dirty
				dirty = p.observe(event, now) || dirty
			}
		case r := <-p.results:
			p.finish(r)
			dirty = true
			finished = true
		case <-ticker.C:
			if !failed {
				dirty = p.advance(time.Now()) || dirty
			}
		}
		if failed {
			p.updateStats(true)
			continue
		}
		// Amortize observations received in one burst into one durable write.
		for range 64 {
			select {
			case event := <-p.events:
				dirty = p.observe(event, time.Now()) || dirty
			default:
				goto drained
			}
		}
	drained:
		if dirty && (finished || time.Since(lastSave) >= time.Second) {
			if !save() {
				p.updateStats(true)
				continue
			}
		}
		if p.state.running == nil {
			peers := p.transport.SeedPeers()
			if len(peers) > 128 {
				peers = peers[:128]
			}
			if a := p.state.start(p.options.Policy, p.transport.ID(), peers, p.options.BytesPerHour); a != nil {
				dirty = true
				if !save() {
					p.state.running = nil
					p.updateStats(true)
					continue
				}
				go p.transfer(*a)
			}
		}
		p.updateStats(false)
	}
}
