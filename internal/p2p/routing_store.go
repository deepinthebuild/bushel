package p2p

import (
	"bytes"
	"container/list"
	"context"
	"errors"
	"strings"
	"sync"

	lru "github.com/hashicorp/golang-lru/simplelru"
	ds "github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

var (
	errRoutingLimit   = errors.New("routing datastore limit exceeded")
	errRoutingClosed  = errors.New("routing datastore or batch closed")
	errRoutingInvalid = errors.New("invalid routing datastore record or query")
)

// Limits count key and value payload bytes; the record counts additionally
// bound map/list metadata. Pending limits apply across all live batches and
// MaxPendingRecords also bounds retained nonempty batch handles. Empty handles
// remain caller-owned. Snapshots each contain at most MaxRecords entries and
// MaxBytes payload bytes.
type routingStoreLimits struct {
	MaxRecords         int
	MaxBytes           int
	MaxKeyBytes        int
	MaxValueBytes      int
	MaxProvidersPerKey int
	MaxKeysPerProvider int
	MaxBatchRecords    int
	MaxPendingRecords  int
	MaxPendingBytes    int
	MaxSnapshots       int
}

// routingStore holds only provider records of the form
// /providers/<encoded-content-key>/<encoded-provider>. It retains the most
// recently written records, evicting within a saturated content-key group or
// provider first, then globally. Reads do not change eviction order.
//
// The datastore owns its byte slices. Batches are atomic, bounded overlays;
// any staging error aborts the batch. Cancellation, Commit, and Close release
// staged copies. Queries return independent snapshots; callers must consume or
// close them. Cancelling the query context also releases its snapshot.
type routingStore struct {
	mu           sync.Mutex
	limits       routingStoreLimits
	closed       bool
	records      map[string]*routingRecord
	groups       map[string]*list.List
	providers    map[string]*list.List
	order        list.List
	bytes        int
	evictions    uint64
	batches      map[*routingBatch]struct{}
	pendingCount int
	pendingBytes int
	snapshots    map[*routingSnapshot]struct{}
}

type routingRecord struct {
	key, group, provider     string
	value                    []byte
	all, byGroup, byProvider *list.Element
}

var _ ds.Batching = (*routingStore)(nil)

func newRoutingStore(limits routingStoreLimits) (*routingStore, error) {
	if limits.MaxRecords <= 0 || limits.MaxBytes <= 0 || limits.MaxKeyBytes <= 0 || limits.MaxValueBytes <= 0 ||
		limits.MaxProvidersPerKey <= 0 || limits.MaxKeysPerProvider <= 0 || limits.MaxBatchRecords <= 0 ||
		limits.MaxPendingRecords < limits.MaxBatchRecords || limits.MaxPendingBytes <= 0 || limits.MaxSnapshots <= 0 ||
		limits.MaxKeyBytes > limits.MaxBytes || limits.MaxValueBytes > limits.MaxBytes-limits.MaxKeyBytes {
		return nil, errRoutingInvalid
	}
	return &routingStore{limits: limits, records: make(map[string]*routingRecord),
		groups: make(map[string]*list.List), providers: make(map[string]*list.List),
		batches: make(map[*routingBatch]struct{}), snapshots: make(map[*routingSnapshot]struct{})}, nil
}

func (s *routingStore) checkLocked(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.closed {
		return errRoutingClosed
	}
	return nil
}

func (s *routingStore) validate(key string, value []byte) error {
	if len(key) > s.limits.MaxKeyBytes || len(value) > s.limits.MaxValueBytes {
		return errRoutingLimit
	}
	const prefix = "/providers/"
	if !strings.HasPrefix(key, prefix) {
		return errRoutingInvalid
	}
	group, provider, ok := strings.Cut(key[len(prefix):], "/")
	if !ok || group == "" || provider == "" || strings.Contains(provider, "/") ||
		group == "." || group == ".." || provider == "." || provider == ".." {
		return errRoutingInvalid
	}
	return nil
}

func (s *routingStore) Put(ctx context.Context, key ds.Key, value []byte) error {
	if err := s.validate(key.String(), value); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLocked(ctx); err != nil {
		return err
	}
	s.putLocked(strings.Clone(key.String()), bytes.Clone(value))
	return nil
}

// putLocked takes ownership of key and value. They have already passed all
// size checks, so eviction always makes room without a fallible allocation
// decision midway through a batch. Each victim is removed from every index.
func (s *routingStore) putLocked(key string, value []byte) {
	if old := s.records[key]; old != nil {
		s.order.MoveToBack(old.all)
		s.groups[old.group].MoveToBack(old.byGroup)
		s.providers[old.provider].MoveToBack(old.byProvider)
		s.bytes -= len(old.value)
		for s.bytes > s.limits.MaxBytes-len(value) {
			s.evictLocked(s.order.Front().Value.(*routingRecord))
		}
		old.value = value
		s.bytes += len(value)
		return
	}
	i := strings.LastIndexByte(key, '/')
	group, provider := key[:i], key[i+1:]
	if entries := s.groups[group]; entries != nil && entries.Len() >= s.limits.MaxProvidersPerKey {
		s.evictLocked(entries.Front().Value.(*routingRecord))
	}
	if entries := s.providers[provider]; entries != nil && entries.Len() >= s.limits.MaxKeysPerProvider {
		s.evictLocked(entries.Front().Value.(*routingRecord))
	}
	size := len(key) + len(value)
	for len(s.records) >= s.limits.MaxRecords || s.bytes > s.limits.MaxBytes-size {
		s.evictLocked(s.order.Front().Value.(*routingRecord))
	}
	r := &routingRecord{key: key, group: group, provider: provider, value: value}
	if s.groups[group] == nil {
		s.groups[group] = new(list.List)
	}
	if s.providers[provider] == nil {
		s.providers[provider] = new(list.List)
	}
	r.all = s.order.PushBack(r)
	r.byGroup = s.groups[group].PushBack(r)
	r.byProvider = s.providers[provider].PushBack(r)
	s.records[key] = r
	s.bytes += size
}

func (s *routingStore) evictLocked(r *routingRecord) {
	s.evictions++
	s.removeLocked(r)
}

func (s *routingStore) removeLocked(r *routingRecord) {
	delete(s.records, r.key)
	s.bytes -= len(r.key) + len(r.value)
	s.order.Remove(r.all)
	group := s.groups[r.group]
	group.Remove(r.byGroup)
	if group.Len() == 0 {
		delete(s.groups, r.group)
	}
	provider := s.providers[r.provider]
	provider.Remove(r.byProvider)
	if provider.Len() == 0 {
		delete(s.providers, r.provider)
	}
}

func (s *routingStore) Get(ctx context.Context, key ds.Key) ([]byte, error) {
	if len(key.String()) > s.limits.MaxKeyBytes {
		return nil, errRoutingLimit
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLocked(ctx); err != nil {
		return nil, err
	}
	if r := s.records[key.String()]; r != nil {
		return bytes.Clone(r.value), nil
	}
	return nil, ds.ErrNotFound
}

func (s *routingStore) Has(ctx context.Context, key ds.Key) (bool, error) {
	if len(key.String()) > s.limits.MaxKeyBytes {
		return false, errRoutingLimit
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLocked(ctx); err != nil {
		return false, err
	}
	return s.records[key.String()] != nil, nil
}

func (s *routingStore) GetSize(ctx context.Context, key ds.Key) (int, error) {
	if len(key.String()) > s.limits.MaxKeyBytes {
		return -1, errRoutingLimit
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLocked(ctx); err != nil {
		return -1, err
	}
	if r := s.records[key.String()]; r != nil {
		return len(r.value), nil
	}
	return -1, ds.ErrNotFound
}

func (s *routingStore) Delete(ctx context.Context, key ds.Key) error {
	if len(key.String()) > s.limits.MaxKeyBytes {
		return errRoutingLimit
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLocked(ctx); err != nil {
		return err
	}
	if r := s.records[key.String()]; r != nil {
		s.removeLocked(r)
	}
	return nil
}

// Sync is a no-op for this intentionally ephemeral provider cache.
func (s *routingStore) Sync(ctx context.Context, _ ds.Key) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.checkLocked(ctx)
}

func (s *routingStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	for batch := range s.batches {
		batch.finishLocked()
	}
	for snapshot := range s.snapshots {
		snapshot.finishLocked()
	}
	clear(s.records)
	clear(s.groups)
	clear(s.providers)
	s.order.Init()
	s.bytes = 0
	return nil
}

type routingChange struct {
	key    string
	value  []byte
	remove bool
}

type routingBatch struct {
	store   *routingStore
	ctx     context.Context
	changes []routingChange
	bytes   int
	closed  bool
	stop    func() bool
}

func (s *routingStore) Batch(ctx context.Context) (ds.Batch, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLocked(ctx); err != nil {
		return nil, err
	}
	// ProviderManager's GC abandons its final empty batch. Retain nothing
	// until an operation is staged, including no cancellation callback.
	return &routingBatch{store: s, ctx: ctx}, nil
}

func (b *routingBatch) Put(ctx context.Context, key ds.Key, value []byte) error {
	return b.stage(ctx, key, value, false)
}

func (b *routingBatch) Delete(ctx context.Context, key ds.Key) error {
	return b.stage(ctx, key, nil, true)
}

func (b *routingBatch) checkLocked(ctx context.Context) error {
	if err := b.store.checkLocked(ctx); err != nil {
		return err
	}
	if err := b.ctx.Err(); err != nil {
		return err
	}
	if b.closed {
		return errRoutingClosed
	}
	return nil
}

func (b *routingBatch) stage(ctx context.Context, key ds.Key, value []byte, remove bool) error {
	s := b.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := b.checkLocked(ctx); err != nil {
		b.finishLocked()
		return err
	}
	if err := s.validate(key.String(), value); err != nil {
		b.finishLocked()
		return err
	}
	size := len(key.String()) + len(value)
	if len(b.changes) >= s.limits.MaxBatchRecords || s.pendingCount >= s.limits.MaxPendingRecords ||
		size > s.limits.MaxPendingBytes-s.pendingBytes {
		b.finishLocked()
		return errRoutingLimit
	}
	if len(b.changes) == 0 {
		s.batches[b] = struct{}{}
		b.stop = context.AfterFunc(b.ctx, func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			b.finishLocked()
		})
	}
	b.changes = append(b.changes, routingChange{key: strings.Clone(key.String()), value: bytes.Clone(value), remove: remove})
	b.bytes += size
	s.pendingCount++
	s.pendingBytes += size
	return nil
}

func (b *routingBatch) Commit(ctx context.Context) error {
	s := b.store
	s.mu.Lock()
	defer s.mu.Unlock()
	defer b.finishLocked()
	if err := b.checkLocked(ctx); err != nil {
		return err
	}
	for _, change := range b.changes {
		if change.remove {
			if r := s.records[change.key]; r != nil {
				s.removeLocked(r)
			}
		} else {
			s.putLocked(change.key, change.value)
		}
	}
	return nil
}

func (b *routingBatch) finishLocked() {
	if b.closed {
		return
	}
	b.closed = true
	if b.stop != nil {
		b.stop()
	}
	b.store.pendingCount -= len(b.changes)
	b.store.pendingBytes -= b.bytes
	delete(b.store.batches, b)
	b.changes = nil
	b.bytes = 0
}

type routingSnapshot struct {
	store   *routingStore
	ctx     context.Context
	entries []dsq.Entry
	closed  bool
	stop    func() bool
}

func (s *routingStore) Query(ctx context.Context, q dsq.Query) (dsq.Results, error) {
	if len(q.Prefix) > s.limits.MaxKeyBytes || q.Offset < 0 || q.Limit < 0 {
		return nil, errRoutingInvalid
	}
	s.mu.Lock()
	if err := s.checkLocked(ctx); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if len(s.snapshots) >= s.limits.MaxSnapshots {
		s.mu.Unlock()
		return nil, errRoutingLimit
	}
	// A prefix is a namespace, not a byte prefix. The common provider lookup
	// addresses its exact group; /providers/A must not scan /providers/AB.
	prefix := ds.NewKey(q.Prefix).String()
	selected := s.groups[prefix]
	if prefix == "/" || prefix == "/providers" {
		selected = &s.order
	}
	var entries []dsq.Entry
	if selected != nil {
		entries = make([]dsq.Entry, 0, selected.Len())
		for e := selected.Front(); e != nil; e = e.Next() {
			r := e.Value.(*routingRecord)
			entry := dsq.Entry{Key: r.key, Size: len(r.value)}
			if !q.KeysOnly || len(q.Filters) != 0 || len(q.Orders) != 0 {
				entry.Value = bytes.Clone(r.value)
			}
			entries = append(entries, entry)
		}
	}
	snapshot := &routingSnapshot{store: s, ctx: ctx}
	s.snapshots[snapshot] = struct{}{}
	s.mu.Unlock()
	// Caller-supplied filters and comparisons run outside the datastore lock.
	kept := entries[:0]
	for _, entry := range entries {
		accept := true
		for _, filter := range q.Filters {
			if !filter.Filter(entry) {
				accept = false
				break
			}
		}
		if accept {
			kept = append(kept, entry)
		}
	}
	clear(entries[len(kept):])
	entries = kept
	if len(q.Orders) != 0 {
		dsq.Sort(q.Orders, entries)
	}
	offset := min(q.Offset, len(entries))
	clear(entries[:offset])
	entries = entries[offset:]
	if q.Limit > 0 && len(entries) > q.Limit {
		clear(entries[q.Limit:])
		entries = entries[:q.Limit]
	}
	if q.KeysOnly {
		for i := range entries {
			entries[i].Value = nil
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.checkLocked(ctx); err != nil {
		snapshot.finishLocked()
		return nil, err
	}
	snapshot.entries = entries
	snapshot.stop = context.AfterFunc(ctx, func() { _ = snapshot.close() })
	return dsq.ResultsFromIterator(q, dsq.Iterator{Next: snapshot.next, Close: snapshot.close}), nil
}

func (snapshot *routingSnapshot) next() (dsq.Result, bool) {
	s := snapshot.store
	s.mu.Lock()
	defer s.mu.Unlock()
	if snapshot.closed {
		return dsq.Result{}, false
	}
	if err := snapshot.ctx.Err(); err != nil {
		snapshot.finishLocked()
		return dsq.Result{Error: err}, true
	}
	if len(snapshot.entries) == 0 {
		snapshot.finishLocked()
		return dsq.Result{}, false
	}
	entry := snapshot.entries[0]
	snapshot.entries[0] = dsq.Entry{}
	snapshot.entries = snapshot.entries[1:]
	return dsq.Result{Entry: entry}, true
}

func (snapshot *routingSnapshot) close() error {
	s := snapshot.store
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot.finishLocked()
	return nil
}

func (snapshot *routingSnapshot) finishLocked() {
	if snapshot.closed {
		return
	}
	snapshot.closed = true
	if snapshot.stop != nil {
		snapshot.stop()
	}
	snapshot.entries = nil
	delete(snapshot.store.snapshots, snapshot)
}

// noProviderCache prevents ProviderManager's LRU values from each growing an
// unbounded provider set. Its separately bounded pending buffer still overlays
// the datastore, so the datastore's per-key cap is not a response-size cap.
type noProviderCache struct{}

var _ lru.LRUCache = noProviderCache{}

func (noProviderCache) Add(any, any) bool              { return false }
func (noProviderCache) Get(any) (any, bool)            { return nil, false }
func (noProviderCache) Contains(any) bool              { return false }
func (noProviderCache) Peek(any) (any, bool)           { return nil, false }
func (noProviderCache) Remove(any) bool                { return false }
func (noProviderCache) RemoveOldest() (any, any, bool) { return nil, nil, false }
func (noProviderCache) GetOldest() (any, any, bool)    { return nil, nil, false }
func (noProviderCache) Keys() []any                    { return nil }
func (noProviderCache) Len() int                       { return 0 }
func (noProviderCache) Purge()                         {}
func (noProviderCache) Resize(int) int                 { return 0 }
