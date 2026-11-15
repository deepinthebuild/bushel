package p2p

import (
	"context"
	"errors"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p/core/peer"
)

type chunkGetter interface {
	Block(context.Context, cas.Digest) ([]byte, error)
}

// readStore exposes verified chunks. Bitswap never persists
// unsolicited or partially downloaded data; Store.Import owns publication.
type readStore struct {
	store   chunkGetter
	timeout time.Duration
}

func (s readStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	d, err := sizedChunkDigest(c)
	if err != nil {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	timeout := s.timeout
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	b, err := s.store.Block(ctx, d)
	if errors.Is(err, cas.ErrNotFound) || errors.Is(err, cas.ErrCorrupt) {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	if err != nil {
		return nil, err
	}
	// NewBlockWithCid normally trusts its caller. Check the complete tuple
	// here so Get, Has, and GetSize cannot expose bytes under a wrong identity.
	if cas.Sum(b) != d {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	return blocks.NewBlockWithCid(b, c)
}

func (s readStore) Has(ctx context.Context, c cid.Cid) (bool, error) {
	_, err := s.Get(ctx, c)
	if ipld.IsNotFound(err) {
		return false, nil
	}
	return err == nil, err
}

func (s readStore) GetSize(ctx context.Context, c cid.Cid) (int, error) {
	b, err := s.Get(ctx, c)
	if err != nil {
		return 0, err
	}
	return len(b.RawData()), nil
}

var errReadOnly = errors.New("Bitswap block store is read-only")

func (readStore) Put(context.Context, blocks.Block) error       { return errReadOnly }
func (readStore) PutMany(context.Context, []blocks.Block) error { return errReadOnly }
func (readStore) DeleteBlock(context.Context, cid.Cid) error    { return errReadOnly }
func (readStore) AllKeysChan(context.Context) (<-chan cid.Cid, error) {
	return nil, errReadOnly
}

// noProviders deliberately has no DHT reference. Bitswap cannot perform a
// provider lookup for a blob or chunk on either the public or private network.
type noProviders struct{}

func (noProviders) FindProvidersAsync(context.Context, cid.Cid, int) <-chan peer.AddrInfo {
	ch := make(chan peer.AddrInfo)
	close(ch)
	return ch
}
