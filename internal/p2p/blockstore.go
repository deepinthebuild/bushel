package p2p

import (
	"context"
	"errors"

	"github.com/deepinthebuild/bushel/internal/cas"
	blocks "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multihash"
)

func chunkCID(hash [32]byte) cid.Cid {
	mh, _ := multihash.Encode(hash[:], multihash.SHA2_256)
	return cid.NewCidV1(cid.Raw, mh)
}

func chunkHash(c cid.Cid) ([32]byte, bool) {
	var hash [32]byte
	if !c.Defined() || c.Version() != 1 || c.Type() != cid.Raw {
		return hash, false
	}
	mh, err := multihash.Decode(c.Hash())
	if err != nil || mh.Code != multihash.SHA2_256 || len(mh.Digest) != len(hash) {
		return hash, false
	}
	copy(hash[:], mh.Digest)
	return hash, true
}

// readStore exposes only committed, verified chunks. Bitswap never persists
// unsolicited or partially downloaded data; Store.Import owns publication.
type readStore struct{ store *cas.Store }

func (s readStore) Get(ctx context.Context, c cid.Cid) (blocks.Block, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	hash, ok := chunkHash(c)
	if !ok {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	b, err := s.store.Block(hash)
	if errors.Is(err, cas.ErrNotFound) || errors.Is(err, cas.ErrCorrupt) {
		return nil, ipld.ErrNotFound{Cid: c}
	}
	if err != nil {
		return nil, err
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
