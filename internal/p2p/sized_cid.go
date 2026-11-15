package p2p

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"
	"slices"

	"github.com/deepinthebuild/bushel/internal/cas"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
	mhcore "github.com/multiformats/go-multihash/core"
)

const (
	// This private-use code describes SHA-256 followed by the byte count.
	// Routing keys continue to use ordinary SHA2-256 multihashes.
	sizedChunkMultihashCode uint64 = 0x300001
	sizedChunkDigestSize           = sha256.Size + 8
	sizedChunkCIDSize              = 47
)

func init() {
	// Ordinary Register permits digest truncation. Every byte of this tuple
	// is required, so only the default and exact-length forms are supported.
	mhcore.RegisterVariableSize(sizedChunkMultihashCode, func(size int) (hash.Hash, bool) {
		if size != -1 && size != sizedChunkDigestSize {
			return nil, false
		}
		return &countedSHA256{digest: sha256.New()}, true
	})
}

// sizedChunkCID encodes an existing REAPI digest without reading its content.
// The caller must still verify downloaded bytes against this digest.
func sizedChunkCID(d cas.Digest) (cid.Cid, error) {
	if d.Size < 1 || d.Size > cas.MaxChunkSize {
		return cid.Undef, fmt.Errorf("%w: sized chunk length out of range", cas.ErrInvalid)
	}
	var payload [sizedChunkDigestSize]byte
	copy(payload[:sha256.Size], d.Hash[:])
	binary.BigEndian.PutUint64(payload[sha256.Size:], uint64(d.Size))
	mh, err := multihash.Encode(payload[:], sizedChunkMultihashCode)
	if err != nil {
		return cid.Undef, err
	}
	return cid.NewCidV1(cid.Raw, mh), nil
}

func sizedChunkDigest(c cid.Cid) (cas.Digest, error) {
	// Bound copies made by Cid.Hash and multihash.Decode before parsing.
	if c.ByteLen() != sizedChunkCIDSize || c.Version() != 1 || c.Type() != cid.Raw {
		return cas.Digest{}, fmt.Errorf("%w: invalid sized chunk CID", cas.ErrInvalid)
	}
	mh, err := multihash.Decode(c.Hash())
	if err != nil || mh.Code != sizedChunkMultihashCode || len(mh.Digest) != sizedChunkDigestSize {
		return cas.Digest{}, fmt.Errorf("%w: invalid sized chunk multihash", cas.ErrInvalid)
	}
	size := binary.BigEndian.Uint64(mh.Digest[sha256.Size:])
	if size < 1 || size > cas.MaxChunkSize {
		return cas.Digest{}, fmt.Errorf("%w: sized chunk length out of range", cas.ErrInvalid)
	}
	d := cas.Digest{Size: int64(size)}
	copy(d.Hash[:], mh.Digest[:sha256.Size])
	return d, nil
}

// countedSHA256 lets the unchanged Bitswap payload decoder reconstruct the
// entire tuple from a CID prefix and bytes. Chunk-size admission belongs to
// sizedChunkCID/sizedChunkDigest; hash.Hash itself accepts arbitrary writes.
type countedSHA256 struct {
	digest hash.Hash
	size   uint64
}

var _ hash.Hash = (*countedSHA256)(nil)

func (h *countedSHA256) Write(p []byte) (int, error) {
	n, err := h.digest.Write(p)
	h.size += uint64(n)
	return n, err
}

func (h *countedSHA256) Sum(b []byte) []byte {
	return binary.BigEndian.AppendUint64(h.digest.Sum(slices.Grow(b, sizedChunkDigestSize)), h.size)
}

func (h *countedSHA256) Reset() {
	h.digest.Reset()
	h.size = 0
}

func (*countedSHA256) Size() int      { return sizedChunkDigestSize }
func (*countedSHA256) BlockSize() int { return sha256.BlockSize }
