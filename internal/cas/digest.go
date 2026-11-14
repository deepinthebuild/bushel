// Package cas defines Bushel's SHA-256 content identities and v1 chunk format.
package cas

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

const MaxBlobSize int64 = 8 << 30

var (
	ErrInvalid  = errors.New("invalid CAS data")
	ErrNotFound = errors.New("blob not found")
	ErrCorrupt  = errors.New("CAS integrity check failed")
)

type Digest struct {
	Hash [sha256.Size]byte
	Size int64
}

func Sum(data []byte) Digest { return Digest{Hash: sha256.Sum256(data), Size: int64(len(data))} }

func ParseDigest(hash string, size int64) (Digest, error) {
	var d Digest
	if len(hash) != 64 || hash != strings.ToLower(hash) {
		return d, fmt.Errorf("%w: expected lowercase SHA-256", ErrInvalid)
	}
	b, err := hex.DecodeString(hash)
	if err != nil {
		return d, fmt.Errorf("%w: malformed SHA-256", ErrInvalid)
	}
	copy(d.Hash[:], b)
	d.Size = size
	return d, d.Validate()
}

func (d Digest) Validate() error {
	if d.Size < 0 || d.Size > MaxBlobSize {
		return fmt.Errorf("%w: blob size out of range", ErrInvalid)
	}
	if d.Size == 0 && d.Hash != sha256.Sum256(nil) {
		return fmt.Errorf("%w: invalid empty digest", ErrInvalid)
	}
	return nil
}

func (d Digest) Hex() string { return hex.EncodeToString(d.Hash[:]) }

func (d Digest) Key() string { return fmt.Sprintf("%s-%d", d.Hex(), d.Size) }
