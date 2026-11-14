package cas

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

const (
	MinChunkSize       = 256 << 10
	MaxChunkSize       = 2*MinChunkSize - 1
	HorizonSize        = 1 << 20
	MaxChunks          = 32768
	ManifestHeaderSize = 52
	MaxManifestSize    = ManifestHeaderSize + 36*MaxChunks
	manifestMagic      = "BSHLM001"
)

type Manifest struct {
	Blob   Digest
	Chunks []Digest
}

func (m Manifest) Validate() error {
	if err := m.Blob.Validate(); err != nil {
		return err
	}
	if len(m.Chunks) > MaxChunks {
		return fmt.Errorf("%w: too many chunks", ErrInvalid)
	}
	var total int64
	for _, c := range m.Chunks {
		if c.Size <= 0 || c.Size > MaxChunkSize || (len(m.Chunks) > 1 && c.Size < MinChunkSize) {
			return fmt.Errorf("%w: invalid chunk size", ErrInvalid)
		}
		total += c.Size
	}
	if total != m.Blob.Size {
		return fmt.Errorf("%w: chunk size sum mismatch", ErrInvalid)
	}
	return nil
}

func (m Manifest) MarshalBinary() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	b := make([]byte, ManifestHeaderSize+36*len(m.Chunks))
	copy(b, manifestMagic)
	copy(b[8:40], m.Blob.Hash[:])
	binary.BigEndian.PutUint64(b[40:48], uint64(m.Blob.Size))
	binary.BigEndian.PutUint32(b[48:52], uint32(len(m.Chunks)))
	for i, c := range m.Chunks {
		off := ManifestHeaderSize + 36*i
		copy(b[off:off+32], c.Hash[:])
		binary.BigEndian.PutUint32(b[off+32:off+36], uint32(c.Size))
	}
	return b, nil
}

func ParseManifest(b []byte) (Manifest, error) {
	var m Manifest
	if len(b) < ManifestHeaderSize || len(b) > MaxManifestSize || !bytes.Equal(b[:8], []byte(manifestMagic)) {
		return m, fmt.Errorf("%w: manifest header", ErrInvalid)
	}
	n := int(binary.BigEndian.Uint32(b[48:52]))
	if n > MaxChunks || len(b) != ManifestHeaderSize+36*n {
		return m, fmt.Errorf("%w: manifest length", ErrInvalid)
	}
	copy(m.Blob.Hash[:], b[8:40])
	m.Blob.Size = int64(binary.BigEndian.Uint64(b[40:48]))
	m.Chunks = make([]Digest, n)
	for i := range m.Chunks {
		off := ManifestHeaderSize + 36*i
		copy(m.Chunks[i].Hash[:], b[off:off+32])
		m.Chunks[i].Size = int64(binary.BigEndian.Uint32(b[off+32 : off+36]))
	}
	return m, m.Validate()
}
