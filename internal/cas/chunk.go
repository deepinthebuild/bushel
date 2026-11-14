package cas

import (
	"bufio"
	"context"
	"crypto/sha256"
	"fmt"
	"io"

	cdc "github.com/buildbarn/go-cdc"
)

var chunker = cdc.NewRepMaxContentDefinedChunker(&cdc.FastContentDefinedChunkerGearTable, MinChunkSize, HorizonSize)

// Chunk verifies a blob while splitting it. emit must consume the borrowed buffer
// before returning. Emitted chunks must remain unpublished until Chunk succeeds.
func Chunk(ctx context.Context, d Digest, r io.Reader, emit func(Digest, []byte) error) (Manifest, error) {
	m := Manifest{Blob: d}
	if err := d.Validate(); err != nil {
		return m, err
	}
	h := sha256.New()
	r = io.TeeReader(io.LimitReader(contextReader{ctx, r}, d.Size+1), h)
	cr := chunker.NewChunkReader(bufio.NewReaderSize(r, chunker.GetMaximumPeekSizeBytes()))
	var size int64
	for {
		b, err := cr.ReadNextChunk()
		if err == io.EOF {
			break
		}
		if err != nil {
			return m, err
		}
		size += int64(len(b))
		if size > d.Size || len(m.Chunks) >= MaxChunks {
			return m, fmt.Errorf("%w: blob size mismatch", ErrCorrupt)
		}
		c := Sum(b)
		if err := emit(c, b); err != nil {
			return m, err
		}
		m.Chunks = append(m.Chunks, c)
	}
	if size != d.Size || [32]byte(h.Sum(nil)) != d.Hash {
		return m, fmt.Errorf("%w: blob digest mismatch", ErrCorrupt)
	}
	if err := ctx.Err(); err != nil {
		return m, err
	}
	return m, m.Validate()
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.r.Read(p)
}
