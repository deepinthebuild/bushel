package p2p

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/deepinthebuild/bushel/internal/cas"
	ipld "github.com/ipfs/go-ipld-format"
)

type chunkGetterFunc func(context.Context, cas.Digest) ([]byte, error)

func (f chunkGetterFunc) Block(ctx context.Context, d cas.Digest) ([]byte, error) {
	return f(ctx, d)
}

func TestReadStoreChecksCompleteSizedTuple(t *testing.T) {
	data := []byte("good chunk")
	d := cas.Sum(data)
	wrongSize := d
	wrongSize.Size++
	for _, tc := range []struct {
		name  string
		want  cas.Digest
		data  []byte
		valid bool
	}{
		{"valid", d, data, true},
		{"wrong size", wrongSize, data, false},
		{"wrong hash", d, []byte("bad! chunk"), false},
		{"empty bytes", d, nil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			key, err := sizedChunkCID(tc.want)
			if err != nil {
				t.Fatal(err)
			}
			ctx := t.Context()
			reads := 0
			s := readStore{store: chunkGetterFunc(func(gotCtx context.Context, got cas.Digest) ([]byte, error) {
				if gotCtx.Err() != ctx.Err() || got != tc.want {
					t.Fatal("backend did not receive the exact CID tuple and context")
				}
				reads++
				return bytes.Clone(tc.data), nil
			})}
			block, getErr := s.Get(ctx, key)
			has, hasErr := s.Has(ctx, key)
			size, sizeErr := s.GetSize(ctx, key)
			if tc.valid {
				if getErr != nil || !block.Cid().Equals(key) || !bytes.Equal(block.RawData(), data) ||
					hasErr != nil || !has || sizeErr != nil || size != len(data) {
					t.Fatalf("valid chunk rejected: get=%v has=%v/%v size=%d/%v", getErr, has, hasErr, size, sizeErr)
				}
			} else if block != nil || !ipld.IsNotFound(getErr) || has || hasErr != nil || size != 0 || !ipld.IsNotFound(sizeErr) {
				t.Fatalf("unverified tuple exposed: get=%v has=%v/%v size=%d/%v", getErr, has, hasErr, size, sizeErr)
			}
			if reads != 3 {
				t.Fatal("Get, Has, and GetSize must each verify backend bytes")
			}
		})
	}
}

func TestReadStoreBoundsStalledBackend(t *testing.T) {
	data := []byte("healthy after timeout")
	d := cas.Sum(data)
	key, err := sizedChunkCID(d)
	if err != nil {
		t.Fatal(err)
	}
	stall := true
	s := readStore{timeout: 10 * time.Millisecond, store: chunkGetterFunc(func(ctx context.Context, _ cas.Digest) ([]byte, error) {
		if stall {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		return data, nil
	})}
	if _, err := s.Get(t.Context(), key); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("stalled backend: %v", err)
	}
	stall = false
	if _, err := s.Get(t.Context(), key); err != nil {
		t.Fatalf("healthy read after timeout: %v", err)
	}
}

func TestReadStorePreservesBackendErrorsAndCancellation(t *testing.T) {
	d := cas.Sum([]byte("chunk"))
	key, err := sizedChunkCID(d)
	if err != nil {
		t.Fatal(err)
	}
	for _, backendErr := range []error{cas.ErrNotFound, cas.ErrCorrupt, errors.New("backend unavailable")} {
		s := readStore{store: chunkGetterFunc(func(context.Context, cas.Digest) ([]byte, error) { return nil, backendErr })}
		_, err := s.Get(t.Context(), key)
		if backendErr == cas.ErrNotFound || backendErr == cas.ErrCorrupt {
			if !ipld.IsNotFound(err) {
				t.Fatal(err)
			}
		} else if !errors.Is(err, backendErr) {
			t.Fatalf("backend error became a cache miss: %v", err)
		}
	}
	s := readStore{store: chunkGetterFunc(func(context.Context, cas.Digest) ([]byte, error) {
		t.Fatal("rejected request reached backend")
		return nil, nil
	})}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := s.Get(ctx, key); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	// Standard routing CIDs have no size and are never content identities.
	if _, err := s.Get(t.Context(), routingCID(d.Hash)); !ipld.IsNotFound(err) {
		t.Fatalf("accepted a hash-only routing CID: %v", err)
	}
}
