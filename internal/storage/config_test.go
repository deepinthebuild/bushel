package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/program"
	storagepb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	digestpb "github.com/buildbarn/bb-storage/pkg/proto/configuration/digest"
	evictionpb "github.com/buildbarn/bb-storage/pkg/proto/configuration/eviction"
	mappb "github.com/buildbarn/bb-storage/pkg/proto/configuration/lossymap"
	"github.com/deepinthebuild/bushel/internal/cas"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestNativePersistentRestart(t *testing.T) {
	root := t.TempDir()
	// Each process owns its mappings. Waiting for exit before opening again
	// also ensures no mapping survives into TempDir's cleanup on Windows.
	runStorageProcess(t, root, "write")
	runStorageProcess(t, root, "read")
	// Empty in-memory primaries must fall through to the persistent CS/CMS.
	runStorageProcess(t, root, "fallback")
}

func TestNativeInMemoryConfiguration(t *testing.T) {
	runStorageProcess(t, t.TempDir(), "memory")
}

func runStorageProcess(t *testing.T, root, mode string) {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestStorageProcess$", "-test.timeout=45s")
	cmd.Env = append(os.Environ(), "BUSHEL_STORAGE_TEST_MODE="+mode, "BUSHEL_STORAGE_TEST_ROOT="+root)
	cmd.WaitDelay = 5 * time.Second
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("storage %s process: %v\n%s", mode, err, output)
	}
}

// TestStorageProcess runs only when selected by runStorageProcess. The real
// Buildbarn factory retains mappings and a release worker until process exit.
func TestStorageProcess(t *testing.T) {
	mode := os.Getenv("BUSHEL_STORAGE_TEST_MODE")
	if mode == "" {
		t.Skip("subprocess helper")
	}
	if mode != "write" && mode != "read" && mode != "fallback" && mode != "memory" {
		t.Fatalf("unknown storage test mode %q", mode)
	}
	root := os.Getenv("BUSHEL_STORAGE_TEST_ROOT")
	if root == "" {
		t.Fatal("missing test state directory")
	}
	var cfg *storagepb.ContentAddressableStorageConfiguration
	if mode == "memory" {
		cfg = memoryConfiguration()
	} else {
		var err error
		cfg, err = Default(filepath.Join(root, "backend"), 32<<20)
		if err != nil {
			t.Fatal(err)
		}
		if mode == "fallback" {
			memory := memoryConfiguration()
			memory.ChunkStorage.GetReadFallback().Secondary = cfg.ChunkStorage
			memory.ChunkMappingStorage = &storagepb.BlobAccessConfiguration{Backend: &storagepb.BlobAccessConfiguration_ReadFallback{
				ReadFallback: &storagepb.ReadFallbackBlobAccessConfiguration{Primary: memory.ChunkMappingStorage, Secondary: cfg.ChunkMappingStorage, Replicator: noReplication()},
			}}
			cfg = memory
		}
	}
	err := program.RunLocal(t.Context(), func(ctx context.Context, _, dependencies program.Group) (retErr error) {
		backend, err := New(ctx, dependencies, cfg, "")
		if err != nil {
			return err
		}
		store, err := cas.NewStore(filepath.Join(root, "state"), 32<<20, backend)
		if err != nil {
			return err
		}
		defer func() { retErr = errors.Join(retErr, store.Close()) }()
		data := storageFixture()
		d := cas.Sum(data)
		if mode == "write" || mode == "memory" {
			if err := store.Put(ctx, d, bytes.NewReader(data)); err != nil {
				return fmt.Errorf("put persistent fixture: %w", err)
			}
		}
		manifest, err := store.Manifest(ctx, d)
		if err != nil || len(manifest.Chunks) < 2 {
			return fmt.Errorf("multi-chunk manifest: chunks=%d, error=%v", len(manifest.Chunks), err)
		}
		present, err := store.Committed(ctx, d)
		if err != nil || !present {
			return fmt.Errorf("committed fixture: present=%t, error=%v", present, err)
		}
		r, err := store.Open(ctx, d)
		if err != nil {
			return fmt.Errorf("open persistent fixture: %w", err)
		}
		got, readErr := io.ReadAll(r)
		if err := errors.Join(readErr, r.Close()); err != nil {
			return err
		}
		if !bytes.Equal(got, data) {
			return errors.New("persistent fixture changed")
		}
		var committed []cas.Digest
		if err := store.WalkCommitted(ctx, func(d cas.Digest) error {
			committed = append(committed, d)
			return nil
		}); err != nil {
			return err
		}
		if len(committed) != 1 || committed[0] != d {
			return errors.New("durable announcement catalog lost its fixture")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func storageFixture() []byte {
	data := make([]byte, 2<<20+117)
	var state uint32 = 1
	for i := range data {
		state ^= state << 13
		state ^= state >> 17
		state ^= state << 5
		data[i] = byte(state)
	}
	return data
}

func memoryConfiguration() *storagepb.ContentAddressableStorageConfiguration {
	local := func(chunking bool) *storagepb.BlobAccessConfiguration {
		c := &storagepb.LocalBlobAccessConfiguration{
			KeyLocationMap: &mappb.HashMapConfiguration{
				Backend:            &mappb.HashMapConfiguration_InMemory_{InMemory: &mappb.HashMapConfiguration_InMemory{Entries: 4096}},
				MaximumGetAttempts: 8, MaximumPutAttempts: 32,
			},
			OldBlocks: 2, CurrentBlocks: 4, NewBlocks: 2,
			BlocksBackend: &storagepb.LocalBlobAccessConfiguration_BlocksInMemory_{
				BlocksInMemory: &storagepb.LocalBlobAccessConfiguration_BlocksInMemory{BlockSizeBytes: 1 << 20},
			},
		}
		if chunking {
			c.ChunkingParameters = &repb.RepMaxCdcParams{MinChunkSizeBytes: cas.MinChunkSize, HorizonSizeBytes: cas.HorizonSize}
		} else {
			c.NewBlocks = 1
		}
		return &storagepb.BlobAccessConfiguration{Backend: &storagepb.BlobAccessConfiguration_Local{Local: c}}
	}
	cache := func(size int64) *digestpb.ExistenceCacheConfiguration {
		return &digestpb.ExistenceCacheConfiguration{CacheSize: size, CacheDuration: durationpb.New(2 * time.Second),
			CacheReplacementPolicy: evictionpb.CacheReplacementPolicy_LEAST_RECENTLY_USED}
	}
	return &storagepb.ContentAddressableStorageConfiguration{
		ChunkStorage: &storagepb.BlobAccessConfiguration{Backend: &storagepb.BlobAccessConfiguration_ReadFallback{
			ReadFallback: &storagepb.ReadFallbackBlobAccessConfiguration{Primary: local(true), Secondary: local(true), Replicator: noReplication()},
		}},
		ChunkMappingStorage: local(false), ChunkMappingCache: cache(32), CdcParameterCache: cache(8), ChunkCache: cache(16),
	}
}

func noReplication() *storagepb.BlobReplicatorConfiguration {
	return &storagepb.BlobReplicatorConfiguration{Mode: &storagepb.BlobReplicatorConfiguration_Noop{Noop: &emptypb.Empty{}}}
}

func TestReadUpstreamJSONAndJsonnetConfiguration(t *testing.T) {
	root := t.TempDir()
	t.Setenv("BUSHEL_STORAGE_TEST_CACHE_DURATION", "2s")
	files := map[string]string{
		"local.libsonnet": `{
  keyLocationMap: { inMemory: { entries: 4096 }, maximumGetAttempts: 8, maximumPutAttempts: 32 },
  oldBlocks: 2, currentBlocks: 4, newBlocks: 2,
  blocksInMemory: { blockSizeBytes: 1048576 },
}`,
		"storage.jsonnet": `local backend = import 'local.libsonnet';
local chunks = { 'local': backend + { chunkingParameters: { minChunkSizeBytes: 262144, horizonSizeBytes: 1048576 } } };
local cache(size) = { cacheSize: size, cacheDuration: std.extVar('BUSHEL_STORAGE_TEST_CACHE_DURATION'), cacheReplacementPolicy: 'LEAST_RECENTLY_USED' };
{
  chunkStorage: { readFallback: { primary: chunks, secondary: chunks, replicator: { noop: {} } } },
  chunkMappingStorage: { 'local': backend + { newBlocks: 1 } },
  chunkMappingCache: cache(32), cdcParameterCache: cache(8), chunkCache: cache(16),
}`,
	}
	for name, contents := range files {
		if err := os.WriteFile(filepath.Join(root, name), []byte(contents), 0600); err != nil {
			t.Fatal(err)
		}
	}
	want := memoryConfiguration()
	got, err := Read(filepath.Join(root, "storage.jsonnet"))
	if err != nil {
		t.Fatal(err)
	}
	if !proto.Equal(got, want) {
		t.Fatal("Jsonnet import or an upstream field was lost")
	}
	encoded, err := protojson.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	jsonPath := filepath.Join(root, "storage.json")
	if err := os.WriteFile(jsonPath, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	got, err = Read(jsonPath)
	if err != nil || !proto.Equal(got, want) {
		t.Fatalf("upstream JSON roundtrip: %v", err)
	}
}

func TestValidateCapabilitiesPreservesDigestAndChunkingContract(t *testing.T) {
	for _, tc := range []struct {
		name       string
		algorithms []repb.DigestFunction_Value
		parameters *repb.RepMaxCdcParams
		valid      bool
	}{
		{"no digests", nil, nil, false},
		{"other digest only", []repb.DigestFunction_Value{repb.DigestFunction_BLAKE3}, nil, false},
		{"SHA256 opaque chunks", []repb.DigestFunction_Value{repb.DigestFunction_SHA256}, nil, true},
		{"SHA256 and other digests", []repb.DigestFunction_Value{repb.DigestFunction_BLAKE3, repb.DigestFunction_SHA256}, nil, true},
		{"frozen CDC", []repb.DigestFunction_Value{repb.DigestFunction_SHA256}, &repb.RepMaxCdcParams{MinChunkSizeBytes: cas.MinChunkSize, HorizonSizeBytes: cas.HorizonSize}, true},
		{"different minimum", []repb.DigestFunction_Value{repb.DigestFunction_SHA256}, &repb.RepMaxCdcParams{MinChunkSizeBytes: cas.MinChunkSize + 1, HorizonSizeBytes: cas.HorizonSize}, false},
		{"different horizon", []repb.DigestFunction_Value{repb.DigestFunction_SHA256}, &repb.RepMaxCdcParams{MinChunkSizeBytes: cas.MinChunkSize, HorizonSizeBytes: cas.HorizonSize - 1}, false},
		{"zero CDC", []repb.DigestFunction_Value{repb.DigestFunction_SHA256}, &repb.RepMaxCdcParams{}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caps := &repb.ServerCapabilities{CacheCapabilities: &repb.CacheCapabilities{DigestFunctions: tc.algorithms, RepMaxCdcParams: tc.parameters}}
			if err := validateCapabilities(caps); (err == nil) != tc.valid {
				t.Fatalf("capabilities accepted=%t, want=%t: %v", err == nil, tc.valid, err)
			}
		})
	}
	if err := validateCapabilities(nil); err == nil {
		t.Fatal("accepted absent capabilities")
	}
}
