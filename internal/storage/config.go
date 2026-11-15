// Package storage constructs Buildbarn backends using their upstream configuration.
package storage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"time"

	repb "github.com/bazelbuild/remote-apis/build/bazel/remote/execution/v2"
	"github.com/buildbarn/bb-storage/pkg/blobstore/configuration"
	"github.com/buildbarn/bb-storage/pkg/digest"
	bbgrpc "github.com/buildbarn/bb-storage/pkg/grpc"
	"github.com/buildbarn/bb-storage/pkg/program"
	storagepb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blobstore"
	blockpb "github.com/buildbarn/bb-storage/pkg/proto/configuration/blockdevice"
	mappb "github.com/buildbarn/bb-storage/pkg/proto/configuration/lossymap"
	"github.com/buildbarn/bb-storage/pkg/util"
	"github.com/deepinthebuild/bushel/internal/cas"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/durationpb"
)

// Read accepts the exact upstream ContentAddressableStorageConfiguration in
// JSON or Jsonnet, including imports and Buildbarn's environment extVars.
func Read(path string) (*storagepb.ContentAddressableStorageConfiguration, error) {
	c := &storagepb.ContentAddressableStorageConfiguration{}
	if err := util.UnmarshalConfigurationFromFile(path, c); err != nil {
		return nil, err
	}
	return c, nil
}

// Default provides a persistent regular-file backend on all supported platforms.
// Its directories belong to the daemon's locked private state directory.
func Default(root string, chunkBytes int64) (*storagepb.ContentAddressableStorageConfiguration, error) {
	if chunkBytes < 16<<20 {
		return nil, fmt.Errorf("local chunk storage must be at least 16 MiB")
	}
	if err := ensureDirectory(root); err != nil {
		return nil, err
	}
	for _, name := range []string{"cs", "cms"} {
		dir := filepath.Join(root, name)
		if err := ensureDirectory(dir); err != nil {
			return nil, err
		}
		for _, file := range []string{"keys", "blocks", "state"} {
			info, err := os.Lstat(filepath.Join(dir, file))
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			if err != nil {
				return nil, err
			}
			if !info.Mode().IsRegular() {
				return nil, fmt.Errorf("default backend %s must be a regular file, not a link", file)
			}
		}
	}
	file := func(path string, size int64) *blockpb.Configuration {
		return &blockpb.Configuration{Source: &blockpb.Configuration_File{File: &blockpb.FileConfiguration{Path: path, SizeBytes: size}}}
	}
	local := func(name string, size, mapBytes int64, params *repb.RepMaxCdcParams) *storagepb.BlobAccessConfiguration {
		dir := filepath.Join(root, name)
		newBlocks := int32(1)
		if params != nil {
			newBlocks = 2
		}
		return &storagepb.BlobAccessConfiguration{Backend: &storagepb.BlobAccessConfiguration_Local{Local: &storagepb.LocalBlobAccessConfiguration{
			KeyLocationMap: &mappb.HashMapConfiguration{
				Backend:            &mappb.HashMapConfiguration_OnBlockDevice{OnBlockDevice: file(filepath.Join(dir, "keys"), mapBytes)},
				MaximumGetAttempts: 16, MaximumPutAttempts: 64,
			},
			OldBlocks: 2, CurrentBlocks: 4, NewBlocks: newBlocks,
			BlocksBackend: &storagepb.LocalBlobAccessConfiguration_BlocksOnBlockDevice_{BlocksOnBlockDevice: &storagepb.LocalBlobAccessConfiguration_BlocksOnBlockDevice{
				Source: file(filepath.Join(dir, "blocks"), size), SpareBlocks: 2,
			}},
			Persistent:         &storagepb.LocalBlobAccessConfiguration_Persistent{StateDirectoryPath: dir, MinimumEpochInterval: durationpb.New(5 * time.Minute)},
			ChunkingParameters: params,
		}}}
	}
	return &storagepb.ContentAddressableStorageConfiguration{
		ChunkStorage:        local("cs", chunkBytes, 16<<20, &repb.RepMaxCdcParams{MinChunkSizeBytes: cas.MinChunkSize, HorizonSizeBytes: cas.HorizonSize}),
		ChunkMappingStorage: local("cms", 32<<20, 4<<20, nil),
	}, nil
}

// The daemon holds its directory lock while checking and opening these paths.
// Deliberately avoid MkdirAll: a link inside owned state must not redirect writes.
func ensureDirectory(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return os.Mkdir(path, 0700)
	}
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("default backend directory must not be a link")
	}
	return nil
}

// New creates process-lifetime storage. Construct it once per daemon process.
// dependencies must outlive every Bushel operation; RunLocal waits for Buildbarn
// flush routines on shutdown. Upstream mappings are released by process exit.
func New(ctx context.Context, dependencies program.Group, c *storagepb.ContentAddressableStorageConfiguration, instance string) (cas.Backend, error) {
	name, err := digest.NewInstanceName(instance)
	if err != nil {
		return cas.Backend{}, err
	}
	const maximumMessageSize = 4 << 20
	dialer := func(_ context.Context, target string, opts ...grpc.DialOption) (grpc.ClientConnInterface, error) {
		opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(maximumMessageSize), grpc.MaxCallSendMsgSize(maximumMessageSize)))
		conn, err := grpc.NewClient(target, opts...)
		if err != nil {
			return nil, err
		}
		dependencies.Go(func(ctx context.Context, _, _ program.Group) error { <-ctx.Done(); return conn.Close() })
		return conn, nil
	}
	clients := bbgrpc.NewDeduplicatingClientFactory(bbgrpc.NewBaseClientFactory(dialer, nil, nil, []grpc.StreamClientInterceptor{boundMappingStream}))
	info, err := configuration.NewCASFromConfiguration(dependencies, c, clients, maximumMessageSize)
	if err != nil {
		return cas.Backend{}, err
	}
	caps, err := info.ChunkStorage.GetCapabilities(ctx, name)
	if err != nil {
		return cas.Backend{}, err
	}
	if err := validateCapabilities(caps); err != nil {
		return cas.Backend{}, err
	}
	return cas.Backend{Chunks: info.ChunkStorage, Mappings: info.ChunkMappingStorage, InstanceName: name,
		ChunkReader: info.ChunkBytesReader, MappingReader: info.ChunkMappingFetcher}, nil
}

func validateCapabilities(caps *repb.ServerCapabilities) error {
	cache := caps.GetCacheCapabilities()
	if !slices.Contains(cache.GetDigestFunctions(), repb.DigestFunction_SHA256) {
		return fmt.Errorf("storage must support SHA-256")
	}
	if p := cache.GetRepMaxCdcParams(); p != nil && (p.MinChunkSizeBytes != cas.MinChunkSize || p.HorizonSizeBytes != cas.HorizonSize) {
		return fmt.Errorf("storage RepMaxCDC parameters must be min=%d, horizon=%d", cas.MinChunkSize, cas.HorizonSize)
	}
	return nil
}
