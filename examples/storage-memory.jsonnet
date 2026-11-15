// Exact Buildbarn ContentAddressableStorageConfiguration; contents are volatile.
local backend(chunkStorage) = {
  'local': {
    keyLocationMap: {
      inMemory: { entries: 16384 },
      maximumGetAttempts: 16,
      maximumPutAttempts: 64,
    },
    oldBlocks: 2,
    currentBlocks: 4,
    newBlocks: 1,
    blocksInMemory: { blockSizeBytes: 4 * 1024 * 1024 },
  } + if chunkStorage then {
    chunkingParameters: {
      minChunkSizeBytes: 262144,
      horizonSizeBytes: 1048576,
    },
  } else {},
};
{
  chunkStorage: backend(true),
  chunkMappingStorage: backend(false),
}
