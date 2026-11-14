# Digest choice: SHA-256

Bushel v1 uses SHA-256 for blobs and chunks, and SHA2-256 raw CIDv1 blocks.
The choice is fixed for v1. There is no runtime digest-algorithm option.

Compared Go's `crypto/sha256` with `github.com/zeebo/blake3` v0.2.4 using reused
streaming hashers, single-threaded writes, three 300 ms samples per size. Median
throughput (decimal MB/s):

| Input | SHA-256 | BLAKE3 |
| --- | ---: | ---: |
| 256 KiB | 5020 | 6506 |
| 384 KiB | 5026 | 6550 |
| 512 KiB | 5037 | 6532 |

Both measured zero per-iteration allocations. These local measurements are not
a ranking across hardware or Go versions; no arm64 performance claim is made.
An 8 MiB pipeline comparison includes the full blob hash, RepMaxCDC, and every
chunk hash, excluding storage and manifest assembly equally. Median throughput
was 1214 MB/s for SHA-256 and 1398 MB/s for BLAKE3 (about 15% faster).
BLAKE3 wins on typical chunks by about 30%; tiny-blob results do not drive this
decision. We accept the measured throughput cost of SHA-256 for one identity
scheme compatible with the
usual Bazel and IPFS path, standard-library acceleration, and no direct Bushel
BLAKE3 dependency. Supporting a second digest would multiply identities and
reduce deduplication unless every participant agreed on it.

Reproduce from the checkout root:

```sh
go -C benchmarks/digests test -run '^$' -bench . -benchmem -benchtime=300ms -count=3
go test ./internal/cas -run '^$' -bench BenchmarkChunk -benchmem
```

The comparison lives in a separate module. IPFS libraries may still include
BLAKE3 transitively; Bushel's identities and verification use only SHA-256.
References: [REAPI digest functions](https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto),
[BLAKE3 Go implementation](https://github.com/zeebo/blake3).
