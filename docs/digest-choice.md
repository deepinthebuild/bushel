# Digest choice: SHA-256

Bushel uses SHA-256 for blobs and chunks. Chunk CIDs carry that hash plus size
using the [Bushel multihash](protocol-v1.md); routing CIDs use standard SHA2-256.
There is no runtime digest-algorithm option.

The original comparison used Go's `crypto/sha256` and `github.com/zeebo/blake3`
v0.2.4 with reused streaming hashers, single-threaded writes, and three 300 ms
samples per size. Median throughput (decimal MB/s):

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
decision. We accept the measured throughput cost of SHA-256 to use SHA-256
REAPI blob identities and one standard-library hash implementation throughout.
Bitswap does not require SHA-256: CIDv1 supports other multihashes, including
BLAKE3. Public IPFS bootstrap discovery does not constrain the content hash.
Supporting alternate blob identities would require rehashing the original bytes
and maintaining verified mappings; hashes cannot be translated directly.
Bushel uses only SHA-256.

IPFS libraries may still include BLAKE3 transitively; Bushel's identities and
verification use only SHA-256.
References: [REAPI digest functions](https://github.com/bazelbuild/remote-apis/blob/main/build/bazel/remote/execution/v2/remote_execution.proto),
[BLAKE3 Go implementation](https://github.com/zeebo/blake3).
