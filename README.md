# Bushel

A peer-to-peer Bazel content-addressable store, written in Go. Bushel bridges
REAPI gRPC to Bitswap, with RepMaxCDC chunks and SHA-256 verification.

Requires Go 1.26.5 or newer. Build and run from the checkout root:

```sh
go build -trimpath -o bin/ ./cmd/bushel
go run ./cmd/bushel
```

The daemon serves plaintext gRPC on `127.0.0.1:8980` and peer traffic on TCP
8981. It prints its peer addresses at startup. State and its generated peer key
live in the ignored `data` directory. Use an empty directory for a new store;
Bushel refuses to initialize over unrelated files. Ctrl+C and SIGTERM shut it down.

Public discovery is enabled by default. A separate IPFS DHT client advertises
only the fixed Bushel rendezvous key, renewing every 12 hours. Blob manifests
and chunk want-lists travel only over Bushel protocols on the content host.
Share only artifacts suitable for other peers to read; digest verification
provides integrity, while peers holding content can redistribute it.

For a local two-node network, start the first node:

```sh
go run ./cmd/bushel -discovery=false -data-dir=data/node-a -p2p-listen=/ip4/127.0.0.1/tcp/8981
```

Use its printed peer ID when starting the second node:

```sh
go run ./cmd/bushel -discovery=false -data-dir=data/node-b -grpc-listen=127.0.0.1:8982 -p2p-listen=/ip4/127.0.0.1/tcp/8983 -peer=/ip4/127.0.0.1/tcp/8981/p2p/PEER_ID
```

`-peer`, `-bootstrap`, and `-p2p-listen` are repeatable. `-instance` selects a
REAPI instance; the default is empty. `-disk-bytes` bounds storage and upload
staging (20 GiB by default). Verified read scratch has a separate equal budget,
so total data usage is bounded to twice that setting, plus metadata and keys.
Run `go run ./cmd/bushel -help` for all flags.

This first version provides SHA-256 CAS, Capabilities, ByteStream, and GetTree.
It verifies chunks and reconstructed blobs before returning bytes to clients.
FindMissingBlobs checks local publication metadata. Read misses fetch from
connected peers, then populate the local CAS. Interrupted uploads restart at
zero; compression and client-side chunk-mapping RPCs are not advertised.

The CAS endpoint is a building block for Bazel infrastructure. A complete build
cache also needs an action-cache service. Follow-up work is tracked for
[eviction and storage performance](https://github.com/deepinthebuild/bushel/issues/3),
[global content routing](https://github.com/deepinthebuild/bushel/issues/4),
[upload resumption and interoperability](https://github.com/deepinthebuild/bushel/issues/5),
and [NAT traversal](https://github.com/deepinthebuild/bushel/issues/6).
Lookup currently covers a bounded set of connected peers; a miss does not prove
global absence. Public discovery requires reachable peer addresses.

The dependency scan reports the upstream DHT censorship advisory
[GO-2024-3218](https://pkg.go.dev/vuln/GO-2024-3218), with no known fixed version.
It can disrupt peer discovery; explicit peers with discovery disabled avoid
that lookup path. Mitigation work is tracked in [issue #7](https://github.com/deepinthebuild/bushel/issues/7).

See [the design](docs/design.md), [v1 wire format](docs/protocol-v1.md), and
[digest decision and benchmarks](docs/digest-choice.md).

Run `go test ./...` and `go vet ./...`. The integration test uploads through one
node's gRPC endpoint and downloads through another node over real Bitswap,
without contacting public IPFS. CI runs native tests on Windows, macOS, and
Linux, a Linux race check, and builds all six amd64/arm64 OS targets.
