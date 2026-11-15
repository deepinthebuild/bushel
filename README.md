# Bushel

A peer-to-peer Bazel content-addressable store, written in Go. Bushel bridges
REAPI gRPC to Bitswap, with RepMaxCDC chunks and SHA-256 verification.

Install [Bazelisk](https://github.com/bazelbuild/bazelisk#installation) as
`bazel`. The checkout pins Bazel 8 LTS and downloads its Go SDK automatically.
Windows also needs Git Bash; set `BAZEL_SH` to that shell if another Bash is
selected from `PATH`. Build and test from the checkout root:

```sh
bazel build //cmd/bushel
bazel test //...
```

Run `bazel-bin/cmd/bushel/bushel_/bushel` from the checkout root; on Windows,
use `bazel-bin/cmd/bushel/bushel_/bushel.exe`. The examples below use the former.
Running the built binary keeps relative state paths anchored to the checkout.

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
bazel-bin/cmd/bushel/bushel_/bushel -discovery=false -data-dir=data/node-a -p2p-listen=/ip4/127.0.0.1/tcp/8981
```

Use its printed peer ID when starting the second node:

```sh
bazel-bin/cmd/bushel/bushel_/bushel -discovery=false -data-dir=data/node-b -grpc-listen=127.0.0.1:8982 -p2p-listen=/ip4/127.0.0.1/tcp/8983 -peer=/ip4/127.0.0.1/tcp/8981/p2p/PEER_ID
```

`-peer`, `-bootstrap`, and `-p2p-listen` are repeatable. `-instance` selects a
REAPI instance; the default is empty. `-disk-bytes` bounds storage and upload
staging (20 GiB by default). Verified read scratch has a separate equal budget,
so total data usage is bounded to twice that setting, plus metadata and keys.
Pass `-help` to the built binary for all flags.

This first version provides SHA-256 CAS, Capabilities, ByteStream, and GetTree.
It verifies chunks and reconstructed blobs before returning bytes to clients.
FindMissingBlobs checks local publication metadata. Read misses fetch from
connected peers or providers discovered through the Bushel DHT, then populate
the local CAS. Interrupted uploads restart at
zero; compression and client-side chunk-mapping RPCs are not advertised.

The CAS endpoint is a building block for Bazel infrastructure. A complete build
cache also needs an action-cache service. Follow-up work is tracked for
[eviction and storage performance](https://github.com/deepinthebuild/bushel/issues/3),
[upload resumption and interoperability](https://github.com/deepinthebuild/bushel/issues/5),
and [NAT traversal](https://github.com/deepinthebuild/bushel/issues/6).
The [Bushel provider DHT](docs/routing-v1.md) routes lookups beyond connected
peers. Verified uploads are announced asynchronously; catalog scans recover
restarts and renew availability hints. A miss does not prove global absence.
Public discovery and serving new peers require reachable TCP addresses.

The routing index defaults to 32,768 blob/provider pairs, at most 16,384 from
one provider. `-routing-records` and `-routing-records-per-peer` tune this RAM
budget independently of CAS disk capacity. Saturation evicts old hints and can
reduce lookup coverage. The daemon reports announcement progress and warns
about capacity evictions or scans that risk provider-record expiry.

The dependency scan reports the upstream DHT censorship advisory
[GO-2024-3218](https://pkg.go.dev/vuln/GO-2024-3218), with no known fixed version.
It can disrupt public peer discovery and Bushel provider routing. Explicit peers
with discovery disabled avoid public bootstrap lookup; the Bushel overlay still
uses Kademlia. Mitigation work is tracked in [issue #7](https://github.com/deepinthebuild/bushel/issues/7).

See [the design](docs/design.md), [v1 wire format](docs/protocol-v1.md), and
[digest decision and benchmarks](docs/digest-choice.md).

The integration test uploads through one node's gRPC endpoint and downloads
through another node over real Bitswap,
without contacting public IPFS. CI runs native tests on Windows, macOS, and
Linux, `bazel test --config=race //...` on Linux, bounded fuzzing, and builds all
six amd64/arm64 OS targets. For example:

```sh
bazel build --platforms=@rules_go//go/toolchain:linux_arm64 //cmd/bushel
```

After changing Go sources or dependencies, run `bazel mod tidy` and
`bazel run //:gazelle`, then commit the generated BUILD files and module lock.
`go.mod` and `go.sum` remain the Go dependency source of truth. See
[agent instructions](AGENTS.md) for the development checks and
[generative testing](docs/testing.md) for properties and fuzzing.

The isolated digest comparison remains a supplemental Go benchmark module,
using the same pinned SDK:

```sh
bazel run @rules_go//go -- -C benchmarks/digests test -run='^$' -bench=. -benchmem
```
