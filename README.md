# Bushel

A peer-to-peer Bazel content-addressable store, written in Go. Bushel bridges
REAPI gRPC to Bitswap, with RepMaxCDC chunks and SHA-256 verification.

Install [Bazelisk](https://github.com/bazelbuild/bazelisk#installation) as
`bazel`. The checkout pins Bazel 9.3.2 and downloads its Go SDK automatically.
Windows also needs Git Bash; set `BAZEL_SH` to that shell if another Bash is
selected from `PATH`. Build and test from the checkout root:

```sh
bazel build //cmd/bushel
bazel test //...
```

Run from the checkout root:

```sh
bazel run //cmd/bushel
```

The checked-in `.bazelrc` enables `--run_in_cwd`, keeping relative state paths
anchored to the directory where you invoke Bazel.

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
bazel run //cmd/bushel -- -discovery=false -data-dir=data/node-a '-p2p-listen=/ip4/127.0.0.1/tcp/8981'
```

Use its printed peer ID when starting the second node:

```sh
bazel run //cmd/bushel -- -discovery=false -data-dir=data/node-b '-grpc-listen=127.0.0.1:8982' '-p2p-listen=/ip4/127.0.0.1/tcp/8983' '-peer=/ip4/127.0.0.1/tcp/8981/p2p/PEER_ID'
```

`-peer`, `-bootstrap`, and `-p2p-listen` are repeatable. `-instance` selects a
REAPI and backend instance; the default is empty. Buildbarn's persistent local
backend stores chunks and chunk mappings, with automatic block eviction.
`-local-storage-bytes` sets the default chunk-file capacity (20 GiB); its mapping
and index files add 52 MiB. `-staging-bytes` separately bounds upload scratch and
catalog payloads (20 GiB), with an equal budget for verified read scratch.
Filesystem allocation overhead and configured backend caches are additional.
Use `-storage-config` for Buildbarn's full JSON/Jsonnet CAS configuration,
including remote backends and storage decorators; see [storage](docs/storage.md).
Use `bazel run //cmd/bushel -- -help` for all flags.

This first version provides SHA-256 CAS, Capabilities, ByteStream, and GetTree.
It verifies chunks and reconstructed blobs before returning bytes to clients.
FindMissingBlobs checks backend mapping and chunk presence. Read misses fetch from
connected peers or providers discovered through the Bushel DHT, then populate
the local CAS. Interrupted uploads restart at
zero; compression and client-side chunk-mapping RPCs are not advertised.

The CAS endpoint is a building block for Bazel infrastructure. A complete build
cache also needs an action-cache service. Follow-up work is tracked for
[storage performance](https://github.com/deepinthebuild/bushel/issues/3),
[upload resumption and interoperability](https://github.com/deepinthebuild/bushel/issues/5),
and [NAT traversal](https://github.com/deepinthebuild/bushel/issues/6).
The [Bushel provider DHT](docs/routing-v1.md) routes lookups beyond connected
peers. Verified uploads are announced asynchronously; catalog scans recover
restarts and renew availability hints. A miss does not prove global absence.
Public discovery and serving new peers require reachable TCP addresses.

Optional federated publication seeds eligible uploads to two distinct willing
peers. Enable a producer with `-federation-publish=external` and receivers with
`-federation-accept`. Workspace metadata selects publication queues; ordinary
writes finish locally and peer reads populate the local CAS. Both federation
options default to off. See [publication policy, limits, and wire format](docs/federation.md).

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

Wire formats may change incompatibly until the first stable release.
See [the design](docs/design.md), [wire format](docs/protocol-v1.md), and
[digest decision and benchmarks](docs/digest-choice.md).

The integration test uploads through one node's gRPC endpoint and downloads
through another node over real Bitswap,
without contacting public IPFS. CI runs native tests on Windows, macOS, and
Linux, `bazel test --config=race //...` on Linux, bounded fuzzing, and builds all
six amd64/arm64 OS targets. For example:

```sh
bazel build --platforms=@rules_go//go/toolchain:linux_arm64 //cmd/bushel
```

Ordinary builds use Go's pure mode. The Linux race tests enable cgo and use
the pinned [hermetic-llvm](https://github.com/hermeticbuild/hermetic-llvm)
toolchain. Native C/C++ dependencies must use this toolchain too; host compiler
autodetection is disabled.
SDK commands through `bazel run @rules_go//go` default to pure mode as well.

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
