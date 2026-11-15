# Bushel

Bushel is a peer-to-peer content-addressable store (CAS) for sharing Bazel build
artifacts across machines. Each daemon stores blobs locally and can fetch missing
blobs from other Bushel peers. Downloaded content is verified and saved locally
so subsequent reads can use the local copy.

Clients address blobs by their SHA-256 hash and byte size through Bazel's Remote
Execution API (REAPI) over gRPC. Bushel finds peers holding the requested blob,
obtains its chunk list, and transfers the chunks using Bitswap. RepMaxCDC
content-defined chunking keeps transfers within Bitswap's block-size limit and
allows repeated content to share chunks. Every chunk and the reconstructed blob
are verified before bytes are returned to the client.

Local storage uses Buildbarn's chunk-storage and chunk-mapping backends and
configuration. The default backend persists chunks and their mappings in regular
files with automatic eviction; Buildbarn's other storage backends and decorators
are also available. See [storage](docs/storage.md) for configuration and platform
restrictions.

The public IPFS network is used only to discover Bushel bootstrap peers through
a fixed rendezvous key. Blob-provider information stays in the separate
[Bushel DHT](docs/routing-v1.md), and blob manifests and chunk requests travel
only between Bushel peers. Connected peers exchange compact
[membership hints](docs/membership.md) to prioritize lookups. Eviction, stale
hints, and filter false positives are normal; lookups retain fallback paths.
A miss does not prove global absence.

Writes complete locally. Optional [federated publication](docs/federation.md)
uses REAPI workspace metadata to select eligible uploads and aims to place
copies on two distinct willing peers in the background. Publication is bounded
and best effort: too few willing peers, partitions, or eviction can leave fewer
copies. Reads from peers populate the local CAS without creating publication
jobs.

Bushel is written in Go for Windows, macOS, and Linux on x86_64 and aarch64.
It provides CAS, Capabilities, ByteStream, and GetTree services. A complete Bazel
build cache also needs an action-cache service; Bushel currently supplies the
blob-storage component. Interrupted uploads restart at zero, and compression
and client-side chunk-mapping RPCs are not advertised. Wire formats may change
incompatibly until the first stable release.

Public discovery and serving new peers require reachable TCP addresses.
The upstream [DHT censorship advisory](https://pkg.go.dev/vuln/GO-2024-3218)
can affect discovery and routing availability; mitigation is tracked in
[issue #7](https://github.com/deepinthebuild/bushel/issues/7). Explicit peers
with discovery disabled avoid public bootstrap lookup, while the Bushel overlay
still uses Kademlia. Share only artifacts suitable for other peers to read:
digest verification provides integrity, and peers holding content can
redistribute it.

See [the design](docs/design.md), [wire format](docs/protocol-v1.md), and
[digest decision](docs/digest-choice.md) for details. Remaining work includes
[storage performance](https://github.com/deepinthebuild/bushel/issues/3),
[upload resumption and interoperability](https://github.com/deepinthebuild/bushel/issues/5),
and [NAT traversal](https://github.com/deepinthebuild/bushel/issues/6).

To run Bushel, install [Bazelisk](https://github.com/bazelbuild/bazelisk#installation)
as `bazel`. The checkout pins Bazel 9.3.2 and downloads its Go SDK automatically.
Windows also needs Git Bash; set `BAZEL_SH` to that shell if another Bash is
selected from `PATH`. From the checkout root:

```sh
bazel run //cmd/bushel
```

The checked-in `.bazelrc` enables `--run_in_cwd`, keeping relative state paths
anchored to the directory where you invoke Bazel. The daemon serves plaintext
gRPC on `127.0.0.1:8980` and peer traffic on TCP 8981. It prints its peer addresses
at startup. State and its generated peer key live in the ignored `data` directory.
Use an empty directory for a new store; Bushel refuses to initialize over unrelated
files. Ctrl+C and SIGTERM shut it down. Public discovery is enabled by default.

For a local two-node network, start the first node:

```sh
bazel run //cmd/bushel -- -discovery=false -data-dir=data/node-a '-p2p-listen=/ip4/127.0.0.1/tcp/8981'
```

Use its printed peer ID when starting the second node:

```sh
bazel run //cmd/bushel -- -discovery=false -data-dir=data/node-b '-grpc-listen=127.0.0.1:8982' '-p2p-listen=/ip4/127.0.0.1/tcp/8983' '-peer=/ip4/127.0.0.1/tcp/8981/p2p/PEER_ID'
```

`-peer`, `-bootstrap`, and `-p2p-listen` are repeatable. `-instance` selects a
REAPI and backend instance; the default is empty. `-local-storage-bytes` sets
the default chunk-file capacity (20 GiB); its mapping and index files add
52 MiB. `-staging-bytes` separately bounds upload scratch and catalog payloads
(20 GiB), with an equal budget for verified read scratch. Filesystem allocation
overhead and configured backend caches are additional. Use `-storage-config`
for Buildbarn's full JSON/Jsonnet CAS configuration.

The routing index defaults to 32,768 blob/provider pairs, at most 16,384 from
one provider. `-routing-records` and `-routing-records-per-peer` tune this RAM
budget independently of CAS disk capacity. Saturation evicts old hints and can
reduce lookup coverage. The daemon reports announcement progress and warns
about capacity evictions or scans that risk provider-record expiry.

To enable federated publication, start a producer with
`-federation-publish=external` and receivers with `-federation-accept`.
Both options default to off. Use `bazel run //cmd/bushel -- -help` for all flags.

Build and test from the checkout root:

```sh
bazel build //cmd/bushel
bazel test //...
```

Builds and tests also run cached [Go static analysis](docs/go-analysis.md).
The integration test uploads through one node's gRPC endpoint and downloads
through another node over real Bitswap without contacting public IPFS. CI runs
native tests on Windows, macOS, and Linux, `bazel test --config=race //...` on
Linux, bounded fuzzing, and builds all six amd64/arm64 OS targets. For example:

```sh
bazel build --platforms=@rules_go//go/toolchain:linux_arm64 //cmd/bushel
```

Ordinary builds use Go's pure mode. The Linux race tests enable cgo and use
the pinned [hermetic-llvm](https://github.com/hermeticbuild/hermetic-llvm)
toolchain. Native C/C++ dependencies must use this toolchain too; host compiler
autodetection is disabled. SDK commands through `bazel run @rules_go//go` default
to pure mode as well.

After changing Go sources or dependencies, run `bazel mod tidy` and
`bazel run //:gazelle`, then commit the generated BUILD files and module lock.
`go.mod` and `go.sum` remain the Go dependency source of truth. See
[agent instructions](AGENTS.md) for the development checks and
[generative testing](docs/testing.md) for properties and fuzzing.
