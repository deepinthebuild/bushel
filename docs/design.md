# Design

- Bazel side: SHA-256 REAPI CAS, Capabilities, ByteStream. No execution service.
- Storage: Buildbarn's CS/CMS backends and upstream configuration, with Bushel's
  fixed RepMaxCDC and SHA-256 checks. Stage and verify uploads before backend
  writes; publish an announcement catalog entry after chunks and mapping succeed.
- Network: Boxo Bitswap for chunks. A separate libp2p lookup protocol resolves
  Bazel digest/size pairs to manifests; these mappings are untrusted hints.
- Discovery: public IPFS DHT in client mode, one fixed Bushel rendezvous CID.
  Never publish or query blob/chunk provider records on that DHT. Bitswap uses
  only Bushel peers, isolated from public bootstrap connections.
- Routing: a separate Bushel DHT finds manifest providers from the REAPI hash
  and size. Bounded asynchronous announcements follow verified publication;
  streaming catalog scans recover restarts and renew provider records.
- Trust: validate manifest bounds, each chunk, and the complete reconstructed
  blob before serving it to Bazel. Bind gRPC to loopback by default.
- Compatibility: provisional protocol IDs and golden fixtures; breaking changes
  are permitted until the first stable release. Pure Go supports Windows, macOS,
  Linux, amd64 and arm64.

Routing reaches beyond connected peers, but partitions, stale records, index
saturation and adversarial routing can still cause misses. Durable replication,
NAT relay service and private artifact confidentiality remain outside
this implementation. Only publish artifacts suitable for peers.
Action-cache entries cannot be authenticated by their action digest and are
outside the initial CAS scope.

References: [Bitswap](https://specs.ipfs.tech/bitswap-protocol/),
[RepMaxCDC](https://github.com/buildbarn/go-cdc),
[REAPI](https://github.com/bazelbuild/remote-apis).
