# Storage

Bushel uses bb-storage's `NewCASFromConfiguration` for chunk storage (CS), chunk
mapping storage (CMS), and their configured caches. `-storage-config` reads the
upstream [ContentAddressableStorageConfiguration](https://github.com/buildbarn/bb-storage/blob/8d2acac49b3b17bad8dade297ef98847685d3699/pkg/proto/configuration/blobstore/blobstore.proto)
unchanged, using Buildbarn's JSON/Jsonnet loader. All backend types and decorators
supported by that factory are available; platform-specific upstream restrictions
still apply. Raw device paths require a supporting OS; regular files work on
Windows, macOS, and Linux. Configuration paths resolve from the invocation
directory, while Jsonnet imports resolve relative to their importing file.

Without a configuration file, Bushel creates persistent regular-file CS and CMS
backends under `data/storage`. Their capacities and eviction are independent of
Bushel's staging budget. An in-memory example is available for experiments:

```sh
bazel run //cmd/bushel -- '-storage-config=examples/storage-memory.jsonnet'
```

The selected backend instance must support SHA-256. If it advertises RepMaxCDC
parameters, they must match Bushel: minimum 262144, horizon 1048576 bytes. Bushel
always applies these parameters, even to backends that advertise no chunking.
Empty and single-chunk blobs require no CMS entry. Peer chunk CIDs carry both
SHA-256 and size, so they convert directly to Buildbarn digest keys.

Uploads are staged and fully verified before backend writes. Chunks are written
first, mappings last. Repeated chunks in one upload are staged and written once;
each later upload writes its distinct chunks again so it can repair corruption or
eviction even when configured caches report stale presence. Staging reservation
still covers the full upload size. Failed or ambiguous writes can leave verified
orphan chunks or a complete blob without an announcement record; retries are safe. Reads verify
chunk digests, canonical boundaries, and the complete blob into bounded scratch
before returning bytes. Presence caches and provider records are only hints:
independent CS/CMS eviction and stale positives are ordinary misses.

Buildbarn has no enumeration API. Bushel keeps a local, fixed-record catalog of
observed publications for announcements; it cannot enumerate preexisting objects
in arbitrary remote backends. Such objects remain fetchable by known digest.
Catalog scans probe availability and prune missing entries without deleting a
concurrent repair. Backend failures preserve entries for retry while healthy
entries continue renewing. ByteStream retries and status checks verify existing
backend blobs and recover missing publication metadata.

Storage is constructed once per daemon process. Shutdown stops network operations
before Buildbarn's registered flush routines; upstream local mappings remain
process-owned until exit. There is no storage hot reload. Persistent data follows
the configured Buildbarn sync policy, and a failing device can delay shutdown.
Tests of persistence and shutdown use subprocesses; fault and eviction properties
use explicit, injected CS/CMS implementations.
