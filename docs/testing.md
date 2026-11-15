# Generative tests

Use the Bazel build and test commands in [README](../README.md) first. Ordinary
tests include Rapid properties and every checked-in Go fuzz seed. Timed fuzzing
adds mutation beyond those seeds.

The pure boundaries are `parseResource(instance, name, write)`,
`advanceUpload(state, part)`, digest/manifest validation, and deterministic
chunking. Keep protocol decisions in those functions; keep stream and disk I/O
in their adapters. Rejected upload transitions return the original state.

Properties check independent invariants and models:

- Chunk bytes reconstruct the input and match Buildbarn's simpler reference
  RepMax implementation across read boundaries, content patterns, and size edges.
- Manifest/digest encodings round-trip, while structural inconsistencies fail.
- Upload partitions preserve identity, sequential offsets, and complete-only
  finishing; rejected parts cannot advance state.
- Store histories compare put, import, read, corruption, repair, cancellation,
  and reopen against an injected Buildbarn backend. Partial or ambiguous writes
  can leave verified backend data, but cannot publish catalog records or hints.
- Independent CS/CMS eviction, stale presence, backend faults, and concurrent
  catalog pruning/repair exercise availability without trusting presence hints.
- Sized CID properties cover hash/size conversion, segmented hashing and actual
  Bitswap framing; mapping stream properties bound aggregate remote responses.
- Peer imports independently verify chunk digests, the whole-blob digest, and
  canonical cuts. Concurrent writers and quota failures preserve publication.
- Announcement histories exercise overflow recovery, retries, restarts, offline
  pauses, and fair catalog progress. Virtual time covers deadlines and shutdown.
- Provider-store models exercise atomic batches, eviction, snapshots, cancellation,
  and global, per-key, per-provider, and pending-memory bounds.
- Membership histories exercise full-tail replacement, restart and reordered
  views, additions during rebuilds, overflow and ordinary stale-positive misses.
  Filter properties require every captured identity to survive wire round trips.
- Federation histories exercise workspace inference, reordered upload observations,
  two distinct peer receipts, aging under small-blob retries, restart accounting,
  journal corruption, and source-only seed transfer allowances.

Rapid shrinks both generated values and action histories. Store cases receive
fresh explicit temporary directories, removed after each case, including each
shrink attempt. Histories have at most 16 actions over four small blobs; chunk
properties exercise realistic 256–512 KiB chunks. Fuzz chunk inputs are capped
at 2 MiB. Increase check counts locally when investigating a failure:

```sh
bazel test //internal/cas:cas_test --test_arg=-rapid.checks=300 --test_arg=-rapid.seed=12345 --test_arg=-rapid.nofailfile
bazel run @rules_go//go -- test ./internal/reapi -run '^$' -fuzz '^FuzzUploadTransition$' -fuzztime=5s -parallel=2
```

Replay with the seed Rapid reports. Turn a useful minimized failure into a
sanitized source regression or fuzz seed; do not commit generated Rapid reports,
which can contain local paths and runtime details.

Fuzz targets include `FuzzParseDigest`, `FuzzParseManifest`,
`FuzzChunkSegmentation`, `FuzzCatalogDigest`, and `FuzzCatalogRecord` in `internal/cas`, plus `FuzzParseResource` and
`FuzzUploadTransition` in `internal/reapi`. Use an exact target expression and
`-run '^$'` for timed fuzzing so each target does not repeat the property suite.
`internal/p2p` also fuzzes `FuzzSizedCIDCanonicalRoundTrip`.
`internal/membership` fuzzes bounded protobuf decoding and filter query safety
with `FuzzMembershipSnapshot` and `FuzzMembershipRequest`.
Federation adds `FuzzSeedOfferParsing`, `FuzzMetadataAndLabel`,
`FuzzPublicationSnapshot`, and `FuzzPublicationJournal`; CI runs bounded fuzzing
alongside the normal properties and Linux race tests.
