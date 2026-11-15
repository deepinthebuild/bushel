# Peer membership hints

Bushel periodically summarizes **complete blob SHA-256 + size identities** in a
binary fuse8 filter, with an exact tail of additions since the scan began.
Peers use positives to prioritize manifest requests. Every other candidate
remains eligible, including when a filter says absent, is incomplete, or expires.
Bitswap and final digest verification still determine availability and integrity.

Blob filters answer the initial lookup when only the REAPI identity is known.
Chunk filters would require a manifest first and a separate chunk inventory;
they do not solve that lookup. We use eight-bit fingerprints (about 1/256 false
positives) to keep summaries small. No content metadata enters public IPFS;
these summaries travel only between connected Bushel content peers.

## Capture and replacement

The daemon subscribes to committed blobs before its first catalog scan. It
captures a sequence watermark at scan start, retains subsequent additions while
the scan and filter construction run, and publishes the baseline and matching
tail atomically. Rebuilds use a separate pending tail so additions are not lost
when a previous snapshot is replaced. Subscription overflow, a truncated scan,
or failed backend probes make the result incomplete and request a later rebuild.
Completeness is diagnostic, never evidence of authoritative absence.

One random 128-bit session identifies a daemon lifetime. Generations increase
on rebuild; sequence numbers count observed commits within that session. Full
snapshot-plus-tail responses avoid dependency on a previously delivered delta.
Same-session replacements cannot regress generation or sequence; a generation's
baseline is immutable and its tail can only grow. Polling is serialized per
peer, so a response from an earlier session cannot arrive after a later request.
Snapshots are not persisted across restarts. Rebuilding an identical summary
without new observations preserves the published view, avoiding transmission
caused only by a generation change.

Evictions need not update the immutable filter immediately. A positive followed
by a manifest miss or Bitswap DONT_HAVE is normal, whether caused by eviction or
a false positive. There is no negative cache or peer penalty. Each manifest
candidate is attempted once per lookup; fallback workers remain available while
positive candidates are slow. Periodic rebuilds remove evicted catalog entries.
Verified peer reads populate the local CAS and enter later membership summaries.

## Wire format

The content-host stream `/bushel/membership/1.0.0` accepts a uint8 request length,
then a canonical protobuf `Request`, then EOF. An empty request (length zero)
asks for a full view. Otherwise the request is exactly 34 bytes and carries the
SHA-256 of the complete canonical snapshot payload already held by the caller.
The response starts with a big-endian uint32: zero means unavailable, one means
unchanged, and larger values give the byte length of the following deterministic
protobuf [`Snapshot`](../internal/membership/pb/membership.proto). EOF terminates
every response. An unchanged response refreshes only the exact cached view
named by that request. Snapshot fields describe the session, generation, baseline
watermark, current sequence, completeness, key algorithm, distinct key count,
binary fuse8 geometry and exact addition identities. There is no Go struct dump
or library-specific serialization. Formats can change in place before the first
stable release.

The membership key is the first eight bytes, interpreted big-endian, of:

```
SHA256(ASCII("bushel:blob-have:") || blob_sha256[32] || uint64be(blob_size))
```

Keys are sorted and deduplicated before construction with
[`xorfilter v0.5.1`](https://github.com/FastFilter/xorfilter/tree/v0.5.1).
The seed, power-of-two segment length, segment count and fingerprint byte array
fully describe the filter. The receiver derives the mask (`length - 1`) and
count length (`count * length`) using checked arithmetic. Empty inventories omit
the filter. Library `Load` is not used on untrusted input.

Lookup uses the pinned library's unsigned 64-bit arithmetic: apply `murmur64`
to `key + seed`, form the low eight bits of `hash XOR (hash >> 32)`, and XOR the
three indexed fingerprints. Index zero is the high 64 bits of
`hash * count_length`. The next two indices are `(index0 + length) XOR
((hash >> 18) AND mask)` and `(index0 + 2*length) XOR (hash AND mask)`.
The result is a positive iff that XOR is zero. Wire and key golden vectors
guard against accidental changes on dependency upgrades.
`murmur64(x)` applies `x ^= x >> 33`, multiplies by `0xff51afd7ed558ccd`,
applies that XOR again, multiplies by `0xc4ceb9fe1a85ec53`, then applies the XOR
once more; all multiplication wraps modulo 2^64.

## Budgets and validation

- At most 262,144 baseline keys, 1,024 exact tail identities, 400 KiB of
  fingerprints, and 512 KiB per encoded view.
- One scan/build at a time; rebuild every five minutes, at least 30 seconds
  apart on overflow. Scans have a two-minute deadline. Capacity limits and probe
  failures retain the regular cadence instead of continuously restarting scans.
- At most 32 cached peer views, independently of the connection limit. Entries
  expire locally after three minutes without a valid refresh.
- A stable polling cohort avoids cache churn above 32 connected peers. While
  reads are active, it rotates every 30 minutes; disconnections refill slots.
  Idle daemons keep their cohort, and other peers remain ordinary fallback.
- Two polling workers, one request per peer at a time, one-minute refresh and
  five-second stream deadlines. An aggregate budget permits a sustained 32 poll
  starts per minute with a burst of four, including failed requests and peer
  churn. Serving has bounded concurrency and request rate.

Catalogs beyond the key budget produce partial hints and continue ordinary
lookup. The library needs more construction scratch than retained fingerprints;
the key cap bounds that work. Requests serve cached encoded bytes, without scans
or filter construction on the request path.

The decoder checks framing, collection sizes and field multiplicity before
generated protobuf unmarshalling. It rejects unknown fields, noncanonical
encodings, wrong digest lengths, invalid sizes and unsafe filter geometry.
Fingerprint length must exactly equal `(segment_count + 2) * segment_length`.
No peer-controlled count is used for an unbounded allocation or array index.

Property tests cover true positives, full-tail replacement, packet reordering,
restart, concurrent additions and overflow. Fuzzing exercises decoding and
queries against accepted shapes. Network tests exercise fallback from false
positives and stale hints, including backend eviction.

Run reproducible microbenchmarks with:

```sh
bazel test //internal/membership:membership_test --test_arg=-test.run=^$ --test_arg=-test.bench=. --test_arg=-test.benchmem --test_arg=-test.count=3 --test_arg=-test.benchtime=200ms --test_output=streamed
```

Measured on Windows/amd64 with the pinned SDK; three-sample medians, excluding
fixture construction and catalog/backend I/O:

| Baseline keys | Snapshot bytes (empty tail) | Build + encode | Cumulative allocation |
| ---: | ---: | ---: | ---: |
| 1,024 | 1,587 | 0.10 ms | 42 KiB |
| 32,768 | 39,990 | 4.32 ms | 1.12 MiB |
| 262,144 | 303,158 | 37.47 ms | 8.60 MiB |

A full 1,024-entry tail adds about 40 KiB. At maximum cardinality, retained
filter/tail payload is 344,064 bytes and the measured encoded fixture is 344,118
bytes (size varints depend on the input). The benchmark's input digest array
occupies 10 MiB at the key cap; the adapter's growing slice can retain additional
capacity. Cumulative allocation is not peak heap. Queries took 86–199 ns without allocations.
Full-tail decode allocated 1.03 MB; timing varied with GC (median 1.45 ms).
Codec timings use a separate three-sample repeat with `-test.benchtime=500ms`.

An unchanged poll carries 39 application bytes including framing, rather than
another full snapshot. Fetching a maximum snapshot initially costs as much as
about 7,821 unsuccessful 40-byte manifest requests plus four-byte empty replies,
excluding libp2p framing. These are byte comparisons, not network latency claims.
Ordinary Bitswap HAVE queries concern chunks already identified by a manifest;
they cannot replace initial blob lookup. Full changed snapshots remain a
bandwidth tradeoff; measure actual lookup workloads before adding delta sync or
a second chunk filter.
