# Blob-provider routing

The public IPFS DHT supplies Bushel bootstrap peers through the fixed rendezvous
key. A separate DHT on the content host uses `/bushel/routing/v1/kad/1.0.0` and
the libp2p Kademlia protobuf format. It serves provider records, disables value
records, and seeds only from Bushel peers. It never defaults to public IPFS
bootstrap nodes. Existing manifest and Bitswap protocol IDs are unchanged.

The provider key is CIDv1/raw/SHA2-256 over this byte sequence:

```text
UTF-8("bushel:blob-provider:v1") || 0x00 || blob_SHA256[32] || blob_size_u64_be
```

The key identifies providers of a blob's manifest. The manifest remains keyed
by the original REAPI hash and size. Bitswap still receives only chunk CIDs and
has no DHT provider finder. No blob or chunk keys reach the public IPFS router.

On a local read miss, direct-peer manifest lookup runs alongside an iterative
provider lookup through the Bushel overlay. Up to 16 provider candidates and
two concurrent referral connections are tried, alongside four direct manifest
lookups. Referrals pass the public-IP/TCP address policy and the dial-time gate;
they never acquire the trust granted to explicitly configured peers. Existing
chunk, whole-blob, and canonical-chunking verification remains authoritative.
Stale providers and ordinary absence trigger fallback, not corruption penalties.

Verified CAS publication emits a nonblocking hint. A startup catalog scan and
periodic scans recover restarts and missed hints without an in-memory list of
the whole CAS. One announcement worker retains at most 1,024 queued/inflight
digests, starts at most 10 calls/second, and gives each call 15 seconds. Retries
use bounded jittered backoff. Overflow or exhausted retries schedule a recovery
scan after 15 minutes. Normal renewal scans run six hours after a completed walk;
provider records expire after 24 hours. Scan errors restart the task after
bounded backoff. Local upload success does not wait for global advertisement.
An announcement call returning success is a best-effort attempt, not confirmation
that remote peers retained the record or that a subsequent lookup will succeed.

Provider metadata is an ephemeral bounded index, independently sized from CAS
bytes. Defaults retain 32,768 blob/provider pairs, at most 16,384 per provider
and 16 per blob, with an 8 MiB key/value payload budget. Go metadata, query
snapshots, and bounded pending writes require additional memory. The upstream
manager may overlay up to 320 pending records before flushing. Its unbounded
per-key cache is disabled. Peer addresses have separate total/per-peer caps.
The content host permits 64 connections by default and prunes unprotected peers
to leave room for routing queries and transfers alongside protected DHT peers.

`-routing-records` and `-routing-records-per-peer` tune index capacity; the
payload budget scales with the total record limit. Saturation evicts old hints
and can reduce lookup coverage. Occupancy, eviction counts, announcement backlog
and sweep duration are observable; capacity evictions produce warnings. Renewal
must finish with ample margin before expiry. A slow or overloaded scan does not
guarantee that every blob stays discoverable, and a miss never proves global
absence. A disk-backed index can extend capacity if catalogs exceed a reasonable
RAM budget.

All nodes serve the application DHT. Serving discovered clients requires a
reachable public TCP address; automatic NAT relaying remains separate work.
Partitions, unreachable providers and adversarial routing can prevent lookup.
The upstream DHT censorship advisory applies to this overlay as well as public
bootstrap discovery; digest verification protects retrieved bytes, not availability.
