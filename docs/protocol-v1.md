# Bushel v1

Integers are unsigned big-endian. SHA-256 values are 32 raw bytes. No native Go
serialization appears on the wire. Incompatible changes get a new protocol ID.

RepMaxCDC: Buildbarn's `NewRepMaxContentDefinedChunker`,
`FastContentDefinedChunkerGearTable` from go-cdc v0.0.10,
minimum 262144 bytes, horizon 1048576 bytes. Chunks are less than 524288 bytes;
only a single-chunk blob may be smaller than the minimum. Empty blobs have no
chunks. All blobs use this pipeline, even when only one chunk results.

Manifest: `BSHLM001` (8 bytes), blob SHA-256 (32), blob size (8), chunk count (4),
then ordered entries of chunk SHA-256 (32) and chunk size (4). No trailing bytes.
Maximum blob size 8 GiB, maximum chunk count 32768; validate total sizes exactly.
Chunks use CIDv1, raw codec, SHA2-256. Manifests are bounded to 1179700 bytes.
Canonical chunking and a full blob hash are checked when importing a manifest.

Lookup stream `/bushel/manifest/1.0.0`: client sends blob hash (32) and size (8).
Server returns a 4-byte manifest length, then exactly that many bytes. Zero means
absent. The client half-closes its write side after the 40-byte request; the
server closes after its response. Extra bytes are rejected. One request per stream.
Receivers enforce size and time limits before
allocation. Unknown protocol versions fail negotiation; they are never decoded
as v1. Manifest bytes are hints until their reconstructed blob is verified.

Public discovery uses only CIDv1/raw/SHA2-256 of the UTF-8 bytes
`bushel:peer-rendezvous:v1`. One provide per rendezvous renewal; never a per-blob
record. Content routing and Bitswap traffic use a separate Bushel host.

Both hosts use the same persistent peer identity. The discovery host is an
outbound-only DHT client; its advertised addresses are the content host's TCP
listeners. A provider record therefore authenticates directly to the content
host without another address-exchange protocol. Public discovery accepts only
literal, publicly routable IP/TCP addresses, excluding IPv6 translation and
tunneling ranges such as NAT64, 6to4, and Teredo; explicitly configured peers may
use private addresses. Reachability through NAT requires an existing outbound
connection or suitable network configuration; v1 supplies no relay service.

Bitswap v1.2 uses its standard protobuf wire format under the application-scoped
protocol ID `/bushel/ipfs/bitswap/1.2.0`. This prevents a non-Bushel IPFS peer from
receiving content want-lists even if it appears in a rendezvous response. The
Bitswap provider finder is a no-op and never holds a public DHT reference.
Manifests are served directly; chunk availability is advertised only through
Bitswap HAVE/DONT_HAVE responses on the content host.

Initial limits: 64 content connections by default (configurable from 8 to 128), 64
discovery connections, 16 concurrent manifest handlers, four blob fetches, and
eight chunks per fetch window. Discovery permits one connection per peer and
prunes unprotected connections above a total of 48 toward eight, in addition to
the DHT's maximum 40 protected routing peers. This leaves at least 16 connection
slots of headroom after pruning. Manifest lookups use four direct workers and
two provider-referral workers with at most a five-second deadline each;
other requests/windows have a 30-second
default deadline. A complete fetch has a five-minute ceiling. Local disk/quota
errors are returned directly; only peer failures allow trying another manifest.
Successful rendezvous
provides renew every 12 hours; failures retry no sooner than 15 minutes. Peer
discovery and explicit-peer reconnect attempts run at most once per five minutes.
The [Bushel provider DHT](routing-v1.md) extends lookup beyond connected peers
without changing these manifest or Bitswap formats. A miss is not proof of
global absence. Cached chunks and full blobs are reverified before acceptance.
