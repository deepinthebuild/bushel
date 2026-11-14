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
absent. One request per stream. Receivers enforce size and time limits before
allocation. Unknown protocol versions fail negotiation; they are never decoded
as v1. Manifest bytes are hints until their reconstructed blob is verified.

Public discovery uses only CIDv1/raw/SHA2-256 of the UTF-8 bytes
`bushel:peer-rendezvous:v1`. One provide per rendezvous renewal; never a per-blob
record. Content routing and Bitswap traffic use a separate Bushel host.
