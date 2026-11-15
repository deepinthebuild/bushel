# Local gRPC admission and memory

The loopback listener accepts at most 16 connections. Each connection has four
HTTP/2 streams and the whole server admits 32 decoder/handler lifetimes. A single
connection therefore leaves both RPC and CAS operation capacity for other
Bazel clients, even when all four streams hold stalled uploads. Requests exceeding
global admission fail with `ResourceExhausted`; HTTP/2 stream quota waits happen
in the client and are cancellable. This is bounded sharing, not a fairness
guarantee against a local process opening every allowed connection.

Unary admission wraps cloned service method descriptors. With pinned grpc-go
v1.85.0, `wrapUnaryHandler` calls `MethodDesc.Handler`, whose decoder calls
`RecvMsg`. The generated method invokes its unary interceptor *after* decoding;
that interceptor cannot protect decode memory. Streaming admission precedes the
first receive. Slots remain owned until handlers and any receive pumps return,
including after client cancellation, and are never released merely because a
transport stream closes. The malformed-protobuf saturation regression checks
this ordering over real gRPC
connections; recheck it on upgrades.

The limits apply in layers (MiB means 2^20 bytes):

| Layer | Bound |
| --- | --- |
| Connections / transport streams | 16 / 64 |
| Header list per stream | 16 KiB including HTTP/2 field overhead |
| Static stream / connection receive windows | 64 KiB / 64 KiB, adaptive growth disabled |
| Message wire size and decompressed size | 4.25 MiB |
| Admitted decoding and handlers | 32 across all connections |
| ByteStream receive pipeline | Current request, one queued request, one receive in progress per admitted upload |
| CAS working operations | Eight, independently bounded by CAS staging and read budgets |

Do not treat the static connection window as a total queued-payload limit:
gRPC replenishes connection credit independently of application consumption.
Stream receive credit also temporarily grows for an admitted message read.
A conservative inbound payload account is `(64 + 32) * 64 KiB + 32 * 4.25 MiB`
for active and retiring transport credit, plus up to `32 * 4 * 4.25 MiB` for upload decoded
payloads in the bounded pipeline plus the receive pump's wire buffer. These are
payload budgets, not an RSS promise:
protobuf object expansion, allocator rounding, bounded headers, gRPC frame/control
objects, outgoing responses, Go stacks/GC, configured Buildbarn caches and
process-owned mappings are additional. Unary protobuf expansion is bounded by
32 messages of at most 4.25 MiB, including malformed repeated fields; the 1024
batch-item check is a handler check, not a predecode allocation limit. Responses
are at most 4.25 MiB each and transport handler concurrency is bounded by four
per connection; response serialization can outlive the application admission slot.

Upstream sequencing and flow-control references:
[server.go](https://github.com/grpc/grpc-go/blob/v1.85.0/server.go),
[http2_server.go](https://github.com/grpc/grpc-go/blob/v1.85.0/internal/transport/http2_server.go),
[flowcontrol.go](https://github.com/grpc/grpc-go/blob/v1.85.0/internal/transport/flowcontrol.go).

Shutdown cancels transports and waits for handlers before closing CAS state,
backend dependencies, and the daemon process lock. Interrupted uploads discard
staging; reopening persistent Buildbarn storage is tested in a new process.
Detected missing/corrupt local bytes invalidate only the catalog generation
observed before verification. A concurrent repair's newer marker survives;
transient backend errors and cancellation retain the marker. Presence remains
an unverified hint, and reads still verify all bytes before serving.
