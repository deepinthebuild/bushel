# Bushel

A peer-to-peer Bazel content-addressable store, written in Go. Bushel bridges
REAPI gRPC to Bitswap, with RepMaxCDC chunks and SHA-256 verification.

See [the design](docs/design.md) and [v1 wire format](docs/protocol-v1.md).

Development requires Go 1.26.5 or newer. Run `go test ./...` and `go vet ./...`.
