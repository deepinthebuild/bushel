Keep design notes concise. Every commit must build and pass tests.
Bazel is the primary build. From the checkout root run:

```sh
bazel build //cmd/bushel
bazel test //...
bazel run @rules_go//go -- vet ./...
```

Use Bazelisk to respect `.bazelversion`; `MODULE.bazel` pins the Go SDK and rules.
Windows needs Git Bash available to Bazel (`BAZEL_SH` can select it).
Run `bazel test --config=race //...` on Linux for concurrency changes.

After adding Go files or changing `go.mod`/`go.sum`, run `bazel mod tidy` and
`bazel run //:gazelle`. Commit generated BUILD files and `MODULE.bazel.lock`.
Check generation with `bazel run //:gazelle -- -mode=diff`. Keep Go dependencies
in `go.mod`; Bzlmod imports them through `go_deps`. Use the pinned SDK for module
edits: `bazel run @rules_go//go -- mod tidy`. The separate `benchmarks/digests`
module is excluded from Gazelle; check it with:

```sh
bazel run @rules_go//go -- -C benchmarks/digests test ./...
```

Prefer pure state transitions with explicit inputs so property tests can cover
invariants. Put property tests and fuzz seeds in ordinary `go_test` targets;
keep network and filesystem adapters thin. Preserve failing fuzz inputs as
small, non-sensitive regression cases. CI additionally runs bounded fuzzing.

Before pushing each commit, request reviews from the persistent Go style,
performance/benchmarking, distributed systems, and security reviewer sessions.
Resolve blocking findings before publishing. Preserve v1 wire compatibility;
changes to chunking parameters require a new protocol version.
After all four reviews and required CI checks pass, agents may merge their own
PRs. Preserve the logically independent commits when merging.

Never commit machine-specific absolute paths, identifying host details, secrets,
or private keys. Use paths relative to the checkout root in committed files.
Review staged content for these before every commit.
