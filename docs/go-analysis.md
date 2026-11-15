Bazel builds and tests run first-party Go analysis through its built-in
validation aspect and rules_go's `nogo` actions. Analysis uses the configured
target sources, platform, build tags, dependency exports, and cached analyzer
facts. Both internal and external test packages are checked. No `go list` or
module-cache lookup is needed during these actions.

`MODULE.bazel` selects the upstream `@rules_go//:tools_nogo` preset. Its checks
follow the pinned rules_go release and intentionally differ from the SDK's
default `go vet` suite; no separate analyzer list is maintained. Analyzer
implementations use the `golang.org/x/tools` version resolved by the shared
Go dependency graph.

The narrow rules_go patch adapts its facts decoder to the newer `x/tools` API
required by libp2p, reusing the existing importer's canonical package objects.
Remove it when a rules_go upgrade includes that adapter; see
[issue #24](https://github.com/deepinthebuild/bushel/issues/24).

The scope includes generated first-party Go code. External dependencies supply
facts but their diagnostics are excluded.

rules_go 0.65 analyzes selected Go sources: it does not supply the analyzer
`OtherFiles` or `IgnoredFiles` lists used for assembly and excluded-source
checks. It also uses the SDK's Go version, currently equal to Bushel's `go.mod`
Go directive. Revisit those limits when adding native sources or changing
module version requirements.
