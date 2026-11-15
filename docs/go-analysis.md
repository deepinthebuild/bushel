Bazel builds and tests run first-party Go analysis through its built-in
validation aspect and rules_go's `nogo` actions. Analysis uses the configured
target sources, platform, build tags, dependency exports, and cached analyzer
facts. Both internal and external test packages are checked. No `go list` or
module-cache lookup is needed during these actions.

`BUILD.bazel` explicitly selects the 35 analyzers in
[Go 1.27.4's default vet suite](https://github.com/golang/go/blob/go1.27.4/src/cmd/vendor/golang.org/x/tools/go/analysis/suite/vet/vet.go).
Keep that list aligned when upgrading the SDK; `nogo(vet = True)` selects only
five checks and `TOOLS_NOGO` includes a different set. Analyzer implementations
use the `golang.org/x/tools` version pinned for the Bazel tool in `MODULE.bazel`,
kept aligned with `go.mod` and resolved by Bzlmod.

The narrow rules_go patch adapts its facts decoder to the newer `x/tools` API,
reusing the existing importer's canonical package objects. Remove it when a
rules_go upgrade includes that adapter.

The scope includes generated first-party Go code. External dependencies supply
facts but their diagnostics are excluded. The separate digest benchmark module
remains outside the Bazel target graph.

rules_go 0.65 analyzes selected Go sources: it does not supply the analyzer
`OtherFiles` or `IgnoredFiles` lists used for assembly and excluded-source
checks. It also uses the SDK's Go version, currently equal to Bushel's `go.mod`
Go directive. Revisit those limits when adding native sources or changing
module version requirements.
