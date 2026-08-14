# Contributing to looprig/flow

Thanks for considering a contribution. `flow` is a durable, pregel-style
workflow engine distributed as a Go library, part of the looprig multi-module
ecosystem. This file is the short guide for working in *this* repository.

## Before you write code

1. Read [`CLAUDE.md`](CLAUDE.md) (a.k.a. `AGENTS.md`). It is the authoritative
   source for the design, security, dependency, build, and code rules this
   module follows. PRs that contradict it will be asked to change.
2. Skim [`docs/plans/2026-06-24-flow-engine-design.md`](docs/plans/2026-06-24-flow-engine-design.md)
   for the architecture, types, and execution model — the design doc is where
   struct definitions and API specifics live, not `CLAUDE.md`. The rest of
   [`docs/plans/`](docs/plans/) shows the design-doc style the project uses.
3. Open an issue for anything non-trivial so we can agree on direction before
   you spend the time.

## Design and security rules (the short version)

- **SOLID, strictly enforced.** Single responsibility per struct/function/
  package. Extend via interfaces and composition, never by modifying a
  working type. Every interface implementation honors the full contract
  (Liskov). Interfaces stay small and focused (Interface Segregation).
  Depend on interfaces, not concrete types, and wire dependencies at the
  composition root (Dependency Inversion).
- **Strict typing everywhere.** No `any`/`interface{}` past a serialization
  boundary or the engine's single validated type-erasure seam. Named types
  (`type UserID string`) over bare primitives when the value has domain
  meaning. All domain concepts are typed structs, never
  `map[string]interface{}`.
- **All errors are typed.** Define a concrete error struct for every distinct
  failure mode so callers can `errors.As`. Never return bare `errors.New`/
  `fmt.Errorf` from package-level APIs; never swallow an error with `_`.
  Sentinel errors are permitted only for context-free leaf errors.
- **Security is first-class.** Validate all external input at the boundary,
  before it reaches business logic. Least privilege: pass narrow interfaces,
  never a whole config or god-object. No hardcoded secrets — fail loudly on
  startup if a required secret is missing. Sanitize anything that flows into
  a query, file path, or log line; run externally-supplied paths through
  `filepath.Clean` and verify they stay within the expected root. Fail
  secure: on error or ambiguity, deny by default. Never log full payloads,
  credentials, or PII at info level. Use `crypto/rand`, never `math/rand`,
  for anything security-sensitive.
- **Prefer stdlib; the engine core targets zero runtime dependencies.**
  External packages require explicit user approval in the conversation that
  adds them — state what the package is, why stdlib is insufficient, and
  what it adds. Do not `go get` or edit `go.mod` without a clear "yes".
  Once approved, the package is recorded in the approved list in
  `CLAUDE.md`. Dev/tool-only dependencies (staticcheck, gosec, govulncheck)
  are already approved and listed there too.

## Build, test, and secure

Run these before pushing. CI runs the same.

```sh
make fmt       # gofmt the module in place (GOWORK=off, GO_DIRS scoped to this module)
make fmt-check # fail if any tracked file isn't gofmt-clean
make build     # CGO_ENABLED=0 go build -trimpath ./...
make test      # go test -race ./...           (always -race)
make lint      # fmt-check + go vet + staticcheck + gosec
make vuln      # go mod verify + govulncheck
make secure    # lint + vuln
make check     # secure + build + test
```

`make check` is the single target that runs everything; use it as the final
gate before opening a PR. Integration tests are tagged
`//go:build integration` and run explicitly with
`go test -tags integration -race ./...`. Fuzz any function that parses
external input: `go test -fuzz=FuzzXxx ./path/to/pkg -fuzztime=30s` (`make
fuzz` prints the usage reminder).

**Dependencies are pinned, not vendored.** `go.mod` pins exact versions and
`go.sum` verifies their content hashes, which is what makes a build
reproducible. This module deliberately has no `vendor/`: a vendor tree is
ignored under a `go.work` but silently satisfies a `GOWORK=off` build, so a
stale one lets standalone verification pass against the vendored copy rather
than the version `go.mod` actually pins — defeating the purpose of verifying
standalone. The Makefile exports `GOWORK=off`, so every target already checks
this module against its real pinned dependencies. Don't run `go get`
casually.

## Tests

- **Table-driven tests, mandatory.** Every test function uses a
  `[]struct{ name string; ... }` table, and each subtest calls
  `t.Parallel()`. Every table covers the happy path, boundary values (zero,
  empty, single element, max), error cases (invalid/missing/wrong type), and
  domain edge cases specific to the unit under test.
- A test that passes without `-race` but fails with it is **not passing**.
  Concurrency is core to this engine, so race coverage is non-negotiable.
- Never assume a test framework or script beyond what's in the `Makefile`;
  it's the source of truth for how tests run.

## Pull requests

- Branch from `main`, name the branch something descriptive.
- One logical change per PR. If a change spans modules, open a PR per
  module and stack them.
- Write a clear description: what, why, the design alternative you
  rejected, and how you verified. `make check` output is welcome in the
  PR body.
- Don't force-push after review; add commits and let the reviewer squash.
- Don't commit secrets, tokens, or credentials. Don't add a new external
  dependency without prior approval (see `CLAUDE.md`).
- Don't update `CLAUDE.md`, `Makefile`, or `go.mod` unless the change is
  the point of the PR.
- Non-trivial design changes go through a short design doc in
  `docs/plans/`, dated the day you start (`YYYY-MM-DD-<topic>-design.md`),
  matching the existing files there.

## Code of conduct

Be excellent to each other. Discussions stay technical and respectful;
personal attacks, harassment, and discrimination are not welcome.

## License

By contributing, you agree that your contributions are licensed under the
Apache License 2.0, as described in [`LICENSE`](LICENSE).
