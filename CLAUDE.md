# CLAUDE.md — Development Guidelines

## Project

`flows` is a durable, pregel-style **workflow engine**, distributed as a Go library with **zero runtime dependencies**. It is the engine layer only; AI agents at nodes are a later layer built on top.

This file is **how we build**, not **what we build**. The architecture, types, execution model, and API live in the design doc — see `docs/plans/2026-06-24-flow-engine-design.md`. Do **not** restate struct definitions, field names, or API specifics here; if the design changes, update the design doc, not this file.

## SOLID Principles (strictly enforced)

**Single Responsibility** — Every struct, function, and package has exactly one reason to change. If you can't describe what it does in one sentence without "and", split it.

**Open/Closed** — Extend behavior via interfaces and composition. Never modify a working type to add new behavior; add a new type or wrap it. A new implementation of an interface must require zero edits to its consumers.

**Liskov Substitution** — Every implementation of an interface must honor the full contract. If a concrete type can't satisfy a method without panicking, returning errors the caller doesn't expect, or silently doing less, redesign the interface.

**Interface Segregation** — Interfaces are small and focused. A caller should never depend on methods it doesn't use. Prefer many small interfaces over one large one; prefer optional, granular callbacks over fat observer interfaces.

**Dependency Inversion** — Depend on interfaces, not concrete types. High-level packages must not import low-level packages directly. Wire dependencies at the composition root (the caller's options/factory), never inside business logic.

## Security — First-Class, Not an Afterthought

**Validate at every boundary.** All external input (deserialized data, user-supplied values, anything loaded from a store) is untrusted until validated. Validate before it enters business logic, not inside it.

**Least privilege always.** Every component and goroutine gets only what it needs. Pass a narrow interface, never a whole config or god-object.

**No secrets in code.** No hardcoded tokens, passwords, keys, or connection strings — ever. Use environment variables or a secrets manager. Fail loudly on startup if required secrets are missing.

**Sanitize before use.** Never interpolate external data into queries, file paths, or log messages without sanitization. Use parameterized queries and `filepath.Clean`. Any externally-supplied identifier used as a storage key must be validated/escaped, never concatenated raw into a path or SQL string.

**Fail secure.** On error or ambiguity, deny by default. A failed validation aborts the operation rather than proceeding with partial/garbage data.

**Log events, not secrets.** Audit lifecycle and failures. Never log full payloads, credentials, or PII at info level.

## Dependencies

**Prefer stdlib.** Always reach for the Go standard library first. If a need can be met with stdlib — even with a bit more code — use stdlib. The engine core targets **zero runtime dependencies** (e.g. ID generation via `crypto/rand`, serialization via `encoding/json`).

**External packages require explicit user approval.** Before adding any external dependency, stop and ask. State what the package is, why stdlib is insufficient, and what it adds. Do not `go get` or edit `go.mod` without a clear "yes" in the current conversation.

**Amend this file when approved.** Once a package is approved, add it here so future sessions know it is sanctioned:

<!-- Approved external packages -->
- _Engine core (`pkg/flow`, `pkg/uuid`) and the stdlib service adapters (`pkg/registry`, `pkg/controlplane`, `pkg/ingress`): **zero runtime dependencies** — this is non-negotiable and verified by `go mod graph` at the repo root._
- Concrete storage and transport adapters are separate repositories or nested modules. They must not be imported by the core module; `flow/store` is the optional neutral-ledger adapter and distributed worker dispatch belongs in a future adapter.

Dev/tool-only (not linked into the library) may be added as approved:
- `honnef.co/go/tools/cmd/staticcheck` — extended static analysis (dev/tool only)
- `github.com/securego/gosec/v2` — security static analysis (dev/tool only)
- `golang.org/x/vuln/cmd/govulncheck` — official Go vulnerability scanner (dev/tool only)

## Secure Coding Patterns

**Randomness** — Use `crypto/rand` for ID generation and anything security-sensitive. Never use `math/rand`.

**Queries** — Any SQL-backed code must use parameterized queries via `database/sql`. Never format SQL with `fmt.Sprintf` or string concatenation.

**Context** — Every I/O call (store, network, file, external service) must accept and honor a `context.Context` with a deadline. Thread `ctx` through; never ignore it. No unbounded blocking.

**File paths** — Any path derived from external input must go through `filepath.Clean` and be verified to stay within an expected root before opening.

**Serialization is a trust boundary.** Decode into known concrete types; never `json.Unmarshal` into an `any` that then flows into business logic. Guard against unbounded sizes.

## Build & Testing Requirements

**Build** — `CGO_ENABLED=0 go build -trimpath ./...`. Never ship a binary without `-trimpath` (leaks local paths).

**Format** — All Go code must be `gofmt`-clean. Run `make fmt`/`make fmt-check`, which are scoped to this module's own package files via `GO_FILES`. Never reformat files belonging to the nested `store/` module or to worktree checkouts.

**Tests** — Always run with `-race`: `go test -race ./...`. A test that passes without `-race` but not with it is not passing. Concurrency is core here, so race coverage is non-negotiable.

**Table-driven tests (mandatory).** Every test function uses a `[]struct{ name string; ... }` table, and each subtest runs `t.Parallel()`. Every table must cover:
- Happy path (valid input → expected output)
- Boundary values (zero, empty, single element, max)
- Error cases (invalid input, missing required fields, type/identity mismatch)
- Domain edge cases specific to the unit under test

```go
func TestFoo(t *testing.T) {
    tests := []struct {
        name    string
        input   Bar
        want    Baz
        wantErr bool
    }{
        {name: "happy path", ...},
        {name: "empty input", ...},
        {name: "nil field returns error", ..., wantErr: true},
    }
    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            t.Parallel()
            got, err := Foo(tt.input)
            if (err != nil) != tt.wantErr {
                t.Fatalf("Foo() error = %v, wantErr %v", err, tt.wantErr)
            }
            if !tt.wantErr && got != tt.want {
                t.Errorf("Foo() = %v, want %v", got, tt.want)
            }
        })
    }
}
```

(The concrete behaviors to cover live in the design doc's testing section, not here.)

**Integration tests** — Tagged `//go:build integration`, in `*_integration_test.go`, for any code that crosses a process boundary (a durable store backend). Excluded from the default run; run with `go test -tags integration -race ./...`.

**Fuzzing** — Any function that parses external input gets a fuzz target, e.g. `go test -fuzz=FuzzXxx ./... -fuzztime=30s`.

## Code Rules

- **Strict typing everywhere.** Never use `any`/`interface{}` except at explicit serialization boundaries (JSON) and a single, validated type-erasure seam inside the engine. Immediately narrow to a concrete type; never pass `any` deeper into business logic. Prefer named types (`type UserID string`) over bare primitives when the value has domain meaning.
- All domain concepts are typed structs — no `map[string]interface{}` for domain data.
- Return errors explicitly; never swallow them with `_`.
- **All errors must be typed.** Define a concrete error struct for every distinct failure mode so callers can `errors.As` to inspect cause and context. Never return bare `errors.New`/`fmt.Errorf` from package-level APIs. Sentinel errors are permitted only for context-free leaf errors.
- Keep packages shallow and cohesive; avoid circular imports.
- **Write the interface first, then the implementation.**
- If a function exceeds ~30 lines, ask whether it violates SRP before adding more.
- The public API surface is generic and typed; internals may erase to `any` but must re-narrow and never leak `any` back to callers.
