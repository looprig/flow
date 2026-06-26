# flows — Durable Pregel Workflow Engine — Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build the `flows` durable, replayable Pregel-style workflow engine in Go — the zero-dependency engine core, the optional in-process HTTP service layers, and the optional NATS distribution/durability adapter — exactly as specified in `docs/plans/2026-06-24-flow-engine-design.md`.

**Architecture:** A BSP super-step coordinator owns a single graph-state `S`; pure typed `Task[I,O]`s are bound into a `Graph[S]` via per-binding selector/reducer adapters; runs are made durable by append-only checkpoints behind a `CheckpointStore` interface. The core (`pkg/flow` + `pkg/uuid`) is stdlib-only; `controlplane`/`registry`/`ingress` are stdlib service adapters; `pkg/nats` is an opt-in external-dep adapter behind the same interfaces, isolated in a nested Go module.

**Tech Stack:** Go (generics), stdlib only for the core (`crypto/rand`, `encoding/json`, `net/http`, `context`, `sync`); dev/tool-only static analysis (`staticcheck`, `gosec`, `govulncheck`); `nats.go` + embedded `nats-server` for the optional adapter only.

**Authoritative spec:** `docs/plans/2026-06-24-flow-engine-design.md` (cited below as `§N`). The design doc is the source of truth for every struct/field/signature — this plan references sections rather than restating them, per CLAUDE.md. If a conflict arises, update the design doc, not this plan.

---

## How to use this plan

- **Read the cited design section before each milestone.** The design doc already contains most type definitions and prose contracts; this plan tells you the *order*, the *tests*, and the *seams*.
- **TDD rhythm (every task).** Each task follows the same five beats — they are written out for the first few tasks and abbreviated thereafter as **`[TDD: test → fail → impl → pass → commit]`**. The beats are always:
  1. Write the failing test(s).
  2. Run the test; confirm it fails for the *expected* reason (compile error / assertion), not an unrelated one.
  3. Write the minimal implementation.
  4. Run the test; confirm it passes.
  5. Commit (message given per task).
- **Every test is table-driven, every subtest calls `t.Parallel()`, every run uses `-race`** (CLAUDE.md). Each table must cover: happy path, boundary (zero/empty/single/max), error cases, and the domain edges listed in the task.
- **Workspace & vendoring:** A parent `go.work` at `/Users/ipotter/code/go.work` does not list `./flows`, so **all `go`/`gofmt` commands in this repo must run with `GOWORK=off`** (flows is a standalone module). The repo is also **vendored** (looprig pattern, Task 0.2): the `Makefile` exports `GOWORK := off` and `GOFLAGS := -mod=vendor`, so prefer the `make` targets. For ad-hoc/targeted commands, prefix with `GOWORK=off GOFLAGS=-mod=vendor` (e.g. running one test).
- **Standard verification** (before every commit): run **`make check`** (= `fmt-check` + `vet` + `staticcheck` + `gosec` + `go mod verify` + `govulncheck` + `build` + `race-test`). For a targeted test loop: `GOWORK=off GOFLAGS=-mod=vendor go test -race ./pkg/<pkg>/ -run TestName -v`. Milestones that add fuzz/integration name the extra command explicitly.
- **Required skills while executing:** `superpowers:test-driven-development` (every task), `superpowers:systematic-debugging` (any failure), `superpowers:requesting-code-review` (end of each phase), `superpowers:verification-before-completion` (before any "done" claim).
- **Module path:** `github.com/ciram-co/flow`; engine package import path `github.com/ciram-co/flow/pkg/flow` (§1). All paths below are relative to repo root `/Users/ipotter/code/flows`.

---

## Phase 0 — Project scaffold & tooling

**Design refs:** §14 (package layout), §18.4 (nested module), CLAUDE.md (build/format/test/deps).
**Dependencies:** none. **Outcome:** a building, formatting, race-clean empty module with the directory skeleton and the dev-tool static-analysis chain wired.

> Note: the environment reports this is **not** a git repository yet. Task 0.1 initializes it so the per-task commits in this plan work.

### Task 0.1: Initialize repo, module, and directory skeleton

**Files:**
- Create: `go.mod`, `.gitignore`
- Create (empty package dirs with a `doc.go` each): `pkg/uuid/`, `pkg/flow/`, `pkg/controlplane/`, `pkg/registry/`, `pkg/ingress/` (all service packages live **under `pkg/`** per §14 and the §18.6 import paths — e.g. `github.com/ciram-co/flow/pkg/controlplane`)

**Steps:**
1. `git init` (repo does not exist yet); confirm `CLAUDE.md`, `AGENTS.md`, `docs/` are present and untracked.
2. Create `go.mod`: `module github.com/ciram-co/flow` with the current Go version (`go 1.23` or newer — generics + `min`/`max` builtins assumed).
3. Create `.gitignore` (binaries, `*.test`, coverage out, `*.prof`, editor dirs).
4. Add a one-line `doc.go` package clause to each new dir so `go build ./...` succeeds on empty packages: `pkg/uuid/doc.go` → `// Package uuid is a stdlib-only v4 UUID. See design §13.`, etc.
5. Run `CGO_ENABLED=0 go build -trimpath ./...` and `gofmt -l .` — both clean.

**Commit:** `chore: initialize flow module, package skeleton, and gitignore`

### Task 0.2: Dev-tool static-analysis chain (mirror looprig's pattern)

> **As built (decided during execution):** mirror the sibling project `looprig`'s dev-tooling notion — Go's native **`tool` directive** (Go 1.24+) instead of a `tools.go` blank-import file, **`go tool <name>`** invocation, and a committed **vendored** tree (`-mod=vendor`) for offline/reproducible/auditable builds. This keeps the library's *runtime* posture at zero third-party deps while pinning the dev tools.

**Files:**
- Modify: `go.mod` — add the `tool ( … )` directive (via `go get -tool`); the three tools + their transitive closure land as `// indirect` requires.
- Create: `vendor/` — committed (`go mod vendor`); contains dev-tool source only. `.gitignore` gets a `!/vendor/**` negation so global ignores can't drop vendored files.
- Create: `Makefile` mirroring looprig — `export GOWORK := off` + `export GOFLAGS := -mod=vendor`, a `GO_DIRS := $(shell GOWORK=off go list -f '{{.Dir}}' ./...)` line (the inline `GOWORK=off` is REQUIRED: `make`'s `export` does not reach parse-time `$(shell)`, and flows is not in the parent go.work), and targets `test`/`fmt`/`fmt-check`/`lint`/`vuln`/`secure`/`fuzz`/`build`/`check`. Tools invoked via `go tool staticcheck`/`go tool gosec`/`go tool govulncheck`; `gosec` scoped to `$(GO_DIRS)` (not module-aware; would otherwise descend into the future nested `pkg/nats` module).

**Steps:**
1. `GOWORK=off go get -tool honnef.co/go/tools/cmd/staticcheck github.com/securego/gosec/v2/cmd/gosec golang.org/x/vuln/cmd/govulncheck` — these are **dev/tool-only** and already sanctioned in CLAUDE.md (no new approval). Then `GOWORK=off go mod tidy`.
2. `GOWORK=off go mod vendor`; confirm the library still imports **zero third-party runtime packages** (`GOWORK=off GOFLAGS=-mod=vendor go list -deps ./pkg/... | grep -E '\.(com|org|net|io|dev)/' | grep -v ciram-co/flow` is empty).
3. Wire `make check` = `secure` (`fmt-check` + `vet` + `staticcheck` + `gosec`) + `vuln` (`go mod verify` + `govulncheck`) + `build` + `test` (`-race`).
4. Run `make check` on the skeleton — passes clean.

**Commit:** `chore: wire dev tooling via go tool directive + vendoring (looprig pattern)`

> Note: `go mod tidy` bumps the `go` directive to `1.25.0` (a tool's transitive minimum), and `vendor/` is ~37 MB / ~2356 files. Both are accepted consequences of the looprig parity choice; vendoring is reversible (`git rm -r vendor`, drop `-mod=vendor`) if a leaner repo is later preferred.

**Phase 0 gate:** `make check` is green. `go list -deps ./pkg/...` confirms zero third-party **runtime** dependencies (tool deps live under the `tool` directive / `vendor/`, never imported by the library).

---

## Phase 1 — UUID foundation (`pkg/uuid`)

**Design refs:** §13. **Dependencies:** Phase 0. **Outcome:** a self-contained, stdlib-only v4 UUID with typed errors and a fuzz target. Everything downstream depends on this.

### Task 1.1: `UUID` type, `IsZero`, and text marshaling

**Files:**
- Create: `pkg/uuid/uuid.go`
- Test: `pkg/uuid/uuid_test.go`

**Step 1 — failing tests** (table-driven, `t.Parallel()`):
- `TestUUIDString`: round-trips a known 16-byte array to canonical `8-4-4-4-12` lowercase hex; covers all-zero, all-`0xff`, and a fixed mid-value.
- `TestUUIDIsZero`: zero array → true; any non-zero → false.
- `TestUUIDMarshalUnmarshalText`: `MarshalText` then `UnmarshalText` is identity for valid inputs; covers zero and a fixed value.

**Step 2:** `go test ./pkg/uuid/ -run TestUUID -v` → FAIL (undefined `UUID`).

**Step 3 — implement:** `type UUID [16]byte`; `String() string`; `IsZero() bool`; `MarshalText() ([]byte, error)`; `UnmarshalText([]byte) error`. Canonical hex formatting via `encoding/hex` + manual hyphen placement (no external dep).

**Step 4:** tests PASS.

**Step 5 — commit:** `feat(uuid): add UUID type with text marshaling and IsZero`

### Task 1.2: `New()` v4 via `crypto/rand` + `GenerateError`

**Files:** modify `pkg/uuid/uuid.go`; extend `uuid_test.go`.

**[TDD: test → fail → impl → pass → commit]**
- **Tests:** `TestNew` asserts version nibble == 4 (byte 6 high nibble) and variant bits (byte 8 top two bits == `10`); two successive `New()` differ; not `IsZero`. Use a table over N iterations. (Do **not** assert specific bytes — it's random.)
- **Impl:** `New() (UUID, error)` reads 16 bytes from `crypto/rand.Reader` (never `math/rand` — CLAUDE.md), sets version/variant bits. On read failure return typed `GenerateError{Err error}` (implements `error`, `Unwrap`).
- **Commit:** `feat(uuid): add crypto/rand v4 New() with typed GenerateError`

### Task 1.3: `Parse`/`MustParse` + `ParseError` + fuzz

**Files:** modify `pkg/uuid/uuid.go`; extend `uuid_test.go`; add fuzz target.

**[TDD: test → fail → impl → pass → commit]**
- **Tests (table):** valid canonical string parses; uppercase accepted; wrong length → `ParseError`; bad hex → `ParseError`; missing hyphens → `ParseError`; empty → `ParseError`. Assert `errors.As(err, &uuid.ParseError{})`.
- **Fuzz:** `FuzzParse` — seed with valid + malformed; property: `Parse` never panics, and any successful parse re-`String()`s to a normalized form that re-parses equal.
- **Impl:** `Parse(string) (UUID, error)` (validate length/hyphens/hex; typed `ParseError{Input string, Err error}`); `MustParse(string) UUID` (panics on error — for pinned consts only, §3).
- **Verify:** `go test ./pkg/uuid/ -race` and `go test ./pkg/uuid/ -fuzz=FuzzParse -fuzztime=30s`.
- **Commit:** `feat(uuid): add Parse/MustParse with ParseError and FuzzParse`

**Phase 1 gate:** `pkg/uuid` is complete, race-clean, fuzz-clean. Code review against §13.

---

## Phase 2 — Core types (`pkg/flow`)

**Design refs:** §3 (ids), §4.1 (state/instrumentation), §5 (task), §12.4 (errors).
**Dependencies:** Phase 1. **Outcome:** all leaf domain types and the full typed-error catalogue, no behavior yet.

### Task 2.1: Identifier types (`ids.go`)

**Files:** Create `pkg/flow/ids.go`, `pkg/flow/ids_test.go`.

**[TDD]**
- **Tests (table):** each named type (`GraphID`, `VertexID`, `GraphRunID`, `VertexRunID`) `String`/`MarshalText`/`UnmarshalText` delegates to `uuid.UUID` and round-trips; `StepID` is a plain `int` and formats as decimal. Boundary: zero IDs; a pinned `MustParse` const (the §3 example). Error: malformed text → the underlying `uuid.ParseError`.
- **Impl:** define the five named types from §3 (`StepID int`; the rest wrap `uuid.UUID`), each with `String`/`MarshalText`/`UnmarshalText` delegating to the embedded/converted `uuid.UUID`. Add `func New<Thing>ID() (<Thing>ID, error)` only for the runtime-minted ones (`GraphRunID`, `VertexRunID`) wrapping `uuid.New()`.
- **Commit:** `feat(flow): add typed identifiers delegating to uuid (§3)`

### Task 2.2: Status enums + instrumentation records (`state.go`)

**Files:** Create `pkg/flow/state.go`, `pkg/flow/state_test.go`.

**[TDD]**
- **Tests (table):** `RunStatus`/`VertexStatus` constant values match §4.1 ordering (guard against accidental reordering — assert `RunRunning==0` etc.); zero-value `time.Time` means "not reached"; JSON round-trip of `GraphRunState` and `VertexState` preserves all fields. Domain edge: a `GraphRunState` is **framework-owned, not user-extensible** — confirm fields exactly match §4.1.
- **Impl:** define `RunStatus`, `VertexStatus`, `GraphRunState`, `VertexState`, `RunInfo` exactly per §4.1. No methods beyond what §4.1 shows yet.
- **Commit:** `feat(flow): add run/vertex status enums and instrumentation records (§4.1)`

### Task 2.3: `IdempotencyKey` (`state.go`)

**Files:** modify `state.go`, `state_test.go`.

**[TDD]**
- **Tests (table):** `RunInfo.IdempotencyKey()` is **stable across `Attempt`/`VertexRunID`** (two `RunInfo`s differing only in those fields → equal keys); changes when any of GraphID/GraphRunID/Step/VertexID changes; exact string format matches §4.1. This is the contract that makes recovery idempotent (§10.1).
- **Impl:** `IdempotencyKey` named type + `RunInfo.IdempotencyKey()` exactly per §4.1.
- **Commit:** `feat(flow): add stable IdempotencyKey for side-effecting tasks (§4.1)`

### Task 2.4: Task model (`task.go`)

**Files:** Create `pkg/flow/task.go`, `pkg/flow/task_test.go`.

**[TDD]**
- **Tests (table):** `NewFuncTask(fn).Execute(ctx,in)` forwards args and returns `fn`'s `(O,error)`; happy path; error passthrough; the **reusability** property — the *same* `FuncTask` value used twice with different inputs behaves identically and holds no per-use state. Boundary: a task returning the zero `O`.
- **Impl:** `TaskFunc[I,O]`, `Task[I,O]` interface, `FuncTask[I,O]`, `NewFuncTask`, `Execute` exactly per §5. Write the **interface first**, then `FuncTask` (CLAUDE.md).
- **Commit:** `feat(flow): add Task[I,O] interface and FuncTask (§5)`

### Task 2.5: Typed error catalogue (`errors.go`)

**Files:** Create `pkg/flow/errors.go`, `pkg/flow/errors_test.go`.

**[TDD]**
- **Tests (table):** for **every** typed error in §12.4, assert: it implements `error`, its `Error()` includes its salient fields, and `errors.As` recovers the concrete type from a wrapped value; where it wraps a cause, `errors.Unwrap` returns it. Group by category (build / runtime / durability). Include `VertexError{VertexID, VertexRunID, Attempt, Err}` `Unwrap`; `ResumeTerminalError{Status}`; `GraphRunExistsError`; `RevisionConflictError`; `GraphVersionMismatchError`.
- **Impl:** define every typed error from §12.4 as a concrete struct (no bare `errors.New`/`fmt.Errorf` from package APIs — CLAUDE.md). Sentinel errors only for context-free leaf cases. Add the **internal interrupt signal** as an internal typed error detected via `errors.As` (used by §13/§6).
- **Commit:** `feat(flow): add full typed-error catalogue (§12.4)`

**Phase 2 gate:** all leaf types + errors compile, round-trip, and are race-clean. Code review against §3/§4.1/§5/§12.4.

---

## Phase 3 — Build API (`pkg/flow`)

**Design refs:** §6 (vertex binding), §7 (build API), §8 (compile/validation/versioning).
**Dependencies:** Phase 2. **Outcome:** a caller can construct, wire, and compile a graph; `Compile` validates and computes `GraphVersion`. No execution yet.

### Task 3.1: Vertex binding internals + `AddVertex` (`vertex.go`)

**Files:** Create `pkg/flow/vertex.go`, `pkg/flow/vertex_test.go`.

**Design contract:** §6.1 (signatures), §6.2 (clone-and-commit shape), §6.3 (options). The **type-erasure seam** lives here: `AddVertex[I,O,S]` is generic, but the graph stores vertices as a non-generic internal `vertex[S]` that closes over `I`/`O` and exposes `any`-typed `runSelector(stepBase S) (any, error)`, `execute(ctx, in any) (any, error)`, `applyReducer(s *S, out any) error`. This is the single validated erasure seam (CLAUDE.md) — narrow back to concrete `I`/`O` *inside* the closures, never leak `any` to callers.

**[TDD]**
- **Tests (table):** `Selector`/`Reducer` are invoked with the right values; the erased `vertex[S]` round-trips a typed task end-to-end (selector→execute→reducer) over a tiny `S`; a reducer error is surfaced; **clone-and-commit**: a reducer that mutates `*S` then returns an error must NOT mutate the caller's `S` (the §6.2 atomicity property — test at this unit level by driving the erased reducer against a clone helper stub). `VertexOption`s accumulate onto a `vertexConfig[S]`.
- **Impl:** `Selector[S,I]`, `Reducer[S,O]`, internal `vertex[S]`, `vertexConfig[S]`, `VertexOption[S]`, and `AddVertex[I,O,S]` per §6.1/§6.3. `AddVertex` records the binding into the graph (Task 3.2 provides `Graph[S]`; if needed, stub a minimal `Graph[S]` here and flesh out next, or reorder so 3.2 lands first — prefer landing `Graph[S]` skeleton first).
- **Commit:** `feat(flow): add vertex binding, selector/reducer, AddVertex erasure seam (§6)`

### Task 3.2: `Graph[S]`, edges, conditions (`graph.go`)

**Files:** Create `pkg/flow/graph.go`, `pkg/flow/graph_test.go`.

**[TDD]**
- **Tests (table):** `NewGraph` with/without `WithVersion`; `AddEdge`/`AddConditionalEdge` record endpoints; `Condition[S]` stores `Targets` + `Pick`; duplicate `VertexID` in `AddVertex` is rejected at add-time or deferred to compile (decide and test — §8 says Compile rejects duplicates, so add-time may just record). Boundary: empty graph; single vertex; self-edge (cycle) allowed.
- **Impl:** `Graph[S]`, `GraphOption`, `WithVersion(n uint64)`, `AddEdge`, `AddConditionalEdge`, `Condition[S]` per §7 and §8.1 (userVersion default 0).
- **Commit:** `feat(flow): add Graph[S], edges, conditional edges (§7)`

### Task 3.3: Compile validation (`compile.go`)

**Files:** Create `pkg/flow/compile.go`, `pkg/flow/compile_test.go`.

**[TDD — this task gets one subtest per validation rule]**
- **Tests (table, one row per §8 rule):** duplicate `VertexID` → `DuplicateVertexError`; unknown edge/condition/error-route endpoint → `UnknownVertexError`; missing `entry`/`finish` → `MissingEntryError`; unreachable vertex from `entry` → `UnreachableVertexError`; `finish` unreachable → error; a vertex with **both** an unconditional out-edge and a conditional edge → `AmbiguousRoutingError`; `Condition.Targets` empty or referencing unknown vertex → error; a valid graph compiles to a non-nil `*Runner[S]`. Boundary: single-vertex graph where `entry==finish`.
- **Impl:** `Compile(entry, finish, opts...)` running every §8 check, returning the first typed error; on success build a `*Runner[S]` skeleton (fields per §9: compiled graph, store, hooks, concurrency, maxSteps, granularity — store wired in Phase 4). Reachability via BFS from `entry` over static + conditional `Targets` edges.
- **Commit:** `feat(flow): add Compile with full validation (§8)`

### Task 3.4: `GraphVersion` fingerprint + `CompileOption`s (`compile.go`)

**Files:** modify `compile.go`, `compile_test.go`.

**[TDD]**
- **Tests (table):** the fingerprint is **deterministic** (same graph → same hash across compiles); **topology-sensitive** — adding a vertex, rewiring an edge, or changing a `Condition.Targets` set changes the hash (§8.1); **behavior-insensitive** — swapping a task/selector/reducer closure does NOT change the hash; `WithVersion(n)` bump changes the `:userVersion` suffix; canonicalization is order-independent (declaring edges in a different order yields the same hash).
- **Impl:** `GraphVersion = sha256(canonical(...)) + ":" + userVersion` exactly per §8.1 (sorted VertexIDs, sorted edges, sorted conditional targets, sorted error-routes, entry, finish). Add `WithStore` as a `CompileOption` (interface-typed; default wired in Phase 4). `Runner.GraphID()`/`GraphVersion()` accessors.
- **Commit:** `feat(flow): add deterministic GraphVersion fingerprint (§8.1)`

**Phase 3 gate:** a graph compiles or fails with the precise typed error; versioning behaves per §8.1. Code review against §6/§7/§8.

---

## Phase 4 — Durability primitives (`pkg/flow`)

**Design refs:** §10.1 (checkpoint), §10.2 (store), §6.2 (clone). **Dependencies:** Phase 3. **Outcome:** clone codec, checkpoint serialization, and the append-only store with compare-and-append + structural immutability — all testable without the engine.

### Task 4.1: Clone codec (`clone.go`)

**Files:** Create `pkg/flow/clone.go`, `pkg/flow/clone_test.go`.

**[TDD]**
- **Tests (table):** JSON round-trip deep-clone of an `S` containing nested maps/slices/pointers produces an **independent** copy (mutating the clone leaves the original untouched, and vice versa); a non-serializable `S` (e.g. contains a channel/func) returns a typed error **early** (§10.2 "surfaces non-serializable states early"); zero-value `S`; large-but-bounded `S`.
- **Impl:** `clone[S](s S) (S, error)` via `json.Marshal`→`json.Unmarshal` into a fresh `S` (the §6.2 "codec deep-clone"). Guard against unbounded input is N/A here (in-memory), but keep the decode into the **concrete `S`**, never `any` (CLAUDE.md serialization-boundary rule).
- **Commit:** `feat(flow): add JSON deep-clone codec for state (§6.2)`

### Task 4.2: Checkpoint types + serialization (`checkpoint.go`)

**Files:** Create `pkg/flow/checkpoint.go`, `pkg/flow/checkpoint_test.go`.

**[TDD]**
- **Tests (table):** `Checkpoint` and every sub-record (`RouteRecord`, `InterruptRecord`, `HaltRecord`, `StepPhase`, `HaltKind`) **round-trip** through JSON with full equality (the §15 "round-trip equality" requirement); `StepBase`/`State` are carried as `json.RawMessage` and survive untouched; `Interrupts` and `Halt` are **mutually exclusive** at the type level (document + test that valid checkpoints set at most one); `Phase`/enum constants match §10.1 ordering.
- **Impl:** define `Checkpoint`, `StepPhase`, `RouteRecord`, `InterruptRecord`, `HaltKind`, `HaltRecord` exactly per §10.1.
- **Commit:** `feat(flow): add Checkpoint and sub-records with JSON round-trip (§10.1)`

### Task 4.3: `FuzzCheckpointRoundTrip` (`checkpoint_test.go`)

**[TDD]**
- **Fuzz:** seed with representative checkpoints (each `Phase`, with/without `Interrupts`, with `Halt`, with `Routes`); property: marshal→unmarshal→marshal is byte-stable and never panics; invalid bytes decode to an error, not a panic (§15 Fuzz).
- **Verify:** `go test ./pkg/flow/ -fuzz=FuzzCheckpointRoundTrip -fuzztime=30s`.
- **Commit:** `test(flow): add FuzzCheckpointRoundTrip (§15)`

### Task 4.4: `CheckpointStore` interface + `MemStore` (`store.go`)

**Files:** Create `pkg/flow/store.go`, `pkg/flow/store_test.go`.

**Design contract:** §10.2 — append-only; `Append` is **compare-and-append** (accept iff `cp.Run.Revision == latest+1`, else `RevisionConflictError`); `(GraphRunID, Revision)` unique; `Latest` = highest revision; `History` ordered; **structural immutability** (store persists independent encoded copies; `Latest`/`History` return freshly-decoded values).

**[TDD]**
- **Write the interface first**, then `MemStore`.
- **Tests (table):** append revisions 0,1,2 → `Latest` returns rev 2, `History` returns ordered [0,1,2]; appending a non-`latest+1` revision → `RevisionConflictError`; **concurrent-resume detection** — two goroutines that both read `Latest` and append the same next revision → exactly one succeeds, other gets `RevisionConflictError`, `History` shows no fork (run under `-race`); **immutability** — mutating a checkpoint returned by `Latest`/`History`, or one passed to `Append`, does not corrupt stored state; `Latest`/`History` of an unknown run → `CheckpointNotFoundError`; empty-run boundary.
- **Impl:** `CheckpointStore` interface (`Append`/`Latest`/`History`) + `MemStore` (`map[GraphRunID][][]byte` of encoded checkpoints + `sync.RWMutex`), encoding on `Append` and decoding fresh on read (round-tripping through the codec). `NewMemStore()`.
- **Verify:** `go test ./pkg/flow/ -race -run TestMemStore`.
- **Commit:** `feat(flow): add CheckpointStore interface and MemStore with compare-and-append (§10.2)`

### Task 4.5: Wire the default store into `Runner`/`Compile`

**Files:** modify `compile.go`, `runner.go` (create skeleton), tests.

**[TDD]**
- **Tests:** `Compile` with no `WithStore` uses an internal `MemStore`; `Compile(WithStore(s))` uses `s`; **all** of `Run`/`Resume`/`Status`/`Get` resolve to the *same* store (no per-run override — §9). (Behavioral assertions land as those methods arrive; here assert the runner holds exactly one store.)
- **Impl:** finalize `Runner[S]` store field + `WithStore` `CompileOption` per §9.
- **Commit:** `feat(flow): wire single CheckpointStore into Runner at Compile (§9)`

**Phase 4 gate:** durability primitives are correct and race/fuzz-clean. Code review against §10.1/§10.2.

---

## Phase 5 — Engine support types (`pkg/flow`)

**Design refs:** §11 (hooks), §10.3 (interrupt/resume API), §12.2/§12.5 (retry & panic). **Dependencies:** Phase 4. **Outcome:** hooks dispatch, the interrupt/resume context API, and the retry/panic-recovery wrapper — the pieces the coordinator composes.

### Task 5.1: Hooks + panic-safe dispatch (`hooks.go`)

**Files:** Create `pkg/flow/hooks.go`, `pkg/flow/hooks_test.go`.

**[TDD]**
- **Tests (table):** each `Hooks` callback fires with the right args; **nil callbacks are skipped**; **multiple hook sets accumulate** (`WithHooks` repeatable — all fire); a **panicking hook is recovered and discarded** and does NOT affect control flow (§12.5); ordering across accumulated sets is stable.
- **Impl:** `Hooks` struct per §11; an internal dispatcher that fan-outs to all registered sets, nil-guards each, and wraps each call in panic recovery (logged-and-ignored). `WithHooks` `RunOption`.
- **Commit:** `feat(flow): add observational Hooks with panic-safe dispatch (§11, §12.5)`

### Task 5.2: Interrupt/resume context API (`interrupt.go`)

**Files:** Create `pkg/flow/interrupt.go`, `pkg/flow/interrupt_test.go`.

**Design contract:** §10.3, §4.2. `Interrupt`/`StatefulInterrupt` return the **internal interrupt signal error** (from Task 2.5) carrying `info`/`continuation` as `any` (serialization boundary). `ResumePayload[T]`/`InterruptState[T]` and `Info`/`Self` read typed values from `ctx`.

**[TDD]**
- **Tests (table):** `Interrupt(ctx, info)` returns an error recognizable via `errors.As` as the interrupt signal, carrying `info`; `StatefulInterrupt` additionally carries `continuation`; `ResumePayload[T](ctx)` returns the injected payload typed (and `ok=false` when absent / wrong type); `InterruptState[T](ctx)` returns the continuation; `Info(ctx)`/`Self(ctx)` return the coordinator-injected `RunInfo`/`VertexState` (and `false` when not in a vertex ctx). Boundary: nil info; type mismatch on `ResumePayload[T]`.
- **Impl:** the four functions + `Interrupt`/`Halt`/`InterruptKind` public structs per §10.3; private ctx keys for payload/continuation/RunInfo/VertexState. Type-erase only at the `any` boundary; `ResumePayload[T]` does the checked narrowing.
- **Commit:** `feat(flow): add interrupt/resume context API (§10.3, §4.2)`

### Task 5.3: Retry policy + panic-recovery wrapper (`retry.go`)

**Files:** Create `pkg/flow/retry.go`, `pkg/flow/retry_test.go`.

**[TDD]**
- **Tests (table):** `RetryPolicy{MaxAttempts,Backoff,Retryable}` retries up to `MaxAttempts`, bumping `Attempt`, honoring `Backoff` (inject a fake/zero backoff to keep tests fast) and `Retryable` predicate (default: any non-interrupt error); an **interrupt signal is never retried** (passes straight through); a **panic in the task** is recovered into `VertexError`; a **panic in `Backoff`/`Retryable`** is recovered and treated as task failure (§12.5); success on attempt 2 stops retrying; `ctx` cancellation aborts retry loop.
- **Impl:** `RetryPolicy` per §12.2; an internal `runWithRetry` that wraps `task.Execute`, recovers panics → `VertexError`, detects the interrupt signal via `errors.As` and short-circuits, and applies the policy. Honor `ctx` between attempts.
- **Commit:** `feat(flow): add RetryPolicy and panic-recovering execution wrapper (§12.2, §12.5)`

**Phase 5 gate:** support types are correct and panic-safe. Code review against §10.3/§11/§12.

---

## Phase 6 — The coordinator / BSP engine (`pkg/flow/engine.go` + `runner.go`)

**Design refs:** §9 (runner & execution — read in full), §2.1 (BSP), §6.2 (clone-and-commit), §9.2–§9.8, §10.4 (resume validation), §12.2 (error policy), §18.2 (control surface).
**Dependencies:** Phase 5. **Outcome:** `Run`/`Resume`/control surface — the heart of the engine. Built in slices so each behavior is independently TDD'd. The coordinator is the **sole owner/writer of `S`** (§4.1) — no mutex on `S`, race-clean by construction.

> Build order is deliberate: prove the happy-path loop first, then layer routing, reduction semantics, error policies, interrupts, granularity, and finally resume + control. Each task ends race-clean.

### Task 6.1: `Run` happy path — single vertex & linear chain (seed → execute → reduce → complete)

**Files:** Create `pkg/flow/engine.go`, `pkg/flow/runner.go`; tests in `pkg/flow/engine_test.go`.

**Design contract:** §9.2 steps 1–4 & 6, restricted to: no conditions, no errors, no interrupts; `PerVertex` granularity. Seed checkpoint at `Revision 0`, `Phase: StepRouted`, `Frontier:[entry]` (§9.2 step 1, §15 "seed checkpoint"). Completion appends a final `RunCompleted` checkpoint (§9.2 step 6, §15 "completion durability").

**[TDD]**
- **Tests (table):** single vertex `entry==finish` runs once → `Result{Completed}`, final `State` is the reducer output; a linear chain `a→b→finish` runs each once in order, `S` threads through; **seed checkpoint** present at rev 0 (`StepRouted`, frontier `[entry]`); **completion checkpoint** present and `Latest` shows `Completed`; `Result.Run.Status` is never `RunRunning` on return (§9); timestamps (`CreatedAt`/`StartedAt`/`CompletedAt`) set; `OnRunStart`/`OnRunFinish` bracket the run (§15 hooks). `Run` mints a fresh `GraphRunID`; `WithGraphRunID(id)` where `id` already has history → `GraphRunExistsError` (§9).
- **Impl:** the coordinator struct (owns `GraphRunID`, `S`, frontier, `GraphRunState`); `Run(ctx, in, opts...)`; `Result[S]` + `RunOption`s (`WithHooks`/`WithConcurrency`/`WithMaxSteps`/`WithGraphRunID`/`WithCheckpointEvery`). Super-step loop limited to the linear case: freeze `StepBase`, run frontier serially-or-parallel (parallelism proven in 6.3), reduce in `VertexID` order via clone-and-commit, checkpoint, route along static single edges, terminate when `finish` executed and frontier drains.
- **Commit:** `feat(flow): add coordinator Run happy path with seed+completion checkpoints (§9.2)`

### Task 6.2: Static routing — edges, fan-out, fan-in, dedup, cycles + `WithMaxSteps`

**Files:** modify `engine.go`; extend tests.

**Design contract:** §9.2 step 5 (static `AddEdge`), §9.5 (`HaltMaxSteps`), §9.6 (fan-in dedup), §2 (cycles allowed).

**[TDD]**
- **Tests (table):** fan-out (one vertex with edges to two) activates both next step; fan-in (two edges into one) → vertex appears **once** (deduped), runs once over merged `S` (§9.6); a **cycle** without `finish` runs until `WithMaxSteps` → `Result.Halt{HaltMaxSteps}`, `Phase: StepHalted`, `Errored`, `OnHalt` fires (§9.5/§9.8); `Routes` records each static decision with `Conditional=false` and correct `(From,To)` (§9.2 step 5, §15 branch attribution).
- **Impl:** static successor computation, frontier dedup, `Routes` recording, `WithMaxSteps` budget + `HaltMaxSteps` halt path (append `StepHalted` final checkpoint, set `Result.Halt`).
- **Commit:** `feat(flow): add static routing, fan-in dedup, cycle/max-steps halt (§9.5, §9.6)`

### Task 6.3: Real parallelism within a step + snapshot safety (`-race`)

**Files:** modify `engine.go`; extend tests.

**Design contract:** §9.2 step 3 (one goroutine per frontier vertex, bounded by `WithConcurrency` default `GOMAXPROCS`, barrier), §2.1 (frozen reads/deferred writes), §4.2 (snapshot safety).

**[TDD — must run under `-race`]**
- **Tests (table):** a step with N parallel vertices over an `S` containing **maps/slices/pointers** is race-clean; all read the **same immutable `StepBase`** and a sibling reducer does not mutate `StepBase` (§15 snapshot safety); `WithConcurrency(1)` serializes; default bounds to `GOMAXPROCS`; barrier waits for all before reducing.
- **Impl:** bounded worker pool (semaphore channel) for the frontier; barrier; selectors computed from `StepBase` before launch; reducers applied after barrier in `VertexID` order. Confirm no mutex on `S` (single-writer coordinator).
- **Commit:** `feat(flow): add bounded parallel step execution with snapshot safety (§9.2, §2.1)`

### Task 6.4: Reduce semantics — clone-and-commit atomicity & same-field ordering

**Files:** modify `engine.go`; extend tests.

**Design contract:** §6.2 (commit only on nil error), §9.4 (same-field defined overwrite in `VertexID` order).

**[TDD]**
- **Tests (table):** a reducer that mutates then returns an error leaves committed `S` **unchanged** (clone discarded — §6.2, §15 reducer atomicity), and the vertex follows its error policy; two reducers writing the same field in one step → **last in `VertexID` order wins** (defined, run-stable — §9.4); disjoint-field parallel reducers all apply.
- **Impl:** the clone-and-commit block exactly per §6.2 around each reducer, ordered by `VertexID`.
- **Commit:** `feat(flow): add clone-and-commit reduce with defined same-field ordering (§6.2, §9.4)`

### Task 6.5: Conditional routing + routing halts

**Files:** modify `engine.go`; extend tests.

**Design contract:** §7 (`Condition.Pick`), §9.5/§9.8 run-level halts: `HaltUndeclaredTarget` (empty / out-of-`Targets`), `HaltCondition` (`Pick` error/panic), `HaltDeadEnd` (drain without `finish`).

**[TDD]**
- **Tests (table):** `Pick` returning one target routes there; returning **multiple** is a fan-out; `Pick` reads `S_{N+1}` (post-reduce committed state) (§9.2 step 5); empty set or target outside `Targets` → `Halt{HaltUndeclaredTarget}`; `Pick` returning error or panicking → `Halt{HaltCondition}` (panic recovered — §12.5); frontier drains without `finish` → `Halt{HaltDeadEnd}`; each halt sets `Result.Halt`, leaves `Interrupts` empty (**mutually exclusive**), `Phase: StepHalted`, fires `OnHalt`; `Routes` records conditional picks with `Conditional=true` (§15 branch attribution).
- **Impl:** conditional successor computation with target validation; the three routing halts; `OnEdge` per traversed edge incl. each conditional pick (§11).
- **Commit:** `feat(flow): add conditional routing and run-level routing halts (§9.5, §9.8)`

### Task 6.6: Per-vertex error policy — Retry / Route / Pause + Timeout

**Files:** modify `engine.go`; extend tests.

**Design contract:** §12.2 (policy table, composition: retry → then route/pause), §6.3 (options), `WithTimeout` (cooperative deadline → `VertexError` wrapping `context.DeadlineExceeded`, retryable), §12.2 record-reducer-failure → pause as `Errored`.

**[TDD]**
- **Tests (table):** `WithRetry` retries then succeeds; retry exhaustion → falls through to `Route`/`Pause`; `WithErrorRoute(handler, record)` clone-and-commits the **record reducer** to fold the error and activates the handler next step (step is **routable**, not paused — §9.2/§9.8); default **Pause** → `Interrupted, Kind: Errored, Cause` with a checkpoint (§12.3); `WithTimeout` expiry → `VertexError` wrapping `context.DeadlineExceeded`, retried by default then policy; a **record-reducer that errors/panics** → vertex pauses as `Errored`, no recursive Route, `S` unchanged (§12.2).
- **Impl:** classify each vertex outcome (success / Route-error / Pause-error / interrupt) after retry; apply the matching terminal effect + checkpoint per §9.2 step 4; `WithTimeout` sets a per-vertex `ctx` deadline.
- **Commit:** `feat(flow): add per-vertex error policy (retry/route/pause) and timeout (§12.2)`

### Task 6.7: Per-vertex interrupts — pause, multiple interrupts, siblings not cancelled

**Files:** modify `engine.go`; extend tests.

**Design contract:** §9.2 step 4–5 (pause path), §9.7 (siblings run to completion; plural interrupts), §10.1 (paused checkpoint already carries its `InterruptRecord`/continuation).

**[TDD]**
- **Tests (table):** a `flow.Interrupt` pauses the step → `Result{Interrupted}`, `Interrupts` has one record, `Phase: StepPaused`, paused vertex re-included in frontier, **no routing**, `InterruptedAt` set (§9.2 step 5); a slow/successful **sibling runs to completion and its reducer commits** (not cancelled — §9.7); **two vertices pausing in one step** → `Interrupts` plural, `OnInterrupt` fires once per paused vertex (§11, §15 OnInterrupt cardinality); a `StatefulInterrupt` writes its `Continuation` into the paused vertex's `InterruptRecord` *before* the `StepPaused` checkpoint (crash-survivable — §10.1); a `Route`-policy failure in the same step as a pause still routes its own branch while the step pauses (route vs pause distinction — §15).
- **Impl:** pause detection at the barrier; per-vertex `InterruptRecord` accumulation (incl. continuation) at each paused vertex's checkpoint; `StepPaused` checkpoint with all paused vertices; suppress routing when any vertex is paused.
- **Commit:** `feat(flow): add per-vertex interrupts, plural pauses, sibling completion (§9.7)`

### Task 6.8: Checkpoint granularity — `PerVertex` vs `PerStep`

**Files:** modify `engine.go`; extend tests.

**Design contract:** §10.1 (`WithCheckpointEvery`; `PerVertex` writes after each terminal vertex with `StepRunning` phase mid-step; `PerStep` writes only at boundaries).

**[TDD]**
- **Tests (table):** `PerVertex` (default) writes a `StepRunning` checkpoint after each terminal vertex within a multi-vertex step; `PerStep` writes **no** `StepRunning` checkpoints — `Latest` sits on the prior boundary; both produce identical final `State`; `OnCheckpoint` fires per appended checkpoint.
- **Impl:** thread `WithCheckpointEvery` (default `PerVertex`) through the reduce loop; gate per-vertex appends.
- **Commit:** `feat(flow): add PerVertex/PerStep checkpoint granularity (§10.1)`

### Task 6.9: Resume — checkpoint validation on load (`§10.4`)

**Files:** modify `runner.go`/`engine.go`; tests in `pkg/flow/resume_test.go`.

**Design contract:** §10.4 — validate the loaded (untrusted) checkpoint **before any task runs**; typed errors.

**[TDD — one subtest per §10.4 rule]**
- **Tests (table):** wrong `GraphID` → `GraphMismatchError`; mismatched `GraphVersion` → `GraphVersionMismatchError`; undecodable `StepBase`/`State` → `CheckpointDecodeError`; unknown vertex in `Frontier`/`Interrupts[].Vertex`/`Routes[].From|To` → `UnknownVertexError`; `Completed`/`Cancelled` run → `ResumeTerminalError{Status}`; invalid `Phase`/`Status`/`Interrupts`/`Halt` combination (e.g. `StepPaused` with 0 interrupts, or both `Interrupts` and `Halt` set) → validation error; non-`Latest` revision rejected. **All validation runs before any task executes.**
- **Impl:** `Resume(ctx, id, payload, opts...)` loads `Latest`, runs the full §10.4 validation, decodes `S`. (Continuation of execution lands in 6.10.)
- **Commit:** `feat(flow): add Resume checkpoint validation on load (§10.4)`

### Task 6.10: Resume — continue by `Phase` (StepRunning / StepPaused / StepRouted / StepHalted)

**Files:** modify `engine.go`; extend `resume_test.go`.

**Design contract:** §9.3 (the four phase behaviors + terminal categories), §9.7 (paused share one payload), §9.8 (halt resume retries routing).

**[TDD — one subtest per phase + the cross-cutting properties]**
- **Tests (table):**
  - **`StepRouted`** → continue from the already-computed next frontier (§9.3).
  - **`StepPaused`** → re-run only **paused** vertices (skip `Done` and `Failed`-`Route`), injecting `payload` + each vertex's recorded continuation; once all terminal, route the whole step (§9.3, §9.7); plural paused vertices **share** the one `payload` (each reads via `ResumePayload[T]`).
  - **`StepRunning`** (mid-reduction crash) → skip terminal (`Done`/`Failed`-`Route`), run **every non-terminal** (both `Pending` and already-checkpointed paused, with continuation), then route-or-pause; committed vertices not re-executed (§15 mid-step recovery).
  - **`StepHalted`** → **retry routing/continue**, not a vertex re-run: re-evaluate routing against committed `State` (`HaltCondition`/`HaltUndeclaredTarget`/`HaltDeadEnd`) or continue from `Frontier` with a higher `WithMaxSteps` (`HaltMaxSteps`); only while `GraphVersion` matches (§9.3/§9.8).
  - Cross-cutting: `StatefulInterrupt` continuation restored on resume (§15 durability); resume **with and without** payload.
- **Impl:** branch on `Phase`; implement terminal-skip / paused-rerun / routing-retry per §9.3; inject payload+continuation into resumed vertices' `ctx`.
- **Commit:** `feat(flow): add Resume continuation across all phases (§9.3, §9.8)`

### Task 6.11: Control surface — `Status` / `Get` / `Cancel` (`control.go`)

**Files:** Create `pkg/flow/control.go`, `pkg/flow/control_test.go`.

**Design contract:** §18.2 — store-backed, no execution. `Status` (latest `GraphRunState`, no `S` decode); `Get` (latest with decoded `State`); `Cancel(id, reason)` appends a terminal `RunCancelled` checkpoint (sets `CancelledAt`/`CancelReason`); a running worker's later append that loses to the cancel sees `RevisionConflictError` against a now-`Cancelled` latest = **observed cancellation** (stops gracefully).

**[TDD]**
- **Tests (table):** `Status` returns latest `GraphRunState` without decoding `S`; `Get` returns decoded `State`; `Cancel` appends terminal `RunCancelled` with `CancelledAt`/`CancelReason`, `OnRunFinish` fires; resuming a cancelled run → `ResumeTerminalError{Status: RunCancelled}` (§15 cancellation); a worker append racing a cancel → `RevisionConflictError` read as observed cancellation, worker stops gracefully (not an infra error); `Status`/`Get` of unknown run → `CheckpointNotFoundError`.
- **Impl:** `Status`, `Get`, `Cancel` per §18.2; ensure the coordinator honors a cancel-signalled `ctx` best-effort.
- **Commit:** `feat(flow): add Status/Get/Cancel control surface (§18.2)`

**Phase 6 gate:** full in-process engine works end to end, race-clean. Run `make check`. **Request thorough code review** of the coordinator against all of §9 + §12 before proceeding — this is the riskiest code in the project.

---

## Phase 7 — Worked example & hardening

**Design refs:** §16 (worked example), §15 (testing strategy — full audit), CLAUDE.md (security tooling).
**Dependencies:** Phase 6. **Outcome:** the motivating flow runs as a test, and the full §15 coverage matrix + static/fuzz/vuln analysis is green.

### Task 7.1: Motivating flow as runnable `example_test.go`

**Files:** Create `pkg/flow/example_test.go`.

**Design contract:** §16 — `Graph[Request]` with `RAGDraft`/`ConsultantReview`/`CreateTicket`; intake→draft→review; conditional (approved→sendToSales→finish; needs-expert→ticket→`Interrupt`; unsatisfied→refine→draft cycle); ticket resumes on expert answer (payload).

**[TDD]**
- **Tests:** drive the full flow: happy approval path completes; the **cycle** (refine→draft) iterates; the **`Awaiting` interrupt** at ticket pauses, then `Resume(payload)` routes back and completes; assert append-only `History`, finish-authoritative completion, retry + error-route on draft, and instrumentation timestamps. This doubles as the §16 example and an end-to-end integration of every engine feature.
- **Impl:** the tasks, selectors, reducers, graph wiring, and `Compile` per §16; pin `VertexID`s as consts (§3).
- **Commit:** `test(flow): add motivating sales→consultant→expert example (§16)`

### Task 7.2: §15 coverage audit — fill every remaining row

**Files:** any `pkg/flow/*_test.go`.

**Steps:**
1. Walk the **entire §15 list** as a checklist; for each bullet not already covered by Phases 1–7, add the table-driven subtest. Notable rows to double-check: graph-versioning resume matrix, store immutability, concurrent-resume detection, idempotency-key stability, route-vs-pause, OnInterrupt cardinality, panic safety across task/selector/reducer/Pick/retry/hook, completion durability.
2. Measure coverage: `go test -race -cover ./pkg/...`; aim for high coverage of `engine.go`/`runner.go` and 100% of validation branches.
3. **Commit per logical group** (e.g. `test(flow): cover graph-version resume matrix (§8.1, §15)`).

### Task 7.3: Static analysis, fuzz, and vulnerability pass

**Steps:**
1. `make fuzz` — run `FuzzParse` and `FuzzCheckpointRoundTrip` ≥60s each; commit any seed corpus that surfaces a bug + its fix (use `superpowers:systematic-debugging`).
2. `staticcheck ./...`, `go vet ./...`, `gosec ./...`, `govulncheck ./...` — all clean. Address findings (CLAUDE.md security rules: no `math/rand`, validated boundaries, `filepath.Clean` where paths appear, `ctx` honored on all I/O).
3. Confirm `gofmt -l .` empty and `CGO_ENABLED=0 go build -trimpath ./...` clean.
4. **Commit:** `chore: pass staticcheck/gosec/govulncheck and extended fuzzing`

**Phase 7 gate:** the engine core (`pkg/flow` + `pkg/uuid`) is **feature-complete, dependency-free, race/fuzz/lint-clean**, with the §16 example green. This is the shippable v1 core. **Full code review of the phase** (`superpowers:requesting-code-review`).

---

## Phase 8 — In-process service layers (Tier B, stdlib only)

**Design refs:** §18.1 (component decomposition), §18.2, §18.3 (ingress), §18.5 (control plane), §18.6 (composition). **Dependencies:** Phase 7. **Outcome:** `registry`, `controlplane.Mem`, `flow.Serve`, and the `ingress` HTTP handler — a zero-external-dep HTTP service (Tier B of §18.6). **Still no external deps.**

### Task 8.1: `registry` — `RunnerHandle` + `(GraphID,GraphVersion)` resolver

**Files:** Create `pkg/registry/registry.go`, `pkg/registry/registry_test.go`; add `RunnerHandle` seam in `pkg/flow` (a graph-agnostic JSON-in/out wrapper over a typed `Runner[S]`, §18.1).

**Design contract:** §18.1 — the registry is a **pure resolver** (no execution, no cross-worker routing); keyed by `(GraphID, GraphVersion)`; one process can serve multiple versions.

**[TDD]**
- **Tests (table):** `Add(runner)` registers by `(GraphID, GraphVersion)`; `Resolve(graphID, version)` returns the handle; resolving a current-version-by-default; unknown key → not-found; **multiple versions** of one graph coexist and resolve independently; `RunnerHandle` accepts JSON `S` in and returns JSON `S` out (round-trips through the typed `Runner`).
- **Impl:** `RunnerHandle` (the JSON-erased wrapper; lives in `pkg/flow` since it wraps `Runner[S]`); `registry.New()`, `Add`, `Resolve`, manifest accessor (graphID → versions, for §18.3 `GET /v1/graphs`).
- **Commit:** `feat(registry): add (GraphID,GraphVersion) resolver and RunnerHandle (§18.1)`

### Task 8.2: `controlplane` — interface, `Work`/`Delivery`, `controlplane.Mem`

**Files:** Create `pkg/controlplane/controlplane.go`, `pkg/controlplane/mem.go`, `pkg/controlplane/controlplane_test.go`.

**Design contract:** §18.5 — `ControlPlane` interface (`Submit`/`Consume`); `Delivery` with explicit `Ack`/`Nack`; **single-flight per run**; control plane is **separate** from `CheckpointStore`.

**[TDD]**
- **Tests (table, `-race`):** `Submit` then `Consume(serves)` delivers `Work` for served versions only; a worker `Ack`s to drop work and `Nack`s to requeue (redelivery); **single-flight per `GraphRunID`** (a second submission for an in-flight run is not delivered concurrently); `Work` carries the pre-minted `GraphRunID` (§18.3); ctx cancellation closes the delivery channel cleanly.
- **Impl:** `ControlPlane` interface, `Work`, `GraphVersionKey`, `Delivery` per §18.5; `controlplane.Mem()` (in-process channels + per-run single-flight map). Ack/Nack are no-op-droppable for the Mem impl but exercised by tests.
- **Commit:** `feat(controlplane): add ControlPlane interface and in-memory impl (§18.5)`

### Task 8.3: `flow.Serve` worker loop

**Files:** Create `pkg/flow/serve.go` (or extend `control.go`), `pkg/flow/serve_test.go`.

**Design contract:** §18.6 — `flow.Serve(ctx, reg, cp)` = consume → resolve via registry → execute → Ack at a **quiescent** result (Completed/Interrupted/Halted/Cancelled), Nack on transient failure (§18.5 ack rules — **not** after the seed checkpoint).

**[TDD]**
- **Tests (table, `-race`):** `Serve` consumes a submitted `Work`, resolves the runner, runs it, and `Ack`s only once the run reaches a quiescent result; a transient failure `Nack`s for redelivery; a `Work` for an unserved version is reported not-found (not silently dropped); ctx cancellation stops the loop gracefully; an at-least-once **redelivery** is absorbed by compare-and-append + idempotency (§18.4 safety, simulated with Mem).
- **Impl:** `Serve` loop per §18.6 driving `RunnerHandle.Run`/`Resume`.
- **Commit:** `feat(flow): add Serve worker loop (§18.6)`

### Task 8.4: `ingress` — HTTP handler, routes, error mapping

**Files:** Create `pkg/ingress/ingress.go`, `pkg/ingress/ingress_test.go`.

**Design contract:** §18.3 — the route table, **async-first** (pre-mint `GraphRunID`, return `202 + GraphRunID`), `graphVersion` on every run response, typed-error→status mapping, secure defaults (explicit server timeouts, TLS ≥1.2 config, body-size limits), `WithAuth` seam (never bakes a scheme), `Idempotency-Key` dedup (persisted `key→GraphRunID`), `409 + X-Graph-Version` on resume version mismatch.

**[TDD — `net/http/httptest`]**
- **Tests (table):** `GET /v1/graphs` returns the manifest; `POST /v1/graphs/{graphID}/runs` (with `?version=`) submits via control plane and returns **202 + GraphRunID + graphVersion**; `POST /v1/runs/{id}/resume` resolves by the run's `GraphVersion`, no match → **409 + X-Graph-Version**; `GET /v1/runs/{id}` returns `Status`/`Get` incl. `graphVersion`; `POST /v1/runs/{id}/cancel` cancels; **error mapping**: `GraphRunExistsError`/`ResumeTerminalError`/`RevisionConflictError`/`GraphVersionMismatchError`→409, validation→400, unknown→404, store down→503; **idempotency** — repeated `Idempotency-Key` returns the **same** `GraphRunID` without re-submitting; `WithAuth` rejects unauthenticated requests; secure defaults present (timeouts/body-limit). Validate **all** path/query/body input at the boundary (CLAUDE.md).
- **Impl:** `ingress.New(reg, cp, store, opts...)` returning an `http.Handler`; `WithAuth` option; idempotency map (store-backed seam); secure `http.Server` config helper. Decode request bodies into **concrete typed structs**, never `any` into business logic (CLAUDE.md). Bound body sizes (`http.MaxBytesReader`).
- **Commit:** `feat(ingress): add async-first HTTP handler with secure defaults and error mapping (§18.3)`

### Task 8.5: Tier-A and Tier-B `cmd/` examples

**Files:** Create `cmd/embed/main.go` (Tier A, §18.6), `cmd/service/main.go` (Tier B, §18.6).

**[TDD-light: compile + smoke]**
- **Tests:** a smoke test builds both binaries (`go build ./cmd/...`); a smoke test for `cmd/service` boots the handler on `httptest.Server`, POSTs a run, polls `GET /v1/runs/{id}` to completion.
- **Impl:** copy-paste-able `main.go`s exactly matching §18.6 Tier A and Tier B snippets.
- **Commit:** `docs(cmd): add Tier-A embed and Tier-B service example mains (§18.6)`

**Phase 8 gate:** the in-process HTTP service runs end-to-end with zero external deps; `make check` green; `go mod graph` still shows no runtime deps. Code review against §18.1/§18.3/§18.5/§18.6.

---

## Phase 9 — NATS distribution & durability (Tier C, external dep, nested module)

**Design refs:** §18.4 (NATS), §18.5 (control plane), §18.6 (Tier C). **Dependencies:** Phase 8. **Outcome:** `pkg/nats` providing `nats.Store` + `nats.ControlPlane` behind the existing interfaces, in a **nested Go module** so the core stays dependency-free.

> **Dependency approval:** The user approved full scope **including NATS** in this plan's brainstorming. Per CLAUDE.md, Task 9.1 records that approval in CLAUDE.md before any `go get`.

### Task 9.1: Record dependency approval + nested module

**Files:** modify `CLAUDE.md`; create `pkg/nats/go.mod`.

**Steps:**
1. Amend CLAUDE.md "Dependencies" section: add `github.com/nats-io/nats.go` and the embedded `github.com/nats-io/nats-server/v2` under approved packages, noting they are **confined to `pkg/nats`** (nested module) and **already sanctioned in looprig** (§18.4).
2. Create `pkg/nats/go.mod` as a **nested module** (`module github.com/ciram-co/flow/pkg/nats`, `require github.com/ciram-co/flow` via `replace ../../` for local dev) so NATS never enters the core module's `go.sum` (§18.4).
3. `go get` NATS **inside `pkg/nats` only**; confirm the **core** module's `go mod graph` is still dependency-free.
4. **Commit:** `chore(nats): record NATS dependency approval and add nested module (§18.4)`

### Task 9.2: `nats.Store` — JetStream `CheckpointStore`

**Files:** Create `pkg/nats/store.go`, `pkg/nats/store_integration_test.go` (build-tagged `//go:build integration`).

**Design contract:** §18.4 — implements `CheckpointStore` on JetStream (KV/object), preserving **append-only + compare-and-append** with `(GraphRunID, Revision)` unique; same interface contract as `MemStore` (so it must pass the **same behavioral tests**).

**[TDD — integration-tagged, against embedded JetStream]**
- **Tests (table, `//go:build integration`):** reuse the `MemStore` behavioral suite against `nats.Store` (append/latest/history ordering; compare-and-append → `RevisionConflictError`; concurrent-resume detection; immutability/independent copies). Run against an **embedded** server in-test.
- **Impl:** `nats.Store(nc)` mapping checkpoints to JetStream KV per run, using revisions/optimistic concurrency for compare-and-append; honor `ctx` deadlines on every op (CLAUDE.md). Validate decoded checkpoints (untrusted on read — §10.4 applies at the store boundary too).
- **Verify:** `cd pkg/nats && go test -tags integration -race ./...`.
- **Commit:** `feat(nats): add JetStream CheckpointStore (§18.4)`

### Task 9.3: `nats.ControlPlane` — work streams + version routing + ack/nack

**Files:** Create `pkg/nats/controlplane.go`, `pkg/nats/controlplane_integration_test.go`.

**Design contract:** §18.4/§18.5 — durable consumers on `work.{graphID}.{graphVersion}.{graphRunID}`; a worker subscribes only to versions in its registry (the **subject space is the version router**); per-`GraphRunID` work-queue for single-flight; **at-least-once** redelivery absorbed by compare-and-append + `IdempotencyKey()`; explicit `Ack`/`Nack`.

**[TDD — integration-tagged]**
- **Tests (table, `//go:build integration`):** reuse the `controlplane.Mem` behavioral suite against `nats.ControlPlane`; plus version routing (a worker serving v1 does not receive v2 work); single-flight per run across two workers; redelivery on Nack/crash absorbed without duplicate committed effects.
- **Impl:** `nats.ControlPlane(nc)` with `Submit`/`Consume` over JetStream work streams + durable consumers; `Delivery.Ack/Nack` mapped to JetStream ack; per-run single-flight.
- **Verify:** `cd pkg/nats && go test -tags integration -race ./...`.
- **Commit:** `feat(nats): add JetStream ControlPlane with version-routed subjects (§18.4)`

### Task 9.4: Embedded server + Tier-C example + `B→C` diff verification

**Files:** Create `pkg/nats/embedded.go` (`nats.Embedded()`/`nats.Connect(url)`), `cmd/distributed/main.go` (Tier C, §18.6).

**[TDD-light + integration]**
- **Tests:** `nats.Embedded()` boots an in-process JetStream server for durable single-process runs (§18.4 "local = embedded"); an integration test runs the §16 example end-to-end through Tier C (ingress → `nats.ControlPlane` → `Serve` worker → `nats.Store`), including an **interrupt + resume across a simulated worker restart** (durability proof); confirm the **`B→C` diff** is exactly "add `import .../pkg/nats` + swap two constructors" (§18.6) by diffing `cmd/service` vs `cmd/distributed`.
- **Impl:** embedded server bootstrap; Tier-C `main.go` per §18.6.
- **Verify:** `cd pkg/nats && go test -tags integration -race ./...`.
- **Commit:** `feat(nats): add embedded server and Tier-C distributed example (§18.4, §18.6)`

**Phase 9 gate:** Tier C runs durably and distributed; **core module `go.mod` remains dependency-free** (verify with `go mod graph` at repo root); `pkg/nats` integration tests green. Final code review against all of §18.

---

## Final acceptance checklist (run before declaring the engine done)

Use `superpowers:verification-before-completion` — paste the actual command output, do not assert from memory.

- [ ] `gofmt -l .` → empty (core) and in `pkg/nats`.
- [ ] `CGO_ENABLED=0 go build -trimpath ./...` → clean.
- [ ] `go test -race ./...` → all pass (core).
- [ ] `cd pkg/nats && go test -tags integration -race ./...` → all pass.
- [ ] `go test ./pkg/... -fuzz=FuzzParse -fuzztime=60s` and `-fuzz=FuzzCheckpointRoundTrip -fuzztime=60s` → no crashes.
- [ ] `staticcheck ./...`, `go vet ./...`, `gosec ./...`, `govulncheck ./...` → clean.
- [ ] `go mod graph` (repo root) → **zero runtime dependencies** in the core module; NATS confined to `pkg/nats`'s `go.mod`.
- [ ] Every §15 testing-strategy bullet has a corresponding passing test.
- [ ] The §16 worked example passes as a runnable test.
- [ ] CLAUDE.md "Dependencies" updated to record the NATS approval (§18.4).
- [ ] All public APIs return typed errors (§12.4); no `any` leaks to callers (CLAUDE.md).

---

## Notes & risks for the executing engineer

- **The coordinator (Phase 6) is the crux.** It carries the most subtle invariants (frozen reads, deferred ordered writes, route-vs-pause, the four resume phases). Do not rush it; it has the most review weight. Re-read §9 in full before each Phase 6 task.
- **`any` discipline.** The only sanctioned erasure seams are: JSON (de)serialization, the `vertex[S]` erasure (Task 3.1), and `RunnerHandle` (Task 8.1). Narrow back to concrete types immediately; never thread `any` into logic (CLAUDE.md).
- **Mutual exclusion of `Interrupts` vs `Halt`** is a recurring invariant — assert it in checkpoint validation (§10.4) and in every pause/halt test.
- **Idempotency is the recovery contract.** Whenever a task has side effects in tests, route them through `RunInfo.IdempotencyKey()` to model the §10.1 "not exactly-once" reality.
- **Keep the nested module truly isolated.** After Phase 9, re-run `go mod graph` at the repo root to prove NATS did not leak into the core `go.sum` (§18.4) — this is the single most important property of the dependency boundary.
- **Commit cadence:** one commit per task as labeled. Request code review at each **phase gate**, not just at the end.
```