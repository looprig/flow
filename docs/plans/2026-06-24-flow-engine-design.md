# flows — Durable Pregel Workflow Engine — Design

**Date:** 2026-06-24
**Status:** Approved design, pre-implementation
**Revision:** incorporates the 2026-06-25 design review (`docs/plans/2026-06-25-flow-engine-design-issues.md`)
**Module:** `github.com/ciram-co/flow` (Go library, imported by callers; engine package `pkg/flow`)

---

## 1. Overview

`flows` is a durable, replayable **pregel-style workflow engine** written in Go and consumed as a library. Two central concepts:

- A **`Task[I, O]`** is a reusable, graph-agnostic unit of work — a typed `In → Out` computation (a plain function today; a subgraph or an AI agent later). Written once, reused anywhere; it knows nothing about any graph or shared state.
- A **vertex** is a `Task` *bound into* a `Graph[S]` via `AddVertex`, given a stable `VertexID` and two adapters — a **selector** (`S → I`) and a **reducer** (`(S, O) → S`) — that connect the task's IO to the graph's shared state `S`. The engine runs vertices; edges connect them.

A caller builds a `Graph[S]` of vertices over a shared, user-defined state `S`, wires them with **edges** and conditional **edges**, compiles once, then runs it. Execution is **Pregel super-steps**: each step runs all currently-ready vertices (in parallel), folds their outputs into `S` via their reducers, then computes the next step's ready set by following edges and evaluating conditions. Cycles are allowed (it is a graph, not a DAG).

Any vertex may **interrupt** the run; the engine appends a **checkpoint** (the graph state `S` + per-vertex records + the execution frontier) to a pluggable **`CheckpointStore`** and returns control. A later **`Resume(graphRunID, payload)`** — typically in a different process, possibly days later — reloads the latest checkpoint and continues from exactly that point.

This is the engine layer only. Tasks are plain functions today. AI agents acting at each vertex are a **later layer** built on top, as a new `Task` kind; the engine knows nothing about LLMs.

### 1.1 Why this shape (the core design decision)

Three properties are in tension: **reusable tasks** (narrow typed IO), **data flowing between tasks**, and **decoupling** (tasks don't reference each other). If tasks are reusable and decoupled, something must map one task's output to the next's input — and that glue must live in the **graph wiring**, never in the task, or the task stops being reusable.

- **LangGraph** uses one shared state schema with per-key *reducers*; nodes read/write known keys, so they are coupled to the schema (reuse only via subgraph transforms).
- **eino** keeps tasks as pure `Runnable[I,O]` and declares field mappings on edges plus optional global-state pre/post handlers.

`flows` takes the cleaner synthesis: tasks are pure and typed; the glue is a **selector + reducer supplied per binding** at `AddVertex`. Tasks never reference each other or know `S`; only the thin per-binding adapters know `S`, and they live in the graph. This yields reusable tasks (eino-like) without reflection or field-path strings, and keeps checkpointing simple: all durable data lands in `S`, so a checkpoint is `S` + bookkeeping, and on resume each frontier vertex's input is **re-derived** by running its selector against the restored `S` — in-flight values are never serialized.

### 1.2 Motivating use case

A sales associate asks a domain question. A consultant reviews. A task drafts an answer from a document repository (RAG). If the consultant approves, it returns to sales. If not, the consultant refines the question (a cycle) or, if the answer isn't in the repository, the flow opens a **service ticket** to experts and **interrupts** — waiting, possibly for days, for a tailored answer before resuming. Each actor's step is a vertex; the human waits are interrupts; the evolving request is the graph state `S`.

### 1.3 Goals

- Reusable, typed **tasks** as units of work, pluggable across graphs unchanged.
- Pregel super-step execution with **real parallelism** within a step.
- Cycles / back-edges and conditional routing.
- **Durable, replayable** execution: append-only checkpoints per vertex execution; resume across process restarts without re-running durably-completed vertices.
- Human-in-the-loop and failure handling via **interrupt → checkpoint → external resume by ID**.
- **Everything identified and instrumented** — graph, run, vertex, step, vertex-run — each with lifecycle timestamps, surfaced through **hooks**.
- **Types intact** at the API surface. `any` appears only at serialization boundaries — which explicitly include **interrupt info and resume payloads** (they are persisted in the checkpoint as `json.RawMessage`; see §10.3).
- The **engine core** (`pkg/flow`, `pkg/uuid`, `controlplane`, `registry`, `ingress`) has **zero third-party runtime dependencies** (stdlib only, incl. a vendored UUID). The optional `pkg/nats` adapter is the *only* package with an external dep, and may be a **nested Go module** so the core module's `go.mod` stays dependency-free (§18.4).

### 1.4 Non-goals (v1)

- No LLM/agent/tool abstractions (built later as a `Task` kind).
- No durable `CheckpointStore` implementation beyond in-memory (the interface ships; SQLite/Postgres/filesystem later).
- No concurrent-resume *prevention*. The append-only store keeps a full history with the **latest revision as source of truth** (§10.2). Concurrent resumers are **detected**, not prevented: `(GraphRunID, Revision)` is unique, so a compare-and-append makes a second resumer fail fast with `RevisionConflictError` (committed history never forks); the duplicate *work* performed before that loss is not avoided (use `IdempotencyKey()`, §4.1). A full up-front claim/lease that also skips the duplicate work is deferred (§17).
- No streaming task outputs.
- The **engine core** is in-process: a run executes within one process/worker until it interrupts or completes. Distribution across workers, an HTTP control plane, and durable/cloud deployment are **optional layers** (§18) in separate packages — not part of the zero-dependency core.
- No automatic field-mapping/reflection — selectors/reducers are explicit typed funcs.
- No durable retry across process restarts (in-step retry only; long waits are modeled as interrupts).

---

## 2. Mental model

```
Task[I,O]       a reusable unit of work (FuncTask, later SubgraphTask, AgentTask). No ID, no state.
   │  AddVertex(graph, id, task, selector, reducer, opts...)  ← binds a Task into a Graph[S] as a vertex
   ▼
Graph[S]        a DEFINITION: vertices (bound tasks) + edges + conditions over state type S
   │  Compile(entry, finish)  → validates once → Runner[S] (immutable, reused across runs)
   ▼
Runner[S].Run(ctx, initialState) ─► one GraphRun (id = GraphRunID), Status: Running
   super-step N: StepBase = committed S_N (immutable) → selector(S_N) per vertex
                 → vertices run in parallel (one goroutine each) → barrier
                 → reducers clone-and-commit in VertexID order → checkpoint per vertex
                 → route ONLY if no vertex is paused (Done & Failed-under-Route are routable)
   ...
   └─ Completed (finish executed)  |  Interrupted: Awaiting (human) or Errored (task failure)
```

- **State in, state out.** `Run` takes an initial `S`; a successful run returns the final `S`.
- **`S` is the bus.** Edges carry *control*, not data. A vertex reads its input via its selector(`S`) and writes via its reducer(`S, O`). No per-edge payloads, no fan-in value-merge.
- **Single-owner state.** A per-run **coordinator** owns `S` exclusively. Tasks are pure (they touch no shared state); the coordinator runs every selector and reducer itself. No mutex on `S`.
- **Definitions vs instances.** `Task`/`Graph`/vertices are reusable definitions and hold no run-scoped data. Run-scoped identity, status, timestamps, and state live in the coordinator and the `Checkpoint`.

### 2.1 How this maps to Pregel / BSP

The engine is a **Bulk-Synchronous-Parallel (BSP)** loop — the model Google's Pregel popularized, and the one LangGraph uses. "The Pregel engine" *is* the coordinator's super-step loop in §9.

- **Super-step = one BSP round.** Each step has a *frontier* (active vertices), runs them all **in parallel** behind a **barrier**, then advances.
- **Frozen reads, deferred writes.** Every vertex's selector reads the **immutable committed snapshot `S_N`** (`StepBase`); reducers are applied **after** the barrier to produce `S_{N+1}`. No read-after-write hazards within a step.
- **Edges = activation, not data.** An edge/condition just marks which vertices are in the next frontier. Data lives in `S`.
- **Determinism.** Reducers apply in fixed `VertexID` order and reads are frozen, so a replay from a checkpoint reproduces the same super-steps.
- **Halting.** The run completes when the `finish` vertex has executed and the frontier drains (§9.5).

---

## 3. Identifiers

All IDs **except `StepID`** are UUIDs (the vendored `uuid.UUID`, v4); each is a distinct named type for compile-time safety, with `String`/`MarshalText`/`UnmarshalText` delegated to `uuid.UUID`. `StepID` is a plain `int` super-step index.

```go
type GraphID     uuid.UUID // stable definition id — PIN AS A CONST
type VertexID    uuid.UUID // stable definition id — PIN AS A CONST
type GraphRunID  uuid.UUID // runtime instance — minted per Run (uuid.New)
type VertexRunID uuid.UUID // runtime instance — minted per vertex execution
type StepID      int       // super-step index within a run: 0,1,2,…
```

Edges have **no separate UUID**: the stable edge identity (the effective `EdgeID` for instrumentation) is its `(from, to)` `VertexID` pair. There are no parallel edges, and edges never appear in the checkpoint. Hooks observe edges by `(from, to)`.

**Stability constraint.** A checkpoint's frontier references vertices by `VertexID`, and a resume (new process, days later) rebuilds the graph from code. So definition IDs must be **stable across restarts** — pin them as constants:

```go
var draftID = flow.VertexID(uuid.MustParse("8f14e45f-ceea-467a-9e8b-2c5f9b6a1d33"))
```

A *freshly* generated `uuid.New()` per build would break resume. `GraphRunID`/`VertexRunID` are minted fresh each run/execution. `Compile` rejects duplicate `VertexID`s.

---

## 4. State & instrumentation model

### 4.1 Graph state vs framework records

- **Graph state `S`** — the caller's domain blackboard. **User-extensible.** One singleton per run, **owned exclusively by the run's coordinator**. The only mutation path is a vertex's **reducer**, applied by the coordinator (single-writer); tasks never touch `S` directly. `S` must be round-trippable through the serialization codec (§10.1) — that is what makes snapshotting and resume well-defined.
- **`VertexState`** and **`GraphRunState`** — framework-owned, **NOT user-extensible** instrumentation records carrying identity, status, attempt, and lifecycle timestamps.

```go
type RunStatus int
const (
    RunRunning     RunStatus = iota // in flight
    RunCompleted
    RunInterrupted                  // Awaiting or Errored — there is no RunFailed (§12)
    RunCancelled                    // terminal: Cancel(id) appended a final checkpoint (§18.2); cannot resume
)

type GraphRunState struct {
    GraphRunID    GraphRunID
    GraphID       GraphID
    GraphVersion  string    // compiled-graph fingerprint (§8.1); a mismatch on resume → GraphVersionMismatchError
    Status        RunStatus
    Step          StepID
    Revision      uint64    // monotonic checkpoint sequence for this run (§10)
    CreatedAt     time.Time // Run() called
    StartedAt     time.Time // first super-step began
    UpdatedAt     time.Time // last checkpoint write
    CompletedAt   time.Time // set when Status == RunCompleted
    InterruptedAt time.Time // set when Status == RunInterrupted (most recent pause)
    CancelledAt   time.Time // set when Status == RunCancelled
    CancelReason  string    // Cancel(id, reason)'s reason, when cancelled
}

type VertexStatus int
const (
    VertexPending VertexStatus = iota
    VertexRunning
    VertexDone
    VertexInterrupted
    VertexFailed
)

type VertexState struct {
    VertexID      VertexID
    VertexRunID   VertexRunID
    Step          StepID
    Status        VertexStatus
    Attempt       int
    CreatedAt     time.Time
    StartedAt     time.Time
    CompletedAt   time.Time // Status == VertexDone
    InterruptedAt time.Time // Status == VertexInterrupted
    FailedAt      time.Time // Status == VertexFailed
    Err           string
}

type RunInfo struct {
    GraphID     GraphID
    GraphRunID  GraphRunID
    VertexID    VertexID
    VertexRunID VertexRunID
    Step        StepID
}

// IdempotencyKey identifies one LOGICAL vertex execution, stable across in-process
// retries AND crash recovery. Side-effecting tasks pass it to external systems that
// support idempotency, so a re-run after a pre-checkpoint crash does not duplicate effects.
// It deliberately EXCLUDES VertexRunID and Attempt, which vary per concrete attempt.
type IdempotencyKey string
func (i RunInfo) IdempotencyKey() IdempotencyKey {
    return IdempotencyKey("graph=" + i.GraphID.String() +
        "/run=" + i.GraphRunID.String() +
        "/step=" + strconv.Itoa(int(i.Step)) +
        "/vertex=" + i.VertexID.String())
}
```

Zero-value `time.Time` means "not reached." A vertex execution reaches exactly one terminal (`CompletedAt` | `InterruptedAt` | `FailedAt`).

### 4.2 Reading identity & vertex metadata

Plain `FuncTask`s never see a state handle — they get `(ctx, in)`. The graph state `S` is read by selectors and written by reducers, both run by the coordinator; there is **no** state-mutation API exposed to tasks. Identity and the vertex record are read from `ctx`:

```go
func Info(ctx context.Context) (RunInfo, bool)     // identity (incl. IdempotencyKey())
func Self(ctx context.Context) (VertexState, bool) // this vertex's fixed record (read-only)
```

**Input contract (snapshot safety).** A vertex's input is `selector(StepBase)`, where `StepBase` is the **immutable** committed state of the previous step (it is immutable because reducers clone-and-commit and never mutate it in place — §6.2). All vertices in a step read the same `StepBase` concurrently, which is safe **provided tasks treat their input as read-only** (do not mutate maps/slices/pointers reachable from the input; copy first if you must). This is the documented contract; an optional strict mode that deep-clones each input is deferred (§17).

**Concurrency.** Tasks are pure and the coordinator is the sole writer of `S`, so there is **no mutex on `S`** and `go test -race` is clean by construction. Two reducers in one step that write the same field is a defined overwrite in `VertexID` order (§9.4), not a race.

---

## 5. Task model

`Task[I, O]` is the reusable unit of work. `FuncTask[I, O]` is the first concrete kind. New kinds — `SubgraphTask`, `AgentTask` — implement the same interface with zero engine changes.

```go
type TaskFunc[I, O any] func(ctx context.Context, in I) (O, error)

type Task[I, O any] interface {
    Execute(ctx context.Context, in I) (O, error)
}

type FuncTask[I, O any] struct{ fn TaskFunc[I, O] }
func NewFuncTask[I, O any](fn TaskFunc[I, O]) *FuncTask[I, O]
func (t *FuncTask[I, O]) Execute(ctx context.Context, in I) (O, error) { return t.fn(ctx, in) }
```

A task carries no `VertexID` and no `S` — it is graph-agnostic and reusable. The same `Task` value can be bound into many graphs, each binding producing a distinct vertex.

---

## 6. Vertex binding

### 6.1 AddVertex

A vertex = a `VertexID` + a `Task[I,O]` + a selector + a reducer + an optional error policy. `AddVertex` is a package-level generic function (it introduces `I`/`O`).

```go
type Selector[S, I any] func(s S) I             // derive a vertex's input from state (read-only)
type Reducer[S, O any]  func(s *S, out O) error // fold a vertex's output into state

func AddVertex[I, O, S any](
    g *Graph[S], id VertexID, task Task[I, O],
    selector Selector[S, I], reducer Reducer[S, O], opts ...VertexOption[S],
) error
```

### 6.2 How the coordinator drives a vertex (clone-and-commit)

The reducer is applied to a **clone** of the accumulator and committed only on `nil` error — so a reducer that mutates and then errors **cannot leave partial state**, and the prior committed `S` (`StepBase`) is never mutated in place (which is what makes it a safe immutable read-snapshot):

```go
in := vertex.selector(stepBase)              // S_N -> I   (coordinator, before the barrier; read-only input)
out, err := vertex.task.Execute(ctx, in)     // I  -> O    (vertex goroutine — PURE)
// ... barrier ...
next := clone(accumulator)                    // codec deep-clone
if rerr := vertex.reducer(&next, out); rerr == nil {
    accumulator = next                        // COMMIT only on success
} else {
    /* discard clone; apply error policy (§12) */
}
```

The same task, bound into two graphs without changing the function:

```go
var DraftAnswer = flow.NewFuncTask(func(ctx context.Context, q Query) (Answer, error) {
    return Answer{Text: rag(q.Text)}, nil
})

flow.AddVertex(crm, draftID, DraftAnswer,
    func(s CRMState) Query            { return Query{Text: s.Question} },
    func(s *CRMState, a Answer) error { s.Draft = a.Text; return nil },
    flow.WithRetry[CRMState](RetryPolicy{MaxAttempts: 3, Backoff: expo}),
    flow.WithErrorRoute[CRMState](ticketID, func(s *CRMState, e error) error { s.LastErr = e.Error(); return nil }))

flow.AddVertex(support, draft2ID, DraftAnswer,
    func(t Ticket) Query            { return Query{Text: t.Subject} },
    func(t *Ticket, a Answer) error { t.AutoReply = a.Text; return nil }) // default policy: Pause
```

### 6.3 Vertex options (error/retry policy — see §12)

```go
type VertexOption[S any] func(*vertexConfig[S])
func WithRetry[S any](p RetryPolicy) VertexOption[S]
func WithErrorRoute[S any](handler VertexID, record Reducer[S, error]) VertexOption[S]
func WithErrorPause[S any]() VertexOption[S]      // explicit default
func WithTimeout[S any](d time.Duration) VertexOption[S]
```

Policies attach to the **vertex** (the binding), never to the reusable `Task`.

---

## 7. Build API

```go
func NewGraph[S any](id GraphID, opts ...GraphOption) *Graph[S]
// AddVertex — see §6.

func (g *Graph[S]) AddEdge(from, to VertexID) error
func (g *Graph[S]) AddConditionalEdge(from VertexID, c Condition[S]) error

type Condition[S any] struct {
    Targets []VertexID // declared possible targets — validated at Compile
    Pick    func(ctx context.Context, s S) ([]VertexID, error) // choose next vertex/vertices (read-only); MUST return ≥1 declared target
}
```

- A `Condition` returning multiple targets is a **fan-out**.
- Cycles are permitted. To end a branch, route to `finish` — `Pick` may not return an empty set (§9.5).
- A `Pick` error or panic, or a target outside `Targets` (or an empty set), → a **run-level halt** (`HaltCondition` / `HaltUndeclaredTarget`) — *not* a per-vertex interrupt (§9.5, §9.8).

---

## 8. Compile & validation

```go
func (g *Graph[S]) Compile(entry, finish VertexID, opts ...CompileOption) (*Runner[S], error)
```

Validation (distinct typed errors — §12.4):
- Every `VertexID` unique; `entry`/`finish` exist.
- Every edge/condition/error-route endpoint references a known vertex.
- Every vertex reachable from `entry`; `finish` reachable.
- No vertex has both an unconditional out-edge and a conditional edge (ambiguous routing).
- `Condition.Targets` non-empty and all exist; `Pick` may only return declared targets (also enforced at runtime).

The returned `Runner[S]` is immutable and safe to reuse across concurrent runs.

### 8.1 Graph versioning (so a changed graph fails resume)

`GraphID` is *identity* (a pinned const, unchanged by edits); `GraphVersion` is the **compatibility fingerprint** `Compile` computes so that resuming an old checkpoint against a changed graph **fails loudly** rather than corrupts:

```
GraphVersion = sha256( canonical( sorted VertexIDs, sorted edges (from,to),
                                   sorted conditional (from, sorted Targets),
                                   sorted error-route (from, handler), entry, finish ) )
               + ":" + userVersion        // userVersion from WithVersion (default 0)
```

- **Topology changes are caught automatically** — add/remove a vertex, rewire an edge, or change a `Condition.Targets` set, and the hash changes.
- **Behavior changes aren't hashable** (task/selector/reducer/`Pick` are closures), so `func NewGraph[S](id GraphID, opts ...GraphOption)` accepts **`WithVersion(n uint64)`** — bump it when you change logic without changing topology.

The checkpoint stores `GraphVersion` (§4.1). `Resume` compares it to the current runner's; any difference → **`GraphVersionMismatchError`** (an engine `error`, validated in §10.4 before any task runs). A future opt-in migration/override for known-compatible changes is deferred (§17).

---

## 9. Runner & execution

```go
type Runner[S any] struct{ /* compiled graph, default store, hooks, concurrency, maxSteps, granularity */ }

func (r *Runner[S]) Run(ctx context.Context, in S, opts ...RunOption) (*Result[S], error)
func (r *Runner[S]) Resume(ctx context.Context, id GraphRunID, payload any, opts ...RunOption) (*Result[S], error)

// Control surface — store-backed, no execution (§18.2):
func (r *Runner[S]) Status(ctx context.Context, id GraphRunID) (GraphRunState, error) // latest run state; no S decode
func (r *Runner[S]) Get(ctx context.Context, id GraphRunID) (*Result[S], error)       // latest, with decoded State S
func (r *Runner[S]) GraphID() GraphID
func (r *Runner[S]) GraphVersion() string                                             // routing key (§8.1)

type Result[S any] struct {
    Run        GraphRunState // ids, status, step, revision, timestamps (§4.1)
    State      S             // output state (final, or last-checkpointed on interrupt)
    Interrupts []Interrupt   // per-vertex pauses (§9.7) — set when paused; mutually exclusive with Halt
    Halt       *Halt         // run-level routing/structural halt (§9.8) — set instead of Interrupts
}

type RunOption func(*runConfig) // WithHooks (repeatable), WithConcurrency,
                                // WithMaxSteps, WithGraphRunID, WithCheckpointEvery (store is set at Compile, §9)
```

The `error` return is for **engine/infrastructure** failures only (store unavailable, decode failure, `GraphID` mismatch) — not task failures (§12.3). `Result.Run.Status` is never `RunRunning` on return; `Running` is what mid-flight checkpoints and store queries observe.

`Run` mints a fresh `GraphRunID` unless `WithGraphRunID` is given; a supplied id that **already has checkpoint history** is rejected with `GraphRunExistsError` — continuing an existing run is `Resume`'s job, not `Run`'s, so a caller cannot accidentally append a new run onto an old run's history.

The `Runner` carries **one `CheckpointStore`**, set at `Compile` (a `WithStore` `CompileOption`; an internal `MemStore` if omitted). **All** operations — `Run`, `Resume`, `Status`, `Get` — use that single store; there is **no per-run override**, so `Run` and `Resume` of the same run can never look in different stores.

### 9.1 Runner vs coordinator

`Runner[S]` is immutable and holds no run state. Each `Run`/`Resume` spins up an internal **coordinator** that owns one run's `GraphRunID`, `S`, frontier, and `GraphRunState`, and is the **sole owner and writer of `S`**.

### 9.2 Super-step loop (BSP)

1. **Seed.** Mint `GraphRunID`; set `S = in`; stamp `CreatedAt`/`StartedAt`, `Status = Running`; append the **seed checkpoint** as `Phase: StepRouted`, `Frontier: [entry]`, `Revision: 0` (the routed state into step 0); fire `OnRunStart`.
2. **Freeze & compute inputs.** `StepBase` = the committed `S` from step N−1 (immutable; §6.2). For each frontier vertex (in `VertexID` order) run `selector(StepBase)`. Stamp `CreatedAt`.
3. **Run in parallel.** One goroutine per frontier vertex (bounded by `WithConcurrency`, default `GOMAXPROCS`), each running only `task.Execute(ctx, in)`. Retry per `WithRetry`; panics recovered into `VertexError`. Stamp `StartedAt`; fire `OnVertexStart`. **Barrier**: wait for all.
4. **Reduce + checkpoint per vertex.** In `VertexID` order, each vertex reaches exactly one terminal state: **success** → clone-and-commit its reducer → `Done`/`CompletedAt`; **`Route`-policy error** → clone-and-commit the record-reducer → `Failed`/`FailedAt` (*routable*, step 5); **`flow.Interrupt`** → `Interrupted`/`InterruptedAt` (*paused*); **`Pause`-policy error** → `Failed`/`FailedAt` (*paused*). **Append a checkpoint after each vertex** (in `PerVertex` mode — the default; `PerStep` defers all writes to the step boundary, §10.1). A paused vertex's checkpoint **already carries its `InterruptRecord`** (Kind, `Info`, and any `StatefulInterrupt` `Continuation`) accumulated into `Interrupts` — so a continuation survives a crash that occurs before the step's final `StepPaused` checkpoint. Fire `OnVertexFinish` + `OnCheckpoint`.
5. **Route — only if no vertex is *paused*.** A vertex is *paused* iff it is `Interrupted` (`Awaiting`) or `Failed` under a `Pause` policy. If **any** vertex is paused, **do not route**: append a checkpoint with `Phase: StepPaused`, all paused vertices re-included in the frontier and recorded in `Interrupts` (**there may be more than one** — §9.7), the successful siblings' reducers already committed; stamp `InterruptedAt`; return `Result{Interrupted}`. Otherwise every vertex is *terminal-and-routable* — `Done` or `Failed`-under-`Route` — so compute successors (`AddEdge`/`Condition.Pick(ctx, S_{N+1})` for `Done`; the **error-route handler** for `Failed`-`Route`), **record each decision in `Routes`** (the chosen branch, attributable by `(from, to)`), append `Phase: StepRouted`, fire `OnEdge`/`OnStep`.
6. **Terminate.** If the `finish` vertex executed this step and the frontier is now empty → set `Status = Completed`/`CompletedAt` and **append a final checkpoint** (so `Latest` durably reflects completion), fire `OnRunFinish`, return `Result{Completed}`. A terminal `Errored` condition (`DeadEnd`/`MaxSteps`, §9.5) likewise **appends a final checkpoint** (`Status = Interrupted`, `Errored`) before returning. Otherwise increment the step and loop.

Reads are frozen, writes are deferred and ordered, inputs re-derived from `StepBase`, and every terminal state is durably checkpointed — replay is deterministic.

### 9.3 Resume

`Resume` loads the **latest** checkpoint (§10.2), validates it (§10.4), decodes `S`, and continues based on `Phase`. First, two precise vertex categories:
- **Terminal** (work committed; **always skipped** on resume): `Done`, and `Failed`-under-`Route` (its record-reducer already committed and it routes to a handler).
- **Paused** (**re-run** on resume): `Interrupted` (`Awaiting`), and `Failed`-under-`Pause`.

By `Phase`:
- **`StepRunning`** — a crash *mid-reduction*: reload `StepBase` and the accumulated `State`; **skip** vertices already checkpointed *terminal* (`Done`/`Failed`-`Route`); **run/re-run every non-terminal vertex** — both not-yet-checkpointed (`Pending`) *and* already-checkpointed *paused* ones (`Interrupted`/`Failed`-`Pause`), injecting the `Resume(…, payload)` value and each vertex's **recorded continuation** (`StatefulInterrupt`); then proceed to route-or-pause (§9.2 step 5). Routing has *not* happened yet, so this is **not** "continue from the frontier."
- **`StepPaused`** — re-run only the **paused** vertices (skip `Done` *and* `Failed`-under-`Route`), injecting the `Resume(…, payload)` value and each vertex's recorded continuation into the resumed vertices' `ctx`; once every vertex is terminal, route the whole step (§9.2 step 5).
- **`StepRouted`** — continue from the already-computed next frontier.
- **`StepHalted`** (a run-level halt, §9.8) — **retry routing / continue**, *not* re-run a vertex: re-evaluate the step's routing against the committed `State` (`HaltCondition`/`HaltUndeclaredTarget`/`HaltDeadEnd`) or continue from the recorded `Frontier` (`HaltMaxSteps`, with a higher `WithMaxSteps`). This only works when the current runner's `GraphVersion` still matches the checkpoint. If recovery requires a topology edit, changed `Condition.Targets`, or bumped `WithVersion`, strict graph-version validation rejects the resume with `GraphVersionMismatchError`; v1 has no migration/override.
- **Terminal** run (`Completed` or `Cancelled`) → `ResumeTerminalError{Status}`.

### 9.4 Same-field reducers (v1 stance)

`Compile` cannot statically know which fields a reducer writes. Two reducers in the same step writing the same field are **not** a race — applied single-threaded in `VertexID` order, last-in-order wins (a defined, run-stable overwrite). Guidance: parallel branches write **disjoint** fields; accumulating fields use **append/commutative** reducers. Runtime/compile-time conflict detection deferred (§17).

### 9.5 Completion (finish is authoritative)

A run **completes only when the `finish` vertex has executed** and the frontier then drains. The failures below are **run-level halts** — *not* per-vertex pauses (no `Interrupts` entry, no vertex to re-run). Each surfaces as `RunInterrupted`/`Errored` with `Result.Halt` set, phase `StepHalted`, the committed `State` checkpointed, and `Resume` **retries routing / continues** (§9.3, §9.8) after the operator fixes the cause **without changing graph compatibility**; none is an engine `error` return. If the fix changes topology, declared condition targets, or the user version, the old checkpoint cannot resume in v1 because `GraphVersion` validation fails.
- `Condition.Pick` must return ≥1 declared target; an empty set or a target outside `Targets` → `HaltUndeclaredTarget` (`UndeclaredTargetError`). End a branch by routing to `finish`.
- `Condition.Pick` returning a non-nil `error` (or panicking) → `HaltCondition` (`ConditionError`).
- Frontier drains **without** `finish` having executed (dead end / mis-wiring) → `HaltDeadEnd` (`DeadEndError`).
- `Steps` over `WithMaxSteps` → `HaltMaxSteps` (`MaxStepsExceededError`); resuming with a higher limit continues.

### 9.6 Fan-in

Multiple edges into one vertex are a control-flow join: the vertex appears once in the next frontier (deduped) and runs once; its selector reads the merged `S`. No value merge.

### 9.7 Parallel siblings on interrupt; multiple interrupts per step

**Siblings are never cancelled.** The barrier (step 3) waits for every goroutine, so when one vertex interrupts the others run to completion and their reducers commit — no wasted work, no re-run, deterministic. The pause therefore lands at the barrier: a slow sibling delays it, and a sibling that must itself wait long should also `Interrupt` (then both are paused). Whole-run abort is a separate mechanism — cancel the parent `ctx` passed to `Run`.

**Multiple vertices can pause in the same step** (two `flow.Interrupt`s, or an interrupt plus a `Pause`-policy failure). All are recorded — `Checkpoint.Interrupts` / `Result.Interrupts` are **plural**, one record per paused vertex. On `Resume`, every **paused** vertex re-runs (terminal `Done` / `Failed`-under-`Route` vertices are skipped — §9.3) and they **share** the single `payload` (each reads it via `ResumePayload[T]`); per-interrupt *targeted* resume with distinct payloads is deferred (§17).

### 9.8 Run-level halts vs per-vertex interrupts

Two distinct things end a run as `RunInterrupted`/`Errored`, and they resume differently:

- **Per-vertex pause** (`Interrupts`, `StepPaused`) — a *vertex* must re-run: a `flow.Interrupt` (`Awaiting`) or a `Pause`-policy task failure. `Resume` re-runs the paused vertices (§9.3).
- **Run-level halt** (`Halt`, `StepHalted`) — *routing or structure* failed, with **no vertex to re-run**: `ConditionError`, `UndeclaredTargetError`, `DeadEndError`, `MaxStepsExceededError` (§9.5). The step's reducers already committed, so `Resume` re-attempts **routing/continuation** against the committed `State` when graph compatibility still matches (for example, a transient condition dependency recovers, or `WithMaxSteps` is raised) — never a vertex. If the fix changes topology/targets/version, v1 rejects resume with `GraphVersionMismatchError`.

They are **mutually exclusive** for a checkpoint: a step either *pauses* (so it never routes → `Interrupts`) or it *routes* (and routing may *halt* → `Halt`). Exactly one of `Interrupts`/`Halt` is set when `RunInterrupted`.

---

## 10. Durability: checkpoint, store, interrupt/resume

### 10.1 Checkpoint (append-only, per vertex execution)

The engine appends checkpoints at a granularity set by `WithCheckpointEvery` (default **`PerVertex`**):

- **`PerVertex`** (default) — append after **each** vertex's terminal effect (reducer commit, pause, or route-fold). `Phase` is `StepRunning` while the step is partly reduced (some vertices terminal, some not). A crash recovers via §9.3 `StepRunning`: skip terminal vertices, run only the rest. Safest for expensive/side-effecting tasks.
- **`PerStep`** — append **only at step boundaries** (`StepRouted`/`StepPaused`/`StepHalted`, plus seed/terminal); **no per-vertex `StepRunning` checkpoints are written.** So `Latest` always sits on a prior step boundary, and a crash re-runs the **entire** active frontier from `StepBase` (no `Done`-skipping — there are no per-vertex records to skip). Fewer writes; a crash re-does the whole step, so side-effecting tasks must be idempotent (§4.1).

```go
type StepPhase int
const ( StepRunning StepPhase = iota; StepPaused; StepRouted; StepHalted )

type Checkpoint struct {
    Run       GraphRunState   // run-level status + Revision + timestamps
    StepBase  json.RawMessage // committed S_N — frozen read snapshot; pending vertices' selectors read THIS
    State     json.RawMessage // accumulated S (S_N + reducers of every TERMINAL — Done or Failed-Route — vertex so far)
    Vertices  []VertexState   // per-vertex records for this step; TERMINAL (Done / Failed-Route) ones are skipped on resume
    Frontier  []VertexID       // the active vertex set this checkpoint resumes from — meaning depends on Phase
                               //   (StepRouted: next set = ∪ Routes[].To; StepPaused: paused vertices; StepRunning: this step's vertices; seed: [entry])
    Routes    []RouteRecord    // routing DECISIONS that produced Frontier — which source picked which target(s) (§9.5)
    Phase     StepPhase
    Interrupts []InterruptRecord // per-vertex pauses, accumulated as paused vertices are checkpointed (present in StepRunning AND StepPaused; ≥1 once StepPaused; §9.7). Mutually exclusive with Halt.
    Halt      *HaltRecord       // run-level routing/structural halt (StepHalted; §9.8). Mutually exclusive with Interrupts.
}

// RouteRecord durably records ONE routing decision — which source vertex activated which
// target(s) this step, and whether via a Condition. The branch is identified by (From, To),
// the stable edge identity / effective EdgeID. Written at the StepRouted boundary.
type RouteRecord struct {
    From        VertexID
    To          []VertexID // chosen target(s); for a Condition, exactly what Pick returned
    Conditional bool       // true if chosen by Condition.Pick; false for a static AddEdge
}

type InterruptRecord struct {
    Vertex       VertexID
    Kind         InterruptKind   // Awaiting | Errored
    Info         json.RawMessage // user-facing reason (Awaiting) — a serialization boundary
    Cause        string          // error message/type name (Errored)
    Continuation json.RawMessage // optional task continuation (StatefulInterrupt) — a serialization boundary
}

type HaltKind int
const (
    HaltCondition       HaltKind = iota // Condition.Pick returned an error
    HaltUndeclaredTarget                // Pick returned empty / a target outside Targets
    HaltDeadEnd                         // frontier drained without finish executing
    HaltMaxSteps                        // step budget (WithMaxSteps) exceeded
)
type HaltRecord struct {
    Kind  HaltKind
    Step  StepID
    Cause string // error message/type name
}
```

`S` decodes back into the concrete type — the only `any`/erasure boundary. **Frontier inputs are not stored**; they are re-derived via selectors on resume. `StepBase` keeps BSP reads correct.

**Recovery is not exactly-once for external effects.** A vertex execution is skipped on recovery **only after its `Done` checkpoint is durable**. If the process crashes *after* a task's side effect but *before* that checkpoint is appended, the vertex re-runs. Therefore: **side-effecting tasks must pass `RunInfo.IdempotencyKey()` to external systems that support idempotency** (§4.1) so the re-run does not duplicate the effect.

### 10.2 Store (append-only; latest revision is source of truth)

```go
type CheckpointStore interface {
    Append(ctx context.Context, cp *Checkpoint) error          // compare-and-append: accept iff cp.Run.Revision == latest+1 (unique per run), else RevisionConflictError
    Latest(ctx context.Context, id GraphRunID) (*Checkpoint, error) // current source of truth
    History(ctx context.Context, id GraphRunID) ([]*Checkpoint, error) // full ordered history (debug/time-travel)
}

type MemStore struct{ /* map[GraphRunID][][]byte (encoded checkpoints) + sync.RWMutex */ }
func NewMemStore() *MemStore
```

Checkpoints are **immutable and append-only**: the engine never edits, only appends, so there are no edit conflicts. Each `Checkpoint.Run.Revision` is a monotonic sequence per run; `Latest` returns the highest. Keeping the full `History` gives time-travel/debugging for free. **`(GraphRunID, Revision)` is unique, so `Append` is a compare-and-append** — a second concurrent resumer that loaded the same `Latest` and tries to append the same next revision fails fast with `RevisionConflictError`, so committed history never forks (concurrent resume is **detected**). It does **not** avoid the duplicate work done before the losing append (use `IdempotencyKey()`, §4.1); a full up-front claim/lease is deferred (§17). A normal run's lone coordinator appends strictly increasing revisions and never conflicts. A `Append`/`Latest` failure is an **engine error** returned from `Run`/`Resume` — never a task outcome, because an interrupt itself requires a successful append (§12.3).

Immutability is **enforced structurally, not just documented**: a store persists an *independent* copy — `Append` serializes the checkpoint, and `Latest`/`History` return *freshly decoded* values — so a caller cannot mutate stored history through a returned pointer. `MemStore` holds the encoded bytes per run and round-trips through the codec (which also surfaces non-serializable states early). The `Store` contract requires this for every implementation.

### 10.3 Interrupt & resume

```go
func Interrupt(ctx context.Context, info any) error                       // pause; vertex re-runs on resume
func StatefulInterrupt(ctx context.Context, info, continuation any) error // pause + persist task continuation
func ResumePayload[T any](ctx context.Context) (T, bool)                  // typed read of Resume() payload
func InterruptState[T any](ctx context.Context) (T, bool)                 // typed read of StatefulInterrupt continuation

type InterruptKind int
const ( Awaiting InterruptKind = iota; Errored )

type Interrupt struct {
    GraphRunID GraphRunID
    Vertex     VertexID
    Kind       InterruptKind
    Info       any   // user reason (Awaiting) — serialization boundary; typed read via ResumePayload/InterruptState
    Cause      error // underlying failure (Errored)
}

// Halt is a RUN-LEVEL (routing/structural) failure — not a vertex pause; resume retries routing (§9.8).
type Halt struct {
    GraphRunID GraphRunID
    Kind       HaltKind // HaltCondition | HaltUndeclaredTarget | HaltDeadEnd | HaltMaxSteps
    Step       StepID
    Cause      error
}
```

`info`/`payload`/`continuation` are `any` because they are **serialization boundaries** (persisted as `json.RawMessage`); the typed *read* side is `ResumePayload[T]`/`InterruptState[T]`. Pause/resume flow is §9.2 step 5, §9.3, and §9.7 (multiple interrupts).

### 10.4 Checkpoint validation on load

A checkpoint loaded from a `CheckpointStore` is **untrusted input** (CLAUDE.md: validate at every boundary). Before the coordinator acts on it, `Resume` validates and returns a typed engine `error` on any failure — *before any task executes*, so a corrupt or mismatched checkpoint fails fast:

- `GraphID` matches the runner's graph → else `GraphMismatchError`; `GraphVersion` matches the compiled graph's fingerprint → else `GraphVersionMismatchError` (§8.1) — **a changed graph cannot resume an old checkpoint**.
- `StepBase` and `State` decode into `S` → else `CheckpointDecodeError`.
- Every `Frontier`, `Interrupts[].Vertex`, and `Routes[].From`/`Routes[].To` references a vertex in the compiled graph → else `UnknownVertexError`.
- `Phase`/`Status` are a valid combination: a `Completed` or `Cancelled` run cannot resume (`ResumeTerminalError{Status}`); `StepRunning` may carry **0+** accumulated `Interrupts` and no `Halt` (mid-step, not yet final); `StepPaused` requires **≥1** `Interrupts` and no `Halt`; `StepHalted` requires `Halt` set and no `Interrupts` (`Interrupts`/`Halt` are **mutually exclusive**); terminal (`Done`/`Failed`-`Route`) vertices in `Vertices` are consistent with the frontier.
- `Run.Revision` is the highest for the run (the loaded checkpoint is genuinely `Latest`).

---

## 11. Observability: hooks

Optional callbacks (ISP-friendly); multiple hook sets accumulate; purely observational. Every event carries the instrumentation records from §4.1 (including timestamps).

```go
type Hooks struct {
    OnRunStart     func(ctx context.Context, ev GraphRunState)
    OnRunFinish    func(ctx context.Context, ev GraphRunState) // Completed, Interrupted, or Cancelled
    OnVertexStart  func(ctx context.Context, ev VertexState)
    OnVertexFinish func(ctx context.Context, ev VertexState)
    OnEdge         func(ctx context.Context, from, to VertexID, run GraphRunState) // every traversed edge (incl. each conditional pick); also durably in Checkpoint.Routes
    OnStep         func(ctx context.Context, run GraphRunState, activated int)      // step-level (run.Step) — no single vertex, so run-level context not RunInfo
    OnCheckpoint   func(ctx context.Context, id GraphRunID, rev uint64, step StepID)
    OnInterrupt    func(ctx context.Context, iv Interrupt) // fired ONCE PER paused vertex — plural interrupts in one step ⇒ multiple calls
    OnHalt         func(ctx context.Context, h Halt)       // run-level routing/structural halt (§9.8) — kind + cause
}
// Registered via WithHooks(h) (repeatable).
```

---

## 12. Error handling & recovery

### 12.1 Outcomes are not errors

Expected branches ("not found", "needs expert") are **outcomes** — values in `S` routed by conditional edges, never Go `error`. Reserve `error` for genuine failure.

### 12.2 Per-vertex error policy

When a task returns a non-interrupt `error` (or panics — recovered into `VertexError`):

| Policy | When | Resolution |
|---|---|---|
| **Retry** (`WithRetry`) | transient | re-run the task (same input from `StepBase`), bounded + backoff, bumping `Attempt`; on success continue |
| **Route** (`WithErrorRoute`) | recoverable in-graph | clone-and-commit the record reducer to fold the error into `S`; activate the **handler vertex** next |
| **Pause** (default) | needs a human / unrecoverable | append a checkpoint as an **`Errored`** interrupt (`Cause` set); resumable after a fix |

`Retry` composes: retry first; only on exhaustion does `Route`/`Pause` fire. There is **no `Fail` action and no `RunFailed` status** — "fail" is `Pause` + `Errored`, which preserves the checkpoint and stays resumable. `DeadEndError`/`MaxStepsExceededError` are **run-level halts** (§9.8), not per-vertex.

**`WithTimeout(d)`** sets a per-vertex deadline on the task's `ctx`. Cancellation is **cooperative** — Go has no forced goroutine kill, so the task must observe `ctx.Done()` and return; a task that ignores `ctx` runs past the deadline. When a task returns after expiry, the engine surfaces a `VertexError` wrapping `context.DeadlineExceeded` — **retryable by default** (so `WithRetry` retries it), then `Route`/`Pause` per policy.

**Record-reducer failure.** If the `Route` policy's record-reducer itself errors or panics, the vertex **pauses as `Errored`** — it does *not* recursively re-apply the `Route` policy — and clone-and-commit leaves `S` unchanged.

```go
type RetryPolicy struct {
    MaxAttempts int
    Backoff     func(attempt int) time.Duration
    Retryable   func(err error) bool // default: any non-interrupt error
}
```

### 12.3 Task failure vs engine failure

| Situation | Surface | Recovery |
|---|---|---|
| Flow in progress | `Run.Status: Running` | — |
| Flow done (finish executed) | `Run.Status: Completed` | — |
| Human wait | `Interrupted, Kind: Awaiting` | `Resume` with payload |
| Task errored (policy `Pause`) | `Interrupted, Kind: Errored, Cause` | fix cause, `Resume` |
| **Engine couldn't operate** (store down, decode/`GraphID` mismatch) | **`error` return** (not a status) | fix infra, `Resume` from latest checkpoint |

The `error` return is required for infra failures precisely because a durable interrupt depends on a successful `CheckpointStore.Append` — if the store is down there is nowhere to persist a pause, so it cannot be an interrupt. It is never a dead end: every appended checkpoint remains, and `Resume` continues from the latest.

### 12.4 Typed errors

All package-level APIs return **typed errors** for `errors.As`: `DuplicateVertexError`, `UnknownVertexError`, `UnreachableVertexError`, `AmbiguousRoutingError`, `MissingEntryError` (build); `MaxStepsExceededError`, `UndeclaredTargetError`, `DeadEndError`, `ConditionError`, `VertexError{VertexID, VertexRunID, Attempt, Err}` (runtime); `CheckpointDecodeError`, `CheckpointNotFoundError`, `ResumeTerminalError{Status RunStatus}`, `GraphMismatchError`, `GraphVersionMismatchError`, `GraphRunExistsError`, `RevisionConflictError`, `StoreError` (durability/engine). The interrupt signal is an internal typed error detected via `errors.As`.

### 12.5 Panic safety for user callbacks

Every user-provided callback runs under **panic recovery** — the engine never lets a user panic crash the process. A recovered panic is converted to a typed error and follows the matching path:
- **task** panic → `VertexError` → the vertex's error policy (§12.2).
- **selector** panic → the vertex cannot produce input → `VertexError` → error policy.
- **reducer** (and error-route record-reducer) panic → treated as a reducer error; clone-and-commit means `S` is left **unchanged** → error policy.
- **condition `Pick`** panic → `ConditionError` → a **run-level halt** (`HaltCondition`, §9.5/§9.8) — not a per-vertex interrupt.
- **hook** panic (§11) → recovered and **discarded**; hooks are purely observational, so a hook panic is logged-and-ignored and **never alters control flow** or fails the run.
- **retry callbacks** (`RetryPolicy.Backoff` / `Retryable`) panic → recovered and treated as the task's failure → `VertexError` → the vertex's error policy (a buggy backoff/predicate pauses, never crashes).

---

## 13. UUID package

A self-contained, **stdlib-only** UUID package (`pkg/uuid`) mirroring looprig's: `type UUID [16]byte`, `New() (UUID, error)` (v4 via `crypto/rand`), `String`, `IsZero`, `MarshalText`/`UnmarshalText`, typed `GenerateError`/`ParseError`. The named ID types in §3 wrap it. No external dependency.

---

## 14. Package layout

```
flows/
  go.mod                       // module github.com/ciram-co/flow — zero runtime deps
  CLAUDE.md / AGENTS.md        // dev guidelines (done)
  docs/plans/                  // design docs
  pkg/
    uuid/                      // package `uuid`
      uuid.go uuid_test.go     // stdlib-only v4 UUID (mirrors looprig)
    flow/                      // package `flow` — the engine; import "github.com/ciram-co/flow/pkg/flow"
      ids.go                   // GraphID, VertexID, StepID, GraphRunID, VertexRunID
      state.go                 // GraphRunState, VertexState, RunInfo (+IdempotencyKey), RunStatus, VertexStatus
      task.go                  // Task[I,O], TaskFunc, FuncTask[I,O]
      vertex.go                // internal vertex[S], Selector/Reducer, AddVertex, VertexOption, clone-and-commit
      graph.go                 // Graph[S], AddEdge/AddConditionalEdge, Condition[S]
      compile.go               // Compile + validation, Runner construction
      runner.go                // Runner[S], Run, Resume, Result, RunOption
      engine.go                // coordinator: BSP loop, frontier, parallel exec, reduce, route, completion
      interrupt.go             // Interrupt/StatefulInterrupt, ResumePayload, InterruptState, Interrupt, InterruptKind
      retry.go                 // RetryPolicy, retry + panic-recovery wrapper
      clone.go                 // codec deep-clone of S (JSON round-trip)
      checkpoint.go            // Checkpoint, InterruptRecord, StepPhase, (de)serialization
      store.go                 // CheckpointStore (Append/Latest/History), MemStore
      hooks.go                 // Hooks, dispatch
      control.go               // Status/Get/GraphID/GraphVersion/Cancel control surface (§18.2) + Serve worker loop (§18.6)
      errors.go                // typed errors
      example_test.go          // the sales→consultant→expert approval flow (runnable example/test)
    controlplane/              // CONTROL PLANE: ControlPlane interface + controlplane.Mem (§18.5); stdlib
    registry/                  // REGISTRY: (GraphID,GraphVersion)→RunnerHandle resolver (§18.1); stdlib
    ingress/                   // INGRESS: HTTP API over registry + control plane (§18.3); stdlib net/http
    nats/                      // OPTIONAL (§18.4): nats.ControlPlane + nats.Store (JetStream); external dep
```

The engine core (`pkg/flow` + `pkg/uuid`) depends only on interfaces (`CheckpointStore`, `ControlPlane`, hooks) and stdlib — **zero runtime dependencies**. `controlplane`/`registry`/`ingress` are stdlib adapters; **`nats` is the only external-dep package** (and optional). Importing `flow` never pulls in `nats`. (`CheckpointStore`/`MemStore` live in `flow`, not a separate `store` package — the `Checkpoint` type references engine types, so splitting it out would cycle.)

---

## 15. Testing strategy

Per CLAUDE.md: **table-driven**, `t.Parallel()` subtests, `-race`. Minimum coverage:

- **Build/compile:** duplicate vertex, unknown endpoint, unreachable vertex, ambiguous routing, missing entry/finish, valid graph compiles.
- **Tasks/binding:** same `Task` bound into two graphs runs unchanged; selector derives input; reducer folds output.
- **Execution:** single vertex; linear chain; **cycle** (max-steps → `Errored`); **fan-out**; **fan-in** (runs once over merged `S`); parallel vertices with disjoint reducers under `-race`.
- **Branch attribution:** a conditional `Pick`'s chosen target(s) are recorded in `Checkpoint.Routes` keyed by `(from, to)` with `Conditional=true`; `History` reconstructs the exact path taken even with multiple conditional sources in one step or shared targets; `OnEdge` fires per traversed edge.
- **Snapshot safety:** states containing **maps/slices/pointers** stay race-clean under parallel vertices; `StepBase` is not mutated by a sibling reducer; a task treating input read-only sees stable data.
- **Reducer atomicity:** a reducer that mutates then returns an error leaves `S` **unchanged** (clone discarded); the vertex follows its error policy.
- **Completion:** `finish` executes → `Completed`; otherwise structural failures surface as run-level `Halt`s (see the Run-level halts row), never `Completed` and never an `error` return.
- **Errors:** retry exhaustion → policy fires; `WithErrorRoute` activates handler + folds error; default `Pause` → `Errored` with `Cause`; panic → `VertexError`; store `Append` failure → `error` return (not a status).
- **GraphRunID reuse:** `Run(WithGraphRunID(id))` where `id` already has checkpoint history → `GraphRunExistsError`; a fresh `Run` mints a unique id; continuing an existing run is only via `Resume`.
- **Timeout:** `WithTimeout` expiry → `VertexError` wrapping `context.DeadlineExceeded`, **retryable by default**, then `Route`/`Pause` per policy.
- **Route record-reducer failure:** record-reducer errors/panics → vertex pauses as `Errored` (no recursive `Route`); `S` unchanged.
- **Seed checkpoint:** revision 0 is `StepRouted` with `Frontier=[entry]`.
- **Graph versioning:** a topology edit (add vertex / rewire edge / change `Targets`) changes `GraphVersion` → resuming an old checkpoint → `GraphVersionMismatchError`; a `WithVersion` bump does the same for logic-only changes; an identical graph resumes fine.
- **Durability (append-only):** checkpoint **round-trip equality**; `History` ordered by `Revision`, `Latest` = highest; resume with/without payload; `StatefulInterrupt` continuation restored.
- **Checkpoint granularity:** `PerVertex` writes per-vertex checkpoints and recovery skips terminal vertices; **`PerStep`** writes **no** per-vertex `StepRunning` checkpoints and a crash re-runs the **whole frontier** from `StepBase` (`Latest` sits on the prior boundary).
- **Completion durability:** a completed run appends a final `RunCompleted` checkpoint so `Latest` shows `Completed`; re-resuming it → `ResumeTerminalError{Status: RunCompleted}`. A `DeadEnd`/`MaxSteps` run appends a final `Errored` checkpoint.
- **Store immutability:** mutating a checkpoint returned by `Latest`/`History`, or one passed to `Append`, does **not** corrupt stored history (independent copies).
- **Concurrent-resume detection:** two resumers load the same `Latest` and both append the next revision → exactly one succeeds, the other gets `RevisionConflictError`; `History` shows no fork (`(GraphRunID, Revision)` unique). A single coordinator's sequential appends never conflict.
- **Checkpoint validation on load:** wrong `GraphID` → `GraphMismatchError`; unknown frontier / interrupt / **route (`Routes[].From`/`To`)** vertex → `UnknownVertexError`; undecodable `StepBase`/`State` → `CheckpointDecodeError`; resume a `Completed`/`Cancelled` run → `ResumeTerminalError{Status}`; validation runs before any task executes.
- **Cancellation:** `Cancel(id, reason)` appends a terminal `RunCancelled` checkpoint with `CancelledAt`/`CancelReason`; resume of a cancelled run → `ResumeTerminalError{Status: RunCancelled}`; a worker append that loses to the cancel sees `RevisionConflictError` and stops gracefully (observed cancellation); `OnRunFinish` fires.
- **Idempotency:** `RunInfo.IdempotencyKey()` is **stable across retry and across recovery** (independent of `VertexRunID`/`Attempt`).
- **Interrupt routing:** a vertex pauses mid-step while a sibling completes → siblings **run to completion** (not cancelled), their reducers committed, step checkpointed `StepPaused`, **no routing**; resume re-runs only the paused vertices then routes.
- **Multiple interrupts / route vs pause:** two vertices pausing in one step → both recorded in `Interrupts` (plural), both re-run on resume sharing the payload; a `Route`-policy failure routes to its handler (step is **not** paused), while `Awaiting`/`Pause` pauses the step.
- **Mid-step crash recovery (`StepRunning`):** a checkpoint at `StepRunning` resumes by skipping terminal (`Done`/`Failed`-`Route`) vertices and running **every non-terminal** vertex — both `Pending` *and* already-checkpointed *paused* (with recorded continuation) — then route-or-pause; committed vertices are not re-executed.
- **Resume skips terminal, re-runs paused only:** a step with both a `Failed`-`Route` vertex and a paused vertex → resume re-runs only the paused one.
- **Run-level halts:** `Pick` error → `Halt{HaltCondition}`; empty/out-of-declared target → `Halt{HaltUndeclaredTarget}`; drain without `finish` → `Halt{HaltDeadEnd}`; over `WithMaxSteps` → `Halt{HaltMaxSteps}`. Each sets `Result.Halt`, leaves `Interrupts` empty (**mutually exclusive**), phase `StepHalted`, fires **`OnHalt`** with kind+cause; resume **retries routing/continues**, not a vertex re-run, only while `GraphVersion` still matches. Topology/target/version fixes for an old checkpoint return `GraphVersionMismatchError`.
- **Panic safety:** panics in task / selector / reducer / `Pick` / retry callbacks are recovered to typed errors and follow the error path; a **hook** panic is recovered and discarded (no control-flow effect); the process never crashes; a reducer panic leaves `S` unchanged.
- **OnInterrupt cardinality:** fires exactly once per paused vertex (N concurrent pauses → N calls).
- **Hooks:** callbacks fire with correct IDs/timestamps/revisions; nil skipped; multiple sets accumulate; `OnRunStart`/`OnRunFinish` bracket the run.
- **UUID:** v4 bits; round-trip; malformed → `ParseError`.
- **Fuzz:** `FuzzCheckpointRoundTrip`.
- **Integration (`//go:build integration`):** reserved for future durable stores.

---

## 16. Worked example (the motivating flow)

A `Graph[Request]` where `Request` is the shared state (question, draft, approval flags, ticket id, expert answer, lastErr). Reusable tasks (`RAGDraft`, `ConsultantReview`, `CreateTicket`) are plain functions in v1, bound with selectors/reducers:

- `intake → draft` (RAG drafts; reducer writes `Request.Draft`; `WithRetry`, `WithErrorRoute(ticket)`)
- `draft → review` (consultant)
- `review` conditional: approved → `sendToSales` → `finish`; needs expert → `ticket` (creates a ticket, then `Interrupt` (`Awaiting`)); unsatisfied → `refine → draft` (cycle)
- `ticket` resumes when the expert answers (payload), routing back to `draft` or `review`.

Exercises cycles, conditions, retry + error-route, `Awaiting` interrupt→resume, append-only checkpointing, finish-authoritative completion, and instrumentation timestamps; ships as a runnable `example_test.go`.

---

## 17. Future extensions (explicitly deferred)

- Durable `CheckpointStore` backends (SQLite, Postgres, filesystem).
- **Concurrent-resume *prevention*** — an up-front claim/lease that also skips the duplicate work (v1 already *detects* the race via compare-and-append → `RevisionConflictError`, §10.2).
- **Graph migration** — opt-in resume across a changed `GraphVersion` for known-compatible edits (via a migration function), instead of the strict `GraphVersionMismatchError` (§8.1).
- Task kinds: `SubgraphTask`, `AgentTask` (LLM/RAG at a vertex).
- Durable retry across process restarts and scheduled/delayed resume for long waits.
- Saga-style compensation/rollback on `Errored`.
- Typed-error → different-handler routing (`errors.As` dispatch).
- `WithCloneInputs()` strict mode (deep-clone each vertex input) and a `Clone()` fast-path interface to skip JSON cloning.
- Optional per-step state snapshots for time-travel beyond the append-only history.
- All-predecessors fan-in join.
- Streaming task progress; graph visualization (DOT) export.
- Pluggable serialization codec (gob/registry) for non-JSON-friendly states.
- `WithStrictState()` runtime reducer write-set conflict detection; declared channel write-sets for compile-time detection.

---

## 18. Deployment & distribution (optional layers, beyond the zero-dep core)

The engine core (`pkg/flow`) is a pure, in-process, transport-agnostic library. Everything here is **optional**, layered on top, each in its own package so the core stays zero-dependency. The unifying rule: **`Runner` executes; nothing else does.**

### 18.1 Component decomposition (who owns what)

| Component | Single responsibility | Executes the graph? |
|---|---|---|
| `Runner[S]` — the **worker** | run the super-step loop for one `(GraphID, GraphVersion)` | **Yes** |
| **Registry** | resolve `(GraphID, GraphVersion) → RunnerHandle` (a lookup) | No |
| **Dispatcher** | resolve via registry, invoke the worker, report not-found | No |
| **Ingress** (`ingress`) | carry external requests in/out, map errors | No |
| **Control plane** (`controlplane` / `nats`) | queue + distribute work; feed workers; single-flight per run (§18.5) | No |
| **CheckpointStore** (`flow` / `nats`) | persist durable run state | No |

A `RunnerHandle` is the graph-agnostic, JSON-in/out wrapper of a typed `Runner[S]` (possible because `S` is JSON-serializable). The registry is keyed by `(GraphID, GraphVersion)`, so one process can serve **multiple versions** of a graph and resolve a resume to the matching one. The registry is a **pure resolver** — it neither executes nor routes across workers. The roles above compose into one or more **processes** as your deployment needs (§18.5); routing across processes is the control plane's job, not the registry's.

### 18.2 In-process control surface (`pkg/flow`)

Besides `Run`/`Resume`, `Runner[S]` exposes thin store-backed wrappers: `Status(id)` (latest `GraphRunState`, no execution, no `S` decode), `Get(id)` (latest with decoded `State`), `GraphID()`, `GraphVersion()`, and optional `Cancel(id, reason)` (appends a terminal `RunCancelled` checkpoint with `CancelledAt`/`CancelReason`; a running worker is signalled best-effort via `ctx`, and if its later append loses to the cancel, that `RevisionConflictError` against a now-`Cancelled` latest is read as **observed cancellation** — the worker stops gracefully, not an infra error; a cancelled run cannot resume). "Know status / start / resume" without any transport.

### 18.3 REST ingress (`pkg/ingress`, optional, stdlib only)

A ready `http.Handler` over a `(GraphID, GraphVersion)` registry of `RunnerHandle`s:

| Method + path | Call |
|---|---|
| `GET /v1/graphs` | manifest `{graphID, versions:[…]}` this worker serves (router advertisement) |
| `POST /v1/graphs/{graphID}/runs` | `Run` on current version (`?version=` to pin); response carries `graphVersion` |
| `POST /v1/runs/{id}/resume` | registry resolves by the run's `GraphVersion`; no match → **`409 + X-Graph-Version`** |
| `GET /v1/runs/{id}` | `Status`/`Get` (includes `graphVersion`) |
| `POST /v1/runs/{id}/cancel` | `Cancel` (optional) |
| `GET /v1/runs?graphID=&status=` | `CheckpointQuery.List` (optional store extension) |

Every run response carries `graphVersion`. Typed errors map to status (`GraphRunExistsError`/`ResumeTerminalError`/`RevisionConflictError`/`GraphVersionMismatchError` → 409, validation → 400, unknown → 404, store down → 503). Ships **secure defaults** (explicit server timeouts, TLS ≥1.2, body-size limits), an **auth seam** (`WithAuth`; never bakes a scheme), and an **`Idempotency-Key`** header (below). Because work flows through the control plane (§18.5), the API is **async-first**: ingress **pre-mints the `GraphRunID`** (for start), carries it in the submitted `Work`, and returns **202 + GraphRunID** immediately; read the result via `GET /v1/runs/{id}` or a webhook. The `Idempotency-Key` header dedupes retried POSTs via a persisted `key → GraphRunID` mapping — a repeated key returns the **same** `GraphRunID` without re-submitting. A synchronous response is optional via control-plane request/reply (fine for fast human-in-the-loop pauses).

### 18.4 NATS distribution & durability (`pkg/nats`, optional, external dep)

NATS supplies the substrate for distributed, durable runs while the `Runner` stays the executor:

- **Durability** — `nats.Store` implements `CheckpointStore` on JetStream (KV/object), preserving the append-only + compare-and-append contract (`(GraphRunID, Revision)` unique). Same interface as `MemStore`.
- **Distribution & version routing** — workers subscribe to durable consumers on subjects encoding the version: `work.{graphID}.{graphVersion}.{graphRunID}`. A worker subscribes only to the versions in its registry, so JetStream delivers each run's work to a capable worker — **the version router *is* the subject space**, no separate LB needed. A per-`GraphRunID` work-queue keeps a run **single-flight**.
- **At-least-once is safe** — JetStream may redeliver; the engine's compare-and-append (`RevisionConflictError`) + task `IdempotencyKey()` (§4.1, §10.2) absorb duplicates.
- **Local = embedded, cloud = cluster** — an in-process **embedded** JetStream server gives durable, zero-infra **local** runs (looprig's pattern); pointing at a NATS **cluster** gives multi-worker **cloud** scale and rolling version deploys (old/new-version workers coexist on old/new version subjects until old runs drain).

The core engine never imports NATS; **`pkg/nats`** (providing `nats.ControlPlane` + `nats.Store`) is an opt-in adapter behind the existing `ControlPlane`/`CheckpointStore` interfaces plus a thin worker/dispatcher loop. NATS (`nats.go`, embedded `nats-server`) requires the dependency-approval step (CLAUDE.md) — already sanctioned in looprig. To keep the core module's `go.mod` dependency-free, `pkg/nats` may be a **nested Go module** (its own `go.mod`), so NATS never enters the core module's `go.sum`.

### 18.5 Control plane & queue

The aspect that **accepts work, distributes it, and feeds workers** is the **control plane** — a queue/dispatch interface kept *separate from the `CheckpointStore`* (the queue is transient consume-once dispatch; the store is durable append-only history):

```go
type ControlPlane interface {
    Submit(ctx context.Context, w Work) error                                       // enqueue (Work carries the ingress-minted GraphRunID; §18.3)
    Consume(ctx context.Context, serves []GraphVersionKey) (<-chan Delivery, error) // a worker pulls work for versions it serves
}

// Delivery wraps Work with explicit ack semantics so durable backends (NATS) survive
// worker crashes: Ack only after the run reaches a durable checkpoint; Nack to requeue.
type Delivery struct {
    Work Work
    Ack  func() error // call when the work reaches a QUIESCENT result (completed/interrupted/halted/cancelled, or safely requeued) — drop from the queue
    Nack func() error // requeue for redelivery (transient failure / shedding load)
}
```

- **In service/distributed mode, `Run`/`Resume` flow through the control plane** — ingress *submits*; a worker *consumes* and invokes the local `Runner`. (Tier-A *embed* usage, §18.6, skips the control plane and calls `Runner.Run`/`Resume` directly.)
- **Two impls, mirroring the store:** `controlplane.Mem` (in-process channel — local, ephemeral) and `nats.ControlPlane` (JetStream work streams — durable, distributed). Control plane and `CheckpointStore` are **separate interfaces**, even when both are NATS-backed (two streams).
- **Registration is implicit:** a worker registers by *consuming* the version subjects it serves (its registry) — no registration RPC.
- **Single-flight per run** is a control-plane guarantee (per-`GraphRunID` work-queue / max-in-flight 1), with compare-and-append (`RevisionConflictError`) as the backstop.
- **Ack / redelivery:** a worker `Ack`s a `Delivery` only once the work reaches a **quiescent result** — `Completed`, `Interrupted`, `Halted`, `Cancelled`, or safely failed-and-requeued — **not** merely after the seed/any checkpoint (`Run` appends a seed checkpoint *before* doing useful work). A crash before `Ack` → the backend redelivers, and compare-and-append + `IdempotencyKey()` absorb the duplicate.
- **Async-first** (§18.3): ingress pre-mints the `GraphRunID`, submits, and returns `202 + GraphRunID`; read via `GET /runs/{id}` or webhook. Synchronous responses are optional via request/reply.

**Composition.** The three roles — ingress, control plane, worker (a registry of `Runner`s) — compose into **one or more processes** in `main()`: all in one binary (co-located) or split apart. The control plane coordinates across whatever processes exist; scale by running more. Topology is a deployment choice, not a design concept — there is no special process type.

**Durability tiers — same code, swap the two impls:**

| Tier | ControlPlane | CheckpointStore | Use |
|---|---|---|---|
| Ephemeral local | `controlplane.Mem` | `MemStore` | dev / tests |
| Durable local | `nats.ControlPlane` (embedded) | `nats.Store` (embedded) | single process, survives restart |
| Distributed | `nats.ControlPlane` (cluster) | `nats.Store` (cluster) | multi-process cloud, rolling version deploys |

### 18.6 Composition & usage (`main()` is dependency injection)

Separation costs **one import + a constructor swap** between tiers; everything else is identical. `flow.Serve(ctx, reg, cp)` is the worker loop (consume → resolve via registry → execute), so callers never hand-roll it. The `CheckpointStore` is owned by each compiled `Runner`; ingress may also receive the same store as a read-side/status dependency, but execution never passes a different store around.

**Tier A — embed the library (zero deps, no service):**

```go
import "github.com/ciram-co/flow/pkg/flow"

runner, _ := g.Compile(intakeID, sendToSalesID, flow.WithStore(flow.NewMemStore())) // store set once
res, _ := runner.Run(ctx, Request{Question: q})
res, _  = runner.Resume(ctx, res.Run.GraphRunID, approval) // later, on a webhook — same store
```

**Tier B — plug-and-play HTTP service, in-process (still zero external deps):**

```go
import (
    "github.com/ciram-co/flow/pkg/flow"
    "github.com/ciram-co/flow/pkg/controlplane"
    "github.com/ciram-co/flow/pkg/registry"
    "github.com/ciram-co/flow/pkg/ingress"
)
store := flow.NewMemStore()
runner, _ := g.Compile(intakeID, sendToSalesID, flow.WithStore(store)) // all registered runners use this store
reg := registry.New(); reg.Add(runner)                                 // register by (GraphID, GraphVersion)
cp := controlplane.Mem()
go flow.Serve(ctx, reg, cp)                                             // worker loop
http.ListenAndServe(":8080", ingress.New(reg, cp, store, ingress.WithAuth(auth)))
```

**Tier C — durable + distributed (one import + two constructors changed):**

```go
import "github.com/ciram-co/flow/pkg/nats"          // the only new import
nc := nats.Connect(url)                              // or nats.Embedded() for durable single-process
cp, store := nats.ControlPlane(nc), nats.Store(nc)   // the only change vs Tier B
runner, _ := g.Compile(intakeID, sendToSalesID, flow.WithStore(store))
reg := registry.New(); reg.Add(runner)
go flow.Serve(ctx, reg, cp)
http.ListenAndServe(":8080", ingress.New(reg, cp, store, ingress.WithAuth(auth)))
```

The `B → C` diff is exactly: add `import .../pkg/nats`, swap `controlplane.Mem()`/`flow.NewMemStore()` → `nats.ControlPlane(nc)`/`nats.Store(nc)`, and compile/register runners with that store. Graph, vertices, worker loop, and ingress wiring are unchanged — the payoff of programming to the `CheckpointStore`/`ControlPlane` interfaces. A `cmd/` example ships the Tier-A and Tier-B/C `main.go` as copy-paste starting points.
