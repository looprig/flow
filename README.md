# flow

`github.com/looprig/flow` is a Go library for building durable, replayable
workflow engines with Pregel-style super-step execution.

It is meant for long-running business workflows where work may pause for a
human, a ticket, an approval, or another external event, then resume later from
a checkpoint without losing the execution frontier.

The engine itself is not an LLM or agent framework. Agents, RAG calls, tools,
and subgraphs can be built as task kinds on top of the same core.

## Status

Implemented and released. The engine (`pkg/flow`), an in-memory control plane
(`pkg/controlplane`), a graph registry (`pkg/registry`) and an HTTP ingress
(`pkg/ingress`) ship in the root module. A durable checkpoint store over the
neutral `storage.Ledger` contract ships as the separately tagged nested module
[`github.com/looprig/flow/store`](store/README.md).

Known limits:

- The only built-in `CheckpointStore` in the root module is the in-memory
  `flow.MemStore`, and the only `ControlPlane` is the in-memory
  `controlplane.Mem`. Durable checkpoints need `flow/store` (or your own
  `CheckpointStore`); a distributed control plane / worker dispatch adapter is
  not implemented.
- `pkg/ingress` has no notion of run ownership or tenancy: any caller that
  passes the configured `WithAuth` authenticator may resume, get or cancel any
  run id. A deployment that needs tenancy must enforce it in the authenticator
  or a fronting proxy (see the package documentation).

## Install

```sh
go get github.com/looprig/flow@latest
# optional durable checkpoint adapter (nested module, tagged store/vX.Y.Z)
go get github.com/looprig/flow/store@latest
```

## What it is

`flow` lets callers define a typed graph over shared state:

- A `Task[I, O]` is a reusable unit of work (`flow.NewFuncTask` wraps a
  function).
- A vertex binds a task into a `Graph[S]` with a stable `VertexID`.
- Selectors derive task input from graph state: `S -> I`.
- Reducers fold task output back into graph state: `(S, O) -> S`.
- Edges and conditional edges control what runs next.
- Vertices in the same super-step run in parallel.
- Checkpoints make runs durable and resumable.

## Why

Most workflow engines make either graph state or task coupling hard to reason
about. `flow` keeps the split explicit:

- Tasks are graph-agnostic and reusable.
- Graph wiring owns the state adapters.
- Checkpoints store graph state and execution bookkeeping.
- Inputs are re-derived from restored state on resume.
- Stable IDs make execution observable and resumable across process restarts.

## Core semantics

- Execution uses Bulk Synchronous Parallel super-steps.
- All vertices in a step read the same frozen state snapshot.
- Reducers apply after the parallel barrier in stable `VertexID` order.
- A vertex can interrupt (`flow.Interrupt`, `flow.StatefulInterrupt`),
  checkpoint, and return control to the caller.
- `Runner.Resume(ctx, graphRunID, payload)` reloads the latest checkpoint and
  continues; paused vertices read the payload with `flow.ResumePayload[T]`.
- `Runner.Cancel` appends a terminal checkpoint; a cancelled run cannot resume.
- Completed vertex work is skipped on recovery only after its checkpoint is
  durable.
- Side-effecting tasks should use `RunInfo.IdempotencyKey()` (from
  `flow.Info(ctx)`) with external systems that support idempotency.

## Packages

| Package | Purpose |
|---|---|
| `pkg/flow` | The engine: `Graph[S]`, `AddVertex`, `Compile`, `Runner[S]` (`Run`, `Resume`, `Get`, `Status`, `Cancel`), checkpoints, `MemStore`, hooks, typed errors, and `Serve` (the worker loop over a `ControlPlane`). |
| `pkg/controlplane` | In-memory `ControlPlane` implementation (`controlplane.Mem`). |
| `pkg/registry` | Concurrency-safe registry of compiled graphs (`flow.RunnerHandle`), keyed by (graph id, graph version), used by the worker loop and ingress. |
| `pkg/ingress` | HTTP ingress: `GET /v1/graphs`, `POST /v1/graphs/{graphID}/runs`, `POST /v1/runs/{id}/resume`, `GET /v1/runs/{id}`, `POST /v1/runs/{id}/cancel`; `ingress.Server` gives secure `http.Server` defaults. |
| `cmd/embed` | Example main: embeds the engine in-process and drives one interrupt/resume run. |
| `cmd/service` | Example main: registry + in-memory control plane + `flow.Serve` + ingress as an HTTP service. |
| `examples/*` | Runnable `Example` tests: `graph`, `branch`, `interrupt`, `resume`. |
| `store/` | Nested module `github.com/looprig/flow/store` (see below). |

## Usage

Adapted from `examples/graph`:

```go
g := flow.NewGraph[state](graphID)

err := flow.AddVertex(g, doubleID,
    flow.NewFuncTask(func(_ context.Context, v int) (int, error) { return v * 2, nil }),
    func(s state) int { return s.Value },               // selector
    func(s *state, v int) error { s.Value = v; return nil }, // reducer
)
if err != nil {
    return err
}
// ... add finishID the same way, then wire it:
if err := g.AddEdge(doubleID, finishID); err != nil {
    return err
}

runner, err := g.Compile(doubleID, finishID) // flow.WithStore(...) for a durable store
if err != nil {
    return err
}

result, err := runner.Run(ctx, state{Value: 4})
if err != nil {
    return err
}
if result.Run.Status == flow.RunInterrupted {
    // Persist result.Run.GraphRunID and call runner.Resume later.
}
```

## Storage and dispatch boundaries

The core module depends only on its neutral `CheckpointStore` and
`ControlPlane` interfaces; its only direct Looprig dependency is
`github.com/looprig/core` (for `core/uuid`). `architecture_test.go` fails the
build if the core packages pull in `storage`, `flow/store`, NATS or a concrete
store backend.

The optional `github.com/looprig/flow/store` nested module adapts a
caller-supplied `storage.Ledger`; concrete local or distributed backends remain
outside the core module. Distributed worker dispatch would be a separate
adapter, not a package in this repository.

## Where it sits

Tier 1 of the Looprig workspace: `flow -> core`. The nested `flow/store`
module is tier 2 (`flow/store -> flow, storage`, with test-only `core` and
`fsstore`) and releases on its own cadence at `store/vX.Y.Z` tags.

## Development

The Go baseline is 1.26.8. Every Makefile target runs with `GOWORK=off`.

```sh
GOWORK=off go test ./...   # standalone tests
make check                 # fmt-check, vet, staticcheck, gosec, govulncheck, race tests, build
make architecture          # core-has-no-backend dependency guard
```

`go test ./...` in the root stops at the nested `store/` module; run its checks
from `store/` (`make check` there as well).

Design documents live in [`docs/plans/`](docs/plans/):

- [Engine design](docs/plans/2026-06-24-flow-engine-design.md)
- [Implementation plan](docs/plans/2026-06-26-flow-engine-implementation.md)
- [Run observability design](docs/plans/2026-07-08-flow-run-observability-design.md)

See [CONTRIBUTING.md](CONTRIBUTING.md) for contribution rules.

## License

Apache License 2.0. See [LICENSE](LICENSE).
