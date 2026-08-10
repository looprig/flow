# flows

`flows` is a planned Go library for building durable, replayable workflow
engines with Pregel-style super-step execution.

It is meant for long-running business workflows where work may pause for a
human, a ticket, an approval, or another external event, then resume later from
a checkpoint without losing the execution frontier.

Status: approved design, pre-implementation.

## What It Is

`flows` lets callers define a typed graph over shared state:

- A `Task[I, O]` is a reusable unit of work.
- A vertex binds a task into a `Graph[S]` with a stable `VertexID`.
- Selectors derive task input from graph state: `S -> I`.
- Reducers fold task output back into graph state: `(S, O) -> S`.
- Edges and conditional edges control what runs next.
- Vertices in the same super-step run in parallel.
- Checkpoints make runs durable and resumable.

The engine itself is not an LLM or agent framework. Agents, RAG calls, tools,
and subgraphs can be built later as task kinds on top of the same core.

## Why

Most workflow engines make either graph state or task coupling hard to reason
about. `flows` keeps the split explicit:

- Tasks are graph-agnostic and reusable.
- Graph wiring owns the state adapters.
- Checkpoints store graph state and execution bookkeeping.
- Inputs are re-derived from restored state on resume.
- Stable IDs make execution observable and resumable across process restarts.

This gives a small Go-first workflow kernel for durable, human-in-the-loop
flows without committing the engine to any AI framework.

## Core Semantics

- Execution uses Bulk Synchronous Parallel super-steps.
- All vertices in a step read the same frozen state snapshot.
- Reducers apply after the parallel barrier in stable `VertexID` order.
- A vertex can interrupt, checkpoint, and return control to the caller.
- `Resume(graphRunID, payload)` reloads the latest checkpoint and continues.
- Completed vertex work is skipped on recovery only after its checkpoint is
  durable.
- Side-effecting tasks should use `RunInfo.IdempotencyKey()` with external
  systems that support idempotency.

## Planned Package Shape

```go
g := flow.NewGraph[Request](graphID)

flow.AddVertex(g, draftID, draftTask,
    func(s Request) Query { return Query{Text: s.Question} },
    func(s *Request, out Answer) error {
        s.Draft = out.Text
        return nil
    },
)

runner, err := g.Compile(entryID, finishID)
if err != nil {
    return err
}

result, err := runner.Run(ctx, initialRequest)
if err != nil {
    return err
}

if result.Run.Status == flow.RunInterrupted {
    // Persist result.Run.GraphRunID and resume later.
}
```

## Design Goals

- Typed public API with generic tasks and graph state.
- Deterministic replay from checkpoints.
- Stable IDs for graphs, runs, vertices, and steps.
- Parallel execution within a super-step.
- Append-only checkpoint history.
- Resumable interrupts for human-in-the-loop flows.
- Zero runtime third-party dependencies.
- Race-clean execution under `go test -race`.

## Storage and dispatch boundaries

The core module depends only on its neutral `CheckpointStore` and
`ControlPlane` interfaces. The optional `github.com/looprig/flow/store` nested
module adapts a caller-supplied `storage.Ledger`; concrete local or distributed
backends remain outside the core module. Distributed worker dispatch is a
separate future adapter, not a package in this repository.

## Current Docs

- [Engine design](docs/plans/2026-06-24-flow-engine-design.md)
- [Design review notes](docs/plans/2026-06-25-flow-engine-design-issues.md)
