package flow

import "context"

// This file defines the engine's task model (design §5). A Task is the reusable,
// graph-agnostic unit of work: a typed In → Out computation that knows nothing
// about any graph or shared state S. It carries no VertexID and no state, so the
// same Task value can be bound into many graphs (each binding produces a distinct
// vertex via AddVertex, §6). FuncTask is the first concrete kind; later kinds
// (SubgraphTask, AgentTask) implement the same interface with zero engine
// changes — which is why the interface is kept minimal (just Execute).

// TaskFunc is a plain typed In → Out computation. It is the function form a
// FuncTask wraps; the engine never calls it directly, only via Task.Execute.
type TaskFunc[I, O any] func(ctx context.Context, in I) (O, error)

// Task is the reusable, graph-agnostic unit of work (§5). Implementations honor
// the contract that they are pure with respect to graph state S (they touch no
// shared state — input is supplied, output is returned) and treat in as
// read-only (§4.2). The interface is intentionally minimal so new kinds plug in
// without engine changes.
type Task[I, O any] interface {
	Execute(ctx context.Context, in I) (O, error)
}

// FuncTask adapts a TaskFunc into a Task. It holds only the function — no
// VertexID, no state — so a single value is safe to reuse across graphs and
// concurrent runs.
type FuncTask[I, O any] struct{ fn TaskFunc[I, O] }

// NewFuncTask wraps fn as a *FuncTask, the first concrete Task[I, O] kind (§5).
func NewFuncTask[I, O any](fn TaskFunc[I, O]) *FuncTask[I, O] {
	return &FuncTask[I, O]{fn: fn}
}

// Execute runs the wrapped TaskFunc, forwarding ctx and in and returning its
// (O, error) verbatim. It holds no per-call state, so repeated calls are
// independent (§5 reusability).
func (t *FuncTask[I, O]) Execute(ctx context.Context, in I) (O, error) {
	return t.fn(ctx, in)
}
