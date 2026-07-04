package flow

import (
	"context"
	"fmt"
	"time"
)

// This file defines vertex binding (design §6) and the engine's SINGLE validated
// type-erasure seam. A vertex is a reusable Task[I,O] bound into a Graph[S] via
// AddVertex, given a stable VertexID and two adapters: a selector (S→I) and a
// reducer ((S,O)→S). The selector and reducer are the only glue that knows S;
// the Task stays pure and graph-agnostic (§1.1).
//
// THE SEAM. A Graph[S] stores vertices of heterogeneous I/O in one map, so each
// vertex's I and O must be erased behind closures typed only in S and any. The
// closures box the concrete I/O at the boundary and narrow back via a checked
// type assertion the instant the value re-enters typed code (§6.2). Per CLAUDE.md
// the assertion is the validated seam: a failed assertion is an internal
// invariant violation (the boxer and the closure were built together in
// AddVertex, so it must never happen in normal flow) and returns a typed
// internalTypeError — never a panic, and any never leaks back to a caller. The
// public surface (Selector, Reducer, AddVertex) is fully typed; any lives only
// inside these three closures. EXECUTION (clone-and-commit, retry loop, routing)
// is a later phase; this file is config/structure only.

// Selector derives a vertex's input I from the graph state S. It is read-only:
// it must not mutate S (the coordinator passes the immutable step-base snapshot,
// §6.2).
type Selector[S, I any] func(s S) I

// Reducer folds a vertex's output O into the graph state S, returning an error to
// reject the fold. The coordinator applies it to a clone and commits only on a
// nil error, so a reducer that mutates then errors leaves S unchanged (§6.2).
type Reducer[S, O any] func(s *S, out O) error

// errorRoute is the WithErrorRoute policy (§12.2): on an exhausted/unrecoverable
// vertex error, the record reducer folds the error into S (clone-and-commit) and
// the handler vertex is activated next. Execution of this policy is a later phase.
type errorRoute[S any] struct {
	handler VertexID          // the vertex activated next after the error is recorded
	record  Reducer[S, error] // folds the error into S before activating handler
}

// vertexConfig is the resolved per-vertex policy assembled from VertexOptions
// (§6.3). A zero vertexConfig is the default: no timeout, no retry, and Pause on
// error (errorRoute nil). Execution of these policies is a later phase.
type vertexConfig[S any] struct {
	timeout    time.Duration  // WithTimeout (0 = none)
	retry      *RetryPolicy   // WithRetry (nil = none)
	errorRoute *errorRoute[S] // WithErrorRoute (nil = default Pause)
}

// VertexOption configures a vertex binding's policy at AddVertex (§6.3). Options
// are applied in order, so a later option overrides an earlier one (last-wins).
type VertexOption[S any] func(*vertexConfig[S])

// WithRetry attaches a retry policy to the vertex binding (§12.2). The policy is
// pure config here; the bounded re-run loop is a later phase.
func WithRetry[S any](p RetryPolicy) VertexOption[S] {
	return func(c *vertexConfig[S]) { c.retry = &p }
}

// WithErrorRoute routes an exhausted/unrecoverable vertex error to handler,
// first folding the error into S via record (§12.2). Applying it overrides any
// prior error policy on the binding (last-wins).
func WithErrorRoute[S any](handler VertexID, record Reducer[S, error]) VertexOption[S] {
	return func(c *vertexConfig[S]) {
		c.errorRoute = &errorRoute[S]{handler: handler, record: record}
	}
}

// WithErrorPause selects the default Pause-on-error policy explicitly (§12.2).
// It clears any error route set by an earlier WithErrorRoute on the same binding
// (last-wins ordering), so the vertex pauses as Errored rather than routing.
func WithErrorPause[S any]() VertexOption[S] {
	return func(c *vertexConfig[S]) { c.errorRoute = nil }
}

// WithTimeout sets a per-vertex deadline on the task's ctx (§12.2). Cancellation
// is cooperative; 0 means no deadline.
func WithTimeout[S any](d time.Duration) VertexOption[S] {
	return func(c *vertexConfig[S]) { c.timeout = d }
}

// vertex is a Task bound into a Graph[S] (§6). It is UNEXPORTED and stored in
// Graph.vertices. Its I and O are erased: the three closures below close over the
// binding's concrete I, O, task, selector, and reducer, exposing operations typed
// only in S and any. This is the only place in the engine that holds any for
// domain data, and each closure narrows it immediately (the seam).
type vertex[S any] struct {
	id     VertexID
	config vertexConfig[S]

	// selectInput boxes selector(s): S → any (the concrete I, boxed).
	selectInput func(s S) any

	// execute narrows in to the concrete I, runs the task, and boxes the concrete
	// O: (ctx, any) → (any, error). A wrong-typed in returns an internalTypeError.
	execute func(ctx context.Context, in any) (any, error)

	// applyReducer narrows out to the concrete O and folds it into *S: a wrong-typed
	// out returns an internalTypeError; the reducer's own error is forwarded.
	applyReducer func(s *S, out any) error
}

// AddVertex binds task into g under id with the given selector and reducer (§6.1).
// It is a package-level generic FUNCTION (not a method) because it introduces the
// type parameters I and O that the method receiver *Graph[S] cannot.
//
// It fails fast on a zero or duplicate id, and on a nil task, selector, or
// reducer (Compile re-checks structural invariants later, §8). On success it
// builds the erased vertex[S] — the three seam closures, each narrowing any back
// to I/O the instant it re-enters typed code — applies opts, and stores it in g.
//
// The task == nil guard rejects only a LITERAL nil task interface. A TYPED-NIL
// task — a non-nil interface wrapping a nil pointer, e.g. (*FuncTask[I,O])(nil) —
// is deliberately NOT caught here: detecting it would require reflect, and the
// engine is reflection-free by design (see typeName). Such a value instead
// surfaces at execution as a recovered VertexError when the execute closure calls
// Execute on the nil receiver (§12.5).
func AddVertex[I, O, S any](
	g *Graph[S], id VertexID, task Task[I, O],
	selector Selector[S, I], reducer Reducer[S, O], opts ...VertexOption[S],
) error {
	if id == (VertexID{}) {
		return &BuildError{Op: "AddVertex", Detail: "zero vertex id"}
	}
	if _, exists := g.vertices[id]; exists {
		return &DuplicateVertexError{VertexID: id}
	}
	if task == nil {
		return &BuildError{Op: "AddVertex", Detail: "nil task"}
	}
	if selector == nil {
		return &BuildError{Op: "AddVertex", Detail: "nil selector"}
	}
	if reducer == nil {
		return &BuildError{Op: "AddVertex", Detail: "nil reducer"}
	}

	cfg := vertexConfig[S]{}
	for _, opt := range opts {
		opt(&cfg)
	}

	g.vertices[id] = &vertex[S]{
		id:     id,
		config: cfg,
		// S → any: box the concrete I produced by the typed selector.
		selectInput: func(s S) any { return selector(s) },
		// (ctx, any) → (any, error): narrow in to I (the seam), run the task, box O.
		execute: func(ctx context.Context, in any) (any, error) {
			typed, ok := in.(I)
			if !ok {
				return nil, &internalTypeError{
					Seam: "execute",
					Want: typeName[I](),
					Got:  fmt.Sprintf("%T", in),
				}
			}
			return task.Execute(ctx, typed)
		},
		// (*S, any) → error: narrow out to O (the seam), then fold via the reducer.
		applyReducer: func(s *S, out any) error {
			typed, ok := out.(O)
			if !ok {
				return &internalTypeError{
					Seam: "applyReducer",
					Want: typeName[O](),
					Got:  fmt.Sprintf("%T", out),
				}
			}
			return reducer(s, typed)
		},
	}
	return nil
}

// typeName returns the Go type name of T for an internalTypeError's Want field.
// It uses the zero value's dynamic type rather than reflect, keeping the engine
// reflection-free; the (*T)(nil) trick handles interface and pointer T whose zero
// value would otherwise format as <nil>.
func typeName[T any]() string {
	var zero T
	if any(zero) == nil {
		return fmt.Sprintf("%T", (*T)(nil))
	}
	return fmt.Sprintf("%T", zero)
}
