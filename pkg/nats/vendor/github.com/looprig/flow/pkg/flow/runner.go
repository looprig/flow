package flow

import (
	"context"
	"errors"
	"runtime"
)

// This file is the run ENTRYPOINT and its public configuration surface (design
// §9): the immutable Runner[S] (moved here from compile.go to match the §14
// layout), the Result[S] a run returns, the CheckpointGranularity selector, and
// the RunOption/runConfig pair with its five options. Run resolves the run id,
// builds the resolved runConfig, and hands off to the coordinator (engine.go) —
// which owns the super-step loop and is the sole writer of S (§9.1). Resume,
// Status, Get and the concurrency BOUND are later sub-tasks; this file is the
// Run happy path only.

// Runner is the immutable, validated form of a Graph[S], produced by Compile
// (§8, §9). It is safe to reuse across concurrent runs. It holds the validated
// graph, the entry/finish roles, the GraphVersion fingerprint (§8.1), and the
// single CheckpointStore set at Compile (§9). Per-run behavior (hooks,
// concurrency, maxSteps, granularity) is supplied per call via RunOptions, not
// stored on the Runner, so the Runner itself stays immutable and reusable.
//
// The store is the INTERFACE (CheckpointStore), not a concrete type (dependency
// inversion): the default MemStore is wired at Compile, the composition point, so
// the Runner never depends on a specific backend. Per §9 it is fixed at Compile —
// ALL operations (Run, Resume, Status, Get) use this one store; there is no
// per-run override.
type Runner[S any] struct {
	graph   *Graph[S]
	entry   VertexID
	finish  VertexID
	version string          // GraphVersion fingerprint (§8.1), computed at Compile
	store   CheckpointStore // the single durable store for all ops (§9)
}

// GraphID returns the runner's stable definition identity (§3, §8.1): the pinned
// GraphID of the compiled graph. It is identity, NOT the compatibility key — use
// GraphVersion for resume compatibility.
func (r *Runner[S]) GraphID() GraphID { return r.graph.id }

// GraphVersion returns the compatibility fingerprint computed at Compile (§8.1):
// a sha256 of the graph's topology plus a ":userVersion" suffix. Resume compares
// it to the checkpoint's; any difference is a GraphVersionMismatchError, so a
// changed graph cannot resume an old checkpoint.
func (r *Runner[S]) GraphVersion() string { return r.version }

// Result is the outcome of a Run/Resume (§9). Run is the framework-owned run
// state (ids, status, step, revision, timestamps); State is the final accumulated
// graph state S. Interrupts (per-vertex pauses, §9.7) and Halt (a run-level
// routing/structural halt, §9.8) are mutually exclusive and set only when the run
// is interrupted; on the happy path both are nil. Result.Run.Status is never
// RunRunning on return.
type Result[S any] struct {
	Run        GraphRunState  // ids, status, step, revision, timestamps (§4.1)
	State      S              // final accumulated state
	Interrupts []Interruption // per-vertex pauses (§9.7) — nil on the happy path
	Halt       *Halt          // run-level halt (§9.8) — nil on the happy path
}

// CheckpointGranularity selects WHEN the coordinator writes checkpoints within a
// super-step (§10.1). PerVertex (the default) appends after each vertex reduces,
// so a crash mid-step loses at most one vertex's work; PerStep defers all writes
// to the step boundary for fewer writes at coarser recovery. This sub-task
// implements PerVertex; the PerStep behavior is a later sub-task.
type CheckpointGranularity int

const (
	PerVertex CheckpointGranularity = iota // append after each vertex (default)
	PerStep                                // defer writes to the step boundary
)

// runConfig is the resolved per-run configuration assembled from RunOptions. It
// is unexported: callers configure it only through the RunOption constructors,
// and the coordinator reads it. concurrency is the goroutine bound (default
// GOMAXPROCS; the BOUND is enforced in a later sub-task), maxSteps a safety bound
// on the super-step loop, granularity the checkpoint cadence, and graphRunID an
// optional caller-supplied run id (nil => mint a fresh one).
type runConfig struct {
	hooks       hookDispatcher
	concurrency int
	maxSteps    int
	granularity CheckpointGranularity
	graphRunID  *GraphRunID
}

// RunOption configures a single Run/Resume (§9). Options are applied in order
// over a defaulted runConfig.
type RunOption func(*runConfig)

// WithHooks registers a set of observational lifecycle callbacks for this run
// (§11). It is repeatable: each call appends another set and all fire, in
// registration order.
func WithHooks(h Hooks) RunOption {
	return func(c *runConfig) { c.hooks.add(h) }
}

// WithConcurrency bounds the number of vertices run in parallel within a
// super-step (§9.2). The default is GOMAXPROCS. A value <= 0 leaves the default
// in place. The bound is enforced in a later sub-task; today linear frontiers are
// size 1, so it is carried but not yet limiting.
func WithConcurrency(n int) RunOption {
	return func(c *runConfig) {
		if n > 0 {
			c.concurrency = n
		}
	}
}

// WithMaxSteps sets the super-step budget for this run (§9.5). The default is a
// generous safety bound; a value <= 0 leaves the default in place. Exceeding the
// budget as a run-level HaltMaxSteps is a later sub-task — today this is only a
// runaway guard.
func WithMaxSteps(n int) RunOption {
	return func(c *runConfig) {
		if n > 0 {
			c.maxSteps = n
		}
	}
}

// WithCheckpointEvery selects the checkpoint cadence for this run (§10.1):
// PerVertex (default) or PerStep.
func WithCheckpointEvery(g CheckpointGranularity) RunOption {
	return func(c *runConfig) { c.granularity = g }
}

// WithGraphRunID supplies the GraphRunID for this run instead of minting a fresh
// one (§9). Run rejects an id that already has checkpoint history with a
// GraphRunExistsError — continuing an existing run is Resume's job, not Run's.
func WithGraphRunID(id GraphRunID) RunOption {
	return func(c *runConfig) { c.graphRunID = &id }
}

// defaultRunConfig returns the resolved runConfig before options: the default
// concurrency (GOMAXPROCS), a generous maxSteps safety bound, PerVertex
// granularity, no hooks, and no supplied run id.
func defaultRunConfig() runConfig {
	return runConfig{
		concurrency: runtime.GOMAXPROCS(0),
		maxSteps:    10000,
		granularity: PerVertex,
	}
}

// Run executes the compiled graph from in, driving the BSP super-step loop to a
// terminal state and returning the final Result (§9, §9.2). It mints a fresh
// GraphRunID unless WithGraphRunID supplies one; a supplied id that already has
// checkpoint history is rejected with *GraphRunExistsError. The error return is
// for engine/infrastructure failures only (store unavailable, id generation,
// non-serializable state) — task outcomes are not errors (§12.3). On the happy
// path Result.Run.Status is RunCompleted with no Interrupts and no Halt.
func (r *Runner[S]) Run(ctx context.Context, in S, opts ...RunOption) (*Result[S], error) {
	cfg := defaultRunConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	id, err := r.resolveRunID(ctx, cfg.graphRunID)
	if err != nil {
		return nil, err
	}

	c := newCoordinator(r, cfg, id, in)
	return c.run(ctx)
}

// resolveRunID returns the GraphRunID to use: a freshly minted one when supplied
// is nil, or the supplied id after verifying it has no checkpoint history (fail
// secure — a duplicate start is rejected, not silently appended). A missing
// history (*CheckpointNotFoundError) means the id is free; any other store error
// is propagated; an existing latest checkpoint is a *GraphRunExistsError.
func (r *Runner[S]) resolveRunID(ctx context.Context, supplied *GraphRunID) (GraphRunID, error) {
	if supplied == nil {
		return NewGraphRunID()
	}
	id := *supplied
	_, err := r.store.Latest(ctx, id)
	if err == nil {
		return GraphRunID{}, &GraphRunExistsError{GraphRunID: id}
	}
	var notFound *CheckpointNotFoundError
	if errors.As(err, &notFound) {
		return id, nil
	}
	return GraphRunID{}, err
}
