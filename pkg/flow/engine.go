package flow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"
)

// errObservedCancellation is the UNEXPORTED sentinel append returns when a lost
// compare-and-append is read as OBSERVED CANCELLATION (§18.2): the coordinator's
// write lost to a concurrent Cancel, so the run must stop gracefully rather than
// surface a *RevisionConflictError. It carries the cancelled GraphRunState from
// the now-Cancelled latest so the converter (gracefulOnCancel) can build the
// terminal Result. It NEVER escapes to callers — run/continueFrom convert it to a
// (Result, nil) before returning.
type errObservedCancellation struct {
	run GraphRunState
}

// Error names the observed cancellation. It is unexported and converted away
// before any public return, so this string is for internal logging only.
func (e *errObservedCancellation) Error() string {
	return "flow: observed cancellation of run " + e.run.GraphRunID.String()
}

// This file is the BSP coordinator (design §9.2): the per-run engine that owns
// one run's GraphRunID, accumulated state S, frontier, and GraphRunState, and is
// the SOLE owner and writer of S (§9.1) — there is no mutex on S because only the
// coordinator goroutine commits it (vertex goroutines return outputs; the
// coordinator reduces them single-threaded in VertexID order, §9.4).
//
// SCOPE: static AND conditional routing (§9.2 steps 1–6, §7) — fan-out (a vertex
// with several static out-edges, or a Condition.Pick returning several declared
// targets, activates them all), fan-in (a vertex reached by several edges is
// deduped to one frontier entry, §9.6), and the run-level halts: the structural
// HaltMaxSteps/HaltDeadEnd and the routing HaltCondition/HaltUndeclaredTarget
// (§9.5/§9.8). Route + terminate are folded into one boundary checkpoint per step
// (finalizeStep). Error policy, interrupts/pause, PerStep behavior, and Resume are
// later sub-tasks. The loop and frontier model are structured so those extensions
// slot in without reworking the skeleton.

// coordinator drives one graph run's super-step loop (§9.2). It is unexported and
// single-use: Run builds one per call. It is the sole writer of state; the
// frontier is the current super-step's active vertex set. cfg is the resolved
// per-run configuration; rs is the framework-owned run-level record updated and
// checkpointed at every step boundary. stepBaseJSON holds the current step's
// frozen read snapshot S_N (the marshaled committed state at the step's start),
// captured in runStep before any vertex runs so every checkpoint written within
// the step carries the SAME base even as state advances during reduce (§9.2.2,
// §10.1). It is nil before the first step (the seed has no frozen base).
// finishRan records whether the finish vertex has executed in ANY step so far —
// completion (§9.5) requires finish to have run at least once and then the
// frontier to drain, so this is a run-spanning latch, NOT a this-step check.
type coordinator[S any] struct {
	graph        *Graph[S]
	entry        VertexID
	finish       VertexID
	store        CheckpointStore
	cfg          runConfig
	state        S
	rs           GraphRunState
	frontier     []VertexID
	stepBaseJSON json.RawMessage
	finishRan    bool
	// nextRev is the revision the next checkpoint will be written at; c.rs.Revision
	// tracks the LAST written revision, so Result.Run.Revision matches the store's
	// Latest. append sets c.rs.Revision = nextRev then advances nextRev on success.
	nextRev uint64
	// carry holds the already-committed TERMINAL VertexStates (Done / Failed-Route)
	// of the step a Resume is re-running, so every intermediate PerVertex
	// StepRunning checkpoint written during that resumed step lists the FULL step,
	// not just the re-run subset (§9.3, §10.1). Without it a crash after an
	// intermediate checkpoint would drop those terminals and the next Resume would
	// re-run a Done vertex. It is nil on the Run path (the full frontier is always
	// in runs) and on fresh steps within a resumed run's loop; resumeStep sets it
	// before classifyStep and clears it after finalizeStep.
	carry []VertexState
}

// newCoordinator constructs a coordinator for one run from the runner, the
// resolved config, the run id, and the seed input. It stamps the run identity and
// the initial seed state but does not yet write any checkpoint — run() drives the
// loop.
func newCoordinator[S any](r *Runner[S], cfg runConfig, id GraphRunID, in S) *coordinator[S] {
	return &coordinator[S]{
		graph:  r.graph,
		entry:  r.entry,
		finish: r.finish,
		store:  r.store,
		cfg:    cfg,
		state:  in,
		rs: GraphRunState{
			GraphRunID:   id,
			GraphID:      r.graph.id,
			GraphVersion: r.version,
		},
	}
}

// vertexRun pairs a vertex's per-execution record with its task output, carrying
// one frontier vertex's result from the run phase to the classify-and-reduce
// phase. err is the outcome of runWithRetry (§12.2): nil on success, the
// interrupt signal on a flow.Interrupt pause (passed through unwrapped), or a
// *VertexError on a task failure after retry. out is set only when err is nil (a
// failed/paused task's partial output is meaningless, §12.2). The coordinator
// classifies each run off err, never off out, so a non-nil err drives the
// reduce/route/pause decision.
type vertexRun[S any] struct {
	v   *vertex[S]
	vs  VertexState
	in  any
	out any
	err error
}

// run executes the full super-step loop to a terminal state (§9.2). It seeds the
// run, then loops: at the loop top the step budget (WithMaxSteps) is a real
// run-level HaltMaxSteps (§9.5) — when the pending frontier would push past the
// budget the run halts with that frontier recorded, so a resume with a higher
// WithMaxSteps continues from it. Otherwise it freezes inputs, runs the frontier,
// reduces + checkpoints, then finalizes (route + terminate folded into one
// boundary checkpoint): finalizeStep returns the final Result when the run
// completes or dead-ends, else advances the step. The error return is for
// engine/infrastructure failures only (store, id, serialization — §12.3); a halt
// is surfaced in Result.Halt, NEVER as an engine error (§9.8, §12.3).
func (c *coordinator[S]) run(ctx context.Context) (*Result[S], error) {
	if err := c.seed(ctx); err != nil {
		return c.gracefulOnCancel(nil, err)
	}
	return c.gracefulOnCancel(c.loop(ctx))
}

// gracefulOnCancel converts an *errObservedCancellation sentinel (raised by append
// when a write lost to a concurrent Cancel, §18.2) into a graceful terminal Result
// reflecting the cancellation — Run is the cancelled GraphRunState from the
// now-Cancelled latest and State is the coordinator's last committed state — with a
// NIL error. Any other error (including a genuine *RevisionConflictError) passes
// through unchanged. It is the single conversion seam every public coordinator
// entrypoint (run, continueFrom) applies to its result.
func (c *coordinator[S]) gracefulOnCancel(res *Result[S], err error) (*Result[S], error) {
	var observed *errObservedCancellation
	if errors.As(err, &observed) {
		return &Result[S]{Run: observed.run, State: c.state}, nil
	}
	return res, err
}

// loop drives the super-step loop from the CURRENT frontier and step to a terminal
// state (§9.2). It is the shared engine both run (after seeding) and resume (after
// reconstructing the coordinator from a checkpoint, §9.3) re-enter: at the loop top
// the step budget is a real HaltMaxSteps (§9.5); otherwise it freezes inputs, runs
// the frontier, reduces + checkpoints, then finalizes (route + terminate folded
// into one boundary checkpoint). finalizeStep returns the final Result on
// completion/halt/pause, else advances the step. The error return is for
// engine/infrastructure failures only (§12.3); a halt is in Result.Halt, never an
// engine error.
func (c *coordinator[S]) loop(ctx context.Context) (*Result[S], error) {
	for {
		if int(c.rs.Step) >= c.cfg.maxSteps {
			return c.haltRun(ctx, HaltMaxSteps, &MaxStepsExceededError{Max: c.cfg.maxSteps, Step: c.rs.Step}, nil)
		}
		runs, err := c.runStep(ctx)
		if err != nil {
			return nil, err
		}
		outcome, err := c.classifyStep(ctx, runs)
		if err != nil {
			return nil, err
		}
		res, done, err := c.finalizeStep(ctx, runs, outcome)
		if err != nil {
			return nil, err
		}
		if done {
			return res, nil
		}
		c.rs.Step++
	}
}

// seed performs §9.2.1: stamp the run-level timestamps and Running status, set
// the frontier to [entry], and append the seed checkpoint (revision 0,
// StepRouted, Frontier [entry]) — the routed state into step 0. It fires
// OnRunStart after the durable seed write so a hook never observes a step the
// store has not recorded.
func (c *coordinator[S]) seed(ctx context.Context) error {
	now := time.Now()
	c.rs.Status = RunRunning
	c.rs.Step = 0
	c.rs.CreatedAt = now
	c.rs.StartedAt = now
	c.frontier = []VertexID{c.entry}

	cp, err := c.checkpoint(StepRouted)
	if err != nil {
		return err
	}
	cp.Frontier = []VertexID{c.entry}
	if err := c.append(ctx, cp); err != nil {
		return err
	}
	c.cfg.hooks.onRunStart(ctx, c.rs)
	return nil
}

// runStep performs §9.2.2–9.2.3: freeze the committed state as the step base
// (marshaled into stepBaseJSON, the frozen S_N every checkpoint in this step
// carries), derive each frontier vertex's input via its selector (in VertexID
// order for determinism), then run the frontier in parallel behind a barrier,
// BOUNDED to c.cfg.concurrency concurrent vertices (§9.2.3, default GOMAXPROCS).
//
// A counting semaphore (a buffered channel sized to the bound) throttles launch:
// the COORDINATOR goroutine acquires a slot before stamping StartedAt, firing
// OnVertexStart, and launching, so (a) at most `concurrency` vertices run at
// once, (b) StartedAt/OnVertexStart stay on the coordinator goroutine — race-free
// and reflecting when the vertex actually got a slot — and (c) each worker
// releases its slot on completion. The WaitGroup is still the barrier: reduce
// does not begin until every vertex has finished. On the happy path every result
// has a nil error.
func (c *coordinator[S]) runStep(ctx context.Context) ([]*vertexRun[S], error) {
	stepBase := c.state
	baseJSON, err := json.Marshal(stepBase)
	if err != nil {
		return nil, &codecError{Op: "marshal", Err: err}
	}
	c.stepBaseJSON = baseJSON
	runs, err := c.prepareRuns(stepBase)
	if err != nil {
		return nil, err
	}
	bound := c.cfg.concurrency
	if bound < 1 {
		bound = 1 // defensive clamp: a buffered channel needs a positive size
	}
	sem := make(chan struct{}, bound)
	var wg sync.WaitGroup
	for _, r := range runs {
		r := r
		sem <- struct{}{} // acquire a slot; the coordinator blocks here at the bound
		r.vs.StartedAt = time.Now()
		c.cfg.hooks.onVertexStart(ctx, r.vs)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }() // release the slot on completion
			c.execVertex(ctx, r)
		}()
	}
	wg.Wait()
	return runs, nil
}

// prepareRuns builds one vertexRun per frontier vertex (sorted by VertexID for
// deterministic ordering): it looks up the bound vertex, derives its input from
// the frozen step base via the selector, and mints a Running VertexState stamped
// CreatedAt. It is the §9.2.2 freeze-and-inputs phase.
func (c *coordinator[S]) prepareRuns(stepBase S) ([]*vertexRun[S], error) {
	ordered := sortFrontier(c.frontier)
	runs := make([]*vertexRun[S], 0, len(ordered))
	for _, id := range ordered {
		v := c.graph.vertices[id]
		vrid, err := NewVertexRunID()
		if err != nil {
			return nil, err
		}
		runs = append(runs, &vertexRun[S]{
			v:  v,
			in: v.selectInput(stepBase),
			vs: VertexState{
				VertexID:    id,
				VertexRunID: vrid,
				Step:        c.rs.Step,
				Status:      VertexRunning,
				CreatedAt:   time.Now(),
			},
		})
	}
	return runs, nil
}

// execVertex runs one vertex's task under its retry policy and per-vertex timeout
// (§9.2.3, §12.2), injecting the run identity and self record into ctx so the task
// can read Info/Self. It writes out, err, and Attempt on the vertexRun (read after
// the barrier, so race-free); StartedAt is stamped by the coordinator in runStep
// before launch. WithTimeout (config.timeout > 0) wraps a per-vertex deadline that
// SPANS all retry attempts (§12.2): a task that observes tctx.Done() and returns
// yields a *VertexError wrapping context.DeadlineExceeded via runWithRetry,
// retryable by default. The classify-and-reduce phase drives the outcome off err.
func (c *coordinator[S]) execVertex(ctx context.Context, r *vertexRun[S]) {
	rinfo := RunInfo{
		GraphID:     c.rs.GraphID,
		GraphRunID:  c.rs.GraphRunID,
		VertexID:    r.vs.VertexID,
		VertexRunID: r.vs.VertexRunID,
		Step:        c.rs.Step,
	}
	vctx := withSelf(withRunInfo(ctx, rinfo), r.vs)
	if r.v.config.timeout > 0 {
		tctx, cancel := context.WithTimeout(vctx, r.v.config.timeout)
		defer cancel()
		vctx = tctx
	}
	out, attempts, err := runWithRetry(vctx, rinfo, r.v.config.retry, func(ec context.Context) (any, error) {
		return r.v.execute(ec, r.in)
	})
	r.vs.Attempt = attempts
	r.err = err
	if err == nil {
		r.out = out
	}
}

// stepOutcome accumulates the classify-and-reduce phase's per-step results (§9.2.4)
// so finalizeStep can make the route-or-pause decision (§9.2.5). pauses holds the
// in-memory Interruption per paused vertex (Awaiting or Errored-Pause), records
// the matching durable InterruptRecords (carried in every per-vertex StepRunning
// checkpoint and the final boundary checkpoint, §10.1), and routable maps each
// Failed-under-Route vertex to its error-route handler so computeRoutes can
// activate the handler — but ONLY when no vertex paused (§9.2.5). A non-empty
// pauses means the step pauses and does not route.
type stepOutcome[S any] struct {
	pauses   []Interruption
	records  []InterruptRecord
	routable map[VertexID]VertexID // failed vertex -> error-route handler
}

func newStepOutcome[S any]() *stepOutcome[S] {
	return &stepOutcome[S]{routable: make(map[VertexID]VertexID)}
}

// paused reports whether any vertex in the step reached a paused terminal state
// (Awaiting or Errored-Pause). When true the step PAUSES and never routes (§9.2.5).
func (o *stepOutcome[S]) paused() bool { return len(o.pauses) > 0 }

// classifyStep performs §9.2.4: in VertexID order each vertex reaches exactly ONE
// terminal state via classifyVertex — Done (success reduced), Awaiting
// (flow.Interrupt), Failed-Route (record reduced, handler activated), or
// Errored-Pause (default Pause, or a record-reducer/success-reducer failure). The
// reduce/commit and the per-vertex HOOKS (OnVertexFinish always, OnInterrupt for a
// paused vertex) fire once per vertex in BOTH granularities; ONLY the durable
// per-vertex StepRunning APPEND is gated on PerVertex (§10.1). In PerStep no
// per-vertex checkpoint is written — Latest sits on the prior step boundary and a
// crash re-runs the whole frontier from StepBase. finishRan latches only on a Done
// finish (a paused/failed finish has not executed to completion, §9.5).
func (c *coordinator[S]) classifyStep(ctx context.Context, runs []*vertexRun[S]) (*stepOutcome[S], error) {
	out := newStepOutcome[S]()
	for _, r := range runs {
		if err := c.classifyVertex(r, out); err != nil {
			return nil, err
		}
		if r.vs.Status == VertexDone && r.vs.VertexID == c.finish {
			c.finishRan = true
		}
		if c.cfg.granularity == PerVertex {
			if err := c.checkpointVertex(ctx, runs, out.records); err != nil {
				return nil, err
			}
		}
		c.cfg.hooks.onVertexFinish(ctx, r.vs)
		if iv, ok := pauseFor(r, out); ok {
			c.cfg.hooks.onInterrupt(ctx, iv)
		}
	}
	return out, nil
}

// classifyVertex drives one vertex to its single terminal state and records its
// effect into out (§9.2.4, §12.2). Success reduces (a success-path reducer error
// falls through to the error policy, §12.5); a flow.Interrupt pauses Awaiting; a
// *VertexError applies the error policy (Route or default Pause). It returns an
// engine error ONLY when an Awaiting pause's record cannot be durably marshaled
// (fail secure, §12.3); every other terminal state is recorded into out.
func (c *coordinator[S]) classifyVertex(r *vertexRun[S], out *stepOutcome[S]) error {
	if r.err == nil {
		err := c.reduceSuccess(r)
		if err == nil {
			return nil
		}
		r.err = err // success-path reducer error follows the error policy (§12.5)
	}
	if sig, ok := asInterrupt(r.err); ok {
		return c.pauseAwaiting(r, sig, out)
	}
	c.applyErrorPolicy(r, out)
	return nil
}

// reduceSuccess clone-and-commits a successful vertex's reducer into the
// accumulator and marks it Done (§9.2.4). On a nil reducer error it commits and
// returns nil; on a reducer error (or recovered reducer panic, §12.5) the clone is
// discarded — S is unchanged — and the error is returned so the caller applies the
// vertex's error policy.
func (c *coordinator[S]) reduceSuccess(r *vertexRun[S]) error {
	next, err := clone(c.state)
	if err != nil {
		return err
	}
	if err := recoverReduce(func() error { return r.v.applyReducer(&next, r.out) }); err != nil {
		return err
	}
	c.state = next
	r.vs.Status = VertexDone
	r.vs.CompletedAt = time.Now()
	return nil
}

// pauseAwaiting records a flow.Interrupt pause (§9.2.4): the vertex is Interrupted,
// no reducer runs, and an Awaiting Interruption + InterruptRecord (Info, and the
// continuation if StatefulInterrupt) are accumulated for the step boundary. The
// durable record is built FIRST: if its Info/Continuation cannot be marshaled the
// pause is not recorded at all and a *codecError engine error is returned (fail
// secure, §12.3) — never a partial pause that drops the task's continuation.
func (c *coordinator[S]) pauseAwaiting(r *vertexRun[S], sig *interruptSignal, out *stepOutcome[S]) error {
	rec, err := awaitingRecord(r.vs.VertexID, sig)
	if err != nil {
		return err
	}
	r.vs.Status = VertexInterrupted
	r.vs.InterruptedAt = time.Now()
	out.pauses = append(out.pauses, Interruption{
		GraphRunID: c.rs.GraphRunID,
		Vertex:     r.vs.VertexID,
		Kind:       Awaiting,
		Info:       sig.info,
	})
	out.records = append(out.records, rec)
	return nil
}

// applyErrorPolicy applies a vertex's *VertexError outcome per §12.2: WithErrorRoute
// folds the error via the record reducer and activates the handler next (Failed); a
// record-reducer failure pauses Errored instead (no recursive route). The default
// (no error route) pauses Errored.
func (c *coordinator[S]) applyErrorPolicy(r *vertexRun[S], out *stepOutcome[S]) {
	if route := r.v.config.errorRoute; route != nil {
		if c.applyErrorRoute(r, route) {
			out.routable[r.vs.VertexID] = route.handler
			return
		}
	}
	c.pauseErrored(r, out)
}

// applyErrorRoute clone-and-commits the error-route record reducer (§12.2): on a
// nil error it commits, marks the vertex Failed-under-Route, and reports true
// (routable to the handler). On a record-reducer error (or recovered panic, §12.5)
// the clone is discarded — S unchanged — and it reports false so the vertex pauses
// Errored instead (no recursive route).
func (c *coordinator[S]) applyErrorRoute(r *vertexRun[S], route *errorRoute[S]) bool {
	next, err := clone(c.state)
	if err != nil {
		return false
	}
	if err := recoverReduce(func() error { return route.record(&next, r.err) }); err != nil {
		return false
	}
	c.state = next
	r.vs.Status = VertexFailed
	r.vs.FailedAt = time.Now()
	return true
}

// pauseErrored records a default-Pause failure (§9.2.4, §12.2): the vertex is
// Failed, no reducer runs, and an Errored Interruption (carrying the underlying
// cause) + InterruptRecord (Cause message) are accumulated for the step boundary.
func (c *coordinator[S]) pauseErrored(r *vertexRun[S], out *stepOutcome[S]) {
	r.vs.Status = VertexFailed
	r.vs.FailedAt = time.Now()
	out.pauses = append(out.pauses, Interruption{
		GraphRunID: c.rs.GraphRunID,
		Vertex:     r.vs.VertexID,
		Kind:       Errored,
		Cause:      r.err,
	})
	out.records = append(out.records, InterruptRecord{
		Vertex: r.vs.VertexID,
		Kind:   Errored,
		Cause:  r.err.Error(),
	})
}

// checkpointVertex appends a per-vertex StepRunning checkpoint (PerVertex only,
// §10.1) carrying the step's per-vertex records and the interrupt records
// accumulated so far, so a paused vertex's InterruptRecord (and any continuation)
// survives a crash before the step's final boundary checkpoint. classifyStep gates
// this call on granularity == PerVertex; in PerStep it is never invoked.
//
// The recorded Vertices are this step's re-run records MERGED with c.carry — the
// already-committed terminal records of a resumed step (§9.3) — so an intermediate
// StepRunning checkpoint lists the FULL step. On the Run path c.carry is nil, so
// this is exactly vertexStates(runs); on a resumed step it restores the terminals
// a crash here would otherwise drop, keeping a mid-resume crash recoverable.
func (c *coordinator[S]) checkpointVertex(ctx context.Context, runs []*vertexRun[S], records []InterruptRecord) error {
	cp, err := c.checkpoint(StepRunning)
	if err != nil {
		return err
	}
	cp.Vertices = mergeVertexStates(vertexStates(runs), c.carry)
	cp.Interrupts = records
	return c.append(ctx, cp)
}

// mergeVertexStates returns the union of two per-vertex record sets, deduped by
// VertexID (a record in `runs` wins over a carried one of the same id, though by
// construction they are disjoint — a carried vertex is terminal and never in the
// re-run set) and sorted by VertexID for deterministic checkpoints matching the
// Run path's full-frontier StepRunning records.
func mergeVertexStates(runs, carry []VertexState) []VertexState {
	if len(carry) == 0 {
		return runs
	}
	seen := make(map[VertexID]struct{}, len(runs))
	merged := make([]VertexState, 0, len(runs)+len(carry))
	for _, vs := range runs {
		seen[vs.VertexID] = struct{}{}
		merged = append(merged, vs)
	}
	for _, vs := range carry {
		if _, dup := seen[vs.VertexID]; dup {
			continue
		}
		merged = append(merged, vs)
	}
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].VertexID.String() < merged[j].VertexID.String()
	})
	return merged
}

// pauseFor returns the Interruption recorded for r in this step (if r paused) so
// the caller can fire OnInterrupt exactly once per paused vertex. A vertex pauses
// iff its status is VertexInterrupted (Awaiting) or VertexFailed without a routable
// handler (Errored-Pause); a Failed-under-Route vertex is terminal, not paused.
func pauseFor[S any](r *vertexRun[S], out *stepOutcome[S]) (Interruption, bool) {
	if _, routed := out.routable[r.vs.VertexID]; routed {
		return Interruption{}, false
	}
	for _, iv := range out.pauses {
		if iv.Vertex == r.vs.VertexID {
			return iv, true
		}
	}
	return Interruption{}, false
}

// awaitingRecord builds the durable InterruptRecord for a flow.Interrupt pause,
// marshaling the user reason into Info and (for a StatefulInterrupt) the live
// continuation into Continuation — the §10.1 serialization boundary. A FAILED
// marshal of either present value is an ENGINE error (a *codecError): a pause whose
// record cannot be durably written cannot be a durable pause (fail secure, §12.3),
// so the classify phase aborts the run rather than recording a partial/empty
// interrupt that would silently lose the task's place on resume.
func awaitingRecord(v VertexID, sig *interruptSignal) (InterruptRecord, error) {
	info, err := marshalRaw(sig.info)
	if err != nil {
		return InterruptRecord{}, err
	}
	rec := InterruptRecord{Vertex: v, Kind: Awaiting, Info: info}
	if sig.stateful {
		cont, err := marshalRaw(sig.continuation)
		if err != nil {
			return InterruptRecord{}, err
		}
		rec.Continuation = cont
	}
	return rec, nil
}

// marshalRaw marshals v to a json.RawMessage, returning a *codecError on a marshal
// failure so a non-serializable interrupt payload fails the run securely rather
// than being silently dropped from the checkpoint (fail secure, §10.1, §12.3).
func marshalRaw(v any) (json.RawMessage, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, &codecError{Op: "marshal", Err: err}
	}
	return b, nil
}

// recoverReduce runs a reducer (or error-route record reducer) under panic recovery
// (§12.5): a panic is converted to an error so clone-and-commit discards the clone
// and the vertex follows its error policy rather than crashing the coordinator.
func recoverReduce(f func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
	}()
	return f()
}

// routeHalt signals that routing produced a run-level halt (§9.8): a Condition
// Pick that errored/panicked (HaltCondition) or returned an empty/undeclared
// target set (HaltUndeclaredTarget). A nil *routeHalt means routing succeeded.
// computeRoutes returns it so finalizeStep can convert it to a Result halt before
// the complete/dead-end/advance dispatch.
type routeHalt struct {
	kind  HaltKind
	cause error
}

// finalizeStep performs §9.2.5–9.2.6 as ONE step boundary. The route-or-pause
// decision is mutually exclusive (§9.7/§9.8): if ANY vertex paused (Awaiting or
// Errored-Pause) the step PAUSES — it does NOT route, Failed-Route handlers wait
// for resume — via pauseStep, a terminal Result with Interrupts set and Halt nil.
// Otherwise it routes (computeRoutes, INCLUDING Failed-Route handlers as
// successors), sets c.frontier, writes EXACTLY ONE boundary checkpoint, and decides
// the step's fate. It returns (result, done, err): done with a non-nil result
// completes/halts/pauses the run; done false advances. Non-pause outcomes (in order):
//   - routing halt (a Condition Pick errored/panicked or returned an
//     empty/undeclared target) — HaltCondition/HaltUndeclaredTarget (§9.5/§9.8);
//   - complete (frontier empty AND finish ran in some step) — §9.5;
//   - dead end (frontier empty, finish never ran) — HaltDeadEnd (§9.5/§9.8);
//   - advance (frontier non-empty) — append the StepRouted boundary and continue.
func (c *coordinator[S]) finalizeStep(ctx context.Context, runs []*vertexRun[S], outcome *stepOutcome[S]) (*Result[S], bool, error) {
	if outcome.paused() {
		res, err := c.pauseStep(ctx, runs, outcome)
		return res, true, err
	}
	routes, next, halt := c.computeRoutes(ctx, runs, outcome)
	if halt != nil {
		// Halt.Step here is the step that drained/failed routing (the routing/dead-end
		// sense), not the step refused/not-started (the HaltMaxSteps sense, §9.5).
		res, err := c.haltRun(ctx, halt.kind, halt.cause, runs)
		return res, true, err
	}
	c.frontier = next

	if len(next) == 0 {
		if !c.finishRan {
			// Dead-end Halt.Step is the step whose frontier drained (cf. HaltMaxSteps,
			// whose Halt.Step is the step refused at the loop top).
			res, err := c.haltRun(ctx, HaltDeadEnd, &DeadEndError{Step: c.rs.Step}, runs)
			return res, true, err
		}
		return c.complete(ctx, runs, routes)
	}
	return c.advance(ctx, runs, routes, next)
}

// pauseStep ends the step as a per-vertex pause (§9.2.5, §9.7): with ≥1 paused
// vertex the step never routes. It appends ONE StepPaused boundary checkpoint
// carrying ALL InterruptRecords and re-including the paused vertices in the
// frontier (so resume re-runs them, §9.3), stamps RunInterrupted/InterruptedAt,
// and returns a terminal Result with Interrupts set and Halt nil (mutually
// exclusive, §9.8). OnInterrupt already fired per-vertex in classifyStep.
func (c *coordinator[S]) pauseStep(ctx context.Context, runs []*vertexRun[S], outcome *stepOutcome[S]) (*Result[S], error) {
	c.rs.Status = RunInterrupted
	c.rs.InterruptedAt = time.Now()
	c.frontier = pausedVertices(outcome.pauses)

	cp, err := c.checkpoint(StepPaused)
	if err != nil {
		return nil, err
	}
	cp.Frontier = c.frontier
	cp.Vertices = vertexStates(runs)
	cp.Interrupts = outcome.records
	if err := c.append(ctx, cp); err != nil {
		return nil, err
	}
	return &Result[S]{Run: c.rs, State: c.state, Interrupts: outcome.pauses}, nil
}

// pausedVertices returns the VertexID-sorted set of paused vertices from the
// step's Interruptions, the frontier a StepPaused checkpoint resumes from (§10.1).
func pausedVertices(pauses []Interruption) []VertexID {
	set := newVertexSet()
	for _, iv := range pauses {
		set.addAll([]VertexID{iv.Vertex})
	}
	return set.ordered()
}

// computeRoutes derives, from the step's executed vertices (in VertexID order),
// the routing decisions and the next deduped, VertexID-sorted frontier (§9.5
// routing, §9.6 fan-in dedup). A Done vertex routes via its STATIC out-edges if it
// has any (Conditional false), else its Condition.Pick (Conditional true, §7), else
// it is a sink. A Failed-under-Route vertex (in outcome.routable) routes to its
// error-route handler (Conditional false, §12.2) — reached only when no vertex
// paused, so handlers activate this step. The FIRST routing halt in VertexID order
// wins; on a halt it returns (nil, nil, halt). It mutates nothing.
func (c *coordinator[S]) computeRoutes(ctx context.Context, runs []*vertexRun[S], outcome *stepOutcome[S]) ([]RouteRecord, []VertexID, *routeHalt) {
	var routes []RouteRecord
	next := newVertexSet()
	for _, r := range runs {
		from := r.vs.VertexID
		if handler, ok := outcome.routable[from]; ok {
			routes = append(routes, RouteRecord{From: from, To: []VertexID{handler}, Conditional: false})
			next.addAll([]VertexID{handler})
			continue
		}
		if succ := c.graph.edges[from]; len(succ) > 0 {
			routes = append(routes, RouteRecord{From: from, To: succ, Conditional: false})
			next.addAll(succ)
			continue
		}
		cond, ok := c.graph.conds[from]
		if !ok {
			continue // sink: no static edge, no conditional edge
		}
		picked, halt := c.evalCondition(ctx, from, cond)
		if halt != nil {
			return nil, nil, halt
		}
		routes = append(routes, RouteRecord{From: from, To: picked, Conditional: true})
		next.addAll(picked)
	}
	return routes, next.ordered(), nil
}

// evalCondition runs from's Condition.Pick over the committed state (S_{N+1},
// since computeRoutes runs post-reduce) under panic recovery (§12.5) and validates
// the result (§7/§9.5): a panic or non-nil error is a HaltCondition; an empty set
// or any target outside cond.Targets is a HaltUndeclaredTarget. On success it
// returns the picked targets and a nil halt. Pick is READ-ONLY and gets the plain
// run ctx — routing is a decision, not a task, so no RunInfo/Self is injected.
func (c *coordinator[S]) evalCondition(ctx context.Context, from VertexID, cond Condition[S]) ([]VertexID, *routeHalt) {
	picked, err := recoverPick(ctx, cond.Pick, c.state)
	if err != nil {
		return nil, &routeHalt{kind: HaltCondition, cause: &ConditionError{From: from, Err: err}}
	}
	if len(picked) == 0 {
		return nil, &routeHalt{kind: HaltUndeclaredTarget, cause: &UndeclaredTargetError{From: from}}
	}
	declared := newVertexSet()
	declared.addAll(cond.Targets)
	for _, t := range picked {
		if _, ok := declared.seen[t]; !ok {
			return nil, &routeHalt{kind: HaltUndeclaredTarget, cause: &UndeclaredTargetError{From: from, Target: t}}
		}
	}
	return picked, nil
}

// recoverPick invokes a Condition.Pick under panic recovery (§12.5): a panic is
// converted to an error (so a misbehaving Pick halts the run rather than crashing
// the coordinator), and the Pick's own error is returned unchanged. It is the sole
// trust boundary around user routing code.
func recoverPick[S any](ctx context.Context, pick func(context.Context, S) ([]VertexID, error), s S) (picked []VertexID, err error) {
	defer func() {
		if r := recover(); r != nil {
			picked, err = nil, fmt.Errorf("panic: %v", r)
		}
	}()
	return pick(ctx, s)
}

// complete finalizes a completed run (§9.2.6, §9.5): mark Completed/CompletedAt,
// append the SINGLE StepRouted boundary checkpoint (empty next frontier), fire
// the routing hooks then OnRunFinish, and return the final Result.
func (c *coordinator[S]) complete(ctx context.Context, runs []*vertexRun[S], routes []RouteRecord) (*Result[S], bool, error) {
	c.rs.Status = RunCompleted
	c.rs.CompletedAt = time.Now()
	if err := c.appendRouted(ctx, runs, routes, nil); err != nil {
		return nil, false, err
	}
	c.fireRouting(ctx, routes, len(runs))
	c.cfg.hooks.onRunFinish(ctx, c.rs)
	return &Result[S]{Run: c.rs, State: c.state}, true, nil
}

// advance finalizes a continuing step (§9.2.5): append the SINGLE StepRouted
// boundary checkpoint carrying the next frontier, fire the routing hooks, and
// signal the loop to advance (done false).
func (c *coordinator[S]) advance(ctx context.Context, runs []*vertexRun[S], routes []RouteRecord, next []VertexID) (*Result[S], bool, error) {
	if err := c.appendRouted(ctx, runs, routes, next); err != nil {
		return nil, false, err
	}
	c.fireRouting(ctx, routes, len(runs))
	return nil, false, nil
}

// appendRouted writes one StepRouted boundary checkpoint carrying the routing
// decisions, the next frontier, and the step's per-vertex records. It is the
// single boundary write per step (the route+terminate fold).
func (c *coordinator[S]) appendRouted(ctx context.Context, runs []*vertexRun[S], routes []RouteRecord, next []VertexID) error {
	cp, err := c.checkpoint(StepRouted)
	if err != nil {
		return err
	}
	cp.Frontier = next
	cp.Routes = routes
	cp.Vertices = vertexStates(runs)
	return c.append(ctx, cp)
}

// fireRouting fires OnEdge for every traversed (from, to) edge then OnStep once
// for the step that activated `activated` vertices.
func (c *coordinator[S]) fireRouting(ctx context.Context, routes []RouteRecord, activated int) {
	for _, rec := range routes {
		for _, to := range rec.To {
			c.cfg.hooks.onEdge(ctx, rec.From, to, c.rs)
		}
	}
	c.cfg.hooks.onStep(ctx, c.rs, activated)
}

// haltRun records a run-level halt (§9.5, §9.8) — the structural HaltMaxSteps /
// HaltDeadEnd and the routing HaltCondition / HaltUndeclaredTarget — as a Result,
// NEVER an engine error. Note Halt.Step (c.rs.Step) means the step REFUSED /
// not-started for HaltMaxSteps (caught at the loop top before any vertex runs) but
// the step that DRAINED / FAILED ROUTING for the routing and dead-end halts
// (caught in finalizeStep after the step's vertices reduced). It marks the run
// RunInterrupted/InterruptedAt, appends a StepHalted checkpoint whose StepBase and
// State are BOTH the committed state (a halt's resume re-derives the frontier's
// inputs from the committed state, so the base IS the current state — overriding
// checkpoint()'s stale stepBaseJSON), records the cause and the frontier that
// would have run (so resume can continue/retry), fires OnHalt, and returns a
// Result with Halt set and Interrupts nil (mutually exclusive, §9.8). runs may be
// empty (HaltMaxSteps at the loop top has no per-step runs).
func (c *coordinator[S]) haltRun(ctx context.Context, kind HaltKind, cause error, runs []*vertexRun[S]) (*Result[S], error) {
	c.rs.Status = RunInterrupted
	c.rs.InterruptedAt = time.Now()

	cp, err := c.checkpoint(StepHalted)
	if err != nil {
		return nil, err
	}
	cp.StepBase = cp.State // a halt resumes from the committed state, not a frozen base
	cp.Halt = &HaltRecord{Kind: kind, Step: c.rs.Step, Cause: cause.Error()}
	cp.Frontier = c.frontier
	cp.Vertices = vertexStates(runs)
	if err := c.append(ctx, cp); err != nil {
		return nil, err
	}

	h := &Halt{GraphRunID: c.rs.GraphRunID, Kind: kind, Step: c.rs.Step, Cause: cause}
	c.cfg.hooks.onHalt(ctx, *h)
	return &Result[S]{Run: c.rs, State: c.state, Halt: h}, nil
}

// checkpoint builds a Checkpoint for the given phase, marshaling the accumulated S
// into the State field (the sanctioned JSON serialization boundary, §10.1) and
// carrying the step's frozen base S_N in StepBase (stepBaseJSON, captured in
// runStep). StepBase is nil for the seed (no frozen base yet) and omitted on
// write. It leaves Run unset — append() stamps the authoritative run record,
// revision, and UpdatedAt as the single write point. A non-serializable S
// surfaces here as a *codecError engine failure (fail secure — no partial
// checkpoint).
func (c *coordinator[S]) checkpoint(phase StepPhase) (*Checkpoint, error) {
	state, err := json.Marshal(c.state)
	if err != nil {
		return nil, &codecError{Op: "marshal", Err: err}
	}
	return &Checkpoint{StepBase: c.stepBaseJSON, State: state, Phase: phase}, nil
}

// append stamps the next monotonic revision and UpdatedAt onto the checkpoint and
// the run, durably appends it (compare-and-append, §10.2), and fires OnCheckpoint.
// It is the single point where a checkpoint is written and the revision advances,
// so revisions stay contiguous (0,1,2,…) across every phase. A lost
// compare-and-append against a now-Cancelled latest is read as OBSERVED
// CANCELLATION (§18.2): instead of the *RevisionConflictError it returns the
// unexported *errObservedCancellation sentinel carrying the cancelled run record,
// which every append-caller path converts into a graceful cancellation Result. A
// genuine concurrent-resume conflict (the latest is NOT Cancelled) propagates the
// *RevisionConflictError unchanged.
func (c *coordinator[S]) append(ctx context.Context, cp *Checkpoint) error {
	// c.rs.Revision tracks the LAST written revision; nextRev is the revision to
	// write now (contiguous 0,1,2,…). Setting c.rs.Revision = nextRev before the
	// snapshot makes cp.Run.Revision the written revision AND leaves c.rs — hence
	// Result.Run.Revision — reflecting the latest persisted revision, matching the
	// store's Latest. nextRev advances only AFTER a successful append, so a failed
	// append does not advance the sequence.
	c.rs.Revision = c.nextRev
	c.rs.UpdatedAt = time.Now()
	cp.Run = c.rs
	if err := c.store.Append(ctx, cp); err != nil {
		return c.classifyAppendErr(ctx, err)
	}
	c.cfg.hooks.onCheckpoint(ctx, c.rs.GraphRunID, c.nextRev, c.rs.Step)
	c.nextRev++
	return nil
}

// classifyAppendErr distinguishes a lost compare-and-append that is an OBSERVED
// CANCELLATION from a genuine concurrent-writer conflict (§18.2). On a
// *RevisionConflictError it loads the latest checkpoint: if its run is Cancelled,
// the worker lost to a concurrent Cancel and must stop gracefully, so it returns
// *errObservedCancellation carrying that cancelled GraphRunState. Otherwise (a
// genuine concurrent-resume conflict, or a Latest read failure — fail secure) it
// returns the original error unchanged.
func (c *coordinator[S]) classifyAppendErr(ctx context.Context, err error) error {
	var conflict *RevisionConflictError
	if !errors.As(err, &conflict) {
		return err
	}
	latest, lerr := c.store.Latest(ctx, c.rs.GraphRunID)
	if lerr != nil {
		return err
	}
	if latest.Run.Status == RunCancelled {
		return &errObservedCancellation{run: latest.Run}
	}
	return err
}

// vertexStates extracts the per-vertex records from a step's runs for a
// checkpoint's Vertices field, preserving the (VertexID-sorted) run order.
func vertexStates[S any](runs []*vertexRun[S]) []VertexState {
	out := make([]VertexState, len(runs))
	for i, r := range runs {
		out[i] = r.vs
	}
	return out
}

// sortFrontier returns a copy of ids sorted by their string form, giving the
// coordinator a deterministic, run-stable vertex order for input derivation and
// reduction (§9.2, §9.4).
func sortFrontier(ids []VertexID) []VertexID {
	out := make([]VertexID, len(ids))
	copy(out, ids)
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

// vertexSet is a small dedup helper for building the next frontier from a union
// of successors (§9.6 fan-in dedup). It keeps insertion membership and emits a
// deterministic, VertexID-sorted order.
type vertexSet struct {
	seen map[VertexID]struct{}
}

func newVertexSet() *vertexSet { return &vertexSet{seen: make(map[VertexID]struct{})} }

func (s *vertexSet) addAll(ids []VertexID) {
	for _, id := range ids {
		s.seen[id] = struct{}{}
	}
}

func (s *vertexSet) ordered() []VertexID {
	out := make([]VertexID, 0, len(s.seen))
	for id := range s.seen {
		out = append(out, id)
	}
	return sortFrontier(out)
}
