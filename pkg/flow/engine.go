package flow

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"
)

// This file is the BSP coordinator (design §9.2): the per-run engine that owns
// one run's GraphRunID, accumulated state S, frontier, and GraphRunState, and is
// the SOLE owner and writer of S (§9.1) — there is no mutex on S because only the
// coordinator goroutine commits it (vertex goroutines return outputs; the
// coordinator reduces them single-threaded in VertexID order, §9.4).
//
// SCOPE: the LINEAR happy path — single-vertex frontiers, single static out-edge
// routing, PerVertex checkpoints (§9.2 steps 1–6 for the linear case). Fan-out /
// cycles / HaltMaxSteps, the concurrency bound, conditional routing, error
// policy, interrupts/pause, PerStep behavior, and Resume are later sub-tasks. The
// loop and frontier model are structured so those extensions slot in without
// reworking the skeleton.

// coordinator drives one graph run's super-step loop (§9.2). It is unexported and
// single-use: Run builds one per call. It is the sole writer of state; the
// frontier is the current super-step's active vertex set. cfg is the resolved
// per-run configuration; rs is the framework-owned run-level record updated and
// checkpointed at every step boundary. stepBaseJSON holds the current step's
// frozen read snapshot S_N (the marshaled committed state at the step's start),
// captured in runStep before any vertex runs so every checkpoint written within
// the step carries the SAME base even as state advances during reduce (§9.2.2,
// §10.1). It is nil before the first step (the seed has no frozen base).
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
// one frontier vertex's result from the run phase to the reduce phase.
type vertexRun[S any] struct {
	v   *vertex[S]
	vs  VertexState
	in  any
	out any
}

// run executes the full super-step loop to a terminal state (§9.2). It seeds the
// run, then loops: freeze inputs, run the frontier, reduce + checkpoint, route,
// and terminate when finish has executed and the frontier drains. It returns the
// final Result or an engine/infrastructure error (store, id, or serialization
// failure — §12.3). The maxSteps guard is a runaway safety bound here, not the
// §9.5 HaltMaxSteps feature (a later sub-task); a linear chain always reaches
// finish well within it. The MaxStepsExceededError it returns is BORROWED from
// the §9.5 HaltMaxSteps feature and will be RE-HOMED to a Result.Halt (not an
// engine error) in Task 6.2; today it only surfaces the runaway guard.
func (c *coordinator[S]) run(ctx context.Context) (*Result[S], error) {
	if err := c.seed(ctx); err != nil {
		return nil, err
	}
	for step := 0; step < c.cfg.maxSteps; step++ {
		runs, err := c.runStep(ctx)
		if err != nil {
			return nil, err
		}
		if err := c.reduceStep(ctx, runs); err != nil {
			return nil, err
		}
		next, err := c.routeStep(ctx, runs)
		if err != nil {
			return nil, err
		}
		done, res, err := c.terminate(ctx, runs, next)
		if err != nil {
			return nil, err
		}
		if done {
			return res, nil
		}
		c.rs.Step++
		c.frontier = next
	}
	// Runaway guard only: borrowed from §9.5 HaltMaxSteps, re-homed to Result.Halt
	// in Task 6.2. A valid linear chain reaches finish before this.
	return nil, &MaxStepsExceededError{Max: c.cfg.maxSteps, Step: c.rs.Step}
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
// order for determinism), then run every vertex in parallel behind a barrier. It
// stamps StartedAt and fires OnVertexStart on the COORDINATOR goroutine before
// launch, so the hook observes a non-zero StartedAt and the field is written
// race-free (the vertex goroutine writes only out/Attempt, read after the
// barrier). On the happy path every result has a nil error. A goroutine-per-
// vertex with a WaitGroup is sufficient here; the concurrency BOUND is later.
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
	var wg sync.WaitGroup
	for _, r := range runs {
		r := r
		r.vs.StartedAt = time.Now()
		c.cfg.hooks.onVertexStart(ctx, r.vs)
		wg.Add(1)
		go func() {
			defer wg.Done()
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

// execVertex runs one vertex's task under its retry policy (§9.2.3), injecting the
// run identity and self record into ctx so the task can read Info/Self. It writes
// only out and Attempt on the vertexRun (read after the barrier, so race-free);
// StartedAt is stamped by the coordinator in runStep before launch. On the happy
// path the error is nil; error policy is a later sub-task.
func (c *coordinator[S]) execVertex(ctx context.Context, r *vertexRun[S]) {
	rinfo := RunInfo{
		GraphID:     c.rs.GraphID,
		GraphRunID:  c.rs.GraphRunID,
		VertexID:    r.vs.VertexID,
		VertexRunID: r.vs.VertexRunID,
		Step:        c.rs.Step,
	}
	vctx := withSelf(withRunInfo(ctx, rinfo), r.vs)
	out, attempts, err := runWithRetry(vctx, rinfo, r.v.config.retry, func(ec context.Context) (any, error) {
		return r.v.execute(ec, r.in)
	})
	r.vs.Attempt = attempts
	if err == nil {
		r.out = out
	}
}

// reduceStep performs §9.2.4 (PerVertex): in VertexID order, clone-and-commit
// each vertex's reducer into the accumulator (so a reducer that mutates then
// errors leaves S unchanged), mark the vertex Done, and append a per-vertex
// checkpoint reflecting the new state. It fires OnVertexFinish + OnCheckpoint per
// vertex. The happy path has no reducer/clone error; full error policy is later.
func (c *coordinator[S]) reduceStep(ctx context.Context, runs []*vertexRun[S]) error {
	for _, r := range runs {
		next, err := clone(c.state)
		if err != nil {
			return err
		}
		if err := r.v.applyReducer(&next, r.out); err != nil {
			return err
		}
		c.state = next
		r.vs.Status = VertexDone
		r.vs.CompletedAt = time.Now()

		cp, err := c.checkpoint(StepRunning)
		if err != nil {
			return err
		}
		cp.Vertices = vertexStates(runs)
		if err := c.append(ctx, cp); err != nil {
			return err
		}
		c.cfg.hooks.onVertexFinish(ctx, r.vs)
	}
	return nil
}

// routeStep performs §9.2.5 (static single-edge routing): for each done vertex
// compute its static successors, record the routing decision, and append a
// StepRouted checkpoint carrying the next deduped frontier. It fires OnEdge per
// edge and OnStep once. Conditional routing and error-route handling are later
// sub-tasks; this handles only static AddEdge out-edges.
func (c *coordinator[S]) routeStep(ctx context.Context, runs []*vertexRun[S]) ([]VertexID, error) {
	var routes []RouteRecord
	next := newVertexSet()
	for _, r := range runs {
		succ := c.graph.edges[r.vs.VertexID]
		if len(succ) > 0 {
			routes = append(routes, RouteRecord{From: r.vs.VertexID, To: succ, Conditional: false})
		}
		next.addAll(succ)
	}
	frontier := next.ordered()

	cp, err := c.checkpoint(StepRouted)
	if err != nil {
		return nil, err
	}
	cp.Frontier = frontier
	cp.Routes = routes
	cp.Vertices = vertexStates(runs)
	if err := c.append(ctx, cp); err != nil {
		return nil, err
	}
	c.fireRoutes(ctx, routes)
	c.cfg.hooks.onStep(ctx, c.rs, len(runs))
	return frontier, nil
}

// fireRoutes fires OnEdge for every (from, to) pair in the recorded routes.
func (c *coordinator[S]) fireRoutes(ctx context.Context, routes []RouteRecord) {
	for _, rec := range routes {
		for _, to := range rec.To {
			c.cfg.hooks.onEdge(ctx, rec.From, to, c.rs)
		}
	}
}

// terminate performs §9.2.6: if the finish vertex executed this step and the next
// frontier is empty, mark the run Completed, append a final checkpoint (so Latest
// durably reflects completion), fire OnRunFinish, and return the Result.
// Otherwise it signals the loop to advance to the next step. A linear chain
// always reaches finish, so the happy path always terminates here.
func (c *coordinator[S]) terminate(ctx context.Context, runs []*vertexRun[S], next []VertexID) (bool, *Result[S], error) {
	if len(next) != 0 || !ranFinish(runs, c.finish) {
		return false, nil, nil
	}
	c.rs.Status = RunCompleted
	c.rs.CompletedAt = time.Now()

	cp, err := c.checkpoint(StepRouted)
	if err != nil {
		return false, nil, err
	}
	cp.Vertices = vertexStates(runs)
	if err := c.append(ctx, cp); err != nil {
		return false, nil, err
	}
	c.cfg.hooks.onRunFinish(ctx, c.rs)
	return true, &Result[S]{Run: c.rs, State: c.state}, nil
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
// so revisions stay contiguous (0,1,2,…) across every phase.
func (c *coordinator[S]) append(ctx context.Context, cp *Checkpoint) error {
	c.rs.UpdatedAt = time.Now()
	// cp.Run snapshots the current run-level record, whose Revision is the next to
	// write; Revision is bumped only AFTER a successful append, so revisions stay
	// contiguous (0,1,2,…) and a failed append does not advance the sequence.
	cp.Run = c.rs
	if err := c.store.Append(ctx, cp); err != nil {
		return err
	}
	c.cfg.hooks.onCheckpoint(ctx, c.rs.GraphRunID, c.rs.Revision, c.rs.Step)
	c.rs.Revision++
	return nil
}

// ranFinish reports whether the finish vertex was among the vertices that
// executed this step.
func ranFinish[S any](runs []*vertexRun[S], finish VertexID) bool {
	for _, r := range runs {
		if r.vs.VertexID == finish {
			return true
		}
	}
	return false
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
