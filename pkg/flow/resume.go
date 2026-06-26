package flow

import (
	"context"
	"encoding/json"
	"sort"
	"sync"
	"time"
)

// This file implements Resume (design §9.3, §10.4): the public Runner.Resume
// entrypoint, the §10.4 validate-on-load checks that treat a loaded checkpoint as
// UNTRUSTED input and fail fast with a typed engine error BEFORE any task runs,
// and the four-phase continuation that rebuilds the BSP coordinator from the
// checkpoint and re-enters the super-step machinery (engine.go).
//
// SECURITY (CLAUDE.md): a checkpoint loaded from a CheckpointStore is untrusted.
// validateCheckpoint runs every §10.4 rule before reconstruct decodes S or any
// task executes, so a corrupt or mismatched checkpoint fails secure with no work
// done. The decode of S is the sanctioned serialization boundary (into the
// concrete S, never an any flowing into business logic).
//
// PHASE CONTINUATION (§9.3). Resume distinguishes Terminal vertices (work
// committed — always skipped) from Paused vertices (re-run on resume) from the
// checkpoint alone, then continues by Phase: StepRouted re-enters the loop from
// the routed frontier; StepPaused/StepRunning re-run the paused/non-terminal
// vertices (injecting the Resume payload and each vertex's recorded continuation)
// then route-or-pause the whole step; StepHalted retries routing/continuation
// against the committed state — never a vertex re-run.

// Resume continues an interrupted run from its latest checkpoint (§9.3). It loads
// Latest, validates it against the compiled graph (§10.4) — returning a typed
// engine error on ANY validation failure BEFORE any task runs — then reconstructs
// the coordinator and continues based on the checkpoint's Phase. payload is the
// live value every re-run paused vertex reads via ResumePayload[T] (§9.7); the
// vertices share the one payload. The error return is for engine/infrastructure
// and validation failures only; a fresh pause or halt on continuation is surfaced
// in the Result exactly like Run (§12.3).
func (r *Runner[S]) Resume(ctx context.Context, id GraphRunID, payload any, opts ...RunOption) (*Result[S], error) {
	cfg := defaultRunConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	cp, err := r.store.Latest(ctx, id)
	if err != nil {
		return nil, err
	}
	state, err := r.validateCheckpoint(cp)
	if err != nil {
		return nil, err
	}

	c := r.reconstruct(ctx, cfg, cp, state)
	return c.continueFrom(ctx, cp, payload)
}

// validateCheckpoint runs every §10.4 rule against the loaded (untrusted)
// checkpoint and returns the decoded state S on success, or the first typed
// validation error. It is the sole trust boundary for a resumed checkpoint: it
// must run to completion before any task executes (fail secure).
func (r *Runner[S]) validateCheckpoint(cp *Checkpoint) (S, error) {
	var zero S
	if err := r.validateIdentity(cp); err != nil {
		return zero, err
	}
	if err := r.validateNotTerminal(cp); err != nil {
		return zero, err
	}
	state, err := r.decodeState(cp)
	if err != nil {
		return zero, err
	}
	if err := r.validateVertices(cp); err != nil {
		return zero, err
	}
	if err := validatePhase(cp); err != nil {
		return zero, err
	}
	return state, nil
}

// validateIdentity enforces the §10.4 graph identity and version rules: the
// checkpoint must belong to this compiled graph (GraphMismatchError) and match its
// fingerprint (GraphVersionMismatchError) — a changed graph cannot resume an old
// checkpoint.
func (r *Runner[S]) validateIdentity(cp *Checkpoint) error {
	if cp.Run.GraphID != r.GraphID() {
		return &GraphMismatchError{Expected: r.GraphID(), Actual: cp.Run.GraphID}
	}
	if cp.Run.GraphVersion != r.GraphVersion() {
		return &GraphVersionMismatchError{Expected: r.GraphVersion(), Actual: cp.Run.GraphVersion}
	}
	return nil
}

// validateNotTerminal enforces the §10.4 rule that a Completed or Cancelled run
// cannot resume (ResumeTerminalError).
func (r *Runner[S]) validateNotTerminal(cp *Checkpoint) error {
	if cp.Run.Status == RunCompleted || cp.Run.Status == RunCancelled {
		return &ResumeTerminalError{Status: cp.Run.Status}
	}
	return nil
}

// decodeState enforces the §10.4 decode rule: State (and StepBase if present)
// decode into the concrete S — the sanctioned serialization boundary. A decode
// failure is a CheckpointDecodeError naming the failing field (fail secure).
func (r *Runner[S]) decodeState(cp *Checkpoint) (S, error) {
	var zero S
	state, err := unmarshalState[S](cp.State)
	if err != nil {
		return zero, &CheckpointDecodeError{Field: "State", Err: err}
	}
	if len(cp.StepBase) > 0 {
		if _, err := unmarshalState[S](cp.StepBase); err != nil {
			return zero, &CheckpointDecodeError{Field: "StepBase", Err: err}
		}
	}
	return state, nil
}

// validateVertices enforces the §10.4 rule that every Frontier, Interrupts[].Vertex,
// and Routes[].From/.To references a vertex in the compiled graph (UnknownVertexError).
func (r *Runner[S]) validateVertices(cp *Checkpoint) error {
	for _, v := range cp.Frontier {
		if err := r.knownVertex(v); err != nil {
			return err
		}
	}
	for _, rec := range cp.Interrupts {
		if err := r.knownVertex(rec.Vertex); err != nil {
			return err
		}
	}
	for _, rt := range cp.Routes {
		if err := r.knownVertex(rt.From); err != nil {
			return err
		}
		for _, to := range rt.To {
			if err := r.knownVertex(to); err != nil {
				return err
			}
		}
	}
	return nil
}

// knownVertex reports an UnknownVertexError if v is not a vertex in the compiled
// graph, the §10.4 endpoint-existence check.
func (r *Runner[S]) knownVertex(v VertexID) error {
	if _, ok := r.graph.vertices[v]; !ok {
		return &UnknownVertexError{VertexID: v}
	}
	return nil
}

// validatePhase enforces the §10.4 Phase/Interrupts/Halt combination rules
// (Interrupts and Halt are mutually exclusive): StepPaused requires ≥1 Interrupts
// and no Halt; StepHalted requires a Halt and no Interrupts; StepRunning allows
// 0+ Interrupts and no Halt; StepRouted allows neither pause-required nor halt. A
// bad combination is a CheckpointDecodeError (fail secure).
func validatePhase(cp *Checkpoint) error {
	bad := func(detail string) error {
		return &CheckpointDecodeError{Field: "Phase", Err: &phaseComboError{detail: detail}}
	}
	switch cp.Phase {
	case StepPaused:
		if len(cp.Interrupts) == 0 || cp.Halt != nil {
			return bad("StepPaused requires ≥1 Interrupts and no Halt")
		}
	case StepHalted:
		if cp.Halt == nil || len(cp.Interrupts) != 0 {
			return bad("StepHalted requires Halt and no Interrupts")
		}
	case StepRunning:
		if cp.Halt != nil {
			return bad("StepRunning must not carry a Halt")
		}
	case StepRouted:
		if cp.Halt != nil || len(cp.Interrupts) != 0 {
			return bad("StepRouted must carry neither Interrupts nor Halt")
		}
	default:
		return bad("unknown Phase")
	}
	return nil
}

// reconstruct rebuilds a coordinator from a validated checkpoint (§9.3): the
// decoded state, the run record reset to RunRunning with the revision sequence
// continued (cp.Run.Revision + 1, so the next checkpoint append continues the
// append-only sequence), the frozen step base carried forward, and finishRan
// re-derived from History (a finish that ran Done in ANY prior step). It does not
// yet write a checkpoint — continueFrom drives the continuation.
func (r *Runner[S]) reconstruct(ctx context.Context, cfg runConfig, cp *Checkpoint, state S) *coordinator[S] {
	rs := cp.Run
	rs.Status = RunRunning
	rs.Revision = cp.Run.Revision + 1
	return &coordinator[S]{
		graph:        r.graph,
		entry:        r.entry,
		finish:       r.finish,
		store:        r.store,
		cfg:          cfg,
		state:        state,
		rs:           rs,
		frontier:     cp.Frontier,
		stepBaseJSON: cp.StepBase,
		finishRan:    r.finishRanInHistory(ctx, cp.Run.GraphRunID),
	}
}

// finishRanInHistory re-derives the finishRan latch (§9.5) by scanning the run's
// History for ANY VertexState{VertexID == finish, Status == VertexDone}: a
// completed finish in any prior step. A paused/errored finish does NOT count, so
// completion (frontier drains after finish ran) is decided correctly on resume —
// notably for a finish-with-out-edges run. A History read failure is treated as
// "not run" (fail secure: the run will dead-end rather than spuriously complete).
func (r *Runner[S]) finishRanInHistory(ctx context.Context, id GraphRunID) bool {
	hist, err := r.store.History(ctx, id)
	if err != nil {
		return false
	}
	for _, cp := range hist {
		for _, vs := range cp.Vertices {
			if vs.VertexID == r.finish && vs.Status == VertexDone {
				return true
			}
		}
	}
	return false
}

// continueFrom dispatches the reconstructed coordinator on the checkpoint's Phase
// (§9.3): StepRouted re-enters the loop from the routed frontier; StepPaused and
// StepRunning re-run the paused/non-terminal vertices then route-or-pause the
// whole step; StepHalted retries routing/continuation. The Phase was validated, so
// the default is unreachable but fails secure.
func (c *coordinator[S]) continueFrom(ctx context.Context, cp *Checkpoint, payload any) (*Result[S], error) {
	switch cp.Phase {
	case StepRouted:
		return c.loop(ctx)
	case StepPaused:
		return c.resumeStep(ctx, cp, cp.Frontier, payload)
	case StepRunning:
		return c.resumeRunning(ctx, cp, payload)
	case StepHalted:
		return c.resumeHalted(ctx, cp)
	default:
		return nil, &CheckpointDecodeError{Field: "Phase", Err: &phaseComboError{detail: "unknown Phase on resume"}}
	}
}

// resumeStep re-runs the rerun vertices of a paused/mid-step checkpoint (§9.3),
// injecting the shared payload and each vertex's recorded continuation, then
// classifies them, folds the already-committed terminal vertices back in for
// routing, finalizes the step (route-or-pause), and — if it advanced — re-enters
// the loop. terminal vertices (Done / Failed-Route) are skipped; their state is
// already committed in c.state. rerun is the set to re-execute.
func (c *coordinator[S]) resumeStep(ctx context.Context, cp *Checkpoint, rerun []VertexID, payload any) (*Result[S], error) {
	c.stepBaseJSON = cp.StepBase
	c.frontier = rerun
	runs, err := c.runRerun(ctx, cp, rerun, payload)
	if err != nil {
		return nil, err
	}
	// Carry the already-committed terminals so every intermediate StepRunning
	// checkpoint this step writes lists the FULL step (§9.3, §10.1 — C1): a crash
	// after such a checkpoint must not drop them, or the next Resume re-runs a Done
	// vertex. Cleared after the boundary so fresh steps in c.loop carry nothing.
	c.carry = terminalStates(cp)
	outcome, err := c.classifyStep(ctx, runs)
	if err != nil {
		return nil, err
	}
	combined := c.foldTerminals(cp, runs, outcome)
	res, done, err := c.finalizeStep(ctx, combined, outcome)
	c.carry = nil
	if err != nil {
		return nil, err
	}
	if done {
		return res, nil
	}
	c.rs.Step++
	return c.loop(ctx)
}

// resumeRunning continues a StepRunning checkpoint (a PerVertex crash mid-step,
// §9.3): the step's full vertex set is the frontier the prior boundary routed
// into; the non-terminal members (Pending — never checkpointed — plus any paused
// ones) re-run, the terminal (Done / Failed-Route) members are skipped, then the
// whole step routes-or-pauses. The step base is the frozen base in the checkpoint.
func (c *coordinator[S]) resumeRunning(ctx context.Context, cp *Checkpoint, payload any) (*Result[S], error) {
	stepFrontier := c.stepFrontier(ctx, cp)
	rerun := nonTerminal(stepFrontier, cp)
	return c.resumeStep(ctx, cp, rerun, payload)
}

// resumeHalted retries routing/continuation for a run-level halt (§9.8) WITHOUT
// re-running any vertex: the step's reducers already committed, so it either
// continues from a genuinely-pending frontier (HaltMaxSteps — the budget refused a
// step before it ran, so its frontier is pending, §9.5) or re-evaluates routing
// against the committed State (HaltCondition/HaltUndeclaredTarget/HaltDeadEnd — the
// recorded frontier is the halting step's OWN, already-run vertices, so it is NOT
// re-run; routing is re-attempted from those committed vertices). The kind, not
// the frontier's emptiness, selects the path: a routing halt's frontier is
// non-empty but must not be re-run.
func (c *coordinator[S]) resumeHalted(ctx context.Context, cp *Checkpoint) (*Result[S], error) {
	if cp.Halt != nil && cp.Halt.Kind == HaltMaxSteps {
		// The budget refused this step before it ran: its frontier is pending.
		c.frontier = cp.Frontier
		return c.loop(ctx)
	}
	// HaltCondition/HaltUndeclaredTarget/HaltDeadEnd: re-route the halting step from
	// its committed vertices against the committed State (no vertex re-run). A routing
	// halt occurs only when no vertex paused, so every committed vertex is Done or
	// Failed-Route; rebuild routable for the Failed-Route ones so they re-activate.
	c.stepBaseJSON = cp.State // routing reads the committed state
	runs := terminalRuns[S](c.graph, cp.Vertices)
	outcome := newStepOutcome[S]()
	for _, vs := range cp.Vertices {
		if isFailedRoute(vs, cp) {
			outcome.routable[vs.VertexID] = c.graph.vertices[vs.VertexID].config.errorRoute.handler
		}
	}
	res, done, err := c.finalizeStep(ctx, runs, outcome)
	if err != nil {
		return nil, err
	}
	if done {
		return res, nil
	}
	c.rs.Step++
	return c.loop(ctx)
}

// runRerun executes the rerun vertices through the normal run machinery, but with
// each vertex's ctx carrying the shared Resume payload and that vertex's recorded
// continuation (§9.3, §9.7). It mirrors runStep's bounded-parallel launch; the
// per-vertex continuation injection is the only difference from a fresh step.
func (c *coordinator[S]) runRerun(ctx context.Context, cp *Checkpoint, rerun []VertexID, payload any) ([]*vertexRun[S], error) {
	stepBase, err := c.stepBaseState(cp)
	if err != nil {
		return nil, err
	}
	c.frontier = rerun
	runs, err := c.prepareRuns(stepBase)
	if err != nil {
		return nil, err
	}
	conts := continuationsByVertex(cp.Interrupts)
	c.execRerunRuns(ctx, runs, payload, conts)
	return runs, nil
}

// execRerunRuns launches the rerun vertices under the concurrency bound (mirroring
// runStep), injecting the shared payload and each vertex's recorded continuation
// into its ctx before execVertex runs the task. The coordinator stamps StartedAt
// and fires OnVertexStart on its own goroutine before launch (race-free), exactly
// as runStep does.
func (c *coordinator[S]) execRerunRuns(ctx context.Context, runs []*vertexRun[S], payload any, conts map[VertexID]json.RawMessage) {
	bound := c.cfg.concurrency
	if bound < 1 {
		bound = 1
	}
	sem := make(chan struct{}, bound)
	var wg sync.WaitGroup
	for _, r := range runs {
		r := r
		sem <- struct{}{}
		r.vs.StartedAt = time.Now()
		c.cfg.hooks.onVertexStart(ctx, r.vs)
		vctx := withResumePayload(ctx, payload)
		vctx = withInterruptState(vctx, conts[r.vs.VertexID])
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			c.execVertex(vctx, r)
		}()
	}
	wg.Wait()
}

// foldTerminals returns the routing run set for a resumed step: the freshly re-run
// vertices PLUS synthesized terminal vertexRuns for the already-committed Done /
// Failed-Route vertices (so computeRoutes routes the WHOLE step), with the
// Failed-Route ones re-added to outcome.routable. The synthesized terminals are
// NOT re-reduced (their reducers already committed into c.state) — they are fed
// only to finalizeStep/computeRoutes for routing. If any rerun vertex paused this
// step, finalizeStep pauses (never routes), so the terminals are inert.
func (c *coordinator[S]) foldTerminals(cp *Checkpoint, runs []*vertexRun[S], outcome *stepOutcome[S]) []*vertexRun[S] {
	rerunSet := make(map[VertexID]struct{}, len(runs))
	for _, r := range runs {
		rerunSet[r.vs.VertexID] = struct{}{}
	}
	combined := runs
	for _, vs := range cp.Vertices {
		if _, isRerun := rerunSet[vs.VertexID]; isRerun {
			continue // the re-run replaces the stale record
		}
		if !isTerminal(vs, cp) {
			continue // a paused vertex not in rerun: it was already re-run
		}
		v := c.graph.vertices[vs.VertexID]
		combined = append(combined, &vertexRun[S]{v: v, vs: vs})
		if isFailedRoute(vs, cp) {
			outcome.routable[vs.VertexID] = c.graph.vertices[vs.VertexID].config.errorRoute.handler
		}
	}
	// Sort by VertexID so routing order matches the normal step path (§9.2, §9.4),
	// keeping the recorded routes/vertices deterministic across resume.
	sort.Slice(combined, func(i, j int) bool {
		return combined[i].vs.VertexID.String() < combined[j].vs.VertexID.String()
	})
	return combined
}

// terminalStates returns the already-committed TERMINAL VertexStates (Done /
// Failed-Route) of a checkpoint's step — the records a resumed step must carry into
// every intermediate StepRunning checkpoint so a mid-resume crash does not drop
// them (§9.3, §10.1). Paused vertices (re-run on resume) are excluded.
func terminalStates(cp *Checkpoint) []VertexState {
	var out []VertexState
	for _, vs := range cp.Vertices {
		if isTerminal(vs, cp) {
			out = append(out, vs)
		}
	}
	return out
}

// stepBaseState decodes the checkpoint's frozen step base into S so re-run
// vertices derive their inputs from the SAME base the original step used (§9.2.2).
// A StepBase absent (the seed has none) falls back to the committed State.
func (c *coordinator[S]) stepBaseState(cp *Checkpoint) (S, error) {
	raw := cp.StepBase
	if len(raw) == 0 {
		raw = cp.State
	}
	state, err := unmarshalState[S](raw)
	if err != nil {
		return state, &CheckpointDecodeError{Field: "StepBase", Err: err}
	}
	return state, nil
}

// stepFrontier returns the full vertex set of the checkpoint's step: the Frontier
// the most recent prior StepRouted boundary routed INTO this step (§10.1: a
// StepRunning checkpoint does not itself carry that frontier, so it is re-derived
// from History). The seed ALWAYS writes a StepRouted boundary at revision 0
// (Frontier == [entry]), so a StepRunning checkpoint (revision ≥ 1) is guaranteed
// at least one prior StepRouted boundary — the empty-frontier fallback to the
// checkpoint's own Frontier is therefore unreachable in a well-formed history and
// kept only as a fail-secure floor (a History read error likewise falls back).
func (c *coordinator[S]) stepFrontier(ctx context.Context, cp *Checkpoint) []VertexID {
	hist, err := c.store.History(ctx, cp.Run.GraphRunID)
	if err != nil {
		return cp.Frontier
	}
	var frontier []VertexID
	for _, h := range hist {
		if h.Run.Revision >= cp.Run.Revision {
			break
		}
		if h.Phase == StepRouted {
			frontier = h.Frontier
		}
	}
	if len(frontier) == 0 {
		return cp.Frontier
	}
	return frontier
}

// --- pure helpers ------------------------------------------------------------

// unmarshalState decodes pre-encoded checkpoint JSON into the concrete S (the
// sanctioned serialization boundary). Empty bytes decode to the zero S.
func unmarshalState[S any](raw json.RawMessage) (S, error) {
	var out S
	if len(raw) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		var zero S
		return zero, err
	}
	return out, nil
}

// continuationsByVertex indexes the recorded StatefulInterrupt continuations by
// vertex so each re-run vertex's ctx can carry its own (§9.3). A record without a
// continuation maps to nil bytes (InterruptState then reports absent).
func continuationsByVertex(records []InterruptRecord) map[VertexID]json.RawMessage {
	out := make(map[VertexID]json.RawMessage, len(records))
	for _, rec := range records {
		out[rec.Vertex] = rec.Continuation
	}
	return out
}

// nonTerminal returns the members of the step frontier that must re-run on resume
// (§9.3): a vertex with no Terminal record (Done / Failed-Route) in the
// checkpoint — i.e. not-yet-checkpointed (Pending) OR checkpointed paused
// (Interrupted / Failed-Pause). It consults isTerminal so a Failed-Pause (Failed
// AND present in Interrupts) correctly re-runs while a Failed-Route is skipped.
// VertexID-sorted for deterministic order.
func nonTerminal(stepFrontier []VertexID, cp *Checkpoint) []VertexID {
	terminal := make(map[VertexID]struct{}, len(cp.Vertices))
	for _, vs := range cp.Vertices {
		if isTerminal(vs, cp) {
			terminal[vs.VertexID] = struct{}{}
		}
	}
	var out []VertexID
	for _, id := range sortFrontier(stepFrontier) {
		if _, done := terminal[id]; !done {
			out = append(out, id)
		}
	}
	return out
}

// terminalRuns synthesizes terminal vertexRuns for every committed vertex of a
// halting step (VertexID-sorted for deterministic routing) so resumeHalted can
// re-route the step without re-running a vertex.
func terminalRuns[S any](graph *Graph[S], vertices []VertexState) []*vertexRun[S] {
	runs := make([]*vertexRun[S], 0, len(vertices))
	for _, vs := range vertices {
		runs = append(runs, &vertexRun[S]{v: graph.vertices[vs.VertexID], vs: vs})
	}
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].vs.VertexID.String() < runs[j].vs.VertexID.String()
	})
	return runs
}

// isTerminal reports whether a checkpointed vertex is Terminal (work committed,
// always skipped on resume, §9.3): Done, or Failed-under-Route. A Failed vertex is
// distinguished from a Failed-under-Pause by the checkpoint: a Failed-Pause appears
// in Interrupts (Errored) AND in Frontier; a Failed-Route does not.
func isTerminal(vs VertexState, cp *Checkpoint) bool {
	if vs.Status == VertexDone {
		return true
	}
	if vs.Status == VertexFailed {
		return !isPausedVertex(vs.VertexID, cp)
	}
	return false
}

// isFailedRoute reports whether vs is a Failed-under-Route vertex (committed its
// record reducer and routes to a handler): Failed and NOT paused (§9.3).
func isFailedRoute(vs VertexState, cp *Checkpoint) bool {
	return vs.Status == VertexFailed && !isPausedVertex(vs.VertexID, cp)
}

// isPausedVertex reports whether v is recorded as paused in the checkpoint: it
// appears in Interrupts (Awaiting or Errored). A Failed vertex that is paused
// (Failed-under-Pause) appears here; a Failed-under-Route does not.
func isPausedVertex(v VertexID, cp *Checkpoint) bool {
	for _, rec := range cp.Interrupts {
		if rec.Vertex == v {
			return true
		}
	}
	return false
}
