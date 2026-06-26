package flow

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
)

// This file white-box tests the BSP coordinator's super-step loop (§9.2): seed →
// run → reduce → finalize (static fan-out/fan-in routing folded into one boundary
// checkpoint) → complete / dead-end / advance, the run-level halts (HaltMaxSteps,
// HaltDeadEnd, §9.5/§9.8), plus the durable checkpoint trail and the lifecycle
// hooks. It uses cnt (runner_test.go) as the accumulator and a MemStore to
// inspect the appended history.

// compileChain compiles a linear chain entry -> mids... -> finish where each
// vertex appends its own tag, so cnt.Vals records execution order and threading.
// It returns the Runner, the ordered vertex ids, and the store for inspection.
func compileChain(t *testing.T, store CheckpointStore, tags ...string) (*Runner[cnt], []VertexID) {
	t.Helper()
	if len(tags) == 0 {
		t.Fatal("compileChain needs at least one tag")
	}
	g := NewGraph[cnt](GraphID{})
	ids := make([]VertexID, len(tags))
	for i, tag := range tags {
		ids[i] = vID(byte(i + 1))
		appendVertex(t, g, ids[i], tag)
	}
	for i := 0; i+1 < len(ids); i++ {
		if err := g.AddEdge(ids[i], ids[i+1]); err != nil {
			t.Fatalf("AddEdge(%d): %v", i, err)
		}
	}
	r, err := g.Compile(ids[0], ids[len(ids)-1], WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return r, ids
}

// decodeState unmarshals a checkpoint's State RawMessage back into cnt.
func decodeState(t *testing.T, raw json.RawMessage) cnt {
	t.Helper()
	var s cnt
	if len(raw) == 0 {
		return s
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("decode State: %v", err)
	}
	return s
}

// TestRunSingleVertex covers entry == finish: the one vertex runs once, Result
// carries its reduced output and RunCompleted, and the history holds a seed
// checkpoint (rev 0, StepRouted, Frontier == [entry]) plus a final checkpoint
// whose Run.Status == RunCompleted that Latest also reflects.
func TestRunSingleVertex(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	r, entry := compileSingle(t, store)
	ctx := context.Background()

	res, err := r.Run(ctx, cnt{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.Run.Status != RunCompleted {
		t.Errorf("Status = %v, want RunCompleted", res.Run.Status)
	}
	if len(res.State.Vals) != 1 || res.State.Vals[0] != "only" {
		t.Errorf("Result.State.Vals = %v, want [only]", res.State.Vals)
	}

	hist, err := store.History(ctx, res.Run.GraphRunID)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(hist) < 2 {
		t.Fatalf("History len = %d, want >= 2 (seed + final)", len(hist))
	}
	seed := hist[0]
	if seed.Run.Revision != 0 || seed.Phase != StepRouted {
		t.Errorf("seed: rev=%d phase=%v, want rev=0 phase=StepRouted", seed.Run.Revision, seed.Phase)
	}
	if len(seed.Frontier) != 1 || seed.Frontier[0] != entry {
		t.Errorf("seed.Frontier = %v, want [%v]", seed.Frontier, entry)
	}
	final := hist[len(hist)-1]
	if final.Run.Status != RunCompleted {
		t.Errorf("final checkpoint Status = %v, want RunCompleted", final.Run.Status)
	}

	latest, err := store.Latest(ctx, res.Run.GraphRunID)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if latest.Run.Status != RunCompleted {
		t.Errorf("Latest Status = %v, want RunCompleted", latest.Run.Status)
	}
}

// TestRunLinearChain proves a->b->finish runs each task exactly once in order,
// threads S through (each reducer feeds the next selector / accumulator), and
// ends RunCompleted with monotonically increasing revisions.
func TestRunLinearChain(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	r, _ := compileChain(t, store, "a", "b", "fin")
	ctx := context.Background()

	res, err := r.Run(ctx, cnt{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	wantVals := []string{"a", "b", "fin"}
	if len(res.State.Vals) != len(wantVals) {
		t.Fatalf("Vals = %v, want %v", res.State.Vals, wantVals)
	}
	for i, v := range wantVals {
		if res.State.Vals[i] != v {
			t.Errorf("Vals[%d] = %q, want %q (order)", i, res.State.Vals[i], v)
		}
	}
	if res.State.N != 3 {
		t.Errorf("N = %d, want 3 (S threaded through every reducer)", res.State.N)
	}
	if res.Run.Status != RunCompleted {
		t.Errorf("Status = %v, want RunCompleted", res.Run.Status)
	}

	hist, err := store.History(ctx, res.Run.GraphRunID)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	for i, cp := range hist {
		if cp.Run.Revision != uint64(i) {
			t.Errorf("history[%d].Revision = %d, want %d (monotonic)", i, cp.Run.Revision, i)
		}
	}
	// The last checkpoint's State equals the Result.State.
	last := decodeState(t, hist[len(hist)-1].State)
	if last.N != res.State.N {
		t.Errorf("final checkpoint State.N = %d, want %d", last.N, res.State.N)
	}
}

// TestRunSeedCheckpoint isolates the seed: revision 0 is StepRouted with the
// frontier exactly [entry] and carries the seeded input state.
func TestRunSeedCheckpoint(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	r, ids := compileChain(t, store, "a", "fin")
	ctx := context.Background()

	res, err := r.Run(ctx, cnt{N: 41})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	hist, err := store.History(ctx, res.Run.GraphRunID)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	seed := hist[0]
	if seed.Phase != StepRouted {
		t.Errorf("seed.Phase = %v, want StepRouted", seed.Phase)
	}
	if seed.Run.Revision != 0 {
		t.Errorf("seed.Revision = %d, want 0", seed.Run.Revision)
	}
	if len(seed.Frontier) != 1 || seed.Frontier[0] != ids[0] {
		t.Errorf("seed.Frontier = %v, want [%v]", seed.Frontier, ids[0])
	}
	if got := decodeState(t, seed.State); got.N != 41 {
		t.Errorf("seed.State.N = %d, want 41 (the seeded input)", got.N)
	}
}

// TestRunTimestamps proves the lifecycle timestamps are set and sanely ordered:
// CreatedAt <= StartedAt <= CompletedAt, and CompletedAt is non-zero on a
// completed run.
func TestRunTimestamps(t *testing.T) {
	t.Parallel()

	r, _ := compileChain(t, NewMemStore(), "a", "fin")
	res, err := r.Run(context.Background(), cnt{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	rs := res.Run
	if rs.CreatedAt.IsZero() {
		t.Error("CreatedAt is zero")
	}
	if rs.StartedAt.IsZero() {
		t.Error("StartedAt is zero")
	}
	if rs.CompletedAt.IsZero() {
		t.Error("CompletedAt is zero on a completed run")
	}
	if rs.StartedAt.Before(rs.CreatedAt) {
		t.Error("StartedAt before CreatedAt")
	}
	if rs.CompletedAt.Before(rs.StartedAt) {
		t.Error("CompletedAt before StartedAt")
	}
	if rs.UpdatedAt.IsZero() {
		t.Error("UpdatedAt is zero")
	}
}

// recorder is a concurrency-safe tally of fired hook events for assertion.
type recorder struct {
	mu                        sync.Mutex
	runStart, runFinish       int
	vertexStart, vertexFinish int
	checkpoints               int
	edges, steps              int
	halts                     int
	lastHalt                  Halt
	startStatus, finishStatus RunStatus
	allStartStamped           bool // every OnVertexStart observed a non-zero StartedAt
}

func (rc *recorder) hooks() Hooks {
	return Hooks{
		OnRunStart: func(_ context.Context, ev GraphRunState) {
			rc.mu.Lock()
			defer rc.mu.Unlock()
			rc.runStart++
			rc.startStatus = ev.Status
		},
		OnRunFinish: func(_ context.Context, ev GraphRunState) {
			rc.mu.Lock()
			defer rc.mu.Unlock()
			rc.runFinish++
			rc.finishStatus = ev.Status
		},
		OnVertexStart: func(_ context.Context, ev VertexState) {
			rc.mu.Lock()
			defer rc.mu.Unlock()
			if rc.vertexStart == 0 {
				rc.allStartStamped = true // seed before the AND below
			}
			rc.allStartStamped = rc.allStartStamped && !ev.StartedAt.IsZero()
			rc.vertexStart++
		},
		OnVertexFinish: func(context.Context, VertexState) { rc.mu.Lock(); rc.vertexFinish++; rc.mu.Unlock() },
		OnCheckpoint:   func(context.Context, GraphRunID, uint64, StepID) { rc.mu.Lock(); rc.checkpoints++; rc.mu.Unlock() },
		OnEdge:         func(context.Context, VertexID, VertexID, GraphRunState) { rc.mu.Lock(); rc.edges++; rc.mu.Unlock() },
		OnStep:         func(context.Context, GraphRunState, int) { rc.mu.Lock(); rc.steps++; rc.mu.Unlock() },
		OnHalt: func(_ context.Context, h Halt) {
			rc.mu.Lock()
			defer rc.mu.Unlock()
			rc.halts++
			rc.lastHalt = h
		},
	}
}

// TestRunHooks proves the lifecycle hooks bracket the run and fire per vertex /
// per appended checkpoint. OnRunStart/OnRunFinish fire exactly once each;
// OnVertexStart/OnVertexFinish fire once per vertex; OnCheckpoint fires once per
// appended checkpoint (matching the store's History length).
func TestRunHooks(t *testing.T) {
	t.Parallel()

	rc := &recorder{}
	store := NewMemStore()
	r, _ := compileChain(t, store, "a", "b", "fin")
	ctx := context.Background()

	res, err := r.Run(ctx, cnt{}, WithHooks(rc.hooks()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if rc.runStart != 1 {
		t.Errorf("OnRunStart fired %d times, want 1", rc.runStart)
	}
	if rc.runFinish != 1 {
		t.Errorf("OnRunFinish fired %d times, want 1", rc.runFinish)
	}
	if rc.startStatus != RunRunning {
		t.Errorf("OnRunStart status = %v, want RunRunning", rc.startStatus)
	}
	if rc.finishStatus != RunCompleted {
		t.Errorf("OnRunFinish status = %v, want RunCompleted", rc.finishStatus)
	}
	if rc.vertexStart != 3 {
		t.Errorf("OnVertexStart fired %d times, want 3", rc.vertexStart)
	}
	if rc.vertexFinish != 3 {
		t.Errorf("OnVertexFinish fired %d times, want 3", rc.vertexFinish)
	}
	if !rc.allStartStamped {
		t.Error("OnVertexStart observed a zero StartedAt (must be stamped before the hook fires)")
	}

	hist, err := store.History(ctx, res.Run.GraphRunID)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if rc.checkpoints != len(hist) {
		t.Errorf("OnCheckpoint fired %d times, want %d (one per appended checkpoint)", rc.checkpoints, len(hist))
	}
}

// TestRunVertexStartStamped proves OnVertexStart observes a non-zero StartedAt:
// the coordinator stamps StartedAt before firing the hook (not inside the vertex
// goroutine), so the hook payload is complete and the write is race-free.
func TestRunVertexStartStamped(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	seen := 0
	allStamped := true
	hooks := Hooks{
		OnVertexStart: func(_ context.Context, ev VertexState) {
			mu.Lock()
			defer mu.Unlock()
			seen++
			if ev.StartedAt.IsZero() {
				allStamped = false
			}
		},
	}
	r, _ := compileChain(t, NewMemStore(), "a", "fin")
	if _, err := r.Run(context.Background(), cnt{}, WithHooks(hooks)); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if seen != 2 {
		t.Fatalf("OnVertexStart fired %d times, want 2", seen)
	}
	if !allStamped {
		t.Error("OnVertexStart saw a zero StartedAt")
	}
}

// TestRunStepBase proves every StepRunning/StepRouted checkpoint WITHIN a step
// carries the step's frozen base S_N in StepBase (the committed state at the
// step's start), while the seed checkpoint has an empty StepBase. In a 2-step
// chain, step 1's StepBase equals step 0's committed State (the threading
// contract that lets resume re-derive selector inputs from S_N, §9.2.2/§10.1).
func TestRunStepBase(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	r, _ := compileChain(t, store, "a", "fin")
	ctx := context.Background()

	res, err := r.Run(ctx, cnt{N: 5})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	hist, err := store.History(ctx, res.Run.GraphRunID)
	if err != nil {
		t.Fatalf("History: %v", err)
	}

	// Seed (rev 0): no frozen base yet.
	if len(hist[0].StepBase) != 0 {
		t.Errorf("seed.StepBase = %s, want empty", hist[0].StepBase)
	}

	// Collect, per super-step, the first StepBase seen and the committed State at
	// each StepRouted boundary, to assert step 1's base == step 0's committed state.
	var step0Base, step1Base json.RawMessage
	var step0Committed cnt
	for _, cp := range hist[1:] {
		switch cp.Run.Step {
		case 0:
			if step0Base == nil {
				step0Base = cp.StepBase
			}
			if cp.Phase == StepRouted {
				step0Committed = decodeState(t, cp.State)
			}
		case 1:
			if step1Base == nil {
				step1Base = cp.StepBase
			}
		}
	}

	if len(step0Base) == 0 {
		t.Fatal("step 0 checkpoints carry no StepBase")
	}
	// Step 0's base is the seeded input (N == 5).
	if got := decodeState(t, step0Base); got.N != 5 {
		t.Errorf("step0 StepBase.N = %d, want 5 (the seeded input)", got.N)
	}
	if len(step1Base) == 0 {
		t.Fatal("step 1 checkpoints carry no StepBase")
	}
	// Step 1's frozen base equals step 0's committed accumulated State.
	wantBase, err := json.Marshal(step0Committed)
	if err != nil {
		t.Fatalf("marshal step0Committed: %v", err)
	}
	if string(step1Base) != string(wantBase) {
		t.Errorf("step1 StepBase = %s, want %s (= step0 committed State)", step1Base, wantBase)
	}
}

// TestRunHooksRepeatable proves WithHooks is repeatable: two registered sets both
// fire.
func TestRunHooksRepeatable(t *testing.T) {
	t.Parallel()

	a, b := &recorder{}, &recorder{}
	r, _ := compileChain(t, NewMemStore(), "a", "fin")
	if _, err := r.Run(context.Background(), cnt{}, WithHooks(a.hooks()), WithHooks(b.hooks())); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if a.runStart != 1 || b.runStart != 1 {
		t.Errorf("OnRunStart fired a=%d b=%d, want both 1", a.runStart, b.runStart)
	}
}

// --- Task 6.2: fan-out / fan-in routing + run-level halts ---------------------

// tagVertex binds id into g with the appendVertex contract (selector reads N,
// reducer appends tag and increments N), so cnt.N counts predecessor reductions
// — the merged-state signal a fan-in join's selector observes (§9.6).
func tagVertex(t *testing.T, g *Graph[cnt], id VertexID, tag string) {
	t.Helper()
	appendVertex(t, g, id, tag)
}

// routesFor returns every RouteRecord found across a run's history, in append
// order, so a test can assert the exact (From,To,Conditional) decisions the
// coordinator recorded (§9.5).
func routesFor(t *testing.T, store CheckpointStore, id GraphRunID) []RouteRecord {
	t.Helper()
	hist, err := store.History(context.Background(), id)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	var out []RouteRecord
	for _, cp := range hist {
		out = append(out, cp.Routes...)
	}
	return out
}

// TestRunFanOut proves a vertex with two static out-edges activates BOTH targets
// in the next step; both run; the routing record names From=v To=[a,b]
// (Conditional=false); then both route to finish and the run completes.
func TestRunFanOut(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	v, a, b, fin := vID(1), vID(2), vID(3), vID(4)
	tagVertex(t, g, v, "v")
	tagVertex(t, g, a, "a")
	tagVertex(t, g, b, "b")
	tagVertex(t, g, fin, "fin")
	for _, e := range [][2]VertexID{{v, a}, {v, b}, {a, fin}, {b, fin}} {
		if err := g.AddEdge(e[0], e[1]); err != nil {
			t.Fatalf("AddEdge(%v->%v): %v", e[0], e[1], err)
		}
	}
	r, err := g.Compile(v, fin, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	res, err := r.Run(context.Background(), cnt{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}
	// v, a, b each ran once and finish ran once over the merged fan-in (§9.6):
	// v(+1) then a,b(+2) then fin(+1) == 4.
	if res.State.N != 4 {
		t.Errorf("State.N = %d, want 4 (v + a + b + fin each reduced once)", res.State.N)
	}

	routes := routesFor(t, store, res.Run.GraphRunID)
	var sawFanOut bool
	for _, rec := range routes {
		if rec.From != v {
			continue
		}
		sawFanOut = true
		if rec.Conditional {
			t.Errorf("fan-out route Conditional = true, want false (static)")
		}
		set := newVertexSet()
		set.addAll(rec.To)
		want := set.ordered()
		if len(want) != 2 || want[0] != a || want[1] != b {
			t.Errorf("fan-out To = %v, want [%v %v]", rec.To, a, b)
		}
	}
	if !sawFanOut {
		t.Error("no RouteRecord with From == v (fan-out not recorded)")
	}
}

// TestRunFanIn proves two predecessors edging into one join j dedup to a SINGLE
// frontier entry: j runs exactly once over the merged committed S, observing both
// predecessors' reductions (§9.6). No double-execution.
func TestRunFanIn(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	entry, a, b, j := vID(1), vID(2), vID(3), vID(4)
	tagVertex(t, g, entry, "entry")
	tagVertex(t, g, a, "a")
	tagVertex(t, g, b, "b")
	// j's reducer records the N it OBSERVED via its selector, proving the merge.
	seen := func(s cnt) int { return s.N }
	jReduce := func(s *cnt, observed int) error {
		s.Vals = append(s.Vals, "j")
		s.N = s.N*100 + observed // encode the observed base so the test can decode it
		return nil
	}
	jTask := NewFuncTask(func(_ context.Context, in int) (int, error) { return in, nil })
	if err := AddVertex(g, j, jTask, seen, jReduce); err != nil {
		t.Fatalf("AddVertex(j): %v", err)
	}
	for _, e := range [][2]VertexID{{entry, a}, {entry, b}, {a, j}, {b, j}} {
		if err := g.AddEdge(e[0], e[1]); err != nil {
			t.Fatalf("AddEdge(%v->%v): %v", e[0], e[1], err)
		}
	}
	r, err := g.Compile(entry, j, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	res, err := r.Run(context.Background(), cnt{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}
	// j appears exactly once in Vals: entry, a, b each appended once before j.
	jCount := 0
	for _, v := range res.State.Vals {
		if v == "j" {
			jCount++
		}
	}
	if jCount != 1 {
		t.Errorf("j executed %d times, want exactly 1 (fan-in dedup, §9.6)", jCount)
	}
	// j observed N == 3 (entry + a + b reduced before it ran). Decode: N = 3*100 + 3.
	if res.State.N != 303 {
		t.Errorf("State.N = %d, want 303 (j observed merged N == 3)", res.State.N)
	}
}

// TestRunCycleHaltMaxSteps proves a non-draining cycle (entry -> a -> entry) run
// under WithMaxSteps halts as a run-level HaltMaxSteps: Result.Halt is set, the
// status is RunInterrupted, Interrupts is nil (mutually exclusive), the last
// checkpoint is StepHalted with HaltMaxSteps, OnHalt fired once, and Run returns
// no engine error.
func TestRunCycleHaltMaxSteps(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	entry, a, fin := vID(1), vID(2), vID(3)
	tagVertex(t, g, entry, "entry")
	tagVertex(t, g, a, "a")
	tagVertex(t, g, fin, "fin")
	// entry <-> a is a cycle; fin is reachable (so it compiles) but never routed to.
	for _, e := range [][2]VertexID{{entry, a}, {a, entry}, {a, fin}} {
		if err := g.AddEdge(e[0], e[1]); err != nil {
			t.Fatalf("AddEdge(%v->%v): %v", e[0], e[1], err)
		}
	}
	r, err := g.Compile(entry, fin, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	const max = 4
	rc := &recorder{}
	res, err := r.Run(context.Background(), cnt{}, WithMaxSteps(max), WithHooks(rc.hooks()))
	if err != nil {
		t.Fatalf("Run returned engine error %v, want nil (halt is a Result, not an error)", err)
	}
	if res.Halt == nil {
		t.Fatal("Result.Halt is nil, want a HaltMaxSteps halt")
	}
	if res.Halt.Kind != HaltMaxSteps {
		t.Errorf("Halt.Kind = %v, want HaltMaxSteps", res.Halt.Kind)
	}
	if res.Halt.Step != StepID(max) {
		t.Errorf("Halt.Step = %v, want %d", res.Halt.Step, max)
	}
	var mse *MaxStepsExceededError
	if !errors.As(res.Halt.Cause, &mse) {
		t.Fatalf("Halt.Cause = %v, want *MaxStepsExceededError", res.Halt.Cause)
	}
	if mse.Max != max {
		t.Errorf("MaxStepsExceededError.Max = %d, want %d", mse.Max, max)
	}
	if res.Run.Status != RunInterrupted {
		t.Errorf("Status = %v, want RunInterrupted", res.Run.Status)
	}
	if res.Interrupts != nil {
		t.Errorf("Result.Interrupts = %v, want nil (mutually exclusive with Halt)", res.Interrupts)
	}
	if res.Run.InterruptedAt.IsZero() {
		t.Error("InterruptedAt is zero on a halted run")
	}

	hist, err := store.History(context.Background(), res.Run.GraphRunID)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	last := hist[len(hist)-1]
	if last.Phase != StepHalted {
		t.Errorf("last checkpoint Phase = %v, want StepHalted", last.Phase)
	}
	if last.Halt == nil || last.Halt.Kind != HaltMaxSteps {
		t.Errorf("last checkpoint Halt = %+v, want HaltMaxSteps record", last.Halt)
	}
	if last.Run.Status != RunInterrupted {
		t.Errorf("last checkpoint Status = %v, want RunInterrupted", last.Run.Status)
	}
	// The recorded frontier is the one that WOULD have run, so resume continues.
	if len(last.Frontier) == 0 {
		t.Error("HaltMaxSteps checkpoint Frontier is empty, want the pending frontier for resume")
	}
	rc.mu.Lock()
	halts := rc.halts
	rc.mu.Unlock()
	if halts != 1 {
		t.Errorf("OnHalt fired %d times, want 1", halts)
	}
}

// TestRunDeadEndHalt proves a frontier that drains WITHOUT finish executing halts
// as a run-level HaltDeadEnd. CONSTRUCTION: entry -> a (static); a has no static
// out-edge but a conditional edge to finish, so the graph COMPILES (finish is
// reachable via the conditional Target, which checkReachable follows) yet at
// runtime 6.2 routes only over static edges (graph.edges), so a's static
// successors are empty, the frontier drains, and finish never ran.
func TestRunDeadEndHalt(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	entry, a, fin := vID(1), vID(2), vID(3)
	tagVertex(t, g, entry, "entry")
	tagVertex(t, g, a, "a")
	tagVertex(t, g, fin, "fin")
	if err := g.AddEdge(entry, a); err != nil {
		t.Fatalf("AddEdge(entry->a): %v", err)
	}
	// a's only outward topology is a conditional edge to finish: it satisfies
	// compile reachability but is NOT a static edge, so 6.2 never traverses it.
	cond := Condition[cnt]{
		Targets: []VertexID{fin},
		Pick:    func(_ context.Context, _ cnt) ([]VertexID, error) { return []VertexID{fin}, nil },
	}
	if err := g.AddConditionalEdge(a, cond); err != nil {
		t.Fatalf("AddConditionalEdge(a): %v", err)
	}
	r, err := g.Compile(entry, fin, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	rc := &recorder{}
	res, err := r.Run(context.Background(), cnt{}, WithHooks(rc.hooks()))
	if err != nil {
		t.Fatalf("Run returned engine error %v, want nil (dead end is a Result halt)", err)
	}
	if res.Halt == nil {
		t.Fatal("Result.Halt is nil, want a HaltDeadEnd halt")
	}
	if res.Halt.Kind != HaltDeadEnd {
		t.Errorf("Halt.Kind = %v, want HaltDeadEnd", res.Halt.Kind)
	}
	var dee *DeadEndError
	if !errors.As(res.Halt.Cause, &dee) {
		t.Fatalf("Halt.Cause = %v, want *DeadEndError", res.Halt.Cause)
	}
	if res.Run.Status != RunInterrupted {
		t.Errorf("Status = %v, want RunInterrupted", res.Run.Status)
	}
	if res.Interrupts != nil {
		t.Errorf("Result.Interrupts = %v, want nil (mutually exclusive)", res.Interrupts)
	}

	hist, err := store.History(context.Background(), res.Run.GraphRunID)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	last := hist[len(hist)-1]
	if last.Phase != StepHalted {
		t.Errorf("last checkpoint Phase = %v, want StepHalted", last.Phase)
	}
	if last.Halt == nil || last.Halt.Kind != HaltDeadEnd {
		t.Errorf("last checkpoint Halt = %+v, want HaltDeadEnd record", last.Halt)
	}
	rc.mu.Lock()
	halts := rc.halts
	rc.mu.Unlock()
	if halts != 1 {
		t.Errorf("OnHalt fired %d times, want 1", halts)
	}
}

// TestRunRoutesAttribution proves Checkpoint.Routes reconstructs the exact static
// routing decisions for a multi-edge step (Conditional=false) and OnEdge fires
// once per traversed edge.
func TestRunRoutesAttribution(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	v, a, b, fin := vID(1), vID(2), vID(3), vID(4)
	tagVertex(t, g, v, "v")
	tagVertex(t, g, a, "a")
	tagVertex(t, g, b, "b")
	tagVertex(t, g, fin, "fin")
	for _, e := range [][2]VertexID{{v, a}, {v, b}, {a, fin}, {b, fin}} {
		if err := g.AddEdge(e[0], e[1]); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
	}
	r, err := g.Compile(v, fin, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	rc := &recorder{}
	res, err := r.Run(context.Background(), cnt{}, WithHooks(rc.hooks()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	routes := routesFor(t, store, res.Run.GraphRunID)
	traversed := 0
	for _, rec := range routes {
		if rec.Conditional {
			t.Errorf("route From %v Conditional = true, want false (all static)", rec.From)
		}
		traversed += len(rec.To)
	}
	// Edges traversed: v->a, v->b, a->fin, b->fin == 4.
	if traversed != 4 {
		t.Errorf("recorded %d traversed edges, want 4", traversed)
	}
	rc.mu.Lock()
	edges := rc.edges
	rc.mu.Unlock()
	if edges != traversed {
		t.Errorf("OnEdge fired %d times, want %d (one per traversed edge)", edges, traversed)
	}
}

// TestRunSingleBoundaryCheckpoint proves the route+terminate fold: a completing
// run appends EXACTLY ONE StepRouted checkpoint at the terminal step — there is
// no duplicate adjacent StepRouted at the same step (the prior double-write is
// gone). It also asserts there is one StepRouted boundary per super-step (seed +
// one per step), so no step writes two boundary checkpoints.
func TestRunSingleBoundaryCheckpoint(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	r, _ := compileChain(t, store, "a", "b", "fin")
	ctx := context.Background()

	res, err := r.Run(ctx, cnt{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	hist, err := store.History(ctx, res.Run.GraphRunID)
	if err != nil {
		t.Fatalf("History: %v", err)
	}

	// No two ADJACENT StepRouted checkpoints share the same step (the fold).
	for i := 1; i < len(hist); i++ {
		prev, cur := hist[i-1], hist[i]
		if prev.Phase == StepRouted && cur.Phase == StepRouted && prev.Run.Step == cur.Run.Step {
			t.Errorf("history[%d] and [%d] are both StepRouted at step %d (double boundary write)",
				i-1, i, cur.Run.Step)
		}
	}
	// Exactly one StepRouted BOUNDARY per super-step. The seed (revision 0) is also
	// StepRouted at step 0 but is the routed-INTO-step-0 record, NOT step 0's own
	// boundary; exclude it so the count measures step boundaries only. With the
	// route+terminate fold each step writes exactly one boundary checkpoint.
	perStep := map[StepID]int{}
	for _, cp := range hist {
		if cp.Phase == StepRouted && cp.Run.Revision != 0 {
			perStep[cp.Run.Step]++
		}
	}
	for step, n := range perStep {
		if n > 1 {
			t.Errorf("step %d has %d StepRouted boundary checkpoints, want 1 (single boundary fold)", step, n)
		}
	}
}

// TestRunFinishWithOutEdgeCompletes proves completion tracks whether finish ran
// EVER (not just this step): finish has an out-edge to a sink that drains, so
// finish executes in step k and the sink runs in step k+1, draining the frontier
// — the run still completes (finishRan-ever, §9.5).
func TestRunFinishWithOutEdgeCompletes(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	entry, fin, sink := vID(1), vID(2), vID(3)
	tagVertex(t, g, entry, "entry")
	tagVertex(t, g, fin, "fin")
	tagVertex(t, g, sink, "sink")
	// entry -> fin -> sink; sink has no out-edge. finish executes in step 1, then
	// sink runs in step 2 and the frontier drains: completion requires finishRan-ever.
	for _, e := range [][2]VertexID{{entry, fin}, {fin, sink}} {
		if err := g.AddEdge(e[0], e[1]); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
	}
	r, err := g.Compile(entry, fin, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	res, err := r.Run(context.Background(), cnt{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted (finish ran in an earlier step)", res.Run.Status)
	}
	if res.Halt != nil {
		t.Errorf("Result.Halt = %v, want nil (finish ran, so no dead end)", res.Halt)
	}
	// entry, fin, sink each ran once.
	if res.State.N != 3 {
		t.Errorf("State.N = %d, want 3 (entry + fin + sink)", res.State.N)
	}
}
