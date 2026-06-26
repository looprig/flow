package flow

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ciram-co/flow/pkg/uuid"
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
// as a run-level HaltDeadEnd — a GENUINE dead end that survives conditional
// routing (§9.5/§9.8). CONSTRUCTION: entry -> a (static); a has a conditional edge
// with Targets [b, fin] whose Pick deterministically returns [b]; b is a TRUE SINK
// (no static OR conditional out-edge) and b != fin. The graph COMPILES because fin
// is reachable as a declared conditional Target, yet at runtime the run routes
// a -> b, b drains, and finish never executes — a real dead end no router can
// rescue, distinct from the prior static-only-ignored-conditional fixture.
func TestRunDeadEndHalt(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	entry, a, b, fin := vID(1), vID(2), vID(3), vID(4)
	tagVertex(t, g, entry, "entry")
	tagVertex(t, g, a, "a")
	tagVertex(t, g, b, "b")
	tagVertex(t, g, fin, "fin")
	if err := g.AddEdge(entry, a); err != nil {
		t.Fatalf("AddEdge(entry->a): %v", err)
	}
	// a's conditional edge declares [b, fin] (so fin is reachable and compile
	// passes) but Pick always routes to the SINK b; b has no out-edge at all.
	cond := Condition[cnt]{
		Targets: []VertexID{b, fin},
		Pick:    func(_ context.Context, _ cnt) ([]VertexID, error) { return []VertexID{b}, nil },
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

// --- Task 6.3: bounded parallelism within a super-step + snapshot safety ------

// pstate is a parallel-fan-out accumulator whose StepBase carries REFERENCE
// fields (a shared map, slice, and pointer) so the snapshot-safety tests can
// prove the frozen base every sibling selector reads is stable while reducers
// commit into DISJOINT keys on the coordinator goroutine. It round-trips through
// the JSON codec (exported fields) so clone-and-commit works.
type pstate struct {
	Seen    map[string]int // disjoint per-vertex contributions (Seen[itsID])
	Log     []string       // append-only order/threading log
	Counter *int           // a shared pointer reference field the selectors read
}

// gate coordinates the concurrency-bound test: each parallel task increments a
// live counter on entry (tracking the running maximum), blocks until released so
// siblings overlap, then decrements on exit. release is closed once to let the
// whole frontier proceed, so the observed maximum reflects the launch bound.
type gate struct {
	live    atomic.Int32
	max     atomic.Int32
	release chan struct{}
}

func newGate() *gate { return &gate{release: make(chan struct{})} }

// enter records a task starting: bump live, lift max to the new high-water mark,
// then block on release so overlapping tasks pile up against the bound.
func (g *gate) enter() {
	n := g.live.Add(1)
	for {
		m := g.max.Load()
		if n <= m || g.max.CompareAndSwap(m, n) {
			break
		}
	}
	<-g.release
}

func (g *gate) leave() { g.live.Add(-1) }
func (g *gate) open()  { close(g.release) }

// addParallelVertex binds a fan-out worker that, on entry, records concurrency
// via gate (if non-nil), appends its id under Seen[id] (a DISJOINT field), and
// returns its id as output. The selector reads the shared Counter reference from
// the frozen base; the reducer writes only this vertex's own Seen key, so sibling
// reducers never touch the same field (single-writer coordinator, §9.4).
func addParallelVertex(t *testing.T, g *Graph[pstate], id VertexID, key string, gt *gate) {
	t.Helper()
	task := NewFuncTask(func(_ context.Context, _ int) (string, error) {
		if gt != nil {
			gt.enter()
			defer gt.leave()
		}
		return key, nil
	})
	// selector reads the shared Counter pointer from the frozen base (read-only).
	sel := func(s pstate) int {
		if s.Counter != nil {
			return *s.Counter
		}
		return 0
	}
	red := func(s *pstate, out string) error {
		if s.Seen == nil {
			s.Seen = map[string]int{}
		}
		s.Seen[out]++ // DISJOINT: each vertex owns its own key
		return nil
	}
	if err := AddVertex(g, id, task, sel, red); err != nil {
		t.Fatalf("AddVertex(%v): %v", id, err)
	}
}

// compileFanOut builds entry -> w_1..w_n -> join -> finish over pstate: entry
// fans out to n workers in the SAME super-step, the workers fan in to join, and
// join routes to finish. Each worker is wired with the shared gate. It returns
// the Runner and the worker keys for assertion.
func compileFanOut(t *testing.T, store CheckpointStore, n int, gt *gate) (*Runner[pstate], []string) {
	t.Helper()
	g := NewGraph[pstate](GraphID{})
	entry, join, finish := vID(1), vID(byte(n+2)), vID(byte(n+3))
	addParallelVertex(t, g, entry, "entry", nil)
	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		wid := vID(byte(i + 2))
		key := workerKey(i)
		keys = append(keys, key)
		addParallelVertex(t, g, wid, key, gt)
		if err := g.AddEdge(entry, wid); err != nil {
			t.Fatalf("AddEdge(entry->%v): %v", wid, err)
		}
		if err := g.AddEdge(wid, join); err != nil {
			t.Fatalf("AddEdge(%v->join): %v", wid, err)
		}
	}
	addParallelVertex(t, g, join, "join", nil)
	addParallelVertex(t, g, finish, "finish", nil)
	if err := g.AddEdge(join, finish); err != nil {
		t.Fatalf("AddEdge(join->finish): %v", err)
	}
	r, err := g.Compile(entry, finish, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return r, keys
}

// workerKey is the stable per-worker Seen key.
func workerKey(i int) string { return "w" + string(rune('A'+i)) }

// seedState returns a pstate whose reference fields are populated so selectors
// reading the frozen base observe non-nil shared data.
func seedState() pstate {
	zero := 0
	return pstate{Seen: map[string]int{}, Log: nil, Counter: &zero}
}

// TestRunParallelRaceClean proves a wide fan-out step runs race-clean under -race:
// N=8 workers run concurrently (their selectors read a SHARED reference field from
// the frozen base while their reducers write DISJOINT Seen keys), and the merged
// state after the join carries all N contributions exactly once.
func TestRunParallelRaceClean(t *testing.T) {
	t.Parallel()

	const n = 8
	store := NewMemStore()
	gt := newGate()
	gt.open() // no throttling needed; just let everyone run
	r, keys := compileFanOut(t, store, n, gt)

	res, err := r.Run(context.Background(), seedState())
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}
	for _, k := range keys {
		if res.State.Seen[k] != 1 {
			t.Errorf("Seen[%q] = %d, want 1 (each worker reduced exactly once)", k, res.State.Seen[k])
		}
	}
	if got := len(res.State.Seen); got < n {
		t.Errorf("len(Seen) = %d, want >= %d (all worker contributions merged)", got, n)
	}
}

// TestRunConcurrencyBound proves WithConcurrency(k) caps the number of vertices
// running at once: with a fan-out of N > k workers that block until released, the
// observed running maximum never exceeds k. k=1 serializes (max == 1); k=2 admits
// real parallelism (max > 1 when N >= 2) yet stays <= 2.
func TestRunConcurrencyBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		k       int
		n       int
		wantMax int32 // exact expected high-water mark of concurrently running tasks
	}{
		{name: "serialized k=1", k: 1, n: 4, wantMax: 1},
		{name: "bounded k=2", k: 2, n: 4, wantMax: 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gt := newGate()
			r, _ := compileFanOut(t, NewMemStore(), tt.n, gt)

			// Once k tasks are blocked at the gate (the bound is saturated), no more
			// can launch, so release to let the run drain. Poll the live count.
			done := make(chan struct{})
			go func() {
				defer close(done)
				deadline := time.Now().Add(2 * time.Second)
				for time.Now().Before(deadline) {
					if gt.live.Load() >= int32(tt.k) {
						break
					}
					time.Sleep(time.Millisecond)
				}
				gt.open()
			}()

			res, err := r.Run(context.Background(), seedState(), WithConcurrency(tt.k))
			<-done
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if res.Run.Status != RunCompleted {
				t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
			}
			got := gt.max.Load()
			if got > int32(tt.k) {
				t.Errorf("observed max concurrency = %d, want <= %d (WithConcurrency bound)", got, tt.k)
			}
			if got != tt.wantMax {
				t.Errorf("observed max concurrency = %d, want exactly %d", got, tt.wantMax)
			}
		})
	}
}

// TestRunStepBaseImmutable proves the frozen StepBase a sibling selector reads is
// the immutable snapshot, unaffected by another vertex's reducer mutating S. A
// 2-worker step runs; vertex A's reducer appends to the merged state while vertex
// B's selector reads the SAME frozen base (the pre-step Counter), so B observes
// stable data. The committed StepBase the NEXT step reads is the clone-committed
// value, NOT a mutated alias.
func TestRunStepBaseImmutable(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	g := NewGraph[pstate](GraphID{})
	entry, a, b, join := vID(1), vID(2), vID(3), vID(4)
	addParallelVertex(t, g, entry, "entry", nil)

	// Both A and B read the frozen base's Counter via their selectors and record
	// what they observed; A's reducer ALSO appends to Log. Because selectors run on
	// the coordinator against the frozen base BEFORE any launch/reduce, both must
	// observe the same pre-step Counter (7), regardless of A's later mutation.
	var aObserved, bObserved atomic.Int32
	mkWorker := func(id VertexID, observed *atomic.Int32, mutates bool) {
		task := NewFuncTask(func(_ context.Context, in int) (int, error) {
			observed.Store(int32(in))
			return in, nil
		})
		sel := func(s pstate) int {
			if s.Counter != nil {
				return *s.Counter
			}
			return -1
		}
		red := func(s *pstate, out int) error {
			if mutates {
				s.Log = append(s.Log, "A-mutated")
			}
			return nil
		}
		if err := AddVertex(g, id, task, sel, red); err != nil {
			t.Fatalf("AddVertex(%v): %v", id, err)
		}
	}
	mkWorker(a, &aObserved, true)
	mkWorker(b, &bObserved, false)
	addParallelVertex(t, g, join, "join", nil)
	for _, e := range [][2]VertexID{{entry, a}, {entry, b}, {a, join}, {b, join}} {
		if err := g.AddEdge(e[0], e[1]); err != nil {
			t.Fatalf("AddEdge(%v->%v): %v", e[0], e[1], err)
		}
	}
	r, err := g.Compile(entry, join, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	seven := 7
	res, err := r.Run(context.Background(), pstate{Seen: map[string]int{}, Counter: &seven})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}
	// Both siblings read the SAME frozen base Counter (7); A's reducer mutation of
	// Log did not perturb the base B's selector read.
	if aObserved.Load() != 7 {
		t.Errorf("A observed base Counter = %d, want 7", aObserved.Load())
	}
	if bObserved.Load() != 7 {
		t.Errorf("B observed base Counter = %d, want 7 (frozen base unchanged by A's reducer)", bObserved.Load())
	}
	// The committed state carries A's single append (single-writer reduce).
	got := 0
	for _, v := range res.State.Log {
		if v == "A-mutated" {
			got++
		}
	}
	if got != 1 {
		t.Errorf("Log has %d 'A-mutated' entries, want 1 (clone-and-commit, no aliasing)", got)
	}
}

// TestRunBarrierWaitsAll proves the WaitGroup barrier holds reduce/route until
// EVERY frontier vertex finished: one slow worker delays the barrier, and the
// reducer for any worker must not run until all workers completed their task. A
// shared ordered log records each task's completion and each reducer's start; the
// first reducer must appear only after all N task completions.
func TestRunBarrierWaitsAll(t *testing.T) {
	t.Parallel()

	const n = 4
	store := NewMemStore()
	g := NewGraph[pstate](GraphID{})
	entry, join := vID(1), vID(byte(n+2))

	var mu sync.Mutex
	var log []string
	record := func(s string) {
		mu.Lock()
		log = append(log, s)
		mu.Unlock()
	}
	addParallelVertex(t, g, entry, "entry", nil)
	for i := 0; i < n; i++ {
		wid := vID(byte(i + 2))
		key := workerKey(i)
		slow := i == 0 // the first worker is the straggler
		task := NewFuncTask(func(_ context.Context, _ int) (string, error) {
			if slow {
				time.Sleep(20 * time.Millisecond)
			}
			record("task-done:" + key)
			return key, nil
		})
		sel := func(s pstate) int { return 0 }
		red := func(s *pstate, out string) error {
			record("reduce:" + out)
			if s.Seen == nil {
				s.Seen = map[string]int{}
			}
			s.Seen[out]++
			return nil
		}
		if err := AddVertex(g, wid, task, sel, red); err != nil {
			t.Fatalf("AddVertex(%v): %v", wid, err)
		}
		if err := g.AddEdge(entry, wid); err != nil {
			t.Fatalf("AddEdge(entry->%v): %v", wid, err)
		}
		if err := g.AddEdge(wid, join); err != nil {
			t.Fatalf("AddEdge(%v->join): %v", wid, err)
		}
	}
	addParallelVertex(t, g, join, "join", nil)
	r, err := g.Compile(entry, join, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	// concurrency >= n so the barrier (not the bound) is what serializes task->reduce.
	res, err := r.Run(context.Background(), seedState(), WithConcurrency(n))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}

	mu.Lock()
	defer mu.Unlock()
	// Find the index of the first reducer start; all N task completions must precede it.
	firstReduce := -1
	for i, e := range log {
		if len(e) >= 7 && e[:7] == "reduce:" {
			firstReduce = i
			break
		}
	}
	if firstReduce == -1 {
		t.Fatalf("no reducer ran; log = %v", log)
	}
	taskDone := 0
	for _, e := range log[:firstReduce] {
		if len(e) >= 10 && e[:10] == "task-done:" {
			taskDone++
		}
	}
	if taskDone != n {
		t.Errorf("only %d/%d worker tasks completed before the first reducer ran (barrier did not wait for all)", taskDone, n)
	}
}

// --- Task 6.4: reduce semantics — clone-and-commit atomicity + same-field order

// rstate is a reduce-semantics accumulator with a REFERENCE field (Log, a slice)
// alongside a scalar (X), so the clone-and-commit atomicity test can prove a
// reducer that mutates the slice/scalar then errors leaves the committed S byte-
// for-byte unchanged (the mutated clone is discarded, §6.2). It round-trips
// through the JSON codec (exported fields) so clone() works.
type rstate struct {
	Log []string
	X   int
}

// TestReduceCloneAndCommitAtomic drives reduceStep DIRECTLY (white-box, package
// flow) to pin §6.2: the coordinator clones the accumulator, applies the reducer
// to the clone, and commits c.state only on a nil error. A reducer that MUTATES
// *S (appends to Log, sets X) and THEN returns a non-nil error must leave the
// committed c.state COMPLETELY UNCHANGED — the mutated clone is discarded, never
// committed — and reduceStep must return that exact error.
func TestReduceCloneAndCommitAtomic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		start   rstate
		reduce  func(s *rstate, out int) error
		out     int
		wantErr error
	}{
		{
			name:  "mutate-then-error leaves committed state unchanged",
			start: rstate{Log: []string{"committed"}, X: 41},
			reduce: func(s *rstate, out int) error {
				s.Log = append(s.Log, "mutated") // mutate the clone
				s.X = out                        // mutate the clone
				return errReduce                 // ... then reject the fold
			},
			out:     999,
			wantErr: errReduce,
		},
		{
			name:  "empty committed state, mutate-then-error stays empty",
			start: rstate{},
			reduce: func(s *rstate, out int) error {
				s.Log = append(s.Log, "x")
				s.X = 7
				return errReduce
			},
			out:     7,
			wantErr: errReduce,
		},
		{
			name:  "happy path commits (nil error advances state)",
			start: rstate{Log: []string{"a"}, X: 1},
			reduce: func(s *rstate, out int) error {
				s.Log = append(s.Log, "b")
				s.X = out
				return nil
			},
			out:     2,
			wantErr: nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Build a real erased vertex via AddVertex so applyReducer is the
			// production seam, then construct the vertexRun and coordinator directly
			// (this is package flow, so the unexported fields are reachable).
			g := NewGraph[rstate](GraphID{})
			id := vID(1)
			task := NewFuncTask(func(_ context.Context, in int) (int, error) { return in, nil })
			sel := func(s rstate) int { return s.X }
			if err := AddVertex(g, id, task, sel, tt.reduce); err != nil {
				t.Fatalf("AddVertex: %v", err)
			}

			c := &coordinator[rstate]{
				graph:  g,
				finish: vID(99), // not this vertex: the finishRan latch must not interfere
				store:  NewMemStore(),
				cfg:    defaultRunConfig(),
				state:  tt.start,
			}
			// A deep, independent snapshot of the committed state taken BEFORE the
			// call: reflect.DeepEqual against it proves nothing leaked from the clone.
			snapshot, err := clone(c.state)
			if err != nil {
				t.Fatalf("snapshot clone: %v", err)
			}

			run := &vertexRun[rstate]{
				v:   g.vertices[id],
				out: tt.out,
				vs:  VertexState{VertexID: id, Status: VertexRunning},
			}

			gotErr := c.reduceStep(context.Background(), []*vertexRun[rstate]{run})

			if tt.wantErr != nil {
				// (a) the exact reducer error is surfaced.
				if !errors.Is(gotErr, tt.wantErr) {
					t.Fatalf("reduceStep error = %v, want %v", gotErr, tt.wantErr)
				}
				// (b) the committed state is COMPLETELY unchanged (clone discarded).
				if !reflect.DeepEqual(c.state, snapshot) {
					t.Errorf("c.state = %+v, want %+v (mutated clone must be discarded, §6.2)", c.state, snapshot)
				}
				return
			}

			// Happy path: nil error and the state advanced to the reduced value.
			if gotErr != nil {
				t.Fatalf("reduceStep error = %v, want nil", gotErr)
			}
			if reflect.DeepEqual(c.state, snapshot) {
				t.Errorf("c.state unchanged = %+v, want the reduced value committed", c.state)
			}
			if c.state.X != tt.out {
				t.Errorf("c.state.X = %d, want %d (committed on nil error)", c.state.X, tt.out)
			}
		})
	}
}

// winState records which fan-out vertex last wrote the shared Winner field plus
// an ordered Order log, both reference/scalar fields that round-trip through JSON.
type winState struct {
	Winner string
	A      string
	B      string
	Order  []string
}

// pinnedFanOut wires entry -> {lo, hi} (a single super-step fan-out) -> finish
// over winState, with lo's and hi's VertexIDs PINNED so loID < hiID lexically
// (uuid.MustParse). Each of lo/hi runs the given reducer in the SAME step; both
// route to finish. It returns the Runner, the store, and (loID, hiID) so a test
// can assert which VertexID's write survived. entry and finish are inert (their
// reducers are no-ops) so only lo/hi touch the asserted fields.
func pinnedFanOut(
	t *testing.T, store CheckpointStore,
	loReduce, hiReduce func(s *winState, out string) error,
) (*Runner[winState], VertexID, VertexID) {
	t.Helper()
	g := NewGraph[winState](GraphID{})

	// Pinned so loID < hiID as canonical strings (00...01 < 00...02), giving a
	// KNOWN, run-stable VertexID order the reduce loop iterates in (§9.4).
	entry := VertexID(uuid.MustParse("00000000-0000-4000-8000-000000000010"))
	loID := VertexID(uuid.MustParse("00000000-0000-4000-8000-000000000001"))
	hiID := VertexID(uuid.MustParse("00000000-0000-4000-8000-000000000002"))
	finish := VertexID(uuid.MustParse("00000000-0000-4000-8000-000000000020"))
	if loID.String() >= hiID.String() {
		t.Fatalf("pinned ids not ordered: lo=%s hi=%s", loID, hiID)
	}

	noop := func(s *winState, _ string) error { return nil }
	task := NewFuncTask(func(_ context.Context, _ int) (string, error) { return "out", nil })
	sel := func(s winState) int { return 0 }

	add := func(id VertexID, red func(s *winState, out string) error) {
		if err := AddVertex(g, id, task, sel, red); err != nil {
			t.Fatalf("AddVertex(%v): %v", id, err)
		}
	}
	add(entry, noop)
	add(loID, loReduce)
	add(hiID, hiReduce)
	add(finish, noop)

	for _, e := range [][2]VertexID{{entry, loID}, {entry, hiID}, {loID, finish}, {hiID, finish}} {
		if err := g.AddEdge(e[0], e[1]); err != nil {
			t.Fatalf("AddEdge(%v->%v): %v", e[0], e[1], err)
		}
	}
	r, err := g.Compile(entry, finish, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return r, loID, hiID
}

// TestReduceSameFieldLastInVertexIDOrderWins proves §9.4: two vertices in the
// SAME super-step that reduce into the SAME field are applied single-threaded in
// VertexID order, so the HIGHER VertexID (last in order) wins — a defined,
// run-stable overwrite, NOT a race. lo writes Winner="lo", hi writes Winner="hi";
// since loID < hiID, the committed Winner must be "hi". Running under -race and
// -count proves the outcome is deterministic, not racy.
func TestReduceSameFieldLastInVertexIDOrderWins(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	loReduce := func(s *winState, _ string) error { s.Winner = "lo"; return nil }
	hiReduce := func(s *winState, _ string) error { s.Winner = "hi"; return nil }
	r, loID, hiID := pinnedFanOut(t, store, loReduce, hiReduce)

	res, err := r.Run(context.Background(), winState{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}
	// hi has the higher VertexID, so it reduces LAST and its write survives.
	if res.State.Winner != "hi" {
		t.Errorf("Winner = %q, want %q (higher VertexID %s reduces last, beating %s, §9.4)",
			res.State.Winner, "hi", hiID, loID)
	}
}

// TestReduceDisjointFieldsAllApply proves a same-step fan-out whose two reducers
// write DISJOINT fields both land in the committed state (single-writer reduce,
// no lost update). lo writes A, hi writes B; the final state carries both.
// Race-clean under -race.
func TestReduceDisjointFieldsAllApply(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	loReduce := func(s *winState, _ string) error { s.A = "from-lo"; return nil }
	hiReduce := func(s *winState, _ string) error { s.B = "from-hi"; return nil }
	r, _, _ := pinnedFanOut(t, store, loReduce, hiReduce)

	res, err := r.Run(context.Background(), winState{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}
	if res.State.A != "from-lo" {
		t.Errorf("State.A = %q, want %q (lo's disjoint contribution)", res.State.A, "from-lo")
	}
	if res.State.B != "from-hi" {
		t.Errorf("State.B = %q, want %q (hi's disjoint contribution)", res.State.B, "from-hi")
	}
}

// TestReduceOrderIsVertexIDStable proves the reduce loop applies reducers in
// VertexID-sorted order deterministically: a 3-vertex fan-out where each reducer
// appends its own pinned VertexID string to Order must yield Order ==
// [lowest, middle, highest] every run. Pinned ids give a known target order;
// -count proves it is stable, not incidental.
func TestReduceOrderIsVertexIDStable(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	g := NewGraph[winState](GraphID{})

	entry := VertexID(uuid.MustParse("00000000-0000-4000-8000-000000000010"))
	lo := VertexID(uuid.MustParse("00000000-0000-4000-8000-000000000001"))
	mid := VertexID(uuid.MustParse("00000000-0000-4000-8000-000000000002"))
	hi := VertexID(uuid.MustParse("00000000-0000-4000-8000-000000000003"))
	finish := VertexID(uuid.MustParse("00000000-0000-4000-8000-000000000020"))

	task := NewFuncTask(func(_ context.Context, _ int) (string, error) { return "out", nil })
	sel := func(s winState) int { return 0 }
	noop := func(s *winState, _ string) error { return nil }
	// Each fan-out reducer appends its own id; the coordinator reduces them
	// single-threaded in VertexID order, so Order reflects that sort.
	mkAppend := func(id VertexID) func(s *winState, out string) error {
		return func(s *winState, _ string) error { s.Order = append(s.Order, id.String()); return nil }
	}
	add := func(id VertexID, red func(s *winState, out string) error) {
		if err := AddVertex(g, id, task, sel, red); err != nil {
			t.Fatalf("AddVertex(%v): %v", id, err)
		}
	}
	add(entry, noop)
	add(lo, mkAppend(lo))
	add(mid, mkAppend(mid))
	add(hi, mkAppend(hi))
	add(finish, noop)
	for _, e := range [][2]VertexID{
		{entry, lo}, {entry, mid}, {entry, hi},
		{lo, finish}, {mid, finish}, {hi, finish},
	} {
		if err := g.AddEdge(e[0], e[1]); err != nil {
			t.Fatalf("AddEdge(%v->%v): %v", e[0], e[1], err)
		}
	}
	r, err := g.Compile(entry, finish, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	res, err := r.Run(context.Background(), winState{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := sortFrontier([]VertexID{lo, mid, hi})
	wantOrder := []string{want[0].String(), want[1].String(), want[2].String()}
	if !reflect.DeepEqual(res.State.Order, wantOrder) {
		t.Errorf("reduce Order = %v, want %v (VertexID-sorted, §9.4)", res.State.Order, wantOrder)
	}
}

// --- Task 6.5: conditional routing (Condition.Pick) + routing halts -----------

// condGraph builds a graph over cnt with a single conditional edge from `from`
// carrying the given Condition, plus inert tag vertices for every id in `verts`.
// It wires no static edges from `from` (a vertex has EITHER static OR conditional
// routing, never both). It returns the compiled Runner; callers add any extra
// static edges before calling via the prep callback.
func condGraph(
	t *testing.T, store CheckpointStore, entry, finish, from VertexID,
	cond Condition[cnt], verts []VertexID, prep func(g *Graph[cnt]),
) *Runner[cnt] {
	t.Helper()
	g := NewGraph[cnt](GraphID{})
	for _, id := range verts {
		tagVertex(t, g, id, id.String())
	}
	if prep != nil {
		prep(g)
	}
	if err := g.AddConditionalEdge(from, cond); err != nil {
		t.Fatalf("AddConditionalEdge(%v): %v", from, err)
	}
	r, err := g.Compile(entry, finish, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return r
}

// TestRunConditionalSingleTarget proves a conditional edge whose Pick returns one
// declared target routes there: entry -> (Pick:[finish]) -> finish completes, the
// recorded RouteRecord is {From:entry, To:[finish], Conditional:true}, and OnEdge
// fires for (entry, finish).
func TestRunConditionalSingleTarget(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	entry, fin := vID(1), vID(2)
	cond := Condition[cnt]{
		Targets: []VertexID{fin},
		Pick:    func(_ context.Context, _ cnt) ([]VertexID, error) { return []VertexID{fin}, nil },
	}
	r := condGraph(t, store, entry, fin, entry, cond, []VertexID{entry, fin}, nil)

	rc := &recorder{}
	res, err := r.Run(context.Background(), cnt{}, WithHooks(rc.hooks()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}

	var saw bool
	for _, rec := range routesFor(t, store, res.Run.GraphRunID) {
		if rec.From != entry {
			continue
		}
		saw = true
		if !rec.Conditional {
			t.Errorf("conditional route Conditional = false, want true")
		}
		if len(rec.To) != 1 || rec.To[0] != fin {
			t.Errorf("conditional route To = %v, want [%v]", rec.To, fin)
		}
	}
	if !saw {
		t.Error("no RouteRecord with From == entry (conditional route not recorded)")
	}
	rc.mu.Lock()
	edges := rc.edges
	rc.mu.Unlock()
	if edges != 1 {
		t.Errorf("OnEdge fired %d times, want 1 (entry->finish)", edges)
	}
}

// TestRunConditionalFanOut proves a Pick returning TWO declared targets activates
// both in the next super-step (a conditional fan-out): entry -> (Pick:[a,b]); both
// a and b route to finish; both run exactly once and the run completes.
func TestRunConditionalFanOut(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	entry, a, b, fin := vID(1), vID(2), vID(3), vID(4)
	cond := Condition[cnt]{
		Targets: []VertexID{a, b},
		Pick:    func(_ context.Context, _ cnt) ([]VertexID, error) { return []VertexID{a, b}, nil },
	}
	r := condGraph(t, store, entry, fin, entry, cond, []VertexID{entry, a, b, fin}, func(g *Graph[cnt]) {
		for _, e := range [][2]VertexID{{a, fin}, {b, fin}} {
			if err := g.AddEdge(e[0], e[1]); err != nil {
				t.Fatalf("AddEdge(%v->%v): %v", e[0], e[1], err)
			}
		}
	})

	res, err := r.Run(context.Background(), cnt{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}
	// entry(+1), a,b(+2 in one step), fin(+1) == 4 reductions.
	if res.State.N != 4 {
		t.Errorf("State.N = %d, want 4 (entry + a + b + fin)", res.State.N)
	}
}

// chstate is a conditional-routing state whose reducer sets Choice, so a Pick can
// read the POST-reduce committed state S_{N+1} (proving Pick sees the freshly
// committed value, not the step's frozen base).
type chstate struct {
	Choice string
	Vals   []string
}

// TestRunConditionalPickReadsCommittedState proves Pick reads S_{N+1}: entry's
// reducer sets Choice from a seeded want; entry's Pick reads Choice and routes to
// left or right accordingly. Both directions are tested.
func TestRunConditionalPickReadsCommittedState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		choice string
		want   string // "left" or "right" tag expected in Vals after the chosen branch
	}{
		{name: "choice left routes left", choice: "left", want: "left"},
		{name: "choice right routes right", choice: "right", want: "right"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := NewMemStore()
			g := NewGraph[chstate](GraphID{})
			entry, left, right := vID(1), vID(2), vID(3)

			noTask := NewFuncTask(func(_ context.Context, _ int) (string, error) { return "", nil })
			noSel := func(_ chstate) int { return 0 }
			// entry's reducer commits the seeded Choice into S (so Pick reads S_{N+1}).
			choice := tt.choice
			entryRed := func(s *chstate, _ string) error { s.Choice = choice; return nil }
			if err := AddVertex(g, entry, noTask, noSel, entryRed); err != nil {
				t.Fatalf("AddVertex(entry): %v", err)
			}
			mkBranch := func(id VertexID, tag string) {
				red := func(s *chstate, _ string) error { s.Vals = append(s.Vals, tag); return nil }
				if err := AddVertex(g, id, noTask, noSel, red); err != nil {
					t.Fatalf("AddVertex(%s): %v", tag, err)
				}
			}
			mkBranch(left, "left")
			mkBranch(right, "right")

			cond := Condition[chstate]{
				Targets: []VertexID{left, right},
				Pick: func(_ context.Context, s chstate) ([]VertexID, error) {
					if s.Choice == "left" {
						return []VertexID{left}, nil
					}
					return []VertexID{right}, nil
				},
			}
			if err := g.AddConditionalEdge(entry, cond); err != nil {
				t.Fatalf("AddConditionalEdge(entry): %v", err)
			}
			// left/right are sinks; finish is whichever branch we expect to run, so the
			// run completes when the chosen branch runs.
			finish := left
			if tt.want == "right" {
				finish = right
			}
			r, err := g.Compile(entry, finish, WithStore(store))
			if err != nil {
				t.Fatalf("Compile: %v", err)
			}

			res, err := r.Run(context.Background(), chstate{})
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if res.Run.Status != RunCompleted {
				t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
			}
			if len(res.State.Vals) != 1 || res.State.Vals[0] != tt.want {
				t.Errorf("Vals = %v, want [%s] (Pick read committed Choice=%q)", res.State.Vals, tt.want, tt.choice)
			}
		})
	}
}

// assertRoutingHalt asserts a run halted with the given kind, set Result.Halt (not
// an engine error), RunInterrupted, nil Interrupts, a StepHalted final checkpoint,
// and OnHalt firing exactly once. It returns the halt cause for further inspection.
func assertRoutingHalt(t *testing.T, r *Runner[cnt], store CheckpointStore, kind HaltKind) error {
	t.Helper()
	rc := &recorder{}
	res, err := r.Run(context.Background(), cnt{}, WithHooks(rc.hooks()))
	if err != nil {
		t.Fatalf("Run returned engine error %v, want nil (halt is a Result, not an error)", err)
	}
	if res.Halt == nil {
		t.Fatalf("Result.Halt is nil, want halt kind %v", kind)
	}
	if res.Halt.Kind != kind {
		t.Fatalf("Halt.Kind = %v, want %v", res.Halt.Kind, kind)
	}
	if res.Run.Status != RunInterrupted {
		t.Errorf("Status = %v, want RunInterrupted", res.Run.Status)
	}
	if res.Interrupts != nil {
		t.Errorf("Result.Interrupts = %v, want nil (mutually exclusive with Halt)", res.Interrupts)
	}
	hist, err := store.History(context.Background(), res.Run.GraphRunID)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	last := hist[len(hist)-1]
	if last.Phase != StepHalted {
		t.Errorf("last checkpoint Phase = %v, want StepHalted", last.Phase)
	}
	if last.Halt == nil || last.Halt.Kind != kind {
		t.Errorf("last checkpoint Halt = %+v, want kind %v", last.Halt, kind)
	}
	rc.mu.Lock()
	halts := rc.halts
	rc.mu.Unlock()
	if halts != 1 {
		t.Errorf("OnHalt fired %d times, want 1", halts)
	}
	return res.Halt.Cause
}

// TestRunConditionalEmptySetHalt proves a Pick returning an empty set is a routing
// halt HaltUndeclaredTarget whose cause is an *UndeclaredTargetError with a zero
// Target (the empty-return sentinel) — a Result halt, not an engine error.
func TestRunConditionalEmptySetHalt(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	entry, fin := vID(1), vID(2)
	cond := Condition[cnt]{
		Targets: []VertexID{fin},
		Pick:    func(_ context.Context, _ cnt) ([]VertexID, error) { return nil, nil },
	}
	r := condGraph(t, store, entry, fin, entry, cond, []VertexID{entry, fin}, nil)

	cause := assertRoutingHalt(t, r, store, HaltUndeclaredTarget)
	var ute *UndeclaredTargetError
	if !errors.As(cause, &ute) {
		t.Fatalf("Halt.Cause = %v, want *UndeclaredTargetError", cause)
	}
	if ute.From != entry {
		t.Errorf("UndeclaredTargetError.From = %v, want %v", ute.From, entry)
	}
	if ute.Target != (VertexID{}) {
		t.Errorf("UndeclaredTargetError.Target = %v, want zero (empty-return sentinel)", ute.Target)
	}
}

// TestRunConditionalUndeclaredTargetHalt proves a Pick returning a target NOT in
// its declared Targets is a routing halt HaltUndeclaredTarget naming the offending
// target — a Result halt, not an engine error.
func TestRunConditionalUndeclaredTargetHalt(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	entry, fin, rogue := vID(1), vID(2), vID(9)
	cond := Condition[cnt]{
		Targets: []VertexID{fin},
		Pick:    func(_ context.Context, _ cnt) ([]VertexID, error) { return []VertexID{rogue}, nil },
	}
	r := condGraph(t, store, entry, fin, entry, cond, []VertexID{entry, fin}, nil)

	cause := assertRoutingHalt(t, r, store, HaltUndeclaredTarget)
	var ute *UndeclaredTargetError
	if !errors.As(cause, &ute) {
		t.Fatalf("Halt.Cause = %v, want *UndeclaredTargetError", cause)
	}
	if ute.Target != rogue {
		t.Errorf("UndeclaredTargetError.Target = %v, want %v (the offending id)", ute.Target, rogue)
	}
	if ute.From != entry {
		t.Errorf("UndeclaredTargetError.From = %v, want %v", ute.From, entry)
	}
}

// TestRunConditionalPickErrorHalt proves a Pick returning a non-nil error is a
// routing halt HaltCondition whose cause is a *ConditionError wrapping the Pick
// error — a Result halt, not an engine error.
func TestRunConditionalPickErrorHalt(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	entry, fin := vID(1), vID(2)
	pickErr := errors.New("pick blew up")
	cond := Condition[cnt]{
		Targets: []VertexID{fin},
		Pick:    func(_ context.Context, _ cnt) ([]VertexID, error) { return nil, pickErr },
	}
	r := condGraph(t, store, entry, fin, entry, cond, []VertexID{entry, fin}, nil)

	cause := assertRoutingHalt(t, r, store, HaltCondition)
	var ce *ConditionError
	if !errors.As(cause, &ce) {
		t.Fatalf("Halt.Cause = %v, want *ConditionError", cause)
	}
	if ce.From != entry {
		t.Errorf("ConditionError.From = %v, want %v", ce.From, entry)
	}
	if !errors.Is(cause, pickErr) {
		t.Errorf("Halt.Cause does not wrap the Pick error %v", pickErr)
	}
}

// TestRunConditionalPickPanicHalt proves a Pick that PANICS is recovered (never
// crashes the run) and surfaces as a routing halt HaltCondition whose cause is a
// *ConditionError mentioning "panic" — a Result halt, not an engine error.
func TestRunConditionalPickPanicHalt(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	entry, fin := vID(1), vID(2)
	cond := Condition[cnt]{
		Targets: []VertexID{fin},
		Pick:    func(_ context.Context, _ cnt) ([]VertexID, error) { panic("boom") },
	}
	r := condGraph(t, store, entry, fin, entry, cond, []VertexID{entry, fin}, nil)

	cause := assertRoutingHalt(t, r, store, HaltCondition)
	var ce *ConditionError
	if !errors.As(cause, &ce) {
		t.Fatalf("Halt.Cause = %v, want *ConditionError", cause)
	}
	if ce.From != entry {
		t.Errorf("ConditionError.From = %v, want %v", ce.From, entry)
	}
	if !strings.Contains(cause.Error(), "panic") {
		t.Errorf("Halt.Cause = %q, want it to mention \"panic\"", cause.Error())
	}
}

// TestRunConditionalCycleHaltMaxSteps proves conditional back-edges traverse: a
// Pick that routes back to entry forms a cycle that never reaches finish, so under
// WithMaxSteps it eventually halts HaltMaxSteps (the conditional router followed
// the back-edge every step).
func TestRunConditionalCycleHaltMaxSteps(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	entry, fin := vID(1), vID(2)
	// entry's conditional edge declares [entry, fin] but always Picks entry: an
	// infinite conditional self-cycle that the step budget must cut off.
	cond := Condition[cnt]{
		Targets: []VertexID{entry, fin},
		Pick:    func(_ context.Context, _ cnt) ([]VertexID, error) { return []VertexID{entry}, nil },
	}
	r := condGraph(t, store, entry, fin, entry, cond, []VertexID{entry, fin}, nil)

	const max = 5
	res, err := r.Run(context.Background(), cnt{}, WithMaxSteps(max))
	if err != nil {
		t.Fatalf("Run returned engine error %v, want nil", err)
	}
	if res.Halt == nil || res.Halt.Kind != HaltMaxSteps {
		t.Fatalf("Halt = %+v, want HaltMaxSteps (conditional back-edge cycle)", res.Halt)
	}
	var mse *MaxStepsExceededError
	if !errors.As(res.Halt.Cause, &mse) {
		t.Fatalf("Halt.Cause = %v, want *MaxStepsExceededError", res.Halt.Cause)
	}
}
