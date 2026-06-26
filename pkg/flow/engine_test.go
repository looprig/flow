package flow

import (
	"context"
	"encoding/json"
	"sync"
	"testing"
)

// This file white-box tests the BSP coordinator's linear super-step loop (§9.2):
// seed → run → reduce → route (static single edge) → terminate, plus the durable
// checkpoint trail and the lifecycle hooks. It uses cnt (runner_test.go) as the
// accumulator and a MemStore to inspect the appended history.

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
