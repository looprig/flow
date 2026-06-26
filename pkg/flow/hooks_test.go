package flow

import (
	"context"
	"errors"
	"testing"
)

// These tests pin the observability contract of §11 and the panic-safety
// guarantee of §12.5: each event fans out to every accumulated hook set in
// registration order, nil callbacks are skipped, and a panicking hook is
// recovered and discarded so it can never alter control flow or stop the
// fan-out to its siblings. The dispatcher is unexported, so these are
// white-box tests in package flow.

// fixedGraphRunState is a small, deterministic GraphRunState used to assert an
// event delivered the exact record it was fired with.
func fixedGraphRunState() GraphRunState {
	return GraphRunState{GraphRunID: GraphRunID{1}, Step: StepID(7), Revision: 3, Status: RunRunning}
}

// fixedVertexState is a small, deterministic VertexState used to assert an
// event delivered the exact record it was fired with.
func fixedVertexState() VertexState {
	return VertexState{VertexID: VertexID{2}, Step: StepID(7), Status: VertexRunning, Attempt: 1}
}

// TestHookDispatcherFiresEachEvent verifies every one of the nine §11 events
// invokes its registered callback exactly once with the arguments it was fired
// with. Each subtest keeps its own recorder local because subtests run in
// parallel.
func TestHookDispatcherFiresEachEvent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	run := fixedGraphRunState()
	vs := fixedVertexState()
	iv := Interruption{GraphRunID: GraphRunID{1}, Vertex: VertexID{2}, Kind: Awaiting, Info: "why"}
	h := Halt{GraphRunID: GraphRunID{1}, Kind: HaltDeadEnd, Step: StepID(7), Cause: errors.New("boom")}

	t.Run("OnRunStart", func(t *testing.T) {
		t.Parallel()
		var got GraphRunState
		calls := 0
		d := &hookDispatcher{}
		d.add(Hooks{OnRunStart: func(_ context.Context, ev GraphRunState) { got, calls = ev, calls+1 }})
		d.onRunStart(ctx, run)
		if calls != 1 {
			t.Fatalf("OnRunStart calls = %d, want 1", calls)
		}
		if got != run {
			t.Errorf("OnRunStart ev = %+v, want %+v", got, run)
		}
	})

	t.Run("OnRunFinish", func(t *testing.T) {
		t.Parallel()
		var got GraphRunState
		calls := 0
		d := &hookDispatcher{}
		d.add(Hooks{OnRunFinish: func(_ context.Context, ev GraphRunState) { got, calls = ev, calls+1 }})
		d.onRunFinish(ctx, run)
		if calls != 1 {
			t.Fatalf("OnRunFinish calls = %d, want 1", calls)
		}
		if got != run {
			t.Errorf("OnRunFinish ev = %+v, want %+v", got, run)
		}
	})

	t.Run("OnVertexStart", func(t *testing.T) {
		t.Parallel()
		var got VertexState
		calls := 0
		d := &hookDispatcher{}
		d.add(Hooks{OnVertexStart: func(_ context.Context, ev VertexState) { got, calls = ev, calls+1 }})
		d.onVertexStart(ctx, vs)
		if calls != 1 {
			t.Fatalf("OnVertexStart calls = %d, want 1", calls)
		}
		if got != vs {
			t.Errorf("OnVertexStart ev = %+v, want %+v", got, vs)
		}
	})

	t.Run("OnVertexFinish", func(t *testing.T) {
		t.Parallel()
		var got VertexState
		calls := 0
		d := &hookDispatcher{}
		d.add(Hooks{OnVertexFinish: func(_ context.Context, ev VertexState) { got, calls = ev, calls+1 }})
		d.onVertexFinish(ctx, vs)
		if calls != 1 {
			t.Fatalf("OnVertexFinish calls = %d, want 1", calls)
		}
		if got != vs {
			t.Errorf("OnVertexFinish ev = %+v, want %+v", got, vs)
		}
	})

	t.Run("OnEdge", func(t *testing.T) {
		t.Parallel()
		from, to := VertexID{3}, VertexID{4}
		var gotFrom, gotTo VertexID
		var gotRun GraphRunState
		calls := 0
		d := &hookDispatcher{}
		d.add(Hooks{OnEdge: func(_ context.Context, f, tt VertexID, r GraphRunState) {
			gotFrom, gotTo, gotRun, calls = f, tt, r, calls+1
		}})
		d.onEdge(ctx, from, to, run)
		if calls != 1 {
			t.Fatalf("OnEdge calls = %d, want 1", calls)
		}
		if gotFrom != from || gotTo != to {
			t.Errorf("OnEdge from,to = %v,%v, want %v,%v", gotFrom, gotTo, from, to)
		}
		if gotRun != run {
			t.Errorf("OnEdge run = %+v, want %+v", gotRun, run)
		}
	})

	t.Run("OnStep", func(t *testing.T) {
		t.Parallel()
		var gotRun GraphRunState
		var gotActivated int
		calls := 0
		d := &hookDispatcher{}
		d.add(Hooks{OnStep: func(_ context.Context, r GraphRunState, activated int) {
			gotRun, gotActivated, calls = r, activated, calls+1
		}})
		d.onStep(ctx, run, 5)
		if calls != 1 {
			t.Fatalf("OnStep calls = %d, want 1", calls)
		}
		if gotRun != run {
			t.Errorf("OnStep run = %+v, want %+v", gotRun, run)
		}
		if gotActivated != 5 {
			t.Errorf("OnStep activated = %d, want 5", gotActivated)
		}
	})

	t.Run("OnCheckpoint", func(t *testing.T) {
		t.Parallel()
		id := GraphRunID{9}
		var gotID GraphRunID
		var gotRev uint64
		var gotStep StepID
		calls := 0
		d := &hookDispatcher{}
		d.add(Hooks{OnCheckpoint: func(_ context.Context, gid GraphRunID, rev uint64, step StepID) {
			gotID, gotRev, gotStep, calls = gid, rev, step, calls+1
		}})
		d.onCheckpoint(ctx, id, 42, StepID(11))
		if calls != 1 {
			t.Fatalf("OnCheckpoint calls = %d, want 1", calls)
		}
		if gotID != id || gotRev != 42 || gotStep != StepID(11) {
			t.Errorf("OnCheckpoint id,rev,step = %v,%d,%d, want %v,42,11", gotID, gotRev, gotStep, id)
		}
	})

	t.Run("OnInterrupt", func(t *testing.T) {
		t.Parallel()
		var got Interruption
		calls := 0
		d := &hookDispatcher{}
		d.add(Hooks{OnInterrupt: func(_ context.Context, got2 Interruption) { got, calls = got2, calls+1 }})
		d.onInterrupt(ctx, iv)
		if calls != 1 {
			t.Fatalf("OnInterrupt calls = %d, want 1", calls)
		}
		if got.GraphRunID != iv.GraphRunID || got.Vertex != iv.Vertex || got.Kind != iv.Kind || got.Info != iv.Info {
			t.Errorf("OnInterrupt iv = %+v, want %+v", got, iv)
		}
	})

	t.Run("OnHalt", func(t *testing.T) {
		t.Parallel()
		var got Halt
		calls := 0
		d := &hookDispatcher{}
		d.add(Hooks{OnHalt: func(_ context.Context, got2 Halt) { got, calls = got2, calls+1 }})
		d.onHalt(ctx, h)
		if calls != 1 {
			t.Fatalf("OnHalt calls = %d, want 1", calls)
		}
		if got.GraphRunID != h.GraphRunID || got.Kind != h.Kind || got.Step != h.Step || got.Cause != h.Cause {
			t.Errorf("OnHalt h = %+v, want %+v", got, h)
		}
	})
}

// TestHookDispatcherSkipsNilCallbacks asserts that an all-nil Hooks set fired
// for every event neither panics nor records anything: a nil callback is
// silently skipped. A zero dispatcher (no sets at all) is covered too.
func TestHookDispatcherSkipsNilCallbacks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fire func(d *hookDispatcher)
	}{
		{"OnRunStart", func(d *hookDispatcher) { d.onRunStart(context.Background(), GraphRunState{}) }},
		{"OnRunFinish", func(d *hookDispatcher) { d.onRunFinish(context.Background(), GraphRunState{}) }},
		{"OnVertexStart", func(d *hookDispatcher) { d.onVertexStart(context.Background(), VertexState{}) }},
		{"OnVertexFinish", func(d *hookDispatcher) { d.onVertexFinish(context.Background(), VertexState{}) }},
		{"OnEdge", func(d *hookDispatcher) { d.onEdge(context.Background(), VertexID{}, VertexID{}, GraphRunState{}) }},
		{"OnStep", func(d *hookDispatcher) { d.onStep(context.Background(), GraphRunState{}, 0) }},
		{"OnCheckpoint", func(d *hookDispatcher) { d.onCheckpoint(context.Background(), GraphRunID{}, 0, StepID(0)) }},
		{"OnInterrupt", func(d *hookDispatcher) { d.onInterrupt(context.Background(), Interruption{}) }},
		{"OnHalt", func(d *hookDispatcher) { d.onHalt(context.Background(), Halt{}) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// One set with all-nil callbacks, plus the zero-set dispatcher, must
			// both fire without panicking and without invoking anything.
			d := &hookDispatcher{}
			d.add(Hooks{})
			tt.fire(d) // would panic on a nil-call bug

			zero := &hookDispatcher{}
			tt.fire(zero) // no sets at all
		})
	}
}

// TestHookDispatcherAccumulatesInOrder proves multiple accumulated sets all
// fire, in registration order (the order add was called).
func TestHookDispatcherAccumulatesInOrder(t *testing.T) {
	t.Parallel()

	var order []int
	d := &hookDispatcher{}
	d.add(Hooks{OnRunStart: func(_ context.Context, _ GraphRunState) { order = append(order, 1) }})
	d.add(Hooks{OnRunStart: func(_ context.Context, _ GraphRunState) { order = append(order, 2) }})
	d.add(Hooks{OnRunStart: func(_ context.Context, _ GraphRunState) { order = append(order, 3) }})

	d.onRunStart(context.Background(), GraphRunState{})

	want := []int{1, 2, 3}
	if len(order) != len(want) {
		t.Fatalf("order = %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order = %v, want %v", order, want)
		}
	}
}

// TestHookDispatcherRecoversPanic proves §12.5: a hook that panics is recovered
// and discarded — the fire method returns normally (no panic escapes; this test
// would itself fail if it did), and a SIBLING set registered AFTER the
// panicking one STILL fires (one panic must not stop the fan-out).
func TestHookDispatcherRecoversPanic(t *testing.T) {
	t.Parallel()

	t.Run("panic does not propagate and siblings still fire", func(t *testing.T) {
		t.Parallel()
		siblingBefore, siblingAfter := false, false
		d := &hookDispatcher{}
		d.add(Hooks{OnRunStart: func(_ context.Context, _ GraphRunState) { siblingBefore = true }})
		d.add(Hooks{OnRunStart: func(_ context.Context, _ GraphRunState) { panic("hook boom") }})
		d.add(Hooks{OnRunStart: func(_ context.Context, _ GraphRunState) { siblingAfter = true }})

		// If the panic escaped onRunStart, the test goroutine would crash and fail.
		d.onRunStart(context.Background(), GraphRunState{})

		if !siblingBefore {
			t.Error("sibling registered before the panicking hook did not fire")
		}
		if !siblingAfter {
			t.Error("sibling registered after the panicking hook did not fire")
		}
	})

	t.Run("every event recovers a panicking callback", func(t *testing.T) {
		t.Parallel()
		boom := func() { panic("hook boom") }
		tests := []struct {
			name string
			fire func(d *hookDispatcher)
			h    Hooks
		}{
			{"OnRunStart",
				func(d *hookDispatcher) { d.onRunStart(context.Background(), GraphRunState{}) },
				Hooks{OnRunStart: func(_ context.Context, _ GraphRunState) { boom() }}},
			{"OnRunFinish",
				func(d *hookDispatcher) { d.onRunFinish(context.Background(), GraphRunState{}) },
				Hooks{OnRunFinish: func(_ context.Context, _ GraphRunState) { boom() }}},
			{"OnVertexStart",
				func(d *hookDispatcher) { d.onVertexStart(context.Background(), VertexState{}) },
				Hooks{OnVertexStart: func(_ context.Context, _ VertexState) { boom() }}},
			{"OnVertexFinish",
				func(d *hookDispatcher) { d.onVertexFinish(context.Background(), VertexState{}) },
				Hooks{OnVertexFinish: func(_ context.Context, _ VertexState) { boom() }}},
			{"OnEdge",
				func(d *hookDispatcher) { d.onEdge(context.Background(), VertexID{}, VertexID{}, GraphRunState{}) },
				Hooks{OnEdge: func(_ context.Context, _, _ VertexID, _ GraphRunState) { boom() }}},
			{"OnStep",
				func(d *hookDispatcher) { d.onStep(context.Background(), GraphRunState{}, 0) },
				Hooks{OnStep: func(_ context.Context, _ GraphRunState, _ int) { boom() }}},
			{"OnCheckpoint",
				func(d *hookDispatcher) { d.onCheckpoint(context.Background(), GraphRunID{}, 0, StepID(0)) },
				Hooks{OnCheckpoint: func(_ context.Context, _ GraphRunID, _ uint64, _ StepID) { boom() }}},
			{"OnInterrupt",
				func(d *hookDispatcher) { d.onInterrupt(context.Background(), Interruption{}) },
				Hooks{OnInterrupt: func(_ context.Context, _ Interruption) { boom() }}},
			{"OnHalt",
				func(d *hookDispatcher) { d.onHalt(context.Background(), Halt{}) },
				Hooks{OnHalt: func(_ context.Context, _ Halt) { boom() }}},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				d := &hookDispatcher{}
				d.add(tt.h)
				// A bare panic from the callback would crash the goroutine and
				// fail this subtest; reaching the next line proves recovery.
				tt.fire(d)
			})
		}
	})
}

// TestSafeHookRecovers asserts the recovery primitive directly: safeHook runs
// its function and swallows any panic, returning normally either way.
func TestSafeHookRecovers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		f       func()
		wantRun bool
	}{
		{name: "non-panicking f runs", f: func() {}, wantRun: true},
		{name: "panicking f is recovered", f: func() { panic("boom") }, wantRun: true},
		{name: "nil f is a no-op", f: nil, wantRun: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ran := false
			f := tt.f
			if f != nil {
				inner := f
				f = func() { ran = true; inner() }
			}
			// safeHook must return normally in every case (no panic escapes).
			safeHook(f)
			if ran != tt.wantRun {
				t.Errorf("ran = %v, want %v", ran, tt.wantRun)
			}
		})
	}
}
