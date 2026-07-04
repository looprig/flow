package flow

import "context"

// This file is the observability layer (design §11) and its panic-safety
// guarantee (§12.5). Hooks is a struct of optional, purely observational
// callbacks — one per lifecycle event — so a caller depends only on the events
// it wires (ISP). hookDispatcher is the unexported fan-out: it accumulates the
// hook sets registered for a run and fires each event to every set, nil-guarding
// each callback and recovering+discarding any panic. Because a hook is purely
// observational, a hook panic must NEVER alter control flow or fail the run
// (§12.5) — it is recovered and silently dropped, and a panic in one set does
// not stop the fan-out to the others.
//
// The WithHooks RunOption that calls add (and the run wiring that fires these
// methods) is Phase 6; this file defines only the struct, the dispatcher, and
// the recovery primitive.

// Hooks is a set of optional, purely observational lifecycle callbacks (§11).
// Every field is nil-able and independent, so a caller wires only the events it
// cares about (interface segregation). A callback receives the framework-owned
// instrumentation records of §4.1 and must not mutate engine state; it is fired
// best-effort and any panic it raises is recovered and discarded (§12.5).
type Hooks struct {
	OnRunStart     func(ctx context.Context, ev GraphRunState)
	OnRunFinish    func(ctx context.Context, ev GraphRunState)
	OnVertexStart  func(ctx context.Context, ev VertexState)
	OnVertexFinish func(ctx context.Context, ev VertexState)
	OnEdge         func(ctx context.Context, from, to VertexID, run GraphRunState)
	OnStep         func(ctx context.Context, run GraphRunState, activated int)
	OnCheckpoint   func(ctx context.Context, id GraphRunID, rev uint64, step StepID)
	OnInterrupt    func(ctx context.Context, iv Interruption)
	OnHalt         func(ctx context.Context, h Halt)
}

// hookDispatcher accumulates the Hooks sets registered for a run and fans each
// lifecycle event out to all of them, in registration order. It is unexported:
// the coordinator constructs one and the Phase-6 WithHooks option feeds it via
// add. The zero value (no sets) is a valid no-op dispatcher.
type hookDispatcher struct {
	sets []Hooks
}

// add appends a hook set, preserving registration order. WithHooks (Phase 6) is
// repeatable, so a run may accumulate several sets; each fires in the order it
// was added.
func (d *hookDispatcher) add(h Hooks) {
	d.sets = append(d.sets, h)
}

// safeHook runs f within a recover so a panicking — or nil — callback can never
// escape into the engine's control flow (§12.5). A nil f is a no-op; any panic
// is recovered and discarded (hooks are purely observational, so the failure is
// dropped rather than propagated). Fire methods additionally skip nil callbacks
// before reaching here, so the nil branch is a defensive backstop.
func safeHook(f func()) {
	if f == nil {
		return
	}
	defer func() { _ = recover() }()
	f()
}

// onRunStart fires OnRunStart for every accumulated set in registration order,
// skipping nil callbacks and isolating each behind safeHook.
func (d *hookDispatcher) onRunStart(ctx context.Context, ev GraphRunState) {
	for _, h := range d.sets {
		if h.OnRunStart == nil {
			continue
		}
		cb := h.OnRunStart
		safeHook(func() { cb(ctx, ev) })
	}
}

// onRunFinish fires OnRunFinish for every accumulated set in registration order,
// skipping nil callbacks and isolating each behind safeHook.
func (d *hookDispatcher) onRunFinish(ctx context.Context, ev GraphRunState) {
	for _, h := range d.sets {
		if h.OnRunFinish == nil {
			continue
		}
		cb := h.OnRunFinish
		safeHook(func() { cb(ctx, ev) })
	}
}

// onVertexStart fires OnVertexStart for every accumulated set in registration
// order, skipping nil callbacks and isolating each behind safeHook.
func (d *hookDispatcher) onVertexStart(ctx context.Context, ev VertexState) {
	for _, h := range d.sets {
		if h.OnVertexStart == nil {
			continue
		}
		cb := h.OnVertexStart
		safeHook(func() { cb(ctx, ev) })
	}
}

// onVertexFinish fires OnVertexFinish for every accumulated set in registration
// order, skipping nil callbacks and isolating each behind safeHook.
func (d *hookDispatcher) onVertexFinish(ctx context.Context, ev VertexState) {
	for _, h := range d.sets {
		if h.OnVertexFinish == nil {
			continue
		}
		cb := h.OnVertexFinish
		safeHook(func() { cb(ctx, ev) })
	}
}

// onEdge fires OnEdge for every accumulated set in registration order, skipping
// nil callbacks and isolating each behind safeHook.
func (d *hookDispatcher) onEdge(ctx context.Context, from, to VertexID, run GraphRunState) {
	for _, h := range d.sets {
		if h.OnEdge == nil {
			continue
		}
		cb := h.OnEdge
		safeHook(func() { cb(ctx, from, to, run) })
	}
}

// onStep fires OnStep for every accumulated set in registration order, skipping
// nil callbacks and isolating each behind safeHook.
func (d *hookDispatcher) onStep(ctx context.Context, run GraphRunState, activated int) {
	for _, h := range d.sets {
		if h.OnStep == nil {
			continue
		}
		cb := h.OnStep
		safeHook(func() { cb(ctx, run, activated) })
	}
}

// onCheckpoint fires OnCheckpoint for every accumulated set in registration
// order, skipping nil callbacks and isolating each behind safeHook.
func (d *hookDispatcher) onCheckpoint(ctx context.Context, id GraphRunID, rev uint64, step StepID) {
	for _, h := range d.sets {
		if h.OnCheckpoint == nil {
			continue
		}
		cb := h.OnCheckpoint
		safeHook(func() { cb(ctx, id, rev, step) })
	}
}

// onInterrupt fires OnInterrupt for every accumulated set in registration order,
// skipping nil callbacks and isolating each behind safeHook. The coordinator
// (Phase 6) calls this once per paused vertex (§11); the dispatcher itself fires
// whatever it is given.
func (d *hookDispatcher) onInterrupt(ctx context.Context, iv Interruption) {
	for _, h := range d.sets {
		if h.OnInterrupt == nil {
			continue
		}
		cb := h.OnInterrupt
		safeHook(func() { cb(ctx, iv) })
	}
}

// onHalt fires OnHalt for every accumulated set in registration order, skipping
// nil callbacks and isolating each behind safeHook.
func (d *hookDispatcher) onHalt(ctx context.Context, h Halt) {
	for _, set := range d.sets {
		if set.OnHalt == nil {
			continue
		}
		cb := set.OnHalt
		safeHook(func() { cb(ctx, h) })
	}
}
