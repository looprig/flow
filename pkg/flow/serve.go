package flow

import (
	"context"
	"errors"
	"sync"
)

// This file implements the worker loop (design §18.6): Serve consumes run/resume
// Work for the graph versions a resolver serves, resolves each to its
// RunnerHandle, executes it, and Acks at a QUIESCENT result / Nacks on a transient
// failure for redelivery (§18.5). It is the ONLY orchestration that ties the
// control-plane seam (consume/ack) to the registry seam (resolve) to the Runner
// (execute), yet it depends on NEITHER concrete package: it takes flow-local
// interfaces (Resolver, ControlPlane) that pkg/registry and pkg/controlplane
// satisfy STRUCTURALLY. That is deliberate (CLAUDE.md dependency inversion): those
// packages import pkg/flow (for GraphID/RunnerHandle), so if Serve imported them
// back it would be an import cycle. Keeping the dependencies as flow-local
// interfaces breaks the cycle and lets a new control plane / resolver drop in with
// zero edits here (open/closed).

// Resolver resolves a (GraphID, GraphVersion) to its RunnerHandle and lists the
// keys this worker serves, so Serve consumes exactly those (§18.5/§18.6 —
// registration is implicit via Consume). It is the SUBSET of the registry surface
// Serve needs (interface segregation: Serve never touches Add/Manifest).
// registry.Registry satisfies it structurally, so no adapter is required.
type Resolver interface {
	// Resolve returns the handle registered under the exact (id, version) and true,
	// or (nil, false) if none is registered.
	Resolve(id GraphID, version string) (RunnerHandle, bool)
	// Keys returns one GraphVersionKey per registration — the exact set of versions
	// this worker serves, which Serve hands to Consume.
	Keys() []GraphVersionKey
}

// Serve runs the worker loop (§18.6): it consumes Work for the versions reg
// serves, resolves each to its RunnerHandle, executes it, and Acks only when the
// work reaches a QUIESCENT result (Completed/Interrupted/Halted — any returned
// Result, §18.5), Nacking on a transient/infra failure for redelivery. It blocks
// until ctx is done (which closes the Consume channel) and all in-flight work has
// settled, then returns ctx.Err() (nil only if ctx had no error). Registration is
// implicit: it consumes exactly reg.Keys(). A failure to subscribe (Consume error)
// is returned immediately. Each delivery is handled in its own goroutine — the
// control plane single-flights per run and distinct runs are independent, so
// concurrent handling is safe — and a WaitGroup ensures Serve drains all in-flight
// work before returning (no goroutine leak).
func Serve(ctx context.Context, reg Resolver, cp ControlPlane) error {
	ch, err := cp.Consume(ctx, reg.Keys())
	if err != nil {
		return err
	}
	var wg sync.WaitGroup
	for d := range ch {
		wg.Add(1)
		go func(d Delivery) {
			defer wg.Done()
			serveOne(ctx, reg, d)
		}(d)
	}
	wg.Wait()
	return ctx.Err()
}

// serveOne handles one Delivery (§18.6): resolve the Work's (GraphID,
// GraphVersion), execute the run/resume, and settle the Delivery with exactly one
// Ack or Nack. An unresolvable key is Nack'd (reported by requeue, never silently
// dropped — Serve consumed only keys it serves, so this is defensive, §18.1).
// Execution dispatches on the WorkOp; the result is classified into Ack/Nack by
// settle.
func serveOne(ctx context.Context, reg Resolver, d Delivery) {
	h, ok := reg.Resolve(d.Work.Key.GraphID, d.Work.Key.GraphVersion)
	if !ok {
		_ = d.Nack() // not-found: redeliver rather than drop (§18.1)
		return
	}
	settle(d, execute(ctx, h, d.Work))
}

// execute runs the Work against the resolved handle by its op (§18.6). OpRun starts
// a fresh run under the PRE-MINTED GraphRunID (ingress mints it, §18.3, so a caller
// can poll GET /runs/{id}); OpResume continues the run id with the resume payload.
// It returns only the error: a non-nil error from Run/Resume that is a returned
// Result is nil (quiescent), so the error alone classifies the outcome. An
// out-of-range WorkOp (only constructible by a corrupt Work) returns a typed
// *UnknownWorkOpError so settle Nacks it rather than silently dropping.
func execute(ctx context.Context, h RunnerHandle, w Work) error {
	switch w.Op {
	case OpRun:
		_, err := h.Run(ctx, w.Input, WithGraphRunID(w.GraphRunID))
		return err
	case OpResume:
		_, err := h.Resume(ctx, w.GraphRunID, w.Input)
		return err
	default:
		return &UnknownWorkOpError{Op: w.Op}
	}
}

// settle classifies an execution outcome into exactly one Ack or Nack (§18.5).
// A nil error means the work reached a quiescent result (Completed/Interrupted/
// Halted are all returned Results, not errors, §12.3) → Ack. A
// *GraphRunExistsError on an OpRun is a duplicate redelivery of an already-started
// run (at-least-once safety, §18.4) → Ack (idempotent absorb, never a Nack loop).
// Any other error is an engine/infra failure (store down, etc., §12.3) → Nack for
// redelivery.
func settle(d Delivery, err error) {
	if err == nil {
		_ = d.Ack()
		return
	}
	var exists *GraphRunExistsError
	if errors.As(err, &exists) {
		_ = d.Ack() // duplicate start already ran — absorb (§18.4)
		return
	}
	_ = d.Nack() // transient → redeliver (§18.5)
}

// UnknownWorkOpError reports a Work carrying a WorkOp outside the closed
// {OpRun, OpResume} domain (§18.5) — only constructible by a corrupt/forged Work
// crossing the transport. Per CLAUDE.md it is a concrete typed error so a caller
// can errors.As it; settle treats it as a transient failure (Nack) so the corrupt
// Work is surfaced via redelivery rather than silently dropped.
type UnknownWorkOpError struct{ Op WorkOp }

// Error names the offending op for an operator log line.
func (e *UnknownWorkOpError) Error() string {
	return "flow: unknown work op " + e.Op.String()
}
