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
// serves, resolves each to its RunnerHandle, executes it, and Acks at a QUIESCENT
// result (Completed/Interrupted/Halted — any returned Result, §18.5) or on a
// PERMANENT failure (Ack-and-abandon — see settle), Nacking only a transient/infra
// failure for redelivery. Registration is implicit: it consumes exactly reg.Keys().
// A failure to subscribe (Consume error) is returned immediately. Each delivery is
// handled in its own goroutine — the control plane single-flights per run and
// distinct runs are independent, so concurrent handling is safe.
//
// Shutdown semantics. On ctx-cancel the control plane closes the Consume channel,
// so the range ends; Serve then waits (via the WaitGroup) for the in-flight
// GOROUTINES to RETURN — it does NOT run their work to completion. Each in-flight
// run executes against the SAME cancelled ctx, so its Run/Resume observes the
// cancellation, returns an error, and is Nack'd: on a DURABLE control plane that
// Nack means the work is redelivered (resumed elsewhere later); on the EPHEMERAL
// in-process plane the Nack's requeue is dropped when the plane shuts down (the run
// is abandoned, its last durable checkpoint intact). Either way Serve returns
// promptly with ctx.Err() (nil only if ctx had no error) and leaks no goroutine.
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
// dropped — Serve consumed only keys it serves, so this is defensive and
// transient: a registration may appear, §18.1). Execution dispatches on the WorkOp;
// the result is classified into Ack (quiescent or permanent-abandon) / Nack
// (transient) by settle.
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
// *UnknownWorkOpError; settle treats it as PERMANENT (Ack-and-abandon) since a
// corrupt op can never dispatch on redelivery (§18.5).
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

// settle classifies an execution outcome into exactly one Ack or Nack (§18.5),
// distinguishing PERMANENT failures from TRANSIENT/infra ones — the H1 poison-
// message fix. The rule: a delivery is Nack'd ONLY when a redelivery could plausibly
// succeed; a failure that can NEVER succeed on redelivery is Ack'd-and-abandoned,
// because Nacking it would be an INFINITE redelivery loop (an externally-triggerable
// 100%-CPU DoS). A real dead-letter queue is a future enhancement; for now the
// abandoned delivery's outcome is observable by the client via GET /runs/{id} (the
// run is simply unchanged).
//
// ACK (quiescent / absorbed):
//   - nil — the work reached a quiescent Result (Completed/Interrupted/Halted are
//     returned Results, not errors, §12.3).
//   - *GraphRunExistsError — a duplicate OpRun redelivery of an already-started run
//     (at-least-once safety, §18.4): the run already ran; absorb idempotently.
//
// ACK-AND-ABANDON (PERMANENT — redelivery can never succeed, so Nacking spins):
//   - *ResumeTerminalError       — the run is Completed/Cancelled; it can never resume.
//   - *GraphMismatchError        — the checkpoint belongs to a different graph.
//   - *GraphVersionMismatchError — the graph changed; an old checkpoint can't resume.
//   - *GraphRunMismatchError     — the store returned a checkpoint for a different run.
//   - *UnknownWorkOpError        — a corrupt/forged WorkOp; no op will ever dispatch.
//   - *CheckpointDecodeError     — the persisted checkpoint is malformed; it won't
//     decode on retry either.
//
// NACK (TRANSIENT/infra — a redelivery may find a healthy dependency or a now-
// resumable/terminal run):
//   - *StoreError            — the store is down/erroring; it may recover.
//   - *RevisionConflictError — a concurrent writer advanced the run; a retry may find
//     the run resumable or now-terminal (observed cancellation, §18.2).
//   - any unclassified error — fail safe toward redelivery so a genuinely transient
//     failure is not silently dropped; a persistently-unclassified error is a bug to
//     reclassify, not a reason to drop work.
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
	if isPermanent(err) {
		_ = d.Ack() // permanent → abandon this delivery (Nack would loop forever)
		return
	}
	_ = d.Nack() // transient/infra → redeliver (§18.5)
}

// isPermanent reports whether err is a failure that can NEVER succeed on redelivery,
// so settle must Ack-and-abandon it rather than Nack into an infinite loop (the H1
// poison-message DoS). The set is closed and enumerated here (open/closed: a new
// permanent class is one new errors.As arm) and matches settle's doc comment.
func isPermanent(err error) bool {
	var (
		resumeTerminal  *ResumeTerminalError
		graphMismatch   *GraphMismatchError
		versionMismatch *GraphVersionMismatchError
		runMismatch     *GraphRunMismatchError
		unknownOp       *UnknownWorkOpError
		decode          *CheckpointDecodeError
	)
	switch {
	case errors.As(err, &resumeTerminal),
		errors.As(err, &graphMismatch),
		errors.As(err, &versionMismatch),
		errors.As(err, &runMismatch),
		errors.As(err, &unknownOp),
		errors.As(err, &decode):
		return true
	default:
		return false
	}
}

// UnknownWorkOpError reports a Work carrying a WorkOp outside the closed
// {OpRun, OpResume} domain (§18.5) — only constructible by a corrupt/forged Work
// crossing the transport. Per CLAUDE.md it is a concrete typed error so a caller
// can errors.As it; settle treats it as a PERMANENT failure (Ack-and-abandon) — a
// corrupt op can never dispatch, so Nacking it would be an infinite redelivery loop
// (the H1 poison-message DoS). The op is named for an operator log line.
type UnknownWorkOpError struct{ Op WorkOp }

// Error names the offending op for an operator log line.
func (e *UnknownWorkOpError) Error() string {
	return "flow: unknown work op " + e.Op.String()
}
