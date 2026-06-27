package flow

import (
	"context"
	"errors"
	"time"
)

// This file implements the in-process control surface (design §18.2): the thin,
// STORE-BACKED Runner methods Status, Get, and Cancel. None of them execute the
// graph — they read or append a single checkpoint via the Runner's one
// CheckpointStore (§9). Status reads the latest run-level record without decoding
// S; Get additionally decodes S at the sanctioned serialization boundary and
// reconstructs the run's current outcome (Interrupts / Halt) best-effort from the
// latest checkpoint; Cancel appends a terminal RunCancelled checkpoint and fires
// OnRunFinish so observers see the cancellation (§11). The coordinator's
// observed-cancellation path (a worker append that loses to a concurrent Cancel)
// lives in engine.go.

// Status returns the latest GraphRunState for id — status, step, revision, and
// timestamps — WITHOUT executing the graph or decoding the graph state S (§18.2).
// It is the cheapest control query: a single store.Latest read. A run with no
// checkpoints propagates the store's *CheckpointNotFoundError; any other store
// failure is propagated unchanged.
func (r *Runner[S]) Status(ctx context.Context, id GraphRunID) (GraphRunState, error) {
	cp, err := r.store.Latest(ctx, id)
	if err != nil {
		return GraphRunState{}, err
	}
	return cp.Run, nil
}

// Get returns the latest checkpoint's run record AND its decoded graph state S
// (§18.2), mirroring what Run/Resume returned for the run's current outcome. The
// decode of cp.State into S is the sanctioned serialization boundary; a failure is
// a *CheckpointDecodeError{Field:"State"}. It also reconstructs Result.Interrupts
// (StepPaused) or Result.Halt (StepHalted) best-effort from the latest checkpoint.
//
// BEST-EFFORT caveat: unlike the live typed Result from Run/Resume, the
// reconstructed Interruption.Info is the RAW json.RawMessage (it cannot be
// re-typed without the original payload type — a caller that knows the type
// json.Unmarshals it) and Cause / Halt.Cause are string-wrapped (errors.New of
// the recorded message, not the original error chain).
func (r *Runner[S]) Get(ctx context.Context, id GraphRunID) (*Result[S], error) {
	cp, err := r.store.Latest(ctx, id)
	if err != nil {
		return nil, err
	}
	state, err := unmarshalState[S](cp.State)
	if err != nil {
		return nil, &CheckpointDecodeError{Field: "State", Err: err}
	}
	res := &Result[S]{Run: cp.Run, State: state}
	switch cp.Phase {
	case StepPaused:
		res.Interrupts = reconstructInterrupts(id, cp.Interrupts)
	case StepHalted:
		res.Halt = reconstructHalt(id, cp.Halt)
	}
	return res, nil
}

// reconstructInterrupts rebuilds the per-vertex Interruptions of a paused
// checkpoint best-effort (§18.2): Info is exposed as the raw json.RawMessage (kept
// as the any value so a caller who knows the type can json.Unmarshal it) and an
// Errored Cause is string-wrapped via errors.New of the recorded message (an empty
// Cause stays nil).
func reconstructInterrupts(id GraphRunID, records []InterruptRecord) []Interruption {
	if len(records) == 0 {
		return nil
	}
	out := make([]Interruption, len(records))
	for i, rec := range records {
		iv := Interruption{GraphRunID: id, Vertex: rec.Vertex, Kind: rec.Kind}
		if len(rec.Info) > 0 {
			iv.Info = rec.Info // raw JSON as the any value (best-effort)
		}
		if rec.Cause != "" {
			iv.Cause = errors.New(rec.Cause)
		}
		out[i] = iv
	}
	return out
}

// reconstructHalt rebuilds the run-level Halt of a halted checkpoint best-effort
// (§18.2): Cause is string-wrapped via errors.New of the recorded message. A nil
// HaltRecord (a malformed StepHalted checkpoint) yields a nil Halt.
func reconstructHalt(id GraphRunID, rec *HaltRecord) *Halt {
	if rec == nil {
		return nil
	}
	return &Halt{GraphRunID: id, Kind: rec.Kind, Step: rec.Step, Cause: errors.New(rec.Cause)}
}

// Cancel terminates run id by appending a terminal RunCancelled checkpoint (§18.2)
// and firing OnRunFinish via the resolved hooks (opts → runConfig), so observers
// see the cancellation (§11). It does NOT execute the graph. A run already in a
// terminal status (RunCompleted or RunCancelled) cannot be cancelled and returns
// *ResumeTerminalError{Status} — cancel is terminal-once, and an already-cancelled
// run is rejected rather than treated as a silent no-op (fail loudly). A run with
// no checkpoints propagates *CheckpointNotFoundError.
//
// The append is a compare-and-append at the latest revision + 1 (§10.2): if it
// loses to a concurrent writer the *RevisionConflictError is propagated unchanged.
// A cancelled run cannot resume — Resume's validateNotTerminal already rejects
// RunCancelled.
func (r *Runner[S]) Cancel(ctx context.Context, id GraphRunID, reason string, opts ...RunOption) error {
	cfg := defaultRunConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	cp, err := r.store.Latest(ctx, id)
	if err != nil {
		return err
	}
	if isTerminalStatus(cp.Run.Status) {
		return &ResumeTerminalError{Status: cp.Run.Status}
	}

	cancelled := cancelCheckpoint(cp, reason)
	if err := r.store.Append(ctx, cancelled); err != nil {
		return err
	}
	cfg.hooks.onRunFinish(ctx, cancelled.Run)
	return nil
}

// isTerminalStatus reports whether a run status is terminal — Completed or
// Cancelled — so it can neither resume nor be cancelled (§9.3, §18.2).
func isTerminalStatus(s RunStatus) bool {
	return s == RunCompleted || s == RunCancelled
}

// cancelCheckpoint builds the terminal RunCancelled checkpoint from the latest
// checkpoint (§18.2): it carries forward the accumulated State (so Get still
// works), the frozen StepBase, the frontier, and the per-vertex records, stamps
// the run RunCancelled with CancelledAt/CancelReason, advances the revision, and
// keeps the latest Phase (a terminal marker — the run is not resumable regardless).
func cancelCheckpoint(latest *Checkpoint, reason string) *Checkpoint {
	now := time.Now()
	rs := latest.Run
	rs.Status = RunCancelled
	rs.CancelledAt = now
	rs.CancelReason = reason
	rs.Revision = latest.Run.Revision + 1
	rs.UpdatedAt = now
	return &Checkpoint{
		Run:      rs,
		StepBase: latest.StepBase,
		State:    latest.State,
		Vertices: latest.Vertices,
		Frontier: latest.Frontier,
		Phase:    latest.Phase,
	}
}
