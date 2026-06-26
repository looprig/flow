package flow

import (
	"errors"
	"strconv"
)

// This file is the engine's typed-error catalogue (design §12.4). Per CLAUDE.md,
// every distinct failure mode is a concrete struct so callers can errors.As to
// inspect its cause and context; no package-level API returns a bare
// errors.New/fmt.Errorf. Each error implements error via a POINTER receiver (so
// errors.As targeting *T is unambiguous and uniform across the catalogue), and
// the cause-carrying ones (those with an Err field) also implement Unwrap so
// errors.Is/errors.As reach the wrapped cause. Every message is prefixed "flow: "
// and names the salient fields so an operator can identify the failure from a
// log line — but interrupt payloads are never printed (see interruptSignal).

// --- Build (validation) errors (§8) -----------------------------------------

// DuplicateVertexError reports that the same VertexID was added to a graph more
// than once; VertexIDs must be unique within a graph (§8).
type DuplicateVertexError struct{ VertexID VertexID }

// Error names the duplicated vertex.
func (e *DuplicateVertexError) Error() string {
	return "flow: duplicate vertex " + e.VertexID.String()
}

// DuplicateConditionalEdgeError reports that a second conditional out-edge was
// added for a vertex that already has one; a vertex may have at most one
// conditional edge, since a second would silently overwrite the first (§7).
type DuplicateConditionalEdgeError struct{ From VertexID }

// Error names the vertex with the duplicate conditional edge.
func (e *DuplicateConditionalEdgeError) Error() string {
	return "flow: duplicate conditional edge for vertex " + e.From.String()
}

// UnknownVertexError reports that an edge, condition, error-route, or checkpoint
// frontier endpoint references a VertexID that is not in the compiled graph.
// Used both at build (§8) and at checkpoint validation on load (§10.4).
type UnknownVertexError struct{ VertexID VertexID }

// Error names the unknown vertex.
func (e *UnknownVertexError) Error() string {
	return "flow: unknown vertex " + e.VertexID.String()
}

// UnreachableVertexError reports that a vertex cannot be reached from the entry
// vertex (or that finish is unreachable); every vertex must be reachable (§8).
type UnreachableVertexError struct{ VertexID VertexID }

// Error names the unreachable vertex.
func (e *UnreachableVertexError) Error() string {
	return "flow: unreachable vertex " + e.VertexID.String()
}

// AmbiguousRoutingError reports that a vertex declares both an unconditional
// out-edge and a conditional edge, so its routing is ambiguous (§8).
type AmbiguousRoutingError struct{ VertexID VertexID }

// Error names the ambiguously-routed vertex.
func (e *AmbiguousRoutingError) Error() string {
	return "flow: ambiguous routing for vertex " + e.VertexID.String()
}

// MissingEntryError reports that a named role vertex is absent from the graph.
// Role is "entry" or "finish" (§8).
type MissingEntryError struct {
	VertexID VertexID
	Role     string // "entry" or "finish"
}

// Error names the absent role and the vertex it should have referenced.
func (e *MissingEntryError) Error() string {
	return "flow: missing " + e.Role + " vertex " + e.VertexID.String()
}

// BuildError reports a fail-fast violation of an add-time build invariant that
// has no more specific typed error: a nil required argument (task, selector,
// reducer, condition Pick) or a malformed identifier/parameter supplied to a
// build call (a zero VertexID, an empty Condition.Targets, a second conditional
// edge on a from that already has one). It is distinct from the structural
// Compile checks (§8); those keep their own typed errors (UnknownVertexError,
// AmbiguousRoutingError, …). Op names the failing build call (e.g. "AddVertex",
// "AddEdge", "AddConditionalEdge") and Detail names the offending parameter so
// an operator can identify the bad call from a log line.
type BuildError struct {
	Op     string // the build call: "AddVertex" | "AddEdge" | "AddConditionalEdge"
	Detail string // the offending parameter, e.g. "nil task" or "zero from vertex"
}

// Error names the failing build call and the offending parameter.
func (e *BuildError) Error() string {
	return "flow: " + e.Op + ": " + e.Detail
}

// internalTypeError reports a failed type assertion at the engine's single
// type-erasure seam (§6): a value boxed as any inside a vertex[S] closure did
// not narrow back to the concrete I/O the binding was built with. This is an
// INTERNAL INVARIANT violation that must never occur in normal flow (AddVertex
// builds the closures and the matching boxers together), so it is unexported and
// surfaced as a typed error — never a panic and never a leaked any — so the
// coordinator can fail securely (CLAUDE.md: fail secure; never leak any to
// callers). Want is the expected Go type, Got the actual one.
type internalTypeError struct {
	Seam string // the seam method: "execute" | "applyReducer"
	Want string // expected concrete type
	Got  string // actual type received
}

// Error names the seam, the expected type, and the type actually received.
func (e *internalTypeError) Error() string {
	return "flow: internal type error at " + e.Seam + ": expected " + e.Want + ", got " + e.Got
}

// --- Runtime errors (§9, §12) -----------------------------------------------

// MaxStepsExceededError reports that the super-step budget (WithMaxSteps) was
// exhausted before the run completed; a run-level halt, not a vertex pause
// (§9.5, §9.8).
type MaxStepsExceededError struct {
	Max  int
	Step StepID
}

// Error names the budget and the step at which it was exceeded.
func (e *MaxStepsExceededError) Error() string {
	return "flow: max steps exceeded: limit " + strconv.Itoa(e.Max) + " reached at step " + e.Step.String()
}

// UndeclaredTargetError reports that a condition's Pick returned a target that
// is not in its declared Targets set; a zero Target denotes an empty return
// (Pick returned no target at all), which is equally illegal (§9.5, §9.8).
type UndeclaredTargetError struct {
	From   VertexID
	Target VertexID // zero value denotes an empty return (no target picked)
}

// Error names the source vertex and the offending target, distinguishing an
// undeclared target from an empty return.
func (e *UndeclaredTargetError) Error() string {
	if e.Target == (VertexID{}) {
		return "flow: condition at vertex " + e.From.String() + " returned an empty target set"
	}
	return "flow: condition at vertex " + e.From.String() + " returned undeclared target " + e.Target.String()
}

// DeadEndError reports that the frontier drained without the finish vertex ever
// executing, so the run can make no further progress; a run-level halt (§9.5,
// §9.8).
type DeadEndError struct{ Step StepID }

// Error names the step at which the frontier drained.
func (e *DeadEndError) Error() string {
	return "flow: dead end at step " + e.Step.String() + ": frontier drained before finish executed"
}

// ConditionError reports that a condition's Pick returned an error or panicked.
// It wraps the cause and surfaces as a run-level halt (HaltCondition, §9.5,
// §9.8).
type ConditionError struct {
	From VertexID
	Err  error
}

// Error names the source vertex and the underlying cause.
func (e *ConditionError) Error() string {
	return "flow: condition at vertex " + e.From.String() + " failed: " + e.Err.Error()
}

// Unwrap returns the underlying cause so errors.Is/As can inspect it.
func (e *ConditionError) Unwrap() error { return e.Err }

// VertexError reports that a vertex's task failed, panicked, or timed out. It
// carries the logical vertex, the concrete execution, the attempt number, and
// the wrapped cause, and drives the vertex's error policy (§12.2, §12.4).
type VertexError struct {
	VertexID    VertexID
	VertexRunID VertexRunID
	Attempt     int
	Err         error
}

// Error names the vertex, its execution, the attempt, and the underlying cause.
func (e *VertexError) Error() string {
	return "flow: vertex " + e.VertexID.String() +
		" (run " + e.VertexRunID.String() + ", attempt " + strconv.Itoa(e.Attempt) + ") failed: " +
		e.Err.Error()
}

// Unwrap returns the underlying cause so errors.Is/As can inspect it.
func (e *VertexError) Unwrap() error { return e.Err }

// --- Durability / engine errors (§10, §8.1) ---------------------------------

// CheckpointDecodeError reports that part of a loaded checkpoint failed to
// decode. Field names the failing part ("StepBase", "State", or "checkpoint").
// It wraps the decode cause (§10.4).
type CheckpointDecodeError struct {
	Field string // "StepBase" | "State" | "checkpoint"
	Err   error
}

// Error names the failing field and the underlying decode cause.
func (e *CheckpointDecodeError) Error() string {
	return "flow: failed to decode checkpoint " + e.Field + ": " + e.Err.Error()
}

// Unwrap returns the underlying decode cause so errors.Is/As can inspect it.
func (e *CheckpointDecodeError) Unwrap() error { return e.Err }

// CheckpointNotFoundError reports that no checkpoint exists for the given run,
// so there is nothing to resume from (§10.2).
type CheckpointNotFoundError struct{ GraphRunID GraphRunID }

// Error names the run with no checkpoint.
func (e *CheckpointNotFoundError) Error() string {
	return "flow: no checkpoint found for run " + e.GraphRunID.String()
}

// ResumeTerminalError reports an attempt to resume a run that is already in a
// terminal state (RunCompleted or RunCancelled), which cannot continue (§9.3,
// §12.4).
type ResumeTerminalError struct{ Status RunStatus }

// Error names the terminal status that blocks the resume.
func (e *ResumeTerminalError) Error() string {
	return "flow: cannot resume run in terminal status " + e.Status.String()
}

// GraphMismatchError reports that a loaded checkpoint's GraphID does not match
// the runner's compiled graph, so it belongs to a different graph (§10.4).
type GraphMismatchError struct {
	Expected GraphID
	Actual   GraphID
}

// Error names the expected and actual graph identities.
func (e *GraphMismatchError) Error() string {
	return "flow: graph mismatch: expected " + e.Expected.String() + ", got " + e.Actual.String()
}

// GraphVersionMismatchError reports that a loaded checkpoint's GraphVersion
// fingerprint does not match the current compiled graph's, so a changed graph
// cannot resume an old checkpoint (§8.1, §10.4).
type GraphVersionMismatchError struct {
	Expected string
	Actual   string
}

// Error names the expected and actual graph-version fingerprints.
func (e *GraphVersionMismatchError) Error() string {
	return "flow: graph version mismatch: expected " + e.Expected + ", got " + e.Actual
}

// GraphRunExistsError reports an attempt to start a run whose GraphRunID already
// exists in the store (a duplicate start) (§18.2).
type GraphRunExistsError struct{ GraphRunID GraphRunID }

// Error names the already-existing run.
func (e *GraphRunExistsError) Error() string {
	return "flow: run " + e.GraphRunID.String() + " already exists"
}

// RevisionConflictError reports a failed compare-and-append: the revision we
// tried to append is no longer next because another writer advanced the run.
// Expected is the revision we attempted; Actual is the current latest (§10.2).
type RevisionConflictError struct {
	GraphRunID GraphRunID
	Expected   uint64 // revision we tried to append
	Actual     uint64 // current latest revision
}

// Error names the run and the attempted versus current revisions.
func (e *RevisionConflictError) Error() string {
	return "flow: revision conflict for run " + e.GraphRunID.String() +
		": tried to append revision " + strconv.FormatUint(e.Expected, 10) +
		", current latest is " + strconv.FormatUint(e.Actual, 10)
}

// StoreError reports a failure of a CheckpointStore operation. Op names the
// operation ("Append", "Latest", or "History"); it wraps the store cause
// (§10.2, §12.3).
type StoreError struct {
	Op  string // "Append" | "Latest" | "History"
	Err error
}

// Error names the failing store operation and the underlying cause.
func (e *StoreError) Error() string {
	return "flow: store operation " + e.Op + " failed: " + e.Err.Error()
}

// Unwrap returns the underlying store cause so errors.Is/As can inspect it.
func (e *StoreError) Unwrap() error { return e.Err }

// --- Internal interrupt signal (the seam Phase 5 builds on) -----------------

// interruptSignal is the UNEXPORTED typed error a vertex raises to pause itself.
// The engine detects it via errors.As (see asInterrupt) and turns it into a
// durable checkpoint interrupt rather than a failure. The public Interrupt /
// StatefulInterrupt constructors and the typed ResumePayload / InterruptState
// readers are Task 5.2; this file defines only the carrier and its detector.
//
// info and continuation are any because they are SERIALIZATION-BOUNDARY values
// (§10.3): they are persisted in the checkpoint as json.RawMessage and read back
// through the typed ResumePayload[T]/InterruptState[T] seam, so they are never
// narrowed to a concrete type inside the engine. They are deliberately NOT
// printed by Error() — an interrupt message must not leak a user payload
// (CLAUDE.md: log events, not secrets).
type interruptSignal struct {
	info         any  // user reason (Awaiting) — serialization boundary (§10.3)
	continuation any  // task continuation (StatefulInterrupt) — serialization boundary (§10.3)
	stateful     bool // true for StatefulInterrupt (a continuation was supplied)
}

// Error reports a vertex interrupt WITHOUT printing the carried payloads.
func (e *interruptSignal) Error() string { return "flow: vertex interrupt" }

// asInterrupt reports whether err is (or wraps) an *interruptSignal, returning
// the signal so the engine can read its info/continuation/stateful. It is the
// single detection seam for the pause path.
func asInterrupt(err error) (*interruptSignal, bool) {
	var sig *interruptSignal
	if errors.As(err, &sig) {
		return sig, true
	}
	return nil, false
}
