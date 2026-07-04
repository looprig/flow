package flow

import (
	"context"
	"encoding/json"
)

// This file implements the RunnerHandle JSON seam (design §18.1): the
// graph-agnostic, JSON-in/out wrapper of a typed Runner[S]. It exists because S
// is JSON-serializable, so a NON-generic caller (the registry/ingress, §18.1) can
// drive ANY Runner[S] over JSON without naming S. The seam is the only place the
// engine accepts external JSON as a run's INITIAL state, so the stateJSON decode
// is an untrusted-input trust boundary: it decodes into the concrete S (never an
// any flowing into business logic) and fails secure with a typed
// CheckpointDecodeError before any task runs (CLAUDE.md: validate at every
// boundary; serialization is a trust boundary).
//
// The handle adds NO new behavior over Runner[S] — it only marshals/unmarshals at
// the edges and delegates (open/closed: a new graph needs zero edits here). It
// does NOT execute the graph itself; Runner[S] remains the sole executor (§18.1).

// RunResult is the graph-agnostic, JSON-friendly form of Result[S] (§18.1): State
// is the marshaled final/last-checkpointed S; Run, Interrupts and Halt are already
// non-generic and copied through unchanged. As in Result[S], Interrupts and Halt
// are mutually exclusive and nil on the happy path.
type RunResult struct {
	Run        GraphRunState   // ids, status, step, revision, timestamps (§4.1)
	State      json.RawMessage // the marshaled final/last-checkpointed S
	Interrupts []Interruption  // per-vertex pauses (§9.7) — nil on the happy path
	Halt       *Halt           // run-level halt (§9.8) — nil on the happy path
}

// RunnerHandle is the non-generic, JSON-in/out facade over a typed Runner[S]
// (§18.1). It lets a graph-agnostic caller (the registry/ingress) start, resume,
// query, and cancel any compiled graph by JSON alone. It mirrors the in-process
// control surface (Run/Resume/Status/Get/Cancel + GraphID/GraphVersion, §18.2)
// with S erased to json.RawMessage at the boundaries. Construct one with
// NewRunnerHandle.
type RunnerHandle interface {
	// GraphID returns the wrapped Runner's stable definition identity (§8.1).
	GraphID() GraphID
	// GraphVersion returns the wrapped Runner's compatibility fingerprint (§8.1).
	GraphVersion() string
	// Run decodes stateJSON into the graph state S and starts a run. A malformed
	// stateJSON is rejected at the decode boundary; an empty/nil stateJSON decodes
	// to the zero S.
	Run(ctx context.Context, stateJSON json.RawMessage, opts ...RunOption) (*RunResult, error)
	// Resume continues run id, passing payloadJSON to the run as the live Resume
	// payload (see the runnerHandle.Resume doc for the payload-typing nuance).
	Resume(ctx context.Context, id GraphRunID, payloadJSON json.RawMessage, opts ...RunOption) (*RunResult, error)
	// Status returns the latest GraphRunState for id without decoding S (§18.2).
	Status(ctx context.Context, id GraphRunID) (GraphRunState, error)
	// Get returns the latest run record with the marshaled current State (§18.2).
	Get(ctx context.Context, id GraphRunID) (*RunResult, error)
	// Cancel appends a terminal RunCancelled checkpoint for id (§18.2).
	Cancel(ctx context.Context, id GraphRunID, reason string, opts ...RunOption) error
}

// NewRunnerHandle wraps a typed Runner[S] in the non-generic RunnerHandle facade
// (§18.1). The single type parameter S is captured at construction; thereafter the
// returned handle is graph-agnostic and JSON-in/out, so a registry keyed by
// (GraphID, GraphVersion) can hold handles for many different S uniformly.
func NewRunnerHandle[S any](r *Runner[S]) RunnerHandle {
	return &runnerHandle[S]{r: r}
}

// runnerHandle is the unexported RunnerHandle implementation: a thin JSON adapter
// over a typed Runner[S]. It owns exactly one responsibility — marshal/unmarshal
// S at the edges and delegate — so it adds no execution logic of its own (§18.1).
type runnerHandle[S any] struct {
	r *Runner[S]
}

// GraphID delegates to the wrapped Runner (§8.1).
func (h *runnerHandle[S]) GraphID() GraphID { return h.r.GraphID() }

// GraphVersion delegates to the wrapped Runner (§8.1).
func (h *runnerHandle[S]) GraphVersion() string { return h.r.GraphVersion() }

// Run decodes stateJSON into the concrete S at the untrusted-input boundary, then
// starts the run and converts the typed Result[S] to a JSON-friendly *RunResult.
// A decode failure is a typed *CheckpointDecodeError{Field:"state"} returned
// BEFORE any task runs (fail secure). A nil/empty stateJSON decodes to the zero S
// (treated as the empty initial state) — decodeRunInput documents that convention.
func (h *runnerHandle[S]) Run(ctx context.Context, stateJSON json.RawMessage, opts ...RunOption) (*RunResult, error) {
	in, err := decodeRunInput[S](stateJSON)
	if err != nil {
		return nil, err
	}
	res, err := h.r.Run(ctx, in, opts...)
	if err != nil {
		return nil, err
	}
	return toRunResult(res)
}

// Resume passes payloadJSON to the wrapped Runner as the live Resume payload and
// converts the typed Result[S] to a *RunResult.
//
// PAYLOAD-TYPING NUANCE (§18.1, finalized in the §18.4 ingress layer): the live
// payload is passed AS the json.RawMessage. The engine's ResumePayload[T]
// type-asserts the live value, so a task reading ResumePayload[json.RawMessage]
// receives the bytes, but a task expecting a typed ResumePayload[SomeStruct] will
// NOT match raw bytes through this seam. HTTP-resume payload typing is finalized
// in the ingress layer (8.4); the in-process Runner.Resume with a live typed
// payload is unaffected by this facade. The payloadJSON is not decoded here, so
// there is no decode boundary to guard at this layer.
func (h *runnerHandle[S]) Resume(ctx context.Context, id GraphRunID, payloadJSON json.RawMessage, opts ...RunOption) (*RunResult, error) {
	res, err := h.r.Resume(ctx, id, payloadJSON, opts...)
	if err != nil {
		return nil, err
	}
	return toRunResult(res)
}

// Status delegates to the wrapped Runner; GraphRunState is already non-generic, so
// there is nothing to convert (§18.2).
func (h *runnerHandle[S]) Status(ctx context.Context, id GraphRunID) (GraphRunState, error) {
	return h.r.Status(ctx, id)
}

// Get reads the latest checkpoint via the wrapped Runner and converts the typed
// Result[S] to a JSON-friendly *RunResult (§18.2).
func (h *runnerHandle[S]) Get(ctx context.Context, id GraphRunID) (*RunResult, error) {
	res, err := h.r.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	return toRunResult(res)
}

// Cancel delegates to the wrapped Runner; there is no S to marshal (§18.2).
func (h *runnerHandle[S]) Cancel(ctx context.Context, id GraphRunID, reason string, opts ...RunOption) error {
	return h.r.Cancel(ctx, id, reason, opts...)
}

// decodeRunInput decodes an untrusted run-input JSON into the concrete S at the
// trust boundary (§18.1). A nil/empty stateJSON is the documented "no initial
// state" case and decodes to the zero S (delegating to unmarshalState, which
// treats empty bytes as the zero value). A malformed body is a typed
// *CheckpointDecodeError{Field:"state"} so the caller can errors.As it; nothing is
// run on failure (fail secure).
func decodeRunInput[S any](stateJSON json.RawMessage) (S, error) {
	in, err := unmarshalState[S](stateJSON)
	if err != nil {
		var zero S
		return zero, &CheckpointDecodeError{Field: "state", Err: err}
	}
	return in, nil
}

// toRunResult converts a typed *Result[S] to the JSON-friendly *RunResult by
// marshaling State to json.RawMessage and copying the already-non-generic Run,
// Interrupts and Halt (§18.1). A marshal failure is a typed
// *CheckpointDecodeError{Field:"state"} (the only failure mode here is an
// unencodable S); it is shared by Run, Resume and Get so the conversion lives in
// exactly one place (single responsibility).
func toRunResult[S any](res *Result[S]) (*RunResult, error) {
	stateJSON, err := json.Marshal(res.State)
	if err != nil {
		return nil, &CheckpointDecodeError{Field: "state", Err: err}
	}
	return &RunResult{
		Run:        res.Run,
		State:      stateJSON,
		Interrupts: res.Interrupts,
		Halt:       res.Halt,
	}, nil
}

// Compile-time assertion that runnerHandle satisfies RunnerHandle. A concrete S
// is used because runnerHandle is generic; the assertion proves the method set
// regardless of S.
var _ RunnerHandle = (*runnerHandle[struct{}])(nil)
