package flow

import (
	"strconv"
	"time"
)

// This file defines the engine's framework-owned, NOT user-extensible
// instrumentation records and their status enums (design §4.1). They carry
// identity, status, attempt, and lifecycle timestamps for a graph run and its
// vertices. The graph state S is the caller's domain blackboard and is a
// separate, user-extensible concept; these records are never part of S.
//
// The integer enum values are PERSISTED in checkpoints, so their iota ordering
// is a compatibility contract — reordering would silently reinterpret historical
// runs. Zero-value time.Time means "not reached", and a vertex execution reaches
// exactly one terminal timestamp (CompletedAt | InterruptedAt | FailedAt).

// RunStatus is the lifecycle status of a graph run. Its values are persisted in
// checkpoints, so the iota ordering is pinned (§4.1).
type RunStatus int

const (
	RunRunning     RunStatus = iota // in flight
	RunCompleted                    // finish executed and the frontier drained
	RunInterrupted                  // Awaiting or Errored — there is no RunFailed (§12)
	RunCancelled                    // terminal: Cancel(id) appended a final checkpoint (§18.2); cannot resume
)

// GraphRunState is the run-level instrumentation record: identity, status, step,
// revision, and lifecycle timestamps for one graph run (§4.1).
type GraphRunState struct {
	GraphRunID    GraphRunID
	GraphID       GraphID
	GraphVersion  string // compiled-graph fingerprint (§8.1); a mismatch on resume → GraphVersionMismatchError
	Status        RunStatus
	Step          StepID
	Revision      uint64    // monotonic checkpoint sequence for this run (§10)
	CreatedAt     time.Time // Run() called
	StartedAt     time.Time // first super-step began
	UpdatedAt     time.Time // last checkpoint write
	CompletedAt   time.Time // set when Status == RunCompleted
	InterruptedAt time.Time // set when Status == RunInterrupted (most recent pause)
	CancelledAt   time.Time // set when Status == RunCancelled
	CancelReason  string    // Cancel(id, reason)'s reason, when cancelled
}

// VertexStatus is the lifecycle status of a single vertex execution. Its values
// are persisted in checkpoints, so the iota ordering is pinned (§4.1).
type VertexStatus int

const (
	VertexPending VertexStatus = iota
	VertexRunning
	VertexDone
	VertexInterrupted
	VertexFailed
)

// VertexState is the per-vertex-execution instrumentation record: identity,
// status, attempt, and lifecycle timestamps (§4.1).
type VertexState struct {
	VertexID      VertexID
	VertexRunID   VertexRunID
	Step          StepID
	Status        VertexStatus
	Attempt       int
	CreatedAt     time.Time
	StartedAt     time.Time
	CompletedAt   time.Time // Status == VertexDone
	InterruptedAt time.Time // Status == VertexInterrupted
	FailedAt      time.Time // Status == VertexFailed
	Err           string
}

// RunInfo is the identity an executing vertex reads from its context (§4.2). It
// names the graph, run, vertex, vertex execution, and super-step, and computes
// the vertex's IdempotencyKey.
type RunInfo struct {
	GraphID     GraphID
	GraphRunID  GraphRunID
	VertexID    VertexID
	VertexRunID VertexRunID
	Step        StepID
}

// IdempotencyKey identifies one LOGICAL vertex execution, stable across
// in-process retries AND crash recovery. Side-effecting tasks pass it to
// external systems that support idempotency, so a re-run after a pre-checkpoint
// crash does not duplicate effects. It deliberately EXCLUDES VertexRunID and
// Attempt, which vary per concrete attempt.
type IdempotencyKey string

// IdempotencyKey derives the stable logical-execution key for this RunInfo. It
// excludes VertexRunID and Attempt so retries and crash-recovery re-runs of the
// same logical execution share one key (§4.1).
func (i RunInfo) IdempotencyKey() IdempotencyKey {
	return IdempotencyKey("graph=" + i.GraphID.String() +
		"/run=" + i.GraphRunID.String() +
		"/step=" + strconv.Itoa(int(i.Step)) +
		"/vertex=" + i.VertexID.String())
}
