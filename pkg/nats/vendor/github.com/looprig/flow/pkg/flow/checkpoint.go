package flow

import "encoding/json"

// This file defines the engine's durable checkpoint data types (design §10.1).
// A Checkpoint is the append-only unit of execution the coordinator writes to a
// CheckpointStore and reloads on resume. These are PLAIN data types — no methods,
// no validation; the JSON codec handles serialization, and the resume-time
// validation that enforces their invariants lives in §10.4 (a later phase).
//
// The integer enum values (StepPhase, InterruptKind, HaltKind) are PERSISTED in
// checkpoints, so their iota ordering is a compatibility contract — reordering
// would silently reinterpret historical runs (mirrors RunStatus/VertexStatus in
// state.go).
//
// The json.RawMessage fields (StepBase, State, InterruptRecord.Info and
// .Continuation) are the sanctioned serialization-boundary `any` of §10.1: they
// carry pre-encoded JSON and must survive a round-trip untouched. The graph
// state S decodes back into its concrete type only at that boundary; the
// frontier inputs are never stored (they are re-derived via selectors on resume).
//
// Those RawMessage fields carry `omitempty`: a json.RawMessage overrides
// UnmarshalJSON, so absent it stays nil but the literal `null` decodes to the
// 4-byte slice []byte("null"). Omitting an empty value keeps the durable
// round-trip IDEMPOTENT (nil → absent → nil) — the §10.1/§15 round-trip-equality
// contract — without disturbing any non-empty pre-encoded JSON.

// StepPhase is the phase of a checkpoint within a super-step (§10.1). Its values
// are persisted in checkpoints, so the iota ordering is pinned.
type StepPhase int

const (
	StepRunning StepPhase = iota // step partly reduced — some vertices terminal, some not
	StepPaused                   // step boundary: ≥1 vertex paused (Interrupts set)
	StepRouted                   // step boundary: routing decisions produced the next Frontier
	StepHalted                   // step boundary: run-level routing/structural halt (Halt set)
)

// Checkpoint is the engine's durable, append-only record of one unit of
// execution (§10.1): the run-level state, the frozen read snapshot S_N, the
// accumulated state S, per-vertex records, the active frontier, the routing
// decisions, and the phase. Interrupts and Halt are mutually exclusive in a
// VALID checkpoint (enforced on load in §10.4); the struct can physically hold
// both, but never does for a well-formed checkpoint.
type Checkpoint struct {
	Run        GraphRunState     // run-level status + Revision + timestamps
	StepBase   json.RawMessage   `json:",omitempty"` // committed S_N — frozen read snapshot; pending vertices' selectors read THIS
	State      json.RawMessage   `json:",omitempty"` // accumulated S (S_N + reducers of every terminal vertex so far)
	Vertices   []VertexState     // per-vertex records for this step; terminal ones are skipped on resume
	Frontier   []VertexID        // the active vertex set this checkpoint resumes from (meaning depends on Phase)
	Routes     []RouteRecord     // routing decisions that produced Frontier (§9.5)
	Phase      StepPhase         // phase of this checkpoint within the super-step
	Interrupts []InterruptRecord // per-vertex pauses; present in StepRunning AND StepPaused; mutually exclusive with Halt (§9.7)
	Halt       *HaltRecord       // run-level routing/structural halt (StepHalted); mutually exclusive with Interrupts (§9.8)
}

// RouteRecord durably records ONE routing decision — which source vertex
// activated which target(s) this step, and whether via a Condition (§10.1).
type RouteRecord struct {
	From        VertexID
	To          []VertexID // chosen target(s); for a Condition, exactly what Pick returned
	Conditional bool       // true if chosen by Condition.Pick; false for a static AddEdge
}

// InterruptKind classifies a vertex pause (§10.1). It is defined HERE because
// InterruptRecord needs it; the public Interrupt struct in §10.3 (Phase 5)
// reuses this SAME type, so its iota ordering is a shared persisted contract.
type InterruptKind int

const (
	Awaiting InterruptKind = iota // user-initiated pause (flow.Interrupt) — Info carries the reason
	Errored                       // failure pause (Pause-policy error) — Cause carries the message
)

// InterruptRecord is the durable record of one paused vertex (§10.1). Info and
// Continuation are serialization-boundary json.RawMessage fields carrying
// pre-encoded JSON (the user reason and an optional StatefulInterrupt
// continuation) that must round-trip untouched.
type InterruptRecord struct {
	Vertex       VertexID
	Kind         InterruptKind   // Awaiting | Errored
	Info         json.RawMessage `json:",omitempty"` // user-facing reason (Awaiting) — a serialization boundary
	Cause        string          // error message/type name (Errored)
	Continuation json.RawMessage `json:",omitempty"` // optional task continuation (StatefulInterrupt) — a serialization boundary
}

// HaltKind classifies a run-level routing/structural halt (§10.1). Persisted in
// checkpoints, so the iota ordering is pinned.
type HaltKind int

const (
	HaltCondition        HaltKind = iota // Condition.Pick returned an error
	HaltUndeclaredTarget                 // Pick returned empty / a target outside Targets
	HaltDeadEnd                          // frontier drained without finish executing
	HaltMaxSteps                         // step budget (WithMaxSteps) exceeded
)

// HaltRecord is the durable record of a run-level halt (§10.1, §9.8). It is set
// only in StepHalted checkpoints and is mutually exclusive with Interrupts.
type HaltRecord struct {
	Kind  HaltKind
	Step  StepID
	Cause string // error message/type name
}
