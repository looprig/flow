package flow

import (
	"context"
	"encoding/json"
)

// This file is the interrupt/resume context API (design §10.3, §4.2): the public
// triggers a task raises to pause itself (Interrupt, StatefulInterrupt), the
// typed readers it uses to recover values from its context (ResumePayload,
// InterruptState, Info, Self), and the public records the coordinator surfaces
// for each pause/halt (Interruption, Halt). The unexported with* injectors are
// the seam the Phase-6 coordinator uses to populate the context before invoking
// a task; they live here so the readers above are testable in isolation.
//
// The carrier (interruptSignal) and its detector (asInterrupt) live in errors.go;
// the kind enums (InterruptKind, HaltKind) live in checkpoint.go. This file does
// NOT redefine them — it constructs the signal and reuses the enums.

// --- Public result records (§10.3) ------------------------------------------

// Interruption is the per-vertex pause surfaced in a run Result and to
// Hooks.OnInterrupt (§10.3). Info carries the user reason for an Awaiting pause
// and is an `any` because it is a serialization-boundary value (the same reason
// passed to Interrupt); Cause carries the underlying failure for an Errored
// pause. Exactly one is meaningful per Kind.
type Interruption struct {
	GraphRunID GraphRunID
	Vertex     VertexID
	Kind       InterruptKind // Awaiting | Errored (defined in checkpoint.go)
	Info       any           // user reason (Awaiting) — serialization boundary (§10.3)
	Cause      error         // underlying failure (Errored)
}

// Halt is a RUN-LEVEL routing/structural failure surfaced in a run Result — not
// a vertex pause (§10.3, §9.8). Kind classifies the structural cause; Step names
// the super-step at which the run halted; Cause wraps the underlying error.
type Halt struct {
	GraphRunID GraphRunID
	Kind       HaltKind // defined in checkpoint.go
	Step       StepID
	Cause      error
}

// --- Trigger constructors (§10.3) -------------------------------------------

// Interrupt pauses the calling vertex with a user-facing reason, returning the
// error a task returns to request the pause. The coordinator detects it via
// asInterrupt and writes an Awaiting interrupt to the checkpoint rather than
// failing the run; info is persisted as the InterruptRecord.Info serialization
// boundary and read back through ResumePayload on resume.
//
// ctx is part of the documented signature and is intentionally unused today; a
// future revision may read run identity from it. It is named ctx and accepted so
// the signature is stable.
func Interrupt(ctx context.Context, info any) error {
	_ = ctx
	return &interruptSignal{info: info}
}

// StatefulInterrupt pauses the calling vertex like Interrupt but also stows a
// LIVE continuation value so the task can pick up where it left off on resume.
// The continuation is held in the signal untouched; the coordinator marshals it
// to the InterruptRecord.Continuation serialization boundary when it writes the
// checkpoint, and the task reads it back through InterruptState on resume. This
// constructor does NOT marshal — it only carries the live value (§10.3).
//
// ctx is part of the documented signature and is intentionally unused today (see
// Interrupt).
func StatefulInterrupt(ctx context.Context, info, continuation any) error {
	_ = ctx
	return &interruptSignal{info: info, continuation: continuation, stateful: true}
}

// --- Context keys ------------------------------------------------------------

// interruptCtxKey is the private key type for this file's context values, so
// engine values can never collide with another package's keys (idiomatic
// context.Value pattern). The iota consts give each injected value a distinct
// key. It is named interruptCtxKey rather than ctxKey because the package's test
// scope already defines an unrelated ctxKey for the Task contract.
type interruptCtxKey int

const (
	ctxKeyResumePayload  interruptCtxKey = iota // live Resume payload (ResumePayload)
	ctxKeyInterruptState                        // restored continuation bytes (InterruptState)
	ctxKeyRunInfo                               // coordinator-injected RunInfo (Info)
	ctxKeySelf                                  // coordinator-injected VertexState (Self)
)

// --- Typed context readers (§10.3, §4.2) ------------------------------------

// ResumePayload returns the value passed to Resume(ctx, id, payload) for the run
// the calling vertex belongs to (§10.3, §9.7). The payload is a LIVE Go value
// supplied in-process at Resume, so it is recovered by TYPE ASSERTION to T — it
// is never decoded from JSON here. (Contrast InterruptState, which decodes a
// persisted continuation.) Returns (zero, false) if no payload was injected or
// it is not a T.
func ResumePayload[T any](ctx context.Context) (T, bool) {
	v := ctx.Value(ctxKeyResumePayload)
	if v == nil {
		var zero T
		return zero, false
	}
	t, ok := v.(T)
	if !ok {
		var zero T
		return zero, false
	}
	return t, true
}

// InterruptState returns the StatefulInterrupt continuation restored for the
// calling vertex on resume (§10.3). Unlike ResumePayload, the continuation was
// PERSISTED as json.RawMessage in the checkpoint's InterruptRecord.Continuation
// and restored into the context as bytes, so it is recovered by json.Unmarshal
// into T — the typed-read side of the §10.3 serialization boundary. Returns
// (zero, false) if no continuation was injected, the bytes are nil, or they do
// not decode into T.
func InterruptState[T any](ctx context.Context) (T, bool) {
	var zero T
	v := ctx.Value(ctxKeyInterruptState)
	if v == nil {
		return zero, false
	}
	raw, ok := v.(json.RawMessage)
	if !ok || len(raw) == 0 {
		return zero, false
	}
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		return zero, false
	}
	return out, true
}

// Info returns the coordinator-injected RunInfo identity for the calling vertex
// (§4.2). Returns (zero, false) if no RunInfo was injected.
func Info(ctx context.Context) (RunInfo, bool) {
	v := ctx.Value(ctxKeyRunInfo)
	if v == nil {
		return RunInfo{}, false
	}
	info, ok := v.(RunInfo)
	if !ok {
		return RunInfo{}, false
	}
	return info, true
}

// Self returns the coordinator-injected VertexState record for the calling
// vertex (§4.2). Returns (zero, false) if no VertexState was injected.
func Self(ctx context.Context) (VertexState, bool) {
	v := ctx.Value(ctxKeySelf)
	if v == nil {
		return VertexState{}, false
	}
	vs, ok := v.(VertexState)
	if !ok {
		return VertexState{}, false
	}
	return vs, true
}

// --- Unexported context injectors (Phase-6 coordinator seam) ----------------

// withResumePayload stows the LIVE Resume payload in ctx for ResumePayload to
// type-assert. It is unexported: only the coordinator populates it.
func withResumePayload(ctx context.Context, payload any) context.Context {
	return context.WithValue(ctx, ctxKeyResumePayload, payload)
}

// withInterruptState stows the restored continuation bytes in ctx for
// InterruptState to json.Unmarshal. It is unexported: only the coordinator
// populates it from the checkpoint's InterruptRecord.Continuation.
func withInterruptState(ctx context.Context, continuation json.RawMessage) context.Context {
	return context.WithValue(ctx, ctxKeyInterruptState, continuation)
}

// withRunInfo stows the run identity in ctx for Info to read. It is unexported:
// only the coordinator populates it.
func withRunInfo(ctx context.Context, info RunInfo) context.Context {
	return context.WithValue(ctx, ctxKeyRunInfo, info)
}

// withSelf stows the vertex record in ctx for Self to read. It is unexported:
// only the coordinator populates it.
func withSelf(ctx context.Context, vs VertexState) context.Context {
	return context.WithValue(ctx, ctxKeySelf, vs)
}
