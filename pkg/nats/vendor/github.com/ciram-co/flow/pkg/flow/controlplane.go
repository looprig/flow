package flow

import (
	"context"
	"encoding/json"
	"strconv"
)

// This file defines the CONTROL-PLANE SEAM (design §18.5): the interface and
// value types through which run/resume work is submitted, distributed, and fed
// to workers. They live in pkg/flow — NOT in pkg/controlplane — on purpose:
// flow.Serve (§18.6) is the worker loop and lives here, so if these types lived
// in pkg/controlplane then pkg/flow would import pkg/controlplane (for Serve),
// while pkg/controlplane already imports pkg/flow (for GraphID/GraphRunID) — an
// import cycle. Keeping the seam flow-local breaks the cycle: concrete control
// planes (controlplane.Mem, the future nats.ControlPlane) IMPLEMENT this
// interface structurally, and flow gains no new dependency.
//
// The control plane is SEPARATE from CheckpointStore (CLAUDE.md, §18.5): the
// queue is transient consume-once dispatch; the store is durable append-only
// history. They are distinct interfaces even when both are NATS-backed.

// WorkOp distinguishes the two kinds of work the control plane carries: starting
// a fresh run (OpRun) versus resuming an interrupted one (OpResume). It is a
// uint8 so it is a cheap comparable value with a small, closed domain.
type WorkOp uint8

const (
	// OpRun starts a fresh run: Work.Input is the initial state JSON.
	OpRun WorkOp = iota
	// OpResume resumes an interrupted run: Work.Input is the resume payload JSON.
	OpResume
)

// String renders the operation for logs and test failures. An out-of-range value
// (only constructible by a deserialization fault) renders explicitly rather than
// silently, so a corrupt Work is visible in a log line.
func (o WorkOp) String() string {
	switch o {
	case OpRun:
		return "run"
	case OpResume:
		return "resume"
	default:
		return "WorkOp(" + strconv.Itoa(int(o)) + ")"
	}
}

// GraphVersionKey routes work to a worker serving exactly this (GraphID,
// GraphVersion) — the version IS the route (§18.5). Both fields are comparable
// value types (GraphID is a [16]byte array, GraphVersion a string), so the
// struct is usable directly as a map key with no allocation.
type GraphVersionKey struct {
	GraphID      GraphID
	GraphVersion string
}

// Work is one unit of run/resume work submitted to the control plane (§18.5).
// The GraphRunID is pre-minted by the submitter (ingress, §18.3) so the submit
// is async-first: the GraphRunID can be returned to the caller immediately while
// a worker picks the work up later. Input is the initial state JSON for OpRun, or
// the resume payload JSON for OpResume — a json.RawMessage so the seam stays a
// pure transport that neither parses nor trusts the payload (the worker's Runner
// validates it on the way into typed business logic).
type Work struct {
	Key        GraphVersionKey
	GraphRunID GraphRunID
	Op         WorkOp
	Input      json.RawMessage
}

// Delivery wraps Work with explicit ack semantics so durable backends survive
// worker crashes (§18.5). A worker calls Ack when the work reaches a QUIESCENT
// result (completed / interrupted / halted / cancelled, or safely requeued) to
// drop it from the queue; it calls Nack to requeue the work for redelivery
// (transient failure / shedding load). Exactly one of Ack/Nack should be called
// per Delivery; the control plane holds the run's single-flight slot until one
// of them fires (§18.5).
type Delivery struct {
	Work Work
	Ack  func() error
	Nack func() error
}

// ControlPlane accepts work and distributes it to workers (§18.5). It is SEPARATE
// from CheckpointStore (transient consume-once dispatch vs durable append-only
// history). Implementations must honor ctx on every call (no unbounded blocking).
// Registration is implicit — a worker registers by Consuming the version keys it
// serves; there is no separate registration RPC.
//
// SINGLE-FLIGHT (best-effort, NOT the correctness boundary). An implementation
// SHOULD avoid delivering two Works for the same GraphRunID concurrently, to spare
// duplicate work — but it need not guarantee it. The in-process plane
// (controlplane.Mem) DOES guarantee it via a central dispatcher; a distributed
// plane (nats.ControlPlane) provides single-flight at MESSAGE granularity (a
// work-queue delivers each message once until it is acked), so two DISTINCT
// messages for the same run can be processed concurrently. That is safe by design:
// CORRECTNESS — no duplicate COMMITTED effects — is guaranteed by the store's
// compare-and-append (RevisionConflictError, §10.2) and IdempotencyKey (§4.1),
// which absorb concurrent duplicate work; the control plane's single-flight is only
// an efficiency optimization. Providing STRICTER single-flight than required still
// satisfies this contract (LSP), so controlplane.Mem remains conformant.
type ControlPlane interface {
	// Submit enqueues w for consumers serving w.Key. It honors ctx and must not
	// block unboundedly.
	Submit(ctx context.Context, w Work) error
	// Consume returns a channel delivering only Work whose Key is in serves. The
	// channel is closed when ctx is done (clean shutdown, no goroutine leak).
	Consume(ctx context.Context, serves []GraphVersionKey) (<-chan Delivery, error)
}
