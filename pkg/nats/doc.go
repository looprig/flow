// Package nats provides the optional Tier-C distribution adapter for the flows
// engine (design §18.4/§18.6): a durable JetStream-backed flow.CheckpointStore
// and a distributed flow.ControlPlane, plus an embedded in-process JetStream
// server for single-process durable runs and tests.
//
// # Why this is a SEPARATE Go module
//
// The engine core (github.com/looprig/flow: pkg/flow, pkg/uuid) and the stdlib
// service adapters (pkg/registry, pkg/controlplane, pkg/ingress) target ZERO
// runtime dependencies (CLAUDE.md). NATS is a heavyweight external dependency, so
// it is confined to this nested module (its own go.mod) — NATS therefore never
// enters the core module's go.sum, and a consumer that does not import pkg/nats
// links no NATS code. This is the single most important property of the boundary:
// `go mod graph` at the repo root shows no nats-io modules. The module depends on
// the core via a replace directive (../..) for local development.
//
// # What it provides (behind the SAME interfaces)
//
//   - Store      — a flow.CheckpointStore backed by a JetStream stream (one subject
//     per run, all revisions retained), honoring the same append-only, compare-and-
//     append, latest-is-source-of-truth contract as flow.MemStore (§10.2).
//   - ControlPlane — a flow.ControlPlane backed by JetStream work streams with
//     version-routed subjects, durable consumers, and per-run single-flight,
//     honoring the same Submit/Consume/Ack/Nack contract as controlplane.Mem
//     (§18.5).
//   - Embedded   — an in-process JetStream server so a single process can run
//     durably without an external nats-server (§18.4 "local = embedded").
//
// Because these satisfy the flow-local interfaces structurally, moving from the
// in-process Tier B to the distributed Tier C is just an import plus swapping the
// two constructors at the composition root (§18.6) — no engine edits.
//
// # Security
//
// Stored checkpoint bytes are UNTRUSTED on read (§10.4): the durable backends here
// bound payload size and decode into concrete types before any value reaches engine
// logic (CLAUDE.md: serialization is a trust boundary). Every operation honors its
// context deadline; no call blocks unboundedly.
package nats
