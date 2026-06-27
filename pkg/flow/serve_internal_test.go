package flow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
)

// This file white-box tests the Serve worker loop's per-delivery handler
// (serveOne) for the paths that are awkward to reach through the public Serve loop
// because Serve only consumes reg.Keys() (§18.6). Chief among them: a Work whose
// (GraphID,GraphVersion) does NOT resolve must be Nack'd (reported by requeue, not
// silently dropped, §18.1) — a defensive path that needs an unresolvable Delivery
// fed straight to serveOne. Being in package flow, it reaches the unexported
// serveOne and constructs Deliveries with observable Ack/Nack hooks directly.

// fakeResolver is a test double for the unresolvable / always-resolvable paths.
// It records the (id,version) Resolve was asked for so the test can assert serveOne
// routed by the Work's Key.
type fakeResolver struct {
	handle  RunnerHandle // returned when resolveOK is true
	resolve bool
	gotID   GraphID
	gotVer  string
}

func (f *fakeResolver) Resolve(id GraphID, version string) (RunnerHandle, bool) {
	f.gotID = id
	f.gotVer = version
	return f.handle, f.resolve
}

func (f *fakeResolver) Keys() []GraphVersionKey { return nil }

// ackTracker builds a Delivery for w whose Ack/Nack increment counters, so a test
// can assert exactly one of them fired and which.
type ackTracker struct {
	acks  atomic.Int64
	nacks atomic.Int64
}

func (a *ackTracker) delivery(w Work) Delivery {
	return Delivery{
		Work: w,
		Ack:  func() error { a.acks.Add(1); return nil },
		Nack: func() error { a.nacks.Add(1); return nil },
	}
}

// siVID / siGID mint deterministic non-zero ids for the internal Serve tests.
func siVID(b byte) VertexID {
	var id VertexID
	id[0] = b
	return id
}

func siGID(b byte) GraphID {
	var id GraphID
	id[0] = b
	return id
}

// siState is a minimal JSON-serializable state for the internal Serve tests.
type siState struct {
	N int `json:"n"`
}

// newInternalIncHandle compiles a one-vertex inc Runner and wraps it as a
// RunnerHandle for serveOne's resolvable paths.
func newInternalIncHandle(t *testing.T) RunnerHandle {
	t.Helper()
	entry := siVID(1)
	g := NewGraph[siState](siGID(30))
	task := NewFuncTask(func(_ context.Context, in int) (int, error) { return in + 1, nil })
	sel := func(s siState) int { return s.N }
	red := func(s *siState, out int) error { s.N = out; return nil }
	if err := AddVertex(g, entry, task, sel, red); err != nil {
		t.Fatalf("AddVertex: %v", err)
	}
	r, err := g.Compile(entry, entry, WithStore(NewMemStore()))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return NewRunnerHandle(r)
}

// TestServeOneRouting drives serveOne directly across its decision table: a
// resolvable OpRun → Ack (quiescent result); an UNRESOLVABLE Work → Nack (reported
// by requeue, not dropped, §18.1); a duplicate OpRun (*GraphRunExistsError) →
// idempotent Ack (absorbed, §18.4). Exactly one of Ack/Nack must fire per case.
func TestServeOneRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		resolve  bool
		op       WorkOp
		wantAck  int64
		wantNack int64
	}{
		{name: "resolvable run acks", resolve: true, op: OpRun, wantAck: 1, wantNack: 0},
		{name: "unresolvable nacks (reported not dropped)", resolve: false, op: OpRun, wantAck: 0, wantNack: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newInternalIncHandle(t)
			res := &fakeResolver{resolve: tt.resolve}
			if tt.resolve {
				res.handle = h
			}
			id, err := NewGraphRunID()
			if err != nil {
				t.Fatalf("NewGraphRunID: %v", err)
			}
			w := Work{
				Key:        GraphVersionKey{GraphID: h.GraphID(), GraphVersion: h.GraphVersion()},
				GraphRunID: id,
				Op:         tt.op,
				Input:      json.RawMessage(`{"n":1}`),
			}
			var tr ackTracker
			serveOne(context.Background(), res, tr.delivery(w))

			if got := tr.acks.Load(); got != tt.wantAck {
				t.Errorf("acks = %d, want %d", got, tt.wantAck)
			}
			if got := tr.nacks.Load(); got != tt.wantNack {
				t.Errorf("nacks = %d, want %d", got, tt.wantNack)
			}
			// Routing: serveOne must have resolved by the Work's Key.
			if res.gotID != w.Key.GraphID || res.gotVer != w.Key.GraphVersion {
				t.Errorf("resolved (%v,%q), want (%v,%q)", res.gotID, res.gotVer, w.Key.GraphID, w.Key.GraphVersion)
			}
		})
	}
}

// TestServeOneDuplicateRunAbsorbed proves a duplicate OpRun (the run already
// exists → *GraphRunExistsError) is ABSORBED as an Ack, not Nack'd (§18.4
// at-least-once safety): the first serveOne runs it, the second sees the exists
// error and Acks.
func TestServeOneDuplicateRunAbsorbed(t *testing.T) {
	t.Parallel()

	h := newInternalIncHandle(t)
	res := &fakeResolver{resolve: true, handle: h}
	id, err := NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID: %v", err)
	}
	w := Work{
		Key:        GraphVersionKey{GraphID: h.GraphID(), GraphVersion: h.GraphVersion()},
		GraphRunID: id,
		Op:         OpRun,
		Input:      json.RawMessage(`{"n":0}`),
	}

	var first ackTracker
	serveOne(context.Background(), res, first.delivery(w))
	if first.acks.Load() != 1 || first.nacks.Load() != 0 {
		t.Fatalf("first delivery acks=%d nacks=%d, want 1/0", first.acks.Load(), first.nacks.Load())
	}

	var dup ackTracker
	serveOne(context.Background(), res, dup.delivery(w)) // same id → GraphRunExistsError
	if got := dup.acks.Load(); got != 1 {
		t.Errorf("duplicate acks = %d, want 1 (absorbed, not requeued)", got)
	}
	if got := dup.nacks.Load(); got != 0 {
		t.Errorf("duplicate nacks = %d, want 0 (must not loop)", got)
	}
}

// TestServeOneUnknownOpAcks proves a Work carrying an out-of-range WorkOp (only
// constructible by a corrupt/forged Work crossing the transport) is classified as
// a PERMANENT failure → Ack-and-abandon (§18.5): a corrupt op can NEVER succeed on
// redelivery, so Nacking it would be an infinite redelivery loop (the H1 poison-
// message DoS). execute still returns the typed *UnknownWorkOpError naming the op
// for an operator log line; settle absorbs it with an Ack.
func TestServeOneUnknownOpAcks(t *testing.T) {
	t.Parallel()

	h := newInternalIncHandle(t)
	res := &fakeResolver{resolve: true, handle: h}
	id, err := NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID: %v", err)
	}
	const corruptOp = WorkOp(99)
	w := Work{
		Key:        GraphVersionKey{GraphID: h.GraphID(), GraphVersion: h.GraphVersion()},
		GraphRunID: id,
		Op:         corruptOp,
		Input:      json.RawMessage(`{}`),
	}

	// execute returns the typed error directly.
	gotErr := execute(context.Background(), h, w)
	var unknown *UnknownWorkOpError
	if !errors.As(gotErr, &unknown) {
		t.Fatalf("execute error = %v, want *UnknownWorkOpError", gotErr)
	}
	if unknown.Op != corruptOp {
		t.Errorf("UnknownWorkOpError.Op = %v, want %v", unknown.Op, corruptOp)
	}
	if msg := unknown.Error(); !strings.Contains(msg, corruptOp.String()) {
		t.Errorf("Error() = %q, want it to name op %v", msg, corruptOp)
	}

	// serveOne routes a PERMANENT error to an Ack-and-abandon (not a Nack loop).
	var tr ackTracker
	serveOne(context.Background(), res, tr.delivery(w))
	if got := tr.acks.Load(); got != 1 {
		t.Errorf("acks = %d, want 1 (permanent error → abandon, not redeliver)", got)
	}
	if got := tr.nacks.Load(); got != 0 {
		t.Errorf("nacks = %d, want 0 (permanent error must not loop)", got)
	}
}

// TestSettleClassification locks the §18.5 permanent-vs-transient classification
// (the H1a poison-message fix): a PERMANENT error can never succeed on redelivery,
// so it is Ack'd-and-abandoned (Nacking it is an infinite redelivery loop); only a
// genuinely transient/infra error is Nack'd. nil and *GraphRunExistsError remain
// Acks (quiescent / idempotent absorb).
func TestSettleClassification(t *testing.T) {
	t.Parallel()

	id, err := NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID: %v", err)
	}

	tests := []struct {
		name     string
		err      error
		wantAck  int64
		wantNack int64
	}{
		// Quiescent / idempotent → Ack.
		{name: "nil quiescent result acks", err: nil, wantAck: 1, wantNack: 0},
		{name: "GraphRunExistsError idempotent acks", err: &GraphRunExistsError{GraphRunID: id}, wantAck: 1, wantNack: 0},

		// Permanent (can never succeed on redelivery) → Ack-and-abandon.
		{name: "ResumeTerminalError permanent acks", err: &ResumeTerminalError{Status: RunCompleted}, wantAck: 1, wantNack: 0},
		{name: "GraphMismatchError permanent acks", err: &GraphMismatchError{}, wantAck: 1, wantNack: 0},
		{name: "GraphVersionMismatchError permanent acks", err: &GraphVersionMismatchError{}, wantAck: 1, wantNack: 0},
		{name: "GraphRunMismatchError permanent acks", err: &GraphRunMismatchError{}, wantAck: 1, wantNack: 0},
		{name: "UnknownWorkOpError permanent acks", err: &UnknownWorkOpError{Op: WorkOp(99)}, wantAck: 1, wantNack: 0},
		{name: "CheckpointDecodeError permanent acks", err: &CheckpointDecodeError{Field: "State", Err: errors.New("boom")}, wantAck: 1, wantNack: 0},

		// Transient / infra (may succeed on redelivery) → Nack.
		{name: "StoreError transient nacks", err: &StoreError{Op: "Append", Err: errors.New("down")}, wantAck: 0, wantNack: 1},
		{name: "RevisionConflictError transient nacks", err: &RevisionConflictError{GraphRunID: id}, wantAck: 0, wantNack: 1},
		{name: "unclassified transient nacks", err: errors.New("unclassified"), wantAck: 0, wantNack: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := Work{GraphRunID: id, Op: OpRun}
			var tr ackTracker
			settle(tr.delivery(w), tt.err)
			if got := tr.acks.Load(); got != tt.wantAck {
				t.Errorf("acks = %d, want %d", got, tt.wantAck)
			}
			if got := tr.nacks.Load(); got != tt.wantNack {
				t.Errorf("nacks = %d, want %d", got, tt.wantNack)
			}
		})
	}
}

// TestServeOneTransientNacks proves an engine/infra error from the handle (not a
// quiescent result, not a duplicate-exists) is Nack'd for redelivery (§18.5).
// Resuming an UNKNOWN run id surfaces the store's not-found error — a transient-
// style failure the worker must requeue rather than drop.
func TestServeOneTransientNacks(t *testing.T) {
	t.Parallel()

	h := newInternalIncHandle(t)
	res := &fakeResolver{resolve: true, handle: h}
	id, err := NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID: %v", err)
	}
	w := Work{
		Key:        GraphVersionKey{GraphID: h.GraphID(), GraphVersion: h.GraphVersion()},
		GraphRunID: id, // never started → Resume fails (not a quiescent result)
		Op:         OpResume,
		Input:      json.RawMessage(`{}`),
	}
	var tr ackTracker
	serveOne(context.Background(), res, tr.delivery(w))

	if got := tr.nacks.Load(); got != 1 {
		t.Errorf("nacks = %d, want 1 (transient → redeliver)", got)
	}
	if got := tr.acks.Load(); got != 0 {
		t.Errorf("acks = %d, want 0", got)
	}
}
