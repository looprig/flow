package flow_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/looprig/flow/pkg/flow"
)

// This file black-box tests the RunnerHandle JSON seam (§18.1): the non-generic,
// JSON-in/out facade over a typed Runner[S] that lets the registry/ingress drive
// any graph without knowing S. It proves Run from stateJSON returns a *RunResult
// whose State unmarshals back to the expected S, that Status/Get round-trip the
// run, that a malformed stateJSON is rejected at the decode boundary with a typed
// error (validate untrusted input), and that Cancel delegates to the Runner.

// hstate is a minimal JSON-serializable graph state for the handle tests.
type hstate struct {
	N int `json:"n"`
}

// Compile-time proof that the public NewRunnerHandle returns the public
// RunnerHandle interface for a concrete S, exercising the seam from outside the
// package (black-box).
var _ flow.RunnerHandle = flow.NewRunnerHandle[hstate](nil)

// hvID mints a deterministic non-zero VertexID from a single byte.
func hvID(b byte) flow.VertexID {
	var id flow.VertexID
	id[0] = b
	return id
}

// hgID mints a deterministic non-zero GraphID from a single byte.
func hgID(b byte) flow.GraphID {
	var id flow.GraphID
	id[0] = b
	return id
}

// newIncRunner compiles a one-vertex Runner[hstate] whose single vertex adds one
// to N, so a Run from {"n":N} completes with State {"n":N+1}. The store is a
// MemStore so Status/Get/Cancel have durable history to read.
func newIncRunner(t *testing.T) *flow.Runner[hstate] {
	t.Helper()
	entry := hvID(1)
	g := flow.NewGraph[hstate](hgID(5))
	task := flow.NewFuncTask(func(_ context.Context, in int) (int, error) { return in + 1, nil })
	sel := func(s hstate) int { return s.N }
	red := func(s *hstate, out int) error { s.N = out; return nil }
	if err := flow.AddVertex(g, entry, task, sel, red); err != nil {
		t.Fatalf("AddVertex: %v", err)
	}
	r, err := g.Compile(entry, entry, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return r
}

// TestRunnerHandleIdentity proves GraphID/GraphVersion delegate to the wrapped
// Runner unchanged.
func TestRunnerHandleIdentity(t *testing.T) {
	t.Parallel()

	r := newIncRunner(t)
	h := flow.NewRunnerHandle(r)
	if h.GraphID() != r.GraphID() {
		t.Errorf("GraphID() = %v, want %v", h.GraphID(), r.GraphID())
	}
	if h.GraphVersion() != r.GraphVersion() {
		t.Errorf("GraphVersion() = %q, want %q", h.GraphVersion(), r.GraphVersion())
	}
}

// TestRunnerHandleRun proves Run decodes stateJSON into S, executes, and returns
// a *RunResult whose State unmarshals back to the expected S. The empty/nil
// stateJSON case proves the documented convention: it decodes to the zero S.
func TestRunnerHandleRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		stateJSON json.RawMessage
		wantN     int
	}{
		{name: "explicit state", stateJSON: json.RawMessage(`{"n":4}`), wantN: 5},
		{name: "zero object", stateJSON: json.RawMessage(`{}`), wantN: 1},
		{name: "nil decodes to zero S", stateJSON: nil, wantN: 1},
		{name: "empty decodes to zero S", stateJSON: json.RawMessage(``), wantN: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := flow.NewRunnerHandle(newIncRunner(t))
			res, err := h.Run(context.Background(), tt.stateJSON)
			if err != nil {
				t.Fatalf("Run: %v", err)
			}
			if res.Run.Status != flow.RunCompleted {
				t.Errorf("Run.Status = %v, want RunCompleted", res.Run.Status)
			}
			if res.Interrupts != nil || res.Halt != nil {
				t.Errorf("happy path Interrupts=%v Halt=%v, want both nil", res.Interrupts, res.Halt)
			}
			var got hstate
			if err := json.Unmarshal(res.State, &got); err != nil {
				t.Fatalf("unmarshal RunResult.State %q: %v", res.State, err)
			}
			if got.N != tt.wantN {
				t.Errorf("final State.N = %d, want %d", got.N, tt.wantN)
			}
		})
	}
}

// TestRunnerHandleRunMalformed proves a malformed stateJSON is rejected at the
// decode boundary (validate untrusted input) with a typed *CheckpointDecodeError
// naming the failing field, and no run is started.
func TestRunnerHandleRunMalformed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		stateJSON json.RawMessage
	}{
		{name: "not json", stateJSON: json.RawMessage(`{`)},
		{name: "wrong type for field", stateJSON: json.RawMessage(`{"n":"not-a-number"}`)},
		{name: "array not object", stateJSON: json.RawMessage(`[1,2,3]`)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := flow.NewRunnerHandle(newIncRunner(t))
			res, err := h.Run(context.Background(), tt.stateJSON)
			if res != nil {
				t.Errorf("Run returned a result on malformed input: %+v", res)
			}
			var decErr *flow.CheckpointDecodeError
			if !errors.As(err, &decErr) {
				t.Fatalf("Run error = %v, want *CheckpointDecodeError", err)
			}
			if decErr.Field == "" {
				t.Error("CheckpointDecodeError.Field is empty")
			}
		})
	}
}

// TestRunnerHandleRunDelegatesError proves a Runner error (here a reused
// GraphRunID → *GraphRunExistsError) is propagated by Run unchanged, with no
// RunResult.
func TestRunnerHandleRunDelegatesError(t *testing.T) {
	t.Parallel()

	h := flow.NewRunnerHandle(newIncRunner(t))
	ctx := context.Background()

	id, err := flow.NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID: %v", err)
	}
	if _, err := h.Run(ctx, json.RawMessage(`{"n":1}`), flow.WithGraphRunID(id)); err != nil {
		t.Fatalf("seed Run: %v", err)
	}
	res, err := h.Run(ctx, json.RawMessage(`{"n":1}`), flow.WithGraphRunID(id))
	if res != nil {
		t.Errorf("Run returned a result on reused id: %+v", res)
	}
	var exists *flow.GraphRunExistsError
	if !errors.As(err, &exists) {
		t.Fatalf("Run reused-id error = %v, want *GraphRunExistsError", err)
	}
}

// TestRunnerHandleGetUnknown proves Get of an unknown run propagates the store's
// *CheckpointNotFoundError unchanged, with no RunResult.
func TestRunnerHandleGetUnknown(t *testing.T) {
	t.Parallel()

	h := flow.NewRunnerHandle(newIncRunner(t))
	id, err := flow.NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID: %v", err)
	}
	res, err := h.Get(context.Background(), id)
	if res != nil {
		t.Errorf("Get returned a result for unknown id: %+v", res)
	}
	var notFound *flow.CheckpointNotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("Get unknown error = %v, want *CheckpointNotFoundError", err)
	}
}

// TestRunnerHandleStatusAndGet proves Status returns the latest GraphRunState and
// Get round-trips the run: its RunResult.State unmarshals back to the completed S.
func TestRunnerHandleStatusAndGet(t *testing.T) {
	t.Parallel()

	h := flow.NewRunnerHandle(newIncRunner(t))
	ctx := context.Background()

	res, err := h.Run(ctx, json.RawMessage(`{"n":10}`))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	id := res.Run.GraphRunID

	st, err := h.Status(ctx, id)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if st.Status != flow.RunCompleted {
		t.Errorf("Status.Status = %v, want RunCompleted", st.Status)
	}
	if st.GraphRunID != id {
		t.Errorf("Status.GraphRunID = %v, want %v", st.GraphRunID, id)
	}

	got, err := h.Get(ctx, id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var s hstate
	if err := json.Unmarshal(got.State, &s); err != nil {
		t.Fatalf("unmarshal Get State %q: %v", got.State, err)
	}
	if s.N != 11 {
		t.Errorf("Get State.N = %d, want 11", s.N)
	}
}

// newPausingRunner compiles a one-vertex Runner whose vertex raises an Awaiting
// interrupt, so a Run leaves the run RunInterrupted (non-terminal) — the state a
// Cancel acts on.
func newPausingRunner(t *testing.T) *flow.Runner[hstate] {
	t.Helper()
	entry := hvID(1)
	g := flow.NewGraph[hstate](hgID(6))
	task := flow.NewFuncTask(func(ctx context.Context, _ int) (int, error) {
		return 0, flow.Interrupt(ctx, "awaiting")
	})
	sel := func(s hstate) int { return s.N }
	red := func(s *hstate, out int) error { s.N = out; return nil }
	if err := flow.AddVertex(g, entry, task, sel, red); err != nil {
		t.Fatalf("AddVertex: %v", err)
	}
	r, err := g.Compile(entry, entry, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return r
}

// newResumableRunner compiles a one-vertex Runner whose vertex interrupts
// (Awaiting) on its first execution and, on resume, reads the live Resume payload
// via ResumePayload[json.RawMessage] — the exact type the RunnerHandle.Resume seam
// passes through (§18.1) — decodes {"n":X} from it, and completes with N=X. This
// exercises the documented payload-typing nuance end to end.
func newResumableRunner(t *testing.T) *flow.Runner[hstate] {
	t.Helper()
	entry := hvID(1)
	g := flow.NewGraph[hstate](hgID(8))
	task := flow.NewFuncTask(func(ctx context.Context, _ int) (int, error) {
		raw, ok := flow.ResumePayload[json.RawMessage](ctx)
		if !ok {
			return 0, flow.Interrupt(ctx, "awaiting payload")
		}
		var p hstate
		if err := json.Unmarshal(raw, &p); err != nil {
			return 0, err
		}
		return p.N, nil
	})
	sel := func(s hstate) int { return s.N }
	red := func(s *hstate, out int) error { s.N = out; return nil }
	if err := flow.AddVertex(g, entry, task, sel, red); err != nil {
		t.Fatalf("AddVertex: %v", err)
	}
	r, err := g.Compile(entry, entry, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return r
}

// TestRunnerHandleResume proves Resume passes payloadJSON through to the run as the
// live payload and converts the result: a paused run resumes with {"n":42} as
// json.RawMessage, the vertex decodes it, and the run completes with State {"n":42}.
func TestRunnerHandleResume(t *testing.T) {
	t.Parallel()

	h := flow.NewRunnerHandle(newResumableRunner(t))
	ctx := context.Background()

	first, err := h.Run(ctx, json.RawMessage(`{"n":0}`))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if first.Run.Status != flow.RunInterrupted {
		t.Fatalf("first Run.Status = %v, want RunInterrupted", first.Run.Status)
	}
	id := first.Run.GraphRunID

	res, err := h.Resume(ctx, id, json.RawMessage(`{"n":42}`))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if res.Run.Status != flow.RunCompleted {
		t.Errorf("Resume Run.Status = %v, want RunCompleted", res.Run.Status)
	}
	var got hstate
	if err := json.Unmarshal(res.State, &got); err != nil {
		t.Fatalf("unmarshal Resume State %q: %v", res.State, err)
	}
	if got.N != 42 {
		t.Errorf("Resume final State.N = %d, want 42", got.N)
	}
}

// TestRunnerHandleResumeDelegatesError proves a Runner error is propagated by
// Resume unchanged, with no RunResult: resuming a completed (terminal) run returns
// the Runner's *ResumeTerminalError.
func TestRunnerHandleResumeDelegatesError(t *testing.T) {
	t.Parallel()

	h := flow.NewRunnerHandle(newIncRunner(t)) // completes immediately
	ctx := context.Background()

	done, err := h.Run(ctx, json.RawMessage(`{"n":1}`))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if done.Run.Status != flow.RunCompleted {
		t.Fatalf("Run.Status = %v, want RunCompleted", done.Run.Status)
	}

	res, err := h.Resume(ctx, done.Run.GraphRunID, json.RawMessage(`{}`))
	if res != nil {
		t.Errorf("Resume returned a result for a terminal run: %+v", res)
	}
	var term *flow.ResumeTerminalError
	if !errors.As(err, &term) {
		t.Fatalf("Resume terminal error = %v, want *ResumeTerminalError", err)
	}
}

// TestRunnerHandleCancel proves Cancel delegates to the Runner: after Cancel the
// latest status is RunCancelled, and a second Cancel of the now-terminal run is
// rejected (the Runner's terminal-once contract surfaces through the facade).
func TestRunnerHandleCancel(t *testing.T) {
	t.Parallel()

	h := flow.NewRunnerHandle(newPausingRunner(t))
	ctx := context.Background()

	// A run that pauses leaves a non-terminal RunInterrupted checkpoint to cancel.
	res, err := h.Run(ctx, json.RawMessage(`{"n":0}`))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Run.Status != flow.RunInterrupted {
		t.Fatalf("Run.Status = %v, want RunInterrupted", res.Run.Status)
	}
	id := res.Run.GraphRunID

	if err := h.Cancel(ctx, id, "operator request"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	st, err := h.Status(ctx, id)
	if err != nil {
		t.Fatalf("Status after Cancel: %v", err)
	}
	if st.Status != flow.RunCancelled {
		t.Errorf("Status.Status after Cancel = %v, want RunCancelled", st.Status)
	}

	// Cancelling a terminal run is rejected (delegation surfaces the typed error).
	err = h.Cancel(ctx, id, "again")
	var term *flow.ResumeTerminalError
	if !errors.As(err, &term) {
		t.Fatalf("second Cancel error = %v, want *ResumeTerminalError", err)
	}
}

// TestRunnerHandleCancelUnknown proves Cancel of an unknown run delegates the
// store's *CheckpointNotFoundError unchanged.
func TestRunnerHandleCancelUnknown(t *testing.T) {
	t.Parallel()

	h := flow.NewRunnerHandle(newIncRunner(t))
	id, err := flow.NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID: %v", err)
	}
	err = h.Cancel(context.Background(), id, "x")
	var notFound *flow.CheckpointNotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("Cancel unknown error = %v, want *CheckpointNotFoundError", err)
	}
}
