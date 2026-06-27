package flow_test

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ciram-co/flow/pkg/controlplane"
	"github.com/ciram-co/flow/pkg/flow"
	"github.com/ciram-co/flow/pkg/registry"
)

// This file black-box tests the Serve worker loop (§18.6): consume work for the
// versions the registry serves, resolve each (GraphID,GraphVersion) to its
// RunnerHandle, execute it (OpRun / OpResume honoring the pre-minted GraphRunID,
// §18.3), and Ack at a QUIESCENT result / Nack on a transient failure (§18.5),
// with a duplicate OpRun redelivery absorbed idempotently as an Ack (§18.4). It
// wires a REAL graph→Compile→NewRunnerHandle, a registry.New(), and a
// controlplane.Mem() so the test exercises the production seam end to end, and it
// locks the structural satisfaction registry.Registry → flow.Resolver.

// registry.Registry must structurally satisfy flow.Resolver (Resolve + Keys), so
// the production wiring (flow.Serve(ctx, reg, cp)) type-checks without an adapter.
var _ flow.Resolver = (*registry.Registry)(nil)

// svState is a minimal JSON-serializable graph state for the Serve tests.
type svState struct {
	N int `json:"n"`
}

// svVID mints a deterministic non-zero VertexID from a single byte.
func svVID(b byte) flow.VertexID {
	var id flow.VertexID
	id[0] = b
	return id
}

// svGID mints a deterministic non-zero GraphID from a single byte.
func svGID(b byte) flow.GraphID {
	var id flow.GraphID
	id[0] = b
	return id
}

// newServeIncRunner compiles a one-vertex Runner[svState] that adds one to N, so
// a run from {"n":N} completes RunCompleted with State {"n":N+1}. The store is a
// MemStore so the worker's Run reaches durable history a test can Get/Status.
func newServeIncRunner(t *testing.T) *flow.Runner[svState] {
	t.Helper()
	entry := svVID(1)
	g := flow.NewGraph[svState](svGID(20))
	task := flow.NewFuncTask(func(_ context.Context, in int) (int, error) { return in + 1, nil })
	sel := func(s svState) int { return s.N }
	red := func(s *svState, out int) error { s.N = out; return nil }
	if err := flow.AddVertex(g, entry, task, sel, red); err != nil {
		t.Fatalf("AddVertex: %v", err)
	}
	r, err := g.Compile(entry, entry, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return r
}

// newServeResumableRunner compiles a one-vertex Runner that interrupts (Awaiting)
// on its first execution and, on resume, reads the live payload via
// ResumePayload[json.RawMessage] — the exact type the RunnerHandle.Resume seam
// passes through (§18.1) — decodes {"n":X}, and completes with N=X.
func newServeResumableRunner(t *testing.T) *flow.Runner[svState] {
	t.Helper()
	entry := svVID(1)
	g := flow.NewGraph[svState](svGID(21))
	task := flow.NewFuncTask(func(ctx context.Context, _ int) (int, error) {
		raw, ok := flow.ResumePayload[json.RawMessage](ctx)
		if !ok {
			return 0, flow.Interrupt(ctx, "awaiting payload")
		}
		var p svState
		if err := json.Unmarshal(raw, &p); err != nil {
			return 0, err
		}
		return p.N, nil
	})
	sel := func(s svState) int { return s.N }
	red := func(s *svState, out int) error { s.N = out; return nil }
	if err := flow.AddVertex(g, entry, task, sel, red); err != nil {
		t.Fatalf("AddVertex: %v", err)
	}
	r, err := g.Compile(entry, entry, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return r
}

// keyFor builds the GraphVersionKey routing work to handle h.
func keyFor(h flow.RunnerHandle) flow.GraphVersionKey {
	return flow.GraphVersionKey{GraphID: h.GraphID(), GraphVersion: h.GraphVersion()}
}

// servedRegistry returns a registry with h registered, failing the test on error.
func servedRegistry(t *testing.T, h flow.RunnerHandle) *registry.Registry {
	t.Helper()
	reg := registry.New()
	if err := reg.Add(h); err != nil {
		t.Fatalf("registry.Add: %v", err)
	}
	return reg
}

// startServe launches Serve in a goroutine on a cancelable ctx and returns the
// cancel func plus a done channel closed when Serve returns. It registers a
// cleanup that cancels and waits for Serve to drain, so no test leaks the loop.
func startServe(t *testing.T, reg flow.Resolver, cp flow.ControlPlane) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- flow.Serve(ctx, reg, cp) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("Serve did not return after ctx cancel (goroutine leak)")
		}
	})
	return cancel, done
}

// waitForStatus polls h.Status(id) until it reaches want or the deadline, so the
// async worker's effect is observed without a fixed sleep.
func waitForStatus(t *testing.T, h flow.RunnerHandle, id flow.GraphRunID, want flow.RunStatus) flow.GraphRunState {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		st, err := h.Status(context.Background(), id)
		if err == nil && st.Status == want {
			return st
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %v did not reach %v in time (last err=%v)", id, want, err)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// TestServeRunWorkExecutesAndAcks proves Serve consumes an OpRun Work, resolves it,
// executes it under the PRE-MINTED GraphRunID (§18.3), and the run reaches the
// store as RunCompleted under that id (so a caller could poll GET /runs/{id}).
func TestServeRunWorkExecutesAndAcks(t *testing.T) {
	t.Parallel()

	h := flow.NewRunnerHandle(newServeIncRunner(t))
	reg := servedRegistry(t, h)
	cp := controlplane.Mem()
	t.Cleanup(cp.Close)

	startServe(t, reg, cp)

	id, err := flow.NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID: %v", err)
	}
	w := flow.Work{
		Key:        keyFor(h),
		GraphRunID: id,
		Op:         flow.OpRun,
		Input:      json.RawMessage(`{"n":4}`),
	}
	if err := cp.Submit(context.Background(), w); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	st := waitForStatus(t, h, id, flow.RunCompleted)
	if st.GraphRunID != id {
		t.Errorf("run id = %v, want pre-minted %v", st.GraphRunID, id)
	}
	got, err := h.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var s svState
	if err := json.Unmarshal(got.State, &s); err != nil {
		t.Fatalf("unmarshal State %q: %v", got.State, err)
	}
	if s.N != 5 {
		t.Errorf("final State.N = %d, want 5", s.N)
	}
}

// TestServeResumeWorkExecutes proves Serve processes an OpResume Work: a run is
// first paused at an Awaiting interrupt (run directly to get its id), then an
// OpResume Work for that id with a payload drives it to completion via the worker.
func TestServeResumeWorkExecutes(t *testing.T) {
	t.Parallel()

	h := flow.NewRunnerHandle(newServeResumableRunner(t))
	reg := servedRegistry(t, h)
	cp := controlplane.Mem()
	t.Cleanup(cp.Close)

	startServe(t, reg, cp)

	// Pause a run directly so we have a concrete interrupted id to resume.
	first, err := h.Run(context.Background(), json.RawMessage(`{"n":0}`))
	if err != nil {
		t.Fatalf("seed Run: %v", err)
	}
	if first.Run.Status != flow.RunInterrupted {
		t.Fatalf("seed Run.Status = %v, want RunInterrupted", first.Run.Status)
	}
	id := first.Run.GraphRunID

	w := flow.Work{
		Key:        keyFor(h),
		GraphRunID: id,
		Op:         flow.OpResume,
		Input:      json.RawMessage(`{"n":42}`),
	}
	if err := cp.Submit(context.Background(), w); err != nil {
		t.Fatalf("Submit resume: %v", err)
	}

	waitForStatus(t, h, id, flow.RunCompleted)
	got, err := h.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var s svState
	if err := json.Unmarshal(got.State, &s); err != nil {
		t.Fatalf("unmarshal State %q: %v", got.State, err)
	}
	if s.N != 42 {
		t.Errorf("resumed State.N = %d, want 42", s.N)
	}
}

// TestServeDuplicateOpRunIdempotentAck proves at-least-once safety (§18.4): the
// SAME OpRun Work submitted twice (same pre-minted GraphRunID) runs the graph
// exactly once; the second delivery sees *GraphRunExistsError and is ABSORBED as
// an Ack (not Nack'd into a redelivery loop), so both deliveries clear the plane.
func TestServeDuplicateOpRunIdempotentAck(t *testing.T) {
	t.Parallel()

	// Count task executions to prove the graph body ran exactly once.
	var runs atomic.Int64
	entry := svVID(1)
	g := flow.NewGraph[svState](svGID(22))
	task := flow.NewFuncTask(func(_ context.Context, in int) (int, error) {
		runs.Add(1)
		return in + 1, nil
	})
	sel := func(s svState) int { return s.N }
	red := func(s *svState, out int) error { s.N = out; return nil }
	if err := flow.AddVertex(g, entry, task, sel, red); err != nil {
		t.Fatalf("AddVertex: %v", err)
	}
	r, err := g.Compile(entry, entry, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	h := flow.NewRunnerHandle(r)
	reg := servedRegistry(t, h)
	cp := controlplane.Mem()
	t.Cleanup(cp.Close)

	startServe(t, reg, cp)

	id, err := flow.NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID: %v", err)
	}
	w := flow.Work{
		Key:        keyFor(h),
		GraphRunID: id,
		Op:         flow.OpRun,
		Input:      json.RawMessage(`{"n":0}`),
	}
	// Submit the FIRST and wait for it to complete, so the SECOND is a true
	// duplicate of an already-started run (deterministic *GraphRunExistsError).
	if err := cp.Submit(context.Background(), w); err != nil {
		t.Fatalf("Submit #1: %v", err)
	}
	waitForStatus(t, h, id, flow.RunCompleted)

	if err := cp.Submit(context.Background(), w); err != nil {
		t.Fatalf("Submit #2: %v", err)
	}

	// The duplicate must be absorbed (Ack), not requeued forever. Give it time to
	// be delivered + settle; the run must still have executed exactly once and the
	// state must be unchanged. A Nack loop would keep re-delivering the work.
	time.Sleep(50 * time.Millisecond)
	if got := runs.Load(); got != 1 {
		t.Errorf("task executions = %d, want exactly 1 (duplicate must not re-run)", got)
	}
	got, err := h.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	var s svState
	if err := json.Unmarshal(got.State, &s); err != nil {
		t.Fatalf("unmarshal State %q: %v", got.State, err)
	}
	if s.N != 1 {
		t.Errorf("final State.N = %d, want 1", s.N)
	}
}

// countingControlPlane wraps a flow.ControlPlane and counts how many times each
// GraphRunID is DELIVERED to a consumer, so a test can prove a permanent-error Work
// is not redelivered hundreds of times (the H1 poison-message DoS). It is a thin
// pass-through that observes the Delivery stream; Submit/Consume/Ack/Nack semantics
// are unchanged.
type countingControlPlane struct {
	inner      flow.ControlPlane
	mu         sync.Mutex
	deliveries map[flow.GraphRunID]int
}

func newCountingControlPlane(inner flow.ControlPlane) *countingControlPlane {
	return &countingControlPlane{inner: inner, deliveries: make(map[flow.GraphRunID]int)}
}

func (c *countingControlPlane) Submit(ctx context.Context, w flow.Work) error {
	return c.inner.Submit(ctx, w)
}

func (c *countingControlPlane) Consume(ctx context.Context, serves []flow.GraphVersionKey) (<-chan flow.Delivery, error) {
	in, err := c.inner.Consume(ctx, serves)
	if err != nil {
		return nil, err
	}
	out := make(chan flow.Delivery)
	go func() {
		defer close(out)
		for d := range in {
			c.mu.Lock()
			c.deliveries[d.Work.GraphRunID]++
			c.mu.Unlock()
			select {
			case out <- d:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out, nil
}

func (c *countingControlPlane) count(id flow.GraphRunID) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.deliveries[id]
}

// TestServePoisonMessageBounded is the H1 poison-message regression: an OpResume on
// an ALREADY-COMPLETED run yields a PERMANENT *ResumeTerminalError that can NEVER
// succeed on redelivery. Before the fix settle Nacked it, so the control plane
// redelivered it forever — a 100%-CPU hot loop an external caller could trigger.
// After the fix settle Ack-and-abandons it, so the run is delivered a BOUNDED
// number of times (effectively once) and the plane goes quiet.
func TestServePoisonMessageBounded(t *testing.T) {
	t.Parallel()

	h := flow.NewRunnerHandle(newServeIncRunner(t))
	reg := servedRegistry(t, h)
	inner := controlplane.Mem()
	t.Cleanup(inner.Close)
	cp := newCountingControlPlane(inner)

	startServe(t, reg, cp)

	// Drive a run to completion directly so a later OpResume is terminal.
	done, err := h.Run(context.Background(), json.RawMessage(`{"n":0}`))
	if err != nil {
		t.Fatalf("seed Run: %v", err)
	}
	if done.Run.Status != flow.RunCompleted {
		t.Fatalf("seed Run.Status = %v, want RunCompleted", done.Run.Status)
	}
	id := done.Run.GraphRunID

	// Submit an OpResume on the completed run: a permanent ResumeTerminalError.
	w := flow.Work{
		Key:        keyFor(h),
		GraphRunID: id,
		Op:         flow.OpResume,
		Input:      json.RawMessage(`{"n":1}`),
	}
	if err := cp.Submit(context.Background(), w); err != nil {
		t.Fatalf("Submit poison: %v", err)
	}

	// Give the worker ample time to (mis)behave. A Nack loop would rack up many
	// hundreds of redeliveries in this window; an Ack-and-abandon caps it.
	time.Sleep(200 * time.Millisecond)
	if got := cp.count(id); got > 3 {
		t.Errorf("poison Work delivered %d times, want bounded (<=3) — redelivery spin", got)
	}
}

// TestServeCtxCancelGracefulStop proves cancelling ctx closes the Consume channel,
// Serve returns ctx.Err() (context.Canceled), and no goroutine is left running.
func TestServeCtxCancelGracefulStop(t *testing.T) {
	t.Parallel()

	h := flow.NewRunnerHandle(newServeIncRunner(t))
	reg := servedRegistry(t, h)
	cp := controlplane.Mem()
	t.Cleanup(cp.Close)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- flow.Serve(ctx, reg, cp) }()

	// Let Serve register its consumer, then cancel and require a prompt return.
	time.Sleep(10 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("Serve returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Serve did not return after ctx cancel (goroutine leak)")
	}
}

// TestServeConsumeError proves that when Consume fails (a closed control plane),
// Serve returns the error rather than blocking or panicking.
func TestServeConsumeError(t *testing.T) {
	t.Parallel()

	h := flow.NewRunnerHandle(newServeIncRunner(t))
	reg := servedRegistry(t, h)
	cp := controlplane.Mem()
	cp.Close() // a closed plane rejects Consume with a typed error

	err := flow.Serve(context.Background(), reg, cp)
	if err == nil {
		t.Fatal("Serve returned nil error on a closed control plane, want the Consume error")
	}
}

// TestServeManyRunsConcurrent stresses Serve under -race: many independent runs
// submitted at once must all complete (different runs are independent; the control
// plane single-flights per run, so a goroutine-per-delivery worker is race-clean).
func TestServeManyRunsConcurrent(t *testing.T) {
	t.Parallel()

	h := flow.NewRunnerHandle(newServeIncRunner(t))
	reg := servedRegistry(t, h)
	cp := controlplane.Mem()
	t.Cleanup(cp.Close)

	startServe(t, reg, cp)

	const n = 16
	ids := make([]flow.GraphRunID, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		id, err := flow.NewGraphRunID()
		if err != nil {
			t.Fatalf("NewGraphRunID: %v", err)
		}
		ids[i] = id
		wg.Add(1)
		go func(id flow.GraphRunID) {
			defer wg.Done()
			w := flow.Work{
				Key:        keyFor(h),
				GraphRunID: id,
				Op:         flow.OpRun,
				Input:      json.RawMessage(`{"n":1}`),
			}
			if err := cp.Submit(context.Background(), w); err != nil {
				t.Errorf("Submit: %v", err)
			}
		}(id)
	}
	wg.Wait()

	for _, id := range ids {
		st := waitForStatus(t, h, id, flow.RunCompleted)
		if st.GraphRunID != id {
			t.Errorf("run id = %v, want %v", st.GraphRunID, id)
		}
	}
}
