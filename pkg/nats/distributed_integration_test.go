//go:build integration

package nats

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ciram-co/flow/pkg/flow"
	"github.com/ciram-co/flow/pkg/registry"
	"github.com/nats-io/nats.go"
)

// This file is the Tier-C END-TO-END DURABILITY PROOF (design §18.4/§18.6): it
// proves a human-in-the-loop run survives a WORKER RESTART through the FULL Tier-C
// path — control plane (nats.ControlPlane) → flow.Serve worker → durable store
// (nats.Store) — by checkpointing an interrupt durably, killing the worker that
// produced it, starting a FRESH worker on the SAME store + control plane, and
// resuming to completion. If the checkpoint did not survive in nats.Store (and the
// resume work did not flow through nats.ControlPlane), the new worker could not
// complete the run.
//
// The graph is minimal: greet → awaitApproval (INTERRUPT, human-in-the-loop) →
// deliver. The interrupt vertex pauses on the FIRST pass (no resume payload) and
// completes on resume (payload present), exactly the §16 interrupt/resume idiom but
// driven over the distributed seams instead of in-process.

const (
	// durRecvTimeout bounds how long a status poll waits for a run to reach a target
	// status before failing — generous because the path crosses the control plane,
	// a worker goroutine, and the durable store.
	durRecvTimeout = 20 * time.Second

	// durPollInterval is the gap between durable-status polls.
	durPollInterval = 10 * time.Millisecond
)

// durState is the demo graph's JSON-serializable blackboard for the durability run.
type durState struct {
	Name     string // who to greet (initial input)
	Message  string // greet's output
	Approved bool   // awaitApproval's output, set from the resume payload
	Sent     bool   // deliver's output: the (approved) greeting was sent
}

// Pinned definition ids for the durability graph (stable across the simulated
// restart so the resumed checkpoint validates against the same compiled graph).
var (
	durGraphID    = flow.GraphID(mustID(0xD1))
	durGreetID    = flow.VertexID(mustID(0xA1))
	durApproveID  = flow.VertexID(mustID(0xB2))
	durDeliverID  = flow.VertexID(mustID(0xC3))
	durResumeNote = `"approved-by-human"` // the resume payload (raw JSON string)
)

// mustID builds a non-zero 16-byte id whose first byte is b (a deterministic,
// distinct definition id without minting a real UUID).
func mustID(b byte) [16]byte {
	var id [16]byte
	id[0] = b
	id[15] = 1 // keep it unambiguously non-zero even if b == 0
	return id
}

// buildDurGraph wires greet → awaitApproval (interrupt) → deliver over durState.
// awaitApproval pauses on the first pass (no resume payload) and, on resume, reads
// the human approval from the resume payload (recovered as raw JSON through the
// RunnerHandle seam) and completes — the human-in-the-loop INTERRUPT the durability
// proof hinges on.
func buildDurGraph() (*flow.Graph[durState], error) {
	g := flow.NewGraph[durState](durGraphID)

	if err := flow.AddVertex(g, durGreetID,
		flow.NewFuncTask(func(_ context.Context, name string) (string, error) {
			return "Hello, " + name + "!", nil
		}),
		func(s durState) string { return s.Name },
		func(s *durState, msg string) error { s.Message = msg; return nil },
	); err != nil {
		return nil, err
	}

	// awaitApproval: the human-in-the-loop gate. First pass (no resume payload) →
	// Interrupt (durable pause). Resume pass (payload present) → return true.
	if err := flow.AddVertex(g, durApproveID,
		flow.NewFuncTask(func(ctx context.Context, _ string) (bool, error) {
			if _, ok := flow.ResumePayload[json.RawMessage](ctx); ok {
				return true, nil // resumed with the human's approval
			}
			return false, flow.Interrupt(ctx, "awaiting human approval")
		}),
		func(s durState) string { return s.Message },
		func(s *durState, approved bool) error { s.Approved = approved; return nil },
	); err != nil {
		return nil, err
	}

	// deliver: the finish vertex — "send" only an approved greeting.
	if err := flow.AddVertex(g, durDeliverID,
		flow.NewFuncTask(func(_ context.Context, approved bool) (bool, error) {
			return approved, nil
		}),
		func(s durState) bool { return s.Approved },
		func(s *durState, sent bool) error { s.Sent = sent; return nil },
	); err != nil {
		return nil, err
	}

	if err := g.AddEdge(durGreetID, durApproveID); err != nil {
		return nil, err
	}
	if err := g.AddEdge(durApproveID, durDeliverID); err != nil {
		return nil, err
	}
	return g, nil
}

// durStack bundles the durable Tier-C seams a worker binds to: the registry (the
// compiled graph), the durable store (to read run status), and the control plane
// (to submit work). store and cp are SHARED across the simulated worker restart;
// each worker gets its OWN reg (a fresh process would rebuild it identically).
type durStack struct {
	store *Store
	cp    *ControlPlane
	key   flow.GraphVersionKey
}

// newDurStack boots the durable backend (embedded JetStream, KEEPING the store dir
// so durable state persists across the simulated restart) and the shared store +
// control plane, registering cleanups. It returns the connection (kept alive for
// the whole test) and the durStack.
func newDurStack(t *testing.T, ctx context.Context) (*nats.Conn, durStack) {
	t.Helper()
	srv, err := Embedded(WithStoreDir(t.TempDir()), WithReadyTimeout(20*time.Second))
	if err != nil {
		t.Fatalf("Embedded: %v", err)
	}
	t.Cleanup(srv.Close)
	nc, err := srv.InProcessConn()
	if err != nil {
		t.Fatalf("InProcessConn: %v", err)
	}
	t.Cleanup(nc.Close)

	store, err := NewStore(ctx, nc)
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	cp, err := NewControlPlane(ctx, nc)
	if err != nil {
		t.Fatalf("NewControlPlane: %v", err)
	}

	// Compile once to learn the GraphVersion (the routing key); each worker rebuilds
	// its own registry from the SAME graph, so the version (and thus the key) match.
	g, err := buildDurGraph()
	if err != nil {
		t.Fatalf("buildDurGraph: %v", err)
	}
	runner, err := g.Compile(durGreetID, durDeliverID, flow.WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	key := flow.GraphVersionKey{GraphID: durGraphID, GraphVersion: runner.GraphVersion()}
	return nc, durStack{store: store, cp: cp, key: key}
}

// startWorker builds a fresh registry over the shared store and starts a flow.Serve
// worker loop bound to its OWN ctx, returning a stop func that cancels the worker
// and waits for Serve to return (a clean, leak-free worker teardown — the simulated
// process exit). Each call models a SEPARATE worker process: it rebuilds the graph
// and registry locally but shares the durable store + control plane.
func startWorker(t *testing.T, st durStack) (stop func()) {
	t.Helper()
	g, err := buildDurGraph()
	if err != nil {
		t.Fatalf("buildDurGraph: %v", err)
	}
	runner, err := g.Compile(durGreetID, durDeliverID, flow.WithStore(st.store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	reg := registry.New()
	if err := reg.Add(flow.NewRunnerHandle(runner)); err != nil {
		t.Fatalf("registry.Add: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = flow.Serve(ctx, reg, st.cp)
	}()
	return func() {
		cancel()
		<-done
	}
}

// waitStatus polls the DURABLE store until run id reaches want (or the deadline),
// returning the latest checkpoint's run state. The status is read from nats.Store —
// proving the state is durable, not in-process — so it is the authoritative signal.
func waitStatus(t *testing.T, st durStack, id flow.GraphRunID, want flow.RunStatus) flow.GraphRunState {
	t.Helper()
	deadline := time.Now().Add(durRecvTimeout)
	var last flow.RunStatus = -1
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		cp, err := st.store.Latest(ctx, id)
		cancel()
		if err == nil {
			last = cp.Run.Status
			if cp.Run.Status == want {
				return cp.Run
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s did not reach %v in time (last durable status=%v, lookup err=%v)", id, want, last, err)
		}
		time.Sleep(durPollInterval)
	}
}

// TestDistributedDurabilityAcrossWorkerRestart is the headline Tier-C durability
// proof. It submits a human-in-the-loop run, waits until it INTERRUPTS (checkpoint
// durable in nats.Store), KILLS the worker that produced the pause, starts a FRESH
// worker on the SAME store + control plane, submits the resume, and asserts the new
// worker resumes from the durable checkpoint and completes the run — proving the
// checkpoint survived the worker restart in nats.Store and the work flowed through
// nats.ControlPlane.
func TestDistributedDurabilityAcrossWorkerRestart(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, st := newDurStack(t, ctx)

	// --- Worker #1: run until the human-in-the-loop interrupt is durable. ---
	stop1 := startWorker(t, st)

	runID := durRunID(0x42)
	input, err := json.Marshal(durState{Name: "Ada"})
	if err != nil {
		t.Fatalf("marshal input: %v", err)
	}
	if err := st.cp.Submit(ctx, flow.Work{
		Key:        st.key,
		GraphRunID: runID,
		Op:         flow.OpRun,
		Input:      input,
	}); err != nil {
		t.Fatalf("Submit OpRun: %v", err)
	}

	// The run must reach RunInterrupted, and that pause must be DURABLE in nats.Store.
	st1 := waitStatus(t, st, runID, flow.RunInterrupted)
	if st1.GraphRunID != runID {
		t.Fatalf("interrupted run id = %s, want %s", st1.GraphRunID, runID)
	}

	// --- Simulate a worker RESTART: kill worker #1, start a fresh worker #2. ---
	stop1() // worker #1 gone (Serve returned, no goroutine left)

	stop2 := startWorker(t, st) // fresh worker on the SAME durable store + control plane
	defer stop2()

	// --- Resume through the control plane; worker #2 must pick it up. ---
	if err := st.cp.Submit(ctx, flow.Work{
		Key:        st.key,
		GraphRunID: runID,
		Op:         flow.OpResume,
		Input:      json.RawMessage(durResumeNote),
	}); err != nil {
		t.Fatalf("Submit OpResume: %v", err)
	}

	// The FRESH worker must resume from the durable checkpoint and COMPLETE the run.
	final := waitStatus(t, st, runID, flow.RunCompleted)
	if final.GraphRunID != runID {
		t.Fatalf("completed run id = %s, want %s", final.GraphRunID, runID)
	}

	// Assert the durable final STATE reflects the full path: greeted, approved, sent.
	cp, err := st.store.Latest(ctx, runID)
	if err != nil {
		t.Fatalf("Latest after completion: %v", err)
	}
	var got durState
	if err := json.Unmarshal(cp.State, &got); err != nil {
		t.Fatalf("decode final state %s: %v", cp.State, err)
	}
	if got.Message == "" || !got.Approved || !got.Sent {
		t.Errorf("final state = %+v, want greeted+approved+sent", got)
	}
}

// durRunID builds a deterministic non-zero GraphRunID from a single byte.
func durRunID(b byte) flow.GraphRunID {
	var id flow.GraphRunID
	id[0] = b
	id[15] = 1
	return id
}
