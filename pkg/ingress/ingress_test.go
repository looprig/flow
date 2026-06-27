package ingress_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ciram-co/flow/pkg/controlplane"
	"github.com/ciram-co/flow/pkg/flow"
	"github.com/ciram-co/flow/pkg/ingress"
	"github.com/ciram-co/flow/pkg/registry"
)

// This file black-box tests the async-first REST ingress handler (design §18.3):
// POSTs pre-mint the GraphRunID, Submit Work to the control plane, and return
// 202 + the GraphRunID immediately; results are read via GET /v1/runs/{id}. The
// tests wire a REAL graph -> Compile -> NewRunnerHandle, a registry.New(), a
// controlplane.Mem(), a flow.NewMemStore(), and a running flow.Serve worker so
// submitted work actually executes end to end (poll-to-completion). They cover
// every route, the typed-error -> HTTP status map (409/400/404), the
// Idempotency-Key dedupe, the body-size limit, the auth seam, and the secure
// server helper's timeouts + TLS minimum version.

// ixState is a minimal JSON-serializable graph state for the ingress tests.
type ixState struct {
	N int `json:"n"`
}

// startResp mirrors the 202 start/resume response body.
type startResp struct {
	GraphRunID   string `json:"graphRunID"`
	GraphVersion string `json:"graphVersion"`
}

// errResp mirrors the sanitized JSON error body.
type errResp struct {
	Error string `json:"error"`
}

// ixVID mints a deterministic non-zero VertexID from a single byte.
func ixVID(b byte) flow.VertexID {
	var id flow.VertexID
	id[0] = b
	return id
}

// ixGID mints a deterministic non-zero GraphID from a single byte.
func ixGID(b byte) flow.GraphID {
	var id flow.GraphID
	id[0] = b
	return id
}

// newIncRunner compiles a one-vertex Runner[ixState] over store that adds one to
// N, so a run from {"n":N} completes RunCompleted with State {"n":N+1}. graphOpts
// (e.g. flow.WithVersion(n)) distinguish two versions of the same GraphID.
func newIncRunner(t *testing.T, gid flow.GraphID, store flow.CheckpointStore, graphOpts ...flow.GraphOption) *flow.Runner[ixState] {
	t.Helper()
	entry := ixVID(1)
	g := flow.NewGraph[ixState](gid, graphOpts...)
	task := flow.NewFuncTask(func(_ context.Context, in int) (int, error) { return in + 1, nil })
	sel := func(s ixState) int { return s.N }
	red := func(s *ixState, out int) error { s.N = out; return nil }
	if err := flow.AddVertex(g, entry, task, sel, red); err != nil {
		t.Fatalf("AddVertex: %v", err)
	}
	r, err := g.Compile(entry, entry, flow.WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return r
}

// newResumableRunner compiles a one-vertex Runner that interrupts (Awaiting) on
// its first execution and, on resume, reads the live payload via
// ResumePayload[json.RawMessage] (the type the RunnerHandle.Resume seam passes
// through), decodes {"n":X}, and completes with N=X.
func newResumableRunner(t *testing.T, gid flow.GraphID, store flow.CheckpointStore) *flow.Runner[ixState] {
	t.Helper()
	entry := ixVID(1)
	g := flow.NewGraph[ixState](gid)
	task := flow.NewFuncTask(func(ctx context.Context, _ int) (int, error) {
		raw, ok := flow.ResumePayload[json.RawMessage](ctx)
		if !ok {
			return 0, flow.Interrupt(ctx, "awaiting payload")
		}
		var p ixState
		if err := json.Unmarshal(raw, &p); err != nil {
			return 0, err
		}
		return p.N, nil
	})
	sel := func(s ixState) int { return s.N }
	red := func(s *ixState, out int) error { s.N = out; return nil }
	if err := flow.AddVertex(g, entry, task, sel, red); err != nil {
		t.Fatalf("AddVertex: %v", err)
	}
	r, err := g.Compile(entry, entry, flow.WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return r
}

// servedRegistry returns a registry with every handle registered.
func servedRegistry(t *testing.T, handles ...flow.RunnerHandle) *registry.Registry {
	t.Helper()
	reg := registry.New()
	for _, h := range handles {
		if err := reg.Add(h); err != nil {
			t.Fatalf("registry.Add: %v", err)
		}
	}
	return reg
}

// startServe launches flow.Serve in a goroutine and cleans it up after the test.
func startServe(t *testing.T, reg flow.Resolver, cp flow.ControlPlane) {
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
}

// pollRun polls GET /v1/runs/{id} until the run reaches want status or the
// deadline, returning the decoded run-state map. It fails the test on timeout.
func pollRun(t *testing.T, srv *httptest.Server, id, want string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		resp, err := http.Get(srv.URL + "/v1/runs/" + id)
		if err != nil {
			t.Fatalf("GET run: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			var got map[string]any
			if err := json.Unmarshal(body, &got); err != nil {
				t.Fatalf("decode run %q: %v", body, err)
			}
			run, _ := got["run"].(map[string]any)
			if run != nil && run["Status"] != nil {
				if statusString(run["Status"]) == want {
					return got
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s did not reach %s in time (last status=%d body=%s)", id, want, resp.StatusCode, body)
		}
		time.Sleep(3 * time.Millisecond)
	}
}

// statusString renders a JSON-decoded Status value (a number) as the RunStatus
// token, matching encoding/json's default numeric encoding of the int enum.
func statusString(v any) string {
	f, ok := v.(float64)
	if !ok {
		return ""
	}
	return flow.RunStatus(int(f)).String()
}

// newTestServer wires a registry+control plane+store+Serve and an ingress
// handler behind an httptest.Server, returning the server and the control plane.
func newTestServer(t *testing.T, reg *registry.Registry, store flow.CheckpointStore, opts ...ingress.Option) (*httptest.Server, *controlplane.MemControlPlane) {
	t.Helper()
	cp := controlplane.Mem()
	t.Cleanup(cp.Close)
	startServe(t, reg, cp)
	h := ingress.New(reg, cp, store, opts...)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return srv, cp
}

// postJSON POSTs body to path with optional headers and returns status + body.
func postJSON(t *testing.T, url string, body string, headers map[string]string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// TestListGraphs proves GET /v1/graphs returns the registered graphID + versions.
func TestListGraphs(t *testing.T) {
	t.Parallel()

	gid := ixGID(30)
	h := flow.NewRunnerHandle(newIncRunner(t, gid, flow.NewMemStore()))
	reg := servedRegistry(t, h)
	srv, _ := newTestServer(t, reg, flow.NewMemStore())

	resp, err := http.Get(srv.URL + "/v1/graphs")
	if err != nil {
		t.Fatalf("GET graphs: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var manifest []struct {
		GraphID  string   `json:"graphID"`
		Versions []string `json:"versions"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&manifest); err != nil {
		t.Fatalf("decode manifest: %v", err)
	}
	if len(manifest) != 1 {
		t.Fatalf("manifest entries = %d, want 1", len(manifest))
	}
	if manifest[0].GraphID != gid.String() {
		t.Errorf("graphID = %q, want %q", manifest[0].GraphID, gid.String())
	}
	if len(manifest[0].Versions) != 1 || manifest[0].Versions[0] != h.GraphVersion() {
		t.Errorf("versions = %v, want [%q]", manifest[0].Versions, h.GraphVersion())
	}
}

// TestStartRunCompletes proves POST /v1/graphs/{id}/runs returns 202 + a
// non-empty graphRunID + graphVersion and, with Serve running, the run completes
// and GET /v1/runs/{id} shows RunCompleted with the final state.
func TestStartRunCompletes(t *testing.T) {
	t.Parallel()

	gid := ixGID(31)
	store := flow.NewMemStore()
	h := flow.NewRunnerHandle(newIncRunner(t, gid, store))
	reg := servedRegistry(t, h)
	srv, _ := newTestServer(t, reg, store)

	status, body := postJSON(t, srv.URL+"/v1/graphs/"+gid.String()+"/runs", `{"n":4}`, nil)
	if status != http.StatusAccepted {
		t.Fatalf("status = %d, want 202; body=%s", status, body)
	}
	var sr startResp
	if err := json.Unmarshal(body, &sr); err != nil {
		t.Fatalf("decode start resp %q: %v", body, err)
	}
	if sr.GraphRunID == "" {
		t.Fatal("graphRunID is empty")
	}
	if sr.GraphVersion != h.GraphVersion() {
		t.Errorf("graphVersion = %q, want %q", sr.GraphVersion, h.GraphVersion())
	}

	got := pollRun(t, srv, sr.GraphRunID, "Completed")
	if gv, _ := got["graphVersion"].(string); gv != h.GraphVersion() {
		t.Errorf("run graphVersion = %q, want %q", gv, h.GraphVersion())
	}
	state, _ := got["state"].(map[string]any)
	if state == nil {
		t.Fatalf("no state in run response: %v", got)
	}
	if n, _ := state["n"].(float64); int(n) != 5 {
		t.Errorf("final state.n = %v, want 5", state["n"])
	}
}

// TestStartRunVersionPinning proves ?version= pins the version, an unknown graph
// is 404, and a graph with multiple versions without ?version= is 400.
func TestStartRunVersionPinning(t *testing.T) {
	t.Parallel()

	gid := ixGID(32)
	// Two versions of the same GraphID: WithVersion bumps the fingerprint.
	hA := flow.NewRunnerHandle(newIncRunner(t, gid, flow.NewMemStore(), flow.WithVersion(1)))
	hB := flow.NewRunnerHandle(newIncRunner(t, gid, flow.NewMemStore(), flow.WithVersion(2)))
	reg := servedRegistry(t, hA, hB)
	srv, _ := newTestServer(t, reg, flow.NewMemStore())

	// Multiple versions, no ?version= -> 400.
	status, body := postJSON(t, srv.URL+"/v1/graphs/"+gid.String()+"/runs", `{"n":1}`, nil)
	if status != http.StatusBadRequest {
		t.Fatalf("multi-version no pin: status = %d, want 400; body=%s", status, body)
	}
	assertErrorBody(t, body)

	// Pin version a -> 202 with that version.
	status, body = postJSON(t, srv.URL+"/v1/graphs/"+gid.String()+"/runs?version="+hA.GraphVersion(), `{"n":1}`, nil)
	if status != http.StatusAccepted {
		t.Fatalf("pinned: status = %d, want 202; body=%s", status, body)
	}
	var sr startResp
	if err := json.Unmarshal(body, &sr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if sr.GraphVersion != hA.GraphVersion() {
		t.Errorf("graphVersion = %q, want pinned %q", sr.GraphVersion, hA.GraphVersion())
	}

	// Unknown graph -> 404.
	other := ixGID(99)
	status, body = postJSON(t, srv.URL+"/v1/graphs/"+other.String()+"/runs", `{"n":1}`, nil)
	if status != http.StatusNotFound {
		t.Fatalf("unknown graph: status = %d, want 404; body=%s", status, body)
	}
	assertErrorBody(t, body)
}

// TestResumeCompletes proves POST /v1/runs/{id}/resume returns 202 and a paused
// run resumes to completion (polled).
func TestResumeCompletes(t *testing.T) {
	t.Parallel()

	gid := ixGID(33)
	store := flow.NewMemStore()
	h := flow.NewRunnerHandle(newResumableRunner(t, gid, store))
	reg := servedRegistry(t, h)
	srv, _ := newTestServer(t, reg, store)

	// Start; the run pauses at the Awaiting interrupt.
	status, body := postJSON(t, srv.URL+"/v1/graphs/"+gid.String()+"/runs", `{"n":0}`, nil)
	if status != http.StatusAccepted {
		t.Fatalf("start: status = %d, want 202; body=%s", status, body)
	}
	var sr startResp
	if err := json.Unmarshal(body, &sr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	pollRun(t, srv, sr.GraphRunID, "Interrupted")

	// Resume with a payload; the run completes with N=42.
	status, body = postJSON(t, srv.URL+"/v1/runs/"+sr.GraphRunID+"/resume", `{"n":42}`, nil)
	if status != http.StatusAccepted {
		t.Fatalf("resume: status = %d, want 202; body=%s", status, body)
	}
	got := pollRun(t, srv, sr.GraphRunID, "Completed")
	state, _ := got["state"].(map[string]any)
	if n, _ := state["n"].(float64); int(n) != 42 {
		t.Errorf("resumed state.n = %v, want 42", state["n"])
	}
}

// TestResumeVersionMismatch proves a resume for a run whose GraphVersion the
// registry no longer serves returns 409 + X-Graph-Version.
func TestResumeVersionMismatch(t *testing.T) {
	t.Parallel()

	gid := ixGID(34)
	store := flow.NewMemStore()
	// Seed a paused run under version "old" using a runner that is NOT registered.
	oldRunner := newResumableRunner(t, gid, store)
	oldHandle := flow.NewRunnerHandle(oldRunner)
	seed, err := oldHandle.Run(context.Background(), json.RawMessage(`{"n":0}`))
	if err != nil {
		t.Fatalf("seed Run: %v", err)
	}
	if seed.Run.Status != flow.RunInterrupted {
		t.Fatalf("seed status = %v, want Interrupted", seed.Run.Status)
	}
	runVersion := seed.Run.GraphVersion

	// Register a DIFFERENT version of the same graph, so the run's version is
	// unresolvable.
	served := flow.NewRunnerHandle(newIncRunner(t, gid, flow.NewMemStore(), flow.WithVersion(7)))
	if served.GraphVersion() == runVersion {
		t.Fatal("served version unexpectedly equals the run version")
	}
	reg := servedRegistry(t, served)
	srv, _ := newTestServer(t, reg, store)

	req, err := http.NewRequest(http.MethodPost, srv.URL+"/v1/runs/"+seed.Run.GraphRunID.String()+"/resume", strings.NewReader(`{"n":1}`))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("resume: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", resp.StatusCode, b)
	}
	if got := resp.Header.Get("X-Graph-Version"); got != runVersion {
		t.Errorf("X-Graph-Version = %q, want %q", got, runVersion)
	}
	assertErrorBody(t, b)
}

// TestGetUnknownRun proves GET /v1/runs/{id} for an unknown id returns 404.
func TestGetUnknownRun(t *testing.T) {
	t.Parallel()

	gid := ixGID(35)
	h := flow.NewRunnerHandle(newIncRunner(t, gid, flow.NewMemStore()))
	reg := servedRegistry(t, h)
	srv, _ := newTestServer(t, reg, flow.NewMemStore())

	unknown, err := flow.NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID: %v", err)
	}
	resp, err := http.Get(srv.URL + "/v1/runs/" + unknown.String())
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d, want 404; body=%s", resp.StatusCode, b)
	}
	assertErrorBody(t, b)
}

// TestCancelRun proves POST /v1/runs/{id}/cancel succeeds (200/204), a later GET
// shows RunCancelled, and cancelling an already-terminal run maps to 409.
func TestCancelRun(t *testing.T) {
	t.Parallel()

	gid := ixGID(36)
	store := flow.NewMemStore()
	h := flow.NewRunnerHandle(newResumableRunner(t, gid, store))
	reg := servedRegistry(t, h)
	srv, _ := newTestServer(t, reg, store)

	// Start; the run pauses (so it is non-terminal and cancellable).
	status, body := postJSON(t, srv.URL+"/v1/graphs/"+gid.String()+"/runs", `{"n":0}`, nil)
	if status != http.StatusAccepted {
		t.Fatalf("start: status = %d, want 202; body=%s", status, body)
	}
	var sr startResp
	if err := json.Unmarshal(body, &sr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	pollRun(t, srv, sr.GraphRunID, "Interrupted")

	// Cancel -> 200/204.
	status, body = postJSON(t, srv.URL+"/v1/runs/"+sr.GraphRunID+"/cancel", `{"reason":"operator stop"}`, nil)
	if status != http.StatusOK && status != http.StatusNoContent {
		t.Fatalf("cancel: status = %d, want 200/204; body=%s", status, body)
	}
	got := pollRun(t, srv, sr.GraphRunID, "Cancelled")
	run, _ := got["run"].(map[string]any)
	if reason, _ := run["CancelReason"].(string); reason != "operator stop" {
		t.Errorf("cancel reason = %q, want %q", reason, "operator stop")
	}

	// Cancelling an already-terminal run -> 409 (ResumeTerminalError mapping).
	status, body = postJSON(t, srv.URL+"/v1/runs/"+sr.GraphRunID+"/cancel", `{"reason":"again"}`, nil)
	if status != http.StatusConflict {
		t.Fatalf("re-cancel: status = %d, want 409; body=%s", status, body)
	}
	assertErrorBody(t, body)
}

// TestMalformedBodyBadRequest proves a malformed JSON body on a start POST maps
// to 400 with a sanitized error body.
func TestMalformedBodyBadRequest(t *testing.T) {
	t.Parallel()

	gid := ixGID(37)
	h := flow.NewRunnerHandle(newIncRunner(t, gid, flow.NewMemStore()))
	reg := servedRegistry(t, h)
	srv, _ := newTestServer(t, reg, flow.NewMemStore())

	status, body := postJSON(t, srv.URL+"/v1/graphs/"+gid.String()+"/runs", `{"n": `, nil)
	if status != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", status, body)
	}
	assertErrorBody(t, body)
}

// TestBadRunIDBadRequest proves a malformed run id on GET maps to 400.
func TestBadRunIDBadRequest(t *testing.T) {
	t.Parallel()

	gid := ixGID(38)
	h := flow.NewRunnerHandle(newIncRunner(t, gid, flow.NewMemStore()))
	reg := servedRegistry(t, h)
	srv, _ := newTestServer(t, reg, flow.NewMemStore())

	resp, err := http.Get(srv.URL + "/v1/runs/not-a-uuid")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", resp.StatusCode, b)
	}
	assertErrorBody(t, b)
}

// TestIdempotencyKey proves two POSTs with the SAME Idempotency-Key return the
// SAME graphRunID and submit Work only ONCE (asserted via a run-count atomic).
func TestIdempotencyKey(t *testing.T) {
	t.Parallel()

	gid := ixGID(39)
	var runs atomic.Int64
	entry := ixVID(1)
	g := flow.NewGraph[ixState](gid)
	task := flow.NewFuncTask(func(_ context.Context, in int) (int, error) {
		runs.Add(1)
		return in + 1, nil
	})
	sel := func(s ixState) int { return s.N }
	red := func(s *ixState, out int) error { s.N = out; return nil }
	if err := flow.AddVertex(g, entry, task, sel, red); err != nil {
		t.Fatalf("AddVertex: %v", err)
	}
	store := flow.NewMemStore()
	r, err := g.Compile(entry, entry, flow.WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	h := flow.NewRunnerHandle(r)
	reg := servedRegistry(t, h)
	srv, _ := newTestServer(t, reg, store)

	headers := map[string]string{"Idempotency-Key": "key-123"}
	status1, body1 := postJSON(t, srv.URL+"/v1/graphs/"+gid.String()+"/runs", `{"n":0}`, headers)
	if status1 != http.StatusAccepted {
		t.Fatalf("#1 status = %d, want 202; body=%s", status1, body1)
	}
	var sr1 startResp
	if err := json.Unmarshal(body1, &sr1); err != nil {
		t.Fatalf("decode #1: %v", err)
	}
	pollRun(t, srv, sr1.GraphRunID, "Completed")

	status2, body2 := postJSON(t, srv.URL+"/v1/graphs/"+gid.String()+"/runs", `{"n":0}`, headers)
	if status2 != http.StatusAccepted {
		t.Fatalf("#2 status = %d, want 202; body=%s", status2, body2)
	}
	var sr2 startResp
	if err := json.Unmarshal(body2, &sr2); err != nil {
		t.Fatalf("decode #2: %v", err)
	}
	if sr1.GraphRunID != sr2.GraphRunID {
		t.Errorf("idempotent ids differ: %q vs %q", sr1.GraphRunID, sr2.GraphRunID)
	}

	// The second POST must NOT have submitted Work: the graph ran exactly once.
	time.Sleep(50 * time.Millisecond)
	if got := runs.Load(); got != 1 {
		t.Errorf("task executions = %d, want exactly 1 (idempotent POST must not re-submit)", got)
	}
}

// TestBodySizeLimit proves an over-limit body is rejected (400/413), not OOM.
func TestBodySizeLimit(t *testing.T) {
	t.Parallel()

	gid := ixGID(40)
	h := flow.NewRunnerHandle(newIncRunner(t, gid, flow.NewMemStore()))
	reg := servedRegistry(t, h)
	srv, _ := newTestServer(t, reg, flow.NewMemStore(), ingress.WithMaxBodyBytes(64))

	big := `{"n":` + strings.Repeat("9", 1024) + `}`
	status, body := postJSON(t, srv.URL+"/v1/graphs/"+gid.String()+"/runs", big, nil)
	if status != http.StatusBadRequest && status != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 400 or 413; body=%s", status, body)
	}
}

// TestAuthSeam proves WithAuth gates ALL routes: a rejecting authenticator -> 401,
// an allowing one -> normal behavior.
func TestAuthSeam(t *testing.T) {
	t.Parallel()

	gid := ixGID(41)
	h := flow.NewRunnerHandle(newIncRunner(t, gid, flow.NewMemStore()))
	reg := servedRegistry(t, h)

	// Reject all.
	rejectAuth := func(*http.Request) error { return errTestReject }
	srv, _ := newTestServer(t, reg, flow.NewMemStore(), ingress.WithAuth(rejectAuth))

	resp, err := http.Get(srv.URL + "/v1/graphs")
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("rejected status = %d, want 401; body=%s", resp.StatusCode, b)
	}

	// Allow all.
	allowAuth := func(*http.Request) error { return nil }
	srv2, _ := newTestServer(t, reg, flow.NewMemStore(), ingress.WithAuth(allowAuth))
	resp2, err := http.Get(srv2.URL + "/v1/graphs")
	if err != nil {
		t.Fatalf("GET2: %v", err)
	}
	resp2.Body.Close()
	if resp2.StatusCode != http.StatusOK {
		t.Errorf("allowed status = %d, want 200", resp2.StatusCode)
	}
}

// errTestReject is a fixed authenticator rejection used by the auth-seam test.
var errTestReject = newTestError("rejected")

// testError is a minimal error type so the auth seam returns a non-nil error.
type testError struct{ msg string }

func newTestError(msg string) error { return &testError{msg: msg} }
func (e *testError) Error() string  { return e.msg }

// TestSecureServer proves the Server helper sets non-zero timeouts and a TLS
// minimum version of TLS 1.2 on the returned *http.Server.
func TestSecureServer(t *testing.T) {
	t.Parallel()

	h := http.NewServeMux()
	srv := ingress.Server(":0", h)
	if srv == nil {
		t.Fatal("Server returned nil")
	}
	if srv.ReadHeaderTimeout <= 0 {
		t.Errorf("ReadHeaderTimeout = %v, want > 0", srv.ReadHeaderTimeout)
	}
	if srv.ReadTimeout <= 0 {
		t.Errorf("ReadTimeout = %v, want > 0", srv.ReadTimeout)
	}
	if srv.WriteTimeout <= 0 {
		t.Errorf("WriteTimeout = %v, want > 0", srv.WriteTimeout)
	}
	if srv.IdleTimeout <= 0 {
		t.Errorf("IdleTimeout = %v, want > 0", srv.IdleTimeout)
	}
	if srv.TLSConfig == nil {
		t.Fatal("TLSConfig is nil")
	}
	if srv.TLSConfig.MinVersion != tls.VersionTLS12 {
		t.Errorf("TLSConfig.MinVersion = %x, want TLS 1.2 (%x)", srv.TLSConfig.MinVersion, tls.VersionTLS12)
	}
	if srv.Handler == nil {
		t.Error("Handler is nil")
	}
}

// TestSubmitFailureServiceUnavailable proves a control-plane failure on Submit
// (a closed control plane -> *controlplane.ClosedError) maps to 503 with a
// sanitized body, exercising the infra-error branch of the status map.
func TestSubmitFailureServiceUnavailable(t *testing.T) {
	t.Parallel()

	gid := ixGID(42)
	h := flow.NewRunnerHandle(newIncRunner(t, gid, flow.NewMemStore()))
	reg := servedRegistry(t, h)

	// Build a handler whose control plane is closed (no Serve), so Submit fails.
	cp := controlplane.Mem()
	cp.Close()
	srv := httptest.NewServer(ingress.New(reg, cp, flow.NewMemStore()))
	t.Cleanup(srv.Close)

	status, body := postJSON(t, srv.URL+"/v1/graphs/"+gid.String()+"/runs", `{"n":1}`, nil)
	if status != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503; body=%s", status, body)
	}
	assertErrorBody(t, body)
}

// TestGetInterruptedRunRendersInterrupts proves GET on a paused run renders the
// per-vertex interrupts in the response (the awaiting reason as info), exercising
// the interrupt-DTO rendering path.
func TestGetInterruptedRunRendersInterrupts(t *testing.T) {
	t.Parallel()

	gid := ixGID(43)
	store := flow.NewMemStore()
	h := flow.NewRunnerHandle(newResumableRunner(t, gid, store))
	reg := servedRegistry(t, h)
	srv, _ := newTestServer(t, reg, store)

	status, body := postJSON(t, srv.URL+"/v1/graphs/"+gid.String()+"/runs", `{"n":0}`, nil)
	if status != http.StatusAccepted {
		t.Fatalf("start: status = %d, want 202; body=%s", status, body)
	}
	var sr startResp
	if err := json.Unmarshal(body, &sr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	got := pollRun(t, srv, sr.GraphRunID, "Interrupted")

	raw, ok := got["interrupts"].([]any)
	if !ok || len(raw) == 0 {
		t.Fatalf("expected interrupts in response, got: %v", got)
	}
	iv, _ := raw[0].(map[string]any)
	if kind, _ := iv["kind"].(string); kind != "awaiting" {
		t.Errorf("interrupt kind = %q, want awaiting", iv["kind"])
	}
}

// TestGetHaltedRunRendersHalt proves GET on a run that halted (a HaltCondition
// from a conditional Pick that errors) renders the run-level halt in the response,
// exercising the halt-DTO rendering path. A condition error halts deterministically
// at the first step regardless of run options (the worker passes no WithMaxSteps).
func TestGetHaltedRunRendersHalt(t *testing.T) {
	t.Parallel()

	gid := ixGID(44)
	store := flow.NewMemStore()
	// a -> (conditional Pick that errors) -> b. The Pick error halts the run as
	// HaltCondition at the first routing decision after a executes.
	a, b := ixVID(1), ixVID(2)
	g := flow.NewGraph[ixState](gid)
	task := flow.NewFuncTask(func(_ context.Context, in int) (int, error) { return in + 1, nil })
	sel := func(s ixState) int { return s.N }
	red := func(s *ixState, out int) error { s.N = out; return nil }
	if err := flow.AddVertex(g, a, task, sel, red); err != nil {
		t.Fatalf("AddVertex a: %v", err)
	}
	if err := flow.AddVertex(g, b, task, sel, red); err != nil {
		t.Fatalf("AddVertex b: %v", err)
	}
	cond := flow.Condition[ixState]{
		Targets: []flow.VertexID{b},
		Pick: func(_ context.Context, _ ixState) ([]flow.VertexID, error) {
			return nil, newTestError("pick boom")
		},
	}
	if err := g.AddConditionalEdge(a, cond); err != nil {
		t.Fatalf("AddConditionalEdge a: %v", err)
	}
	r, err := g.Compile(a, b, flow.WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	h := flow.NewRunnerHandle(r)
	reg := servedRegistry(t, h)
	srv, _ := newTestServer(t, reg, store)

	status, body := postJSON(t, srv.URL+"/v1/graphs/"+gid.String()+"/runs", `{"n":0}`, nil)
	if status != http.StatusAccepted {
		t.Fatalf("start: status = %d, want 202; body=%s", status, body)
	}
	var sr startResp
	if err := json.Unmarshal(body, &sr); err != nil {
		t.Fatalf("decode: %v", err)
	}

	// The halted run is terminal-ish: poll GET until a halt appears in the body.
	deadline := time.Now().Add(3 * time.Second)
	for {
		resp, err := http.Get(srv.URL + "/v1/runs/" + sr.GraphRunID)
		if err != nil {
			t.Fatalf("GET: %v", err)
		}
		b2, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			var got map[string]any
			if err := json.Unmarshal(b2, &got); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if halt, ok := got["halt"].(map[string]any); ok && halt != nil {
				if kind, _ := halt["kind"].(string); kind != "condition" {
					t.Errorf("halt kind = %q, want condition", halt["kind"])
				}
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s never showed a halt (last body=%s)", sr.GraphRunID, b2)
		}
		time.Sleep(3 * time.Millisecond)
	}
}

// TestGetRunVersionNotServed proves GET (and cancel) on a run whose GraphVersion
// the registry does not serve returns 409 + X-Graph-Version, since the handler
// cannot resolve a handle to decode/cancel the run.
func TestGetRunVersionNotServed(t *testing.T) {
	t.Parallel()

	gid := ixGID(45)
	store := flow.NewMemStore()
	// Seed a paused run under one version using an UNREGISTERED runner.
	seedHandle := flow.NewRunnerHandle(newResumableRunner(t, gid, store))
	seed, err := seedHandle.Run(context.Background(), json.RawMessage(`{"n":0}`))
	if err != nil {
		t.Fatalf("seed Run: %v", err)
	}
	runVersion := seed.Run.GraphVersion

	// Register a DIFFERENT version of the graph so the run's version is unserved.
	served := flow.NewRunnerHandle(newIncRunner(t, gid, flow.NewMemStore(), flow.WithVersion(11)))
	reg := servedRegistry(t, served)
	srv, _ := newTestServer(t, reg, store)

	// GET -> 409 + X-Graph-Version.
	resp, err := http.Get(srv.URL + "/v1/runs/" + seed.Run.GraphRunID.String())
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	b, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != http.StatusConflict {
		t.Fatalf("GET status = %d, want 409; body=%s", resp.StatusCode, b)
	}
	if got := resp.Header.Get("X-Graph-Version"); got != runVersion {
		t.Errorf("GET X-Graph-Version = %q, want %q", got, runVersion)
	}
	assertErrorBody(t, b)

	// Cancel -> 409 + X-Graph-Version as well.
	status, body := postJSON(t, srv.URL+"/v1/runs/"+seed.Run.GraphRunID.String()+"/cancel", "", nil)
	if status != http.StatusConflict {
		t.Fatalf("cancel status = %d, want 409; body=%s", status, body)
	}
}

// TestCancelMalformedBody proves a malformed (or unknown-field) cancel body maps
// to 400 at the boundary before any Cancel runs.
func TestCancelMalformedBody(t *testing.T) {
	t.Parallel()

	gid := ixGID(46)
	store := flow.NewMemStore()
	h := flow.NewRunnerHandle(newResumableRunner(t, gid, store))
	reg := servedRegistry(t, h)
	srv, _ := newTestServer(t, reg, store)

	status, body := postJSON(t, srv.URL+"/v1/graphs/"+gid.String()+"/runs", `{"n":0}`, nil)
	if status != http.StatusAccepted {
		t.Fatalf("start: status = %d, want 202; body=%s", status, body)
	}
	var sr startResp
	if err := json.Unmarshal(body, &sr); err != nil {
		t.Fatalf("decode: %v", err)
	}
	pollRun(t, srv, sr.GraphRunID, "Interrupted")

	// Unknown field -> rejected at the decode boundary (DisallowUnknownFields).
	status, body = postJSON(t, srv.URL+"/v1/runs/"+sr.GraphRunID+"/cancel", `{"bogus":true}`, nil)
	if status != http.StatusBadRequest {
		t.Fatalf("malformed cancel: status = %d, want 400; body=%s", status, body)
	}
	assertErrorBody(t, body)
}

// recordingControlPlane wraps a flow.ControlPlane and records the Op of every
// Submitted Work, so a test can prove the ingress did (or did not) submit a given
// op. It is a thin pass-through; delivery/ack semantics are unchanged.
type recordingControlPlane struct {
	inner flow.ControlPlane
	mu    sync.Mutex
	ops   []flow.WorkOp
}

func newRecordingControlPlane(inner flow.ControlPlane) *recordingControlPlane {
	return &recordingControlPlane{inner: inner}
}

func (c *recordingControlPlane) Submit(ctx context.Context, w flow.Work) error {
	c.mu.Lock()
	c.ops = append(c.ops, w.Op)
	c.mu.Unlock()
	return c.inner.Submit(ctx, w)
}

func (c *recordingControlPlane) Consume(ctx context.Context, serves []flow.GraphVersionKey) (<-chan flow.Delivery, error) {
	return c.inner.Consume(ctx, serves)
}

func (c *recordingControlPlane) submittedOps() []flow.WorkOp {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]flow.WorkOp, len(c.ops))
	copy(out, c.ops)
	return out
}

// newErroringRunner compiles a one-vertex Runner whose task returns a plain error
// (default Pause-on-error policy), so the run pauses as an Errored interruption
// carrying that error as its Cause. The error message embeds secret so a test can
// assert it is (not) leaked on the wire.
func newErroringRunner(t *testing.T, gid flow.GraphID, store flow.CheckpointStore, secret string) *flow.Runner[ixState] {
	t.Helper()
	entry := ixVID(1)
	g := flow.NewGraph[ixState](gid)
	task := flow.NewFuncTask(func(_ context.Context, _ int) (int, error) {
		return 0, newTestError(secret)
	})
	sel := func(s ixState) int { return s.N }
	red := func(s *ixState, out int) error { s.N = out; return nil }
	if err := flow.AddVertex(g, entry, task, sel, red); err != nil {
		t.Fatalf("AddVertex: %v", err)
	}
	r, err := g.Compile(entry, entry, flow.WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return r
}

// TestResumeTerminalRunConflict is the H1c regression: POST resume on a TERMINAL
// run (Completed or Cancelled) must return 409 WITHOUT submitting an OpResume Work
// — a terminal run can never resume, so the handler must not enqueue doomed work
// (the external trigger for the poison-message spin). It records the submitted ops
// and asserts no OpResume was enqueued.
func TestResumeTerminalRunConflict(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		drive    func(t *testing.T, h flow.RunnerHandle, id flow.GraphRunID)
		wantStat string
	}{
		{
			name: "completed run",
			drive: func(t *testing.T, h flow.RunnerHandle, id flow.GraphRunID) {
				waitForHandleStatus(t, h, id, flow.RunCompleted)
			},
			wantStat: "Completed",
		},
		{
			name: "cancelled run",
			drive: func(t *testing.T, h flow.RunnerHandle, id flow.GraphRunID) {
				// Wait for the run to pause (so it has a checkpoint) before cancelling.
				waitForHandleStatus(t, h, id, flow.RunInterrupted)
				if err := h.Cancel(context.Background(), id, "operator"); err != nil {
					t.Fatalf("Cancel: %v", err)
				}
			},
			wantStat: "Cancelled",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gid := ixGID(50)
			store := flow.NewMemStore()
			// A resumable runner so a started run pauses; for the completed case we
			// instead use an inc runner driven to completion. Use inc for completed,
			// resumable (then cancel) for cancelled.
			var h flow.RunnerHandle
			if tt.wantStat == "Completed" {
				h = flow.NewRunnerHandle(newIncRunner(t, gid, store))
			} else {
				h = flow.NewRunnerHandle(newResumableRunner(t, gid, store))
			}
			reg := servedRegistry(t, h)

			inner := controlplane.Mem()
			t.Cleanup(inner.Close)
			rec := newRecordingControlPlane(inner)
			startServe(t, reg, rec)
			handler := ingress.New(reg, rec, store)
			srv := httptest.NewServer(handler)
			t.Cleanup(srv.Close)

			// Start the run via the ingress so an OpRun is the only submit so far.
			status, body := postJSON(t, srv.URL+"/v1/graphs/"+gid.String()+"/runs", `{"n":0}`, nil)
			if status != http.StatusAccepted {
				t.Fatalf("start: status = %d, want 202; body=%s", status, body)
			}
			var sr startResp
			if err := json.Unmarshal(body, &sr); err != nil {
				t.Fatalf("decode: %v", err)
			}
			runID, perr := parseRunIDForTest(sr.GraphRunID)
			if perr != nil {
				t.Fatalf("parse run id: %v", perr)
			}
			tt.drive(t, h, runID)

			// Resume the terminal run -> 409, no OpResume submitted.
			status, body = postJSON(t, srv.URL+"/v1/runs/"+sr.GraphRunID+"/resume", `{"n":1}`, nil)
			if status != http.StatusConflict {
				t.Fatalf("resume terminal: status = %d, want 409; body=%s", status, body)
			}
			assertErrorBody(t, body)

			for _, op := range rec.submittedOps() {
				if op == flow.OpResume {
					t.Errorf("an OpResume Work was submitted for a terminal run; want none (ops=%v)", rec.submittedOps())
				}
			}
		})
	}
}

// parseRunIDForTest parses a run id string the same way the ingress does, for the
// terminal-resume test (which needs the typed id to drive the handle directly).
func parseRunIDForTest(s string) (flow.GraphRunID, error) {
	var id flow.GraphRunID
	if err := id.UnmarshalText([]byte(s)); err != nil {
		return flow.GraphRunID{}, err
	}
	return id, nil
}

// waitForHandleStatus polls h.Status until it reaches want, for the H1c completed
// case (driven by the worker through the ingress submit).
func waitForHandleStatus(t *testing.T, h flow.RunnerHandle, id flow.GraphRunID, want flow.RunStatus) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		st, err := h.Status(context.Background(), id)
		if err == nil && st.Status == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s did not reach %v in time (last err=%v)", id, want, err)
		}
		time.Sleep(3 * time.Millisecond)
	}
}

// TestGetErroredRunDoesNotLeakCause is the M1 regression: a run paused at an
// Errored interrupt whose task error embeds a secret must NOT leak that secret on
// GET /v1/runs/{id} by DEFAULT — the cause is rendered as a generic token. With
// WithVerboseErrors() (opt-in, for trusted/debug deployments) the raw cause IS
// surfaced. Task error messages are a wire boundary; secrets must not ride there.
func TestGetErroredRunDoesNotLeakCause(t *testing.T) {
	t.Parallel()

	const secret = "SECRET-do-not-leak-payload-9f3a"

	tests := []struct {
		name       string
		verbose    bool
		wantSecret bool
	}{
		{name: "default hides cause", verbose: false, wantSecret: false},
		{name: "verbose surfaces cause", verbose: true, wantSecret: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			gid := ixGID(51)
			store := flow.NewMemStore()
			h := flow.NewRunnerHandle(newErroringRunner(t, gid, store, secret))
			reg := servedRegistry(t, h)

			var opts []ingress.Option
			if tt.verbose {
				opts = append(opts, ingress.WithVerboseErrors())
			}
			srv, _ := newTestServer(t, reg, store, opts...)

			status, body := postJSON(t, srv.URL+"/v1/graphs/"+gid.String()+"/runs", `{"n":0}`, nil)
			if status != http.StatusAccepted {
				t.Fatalf("start: status = %d, want 202; body=%s", status, body)
			}
			var sr startResp
			if err := json.Unmarshal(body, &sr); err != nil {
				t.Fatalf("decode: %v", err)
			}
			// The run pauses Errored.
			got := pollRun(t, srv, sr.GraphRunID, "Interrupted")

			// Fetch the raw GET body so we can check the exact bytes on the wire.
			resp, err := http.Get(srv.URL + "/v1/runs/" + sr.GraphRunID)
			if err != nil {
				t.Fatalf("GET: %v", err)
			}
			raw, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			leaks := bytes.Contains(raw, []byte(secret))
			if leaks != tt.wantSecret {
				t.Errorf("GET body secret-present = %v, want %v; body=%s", leaks, tt.wantSecret, raw)
			}

			// In all cases the interrupt must still be rendered (kind present), so the
			// client learns the run errored even without the raw cause.
			ivs, _ := got["interrupts"].([]any)
			if len(ivs) == 0 {
				t.Fatalf("expected interrupts rendered, got: %v", got)
			}
			iv, _ := ivs[0].(map[string]any)
			if kind, _ := iv["kind"].(string); kind != "errored" {
				t.Errorf("interrupt kind = %q, want errored", iv["kind"])
			}
		})
	}
}

// assertErrorBody asserts the body is a small sanitized JSON {"error":"..."} that
// does not leak the engine's internal "flow:" / payload prefixes.
func assertErrorBody(t *testing.T, body []byte) {
	t.Helper()
	var er errResp
	if err := json.Unmarshal(body, &er); err != nil {
		t.Fatalf("error body is not JSON %q: %v", body, err)
	}
	if er.Error == "" {
		t.Errorf("error body has empty message: %q", body)
	}
	if bytes.Contains(body, []byte("flow:")) {
		t.Errorf("error body leaks engine internals: %q", body)
	}
}
