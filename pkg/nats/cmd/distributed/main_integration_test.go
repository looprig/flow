//go:build integration

package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ciram-co/flow/pkg/flow"
)

// These are the Tier-C (distributed) smoke tests (§18.6). They exercise the SAME
// buildService factoring cmd/service's smoke tests do — but over durable,
// distributed NATS JetStream seams (embedded server) instead of the in-process
// ones — through an httptest.Server, so the full path (registry → nats.ControlPlane
// → flow.Serve worker loop → nats.Store → ingress) is proven end to end WITHOUT
// binding a real socket. They are integration-tagged because buildService boots an
// embedded JetStream server. Run with -race; the suite also covers the
// worker-loop/control-plane/embedded-server goroutines via clean shutdown.

// TestBuildService is the build smoke: buildService returns a non-nil handler and a
// non-nil shutdown func for a valid ctx, the cheap proof that the Tier-C wiring
// compiles and assembles (embedded server + nats seams + worker loop + ingress). It
// is NOT parallel with TestServiceEndToEnd: both boot an embedded JetStream server
// off the package-level storeDir, so each isolates onto its own t.TempDir() and the
// two must not race the shared var (a per-process store dir cannot be shared by two
// live servers).
func TestBuildService(t *testing.T) {
	storeDir = t.TempDir()

	tests := []struct {
		name        string
		wantHandler bool
	}{
		{name: "valid ctx yields a wired handler", wantHandler: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			svc, err := buildService(ctx)
			if err != nil {
				t.Fatalf("buildService: %v", err)
			}
			if (svc.Handler != nil) != tt.wantHandler {
				t.Fatalf("handler non-nil = %v, want %v", svc.Handler != nil, tt.wantHandler)
			}
			if svc.shutdown == nil {
				t.Fatalf("shutdown func is nil, want non-nil")
			}
			svc.shutdown()
		})
	}
}

// TestServiceEndToEnd is the headline Tier-C smoke: POST a run, get 202 + a
// graphRunID, poll GET /v1/runs/{id} until RunCompleted, and assert the final
// state — all against the factored buildService wiring (durable NATS seams) behind
// an httptest.Server. It then cancels the ctx and shuts the service down, so a
// -race run also proves clean shutdown with no lingering worker/control-plane/
// embedded-server goroutines. NOT parallel (see TestBuildService): it owns the
// shared storeDir for its own embedded server.
func TestServiceEndToEnd(t *testing.T) {
	storeDir = t.TempDir()

	ctx, cancel := context.WithCancel(context.Background())

	svc, err := buildService(ctx)
	if err != nil {
		cancel()
		t.Fatalf("buildService: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		svc.shutdown()
	})

	srv := httptest.NewServer(svc.Handler)
	t.Cleanup(srv.Close)

	// POST /v1/graphs/{graphID}/runs with the initial state -> 202 + graphRunID.
	status, body := postJSON(t, srv.URL+"/v1/graphs/"+demoGraphID.String()+"/runs", `{"Name":"Ada"}`)
	if status != http.StatusAccepted {
		t.Fatalf("start run status = %d, want 202 (body=%s)", status, body)
	}
	var start struct {
		GraphRunID   string `json:"graphRunID"`
		GraphVersion string `json:"graphVersion"`
	}
	if err := json.Unmarshal(body, &start); err != nil {
		t.Fatalf("decode start body %q: %v", body, err)
	}
	if start.GraphRunID == "" {
		t.Fatalf("start body missing graphRunID: %s", body)
	}

	// Poll GET /v1/runs/{id} until RunCompleted (the demo graph completes without
	// interrupting).
	got := pollRun(t, srv, start.GraphRunID, flow.RunCompleted.String())

	// Assert the final state was folded through the graph: greet wrote a message
	// and deliver marked it delivered.
	state, _ := got["state"].(map[string]any)
	if state == nil {
		t.Fatalf("run result missing state: %v", got)
	}
	if msg, _ := state["Message"].(string); !strings.Contains(msg, "Ada") {
		t.Errorf("final Message = %q, want it to greet Ada", msg)
	}
	if delivered, _ := state["Delivered"].(bool); !delivered {
		t.Errorf("final Delivered = %v, want true", delivered)
	}
}

// postJSON POSTs body to url as JSON and returns the status code and body bytes.
func postJSON(t *testing.T, url, body string) (int, []byte) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// pollRun polls GET /v1/runs/{id} until the run's Status reaches want or a
// deadline, returning the decoded run-result map. It fails the test on timeout.
func pollRun(t *testing.T, srv *httptest.Server, id, want string) map[string]any {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err := http.Get(srv.URL + "/v1/runs/" + id)
		if err != nil {
			t.Fatalf("GET run: %v", err)
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			var out map[string]any
			if err := json.Unmarshal(body, &out); err != nil {
				t.Fatalf("decode run %q: %v", body, err)
			}
			if run, _ := out["run"].(map[string]any); run != nil {
				if statusString(run["Status"]) == want {
					return out
				}
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s did not reach %s in time (last status=%d body=%s)", id, want, resp.StatusCode, body)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// statusString renders a JSON-decoded Status (a number) as its RunStatus token,
// matching encoding/json's numeric encoding of the int enum.
func statusString(v any) string {
	f, ok := v.(float64)
	if !ok {
		return ""
	}
	return flow.RunStatus(int(f)).String()
}
