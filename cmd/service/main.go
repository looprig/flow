// Command service is the Tier-B example main from the flows design (§18.6): a
// plug-and-play HTTP service that runs the engine IN-PROCESS with still ZERO
// third-party dependencies. It wires the full service stack — a registry of
// compiled graphs, an in-memory control plane, the worker loop (flow.Serve), and
// the REST ingress — behind a single, secure-defaults http.Server.
//
// The composition mirrors §18.6's Tier-B snippet exactly, so the Tier-B→C
// upgrade (durable + distributed over NATS, Phase 9) is just two constructor
// swaps — controlplane.Mem()/flow.NewMemStore() → nats.ControlPlane(nc)/
// nats.Store(nc) — with the graph, worker loop, and ingress wiring unchanged.
//
// main() IS dependency injection: it constructs the concretions (store, control
// plane, registry, auth) and hands them to the engine seams. The wiring is
// factored into buildService(ctx) so it is testable WITHOUT binding a real port
// (the smoke test drives the returned handler via httptest); main() then serves
// it on a real socket via ingress.Server's secure defaults and shuts down
// cleanly on SIGINT/SIGTERM.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/looprig/flow/pkg/controlplane"
	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/flow/pkg/ingress"
	"github.com/looprig/flow/pkg/registry"
	"github.com/looprig/flow/pkg/uuid"
)

// listenAddr is the bind address for the demo service. A real deployment would
// read this from config/env; it is a const here to keep the example self-contained.
const listenAddr = ":8080"

// Pinned DEFINITION ids (§3): stable across restarts so checkpoints/resume stay
// valid. Pinned as consts via uuid.MustParse, never minted per build.
var (
	demoGraphID = flow.GraphID(uuid.MustParse("d1e2f3a4-0000-4000-8000-000000000010"))

	greetID   = flow.VertexID(uuid.MustParse("aaaaaaaa-0000-4000-8000-000000000011"))
	deliverID = flow.VertexID(uuid.MustParse("cccccccc-0000-4000-8000-000000000013"))
)

// Greeting is the demo graph state S — the caller's JSON-serializable blackboard.
// Tasks are pure with respect to it; only the per-vertex reducers (the engine is
// the single writer) mutate it. Exported fields so S round-trips through JSON for
// the HTTP boundary and checkpointing.
type Greeting struct {
	Name      string // who we are greeting (POST body input)
	Message   string // greet's output, folded in by its reducer
	Delivered bool   // deliver's output: the greeting was sent
}

// buildGraph wires the deterministic demo graph over Greeting: greet → deliver.
// It completes without interrupting, so the service smoke test is a clean
// POST → poll → RunCompleted. (Tier A demonstrates the interrupt/resume path.)
func buildGraph() (*flow.Graph[Greeting], error) {
	g := flow.NewGraph[Greeting](demoGraphID)

	// greet: reusable string→string task; selector/reducer are the only glue to S.
	if err := flow.AddVertex(g, greetID,
		flow.NewFuncTask(func(_ context.Context, name string) (string, error) {
			return "Hello, " + strings.TrimSpace(name) + "!", nil
		}),
		func(s Greeting) string { return s.Name },
		func(s *Greeting, msg string) error { s.Message = msg; return nil },
	); err != nil {
		return nil, err
	}

	// deliver: the finish vertex — "send" the greeting. No out-edge, so the run
	// completes once it runs (finish-authoritative).
	if err := flow.AddVertex(g, deliverID,
		flow.NewFuncTask(func(_ context.Context, msg string) (bool, error) {
			return msg != "", nil
		}),
		func(s Greeting) string { return s.Message },
		func(s *Greeting, delivered bool) error { s.Delivered = delivered; return nil },
	); err != nil {
		return nil, err
	}

	if err := g.AddEdge(greetID, deliverID); err != nil {
		return nil, err
	}
	return g, nil
}

// service bundles the wired HTTP handler with a shutdown func that tears the
// background worker loop and control plane down. buildService returns it so the
// caller (main or a test) owns the lifecycle explicitly: serve Handler, then call
// shutdown to drain. It is a small value object — one responsibility: carry the
// service's two halves (serve + stop).
type service struct {
	Handler  http.Handler
	shutdown func() // stops the worker loop and closes the control plane
}

// allowAll is a PLACEHOLDER authenticator for the demo: it authorizes every
// request. ingress.WithAuth installs it on every route; a real deployment MUST
// replace it with a genuine scheme (mTLS, bearer-token verification, etc.). The
// ingress bakes in no default scheme (least privilege is the caller's policy), so
// this explicit allow-all makes the demo's "no real auth" stance visible rather
// than implicit.
func allowAll(*http.Request) error { return nil }

// buildService wires the full in-process service (§18.6) and returns it without
// binding any port, so it is testable behind an httptest.Server. The composition:
//
//	store  := flow.NewMemStore()                 // durable checkpoint store seam
//	runner := g.Compile(..., flow.WithStore(store))
//	reg    := registry.New(); reg.Add(NewRunnerHandle(runner))
//	cp     := controlplane.Mem()                 // control-plane seam
//	go flow.Serve(ctx, reg, cp)                  // the worker loop
//	handler := ingress.New(reg, cp, store, WithAuth(allowAll))
//
// The Tier-B→C diff is exactly the two seam constructors (store, cp); everything
// here is unchanged. buildService starts the worker loop bound to ctx and returns
// a shutdown func that cancels it and closes the control plane (so flow.Serve's
// Consume channel closes and the worker loop drains — no goroutine leak).
func buildService(ctx context.Context) (service, error) {
	g, err := buildGraph()
	if err != nil {
		return service{}, err
	}

	store := flow.NewMemStore()
	runner, err := g.Compile(greetID, deliverID, flow.WithStore(store))
	if err != nil {
		return service{}, err
	}

	reg := registry.New()
	if err := reg.Add(flow.NewRunnerHandle(runner)); err != nil {
		return service{}, err
	}

	cp := controlplane.Mem()

	// Bind the worker loop to a child ctx so shutdown is deterministic and
	// independent of the caller's ctx lifetime. flow.Serve returns when the child
	// ctx is cancelled (Consume closes its channel and the loop drains).
	serveCtx, cancelServe := context.WithCancel(ctx)
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		if err := flow.Serve(serveCtx, reg, cp); err != nil && !errors.Is(err, context.Canceled) {
			log.Printf("worker loop exited: %v", err)
		}
	}()

	handler := ingress.New(reg, cp, store, ingress.WithAuth(allowAll))

	shutdown := func() {
		cancelServe() // stop accepting new deliveries; flow.Serve will return
		cp.Close()    // close the control plane (idempotent), unblocking Consume
		<-serveDone   // wait for the worker loop goroutine to fully exit
	}

	return service{Handler: handler, shutdown: shutdown}, nil
}

// main is the dependency-injection composition root (§18.6): it derives a
// signal-aware ctx (SIGINT/SIGTERM → graceful shutdown), builds the service,
// serves it on a real socket with ingress.Server's secure defaults, and shuts
// both the HTTP server and the worker loop down cleanly on signal.
func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	svc, err := buildService(ctx)
	if err != nil {
		fmt.Fprintln(os.Stderr, "service wiring failed:", err)
		os.Exit(1)
	}

	// ingress.Server applies secure transport defaults (timeouts + TLS 1.2 floor)
	// the bare handler cannot set itself.
	srv := ingress.Server(listenAddr, svc.Handler)

	// Serve until a signal cancels ctx; then drain the HTTP server and the worker.
	serverErr := make(chan error, 1)
	go func() { serverErr <- srv.ListenAndServe() }()
	log.Printf("flows service listening on %s", listenAddr)

	select {
	case err := <-serverErr:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			fmt.Fprintln(os.Stderr, "http server failed:", err)
			svc.shutdown()
			os.Exit(1)
		}
	case <-ctx.Done():
		log.Print("shutdown signal received; draining")
		shutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutCtx); err != nil {
			log.Printf("http shutdown: %v", err)
		}
	}
	svc.shutdown()
	log.Print("flows service stopped")
}
