// Command embed is the Tier-A example main from the flows design (§18.6): it
// EMBEDS the engine as a plain library — zero third-party dependencies, no
// service, no control plane — and drives one run end to end in-process.
//
// The pattern it proves (copy-paste-able): build a small Graph[S] in main(),
// Compile it once with an in-memory CheckpointStore, Run it from an initial
// state, and — when a vertex Interrupts (human-in-the-loop) — Resume it by id
// with a payload, all in the SAME process against the SAME store. This is the
// in-process embed story: no HTTP, no registry, no worker loop.
//
// To keep the demo deterministic and testable WITHOUT binding a port or hanging,
// the wiring lives in a factored run(ctx) that main() simply calls and reports.
// The graph is a tiny three-vertex pipeline that pauses once for "approval" and
// then completes:
//
//	greet ──▶ approve ──▶ deliver
//	            (Interrupts on the first pass; Resume(payload) feeds the
//	             approval and the run routes on to deliver → finish)
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
)

// Pinned DEFINITION ids (§3). GraphID and VertexID are stable across process
// restarts because a checkpoint frontier references vertices by VertexID and
// Resume rebuilds the graph from code; they are pinned as consts via
// uuid.MustParse rather than minted per build (which would break resume).
var (
	graphID = flow.GraphID(uuid.MustParse("e3b0c442-0000-4000-8000-000000000001"))

	greetID   = flow.VertexID(uuid.MustParse("aaaaaaaa-0000-4000-8000-000000000001"))
	approveID = flow.VertexID(uuid.MustParse("bbbbbbbb-0000-4000-8000-000000000002"))
	deliverID = flow.VertexID(uuid.MustParse("cccccccc-0000-4000-8000-000000000003"))
)

// Greeting is the shared graph state S — the caller's blackboard threaded through
// the flow. Tasks never touch it; only the per-vertex reducers (the engine is the
// single writer) mutate it. Every field is exported so S round-trips through JSON
// for checkpointing and resume.
type Greeting struct {
	Name      string // who we are greeting (initial input)
	Message   string // greet's output, folded in by its reducer
	Approved  bool   // set true on Resume so approve routes onward
	Delivered bool   // deliver's output: the greeting was sent
}

// buildGraph wires the deterministic three-vertex demo over Greeting. Keeping the
// wiring in one function (separate from Compile/Run) keeps main() readable and
// the topology reusable.
func buildGraph() (*flow.Graph[Greeting], error) {
	g := flow.NewGraph[Greeting](graphID)

	// greet: a reusable string→string task that formats a greeting. It knows
	// nothing about Greeting; the selector (S→I) and reducer ((S,O)→S) are the glue.
	if err := flow.AddVertex(g, greetID,
		flow.NewFuncTask(func(_ context.Context, name string) (string, error) {
			return "Hello, " + strings.TrimSpace(name) + "!", nil
		}),
		func(s Greeting) string { return s.Name },
		func(s *Greeting, msg string) error { s.Message = msg; return nil },
	); err != nil {
		return nil, err
	}

	// approve: the human-in-the-loop gate. On the first pass (no resume payload) it
	// Interrupts (Awaiting); on Resume it reads the approver's decision from the
	// payload and returns it, and the reducer folds it into S so the run routes on.
	if err := flow.AddVertex(g, approveID,
		flow.NewFuncTask(func(ctx context.Context, msg string) (bool, error) {
			if ok, present := flow.ResumePayload[bool](ctx); present {
				return ok, nil // Resume pass: the approver decided.
			}
			return false, flow.Interrupt(ctx, "awaiting approval for: "+msg)
		}),
		func(s Greeting) string { return s.Message },
		func(s *Greeting, approved bool) error { s.Approved = approved; return nil },
	); err != nil {
		return nil, err
	}

	// deliver: the finish vertex — "send" the approved greeting. It has no out-edge,
	// so once it runs the frontier drains and the run completes (finish-authoritative).
	if err := flow.AddVertex(g, deliverID,
		flow.NewFuncTask(func(_ context.Context, approved bool) (bool, error) {
			return approved, nil
		}),
		func(s Greeting) bool { return s.Approved },
		func(s *Greeting, delivered bool) error { s.Delivered = delivered; return nil },
	); err != nil {
		return nil, err
	}

	if err := g.AddEdge(greetID, approveID); err != nil {
		return nil, err
	}
	if err := g.AddEdge(approveID, deliverID); err != nil {
		return nil, err
	}
	return g, nil
}

// run is the factored, testable body of the embed example (§18.6): build →
// Compile(WithStore) → Run → (on interrupt) Resume → print. It honors ctx so a
// cancelled context aborts rather than hangs, and returns any engine error to the
// caller. main() is a thin shell over it.
func run(ctx context.Context) error {
	g, err := buildGraph()
	if err != nil {
		return err
	}

	// Compile once with an in-memory store set at WithStore; the same store backs
	// Run and the later Resume.
	runner, err := g.Compile(greetID, deliverID, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		return err
	}

	// Run from the initial state. The approve vertex Interrupts, so this first
	// Result is RunInterrupted with one Awaiting interruption.
	res, err := runner.Run(ctx, Greeting{Name: "Ada"})
	if err != nil {
		return err
	}
	fmt.Printf("run %s -> %s\n", res.Run.GraphRunID, res.Run.Status)
	for _, iv := range res.Interrupts {
		fmt.Printf("  interrupt on %s: %v\n", iv.Vertex, iv.Info)
	}

	// Completed already (no interrupt)? Nothing to resume — report and return.
	if res.Run.Status == flow.RunCompleted {
		fmt.Printf("final: message=%q delivered=%v\n", res.State.Message, res.State.Delivered)
		return nil
	}

	// Later (here, immediately; in production on a webhook days later): Resume the
	// SAME run id with the approval payload. The engine re-runs the paused vertex,
	// reads the payload, folds it in, and routes on to deliver → finish.
	res, err = runner.Resume(ctx, res.Run.GraphRunID, true)
	if err != nil {
		return err
	}
	fmt.Printf("resume %s -> %s\n", res.Run.GraphRunID, res.Run.Status)
	fmt.Printf("final: message=%q delivered=%v\n", res.State.Message, res.State.Delivered)
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "embed example failed:", err)
		os.Exit(1)
	}
}
