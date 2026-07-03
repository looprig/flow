package flow_test

// This file is the motivating worked example of the design doc's §16 — a single,
// black-box, end-to-end integration test of the whole public surface of package
// flow. It is in package flow_test on purpose: it may touch ONLY the exported
// API, so it doubles as proof that the public surface is sufficient to build,
// run, pause, resume, retry, route, and observe the §16 flow without reaching
// into the engine.
//
// THE §16 FLOW (a sales associate's question through a consultant, a RAG draft,
// and — when the answer is not in the repository — a human expert):
//
//	intake ─▶ draft ─▶ review ─┬─(approved)─────▶ sendToSales ─▶ finish
//	             ▲              ├─(needs expert)─▶ ticket ─▶ awaitExpert
//	             │              │                  (opens     (Interrupts; the
//	             │              │                   ticket,    expert answers via
//	             │              │                   sets       Resume, then routes
//	             └──(refine)────┘                   TicketID)  back to review)
//	                            (refine adjusts the question and loops back to draft)
//
// §16 describes "ticket (creates a ticket (sets TicketID) then Interrupt)" — here
// modeled as TWO vertices so the ticket side effect COMMITS to S (TicketID is
// durably checkpointed) before a distinct, resumable pause vertex waits for the
// expert. Same flow, cleaner durability.
//
// The flow exercises, all via the exported API: a cycle (refine → draft), a
// conditional edge (review's three-way Pick), per-vertex retry + error-route on
// draft, an Awaiting interrupt → external Resume by id on awaitExpert,
// append-only durable checkpoints, finish-authoritative completion, and
// instrumentation hooks with lifecycle timestamps.
//
// DETERMINISM. The tasks are plain deterministic stand-ins (no LLM): RAGDraft
// returns a canned answer, and ConsultantReview decides approve / needs-expert /
// unsatisfied purely from fields the test seeds in Request plus a bounded refine
// counter. Every path is therefore reproducible and asserted on State/Status/
// Interrupts/History/hooks, not merely on "no error".

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/flow/pkg/uuid"
)

// --- Pinned definition IDs (§3) ---------------------------------------------
//
// GraphID and VertexID are stable DEFINITION ids: a checkpoint frontier
// references vertices by VertexID and Resume rebuilds the graph from code, so
// they must be identical across process restarts. They are pinned as consts via
// uuid.MustParse (§3) — a freshly generated uuid.New() per build would break
// resume.

var (
	exGraphID = flow.GraphID(uuid.MustParse("a1b2c3d4-0000-4000-8000-000000000000"))

	intakeID      = flow.VertexID(uuid.MustParse("11111111-1111-4111-8111-111111111111"))
	draftID       = flow.VertexID(uuid.MustParse("22222222-2222-4222-8222-222222222222"))
	reviewID      = flow.VertexID(uuid.MustParse("33333333-3333-4333-8333-333333333333"))
	sendToSalesID = flow.VertexID(uuid.MustParse("44444444-4444-4444-8444-444444444444"))
	ticketID      = flow.VertexID(uuid.MustParse("55555555-5555-4555-8555-555555555555"))
	refineID      = flow.VertexID(uuid.MustParse("66666666-6666-4666-8666-666666666666"))
	awaitExpertID = flow.VertexID(uuid.MustParse("77777777-7777-4777-8777-777777777777"))
)

// --- Shared graph state S (§16) ---------------------------------------------

// Request is the §16 graph state S — the caller's domain blackboard threaded
// through the flow. The only mutation path is a vertex's reducer (the engine is
// the single writer); tasks are pure and never touch it. It must round-trip
// through JSON so it can be checkpointed and resumed, so every field is exported.
type Request struct {
	Question     string // the sales associate's question
	Draft        string // RAGDraft's answer, written by the draft reducer
	Approved     bool   // consultant approved the draft (route → sendToSales)
	NeedsExpert  bool   // answer not in the repository (route → ticket)
	Refined      int    // how many refine cycles have run (bounds the loop)
	MaxRefines   int    // refine budget: after this many, the consultant approves
	TicketID     string // service ticket opened by the ticket vertex
	ExpertAnswer string // the expert's tailored answer, folded in on Resume
	Sent         bool   // sendToSales delivered the answer to the associate
	LastErr      string // last error recorded by draft's WithErrorRoute
}

// --- Reusable, graph-agnostic tasks (§5, §16) -------------------------------
//
// Each task is a pure Task[I,O] over its OWN narrow IO — it knows nothing about
// Request or any graph. The selector (S→I) and reducer ((S,O)→S) supplied at
// AddVertex are the only glue that knows S, so the same task value is reusable
// across bindings and graphs.

// query is RAGDraft's input; answer is its output.
type query struct{ Text string }
type answer struct{ Text string }

// ragDraft is the "RAG" drafting task: a deterministic stand-in that returns a
// canned answer for the question. The real engine would run retrieval + an LLM
// here; v1 is a plain function so the example is reproducible.
func ragDraft() *flow.FuncTask[query, answer] {
	return flow.NewFuncTask(func(_ context.Context, in query) (answer, error) {
		return answer{Text: "draft answer for: " + in.Text}, nil
	})
}

// reviewIn is ConsultantReview's input — the fields the consultant weighs.
type reviewIn struct {
	Draft       string
	Approved    bool
	NeedsExpert bool
	Refined     int
	MaxRefines  int
}

// verdict is ConsultantReview's output: exactly one of the three routing
// outcomes. Outcomes are VALUES in S routed by the conditional edge, never Go
// errors (§12.1).
type verdict struct {
	Approve     bool
	NeedExpert  bool
	Unsatisfied bool
}

// consultantReview is the consultant: a deterministic decision derived only from
// its input. NeedsExpert (answer not in the repository) wins first; otherwise an
// explicit Approved flag or an exhausted refine budget approves; otherwise the
// consultant is unsatisfied and asks for one more refine cycle. This makes each
// of the three branches selectable purely from seeded Request fields.
func consultantReview() *flow.FuncTask[reviewIn, verdict] {
	return flow.NewFuncTask(func(_ context.Context, in reviewIn) (verdict, error) {
		switch {
		case in.NeedsExpert:
			return verdict{NeedExpert: true}, nil
		case in.Approved || in.Refined >= in.MaxRefines:
			return verdict{Approve: true}, nil
		default:
			return verdict{Unsatisfied: true}, nil
		}
	})
}

// createTicket is the ticket-opening task: it mints a deterministic ticket id
// from the question. The PAUSE itself (flow.Interrupt) lives in the ticket
// vertex's selector-free task body below, because pausing needs ctx and the
// resume payload; createTicket only models the side effect of opening a ticket.
func createTicket() *flow.FuncTask[string, string] {
	return flow.NewFuncTask(func(_ context.Context, question string) (string, error) {
		return "TICKET-" + strings.ToUpper(strings.ReplaceAll(question, " ", "-")), nil
	})
}

// --- Graph wiring (§16) -----------------------------------------------------

// buildGraph wires the full §16 topology over Request and returns the mutable
// Graph. Callers add per-vertex options (e.g. draft's retry/error-route, or a
// substitute draft task) by supplying draftTask and draftOpts, then Compile it.
// Keeping wiring in one helper lets each test scenario reuse the identical
// topology while varying only the draft binding under test.
func buildGraph(
	t *testing.T,
	draftTask flow.Task[query, answer],
	awaitTask flow.Task[string, string],
	draftOpts ...flow.VertexOption[Request],
) *flow.Graph[Request] {
	t.Helper()
	g := flow.NewGraph[Request](exGraphID)

	// intake: normalize the question (here, a pass-through that proves the entry
	// vertex ran). Reusable: a trivial string→string task.
	mustAddVertex(t, g, intakeID,
		flow.NewFuncTask(func(_ context.Context, q string) (string, error) {
			return strings.TrimSpace(q), nil
		}),
		func(s Request) string { return s.Question },
		func(s *Request, q string) error { s.Question = q; return nil },
	)

	// draft: RAG drafts an answer; the reducer writes Request.Draft. The caller
	// supplies draftOpts (WithRetry AND WithErrorRoute(ticket,…) in §16).
	mustAddVertex(t, g, draftID, draftTask,
		func(s Request) query { return query{Text: s.Question} },
		func(s *Request, a answer) error { s.Draft = a.Text; return nil },
		draftOpts...,
	)

	// review: the consultant. Pure decision; the reducer folds the verdict into S
	// so the conditional edge can route on it.
	mustAddVertex(t, g, reviewID, consultantReview(),
		func(s Request) reviewIn {
			return reviewIn{
				Draft:       s.Draft,
				Approved:    s.Approved,
				NeedsExpert: s.NeedsExpert,
				Refined:     s.Refined,
				MaxRefines:  s.MaxRefines,
			}
		},
		func(s *Request, v verdict) error {
			s.Approved = v.Approve
			s.NeedsExpert = v.NeedExpert
			return nil
		},
	)

	// sendToSales: deliver the approved answer back to the associate.
	mustAddVertex(t, g, sendToSalesID,
		flow.NewFuncTask(func(_ context.Context, draft string) (bool, error) {
			return draft != "", nil
		}),
		func(s Request) string { return s.Draft },
		func(s *Request, ok bool) error { s.Sent = ok; return nil },
	)

	// ticket: open a service ticket. This is a plain Done vertex — its reducer
	// writes Request.TicketID, which is therefore committed and durably
	// checkpointed BEFORE the run pauses, so a paused run's TicketID is visible
	// via Get(id). It then routes (static edge) to awaitExpert. (§16: "creates a
	// ticket (sets TicketID) then ... interrupts" — modeled as two vertices so the
	// ticket side effect commits to S and the pause is a distinct, resumable step.)
	mustAddVertex(t, g, ticketID, createTicket(),
		func(s Request) string { return s.Question },
		func(s *Request, ticket string) error { s.TicketID = ticket; return nil },
	)

	// awaitExpert: PAUSE (Awaiting) for the human expert. On the first pass (no
	// resume payload) it Interrupts. On Resume it reads the expert's answer from
	// the payload, and its reducer folds the answer into S and approves the draft
	// so the run routes onward (awaitExpert → review → sendToSales). The caller may
	// substitute awaitTask (e.g. to assert the error-route path lands here).
	mustAddVertex(t, g, awaitExpertID, awaitTask,
		func(s Request) string { return s.TicketID },
		func(s *Request, out string) error {
			// On the first pass the task interrupts before returning, so this reducer
			// only runs on resume, with out == the expert's answer.
			s.ExpertAnswer = out
			// Folding the expert answer makes the draft acceptable and clears the
			// expert flag, so review approves on the next pass.
			s.Draft = "expert: " + out
			s.NeedsExpert = false
			s.Approved = true
			return nil
		},
	)

	// refine: adjust the question and bump the refine counter, then loop back to
	// draft (the cycle / back-edge).
	mustAddVertex(t, g, refineID,
		flow.NewFuncTask(func(_ context.Context, q string) (string, error) {
			return q + " (refined)", nil
		}),
		func(s Request) string { return s.Question },
		func(s *Request, q string) error {
			s.Question = q
			s.Refined++
			return nil
		},
	)

	// Edges: intake → draft → review; refine → draft (cycle). sendToSales is the
	// finish vertex and has NO out-edge: once it executes the frontier drains and
	// the run completes (finish is authoritative, §9.5).
	mustAddEdge(t, g, intakeID, draftID)
	mustAddEdge(t, g, draftID, reviewID)
	mustAddEdge(t, g, refineID, draftID)

	// review's conditional three-way edge: approved → sendToSales, needs expert →
	// ticket, otherwise (unsatisfied) → refine.
	mustAddConditionalEdge(t, g, reviewID, flow.Condition[Request]{
		Targets: []flow.VertexID{sendToSalesID, ticketID, refineID},
		Pick: func(_ context.Context, s Request) ([]flow.VertexID, error) {
			switch {
			case s.Approved:
				return []flow.VertexID{sendToSalesID}, nil
			case s.NeedsExpert:
				return []flow.VertexID{ticketID}, nil
			default:
				return []flow.VertexID{refineID}, nil
			}
		},
	})

	// ticket → awaitExpert (open the ticket, then wait); awaitExpert → review once
	// the expert has answered (the run re-reviews with the expert draft).
	mustAddEdge(t, g, ticketID, awaitExpertID)
	mustAddEdge(t, g, awaitExpertID, reviewID)

	return g
}

// awaitExpertBody builds the awaitExpert vertex's task: Interrupt (Awaiting) on
// the first pass, and on resume return the expert answer read from the payload so
// the reducer folds it in. The interrupt-vs-resume branch is keyed on the resume
// payload: first pass (no payload) → Interrupt; resume pass (payload present) →
// return the answer.
func awaitExpertBody() flow.Task[string, string] {
	return flow.NewFuncTask(func(ctx context.Context, ticket string) (string, error) {
		if ans, ok := flow.ResumePayload[string](ctx); ok && ans != "" {
			// Resume pass: the expert answered. Return it so the reducer folds it in.
			return ans, nil
		}
		// First pass: pause Awaiting the expert (the ticket is already open in S).
		return "", flow.Interrupt(ctx, "awaiting expert answer for "+ticket)
	})
}

// --- Build helpers (fail fast, keep the scenarios readable) -----------------

func mustAddVertex[I, O any](
	t *testing.T, g *flow.Graph[Request], id flow.VertexID,
	task flow.Task[I, O], sel flow.Selector[Request, I], red flow.Reducer[Request, O],
	opts ...flow.VertexOption[Request],
) {
	t.Helper()
	if err := flow.AddVertex(g, id, task, sel, red, opts...); err != nil {
		t.Fatalf("AddVertex(%s): %v", id, err)
	}
}

func mustAddEdge(t *testing.T, g *flow.Graph[Request], from, to flow.VertexID) {
	t.Helper()
	if err := g.AddEdge(from, to); err != nil {
		t.Fatalf("AddEdge(%s→%s): %v", from, to, err)
	}
}

func mustAddConditionalEdge(t *testing.T, g *flow.Graph[Request], from flow.VertexID, c flow.Condition[Request]) {
	t.Helper()
	if err := g.AddConditionalEdge(from, c); err != nil {
		t.Fatalf("AddConditionalEdge(%s): %v", from, err)
	}
}

// --- Scenario 1: happy approval ---------------------------------------------

// TestExampleHappyApproval drives the approve branch end to end:
// intake → draft → review → sendToSales → finish. The consultant approves
// immediately (seeded Approved), so no cycle and no ticket; the run Completes
// with the draft written and delivered.
func TestExampleHappyApproval(t *testing.T) {
	t.Parallel()

	store := flow.NewMemStore()
	g := buildGraph(t, ragDraft(), awaitExpertBody())
	runner, err := g.Compile(intakeID, sendToSalesID, flow.WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	res, err := runner.Run(context.Background(), Request{
		Question: "what is the SLA?",
		Approved: true, // consultant approves on the first review
	})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.Run.Status != flow.RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}
	if len(res.Interrupts) != 0 || res.Halt != nil {
		t.Fatalf("unexpected pause/halt: interrupts=%v halt=%v", res.Interrupts, res.Halt)
	}
	if res.State.Draft != "draft answer for: what is the SLA?" {
		t.Errorf("Draft = %q, want the canned RAG answer", res.State.Draft)
	}
	if !res.State.Approved {
		t.Errorf("Approved = false, want true")
	}
	if !res.State.Sent {
		t.Errorf("Sent = false, want true (sendToSales must have delivered)")
	}
	if !res.Run.CompletedAt.After(res.Run.CreatedAt) && !res.Run.CompletedAt.Equal(res.Run.CreatedAt) {
		t.Errorf("CompletedAt %v not >= CreatedAt %v", res.Run.CompletedAt, res.Run.CreatedAt)
	}
}

// --- Scenario 2: refine cycle -----------------------------------------------

// TestExampleRefineCycle drives the back-edge: the consultant is unsatisfied
// until the refine budget is hit, so review → refine → draft → review repeats,
// then approves and completes. It asserts the cycle actually ran (draft executed
// more than once) via a hook-counted draft, and that the run terminated WITHOUT
// relying on the maxSteps safety bound (the refine budget bounds the loop).
func TestExampleRefineCycle(t *testing.T) {
	t.Parallel()

	var draftRuns atomic.Int32
	store := flow.NewMemStore()
	g := buildGraph(t, ragDraft(), awaitExpertBody())
	runner, err := g.Compile(intakeID, sendToSalesID, flow.WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	res, err := runner.Run(context.Background(), Request{
		Question:   "explain pricing tiers",
		MaxRefines: 1, // unsatisfied once (Refined 0 < 1), then approved at Refined 1
	}, flow.WithHooks(flow.Hooks{
		OnVertexStart: func(_ context.Context, ev flow.VertexState) {
			if ev.VertexID == draftID {
				draftRuns.Add(1)
			}
		},
	}))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.Run.Status != flow.RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}
	if got := draftRuns.Load(); got < 2 {
		t.Errorf("draft ran %d times, want >= 2 (the refine cycle must loop back to draft)", got)
	}
	if res.State.Refined != 1 {
		t.Errorf("Refined = %d, want 1 (exactly one refine cycle)", res.State.Refined)
	}
	if !res.State.Approved || !res.State.Sent {
		t.Errorf("Approved=%v Sent=%v, want both true", res.State.Approved, res.State.Sent)
	}
	if !strings.Contains(res.State.Question, "(refined)") {
		t.Errorf("Question = %q, want it to carry the refine marker", res.State.Question)
	}
}

// --- Scenario 3: expert interrupt → resume (the headline) -------------------

// TestExampleExpertInterruptResume is the headline §16 scenario. The consultant
// needs an expert, so review → ticket → awaitExpert: the ticket vertex opens a
// ticket (committing Request.TicketID) and the awaitExpert vertex Interrupts
// (Awaiting). Run returns RunInterrupted with one Awaiting Interruption for the
// awaitExpert vertex, and the opened TicketID is durably persisted (visible via
// Get(id)). Then Resume(id, expertAnswer) re-runs the awaitExpert vertex, which
// reads the payload, folds the expert answer in, and routes onward to
// RunCompleted. The append-only History grew and already-Done vertices (intake)
// did NOT re-run.
func TestExampleExpertInterruptResume(t *testing.T) {
	t.Parallel()

	var intakeRuns atomic.Int32
	store := flow.NewMemStore()
	g := buildGraph(t, ragDraft(), awaitExpertBody())
	runner, err := g.Compile(intakeID, sendToSalesID, flow.WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	countIntake := flow.WithHooks(flow.Hooks{
		OnVertexStart: func(_ context.Context, ev flow.VertexState) {
			if ev.VertexID == intakeID {
				intakeRuns.Add(1)
			}
		},
	})

	// First pass: the consultant needs an expert → ticket (opens it) → awaitExpert
	// (Interrupts).
	res, err := runner.Run(context.Background(), Request{
		Question:    "is FedRAMP High supported?",
		NeedsExpert: true,
	}, countIntake)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if res.Run.Status != flow.RunInterrupted {
		t.Fatalf("Status = %v, want RunInterrupted", res.Run.Status)
	}
	if res.Halt != nil {
		t.Fatalf("Halt = %v, want nil (this is a per-vertex pause, not a run-level halt)", res.Halt)
	}
	if len(res.Interrupts) != 1 {
		t.Fatalf("Interrupts = %d, want exactly 1", len(res.Interrupts))
	}
	iv := res.Interrupts[0]
	if iv.Vertex != awaitExpertID {
		t.Errorf("interrupt vertex = %s, want awaitExpert %s", iv.Vertex, awaitExpertID)
	}
	if iv.Kind != flow.Awaiting {
		t.Errorf("interrupt kind = %v, want Awaiting", iv.Kind)
	}
	// The ticket vertex ran and committed BEFORE the pause, so TicketID is set in
	// the paused state.
	if res.State.TicketID == "" {
		t.Fatalf("TicketID empty at pause, want it set (the ticket vertex committed before awaitExpert paused)")
	}

	id := res.Run.GraphRunID

	// The pause is durable: Get(id) reflects the interrupted run and the persisted
	// TicketID from the store.
	got, err := runner.Get(context.Background(), id)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Run.Status != flow.RunInterrupted {
		t.Errorf("Get Status = %v, want RunInterrupted (the pause is persisted)", got.Run.Status)
	}
	if got.State.TicketID != res.State.TicketID {
		t.Errorf("Get TicketID = %q, want persisted %q", got.State.TicketID, res.State.TicketID)
	}

	histBefore, err := store.History(context.Background(), id)
	if err != nil {
		t.Fatalf("History before resume: %v", err)
	}

	// The expert answers days later: Resume by id with the tailored answer.
	const expert = "Yes, FedRAMP High is supported in GovCloud."
	res2, err := runner.Resume(context.Background(), id, expert)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}

	if res2.Run.Status != flow.RunCompleted {
		t.Fatalf("Resume Status = %v, want RunCompleted", res2.Run.Status)
	}
	if res2.State.ExpertAnswer != expert {
		t.Errorf("ExpertAnswer = %q, want the resumed payload", res2.State.ExpertAnswer)
	}
	if !strings.Contains(res2.State.Draft, expert) {
		t.Errorf("Draft = %q, want the expert answer folded in", res2.State.Draft)
	}
	if !res2.State.Sent {
		t.Errorf("Sent = false, want true (run routed on to sendToSales)")
	}
	if intakeRuns.Load() != 1 {
		t.Errorf("intake ran %d times, want 1 (a Done vertex must not re-run on resume)", intakeRuns.Load())
	}

	histAfter, err := store.History(context.Background(), id)
	if err != nil {
		t.Fatalf("History after resume: %v", err)
	}
	if len(histAfter) <= len(histBefore) {
		t.Errorf("History did not grow on resume: before=%d after=%d", len(histBefore), len(histAfter))
	}
	if res2.Run.Revision <= res.Run.Revision {
		t.Errorf("Revision = %d, want > pause revision %d (resume continues the append sequence)",
			res2.Run.Revision, res.Run.Revision)
	}
}

// --- Scenario 4: draft retry + error-route ----------------------------------

// flakyDraft is a draft task that fails its first failUntil-1 attempts with a
// transient error, then succeeds. It counts attempts so a test can assert the
// retry loop actually re-ran the task.
func flakyDraft(failUntil int, attempts *atomic.Int32) flow.Task[query, answer] {
	return flow.NewFuncTask(func(_ context.Context, in query) (answer, error) {
		n := attempts.Add(1)
		if int(n) < failUntil {
			return answer{}, errors.New("transient retrieval error")
		}
		return answer{Text: "draft answer for: " + in.Text}, nil
	})
}

// alwaysFailDraft is a draft task that always fails, to exhaust retries and
// trigger the WithErrorRoute fold.
func alwaysFailDraft(attempts *atomic.Int32) flow.Task[query, answer] {
	return flow.NewFuncTask(func(_ context.Context, _ query) (answer, error) {
		attempts.Add(1)
		return answer{}, errors.New("repository unreachable")
	})
}

// TestExampleDraftRetryAndErrorRoute covers draft's two §16 failure policies in
// one table: (a) a transient failure that WithRetry re-runs to success (the run
// completes and Attempt > 1), and (b) an exhausted failure that WithErrorRoute
// folds into Request.LastErr and routes to the ticket handler (LastErr set and
// the handler ran, surfacing as a pause on the resumed ticket vertex).
func TestExampleDraftRetryAndErrorRoute(t *testing.T) {
	t.Parallel()

	noBackoff := func(int) time.Duration { return 0 }

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "retry recovers then completes",
			run: func(t *testing.T) {
				var attempts, draftFinishAttempt atomic.Int32
				store := flow.NewMemStore()
				draft := flakyDraft(3, &attempts) // fail twice, succeed on the 3rd
				g := buildGraph(t, draft, awaitExpertBody(),
					flow.WithRetry[Request](flow.RetryPolicy{
						MaxAttempts: 3,
						Backoff:     noBackoff,
					}),
					flow.WithErrorRoute[Request](ticketID, func(s *Request, e error) error {
						s.LastErr = e.Error()
						return nil
					}),
				)
				runner, err := g.Compile(intakeID, sendToSalesID, flow.WithStore(store))
				if err != nil {
					t.Fatalf("Compile: %v", err)
				}

				res, err := runner.Run(context.Background(), Request{
					Question: "what regions are available?",
					Approved: true,
				}, flow.WithHooks(flow.Hooks{
					OnVertexFinish: func(_ context.Context, ev flow.VertexState) {
						if ev.VertexID == draftID {
							draftFinishAttempt.Store(int32(ev.Attempt))
						}
					},
				}))
				if err != nil {
					t.Fatalf("Run: %v", err)
				}

				if res.Run.Status != flow.RunCompleted {
					t.Fatalf("Status = %v, want RunCompleted (retry recovered)", res.Run.Status)
				}
				if attempts.Load() != 3 {
					t.Errorf("draft executed %d times, want 3 (2 failures + 1 success)", attempts.Load())
				}
				if got := draftFinishAttempt.Load(); got <= 1 {
					t.Errorf("draft VertexState.Attempt = %d, want > 1 (the retry loop bumped it)", got)
				}
				if res.State.LastErr != "" {
					t.Errorf("LastErr = %q, want empty (error route must NOT fire when retry succeeds)", res.State.LastErr)
				}
			},
		},
		{
			name: "exhausted retries route the error to the ticket handler",
			run: func(t *testing.T) {
				var attempts atomic.Int32
				store := flow.NewMemStore()
				draft := alwaysFailDraft(&attempts)
				// Draft exhausts its retries; WithErrorRoute folds the error into
				// Request.LastErr and activates the ticket handler vertex (createTicket),
				// which opens a ticket (sets TicketID) and routes on to awaitExpert,
				// which pauses — a deterministic, observable landing point.
				g := buildGraph(t, draft, awaitExpertBody(),
					flow.WithRetry[Request](flow.RetryPolicy{
						MaxAttempts: 2,
						Backoff:     noBackoff,
					}),
					flow.WithErrorRoute[Request](ticketID, func(s *Request, e error) error {
						s.LastErr = e.Error()
						return nil
					}),
				)
				runner, err := g.Compile(intakeID, sendToSalesID, flow.WithStore(store))
				if err != nil {
					t.Fatalf("Compile: %v", err)
				}

				res, err := runner.Run(context.Background(), Request{
					Question: "exotic compliance question",
				})
				if err != nil {
					t.Fatalf("Run: %v", err)
				}

				if attempts.Load() != 2 {
					t.Errorf("draft executed %d times, want 2 (MaxAttempts exhausted)", attempts.Load())
				}
				if res.State.LastErr == "" {
					t.Errorf("LastErr empty, want the draft error folded in by the record reducer")
				}
				if !strings.Contains(res.State.LastErr, "repository unreachable") {
					t.Errorf("LastErr = %q, want it to wrap the draft failure", res.State.LastErr)
				}
				// The error route activated the ticket handler, which ran and opened a
				// ticket — proven by the committed TicketID.
				if res.State.TicketID == "" {
					t.Errorf("TicketID empty, want it set (the error-route handler vertex ran)")
				}
				// The handler routes on to awaitExpert, which interrupts, so the
				// error-routed run pauses there.
				if res.Run.Status != flow.RunInterrupted {
					t.Fatalf("Status = %v, want RunInterrupted (awaitExpert paused)", res.Run.Status)
				}
				if len(res.Interrupts) != 1 || res.Interrupts[0].Vertex != awaitExpertID {
					t.Fatalf("Interrupts = %v, want one on awaitExpert", res.Interrupts)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}

// --- Scenario 5: instrumentation --------------------------------------------

// recorder collects the hook events fired during a run so the test can assert
// their ordering, identity, and timestamps. It is guarded by a mutex because the
// engine runs vertices in parallel goroutines and fires hooks from them.
type recorder struct {
	mu sync.Mutex

	runStart, runFinish int
	checkpoints         int
	vertexStart         []flow.VertexState
	vertexFinish        []flow.VertexState
	edges               [][2]flow.VertexID
}

func (r *recorder) hooks() flow.Hooks {
	return flow.Hooks{
		OnRunStart:  func(_ context.Context, _ flow.GraphRunState) { r.mu.Lock(); r.runStart++; r.mu.Unlock() },
		OnRunFinish: func(_ context.Context, _ flow.GraphRunState) { r.mu.Lock(); r.runFinish++; r.mu.Unlock() },
		OnVertexStart: func(_ context.Context, ev flow.VertexState) {
			r.mu.Lock()
			r.vertexStart = append(r.vertexStart, ev)
			r.mu.Unlock()
		},
		OnVertexFinish: func(_ context.Context, ev flow.VertexState) {
			r.mu.Lock()
			r.vertexFinish = append(r.vertexFinish, ev)
			r.mu.Unlock()
		},
		OnEdge: func(_ context.Context, from, to flow.VertexID, _ flow.GraphRunState) {
			r.mu.Lock()
			r.edges = append(r.edges, [2]flow.VertexID{from, to})
			r.mu.Unlock()
		},
		OnCheckpoint: func(_ context.Context, _ flow.GraphRunID, _ uint64, _ flow.StepID) {
			r.mu.Lock()
			r.checkpoints++
			r.mu.Unlock()
		},
	}
}

// snapshot copies the scalar counters and the vertex-event slices under the lock
// so callers can assert without holding it (and without racing late hooks). The
// run has returned by the time tests call it, so the data is already quiescent;
// the lock is belt-and-suspenders for the race detector.
func (r *recorder) snapshot() (runStart, runFinish, checkpoints int, vStart, vFinish []flow.VertexState) {
	r.mu.Lock()
	defer r.mu.Unlock()
	vStart = append(vStart, r.vertexStart...)
	vFinish = append(vFinish, r.vertexFinish...)
	return r.runStart, r.runFinish, r.checkpoints, vStart, vFinish
}

func (r *recorder) sawEdge(from, to flow.VertexID) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, e := range r.edges {
		if e[0] == from && e[1] == to {
			return true
		}
	}
	return false
}

// TestExampleInstrumentation registers Hooks and asserts the full observability
// contract over a happy-path run: OnRunStart/OnRunFinish bracket the run exactly
// once each; OnVertexStart/OnVertexFinish fire per vertex with non-zero
// timestamps; OnEdge fires for traversed edges including the conditional pick
// (review → sendToSales); and OnCheckpoint fires (append-only durability is
// observed). All records carry sane ids/timestamps.
func TestExampleInstrumentation(t *testing.T) {
	t.Parallel()

	var rec recorder
	store := flow.NewMemStore()
	g := buildGraph(t, ragDraft(), awaitExpertBody())
	runner, err := g.Compile(intakeID, sendToSalesID, flow.WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	res, err := runner.Run(context.Background(), Request{
		Question: "how is data encrypted?",
		Approved: true,
	}, flow.WithHooks(rec.hooks()))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Run.Status != flow.RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}

	// Run has returned, so all hooks have fired and the recorder is stable. Snapshot
	// under the lock once, then assert on the copy WITHOUT holding the lock (sawEdge
	// takes its own lock — holding rec.mu here would self-deadlock).
	runStart, runFinish, checkpoints, vStart, vFinish := rec.snapshot()

	if runStart != 1 {
		t.Errorf("OnRunStart fired %d times, want 1", runStart)
	}
	if runFinish != 1 {
		t.Errorf("OnRunFinish fired %d times, want 1", runFinish)
	}
	if checkpoints == 0 {
		t.Errorf("OnCheckpoint never fired, want >= 1 (durable checkpoints)")
	}

	// Every vertex that started also finished, with non-zero timestamps.
	if len(vStart) == 0 {
		t.Fatalf("no OnVertexStart events")
	}
	if len(vStart) != len(vFinish) {
		t.Errorf("vertexStart=%d vertexFinish=%d, want equal (each start brackets a finish)",
			len(vStart), len(vFinish))
	}
	for _, ev := range vStart {
		if ev.StartedAt.IsZero() {
			t.Errorf("vertex %s OnVertexStart has zero StartedAt", ev.VertexID)
		}
		if (ev.VertexRunID == flow.VertexRunID{}) {
			t.Errorf("vertex %s OnVertexStart has zero VertexRunID", ev.VertexID)
		}
	}
	for _, ev := range vFinish {
		if ev.Status != flow.VertexDone {
			t.Errorf("vertex %s finished with status %v, want VertexDone", ev.VertexID, ev.Status)
		}
		if ev.CompletedAt.IsZero() {
			t.Errorf("vertex %s OnVertexFinish has zero CompletedAt", ev.VertexID)
		}
	}

	// Edges traversed: the static intake→draft→review chain and the conditional
	// pick review→sendToSales must all be observed.
	for _, want := range [][2]flow.VertexID{
		{intakeID, draftID},
		{draftID, reviewID},
		{reviewID, sendToSalesID},
	} {
		if !rec.sawEdge(want[0], want[1]) {
			t.Errorf("OnEdge did not fire for %s→%s", want[0], want[1])
		}
	}
}

// --- Runnable godoc example -------------------------------------------------

// ExampleRunner_Run is the godoc-facing, verified-by-output rendering of the §16
// happy path: build the graph, compile to a Runner with an in-memory store, run
// it, and read the final state. The // Output line is checked by `go test`, so
// this doubles as a compile-and-behavior guarantee for the snippet a reader
// copies from the docs. (The Test* functions above carry the exhaustive
// assertions; this is the readable one-screen tour.)
func ExampleRunner_Run() {
	// buildGraph takes *testing.T for its fail-fast helpers, which a godoc example
	// (no T) cannot supply. So this example is a thin, self-contained mirror of the
	// happy path, built directly with the same exported API and checking errors
	// inline instead.
	g := flow.NewGraph[Request](exGraphID)
	_ = flow.AddVertex(g, intakeID, ragDraft(),
		func(s Request) query { return query{Text: s.Question} },
		func(s *Request, a answer) error { s.Draft = a.Text; s.Approved = true; return nil },
	)
	_ = flow.AddVertex(g, sendToSalesID,
		flow.NewFuncTask(func(_ context.Context, draft string) (bool, error) { return draft != "", nil }),
		func(s Request) string { return s.Draft },
		func(s *Request, ok bool) error { s.Sent = ok; return nil },
	)
	_ = g.AddEdge(intakeID, sendToSalesID)

	runner, err := g.Compile(intakeID, sendToSalesID, flow.WithStore(flow.NewMemStore()))
	if err != nil {
		panic(err)
	}
	res, err := runner.Run(context.Background(), Request{Question: "what is the SLA?"})
	if err != nil {
		panic(err)
	}

	fmt.Println(res.Run.Status)
	fmt.Println(res.State.Draft)
	fmt.Println(res.State.Sent)
	// Output:
	// Completed
	// draft answer for: what is the SLA?
	// true
}
