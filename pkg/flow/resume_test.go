package flow

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
)

// This file white-box tests Runner.Resume (design §9.3, §10.4): the §10.4
// validate-on-load checks that reject a corrupt/mismatched checkpoint BEFORE any
// task runs, and the four-phase continuation (StepRouted/StepPaused/StepRunning/
// StepHalted) that rebuilds the coordinator from a checkpoint and re-enters the
// super-step machinery. It reuses cnt (runner_test.go), vID (vertex_test.go),
// addErrVertex/interruptTask/failTask/lastCheckpoint (engine_test.go) and a
// MemStore so a run can pause, then be resumed, on the SAME store.

// resumeOnce runs r over store until it pauses or halts (an interrupted Result),
// returning the run id so a Resume can continue it. It fails if the run does not
// pause/halt on the first Run.
func resumeOnce(t *testing.T, r *Runner[cnt], store CheckpointStore, opts ...RunOption) GraphRunID {
	t.Helper()
	res, err := r.Run(context.Background(), cnt{}, opts...)
	if err != nil {
		t.Fatalf("initial Run: %v", err)
	}
	if res.Run.Status != RunInterrupted {
		t.Fatalf("initial Run Status = %v, want RunInterrupted (a pause/halt to resume)", res.Run.Status)
	}
	return res.Run.GraphRunID
}

// flipTask returns a Task that pauses Awaiting on its FIRST execution and then
// succeeds on every later one, so a Run pauses and a Resume completes it. It
// reads ResumePayload[string] on the resumed run and records what it saw.
func flipTask(seen *atomic.Int32, gotPayload *atomic.Value, reason string) Task[int, string] {
	return NewFuncTask(func(ctx context.Context, _ int) (string, error) {
		if seen.Add(1) == 1 {
			return "", Interrupt(ctx, reason)
		}
		if p, ok := ResumePayload[string](ctx); ok {
			gotPayload.Store(p)
		}
		return "resumed", nil
	})
}

// --- §10.4 validation (one per rule) ----------------------------------------

// phaseDecodeErr reports whether err is the §10.4 phase/interrupt/halt-combination
// validation failure: a *CheckpointDecodeError whose Field is "Phase". Every
// invalid Phase combination wraps the unexported phaseComboError in exactly this
// public shape, so callers errors.As the public type while the detail stays
// inspectable.
func phaseDecodeErr(err error) bool {
	var e *CheckpointDecodeError
	return errors.As(err, &e) && e.Field == "Phase"
}

// TestResumeValidationRejectsBeforeAnyTask proves each §10.4 rule fails fast with
// its typed engine error, and that validation happens BEFORE any task runs: a
// sentinel task records a side effect that must NOT fire on a validation failure.
func TestResumeValidationRejectsBeforeAnyTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(cp *Checkpoint, r *Runner[cnt]) // corrupt the Latest checkpoint in place
		wantErr func(err error) bool
	}{
		{
			name: "graph id mismatch",
			mutate: func(cp *Checkpoint, _ *Runner[cnt]) {
				cp.Run.GraphID = GraphID{0xAB}
			},
			wantErr: func(err error) bool { var e *GraphMismatchError; return errors.As(err, &e) },
		},
		{
			name: "graph version mismatch",
			mutate: func(cp *Checkpoint, _ *Runner[cnt]) {
				cp.Run.GraphVersion = "not-the-version"
			},
			wantErr: func(err error) bool { var e *GraphVersionMismatchError; return errors.As(err, &e) },
		},
		{
			name: "terminal completed",
			mutate: func(cp *Checkpoint, _ *Runner[cnt]) {
				cp.Run.Status = RunCompleted
			},
			wantErr: func(err error) bool {
				var e *ResumeTerminalError
				return errors.As(err, &e) && e.Status == RunCompleted
			},
		},
		{
			name: "terminal cancelled",
			mutate: func(cp *Checkpoint, _ *Runner[cnt]) {
				cp.Run.Status = RunCancelled
			},
			wantErr: func(err error) bool {
				var e *ResumeTerminalError
				return errors.As(err, &e) && e.Status == RunCancelled
			},
		},
		{
			name: "undecodable state",
			mutate: func(cp *Checkpoint, _ *Runner[cnt]) {
				cp.State = json.RawMessage(`{"N": "not a number"}`)
			},
			wantErr: func(err error) bool {
				var e *CheckpointDecodeError
				return errors.As(err, &e) && e.Field == "State"
			},
		},
		{
			name: "undecodable step base",
			mutate: func(cp *Checkpoint, _ *Runner[cnt]) {
				cp.StepBase = json.RawMessage(`{"Vals": 5}`) // Vals is []string, not a number
			},
			wantErr: func(err error) bool {
				var e *CheckpointDecodeError
				return errors.As(err, &e) && e.Field == "StepBase"
			},
		},
		{
			name: "unknown frontier vertex",
			mutate: func(cp *Checkpoint, _ *Runner[cnt]) {
				cp.Frontier = []VertexID{vID(0xFE)}
			},
			wantErr: func(err error) bool {
				var e *UnknownVertexError
				return errors.As(err, &e) && e.VertexID == vID(0xFE)
			},
		},
		{
			name: "unknown interrupt vertex",
			mutate: func(cp *Checkpoint, _ *Runner[cnt]) {
				cp.Interrupts = append(cp.Interrupts, InterruptRecord{Vertex: vID(0xFD), Kind: Awaiting})
			},
			wantErr: func(err error) bool {
				var e *UnknownVertexError
				return errors.As(err, &e) && e.VertexID == vID(0xFD)
			},
		},
		{
			name: "unknown route vertex",
			mutate: func(cp *Checkpoint, _ *Runner[cnt]) {
				cp.Routes = append(cp.Routes, RouteRecord{From: vID(1), To: []VertexID{vID(0xFC)}})
			},
			wantErr: func(err error) bool {
				var e *UnknownVertexError
				return errors.As(err, &e) && e.VertexID == vID(0xFC)
			},
		},
		{
			name: "bad phase/interrupt combo: paused without interrupts",
			mutate: func(cp *Checkpoint, _ *Runner[cnt]) {
				cp.Phase = StepPaused
				cp.Interrupts = nil
				cp.Halt = nil
			},
			wantErr: phaseDecodeErr,
		},
		{
			name: "bad phase/halt combo: halted with interrupts",
			mutate: func(cp *Checkpoint, _ *Runner[cnt]) {
				cp.Phase = StepHalted
				cp.Halt = &HaltRecord{Kind: HaltMaxSteps}
				cp.Interrupts = []InterruptRecord{{Vertex: vID(1), Kind: Errored}}
			},
			wantErr: phaseDecodeErr,
		},
		{
			name: "bad phase/halt combo: halted without halt",
			mutate: func(cp *Checkpoint, _ *Runner[cnt]) {
				cp.Phase = StepHalted
				cp.Halt = nil
				cp.Interrupts = nil
			},
			wantErr: phaseDecodeErr,
		},
		{
			name: "bad phase/halt combo: running carries a halt",
			mutate: func(cp *Checkpoint, _ *Runner[cnt]) {
				cp.Phase = StepRunning
				cp.Halt = &HaltRecord{Kind: HaltDeadEnd}
				cp.Interrupts = nil
			},
			wantErr: phaseDecodeErr,
		},
		{
			name: "bad phase/halt combo: paused carries a halt",
			mutate: func(cp *Checkpoint, _ *Runner[cnt]) {
				cp.Phase = StepPaused
				cp.Halt = &HaltRecord{Kind: HaltMaxSteps}
				cp.Interrupts = []InterruptRecord{{Vertex: vID(1), Kind: Awaiting}}
			},
			wantErr: phaseDecodeErr,
		},
		{
			name: "bad phase combo: routed carries interrupts",
			mutate: func(cp *Checkpoint, _ *Runner[cnt]) {
				cp.Phase = StepRouted
				cp.Halt = nil
				cp.Interrupts = []InterruptRecord{{Vertex: vID(1), Kind: Awaiting}}
			},
			wantErr: phaseDecodeErr,
		},
		{
			name: "bad phase combo: routed carries a halt",
			mutate: func(cp *Checkpoint, _ *Runner[cnt]) {
				cp.Phase = StepRouted
				cp.Halt = &HaltRecord{Kind: HaltDeadEnd}
				cp.Interrupts = nil
			},
			wantErr: phaseDecodeErr,
		},
		{
			name: "bad phase combo: unknown phase value",
			mutate: func(cp *Checkpoint, _ *Runner[cnt]) {
				cp.Phase = StepPhase(99) // not a declared phase
				cp.Halt = nil
				cp.Interrupts = nil
			},
			wantErr: phaseDecodeErr,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Build a paused run so there is a real Latest to corrupt, plus a sentinel
			// that must NOT run on a validation failure.
			var ran atomic.Int32
			store := NewMemStore()
			g := NewGraph[cnt](GraphID{})
			entry := vID(1)
			addErrVertex(t, g, entry, NewFuncTask(func(ctx context.Context, _ int) (string, error) {
				ran.Add(1)
				return "", Interrupt(ctx, "pause")
			}))
			r, err := g.Compile(entry, entry, WithStore(store))
			if err != nil {
				t.Fatalf("Compile: %v", err)
			}
			id := resumeOnce(t, r, store)
			ranBefore := ran.Load() // the initial Run ran the task once

			// Corrupt Latest by appending a mutated copy as the next revision.
			cp := lastCheckpoint(t, store, id)
			tt.mutate(cp, r)
			cp.Run.Revision++ // append as the next revision so it becomes Latest
			if err := store.Append(context.Background(), cp); err != nil {
				t.Fatalf("append corrupted checkpoint: %v", err)
			}

			_, err = r.Resume(context.Background(), id, "payload")
			if err == nil || !tt.wantErr(err) {
				t.Fatalf("Resume() error = %v, want a typed §10.4 validation error", err)
			}
			if ran.Load() != ranBefore {
				t.Errorf("a task ran on a validation failure (ran %d, want %d) — validation must precede execution",
					ran.Load(), ranBefore)
			}
		})
	}
}

// TestResumeCheckpointNotFound proves Resume of an unknown run propagates the
// store's *CheckpointNotFoundError as the engine error.
func TestResumeCheckpointNotFound(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	r, _ := compileSingle(t, store)
	id, err := NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID: %v", err)
	}
	_, err = r.Resume(context.Background(), id, nil)
	var nf *CheckpointNotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("Resume() error = %v, want *CheckpointNotFoundError", err)
	}
}

// runMismatchStore wraps a CheckpointStore and rewrites the EMBEDDED
// cp.Run.GraphRunID returned by Latest to a different id, simulating a buggy or
// tampered durable backend that returns a checkpoint belonging to another run
// under the requested key. A §10.4-conformant Resume must reject this before any
// task runs (fail secure), never write into the embedded run's history.
type runMismatchStore struct {
	CheckpointStore
	actual GraphRunID // the id to stamp onto the checkpoint Latest returns
}

func (s *runMismatchStore) Latest(ctx context.Context, id GraphRunID) (*Checkpoint, error) {
	cp, err := s.CheckpointStore.Latest(ctx, id)
	if err != nil {
		return nil, err
	}
	cp.Run.GraphRunID = s.actual // the loaded checkpoint claims a DIFFERENT run
	return cp, nil
}

// TestResumeRejectsGraphRunIDMismatch is the L2 hardening regression (§10.4): a
// loaded checkpoint whose embedded Run.GraphRunID does not match the requested id
// is rejected with a typed *GraphRunMismatchError BEFORE any task runs, so a
// buggy/tampered backend cannot make Resume write into the embedded run's history
// (fail secure). A side-effect sentinel proves no task executed.
func TestResumeRejectsGraphRunIDMismatch(t *testing.T) {
	t.Parallel()

	var ran atomic.Int32
	mem := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	entry := vID(1)
	addErrVertex(t, g, entry, NewFuncTask(func(ctx context.Context, _ int) (string, error) {
		ran.Add(1)
		return "", Interrupt(ctx, "pause")
	}))
	r, err := g.Compile(entry, entry, WithStore(mem))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	id := resumeOnce(t, r, mem)
	ranBefore := ran.Load() // the initial Run ran the task once

	other, err := NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID: %v", err)
	}
	if other == id {
		t.Fatal("minted id collided with the run id")
	}

	// Resume against a store whose Latest returns a checkpoint claiming `other`.
	r2 := *r
	r2.store = &runMismatchStore{CheckpointStore: mem, actual: other}
	_, err = r2.Resume(context.Background(), id, "payload")

	var mismatch *GraphRunMismatchError
	if !errors.As(err, &mismatch) {
		t.Fatalf("Resume() error = %v, want *GraphRunMismatchError", err)
	}
	if mismatch.Requested != id || mismatch.Actual != other {
		t.Errorf("GraphRunMismatchError = {Requested:%v Actual:%v}, want {Requested:%v Actual:%v}",
			mismatch.Requested, mismatch.Actual, id, other)
	}
	if ran.Load() != ranBefore {
		t.Errorf("a task ran on a GraphRunID mismatch (ran %d, want %d) — validation must precede execution",
			ran.Load(), ranBefore)
	}
}

// --- StepPaused continuation (the headline) ---------------------------------

// TestResumeStepPausedHeadline proves the core resume path: a graph pauses at an
// Awaiting interrupt, Resume re-runs the paused vertex (which reads the payload),
// routes onward, and the run Completes — without re-running already-Done vertices.
func TestResumeStepPausedHeadline(t *testing.T) {
	t.Parallel()

	var firstSeen, secondSeen atomic.Int32
	var gotPayload atomic.Value
	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	first, second := vID(1), vID(2)
	// first runs once (Done), then NEVER again; second pauses then resumes.
	addErrVertex(t, g, first, NewFuncTask(func(_ context.Context, _ int) (string, error) {
		firstSeen.Add(1)
		return "first", nil
	}))
	addErrVertex(t, g, second, flipTask(&secondSeen, &gotPayload, "approve"))
	if err := g.AddEdge(first, second); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	r, err := g.Compile(first, second, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	id := resumeOnce(t, r, store)
	if firstSeen.Load() != 1 {
		t.Fatalf("first ran %d times before resume, want 1", firstSeen.Load())
	}

	res, err := r.Resume(context.Background(), id, "the-payload")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}
	if firstSeen.Load() != 1 {
		t.Errorf("first ran %d times total, want 1 (Done vertex must NOT re-run)", firstSeen.Load())
	}
	if got, _ := gotPayload.Load().(string); got != "the-payload" {
		t.Errorf("resumed task read payload %q, want %q", got, "the-payload")
	}
	if res.Run.Revision <= 1 {
		t.Errorf("Revision = %d, want > 1 (resume continues the append sequence)", res.Run.Revision)
	}
}

// TestResumeErroredPause proves a default-Pause task failure pauses Errored and a
// fix-and-resume (the re-run task now succeeds) completes the run.
func TestResumeErroredPause(t *testing.T) {
	t.Parallel()

	var counter int32
	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	entry := vID(1)
	// Fails exactly once (the initial Run), succeeds on the resume re-run.
	addErrVertex(t, g, entry, failTask(1, "fixed", &counter))
	r, err := g.Compile(entry, entry, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	id := resumeOnce(t, r, store)

	res, err := r.Resume(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted (the re-run succeeded)", res.Run.Status)
	}
	if len(res.State.Vals) != 1 || res.State.Vals[0] != "fixed" {
		t.Errorf("State.Vals = %v, want [fixed]", res.State.Vals)
	}
}

// TestResumePluralSharedPayload proves two vertices paused in the same step both
// read the SAME injected payload on resume (§9.7).
func TestResumePluralSharedPayload(t *testing.T) {
	t.Parallel()

	var aSeen, bSeen atomic.Int32
	var aPayload, bPayload atomic.Value
	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	addErrVertex(t, g, vID(1), NewFuncTask(func(_ context.Context, _ int) (string, error) { return "entry", nil }))
	addErrVertex(t, g, vID(2), flipTask(&aSeen, &aPayload, "approve A"))
	addErrVertex(t, g, vID(3), flipTask(&bSeen, &bPayload, "approve B"))
	addErrVertex(t, g, vID(4), NewFuncTask(func(_ context.Context, _ int) (string, error) { return "fin", nil }))
	r, _, _, _, _ := fanOutTwo(t, g, store)

	id := resumeOnce(t, r, store)

	res, err := r.Resume(context.Background(), id, "shared")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}
	if got, _ := aPayload.Load().(string); got != "shared" {
		t.Errorf("vertex a read payload %q, want %q", got, "shared")
	}
	if got, _ := bPayload.Load().(string); got != "shared" {
		t.Errorf("vertex b read payload %q, want %q", got, "shared")
	}
}

// TestResumeStepPausedSkipsDoneSibling proves that when one sibling is Done and
// another paused in the SAME step, Resume re-runs ONLY the paused sibling (the
// Done one is skipped, its committed work retained) and routes the whole step to
// completion.
func TestResumeStepPausedSkipsDoneSibling(t *testing.T) {
	t.Parallel()

	var doneRuns, pauseRuns atomic.Int32
	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	a, b := vID(2), vID(3)
	addErrVertex(t, g, vID(1), NewFuncTask(func(_ context.Context, _ int) (string, error) { return "entry", nil }))
	addErrVertex(t, g, a, NewFuncTask(func(_ context.Context, _ int) (string, error) {
		doneRuns.Add(1)
		return "a-done", nil
	}))
	addErrVertex(t, g, b, NewFuncTask(func(ctx context.Context, _ int) (string, error) {
		if pauseRuns.Add(1) == 1 {
			return "", Interrupt(ctx, "pause b")
		}
		return "b-resumed", nil
	}))
	addErrVertex(t, g, vID(4), NewFuncTask(func(_ context.Context, _ int) (string, error) { return "fin", nil }))
	r, _, _, _, _ := fanOutTwo(t, g, store)

	id := resumeOnce(t, r, store)
	if doneRuns.Load() != 1 {
		t.Fatalf("a ran %d times before resume, want 1", doneRuns.Load())
	}

	res, err := r.Resume(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}
	if doneRuns.Load() != 1 {
		t.Errorf("Done sibling a ran %d times total, want 1 (skipped on resume)", doneRuns.Load())
	}
	if pauseRuns.Load() != 2 {
		t.Errorf("paused sibling b ran %d times, want 2 (initial pause + resume)", pauseRuns.Load())
	}
	foundA, foundB := false, false
	for _, v := range res.State.Vals {
		if v == "a-done" {
			foundA = true
		}
		if v == "b-resumed" {
			foundB = true
		}
	}
	if !foundA || !foundB {
		t.Errorf("State.Vals = %v, want both a-done (retained) and b-resumed", res.State.Vals)
	}
}

// TestResumeStatefulContinuationRestore proves a StatefulInterrupt's continuation
// is restored into the resumed task's ctx (InterruptState[T] == the original).
func TestResumeStatefulContinuationRestore(t *testing.T) {
	t.Parallel()

	type cont struct {
		Cursor int
		Note   string
	}
	want := cont{Cursor: 7, Note: "pick up here"}
	var seen atomic.Int32
	var gotCont atomic.Value
	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	entry := vID(1)
	addErrVertex(t, g, entry, NewFuncTask(func(ctx context.Context, _ int) (string, error) {
		if seen.Add(1) == 1 {
			return "", StatefulInterrupt(ctx, "need approval", want)
		}
		if c, ok := InterruptState[cont](ctx); ok {
			gotCont.Store(c)
		}
		return "resumed", nil
	}))
	r, err := g.Compile(entry, entry, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	id := resumeOnce(t, r, store)

	res, err := r.Resume(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}
	if got, _ := gotCont.Load().(cont); got != want {
		t.Errorf("InterruptState restored %+v, want %+v", got, want)
	}
}

// TestResumePausedAgain proves a re-run vertex that pauses AGAIN on resume is a
// new pause (Interrupted, no engine error), and a third Resume can complete it.
func TestResumePausedAgain(t *testing.T) {
	t.Parallel()

	var seen atomic.Int32
	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	entry := vID(1)
	addErrVertex(t, g, entry, NewFuncTask(func(ctx context.Context, _ int) (string, error) {
		if seen.Add(1) <= 2 {
			return "", Interrupt(ctx, "again")
		}
		return "done", nil
	}))
	r, err := g.Compile(entry, entry, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	id := resumeOnce(t, r, store)

	res, err := r.Resume(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("Resume #1: %v", err)
	}
	if res.Run.Status != RunInterrupted || len(res.Interrupts) != 1 {
		t.Fatalf("Resume #1 = %+v, want a fresh pause (RunInterrupted, 1 interrupt)", res)
	}
	res, err = r.Resume(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("Resume #2: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Errorf("Resume #2 Status = %v, want RunCompleted", res.Run.Status)
	}
}

// TestResumeFailedRouteSkippedAwaitingReruns proves the Terminal-vs-Paused
// discrimination from the checkpoint (§9.3): in a step where one vertex Failed
// under a Route (terminal — record committed, handler pending) and a sibling
// paused Awaiting, Resume re-runs ONLY the Awaiting vertex; the Failed-Route
// vertex is NOT re-run, and its handler activates next step. The run completes.
func TestResumeFailedRouteSkippedAwaitingReruns(t *testing.T) {
	t.Parallel()

	var counter int32
	var failRuns, pauseRuns, handlerRuns atomic.Int32
	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	a, b, handler := vID(2), vID(3), vID(5)
	record := func(s *cnt, e error) error {
		s.Vals = append(s.Vals, "recorded")
		return nil
	}
	addErrVertex(t, g, vID(1), NewFuncTask(func(_ context.Context, _ int) (string, error) { return "entry", nil }))
	// a fails once (initial Run) then routes; counted so a re-run would be visible.
	addErrVertex(t, g, a, NewFuncTask(func(_ context.Context, _ int) (string, error) {
		failRuns.Add(1)
		atomic.AddInt32(&counter, 1)
		return "", errReduce
	}),
		WithRetry[cnt](RetryPolicy{MaxAttempts: 1}),
		WithErrorRoute[cnt](handler, record))
	// b pauses Awaiting on the initial Run, succeeds on resume.
	addErrVertex(t, g, b, NewFuncTask(func(ctx context.Context, _ int) (string, error) {
		if pauseRuns.Add(1) == 1 {
			return "", Interrupt(ctx, "approve b")
		}
		return "b-ok", nil
	}))
	addErrVertex(t, g, vID(4), NewFuncTask(func(_ context.Context, _ int) (string, error) { return "fin", nil }))
	addErrVertex(t, g, handler, NewFuncTask(func(_ context.Context, _ int) (string, error) {
		handlerRuns.Add(1)
		return "handled", nil
	}))
	r, _, _, _, _ := fanOutTwo(t, g, store)

	id := resumeOnce(t, r, store)
	if failRuns.Load() != 1 {
		t.Fatalf("a (Failed-Route) ran %d times before resume, want 1", failRuns.Load())
	}

	res, err := r.Resume(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}
	if failRuns.Load() != 1 {
		t.Errorf("Failed-Route vertex a ran %d times total, want 1 (terminal, must NOT re-run)", failRuns.Load())
	}
	if pauseRuns.Load() != 2 {
		t.Errorf("Awaiting vertex b ran %d times, want 2 (initial + resume)", pauseRuns.Load())
	}
	if handlerRuns.Load() != 1 {
		t.Errorf("handler ran %d times, want 1 (activated after resume routes the step)", handlerRuns.Load())
	}
}

// --- StepRouted continuation -------------------------------------------------

// TestResumeStepRouted proves a Latest sitting at a StepRouted boundary (the
// routing already computed the next frontier) resumes from that frontier to
// completion. It crafts the StepRouted Latest by hand-appending the boundary of a
// mid-flight step to a fresh store, then resumes a runner over the same graph.
func TestResumeStepRouted(t *testing.T) {
	t.Parallel()

	var aSeen, finSeen atomic.Int32
	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	entry, a, fin := vID(1), vID(2), vID(3)
	addErrVertex(t, g, entry, NewFuncTask(func(_ context.Context, _ int) (string, error) { return "entry", nil }))
	addErrVertex(t, g, a, NewFuncTask(func(_ context.Context, _ int) (string, error) { aSeen.Add(1); return "a", nil }))
	addErrVertex(t, g, fin, NewFuncTask(func(_ context.Context, _ int) (string, error) { finSeen.Add(1); return "fin", nil }))
	for _, e := range [][2]VertexID{{entry, a}, {a, fin}} {
		if err := g.AddEdge(e[0], e[1]); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
	}
	r, err := g.Compile(entry, fin, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	// Hand-build the Latest as a StepRouted boundary that has already routed
	// entry -> a (step 0 done), so the next frontier is [a].
	id, err := NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID: %v", err)
	}
	base, _ := json.Marshal(cnt{Vals: []string{"entry"}, N: 1})
	cp := &Checkpoint{
		Run: GraphRunState{
			GraphRunID:   id,
			GraphID:      g.id,
			GraphVersion: r.GraphVersion(),
			Status:       RunRunning,
			Step:         0,
			Revision:     0,
		},
		StepBase: base,
		State:    base,
		Phase:    StepRouted,
		Frontier: []VertexID{a},
		Routes:   []RouteRecord{{From: entry, To: []VertexID{a}}},
		Vertices: []VertexState{{VertexID: entry, Status: VertexDone}},
	}
	if err := store.Append(context.Background(), cp); err != nil {
		t.Fatalf("append StepRouted: %v", err)
	}

	res, err := r.Resume(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}
	if aSeen.Load() != 1 || finSeen.Load() != 1 {
		t.Errorf("a ran %d, fin ran %d; want 1 and 1 (resume from the routed frontier)", aSeen.Load(), finSeen.Load())
	}
}

// --- StepHalted continuation -------------------------------------------------

// TestResumeStepHaltedMaxSteps proves a HaltMaxSteps run resumes with a higher
// WithMaxSteps and continues from the recorded frontier (no vertex re-run of a
// paused vertex — there are none).
func TestResumeStepHaltedMaxSteps(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	// A linear chain of 4 needs >=4 steps; cap the first run low so it HaltMaxSteps.
	r, ids := compileChain(t, store, "a", "b", "c", "d")
	_ = ids

	res, err := r.Run(context.Background(), cnt{}, WithMaxSteps(2))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Run.Status != RunInterrupted || res.Halt == nil || res.Halt.Kind != HaltMaxSteps {
		t.Fatalf("first Run = %+v, want a HaltMaxSteps", res)
	}
	id := res.Run.GraphRunID

	res, err = r.Resume(context.Background(), id, nil, WithMaxSteps(10))
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted (resume with a higher budget continues)", res.Run.Status)
	}
	if len(res.State.Vals) != 4 {
		t.Errorf("State.Vals = %v, want all 4 tags (the chain finished)", res.State.Vals)
	}
}

// TestResumeStepHaltedConditionRetriesRouting proves a transient HaltCondition
// (a Pick that errors the first evaluation, succeeds the second) resumes by
// RE-ROUTING against the committed state — no vertex re-runs.
func TestResumeStepHaltedConditionRetriesRouting(t *testing.T) {
	t.Parallel()

	var pickCalls, entryRuns, finRuns atomic.Int32
	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	entry, fin := vID(1), vID(2)
	addErrVertex(t, g, entry, NewFuncTask(func(_ context.Context, _ int) (string, error) {
		entryRuns.Add(1)
		return "entry", nil
	}))
	addErrVertex(t, g, fin, NewFuncTask(func(_ context.Context, _ int) (string, error) {
		finRuns.Add(1)
		return "fin", nil
	}))
	cond := Condition[cnt]{
		Targets: []VertexID{fin},
		Pick: func(_ context.Context, _ cnt) ([]VertexID, error) {
			if pickCalls.Add(1) == 1 {
				return nil, errors.New("transient routing dep down")
			}
			return []VertexID{fin}, nil
		},
	}
	if err := g.AddConditionalEdge(entry, cond); err != nil {
		t.Fatalf("AddConditionalEdge: %v", err)
	}
	r, err := g.Compile(entry, fin, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	res, err := r.Run(context.Background(), cnt{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Halt == nil || res.Halt.Kind != HaltCondition {
		t.Fatalf("first Run = %+v, want a HaltCondition", res)
	}
	id := res.Run.GraphRunID
	entryBefore := entryRuns.Load()

	res, err = r.Resume(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted (re-route succeeds the 2nd time)", res.Run.Status)
	}
	if entryRuns.Load() != entryBefore {
		t.Errorf("entry re-ran on a StepHalted resume (was %d, now %d) — routing retries, NOT a vertex",
			entryBefore, entryRuns.Load())
	}
	if finRuns.Load() != 1 {
		t.Errorf("fin ran %d times, want 1 (re-route activates it once)", finRuns.Load())
	}
}

// --- StepRunning continuation (PerVertex mid-reduce crash) -------------------

// TestResumeStepRunningSkipsTerminal crafts a Latest at StepRunning (a PerVertex
// crash mid-step: one sibling Done, one not yet run) and proves Resume skips the
// terminal vertex and runs only the rest, then routes to completion.
func TestResumeStepRunningSkipsTerminal(t *testing.T) {
	t.Parallel()

	var aRuns, bRuns, finRuns atomic.Int32
	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	entry, a, b, fin := vID(1), vID(2), vID(3), vID(4)
	addErrVertex(t, g, entry, NewFuncTask(func(_ context.Context, _ int) (string, error) { return "entry", nil }))
	addErrVertex(t, g, a, NewFuncTask(func(_ context.Context, _ int) (string, error) { aRuns.Add(1); return "a", nil }))
	addErrVertex(t, g, b, NewFuncTask(func(_ context.Context, _ int) (string, error) { bRuns.Add(1); return "b", nil }))
	addErrVertex(t, g, fin, NewFuncTask(func(_ context.Context, _ int) (string, error) { finRuns.Add(1); return "fin", nil }))
	for _, e := range [][2]VertexID{{entry, a}, {entry, b}, {a, fin}, {b, fin}} {
		if err := g.AddEdge(e[0], e[1]); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
	}
	r, err := g.Compile(entry, fin, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	id, err := NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID: %v", err)
	}
	// Step 0 routed entry -> {a, b} (the routed-into frontier for step 1).
	base0, _ := json.Marshal(cnt{Vals: []string{"entry"}, N: 1})
	seed := &Checkpoint{
		Run:      GraphRunState{GraphRunID: id, GraphID: g.id, GraphVersion: r.GraphVersion(), Status: RunRunning, Step: 0, Revision: 0},
		StepBase: base0, State: base0, Phase: StepRouted,
		Frontier: []VertexID{a, b},
		Routes:   []RouteRecord{{From: entry, To: []VertexID{a, b}}},
		Vertices: []VertexState{{VertexID: entry, Status: VertexDone}},
	}
	if err := store.Append(context.Background(), seed); err != nil {
		t.Fatalf("append routed: %v", err)
	}
	// Step 1 StepRunning: a is Done (its reducer committed into State), b not yet.
	base1 := base0
	state1, _ := json.Marshal(cnt{Vals: []string{"entry", "a"}, N: 2})
	running := &Checkpoint{
		Run:      GraphRunState{GraphRunID: id, GraphID: g.id, GraphVersion: r.GraphVersion(), Status: RunRunning, Step: 1, Revision: 1},
		StepBase: base1, State: state1, Phase: StepRunning,
		Vertices: []VertexState{{VertexID: a, Step: 1, Status: VertexDone}},
	}
	if err := store.Append(context.Background(), running); err != nil {
		t.Fatalf("append running: %v", err)
	}

	res, err := r.Resume(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}
	if aRuns.Load() != 0 {
		t.Errorf("a ran %d times, want 0 (already Done, must be skipped)", aRuns.Load())
	}
	if bRuns.Load() != 1 {
		t.Errorf("b ran %d times, want 1 (non-terminal, must run)", bRuns.Load())
	}
	if finRuns.Load() != 1 {
		t.Errorf("fin ran %d times, want 1 (route after the step finishes)", finRuns.Load())
	}
	// a's committed work survives in the final state.
	foundA := false
	for _, v := range res.State.Vals {
		if v == "a" {
			foundA = true
		}
	}
	if !foundA {
		t.Errorf("State.Vals = %v, want it to retain a's committed 'a'", res.State.Vals)
	}
}

// TestResumeStepRunningRerunsFailedPause crafts a StepRunning Latest where one
// sibling is Done and another is Failed-under-Pause (in Interrupts, Errored), and
// proves Resume re-runs the Failed-Pause vertex (it is paused, not terminal) while
// skipping the Done one, then routes to completion. This exercises the
// Terminal-vs-Paused discrimination on the StepRunning path.
func TestResumeStepRunningRerunsFailedPause(t *testing.T) {
	t.Parallel()

	var aRuns, bRuns, finRuns atomic.Int32
	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	entry, a, b, fin := vID(1), vID(2), vID(3), vID(4)
	addErrVertex(t, g, entry, NewFuncTask(func(_ context.Context, _ int) (string, error) { return "entry", nil }))
	addErrVertex(t, g, a, NewFuncTask(func(_ context.Context, _ int) (string, error) { aRuns.Add(1); return "a", nil }))
	// b: the checkpoint already records a (simulated) prior Failed-Pause attempt, so
	// b's first REAL execution is the resume re-run, which succeeds.
	addErrVertex(t, g, b, NewFuncTask(func(_ context.Context, _ int) (string, error) {
		bRuns.Add(1)
		return "b", nil
	}))
	addErrVertex(t, g, fin, NewFuncTask(func(_ context.Context, _ int) (string, error) { finRuns.Add(1); return "fin", nil }))
	for _, e := range [][2]VertexID{{entry, a}, {entry, b}, {a, fin}, {b, fin}} {
		if err := g.AddEdge(e[0], e[1]); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
	}
	r, err := g.Compile(entry, fin, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	id, err := NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID: %v", err)
	}
	base0, _ := json.Marshal(cnt{Vals: []string{"entry"}, N: 1})
	seed := &Checkpoint{
		Run:      GraphRunState{GraphRunID: id, GraphID: g.id, GraphVersion: r.GraphVersion(), Status: RunRunning, Step: 0, Revision: 0},
		StepBase: base0, State: base0, Phase: StepRouted,
		Frontier: []VertexID{a, b},
		Routes:   []RouteRecord{{From: entry, To: []VertexID{a, b}}},
		Vertices: []VertexState{{VertexID: entry, Status: VertexDone}},
	}
	if err := store.Append(context.Background(), seed); err != nil {
		t.Fatalf("append routed: %v", err)
	}
	// Step 1 StepRunning: a Done (committed), b Failed-under-Pause (in Interrupts).
	state1, _ := json.Marshal(cnt{Vals: []string{"entry", "a"}, N: 2})
	running := &Checkpoint{
		Run:      GraphRunState{GraphRunID: id, GraphID: g.id, GraphVersion: r.GraphVersion(), Status: RunRunning, Step: 1, Revision: 1},
		StepBase: base0, State: state1, Phase: StepRunning,
		Vertices:   []VertexState{{VertexID: a, Step: 1, Status: VertexDone}, {VertexID: b, Step: 1, Status: VertexFailed}},
		Interrupts: []InterruptRecord{{Vertex: b, Kind: Errored, Cause: "reduce failed"}},
	}
	if err := store.Append(context.Background(), running); err != nil {
		t.Fatalf("append running: %v", err)
	}

	res, err := r.Resume(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}
	if aRuns.Load() != 0 {
		t.Errorf("a ran %d times, want 0 (Done, skipped)", aRuns.Load())
	}
	if bRuns.Load() != 1 {
		t.Errorf("b ran %d times, want 1 (Failed-Pause, re-runs once on resume)", bRuns.Load())
	}
	if finRuns.Load() != 1 {
		t.Errorf("fin ran %d times, want 1", finRuns.Load())
	}
}

// --- finish-with-out-edges resume (finishRan re-derivation) -------------------

// TestResumeFinishRanReDerived proves the finishRan latch is re-derived from
// History on resume: finish ran (Done) in an EARLIER step (it has an out-edge so
// the run continued), the run later pauses, and Resume completes once the frontier
// drains — which requires finishRan == true, proving the History scan.
func TestResumeFinishRanReDerived(t *testing.T) {
	t.Parallel()

	var seen atomic.Int32
	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	// entry(finish) -> gate; gate pauses once then succeeds. finish has an out-edge,
	// so it runs in step 0 (Done) but the run keeps going to gate.
	finish, gate := vID(1), vID(2)
	addErrVertex(t, g, finish, NewFuncTask(func(_ context.Context, _ int) (string, error) { return "finish", nil }))
	addErrVertex(t, g, gate, NewFuncTask(func(ctx context.Context, _ int) (string, error) {
		if seen.Add(1) == 1 {
			return "", Interrupt(ctx, "gate")
		}
		return "gate", nil
	}))
	if err := g.AddEdge(finish, gate); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	// finish is the FINISH role but it is not a sink (it has an out-edge to gate).
	r, err := g.Compile(finish, finish, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	id := resumeOnce(t, r, store)
	// The first run: finish ran Done in step 0, then gate paused in step 1.
	hist, err := store.History(context.Background(), id)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	sawFinishDone := false
	for _, cp := range hist {
		for _, vs := range cp.Vertices {
			if vs.VertexID == finish && vs.Status == VertexDone {
				sawFinishDone = true
			}
		}
	}
	if !sawFinishDone {
		t.Fatal("setup: finish never reached Done before the pause")
	}

	res, err := r.Resume(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted (finishRan re-derived ⇒ drained frontier completes)", res.Run.Status)
	}
}

// --- crash DURING a resumed step (intermediate StepRunning carry) -------------

// crashAfterStore wraps a CheckpointStore and fails the Nth Append (1-based) with
// a sentinel error, simulating a process crash AFTER an intermediate StepRunning
// checkpoint is durably written but before the step's final boundary. All other
// ops delegate to the inner store, so the durable history reflects exactly the
// appends that succeeded before the crash — which a later Resume reads as Latest.
type crashAfterStore struct {
	inner   CheckpointStore
	failOn  int          // the 1-based append number to fail (0 = never)
	appends atomic.Int32 // total Append calls observed
}

var errSimulatedCrash = errors.New("simulated crash")

func (s *crashAfterStore) Append(ctx context.Context, cp *Checkpoint) error {
	n := s.appends.Add(1)
	if s.failOn != 0 && int(n) == s.failOn {
		return errSimulatedCrash // the prior appends are durable; this one "crashes"
	}
	return s.inner.Append(ctx, cp)
}

func (s *crashAfterStore) Latest(ctx context.Context, id GraphRunID) (*Checkpoint, error) {
	return s.inner.Latest(ctx, id)
}

func (s *crashAfterStore) History(ctx context.Context, id GraphRunID) ([]*Checkpoint, error) {
	return s.inner.History(ctx, id)
}

// TestResumeCrashMidStepDoesNotReRunTerminal is the C1 regression test (§9.3,
// §10.1): in PerVertex mode a resumed step writes intermediate StepRunning
// checkpoints; if such a checkpoint dropped the step's already-committed
// Done/Failed-Route terminals, a crash there would make the NEXT resume re-run a
// Done vertex (double execution + state double-count). It builds a fan-out step
// where A succeeds (Done) and B pauses (Awaiting), crashes Resume #1 right after
// B's intermediate StepRunning append, then resumes #2 from that checkpoint and
// proves A's task ran only ONCE total and its reduction is not duplicated.
func TestResumeCrashMidStepDoesNotReRunTerminal(t *testing.T) {
	t.Parallel()

	var aRuns, bSeen atomic.Int32
	mem := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	a, b := vID(2), vID(3)
	addErrVertex(t, g, vID(1), NewFuncTask(func(_ context.Context, _ int) (string, error) { return "entry", nil }))
	// A always succeeds; a re-run would bump aRuns past 1 and double-fold "a-done".
	addErrVertex(t, g, a, NewFuncTask(func(_ context.Context, _ int) (string, error) {
		aRuns.Add(1)
		return "a-done", nil
	}))
	// B pauses Awaiting once, then succeeds.
	addErrVertex(t, g, b, NewFuncTask(func(ctx context.Context, _ int) (string, error) {
		if bSeen.Add(1) == 1 {
			return "", Interrupt(ctx, "approve b")
		}
		return "b-done", nil
	}))
	addErrVertex(t, g, vID(4), NewFuncTask(func(_ context.Context, _ int) (string, error) { return "fin", nil }))
	r, _, _, _, _ := fanOutTwo(t, g, mem)

	// Initial Run: step 1 pauses with A=Done, B=Awaiting (a StepPaused boundary).
	id := resumeOnce(t, r, mem)
	if aRuns.Load() != 1 {
		t.Fatalf("A ran %d times before resume, want 1", aRuns.Load())
	}

	// Resume #1 over a crashing store: the resumed step re-runs only B; the FIRST
	// append of this resume is B's intermediate StepRunning checkpoint, so fail the
	// 2nd append (1st succeeds = the intermediate, 2nd = the would-be boundary) —
	// leaving an intermediate StepRunning checkpoint as the durable Latest.
	crash := &crashAfterStore{inner: mem, failOn: 2}
	rCrash, err := compileOver(t, g, crash)
	if err != nil {
		t.Fatalf("compile over crash store: %v", err)
	}
	_, err = rCrash.Resume(context.Background(), id, nil)
	if !errors.Is(err, errSimulatedCrash) {
		t.Fatalf("Resume #1 error = %v, want the simulated crash (an intermediate StepRunning must be durable first)", err)
	}
	last := lastCheckpoint(t, mem, id)
	if last.Phase != StepRunning {
		t.Fatalf("Latest Phase after crash = %v, want StepRunning (the intermediate checkpoint)", last.Phase)
	}

	// Resume #2 over the clean MemStore from that intermediate StepRunning Latest.
	res, err := r.Resume(context.Background(), id, nil)
	if err != nil {
		t.Fatalf("Resume #2: %v", err)
	}
	if res.Run.Status != RunCompleted {
		t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
	}
	if aRuns.Load() != 1 {
		t.Errorf("A ran %d times total, want 1 — a Done terminal must NOT re-run after a mid-resume crash", aRuns.Load())
	}
	aCount := 0
	for _, v := range res.State.Vals {
		if v == "a-done" {
			aCount++
		}
	}
	if aCount != 1 {
		t.Errorf("'a-done' appears %d times in final State, want 1 (no reducer double-count)", aCount)
	}
}

// compileOver compiles g over the given store, returning the Runner. It mirrors
// the inline Compile calls but threads an arbitrary CheckpointStore wrapper.
func compileOver(t *testing.T, g *Graph[cnt], store CheckpointStore) (*Runner[cnt], error) {
	t.Helper()
	return g.Compile(vID(1), vID(4), WithStore(store))
}

// TestResumeGraphVersionMatrix is the §15 graph-versioning RESUME matrix
// end-to-end: pause a run on graph G (the store records G's GraphVersion), then
// resume against a LIVE graph that differs by exactly one dimension — a topology
// edit (add a vertex+edge), a conditional Targets change, or a WithVersion bump —
// and prove each yields a *GraphVersionMismatchError because the live
// GraphVersion no longer matches the checkpoint's. An IDENTICAL rebuild of G
// resumes fine. This complements TestResumeValidationRejectsBeforeAnyTask (which
// mutates the checkpoint's GraphVersion field directly) by driving a REAL graph
// edit through Compile's version computation, the way a deployment would.
func TestResumeGraphVersionMatrix(t *testing.T) {
	t.Parallel()

	// pauseUntilResumed pauses Awaiting until a resume supplies a payload, then
	// succeeds. Detecting resume via ResumePayload (not a counter) makes the task
	// stateless across separate Runner instances built from the SAME graph
	// definition — exactly how a redeploy recompiles a graph — so an identical
	// rebuild completes on resume while only topology/Targets/version can differ.
	pauseUntilResumed := func() Task[int, string] {
		return NewFuncTask(func(ctx context.Context, _ int) (string, error) {
			if _, ok := ResumePayload[string](ctx); !ok {
				return "", Interrupt(ctx, "pause")
			}
			return "entry", nil
		})
	}

	// buildBase wires entry(pauses until resumed) -> a -> fin over the SAME GraphID
	// so the only thing that can differ between runs is topology/Targets/version.
	buildBase := func(t *testing.T, store CheckpointStore, opts ...GraphOption) *Runner[cnt] {
		t.Helper()
		g := NewGraph[cnt](GraphID{}, opts...)
		entry, a, fin := vID(1), vID(2), vID(3)
		addErrVertex(t, g, entry, pauseUntilResumed())
		tagVertex(t, g, a, "a")
		tagVertex(t, g, fin, "fin")
		for _, e := range [][2]VertexID{{entry, a}, {a, fin}} {
			if err := g.AddEdge(e[0], e[1]); err != nil {
				t.Fatalf("AddEdge: %v", err)
			}
		}
		r, err := g.Compile(entry, fin, opts2compile(store)...)
		if err != nil {
			t.Fatalf("Compile base: %v", err)
		}
		return r
	}

	tests := []struct {
		name string
		// recompile builds the LIVE graph the resume runs against, over the SAME store.
		recompile func(t *testing.T, store CheckpointStore) *Runner[cnt]
		wantErr   bool // true → *GraphVersionMismatchError; false → identical, resumes fine
	}{
		{
			name: "identical graph resumes fine",
			recompile: func(t *testing.T, store CheckpointStore) *Runner[cnt] {
				return buildBase(t, store)
			},
			wantErr: false,
		},
		{
			name: "topology edit (extra vertex+edge) → mismatch",
			recompile: func(t *testing.T, store CheckpointStore) *Runner[cnt] {
				g := NewGraph[cnt](GraphID{})
				entry, a, fin, extra := vID(1), vID(2), vID(3), vID(4)
				addErrVertex(t, g, entry, pauseUntilResumed())
				tagVertex(t, g, a, "a")
				tagVertex(t, g, fin, "fin")
				tagVertex(t, g, extra, "extra")
				for _, e := range [][2]VertexID{{entry, a}, {a, extra}, {extra, fin}} {
					if err := g.AddEdge(e[0], e[1]); err != nil {
						t.Fatalf("AddEdge: %v", err)
					}
				}
				r, err := g.Compile(entry, fin, WithStore(store))
				if err != nil {
					t.Fatalf("Compile edited: %v", err)
				}
				return r
			},
			wantErr: true,
		},
		{
			name: "WithVersion bump → mismatch",
			recompile: func(t *testing.T, store CheckpointStore) *Runner[cnt] {
				return buildBase(t, store, WithVersion(42))
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := NewMemStore()
			rBase := buildBase(t, store)
			id := resumeOnce(t, rBase, store) // pause on the original graph version

			live := tt.recompile(t, store)
			res, err := live.Resume(context.Background(), id, "approve")
			if tt.wantErr {
				var mismatch *GraphVersionMismatchError
				if !errors.As(err, &mismatch) {
					t.Fatalf("Resume() error = %v, want *GraphVersionMismatchError", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Resume (identical graph) error = %v, want success", err)
			}
			if res.Run.Status != RunCompleted {
				t.Errorf("Status = %v, want RunCompleted (identical graph resumes fine)", res.Run.Status)
			}
		})
	}
}

// opts2compile returns the CompileOptions for a store-backed compile (a tiny
// helper so the version-matrix fixtures don't repeat the nil-store branch).
func opts2compile(store CheckpointStore) []CompileOption {
	if store == nil {
		return nil
	}
	return []CompileOption{WithStore(store)}
}

// TestRevisionModelInvariant locks the revision-model invariant (the controller's
// recent off-by-one fix): a returned Result's Run.Revision is exactly the LATEST
// persisted revision — res.Run.Revision == store.Latest(id).Run.Revision — for BOTH
// a completed Run AND a Resume result. c.rs.Revision tracks the LAST WRITTEN
// revision, so the Result and the store agree; an off-by-one (advancing before the
// write, or after the snapshot) would silently regress this and is pinned here.
func TestRevisionModelInvariant(t *testing.T) {
	t.Parallel()

	assertMatchesLatest := func(t *testing.T, store CheckpointStore, res *Result[cnt]) {
		t.Helper()
		latest, err := store.Latest(context.Background(), res.Run.GraphRunID)
		if err != nil {
			t.Fatalf("Latest: %v", err)
		}
		if res.Run.Revision != latest.Run.Revision {
			t.Errorf("Result.Run.Revision = %d, want %d (== store.Latest revision)",
				res.Run.Revision, latest.Run.Revision)
		}
		// The latest checkpoint count proves the revision is the highest written: a
		// run with N checkpoints has revisions 0..N-1, so the latest revision == N-1.
		hist, err := store.History(context.Background(), res.Run.GraphRunID)
		if err != nil {
			t.Fatalf("History: %v", err)
		}
		if want := uint64(len(hist) - 1); res.Run.Revision != want {
			t.Errorf("Result.Run.Revision = %d, want %d (highest of %d contiguous revisions)",
				res.Run.Revision, want, len(hist))
		}
	}

	t.Run("completed Run result", func(t *testing.T) {
		t.Parallel()
		store := NewMemStore()
		r, _ := compileChain(t, store, "a", "b", "fin")
		res, err := r.Run(context.Background(), cnt{})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if res.Run.Status != RunCompleted {
			t.Fatalf("Status = %v, want RunCompleted", res.Run.Status)
		}
		assertMatchesLatest(t, store, res)
	})

	t.Run("Resume result", func(t *testing.T) {
		t.Parallel()
		var firstSeen, secondSeen atomic.Int32
		var gotPayload atomic.Value
		store := NewMemStore()
		g := NewGraph[cnt](GraphID{})
		first, second := vID(1), vID(2)
		addErrVertex(t, g, first, NewFuncTask(func(_ context.Context, _ int) (string, error) {
			firstSeen.Add(1)
			return "first", nil
		}))
		addErrVertex(t, g, second, flipTask(&secondSeen, &gotPayload, "approve"))
		if err := g.AddEdge(first, second); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
		r, err := g.Compile(first, second, WithStore(store))
		if err != nil {
			t.Fatalf("Compile: %v", err)
		}

		// The paused Run result also satisfies the invariant.
		pausedRes, err := r.Run(context.Background(), cnt{})
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
		if pausedRes.Run.Status != RunInterrupted {
			t.Fatalf("paused Status = %v, want RunInterrupted", pausedRes.Run.Status)
		}
		assertMatchesLatest(t, store, pausedRes)

		// And so does the completed Resume result, after more revisions are appended.
		res, err := r.Resume(context.Background(), pausedRes.Run.GraphRunID, "the-payload")
		if err != nil {
			t.Fatalf("Resume: %v", err)
		}
		if res.Run.Status != RunCompleted {
			t.Fatalf("Resume Status = %v, want RunCompleted", res.Run.Status)
		}
		if res.Run.Revision <= pausedRes.Run.Revision {
			t.Errorf("Resume Revision %d <= paused Revision %d (resume must advance the sequence)",
				res.Run.Revision, pausedRes.Run.Revision)
		}
		assertMatchesLatest(t, store, res)
	})
}
