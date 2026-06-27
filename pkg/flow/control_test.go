package flow

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
)

// This file white-box tests the in-process control surface (§18.2): Status, Get,
// and Cancel — all store-backed, NO graph execution — plus the coordinator's
// observed-cancellation path (a worker append that loses to a concurrent Cancel
// reads the now-Cancelled latest as a graceful stop, not a RevisionConflictError).
// It reuses cnt (runner_test.go), compileChain/compileSingle, and the recorder
// hooks from engine_test.go / runner_test.go.

// --- Status -----------------------------------------------------------------

// TestControlStatus proves Status returns the latest GraphRunState (status, step,
// revision) without decoding S, and that an unknown id propagates the store's
// *CheckpointNotFoundError.
func TestControlStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		seed       bool // run to completion first so a checkpoint exists
		wantStatus RunStatus
		wantErr    bool
	}{
		{name: "completed run", seed: true, wantStatus: RunCompleted},
		{name: "unknown id", seed: false, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := NewMemStore()
			r, _ := compileChain(t, store, "a", "fin")
			ctx := context.Background()

			var id GraphRunID
			if tt.seed {
				res, err := r.Run(ctx, cnt{})
				if err != nil {
					t.Fatalf("Run: %v", err)
				}
				id = res.Run.GraphRunID
			} else {
				var err error
				if id, err = NewGraphRunID(); err != nil {
					t.Fatalf("NewGraphRunID: %v", err)
				}
			}

			rs, err := r.Status(ctx, id)
			if tt.wantErr {
				var notFound *CheckpointNotFoundError
				if !errors.As(err, &notFound) {
					t.Fatalf("Status() error = %v, want *CheckpointNotFoundError", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Status: %v", err)
			}
			if rs.Status != tt.wantStatus {
				t.Errorf("Status = %v, want %v", rs.Status, tt.wantStatus)
			}
			if rs.GraphRunID != id {
				t.Errorf("GraphRunID = %v, want %v", rs.GraphRunID, id)
			}
			// The latest revision is the boundary checkpoint, so Revision advanced.
			latest, err := store.Latest(ctx, id)
			if err != nil {
				t.Fatalf("Latest: %v", err)
			}
			if rs.Revision != latest.Run.Revision || rs.Step != latest.Run.Step {
				t.Errorf("Status{Revision:%d,Step:%v} != latest{Revision:%d,Step:%v}",
					rs.Revision, rs.Step, latest.Run.Revision, latest.Run.Step)
			}
		})
	}
}

// TestControlStatusNoStateDecode proves Status does NOT decode S: a checkpoint
// whose State bytes cannot decode into the runner's S still yields a correct
// GraphRunState. We poison the store with a checkpoint whose State is invalid JSON
// for cnt and assert Status succeeds while Get fails.
func TestControlStatusNoStateDecode(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	r, _ := compileSingle(t, store)
	ctx := context.Background()

	id, err := NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID: %v", err)
	}
	cp := &Checkpoint{
		Run: GraphRunState{
			GraphRunID:   id,
			GraphID:      r.GraphID(),
			GraphVersion: r.GraphVersion(),
			Status:       RunInterrupted,
			Step:         StepID(3),
		},
		State: json.RawMessage(`"not a cnt object"`), // a string, not a cnt — decode into cnt fails
		Phase: StepRouted,
	}
	if err := store.Append(ctx, cp); err != nil {
		t.Fatalf("Append: %v", err)
	}

	rs, err := r.Status(ctx, id)
	if err != nil {
		t.Fatalf("Status must not decode S, got error %v", err)
	}
	if rs.Status != RunInterrupted || rs.Step != StepID(3) {
		t.Errorf("Status = {%v,%v}, want {RunInterrupted,3}", rs.Status, rs.Step)
	}
	// Get DOES decode S, so it must fail on the same poisoned checkpoint.
	if _, err := r.Get(ctx, id); err == nil {
		t.Error("Get returned nil error on a checkpoint with undecodable State")
	}
}

// --- Get --------------------------------------------------------------------

// TestControlGetCompleted proves Get decodes the final State after a completed run.
func TestControlGetCompleted(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	r, _ := compileChain(t, store, "a", "fin")
	ctx := context.Background()

	res, err := r.Run(ctx, cnt{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, err := r.Get(ctx, res.Run.GraphRunID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Run.Status != RunCompleted {
		t.Errorf("Get.Run.Status = %v, want RunCompleted", got.Run.Status)
	}
	if len(got.State.Vals) != 2 || got.State.Vals[0] != "a" || got.State.Vals[1] != "fin" {
		t.Errorf("Get.State.Vals = %v, want [a fin]", got.State.Vals)
	}
	if got.Interrupts != nil || got.Halt != nil {
		t.Errorf("completed Get has Interrupts=%v Halt=%v, want both nil", got.Interrupts, got.Halt)
	}
}

// TestControlGetPaused proves Get reconstructs Result.Interrupts (vertex + kind,
// Info raw) from a paused (StepPaused) checkpoint and reports RunInterrupted.
func TestControlGetPaused(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	entry := vID(1)
	addErrVertex(t, g, entry, NewFuncTask(func(ctx context.Context, _ int) (string, error) {
		return "", Interrupt(ctx, "need approval")
	}))
	r, err := g.Compile(entry, entry, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	res, err := r.Run(context.Background(), cnt{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, err := r.Get(context.Background(), res.Run.GraphRunID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Run.Status != RunInterrupted {
		t.Errorf("Get.Run.Status = %v, want RunInterrupted", got.Run.Status)
	}
	if got.Halt != nil {
		t.Errorf("Get.Halt = %v, want nil (mutually exclusive with Interrupts)", got.Halt)
	}
	if len(got.Interrupts) != 1 {
		t.Fatalf("Get.Interrupts = %v, want exactly 1", got.Interrupts)
	}
	iv := got.Interrupts[0]
	if iv.Vertex != entry || iv.Kind != Awaiting {
		t.Errorf("Interruption = {%v,%v}, want {%v,Awaiting}", iv.Vertex, iv.Kind, entry)
	}
	// Info is best-effort raw JSON: a caller json.Unmarshals it.
	raw, ok := iv.Info.(json.RawMessage)
	if !ok {
		t.Fatalf("Interruption.Info type = %T, want json.RawMessage (best-effort raw)", iv.Info)
	}
	var reason string
	if err := json.Unmarshal(raw, &reason); err != nil || reason != "need approval" {
		t.Errorf("Info raw = %s (err %v), want marshaled 'need approval'", raw, err)
	}
}

// TestControlGetErroredPause proves Get reconstructs an Errored Interruption with
// a string-wrapped Cause from a default-Pause failure checkpoint.
func TestControlGetErroredPause(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	entry := vID(1)
	addErrVertex(t, g, entry, NewFuncTask(func(_ context.Context, _ int) (string, error) {
		return "", errors.New("boom")
	}))
	r, err := g.Compile(entry, entry, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	res, err := r.Run(context.Background(), cnt{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, err := r.Get(context.Background(), res.Run.GraphRunID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if len(got.Interrupts) != 1 {
		t.Fatalf("Get.Interrupts = %v, want exactly 1", got.Interrupts)
	}
	iv := got.Interrupts[0]
	if iv.Kind != Errored {
		t.Errorf("Kind = %v, want Errored", iv.Kind)
	}
	if iv.Cause == nil || iv.Cause.Error() == "" {
		t.Errorf("Cause = %v, want a non-empty string-wrapped cause", iv.Cause)
	}
}

// TestControlGetHalted proves Get reconstructs Result.Halt from a StepHalted
// checkpoint (HaltMaxSteps).
func TestControlGetHalted(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	entry, a, fin := vID(1), vID(2), vID(3)
	tagVertex(t, g, entry, "entry")
	tagVertex(t, g, a, "a")
	tagVertex(t, g, fin, "fin")
	for _, e := range [][2]VertexID{{entry, a}, {a, entry}, {a, fin}} {
		if err := g.AddEdge(e[0], e[1]); err != nil {
			t.Fatalf("AddEdge: %v", err)
		}
	}
	r, err := g.Compile(entry, fin, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	res, err := r.Run(context.Background(), cnt{}, WithMaxSteps(3))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	got, err := r.Get(context.Background(), res.Run.GraphRunID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Interrupts != nil {
		t.Errorf("Get.Interrupts = %v, want nil (mutually exclusive with Halt)", got.Interrupts)
	}
	if got.Halt == nil {
		t.Fatal("Get.Halt = nil, want a reconstructed HaltMaxSteps")
	}
	if got.Halt.Kind != HaltMaxSteps {
		t.Errorf("Halt.Kind = %v, want HaltMaxSteps", got.Halt.Kind)
	}
	if got.Halt.GraphRunID != res.Run.GraphRunID {
		t.Errorf("Halt.GraphRunID = %v, want %v", got.Halt.GraphRunID, res.Run.GraphRunID)
	}
	if got.Halt.Cause == nil || got.Halt.Cause.Error() == "" {
		t.Errorf("Halt.Cause = %v, want a non-empty string-wrapped cause", got.Halt.Cause)
	}
}

// TestControlGetUnknown proves Get on an unknown id propagates *CheckpointNotFoundError.
func TestControlGetUnknown(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	r, _ := compileSingle(t, store)
	id, err := NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID: %v", err)
	}
	if _, err := r.Get(context.Background(), id); !errors.As(err, new(*CheckpointNotFoundError)) {
		t.Fatalf("Get() error = %v, want *CheckpointNotFoundError", err)
	}
}

// --- Cancel -----------------------------------------------------------------

// TestControlCancel proves Cancel appends a terminal RunCancelled checkpoint
// (CancelledAt set, CancelReason recorded, Revision advanced), fires OnRunFinish,
// and blocks a subsequent Resume with *ResumeTerminalError{RunCancelled}.
func TestControlCancel(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	entry := vID(1)
	addErrVertex(t, g, entry, NewFuncTask(func(ctx context.Context, _ int) (string, error) {
		return "", Interrupt(ctx, "pause")
	}))
	r, err := g.Compile(entry, entry, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ctx := context.Background()
	res, err := r.Run(ctx, cnt{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	id := res.Run.GraphRunID
	beforeRev := res.Run.Revision

	rc := &recorder{}
	if err := r.Cancel(ctx, id, "operator stopped it", WithHooks(rc.hooks())); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	rs, err := r.Status(ctx, id)
	if err != nil {
		t.Fatalf("Status after Cancel: %v", err)
	}
	if rs.Status != RunCancelled {
		t.Errorf("Status = %v, want RunCancelled", rs.Status)
	}
	if rs.CancelledAt.IsZero() {
		t.Error("CancelledAt is zero after Cancel")
	}
	if rs.CancelReason != "operator stopped it" {
		t.Errorf("CancelReason = %q, want 'operator stopped it'", rs.CancelReason)
	}
	if rs.Revision != beforeRev+1 {
		t.Errorf("Revision = %d, want %d (cancel appends exactly one checkpoint)", rs.Revision, beforeRev+1)
	}

	rc.mu.Lock()
	finishes, finishStatus := rc.runFinish, rc.finishStatus
	rc.mu.Unlock()
	if finishes != 1 {
		t.Errorf("OnRunFinish fired %d times, want 1", finishes)
	}
	if finishStatus != RunCancelled {
		t.Errorf("OnRunFinish observed Status %v, want RunCancelled", finishStatus)
	}

	// A cancelled run cannot resume.
	if _, err := r.Resume(ctx, id, nil); !errors.As(err, new(*ResumeTerminalError)) {
		t.Fatalf("Resume after Cancel error = %v, want *ResumeTerminalError", err)
	}
	// Get still works (State carried forward).
	if _, err := r.Get(ctx, id); err != nil {
		t.Errorf("Get after Cancel: %v", err)
	}
}

// TestControlCancelTerminal proves Cancel on an already-COMPLETED run is rejected
// with *ResumeTerminalError{RunCompleted} (a finished run cannot be cancelled),
// and that an unknown id (no history) → *CheckpointNotFoundError. The
// already-cancelled case is covered by TestControlCancelTwice.
func TestControlCancelTerminal(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		setup      func(t *testing.T, r *Runner[cnt], ctx context.Context) GraphRunID
		wantStatus RunStatus // expected ResumeTerminalError.Status
		wantNotFnd bool      // expect *CheckpointNotFoundError instead
	}{
		{
			name: "already completed",
			setup: func(t *testing.T, r *Runner[cnt], ctx context.Context) GraphRunID {
				res, err := r.Run(ctx, cnt{})
				if err != nil {
					t.Fatalf("Run: %v", err)
				}
				return res.Run.GraphRunID
			},
			wantStatus: RunCompleted,
		},
		{
			name: "unknown id",
			setup: func(t *testing.T, r *Runner[cnt], ctx context.Context) GraphRunID {
				id, err := NewGraphRunID()
				if err != nil {
					t.Fatalf("NewGraphRunID: %v", err)
				}
				return id
			},
			wantNotFnd: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := NewMemStore()
			r, _ := compileSingle(t, store)
			ctx := context.Background()
			id := tt.setup(t, r, ctx)

			err := r.Cancel(ctx, id, "reason")
			if tt.wantNotFnd {
				if !errors.As(err, new(*CheckpointNotFoundError)) {
					t.Fatalf("Cancel() error = %v, want *CheckpointNotFoundError", err)
				}
				return
			}
			var terminal *ResumeTerminalError
			if !errors.As(err, &terminal) {
				t.Fatalf("Cancel() error = %v, want *ResumeTerminalError", err)
			}
			if terminal.Status != tt.wantStatus {
				t.Errorf("ResumeTerminalError.Status = %v, want %v", terminal.Status, tt.wantStatus)
			}
		})
	}
}

// TestControlCancelTwice proves cancelling an already-cancelled run is rejected
// with *ResumeTerminalError{RunCancelled}: cancel is terminal-once.
func TestControlCancelTwice(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	entry := vID(1)
	addErrVertex(t, g, entry, NewFuncTask(func(ctx context.Context, _ int) (string, error) {
		return "", Interrupt(ctx, "pause")
	}))
	r, err := g.Compile(entry, entry, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	ctx := context.Background()
	res, err := r.Run(ctx, cnt{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	id := res.Run.GraphRunID
	if err := r.Cancel(ctx, id, "first"); err != nil {
		t.Fatalf("first Cancel: %v", err)
	}
	var terminal *ResumeTerminalError
	if err := r.Cancel(ctx, id, "second"); !errors.As(err, &terminal) {
		t.Fatalf("second Cancel error = %v, want *ResumeTerminalError", err)
	} else if terminal.Status != RunCancelled {
		t.Errorf("Status = %v, want RunCancelled", terminal.Status)
	}
}

// --- Observed cancellation (the race) ---------------------------------------

// blockGate coordinates the observed-cancellation race: a task signals it has
// started (closing started) and then blocks on release until the test lets it
// finish. The test cancels the run while the task is blocked, so the coordinator's
// NEXT append (after release) loses the compare-and-append to the cancel.
type blockGate struct {
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockGate() *blockGate {
	return &blockGate{started: make(chan struct{}), release: make(chan struct{})}
}

func (g *blockGate) enter() {
	g.once.Do(func() { close(g.started) })
	<-g.release
}

func (g *blockGate) open() { close(g.release) }

// TestControlObservedCancellation proves the coordinator reads a lost
// compare-and-append against a now-Cancelled latest as OBSERVED CANCELLATION: the
// run returns a graceful Result with Run.Status == RunCancelled and a NIL error,
// not a *RevisionConflictError. CONSTRUCTION: entry -> fin; entry's task blocks
// until released. While it blocks the test Cancels the run (appending the terminal
// checkpoint at the revision the coordinator's NEXT append needs), then releases
// the task so the coordinator's per-vertex append loses the race.
func TestControlObservedCancellation(t *testing.T) {
	t.Parallel()

	gate := newBlockGate()
	store := NewMemStore()
	g := NewGraph[cnt](GraphID{})
	entry, fin := vID(1), vID(2)
	addErrVertex(t, g, entry, NewFuncTask(func(_ context.Context, _ int) (string, error) {
		gate.enter()
		return "entry", nil
	}))
	tagVertex(t, g, fin, "fin")
	if err := g.AddEdge(entry, fin); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	r, err := g.Compile(entry, fin, WithStore(store))
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	id, err := NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID: %v", err)
	}
	ctx := context.Background()

	var res *Result[cnt]
	var runErr error
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		res, runErr = r.Run(ctx, cnt{}, WithGraphRunID(id))
	}()

	<-gate.started // the entry task is running and blocked
	if err := r.Cancel(ctx, id, "cancelled mid-flight"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	gate.open() // release the task; the coordinator's next append now loses the race
	wg.Wait()

	if runErr != nil {
		t.Fatalf("Run returned %v, want nil (observed cancellation is graceful)", runErr)
	}
	if res == nil {
		t.Fatal("Run returned nil Result on observed cancellation")
	}
	if res.Run.Status != RunCancelled {
		t.Errorf("Result.Run.Status = %v, want RunCancelled", res.Run.Status)
	}
	// No genuine RevisionConflictError leaked through.
	if errors.As(runErr, new(*RevisionConflictError)) {
		t.Error("observed cancellation surfaced a *RevisionConflictError, want graceful nil")
	}
	// The store's terminal checkpoint is the cancel.
	rs, err := r.Status(ctx, id)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if rs.Status != RunCancelled {
		t.Errorf("final Status = %v, want RunCancelled", rs.Status)
	}
}

// conflictStore wraps a CheckpointStore and injects ONE extra checkpoint append
// (at a chosen revision) the first time the coordinator tries to append that
// revision, so the coordinator's own append then loses the compare-and-append —
// a GENUINE concurrent-writer conflict that is NOT a cancellation. The injected
// checkpoint keeps Status == RunRunning (not Cancelled), so the coordinator must
// propagate the *RevisionConflictError rather than read it as observed cancel.
type conflictStore struct {
	CheckpointStore
	atRevision uint64
	injectOnce sync.Once
	injected   atomic.Bool
}

func (s *conflictStore) Append(ctx context.Context, cp *Checkpoint) error {
	if cp.Run.Revision == s.atRevision && !s.injected.Load() {
		s.injectOnce.Do(func() {
			rival := *cp
			rival.Run.Status = RunRunning // a rival running writer, NOT a cancel
			_ = s.CheckpointStore.Append(ctx, &rival)
			s.injected.Store(true)
		})
	}
	return s.CheckpointStore.Append(ctx, cp)
}

// TestControlGenuineConflictPropagates proves a genuine concurrent-writer conflict
// (the latest is NOT Cancelled) still surfaces as a *RevisionConflictError from
// the coordinator — observed cancellation must not swallow real conflicts.
func TestControlGenuineConflictPropagates(t *testing.T) {
	t.Parallel()

	// The seed append is revision 0; the next coordinator append is revision 1.
	// Inject a rival running writer at revision 1 so the coordinator's revision-1
	// append loses the compare-and-append.
	store := &conflictStore{CheckpointStore: NewMemStore(), atRevision: 1}
	r, _ := compileChain(t, store, "a", "fin")

	_, err := r.Run(context.Background(), cnt{})
	if !errors.As(err, new(*RevisionConflictError)) {
		t.Fatalf("Run() error = %v, want *RevisionConflictError (genuine conflict)", err)
	}
}

// conflictThenLatestFailStore is conflictStore PLUS a failing Latest: after the
// injected revision-conflict it makes Latest return a *StoreError, exercising
// classifyAppendErr's Latest-read-failure branch (engine.go). The coordinator
// cannot prove the conflict was a cancellation (it can't read the latest), so it
// must FAIL SECURE — propagate the conflict error rather than treat it as an
// observed cancellation and silently complete.
type conflictThenLatestFailStore struct {
	CheckpointStore
	atRevision uint64
	injectOnce sync.Once
	injected   atomic.Bool
	latestErr  error // returned by Latest once the conflict has been injected
}

func (s *conflictThenLatestFailStore) Append(ctx context.Context, cp *Checkpoint) error {
	if cp.Run.Revision == s.atRevision && !s.injected.Load() {
		s.injectOnce.Do(func() {
			rival := *cp
			rival.Run.Status = RunRunning // a rival running writer, NOT a cancel
			_ = s.CheckpointStore.Append(ctx, &rival)
			s.injected.Store(true)
		})
	}
	return s.CheckpointStore.Append(ctx, cp)
}

func (s *conflictThenLatestFailStore) Latest(ctx context.Context, id GraphRunID) (*Checkpoint, error) {
	if s.injected.Load() {
		return nil, s.latestErr // the classify-time read cannot prove a cancellation
	}
	return s.CheckpointStore.Latest(ctx, id)
}

// TestControlClassifyAppendErrLatestFailureFailsSecure pins the Latest-read-failure
// branch of classifyAppendErr (gap from the 6.11 review): a racy append loses with
// a *RevisionConflictError AND the subsequent Latest read fails, so the coordinator
// cannot distinguish an observed cancellation from a genuine conflict. Fail secure:
// it propagates a REAL error (never a graceful nil Result), and specifically NOT an
// observed-cancellation completion. We assert the original conflict propagates.
func TestControlClassifyAppendErrLatestFailureFailsSecure(t *testing.T) {
	t.Parallel()

	store := &conflictThenLatestFailStore{
		CheckpointStore: NewMemStore(),
		atRevision:      1, // seed = rev 0; the coordinator's next append = rev 1
		latestErr:       &StoreError{Op: "Latest", Err: errors.New("store unavailable")},
	}
	r, _ := compileChain(t, store, "a", "fin")

	res, err := r.Run(context.Background(), cnt{})
	if err == nil {
		t.Fatalf("Run() returned nil error (res=%+v); want the conflict propagated (fail secure)", res)
	}
	// The original conflict is returned UNCHANGED — not swallowed as a cancellation.
	if !errors.As(err, new(*RevisionConflictError)) {
		t.Fatalf("Run() error = %v, want the original *RevisionConflictError (Latest read failed → fail secure)", err)
	}
	// A run whose Latest read failed must NOT be reported as gracefully Cancelled.
	if res != nil && res.Run.Status == RunCancelled {
		t.Error("classifyAppendErr treated a Latest-read-failure as observed cancellation; want fail-secure error")
	}
}

// appendFailStore wraps a CheckpointStore and makes the FIRST Append fail with a
// chosen error (delegating all other ops), so a single control-plane Append (e.g.
// Cancel's terminal write) can be made to fail without execution. Unlike
// crashAfterStore (counts appends) this fails the next append outright, which is
// what Cancel issues exactly once.
type appendFailStore struct {
	CheckpointStore
	failErr  error
	failOnce sync.Once
	failed   atomic.Bool
}

func (s *appendFailStore) Append(ctx context.Context, cp *Checkpoint) error {
	var inject error
	s.failOnce.Do(func() { inject = s.failErr; s.failed.Store(true) })
	if inject != nil {
		return inject
	}
	return s.CheckpointStore.Append(ctx, cp)
}

// TestControlCancelAppendErrorPropagates pins Cancel's store-append-error path (gap
// from the 6.11 review): when the terminal RunCancelled append fails with a
// non-terminal error, Cancel returns that error UNCHANGED and OnRunFinish does NOT
// fire (no observer is told the run was cancelled when the cancel did not persist).
func TestControlCancelAppendErrorPropagates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		failErr error
		assert  func(t *testing.T, err error)
	}{
		{
			name:    "revision conflict on cancel append",
			failErr: &RevisionConflictError{Expected: 9, Actual: 1},
			assert: func(t *testing.T, err error) {
				if !errors.As(err, new(*RevisionConflictError)) {
					t.Fatalf("Cancel() error = %v, want *RevisionConflictError unchanged", err)
				}
			},
		},
		{
			name:    "store error on cancel append",
			failErr: &StoreError{Op: "Append", Err: errors.New("disk full")},
			assert: func(t *testing.T, err error) {
				if !errors.As(err, new(*StoreError)) {
					t.Fatalf("Cancel() error = %v, want *StoreError unchanged", err)
				}
			},
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Seed a non-terminal run via a real run so Latest reads a cancellable
			// checkpoint; THEN wrap the same backing store so only the cancel append
			// fails (the prior run's appends already succeeded on the inner store).
			mem := NewMemStore()
			g := NewGraph[cnt](GraphID{})
			entry := vID(1)
			addErrVertex(t, g, entry, interruptTask("pause"))
			rSeed, err := g.Compile(entry, entry, WithStore(mem))
			if err != nil {
				t.Fatalf("Compile(seed): %v", err)
			}
			ctx := context.Background()
			res, err := rSeed.Run(ctx, cnt{})
			if err != nil {
				t.Fatalf("Run(seed): %v", err)
			}
			id := res.Run.GraphRunID

			fail := &appendFailStore{CheckpointStore: mem, failErr: tt.failErr}
			rCancel, err := g.Compile(entry, entry, WithStore(fail))
			if err != nil {
				t.Fatalf("Compile(cancel): %v", err)
			}

			rc := &recorder{}
			cancelErr := rCancel.Cancel(ctx, id, "reason", WithHooks(rc.hooks()))
			tt.assert(t, cancelErr)

			// OnRunFinish must NOT fire when the terminal append failed.
			rc.mu.Lock()
			finishes := rc.runFinish
			rc.mu.Unlock()
			if finishes != 0 {
				t.Errorf("OnRunFinish fired %d times after a failed cancel append, want 0", finishes)
			}
			// The run is still cancellable (status unchanged) — the cancel did not persist.
			rs, err := rSeed.Status(ctx, id)
			if err != nil {
				t.Fatalf("Status: %v", err)
			}
			if rs.Status == RunCancelled {
				t.Error("Status = RunCancelled after a FAILED cancel append, want the prior non-terminal status")
			}
		})
	}
}

// TestControlGetHaltNilRecordNoPanic pins reconstructHalt's nil-record branch (gap
// from the 6.11 review): a hand-built StepHalted checkpoint with a NIL HaltRecord (a
// corrupt store) makes Get return a Result with Halt == nil — no panic, no Interrupts.
func TestControlGetHaltNilRecordNoPanic(t *testing.T) {
	t.Parallel()

	store := NewMemStore()
	r, _ := compileSingle(t, store)
	ctx := context.Background()

	id, err := NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID: %v", err)
	}
	// A StepHalted checkpoint whose Halt record is nil (corrupt) — State is a valid
	// cnt so the decode succeeds and execution reaches reconstructHalt(id, nil).
	cp := &Checkpoint{
		Run: GraphRunState{
			GraphRunID:   id,
			GraphID:      r.GraphID(),
			GraphVersion: r.GraphVersion(),
			Status:       RunInterrupted, // a halted run carries RunInterrupted (§9.8)
			Step:         StepID(2),
		},
		State: json.RawMessage(`{"Vals":["x"],"N":1}`),
		Phase: StepHalted,
		Halt:  nil, // the corrupt-store case
	}
	if err := store.Append(ctx, cp); err != nil {
		t.Fatalf("Append: %v", err)
	}

	got, err := r.Get(ctx, id) // must not panic
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Halt != nil {
		t.Errorf("Get.Halt = %+v, want nil (nil HaltRecord → nil Halt)", got.Halt)
	}
	if got.Interrupts != nil {
		t.Errorf("Get.Interrupts = %v, want nil (StepHalted reconstructs Halt, not Interrupts)", got.Interrupts)
	}
	// State still decodes so the Result is otherwise usable.
	if len(got.State.Vals) != 1 || got.State.Vals[0] != "x" {
		t.Errorf("Get.State.Vals = %v, want [x]", got.State.Vals)
	}
}
