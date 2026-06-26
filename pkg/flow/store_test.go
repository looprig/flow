package flow

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
)

// This file tests the CheckpointStore contract and its in-memory MemStore impl
// (design §10.2). The contract is what EVERY implementation must honor, so the
// assertions here double as the spec a future durable backend is held to:
//
//  1. Append-only compare-and-append — accept cp iff cp.Run.Revision == latest+1
//     (i.e. equals the current count: revisions are 0,1,2,…). A gap, a dup, or a
//     stale next-revision loses to a RevisionConflictError, so (GraphRunID,
//     Revision) is unique and committed history never forks (concurrent resume
//     is DETECTED, not prevented).
//  2. Latest = highest revision; History = all checkpoints ordered by revision;
//     both on an unknown run → CheckpointNotFoundError.
//  3. Structural immutability on BOTH sides — Append persists an independent copy
//     (mutating cp afterward cannot change stored history) and Latest/History
//     return freshly decoded values (mutating a returned *Checkpoint cannot
//     corrupt stored history).
//  4. ctx is honored even in-memory: a cancelled ctx aborts with a StoreError
//     wrapping ctx.Err().
//  5. Concurrency: a write lock makes the revision check + append atomic, so two
//     racing appenders of the same next revision yield exactly one winner.

// runIDForTest mints a fresh GraphRunID, failing the test if randomness fails.
func runIDForTest(t *testing.T) GraphRunID {
	t.Helper()
	id, err := NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID() error = %v", err)
	}
	return id
}

// cpForTest builds a small but fully-formed Checkpoint for run id at revision
// rev, with distinguishable State/Frontier so immutability mutations are
// observable.
func cpForTest(id GraphRunID, rev uint64) *Checkpoint {
	return &Checkpoint{
		Run: GraphRunState{
			GraphRunID: id,
			Status:     RunRunning,
			Step:       StepID(rev),
			Revision:   rev,
		},
		State:    json.RawMessage(`{"count":` + itoa(int(rev)) + `}`),
		Vertices: []VertexState{{Status: VertexDone, Attempt: 1}},
		Frontier: []VertexID{},
		Phase:    StepRunning,
	}
}

func TestMemStoreSequentialAppend(t *testing.T) {
	t.Parallel()
	store := NewMemStore()
	ctx := context.Background()
	id := runIDForTest(t)

	for rev := uint64(0); rev < 3; rev++ {
		if err := store.Append(ctx, cpForTest(id, rev)); err != nil {
			t.Fatalf("Append(rev=%d) error = %v", rev, err)
		}
	}

	latest, err := store.Latest(ctx, id)
	if err != nil {
		t.Fatalf("Latest() error = %v", err)
	}
	if latest.Run.Revision != 2 {
		t.Errorf("Latest().Run.Revision = %d, want 2", latest.Run.Revision)
	}

	hist, err := store.History(ctx, id)
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	if len(hist) != 3 {
		t.Fatalf("len(History()) = %d, want 3", len(hist))
	}
	for i, cp := range hist {
		if cp.Run.Revision != uint64(i) {
			t.Errorf("History()[%d].Run.Revision = %d, want %d", i, cp.Run.Revision, i)
		}
	}
}

func TestMemStoreCompareAndAppendRejects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// seed appends applied (in order) before the conflicting append.
		seed []uint64
		// bad is the revision whose append must conflict.
		bad uint64
		// wantExpected/wantActual on the RevisionConflictError.
		wantExpected uint64
		wantActual   uint64
	}{
		{name: "first append must be revision 0", seed: nil, bad: 1, wantExpected: 0, wantActual: 1},
		{name: "duplicate revision 0", seed: []uint64{0}, bad: 0, wantExpected: 1, wantActual: 0},
		{name: "gap: revision 2 when latest is 0", seed: []uint64{0}, bad: 2, wantExpected: 1, wantActual: 2},
		{name: "stale next: revision 1 when latest is 1", seed: []uint64{0, 1}, bad: 1, wantExpected: 2, wantActual: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := NewMemStore()
			ctx := context.Background()
			id := runIDForTest(t)
			for _, rev := range tt.seed {
				if err := store.Append(ctx, cpForTest(id, rev)); err != nil {
					t.Fatalf("seed Append(rev=%d) error = %v", rev, err)
				}
			}

			err := store.Append(ctx, cpForTest(id, tt.bad))
			var conflict *RevisionConflictError
			if !errors.As(err, &conflict) {
				t.Fatalf("Append(rev=%d) error = %v, want *RevisionConflictError", tt.bad, err)
			}
			if conflict.GraphRunID != id {
				t.Errorf("conflict.GraphRunID = %v, want %v", conflict.GraphRunID, id)
			}
			if conflict.Expected != tt.wantExpected {
				t.Errorf("conflict.Expected = %d, want %d", conflict.Expected, tt.wantExpected)
			}
			if conflict.Actual != tt.wantActual {
				t.Errorf("conflict.Actual = %d, want %d", conflict.Actual, tt.wantActual)
			}

			// History must not have grown from the rejected append.
			hist, herr := store.History(ctx, id)
			if len(tt.seed) == 0 {
				var nf *CheckpointNotFoundError
				if !errors.As(herr, &nf) {
					t.Fatalf("History() after rejected first append error = %v, want *CheckpointNotFoundError", herr)
				}
			} else {
				if herr != nil {
					t.Fatalf("History() error = %v", herr)
				}
				if len(hist) != len(tt.seed) {
					t.Errorf("len(History()) = %d, want %d (rejected append must not grow history)", len(hist), len(tt.seed))
				}
			}
		})
	}
}

func TestMemStoreConcurrentResumeDetected(t *testing.T) {
	t.Parallel()
	store := NewMemStore()
	ctx := context.Background()
	id := runIDForTest(t)

	// Seed revision 0 so both racers load the same Latest (rev 0) and both try
	// to append rev 1 — the classic concurrent-resume race.
	if err := store.Append(ctx, cpForTest(id, 0)); err != nil {
		t.Fatalf("seed Append error = %v", err)
	}

	const rounds = 200
	for round := 0; round < rounds; round++ {
		// Both goroutines load Latest (each sees the same revision N) and attempt
		// to append N+1. Exactly one must win.
		latest, err := store.Latest(ctx, id)
		if err != nil {
			t.Fatalf("Latest() error = %v", err)
		}
		next := latest.Run.Revision + 1

		var (
			start    sync.WaitGroup
			done     sync.WaitGroup
			wins     int32
			conflict int32
		)
		start.Add(1)
		done.Add(2)
		for g := 0; g < 2; g++ {
			go func() {
				defer done.Done()
				start.Wait()
				err := store.Append(ctx, cpForTest(id, next))
				if err == nil {
					atomic.AddInt32(&wins, 1)
					return
				}
				var rc *RevisionConflictError
				if errors.As(err, &rc) {
					atomic.AddInt32(&conflict, 1)
					return
				}
				t.Errorf("unexpected Append error = %v", err)
			}()
		}
		start.Done() // release both at once to maximize the race
		done.Wait()

		if wins != 1 {
			t.Fatalf("round %d: winners = %d, want exactly 1", round, wins)
		}
		if conflict != 1 {
			t.Fatalf("round %d: conflicts = %d, want exactly 1", round, conflict)
		}
	}

	// No fork: history advanced by exactly `rounds` and revisions are unique 0..rounds.
	hist, err := store.History(ctx, id)
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	if len(hist) != rounds+1 {
		t.Fatalf("len(History()) = %d, want %d (no fork)", len(hist), rounds+1)
	}
	seen := make(map[uint64]bool, len(hist))
	for i, cp := range hist {
		if cp.Run.Revision != uint64(i) {
			t.Errorf("History()[%d].Run.Revision = %d, want %d (must stay ordered)", i, cp.Run.Revision, i)
		}
		if seen[cp.Run.Revision] {
			t.Errorf("duplicate revision %d in history ((GraphRunID,Revision) must be unique)", cp.Run.Revision)
		}
		seen[cp.Run.Revision] = true
	}
}

func TestMemStoreSingleCoordinatorNeverConflicts(t *testing.T) {
	t.Parallel()
	store := NewMemStore()
	ctx := context.Background()
	id := runIDForTest(t)

	const k = 50
	for rev := uint64(0); rev <= k; rev++ {
		if err := store.Append(ctx, cpForTest(id, rev)); err != nil {
			t.Fatalf("Append(rev=%d) by single coordinator error = %v", rev, err)
		}
	}
	latest, err := store.Latest(ctx, id)
	if err != nil {
		t.Fatalf("Latest() error = %v", err)
	}
	if latest.Run.Revision != k {
		t.Errorf("Latest().Run.Revision = %d, want %d", latest.Run.Revision, k)
	}
}

func TestMemStoreImmutabilityInputSide(t *testing.T) {
	t.Parallel()
	store := NewMemStore()
	ctx := context.Background()
	id := runIDForTest(t)

	cp := cpForTest(id, 0)
	cp.Frontier = []VertexID{}
	if err := store.Append(ctx, cp); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Mutate cp AFTER Append — stored history must be unaffected.
	cp.Run.Step = 999
	cp.State = json.RawMessage(`{"count":999}`)
	cp.Frontier = append(cp.Frontier, VertexID{})
	cp.Vertices[0].Status = VertexFailed

	latest, err := store.Latest(ctx, id)
	if err != nil {
		t.Fatalf("Latest() error = %v", err)
	}
	if latest.Run.Step != 0 {
		t.Errorf("latest.Run.Step = %d, want 0 (post-Append mutation leaked in)", latest.Run.Step)
	}
	if string(latest.State) != `{"count":0}` {
		t.Errorf("latest.State = %s, want {\"count\":0}", latest.State)
	}
	if len(latest.Frontier) != 0 {
		t.Errorf("len(latest.Frontier) = %d, want 0 (post-Append append leaked in)", len(latest.Frontier))
	}
	if latest.Vertices[0].Status != VertexDone {
		t.Errorf("latest.Vertices[0].Status = %v, want VertexDone", latest.Vertices[0].Status)
	}
}

func TestMemStoreImmutabilityOutputSide(t *testing.T) {
	t.Parallel()
	store := NewMemStore()
	ctx := context.Background()
	id := runIDForTest(t)
	if err := store.Append(ctx, cpForTest(id, 0)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	// Mutate the value returned by Latest — a second Latest must be unchanged.
	got1, err := store.Latest(ctx, id)
	if err != nil {
		t.Fatalf("Latest() error = %v", err)
	}
	got1.Run.Step = 999
	got1.State = json.RawMessage(`{"count":999}`)
	got1.Frontier = append(got1.Frontier, VertexID{})
	got1.Vertices[0].Status = VertexFailed

	got2, err := store.Latest(ctx, id)
	if err != nil {
		t.Fatalf("Latest() second call error = %v", err)
	}
	if got2.Run.Step != 0 || string(got2.State) != `{"count":0}` || len(got2.Frontier) != 0 || got2.Vertices[0].Status != VertexDone {
		t.Errorf("mutating Latest()'s result corrupted stored history: %+v", got2)
	}

	// Same guarantee for History.
	h1, err := store.History(ctx, id)
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	h1[0].Run.Step = 777
	h1[0].State = json.RawMessage(`{"count":777}`)
	h1[0].Vertices[0].Status = VertexFailed

	h2, err := store.History(ctx, id)
	if err != nil {
		t.Fatalf("History() second call error = %v", err)
	}
	if h2[0].Run.Step != 0 || string(h2[0].State) != `{"count":0}` || h2[0].Vertices[0].Status != VertexDone {
		t.Errorf("mutating History()'s result corrupted stored history: %+v", h2[0])
	}
}

func TestMemStoreUnknownRun(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		op   string
	}{
		{name: "Latest on unknown run", op: "Latest"},
		{name: "History on unknown run", op: "History"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := NewMemStore()
			ctx := context.Background()
			id := runIDForTest(t)

			var err error
			switch tt.op {
			case "Latest":
				_, err = store.Latest(ctx, id)
			case "History":
				_, err = store.History(ctx, id)
			}
			var nf *CheckpointNotFoundError
			if !errors.As(err, &nf) {
				t.Fatalf("%s on unknown run error = %v, want *CheckpointNotFoundError", tt.op, err)
			}
			if nf.GraphRunID != id {
				t.Errorf("nf.GraphRunID = %v, want %v", nf.GraphRunID, id)
			}
		})
	}
}

func TestMemStoreContextCancelled(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		op   string
	}{
		{name: "Append honors cancelled ctx", op: "Append"},
		{name: "Latest honors cancelled ctx", op: "Latest"},
		{name: "History honors cancelled ctx", op: "History"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := NewMemStore()
			id := runIDForTest(t)
			// Seed one checkpoint with a live ctx so Latest/History have data to
			// otherwise return — proving the cancellation, not emptiness, aborts.
			if err := store.Append(context.Background(), cpForTest(id, 0)); err != nil {
				t.Fatalf("seed Append error = %v", err)
			}

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			var err error
			switch tt.op {
			case "Append":
				err = store.Append(ctx, cpForTest(id, 1))
			case "Latest":
				_, err = store.Latest(ctx, id)
			case "History":
				_, err = store.History(ctx, id)
			}
			var se *StoreError
			if !errors.As(err, &se) {
				t.Fatalf("%s with cancelled ctx error = %v, want *StoreError", tt.op, err)
			}
			if se.Op != tt.op {
				t.Errorf("se.Op = %q, want %q", se.Op, tt.op)
			}
			if !errors.Is(err, context.Canceled) {
				t.Errorf("errors.Is(err, context.Canceled) = false, want true (err = %v)", err)
			}
		})
	}
}

func TestMemStoreMultipleRunsIsolated(t *testing.T) {
	t.Parallel()
	store := NewMemStore()
	ctx := context.Background()
	idA := runIDForTest(t)
	idB := runIDForTest(t)

	if err := store.Append(ctx, cpForTest(idA, 0)); err != nil {
		t.Fatalf("Append(A,0) error = %v", err)
	}
	if err := store.Append(ctx, cpForTest(idA, 1)); err != nil {
		t.Fatalf("Append(A,1) error = %v", err)
	}
	if err := store.Append(ctx, cpForTest(idB, 0)); err != nil {
		t.Fatalf("Append(B,0) error = %v", err)
	}

	histA, err := store.History(ctx, idA)
	if err != nil {
		t.Fatalf("History(A) error = %v", err)
	}
	histB, err := store.History(ctx, idB)
	if err != nil {
		t.Fatalf("History(B) error = %v", err)
	}
	if len(histA) != 2 {
		t.Errorf("len(History(A)) = %d, want 2", len(histA))
	}
	if len(histB) != 1 {
		t.Errorf("len(History(B)) = %d, want 1", len(histB))
	}
	for _, cp := range histA {
		if cp.Run.GraphRunID != idA {
			t.Errorf("History(A) contains foreign run %v", cp.Run.GraphRunID)
		}
	}
	if histB[0].Run.GraphRunID != idB {
		t.Errorf("History(B)[0] run = %v, want %v", histB[0].Run.GraphRunID, idB)
	}
}

func TestMemStoreRoundTripFidelity(t *testing.T) {
	t.Parallel()
	store := NewMemStore()
	ctx := context.Background()
	id := runIDForTest(t)

	want := cpForTest(id, 0)
	if err := store.Append(ctx, want); err != nil {
		t.Fatalf("Append() error = %v", err)
	}
	got, err := store.Latest(ctx, id)
	if err != nil {
		t.Fatalf("Latest() error = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("round-trip mismatch:\n got = %+v\nwant = %+v", got, want)
	}
}

func TestMemStoreAppendMarshalFailure(t *testing.T) {
	t.Parallel()
	store := NewMemStore()
	ctx := context.Background()
	id := runIDForTest(t)

	// A json.RawMessage carrying INVALID JSON makes json.Marshal fail, surfacing
	// a non-serializable checkpoint early as a StoreError — and the bad append
	// must not mutate stored history.
	cp := cpForTest(id, 0)
	cp.State = json.RawMessage(`{not valid json`)

	err := store.Append(ctx, cp)
	var se *StoreError
	if !errors.As(err, &se) {
		t.Fatalf("Append() with unserializable checkpoint error = %v, want *StoreError", err)
	}
	if se.Op != "Append" {
		t.Errorf("se.Op = %q, want \"Append\"", se.Op)
	}
	if _, herr := store.History(ctx, id); !errors.As(herr, new(*CheckpointNotFoundError)) {
		t.Errorf("History() after failed Append error = %v, want *CheckpointNotFoundError (no partial write)", herr)
	}
}

func TestMemStoreDecodeFailureOnRead(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		op   string
	}{
		{name: "Latest surfaces decode failure", op: "Latest"},
		{name: "History surfaces decode failure", op: "History"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := NewMemStore()
			ctx := context.Background()
			id := runIDForTest(t)
			// White-box: plant corrupt bytes directly so the read-side decode path
			// fails — proving Latest/History return a StoreError, not a panic, on a
			// store holding garbage (fail secure on untrusted stored data).
			store.mu.Lock()
			store.runs[id] = [][]byte{[]byte(`{not valid json`)}
			store.mu.Unlock()

			var err error
			switch tt.op {
			case "Latest":
				_, err = store.Latest(ctx, id)
			case "History":
				_, err = store.History(ctx, id)
			}
			var se *StoreError
			if !errors.As(err, &se) {
				t.Fatalf("%s on corrupt store error = %v, want *StoreError", tt.op, err)
			}
			if se.Op != tt.op {
				t.Errorf("se.Op = %q, want %q", se.Op, tt.op)
			}
		})
	}
}

// memStoreSatisfiesInterface is a compile-time assertion that *MemStore
// implements CheckpointStore (Liskov: a concrete type honors the full contract).
var _ CheckpointStore = (*MemStore)(nil)
