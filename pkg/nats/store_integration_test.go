//go:build integration

package nats

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ciram-co/flow/pkg/flow"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// This suite holds the JetStream-backed Store to the SAME CheckpointStore
// contract that pkg/flow/store_test.go holds MemStore to (design §10.2): append-
// only compare-and-append on (GraphRunID, Revision), Latest = highest revision,
// ordered History, structural immutability, CheckpointNotFoundError for unknown
// runs, and — the distributed crux — concurrent-resume DETECTION (two appenders
// of the same next revision: exactly one wins, the other gets a
// RevisionConflictError). It boots ONE in-process JetStream server and gives each
// subtest a fresh GraphRunID (subject-per-run) so subtests run race-clean in
// parallel against the shared server.

// testStore boots a shared embedded server once per top-level test and returns a
// Store over an in-process connection, registering cleanup.
func testStore(t *testing.T) *Store {
	t.Helper()
	store, _ := testStoreWithConn(t)
	return store
}

// testStoreWithConn is like testStore but also returns the underlying connection,
// so corrupt-store tests can publish hand-crafted bad messages straight onto a
// run's subject (bypassing the Store's invariants) and then assert the Store's
// defensive read behavior.
func testStoreWithConn(t *testing.T) (*Store, *nats.Conn) {
	t.Helper()
	srv, err := Embedded(WithStoreDir(t.TempDir()), WithReadyTimeout(15*time.Second))
	if err != nil {
		t.Fatalf("Embedded() error = %v", err)
	}
	t.Cleanup(srv.Close)

	nc, err := srv.InProcessConn()
	if err != nil {
		t.Fatalf("InProcessConn() error = %v", err)
	}
	t.Cleanup(nc.Close)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	store, err := NewStore(ctx, nc)
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	return store, nc
}

// publishRaw puts arbitrary bytes onto a run's checkpoint subject via JetStream,
// optionally carrying a Flow-Revision header. It is the test's way to plant
// corruption that the Store must defend against on read.
func publishRaw(t *testing.T, nc *nats.Conn, id flow.GraphRunID, data []byte, revHeader string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}
	subj := subjectPrefix + id.String()
	msg := &nats.Msg{Subject: subj, Data: data}
	if revHeader != "" {
		msg.Header = nats.Header{revisionHeader: []string{revHeader}}
	}
	if _, err := js.PublishMsg(ctx, msg); err != nil {
		t.Fatalf("publishRaw PublishMsg() error = %v", err)
	}
}

func runIDForTest(t *testing.T) flow.GraphRunID {
	t.Helper()
	id, err := flow.NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID() error = %v", err)
	}
	return id
}

func cpForTest(id flow.GraphRunID, rev uint64) *flow.Checkpoint {
	return &flow.Checkpoint{
		Run: flow.GraphRunState{
			GraphRunID: id,
			Status:     flow.RunRunning,
			Step:       flow.StepID(rev),
			Revision:   rev,
		},
		State:    json.RawMessage(`{"count":` + strconv.FormatUint(rev, 10) + `}`),
		Vertices: []flow.VertexState{{Status: flow.VertexDone, Attempt: 1}},
		Frontier: []flow.VertexID{},
		Phase:    flow.StepRunning,
	}
}

func TestStoreSequentialAppend(t *testing.T) {
	t.Parallel()
	store := testStore(t)
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

func TestStoreCompareAndAppendRejects(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		seed         []uint64
		bad          uint64
		wantExpected uint64
		wantActual   uint64
	}{
		{name: "first append must be revision 0", seed: nil, bad: 1, wantExpected: 0, wantActual: 1},
		{name: "duplicate revision 0", seed: []uint64{0}, bad: 0, wantExpected: 1, wantActual: 0},
		{name: "gap: revision 2 when latest is 0", seed: []uint64{0}, bad: 2, wantExpected: 1, wantActual: 2},
		{name: "stale next: revision 1 when latest is 1", seed: []uint64{0, 1}, bad: 1, wantExpected: 2, wantActual: 1},
	}
	store := testStore(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			id := runIDForTest(t)
			for _, rev := range tt.seed {
				if err := store.Append(ctx, cpForTest(id, rev)); err != nil {
					t.Fatalf("seed Append(rev=%d) error = %v", rev, err)
				}
			}

			err := store.Append(ctx, cpForTest(id, tt.bad))
			var conflict *flow.RevisionConflictError
			if !errors.As(err, &conflict) {
				t.Fatalf("Append(rev=%d) error = %v, want *flow.RevisionConflictError", tt.bad, err)
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

			hist, herr := store.History(ctx, id)
			if len(tt.seed) == 0 {
				var nf *flow.CheckpointNotFoundError
				if !errors.As(herr, &nf) {
					t.Fatalf("History() after rejected first append error = %v, want *flow.CheckpointNotFoundError", herr)
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

func TestStoreConcurrentResumeDetected(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	ctx := context.Background()
	id := runIDForTest(t)

	// Seed revision 0 so both racers load the same Latest and both try to append
	// rev 1 — the classic concurrent-resume race. On a durable backend, the
	// server's per-subject expected-last-sequence gate is the authoritative
	// arbiter: exactly one publish wins, the other is rejected as a conflict.
	if err := store.Append(ctx, cpForTest(id, 0)); err != nil {
		t.Fatalf("seed Append error = %v", err)
	}

	const rounds = 25
	for round := 0; round < rounds; round++ {
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
				var rc *flow.RevisionConflictError
				if errors.As(err, &rc) {
					atomic.AddInt32(&conflict, 1)
					return
				}
				t.Errorf("unexpected Append error = %v", err)
			}()
		}
		start.Done()
		done.Wait()

		if wins != 1 {
			t.Fatalf("round %d: winners = %d, want exactly 1", round, wins)
		}
		if conflict != 1 {
			t.Fatalf("round %d: conflicts = %d, want exactly 1", round, conflict)
		}
	}

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

func TestStoreImmutabilityInputSide(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	ctx := context.Background()
	id := runIDForTest(t)

	cp := cpForTest(id, 0)
	if err := store.Append(ctx, cp); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	cp.Run.Step = 999
	cp.State = json.RawMessage(`{"count":999}`)
	cp.Frontier = append(cp.Frontier, flow.VertexID{})
	cp.Vertices[0].Status = flow.VertexFailed

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
		t.Errorf("len(latest.Frontier) = %d, want 0", len(latest.Frontier))
	}
	if latest.Vertices[0].Status != flow.VertexDone {
		t.Errorf("latest.Vertices[0].Status = %v, want VertexDone", latest.Vertices[0].Status)
	}
}

func TestStoreImmutabilityOutputSide(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	ctx := context.Background()
	id := runIDForTest(t)
	if err := store.Append(ctx, cpForTest(id, 0)); err != nil {
		t.Fatalf("Append() error = %v", err)
	}

	got1, err := store.Latest(ctx, id)
	if err != nil {
		t.Fatalf("Latest() error = %v", err)
	}
	got1.Run.Step = 999
	got1.State = json.RawMessage(`{"count":999}`)
	got1.Frontier = append(got1.Frontier, flow.VertexID{})
	got1.Vertices[0].Status = flow.VertexFailed

	got2, err := store.Latest(ctx, id)
	if err != nil {
		t.Fatalf("Latest() second call error = %v", err)
	}
	if got2.Run.Step != 0 || string(got2.State) != `{"count":0}` || len(got2.Frontier) != 0 || got2.Vertices[0].Status != flow.VertexDone {
		t.Errorf("mutating Latest()'s result corrupted stored history: %+v", got2)
	}

	h1, err := store.History(ctx, id)
	if err != nil {
		t.Fatalf("History() error = %v", err)
	}
	h1[0].Run.Step = 777
	h1[0].State = json.RawMessage(`{"count":777}`)
	h1[0].Vertices[0].Status = flow.VertexFailed

	h2, err := store.History(ctx, id)
	if err != nil {
		t.Fatalf("History() second call error = %v", err)
	}
	if h2[0].Run.Step != 0 || string(h2[0].State) != `{"count":0}` || h2[0].Vertices[0].Status != flow.VertexDone {
		t.Errorf("mutating History()'s result corrupted stored history: %+v", h2[0])
	}
}

func TestStoreUnknownRun(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		op   string
	}{
		{name: "Latest on unknown run", op: "Latest"},
		{name: "History on unknown run", op: "History"},
	}
	store := testStore(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := context.Background()
			id := runIDForTest(t)

			var err error
			switch tt.op {
			case "Latest":
				_, err = store.Latest(ctx, id)
			case "History":
				_, err = store.History(ctx, id)
			}
			var nf *flow.CheckpointNotFoundError
			if !errors.As(err, &nf) {
				t.Fatalf("%s on unknown run error = %v, want *flow.CheckpointNotFoundError", tt.op, err)
			}
			if nf.GraphRunID != id {
				t.Errorf("nf.GraphRunID = %v, want %v", nf.GraphRunID, id)
			}
		})
	}
}

func TestStoreMultipleRunsIsolated(t *testing.T) {
	t.Parallel()
	store := testStore(t)
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

func TestStoreRoundTripFidelity(t *testing.T) {
	t.Parallel()
	store := testStore(t)
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

func TestStoreAppendMarshalFailure(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	ctx := context.Background()
	id := runIDForTest(t)

	cp := cpForTest(id, 0)
	cp.State = json.RawMessage(`{not valid json`)

	err := store.Append(ctx, cp)
	var se *flow.StoreError
	if !errors.As(err, &se) {
		t.Fatalf("Append() with unserializable checkpoint error = %v, want *flow.StoreError", err)
	}
	if se.Op != "Append" {
		t.Errorf("se.Op = %q, want \"Append\"", se.Op)
	}
	if _, herr := store.History(ctx, id); !errors.As(herr, new(*flow.CheckpointNotFoundError)) {
		t.Errorf("History() after failed Append error = %v, want *flow.CheckpointNotFoundError (no partial write)", herr)
	}
}

func TestStoreContextCancelled(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		op   string
	}{
		{name: "Append honors cancelled ctx", op: "Append"},
		{name: "Latest honors cancelled ctx", op: "Latest"},
		{name: "History honors cancelled ctx", op: "History"},
	}
	store := testStore(t)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			id := runIDForTest(t)
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
			var se *flow.StoreError
			if !errors.As(err, &se) {
				t.Fatalf("%s with cancelled ctx error = %v, want *flow.StoreError", tt.op, err)
			}
			if se.Op != tt.op {
				t.Errorf("se.Op = %q, want %q", se.Op, tt.op)
			}
		})
	}
}

func TestStoreZeroRunIDRejected(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	ctx := context.Background()
	var zero flow.GraphRunID

	tests := []struct {
		name string
		op   string
	}{
		{name: "Append rejects zero id", op: "Append"},
		{name: "Latest rejects zero id", op: "Latest"},
		{name: "History rejects zero id", op: "History"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var err error
			switch tt.op {
			case "Append":
				err = store.Append(ctx, cpForTest(zero, 0))
			case "Latest":
				_, err = store.Latest(ctx, zero)
			case "History":
				_, err = store.History(ctx, zero)
			}
			var se *flow.StoreError
			if !errors.As(err, &se) {
				t.Fatalf("%s with zero id error = %v, want *flow.StoreError", tt.op, err)
			}
		})
	}
}

func TestStoreMaxMsgSizeEnforced(t *testing.T) {
	t.Parallel()
	srv, err := Embedded(WithStoreDir(t.TempDir()))
	if err != nil {
		t.Fatalf("Embedded() error = %v", err)
	}
	t.Cleanup(srv.Close)
	nc, err := srv.InProcessConn()
	if err != nil {
		t.Fatalf("InProcessConn() error = %v", err)
	}
	t.Cleanup(nc.Close)

	ctx := context.Background()
	store, err := NewStore(ctx, nc, WithMaxMsgSize(256))
	if err != nil {
		t.Fatalf("NewStore() error = %v", err)
	}
	id := runIDForTest(t)
	cp := cpForTest(id, 0)
	big := make([]byte, 1024)
	for i := range big {
		big[i] = 'a'
	}
	cp.State = json.RawMessage(`"` + string(big) + `"`)

	err = store.Append(ctx, cp)
	var se *flow.StoreError
	if !errors.As(err, &se) {
		t.Fatalf("Append() of oversized checkpoint error = %v, want *flow.StoreError", err)
	}
}

// TestStoreCorruptBodyOnRead plants an undecodable body directly on a run's
// subject and asserts BOTH Latest and History fail with a typed *flow.StoreError
// wrapping a *flow.CheckpointDecodeError (untrusted stored bytes, §10.4).
func TestStoreCorruptBodyOnRead(t *testing.T) {
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
			store, nc := testStoreWithConn(t)
			ctx := context.Background()
			id := runIDForTest(t)
			// A revision-0 header so History's count derivation does not short out
			// before it ever decodes the (corrupt) body.
			publishRaw(t, nc, id, []byte(`{not valid json`), "0")

			var err error
			switch tt.op {
			case "Latest":
				_, err = store.Latest(ctx, id)
			case "History":
				_, err = store.History(ctx, id)
			}
			var se *flow.StoreError
			if !errors.As(err, &se) {
				t.Fatalf("%s on corrupt body error = %v, want *flow.StoreError", tt.op, err)
			}
			if se.Op != tt.op {
				t.Errorf("se.Op = %q, want %q", se.Op, tt.op)
			}
			var de *flow.CheckpointDecodeError
			if !errors.As(err, &de) {
				t.Fatalf("%s on corrupt body error = %v, want it to wrap *flow.CheckpointDecodeError", tt.op, err)
			}
		})
	}
}

// TestStoreForeignRunIDOnRead plants a well-formed checkpoint whose embedded run
// id belongs to a DIFFERENT run on the subject of id, and asserts Latest/History
// reject it as a *flow.GraphRunMismatchError (misrouted/corrupt store, §10.4).
func TestStoreForeignRunIDOnRead(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		op   string
	}{
		{name: "Latest detects foreign run id", op: "Latest"},
		{name: "History detects foreign run id", op: "History"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store, nc := testStoreWithConn(t)
			ctx := context.Background()
			id := runIDForTest(t)
			foreign := runIDForTest(t)
			// A valid checkpoint body, but its Run.GraphRunID is `foreign`, planted on
			// id's subject. Revision 0 so History attempts to read it.
			body, err := json.Marshal(cpForTest(foreign, 0))
			if err != nil {
				t.Fatalf("marshal foreign cp error = %v", err)
			}
			publishRaw(t, nc, id, body, "0")

			switch tt.op {
			case "Latest":
				_, err = store.Latest(ctx, id)
			case "History":
				_, err = store.History(ctx, id)
			}
			var mm *flow.GraphRunMismatchError
			if !errors.As(err, &mm) {
				t.Fatalf("%s on foreign run id error = %v, want *flow.GraphRunMismatchError", tt.op, err)
			}
			if mm.Requested != id || mm.Actual != foreign {
				t.Errorf("mismatch = {Requested:%v Actual:%v}, want {Requested:%v Actual:%v}", mm.Requested, mm.Actual, id, foreign)
			}
			var se *flow.StoreError
			if !errors.As(err, &se) {
				t.Errorf("%s on foreign run id error = %v, want it wrapped in *flow.StoreError", tt.op, err)
			}
		})
	}
}

// TestStoreHistoryUnderReportingHeaderDefended is the High-1 regression test: a
// run with TWO committed revisions whose LAST message carries a LYING
// Flow-Revision header ("0", under-reporting the real revision 1). A backend that
// trusted the header would compute want=1 and silently return a TRUNCATED history
// of just revision 0. History must instead derive its count from the body and/or
// drain-check, so it either returns the full 2-revision history or fails with a
// corrupt-store error — but NEVER a silent prefix.
func TestStoreHistoryUnderReportingHeaderDefended(t *testing.T) {
	t.Parallel()
	store, nc := testStoreWithConn(t)
	ctx := context.Background()
	id := runIDForTest(t)

	// Commit revision 0 normally.
	if err := store.Append(ctx, cpForTest(id, 0)); err != nil {
		t.Fatalf("Append(rev=0) error = %v", err)
	}
	// Plant revision 1 with a LYING header claiming revision 0. Body is honest
	// (revision 1) so the server's per-subject append accepts it as the 2nd msg.
	body, err := json.Marshal(cpForTest(id, 1))
	if err != nil {
		t.Fatalf("marshal rev1 error = %v", err)
	}
	publishRaw(t, nc, id, body, "0") // header lies: says rev 0

	hist, err := store.History(ctx, id)
	if err != nil {
		// Acceptable: a corrupt-store error. What is NOT acceptable is a silent
		// truncated success — that is asserted in the success branch below.
		var se *flow.StoreError
		if !errors.As(err, &se) {
			t.Fatalf("History() error = %v, want either full history or a *flow.StoreError", err)
		}
		return
	}
	if len(hist) != 2 {
		t.Fatalf("History() silently returned %d revisions, want 2 (header under-report must NOT truncate)", len(hist))
	}
	for i, cp := range hist {
		if cp.Run.Revision != uint64(i) {
			t.Errorf("History()[%d].Run.Revision = %d, want %d", i, cp.Run.Revision, i)
		}
	}
}

// TestStoreTwoFirstAppendsRaceRev0 (Med-3) races TWO appends of revision 0 (the
// empty-subject case: expectedSeq=0). Exactly one must win; the other must get a
// *flow.RevisionConflictError. This exercises the server gate at the bootstrap
// boundary where expectedSeq is 0.
func TestStoreTwoFirstAppendsRaceRev0(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	ctx := context.Background()

	const rounds = 20
	for round := 0; round < rounds; round++ {
		id := runIDForTest(t)
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
				err := store.Append(ctx, cpForTest(id, 0))
				if err == nil {
					atomic.AddInt32(&wins, 1)
					return
				}
				var rc *flow.RevisionConflictError
				if errors.As(err, &rc) {
					atomic.AddInt32(&conflict, 1)
					return
				}
				t.Errorf("unexpected Append error = %v", err)
			}()
		}
		start.Done()
		done.Wait()

		if wins != 1 {
			t.Fatalf("round %d: winners = %d, want exactly 1", round, wins)
		}
		if conflict != 1 {
			t.Fatalf("round %d: conflicts = %d, want exactly 1", round, conflict)
		}
	}
}

// TestStoreHistoryHonorsCancelledCtx asserts History aborts promptly on a ctx that
// is already cancelled — the High-2 fix threads ctx into Fetch so a deadline is
// honored mid-pull, not just between rounds.
func TestStoreHistoryHonorsCancelledCtx(t *testing.T) {
	t.Parallel()
	store := testStore(t)
	id := runIDForTest(t)
	if err := store.Append(context.Background(), cpForTest(id, 0)); err != nil {
		t.Fatalf("seed Append error = %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := store.History(ctx, id)
	var se *flow.StoreError
	if !errors.As(err, &se) {
		t.Fatalf("History() with cancelled ctx error = %v, want *flow.StoreError", err)
	}
	if se.Op != "History" {
		t.Errorf("se.Op = %q, want \"History\"", se.Op)
	}
}

// TestStoreHistoryDeletesConsumer (High-3) asserts History does NOT leak the
// ephemeral ordered consumer it creates: after several History calls the stream
// has zero consumers (each call deletes its own on exit, rather than leaving it to
// time out after minutes).
func TestStoreHistoryDeletesConsumer(t *testing.T) {
	t.Parallel()
	store, nc := testStoreWithConn(t)
	ctx := context.Background()
	id := runIDForTest(t)
	for rev := uint64(0); rev < 3; rev++ {
		if err := store.Append(ctx, cpForTest(id, rev)); err != nil {
			t.Fatalf("Append(rev=%d) error = %v", rev, err)
		}
	}

	for i := 0; i < 5; i++ {
		if _, err := store.History(ctx, id); err != nil {
			t.Fatalf("History() call %d error = %v", i, err)
		}
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New() error = %v", err)
	}
	stream, err := js.Stream(ctx, defaultStreamName)
	if err != nil {
		t.Fatalf("Stream() error = %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("Stream.Info() error = %v", err)
	}
	if info.State.Consumers != 0 {
		t.Errorf("stream has %d consumers after History calls, want 0 (consumer leak)", info.State.Consumers)
	}
}

// satisfiesInterface is a compile-time assertion that *Store implements
// flow.CheckpointStore (Liskov: a durable backend honors the full contract).
var _ flow.CheckpointStore = (*Store)(nil)
