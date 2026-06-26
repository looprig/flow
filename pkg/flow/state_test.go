package flow

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/ciram-co/flow/pkg/uuid"
)

// TestRunStatusValues pins the iota ordering of RunStatus. These integer values
// are persisted in checkpoints (§4.1), so an accidental reorder would silently
// reinterpret historical runs — this guards against that.
func TestRunStatusValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		got  RunStatus
		want int
	}{
		{name: "RunRunning is 0", got: RunRunning, want: 0},
		{name: "RunCompleted is 1", got: RunCompleted, want: 1},
		{name: "RunInterrupted is 2", got: RunInterrupted, want: 2},
		{name: "RunCancelled is 3", got: RunCancelled, want: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if int(tt.got) != tt.want {
				t.Errorf("RunStatus = %d, want %d", int(tt.got), tt.want)
			}
		})
	}
}

// TestVertexStatusValues pins the iota ordering of VertexStatus. Persisted in
// checkpoints (§4.1); a reorder must not silently change meaning.
func TestVertexStatusValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		got  VertexStatus
		want int
	}{
		{name: "VertexPending is 0", got: VertexPending, want: 0},
		{name: "VertexRunning is 1", got: VertexRunning, want: 1},
		{name: "VertexDone is 2", got: VertexDone, want: 2},
		{name: "VertexInterrupted is 3", got: VertexInterrupted, want: 3},
		{name: "VertexFailed is 4", got: VertexFailed, want: 4},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if int(tt.got) != tt.want {
				t.Errorf("VertexStatus = %d, want %d", int(tt.got), tt.want)
			}
		})
	}
}

// graphIDLiteral / runIDLiteral / vertexIDLiteral / vertexRunIDLiteral are
// distinct pinned UUID literals so round-trip and IdempotencyKey assertions can
// tell the fields apart in a single fixture.
const (
	graphIDLiteral     = "8f14e45f-ceea-467a-9e8b-2c5f9b6a1d33"
	runIDLiteral       = "11111111-1111-4111-8111-111111111111"
	vertexIDLiteral    = "22222222-2222-4222-8222-222222222222"
	vertexRunIDLiteral = "33333333-3333-4333-8333-333333333333"
	altVertexRunLit    = "44444444-4444-4444-8444-444444444444"
)

// TestGraphRunStateJSONRoundTrip asserts a fully-populated GraphRunState (every
// field set, distinct timestamps) survives Marshal→Unmarshal with full equality,
// and that the zero value round-trips with zero-value time.Time meaning "not
// reached" (§4.1).
func TestGraphRunStateJSONRoundTrip(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 6, 26, 10, 0, 0, 0, time.UTC)
	full := GraphRunState{
		GraphRunID:    GraphRunID(uuid.MustParse(runIDLiteral)),
		GraphID:       GraphID(uuid.MustParse(graphIDLiteral)),
		GraphVersion:  "sha256-abc:7",
		Status:        RunInterrupted,
		Step:          StepID(42),
		Revision:      uint64(99),
		CreatedAt:     base,
		StartedAt:     base.Add(1 * time.Second),
		UpdatedAt:     base.Add(2 * time.Second),
		CompletedAt:   base.Add(3 * time.Second),
		InterruptedAt: base.Add(4 * time.Second),
		CancelledAt:   base.Add(5 * time.Second),
		CancelReason:  "operator requested",
	}

	tests := []struct {
		name string
		in   GraphRunState
	}{
		{name: "fully populated", in: full},
		{name: "zero value", in: GraphRunState{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b, err := json.Marshal(tt.in)
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}
			var got GraphRunState
			if err := json.Unmarshal(b, &got); err != nil {
				t.Fatalf("json.Unmarshal() error = %v", err)
			}
			if !got.CreatedAt.Equal(tt.in.CreatedAt) ||
				!got.StartedAt.Equal(tt.in.StartedAt) ||
				!got.UpdatedAt.Equal(tt.in.UpdatedAt) ||
				!got.CompletedAt.Equal(tt.in.CompletedAt) ||
				!got.InterruptedAt.Equal(tt.in.InterruptedAt) ||
				!got.CancelledAt.Equal(tt.in.CancelledAt) {
				t.Fatalf("timestamp mismatch: got %+v want %+v", got, tt.in)
			}
			// Normalize the monotonic-clock/location difference time.Time picks
			// up so the remaining whole-struct equality compares the rest.
			got.CreatedAt = tt.in.CreatedAt
			got.StartedAt = tt.in.StartedAt
			got.UpdatedAt = tt.in.UpdatedAt
			got.CompletedAt = tt.in.CompletedAt
			got.InterruptedAt = tt.in.InterruptedAt
			got.CancelledAt = tt.in.CancelledAt
			if got != tt.in {
				t.Errorf("round-trip = %+v, want %+v", got, tt.in)
			}
			if tt.name == "zero value" && !got.CompletedAt.IsZero() {
				t.Errorf("zero-value CompletedAt should report IsZero (not reached)")
			}
		})
	}
}

// TestVertexStateJSONRoundTrip is the VertexState analogue of the GraphRunState
// round-trip: full value survives with equality, zero value round-trips, and a
// zero-value terminal timestamp means "not reached" (§4.1).
func TestVertexStateJSONRoundTrip(t *testing.T) {
	t.Parallel()

	base := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)
	full := VertexState{
		VertexID:      VertexID(uuid.MustParse(vertexIDLiteral)),
		VertexRunID:   VertexRunID(uuid.MustParse(vertexRunIDLiteral)),
		Step:          StepID(7),
		Status:        VertexFailed,
		Attempt:       3,
		CreatedAt:     base,
		StartedAt:     base.Add(1 * time.Second),
		CompletedAt:   base.Add(2 * time.Second),
		InterruptedAt: base.Add(3 * time.Second),
		FailedAt:      base.Add(4 * time.Second),
		Err:           "boom",
	}

	tests := []struct {
		name string
		in   VertexState
	}{
		{name: "fully populated", in: full},
		{name: "zero value", in: VertexState{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b, err := json.Marshal(tt.in)
			if err != nil {
				t.Fatalf("json.Marshal() error = %v", err)
			}
			var got VertexState
			if err := json.Unmarshal(b, &got); err != nil {
				t.Fatalf("json.Unmarshal() error = %v", err)
			}
			if !got.CreatedAt.Equal(tt.in.CreatedAt) ||
				!got.StartedAt.Equal(tt.in.StartedAt) ||
				!got.CompletedAt.Equal(tt.in.CompletedAt) ||
				!got.InterruptedAt.Equal(tt.in.InterruptedAt) ||
				!got.FailedAt.Equal(tt.in.FailedAt) {
				t.Fatalf("timestamp mismatch: got %+v want %+v", got, tt.in)
			}
			got.CreatedAt = tt.in.CreatedAt
			got.StartedAt = tt.in.StartedAt
			got.CompletedAt = tt.in.CompletedAt
			got.InterruptedAt = tt.in.InterruptedAt
			got.FailedAt = tt.in.FailedAt
			if got != tt.in {
				t.Errorf("round-trip = %+v, want %+v", got, tt.in)
			}
			if tt.name == "zero value" && !got.FailedAt.IsZero() {
				t.Errorf("zero-value FailedAt should report IsZero (not reached)")
			}
		})
	}
}

// baseRunInfo is the reference RunInfo used by the IdempotencyKey table; each
// "changes" row mutates exactly one identity field relative to it.
func baseRunInfo() RunInfo {
	return RunInfo{
		GraphID:     GraphID(uuid.MustParse(graphIDLiteral)),
		GraphRunID:  GraphRunID(uuid.MustParse(runIDLiteral)),
		VertexID:    VertexID(uuid.MustParse(vertexIDLiteral)),
		VertexRunID: VertexRunID(uuid.MustParse(vertexRunIDLiteral)),
		Step:        StepID(5),
	}
}

// TestIdempotencyKey is the core behavior: the key identifies ONE logical
// execution. It must be stable across VertexRunID (the per-attempt field, §4.1),
// change when any of GraphID/GraphRunID/Step/VertexID changes, and match the
// exact §4.1 format for a known input.
func TestIdempotencyKey(t *testing.T) {
	t.Parallel()

	t.Run("excludes VertexRunID (stable across attempts)", func(t *testing.T) {
		t.Parallel()
		a := baseRunInfo()
		b := baseRunInfo()
		b.VertexRunID = VertexRunID(uuid.MustParse(altVertexRunLit))
		if a.VertexRunID == b.VertexRunID {
			t.Fatal("fixture error: VertexRunIDs should differ")
		}
		if a.IdempotencyKey() != b.IdempotencyKey() {
			t.Errorf("key changed with VertexRunID: %q vs %q",
				a.IdempotencyKey(), b.IdempotencyKey())
		}
	})

	t.Run("exact §4.1 format for known input", func(t *testing.T) {
		t.Parallel()
		want := IdempotencyKey("graph=" + graphIDLiteral +
			"/run=" + runIDLiteral +
			"/step=5" +
			"/vertex=" + vertexIDLiteral)
		if got := baseRunInfo().IdempotencyKey(); got != want {
			t.Errorf("IdempotencyKey() = %q, want %q", got, want)
		}
	})

	// "changes" rows: each mutates exactly one identity field and must yield a
	// key distinct from the base.
	changeTests := []struct {
		name   string
		mutate func(*RunInfo)
	}{
		{
			name:   "GraphID changes key",
			mutate: func(r *RunInfo) { r.GraphID = GraphID(uuid.MustParse(altVertexRunLit)) },
		},
		{
			name:   "GraphRunID changes key",
			mutate: func(r *RunInfo) { r.GraphRunID = GraphRunID(uuid.MustParse(altVertexRunLit)) },
		},
		{
			name:   "Step changes key",
			mutate: func(r *RunInfo) { r.Step = StepID(6) },
		},
		{
			name:   "VertexID changes key",
			mutate: func(r *RunInfo) { r.VertexID = VertexID(uuid.MustParse(altVertexRunLit)) },
		},
	}
	baseKey := baseRunInfo().IdempotencyKey()
	for _, tt := range changeTests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ri := baseRunInfo()
			tt.mutate(&ri)
			if ri.IdempotencyKey() == baseKey {
				t.Errorf("IdempotencyKey() unchanged after mutating field; got %q", ri.IdempotencyKey())
			}
		})
	}
}
