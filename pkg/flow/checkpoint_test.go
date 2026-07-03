package flow

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/looprig/flow/pkg/uuid"
)

// This file tests the checkpoint data types (design §10.1). A checkpoint is the
// engine's durable, append-only unit of execution and is RELOADED FROM AN
// UNTRUSTED STORE on resume (§10.4), so two contracts are pinned here:
//
//  1. Persisted enum ordering — StepPhase, InterruptKind, and HaltKind are
//     written as integers into checkpoints, so their iota order is a
//     compatibility contract; a reorder would silently reinterpret historical
//     runs (mirrors state.go's RunStatus/VertexStatus pins).
//  2. JSON round-trip equality — a Checkpoint survives Marshal→Unmarshal deeply
//     equal across all four Phases, and the json.RawMessage serialization-
//     boundary fields (StepBase/State/Info/Continuation) pass through untouched.
//
// The types are PLAIN data (no methods, no validation): runtime mutual-exclusion
// ENFORCEMENT of Interrupts-vs-Halt lives in §10.4 resume-validation, a later
// phase — here we only document via fixtures that a valid checkpoint sets at
// most one.

// sampleRun builds a representative, fully-populated GraphRunState for fixtures.
func sampleRun() GraphRunState {
	ts := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)
	return GraphRunState{
		GraphRunID:   GraphRunID(uuid.MustParse("11111111-1111-4111-8111-111111111111")),
		GraphID:      GraphID(uuid.MustParse("22222222-2222-4222-8222-222222222222")),
		GraphVersion: "v-abc123",
		Status:       RunRunning,
		Step:         3,
		Revision:     7,
		CreatedAt:    ts,
		StartedAt:    ts.Add(time.Second),
		UpdatedAt:    ts.Add(2 * time.Second),
	}
}

// sampleVertex builds a representative VertexState for fixtures.
func sampleVertex() VertexState {
	ts := time.Date(2026, 6, 26, 12, 0, 0, 0, time.UTC)
	return VertexState{
		VertexID:    vID(0x33),
		VertexRunID: VertexRunID(uuid.MustParse("44444444-4444-4444-8444-444444444444")),
		Step:        3,
		Status:      VertexDone,
		Attempt:     1,
		CreatedAt:   ts,
		StartedAt:   ts.Add(time.Second),
		CompletedAt: ts.Add(2 * time.Second),
	}
}

// fullInterruptCheckpoint is a fully-populated checkpoint in StepRunning with a
// plural Interrupts slice (one Awaiting carrying Info, one Errored carrying
// Cause + a StatefulInterrupt Continuation) and a Routes slice (one conditional,
// one static). Halt is nil — Interrupts XOR Halt.
func fullInterruptCheckpoint() Checkpoint {
	return Checkpoint{
		Run:      sampleRun(),
		StepBase: json.RawMessage(`{"counter":1,"label":"base"}`),
		State:    json.RawMessage(`{"counter":2,"label":"accumulated"}`),
		Vertices: []VertexState{sampleVertex()},
		Frontier: []VertexID{
			vID(0x33),
			vID(0x55),
		},
		Routes: []RouteRecord{
			{
				From:        vID(0x33),
				To:          []VertexID{vID(0x55)},
				Conditional: true,
			},
			{
				From:        vID(0x55),
				To:          []VertexID{vID(0x66)},
				Conditional: false,
			},
		},
		Phase: StepRunning,
		Interrupts: []InterruptRecord{
			{
				Vertex: vID(0x33),
				Kind:   Awaiting,
				Info:   json.RawMessage(`{"reason":"need approval","ticket":42}`),
			},
			{
				Vertex:       vID(0x55),
				Kind:         Errored,
				Cause:        "*flow.VertexError: boom",
				Continuation: json.RawMessage(`{"cursor":"page-2"}`),
			},
		},
	}
}

// haltedCheckpoint is a fully-populated checkpoint in StepHalted with Halt set
// and Interrupts nil — Interrupts XOR Halt.
func haltedCheckpoint() Checkpoint {
	cp := fullInterruptCheckpoint()
	cp.Phase = StepHalted
	cp.Interrupts = nil
	cp.Halt = &HaltRecord{
		Kind:  HaltMaxSteps,
		Step:  9,
		Cause: "step budget exceeded",
	}
	return cp
}

// routedCheckpoint is a checkpoint at a StepRouted boundary: Routes set,
// neither Interrupts nor Halt.
func routedCheckpoint() Checkpoint {
	cp := fullInterruptCheckpoint()
	cp.Phase = StepRouted
	cp.Interrupts = nil
	return cp
}

// pausedCheckpoint is a checkpoint at a StepPaused boundary: ≥1 Interrupts, no
// Halt.
func pausedCheckpoint() Checkpoint {
	cp := fullInterruptCheckpoint()
	cp.Phase = StepPaused
	return cp
}

// TestStepPhaseValues pins the iota ordering of StepPhase. These integers are
// persisted in checkpoints (§10.1), so a reorder would silently reinterpret
// historical runs.
func TestStepPhaseValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		got  StepPhase
		want int
	}{
		{name: "StepRunning is 0", got: StepRunning, want: 0},
		{name: "StepPaused is 1", got: StepPaused, want: 1},
		{name: "StepRouted is 2", got: StepRouted, want: 2},
		{name: "StepHalted is 3", got: StepHalted, want: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if int(tt.got) != tt.want {
				t.Errorf("StepPhase = %d, want %d", int(tt.got), tt.want)
			}
		})
	}
}

// TestInterruptKindValues pins the iota ordering of InterruptKind. Persisted in
// checkpoints (§10.1); reused by §10.3's public Interrupt (Phase 5).
func TestInterruptKindValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		got  InterruptKind
		want int
	}{
		{name: "Awaiting is 0", got: Awaiting, want: 0},
		{name: "Errored is 1", got: Errored, want: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if int(tt.got) != tt.want {
				t.Errorf("InterruptKind = %d, want %d", int(tt.got), tt.want)
			}
		})
	}
}

// TestHaltKindValues pins the iota ordering of HaltKind. Persisted in
// checkpoints (§10.1); a reorder would silently change a halt's meaning.
func TestHaltKindValues(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		got  HaltKind
		want int
	}{
		{name: "HaltCondition is 0", got: HaltCondition, want: 0},
		{name: "HaltUndeclaredTarget is 1", got: HaltUndeclaredTarget, want: 1},
		{name: "HaltDeadEnd is 2", got: HaltDeadEnd, want: 2},
		{name: "HaltMaxSteps is 3", got: HaltMaxSteps, want: 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if int(tt.got) != tt.want {
				t.Errorf("HaltKind = %d, want %d", int(tt.got), tt.want)
			}
		})
	}
}

// TestCheckpointRoundTrip asserts a Checkpoint survives Marshal→Unmarshal deeply
// equal across all four Phases, plus the zero-value Checkpoint. This is the
// durability "round-trip equality" contract (§10.1, §15).
func TestCheckpointRoundTrip(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		cp   Checkpoint
	}{
		{name: "StepRunning with plural Interrupts", cp: fullInterruptCheckpoint()},
		{name: "StepPaused with Interrupts", cp: pausedCheckpoint()},
		{name: "StepRouted no Interrupts no Halt", cp: routedCheckpoint()},
		{name: "StepHalted with Halt no Interrupts", cp: haltedCheckpoint()},
		{name: "zero Checkpoint", cp: Checkpoint{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b, err := json.Marshal(tt.cp)
			if err != nil {
				t.Fatalf("Marshal() error = %v", err)
			}
			var got Checkpoint
			if err := json.Unmarshal(b, &got); err != nil {
				t.Fatalf("Unmarshal() error = %v", err)
			}
			if !reflect.DeepEqual(got, tt.cp) {
				t.Errorf("round-trip mismatch:\n got = %#v\nwant = %#v", got, tt.cp)
			}
		})
	}
}

// TestCheckpointRawMessagePassthrough asserts the serialization-boundary
// json.RawMessage fields (StepBase/State/Info/Continuation) survive a round-trip
// byte-for-byte: pre-encoded JSON is carried untouched (§10.1).
func TestCheckpointRawMessagePassthrough(t *testing.T) {
	t.Parallel()
	cp := fullInterruptCheckpoint()
	b, err := json.Marshal(cp)
	if err != nil {
		t.Fatalf("Marshal() error = %v", err)
	}
	var got Checkpoint
	if err := json.Unmarshal(b, &got); err != nil {
		t.Fatalf("Unmarshal() error = %v", err)
	}
	tests := []struct {
		name string
		got  json.RawMessage
		want json.RawMessage
	}{
		{name: "StepBase", got: got.StepBase, want: cp.StepBase},
		{name: "State", got: got.State, want: cp.State},
		{name: "Awaiting Info", got: got.Interrupts[0].Info, want: cp.Interrupts[0].Info},
		{name: "Errored Continuation", got: got.Interrupts[1].Continuation, want: cp.Interrupts[1].Continuation},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if string(tt.got) != string(tt.want) {
				t.Errorf("raw passthrough = %s, want %s", tt.got, tt.want)
			}
		})
	}
}

// TestCheckpointInterruptsHaltMutualExclusion documents the §10.4 invariant at
// the fixture level: the Checkpoint struct CAN hold both fields, but every VALID
// representative checkpoint sets exactly one of Interrupts / Halt (XOR). The
// RUNTIME ENFORCEMENT lives in §10.4 resume-validation (a later phase), not in
// these plain-data types.
func TestCheckpointInterruptsHaltMutualExclusion(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name         string
		cp           Checkpoint
		wantInterrup bool
		wantHalt     bool
	}{
		{name: "StepRunning has Interrupts not Halt", cp: fullInterruptCheckpoint(), wantInterrup: true},
		{name: "StepPaused has Interrupts not Halt", cp: pausedCheckpoint(), wantInterrup: true},
		{name: "StepHalted has Halt not Interrupts", cp: haltedCheckpoint(), wantHalt: true},
		{name: "StepRouted has neither", cp: routedCheckpoint()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			hasInterrupts := len(tt.cp.Interrupts) > 0
			hasHalt := tt.cp.Halt != nil
			if hasInterrupts && hasHalt {
				t.Fatalf("valid fixture sets BOTH Interrupts and Halt; must be XOR")
			}
			if hasInterrupts != tt.wantInterrup {
				t.Errorf("hasInterrupts = %v, want %v", hasInterrupts, tt.wantInterrup)
			}
			if hasHalt != tt.wantHalt {
				t.Errorf("hasHalt = %v, want %v", hasHalt, tt.wantHalt)
			}
		})
	}
}

// FuzzCheckpointRoundTrip treats stored checkpoint bytes as untrusted input
// (§10.4): json.Unmarshal must never panic on arbitrary bytes (invalid bytes
// return an error, not a panic — §15), and decode is IDEMPOTENT ON THE
// NORMALIZED FORM.
//
// Why "normalized form" and not raw byte-equality: a Checkpoint carries
// json.RawMessage serialization-boundary fields (StepBase/State/Info/
// Continuation). The FIRST Marshal of an arbitrary-bytes decode normalizes those
// raw bytes — Go's encoder HTML-escapes &,<,> (e.g. a literal & becomes &)
// and compacts internal whitespace — so cp != Marshal→Unmarshal(cp) on such
// inputs. That is INHERENT JSON RawMessage normalization, not corruption: the
// values are semantically identical (a RawMessage is always consumed via
// json.Unmarshal, where & and & are the same key) and the engine itself
// ALWAYS produces StepBase/State via json.Marshal (already-normalized), so real
// checkpoints round-trip stably. The invariant we pin is the fixed point: once
// normalized by one round-trip, further round-trips are deeply equal (and emit
// identical bytes). The corpus seed 07484ae250e7d32e (StepBase {"&...":0,...})
// is the regression guard for this normalization class.
func FuzzCheckpointRoundTrip(f *testing.F) {
	seeds := []Checkpoint{
		fullInterruptCheckpoint(),
		pausedCheckpoint(),
		routedCheckpoint(),
		haltedCheckpoint(),
		{},
	}
	for _, s := range seeds {
		b, err := json.Marshal(s)
		if err != nil {
			f.Fatalf("seed Marshal() error = %v", err)
		}
		f.Add(b)
	}
	f.Add([]byte(`{"Phase":1,"Interrupts":[{"Kind":0}]}`))
	f.Add([]byte(`not json`))
	f.Add([]byte(``))

	f.Fuzz(func(t *testing.T, data []byte) {
		var cp Checkpoint
		if err := json.Unmarshal(data, &cp); err != nil {
			return // invalid bytes: an error, not a panic — acceptable (§10.4).
		}
		// Re-marshal NORMALIZES json.RawMessage fields (StepBase/State): Go's
		// encoder HTML-escapes &,<,> and compacts whitespace, so the FIRST
		// round-trip may change those raw bytes (the values are unchanged — a
		// RawMessage is always consumed via Unmarshal). The invariant is that the
		// NORMALIZED form is a fixed point: after one round-trip, further
		// round-trips are deeply equal (and byte-identical). The engine always
		// produces normalized StepBase via json.Marshal, so real checkpoints are
		// stable; the engine is correct, the assertion was over-strict.
		b1, err := json.Marshal(cp)
		if err != nil {
			t.Fatalf("re-Marshal() error = %v", err)
		}
		var cp2 Checkpoint
		if err := json.Unmarshal(b1, &cp2); err != nil {
			t.Fatalf("re-Unmarshal() error = %v", err)
		}
		b2, err := json.Marshal(cp2)
		if err != nil {
			t.Fatalf("re-Marshal(2) error = %v", err)
		}
		var cp3 Checkpoint
		if err := json.Unmarshal(b2, &cp3); err != nil {
			t.Fatalf("re-Unmarshal(2) error = %v", err)
		}
		if !reflect.DeepEqual(cp2, cp3) {
			t.Errorf("decode not idempotent on normalized form:\n cp2 = %#v\n cp3 = %#v", cp2, cp3)
		}
		if !bytes.Equal(b1, b2) {
			t.Errorf("normalized form not a byte fixed point:\n b1 = %s\n b2 = %s", b1, b2)
		}
	})
}
