package flow

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
)

// payload is a struct value used to exercise the typed readers with a
// non-primitive: ResumePayload type-asserts it, InterruptState JSON-decodes it.
type payload struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// TestInterruptTriggers asserts the two trigger constructors return an error
// that asInterrupt recognizes, carrying the info/continuation/stateful the
// caller supplied (§10.3). Interrupt is stateful=false with a nil continuation;
// StatefulInterrupt is stateful=true and stows the live continuation untouched.
func TestInterruptTriggers(t *testing.T) {
	t.Parallel()

	info := payload{Name: "approve", Count: 1}
	cont := payload{Name: "resume-here", Count: 2}

	tests := []struct {
		name         string
		err          error
		wantInfo     any
		wantCont     any
		wantStateful bool
	}{
		{
			name:         "Interrupt carries info, stateful false, nil continuation",
			err:          Interrupt(context.Background(), info),
			wantInfo:     info,
			wantCont:     nil,
			wantStateful: false,
		},
		{
			name:         "Interrupt with nil info",
			err:          Interrupt(context.Background(), nil),
			wantInfo:     nil,
			wantCont:     nil,
			wantStateful: false,
		},
		{
			name:         "StatefulInterrupt carries info and continuation, stateful true",
			err:          StatefulInterrupt(context.Background(), info, cont),
			wantInfo:     info,
			wantCont:     cont,
			wantStateful: true,
		},
		{
			name:         "StatefulInterrupt with nil continuation still stateful",
			err:          StatefulInterrupt(context.Background(), info, nil),
			wantInfo:     info,
			wantCont:     nil,
			wantStateful: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if tt.err == nil {
				t.Fatalf("trigger returned nil error")
			}
			sig, ok := asInterrupt(tt.err)
			if !ok {
				t.Fatalf("asInterrupt did not recognize the trigger error %v", tt.err)
			}
			if sig.info != tt.wantInfo {
				t.Errorf("info = %v, want %v", sig.info, tt.wantInfo)
			}
			if sig.continuation != tt.wantCont {
				t.Errorf("continuation = %v, want %v", sig.continuation, tt.wantCont)
			}
			if sig.stateful != tt.wantStateful {
				t.Errorf("stateful = %v, want %v", sig.stateful, tt.wantStateful)
			}
		})
	}
}

// TestResumePayloadStruct asserts ResumePayload reads the LIVE value injected via
// withResumePayload and type-asserts it: present+right-type → (value,true);
// wrong-type request → (zero,false); absent → (zero,false).
func TestResumePayloadStruct(t *testing.T) {
	t.Parallel()

	want := payload{Name: "live", Count: 7}

	tests := []struct {
		name   string
		ctx    context.Context
		want   payload
		wantOK bool
	}{
		{
			name:   "present and right type",
			ctx:    withResumePayload(context.Background(), want),
			want:   want,
			wantOK: true,
		},
		{
			name:   "absent",
			ctx:    context.Background(),
			want:   payload{},
			wantOK: false,
		},
		{
			name:   "wrong type (int injected, struct requested)",
			ctx:    withResumePayload(context.Background(), 42),
			want:   payload{},
			wantOK: false,
		},
		{
			name:   "nil payload injected",
			ctx:    withResumePayload(context.Background(), nil),
			want:   payload{},
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := ResumePayload[payload](tt.ctx)
			if ok != tt.wantOK {
				t.Fatalf("ResumePayload ok = %v, want %v", ok, tt.wantOK)
			}
			if got != tt.want {
				t.Errorf("ResumePayload = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestResumePayloadPrimitive asserts ResumePayload works with a primitive type
// param, covering the boundary of a non-struct live value.
func TestResumePayloadPrimitive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		ctx    context.Context
		want   string
		wantOK bool
	}{
		{
			name:   "present string",
			ctx:    withResumePayload(context.Background(), "hello"),
			want:   "hello",
			wantOK: true,
		},
		{
			name:   "wrong type (struct injected, string requested)",
			ctx:    withResumePayload(context.Background(), payload{Name: "x"}),
			want:   "",
			wantOK: false,
		},
		{
			name:   "absent",
			ctx:    context.Background(),
			want:   "",
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := ResumePayload[string](tt.ctx)
			if ok != tt.wantOK {
				t.Fatalf("ResumePayload ok = %v, want %v", ok, tt.wantOK)
			}
			if got != tt.want {
				t.Errorf("ResumePayload = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestInterruptState asserts InterruptState reads the restored json.RawMessage
// continuation injected via withInterruptState and json.Unmarshals it into T:
// decodable → (value,true); bytes that don't decode into T → (zero,false);
// absent/nil → (zero,false).
func TestInterruptState(t *testing.T) {
	t.Parallel()

	want := payload{Name: "saved", Count: 3}
	good, err := json.Marshal(want)
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}

	tests := []struct {
		name   string
		ctx    context.Context
		want   payload
		wantOK bool
	}{
		{
			name:   "decodable into T",
			ctx:    withInterruptState(context.Background(), good),
			want:   want,
			wantOK: true,
		},
		{
			name:   "raw bytes do not decode into T",
			ctx:    withInterruptState(context.Background(), json.RawMessage(`"a string, not an object"`)),
			want:   payload{},
			wantOK: false,
		},
		{
			name:   "absent",
			ctx:    context.Background(),
			want:   payload{},
			wantOK: false,
		},
		{
			name:   "nil RawMessage",
			ctx:    withInterruptState(context.Background(), nil),
			want:   payload{},
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := InterruptState[payload](tt.ctx)
			if ok != tt.wantOK {
				t.Fatalf("InterruptState ok = %v, want %v", ok, tt.wantOK)
			}
			if got != tt.want {
				t.Errorf("InterruptState = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestInfo asserts Info reads the coordinator-injected RunInfo identity (§4.2):
// present → (value,true); absent → (zero,false).
func TestInfo(t *testing.T) {
	t.Parallel()

	want := RunInfo{
		GraphID:     GraphID{1},
		GraphRunID:  GraphRunID{2},
		VertexID:    VertexID{3},
		VertexRunID: VertexRunID{4},
		Step:        5,
	}

	tests := []struct {
		name   string
		ctx    context.Context
		want   RunInfo
		wantOK bool
	}{
		{
			name:   "present",
			ctx:    withRunInfo(context.Background(), want),
			want:   want,
			wantOK: true,
		},
		{
			name:   "absent",
			ctx:    context.Background(),
			want:   RunInfo{},
			wantOK: false,
		},
		{
			// Defensive: a non-RunInfo value stored under the key must not be
			// returned. The injector never does this; only a same-package raw
			// WithValue can, so we exercise the !ok narrowing branch directly.
			name:   "wrong type under key",
			ctx:    context.WithValue(context.Background(), ctxKeyRunInfo, "not a RunInfo"),
			want:   RunInfo{},
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := Info(tt.ctx)
			if ok != tt.wantOK {
				t.Fatalf("Info ok = %v, want %v", ok, tt.wantOK)
			}
			if got != tt.want {
				t.Errorf("Info = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestSelf asserts Self reads the coordinator-injected VertexState record (§4.2):
// present → (value,true); absent → (zero,false).
func TestSelf(t *testing.T) {
	t.Parallel()

	want := VertexState{
		VertexID:    VertexID{1},
		VertexRunID: VertexRunID{2},
		Step:        3,
		Status:      VertexRunning,
		Attempt:     1,
	}

	tests := []struct {
		name   string
		ctx    context.Context
		want   VertexState
		wantOK bool
	}{
		{
			name:   "present",
			ctx:    withSelf(context.Background(), want),
			want:   want,
			wantOK: true,
		},
		{
			name:   "absent",
			ctx:    context.Background(),
			want:   VertexState{},
			wantOK: false,
		},
		{
			// Defensive: a non-VertexState value stored under the key must not be
			// returned. Exercises the !ok narrowing branch (see TestInfo).
			name:   "wrong type under key",
			ctx:    context.WithValue(context.Background(), ctxKeySelf, 123),
			want:   VertexState{},
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := Self(tt.ctx)
			if ok != tt.wantOK {
				t.Fatalf("Self ok = %v, want %v", ok, tt.wantOK)
			}
			if got != tt.want {
				t.Errorf("Self = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestResultStructsCompile is a trivial construct-and-read test proving the
// public Interruption and Halt records carry the §10.3 fields with the expected
// types (InterruptKind/HaltKind from checkpoint.go, error causes via errors.Is).
func TestResultStructsCompile(t *testing.T) {
	t.Parallel()

	cause := errors.New("boom")

	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "Interruption fields",
			run: func(t *testing.T) {
				in := Interruption{
					GraphRunID: GraphRunID{1},
					Vertex:     VertexID{2},
					Kind:       Awaiting,
					Info:       payload{Name: "why"},
					Cause:      cause,
				}
				if in.Kind != Awaiting {
					t.Errorf("Kind = %v, want Awaiting", in.Kind)
				}
				if got, ok := in.Info.(payload); !ok || got.Name != "why" {
					t.Errorf("Info = %v, want payload{why}", in.Info)
				}
				if !errors.Is(in.Cause, cause) {
					t.Errorf("Cause does not wrap the supplied error")
				}
			},
		},
		{
			name: "Halt fields",
			run: func(t *testing.T) {
				h := Halt{
					GraphRunID: GraphRunID{1},
					Kind:       HaltDeadEnd,
					Step:       7,
					Cause:      cause,
				}
				if h.Kind != HaltDeadEnd {
					t.Errorf("Kind = %v, want HaltDeadEnd", h.Kind)
				}
				if h.Step != 7 {
					t.Errorf("Step = %v, want 7", h.Step)
				}
				if !errors.Is(h.Cause, cause) {
					t.Errorf("Cause does not wrap the supplied error")
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
