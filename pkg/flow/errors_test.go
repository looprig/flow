package flow

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

// errCause is a context-free sentinel used as the wrapped cause for the
// cause-carrying error types, so the tests can assert errors.Is(err, errCause)
// reaches through Unwrap.
var errCause = errors.New("underlying cause")

// TestErrorImplementsError asserts every exported error type satisfies the
// error interface, that Error() is non-empty and mentions its salient fields,
// and that errors.As recovers the concrete type through a %w wrap (§12.4). The
// `match` substrings are the field values the message must surface so callers
// (and operators reading logs) can identify the failure without secrets.
func TestErrorImplementsError(t *testing.T) {
	t.Parallel()

	// Two stable definition ids for messages; their String() forms are the
	// substrings we assert appear in Error().
	vA := VertexID{1}
	vB := VertexID{2}
	gA := GraphID{3}
	gB := GraphID{4}
	grid := GraphRunID{5}
	vrid := VertexRunID{6}

	tests := []struct {
		name string
		err  error
		// match are substrings every one of which Error() must contain.
		match []string
		// as is a pointer to a value of the concrete type, used as the
		// errors.As target to prove recovery through a %w wrap.
		as func() (any, func(error) bool)
	}{
		{
			name:  "DuplicateVertexError",
			err:   &DuplicateVertexError{VertexID: vA},
			match: []string{vA.String()},
			as: func() (any, func(error) bool) {
				var t *DuplicateVertexError
				return &t, func(e error) bool { return errors.As(e, &t) }
			},
		},
		{
			name:  "UnknownVertexError",
			err:   &UnknownVertexError{VertexID: vA},
			match: []string{vA.String()},
			as: func() (any, func(error) bool) {
				var t *UnknownVertexError
				return &t, func(e error) bool { return errors.As(e, &t) }
			},
		},
		{
			name:  "UnreachableVertexError",
			err:   &UnreachableVertexError{VertexID: vA},
			match: []string{vA.String()},
			as: func() (any, func(error) bool) {
				var t *UnreachableVertexError
				return &t, func(e error) bool { return errors.As(e, &t) }
			},
		},
		{
			name:  "AmbiguousRoutingError",
			err:   &AmbiguousRoutingError{VertexID: vA},
			match: []string{vA.String()},
			as: func() (any, func(error) bool) {
				var t *AmbiguousRoutingError
				return &t, func(e error) bool { return errors.As(e, &t) }
			},
		},
		{
			name:  "MissingEntryError",
			err:   &MissingEntryError{VertexID: vA, Role: "entry"},
			match: []string{vA.String(), "entry"},
			as: func() (any, func(error) bool) {
				var t *MissingEntryError
				return &t, func(e error) bool { return errors.As(e, &t) }
			},
		},
		{
			name:  "MaxStepsExceededError",
			err:   &MaxStepsExceededError{Max: 100, Step: StepID(100)},
			match: []string{"100"},
			as: func() (any, func(error) bool) {
				var t *MaxStepsExceededError
				return &t, func(e error) bool { return errors.As(e, &t) }
			},
		},
		{
			name:  "UndeclaredTargetError with target",
			err:   &UndeclaredTargetError{From: vA, Target: vB},
			match: []string{vA.String(), vB.String()},
			as: func() (any, func(error) bool) {
				var t *UndeclaredTargetError
				return &t, func(e error) bool { return errors.As(e, &t) }
			},
		},
		{
			name:  "UndeclaredTargetError empty return",
			err:   &UndeclaredTargetError{From: vA},
			match: []string{vA.String(), "empty"},
			as: func() (any, func(error) bool) {
				var t *UndeclaredTargetError
				return &t, func(e error) bool { return errors.As(e, &t) }
			},
		},
		{
			name:  "DeadEndError",
			err:   &DeadEndError{Step: StepID(7)},
			match: []string{"7"},
			as: func() (any, func(error) bool) {
				var t *DeadEndError
				return &t, func(e error) bool { return errors.As(e, &t) }
			},
		},
		{
			name:  "ConditionError",
			err:   &ConditionError{From: vA, Err: errCause},
			match: []string{vA.String(), errCause.Error()},
			as: func() (any, func(error) bool) {
				var t *ConditionError
				return &t, func(e error) bool { return errors.As(e, &t) }
			},
		},
		{
			name:  "VertexError",
			err:   &VertexError{VertexID: vA, VertexRunID: vrid, Attempt: 3, Err: errCause},
			match: []string{vA.String(), vrid.String(), "3", errCause.Error()},
			as: func() (any, func(error) bool) {
				var t *VertexError
				return &t, func(e error) bool { return errors.As(e, &t) }
			},
		},
		{
			name:  "CheckpointDecodeError",
			err:   &CheckpointDecodeError{Field: "State", Err: errCause},
			match: []string{"State", errCause.Error()},
			as: func() (any, func(error) bool) {
				var t *CheckpointDecodeError
				return &t, func(e error) bool { return errors.As(e, &t) }
			},
		},
		{
			name:  "CheckpointNotFoundError",
			err:   &CheckpointNotFoundError{GraphRunID: grid},
			match: []string{grid.String()},
			as: func() (any, func(error) bool) {
				var t *CheckpointNotFoundError
				return &t, func(e error) bool { return errors.As(e, &t) }
			},
		},
		{
			name:  "ResumeTerminalError",
			err:   &ResumeTerminalError{Status: RunCompleted},
			match: []string{"Completed"},
			as: func() (any, func(error) bool) {
				var t *ResumeTerminalError
				return &t, func(e error) bool { return errors.As(e, &t) }
			},
		},
		{
			name:  "ResumeTerminalError cancelled",
			err:   &ResumeTerminalError{Status: RunCancelled},
			match: []string{"Cancelled"},
			as: func() (any, func(error) bool) {
				var t *ResumeTerminalError
				return &t, func(e error) bool { return errors.As(e, &t) }
			},
		},
		{
			name:  "GraphMismatchError",
			err:   &GraphMismatchError{Expected: gA, Actual: gB},
			match: []string{gA.String(), gB.String()},
			as: func() (any, func(error) bool) {
				var t *GraphMismatchError
				return &t, func(e error) bool { return errors.As(e, &t) }
			},
		},
		{
			name:  "GraphVersionMismatchError",
			err:   &GraphVersionMismatchError{Expected: "abc:0", Actual: "def:1"},
			match: []string{"abc:0", "def:1"},
			as: func() (any, func(error) bool) {
				var t *GraphVersionMismatchError
				return &t, func(e error) bool { return errors.As(e, &t) }
			},
		},
		{
			name:  "GraphRunExistsError",
			err:   &GraphRunExistsError{GraphRunID: grid},
			match: []string{grid.String()},
			as: func() (any, func(error) bool) {
				var t *GraphRunExistsError
				return &t, func(e error) bool { return errors.As(e, &t) }
			},
		},
		{
			name:  "RevisionConflictError",
			err:   &RevisionConflictError{GraphRunID: grid, Expected: 5, Actual: 4},
			match: []string{grid.String(), "5", "4"},
			as: func() (any, func(error) bool) {
				var t *RevisionConflictError
				return &t, func(e error) bool { return errors.As(e, &t) }
			},
		},
		{
			name:  "StoreError",
			err:   &StoreError{Op: "Append", Err: errCause},
			match: []string{"Append", errCause.Error()},
			as: func() (any, func(error) bool) {
				var t *StoreError
				return &t, func(e error) bool { return errors.As(e, &t) }
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			msg := tt.err.Error()
			if msg == "" {
				t.Fatalf("Error() returned empty string")
			}
			if !strings.HasPrefix(msg, "flow: ") {
				t.Errorf("Error() = %q, want prefix %q", msg, "flow: ")
			}
			for _, want := range tt.match {
				if !strings.Contains(msg, want) {
					t.Errorf("Error() = %q, missing salient field %q", msg, want)
				}
			}

			// errors.As must recover the concrete type through a %w wrap.
			wrapped := fmt.Errorf("context: %w", tt.err)
			_, asFn := tt.as()
			if !asFn(wrapped) {
				t.Errorf("errors.As failed to recover %T through a %%w wrap", tt.err)
			}
		})
	}
}

// TestErrorUnwrap covers the cause-carrying types: Unwrap returns the wrapped
// cause and errors.Is reaches it, the contract callers rely on to inspect the
// underlying failure (§12.4).
func TestErrorUnwrap(t *testing.T) {
	t.Parallel()

	vA := VertexID{1}
	vrid := VertexRunID{2}

	tests := []struct {
		name string
		err  error
	}{
		{name: "ConditionError", err: &ConditionError{From: vA, Err: errCause}},
		{name: "VertexError", err: &VertexError{VertexID: vA, VertexRunID: vrid, Attempt: 1, Err: errCause}},
		{name: "CheckpointDecodeError", err: &CheckpointDecodeError{Field: "checkpoint", Err: errCause}},
		{name: "StoreError", err: &StoreError{Op: "Latest", Err: errCause}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := errors.Unwrap(tt.err); got != errCause {
				t.Errorf("Unwrap() = %v, want %v", got, errCause)
			}
			if !errors.Is(tt.err, errCause) {
				t.Errorf("errors.Is(err, errCause) = false, want true")
			}
			// And reachable through an additional %w wrap.
			wrapped := fmt.Errorf("outer: %w", tt.err)
			if !errors.Is(wrapped, errCause) {
				t.Errorf("errors.Is(wrapped, errCause) = false, want true")
			}
		})
	}
}

// TestInterruptSignal verifies the internal interrupt seam: asInterrupt detects
// an *interruptSignal directly and through a %w wrap, returns the carried
// info/continuation/stateful unchanged, and reports false for a non-interrupt
// error. Error() must NOT print the payload values (no secret leakage).
func TestInterruptSignal(t *testing.T) {
	t.Parallel()

	type payload struct{ Secret string }

	tests := []struct {
		name         string
		err          error
		wantOK       bool
		wantInfo     any
		wantCont     any
		wantStateful bool
	}{
		{
			name:         "direct stateless interrupt",
			err:          &interruptSignal{info: payload{Secret: "info-secret"}, continuation: nil, stateful: false},
			wantOK:       true,
			wantInfo:     payload{Secret: "info-secret"},
			wantCont:     nil,
			wantStateful: false,
		},
		{
			name:         "direct stateful interrupt",
			err:          &interruptSignal{info: payload{Secret: "info-secret"}, continuation: payload{Secret: "cont-secret"}, stateful: true},
			wantOK:       true,
			wantInfo:     payload{Secret: "info-secret"},
			wantCont:     payload{Secret: "cont-secret"},
			wantStateful: true,
		},
		{
			name:         "wrapped interrupt via %w",
			err:          fmt.Errorf("vertex paused: %w", &interruptSignal{info: 42, continuation: "k", stateful: true}),
			wantOK:       true,
			wantInfo:     42,
			wantCont:     "k",
			wantStateful: true,
		},
		{
			name:   "non-interrupt error returns false",
			err:    errors.New("ordinary"),
			wantOK: false,
		},
		{
			name:   "typed non-interrupt error returns false",
			err:    &StoreError{Op: "Append", Err: errCause},
			wantOK: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sig, ok := asInterrupt(tt.err)
			if ok != tt.wantOK {
				t.Fatalf("asInterrupt() ok = %v, want %v", ok, tt.wantOK)
			}
			if !tt.wantOK {
				if sig != nil {
					t.Errorf("asInterrupt() sig = %v, want nil when not an interrupt", sig)
				}
				return
			}
			if sig.info != tt.wantInfo {
				t.Errorf("sig.info = %v, want %v", sig.info, tt.wantInfo)
			}
			if sig.continuation != tt.wantCont {
				t.Errorf("sig.continuation = %v, want %v", sig.continuation, tt.wantCont)
			}
			if sig.stateful != tt.wantStateful {
				t.Errorf("sig.stateful = %v, want %v", sig.stateful, tt.wantStateful)
			}
		})
	}
}

// TestInterruptSignalNoPayloadLeak asserts Error() never embeds the carried
// info/continuation values, so persisted/logged interrupt messages cannot leak
// user payloads (CLAUDE.md: log events, not secrets).
func TestInterruptSignalNoPayloadLeak(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		sig    *interruptSignal
		secret string
	}{
		{
			name:   "info secret not printed",
			sig:    &interruptSignal{info: "TOP-SECRET-INFO", stateful: false},
			secret: "TOP-SECRET-INFO",
		},
		{
			name:   "continuation secret not printed",
			sig:    &interruptSignal{continuation: "TOP-SECRET-CONT", stateful: true},
			secret: "TOP-SECRET-CONT",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			msg := tt.sig.Error()
			if msg == "" {
				t.Fatalf("Error() returned empty string")
			}
			if strings.Contains(msg, tt.secret) {
				t.Errorf("Error() = %q leaked payload %q", msg, tt.secret)
			}
		})
	}
}
