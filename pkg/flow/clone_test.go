package flow

import (
	"errors"
	"reflect"
	"testing"
)

// This file tests the JSON deep-clone codec (design §6.2 clone-and-commit, §10.2
// "round-trips through the codec ... surfaces non-serializable states early").
// The two contracts under test: (1) clone produces a DEEP copy — mutating the
// clone's reference fields (map/slice/pointee) never touches the original, and
// vice versa — which is what makes the coordinator's clone-and-commit safe; and
// (2) a non-serializable S surfaces as a typed *codecError rather than silently
// sharing references or panicking. The shared state type 'st' is already taken by
// vertex_test.go, so this file uses cloneSt (reference fields exercise depth).

// cloneSt is a small state type with REFERENCE fields (map, slice, pointer) so a
// shallow copy would alias them and a deep copy would not. Exported fields so the
// JSON codec can see them.
type cloneSt struct {
	N  int
	M  map[string]int
	Sl []int
	P  *int
}

// intp returns a pointer to v so tests can populate cloneSt.P concisely.
func intp(v int) *int { return &v }

// chanState is non-serializable: encoding/json cannot marshal a channel, so
// clone's marshal branch must fail with *codecError{Op: "marshal"}.
type chanState struct {
	C chan int
}

// badRoundTrip marshals to a JSON string but unmarshals from an object, so its
// MarshalJSON emits bytes its own UnmarshalJSON rejects. It exists solely to
// reach clone's defensive unmarshal branch, which the public clone API cannot
// otherwise trigger for a well-formed S.
type badRoundTrip struct{}

// MarshalJSON emits a JSON string (valid JSON, so marshal succeeds).
func (badRoundTrip) MarshalJSON() ([]byte, error) { return []byte(`"not-an-object"`), nil }

// UnmarshalJSON rejects everything, forcing the unmarshal branch to error.
func (*badRoundTrip) UnmarshalJSON([]byte) error { return errors.New("badRoundTrip: cannot decode") }

// TestCloneDeepIndependence proves clone produces a DEEP copy: mutating the
// clone's reference fields leaves the original untouched, and mutating the
// original leaves the clone untouched. A shallow copy would alias the map/slice/
// pointee and fail one direction or the other.
func TestCloneDeepIndependence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		in   cloneSt
	}{
		{
			name: "populated reference fields",
			in:   cloneSt{N: 7, M: map[string]int{"a": 1}, Sl: []int{10, 20}, P: intp(99)},
		},
		{
			name: "single-element collections",
			in:   cloneSt{N: 1, M: map[string]int{"k": 0}, Sl: []int{5}, P: intp(0)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := clone(tt.in)
			if err != nil {
				t.Fatalf("clone() error = %v, want nil", err)
			}
			if !reflect.DeepEqual(got, tt.in) {
				t.Fatalf("clone() = %#v, want equal to %#v", got, tt.in)
			}

			// Mutate the CLONE; the ORIGINAL must be unchanged.
			got.M["mutant"] = 42
			got.Sl[0] = -1
			*got.P = -1
			if _, exists := tt.in.M["mutant"]; exists {
				t.Errorf("mutating clone.M leaked into original: %#v", tt.in.M)
			}
			if tt.in.Sl[0] == -1 {
				t.Errorf("mutating clone.Sl leaked into original: %#v", tt.in.Sl)
			}
			if *tt.in.P == -1 {
				t.Errorf("mutating *clone.P leaked into original: %d", *tt.in.P)
			}

			// Mutate the ORIGINAL; the CLONE must be unchanged.
			origSlBefore := got.Sl[0]
			origPBefore := *got.P
			tt.in.M["origside"] = 7
			tt.in.Sl[0] = 1000
			*tt.in.P = 1000
			if _, exists := got.M["origside"]; exists {
				t.Errorf("mutating original.M leaked into clone: %#v", got.M)
			}
			if got.Sl[0] != origSlBefore {
				t.Errorf("mutating original.Sl leaked into clone.Sl: got %d", got.Sl[0])
			}
			if *got.P != origPBefore {
				t.Errorf("mutating *original.P leaked into *clone.P: got %d", *got.P)
			}
		})
	}
}

// TestCloneValues covers value-level round-trips: the zero state, empty (non-nil)
// collections, and a bounded-large state must all clone equal to their input.
func TestCloneValues(t *testing.T) {
	t.Parallel()

	// bounded-large fixture: a sizeable but BOUNDED map/slice.
	const n = 1000
	bigM := make(map[string]int, n)
	bigSl := make([]int, n)
	for i := 0; i < n; i++ {
		bigM["k"+itoa(i)] = i
		bigSl[i] = i
	}

	tests := []struct {
		name string
		in   cloneSt
	}{
		{
			name: "zero value",
			in:   cloneSt{},
		},
		{
			name: "empty non-nil collections",
			in:   cloneSt{N: 0, M: map[string]int{}, Sl: []int{}, P: intp(0)},
		},
		{
			name: "bounded-large",
			in:   cloneSt{N: n, M: bigM, Sl: bigSl, P: intp(n)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := clone(tt.in)
			if err != nil {
				t.Fatalf("clone() error = %v, want nil", err)
			}
			if !reflect.DeepEqual(got, tt.in) {
				t.Fatalf("clone() = %#v, want equal to %#v", got, tt.in)
			}
		})
	}
}

// TestCloneErrors covers the failure modes: a non-serializable S fails at the
// marshal branch, and a type whose MarshalJSON/UnmarshalJSON disagree fails at
// the unmarshal branch. Both must return the ZERO S and an *codecError carrying
// the correct Op, recoverable via errors.As.
func TestCloneErrors(t *testing.T) {
	t.Parallel()

	t.Run("non-serializable marshal", func(t *testing.T) {
		t.Parallel()

		got, err := clone(chanState{C: make(chan int)})
		if err == nil {
			t.Fatalf("clone(chanState) error = nil, want non-nil")
		}
		var ce *codecError
		if !errors.As(err, &ce) {
			t.Fatalf("clone(chanState) error = %v, want errors.As(*codecError)", err)
		}
		if ce.Op != "marshal" {
			t.Errorf("codecError.Op = %q, want %q", ce.Op, "marshal")
		}
		if ce.Unwrap() == nil {
			t.Errorf("codecError.Unwrap() = nil, want the wrapped cause")
		}
		if !reflect.DeepEqual(got, chanState{}) {
			t.Errorf("clone(chanState) = %#v, want zero value", got)
		}
	})

	t.Run("unmarshal rejects round-trip", func(t *testing.T) {
		t.Parallel()

		got, err := clone(badRoundTrip{})
		if err == nil {
			t.Fatalf("clone(badRoundTrip) error = nil, want non-nil")
		}
		var ce *codecError
		if !errors.As(err, &ce) {
			t.Fatalf("clone(badRoundTrip) error = %v, want errors.As(*codecError)", err)
		}
		if ce.Op != "unmarshal" {
			t.Errorf("codecError.Op = %q, want %q", ce.Op, "unmarshal")
		}
		if ce.Unwrap() == nil {
			t.Errorf("codecError.Unwrap() = nil, want the wrapped cause")
		}
		if !reflect.DeepEqual(got, badRoundTrip{}) {
			t.Errorf("clone(badRoundTrip) = %#v, want zero value", got)
		}
	})
}

// TestCodecErrorMessage pins the *codecError message format and Unwrap so callers
// (store/coordinator) that wrap it later get a stable, cause-carrying string.
func TestCodecErrorMessage(t *testing.T) {
	t.Parallel()

	cause := errors.New("boom")
	tests := []struct {
		name string
		err  *codecError
		want string
	}{
		{
			name: "marshal",
			err:  &codecError{Op: "marshal", Err: cause},
			want: "flow: codec marshal: boom",
		},
		{
			name: "unmarshal",
			err:  &codecError{Op: "unmarshal", Err: cause},
			want: "flow: codec unmarshal: boom",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			if got := tt.err.Error(); got != tt.want {
				t.Errorf("Error() = %q, want %q", got, tt.want)
			}
			if !errors.Is(tt.err, cause) {
				t.Errorf("errors.Is(err, cause) = false, want true")
			}
		})
	}
}
