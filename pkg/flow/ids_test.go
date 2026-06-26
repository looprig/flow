package flow

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/ciram-co/flow/pkg/uuid"
)

// pinnedLiteral is the design §3 pinned UUID literal used to assert that
// authored definition IDs round-trip through every encoding path.
const pinnedLiteral = "8f14e45f-ceea-467a-9e8b-2c5f9b6a1d33"

// pinned decodes the §3 literal once for use as a boundary fixture.
var pinned = uuid.MustParse(pinnedLiteral)

// uuidIDCase is one row of the shared table that exercises every UUID-backed
// identifier type. Each field is a closure so the table can drive the four
// distinct named types (GraphID/VertexID/GraphRunID/VertexRunID) through one
// set of subtests without generics or reflection leaking into the assertions.
type uuidIDCase struct {
	name string
	// str returns id.String() for a value built from underlying.
	str func(uuid.UUID) string
	// marshalText returns id.MarshalText() for a value built from underlying.
	marshalText func(uuid.UUID) ([]byte, error)
	// marshalJSON returns json.Marshal(id) for a value built from underlying.
	marshalJSON func(uuid.UUID) ([]byte, error)
	// unmarshalTextEq reports whether unmarshalling b via *T.UnmarshalText
	// yields an ID equal to one built from underlying, plus any error.
	unmarshalTextEq func(b []byte, underlying uuid.UUID) (bool, error)
	// unmarshalJSONEq is the json.Unmarshal analogue of unmarshalTextEq.
	unmarshalJSONEq func(b []byte, underlying uuid.UUID) (bool, error)
}

// uuidIDTypes builds, for each of the four UUID-backed types, a row whose
// closures convert the given underlying uuid.UUID into that concrete type and
// drive each method. Keeping the closures here means each named type is
// exercised through its own (non-generic) method set.
func uuidIDTypes() []uuidIDCase {
	return []uuidIDCase{
		{
			name:        "GraphID",
			str:         func(u uuid.UUID) string { return GraphID(u).String() },
			marshalText: func(u uuid.UUID) ([]byte, error) { return GraphID(u).MarshalText() },
			marshalJSON: func(u uuid.UUID) ([]byte, error) { return json.Marshal(GraphID(u)) },
			unmarshalTextEq: func(b []byte, underlying uuid.UUID) (bool, error) {
				var got GraphID
				err := got.UnmarshalText(b)
				return got == GraphID(underlying), err
			},
			unmarshalJSONEq: func(b []byte, underlying uuid.UUID) (bool, error) {
				var got GraphID
				err := json.Unmarshal(b, &got)
				return got == GraphID(underlying), err
			},
		},
		{
			name:        "VertexID",
			str:         func(u uuid.UUID) string { return VertexID(u).String() },
			marshalText: func(u uuid.UUID) ([]byte, error) { return VertexID(u).MarshalText() },
			marshalJSON: func(u uuid.UUID) ([]byte, error) { return json.Marshal(VertexID(u)) },
			unmarshalTextEq: func(b []byte, underlying uuid.UUID) (bool, error) {
				var got VertexID
				err := got.UnmarshalText(b)
				return got == VertexID(underlying), err
			},
			unmarshalJSONEq: func(b []byte, underlying uuid.UUID) (bool, error) {
				var got VertexID
				err := json.Unmarshal(b, &got)
				return got == VertexID(underlying), err
			},
		},
		{
			name:        "GraphRunID",
			str:         func(u uuid.UUID) string { return GraphRunID(u).String() },
			marshalText: func(u uuid.UUID) ([]byte, error) { return GraphRunID(u).MarshalText() },
			marshalJSON: func(u uuid.UUID) ([]byte, error) { return json.Marshal(GraphRunID(u)) },
			unmarshalTextEq: func(b []byte, underlying uuid.UUID) (bool, error) {
				var got GraphRunID
				err := got.UnmarshalText(b)
				return got == GraphRunID(underlying), err
			},
			unmarshalJSONEq: func(b []byte, underlying uuid.UUID) (bool, error) {
				var got GraphRunID
				err := json.Unmarshal(b, &got)
				return got == GraphRunID(underlying), err
			},
		},
		{
			name:        "VertexRunID",
			str:         func(u uuid.UUID) string { return VertexRunID(u).String() },
			marshalText: func(u uuid.UUID) ([]byte, error) { return VertexRunID(u).MarshalText() },
			marshalJSON: func(u uuid.UUID) ([]byte, error) { return json.Marshal(VertexRunID(u)) },
			unmarshalTextEq: func(b []byte, underlying uuid.UUID) (bool, error) {
				var got VertexRunID
				err := got.UnmarshalText(b)
				return got == VertexRunID(underlying), err
			},
			unmarshalJSONEq: func(b []byte, underlying uuid.UUID) (bool, error) {
				var got VertexRunID
				err := json.Unmarshal(b, &got)
				return got == VertexRunID(underlying), err
			},
		},
	}
}

// underlyingValues are the boundary fixtures every UUID-backed type is tested
// against: the zero ID and the §3 pinned const.
func underlyingValues() []struct {
	name string
	u    uuid.UUID
} {
	return []struct {
		name string
		u    uuid.UUID
	}{
		{name: "zero", u: uuid.UUID{}},
		{name: "pinned const", u: pinned},
	}
}

func TestUUIDIDString(t *testing.T) {
	t.Parallel()
	for _, tc := range uuidIDTypes() {
		tc := tc
		for _, uv := range underlyingValues() {
			uv := uv
			t.Run(tc.name+"/"+uv.name, func(t *testing.T) {
				t.Parallel()
				want := uv.u.String()
				if got := tc.str(uv.u); got != want {
					t.Errorf("String() = %q, want %q", got, want)
				}
			})
		}
	}
}

func TestUUIDIDMarshalTextRoundTrip(t *testing.T) {
	t.Parallel()
	for _, tc := range uuidIDTypes() {
		tc := tc
		for _, uv := range underlyingValues() {
			uv := uv
			t.Run(tc.name+"/"+uv.name, func(t *testing.T) {
				t.Parallel()
				b, err := tc.marshalText(uv.u)
				if err != nil {
					t.Fatalf("MarshalText() error = %v", err)
				}
				if string(b) != uv.u.String() {
					t.Errorf("MarshalText() = %q, want %q", b, uv.u.String())
				}
				eq, err := tc.unmarshalTextEq(b, uv.u)
				if err != nil {
					t.Fatalf("UnmarshalText() error = %v", err)
				}
				if !eq {
					t.Errorf("UnmarshalText(MarshalText()) did not round-trip")
				}
			})
		}
	}
}

func TestUUIDIDJSONRoundTrip(t *testing.T) {
	t.Parallel()
	for _, tc := range uuidIDTypes() {
		tc := tc
		for _, uv := range underlyingValues() {
			uv := uv
			t.Run(tc.name+"/"+uv.name, func(t *testing.T) {
				t.Parallel()
				b, err := tc.marshalJSON(uv.u)
				if err != nil {
					t.Fatalf("json.Marshal() error = %v", err)
				}
				// A TextMarshaler emits the canonical string quoted as JSON.
				want := `"` + uv.u.String() + `"`
				if string(b) != want {
					t.Errorf("json.Marshal() = %s, want %s", b, want)
				}
				eq, err := tc.unmarshalJSONEq(b, uv.u)
				if err != nil {
					t.Fatalf("json.Unmarshal() error = %v", err)
				}
				if !eq {
					t.Errorf("json.Unmarshal(json.Marshal()) did not round-trip")
				}
			})
		}
	}
}

func TestUUIDIDUnmarshalTextMalformed(t *testing.T) {
	t.Parallel()
	malformed := []struct {
		name string
		text string
	}{
		{name: "empty", text: ""},
		{name: "too short", text: "abc"},
		{name: "wrong length no hyphens", text: "8f14e45fceea467a9e8b2c5f9b6a1d33"},
		{name: "non-hex digit", text: "zf14e45f-ceea-467a-9e8b-2c5f9b6a1d33"},
		{name: "hyphen off offset", text: "8f14e45fc-eea-467a-9e8b-2c5f9b6a1d33"},
	}
	for _, tc := range uuidIDTypes() {
		tc := tc
		for _, m := range malformed {
			m := m
			t.Run(tc.name+"/"+m.name, func(t *testing.T) {
				t.Parallel()
				_, err := tc.unmarshalTextEq([]byte(m.text), uuid.UUID{})
				if err == nil {
					t.Fatalf("UnmarshalText(%q) error = nil, want error", m.text)
				}
				var pe *uuid.ParseError
				if !errors.As(err, &pe) {
					t.Errorf("UnmarshalText(%q) error = %v, want errors.As *uuid.ParseError", m.text, err)
				}
			})
		}
	}
}

func TestStepIDString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   StepID
		want string
	}{
		{name: "zero", in: 0, want: "0"},
		{name: "one", in: 1, want: "1"},
		{name: "positive multi-digit", in: 1234, want: "1234"},
		{name: "negative", in: -7, want: "-7"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.in.String(); got != tt.want {
				t.Errorf("StepID(%d).String() = %q, want %q", int(tt.in), got, tt.want)
			}
		})
	}
}

func TestStepIDJSONNumber(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		in   StepID
		want string
	}{
		{name: "zero serializes as number", in: 0, want: "0"},
		{name: "positive serializes as number", in: 42, want: "42"},
		{name: "negative serializes as number", in: -1, want: "-1"},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b, err := json.Marshal(tt.in)
			if err != nil {
				t.Fatalf("json.Marshal(StepID) error = %v", err)
			}
			if string(b) != tt.want {
				t.Errorf("json.Marshal(StepID) = %s, want %s (must be a JSON number)", b, tt.want)
			}
			var got StepID
			if err := json.Unmarshal(b, &got); err != nil {
				t.Fatalf("json.Unmarshal(StepID) error = %v", err)
			}
			if got != tt.in {
				t.Errorf("json round-trip = %d, want %d", int(got), int(tt.in))
			}
		})
	}
}

func TestNewGraphRunID(t *testing.T) {
	t.Parallel()
	a, err := NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID() error = %v", err)
	}
	if uuid.UUID(a).IsZero() {
		t.Errorf("NewGraphRunID() returned the zero ID")
	}
	b, err := NewGraphRunID()
	if err != nil {
		t.Fatalf("NewGraphRunID() second call error = %v", err)
	}
	if a == b {
		t.Errorf("NewGraphRunID() returned equal IDs on successive calls: %v", a)
	}
}

func TestNewVertexRunID(t *testing.T) {
	t.Parallel()
	a, err := NewVertexRunID()
	if err != nil {
		t.Fatalf("NewVertexRunID() error = %v", err)
	}
	if uuid.UUID(a).IsZero() {
		t.Errorf("NewVertexRunID() returned the zero ID")
	}
	b, err := NewVertexRunID()
	if err != nil {
		t.Fatalf("NewVertexRunID() second call error = %v", err)
	}
	if a == b {
		t.Errorf("NewVertexRunID() returned equal IDs on successive calls: %v", a)
	}
}

// TestRuntimeConstructorsPropagateError verifies the runtime constructors
// surface a uuid generation failure unchanged and yield the zero ID
// (fail-secure). It swaps the unexported newUUID seam, so it is intentionally
// NOT parallel and does not call t.Parallel(): it mutates package state that the
// parallel constructor tests read, and the table runs these cases serially under
// the single restore guard rather than racing subtests on the global.
func TestRuntimeConstructorsPropagateError(t *testing.T) {
	sentinel := &uuid.GenerateError{Err: errors.New("rand failure")}
	orig := newUUID
	t.Cleanup(func() { newUUID = orig })
	newUUID = func() (uuid.UUID, error) { return uuid.UUID{}, sentinel }

	tests := []struct {
		name string
		// call invokes the constructor and returns whether the returned ID is
		// the zero value plus the error, erasing the concrete return type.
		call func() (zero bool, err error)
	}{
		{
			name: "NewGraphRunID",
			call: func() (bool, error) {
				id, err := NewGraphRunID()
				return id == GraphRunID{}, err
			},
		},
		{
			name: "NewVertexRunID",
			call: func() (bool, error) {
				id, err := NewVertexRunID()
				return id == VertexRunID{}, err
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			zero, err := tt.call()
			if !errors.Is(err, sentinel) {
				t.Fatalf("%s error = %v, want the injected sentinel", tt.name, err)
			}
			var ge *uuid.GenerateError
			if !errors.As(err, &ge) {
				t.Errorf("%s error = %v, want errors.As *uuid.GenerateError", tt.name, err)
			}
			if !zero {
				t.Errorf("%s returned a non-zero ID on error, want the zero ID (fail-secure)", tt.name)
			}
		})
	}
}
