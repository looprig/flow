package flow

import (
	"strconv"

	"github.com/ciram-co/flow/pkg/uuid"
)

// The identifier types of the engine (design §3). The four UUID-backed types are
// distinct named types — not aliases — so the compiler rejects passing a
// GraphID where a VertexID is wanted. Each delegates String/MarshalText/
// UnmarshalText to the underlying uuid.UUID by conversion, so they serialize as
// readable canonical strings (not 16-int arrays) in checkpoints.
//
// GraphID and VertexID are stable DEFINITION ids: a checkpoint frontier
// references vertices by VertexID and a resume rebuilds the graph from code, so
// they must be stable across restarts and are pinned as consts by callers via
// uuid.MustParse. They therefore have no generating constructor here — minting a
// fresh one per build would break resume. GraphRunID and VertexRunID are
// runtime instances minted fresh each run/execution via uuid.New (see the
// NewGraphRunID/NewVertexRunID constructors below).
type (
	GraphID     uuid.UUID // stable definition id — pinned as a const by callers.
	VertexID    uuid.UUID // stable definition id — pinned as a const by callers.
	GraphRunID  uuid.UUID // runtime instance — minted per run via NewGraphRunID.
	VertexRunID uuid.UUID // runtime instance — minted per vertex execution.
	StepID      int       // super-step index within a run: 0, 1, 2, …
)

// String returns the canonical 8-4-4-4-12 hyphenated encoding of the id.
func (id GraphID) String() string { return uuid.UUID(id).String() }

// MarshalText encodes the id as its canonical string form so JSON (and any
// other encoding.TextMarshaler consumer) emits a readable string.
func (id GraphID) MarshalText() ([]byte, error) { return uuid.UUID(id).MarshalText() }

// UnmarshalText parses the canonical string form back into the id, returning a
// *uuid.ParseError (surfaced from the underlying uuid.UUID) on malformed input.
func (id *GraphID) UnmarshalText(b []byte) error { return (*uuid.UUID)(id).UnmarshalText(b) }

// String returns the canonical 8-4-4-4-12 hyphenated encoding of the id.
func (id VertexID) String() string { return uuid.UUID(id).String() }

// MarshalText encodes the id as its canonical string form so JSON (and any
// other encoding.TextMarshaler consumer) emits a readable string.
func (id VertexID) MarshalText() ([]byte, error) { return uuid.UUID(id).MarshalText() }

// UnmarshalText parses the canonical string form back into the id, returning a
// *uuid.ParseError (surfaced from the underlying uuid.UUID) on malformed input.
func (id *VertexID) UnmarshalText(b []byte) error { return (*uuid.UUID)(id).UnmarshalText(b) }

// String returns the canonical 8-4-4-4-12 hyphenated encoding of the id.
func (id GraphRunID) String() string { return uuid.UUID(id).String() }

// MarshalText encodes the id as its canonical string form so JSON (and any
// other encoding.TextMarshaler consumer) emits a readable string.
func (id GraphRunID) MarshalText() ([]byte, error) { return uuid.UUID(id).MarshalText() }

// UnmarshalText parses the canonical string form back into the id, returning a
// *uuid.ParseError (surfaced from the underlying uuid.UUID) on malformed input.
func (id *GraphRunID) UnmarshalText(b []byte) error { return (*uuid.UUID)(id).UnmarshalText(b) }

// String returns the canonical 8-4-4-4-12 hyphenated encoding of the id.
func (id VertexRunID) String() string { return uuid.UUID(id).String() }

// MarshalText encodes the id as its canonical string form so JSON (and any
// other encoding.TextMarshaler consumer) emits a readable string.
func (id VertexRunID) MarshalText() ([]byte, error) { return uuid.UUID(id).MarshalText() }

// UnmarshalText parses the canonical string form back into the id, returning a
// *uuid.ParseError (surfaced from the underlying uuid.UUID) on malformed input.
func (id *VertexRunID) UnmarshalText(b []byte) error { return (*uuid.UUID)(id).UnmarshalText(b) }

// String returns the decimal form of the super-step index. StepID is a plain
// int and serializes as a JSON number natively, so it has no TextMarshaler.
func (s StepID) String() string { return strconv.Itoa(int(s)) }

// newUUID is the single seam through which the runtime constructors mint UUIDs.
// In production it is uuid.New (crypto/rand); a test may swap it to inject a
// *uuid.GenerateError and verify the constructors propagate it unchanged. It is
// unexported, so the public surface is exactly NewGraphRunID/NewVertexRunID.
var newUUID = uuid.New

// NewGraphRunID mints a fresh runtime GraphRunID for a new run, propagating any
// *uuid.GenerateError if the randomness source fails.
func NewGraphRunID() (GraphRunID, error) {
	u, err := newUUID()
	if err != nil {
		return GraphRunID{}, err
	}
	return GraphRunID(u), nil
}

// NewVertexRunID mints a fresh runtime VertexRunID for a vertex execution,
// propagating any *uuid.GenerateError if the randomness source fails.
func NewVertexRunID() (VertexRunID, error) {
	u, err := newUUID()
	if err != nil {
		return VertexRunID{}, err
	}
	return VertexRunID(u), nil
}
