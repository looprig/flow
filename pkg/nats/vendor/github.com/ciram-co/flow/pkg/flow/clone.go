package flow

import "encoding/json"

// This file is the engine's JSON deep-clone codec for the graph state S (design
// §6.2 clone-and-commit, §10.2 "round-trips through the codec ... surfaces
// non-serializable states early"). The coordinator clones the accumulator before
// applying a reducer so a reducer that mutates then errors cannot leave partial
// state, and the prior committed S is never mutated in place. The same round-trip
// is what makes snapshotting/resume well-defined and surfaces a non-serializable
// S early (§4.1: S must be round-trippable through the codec).
//
// SANCTIONED SERIALIZATION BOUNDARY (CLAUDE.md): this is the explicit JSON seam.
// We decode into the CONCRETE S (a fresh var out S), never into an any that then
// flows into business logic — the type parameter is fixed by the caller, so the
// decoded value is exactly S and is returned as S.

// codecError reports a failure in the JSON deep-clone round-trip. Op is "marshal"
// or "unmarshal"; Err wraps the underlying encoding/json cause. It is UNEXPORTED:
// callers at the boundary (store / coordinator, later phases) wrap it in the
// exported CheckpointDecodeError / StoreError; internally it stays errors.As-able
// so those wrappers can inspect the cause and the failing operation.
type codecError struct {
	Op  string // "marshal" | "unmarshal"
	Err error
}

// Error names the failing codec operation and the underlying cause.
func (e *codecError) Error() string {
	return "flow: codec " + e.Op + ": " + e.Err.Error()
}

// Unwrap returns the underlying encoding/json cause so errors.Is/As can inspect it.
func (e *codecError) Unwrap() error { return e.Err }

// clone returns a deep copy of s by round-tripping it through JSON: it serializes
// s and decodes the bytes into a fresh S, so reference fields (maps, slices,
// pointers) are independent of the original. On a marshal or unmarshal failure it
// returns the ZERO S and a *codecError (fail secure: never a partial copy). A
// non-serializable S (e.g. a channel or func field) surfaces here as the marshal
// branch — this is the early-detection contract of §10.2.
func clone[S any](s S) (S, error) {
	var out S

	data, err := json.Marshal(s)
	if err != nil {
		return out, &codecError{Op: "marshal", Err: err}
	}

	// Decode into the concrete S (out), never into an any — sanctioned boundary.
	if err := json.Unmarshal(data, &out); err != nil {
		var zero S
		return zero, &codecError{Op: "unmarshal", Err: err}
	}

	return out, nil
}
