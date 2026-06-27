package ingress

import (
	"encoding/json"

	"github.com/ciram-co/flow/pkg/flow"
)

// This file defines the ingress response DTOs (design §18.3): the JSON-friendly
// shapes the handler marshals. They are SEPARATE from the engine's types on
// purpose — the handler controls exactly what crosses the wire (CLAUDE.md: no
// internal leak), adds graphVersion at the top level of every run response, and
// renders the engine's non-JSON-clean fields (an Interruption.Cause is an `error`,
// which does not marshal usefully) as plain strings. Every DTO has json tags so
// the wire shape is explicit and stable rather than reflection-derived from the
// engine structs.

// startRunDTO is the 202 body for start/resume/cancel (§18.3): the pre-minted (or
// resolved) GraphRunID and the resolved graphVersion the work runs under.
type startRunDTO struct {
	GraphRunID   string `json:"graphRunID"`
	GraphVersion string `json:"graphVersion"`
}

// graphManifestDTO is one entry of GET /v1/graphs (§18.3): a served GraphID and
// its sorted versions — the router advertisement.
type graphManifestDTO struct {
	GraphID  string   `json:"graphID"`
	Versions []string `json:"versions"`
}

// runResultDTO is the GET /v1/runs/{id} body (§18.3): the run-level record, the
// decoded state (as raw JSON, passed through untouched), the per-vertex
// interrupts, the run-level halt, and graphVersion lifted to the top level for a
// client that reads it without descending into Run. Interrupts/Halt are omitted
// when empty (the happy path), mirroring the engine's mutually-exclusive nil
// convention.
type runResultDTO struct {
	GraphVersion string             `json:"graphVersion"`
	Run          flow.GraphRunState `json:"run"`
	State        json.RawMessage    `json:"state,omitempty"`
	Interrupts   []interruptionDTO  `json:"interrupts,omitempty"`
	Halt         *haltDTO           `json:"halt,omitempty"`
}

// interruptionDTO is the JSON-friendly form of a flow.Interruption (§18.3): Info
// is the raw reason JSON (best-effort, as the engine reconstructs it from the
// checkpoint), and Cause is the error message as a plain string (a flow.Halt /
// Interruption Cause is a Go error, which does not marshal usefully on its own).
type interruptionDTO struct {
	Vertex string          `json:"vertex"`
	Kind   string          `json:"kind"`
	Info   json.RawMessage `json:"info,omitempty"`
	Cause  string          `json:"cause,omitempty"`
}

// haltDTO is the JSON-friendly form of a flow.Halt (§18.3): the structural kind,
// the super-step, and the cause message as a plain string.
type haltDTO struct {
	Kind  string `json:"kind"`
	Step  int    `json:"step"`
	Cause string `json:"cause,omitempty"`
}

// newRunResultDTO converts a *flow.RunResult to its wire DTO (§18.3): it lifts
// graphVersion to the top level, passes the already-marshaled State through, and
// renders interrupts/halt with their error causes as strings. A nil res yields a
// zero DTO (defensive — the handler only calls this on a non-nil Get result).
func newRunResultDTO(res *flow.RunResult) runResultDTO {
	if res == nil {
		return runResultDTO{}
	}
	out := runResultDTO{
		GraphVersion: res.Run.GraphVersion,
		Run:          res.Run,
		State:        res.State,
		Halt:         haltToDTO(res.Halt),
	}
	if len(res.Interrupts) > 0 {
		out.Interrupts = make([]interruptionDTO, len(res.Interrupts))
		for i, iv := range res.Interrupts {
			out.Interrupts[i] = interruptionToDTO(iv)
		}
	}
	return out
}

// interruptionToDTO renders one flow.Interruption as its wire DTO (§18.3). Info
// is the engine's serialization-boundary any; when it is the reconstructed raw
// JSON (json.RawMessage) it is passed through, otherwise it is best-effort
// re-marshaled. Cause is rendered as its message string (nil -> "").
func interruptionToDTO(iv flow.Interruption) interruptionDTO {
	dto := interruptionDTO{
		Vertex: iv.Vertex.String(),
		Kind:   interruptKindString(iv.Kind),
	}
	dto.Info = infoToRaw(iv.Info)
	if iv.Cause != nil {
		dto.Cause = iv.Cause.Error()
	}
	return dto
}

// infoToRaw renders an Interruption.Info serialization-boundary value as raw JSON
// (§18.3). The engine reconstructs Info as a json.RawMessage from the checkpoint,
// so that case is passed through directly; any other concrete value is marshaled
// best-effort, and an unencodable one (or nil) yields nil (omitted).
func infoToRaw(info any) json.RawMessage {
	if info == nil {
		return nil
	}
	if raw, ok := info.(json.RawMessage); ok {
		return raw
	}
	b, err := json.Marshal(info)
	if err != nil {
		return nil
	}
	return b
}

// haltToDTO renders a *flow.Halt as its wire DTO, or nil for a nil halt (§18.3).
func haltToDTO(h *flow.Halt) *haltDTO {
	if h == nil {
		return nil
	}
	dto := &haltDTO{Kind: haltKindString(h.Kind), Step: int(h.Step)}
	if h.Cause != nil {
		dto.Cause = h.Cause.Error()
	}
	return dto
}

// interruptKindString renders an InterruptKind as a stable wire token (§18.3). An
// unknown value falls back to a decimal-suffixed token so the integer is never
// lost (the engine's enums have no Stringer of their own).
func interruptKindString(k flow.InterruptKind) string {
	switch k {
	case flow.Awaiting:
		return "awaiting"
	case flow.Errored:
		return "errored"
	default:
		return "unknown"
	}
}

// haltKindString renders a HaltKind as a stable wire token (§18.3). An unknown
// value falls back to "unknown".
func haltKindString(k flow.HaltKind) string {
	switch k {
	case flow.HaltCondition:
		return "condition"
	case flow.HaltUndeclaredTarget:
		return "undeclared_target"
	case flow.HaltDeadEnd:
		return "dead_end"
	case flow.HaltMaxSteps:
		return "max_steps"
	default:
		return "unknown"
	}
}
