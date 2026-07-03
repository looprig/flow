package ingress

import (
	"encoding/json"

	"github.com/looprig/flow/pkg/flow"
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
// checkpoint), and Cause is a string describing the failure. By DEFAULT (M1) Cause
// is a GENERIC, non-leaking token (the interrupt kind, e.g. "errored") — a task's
// raw error message can carry PII/payloads and GET is a wire boundary; the raw
// cause is surfaced only under WithVerboseErrors (a trusted/debug deployment).
type interruptionDTO struct {
	Vertex string          `json:"vertex"`
	Kind   string          `json:"kind"`
	Info   json.RawMessage `json:"info,omitempty"`
	Cause  string          `json:"cause,omitempty"`
}

// haltDTO is the JSON-friendly form of a flow.Halt (§18.3): the structural kind,
// the super-step, and a cause string. By DEFAULT (M1) Cause is the generic halt
// kind (e.g. "condition"); the raw cause is surfaced only under WithVerboseErrors.
type haltDTO struct {
	Kind  string `json:"kind"`
	Step  int    `json:"step"`
	Cause string `json:"cause,omitempty"`
}

// newRunResultDTO converts a *flow.RunResult to its wire DTO (§18.3): it lifts
// graphVersion to the top level, passes the already-marshaled State through, and
// renders interrupts/halt. The verbose flag (WithVerboseErrors, default false)
// controls whether a raw task/halt error string is surfaced (M1): by default a
// generic, non-leaking token is rendered instead, since a task's error message can
// carry PII/payloads and GET is a wire boundary. A nil res yields a zero DTO
// (defensive — the handler only calls this on a non-nil Get result).
func newRunResultDTO(res *flow.RunResult, verbose bool) runResultDTO {
	if res == nil {
		return runResultDTO{}
	}
	out := runResultDTO{
		GraphVersion: res.Run.GraphVersion,
		Run:          res.Run,
		State:        res.State,
		Halt:         haltToDTO(res.Halt, verbose),
	}
	if len(res.Interrupts) > 0 {
		out.Interrupts = make([]interruptionDTO, len(res.Interrupts))
		for i, iv := range res.Interrupts {
			out.Interrupts[i] = interruptionToDTO(iv, verbose)
		}
	}
	return out
}

// interruptionToDTO renders one flow.Interruption as its wire DTO (§18.3). Info
// is the engine's serialization-boundary any; when it is the reconstructed raw
// JSON (json.RawMessage) it is passed through, otherwise it is best-effort
// re-marshaled. Cause defaults to a GENERIC token (M1): for an Errored interrupt
// the kind name ("errored") — NEVER the raw iv.Cause.Error(), which can leak
// PII/payloads on the wire. Only verbose surfaces the raw cause.
func interruptionToDTO(iv flow.Interruption, verbose bool) interruptionDTO {
	dto := interruptionDTO{
		Vertex: iv.Vertex.String(),
		Kind:   interruptKindString(iv.Kind),
	}
	dto.Info = infoToRaw(iv.Info)
	dto.Cause = causeString(iv.Cause, dto.Kind, verbose)
	return dto
}

// causeString renders an error cause for the wire (M1): under verbose it is the
// raw err.Error() (trusted/debug deployments); by default it is the supplied
// generic token (the interrupt/halt kind), so a task's error message — which can
// carry PII/payloads — never crosses the wire boundary. A nil cause yields "" (the
// field is omitted) regardless of mode.
func causeString(cause error, generic string, verbose bool) string {
	if cause == nil {
		return ""
	}
	if verbose {
		return cause.Error()
	}
	return generic
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
// Cause defaults to the generic halt kind (M1); only verbose surfaces the raw
// h.Cause.Error(), which can leak PII/payloads on the wire.
func haltToDTO(h *flow.Halt, verbose bool) *haltDTO {
	if h == nil {
		return nil
	}
	kind := haltKindString(h.Kind)
	return &haltDTO{Kind: kind, Step: int(h.Step), Cause: causeString(h.Cause, kind, verbose)}
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
