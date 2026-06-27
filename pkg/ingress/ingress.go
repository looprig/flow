package ingress

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/ciram-co/flow/pkg/controlplane"
	"github.com/ciram-co/flow/pkg/flow"
	"github.com/ciram-co/flow/pkg/registry"
)

// This file implements the async-first REST ingress (design §18.3): a ready
// http.Handler over a (GraphID, GraphVersion) registry of RunnerHandles, a
// ControlPlane, and a CheckpointStore. It carries external requests in/out and
// maps engine errors to HTTP status — it never executes a graph (that is the
// Runner's job, driven by flow.Serve consuming the work this handler submits).
//
// ASYNC-FIRST (§18.3): a start/resume POST PRE-MINTS the GraphRunID, submits a
// flow.Work to the control plane, and returns 202 + the GraphRunID immediately;
// a worker (flow.Serve) picks the work up and the caller polls GET /v1/runs/{id}
// for the result. Every run response carries graphVersion at the top level.
//
// SECURITY (CLAUDE.md, §18.3): every external input is validated at the boundary
// (path/query/body decoded into concrete typed structs, never an any flowing into
// logic); request bodies are bounded with http.MaxBytesReader; ctx is threaded
// from the request to every store/control-plane/handle call; error responses are
// sanitized JSON {"error":"..."} that never leak the engine's internal messages,
// payloads, or stack; auth is a caller-supplied seam (no scheme is baked in);
// and the companion Server helper sets explicit timeouts and TLS >= 1.2.

// DefaultMaxBodyBytes bounds a decoded request body unless WithMaxBodyBytes
// overrides it (§18.3, CLAUDE.md: guard against unbounded input). 1 MiB is a sane
// default for an initial-state / resume-payload JSON.
const DefaultMaxBodyBytes int64 = 1 << 20

// config is the resolved ingress configuration assembled from Options. It is
// unexported: callers build it through functional Options, never by naming fields.
type config struct {
	maxBodyBytes int64
	authn        func(*http.Request) error // nil = no auth (documented default)
}

// Option configures the ingress handler at New (§18.3). It is the functional-
// options seam (CLAUDE.md: wire dependencies at the composition root); a new knob
// is a new Option with zero edits to existing callers (open/closed).
type Option func(*config)

// WithMaxBodyBytes sets the per-request body-size limit in bytes (§18.3). A value
// <= 0 is ignored (the DefaultMaxBodyBytes stays in force), so a caller cannot
// accidentally disable the unbounded-input guard.
func WithMaxBodyBytes(n int64) Option {
	return func(c *config) {
		if n > 0 {
			c.maxBodyBytes = n
		}
	}
}

// WithAuth installs a caller-supplied authenticator applied to EVERY route
// (§18.3): a non-nil return rejects the request with 401. The ingress NEVER bakes
// in a scheme — the default is no auth (every request is allowed), which a caller
// must opt out of by supplying an authenticator (CLAUDE.md: least privilege; the
// auth policy is the caller's, wired at the composition root). A nil authn is
// ignored (stays no-auth).
func WithAuth(authn func(*http.Request) error) Option {
	return func(c *config) {
		if authn != nil {
			c.authn = authn
		}
	}
}

// handler is the ingress http.Handler: it owns the registry, control plane, and
// store seams, the resolved config, and the in-process idempotency map. It is the
// single struct the route handlers hang off, each method having one
// responsibility (a route). It satisfies http.Handler via the ServeMux it builds.
type handler struct {
	reg   *registry.Registry
	cp    flow.ControlPlane
	store flow.CheckpointStore
	cfg   config

	idemMu sync.Mutex                 // guards idem
	idem   map[string]flow.GraphRunID // Idempotency-Key -> minted GraphRunID (in-process tier)
	mux    *http.ServeMux
}

// New returns an async-first REST http.Handler over reg, cp, and store (§18.3).
// It registers the §18.3 routes on a stdlib ServeMux using Go 1.22+ method+pattern
// routing and wraps the whole mux in the auth and error-recovery middleware. The
// idempotency map is the in-process (Tier-B) dedupe tier; durable cross-restart
// idempotency is the NATS layer (Phase 9).
func New(reg *registry.Registry, cp flow.ControlPlane, store flow.CheckpointStore, opts ...Option) http.Handler {
	cfg := config{maxBodyBytes: DefaultMaxBodyBytes}
	for _, opt := range opts {
		opt(&cfg)
	}
	h := &handler{
		reg:   reg,
		cp:    cp,
		store: store,
		cfg:   cfg,
		idem:  make(map[string]flow.GraphRunID),
		mux:   http.NewServeMux(),
	}
	h.mux.HandleFunc("GET /v1/graphs", h.listGraphs)
	h.mux.HandleFunc("POST /v1/graphs/{graphID}/runs", h.startRun)
	h.mux.HandleFunc("POST /v1/runs/{id}/resume", h.resumeRun)
	h.mux.HandleFunc("GET /v1/runs/{id}", h.getRun)
	h.mux.HandleFunc("POST /v1/runs/{id}/cancel", h.cancelRun)
	return h.withAuth(h.mux)
}

// withAuth wraps next so the caller-supplied authenticator gates EVERY route
// (§18.3). With no authenticator configured (the default) it passes through
// unchanged; with one, a non-nil error denies the request with 401 BEFORE the
// route runs (fail secure). The authenticator's error is never echoed to the
// client — only a generic 401 — so an auth implementation cannot leak its
// internals through the response.
func (h *handler) withAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if h.cfg.authn != nil {
			if err := h.cfg.authn(r); err != nil {
				writeError(w, http.StatusUnauthorized, "unauthorized")
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// --- Route handlers ---------------------------------------------------------

// listGraphs answers GET /v1/graphs with the registry's manifest (§18.3): one
// {graphID, versions:[…]} per served GraphID — the router advertisement. It reads
// nothing external, so there is no body/path to validate.
func (h *handler) listGraphs(w http.ResponseWriter, _ *http.Request) {
	manifest := h.reg.Manifest()
	out := make([]graphManifestDTO, 0, len(manifest))
	for _, m := range manifest {
		out = append(out, graphManifestDTO{GraphID: m.GraphID.String(), Versions: m.Versions})
	}
	writeJSON(w, http.StatusOK, out)
}

// startRun answers POST /v1/graphs/{graphID}/runs (§18.3): it parses+validates the
// graphID path value, resolves the version (?version= pins; else the sole
// registered version, else 400), pre-mints the GraphRunID, reads+bounds the
// initial-state body, submits an OpRun Work, and returns 202 + the GraphRunID and
// graphVersion. An Idempotency-Key dedupes a retried POST: a repeated key returns
// the SAME GraphRunID without re-submitting Work.
func (h *handler) startRun(w http.ResponseWriter, r *http.Request) {
	graphID, err := parseGraphID(r.PathValue("graphID"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid graphID")
		return
	}
	version, hdl, ok, multi := h.resolveStartHandle(graphID, r.URL.Query().Get("version"))
	if !ok {
		if multi {
			writeError(w, http.StatusBadRequest, "multiple versions registered; specify ?version=")
			return
		}
		writeError(w, http.StatusNotFound, "unknown graph or version")
		return
	}

	// Idempotency: a previously-seen key returns the same id without re-submitting.
	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey != "" {
		if id, seen := h.lookupIdempotent(idemKey); seen {
			writeJSON(w, http.StatusAccepted, startRunDTO{GraphRunID: id.String(), GraphVersion: version})
			return
		}
	}

	input, err := readBody(w, r, h.cfg.maxBodyBytes)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	id, err := flow.NewGraphRunID()
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "could not mint run id")
		return
	}

	// Record the idempotency mapping BEFORE submitting; a concurrent duplicate that
	// won the race reuses its id and we skip a second submit.
	if idemKey != "" {
		if existing, won := h.recordIdempotent(idemKey, id); !won {
			writeJSON(w, http.StatusAccepted, startRunDTO{GraphRunID: existing.String(), GraphVersion: version})
			return
		}
	}

	work := flow.Work{
		Key:        flow.GraphVersionKey{GraphID: hdl.GraphID(), GraphVersion: hdl.GraphVersion()},
		GraphRunID: id,
		Op:         flow.OpRun,
		Input:      input,
	}
	if err := h.cp.Submit(r.Context(), work); err != nil {
		h.forgetIdempotent(idemKey, id)
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, startRunDTO{GraphRunID: id.String(), GraphVersion: version})
}

// resumeRun answers POST /v1/runs/{id}/resume (§18.3): it parses the run id,
// resolves the run's (graphID, version) from the latest checkpoint, and requires
// the registry to serve that version — no match is 409 + X-Graph-Version (the
// run's version). It then reads+bounds the resume-payload body and submits an
// OpResume Work, returning 202 + the GraphRunID and graphVersion.
func (h *handler) resumeRun(w http.ResponseWriter, r *http.Request) {
	id, err := parseRunID(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid run id")
		return
	}
	run, err := h.loadRun(r.Context(), id)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	hdl, ok := h.reg.Resolve(run.GraphID, run.GraphVersion)
	if !ok {
		// The run's version is not served here: a resume cannot be routed.
		w.Header().Set("X-Graph-Version", run.GraphVersion)
		writeError(w, http.StatusConflict, "graph version not served")
		return
	}

	input, err := readBody(w, r, h.cfg.maxBodyBytes)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	work := flow.Work{
		Key:        flow.GraphVersionKey{GraphID: hdl.GraphID(), GraphVersion: hdl.GraphVersion()},
		GraphRunID: id,
		Op:         flow.OpResume,
		Input:      input,
	}
	if err := h.cp.Submit(r.Context(), work); err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, startRunDTO{GraphRunID: id.String(), GraphVersion: run.GraphVersion})
}

// getRun answers GET /v1/runs/{id} (§18.3): it parses the run id, resolves the
// run's handle by its GraphVersion, loads the decoded RunResult via the handle's
// Get, and returns 200 + the JSON-friendly run DTO (run record + state +
// interrupts + halt + graphVersion). An unknown run is 404.
func (h *handler) getRun(w http.ResponseWriter, r *http.Request) {
	id, err := parseRunID(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid run id")
		return
	}
	run, err := h.loadRun(r.Context(), id)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	hdl, ok := h.reg.Resolve(run.GraphID, run.GraphVersion)
	if !ok {
		// The version this run ran under is not served here, so we cannot decode S.
		w.Header().Set("X-Graph-Version", run.GraphVersion)
		writeError(w, http.StatusConflict, "graph version not served")
		return
	}
	res, err := hdl.Get(r.Context(), id)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, newRunResultDTO(res))
}

// cancelRun answers POST /v1/runs/{id}/cancel (§18.3): it parses the run id,
// reads an optional {"reason":"..."} body, resolves the run's handle by its
// GraphVersion, and calls Cancel. It returns 200 on success; engine errors map
// per the catalogue (cancelling a terminal run -> 409 via ResumeTerminalError).
func (h *handler) cancelRun(w http.ResponseWriter, r *http.Request) {
	id, err := parseRunID(r.PathValue("id"))
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid run id")
		return
	}
	reason, err := readCancelReason(w, r, h.cfg.maxBodyBytes)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	run, err := h.loadRun(r.Context(), id)
	if err != nil {
		writeMappedError(w, err)
		return
	}
	hdl, ok := h.reg.Resolve(run.GraphID, run.GraphVersion)
	if !ok {
		w.Header().Set("X-Graph-Version", run.GraphVersion)
		writeError(w, http.StatusConflict, "graph version not served")
		return
	}
	if err := hdl.Cancel(r.Context(), id, reason); err != nil {
		writeMappedError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, startRunDTO{GraphRunID: id.String(), GraphVersion: run.GraphVersion})
}

// --- Resolution helpers -----------------------------------------------------

// resolveStartHandle resolves the (handle, version) for a start request (§18.3).
// A non-empty pinned version is resolved exactly; otherwise, if the graph has
// exactly ONE registered version it is used, else multi=true signals the caller to
// answer 400 ("specify ?version="). ok=false with multi=false means the graph (or
// pinned version) is unknown -> 404.
func (h *handler) resolveStartHandle(graphID flow.GraphID, pinned string) (version string, hdl flow.RunnerHandle, ok, multi bool) {
	if pinned != "" {
		hdl, ok = h.reg.Resolve(graphID, pinned)
		return pinned, hdl, ok, false
	}
	versions := h.versionsFor(graphID)
	switch len(versions) {
	case 0:
		return "", nil, false, false
	case 1:
		hdl, ok = h.reg.Resolve(graphID, versions[0])
		return versions[0], hdl, ok, false
	default:
		return "", nil, false, true
	}
}

// versionsFor returns the sorted versions the registry serves for graphID, or nil
// (§18.1). It reads the manifest (the registry's public advertisement) so the
// handler never reaches into registry internals (interface segregation).
func (h *handler) versionsFor(graphID flow.GraphID) []string {
	for _, m := range h.reg.Manifest() {
		if m.GraphID == graphID {
			return m.Versions
		}
	}
	return nil
}

// loadRun resolves a run id to its run-level state (GraphID, GraphVersion, status)
// via the store's latest checkpoint (§18.3). It is the single seam that turns a
// run id into the (graphID, version) the registry resolves a handle by, for
// resume/get/cancel. A run with no checkpoints surfaces the store's
// *CheckpointNotFoundError, which writeMappedError maps to 404. It threads ctx.
func (h *handler) loadRun(ctx context.Context, id flow.GraphRunID) (flow.GraphRunState, error) {
	cp, err := h.store.Latest(ctx, id)
	if err != nil {
		return flow.GraphRunState{}, err
	}
	return cp.Run, nil
}

// --- Idempotency (in-process Tier-B map) ------------------------------------

// lookupIdempotent returns the GraphRunID previously minted for key, if any
// (§18.3). It is the read half of the in-process dedupe map.
func (h *handler) lookupIdempotent(key string) (flow.GraphRunID, bool) {
	h.idemMu.Lock()
	defer h.idemMu.Unlock()
	id, ok := h.idem[key]
	return id, ok
}

// recordIdempotent atomically claims key for id, returning the WINNING id and
// whether THIS caller won (§18.3). If another caller already claimed key, the
// existing id is returned with won=false so the caller skips a second submit and
// returns the existing id — making concurrent retries with the same key converge
// on one run.
func (h *handler) recordIdempotent(key string, id flow.GraphRunID) (flow.GraphRunID, bool) {
	h.idemMu.Lock()
	defer h.idemMu.Unlock()
	if existing, ok := h.idem[key]; ok {
		return existing, false
	}
	h.idem[key] = id
	return id, true
}

// forgetIdempotent drops a key->id mapping after a failed Submit so a later retry
// with the same key can start the run rather than aliasing a run that was never
// enqueued (fail secure: a dropped submit must not look idempotently-succeeded).
func (h *handler) forgetIdempotent(key string, id flow.GraphRunID) {
	if key == "" {
		return
	}
	h.idemMu.Lock()
	defer h.idemMu.Unlock()
	if existing, ok := h.idem[key]; ok && existing == id {
		delete(h.idem, key)
	}
}

// --- Request body reading (bounded, concrete-typed) -------------------------

// readBody reads and bounds the request body, returning the raw JSON for the
// engine's typed decode boundary (§18.3). It wraps the body with
// http.MaxBytesReader so an over-limit body fails the read rather than buffering
// unbounded input (CLAUDE.md). An empty body returns nil (the documented "no
// initial state / no payload" case the RunnerHandle treats as the zero value). A
// non-empty body is validated to be well-formed JSON here, so a malformed body is
// rejected at the boundary as 400 before any Work is submitted (fail secure).
func readBody(w http.ResponseWriter, r *http.Request, maxBytes int64) (json.RawMessage, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	if !json.Valid(raw) {
		return nil, errInvalidJSON
	}
	return json.RawMessage(raw), nil
}

// cancelBody is the concrete type a cancel request body decodes into (§18.3): an
// optional reason. Decoding into this named struct (never an any flowing into
// logic) is the validated boundary for the cancel payload.
type cancelBody struct {
	Reason string `json:"reason"`
}

// readCancelReason reads the OPTIONAL cancel body and returns its reason (§18.3).
// An empty body is allowed (reason ""). A present body is bounded and decoded into
// the concrete cancelBody with unknown fields rejected, so a malformed body is a
// 400 at the boundary.
func readCancelReason(w http.ResponseWriter, r *http.Request, maxBytes int64) (string, error) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	raw, err := io.ReadAll(r.Body)
	if err != nil {
		return "", err
	}
	if len(raw) == 0 {
		return "", nil
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var body cancelBody
	if err := dec.Decode(&body); err != nil {
		return "", err
	}
	return body.Reason, nil
}

// --- Identifier parsing (validation boundary) -------------------------------

// parseGraphID validates and parses a GraphID path value (§18.3). External input
// is untrusted: it goes through the typed UnmarshalText (a *uuid.ParseError on
// malformed input), never concatenated raw anywhere.
func parseGraphID(s string) (flow.GraphID, error) {
	var id flow.GraphID
	if err := id.UnmarshalText([]byte(s)); err != nil {
		return flow.GraphID{}, err
	}
	return id, nil
}

// parseRunID validates and parses a GraphRunID path value (§18.3), via the typed
// UnmarshalText so a malformed id is a *uuid.ParseError the caller maps to 400.
func parseRunID(s string) (flow.GraphRunID, error) {
	var id flow.GraphRunID
	if err := id.UnmarshalText([]byte(s)); err != nil {
		return flow.GraphRunID{}, err
	}
	return id, nil
}

// --- Error mapping (typed catalogue -> HTTP status) -------------------------

// errInvalidJSON is the leaf sentinel for a body that is not well-formed JSON; it
// is mapped to 400. It carries no external data (no leak).
var errInvalidJSON = errors.New("ingress: request body is not valid JSON")

// writeMappedError maps an engine/store/control-plane error to its HTTP status
// and writes a SANITIZED JSON body (§18.3). It uses errors.As over the typed
// catalogue (CLAUDE.md: all errors typed; fail secure). The client message is a
// fixed, generic token per class — the engine's internal err.Error() (which can
// name payloads/ids) is NEVER echoed. A GraphVersionMismatchError additionally
// sets X-Graph-Version so a resume client learns the run's version.
func writeMappedError(w http.ResponseWriter, err error) {
	// 409 conflicts.
	var runExists *flow.GraphRunExistsError
	var resumeTerminal *flow.ResumeTerminalError
	var revConflict *flow.RevisionConflictError
	var versionMismatch *flow.GraphVersionMismatchError
	switch {
	case errors.As(err, &versionMismatch):
		w.Header().Set("X-Graph-Version", versionMismatch.Actual)
		writeError(w, http.StatusConflict, "graph version mismatch")
		return
	case errors.As(err, &runExists):
		writeError(w, http.StatusConflict, "run already exists")
		return
	case errors.As(err, &resumeTerminal):
		writeError(w, http.StatusConflict, "run is in a terminal state")
		return
	case errors.As(err, &revConflict):
		writeError(w, http.StatusConflict, "revision conflict")
		return
	}

	// 404 not-found.
	var notFound *flow.CheckpointNotFoundError
	if errors.As(err, &notFound) {
		writeError(w, http.StatusNotFound, "run not found")
		return
	}

	// 400 validation/decode.
	var decode *flow.CheckpointDecodeError
	if errors.As(err, &decode) || errors.Is(err, errInvalidJSON) {
		writeError(w, http.StatusBadRequest, "invalid request")
		return
	}

	// 503 store/control-plane unavailable (infra).
	var storeErr *flow.StoreError
	var closed *controlplane.ClosedError
	if errors.As(err, &storeErr) || errors.As(err, &closed) {
		writeError(w, http.StatusServiceUnavailable, "service unavailable")
		return
	}

	// Otherwise: an unclassified failure.
	writeError(w, http.StatusInternalServerError, "internal error")
}

// --- Response writing (sanitized) -------------------------------------------

// writeJSON writes v as a JSON body with the given status and the JSON content
// type. A marshal failure (an unencodable v — should not happen for the handler's
// own DTOs) falls back to a 500 sanitized error rather than a partial body.
func writeJSON(w http.ResponseWriter, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// errorDTO is the small sanitized error body shape: {"error":"..."} (§18.3). The
// message is a fixed generic token chosen by the handler, never the engine's
// internal error text.
type errorDTO struct {
	Error string `json:"error"`
}

// writeError writes a sanitized {"error":"..."} body with the given status
// (§18.3). The message is caller-chosen and generic; no internal error, payload,
// or stack is ever serialized (CLAUDE.md: log events, not secrets).
func writeError(w http.ResponseWriter, status int, message string) {
	body, _ := json.Marshal(errorDTO{Error: message})
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// --- Server helper (secure defaults) ----------------------------------------

// ServerOption configures the secure Server helper (§18.3). It is a separate
// option type from Option so the handler's knobs and the server's transport knobs
// stay segregated (interface segregation).
type ServerOption func(*http.Server)

// Server returns an *http.Server bound to addr serving h with SECURE DEFAULTS
// (§18.3, CLAUDE.md): explicit non-zero ReadHeaderTimeout/ReadTimeout/
// WriteTimeout/IdleTimeout (a slow-client / Slowloris guard that the Handler alone
// cannot set — only the http.Server can), and a TLSConfig pinning MinVersion to
// TLS 1.2. Callers SHOULD use this (or set equivalent timeouts) rather than a bare
// &http.Server{Handler: h}. ServerOptions can override any default.
func Server(addr string, h http.Handler, opts ...ServerOption) *http.Server {
	srv := &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       120 * time.Second,
		TLSConfig:         &tls.Config{MinVersion: tls.VersionTLS12},
	}
	for _, opt := range opts {
		opt(srv)
	}
	return srv
}
