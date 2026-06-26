package flow

import (
	"context"
	"slices"
)

// This file defines the graph build API (design §7): the Graph[S] container, its
// constructor with options, and the edge/conditional-edge builders. A Graph[S] is
// a MUTABLE builder over a caller-defined shared state S; AddVertex (§6, in
// vertex.go) and the methods here accumulate vertices, static edges, and
// conditional edges. Compile (§8) — which validates the structure, computes the
// GraphVersion fingerprint, and produces an immutable Runner — is a later task
// and deliberately not here.
//
// VALIDATION SPLIT. Build methods fail fast only on locally-decidable, obviously
// bad input (a zero id, a nil Pick, an empty Targets, a duplicate conditional).
// STRUCTURAL checks that need the whole graph — endpoint existence, reachability,
// and static-vs-conditional routing ambiguity on the same from — are deferred to
// Compile (§8), because edges may legitimately be declared before their endpoint
// vertices are added.

// Condition is a conditional out-edge from a vertex (§7): a declared set of
// possible Targets and a Pick that chooses one or more of them from the current
// state. Pick is read-only and must return at least one declared target (a
// multi-target return is a fan-out); the empty-set and undeclared-target rules
// are enforced at runtime (§9.5). Existence of Targets is validated at Compile.
type Condition[S any] struct {
	Targets []VertexID                                         // declared possible targets — validated at Compile (§8)
	Pick    func(ctx context.Context, s S) ([]VertexID, error) // choose ≥1 declared target (read-only)
}

// Graph is the mutable build-time definition of a workflow over shared state S
// (§7). It is UNEXPORTED in its fields: callers build it through NewGraph,
// AddVertex, AddEdge, and AddConditionalEdge, then Compile it (later task) into
// an immutable Runner. It is not safe for concurrent mutation; build it on one
// goroutine before compiling.
type Graph[S any] struct {
	id          GraphID                   // stable definition identity (§3); pinned by the caller
	userVersion uint64                    // WithVersion (§8.1); part of the compatibility fingerprint
	vertices    map[VertexID]*vertex[S]   // bound vertices by id (filled by AddVertex)
	edges       map[VertexID][]VertexID   // static out-edges; multiple per from = fan-out
	conds       map[VertexID]Condition[S] // at most one conditional out-edge per from
}

// graphConfig is the resolved graph-level configuration assembled from
// GraphOptions. It is NON-generic (GraphOption is too) so options can be shared
// across any Graph[S]; only userVersion lives here today (§8.1).
type graphConfig struct {
	userVersion uint64
}

// GraphOption configures a Graph at NewGraph (§7). It is non-generic so the same
// option value works for any state type S.
type GraphOption func(*graphConfig)

// WithVersion sets the graph's userVersion (§8.1), the manual bump callers use to
// invalidate resume after a BEHAVIOR change (task/selector/reducer/Pick logic)
// that topology hashing cannot see. Default is 0; last application wins.
func WithVersion(n uint64) GraphOption {
	return func(c *graphConfig) { c.userVersion = n }
}

// NewGraph creates an empty, mutable Graph[S] with the given stable identity and
// options (§7). It initializes all maps so AddVertex/AddEdge/AddConditionalEdge
// never nil-deref, and resolves options into the stored userVersion.
func NewGraph[S any](id GraphID, opts ...GraphOption) *Graph[S] {
	cfg := graphConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Graph[S]{
		id:          id,
		userVersion: cfg.userVersion,
		vertices:    make(map[VertexID]*vertex[S]),
		edges:       make(map[VertexID][]VertexID),
		conds:       make(map[VertexID]Condition[S]),
	}
}

// AddEdge records a static out-edge from→to (§7). Multiple edges from one from
// are a fan-out and are allowed; a self-edge (from == to) is allowed (cycles are
// legal). It rejects only a zero from or to (a BuildError); endpoint existence
// and routing-ambiguity are deferred to Compile (§8), since an edge may be
// declared before its endpoint vertices are added.
func (g *Graph[S]) AddEdge(from, to VertexID) error {
	if from == (VertexID{}) {
		return &BuildError{Op: "AddEdge", Detail: "zero from vertex"}
	}
	if to == (VertexID{}) {
		return &BuildError{Op: "AddEdge", Detail: "zero to vertex"}
	}
	g.edges[from] = append(g.edges[from], to)
	return nil
}

// AddConditionalEdge records the conditional out-edge c for from (§7). It rejects
// a zero from, a nil c.Pick, or empty c.Targets (each a malformed-argument
// BuildError), and rejects a SECOND conditional edge on the same from (a
// uniqueness violation — DuplicateConditionalEdgeError — since a second would
// silently overwrite the first). Target existence — and the rule that a from may
// not have both a static and a conditional edge — are deferred to Compile (§8).
//
// The stored Condition's Targets are a defensive copy, so a caller mutating their
// original slice afterward cannot mutate graph state.
func (g *Graph[S]) AddConditionalEdge(from VertexID, c Condition[S]) error {
	if from == (VertexID{}) {
		return &BuildError{Op: "AddConditionalEdge", Detail: "zero from vertex"}
	}
	if c.Pick == nil {
		return &BuildError{Op: "AddConditionalEdge", Detail: "nil Pick"}
	}
	if len(c.Targets) == 0 {
		return &BuildError{Op: "AddConditionalEdge", Detail: "empty Targets"}
	}
	if _, exists := g.conds[from]; exists {
		return &DuplicateConditionalEdgeError{From: from}
	}
	c.Targets = slices.Clone(c.Targets)
	g.conds[from] = c
	return nil
}
