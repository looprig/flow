package flow

// This file implements Compile (design §8): the whole-graph validation that turns
// a mutable Graph[S] builder into an immutable, reusable Runner[S]. The build API
// (graph.go, vertex.go) only fails fast on locally-decidable bad input; the
// STRUCTURAL checks that need the entire graph live here, because edges may
// legitimately be declared before their endpoint vertices are added.
//
// FIRST-ERROR ORDER. Compile returns the FIRST violation in a fixed, documented
// order, cheapest/most-fundamental first so the error an operator sees is the
// root cause, not a downstream symptom:
//  1. entry exists, then finish exists          (MissingEntryError)
//  2. every edge/conditional/error-route endpoint is a known vertex
//                                               (UnknownVertexError)
//  3. no vertex has both a static and a conditional out-edge
//                                               (AmbiguousRoutingError)
//  4. every vertex is reachable from entry following static + conditional +
//     error-route edges; this also guarantees finish is reachable
//                                               (UnreachableVertexError)
//
// VertexID UNIQUENESS is NOT re-checked here: Graph.vertices is a map keyed by
// VertexID and AddVertex rejects a duplicate id at add-time with
// DuplicateVertexError (an add-time error, §12.4), so duplicates are structurally
// impossible by the time Compile runs.
//
// SCOPE. This is validation + the GraphVersion fingerprint (§8.1) + a minimal
// Runner skeleton carrying its single CheckpointStore (§9). The hooks,
// concurrency, maxSteps, granularity options and Run/Resume/Status are later
// phases.

// Runner is the immutable, validated form of a Graph[S], produced by Compile
// (§8, §9). It is safe to reuse across concurrent runs. Today it holds the
// validated graph, the entry/finish roles, the GraphVersion fingerprint (§8.1),
// and the single CheckpointStore set at Compile (§9); later phases extend it with
// hooks, concurrency, maxSteps, and granularity needed to actually Run (§9). Its
// only methods are the §8.1 accessors (GraphID/GraphVersion); execution and the
// rest of the control surface are later phases.
//
// The store is the INTERFACE (CheckpointStore), not a concrete type (dependency
// inversion): the default MemStore is wired at Compile, the composition point, so
// the Runner never depends on a specific backend. Per §9 it is fixed at Compile —
// ALL operations (Run, Resume, Status, Get) use this one store; there is no
// per-run override.
type Runner[S any] struct {
	graph   *Graph[S]
	entry   VertexID
	finish  VertexID
	version string          // GraphVersion fingerprint (§8.1), computed at Compile
	store   CheckpointStore // the single durable store for all ops (§9)
}

// GraphID returns the runner's stable definition identity (§3, §8.1): the pinned
// GraphID of the compiled graph. It is identity, NOT the compatibility key — use
// GraphVersion for resume compatibility.
func (r *Runner[S]) GraphID() GraphID { return r.graph.id }

// GraphVersion returns the compatibility fingerprint computed at Compile (§8.1):
// a sha256 of the graph's topology plus a ":userVersion" suffix. Resume compares
// it to the checkpoint's; any difference is a GraphVersionMismatchError, so a
// changed graph cannot resume an old checkpoint.
func (r *Runner[S]) GraphVersion() string { return r.version }

// compileConfig is the resolved Compile-time configuration assembled from
// CompileOptions (§8). store holds the optional caller-supplied CheckpointStore;
// a nil store (option unset, or WithStore(nil)) means "use the default MemStore",
// resolved in Compile. Later phases add hooks/concurrency/maxSteps/granularity
// fields here.
type compileConfig struct {
	store CheckpointStore // nil => Compile wires a default MemStore (§9)
}

// CompileOption configures Compile (§8). It is non-generic so the same option
// value works for any state type S, since the store is fixed at Compile (§9) and
// not parameterized by S.
type CompileOption func(*compileConfig)

// WithStore pins the CheckpointStore the Runner uses for ALL operations — Run,
// Resume, Status, Get (§9). The store is fixed at Compile; there is no per-run
// override. Passing nil is not an error: Compile falls back to a fresh default
// MemStore, so a zero/nil store never reaches the Runner (fail safe).
func WithStore(s CheckpointStore) CompileOption {
	return func(c *compileConfig) { c.store = s }
}

// Compile validates the whole graph (§8) and, on success, returns an immutable
// *Runner[S] bound to entry and finish and to a single CheckpointStore (§9). It
// runs every §8 check in the documented first-error order (see the file comment)
// and returns the FIRST typed violation; on success the returned Runner is
// non-nil. The store comes from WithStore if supplied non-nil, else a fresh
// default MemStore (the default is wired here, the composition point). A malformed
// graph fails secure with no Runner.
func (g *Graph[S]) Compile(entry, finish VertexID, opts ...CompileOption) (*Runner[S], error) {
	cfg := compileConfig{}
	for _, opt := range opts {
		opt(&cfg)
	}

	if _, ok := g.vertices[entry]; !ok {
		return nil, &MissingEntryError{VertexID: entry, Role: "entry"}
	}
	if _, ok := g.vertices[finish]; !ok {
		return nil, &MissingEntryError{VertexID: finish, Role: "finish"}
	}
	if err := g.validateEndpoints(); err != nil {
		return nil, err
	}
	if err := g.checkAmbiguous(); err != nil {
		return nil, err
	}
	if err := g.checkReachable(entry); err != nil {
		return nil, err
	}

	// Validation passed. Resolve the single store (§9): the caller's via WithStore
	// if non-nil, else a fresh default MemStore — wired here, the composition point,
	// so the Runner depends only on the CheckpointStore interface. Stamp the §8.1
	// fingerprint so a later Resume against a changed graph fails loudly
	// (GraphVersionMismatchError).
	store := cfg.store
	if store == nil {
		store = NewMemStore()
	}
	return &Runner[S]{
		graph:   g,
		entry:   entry,
		finish:  finish,
		version: graphVersion(g, entry, finish),
		store:   store,
	}, nil
}

// validateEndpoints checks that every topology endpoint references a known vertex
// (§8): each static-edge from and to, each conditional-edge from and Target, and
// each error-route handler. It returns the first *UnknownVertexError it finds.
func (g *Graph[S]) validateEndpoints() error {
	for from, tos := range g.edges {
		if _, ok := g.vertices[from]; !ok {
			return &UnknownVertexError{VertexID: from}
		}
		for _, to := range tos {
			if _, ok := g.vertices[to]; !ok {
				return &UnknownVertexError{VertexID: to}
			}
		}
	}
	for from, cond := range g.conds {
		if _, ok := g.vertices[from]; !ok {
			return &UnknownVertexError{VertexID: from}
		}
		for _, target := range cond.Targets {
			if _, ok := g.vertices[target]; !ok {
				return &UnknownVertexError{VertexID: target}
			}
		}
	}
	for _, v := range g.vertices {
		if v.config.errorRoute == nil {
			continue
		}
		if _, ok := g.vertices[v.config.errorRoute.handler]; !ok {
			return &UnknownVertexError{VertexID: v.config.errorRoute.handler}
		}
	}
	return nil
}

// checkAmbiguous rejects any vertex that has BOTH a static out-edge and a
// conditional edge, which would make its routing ambiguous (§8). It returns the
// first *AmbiguousRoutingError it finds. Every key in g.edges has ≥1 out-edge
// (AddEdge always appends), so a from present in g.edges and in g.conds is the
// ambiguity.
func (g *Graph[S]) checkAmbiguous() error {
	for from := range g.edges {
		if _, ok := g.conds[from]; ok {
			return &AmbiguousRoutingError{VertexID: from}
		}
	}
	return nil
}

// checkReachable proves every vertex is reachable from entry (§8) by a BFS that
// follows static edges, conditional Targets, AND error-route handlers (the
// error-route endpoint is part of the topology — §8/§8.1). Reaching every vertex
// subsumes "finish is reachable". It returns the first *UnreachableVertexError
// for any vertex left unvisited.
func (g *Graph[S]) checkReachable(entry VertexID) error {
	visited := make(map[VertexID]bool, len(g.vertices))
	queue := []VertexID{entry}
	visited[entry] = true
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		for _, next := range g.successors(cur) {
			if !visited[next] {
				visited[next] = true
				queue = append(queue, next)
			}
		}
	}
	for id := range g.vertices {
		if !visited[id] {
			return &UnreachableVertexError{VertexID: id}
		}
	}
	return nil
}

// successors returns every vertex reachable in one hop from id following the
// routable topology: static out-edges, conditional Targets, and the error-route
// handler. Endpoint existence is validated separately (validateEndpoints), so by
// the time reachability runs every returned id is a known vertex.
//
// The error-route lookup's `ok` is a DEFENSIVE guard: in the current call graph
// successors only runs over ids that are graph vertices (BFS seeds with entry,
// validated to exist, and only ever enqueues known vertices), so the lookup never
// misses today. It is kept so a future caller that passes an unknown id gets a
// safe nil-route skip rather than a map-miss panic, not because it can trip now.
func (g *Graph[S]) successors(id VertexID) []VertexID {
	var next []VertexID
	next = append(next, g.edges[id]...)
	if cond, ok := g.conds[id]; ok {
		next = append(next, cond.Targets...)
	}
	if v, ok := g.vertices[id]; ok && v.config.errorRoute != nil {
		next = append(next, v.config.errorRoute.handler)
	}
	return next
}
