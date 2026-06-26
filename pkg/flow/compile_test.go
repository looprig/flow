package flow

import (
	"context"
	"errors"
	"testing"
)

// addV is a compile-test helper that binds a trivial int→string vertex into g
// under id, using identity-ish selector/reducer over the shared st (vertex_test.go
// defines st and vID). It fails the test on any AddVertex error so fixtures stay
// readable. opts thread through error-route policies used by the topology tests.
func addV(t *testing.T, g *Graph[st], id VertexID, opts ...VertexOption[st]) {
	t.Helper()
	task := NewFuncTask(func(_ context.Context, in int) (string, error) { return "ok", nil })
	sel := func(s st) int { return s.in }
	red := func(s *st, out string) error { s.out = out; return nil }
	if err := AddVertex(g, id, task, sel, red, opts...); err != nil {
		t.Fatalf("AddVertex(%v) error = %v", id, err)
	}
}

// recordReducer is a no-op error-route record reducer for WithErrorRoute fixtures.
func recordReducer(_ *st, _ error) error { return nil }

// TestCompileValid proves a well-formed graph compiles: every vertex reachable
// from entry via static/conditional/error-route edges, no ambiguity, all
// endpoints known. It returns a non-nil *Runner[st] carrying entry and finish,
// and nil error. Covers the happy path, the single-vertex entry==finish
// boundary, and a back-edge (cycle), which is legal.
func TestCompileValid(t *testing.T) {
	t.Parallel()

	entry, a, finish := vID(1), vID(2), vID(3)

	tests := []struct {
		name   string
		build  func(g *Graph[st])
		entry  VertexID
		finish VertexID
	}{
		{
			name: "linear entry->a->finish all reachable",
			build: func(g *Graph[st]) {
				addV(t, g, entry)
				addV(t, g, a)
				addV(t, g, finish)
				if err := g.AddEdge(entry, a); err != nil {
					t.Fatalf("AddEdge: %v", err)
				}
				if err := g.AddEdge(a, finish); err != nil {
					t.Fatalf("AddEdge: %v", err)
				}
			},
			entry:  entry,
			finish: finish,
		},
		{
			name: "single vertex entry==finish, no edges",
			build: func(g *Graph[st]) {
				addV(t, g, entry)
			},
			entry:  entry,
			finish: entry,
		},
		{
			name: "cycle: back-edge entry<-a is legal",
			build: func(g *Graph[st]) {
				addV(t, g, entry)
				addV(t, g, a)
				addV(t, g, finish)
				// entry -> a, a -> entry (back-edge / cycle), entry -> finish.
				if err := g.AddEdge(entry, a); err != nil {
					t.Fatalf("AddEdge: %v", err)
				}
				if err := g.AddEdge(a, entry); err != nil {
					t.Fatalf("AddEdge: %v", err)
				}
				if err := g.AddEdge(entry, finish); err != nil {
					t.Fatalf("AddEdge: %v", err)
				}
			},
			entry:  entry,
			finish: finish,
		},
		{
			name: "conditional edge reaches all targets",
			build: func(g *Graph[st]) {
				addV(t, g, entry)
				addV(t, g, a)
				addV(t, g, finish)
				if err := g.AddConditionalEdge(entry, Condition[st]{
					Targets: []VertexID{a, finish},
					Pick:    func(_ context.Context, _ st) ([]VertexID, error) { return []VertexID{a}, nil },
				}); err != nil {
					t.Fatalf("AddConditionalEdge: %v", err)
				}
				if err := g.AddEdge(a, finish); err != nil {
					t.Fatalf("AddEdge: %v", err)
				}
			},
			entry:  entry,
			finish: finish,
		},
		{
			name: "error-route handler counts for reachability",
			build: func(g *Graph[st]) {
				// entry routes its error to a (the handler); a -> finish.
				addV(t, g, entry, WithErrorRoute[st](a, recordReducer))
				addV(t, g, a)
				addV(t, g, finish)
				if err := g.AddEdge(entry, finish); err != nil {
					t.Fatalf("AddEdge: %v", err)
				}
				if err := g.AddEdge(a, finish); err != nil {
					t.Fatalf("AddEdge: %v", err)
				}
			},
			entry:  entry,
			finish: finish,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := NewGraph[st](GraphID{})
			tt.build(g)

			r, err := g.Compile(tt.entry, tt.finish)
			if err != nil {
				t.Fatalf("Compile() error = %v, want nil", err)
			}
			if r == nil {
				t.Fatal("Compile() returned nil Runner, want non-nil")
			}
			if r.entry != tt.entry {
				t.Errorf("Runner.entry = %v, want %v", r.entry, tt.entry)
			}
			if r.finish != tt.finish {
				t.Errorf("Runner.finish = %v, want %v", r.finish, tt.finish)
			}
			if r.graph != g {
				t.Error("Runner.graph is not the compiled graph")
			}
		})
	}
}

// TestCompileMissingEntry proves Compile rejects a missing entry / finish vertex
// with a *MissingEntryError carrying the right Role and VertexID, and that entry
// is checked before finish (first-error order).
func TestCompileMissingEntry(t *testing.T) {
	t.Parallel()

	entry, finish := vID(1), vID(2)

	tests := []struct {
		name     string
		build    func(g *Graph[st])
		entry    VertexID
		finish   VertexID
		wantRole string
		wantID   VertexID
	}{
		{
			name:     "entry absent",
			build:    func(g *Graph[st]) { addV(t, g, finish) },
			entry:    entry,
			finish:   finish,
			wantRole: "entry",
			wantID:   entry,
		},
		{
			name:     "finish absent",
			build:    func(g *Graph[st]) { addV(t, g, entry) },
			entry:    entry,
			finish:   finish,
			wantRole: "finish",
			wantID:   finish,
		},
		{
			name:     "both absent -> entry reported first",
			build:    func(g *Graph[st]) {},
			entry:    entry,
			finish:   finish,
			wantRole: "entry",
			wantID:   entry,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := NewGraph[st](GraphID{})
			tt.build(g)

			_, err := g.Compile(tt.entry, tt.finish)
			var me *MissingEntryError
			if !errors.As(err, &me) {
				t.Fatalf("Compile() error = %v (%T), want *MissingEntryError", err, err)
			}
			if me.Role != tt.wantRole {
				t.Errorf("MissingEntryError.Role = %q, want %q", me.Role, tt.wantRole)
			}
			if me.VertexID != tt.wantID {
				t.Errorf("MissingEntryError.VertexID = %v, want %v", me.VertexID, tt.wantID)
			}
		})
	}
}

// TestCompileUnknownEndpoint proves Compile rejects an edge / conditional-target
// / error-route handler that references a vertex absent from the graph, with a
// *UnknownVertexError naming the missing one.
func TestCompileUnknownEndpoint(t *testing.T) {
	t.Parallel()

	entry, finish, missing := vID(1), vID(2), vID(9)

	tests := []struct {
		name   string
		build  func(g *Graph[st])
		wantID VertexID
	}{
		{
			name: "static edge to unknown vertex",
			build: func(g *Graph[st]) {
				addV(t, g, entry)
				addV(t, g, finish)
				if err := g.AddEdge(entry, missing); err != nil {
					t.Fatalf("AddEdge: %v", err)
				}
			},
			wantID: missing,
		},
		{
			name: "static edge from unknown vertex",
			build: func(g *Graph[st]) {
				addV(t, g, entry)
				addV(t, g, finish)
				// from=missing key in g.edges; to=finish exists.
				if err := g.AddEdge(missing, finish); err != nil {
					t.Fatalf("AddEdge: %v", err)
				}
			},
			wantID: missing,
		},
		{
			name: "conditional target unknown",
			build: func(g *Graph[st]) {
				addV(t, g, entry)
				addV(t, g, finish)
				if err := g.AddConditionalEdge(entry, Condition[st]{
					Targets: []VertexID{missing},
					Pick:    func(_ context.Context, _ st) ([]VertexID, error) { return []VertexID{missing}, nil },
				}); err != nil {
					t.Fatalf("AddConditionalEdge: %v", err)
				}
			},
			wantID: missing,
		},
		{
			name: "conditional from unknown",
			build: func(g *Graph[st]) {
				addV(t, g, entry)
				addV(t, g, finish)
				if err := g.AddConditionalEdge(missing, Condition[st]{
					Targets: []VertexID{finish},
					Pick:    func(_ context.Context, _ st) ([]VertexID, error) { return []VertexID{finish}, nil },
				}); err != nil {
					t.Fatalf("AddConditionalEdge: %v", err)
				}
			},
			wantID: missing,
		},
		{
			name: "error-route handler unknown",
			build: func(g *Graph[st]) {
				addV(t, g, entry, WithErrorRoute[st](missing, recordReducer))
				addV(t, g, finish)
				if err := g.AddEdge(entry, finish); err != nil {
					t.Fatalf("AddEdge: %v", err)
				}
			},
			wantID: missing,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := NewGraph[st](GraphID{})
			tt.build(g)

			_, err := g.Compile(entry, finish)
			var ue *UnknownVertexError
			if !errors.As(err, &ue) {
				t.Fatalf("Compile() error = %v (%T), want *UnknownVertexError", err, err)
			}
			if ue.VertexID != tt.wantID {
				t.Errorf("UnknownVertexError.VertexID = %v, want %v", ue.VertexID, tt.wantID)
			}
		})
	}
}

// TestCompileAmbiguousRouting proves Compile rejects a vertex that has BOTH a
// static out-edge and a conditional edge, with a *AmbiguousRoutingError naming it.
func TestCompileAmbiguousRouting(t *testing.T) {
	t.Parallel()

	entry, a, finish := vID(1), vID(2), vID(3)

	g := NewGraph[st](GraphID{})
	addV(t, g, entry)
	addV(t, g, a)
	addV(t, g, finish)
	// entry has BOTH a static edge and a conditional edge -> ambiguous.
	if err := g.AddEdge(entry, a); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}
	if err := g.AddConditionalEdge(entry, Condition[st]{
		Targets: []VertexID{finish},
		Pick:    func(_ context.Context, _ st) ([]VertexID, error) { return []VertexID{finish}, nil },
	}); err != nil {
		t.Fatalf("AddConditionalEdge: %v", err)
	}
	if err := g.AddEdge(a, finish); err != nil {
		t.Fatalf("AddEdge: %v", err)
	}

	_, err := g.Compile(entry, finish)
	var ar *AmbiguousRoutingError
	if !errors.As(err, &ar) {
		t.Fatalf("Compile() error = %v (%T), want *AmbiguousRoutingError", err, err)
	}
	if ar.VertexID != entry {
		t.Errorf("AmbiguousRoutingError.VertexID = %v, want %v", ar.VertexID, entry)
	}
}

// TestCompileUnreachable proves Compile rejects a graph with a vertex that
// cannot be reached from entry following static, conditional, and error-route
// edges, with a *UnreachableVertexError naming the orphan.
func TestCompileUnreachable(t *testing.T) {
	t.Parallel()

	entry, finish, orphan := vID(1), vID(2), vID(3)

	tests := []struct {
		name   string
		build  func(g *Graph[st])
		wantID VertexID
	}{
		{
			name: "orphan vertex unreachable from entry",
			build: func(g *Graph[st]) {
				addV(t, g, entry)
				addV(t, g, finish)
				addV(t, g, orphan) // never wired in
				if err := g.AddEdge(entry, finish); err != nil {
					t.Fatalf("AddEdge: %v", err)
				}
			},
			wantID: orphan,
		},
		{
			name: "finish unreachable (entry isolated)",
			build: func(g *Graph[st]) {
				addV(t, g, entry)
				addV(t, g, finish)
				// no edge from entry; finish is unreachable.
			},
			wantID: finish,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := NewGraph[st](GraphID{})
			tt.build(g)

			_, err := g.Compile(entry, finish)
			var ur *UnreachableVertexError
			if !errors.As(err, &ur) {
				t.Fatalf("Compile() error = %v (%T), want *UnreachableVertexError", err, err)
			}
			if ur.VertexID != tt.wantID {
				t.Errorf("UnreachableVertexError.VertexID = %v, want %v", ur.VertexID, tt.wantID)
			}
		})
	}
}

// TestCompileOptionApplied proves Compile accepts and applies CompileOptions.
// No option exists yet (compileConfig is empty), so this asserts a no-op option
// is invoked and does not break compilation — locking the signature.
func TestCompileOptionApplied(t *testing.T) {
	t.Parallel()

	entry := vID(1)
	g := NewGraph[st](GraphID{})
	addV(t, g, entry)

	called := false
	opt := func(_ *compileConfig) { called = true }

	r, err := g.Compile(entry, entry, opt)
	if err != nil {
		t.Fatalf("Compile() error = %v, want nil", err)
	}
	if r == nil {
		t.Fatal("Compile() returned nil Runner")
	}
	if !called {
		t.Error("CompileOption was not applied")
	}
}
