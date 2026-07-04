package flow

import (
	"context"
	"strings"
	"testing"

	"github.com/looprig/core/uuid"
)

// This file tests the GraphVersion compatibility fingerprint and the GraphID()/
// GraphVersion() accessors (design §8.1). The fingerprint is computed at Compile
// over the graph's TOPOLOGY ONLY (sorted vertex ids, sorted edges, sorted
// conditional targets, sorted error-routes, entry, finish) plus the userVersion
// suffix, so a changed graph fails resume loudly. The properties under test:
// deterministic, topology-sensitive, behavior-insensitive (closures don't count),
// WithVersion-sensitive (suffix only), and order-independent.

// buildLinear wires entry->a->finish into g (all three vertices added) using the
// shared addV helper from compile_test.go. It is the canonical topology fixture
// the determinism/behavior/order tests reuse with different closures or orders.
func buildLinear(t *testing.T, g *Graph[st], entry, a, finish VertexID) {
	t.Helper()
	addV(t, g, entry)
	addV(t, g, a)
	addV(t, g, finish)
	if err := g.AddEdge(entry, a); err != nil {
		t.Fatalf("AddEdge(entry,a): %v", err)
	}
	if err := g.AddEdge(a, finish); err != nil {
		t.Fatalf("AddEdge(a,finish): %v", err)
	}
}

// compileLinear builds buildLinear into a fresh graph (with opts) and compiles it,
// failing the test on any error. It returns the Runner so callers can read its
// GraphVersion()/GraphID().
func compileLinear(t *testing.T, entry, a, finish VertexID, opts ...GraphOption) *Runner[st] {
	t.Helper()
	g := NewGraph[st](GraphID{}, opts...)
	buildLinear(t, g, entry, a, finish)
	r, err := g.Compile(entry, finish)
	if err != nil {
		t.Fatalf("Compile() error = %v, want nil", err)
	}
	return r
}

// TestGraphVersionDeterministic proves compiling the SAME graph definition twice
// yields the SAME GraphVersion: the fingerprint is a pure function of topology +
// userVersion, stable across separate builds and compiles.
func TestGraphVersionDeterministic(t *testing.T) {
	t.Parallel()

	entry, a, finish := vID(1), vID(2), vID(3)

	first := compileLinear(t, entry, a, finish).GraphVersion()
	second := compileLinear(t, entry, a, finish).GraphVersion()

	if first != second {
		t.Errorf("GraphVersion not deterministic: %q != %q", first, second)
	}
	if first == "" {
		t.Error("GraphVersion is empty")
	}
	if got := strings.Count(first, ":"); got != 1 {
		t.Errorf("GraphVersion %q has %d colons, want exactly 1", first, got)
	}
}

// TestGraphVersionTopologySensitive proves every topology change moves the hash:
// adding a vertex, rewiring an edge, changing a Condition.Targets set, and
// adding/removing an error-route all differ from the baseline fingerprint.
func TestGraphVersionTopologySensitive(t *testing.T) {
	t.Parallel()

	entry, a, b, finish := vID(1), vID(2), vID(4), vID(3)
	baseline := compileLinear(t, entry, a, finish).GraphVersion()

	tests := []struct {
		name  string
		build func(g *Graph[st]) (VertexID, VertexID)
	}{
		{
			name: "add a vertex (extra reachable node)",
			build: func(g *Graph[st]) (VertexID, VertexID) {
				buildLinear(t, g, entry, a, finish)
				addV(t, g, b)
				if err := g.AddEdge(a, b); err != nil {
					t.Fatalf("AddEdge(a,b): %v", err)
				}
				if err := g.AddEdge(b, finish); err != nil {
					t.Fatalf("AddEdge(b,finish): %v", err)
				}
				return entry, finish
			},
		},
		{
			name: "rewire an edge (entry->finish instead of entry->a->finish)",
			build: func(g *Graph[st]) (VertexID, VertexID) {
				addV(t, g, entry)
				addV(t, g, a)
				addV(t, g, finish)
				if err := g.AddEdge(entry, finish); err != nil {
					t.Fatalf("AddEdge(entry,finish): %v", err)
				}
				if err := g.AddEdge(entry, a); err != nil {
					t.Fatalf("AddEdge(entry,a): %v", err)
				}
				if err := g.AddEdge(a, finish); err != nil {
					t.Fatalf("AddEdge(a,finish): %v", err)
				}
				return entry, finish
			},
		},
		{
			name: "change conditional Targets set",
			build: func(g *Graph[st]) (VertexID, VertexID) {
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
					t.Fatalf("AddEdge(a,finish): %v", err)
				}
				return entry, finish
			},
		},
		{
			name: "add an error-route",
			build: func(g *Graph[st]) (VertexID, VertexID) {
				addV(t, g, entry, WithErrorRoute[st](a, recordReducer))
				addV(t, g, a)
				addV(t, g, finish)
				if err := g.AddEdge(entry, finish); err != nil {
					t.Fatalf("AddEdge(entry,finish): %v", err)
				}
				if err := g.AddEdge(a, finish); err != nil {
					t.Fatalf("AddEdge(a,finish): %v", err)
				}
				return entry, finish
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := NewGraph[st](GraphID{})
			e, f := tt.build(g)
			r, err := g.Compile(e, f)
			if err != nil {
				t.Fatalf("Compile() error = %v, want nil", err)
			}
			if got := r.GraphVersion(); got == baseline {
				t.Errorf("GraphVersion %q matches baseline; topology change not reflected", got)
			}
		})
	}
}

// TestGraphVersionErrorRouteRemoval proves removing an error-route also changes
// the fingerprint: a graph with an error-route and the same graph without it
// produce different GraphVersions (the complement of the add case).
func TestGraphVersionErrorRouteRemoval(t *testing.T) {
	t.Parallel()

	entry, a, finish := vID(1), vID(2), vID(3)

	withRoute := func() *Runner[st] {
		g := NewGraph[st](GraphID{})
		addV(t, g, entry, WithErrorRoute[st](a, recordReducer))
		addV(t, g, a)
		addV(t, g, finish)
		if err := g.AddEdge(entry, finish); err != nil {
			t.Fatalf("AddEdge(entry,finish): %v", err)
		}
		if err := g.AddEdge(a, finish); err != nil {
			t.Fatalf("AddEdge(a,finish): %v", err)
		}
		r, err := g.Compile(entry, finish)
		if err != nil {
			t.Fatalf("Compile(withRoute): %v", err)
		}
		return r
	}

	noRoute := compileLinear(t, entry, a, finish)

	if withRoute().GraphVersion() == noRoute.GraphVersion() {
		t.Error("GraphVersion identical with and without error-route")
	}
}

// TestGraphVersionBehaviorInsensitive proves two graphs with IDENTICAL topology
// but DIFFERENT task/selector/reducer/Pick closures produce the SAME
// GraphVersion: closures are not hashable, so only topology counts (behavior
// changes require an explicit WithVersion bump, not an automatic hash change).
func TestGraphVersionBehaviorInsensitive(t *testing.T) {
	t.Parallel()

	entry, a, finish := vID(1), vID(2), vID(3)

	// buildWith wires identical topology (entry->a static, a->{finish} conditional)
	// but with DIFFERENT task/selector/reducer/Pick closures per call, so the only
	// thing that varies between g1 and g2 is unhashable behavior.
	buildWith := func(g *Graph[st], out string, picked VertexID) {
		task := NewFuncTask(func(_ context.Context, in int) (string, error) { return out, nil })
		sel := func(s st) int { return s.in }
		red := func(s *st, o string) error { s.out = o; return nil }
		if err := AddVertex(g, entry, task, sel, red); err != nil {
			t.Fatalf("AddVertex(entry): %v", err)
		}
		addV(t, g, a)
		addV(t, g, finish)
		if err := g.AddEdge(entry, a); err != nil {
			t.Fatalf("AddEdge(entry,a): %v", err)
		}
		// a has a conditional edge to {finish} with a behavior-specific Pick.
		if err := g.AddConditionalEdge(a, Condition[st]{
			Targets: []VertexID{finish},
			Pick:    func(_ context.Context, _ st) ([]VertexID, error) { return []VertexID{picked}, nil },
		}); err != nil {
			t.Fatalf("AddConditionalEdge(a): %v", err)
		}
	}

	g1 := NewGraph[st](GraphID{})
	buildWith(g1, "first", finish)
	r1, err := g1.Compile(entry, finish)
	if err != nil {
		t.Fatalf("Compile(g1): %v", err)
	}

	g2 := NewGraph[st](GraphID{})
	buildWith(g2, "second-different-closure", finish)
	r2, err := g2.Compile(entry, finish)
	if err != nil {
		t.Fatalf("Compile(g2): %v", err)
	}

	if r1.GraphVersion() != r2.GraphVersion() {
		t.Errorf("GraphVersion differs for identical topology with different closures: %q != %q",
			r1.GraphVersion(), r2.GraphVersion())
	}
}

// TestGraphVersionWithVersionBump proves the userVersion suffix differentiates an
// otherwise-identical topology: WithVersion(1) vs WithVersion(2) vs default (0)
// all differ in GraphVersion, while the sha256 PREFIX (before the ":") is
// IDENTICAL across them (only the suffix changes).
func TestGraphVersionWithVersionBump(t *testing.T) {
	t.Parallel()

	entry, a, finish := vID(1), vID(2), vID(3)

	v0 := compileLinear(t, entry, a, finish).GraphVersion()
	v1 := compileLinear(t, entry, a, finish, WithVersion(1)).GraphVersion()
	v2 := compileLinear(t, entry, a, finish, WithVersion(2)).GraphVersion()

	if v0 == v1 || v0 == v2 || v1 == v2 {
		t.Errorf("GraphVersions not distinct across userVersions: v0=%q v1=%q v2=%q", v0, v1, v2)
	}

	prefix := func(v string) string {
		i := strings.LastIndex(v, ":")
		if i < 0 {
			t.Fatalf("GraphVersion %q has no colon", v)
		}
		return v[:i]
	}
	suffix := func(v string) string {
		i := strings.LastIndex(v, ":")
		return v[i+1:]
	}

	if prefix(v0) != prefix(v1) || prefix(v1) != prefix(v2) {
		t.Errorf("sha256 prefix differs across userVersions: %q %q %q", prefix(v0), prefix(v1), prefix(v2))
	}
	if suffix(v0) != "0" || suffix(v1) != "1" || suffix(v2) != "2" {
		t.Errorf("userVersion suffixes = %q/%q/%q, want 0/1/2", suffix(v0), suffix(v1), suffix(v2))
	}
}

// TestGraphVersionOrderIndependent proves declaring the same edges and conditional
// Targets in a DIFFERENT order yields the SAME GraphVersion: the canonical form
// sorts every list, so declaration order cannot affect the fingerprint.
func TestGraphVersionOrderIndependent(t *testing.T) {
	t.Parallel()

	entry, a, b, finish := vID(1), vID(2), vID(4), vID(3)

	// forward declares edges + conditional Targets in one order; reverse declares
	// the same set in the opposite order. Both must hash identically.
	build := func(g *Graph[st], reverse bool) {
		addV(t, g, entry)
		addV(t, g, a)
		addV(t, g, b)
		addV(t, g, finish)

		edges := [][2]VertexID{{entry, a}, {entry, b}}
		targets := []VertexID{a, b, finish}
		if reverse {
			edges = [][2]VertexID{{entry, b}, {entry, a}}
			targets = []VertexID{finish, b, a}
		}
		for _, e := range edges {
			if err := g.AddEdge(e[0], e[1]); err != nil {
				t.Fatalf("AddEdge: %v", err)
			}
		}
		if err := g.AddConditionalEdge(a, Condition[st]{
			Targets: targets,
			Pick:    func(_ context.Context, _ st) ([]VertexID, error) { return []VertexID{finish}, nil },
		}); err != nil {
			t.Fatalf("AddConditionalEdge(a): %v", err)
		}
		if err := g.AddConditionalEdge(b, Condition[st]{
			Targets: []VertexID{finish},
			Pick:    func(_ context.Context, _ st) ([]VertexID, error) { return []VertexID{finish}, nil },
		}); err != nil {
			t.Fatalf("AddConditionalEdge(b): %v", err)
		}
	}

	gf := NewGraph[st](GraphID{})
	build(gf, false)
	rf, err := gf.Compile(entry, finish)
	if err != nil {
		t.Fatalf("Compile(forward): %v", err)
	}

	gr := NewGraph[st](GraphID{})
	build(gr, true)
	rr, err := gr.Compile(entry, finish)
	if err != nil {
		t.Fatalf("Compile(reverse): %v", err)
	}

	if rf.GraphVersion() != rr.GraphVersion() {
		t.Errorf("GraphVersion depends on declaration order: %q != %q",
			rf.GraphVersion(), rr.GraphVersion())
	}
}

// TestGraphVersionEntryFinishSensitive proves entry and finish are part of the
// fingerprint: the same vertex set/edges with different entry or finish roles
// yields a different GraphVersion (a relabeled run is not resume-compatible).
func TestGraphVersionEntryFinishSensitive(t *testing.T) {
	t.Parallel()

	a, b, c := vID(1), vID(2), vID(3)

	// A fully-connected triangle so any (entry,finish) pair compiles (every vertex
	// reachable from any entry). Swapping the roles must change the hash.
	build := func(g *Graph[st]) {
		addV(t, g, a)
		addV(t, g, b)
		addV(t, g, c)
		for _, e := range [][2]VertexID{{a, b}, {b, c}, {c, a}} {
			if err := g.AddEdge(e[0], e[1]); err != nil {
				t.Fatalf("AddEdge: %v", err)
			}
		}
	}

	g1 := NewGraph[st](GraphID{})
	build(g1)
	r1, err := g1.Compile(a, c)
	if err != nil {
		t.Fatalf("Compile(a,c): %v", err)
	}

	g2 := NewGraph[st](GraphID{})
	build(g2)
	r2, err := g2.Compile(b, c)
	if err != nil {
		t.Fatalf("Compile(b,c): %v", err)
	}

	if r1.GraphVersion() == r2.GraphVersion() {
		t.Error("GraphVersion ignores entry role; want different fingerprint")
	}
}

// TestGraphIDAccessor proves GraphID() returns the graph's pinned identity, and
// GraphVersion() returns the stored fingerprint (non-empty, exactly one colon).
func TestGraphIDAccessor(t *testing.T) {
	t.Parallel()

	var want GraphID
	want[0] = 7
	entry, a, finish := vID(1), vID(2), vID(3)

	g := NewGraph[st](want)
	buildLinear(t, g, entry, a, finish)
	r, err := g.Compile(entry, finish)
	if err != nil {
		t.Fatalf("Compile() error = %v", err)
	}

	if r.GraphID() != want {
		t.Errorf("GraphID() = %v, want %v", r.GraphID(), want)
	}
	gv := r.GraphVersion()
	if gv == "" {
		t.Error("GraphVersion() is empty")
	}
	if c := strings.Count(gv, ":"); c != 1 {
		t.Errorf("GraphVersion() %q has %d colons, want 1", gv, c)
	}
}

// pinnedVID returns a VertexID parsed from a fixed canonical UUID literal, so the
// underlying bytes (and therefore the canonical String() form) are deterministic
// across machines — required for the golden-bytes assertion below.
func pinnedVID(s string) VertexID { return VertexID(uuid.MustParse(s)) }

// TestCanonicalFormGolden pins the EXACT byte layout of canonicalForm (white-box,
// package flow): section order (V/E/C/X/R), the per-section tag, the "->","?","!"
// entry tags, the "," Targets separator, the "\n" entry separator, and the "\f"
// section separator. A refactor that changes any of these — e.g. swapping "\f"
// for "\n" — alters the resume key silently and is caught here immediately.
func TestCanonicalFormGolden(t *testing.T) {
	t.Parallel()

	entry := pinnedVID("11111111-1111-1111-1111-111111111111")
	a := pinnedVID("22222222-2222-2222-2222-222222222222")
	finish := pinnedVID("33333333-3333-3333-3333-333333333333")

	g := NewGraph[st](GraphID{})
	addV(t, g, entry, WithErrorRoute[st](a, recordReducer))
	addV(t, g, a)
	addV(t, g, finish)
	if err := g.AddEdge(entry, a); err != nil {
		t.Fatalf("AddEdge(entry,a): %v", err)
	}
	// Targets declared {finish, entry}; the canonical form must sort them to
	// entry,finish (1111…,3333…), independent of declaration order.
	if err := g.AddConditionalEdge(a, Condition[st]{
		Targets: []VertexID{finish, entry},
		Pick:    func(_ context.Context, _ st) ([]VertexID, error) { return []VertexID{finish}, nil },
	}); err != nil {
		t.Fatalf("AddConditionalEdge(a): %v", err)
	}

	want := "V\n" +
		"11111111-1111-1111-1111-111111111111\n" +
		"22222222-2222-2222-2222-222222222222\n" +
		"33333333-3333-3333-3333-333333333333" +
		"\fE\n" +
		"11111111-1111-1111-1111-111111111111->22222222-2222-2222-2222-222222222222" +
		"\fC\n" +
		"22222222-2222-2222-2222-222222222222?11111111-1111-1111-1111-111111111111,33333333-3333-3333-3333-333333333333" +
		"\fX\n" +
		"11111111-1111-1111-1111-111111111111!22222222-2222-2222-2222-222222222222" +
		"\fR\n" +
		"11111111-1111-1111-1111-111111111111\n" +
		"33333333-3333-3333-3333-333333333333"

	if got := string(canonicalForm(g, entry, finish)); got != want {
		t.Errorf("canonicalForm mismatch:\n got = %q\nwant = %q", got, want)
	}
}

// TestGraphVersionStaticVsConditionalDistinct proves a static edge a→b and a
// conditional edge from a with Targets {b} hash DIFFERENTLY: the "a->b" (E) and
// "a?b" (C) sections live in distinct, tagged sections and cannot alias, so the
// two routing kinds are never confused on resume.
func TestGraphVersionStaticVsConditionalDistinct(t *testing.T) {
	t.Parallel()

	entry, a, b := vID(1), vID(2), vID(3)

	// Static: entry->a (so a is reachable) and a->b (the edge under test).
	static := NewGraph[st](GraphID{})
	addV(t, static, entry)
	addV(t, static, a)
	addV(t, static, b)
	if err := static.AddEdge(entry, a); err != nil {
		t.Fatalf("AddEdge(entry,a): %v", err)
	}
	if err := static.AddEdge(a, b); err != nil {
		t.Fatalf("AddEdge(a,b): %v", err)
	}
	rStatic, err := static.Compile(entry, b)
	if err != nil {
		t.Fatalf("Compile(static): %v", err)
	}

	// Conditional: entry->a, then a?{b} instead of a->b. Same vertex set + roles.
	cond := NewGraph[st](GraphID{})
	addV(t, cond, entry)
	addV(t, cond, a)
	addV(t, cond, b)
	if err := cond.AddEdge(entry, a); err != nil {
		t.Fatalf("AddEdge(entry,a): %v", err)
	}
	if err := cond.AddConditionalEdge(a, Condition[st]{
		Targets: []VertexID{b},
		Pick:    func(_ context.Context, _ st) ([]VertexID, error) { return []VertexID{b}, nil },
	}); err != nil {
		t.Fatalf("AddConditionalEdge(a): %v", err)
	}
	rCond, err := cond.Compile(entry, b)
	if err != nil {
		t.Fatalf("Compile(cond): %v", err)
	}

	if rStatic.GraphVersion() == rCond.GraphVersion() {
		t.Error("static a->b and conditional a?{b} produced the same GraphVersion; sections alias")
	}
}

// TestGraphVersionEdgeDirectionality proves edge endpoints are NOT sorted within
// an edge: with the SAME vertices and the SAME entry/finish roles, a graph with
// edge a→b and a graph with edge b→a hash DIFFERENTLY. entry fans out to both a
// and b (so both are reachable and finish is fixed regardless of the flipped
// edge), isolating the one reversed edge as the only difference.
func TestGraphVersionEdgeDirectionality(t *testing.T) {
	t.Parallel()

	entry, a, b, finish := vID(1), vID(2), vID(3), vID(4)

	build := func(reverse bool) *Runner[st] {
		g := NewGraph[st](GraphID{})
		addV(t, g, entry)
		addV(t, g, a)
		addV(t, g, b)
		addV(t, g, finish)
		// entry -> a, entry -> b, and a -> finish keep the graph valid and the
		// roles fixed; only the edge under test (a->b vs b->a) flips.
		for _, e := range [][2]VertexID{{entry, a}, {entry, b}, {a, finish}} {
			if err := g.AddEdge(e[0], e[1]); err != nil {
				t.Fatalf("AddEdge: %v", err)
			}
		}
		under := [2]VertexID{a, b}
		if reverse {
			under = [2]VertexID{b, a}
		}
		if err := g.AddEdge(under[0], under[1]); err != nil {
			t.Fatalf("AddEdge(under test): %v", err)
		}
		r, err := g.Compile(entry, finish)
		if err != nil {
			t.Fatalf("Compile(reverse=%v): %v", reverse, err)
		}
		return r
	}

	if build(false).GraphVersion() == build(true).GraphVersion() {
		t.Error("a->b and b->a produced the same GraphVersion; edge directionality lost")
	}
}
