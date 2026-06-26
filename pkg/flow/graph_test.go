package flow

import (
	"context"
	"errors"
	"testing"
)

// TestNewGraph proves NewGraph initializes an empty graph with its maps ready
// (so AddVertex/AddEdge never nil-deref) and applies WithVersion to userVersion.
func TestNewGraph(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		opts        []GraphOption
		wantVersion uint64
	}{
		{name: "default version is zero", opts: nil, wantVersion: 0},
		{name: "WithVersion sets userVersion", opts: []GraphOption{WithVersion(7)}, wantVersion: 7},
		{name: "WithVersion boundary zero", opts: []GraphOption{WithVersion(0)}, wantVersion: 0},
		{name: "last WithVersion wins", opts: []GraphOption{WithVersion(1), WithVersion(9)}, wantVersion: 9},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			id := GraphID{}
			id[0] = 0xAB
			g := NewGraph[st](id, tt.opts...)
			if g == nil {
				t.Fatal("NewGraph returned nil")
			}
			if g.userVersion != tt.wantVersion {
				t.Errorf("userVersion = %d, want %d", g.userVersion, tt.wantVersion)
			}
			if g.id != id {
				t.Errorf("id = %v, want %v", g.id, id)
			}
			if g.vertices == nil || g.edges == nil || g.conds == nil {
				t.Fatal("NewGraph did not initialize maps")
			}
			if len(g.vertices) != 0 || len(g.edges) != 0 || len(g.conds) != 0 {
				t.Error("NewGraph maps not empty")
			}
		})
	}
}

// TestAddEdge proves static out-edges are recorded (append, ordered), fan-out
// from one vertex is allowed (two out-edges), self-edges (cycles) are allowed,
// and zero from/to are rejected with a BuildError. Endpoint existence is NOT
// checked here (deferred to Compile §8).
func TestAddEdge(t *testing.T) {
	t.Parallel()

	a, b, c := vID(1), vID(2), vID(3)

	tests := []struct {
		name      string
		edges     [][2]VertexID
		wantErr   bool
		wantEdges map[VertexID][]VertexID
	}{
		{
			name:      "single edge recorded",
			edges:     [][2]VertexID{{a, b}},
			wantEdges: map[VertexID][]VertexID{a: {b}},
		},
		{
			name:      "fan-out: two edges from one vertex recorded in order",
			edges:     [][2]VertexID{{a, b}, {a, c}},
			wantEdges: map[VertexID][]VertexID{a: {b, c}},
		},
		{
			name:      "self-edge (cycle) allowed",
			edges:     [][2]VertexID{{a, a}},
			wantEdges: map[VertexID][]VertexID{a: {a}},
		},
		{
			name:      "edge to not-yet-added vertex allowed (existence deferred to Compile)",
			edges:     [][2]VertexID{{a, vID(99)}},
			wantEdges: map[VertexID][]VertexID{a: {vID(99)}},
		},
		{
			name:    "zero from rejected",
			edges:   [][2]VertexID{{VertexID{}, b}},
			wantErr: true,
		},
		{
			name:    "zero to rejected",
			edges:   [][2]VertexID{{a, VertexID{}}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := NewGraph[st](GraphID{})
			var lastErr error
			for _, e := range tt.edges {
				lastErr = g.AddEdge(e[0], e[1])
			}
			if tt.wantErr {
				var be *BuildError
				if !errors.As(lastErr, &be) {
					t.Fatalf("AddEdge() error = %v (%T), want *BuildError", lastErr, lastErr)
				}
				return
			}
			if lastErr != nil {
				t.Fatalf("AddEdge() error = %v, want nil", lastErr)
			}
			for from, want := range tt.wantEdges {
				got := g.edges[from]
				if len(got) != len(want) {
					t.Fatalf("edges[%v] = %v, want %v", from, got, want)
				}
				for i := range want {
					if got[i] != want[i] {
						t.Errorf("edges[%v][%d] = %v, want %v", from, i, got[i], want[i])
					}
				}
			}
		})
	}
}

// TestAddConditionalEdge proves a condition is recorded with its Targets and
// Pick; a second conditional on the same from is rejected with a typed
// DuplicateConditionalEdgeError (would overwrite); nil Pick, empty Targets, and
// a zero from are rejected with a BuildError. Target existence is NOT checked here.
func TestAddConditionalEdge(t *testing.T) {
	t.Parallel()

	a, b, c := vID(1), vID(2), vID(3)
	okPick := func(_ context.Context, _ st) ([]VertexID, error) { return []VertexID{b}, nil }

	tests := []struct {
		name    string
		from    VertexID
		conds   []Condition[st] // applied in order to `from`
		wantErr bool            // expect a malformed-argument BuildError
		wantDup bool            // expect a DuplicateConditionalEdgeError
	}{
		{
			name:  "happy path records targets and pick",
			from:  a,
			conds: []Condition[st]{{Targets: []VertexID{b, c}, Pick: okPick}},
		},
		{
			name:  "single target condition allowed",
			from:  a,
			conds: []Condition[st]{{Targets: []VertexID{b}, Pick: okPick}},
		},
		{
			name: "second conditional on same from rejected",
			from: a,
			conds: []Condition[st]{
				{Targets: []VertexID{b}, Pick: okPick},
				{Targets: []VertexID{c}, Pick: okPick},
			},
			wantDup: true,
		},
		{
			name:    "nil Pick rejected",
			from:    a,
			conds:   []Condition[st]{{Targets: []VertexID{b}, Pick: nil}},
			wantErr: true,
		},
		{
			name:    "empty Targets rejected",
			from:    a,
			conds:   []Condition[st]{{Targets: nil, Pick: okPick}},
			wantErr: true,
		},
		{
			name:    "zero from rejected",
			from:    VertexID{},
			conds:   []Condition[st]{{Targets: []VertexID{b}, Pick: okPick}},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := NewGraph[st](GraphID{})
			var lastErr error
			for _, c := range tt.conds {
				lastErr = g.AddConditionalEdge(tt.from, c)
			}
			if tt.wantDup {
				var dup *DuplicateConditionalEdgeError
				if !errors.As(lastErr, &dup) {
					t.Fatalf("AddConditionalEdge() error = %v (%T), want *DuplicateConditionalEdgeError", lastErr, lastErr)
				}
				if dup.From != tt.from {
					t.Errorf("DuplicateConditionalEdgeError.From = %v, want %v", dup.From, tt.from)
				}
				want := "flow: duplicate conditional edge for vertex " + tt.from.String()
				if got := dup.Error(); got != want {
					t.Errorf("Error() = %q, want %q", got, want)
				}
				return
			}
			if tt.wantErr {
				var be *BuildError
				if !errors.As(lastErr, &be) {
					t.Fatalf("AddConditionalEdge() error = %v (%T), want *BuildError", lastErr, lastErr)
				}
				return
			}
			if lastErr != nil {
				t.Fatalf("AddConditionalEdge() error = %v, want nil", lastErr)
			}
			got, ok := g.conds[tt.from]
			if !ok {
				t.Fatal("condition not recorded")
			}
			want := tt.conds[len(tt.conds)-1]
			if len(got.Targets) != len(want.Targets) {
				t.Fatalf("Targets = %v, want %v", got.Targets, want.Targets)
			}
			if got.Pick == nil {
				t.Error("Pick not recorded")
			}
		})
	}
}

// TestMixedRoutingAllowedAtBuild proves a static edge AND a conditional edge on
// the same from are both accepted at build time (the ambiguity is a Compile
// check, §8 — not enforced here).
func TestMixedRoutingAllowedAtBuild(t *testing.T) {
	t.Parallel()

	a, b, c := vID(1), vID(2), vID(3)
	g := NewGraph[st](GraphID{})
	if err := g.AddEdge(a, b); err != nil {
		t.Fatalf("AddEdge() error = %v", err)
	}
	if err := g.AddConditionalEdge(a, Condition[st]{
		Targets: []VertexID{c},
		Pick:    func(_ context.Context, _ st) ([]VertexID, error) { return []VertexID{c}, nil },
	}); err != nil {
		t.Fatalf("AddConditionalEdge() error = %v, want nil (ambiguity is a Compile check)", err)
	}
	if len(g.edges[a]) != 1 || g.conds[a].Pick == nil {
		t.Error("both static edge and conditional edge should be recorded at build time")
	}
}

// TestAddConditionalEdgeCopiesTargets proves AddConditionalEdge defensively
// copies c.Targets: mutating the caller's ORIGINAL slice afterward must not
// change the Targets stored in the graph (graph state is owned, not aliased).
func TestAddConditionalEdgeCopiesTargets(t *testing.T) {
	t.Parallel()

	a, b, c := vID(1), vID(2), vID(3)
	original := []VertexID{b, c}
	g := NewGraph[st](GraphID{})
	if err := g.AddConditionalEdge(a, Condition[st]{
		Targets: original,
		Pick:    func(_ context.Context, _ st) ([]VertexID, error) { return []VertexID{b}, nil },
	}); err != nil {
		t.Fatalf("AddConditionalEdge() error = %v", err)
	}

	// Mutate the caller's original slice after the add.
	original[0] = vID(99)
	original[1] = vID(98)

	got := g.conds[a].Targets
	if len(got) != 2 || got[0] != b || got[1] != c {
		t.Errorf("stored Targets = %v, want %v (caller mutation must not leak into graph)", got, []VertexID{b, c})
	}
}
