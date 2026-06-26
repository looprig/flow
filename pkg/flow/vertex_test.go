package flow

import (
	"context"
	"errors"
	"testing"
	"time"
)

// st is a tiny shared state used to drive the erasure seam end-to-end: a vertex
// reads in (the selector input) and writes out (the reduced output).
type st struct {
	in  int
	out string
}

// vID is a small helper minting a deterministic non-zero VertexID from a single
// byte so tests need not depend on uuid generation.
func vID(b byte) VertexID {
	var id VertexID
	id[0] = b
	return id
}

// errReduce is a sentinel a reducer can return to prove applyReducer surfaces it.
var errReduce = errors.New("reduce failed")

// TestErasureRoundTrip is the core contract of the type-erasure seam: a vertex
// bound via AddVertex over a real typed FuncTask[int,string] with a selector
// (S→int) and reducer (string→S) must, when driven through the three erased
// closures, round-trip the concrete types and mutate state correctly (§6.2).
func TestErasureRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		start     st
		taskFn    TaskFunc[int, string]
		selector  Selector[st, int]
		reducer   Reducer[st, string]
		wantInput int
		wantOut   string
		wantState st
	}{
		{
			name:      "happy path round-trips int->string and folds into state",
			start:     st{in: 7},
			taskFn:    func(_ context.Context, in int) (string, error) { return "v=" + itoa(in), nil },
			selector:  func(s st) int { return s.in },
			reducer:   func(s *st, out string) error { s.out = out; return nil },
			wantInput: 7,
			wantOut:   "v=7",
			wantState: st{in: 7, out: "v=7"},
		},
		{
			name:      "boundary: zero input and empty output",
			start:     st{in: 0},
			taskFn:    func(_ context.Context, _ int) (string, error) { return "", nil },
			selector:  func(s st) int { return s.in },
			reducer:   func(s *st, out string) error { s.out = out; return nil },
			wantInput: 0,
			wantOut:   "",
			wantState: st{in: 0, out: ""},
		},
		{
			name:      "negative input round-trips",
			start:     st{in: -3},
			taskFn:    func(_ context.Context, in int) (string, error) { return "v=" + itoa(in), nil },
			selector:  func(s st) int { return s.in },
			reducer:   func(s *st, out string) error { s.out = out; return nil },
			wantInput: -3,
			wantOut:   "v=-3",
			wantState: st{in: -3, out: "v=-3"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := NewGraph[st](GraphID{})
			id := vID(1)
			if err := AddVertex(g, id, NewFuncTask(tt.taskFn), tt.selector, tt.reducer); err != nil {
				t.Fatalf("AddVertex() error = %v", err)
			}
			v := g.vertices[id]
			if v == nil {
				t.Fatal("vertex not stored in graph")
			}

			// 1) selectInput: S -> any (boxes the concrete I).
			boxedIn := v.selectInput(tt.start)
			gotIn, ok := boxedIn.(int)
			if !ok {
				t.Fatalf("selectInput returned %T, want int", boxedIn)
			}
			if gotIn != tt.wantInput {
				t.Errorf("selectInput = %d, want %d", gotIn, tt.wantInput)
			}

			// 2) execute: asserts in.(int), runs task, returns O boxed.
			boxedOut, err := v.execute(context.Background(), boxedIn)
			if err != nil {
				t.Fatalf("execute() error = %v", err)
			}
			gotOut, ok := boxedOut.(string)
			if !ok {
				t.Fatalf("execute returned %T, want string", boxedOut)
			}
			if gotOut != tt.wantOut {
				t.Errorf("execute = %q, want %q", gotOut, tt.wantOut)
			}

			// 3) applyReducer: asserts out.(string), folds into *S.
			state := tt.start
			if err := v.applyReducer(&state, boxedOut); err != nil {
				t.Fatalf("applyReducer() error = %v", err)
			}
			if state != tt.wantState {
				t.Errorf("state = %+v, want %+v", state, tt.wantState)
			}
		})
	}
}

// TestExecuteSurfacesTaskError proves the erased execute closure forwards a
// task's error verbatim (and does not box a garbage output that the caller would
// then try to reduce).
func TestExecuteSurfacesTaskError(t *testing.T) {
	t.Parallel()

	g := NewGraph[st](GraphID{})
	id := vID(2)
	task := NewFuncTask(func(_ context.Context, _ int) (string, error) { return "", errTask })
	if err := AddVertex(g, id, task,
		func(s st) int { return s.in },
		func(s *st, out string) error { s.out = out; return nil }); err != nil {
		t.Fatalf("AddVertex() error = %v", err)
	}
	v := g.vertices[id]
	if _, err := v.execute(context.Background(), 1); !errors.Is(err, errTask) {
		t.Fatalf("execute() error = %v, want %v", err, errTask)
	}
}

// TestApplyReducerSurfacesError proves applyReducer surfaces a reducer error.
func TestApplyReducerSurfacesError(t *testing.T) {
	t.Parallel()

	g := NewGraph[st](GraphID{})
	id := vID(3)
	if err := AddVertex(g, id, NewFuncTask(func(_ context.Context, in int) (string, error) { return "ok", nil }),
		func(s st) int { return s.in },
		func(s *st, out string) error { return errReduce }); err != nil {
		t.Fatalf("AddVertex() error = %v", err)
	}
	v := g.vertices[id]
	state := st{}
	if err := v.applyReducer(&state, "ok"); !errors.Is(err, errReduce) {
		t.Fatalf("applyReducer() error = %v, want %v", err, errReduce)
	}
}

// TestSeamTypeMismatch proves the validated seam: feeding execute/applyReducer a
// wrong-typed any returns the typed internal error and NEVER panics. The recover
// guard fails the test if any closure panics, proving the assertion is checked
// rather than letting Go's runtime panic on a bad type assertion.
func TestSeamTypeMismatch(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func(v *vertex[st]) error
	}{
		{
			name: "execute with wrong input type",
			call: func(v *vertex[st]) error {
				_, err := v.execute(context.Background(), "not an int")
				return err
			},
		},
		{
			name: "applyReducer with wrong output type",
			call: func(v *vertex[st]) error {
				s := st{}
				return v.applyReducer(&s, 42) // want string, got int
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := NewGraph[st](GraphID{})
			id := vID(4)
			if err := AddVertex(g, id, NewFuncTask(func(_ context.Context, in int) (string, error) { return "ok", nil }),
				func(s st) int { return s.in },
				func(s *st, out string) error { s.out = out; return nil }); err != nil {
				t.Fatalf("AddVertex() error = %v", err)
			}
			v := g.vertices[id]

			var err error
			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("seam panicked instead of returning a typed error: %v", r)
					}
				}()
				err = tt.call(v)
			}()

			var ite *internalTypeError
			if !errors.As(err, &ite) {
				t.Fatalf("error = %v (%T), want *internalTypeError", err, err)
			}
		})
	}
}

// TestSeamErrorMessages pins the human-readable Error() strings of the two new
// typed errors so a log line identifies the failure; it also drives the
// internalTypeError message through both an interface-typed binding (I = error,
// exercising typeName's pointer/interface branch) and a concrete one.
func TestSeamErrorMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want string
	}{
		{
			name: "BuildError formats Op and Detail",
			err:  &BuildError{Op: "AddVertex", Detail: "nil task"},
			want: "flow: AddVertex: nil task",
		},
		{
			name: "internalTypeError formats seam, want, and got",
			err:  &internalTypeError{Seam: "execute", Want: "int", Got: "string"},
			want: "flow: internal type error at execute: expected int, got string",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.err.Error(); got != tt.want {
				t.Errorf("Error() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestSeamInterfaceTypedBinding drives the seam with an interface input type
// (I = error) so typeName must format an interface zero value via the (*T)(nil)
// trick rather than the bare <nil>. A wrong-typed input must still return a
// typed internalTypeError (and a matching input must round-trip).
func TestSeamInterfaceTypedBinding(t *testing.T) {
	t.Parallel()

	// A binding whose input type is the interface `error`.
	type ifState struct{ msg string }
	g := NewGraph[ifState](GraphID{})
	id := vID(8)
	if err := AddVertex(g, id,
		NewFuncTask(func(_ context.Context, in error) (string, error) {
			if in == nil {
				return "", nil
			}
			return in.Error(), nil
		}),
		func(s ifState) error {
			if s.msg == "" {
				return nil
			}
			return errors.New(s.msg)
		},
		func(s *ifState, out string) error { s.msg = out; return nil }); err != nil {
		t.Fatalf("AddVertex() error = %v", err)
	}
	v := g.vertices[id]

	// Wrong-typed input (int, not error) → typed internalTypeError naming the
	// interface type, no panic.
	_, err := v.execute(context.Background(), 123)
	var ite *internalTypeError
	if !errors.As(err, &ite) {
		t.Fatalf("error = %v (%T), want *internalTypeError", err, err)
	}
	if ite.Want == "" || ite.Want == "<nil>" {
		t.Errorf("internalTypeError.Want = %q, want a real interface type name", ite.Want)
	}

	// Matching input (an error) round-trips.
	boxedIn := v.selectInput(ifState{msg: "boom"})
	boxedOut, err := v.execute(context.Background(), boxedIn)
	if err != nil {
		t.Fatalf("execute() error = %v", err)
	}
	if got, _ := boxedOut.(string); got != "boom" {
		t.Errorf("execute = %q, want %q", got, "boom")
	}
}

// TestVertexOptions proves each option mutates the expected vertexConfig field
// and that WithErrorPause after WithErrorRoute clears the route (last-wins
// ordering documented on WithErrorPause).
func TestVertexOptions(t *testing.T) {
	t.Parallel()

	handler := vID(9)
	rec := func(s *st, e error) error { return nil }
	policy := RetryPolicy{MaxAttempts: 3}

	tests := []struct {
		name  string
		opts  []VertexOption[st]
		check func(t *testing.T, c vertexConfig[st])
	}{
		{
			name: "default config has no options",
			opts: nil,
			check: func(t *testing.T, c vertexConfig[st]) {
				if c.timeout != 0 || c.retry != nil || c.errorRoute != nil {
					t.Errorf("default config not empty: %+v", c)
				}
			},
		},
		{
			name: "WithTimeout sets timeout",
			opts: []VertexOption[st]{WithTimeout[st](5 * time.Second)},
			check: func(t *testing.T, c vertexConfig[st]) {
				if c.timeout != 5*time.Second {
					t.Errorf("timeout = %v, want 5s", c.timeout)
				}
			},
		},
		{
			name: "WithRetry sets retry policy",
			opts: []VertexOption[st]{WithRetry[st](policy)},
			check: func(t *testing.T, c vertexConfig[st]) {
				if c.retry == nil || c.retry.MaxAttempts != 3 {
					t.Errorf("retry = %+v, want MaxAttempts 3", c.retry)
				}
			},
		},
		{
			name: "WithErrorRoute sets error route handler and record",
			opts: []VertexOption[st]{WithErrorRoute[st](handler, rec)},
			check: func(t *testing.T, c vertexConfig[st]) {
				if c.errorRoute == nil {
					t.Fatal("errorRoute = nil, want set")
				}
				if c.errorRoute.handler != handler {
					t.Errorf("handler = %v, want %v", c.errorRoute.handler, handler)
				}
				if c.errorRoute.record == nil {
					t.Error("record reducer = nil, want set")
				}
			},
		},
		{
			name: "WithErrorPause clears a prior route (last-wins)",
			opts: []VertexOption[st]{WithErrorRoute[st](handler, rec), WithErrorPause[st]()},
			check: func(t *testing.T, c vertexConfig[st]) {
				if c.errorRoute != nil {
					t.Errorf("errorRoute = %+v, want nil after WithErrorPause", c.errorRoute)
				}
			},
		},
		{
			name: "options accumulate across multiple opts",
			opts: []VertexOption[st]{WithTimeout[st](time.Second), WithRetry[st](policy), WithErrorRoute[st](handler, rec)},
			check: func(t *testing.T, c vertexConfig[st]) {
				if c.timeout != time.Second {
					t.Errorf("timeout = %v, want 1s", c.timeout)
				}
				if c.retry == nil {
					t.Error("retry = nil, want set")
				}
				if c.errorRoute == nil {
					t.Error("errorRoute = nil, want set")
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := NewGraph[st](GraphID{})
			id := vID(5)
			if err := AddVertex(g, id, NewFuncTask(func(_ context.Context, in int) (string, error) { return "ok", nil }),
				func(s st) int { return s.in },
				func(s *st, out string) error { s.out = out; return nil },
				tt.opts...); err != nil {
				t.Fatalf("AddVertex() error = %v", err)
			}
			tt.check(t, g.vertices[id].config)
		})
	}
}

// TestAddVertexValidation covers AddVertex's fail-fast guards: duplicate id,
// zero id, and nil task/selector/reducer.
func TestAddVertexValidation(t *testing.T) {
	t.Parallel()

	okTask := NewFuncTask(func(_ context.Context, in int) (string, error) { return "ok", nil })
	okSel := func(s st) int { return s.in }
	okRed := func(s *st, out string) error { s.out = out; return nil }

	tests := []struct {
		name      string
		setup     func(g *Graph[st]) // optional: pre-seed graph
		id        VertexID
		task      Task[int, string]
		selector  Selector[st, int]
		reducer   Reducer[st, string]
		wantDup   bool
		wantBuild bool // expect a BuildError (nil arg or zero id)
	}{
		{
			name:     "happy path adds",
			id:       vID(1),
			task:     okTask,
			selector: okSel,
			reducer:  okRed,
		},
		{
			name: "duplicate id rejected",
			setup: func(g *Graph[st]) {
				_ = AddVertex(g, vID(1), okTask, okSel, okRed)
			},
			id:       vID(1),
			task:     okTask,
			selector: okSel,
			reducer:  okRed,
			wantDup:  true,
		},
		{
			name:      "zero id rejected",
			id:        VertexID{},
			task:      okTask,
			selector:  okSel,
			reducer:   okRed,
			wantBuild: true,
		},
		{
			name:      "nil task rejected",
			id:        vID(1),
			task:      nil,
			selector:  okSel,
			reducer:   okRed,
			wantBuild: true,
		},
		{
			name:      "nil selector rejected",
			id:        vID(1),
			task:      okTask,
			selector:  nil,
			reducer:   okRed,
			wantBuild: true,
		},
		{
			name:      "nil reducer rejected",
			id:        vID(1),
			task:      okTask,
			selector:  okSel,
			reducer:   nil,
			wantBuild: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			g := NewGraph[st](GraphID{})
			if tt.setup != nil {
				tt.setup(g)
			}
			err := AddVertex(g, tt.id, tt.task, tt.selector, tt.reducer)

			switch {
			case tt.wantDup:
				var dup *DuplicateVertexError
				if !errors.As(err, &dup) {
					t.Fatalf("error = %v (%T), want *DuplicateVertexError", err, err)
				}
				if dup.VertexID != tt.id {
					t.Errorf("DuplicateVertexError.VertexID = %v, want %v", dup.VertexID, tt.id)
				}
			case tt.wantBuild:
				var be *BuildError
				if !errors.As(err, &be) {
					t.Fatalf("error = %v (%T), want *BuildError", err, err)
				}
			default:
				if err != nil {
					t.Fatalf("AddVertex() error = %v, want nil", err)
				}
				if _, ok := g.vertices[tt.id]; !ok {
					t.Error("vertex not stored after successful AddVertex")
				}
			}
		})
	}
}
