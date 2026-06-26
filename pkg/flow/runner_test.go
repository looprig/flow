package flow

import (
	"context"
	"errors"
	"testing"
)

// This file white-box tests the Run entrypoint and its RunOption surface (§9):
// the runConfig defaults/overrides, the fresh-vs-supplied GraphRunID resolution,
// and the WithGraphRunID reuse rejection. The super-step loop itself is exercised
// in engine_test.go. cnt is a tiny accumulator state the linear-chain fixtures
// thread through reducers and selectors.

// cnt is a minimal graph state for Run/coordinator tests: an ordered append-only
// log of vertex outputs (Vals) and a numeric accumulator (N). It round-trips
// through the JSON codec (exported fields) so clone-and-commit works.
type cnt struct {
	Vals []string
	N    int
}

// appendTask returns a vertex Task that appends a fixed tag plus the selected
// input, so a chain's order and threading are observable in cnt.Vals.
func appendVertex(t *testing.T, g *Graph[cnt], id VertexID, tag string) {
	t.Helper()
	task := NewFuncTask(func(_ context.Context, in int) (string, error) {
		return tag, nil
	})
	sel := func(s cnt) int { return s.N }
	red := func(s *cnt, out string) error {
		s.Vals = append(s.Vals, out)
		s.N++
		return nil
	}
	if err := AddVertex(g, id, task, sel, red); err != nil {
		t.Fatalf("AddVertex(%v): %v", id, err)
	}
}

// compileSingle compiles a one-vertex graph where entry == finish.
func compileSingle(t *testing.T, store CheckpointStore) (*Runner[cnt], VertexID) {
	t.Helper()
	entry := vID(1)
	g := NewGraph[cnt](GraphID{})
	appendVertex(t, g, entry, "only")
	var opts []CompileOption
	if store != nil {
		opts = append(opts, WithStore(store))
	}
	r, err := g.Compile(entry, entry, opts...)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return r, entry
}

// TestRunFreshGraphRunID proves Run mints a fresh, non-zero GraphRunID by default
// and that two runs of the same Runner get distinct ids.
func TestRunFreshGraphRunID(t *testing.T) {
	t.Parallel()

	r, _ := compileSingle(t, nil)
	ctx := context.Background()

	res1, err := r.Run(ctx, cnt{})
	if err != nil {
		t.Fatalf("Run #1: %v", err)
	}
	res2, err := r.Run(ctx, cnt{})
	if err != nil {
		t.Fatalf("Run #2: %v", err)
	}
	if res1.Run.GraphRunID == (GraphRunID{}) {
		t.Error("Run minted a zero GraphRunID")
	}
	if res1.Run.GraphRunID == res2.Run.GraphRunID {
		t.Errorf("two runs share GraphRunID %v", res1.Run.GraphRunID)
	}
}

// TestRunWithGraphRunID covers the supplied-id path: a fresh id with no history
// is accepted and used; an id that already has history is rejected with
// *GraphRunExistsError (continuing an existing run is Resume's job, not Run's).
func TestRunWithGraphRunID(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		seedFirst bool // run once with the id first, so it already has history
		wantErr   bool
	}{
		{name: "fresh supplied id accepted", seedFirst: false, wantErr: false},
		{name: "reused id rejected", seedFirst: true, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := NewMemStore()
			r, _ := compileSingle(t, store)
			ctx := context.Background()

			id, err := NewGraphRunID()
			if err != nil {
				t.Fatalf("NewGraphRunID: %v", err)
			}
			if tt.seedFirst {
				if _, err := r.Run(ctx, cnt{}, WithGraphRunID(id)); err != nil {
					t.Fatalf("seed Run: %v", err)
				}
			}

			res, err := r.Run(ctx, cnt{}, WithGraphRunID(id))
			if tt.wantErr {
				var exists *GraphRunExistsError
				if !errors.As(err, &exists) {
					t.Fatalf("Run() error = %v, want *GraphRunExistsError", err)
				}
				if exists.GraphRunID != id {
					t.Errorf("GraphRunExistsError id = %v, want %v", exists.GraphRunID, id)
				}
				return
			}
			if err != nil {
				t.Fatalf("Run() error = %v, want nil", err)
			}
			if res.Run.GraphRunID != id {
				t.Errorf("Run used id %v, want supplied %v", res.Run.GraphRunID, id)
			}
		})
	}
}

// TestRunStatusNeverRunning proves Result.Run.Status is never RunRunning on
// return: a completed happy-path run reports RunCompleted (§9).
func TestRunStatusNeverRunning(t *testing.T) {
	t.Parallel()

	r, _ := compileSingle(t, nil)
	res, err := r.Run(context.Background(), cnt{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Run.Status == RunRunning {
		t.Error("Result.Run.Status is RunRunning on return")
	}
	if res.Run.Status != RunCompleted {
		t.Errorf("Result.Run.Status = %v, want RunCompleted", res.Run.Status)
	}
}

// TestRunOptionsResolve proves each RunOption resolves into the runConfig the
// coordinator reads, including the <=0 guards that leave defaults in place. It
// tests the option PLUMBING only; the behavioral effects of concurrency/maxSteps/
// granularity are later sub-tasks.
func TestRunOptionsResolve(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		opts            []RunOption
		wantConcurrency int
		wantMaxSteps    int
		wantGranularity CheckpointGranularity
		wantHookSets    int
	}{
		{
			name:            "defaults",
			wantConcurrency: runtimeGOMAXPROCS(),
			wantMaxSteps:    10000,
			wantGranularity: PerVertex,
		},
		{
			name:            "all set",
			opts:            []RunOption{WithConcurrency(4), WithMaxSteps(7), WithCheckpointEvery(PerStep), WithHooks(Hooks{})},
			wantConcurrency: 4,
			wantMaxSteps:    7,
			wantGranularity: PerStep,
			wantHookSets:    1,
		},
		{
			name:            "non-positive concurrency and maxSteps keep defaults",
			opts:            []RunOption{WithConcurrency(0), WithMaxSteps(-1)},
			wantConcurrency: runtimeGOMAXPROCS(),
			wantMaxSteps:    10000,
			wantGranularity: PerVertex,
		},
		{
			name:            "repeated hooks accumulate",
			opts:            []RunOption{WithHooks(Hooks{}), WithHooks(Hooks{})},
			wantConcurrency: runtimeGOMAXPROCS(),
			wantMaxSteps:    10000,
			wantGranularity: PerVertex,
			wantHookSets:    2,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cfg := defaultRunConfig()
			for _, opt := range tt.opts {
				opt(&cfg)
			}
			if cfg.concurrency != tt.wantConcurrency {
				t.Errorf("concurrency = %d, want %d", cfg.concurrency, tt.wantConcurrency)
			}
			if cfg.maxSteps != tt.wantMaxSteps {
				t.Errorf("maxSteps = %d, want %d", cfg.maxSteps, tt.wantMaxSteps)
			}
			if cfg.granularity != tt.wantGranularity {
				t.Errorf("granularity = %v, want %v", cfg.granularity, tt.wantGranularity)
			}
			if len(cfg.hooks.sets) != tt.wantHookSets {
				t.Errorf("hook sets = %d, want %d", len(cfg.hooks.sets), tt.wantHookSets)
			}
		})
	}
}

// runtimeGOMAXPROCS mirrors the default the runConfig resolves to, isolating the
// test's expectation from the import so the table stays readable.
func runtimeGOMAXPROCS() int { return defaultRunConfig().concurrency }

// TestRunResultIsHappyPath proves the happy path returns no interrupts and no
// halt (those are later sub-tasks).
func TestRunResultIsHappyPath(t *testing.T) {
	t.Parallel()

	r, _ := compileSingle(t, nil)
	res, err := r.Run(context.Background(), cnt{})
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.Interrupts != nil {
		t.Errorf("Result.Interrupts = %v, want nil on happy path", res.Interrupts)
	}
	if res.Halt != nil {
		t.Errorf("Result.Halt = %v, want nil on happy path", res.Halt)
	}
}
