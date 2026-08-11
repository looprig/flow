package graphexample_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
)

type state struct {
	Value int
	Trace []string
}

func Example_deterministicExecution() {
	graphID := flow.GraphID(uuid.MustParse("10000000-0000-4000-8000-000000000001"))
	doubleID := flow.VertexID(uuid.MustParse("10000000-0000-4000-8000-000000000002"))
	finishID := flow.VertexID(uuid.MustParse("10000000-0000-4000-8000-000000000003"))
	g := flow.NewGraph[state](graphID)

	// Builder methods reject malformed topology with typed errors.
	err := g.AddEdge(flow.VertexID{}, finishID)
	var buildErr *flow.BuildError
	fmt.Println("typed build error:", errors.As(err, &buildErr))

	err = flow.AddVertex(
		g,
		doubleID,
		flow.NewFuncTask(func(_ context.Context, value int) (int, error) {
			return value * 2, nil
		}),
		func(s state) int { return s.Value },
		func(s *state, value int) error {
			s.Value = value
			s.Trace = append(s.Trace, "doubled")
			return nil
		},
	)
	if err != nil {
		panic(err)
	}
	err = flow.AddVertex(
		g,
		finishID,
		flow.NewFuncTask(func(_ context.Context, value int) (string, error) {
			return "finished", nil
		}),
		func(s state) int { return s.Value },
		func(s *state, event string) error {
			s.Trace = append(s.Trace, event)
			return nil
		},
	)
	if err != nil {
		panic(err)
	}
	if err := g.AddEdge(doubleID, finishID); err != nil {
		panic(err)
	}

	runner, err := g.Compile(doubleID, finishID)
	if err != nil {
		panic(err)
	}
	var lifecycle []string
	result, err := runner.Run(context.Background(), state{Value: 4}, flow.WithHooks(flow.Hooks{
		OnRunStart: func(_ context.Context, run flow.GraphRunState) {
			lifecycle = append(lifecycle, "start:"+run.Status.String())
		},
		OnRunFinish: func(_ context.Context, run flow.GraphRunState) {
			lifecycle = append(lifecycle, "finish:"+run.Status.String())
		},
	}))
	if err != nil {
		panic(err)
	}
	fmt.Printf("value=%d trace=%v\n", result.State.Value, result.State.Trace)
	fmt.Printf("status=%s lifecycle=%v\n", result.Run.Status, lifecycle)

	// Output:
	// typed build error: true
	// value=8 trace=[doubled finished]
	// status=Completed lifecycle=[start:Running finish:Completed]
}
