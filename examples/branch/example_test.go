package branchexample_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
)

type routeState struct {
	Score int
	High  bool
	Trace []string
}

func Example_conditionalRouting() {
	graphID := flow.GraphID(uuid.MustParse("20000000-0000-4000-8000-000000000001"))
	classifyID := flow.VertexID(uuid.MustParse("20000000-0000-4000-8000-000000000002"))
	lowID := flow.VertexID(uuid.MustParse("20000000-0000-4000-8000-000000000003"))
	highID := flow.VertexID(uuid.MustParse("20000000-0000-4000-8000-000000000004"))
	finishID := flow.VertexID(uuid.MustParse("20000000-0000-4000-8000-000000000005"))
	g := flow.NewGraph[routeState](graphID)

	if err := flow.AddVertex(
		g,
		classifyID,
		flow.NewFuncTask(func(_ context.Context, score int) (bool, error) {
			return score >= 5, nil
		}),
		func(s routeState) int { return s.Score },
		func(s *routeState, high bool) error {
			s.High = high
			s.Trace = append(s.Trace, "classify")
			return nil
		},
	); err != nil {
		panic(err)
	}
	for _, step := range []struct {
		id    flow.VertexID
		label string
	}{
		{id: lowID, label: "low"},
		{id: highID, label: "high"},
		{id: finishID, label: "finish"},
	} {
		label := step.label
		if err := flow.AddVertex(
			g,
			step.id,
			flow.NewFuncTask(func(_ context.Context, _ int) (string, error) {
				return label, nil
			}),
			func(s routeState) int { return s.Score },
			func(s *routeState, label string) error {
				s.Trace = append(s.Trace, label)
				return nil
			},
		); err != nil {
			panic(err)
		}
	}
	if err := g.AddConditionalEdge(classifyID, flow.Condition[routeState]{
		Targets: []flow.VertexID{lowID, highID},
		Pick: func(_ context.Context, s routeState) ([]flow.VertexID, error) {
			if s.Score < 0 {
				return nil, errors.New("score must be non-negative")
			}
			if s.High {
				return []flow.VertexID{highID}, nil
			}
			return []flow.VertexID{lowID}, nil
		},
	}); err != nil {
		panic(err)
	}
	if err := g.AddEdge(lowID, finishID); err != nil {
		panic(err)
	}
	if err := g.AddEdge(highID, finishID); err != nil {
		panic(err)
	}
	runner, err := g.Compile(classifyID, finishID)
	if err != nil {
		panic(err)
	}

	low, err := runner.Run(context.Background(), routeState{Score: 2})
	if err != nil {
		panic(err)
	}
	high, err := runner.Run(context.Background(), routeState{Score: 8})
	if err != nil {
		panic(err)
	}
	invalid, err := runner.Run(context.Background(), routeState{Score: -1})
	if err != nil {
		panic(err)
	}
	var conditionErr *flow.ConditionError
	fmt.Println("low:", low.State.Trace)
	fmt.Println("high:", high.State.Trace)
	fmt.Printf("condition halt: %t typed error: %t\n",
		invalid.Halt != nil && invalid.Halt.Kind == flow.HaltCondition,
		invalid.Halt != nil && errors.As(invalid.Halt.Cause, &conditionErr),
	)

	// Output:
	// low: [classify low finish]
	// high: [classify high finish]
	// condition halt: true typed error: true
}
