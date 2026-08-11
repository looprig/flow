package interruptexample_test

import (
	"context"
	"fmt"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
)

type approvalState struct {
	OrderID string
}

func Example_typedInterruption() {
	graphID := flow.GraphID(uuid.MustParse("30000000-0000-4000-8000-000000000001"))
	waitID := flow.VertexID(uuid.MustParse("30000000-0000-4000-8000-000000000002"))
	g := flow.NewGraph[approvalState](graphID)
	if err := flow.AddVertex(
		g,
		waitID,
		flow.NewFuncTask(func(ctx context.Context, orderID string) (bool, error) {
			return false, flow.Interrupt(ctx, "review "+orderID)
		}),
		func(s approvalState) string { return s.OrderID },
		func(_ *approvalState, _ bool) error { return nil },
	); err != nil {
		panic(err)
	}
	runner, err := g.Compile(waitID, waitID)
	if err != nil {
		panic(err)
	}

	var observed flow.Interruption
	result, err := runner.Run(
		context.Background(),
		approvalState{OrderID: "order-42"},
		flow.WithHooks(flow.Hooks{
			OnInterrupt: func(_ context.Context, interruption flow.Interruption) {
				observed = interruption
			},
		}),
	)
	if err != nil {
		panic(err)
	}
	interruption := result.Interrupts[0]
	fmt.Printf("status=%s awaiting=%t\n", result.Run.Status, interruption.Kind == flow.Awaiting)
	fmt.Printf("info=%v same-vertex=%t hook=%t\n",
		interruption.Info,
		interruption.Vertex == waitID,
		observed.GraphRunID == result.Run.GraphRunID && observed.Kind == flow.Awaiting,
	)

	// Output:
	// status=Interrupted awaiting=true
	// info=review order-42 same-vertex=true hook=true
}
