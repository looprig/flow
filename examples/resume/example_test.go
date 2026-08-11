package resumeexample_test

import (
	"context"
	"errors"
	"fmt"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
)

type reviewState struct {
	Change     string
	ApprovedBy string
	Attempts   int
}

type decision struct {
	Approver string
	Attempt  int
}

func Example_checkpointResume() {
	ctx := context.Background()
	graphID := flow.GraphID(uuid.MustParse("40000000-0000-4000-8000-000000000001"))
	reviewID := flow.VertexID(uuid.MustParse("40000000-0000-4000-8000-000000000002"))
	store := flow.NewMemStore()
	g := flow.NewGraph[reviewState](graphID)
	if err := flow.AddVertex(
		g,
		reviewID,
		flow.NewFuncTask(func(ctx context.Context, change string) (decision, error) {
			attempt, resuming := flow.InterruptState[int](ctx)
			if !resuming {
				return decision{}, flow.StatefulInterrupt(ctx, "approve "+change, 1)
			}
			approver, ok := flow.ResumePayload[string](ctx)
			if !ok {
				return decision{}, errors.New("resume payload must name an approver")
			}
			return decision{Approver: approver, Attempt: attempt + 1}, nil
		}),
		func(s reviewState) string { return s.Change },
		func(s *reviewState, approved decision) error {
			s.ApprovedBy = approved.Approver
			s.Attempts = approved.Attempt
			return nil
		},
	); err != nil {
		panic(err)
	}
	runner, err := g.Compile(reviewID, reviewID, flow.WithStore(store))
	if err != nil {
		panic(err)
	}

	paused, err := runner.Run(ctx, reviewState{Change: "change-7"})
	if err != nil {
		panic(err)
	}
	checkpoint, err := store.Latest(ctx, paused.Run.GraphRunID)
	if err != nil {
		panic(err)
	}
	before, err := store.History(ctx, paused.Run.GraphRunID)
	if err != nil {
		panic(err)
	}
	fmt.Printf("checkpoint paused=%t persisted-attempts=%d\n",
		checkpoint.Phase == flow.StepPaused && checkpoint.Run.Status == flow.RunInterrupted,
		paused.State.Attempts,
	)

	completed, err := runner.Resume(ctx, paused.Run.GraphRunID, "alice")
	if err != nil {
		panic(err)
	}
	after, err := store.History(ctx, paused.Run.GraphRunID)
	if err != nil {
		panic(err)
	}
	fmt.Printf("resume=%s approver=%s attempts=%d history-grew=%t\n",
		completed.Run.Status,
		completed.State.ApprovedBy,
		completed.State.Attempts,
		len(after) > len(before),
	)

	_, err = runner.Resume(ctx, paused.Run.GraphRunID, "bob")
	var terminal *flow.ResumeTerminalError
	fmt.Println("terminal resume error:", errors.As(err, &terminal))

	// Output:
	// checkpoint paused=true persisted-attempts=0
	// resume=Completed approver=alice attempts=2 history-grew=true
	// terminal resume error: true
}
