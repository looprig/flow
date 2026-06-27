package main

import (
	"context"
	"testing"
	"time"
)

// This is the Tier-A (embed) smoke test (§18.6): it proves the factored run(ctx)
// drives the in-process embed example — build → Compile(WithStore) → Run →
// Resume — to completion with no error and, critically, WITHOUT hanging. main()
// is a thin shell over run(ctx), so exercising run here covers the whole binary's
// behavior under -race.
func TestRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		ctx     func() (context.Context, context.CancelFunc)
		wantErr bool
	}{
		{
			name: "embed example runs to completion",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithTimeout(context.Background(), 10*time.Second)
			},
			wantErr: false,
		},
		{
			name: "cancelled context fails fast without hanging",
			ctx: func() (context.Context, context.CancelFunc) {
				ctx, cancel := context.WithCancel(context.Background())
				cancel() // already done before run sees it
				return ctx, func() {}
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := tt.ctx()
			defer cancel()

			// A watchdog so a hung run fails the test instead of stalling the suite.
			done := make(chan error, 1)
			go func() { done <- run(ctx) }()

			select {
			case err := <-done:
				if (err != nil) != tt.wantErr {
					t.Fatalf("run() error = %v, wantErr %v", err, tt.wantErr)
				}
			case <-time.After(15 * time.Second):
				t.Fatal("run() did not return in time (possible hang)")
			}
		})
	}
}
