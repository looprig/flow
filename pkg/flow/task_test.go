package flow

import (
	"context"
	"errors"
	"testing"
)

// _ asserts at compile time that *FuncTask[I,O] satisfies the Task[I,O]
// interface — the substitutability contract for the first concrete Task kind
// (§5). Later kinds (SubgraphTask, AgentTask) must satisfy the same assertion.
var _ Task[int, string] = (*FuncTask[int, string])(nil)

// ctxKey is a private context key used to verify FuncTask threads the caller's
// ctx (not a fresh one) into the wrapped TaskFunc.
type ctxKey struct{}

var errTask = errors.New("task failed")

// TestFuncTaskExecute is the core forwarding contract: NewFuncTask(fn).Execute
// passes ctx and in to fn and returns exactly fn's (O, error) — value, error,
// and zero-value cases (§5). Instantiated concretely as Task[int, string].
func TestFuncTaskExecute(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		fn      TaskFunc[int, string]
		in      int
		want    string
		wantErr error
	}{
		{
			name:    "happy path returns value",
			fn:      func(_ context.Context, in int) (string, error) { return "v=" + itoa(in), nil },
			in:      7,
			want:    "v=7",
			wantErr: nil,
		},
		{
			name:    "error passthrough returns same error and zero O",
			fn:      func(_ context.Context, _ int) (string, error) { return "", errTask },
			in:      1,
			want:    "",
			wantErr: errTask,
		},
		{
			name:    "boundary: fn returns the zero O",
			fn:      func(_ context.Context, _ int) (string, error) { return "", nil },
			in:      0,
			want:    "",
			wantErr: nil,
		},
		{
			name:    "boundary: error alongside a non-zero O is forwarded verbatim",
			fn:      func(_ context.Context, _ int) (string, error) { return "partial", errTask },
			in:      -1,
			want:    "partial",
			wantErr: errTask,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			task := NewFuncTask(tt.fn)
			got, err := task.Execute(context.Background(), tt.in)
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("Execute() error = %v, want %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("Execute() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestFuncTaskThreadsContext verifies Execute forwards the SAME ctx (the fn can
// read a value set on the ctx the caller passed in), confirming ctx is threaded
// rather than dropped or replaced (§5/§2.4).
func TestFuncTaskThreadsContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		ctx  context.Context
		want string
	}{
		{
			name: "value set on caller ctx is visible to fn",
			ctx:  context.WithValue(context.Background(), ctxKey{}, "threaded"),
			want: "threaded",
		},
		{
			name: "absent value yields empty (fn observes the real ctx)",
			ctx:  context.Background(),
			want: "",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			task := NewFuncTask(func(ctx context.Context, _ int) (string, error) {
				v, _ := ctx.Value(ctxKey{}).(string)
				return v, nil
			})
			got, err := task.Execute(tt.ctx, 0)
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("Execute() read ctx value = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestFuncTaskReusable proves a Task carries no per-use state: ONE *FuncTask
// value invoked repeatedly with different inputs returns results consistent with
// fn each time, independent of call order (§5 reusability property).
func TestFuncTaskReusable(t *testing.T) {
	t.Parallel()

	// A single shared instance, reused across every subtest and input.
	task := NewFuncTask(func(_ context.Context, in int) (string, error) {
		return "v=" + itoa(in), nil
	})

	tests := []struct {
		name string
		in   int
		want string
	}{
		{name: "first input", in: 1, want: "v=1"},
		{name: "second input", in: 2, want: "v=2"},
		{name: "zero input", in: 0, want: "v=0"},
		{name: "negative input", in: -5, want: "v=-5"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := task.Execute(context.Background(), tt.in)
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("Execute(%d) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// itoa is a tiny stdlib-free int-to-string for test expectations (avoids pulling
// strconv just for fixtures and keeps the want values self-evident).
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
