package flow

import (
	"context"
	"errors"
	"testing"
	"time"
)

// These tests pin the contract of the retry + panic-recovery execution wrapper
// (design §12.2 + §12.5): runWithRetry runs a vertex task under a bounded retry
// loop, recovers panics from the task / Retryable / Backoff so none ever escape,
// passes an interrupt straight through UNWRAPPED (never retried), and surfaces
// every other failure as a *VertexError carrying the 1-based attempt and the
// underlying cause. runWithRetry is unexported, so these are white-box tests in
// package flow. Backoff returns zero in the timing-agnostic tests to keep them
// fast; only the ctx-cancellation tests exercise the wait, deterministically.

// testRunInfo is a small, deterministic RunInfo whose identity fields the
// wrapper must copy verbatim into any *VertexError it produces.
func testRunInfo() RunInfo {
	return RunInfo{VertexID: VertexID{9}, VertexRunID: VertexRunID{4}}
}

// fakeExec builds an exec closure that returns the programmed (out, err) for
// each successive call and records how many times it was invoked. results is
// consumed in order; calls past its end repeat the last entry.
type execResult struct {
	out any
	err error
}

func fakeExec(calls *int, results ...execResult) func(context.Context) (any, error) {
	return func(context.Context) (any, error) {
		i := *calls
		*calls++
		if i >= len(results) {
			i = len(results) - 1
		}
		return results[i].out, results[i].err
	}
}

// zeroBackoff is a Backoff that never blocks; recordBackoff also captures the
// attempt numbers it was asked for so a test can assert it was consulted for
// the right attempts.
func zeroBackoff(int) time.Duration { return 0 }

// TestRunWithRetrySuccessFirstTry verifies a task that succeeds immediately
// returns its output at attempt 1 with no error and is invoked exactly once.
func TestRunWithRetrySuccessFirstTry(t *testing.T) {
	t.Parallel()

	var calls int
	exec := fakeExec(&calls, execResult{out: "ok", err: nil})
	policy := &RetryPolicy{MaxAttempts: 5, Backoff: zeroBackoff}

	out, attempts, err := runWithRetry(context.Background(), testRunInfo(), policy, exec)
	if err != nil {
		t.Fatalf("runWithRetry() err = %v, want nil", err)
	}
	if out != "ok" {
		t.Errorf("out = %v, want %q", out, "ok")
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1", attempts)
	}
	if calls != 1 {
		t.Errorf("exec calls = %d, want 1", calls)
	}
}

// TestRunWithRetryRetryThenSucceed verifies a task that fails K times then
// succeeds returns the success at attempt K+1, reports the right attempt count,
// and consults Backoff once per failed attempt for the NEXT attempt number
// (Backoff returns "the delay before attempt n", so before attempts 2 and 3 it
// is asked for 2 and 3).
func TestRunWithRetryRetryThenSucceed(t *testing.T) {
	t.Parallel()

	const k = 2
	var calls int
	boom := errors.New("transient")
	exec := fakeExec(&calls,
		execResult{err: boom},
		execResult{err: boom},
		execResult{out: "done"},
	)
	var backoffAttempts []int
	policy := &RetryPolicy{
		MaxAttempts: 5,
		Backoff: func(attempt int) time.Duration {
			backoffAttempts = append(backoffAttempts, attempt)
			return 0
		},
	}

	out, attempts, err := runWithRetry(context.Background(), testRunInfo(), policy, exec)
	if err != nil {
		t.Fatalf("runWithRetry() err = %v, want nil", err)
	}
	if out != "done" {
		t.Errorf("out = %v, want %q", out, "done")
	}
	if attempts != k+1 {
		t.Errorf("attempts = %d, want %d", attempts, k+1)
	}
	if calls != k+1 {
		t.Errorf("exec calls = %d, want %d", calls, k+1)
	}
	want := []int{2, 3}
	if len(backoffAttempts) != len(want) {
		t.Fatalf("backoff attempts = %v, want %v", backoffAttempts, want)
	}
	for i := range want {
		if backoffAttempts[i] != want[i] {
			t.Errorf("backoff attempt[%d] = %d, want %d", i, backoffAttempts[i], want[i])
		}
	}
}

// TestRunWithRetryExhaustion verifies a task that always fails returns a
// *VertexError after MaxAttempts tries, carrying the right Attempt and identity
// and wrapping the underlying cause reachable via errors.As / Unwrap.
func TestRunWithRetryExhaustion(t *testing.T) {
	t.Parallel()

	var calls int
	cause := errors.New("always fails")
	exec := fakeExec(&calls, execResult{err: cause})
	policy := &RetryPolicy{MaxAttempts: 3, Backoff: zeroBackoff}
	rinfo := testRunInfo()

	out, attempts, err := runWithRetry(context.Background(), rinfo, policy, exec)
	if out != nil {
		t.Errorf("out = %v, want nil", out)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
	if calls != 3 {
		t.Errorf("exec calls = %d, want 3", calls)
	}
	var ve *VertexError
	if !errors.As(err, &ve) {
		t.Fatalf("err = %v, want *VertexError", err)
	}
	if ve.Attempt != 3 {
		t.Errorf("VertexError.Attempt = %d, want 3", ve.Attempt)
	}
	if ve.VertexID != rinfo.VertexID || ve.VertexRunID != rinfo.VertexRunID {
		t.Errorf("VertexError identity = (%v,%v), want (%v,%v)", ve.VertexID, ve.VertexRunID, rinfo.VertexID, rinfo.VertexRunID)
	}
	if !errors.Is(err, cause) {
		t.Errorf("errors.Is(err, cause) = false, want true (Unwrap should reach cause)")
	}
	if ve.Err != cause {
		t.Errorf("VertexError.Err = %v, want %v", ve.Err, cause)
	}
}

// TestRunWithRetryNoPolicy verifies a nil policy defaults to a single attempt:
// the first failure is surfaced immediately as a *VertexError{Attempt:1} with
// exactly one exec call.
func TestRunWithRetryNoPolicy(t *testing.T) {
	t.Parallel()

	var calls int
	cause := errors.New("one-shot failure")
	exec := fakeExec(&calls, execResult{err: cause})

	out, attempts, err := runWithRetry(context.Background(), testRunInfo(), nil, exec)
	if out != nil {
		t.Errorf("out = %v, want nil", out)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1", attempts)
	}
	if calls != 1 {
		t.Errorf("exec calls = %d, want 1", calls)
	}
	var ve *VertexError
	if !errors.As(err, &ve) {
		t.Fatalf("err = %v, want *VertexError", err)
	}
	if ve.Attempt != 1 {
		t.Errorf("VertexError.Attempt = %d, want 1", ve.Attempt)
	}
	if !errors.Is(err, cause) {
		t.Errorf("errors.Is(err, cause) = false, want true")
	}
}

// TestRunWithRetryInterruptNeverRetried verifies an interrupt signal returned
// by the task is passed straight through UNWRAPPED (so the coordinator's
// asInterrupt still matches), at attempt 1, with exactly one exec call, even
// when the policy would otherwise allow many retries.
func TestRunWithRetryInterruptNeverRetried(t *testing.T) {
	t.Parallel()

	var calls int
	sig := Interrupt(context.Background(), "pause-here")
	exec := fakeExec(&calls, execResult{out: "partial", err: sig})
	policy := &RetryPolicy{MaxAttempts: 5, Backoff: zeroBackoff}

	out, attempts, err := runWithRetry(context.Background(), testRunInfo(), policy, exec)
	if err != sig {
		t.Fatalf("err = %v, want the raw interrupt signal (unwrapped)", err)
	}
	if _, ok := asInterrupt(err); !ok {
		t.Errorf("asInterrupt(err) = false, want true (must be passed through unwrapped)")
	}
	var ve *VertexError
	if errors.As(err, &ve) {
		t.Errorf("err must NOT be a *VertexError, got %v", ve)
	}
	if out != "partial" {
		t.Errorf("out = %v, want %q", out, "partial")
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1", attempts)
	}
	if calls != 1 {
		t.Errorf("exec calls = %d, want 1", calls)
	}
}

// TestRunWithRetryTaskPanicRetryableByDefault verifies a panicking task is
// recovered into a failure (never escapes) whose cause mentions "panic", that
// such a failure is retryable by default, and that exhaustion surfaces a
// *VertexError whose Err mentions "panic".
func TestRunWithRetryTaskPanicRetryableByDefault(t *testing.T) {
	t.Parallel()

	t.Run("panic then succeed", func(t *testing.T) {
		t.Parallel()
		var calls int
		exec := func(context.Context) (any, error) {
			i := calls
			calls++
			if i == 0 {
				panic("kaboom")
			}
			return "recovered", nil
		}
		policy := &RetryPolicy{MaxAttempts: 2, Backoff: zeroBackoff}

		out, attempts, err := runWithRetry(context.Background(), testRunInfo(), policy, exec)
		if err != nil {
			t.Fatalf("runWithRetry() err = %v, want nil (panic should be retried)", err)
		}
		if out != "recovered" {
			t.Errorf("out = %v, want %q", out, "recovered")
		}
		if attempts != 2 {
			t.Errorf("attempts = %d, want 2", attempts)
		}
		if calls != 2 {
			t.Errorf("exec calls = %d, want 2", calls)
		}
	})

	t.Run("always panic exhausts", func(t *testing.T) {
		t.Parallel()
		var calls int
		exec := func(context.Context) (any, error) {
			calls++
			panic("always")
		}
		policy := &RetryPolicy{MaxAttempts: 2, Backoff: zeroBackoff}

		_, attempts, err := runWithRetry(context.Background(), testRunInfo(), policy, exec)
		var ve *VertexError
		if !errors.As(err, &ve) {
			t.Fatalf("err = %v, want *VertexError", err)
		}
		if ve.Attempt != 2 {
			t.Errorf("VertexError.Attempt = %d, want 2", ve.Attempt)
		}
		if attempts != 2 {
			t.Errorf("attempts = %d, want 2", attempts)
		}
		if calls != 2 {
			t.Errorf("exec calls = %d, want 2", calls)
		}
		if !containsPanic(ve.Err) {
			t.Errorf("VertexError.Err = %v, want a cause mentioning %q", ve.Err, "panic")
		}
	})
}

// TestRunWithRetryRetryablePanicStops verifies a panic inside Retryable is
// itself a task failure: it is recovered (never escapes), stops the loop, and
// returns a *VertexError whose Err mentions "panic".
func TestRunWithRetryRetryablePanicStops(t *testing.T) {
	t.Parallel()

	var calls int
	exec := fakeExec(&calls, execResult{err: errors.New("boom")})
	policy := &RetryPolicy{
		MaxAttempts: 5,
		Backoff:     zeroBackoff,
		Retryable:   func(error) bool { panic("retryable kaboom") },
	}

	_, attempts, err := runWithRetry(context.Background(), testRunInfo(), policy, exec)
	var ve *VertexError
	if !errors.As(err, &ve) {
		t.Fatalf("err = %v, want *VertexError", err)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (must stop, no further attempts)", attempts)
	}
	if calls != 1 {
		t.Errorf("exec calls = %d, want 1", calls)
	}
	if !containsPanic(ve.Err) {
		t.Errorf("VertexError.Err = %v, want a cause mentioning %q", ve.Err, "panic")
	}
}

// TestRunWithRetryBackoffPanicStops verifies a panic inside Backoff is itself a
// task failure: it is recovered (never escapes), stops the loop, and returns a
// *VertexError whose Err mentions "panic".
func TestRunWithRetryBackoffPanicStops(t *testing.T) {
	t.Parallel()

	var calls int
	exec := fakeExec(&calls, execResult{err: errors.New("boom")})
	policy := &RetryPolicy{
		MaxAttempts: 5,
		Backoff:     func(int) time.Duration { panic("backoff kaboom") },
	}

	_, attempts, err := runWithRetry(context.Background(), testRunInfo(), policy, exec)
	var ve *VertexError
	if !errors.As(err, &ve) {
		t.Fatalf("err = %v, want *VertexError", err)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1 (must stop, no further attempts)", attempts)
	}
	if calls != 1 {
		t.Errorf("exec calls = %d, want 1", calls)
	}
	if !containsPanic(ve.Err) {
		t.Errorf("VertexError.Err = %v, want a cause mentioning %q", ve.Err, "panic")
	}
}

// TestRunWithRetryRetryableFalseStops verifies a Retryable predicate returning
// false stops the loop after the first attempt and surfaces a *VertexError. The
// predicate is also handed the UNDERLYING cause so errors.Is/As works inside it.
func TestRunWithRetryRetryableFalseStops(t *testing.T) {
	t.Parallel()

	var calls int
	cause := errors.New("fatal")
	exec := fakeExec(&calls, execResult{err: cause})
	var sawCause error
	policy := &RetryPolicy{
		MaxAttempts: 5,
		Backoff:     zeroBackoff,
		Retryable: func(err error) bool {
			sawCause = err
			return false
		},
	}

	_, attempts, err := runWithRetry(context.Background(), testRunInfo(), policy, exec)
	var ve *VertexError
	if !errors.As(err, &ve) {
		t.Fatalf("err = %v, want *VertexError", err)
	}
	if attempts != 1 {
		t.Errorf("attempts = %d, want 1", attempts)
	}
	if calls != 1 {
		t.Errorf("exec calls = %d, want 1", calls)
	}
	if sawCause != cause {
		t.Errorf("Retryable saw %v, want the underlying cause %v", sawCause, cause)
	}
	if ve.Err != cause {
		t.Errorf("VertexError.Err = %v, want %v", ve.Err, cause)
	}
}

// TestRunWithRetryCtxCancelAborts verifies a cancelled ctx during the backoff
// wait aborts the loop early (does not spin to MaxAttempts) and returns the
// *VertexError wrapping the last cause rather than ctx.Err.
func TestRunWithRetryCtxCancelAborts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		preCancel   bool // cancel before the call
		cancelInBO  bool // cancel inside Backoff, then return a real delay
		wantAttempt int
	}{
		{name: "already cancelled aborts after first failure", preCancel: true, wantAttempt: 1},
		{name: "cancel during backoff aborts the wait", cancelInBO: true, wantAttempt: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tt.preCancel {
				cancel()
			}

			var calls int
			cause := errors.New("keeps failing")
			exec := fakeExec(&calls, execResult{err: cause})
			policy := &RetryPolicy{
				MaxAttempts: 10,
				Backoff: func(int) time.Duration {
					if tt.cancelInBO {
						cancel()
					}
					return 50 * time.Millisecond
				},
			}

			_, attempts, err := runWithRetry(ctx, testRunInfo(), policy, exec)
			var ve *VertexError
			if !errors.As(err, &ve) {
				t.Fatalf("err = %v, want *VertexError", err)
			}
			if ve.Err != cause {
				t.Errorf("VertexError.Err = %v, want last cause %v", ve.Err, cause)
			}
			if attempts != tt.wantAttempt {
				t.Errorf("attempts = %d, want %d (must abort, not spin to MaxAttempts)", attempts, tt.wantAttempt)
			}
			if calls != tt.wantAttempt {
				t.Errorf("exec calls = %d, want %d", calls, tt.wantAttempt)
			}
		})
	}
}

// TestRunWithRetryZeroDelayNoBlock verifies a zero/negative Backoff delay does
// not block: an always-failing task with a zero delay still runs every attempt
// quickly and exhausts to a *VertexError.
func TestRunWithRetryZeroDelayNoBlock(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		delay time.Duration
	}{
		{name: "zero delay", delay: 0},
		{name: "negative delay", delay: -time.Second},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			var calls int
			cause := errors.New("fail")
			exec := fakeExec(&calls, execResult{err: cause})
			policy := &RetryPolicy{
				MaxAttempts: 3,
				Backoff:     func(int) time.Duration { return tt.delay },
			}

			done := make(chan struct{})
			go func() {
				_, _, _ = runWithRetry(context.Background(), testRunInfo(), policy, exec)
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				t.Fatal("runWithRetry blocked on a zero/negative delay")
			}
			if calls != 3 {
				t.Errorf("exec calls = %d, want 3", calls)
			}
		})
	}
}

// TestRunWithRetryNilBackoffRetries verifies a policy that allows retries but
// has a nil Backoff still retries with no delay between attempts, exhausting to
// a *VertexError after MaxAttempts.
func TestRunWithRetryNilBackoffRetries(t *testing.T) {
	t.Parallel()

	var calls int
	cause := errors.New("fail")
	exec := fakeExec(&calls, execResult{err: cause})
	policy := &RetryPolicy{MaxAttempts: 3} // Backoff nil, Retryable nil

	_, attempts, err := runWithRetry(context.Background(), testRunInfo(), policy, exec)
	var ve *VertexError
	if !errors.As(err, &ve) {
		t.Fatalf("err = %v, want *VertexError", err)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
	if calls != 3 {
		t.Errorf("exec calls = %d, want 3", calls)
	}
}

// TestRunWithRetryPositiveBackoffElapses verifies a real positive backoff delay
// actually elapses (the wait completes rather than being cancelled) and the
// retry then proceeds to success. The delay is tiny to keep the test fast.
func TestRunWithRetryPositiveBackoffElapses(t *testing.T) {
	t.Parallel()

	var calls int
	exec := fakeExec(&calls,
		execResult{err: errors.New("transient")},
		execResult{out: "ok"},
	)
	policy := &RetryPolicy{
		MaxAttempts: 3,
		Backoff:     func(int) time.Duration { return time.Millisecond },
	}

	out, attempts, err := runWithRetry(context.Background(), testRunInfo(), policy, exec)
	if err != nil {
		t.Fatalf("runWithRetry() err = %v, want nil", err)
	}
	if out != "ok" {
		t.Errorf("out = %v, want %q", out, "ok")
	}
	if attempts != 2 {
		t.Errorf("attempts = %d, want 2", attempts)
	}
	if calls != 2 {
		t.Errorf("exec calls = %d, want 2", calls)
	}
}

// containsPanic reports whether err's message mentions "panic:", used to assert
// that a recovered panic was wrapped as the underlying cause.
func containsPanic(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	const want = "panic:"
	for i := 0; i+len(want) <= len(msg); i++ {
		if msg[i:i+len(want)] == want {
			return true
		}
	}
	return false
}
