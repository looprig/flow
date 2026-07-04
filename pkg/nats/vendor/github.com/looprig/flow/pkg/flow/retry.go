package flow

import (
	"context"
	"fmt"
	"time"
)

// This file defines the RetryPolicy CONFIG struct (design §12.2). It is pure
// configuration attached to a vertex binding via WithRetry; the retry EXECUTION
// wrapper (the bounded re-run loop, backoff sleeping, Retryable evaluation, and
// Attempt bumping) is a later phase and deliberately lives nowhere here.
//
// A policy attaches to the vertex (the binding), never to the reusable Task —
// the same Task plugged into two graphs may carry different retry policies (§6.3).

// RetryPolicy is the per-vertex retry configuration (§12.2). A zero RetryPolicy
// is meaningful only as the absence of retry (vertexConfig.retry stays nil when
// WithRetry is not applied); when set, MaxAttempts bounds the re-runs, Backoff
// computes the delay before attempt n, and Retryable decides whether a given
// error is worth retrying.
type RetryPolicy struct {
	// MaxAttempts bounds the total number of task executions (the first try plus
	// retries). Values <= 1 mean no retry.
	MaxAttempts int

	// Backoff returns the delay to wait before the given 1-based attempt. A nil
	// Backoff means no delay between attempts. The execution wrapper (later phase)
	// invokes it under panic recovery (§12.5).
	Backoff func(attempt int) time.Duration

	// Retryable reports whether err warrants a retry. A nil Retryable means any
	// non-interrupt error is retryable (§12.2). The execution wrapper (later
	// phase) invokes it under panic recovery (§12.5).
	Retryable func(err error) bool
}

// runWithRetry runs exec under policy, recovering panics. It returns the task
// output, the number of attempts made (1-based), and the final error (§12.2,
// §12.5):
//   - nil on success;
//   - the interrupt signal passed straight through UNWRAPPED, NEVER retried (a
//     pause is not a failure, and the coordinator's asInterrupt must still
//     match it);
//   - a *VertexError{...Attempt} on any task failure — a panic OR an exhausted
//     / non-retryable returned error — wrapping the underlying cause.
//
// On any non-nil error, the returned out is UNSPECIFIED: a failing or paused
// task's partial output is meaningless, so the coordinator drives reduce/route/
// pause off the error and reads out only when err == nil.
//
// A panic in the task, in policy.Retryable, or in policy.Backoff is itself a
// task failure: it is recovered (it never escapes) and surfaced as a
// *VertexError. The backoff wait honors ctx; a cancellation during the wait
// aborts the loop and returns the last failure as a *VertexError (not ctx.Err),
// since the coordinator drives error policy off the VertexError. runWithRetry is
// timeout-agnostic: it honors whatever ctx it is handed and applies neither
// Route nor Pause — the Phase-6 coordinator applies those to the returned error.
func runWithRetry(ctx context.Context, rinfo RunInfo, policy *RetryPolicy, exec func(context.Context) (any, error)) (any, int, error) {
	maxAttempts := 1
	if policy != nil && policy.MaxAttempts > 0 {
		maxAttempts = policy.MaxAttempts
	}
	// maxAttempts >= 1 (clamped above), so the attempt >= maxAttempts guard below
	// always terminates the loop; every other branch returns as well.
	for attempt := 1; ; attempt++ {
		out, cause := recoverExec(ctx, exec)
		if cause == nil {
			return out, attempt, nil // success
		}
		if _, ok := asInterrupt(cause); ok {
			return out, attempt, cause // pause: never retried, passed through unwrapped
		}
		if attempt >= maxAttempts {
			return out, attempt, vertexErr(rinfo, attempt, cause) // exhausted
		}
		retry, retryErr := retryable(policy, cause)
		if retryErr != nil {
			return out, attempt, vertexErr(rinfo, attempt, retryErr) // Retryable panicked
		}
		if !retry {
			return out, attempt, vertexErr(rinfo, attempt, cause) // declared non-retryable
		}
		delay, boErr := backoffDelay(policy, attempt+1)
		if boErr != nil {
			return out, attempt, vertexErr(rinfo, attempt, boErr) // Backoff panicked
		}
		if !waitBackoff(ctx, delay) {
			return out, attempt, vertexErr(rinfo, attempt, cause) // ctx cancelled mid-wait
		}
	}
}

// recoverExec runs exec under panic recovery and returns its output and the
// underlying failure cause, or (out, nil) on success. A panic is converted to a
// "panic: <value>" error so it becomes an ordinary task failure (§12.5) and can
// never escape into the coordinator's control flow.
func recoverExec(ctx context.Context, exec func(context.Context) (any, error)) (out any, cause error) {
	defer func() {
		if r := recover(); r != nil {
			out, cause = nil, fmt.Errorf("panic: %v", r)
		}
	}()
	out, cause = exec(ctx)
	return out, cause
}

// retryable decides whether cause warrants a retry. A nil policy or nil
// policy.Retryable means "retry any non-interrupt error"; cause is already
// non-interrupt here, so the default is retry. A user Retryable is called under
// panic recovery against the UNDERLYING cause (so errors.Is/As in the predicate
// work); a panic in it is itself a task failure (§12.5), returned as the second
// result so the caller surfaces it as a *VertexError.
func retryable(policy *RetryPolicy, cause error) (retry bool, panicErr error) {
	if policy == nil || policy.Retryable == nil {
		return true, nil
	}
	defer func() {
		if r := recover(); r != nil {
			retry, panicErr = false, fmt.Errorf("panic: %v", r)
		}
	}()
	return policy.Retryable(cause), nil
}

// backoffDelay computes the delay to wait before the given 1-based attempt. A
// nil policy or nil Backoff means no delay. Backoff is called under panic
// recovery; a panic in it is itself a task failure (§12.5), returned as the
// second result so the caller surfaces it as a *VertexError.
func backoffDelay(policy *RetryPolicy, nextAttempt int) (d time.Duration, panicErr error) {
	if policy == nil || policy.Backoff == nil {
		return 0, nil
	}
	defer func() {
		if r := recover(); r != nil {
			d, panicErr = 0, fmt.Errorf("panic: %v", r)
		}
	}()
	return policy.Backoff(nextAttempt), nil
}

// waitBackoff waits d honoring ctx. A zero or negative delay returns true
// immediately without blocking. It reports true if the wait elapsed (retry may
// proceed) and false if ctx was cancelled during the wait (abort the loop).
func waitBackoff(ctx context.Context, d time.Duration) bool {
	if d <= 0 {
		return ctx.Err() == nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// vertexErr wraps cause in a *VertexError stamped with rinfo's identity and the
// 1-based attempt at which the failure became final (§12.4).
func vertexErr(rinfo RunInfo, attempt int, cause error) *VertexError {
	return &VertexError{
		VertexID:    rinfo.VertexID,
		VertexRunID: rinfo.VertexRunID,
		Attempt:     attempt,
		Err:         cause,
	}
}
