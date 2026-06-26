package flow

import "time"

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
