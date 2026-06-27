package controlplane_test

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/ciram-co/flow/pkg/controlplane"
	"github.com/ciram-co/flow/pkg/flow"
)

// This file black-box tests the in-process ControlPlane (§18.5): Submit→Consume
// delivery, version routing (a consumer only receives Work for keys it serves),
// single-flight per run (never two Works for the same GraphRunID in flight at
// once), Nack requeue, concurrent in-flight for DIFFERENT runs, and clean
// ctx-cancel shutdown (the Consume channel closes, no goroutine leak). The
// concurrency rows are deterministic — they assert "delivered within a generous
// window" and "NOT delivered within a short window", never sleeps-as-sync.
//
// recvTimeout is generous: a delivery that is going to happen happens fast in a
// pure in-memory channel, so a real delivery never approaches this bound.
const recvTimeout = 2 * time.Second

// notRecvWindow is short: it bounds how long we wait to *prove a negative* (a
// Work that must be held back). It is long enough that a buggy implementation
// that delivers eagerly would lose the race, but short enough to keep the suite
// fast.
const notRecvWindow = 150 * time.Millisecond

// gvk builds a GraphVersionKey from a single byte (GraphID) and a version string,
// so tests need not depend on uuid generation.
func gvk(b byte, version string) flow.GraphVersionKey {
	var id flow.GraphID
	id[0] = b
	return flow.GraphVersionKey{GraphID: id, GraphVersion: version}
}

// runID builds a deterministic non-zero GraphRunID from a single byte so distinct
// runs are distinguishable without minting UUIDs.
func runID(b byte) flow.GraphRunID {
	var id flow.GraphRunID
	id[0] = b
	return id
}

// work builds a Work for key with the given run id and OpRun, carrying input as
// raw JSON so equality/identity is checkable.
func work(key flow.GraphVersionKey, run flow.GraphRunID, input string) flow.Work {
	return flow.Work{
		Key:        key,
		GraphRunID: run,
		Op:         flow.OpRun,
		Input:      json.RawMessage(input),
	}
}

// recv waits up to recvTimeout for one Delivery on ch. It fails the test if the
// channel is closed or nothing arrives — used where a delivery is REQUIRED.
func recv(t *testing.T, ch <-chan flow.Delivery) flow.Delivery {
	t.Helper()
	select {
	case d, ok := <-ch:
		if !ok {
			t.Fatalf("Consume channel closed unexpectedly while awaiting a delivery")
		}
		return d
	case <-time.After(recvTimeout):
		t.Fatalf("timed out after %s awaiting a required delivery", recvTimeout)
		return flow.Delivery{} // unreachable
	}
}

// assertNoRecv asserts that NO Delivery arrives on ch within notRecvWindow —
// used to prove a Work is correctly held back (single-flight) or routed away.
func assertNoRecv(t *testing.T, ch <-chan flow.Delivery, why string) {
	t.Helper()
	select {
	case d, ok := <-ch:
		if !ok {
			return // a closed channel is not a spurious delivery
		}
		t.Fatalf("unexpected delivery (%s): run=%s key=%v", why, d.Work.GraphRunID, d.Work.Key)
	case <-time.After(notRecvWindow):
		// success: nothing arrived in the window.
	}
}

// TestSubmitConsumeDelivery proves the happy path: a Work submitted for key K is
// delivered to a consumer serving [K], carries the submitted fields intact, and
// Ack drops it cleanly.
func TestSubmitConsumeDelivery(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cp := controlplane.Mem()
	defer cp.Close()
	k := gvk(1, "v1")
	ch, err := cp.Consume(ctx, []flow.GraphVersionKey{k})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	w := work(k, runID(1), `{"n":1}`)
	if err := cp.Submit(ctx, w); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	d := recv(t, ch)
	if d.Work.GraphRunID != w.GraphRunID {
		t.Errorf("GraphRunID = %s, want %s", d.Work.GraphRunID, w.GraphRunID)
	}
	if d.Work.Key != w.Key {
		t.Errorf("Key = %v, want %v", d.Work.Key, w.Key)
	}
	if string(d.Work.Input) != string(w.Input) {
		t.Errorf("Input = %s, want %s", d.Work.Input, w.Input)
	}
	if d.Ack == nil || d.Nack == nil {
		t.Fatalf("Delivery must carry non-nil Ack and Nack")
	}
	if err := d.Ack(); err != nil {
		t.Errorf("Ack: %v", err)
	}
}

// TestVersionRouting proves a Work is delivered only to consumers whose serves
// includes its Key: a consumer serving [v2] never sees v1 work, while a consumer
// serving [v1] does. Each subtest stands up its own control plane.
func TestVersionRouting(t *testing.T) {
	t.Parallel()

	v1, v2 := gvk(1, "v1"), gvk(1, "v2")

	tests := []struct {
		name      string
		serves    []flow.GraphVersionKey
		submitKey flow.GraphVersionKey
		wantRecv  bool
	}{
		{name: "match same version", serves: []flow.GraphVersionKey{v1}, submitKey: v1, wantRecv: true},
		{name: "no match different version", serves: []flow.GraphVersionKey{v1}, submitKey: v2, wantRecv: false},
		{name: "match among multiple served", serves: []flow.GraphVersionKey{v1, v2}, submitKey: v2, wantRecv: true},
		{name: "no match empty serves", serves: nil, submitKey: v1, wantRecv: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			cp := controlplane.Mem()
			defer cp.Close()
			ch, err := cp.Consume(ctx, tt.serves)
			if err != nil {
				t.Fatalf("Consume: %v", err)
			}
			if err := cp.Submit(ctx, work(tt.submitKey, runID(1), `{}`)); err != nil {
				t.Fatalf("Submit: %v", err)
			}

			if tt.wantRecv {
				d := recv(t, ch)
				if d.Work.Key != tt.submitKey {
					t.Errorf("delivered key = %v, want %v", d.Work.Key, tt.submitKey)
				}
				_ = d.Ack()
			} else {
				assertNoRecv(t, ch, "work routed to a consumer that does not serve its key")
			}
		})
	}
}

// TestSingleFlightPerRun proves the §18.5 guarantee: two Works for the SAME
// GraphRunID are never in flight at once. The second is held back until the
// first's slot is released, then delivered. The test runs both the Ack-release
// and Nack-release paths.
func TestSingleFlightPerRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		release func(d flow.Delivery) error
	}{
		{name: "Ack releases slot", release: func(d flow.Delivery) error { return d.Ack() }},
		{name: "Nack releases slot", release: func(d flow.Delivery) error { return d.Nack() }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			cp := controlplane.Mem()
			defer cp.Close()
			k := gvk(1, "v1")
			run := runID(9)
			ch, err := cp.Consume(ctx, []flow.GraphVersionKey{k})
			if err != nil {
				t.Fatalf("Consume: %v", err)
			}

			// Two works, SAME run id, distinct payloads so we can tell them apart.
			w1 := work(k, run, `{"seq":1}`)
			w2 := work(k, run, `{"seq":2}`)
			if err := cp.Submit(ctx, w1); err != nil {
				t.Fatalf("Submit w1: %v", err)
			}
			if err := cp.Submit(ctx, w2); err != nil {
				t.Fatalf("Submit w2: %v", err)
			}

			// First work is delivered.
			d1 := recv(t, ch)

			// Second work for the SAME run must be held back while d1 is in flight.
			assertNoRecv(t, ch, "second Work for an in-flight run delivered before slot released")

			// Releasing the slot (Ack or Nack of the FIRST) frees the run.
			if err := tt.release(d1); err != nil {
				t.Fatalf("release first: %v", err)
			}

			// Now a delivery for this run is allowed again. Depending on
			// release path, the redelivered/held work arrives; drain until we see
			// the held second work (Nack may redeliver the first too).
			d2 := recv(t, ch)
			if d2.Work.GraphRunID != run {
				t.Errorf("second delivery run = %s, want %s", d2.Work.GraphRunID, run)
			}
			_ = d2.Ack()
		})
	}
}

// TestNackRequeues proves a Nack'd Work is redelivered (and the run's slot is
// released so the redelivery can happen). A single consumer Nacks the first
// delivery and must then receive the SAME work again.
func TestNackRequeues(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cp := controlplane.Mem()
	defer cp.Close()
	k := gvk(2, "v1")
	run := runID(3)
	ch, err := cp.Consume(ctx, []flow.GraphVersionKey{k})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	w := work(k, run, `{"attempt":1}`)
	if err := cp.Submit(ctx, w); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	d1 := recv(t, ch)
	if err := d1.Nack(); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	d2 := recv(t, ch)
	if d2.Work.GraphRunID != run || string(d2.Work.Input) != string(w.Input) {
		t.Errorf("redelivered work = (run %s, input %s), want (run %s, input %s)",
			d2.Work.GraphRunID, d2.Work.Input, run, w.Input)
	}
	if err := d2.Ack(); err != nil {
		t.Errorf("Ack redelivery: %v", err)
	}
}

// TestNackBackoffBoundsRate is the H1b defense-in-depth regression: a repeatedly-
// Nack'd Work must be requeued AFTER a small backoff delay, not instantly. Without
// a delay a transiently-failing dependency turns redelivery into a 100%-CPU hot
// loop (the Work is re-delivered as fast as the worker can Nack it). The test Nacks
// the same Work several times and asserts the total elapsed time reflects a per-
// redelivery delay (a bounded rate), proving the requeue is not instantaneous.
func TestNackBackoffBoundsRate(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// An explicit (and larger) backoff makes the rate-bound assertion deterministic
	// and also exercises the WithNackBackoff option seam.
	const backoff = 5 * time.Millisecond
	cp := controlplane.Mem(controlplane.WithNackBackoff(backoff))
	defer cp.Close()
	k := gvk(7, "v1")
	run := runID(7)
	ch, err := cp.Consume(ctx, []flow.GraphVersionKey{k})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	if err := cp.Submit(ctx, work(k, run, `{"attempt":1}`)); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	// Nack the work several times; each requeue must wait out a backoff before the
	// next delivery, so N nacks take at least roughly (N-1) * base delay.
	const nacks = 5
	start := time.Now()
	for i := 0; i < nacks; i++ {
		d := recv(t, ch)
		if d.Work.GraphRunID != run {
			t.Fatalf("redelivery #%d run = %s, want %s", i, d.Work.GraphRunID, run)
		}
		if err := d.Nack(); err != nil {
			t.Fatalf("Nack #%d: %v", i, err)
		}
	}
	elapsed := time.Since(start)

	// A spin (immediate requeue) would complete in well under a millisecond. A
	// bounded backoff makes the same loop take at least roughly (N-1) * backoff. Use
	// half the nominal total as a conservative lower bound so the assertion is not
	// flaky on a busy CI box while still failing decisively against a spin.
	wantMin := time.Duration(nacks-1) * backoff / 2
	if elapsed < wantMin {
		t.Errorf("elapsed %v for %d nacks, want >= %v (a Nack must back off, not spin)", elapsed, nacks, wantMin)
	}

	// The work must still be redeliverable (the backoff must not drop it).
	d := recv(t, ch)
	if d.Work.GraphRunID != run {
		t.Errorf("final redelivery run = %s, want %s", d.Work.GraphRunID, run)
	}
	_ = d.Ack()
}

// TestDifferentRunsConcurrent proves single-flight is PER RUN, not global: Works
// for two DIFFERENT GraphRunIDs can be in flight simultaneously.
func TestDifferentRunsConcurrent(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cp := controlplane.Mem()
	defer cp.Close()
	k := gvk(1, "v1")
	ch, err := cp.Consume(ctx, []flow.GraphVersionKey{k})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	wA := work(k, runID(10), `{"r":"A"}`)
	wB := work(k, runID(20), `{"r":"B"}`)
	if err := cp.Submit(ctx, wA); err != nil {
		t.Fatalf("Submit A: %v", err)
	}
	if err := cp.Submit(ctx, wB); err != nil {
		t.Fatalf("Submit B: %v", err)
	}

	// Both must be deliverable without Ack'ing the first — different runs.
	d1 := recv(t, ch)
	d2 := recv(t, ch)

	got := map[flow.GraphRunID]bool{d1.Work.GraphRunID: true, d2.Work.GraphRunID: true}
	if !got[wA.GraphRunID] || !got[wB.GraphRunID] {
		t.Fatalf("expected both runs %s and %s in flight, got %v", wA.GraphRunID, wB.GraphRunID, got)
	}
	_ = d1.Ack()
	_ = d2.Ack()
}

// TestContextCancelClosesChannel proves clean shutdown: cancelling the Consume
// ctx closes its Delivery channel (so a worker loop's range exits), and a Submit
// after the control plane is cancelled returns a typed error (fail-secure — a
// shut-down control plane rejects new work rather than silently dropping it).
func TestContextCancelClosesChannel(t *testing.T) {
	t.Parallel()

	consumeCtx, cancelConsume := context.WithCancel(context.Background())

	cp := controlplane.Mem()
	defer cp.Close()
	k := gvk(1, "v1")
	ch, err := cp.Consume(consumeCtx, []flow.GraphVersionKey{k})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	cancelConsume()

	// The channel must close (eventually) so a range over it terminates.
	select {
	case _, ok := <-ch:
		if ok {
			// A buffered delivery before close is acceptable; drain until closed.
			for range ch {
			}
		}
	case <-time.After(recvTimeout):
		t.Fatalf("Consume channel not closed within %s after ctx cancel", recvTimeout)
	}

	// Submit after the consumer's ctx is cancelled must honor a cancelled SUBMIT
	// ctx by returning that ctx error.
	cancelledCtx, cancelSubmit := context.WithCancel(context.Background())
	cancelSubmit()
	if err := cp.Submit(cancelledCtx, work(k, runID(1), `{}`)); err == nil {
		t.Errorf("Submit with cancelled ctx = nil, want a context error")
	} else if !errors.Is(err, context.Canceled) {
		t.Errorf("Submit error = %v, want errors.Is context.Canceled", err)
	}
}

// TestCloseRejectsSubmit proves fail-secure shutdown via Close: after Close, a
// Submit (with a LIVE ctx, so the rejection is the control plane's, not the ctx's)
// returns a typed *controlplane.ClosedError rather than silently dropping work.
func TestCloseRejectsSubmit(t *testing.T) {
	t.Parallel()

	cp := controlplane.Mem()
	cp.Close()

	err := cp.Submit(context.Background(), work(gvk(1, "v1"), runID(1), `{}`))
	if err == nil {
		t.Fatalf("Submit after Close = nil, want *ClosedError")
	}
	var closed *controlplane.ClosedError
	if !errors.As(err, &closed) {
		t.Errorf("Submit after Close error = %v, want *controlplane.ClosedError", err)
	}
}

// TestCloseClosesConsumerChannels proves Close closes every live consumer's
// Delivery channel so each worker's `for range ch` loop terminates.
func TestCloseClosesConsumerChannels(t *testing.T) {
	t.Parallel()

	cp := controlplane.Mem()
	chA, err := cp.Consume(context.Background(), []flow.GraphVersionKey{gvk(1, "v1")})
	if err != nil {
		t.Fatalf("Consume A: %v", err)
	}
	chB, err := cp.Consume(context.Background(), []flow.GraphVersionKey{gvk(2, "v1")})
	if err != nil {
		t.Fatalf("Consume B: %v", err)
	}

	cp.Close()

	for name, ch := range map[string]<-chan flow.Delivery{"A": chA, "B": chB} {
		select {
		case _, ok := <-ch:
			if ok {
				t.Errorf("consumer %s channel delivered after Close; want closed", name)
			}
		case <-time.After(recvTimeout):
			t.Errorf("consumer %s channel not closed within %s after Close", name, recvTimeout)
		}
	}
}

// TestClosedErrorMessage pins the typed *ClosedError's message so callers and log
// readers get a stable, namespaced string.
func TestClosedErrorMessage(t *testing.T) {
	t.Parallel()

	got := (&controlplane.ClosedError{}).Error()
	const want = "controlplane: control plane is closed"
	if got != want {
		t.Errorf("ClosedError.Error() = %q, want %q", got, want)
	}
}

// TestConsumeContextErrors proves Consume honors its ctx and the control plane's
// lifetime: a pre-cancelled Consume ctx returns that ctx's error, and a Consume
// after Close returns a typed *ClosedError (fail-secure).
func TestConsumeContextErrors(t *testing.T) {
	t.Parallel()

	t.Run("cancelled consume ctx", func(t *testing.T) {
		t.Parallel()
		cp := controlplane.Mem()
		defer cp.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		ch, err := cp.Consume(ctx, []flow.GraphVersionKey{gvk(1, "v1")})
		if ch != nil {
			t.Errorf("Consume with cancelled ctx returned a non-nil channel")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("Consume error = %v, want errors.Is context.Canceled", err)
		}
	})

	t.Run("consume after close", func(t *testing.T) {
		t.Parallel()
		cp := controlplane.Mem()
		cp.Close()

		ch, err := cp.Consume(context.Background(), []flow.GraphVersionKey{gvk(1, "v1")})
		if ch != nil {
			t.Errorf("Consume after Close returned a non-nil channel")
		}
		var closed *controlplane.ClosedError
		if !errors.As(err, &closed) {
			t.Errorf("Consume after Close error = %v, want *controlplane.ClosedError", err)
		}
	})
}

// TestDispatcherResponsiveDuringSend exercises the dispatcher's send path while it
// is mid-delivery to a consumer that is not yet receiving: it must still service a
// concurrent Submit (for a different run, which then also gets delivered), an Ack
// (releasing a slot), and a second consumer's registration — i.e. the dispatcher
// never blocks on one peer. The first consumer deliberately delays its first
// receive so the dispatcher is genuinely parked in send when the other activity
// arrives.
func TestDispatcherResponsiveDuringSend(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cp := controlplane.Mem()
	defer cp.Close()
	k := gvk(1, "v1")

	ch, err := cp.Consume(ctx, []flow.GraphVersionKey{k})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// Submit work for run A; the dispatcher parks in send waiting for ch's reader.
	if err := cp.Submit(ctx, work(k, runID(1), `{"r":"A"}`)); err != nil {
		t.Fatalf("Submit A: %v", err)
	}

	// While the dispatcher is parked, register a SECOND consumer and submit work
	// for a DIFFERENT run B — both must be serviced inside send without deadlock.
	ch2, err := cp.Consume(ctx, []flow.GraphVersionKey{k})
	if err != nil {
		t.Fatalf("Consume 2: %v", err)
	}
	if err := cp.Submit(ctx, work(k, runID(2), `{"r":"B"}`)); err != nil {
		t.Fatalf("Submit B: %v", err)
	}

	// Now drain both consumers; between them they must yield both runs. Each Ack
	// is serviced by the dispatcher mid-flight.
	got := make(map[flow.GraphRunID]bool)
	for len(got) < 2 {
		select {
		case d := <-ch:
			got[d.Work.GraphRunID] = true
			_ = d.Ack()
		case d := <-ch2:
			got[d.Work.GraphRunID] = true
			_ = d.Ack()
		case <-time.After(recvTimeout):
			t.Fatalf("timed out; received runs so far: %v", got)
		}
	}
	if !got[runID(1)] || !got[runID(2)] {
		t.Errorf("expected both runs delivered, got %v", got)
	}
}

// TestNoGoroutineLeak proves the dispatcher and per-consumer watcher goroutines
// all exit on shutdown: it snapshots the goroutine count, spins up control planes
// with cancelled consumers, Closes them, and asserts the count returns to the
// baseline (within a settle window). It is NOT parallel — it reads the global
// goroutine count, which other parallel tests would perturb.
func TestNoGoroutineLeak(t *testing.T) {
	baseline := stableGoroutines()

	for i := 0; i < 8; i++ {
		cp := controlplane.Mem()
		ctx, cancel := context.WithCancel(context.Background())
		if _, err := cp.Consume(ctx, []flow.GraphVersionKey{gvk(byte(i), "v1")}); err != nil {
			t.Fatalf("Consume: %v", err)
		}
		if _, err := cp.Consume(context.Background(), []flow.GraphVersionKey{gvk(byte(i), "v2")}); err != nil {
			t.Fatalf("Consume: %v", err)
		}
		cancel()   // ends one consumer's subscription (its watcher must exit)
		cp.Close() // ends the dispatcher and the remaining watcher
	}

	got := stableGoroutines()
	// Allow a small slack for the test runtime's own bookkeeping goroutines.
	if got > baseline+2 {
		t.Errorf("goroutine count grew from %d to %d after Close cycles; suspected leak", baseline, got)
	}
}

// stableGoroutines returns runtime.NumGoroutine once it has been unchanged across
// a few short polls, so transient teardown goroutines settle before the snapshot.
func stableGoroutines() int {
	prev := runtime.NumGoroutine()
	stable := 0
	for i := 0; i < 50; i++ {
		time.Sleep(10 * time.Millisecond)
		n := runtime.NumGoroutine()
		if n == prev {
			stable++
			if stable >= 3 {
				return n
			}
		} else {
			stable = 0
			prev = n
		}
	}
	return prev
}

// TestImplementsControlPlane is a compile-time assertion that *MemControlPlane
// satisfies flow.ControlPlane (§18.5).
func TestImplementsControlPlane(t *testing.T) {
	t.Parallel()
	cp := controlplane.Mem()
	defer cp.Close()
	var _ flow.ControlPlane = cp
}
