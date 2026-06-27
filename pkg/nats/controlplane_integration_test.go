//go:build integration

package nats

import (
	"context"
	"encoding/json"
	"errors"
	"runtime"
	"testing"
	"time"

	"github.com/ciram-co/flow/pkg/flow"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// This file behaviorally tests the JetStream-backed flow.ControlPlane (§18.5)
// against a REAL embedded JetStream server. It mirrors controlplane.Mem's suite
// where the contract is shared (Submit→Consume delivery, version routing, Nack
// redelivery, ctx-cancel shutdown) and adds the distributed-specific cases the
// design demands: COMPETING CONSUMERS over a shared per-token durable (load
// balancing, no duplicates), bounded-rate Nack redelivery (the H1 lesson, no
// spin), and POISON-message termination (an undecodable body is Term'd, not
// redelivered forever, and good messages keep flowing).
//
// Every assertion is deterministic: a required delivery waits up to cpRecvTimeout;
// a proven-negative (e.g. v2 work must NOT reach a v1 worker) waits a short
// cpNotRecvWindow. No sleeps-as-synchronization.

const (
	// cpRecvTimeout generously bounds a REQUIRED delivery. JetStream over an
	// in-process connection delivers promptly, so a real delivery never nears it.
	cpRecvTimeout = 10 * time.Second

	// cpNotRecvWindow bounds proving a NEGATIVE (a work that must not arrive). It
	// is long enough that an eager/buggy delivery would lose the race, yet short
	// enough to keep the suite fast.
	cpNotRecvWindow = 750 * time.Millisecond
)

// cpRunID builds a deterministic non-zero GraphRunID from a single byte.
func cpRunID(b byte) flow.GraphRunID {
	var id flow.GraphRunID
	id[0] = b
	return id
}

// cpWork builds a Work for key with the given run id and OpRun, carrying input as
// raw JSON so equality/identity is checkable.
func cpWork(key flow.GraphVersionKey, run flow.GraphRunID, input string) flow.Work {
	return flow.Work{
		Key:        key,
		GraphRunID: run,
		Op:         flow.OpRun,
		Input:      json.RawMessage(input),
	}
}

// newTestCP boots an embedded JetStream server, an in-process connection, and a
// ControlPlane bound to runCtx, registering cleanups so each test tears its own
// stack down. It returns the connection (for raw publishes in the poison test)
// and the control plane.
func newTestCP(t *testing.T, runCtx context.Context, opts ...ControlPlaneOption) (*nats.Conn, *ControlPlane) {
	t.Helper()
	srv, err := Embedded(WithStoreDir(t.TempDir()), WithReadyTimeout(20*time.Second))
	if err != nil {
		t.Fatalf("Embedded() error = %v", err)
	}
	t.Cleanup(srv.Close)

	nc, err := srv.InProcessConn()
	if err != nil {
		t.Fatalf("InProcessConn() error = %v", err)
	}
	t.Cleanup(nc.Close)

	cp, err := NewControlPlane(runCtx, nc, opts...)
	if err != nil {
		t.Fatalf("NewControlPlane() error = %v", err)
	}
	return nc, cp
}

// cpRecv waits up to cpRecvTimeout for one Delivery on ch; fails if the channel
// closes early or nothing arrives.
func cpRecv(t *testing.T, ch <-chan flow.Delivery) flow.Delivery {
	t.Helper()
	select {
	case d, ok := <-ch:
		if !ok {
			t.Fatalf("Consume channel closed unexpectedly while awaiting a delivery")
		}
		return d
	case <-time.After(cpRecvTimeout):
		t.Fatalf("timed out after %s awaiting a required delivery", cpRecvTimeout)
		return flow.Delivery{}
	}
}

// cpAssertNoRecv asserts NO Delivery arrives within cpNotRecvWindow.
func cpAssertNoRecv(t *testing.T, ch <-chan flow.Delivery, why string) {
	t.Helper()
	select {
	case d, ok := <-ch:
		if !ok {
			return
		}
		t.Fatalf("unexpected delivery (%s): run=%s key=%v", why, d.Work.GraphRunID, d.Work.Key)
	case <-time.After(cpNotRecvWindow):
	}
}

// TestControlPlaneSubmitConsumeAck proves the happy path against JetStream: a
// submitted Work is delivered to a consumer serving its key with all fields
// intact, and Ack removes it from the work queue (it is not redelivered).
func TestControlPlaneSubmitConsumeAck(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, cp := newTestCP(t, ctx)

	k := cpGVK(1, "v1")
	ch, err := cp.Consume(ctx, []flow.GraphVersionKey{k})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	w := cpWork(k, cpRunID(1), `{"n":1}`)
	if err := cp.Submit(ctx, w); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	d := cpRecv(t, ch)
	if d.Work.GraphRunID != w.GraphRunID {
		t.Errorf("GraphRunID = %s, want %s", d.Work.GraphRunID, w.GraphRunID)
	}
	if d.Work.Key != w.Key {
		t.Errorf("Key = %v, want %v", d.Work.Key, w.Key)
	}
	if d.Work.Op != w.Op {
		t.Errorf("Op = %v, want %v", d.Work.Op, w.Op)
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

	// After Ack the work is removed: nothing more should arrive.
	cpAssertNoRecv(t, ch, "acked work was redelivered")
}

// TestControlPlaneNackRedelivers proves a Nack'd Work comes back AND that the
// redelivery is bounded-rate (the H1 lesson): it does not return instantly in a
// 100%-CPU spin. The configured nack delay makes the gap between deliveries
// measurable; the test asserts the redelivery is delayed by at least a fraction
// of that delay.
func TestControlPlaneNackRedelivers(t *testing.T) {
	t.Parallel()

	const nackDelay = 400 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, cp := newTestCP(t, ctx, WithNackDelay(nackDelay))

	k := cpGVK(2, "v1")
	run := cpRunID(3)
	ch, err := cp.Consume(ctx, []flow.GraphVersionKey{k})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	w := cpWork(k, run, `{"attempt":1}`)
	if err := cp.Submit(ctx, w); err != nil {
		t.Fatalf("Submit: %v", err)
	}

	d1 := cpRecv(t, ch)
	nackedAt := time.Now()
	if err := d1.Nack(); err != nil {
		t.Fatalf("Nack: %v", err)
	}

	d2 := cpRecv(t, ch)
	gap := time.Since(nackedAt)
	if d2.Work.GraphRunID != run || string(d2.Work.Input) != string(w.Input) {
		t.Errorf("redelivered work = (run %s, input %s), want (run %s, input %s)",
			d2.Work.GraphRunID, d2.Work.Input, run, w.Input)
	}
	// A bounded-rate Nack must NOT return instantly. Use a conservative lower bound
	// (half the nominal delay) so the assertion is robust on a busy CI box while
	// still failing decisively against an instant-redelivery spin.
	if gap < nackDelay/2 {
		t.Errorf("redelivery gap %v < %v: a Nack must back off, not spin", gap, nackDelay/2)
	}
	if err := d2.Ack(); err != nil {
		t.Errorf("Ack redelivery: %v", err)
	}
}

// TestControlPlaneVersionRouting proves version isolation: a worker serving v1
// receives v1 work but NEVER v2 work, even when both are submitted. The version
// IS the route (hashed into the subject), and the work-queue's per-token consumer
// only filters its own subject.
func TestControlPlaneVersionRouting(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, cp := newTestCP(t, ctx)

	v1, v2 := cpGVK(1, "v1"), cpGVK(1, "v2")
	ch, err := cp.Consume(ctx, []flow.GraphVersionKey{v1})
	if err != nil {
		t.Fatalf("Consume v1: %v", err)
	}

	// Submit a v2 work first (the one that must be filtered away), then a v1 work.
	if err := cp.Submit(ctx, cpWork(v2, cpRunID(2), `{"v":2}`)); err != nil {
		t.Fatalf("Submit v2: %v", err)
	}
	if err := cp.Submit(ctx, cpWork(v1, cpRunID(1), `{"v":1}`)); err != nil {
		t.Fatalf("Submit v1: %v", err)
	}

	// The v1 consumer must receive ONLY the v1 work.
	d := cpRecv(t, ch)
	if d.Work.Key != v1 {
		t.Fatalf("delivered key = %v, want v1 %v", d.Work.Key, v1)
	}
	_ = d.Ack()

	// And it must never see the v2 work.
	cpAssertNoRecv(t, ch, "v2 work routed to a v1-only consumer")
}

// TestControlPlaneCompetingConsumers proves the core distributed design: two
// Consume() subscriptions serving the SAME key share ONE durable consumer
// (competing consumers), so N submitted works are LOAD-BALANCED — each delivered
// to exactly one subscription, the union equal to N with NO duplicates. This is
// the property a WorkQueuePolicy stream + shared-per-token durable provides.
func TestControlPlaneCompetingConsumers(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, cp := newTestCP(t, ctx)

	k := cpGVK(5, "v1")
	chA, err := cp.Consume(ctx, []flow.GraphVersionKey{k})
	if err != nil {
		t.Fatalf("Consume A: %v", err)
	}
	chB, err := cp.Consume(ctx, []flow.GraphVersionKey{k})
	if err != nil {
		t.Fatalf("Consume B: %v", err)
	}

	const n = 12
	for i := 0; i < n; i++ {
		if err := cp.Submit(ctx, cpWork(k, cpRunID(byte(100+i)), `{}`)); err != nil {
			t.Fatalf("Submit %d: %v", i, err)
		}
	}

	// Collect exactly n deliveries across BOTH subscriptions; assert no run id is
	// delivered twice (each message goes to exactly one competing consumer).
	seen := make(map[flow.GraphRunID]int)
	deadline := time.After(cpRecvTimeout)
	for len(seen) < n {
		select {
		case d := <-chA:
			seen[d.Work.GraphRunID]++
			_ = d.Ack()
		case d := <-chB:
			seen[d.Work.GraphRunID]++
			_ = d.Ack()
		case <-deadline:
			t.Fatalf("timed out: got %d of %d distinct works", len(seen), n)
		}
	}
	for run, count := range seen {
		if count != 1 {
			t.Errorf("run %s delivered %d times, want exactly 1 (no duplicates)", run, count)
		}
	}
	if len(seen) != n {
		t.Errorf("delivered %d distinct works, want %d", len(seen), n)
	}
}

// TestControlPlaneMultiKeyFanIn proves multi-key fan-in: a SINGLE Consume over two
// DISTINCT keys (k1, k2) feeds BOTH keys' work onto the ONE returned channel. Each
// key gets its own shared durable (distinct FilterSubjects, no work-queue overlap),
// and every key's deliveries fan in to the single channel a `range`-ing worker
// loop reads — the property flow.Serve relies on to serve many versions from one
// worker. Submitting to each key and collecting both off the one channel locks it
// in.
func TestControlPlaneMultiKeyFanIn(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, cp := newTestCP(t, ctx)

	k1, k2 := cpGVK(7, "v1"), cpGVK(8, "v2")
	ch, err := cp.Consume(ctx, []flow.GraphVersionKey{k1, k2})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	w1 := cpWork(k1, cpRunID(71), `{"k":1}`)
	w2 := cpWork(k2, cpRunID(82), `{"k":2}`)
	if err := cp.Submit(ctx, w1); err != nil {
		t.Fatalf("Submit k1: %v", err)
	}
	if err := cp.Submit(ctx, w2); err != nil {
		t.Fatalf("Submit k2: %v", err)
	}

	// Collect exactly two deliveries off the single fanned-in channel and assert one
	// for EACH key arrived (order is not guaranteed across two subscriptions).
	got := make(map[flow.GraphVersionKey]flow.GraphRunID, 2)
	for len(got) < 2 {
		d := cpRecv(t, ch)
		if _, dup := got[d.Work.Key]; dup {
			t.Fatalf("key %v delivered twice; want one per key", d.Work.Key)
		}
		got[d.Work.Key] = d.Work.GraphRunID
		if err := d.Ack(); err != nil {
			t.Errorf("Ack: %v", err)
		}
	}
	if got[k1] != w1.GraphRunID {
		t.Errorf("k1 delivered run = %s, want %s", got[k1], w1.GraphRunID)
	}
	if got[k2] != w2.GraphRunID {
		t.Errorf("k2 delivered run = %s, want %s", got[k2], w2.GraphRunID)
	}
}

// TestControlPlaneContextCancelClosesChannel proves clean shutdown: cancelling
// the run ctx closes the returned Delivery channel (so a worker's `range`
// terminates) and a subsequent Submit honoring a cancelled ctx returns an error.
func TestControlPlaneContextCancelClosesChannel(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	_, cp := newTestCP(t, ctx)

	k := cpGVK(1, "v1")
	ch, err := cp.Consume(ctx, []flow.GraphVersionKey{k})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	cancel()

	select {
	case _, ok := <-ch:
		if ok {
			for range ch { // drain any buffered delivery until close
			}
		}
	case <-time.After(cpRecvTimeout):
		t.Fatalf("Consume channel not closed within %s after ctx cancel", cpRecvTimeout)
	}

	// Submit honoring a cancelled ctx returns a context error.
	cctx, ccancel := context.WithCancel(context.Background())
	ccancel()
	if err := cp.Submit(cctx, cpWork(k, cpRunID(1), `{}`)); err == nil {
		t.Errorf("Submit with cancelled ctx = nil, want a context error")
	} else if !errors.Is(err, context.Canceled) {
		t.Errorf("Submit error = %v, want errors.Is context.Canceled", err)
	}
}

// TestControlPlaneNoGoroutineLeak proves ctx-cancel shutdown leaks no CLIENT-side
// goroutine: it repeatedly Consumes and cancels, then asserts the goroutine count
// returns to a baseline. It deliberately reuses the SAME served key every
// iteration so each Consume attaches to the ONE shared durable consumer — its
// server-side goroutines are durable and INTENTIONALLY persist (the design does
// not delete durables on shutdown), so reusing the key isolates the measurement to
// the plane's own subscription/reaper goroutines, which MUST all exit on cancel.
// A baseline taken AFTER one warm-up cycle excludes the durable's one-time
// server-side goroutines from the delta. NOT parallel — it reads the global count.
func TestControlPlaneNoGoroutineLeak(t *testing.T) {
	srv, err := Embedded(WithStoreDir(t.TempDir()), WithReadyTimeout(20*time.Second))
	if err != nil {
		t.Fatalf("Embedded: %v", err)
	}
	defer srv.Close()
	nc, err := srv.InProcessConn()
	if err != nil {
		t.Fatalf("InProcessConn: %v", err)
	}
	defer nc.Close()

	key := cpGVK(33, "v1")
	cycle := func() {
		ctx, cancel := context.WithCancel(context.Background())
		cp, err := NewControlPlane(ctx, nc)
		if err != nil {
			t.Fatalf("NewControlPlane: %v", err)
		}
		ch, err := cp.Consume(ctx, []flow.GraphVersionKey{key})
		if err != nil {
			t.Fatalf("Consume: %v", err)
		}
		cancel()
		for range ch { // drain until the channel closes (clean shutdown)
		}
	}

	// Warm up so the shared durable's server-side goroutines exist before baseline.
	cycle()
	baseline := cpStableGoroutines()

	for i := 0; i < 5; i++ {
		cycle()
	}

	got := cpStableGoroutines()
	if got > baseline+3 {
		t.Errorf("goroutine count grew from %d to %d after cancel cycles; suspected client-side leak", baseline, got)
	}
}

// TestControlPlanePoisonMessageTermed proves an undecodable work body is
// TERMINATED (not Nak-looped forever, the H1 lesson) and that the consumer keeps
// serving GOOD messages afterward. A raw, non-JSON body is published directly to
// the work subject; the control plane must Term it and continue.
func TestControlPlanePoisonMessageTermed(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	nc, cp := newTestCP(t, ctx)

	k := cpGVK(9, "v1")
	ch, err := cp.Consume(ctx, []flow.GraphVersionKey{k})
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}

	// Publish a poison (undecodable) body directly to the routing subject.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if _, err := js.Publish(ctx, workSubjectFor(k), []byte("this is not valid work json")); err != nil {
		t.Fatalf("publish poison: %v", err)
	}

	// The poison must NOT be delivered to the worker channel (it cannot decode), and
	// it must NOT loop: a good message published afterward must arrive promptly.
	good := cpWork(k, cpRunID(1), `{"ok":true}`)
	if err := cp.Submit(ctx, good); err != nil {
		t.Fatalf("Submit good: %v", err)
	}

	d := cpRecv(t, ch)
	if d.Work.GraphRunID != good.GraphRunID {
		t.Errorf("delivered run = %s, want good %s (poison should be Term'd, not delivered)",
			d.Work.GraphRunID, good.GraphRunID)
	}
	_ = d.Ack()

	// No further deliveries: the poison was terminated, not requeued.
	cpAssertNoRecv(t, ch, "poison message was redelivered instead of Term'd")
}

// TestControlPlaneImplementsInterface is a compile-time assertion that
// *ControlPlane satisfies flow.ControlPlane (§18.5).
func TestControlPlaneImplementsInterface(t *testing.T) {
	t.Parallel()
	var _ flow.ControlPlane = (*ControlPlane)(nil)
}

// cpStableGoroutines returns runtime.NumGoroutine once it has held steady across
// a few polls, so transient teardown goroutines settle before the snapshot.
func cpStableGoroutines() int {
	prev := runtime.NumGoroutine()
	stable := 0
	for i := 0; i < 80; i++ {
		time.Sleep(15 * time.Millisecond)
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
