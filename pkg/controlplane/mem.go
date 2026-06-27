package controlplane

import (
	"context"
	"sync"

	"github.com/ciram-co/flow/pkg/flow"
)

// This file implements the in-process ControlPlane (design §18.5): a local,
// ephemeral, channel-based queue + dispatcher that accepts run/resume Work and
// feeds it to workers. It implements flow.ControlPlane structurally (the seam
// types live in pkg/flow to avoid an import cycle — see pkg/flow/controlplane.go).
// It is the Tier-A/B counterpart to the future durable nats.ControlPlane, and is
// SEPARATE from the CheckpointStore (transient consume-once dispatch vs durable
// append-only history, §18.5).
//
// Concurrency model — a SINGLE OWNER GOROUTINE.
//
// All mutable state (the pending Work queue, the set of in-flight GraphRunIDs,
// and the list of registered consumers) is owned by ONE dispatcher goroutine.
// Every mutation arrives as a message on a channel; the dispatcher serializes
// them. Nothing else touches that state, so the §18.5 invariants — most
// importantly single-flight per run — hold without fine-grained locks and are
// trivially race-clean. Public methods are thin: they marshal a request onto a
// channel and (where a reply is needed) wait on a per-request reply channel,
// always selecting on ctx so no call blocks unboundedly.
//
// Critically, the dispatcher NEVER blocks indefinitely on a single peer. When it
// hands a Delivery to a consumer it does so in a select that ALSO services
// submits/consumes/releases, so a worker that Acks/Nacks (or another worker that
// submits) while the dispatcher is mid-send cannot deadlock it. A consumer that
// is not currently receiving simply does not get that Work this turn; the work
// stays pending and is retried on the next state change.
//
// Single-flight per run (§18.5). The dispatcher delivers a Work only if NO other
// Work for the same GraphRunID is already in flight (delivered, not yet
// Ack/Nack'd). It records the run id in inFlight on delivery; Ack releases the
// slot and drops the Work; Nack releases the slot and re-enqueues the Work for
// redelivery. Releasing a slot re-runs dispatch, so a held Work for that run (or
// the requeued one) flows out next. Different runs are independent, so they can be
// in flight concurrently.
//
// Routing / implicit registration (§18.5). A consumer registers by Consuming the
// version keys it serves; the dispatcher delivers a Work only to a registered
// consumer whose serves-set contains the Work's Key — the version IS the route.
//
// Shutdown. When the control plane's lifetime ctx is done the dispatcher closes
// every consumer's Delivery channel (so a worker's `range` terminates) and exits;
// when a single consumer's Consume ctx is done that one consumer is unregistered
// and its channel closed. Either way there is no goroutine leak.

// MemControlPlane is the in-process, channel-based control plane (§18.5). It is
// local and ephemeral: enqueued Work lives only in memory and is lost on process
// exit (durability is the CheckpointStore's job, separate). Construct it with Mem.
type MemControlPlane struct {
	ctx    context.Context
	cancel context.CancelFunc

	submits     chan submitReq  // Submit -> dispatcher
	consumes    chan consumeReq // Consume -> dispatcher (register a consumer)
	releases    chan releaseReq // Ack/Nack -> dispatcher (free a run's slot)
	deregisters chan int        // a consumer watcher -> dispatcher: this consumer's ctx is done
	done        chan struct{}   // closed when the dispatcher has fully exited
}

// submitReq carries one Submit onto the dispatcher with a reply channel for the
// outcome (accepted, or rejected because the control plane is shutting down).
type submitReq struct {
	work  flow.Work
	reply chan error
}

// consumeReq carries one Consume registration onto the dispatcher with a reply
// channel for the freshly created Delivery channel.
type consumeReq struct {
	serves []flow.GraphVersionKey
	ctx    context.Context
	reply  chan (<-chan flow.Delivery)
}

// releaseKind distinguishes how a Delivery's slot is being released.
type releaseKind uint8

const (
	relAck  releaseKind = iota // drop the work, free the run slot
	relNack                    // requeue the work, free the run slot
)

// releaseReq frees an in-flight run's single-flight slot. On relNack the work is
// re-enqueued for redelivery.
type releaseReq struct {
	run  flow.GraphRunID
	work flow.Work
	kind releaseKind
}

// consumer is the dispatcher's view of one registered worker: the version keys it
// serves, the channel it receives Deliveries on, the ctx that scopes its
// subscription, and a removed channel the dispatcher closes when it unregisters
// the consumer (so the consumer's watcher goroutine exits without leaking). It is
// owned solely by the dispatcher goroutine.
type consumer struct {
	id      int
	serves  map[flow.GraphVersionKey]struct{}
	out     chan flow.Delivery
	ctx     context.Context
	removed chan struct{}
}

// servesKey reports whether this consumer is subscribed to key k.
func (c *consumer) servesKey(k flow.GraphVersionKey) bool {
	_, ok := c.serves[k]
	return ok
}

// Mem constructs and starts an in-process control plane (§18.5). The returned
// *MemControlPlane runs a background dispatcher goroutine bound to a fresh
// lifetime ctx; the dispatcher exits (closing all consumer channels) when that
// ctx is cancelled — see Close. Submit/Consume ctxs passed to the methods scope
// individual calls/subscriptions and are independent of the lifetime ctx.
func Mem() *MemControlPlane {
	ctx, cancel := context.WithCancel(context.Background())
	cp := &MemControlPlane{
		ctx:         ctx,
		cancel:      cancel,
		submits:     make(chan submitReq),
		consumes:    make(chan consumeReq),
		releases:    make(chan releaseReq),
		deregisters: make(chan int),
		done:        make(chan struct{}),
	}
	go cp.dispatch()
	return cp
}

// Close cancels the control plane's lifetime ctx, closing every consumer's
// Delivery channel and stopping the dispatcher. It blocks until the dispatcher
// has fully exited, so after Close returns no control-plane goroutine remains. It
// is idempotent (cancelling an already-cancelled ctx is a no-op).
func (cp *MemControlPlane) Close() {
	cp.cancel()
	<-cp.done
}

// Submit enqueues w for consumers serving w.Key (§18.5). It honors ctx and the
// control plane's lifetime: it returns ctx.Err() if ctx is done, or a typed
// *ClosedError if the control plane has been shut down (fail-secure — a shut-down
// control plane rejects new work rather than silently dropping it). It does not
// block on consumer availability: enqueue is decoupled from delivery.
func (cp *MemControlPlane) Submit(ctx context.Context, w flow.Work) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	reply := make(chan error, 1)
	select {
	case cp.submits <- submitReq{work: w, reply: reply}:
	case <-ctx.Done():
		return ctx.Err()
	case <-cp.ctx.Done():
		return &ClosedError{}
	}
	select {
	case err := <-reply:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-cp.ctx.Done():
		return &ClosedError{}
	}
}

// Consume registers a worker serving the given version keys and returns a channel
// delivering only Work whose Key is in serves (§18.5). Registration is implicit:
// consuming IS registering — there is no separate RPC. The channel is closed when
// ctx is done (this subscription ends) or when the control plane is shut down
// (Close), so a `for d := range ch` worker loop terminates cleanly with no
// goroutine leak. A nil/empty serves registers a consumer that receives nothing.
func (cp *MemControlPlane) Consume(ctx context.Context, serves []flow.GraphVersionKey) (<-chan flow.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	reply := make(chan (<-chan flow.Delivery), 1)
	select {
	case cp.consumes <- consumeReq{serves: serves, ctx: ctx, reply: reply}:
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-cp.ctx.Done():
		return nil, &ClosedError{}
	}
	select {
	case ch := <-reply:
		return ch, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-cp.ctx.Done():
		return nil, &ClosedError{}
	}
}

// dispState is the dispatcher's private mutable state. It is owned solely by the
// dispatch goroutine; bundling it keeps the helper methods' signatures small.
type dispState struct {
	pending   []flow.Work                  // FIFO queue of undelivered Work
	inFlight  map[flow.GraphRunID]struct{} // runs with a Work currently delivered (single-flight)
	consumers map[int]*consumer            // id -> registered consumer
	nextID    int
}

// dispatch is the single owner goroutine: it serializes every state mutation and
// is the ONLY accessor of dispState, so the §18.5 invariants hold without locks.
// It loops until the lifetime ctx is done, then closes all consumer channels.
func (cp *MemControlPlane) dispatch() {
	defer close(cp.done)

	st := &dispState{
		inFlight:  make(map[flow.GraphRunID]struct{}),
		consumers: make(map[int]*consumer),
	}

	for {
		// Place every Work that is currently deliverable. Each delivery send is
		// itself a select that stays responsive to releases/submits/consumes, so
		// it can never deadlock the dispatcher against a worker that Acks mid-send.
		cp.drain(st)

		select {
		case <-cp.ctx.Done():
			for id := range st.consumers {
				cp.removeConsumer(st, id)
			}
			return

		case req := <-cp.submits:
			st.pending = append(st.pending, req.work)
			req.reply <- nil

		case req := <-cp.consumes:
			req.reply <- cp.register(st, req).out

		case id := <-cp.deregisters:
			cp.removeConsumer(st, id)

		case rel := <-cp.releases:
			delete(st.inFlight, rel.run)
			if rel.kind == relNack {
				st.pending = append(st.pending, rel.work) // requeue for redelivery
			}
		}
	}
}

// drain places every Work that is currently deliverable. It repeatedly scans
// pending for the first Work that is (a) not blocked by an in-flight Work for the
// same run and (b) servable by some live consumer, delivers it, and rescans —
// until a full scan finds nothing deliverable. Each delivery send selects on the
// control plane's other channels too, so while waiting for the chosen consumer to
// receive, the dispatcher still services releases/submits/consumes (no deadlock).
// Draining fully each turn (rather than one-per-turn) keeps deliverable work from
// stalling when several Works become eligible at once.
func (cp *MemControlPlane) drain(st *dispState) {
	for {
		// Stop draining once the control plane is shutting down: send would return
		// false immediately on the closed lifetime ctx, which (treated as progress)
		// would spin drain forever. The main loop's ctx case then tears everything
		// down.
		if cp.ctx.Err() != nil {
			return
		}
		placed := false
		for i := 0; i < len(st.pending); i++ {
			w := st.pending[i]
			if _, busy := st.inFlight[w.GraphRunID]; busy {
				continue // single-flight: a Work for this run is already in flight
			}
			c := cp.pickConsumer(st, w.Key)
			if c == nil {
				continue // no live consumer serves this key (yet)
			}
			landed := cp.send(st, c, w)
			if landed {
				st.inFlight[w.GraphRunID] = struct{}{}
				st.pending = append(st.pending[:i], st.pending[i+1:]...)
			}
			placed = true // state changed (delivered, or consumer removed); rescan
			break
		}
		if !placed {
			return
		}
	}
}

// send hands one Delivery for w to consumer c. While waiting for c to receive, it
// stays responsive to the control plane's other inputs (so an Ack from a worker,
// or a concurrent Submit, cannot deadlock the dispatcher): a release is applied
// inline, a submit/consume is buffered into state, and a ctx-done reaps the
// consumer. It returns true iff c actually received the Delivery. A returned false
// means the Work stays pending (it will be retried) and no in-flight slot is taken.
func (cp *MemControlPlane) send(st *dispState, c *consumer, w flow.Work) bool {
	var once sync.Once
	release := func(kind releaseKind) error {
		once.Do(func() {
			select {
			case cp.releases <- releaseReq{run: w.GraphRunID, work: w, kind: kind}:
			case <-cp.ctx.Done():
			}
		})
		return nil
	}
	d := flow.Delivery{
		Work: w,
		Ack:  func() error { return release(relAck) },
		Nack: func() error { return release(relNack) },
	}
	for {
		select {
		case c.out <- d:
			return true

		case <-c.ctx.Done():
			cp.removeConsumer(st, c.id) // consumer gone; do not deliver to it
			return false

		case <-cp.ctx.Done():
			return false // shutting down; loop's ctx case will close channels

		case req := <-cp.submits:
			st.pending = append(st.pending, req.work)
			req.reply <- nil

		case req := <-cp.consumes:
			req.reply <- cp.register(st, req).out

		case id := <-cp.deregisters:
			cp.removeConsumer(st, id)
			if id == c.id {
				return false // the consumer we were sending to is gone
			}

		case rel := <-cp.releases:
			delete(st.inFlight, rel.run)
			if rel.kind == relNack {
				st.pending = append(st.pending, rel.work)
			}
		}
	}
}

// register builds a consumer for req, records it in state, and spawns a watcher
// goroutine that notifies the dispatcher (via deregisters) when the consumer's ctx
// ends — so a Consume whose ctx is cancelled has its channel closed PROMPTLY, even
// when the dispatcher is otherwise idle (no other input to wake it). It returns
// the consumer so the caller can hand its out channel back to the Consume call.
// Only the dispatcher calls register, so the state mutation is race-free.
func (cp *MemControlPlane) register(st *dispState, req consumeReq) *consumer {
	c := &consumer{
		id:      st.nextID,
		serves:  keySet(req.serves),
		out:     make(chan flow.Delivery),
		ctx:     req.ctx,
		removed: make(chan struct{}),
	}
	st.consumers[c.id] = c
	st.nextID++
	go cp.watch(c)
	return c
}

// watch waits for consumer c's ctx to end (its subscription is over) and asks the
// dispatcher to deregister it. It also exits if the dispatcher already removed c
// (c.removed closed) or the control plane shut down (lifetime ctx done), so it
// never leaks. The dispatcher — sole owner of c.out — performs the actual close.
func (cp *MemControlPlane) watch(c *consumer) {
	select {
	case <-c.ctx.Done():
		select {
		case cp.deregisters <- c.id:
		case <-c.removed:
		case <-cp.ctx.Done():
		}
	case <-c.removed:
	case <-cp.ctx.Done():
	}
}

// pickConsumer returns a live consumer (ctx not done) serving k, or nil. Selecting
// the first match is fine for the in-memory plane; durable backends balance via
// the queue. A consumer whose ctx is already done is skipped (its watcher will
// deregister it shortly).
func (cp *MemControlPlane) pickConsumer(st *dispState, k flow.GraphVersionKey) *consumer {
	for _, c := range st.consumers {
		if c.ctx.Err() != nil {
			continue
		}
		if c.servesKey(k) {
			return c
		}
	}
	return nil
}

// removeConsumer unregisters the consumer with id and closes its channel and its
// removed signal (waking its watcher). It is a no-op if already removed, so a
// ctx-done in send and a deregisters message racing to remove the same consumer
// are both safe — the dispatcher is the single owner of the close. Only the
// dispatcher calls it.
func (cp *MemControlPlane) removeConsumer(st *dispState, id int) {
	c, ok := st.consumers[id]
	if !ok {
		return
	}
	delete(st.consumers, id)
	close(c.out)
	close(c.removed)
}

// keySet builds a set from a serves slice for O(1) membership tests. A nil/empty
// slice yields an empty set (a consumer that serves nothing).
func keySet(serves []flow.GraphVersionKey) map[flow.GraphVersionKey]struct{} {
	set := make(map[flow.GraphVersionKey]struct{}, len(serves))
	for _, k := range serves {
		set[k] = struct{}{}
	}
	return set
}
