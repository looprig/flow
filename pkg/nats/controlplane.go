package nats

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"time"

	"github.com/looprig/flow/pkg/flow"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// This file implements a DISTRIBUTED flow.ControlPlane (design §18.5) over a
// single JetStream WORK stream, version-routed by subject. It honors the same
// Submit/Consume/Ack/Nack contract controlplane.Mem satisfies, but distributes
// work across processes. It is the Tier-C counterpart to the in-process plane and
// is SEPARATE from the CheckpointStore (transient consume-once dispatch vs durable
// append-only history) — it uses its OWN stream (FLOW_WORK), never the store's.
//
// THE WORK QUEUE.
//
// The stream uses WorkQueuePolicy retention: each message is delivered once and
// REMOVED on ack (this is single-flight at MESSAGE granularity — the §18.5
// best-effort guarantee; correctness against duplicate COMMITTED effects is the
// store's job via compare-and-append + IdempotencyKey, never this transport's).
// The control plane is a PURE TRANSPORT: it decodes the Work ENVELOPE but never
// parses or trusts Work.Input (a json.RawMessage that passes straight through to
// the worker's Runner, which validates it).
//
// VERSION ROUTING (the version IS the route, §18.5).
//
// GraphVersion is an ARBITRARY user string, so it can NEVER appear raw in a NATS
// subject (CLAUDE.md: sanitize before use). workSubjectFor hashes (GraphID,
// GraphVersion) to a single hex token under "flow.work." — the same pure function
// for Submit and Consume, so routing matches by construction. The GraphRunID rides
// in the PAYLOAD, not the subject.
//
// COMPETING CONSUMERS over a SHARED per-token durable (the non-overlap constraint).
//
// A WorkQueuePolicy stream REQUIRES non-overlapping consumer FilterSubjects: two
// consumers cannot both filter the same subject (the server rejects the second).
// So workers co-serving a key MUST SHARE one durable consumer (competing consumers
// / load balancing). Consume therefore ensures, per served key, a durable pull
// consumer named DETERMINISTICALLY by the routing token (durableFor) so every
// worker serving that key derives — and shares — the same one. Creation is via
// CreateOrUpdateConsumer, which is idempotent for an identical name+config: the
// first worker creates it, the rest attach to it. The hex token is name-safe (no
// whitespace/'.'/'*'/'>'/slashes). A worker serving MULTIPLE keys gets one durable
// PER KEY (distinct FilterSubjects), so no overlap arises within a single worker.
//
// FAN-IN + CLEAN SHUTDOWN.
//
// All served keys' consumers feed ONE returned channel. Each key is consumed via
// consumer.Consume(handler) (a push subscription) whose handler forwards decoded
// Deliveries onto the shared channel, selecting on the run ctx so a blocked send
// during shutdown cannot leak. On ctx.Done the plane Stops every per-key
// subscription and closes the channel once all handlers have drained (a WaitGroup
// gates the close), so a `for d := range ch` worker loop terminates with NO
// goroutine leak. The durable consumers are NOT deleted on shutdown — they are
// SHARED and durable (other workers and restarts depend on them); only the LOCAL
// subscription is stopped.
//
// POISON HANDLING (the H1 lesson: bounded-rate, no spin).
//
// A message whose body is not a valid Work envelope can NEVER decode, so Nak-looping
// it would be an infinite redelivery spin. Such a message is Term'd (dropped, never
// redelivered) and the consumer keeps serving good messages. For a worker's explicit
// Nack (a transient failure it wants retried), Delivery.Nack uses NakWithDelay so
// redelivery is bounded-rate, not an instant spin — JetStream's plain Nak triggers
// IMMEDIATE redelivery and ignores AckWait/BackOff, which would reintroduce the hot
// loop H1 warns against. The consumer's AckWait/BackOff/MaxDeliver cover the
// no-ack (crash) path with a capped, increasing redelivery schedule.

const (
	// workSubjectPrefix is the namespace for all version-routed work subjects. Each
	// (GraphID, GraphVersion) hashes to exactly one token appended to this prefix.
	workSubjectPrefix = "flow.work."

	// workStreamSubjectWildcard is the single subject the work stream binds.
	workStreamSubjectWildcard = workSubjectPrefix + ">"

	// defaultWorkStreamName is the JetStream work stream (SEPARATE from the store's
	// FLOW_CHECKPOINTS): it carries transient run/resume dispatch, not durable
	// history.
	defaultWorkStreamName = "FLOW_WORK"

	// durablePrefix prefixes every per-token shared durable consumer name. The
	// routing-token hex is appended; the whole name is JetStream-name-safe.
	durablePrefix = "flow_work_"

	// defaultWorkMaxMsgSize bounds a single Work message at the server AND on the
	// read side (CLAUDE.md: guard against unbounded sizes). 1 MiB is generous for a
	// run/resume envelope while capping a pathological payload.
	defaultWorkMaxMsgSize = 1 << 20

	// defaultAckWait bounds how long the server waits for an ack before it considers
	// a delivery failed and (subject to BackOff/MaxDeliver) redelivers it — covering
	// a crashed worker that never acks. It is generous so a slow-but-live worker is
	// not redelivered out from under.
	defaultAckWait = 30 * time.Second

	// defaultMaxDeliver caps redelivery attempts for the no-ack path so a persistently
	// failing message is eventually abandoned rather than redelivered forever (H1:
	// bounded, not infinite).
	defaultMaxDeliver = 8

	// defaultNackDelay is the redelivery delay applied to an explicit worker Nack
	// (NakWithDelay), converting redelivery from a 100%-CPU spin into a bounded rate
	// (H1). Small enough to be invisible to a healthy retry, large enough to defang a
	// transiently-failing dependency's redelivery storm.
	defaultNackDelay = 100 * time.Millisecond
)

// backoffSchedule is the capped, increasing redelivery schedule for the no-ack
// (crash) path (H1: bounded-rate, never a tight loop). With fewer entries than
// MaxDeliver, JetStream reuses the last interval for the remaining attempts.
func backoffSchedule() []time.Duration {
	return []time.Duration{
		1 * time.Second,
		5 * time.Second,
		15 * time.Second,
		30 * time.Second,
	}
}

// ControlPlaneOption configures a ControlPlane at construction (§18.5). It is the
// functional-options seam (CLAUDE.md: wire dependencies at the composition root);
// a new knob is a new option with zero edits to existing callers (open/closed).
type ControlPlaneOption func(*controlPlaneConfig)

// WithWorkStreamName overrides the JetStream work stream name (default FLOW_WORK).
// A blank/whitespace-only name is rejected by validate (fail-secure).
func WithWorkStreamName(name string) ControlPlaneOption {
	return func(c *controlPlaneConfig) { c.streamName = name }
}

// WithWorkMaxMsgSize bounds the maximum size of a single Work message at the
// server and on decode. A non-positive value leaves the default in force (the
// caller cannot accidentally disable the bound).
func WithWorkMaxMsgSize(n int32) ControlPlaneOption {
	return func(c *controlPlaneConfig) {
		if n > 0 {
			c.maxMsgSize = n
		}
	}
}

// WithAckWait overrides the server ack-wait before a no-ack delivery is retried.
// A non-positive value is ignored (default stays in force).
func WithAckWait(d time.Duration) ControlPlaneOption {
	return func(c *controlPlaneConfig) {
		if d > 0 {
			c.ackWait = d
		}
	}
}

// WithMaxDeliver caps redelivery attempts for the no-ack path. A non-positive
// value is ignored (default stays in force) so redelivery is always bounded.
func WithMaxDeliver(n int) ControlPlaneOption {
	return func(c *controlPlaneConfig) {
		if n > 0 {
			c.maxDeliver = n
		}
	}
}

// WithNackDelay sets the redelivery delay for an explicit worker Nack (H1:
// bounded-rate, not an instant spin). A non-positive value is ignored so the
// spin-guard cannot be disabled.
func WithNackDelay(d time.Duration) ControlPlaneOption {
	return func(c *controlPlaneConfig) {
		if d > 0 {
			c.nackDelay = d
		}
	}
}

// controlPlaneConfig holds resolved ControlPlane options. It is unexported:
// callers configure it only through ControlPlaneOption functions.
type controlPlaneConfig struct {
	streamName string
	maxMsgSize int32
	ackWait    time.Duration
	maxDeliver int
	nackDelay  time.Duration
}

// defaultControlPlaneConfig returns the baseline config (safe defaults).
func defaultControlPlaneConfig() controlPlaneConfig {
	return controlPlaneConfig{
		streamName: defaultWorkStreamName,
		maxMsgSize: defaultWorkMaxMsgSize,
		ackWait:    defaultAckWait,
		maxDeliver: defaultMaxDeliver,
		nackDelay:  defaultNackDelay,
	}
}

// validate rejects a config that would produce an unsafe control plane. Only the
// stream name needs validation here: the size/wait/deliver/delay options already
// clamp non-positive values to their defaults at the option seam.
func (c controlPlaneConfig) validate() error {
	if strings.TrimSpace(c.streamName) == "" {
		return &ControlPlaneConfigError{Reason: "work stream name must be non-empty"}
	}
	return nil
}

// ControlPlaneConfigError reports an invalid ControlPlane configuration, returned
// BEFORE any stream is touched so a misconfiguration fails fast (fail-secure).
type ControlPlaneConfigError struct{ Reason string }

// Error names the configuration problem.
func (e *ControlPlaneConfigError) Error() string {
	return "flow/nats: invalid control plane config: " + e.Reason
}

// SubmitError reports that submitting a Work to the work stream failed (marshal or
// publish). It wraps the underlying cause so callers can errors.As/Is it.
type SubmitError struct{ Err error }

// Error names the submit failure and its cause.
func (e *SubmitError) Error() string {
	return "flow/nats: control plane submit failed: " + e.Err.Error()
}

// Unwrap returns the underlying cause so errors.Is/As can inspect it.
func (e *SubmitError) Unwrap() error { return e.Err }

// ConsumeError reports that registering a consumer for a served key failed (e.g.
// the durable consumer could not be created or the subscription could not start).
// It wraps the underlying cause.
type ConsumeError struct{ Err error }

// Error names the consume failure and its cause.
func (e *ConsumeError) Error() string {
	return "flow/nats: control plane consume failed: " + e.Err.Error()
}

// Unwrap returns the underlying cause so errors.Is/As can inspect it.
func (e *ConsumeError) Unwrap() error { return e.Err }

// SetupError reports that the control plane could not be constructed (nil
// connection, JetStream client build failure, or work-stream ensure failure). It
// wraps the underlying cause.
type SetupError struct{ Err error }

// Error names the setup failure and its cause.
func (e *SetupError) Error() string {
	return "flow/nats: control plane setup failed: " + e.Err.Error()
}

// Unwrap returns the underlying cause so errors.Is/As can inspect it.
func (e *SetupError) Unwrap() error { return e.Err }

// ControlPlane is a JetStream-backed flow.ControlPlane (§18.5). It is safe for
// concurrent use by many goroutines and many processes: load-balancing across
// co-serving workers comes from a shared per-token durable consumer, and
// message-granularity single-flight comes from the work-queue retention — neither
// relies on in-process locking.
type ControlPlane struct {
	js         jetstream.JetStream
	streamName string
	maxMsgSize int32
	ackWait    time.Duration
	maxDeliver int
	nackDelay  time.Duration
}

// NewControlPlane builds a JetStream client over nc and ensures the work stream
// exists with WorkQueuePolicy retention, bounded size, file storage. It honors ctx
// for the stream-ensure call. A nil connection, a client build failure, a config
// error, or a stream-ensure failure is returned as a typed pkg/nats error
// (fail-secure — no half-constructed plane escapes).
//
// SHARED-DURABLE CONFIG (co-serving workers MUST use identical options). Workers
// that co-serve the same key CONVERGE on ONE shared durable consumer, named
// deterministically by the routing token (durableFor), via CreateOrUpdateConsumer.
// That call is "create-or-UPDATE": the FIRST worker creates the durable with its
// resolved AckWait/MaxDeliver/BackOff; a later worker passing DIFFERENT options
// would silently UPDATE the shared durable's config — changing redelivery behavior
// out from under every peer already attached to it. Therefore every process
// co-serving a key MUST construct its ControlPlane with IDENTICAL options
// (WithAckWait/WithMaxDeliver/WithNackDelay/WithWorkStreamName/WithWorkMaxMsgSize).
// The same caveat applies to Consume, which is where the shared durable is ensured.
func NewControlPlane(ctx context.Context, nc *nats.Conn, opts ...ControlPlaneOption) (*ControlPlane, error) {
	if nc == nil {
		return nil, &SetupError{Err: errors.New("nil nats connection")}
	}
	cfg := defaultControlPlaneConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := cfg.validate(); err != nil {
		return nil, err
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, &SetupError{Err: err}
	}

	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       cfg.streamName,
		Subjects:   []string{workStreamSubjectWildcard},
		Retention:  jetstream.WorkQueuePolicy, // each msg delivered once, removed on ack
		Storage:    jetstream.FileStorage,
		MaxMsgSize: cfg.maxMsgSize,
	})
	if err != nil {
		return nil, &SetupError{Err: err}
	}

	return &ControlPlane{
		js:         js,
		streamName: cfg.streamName,
		maxMsgSize: cfg.maxMsgSize,
		ackWait:    cfg.ackWait,
		maxDeliver: cfg.maxDeliver,
		nackDelay:  cfg.nackDelay,
	}, nil
}

// workSubjectFor maps a version key to its work subject. GraphVersion is an
// arbitrary user string, so it is NEVER placed raw in the subject (CLAUDE.md:
// sanitize before use): the subject is the work prefix plus a hex SHA-256 of
// (GraphID || NUL || GraphVersion). The NUL separator makes the hash injective in
// the pair (no (id,ver) collision via concatenation ambiguity). Hex output is a
// single safe subject token (no dots/wildcards/whitespace). The SAME function
// serves Submit and Consume, so their routing matches by construction.
func workSubjectFor(key flow.GraphVersionKey) string {
	h := sha256.New()
	id := key.GraphID
	h.Write(id[:])
	h.Write([]byte{0})
	h.Write([]byte(key.GraphVersion))
	return workSubjectPrefix + hex.EncodeToString(h.Sum(nil))
}

// durableFor maps a version key to the deterministic name of the SHARED durable
// pull consumer for that key's subject. Every worker serving the key derives the
// same name, so CreateOrUpdateConsumer attaches them all to one durable (competing
// consumers / load balancing). It reuses the routing token's hex (name-safe: no
// whitespace/'.'/'*'/'>'/slashes) under a fixed prefix.
func durableFor(key flow.GraphVersionKey) string {
	return durablePrefix + strings.TrimPrefix(workSubjectFor(key), workSubjectPrefix)
}

// Submit enqueues w on the work stream for consumers serving w.Key (§18.5). It
// marshals the Work envelope, then publishes to the version-routed subject,
// honoring ctx (the publish carries it). Marshaling happens BEFORE the network
// write so an unserializable Work fails fast. A cancelled ctx or a publish failure
// is returned as a typed error.
func (cp *ControlPlane) Submit(ctx context.Context, w flow.Work) error {
	if err := ctx.Err(); err != nil {
		return &SubmitError{Err: err}
	}
	data, err := json.Marshal(w)
	if err != nil {
		return &SubmitError{Err: err}
	}
	if _, err := cp.js.Publish(ctx, workSubjectFor(w.Key), data); err != nil {
		return &SubmitError{Err: err}
	}
	return nil
}

// Consume registers this worker for the given version keys and returns a single
// channel delivering only Work whose Key is in serves (§18.5). Registration is
// implicit: consuming IS registering. Per served key it ensures a shared durable
// pull consumer (competing consumers) and starts a local push subscription whose
// handler decodes the Work envelope and forwards a Delivery onto the returned
// channel. All keys fan in to that one channel. The channel is closed when ctx is
// done (clean shutdown, no goroutine leak); the SHARED durable consumers are NOT
// deleted (other workers/restarts depend on them) — only the local subscriptions
// are stopped. A nil/empty serves registers a worker that receives nothing (its
// channel still closes on ctx.Done). A duplicate key in serves is consumed once.
//
// SHARED-DURABLE CONFIG: this is where the per-key shared durable is ensured
// (CreateOrUpdateConsumer). All workers co-serving a key attach to ONE durable, so
// they MUST construct their ControlPlanes with IDENTICAL options — see
// NewControlPlane's "SHARED-DURABLE CONFIG" note for why differing options would
// silently re-config the shared durable for every peer.
func (cp *ControlPlane) Consume(ctx context.Context, serves []flow.GraphVersionKey) (<-chan flow.Delivery, error) {
	if err := ctx.Err(); err != nil {
		return nil, &ConsumeError{Err: err}
	}

	out := make(chan flow.Delivery)
	var sends sync.WaitGroup // in-flight handler sends; gates close(out)

	// Deduplicate served keys so we never create two subscriptions on one durable
	// from a single worker (which the work-queue would reject as overlap).
	seen := make(map[flow.GraphVersionKey]struct{}, len(serves))
	var subs []jetstream.ConsumeContext
	for _, key := range serves {
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}

		cons, err := cp.ensureConsumer(ctx, key)
		if err != nil {
			cp.stopAndWait(subs) // roll back local subscriptions started so far
			return nil, &ConsumeError{Err: err}
		}
		cctx, err := cons.Consume(cp.handler(ctx, out, &sends))
		if err != nil {
			cp.stopAndWait(subs)
			return nil, &ConsumeError{Err: err}
		}
		subs = append(subs, cctx)
	}

	// One reaper goroutine: on ctx.Done stop every local subscription (NOT the
	// shared durable) and wait for each to be FULLY torn down (its Closed channel),
	// so no pull/heartbeat goroutine outlives the subscription; then wait until
	// every in-flight handler send has unparked (the handler's ctx-Done branch wakes
	// a parked send) and close the channel so a `range` worker loop terminates. With
	// no served keys it still closes on ctx.Done — no leak.
	go func() {
		<-ctx.Done()
		cp.stopAndWait(subs)
		sends.Wait()
		close(out)
	}()

	return out, nil
}

// handler returns the per-message callback for one served key's subscription. It
// bounds the body size, decodes the UNTRUSTED bytes into a concrete flow.Work
// (never any), Term's a poison (undecodable) message so it is not redelivered in a
// spin (H1), and otherwise forwards a Delivery onto out — selecting on ctx so a
// blocked send during shutdown neither blocks nor leaks. Each invocation that may
// send registers with sends (Add before the select, Done after) so the reaper can
// Wait for all in-flight sends to unpark before closing out. This is race-free:
// the reaper Stops every subscription BEFORE it Waits, and after Stop returns no
// new handler invocation begins — so no Add races a Wait that has already seen
// zero.
func (cp *ControlPlane) handler(ctx context.Context, out chan<- flow.Delivery, sends *sync.WaitGroup) jetstream.MessageHandler {
	return func(msg jetstream.Msg) {
		w, ok := cp.decodeWork(msg)
		if !ok {
			// Poison: a body that cannot decode will NEVER decode on redelivery, so
			// Term it (drop, no redelivery) rather than Nak-loop it (H1). A Term
			// failure is non-fatal: the message stays and AckWait/MaxDeliver bound it.
			_ = msg.Term()
			return
		}
		d := flow.Delivery{
			Work: w,
			Ack:  msg.Ack,
			Nack: func() error { return msg.NakWithDelay(cp.nackDelay) },
		}
		sends.Add(1)
		defer sends.Done()
		select {
		case out <- d:
		case <-ctx.Done():
			// Shutting down: do not deliver. Leave the message un-acked so the work
			// queue redelivers it to another/this worker after AckWait (no loss).
		}
	}
}

// decodeWork bounds and decodes one message's UNTRUSTED body into a concrete
// flow.Work (never any). It returns ok=false for an over-size or undecodable body
// (a poison message). It validates only the ENVELOPE; Work.Input is a
// json.RawMessage that is neither parsed nor trusted here (pure transport).
func (cp *ControlPlane) decodeWork(msg jetstream.Msg) (flow.Work, bool) {
	data := msg.Data()
	if cp.maxMsgSize > 0 && len(data) > int(cp.maxMsgSize) {
		return flow.Work{}, false
	}
	var w flow.Work
	if err := json.Unmarshal(data, &w); err != nil {
		return flow.Work{}, false
	}
	return w, true
}

// ensureConsumer ensures the SHARED durable pull consumer for key's subject exists
// and returns a handle to it. CreateOrUpdateConsumer is idempotent for an
// identical name+config, so concurrent workers serving the same key converge on
// one durable (competing consumers). The config uses explicit ack, a bounded
// AckWait + increasing BackOff + capped MaxDeliver for the no-ack/crash path
// (H1: bounded-rate redelivery, never infinite), and a single FilterSubject (the
// work-queue's non-overlap requirement: one durable per subject).
func (cp *ControlPlane) ensureConsumer(ctx context.Context, key flow.GraphVersionKey) (jetstream.Consumer, error) {
	cons, err := cp.js.CreateOrUpdateConsumer(ctx, cp.streamName, jetstream.ConsumerConfig{
		Durable:       durableFor(key),
		FilterSubject: workSubjectFor(key),
		AckPolicy:     jetstream.AckExplicitPolicy,
		// DeliverAll is the ONLY valid policy for a WorkQueuePolicy stream's consumer:
		// the queue's contract is "deliver every enqueued message exactly once until
		// acked", so the consumer must start from the first un-acked message, never
		// skip to "new" (which would silently drop already-enqueued work). It is the
		// jetstream default today, but set it EXPLICITLY so the contract is
		// self-documenting and robust to a future default change (CLAUDE.md: fail
		// secure; don't rely on an implicit default for a correctness property).
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckWait:       cp.ackWait,
		MaxDeliver:    cp.maxDeliver,
		BackOff:       backoffSchedule(),
	})
	if err != nil {
		return nil, err
	}
	return cons, nil
}

// stopAndWait stops every local push subscription (NOT the shared durable
// consumers, which must survive for other workers/restarts) and blocks until each
// is FULLY torn down via its Closed channel — so no pull/heartbeat goroutine
// outlives the subscription (leak-free shutdown). Stop discards buffered messages
// and ends the callback loop; un-acked in-flight work is redelivered by the work
// queue, so no work is lost.
func (cp *ControlPlane) stopAndWait(subs []jetstream.ConsumeContext) {
	for _, s := range subs {
		s.Stop()
	}
	for _, s := range subs {
		<-s.Closed()
	}
}
