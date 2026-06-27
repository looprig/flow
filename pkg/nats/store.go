package nats

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/ciram-co/flow/pkg/flow"
	"github.com/ciram-co/flow/pkg/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// This file implements a durable flow.CheckpointStore backed by a single
// JetStream stream, ONE SUBJECT PER RUN (design §10.2/§18.4). It honors the full
// CheckpointStore contract — the same one MemStore satisfies — but distributes
// across processes:
//
//   - APPEND-ONLY: the stream keeps ALL revisions of a run (MaxMsgsPerSubject -1,
//     LimitsPolicy, no MaxAge), so History/time-travel is intact.
//   - COMPARE-AND-APPEND is enforced by the SERVER, not the client: a publish
//     carries WithExpectLastSequencePerSubject(expectedSeq); if another writer has
//     advanced the subject the server rejects it with a wrong-last-sequence error
//     (code 10071). That is the authoritative gate, so two concurrent resumers of
//     the same next revision cannot BOTH win — exactly one publish lands, the other
//     gets a *flow.RevisionConflictError (concurrent-resume DETECTION, §10.2). A
//     cheap client-side pre-check on the revision short-circuits the obvious cases
//     and produces a precise Expected/Actual, but is never the sole guard.
//   - LATEST = HIGHEST REVISION is structural: GetLastMsgForSubject returns the
//     last message on the subject, which (because revisions are appended strictly
//     in order under the compare-and-append gate) is the highest revision.
//   - IMMUTABILITY: Append serializes an INDEPENDENT JSON copy before publishing
//     and reads decode fresh values, so neither the caller's *Checkpoint nor a
//     returned pointer can mutate stored history.
//
// SECURITY: the run id is VALIDATED (non-zero) before it is ever concatenated into
// a subject token (CLAUDE.md: never build a key from a raw external identifier).
// Stored bytes are UNTRUSTED on read (§10.4): MaxMsgSize bounds them at the server,
// and reads decode into the concrete flow.Checkpoint (never any) and validate the
// embedded run id matches the requested one (a mismatch ⇒ corrupt/misrouted store).

const (
	// subjectPrefix is the per-run subject namespace; each run gets exactly one
	// token (its GraphRunID) appended, e.g. "flow.ckpt.<uuid>". The trailing '>'
	// wildcard form ("flow.ckpt.>") is the stream's bound subject.
	subjectPrefix = "flow.ckpt."

	// defaultStreamName is the JetStream stream that holds every run's history.
	defaultStreamName = "FLOW_CHECKPOINTS"

	// streamSubjectWildcard is the single subject the stream binds (all runs).
	streamSubjectWildcard = subjectPrefix + ">"

	// defaultMaxMsgSize bounds a single checkpoint message (CLAUDE.md: guard
	// against unbounded sizes). 1 MiB is generous for a checkpoint while still
	// capping a pathological payload.
	defaultMaxMsgSize = 1 << 20

	// revisionHeader carries the checkpoint's revision so Append can read the prior
	// revision WITHOUT decoding the whole last message. It is advisory for the fast
	// pre-check; the server's expected-last-sequence gate is authoritative.
	revisionHeader = "Flow-Revision"

	// historyFetchBatch bounds how many messages History pulls per fetch round, so
	// a very long run is read in bounded batches honoring ctx.
	historyFetchBatch = 256

	// fetchWait bounds a single History fetch round. It is short because all
	// messages already exist on the stream — Fetch should return them promptly, not
	// wait for new ones — yet long enough to absorb consumer setup latency.
	fetchWait = 5 * time.Second

	// historyInactiveThreshold caps how long the server keeps the ephemeral
	// ordered consumer History creates if (despite explicit deletion) cleanup is
	// missed — e.g. the process dies mid-read. Without it the client defaults to 5
	// MINUTES, leaving a server-side consumer alive that long per call.
	historyInactiveThreshold = 30 * time.Second

	// consumerDeleteTimeout bounds the best-effort consumer deletion on History's
	// exit paths, so cleanup itself never blocks unboundedly.
	consumerDeleteTimeout = 5 * time.Second
)

// jsWrongLastSequence is the JetStream error code for a rejected
// expected-last-sequence publish (a concurrent writer advanced the subject).
const jsWrongLastSequence = jetstream.JSErrCodeStreamWrongLastSequence

// Store is a JetStream-backed flow.CheckpointStore. It is safe for concurrent use
// by many goroutines and many processes: correctness comes from the server's
// per-subject compare-and-append, not from in-process locking.
type Store struct {
	js         jetstream.JetStream
	stream     jetstream.Stream
	streamName string
	maxMsgSize int32 // read-side size bound (independent of the stream's own limit)
}

// corruptHistoryError reports that a run's stored history is internally
// inconsistent on read: a revision gap, a non-contiguous revision, or more
// messages on the subject than the authoritative count expected. Each is a
// distinct durability fault that callers can errors.As to distinguish from a
// transient store/transport failure (CLAUDE.md: all errors typed).
type corruptHistoryError struct{ Reason string }

// Error names the corruption detected while reading a run's history.
func (e *corruptHistoryError) Error() string {
	return "flow/nats: corrupt checkpoint history: " + e.Reason
}

// corruptHeaderError reports that a stored message's advisory Flow-Revision
// header is present but unparseable. It is typed (not a bare formatted string) so
// callers can errors.As it; it wraps the parse cause.
type corruptHeaderError struct {
	Value string
	Err   error
}

// Error names the bad header value and its parse cause.
func (e *corruptHeaderError) Error() string {
	return "flow/nats: invalid " + revisionHeader + " header " + strconv.Quote(e.Value) + ": " + e.Err.Error()
}

// Unwrap returns the parse cause so errors.Is/As can inspect it.
func (e *corruptHeaderError) Unwrap() error { return e.Err }

// storeConfig holds resolved Store options.
type storeConfig struct {
	streamName string
	maxMsgSize int32
}

func defaultStoreConfig() storeConfig {
	return storeConfig{streamName: defaultStreamName, maxMsgSize: defaultMaxMsgSize}
}

// Option configures a Store.
type Option func(*storeConfig)

// WithStreamName overrides the JetStream stream name (default FLOW_CHECKPOINTS).
func WithStreamName(name string) Option {
	return func(c *storeConfig) { c.streamName = name }
}

// WithMaxMsgSize bounds the maximum size of a single checkpoint message at the
// server (CLAUDE.md: guard against unbounded sizes). A non-positive value leaves
// the default in place.
func WithMaxMsgSize(n int32) Option {
	return func(c *storeConfig) {
		if n > 0 {
			c.maxMsgSize = n
		}
	}
}

// NewStore builds a JetStream client over nc and ensures the checkpoint stream
// exists with append-only, durable, bounded config. It honors ctx for the
// stream-ensure call. A nil connection or a stream-ensure failure is a
// *flow.StoreError.
func NewStore(ctx context.Context, nc *nats.Conn, opts ...Option) (*Store, error) {
	if nc == nil {
		return nil, &flow.StoreError{Op: "NewStore", Err: errors.New("nil nats connection")}
	}
	cfg := defaultStoreConfig()
	for _, opt := range opts {
		opt(&cfg)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, &flow.StoreError{Op: "NewStore", Err: err}
	}

	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:              cfg.streamName,
		Subjects:          []string{streamSubjectWildcard},
		Retention:         jetstream.LimitsPolicy,
		MaxMsgsPerSubject: -1, // keep ALL revisions: append-only history
		Storage:           jetstream.FileStorage,
		AllowDirect:       true, // efficient GetLastMsgForSubject
		MaxMsgSize:        cfg.maxMsgSize,
	})
	if err != nil {
		return nil, &flow.StoreError{Op: "NewStore", Err: err}
	}

	return &Store{js: js, stream: stream, streamName: cfg.streamName, maxMsgSize: cfg.maxMsgSize}, nil
}

// subjectFor maps a run id to its per-run subject, validating the id is non-zero
// BEFORE it is concatenated into the subject (fail-secure). The id's String() is a
// canonical UUID — a single safe subject token (no dots/wildcards) — so the result
// is exactly subjectPrefix + id.String().
func subjectFor(id flow.GraphRunID) (string, error) {
	if uuid.UUID(id).IsZero() {
		return "", &flow.StoreError{Op: "subject", Err: errors.New("zero GraphRunID")}
	}
	return subjectPrefix + id.String(), nil
}

// Append durably records cp iff cp.Run.Revision is the next revision in sequence
// for cp.Run.GraphRunID. The server's per-subject expected-last-sequence is the
// authoritative compare-and-append gate; a client pre-check yields precise
// Expected/Actual for the common cases. Marshaling happens BEFORE any network
// write so a non-serializable checkpoint fails fast as a *flow.StoreError.
func (s *Store) Append(ctx context.Context, cp *flow.Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return &flow.StoreError{Op: "Append", Err: err}
	}
	subj, err := subjectFor(cp.Run.GraphRunID)
	if err != nil {
		return &flow.StoreError{Op: "Append", Err: err}
	}
	// Serialize BEFORE the network write: an unserializable checkpoint must fail
	// before any state change (fail secure).
	data, err := json.Marshal(cp)
	if err != nil {
		return &flow.StoreError{Op: "Append", Err: err}
	}

	expectedRev, expectedSeq, err := s.lastRevisionAndSeq(ctx, subj)
	if err != nil {
		return &flow.StoreError{Op: "Append", Err: err}
	}
	// Header-trust is SAFE here (but NOT in History): expectedSeq is the stream's
	// authoritative last.Sequence, and the server's WithExpectLastSequencePerSubject
	// gate below — keyed on that sequence, not the revision — is the real arbiter. A
	// lying revision header could at worst skew this client-side pre-check's
	// Expected/Actual; it cannot let a wrong append commit. History, by contrast,
	// must NOT trust the header because it derives a COUNT from it.
	//
	// Fast, precise pre-check (the server sequence gate below is the real gate).
	if cp.Run.Revision != expectedRev {
		return &flow.RevisionConflictError{
			GraphRunID: cp.Run.GraphRunID,
			Expected:   expectedRev,
			Actual:     cp.Run.Revision,
		}
	}

	msg := &nats.Msg{
		Subject: subj,
		Data:    data,
		Header:  nats.Header{revisionHeader: []string{strconv.FormatUint(cp.Run.Revision, 10)}},
	}
	_, err = s.js.PublishMsg(ctx, msg, jetstream.WithExpectLastSequencePerSubject(expectedSeq))
	if err != nil {
		if isWrongLastSequence(err) {
			// A concurrent writer advanced the subject between our read and publish:
			// concurrent-resume DETECTED. Report it as a revision conflict.
			return &flow.RevisionConflictError{
				GraphRunID: cp.Run.GraphRunID,
				Expected:   expectedRev,
				Actual:     cp.Run.Revision,
			}
		}
		return &flow.StoreError{Op: "Append", Err: err}
	}
	return nil
}

// Latest returns the highest-revision checkpoint for id (the source of truth), or
// a *flow.CheckpointNotFoundError if the run has none. GetLastMsgForSubject
// returns the last message on the subject, which is the highest revision because
// revisions are appended strictly in order under the compare-and-append gate.
func (s *Store) Latest(ctx context.Context, id flow.GraphRunID) (*flow.Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, &flow.StoreError{Op: "Latest", Err: err}
	}
	subj, err := subjectFor(id)
	if err != nil {
		return nil, &flow.StoreError{Op: "Latest", Err: err}
	}
	last, err := s.stream.GetLastMsgForSubject(ctx, subj)
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return nil, &flow.CheckpointNotFoundError{GraphRunID: id}
		}
		return nil, &flow.StoreError{Op: "Latest", Err: err}
	}
	cp, err := s.decodeCheckpoint(last.Data, id)
	if err != nil {
		return nil, &flow.StoreError{Op: "Latest", Err: err}
	}
	return cp, nil
}

// History returns every checkpoint for id ordered by revision (0,1,2,…), or a
// *flow.CheckpointNotFoundError if the run has none. It reads via an ephemeral
// ordered consumer filtered to the run's subject, in bounded batches honoring ctx,
// and verifies the result is contiguous 0..N (a gap ⇒ corrupt store).
//
// AUTHORITATIVE COUNT (High-1): the expected count comes from the BODY of the last
// message (its Run.Revision), never from the advisory Flow-Revision header — a
// lying header that under-reported the revision would make History read only a
// prefix and silently return a TRUNCATED history, the worst failure for a
// durability store. After reading exactly `want` messages, History also DRAINS the
// subject to confirm none remain beyond `want`; extra messages mean the body
// under-reported and the result would have been a silent prefix — a corrupt-store
// error, not a quiet success.
func (s *Store) History(ctx context.Context, id flow.GraphRunID) ([]*flow.Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, &flow.StoreError{Op: "History", Err: err}
	}
	subj, err := subjectFor(id)
	if err != nil {
		return nil, &flow.StoreError{Op: "History", Err: err}
	}

	// Determine the expected count from the highest revision; also gives the
	// not-found short-circuit. The count is read from the BODY (decodeCheckpoint),
	// which also enforces the run-id match — never from the advisory header.
	last, err := s.stream.GetLastMsgForSubject(ctx, subj)
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return nil, &flow.CheckpointNotFoundError{GraphRunID: id}
		}
		return nil, &flow.StoreError{Op: "History", Err: err}
	}
	lastCP, err := s.decodeCheckpoint(last.Data, id)
	if err != nil {
		return nil, &flow.StoreError{Op: "History", Err: err}
	}
	want := lastCP.Run.Revision + 1

	history, err := s.fetchHistory(ctx, subj, id, want)
	if err != nil {
		return nil, err // already a typed *flow.StoreError
	}
	return history, nil
}

// fetchHistory reads exactly `want` messages from the run's subject IN ORDER via
// an ephemeral ordered consumer, decodes each into a fresh concrete checkpoint,
// validates the revisions form a contiguous 0..want-1 sequence, and finally drains
// the subject to ensure NO further messages remain (else the authoritative count
// under-reported and we would have returned a silent prefix). The ordered consumer
// is explicitly DELETED on every return path (it does NOT self-clean: a zero
// InactiveThreshold defaults to 5 minutes server-side), with a short threshold as
// a backstop if the process dies before cleanup runs. Every Fetch carries ctx, so
// a cancelled/expired deadline aborts the pull immediately, not after fetchWait.
func (s *Store) fetchHistory(ctx context.Context, subj string, id flow.GraphRunID, want uint64) ([]*flow.Checkpoint, error) {
	cons, err := s.stream.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{
		FilterSubjects:    []string{subj},
		InactiveThreshold: historyInactiveThreshold,
	})
	if err != nil {
		return nil, &flow.StoreError{Op: "History", Err: err}
	}
	// Delete the ephemeral consumer on EVERY exit path (error, success, or
	// ctx-cancel) so no named server-side consumer outlives this call. The name is
	// read inside the defer via CachedInfo so it reflects the consumer's CURRENT
	// identity even if the ordered consumer reset (and renamed) mid-read. Cleanup
	// uses a ctx that survives the caller's cancellation but is itself bounded, so
	// it still runs when the caller's deadline already fired.
	defer func() {
		info := cons.CachedInfo()
		if info == nil || info.Name == "" {
			return
		}
		dctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), consumerDeleteTimeout)
		defer cancel()
		_ = s.stream.DeleteConsumer(dctx, info.Name)
	}()

	history := make([]*flow.Checkpoint, 0, want)
	for uint64(len(history)) < want {
		if err := ctx.Err(); err != nil {
			return nil, &flow.StoreError{Op: "History", Err: err}
		}
		remaining := want - uint64(len(history))
		batchN := historyFetchBatch
		if remaining < uint64(batchN) {
			batchN = int(remaining)
		}
		got, err := s.fetchRound(ctx, cons, batchN, id, &history)
		if err != nil {
			return nil, err // already typed *flow.StoreError
		}
		if got == 0 {
			// No progress in a full batch round but we still need more: the store is
			// missing messages the authoritative count claimed (fail secure).
			return nil, &flow.StoreError{Op: "History", Err: &corruptHistoryError{Reason: fmt.Sprintf(
				"truncated: got %d of %d expected", len(history), want)}}
		}
	}

	// Drain check (High-1 hardening): `want` came from the last message's BODY; if
	// that body under-reported, MORE messages exist on the subject and `history`
	// would be a silent prefix. The SERVER's authoritative per-subject message
	// count is the ground truth — if it disagrees with what we read, the store is
	// corrupt (fail secure rather than truncate). This count is independent of both
	// the advisory header and the (possibly lying) body.
	if err := ctx.Err(); err != nil {
		return nil, &flow.StoreError{Op: "History", Err: err}
	}
	count, err := s.subjectMsgCount(ctx, subj)
	if err != nil {
		return nil, &flow.StoreError{Op: "History", Err: err}
	}
	if count != uint64(len(history)) {
		return nil, &flow.StoreError{Op: "History", Err: &corruptHistoryError{Reason: fmt.Sprintf(
			"read %d messages but subject holds %d (authoritative count mismatch)", len(history), count)}}
	}
	return history, nil
}

// subjectMsgCount returns the SERVER's authoritative count of messages on subj,
// via a subject-filtered stream-info request. It is the ground truth for the
// History drain check — independent of any message header or body.
//
// CONCURRENCY: it obtains a FRESH stream handle per call (s.js.Stream) rather than
// reusing s.stream, because jetstream's Stream.Info mutates a per-handle info
// cache and is NOT safe for concurrent use on a shared handle. A fresh handle per
// call keeps that mutation thread-local, so many parallel History calls on one
// Store stay race-free.
func (s *Store) subjectMsgCount(ctx context.Context, subj string) (uint64, error) {
	stream, err := s.js.Stream(ctx, s.streamName)
	if err != nil {
		return 0, err
	}
	info, err := stream.Info(ctx, jetstream.WithSubjectFilter(subj))
	if err != nil {
		return 0, err
	}
	// With a subject filter the server populates State.Subjects[subj] with the
	// count; an absent entry means zero messages on the subject.
	return info.State.Subjects[subj], nil
}

// fetchRound pulls up to batchN messages, decodes each into a fresh concrete
// checkpoint, verifies contiguity (revision == position), and appends them to
// *history. It returns the count delivered this round. The pull is bounded by a
// per-round child context capped at fetchWait — FetchContext threads cancellation
// in (High-2) and the cap bounds an otherwise-deadline-free caller; the cancel is
// released before returning so no timer leaks. All errors are typed *flow.StoreError.
func (s *Store) fetchRound(ctx context.Context, cons jetstream.Consumer, batchN int, id flow.GraphRunID, history *[]*flow.Checkpoint) (int, error) {
	fctx, cancel := context.WithTimeout(ctx, fetchWait)
	defer cancel()

	batch, err := cons.Fetch(batchN, jetstream.FetchContext(fctx))
	if err != nil {
		return 0, &flow.StoreError{Op: "History", Err: err}
	}
	got := 0
	for msg := range batch.Messages() {
		cp, derr := s.decodeCheckpoint(msg.Data(), id)
		if derr != nil {
			return got, &flow.StoreError{Op: "History", Err: derr}
		}
		// Contiguity: revision must equal its position (0..N).
		if cp.Run.Revision != uint64(len(*history)) {
			return got, &flow.StoreError{Op: "History", Err: &corruptHistoryError{Reason: fmt.Sprintf(
				"position %d has revision %d (expected contiguous)", len(*history), cp.Run.Revision)}}
		}
		*history = append(*history, cp)
		got++
	}
	if err := batch.Error(); err != nil {
		return got, &flow.StoreError{Op: "History", Err: err}
	}
	return got, nil
}

// lastRevisionAndSeq returns the prior revision and stream sequence for subj. When
// the subject has no messages yet it returns (0, 0): the next revision is 0 and
// the expected-last-sequence gate of 0 means "subject is empty".
//
// This serves ONLY Append's client-side pre-check; the returned expectedRev may
// derive from the advisory header (cheap), but Append's correctness rests on the
// returned expectedSeq (the authoritative stream sequence) feeding the server
// gate, not on the revision. History never uses this — it decodes the body for an
// authoritative count (see History).
func (s *Store) lastRevisionAndSeq(ctx context.Context, subj string) (expectedRev, expectedSeq uint64, err error) {
	last, err := s.stream.GetLastMsgForSubject(ctx, subj)
	if err != nil {
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return 0, 0, nil
		}
		return 0, 0, err
	}
	lastRev, err := revisionOf(last)
	if err != nil {
		return 0, 0, err
	}
	return lastRev + 1, last.Sequence, nil
}

// revisionOf reads the revision a message carries, preferring the header (cheap,
// no full decode) and falling back to decoding the body. A message with neither is
// corrupt.
func revisionOf(msg *jetstream.RawStreamMsg) (uint64, error) {
	if msg.Header != nil {
		if v := msg.Header.Get(revisionHeader); v != "" {
			rev, err := strconv.ParseUint(v, 10, 64)
			if err != nil {
				return 0, &corruptHeaderError{Value: v, Err: err}
			}
			return rev, nil
		}
	}
	var cp flow.Checkpoint
	if err := json.Unmarshal(msg.Data, &cp); err != nil {
		return 0, &flow.CheckpointDecodeError{Field: "checkpoint", Err: err}
	}
	return cp.Run.Revision, nil
}

// decodeCheckpoint unmarshals UNTRUSTED stored bytes into a FRESH concrete
// flow.Checkpoint (never any), bounding the payload size BEFORE decode (§10.4:
// guard against unbounded/oversized input independently of the stream's own limit,
// which can drift if a stream pre-existed), then validates the embedded run id
// matches the requested id — a mismatch means the store returned a misrouted or
// corrupt message. A fresh value per call also gives output-side immutability.
func (s *Store) decodeCheckpoint(data []byte, want flow.GraphRunID) (*flow.Checkpoint, error) {
	if s.maxMsgSize > 0 && len(data) > int(s.maxMsgSize) {
		return nil, &flow.CheckpointDecodeError{
			Field: "checkpoint",
			Err:   fmt.Errorf("stored checkpoint is %d bytes, exceeds limit %d", len(data), s.maxMsgSize),
		}
	}
	var cp flow.Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, &flow.CheckpointDecodeError{Field: "checkpoint", Err: err}
	}
	if cp.Run.GraphRunID != want {
		return nil, &flow.GraphRunMismatchError{Requested: want, Actual: cp.Run.GraphRunID}
	}
	return &cp, nil
}

// isWrongLastSequence reports whether err is JetStream's rejected-expected-last-
// sequence error (a concurrent writer won the compare-and-append race).
func isWrongLastSequence(err error) bool {
	var apiErr *jetstream.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode == jsWrongLastSequence
	}
	return false
}
