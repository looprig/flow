package flow

import (
	"context"
	"encoding/json"
	"sync"
)

// This file defines the engine's durable checkpoint store contract and its
// in-memory implementation (design §10.2). A run's history is APPEND-ONLY: the
// coordinator never edits a checkpoint, only appends the next revision, so there
// are no edit conflicts. The latest revision is the source of truth; the full
// History is retained for time-travel and debugging.
//
// Append is a COMPARE-AND-APPEND keyed on (GraphRunID, Revision): a checkpoint
// is accepted iff its Revision is exactly the next in sequence (revisions are
// 0,1,2,… so the next required revision equals the current count of stored
// checkpoints for that run). This makes (GraphRunID, Revision) unique and the
// append atomic, so two concurrent resumers that both loaded the same Latest and
// both try to append the same next revision cannot fork committed history —
// exactly one wins and the other gets a RevisionConflictError (concurrent resume
// is DETECTED, not prevented; the duplicate work done before the loss is absorbed
// by IdempotencyKey(), §4.1).
//
// Immutability is ENFORCED STRUCTURALLY, not just documented: the store persists
// an INDEPENDENT copy of each checkpoint (Append serializes cp to JSON before
// storing) and returns FRESHLY DECODED values (Latest/History unmarshal a fresh
// *Checkpoint per call). So a caller can neither change stored history by
// mutating cp after Append, nor corrupt it by mutating a returned pointer. The
// JSON round-trip also surfaces a non-serializable checkpoint early, as a
// StoreError. This is a contract requirement for EVERY CheckpointStore impl, not
// a MemStore quirk.

// CheckpointStore is the engine's durable, append-only checkpoint history for
// graph runs (§10.2). Every implementation must honor the same contract:
// compare-and-append on (GraphRunID, Revision), latest-revision-as-source-of-
// truth, ordered History, structural immutability of stored checkpoints, and a
// CheckpointNotFoundError for an unknown run. Every method honors ctx.
//
// SECURITY (durable backends): a backend that decodes UNTRUSTED stored bytes on
// read (§10.4) MUST bound the payload size and nesting before decoding, to guard
// against oversized or deeply-nested input (CLAUDE.md: guard against unbounded
// sizes). MemStore is exempt only because its bytes are self-produced in-memory.
//
// CONTRACT (Latest is genuinely the latest, §10.4): Latest MUST return the
// checkpoint with the HIGHEST Run.Revision for the run. The resume contract
// (§10.4) depends on the loaded checkpoint being the true latest — Resume
// continues the append-only sequence from cp.Run.Revision, so a backend that
// returns a STALE checkpoint would fork or overwrite committed history. A durable
// backend's conformance test MUST cover this; MemStore guarantees it structurally
// (it stores the contiguous 0..len-1 revisions and returns the last).
type CheckpointStore interface {
	// Append durably records cp iff cp.Run.Revision is the next revision in
	// sequence for cp.Run.GraphRunID (compare-and-append). Otherwise it returns a
	// *RevisionConflictError. A serialization failure is a *StoreError.
	Append(ctx context.Context, cp *Checkpoint) error
	// Latest returns the highest-revision checkpoint for id (the source of truth),
	// or a *CheckpointNotFoundError if the run has no checkpoints. It MUST return
	// the checkpoint with the HIGHEST Run.Revision for the run: the §10.4 resume
	// contract depends on the loaded checkpoint being genuinely the latest, and a
	// backend that returns a stale revision violates the contract (it would fork or
	// overwrite committed history on the next append).
	Latest(ctx context.Context, id GraphRunID) (*Checkpoint, error)
	// History returns every checkpoint for id ordered by revision (0,1,2,…), or a
	// *CheckpointNotFoundError if the run has no checkpoints.
	History(ctx context.Context, id GraphRunID) ([]*Checkpoint, error)
}

// MemStore is an in-memory CheckpointStore for development and tests (§10.2). It
// holds the ENCODED bytes of each checkpoint per run (so stored history is
// independent of any caller-held *Checkpoint) under a sync.RWMutex. It is the
// reference implementation of the CheckpointStore contract; a durable backend
// (SQLite/Postgres/NATS) honors the same behavior.
type MemStore struct {
	mu   sync.RWMutex
	runs map[GraphRunID][][]byte // per run: encoded checkpoints ordered by revision
}

// NewMemStore returns an empty in-memory CheckpointStore ready for use.
func NewMemStore() *MemStore {
	return &MemStore{runs: make(map[GraphRunID][][]byte)}
}

// Append performs a compare-and-append under the write lock: it serializes cp to
// an independent JSON copy, then accepts it iff cp.Run.Revision equals the next
// required revision (the current count of stored checkpoints for the run).
// Otherwise it returns a *RevisionConflictError with Expected = next required
// revision and Actual = the supplied revision. The revision check and the append
// are a single critical section, so concurrent appenders of the same next
// revision cannot both win.
func (s *MemStore) Append(ctx context.Context, cp *Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return &StoreError{Op: "Append", Err: err}
	}
	// Serialize BEFORE taking the lock: marshaling cannot affect the map and a
	// non-serializable checkpoint must fail before any state changes (fail secure).
	encoded, err := json.Marshal(cp)
	if err != nil {
		return &StoreError{Op: "Append", Err: err}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// The count IS the next required revision: compare-and-append forbids gaps,
	// so stored revisions are always the contiguous 0..len-1 and len == latest+1.
	next := uint64(len(s.runs[cp.Run.GraphRunID]))
	if cp.Run.Revision != next {
		return &RevisionConflictError{
			GraphRunID: cp.Run.GraphRunID,
			Expected:   next,
			Actual:     cp.Run.Revision,
		}
	}
	s.runs[cp.Run.GraphRunID] = append(s.runs[cp.Run.GraphRunID], encoded)
	return nil
}

// Latest returns a freshly decoded copy of the highest-revision checkpoint for
// id under the read lock, or a *CheckpointNotFoundError if id has no checkpoints.
func (s *MemStore) Latest(ctx context.Context, id GraphRunID) (*Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, &StoreError{Op: "Latest", Err: err}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	encoded := s.runs[id]
	if len(encoded) == 0 {
		return nil, &CheckpointNotFoundError{GraphRunID: id}
	}
	cp, err := decodeCheckpoint(encoded[len(encoded)-1])
	if err != nil {
		return nil, &StoreError{Op: "Latest", Err: err}
	}
	return cp, nil
}

// History returns a fresh slice of freshly decoded checkpoints for id ordered by
// revision under the read lock, or a *CheckpointNotFoundError if id has no
// checkpoints. The returned slice and every element are independent of stored
// state.
func (s *MemStore) History(ctx context.Context, id GraphRunID) ([]*Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, &StoreError{Op: "History", Err: err}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	encoded := s.runs[id]
	if len(encoded) == 0 {
		return nil, &CheckpointNotFoundError{GraphRunID: id}
	}
	history := make([]*Checkpoint, len(encoded))
	for i, b := range encoded {
		cp, err := decodeCheckpoint(b)
		if err != nil {
			return nil, &StoreError{Op: "History", Err: err}
		}
		history[i] = cp
	}
	return history, nil
}

// decodeCheckpoint unmarshals stored bytes into a fresh *Checkpoint so callers
// can never mutate stored history through the returned pointer.
func decodeCheckpoint(b []byte) (*Checkpoint, error) {
	var cp Checkpoint
	if err := json.Unmarshal(b, &cp); err != nil {
		return nil, err
	}
	return &cp, nil
}
