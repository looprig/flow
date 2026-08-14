// Package flowstore adapts the neutral storage.Ledger contract to Flow's
// append-only checkpoint store.
package flowstore

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/storage"
)

const ledgerPrefix = "flow/runs/"

type checkpointStore struct {
	ledger storage.Ledger
}

var _ flow.CheckpointStore = (*checkpointStore)(nil)

// New constructs a Flow checkpoint store over exactly one neutral Ledger.
// Concrete backends such as fsstore are supplied by the caller.
func New(ledger storage.Ledger) flow.CheckpointStore {
	if ledger == nil {
		return nil
	}
	return &checkpointStore{ledger: ledger}
}

func ledgerName(id flow.GraphRunID) string {
	return ledgerPrefix + id.String()
}

func (s *checkpointStore) Append(ctx context.Context, cp *flow.Checkpoint) error {
	if err := ctx.Err(); err != nil {
		return &flow.StoreError{Op: "Append", Err: err}
	}
	if cp == nil {
		return &flow.StoreError{Op: "Append", Err: errNilCheckpoint}
	}

	id := cp.Run.GraphRunID
	revision := cp.Run.Revision
	payload, err := encodeCheckpoint(cp, revision)
	if err != nil {
		return &flow.StoreError{Op: "Append", Err: err}
	}
	name := ledgerName(id)
	tip, err := s.ledger.Tip(ctx, name)
	if err != nil {
		return &flow.StoreError{Op: "Append", Err: err}
	}
	if tip != revision {
		return &flow.RevisionConflictError{GraphRunID: id, Expected: tip, Actual: revision}
	}

	if err := storage.AppendDefinite(ctx, s.ledger, name, revision, payload); err != nil {
		var conflict *storage.ConflictError
		if errors.As(err, &conflict) {
			currentTip, tipErr := s.ledger.Tip(ctx, name)
			if tipErr != nil {
				return &flow.StoreError{Op: "Append", Err: tipErr}
			}
			return &flow.RevisionConflictError{GraphRunID: id, Expected: currentTip, Actual: revision}
		}
		return &flow.StoreError{Op: "Append", Err: err}
	}
	return nil
}

func (s *checkpointStore) Latest(ctx context.Context, id flow.GraphRunID) (*flow.Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, &flow.StoreError{Op: "Latest", Err: err}
	}
	name := ledgerName(id)
	tip, err := s.ledger.Tip(ctx, name)
	if err != nil {
		return nil, &flow.StoreError{Op: "Latest", Err: err}
	}
	if tip == 0 {
		return nil, &flow.CheckpointNotFoundError{GraphRunID: id}
	}
	cp, err := s.readOne(ctx, name, id, tip)
	if err != nil {
		return nil, &flow.StoreError{Op: "Latest", Err: err}
	}
	return cp, nil
}

func (s *checkpointStore) History(ctx context.Context, id flow.GraphRunID) ([]*flow.Checkpoint, error) {
	if err := ctx.Err(); err != nil {
		return nil, &flow.StoreError{Op: "History", Err: err}
	}
	name := ledgerName(id)
	tip, err := s.ledger.Tip(ctx, name)
	if err != nil {
		return nil, &flow.StoreError{Op: "History", Err: err}
	}
	if tip == 0 {
		return nil, &flow.CheckpointNotFoundError{GraphRunID: id}
	}
	if tip > maxHistoryRecords {
		return nil, &flow.StoreError{Op: "History", Err: fmt.Errorf("flowstore: history has %d records, maximum is %d", tip, maxHistoryRecords)}
	}

	history := make([]*flow.Checkpoint, 0, tip)
	next := uint64(1)
	for next <= tip {
		if err := ctx.Err(); err != nil {
			return nil, &flow.StoreError{Op: "History", Err: err}
		}
		cursor, err := s.ledger.Read(ctx, name, next)
		if err != nil {
			return nil, &flow.StoreError{Op: "History", Err: err}
		}
		if cursor == nil {
			return nil, &flow.StoreError{Op: "History", Err: errNilCursor}
		}

		readInPage := 0
		pageErr := error(nil)
		for readInPage < historyPageSize && next <= tip {
			record, readErr := cursor.Next(ctx)
			if errors.Is(readErr, io.EOF) {
				break
			}
			if readErr != nil {
				pageErr = readErr
				break
			}
			if record.Seq != next {
				pageErr = fmt.Errorf("flowstore: history sequence %d, want %d", record.Seq, next)
				break
			}
			cp, decodeErr := decodeCheckpoint(record.Payload, id, record.Seq)
			if decodeErr != nil {
				pageErr = decodeErr
				break
			}
			history = append(history, cp)
			next++
			readInPage++
		}
		closeErr := cursor.Close()
		if pageErr != nil {
			return nil, &flow.StoreError{Op: "History", Err: pageErr}
		}
		if closeErr != nil {
			return nil, &flow.StoreError{Op: "History", Err: closeErr}
		}
		if next <= tip && readInPage == 0 {
			return nil, &flow.StoreError{Op: "History", Err: fmt.Errorf("flowstore: history ended at sequence %d, want %d", next-1, next)}
		}
	}
	return history, nil
}

func (s *checkpointStore) readOne(ctx context.Context, name string, id flow.GraphRunID, sequence uint64) (*flow.Checkpoint, error) {
	cursor, err := s.ledger.Read(ctx, name, sequence)
	if err != nil {
		return nil, err
	}
	if cursor == nil {
		return nil, errNilCursor
	}
	record, readErr := cursor.Next(ctx)
	closeErr := cursor.Close()
	if readErr != nil {
		if errors.Is(readErr, io.EOF) {
			return nil, fmt.Errorf("flowstore: ledger ended before sequence %d", sequence)
		}
		return nil, readErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	if record.Seq != sequence {
		return nil, fmt.Errorf("flowstore: ledger sequence %d, want %d", record.Seq, sequence)
	}
	return decodeCheckpoint(record.Payload, id, record.Seq)
}
