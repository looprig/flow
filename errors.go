package flowstore

import "errors"

const (
	checkpointSchemaVersion = 1
	maxRecordBytes          = 1 << 20
	maxJSONDepth            = 64
	historyPageSize         = 64
	maxHistoryRecords       = 100_000
)

var (
	errNilCheckpoint   = errors.New("flowstore: checkpoint is nil")
	errNilCursor       = errors.New("flowstore: ledger returned a nil cursor")
	errInvalidSequence = errors.New("flowstore: ledger sequence is invalid")
)
