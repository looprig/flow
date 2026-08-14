package flowstore

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/looprig/flow/pkg/flow"
)

type checkpointEnvelope struct {
	SchemaVersion uint8            `json:"schema_version"`
	RunID         flow.GraphRunID  `json:"run_id"`
	Revision      uint64           `json:"revision"`
	Checkpoint    *flow.Checkpoint `json:"checkpoint"`
}

func encodeCheckpoint(cp *flow.Checkpoint, expectedRevision uint64) ([]byte, error) {
	if cp == nil {
		return nil, errNilCheckpoint
	}
	if cp.Run.Revision != expectedRevision {
		return nil, fmt.Errorf("flowstore: checkpoint revision %d does not match expected revision %d", cp.Run.Revision, expectedRevision)
	}
	envelope := checkpointEnvelope{
		SchemaVersion: checkpointSchemaVersion,
		RunID:         cp.Run.GraphRunID,
		Revision:      cp.Run.Revision,
		Checkpoint:    cp,
	}
	payload, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	if len(payload) > maxRecordBytes {
		return nil, fmt.Errorf("flowstore: encoded checkpoint is %d bytes, maximum is %d", len(payload), maxRecordBytes)
	}
	if err := validateJSONDepth(payload); err != nil {
		return nil, err
	}
	return payload, nil
}

func decodeCheckpoint(payload []byte, requestedID flow.GraphRunID, sequence uint64) (*flow.Checkpoint, error) {
	if len(payload) == 0 {
		return nil, errors.New("flowstore: checkpoint record is empty")
	}
	if len(payload) > maxRecordBytes {
		return nil, fmt.Errorf("flowstore: checkpoint record is %d bytes, maximum is %d", len(payload), maxRecordBytes)
	}
	if sequence == 0 {
		return nil, errInvalidSequence
	}
	if err := validateJSONDepth(payload); err != nil {
		return nil, err
	}

	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	var envelope checkpointEnvelope
	if err := decoder.Decode(&envelope); err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("flowstore: checkpoint record has trailing JSON")
		}
		return nil, err
	}

	if envelope.SchemaVersion != checkpointSchemaVersion {
		return nil, fmt.Errorf("flowstore: unsupported checkpoint schema version %d", envelope.SchemaVersion)
	}
	if envelope.Checkpoint == nil {
		return nil, errNilCheckpoint
	}
	if envelope.RunID != requestedID || envelope.Checkpoint.Run.GraphRunID != requestedID {
		return nil, fmt.Errorf("flowstore: checkpoint run ID does not match requested run")
	}
	if envelope.Revision != envelope.Checkpoint.Run.Revision {
		return nil, fmt.Errorf("flowstore: envelope revision %d does not match checkpoint revision %d", envelope.Revision, envelope.Checkpoint.Run.Revision)
	}
	if envelope.Checkpoint.Run.Revision != sequence-1 {
		return nil, fmt.Errorf("flowstore: checkpoint revision %d does not match ledger sequence %d", envelope.Checkpoint.Run.Revision, sequence)
	}
	return envelope.Checkpoint, nil
}

func validateJSONDepth(payload []byte) error {
	depth := 0
	inString := false
	escaped := false
	for _, b := range payload {
		if inString {
			if escaped {
				escaped = false
				continue
			}
			switch b {
			case '\\':
				escaped = true
			case '"':
				inString = false
			}
			continue
		}
		if b == '"' {
			inString = true
			continue
		}
		switch b {
		case '{', '[':
			depth++
			if depth > maxJSONDepth {
				return fmt.Errorf("flowstore: JSON nesting exceeds maximum depth %d", maxJSONDepth)
			}
		case '}', ']':
			depth--
			if depth < 0 {
				return errors.New("flowstore: JSON nesting closes before it opens")
			}
		}
	}
	if inString || escaped || depth != 0 {
		return errors.New("flowstore: JSON nesting is incomplete")
	}
	return nil
}
