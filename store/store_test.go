package flowstore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"

	"github.com/looprig/core/uuid"
	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/storage"
	"github.com/looprig/storage/memstore"
)

var (
	testRunA = flow.GraphRunID(uuid.MustParse("11111111-1111-4111-8111-111111111111"))
	testRunB = flow.GraphRunID(uuid.MustParse("22222222-2222-4222-8222-222222222222"))
)

func checkpoint(id flow.GraphRunID, revision uint64) *flow.Checkpoint {
	return &flow.Checkpoint{Run: flow.GraphRunState{GraphRunID: id, Revision: revision}}
}

func TestAppendRevisionZeroUsesSequenceOne(t *testing.T) {
	t.Parallel()

	ledger := memstore.New().Ledger
	store := New(ledger)
	if err := store.Append(context.Background(), checkpoint(testRunA, 0)); err != nil {
		t.Fatalf("Append: %v", err)
	}
	tip, err := ledger.Tip(context.Background(), ledgerName(testRunA))
	if err != nil {
		t.Fatalf("Tip: %v", err)
	}
	if tip != 1 {
		t.Fatalf("Tip = %d, want 1", tip)
	}
	latest, err := store.Latest(context.Background(), testRunA)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if latest.Run.Revision != 0 {
		t.Fatalf("Latest revision = %d, want 0", latest.Run.Revision)
	}
}

func TestAppendRevisionNUsesExpectedTipAndLandsAtNPlusOne(t *testing.T) {
	t.Parallel()

	ledger := memstore.New().Ledger
	store := New(ledger)
	for revision := uint64(0); revision < 3; revision++ {
		if err := store.Append(context.Background(), checkpoint(testRunA, revision)); err != nil {
			t.Fatalf("Append revision %d: %v", revision, err)
		}
	}
	tip, err := ledger.Tip(context.Background(), ledgerName(testRunA))
	if err != nil {
		t.Fatalf("Tip: %v", err)
	}
	if tip != 3 {
		t.Fatalf("Tip = %d, want 3", tip)
	}
	latest, err := store.Latest(context.Background(), testRunA)
	if err != nil {
		t.Fatalf("Latest: %v", err)
	}
	if latest.Run.Revision != 2 {
		t.Fatalf("Latest revision = %d, want 2", latest.Run.Revision)
	}
}

func TestLatestEmptyReturnsCheckpointNotFound(t *testing.T) {
	t.Parallel()

	_, err := New(memstore.New().Ledger).Latest(context.Background(), testRunA)
	var notFound *flow.CheckpointNotFoundError
	if !errors.As(err, &notFound) {
		t.Fatalf("Latest error = %v, want *flow.CheckpointNotFoundError", err)
	}
}

func TestLatestRejectsRevisionThatDoesNotMatchLedgerSequence(t *testing.T) {
	t.Parallel()

	ledger := memstore.New().Ledger
	payload := envelopeBytes(t, testRunA, 1, checkpoint(testRunA, 1))
	if err := ledger.Append(context.Background(), ledgerName(testRunA), 0, payload); err != nil {
		t.Fatalf("seed ledger: %v", err)
	}
	_, err := New(ledger).Latest(context.Background(), testRunA)
	var storeErr *flow.StoreError
	if !errors.As(err, &storeErr) {
		t.Fatalf("Latest error = %v, want *flow.StoreError", err)
	}
}

func TestHistoryReturnsSequenceOrderBeginningAtOne(t *testing.T) {
	t.Parallel()

	store := New(memstore.New().Ledger)
	for revision := uint64(0); revision < 70; revision++ {
		if err := store.Append(context.Background(), checkpoint(testRunA, revision)); err != nil {
			t.Fatalf("Append revision %d: %v", revision, err)
		}
	}
	history, err := store.History(context.Background(), testRunA)
	if err != nil {
		t.Fatalf("History: %v", err)
	}
	if len(history) != 70 {
		t.Fatalf("History length = %d, want 70", len(history))
	}
	for i, cp := range history {
		if cp.Run.Revision != uint64(i) {
			t.Fatalf("History[%d] revision = %d, want %d", i, cp.Run.Revision, i)
		}
	}
}

func TestLedgerConflictBecomesRevisionConflict(t *testing.T) {
	t.Parallel()

	store := New(memstore.New().Ledger)
	if err := store.Append(context.Background(), checkpoint(testRunA, 0)); err != nil {
		t.Fatalf("first Append: %v", err)
	}
	err := store.Append(context.Background(), checkpoint(testRunA, 0))
	var conflict *flow.RevisionConflictError
	if !errors.As(err, &conflict) {
		t.Fatalf("second Append error = %v, want *flow.RevisionConflictError", err)
	}
	if conflict.Expected != 1 || conflict.Actual != 0 {
		t.Fatalf("conflict = %+v, want expected=1 actual=0", conflict)
	}
}

func TestConstructorAuthorityIsStorageLedger(t *testing.T) {
	t.Parallel()

	var ledger storage.Ledger = memstore.New().Ledger
	var _ flow.CheckpointStore = New(ledger)
}

func TestDifferentRunIDsUseIsolatedCanonicalLedgers(t *testing.T) {
	t.Parallel()

	recording := &recordingLedger{Ledger: memstore.New().Ledger}
	store := New(recording)
	if err := store.Append(context.Background(), checkpoint(testRunA, 0)); err != nil {
		t.Fatalf("Append A: %v", err)
	}
	if err := store.Append(context.Background(), checkpoint(testRunB, 0)); err != nil {
		t.Fatalf("Append B: %v", err)
	}
	want := map[string]bool{ledgerName(testRunA): true, ledgerName(testRunB): true}
	if len(recording.names) != 2 {
		t.Fatalf("ledger names = %v, want two names", recording.names)
	}
	for name := range recording.names {
		if !want[name] {
			t.Fatalf("unexpected ledger name %q", name)
		}
	}
}

func TestUnresolvedStorageErrorsBecomeStoreError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
	}{
		{name: "ambiguous", err: &storage.AmbiguousError{Name: ledgerName(testRunA), Expected: 0}},
		{name: "append verify", err: &storage.AppendVerifyError{Name: ledgerName(testRunA), Seq: 1, Cause: errors.New("read failed")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := New(&faultLedger{appendErr: tt.err})
			err := store.Append(context.Background(), checkpoint(testRunA, 0))
			var storeErr *flow.StoreError
			if !errors.As(err, &storeErr) {
				t.Fatalf("Append error = %v, want *flow.StoreError", err)
			}
			if !errors.Is(err, tt.err) {
				t.Fatalf("Append error = %v, want wrapped %T", err, tt.err)
			}
		})
	}
}

func TestReadFailureBecomesStoreError(t *testing.T) {
	t.Parallel()

	errRead := errors.New("read failed")
	store := New(&faultLedger{tip: 1, readErr: errRead})
	_, err := store.Latest(context.Background(), testRunA)
	var storeErr *flow.StoreError
	if !errors.As(err, &storeErr) || !errors.Is(err, errRead) {
		t.Fatalf("Latest error = %v, want wrapped read *flow.StoreError", err)
	}
}

func TestMalformedUnknownAndOversizedRecordsBecomeStoreError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		payload []byte
	}{
		{name: "malformed", payload: []byte("{")},
		{name: "unknown field", payload: append(envelopeBytes(t, testRunA, 0, checkpoint(testRunA, 0)), []byte(" ")...)},
		{name: "oversized", payload: bytes.Repeat([]byte("x"), maxRecordBytes+1)},
	}
	tests[1].payload = bytes.Replace(tests[1].payload, []byte("}"), []byte(",\"extra\":true}"), 1)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ledger := memstore.New().Ledger
			if err := ledger.Append(context.Background(), ledgerName(testRunA), 0, tt.payload); err != nil {
				t.Fatalf("seed ledger: %v", err)
			}
			_, err := New(ledger).Latest(context.Background(), testRunA)
			var storeErr *flow.StoreError
			if !errors.As(err, &storeErr) {
				t.Fatalf("Latest error = %v, want *flow.StoreError", err)
			}
		})
	}
}

func envelopeBytes(t *testing.T, id flow.GraphRunID, revision uint64, cp *flow.Checkpoint) []byte {
	t.Helper()
	b, err := json.Marshal(struct {
		SchemaVersion uint8            `json:"schema_version"`
		RunID         flow.GraphRunID  `json:"run_id"`
		Revision      uint64           `json:"revision"`
		Checkpoint    *flow.Checkpoint `json:"checkpoint"`
	}{1, id, revision, cp})
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return b
}

type recordingLedger struct {
	storage.Ledger
	names map[string]bool
}

func (l *recordingLedger) Append(ctx context.Context, name string, expected uint64, payload []byte) error {
	if l.names == nil {
		l.names = make(map[string]bool)
	}
	l.names[name] = true
	return l.Ledger.Append(ctx, name, expected, payload)
}

type faultLedger struct {
	appendErr error
	readErr   error
	tip       uint64
}

func (l *faultLedger) Append(context.Context, string, uint64, []byte) error { return l.appendErr }
func (l *faultLedger) Read(context.Context, string, uint64) (storage.Cursor, error) {
	return nil, l.readErr
}
func (l *faultLedger) Tip(context.Context, string) (uint64, error) { return l.tip, nil }
func (l *faultLedger) Delete(context.Context, string) error        { return nil }

var _ storage.Ledger = (*faultLedger)(nil)
var _ storage.Ledger = (*recordingLedger)(nil)
var _ = io.EOF
