//go:build integration

package flowstore

import (
	"context"
	"testing"

	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/fsstore"
)

func TestFSStoreCloseReopenRecoveryAndRunIsolation(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	ctx := context.Background()
	fs, err := fsstore.Open(fsstore.Options{Root: root})
	if err != nil {
		t.Fatalf("fsstore.Open: %v", err)
	}
	store := New(fs.Ledger)
	if err := store.Append(ctx, checkpoint(testRunA, 0)); err != nil {
		t.Fatalf("Append A: %v", err)
	}
	if err := store.Append(ctx, checkpoint(testRunB, 0)); err != nil {
		t.Fatalf("Append B: %v", err)
	}
	if err := fs.Close(); err != nil {
		t.Fatalf("fsstore.Close: %v", err)
	}

	fs, err = fsstore.Open(fsstore.Options{Root: root})
	if err != nil {
		t.Fatalf("reopen fsstore: %v", err)
	}
	defer fs.Close()
	store = New(fs.Ledger)
	latest, err := store.Latest(ctx, testRunA)
	if err != nil {
		t.Fatalf("Latest A after reopen: %v", err)
	}
	if latest.Run.GraphRunID != testRunA || latest.Run.Revision != 0 {
		t.Fatalf("Latest A = %+v", latest.Run)
	}
	history, err := store.History(ctx, testRunB)
	if err != nil {
		t.Fatalf("History B after reopen: %v", err)
	}
	if len(history) != 1 || history[0].Run.GraphRunID != testRunB {
		t.Fatalf("History B = %+v, want one isolated checkpoint", history)
	}
	if err := store.Append(ctx, &flow.Checkpoint{Run: flow.GraphRunState{GraphRunID: testRunA, Revision: 0}}); err == nil {
		t.Fatal("duplicate append succeeded, want revision conflict")
	}
}
