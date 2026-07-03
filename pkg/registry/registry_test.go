package registry_test

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/flow/pkg/registry"
)

// This file black-box tests the pure (GraphID, GraphVersion) resolver (§18.1):
// Add keyed on BOTH id and version (so multiple versions of one graph coexist),
// duplicate (id+version) rejection with a typed error, exact Resolve, the sorted
// per-id Manifest for §18.3's GET /v1/graphs advertisement, and concurrency
// safety under -race. The registry NEVER executes — it is a lookup only; the
// handles it stores are real compiled Runners wrapped via flow.NewRunnerHandle.

// rstate is a minimal JSON-serializable graph state for the test Runners.
type rstate struct {
	N int
}

// vID mints a deterministic non-zero VertexID from a single byte so the test
// graphs need not depend on uuid generation.
func vID(b byte) flow.VertexID {
	var id flow.VertexID
	id[0] = b
	return id
}

// gID mints a deterministic non-zero GraphID from a single byte so the test
// graphs have a stable definition identity to key on.
func gID(b byte) flow.GraphID {
	var id flow.GraphID
	id[0] = b
	return id
}

// newHandle compiles a tiny one-vertex Runner[rstate] for graphID at userVersion
// and wraps it in a RunnerHandle. Two distinct userVersions of the same graphID
// yield handles whose GraphVersion fingerprints differ (the ":userVersion"
// suffix, §8.1), so the registry can key both under one GraphID (multi-version
// coexistence).
func newHandle(t *testing.T, graphID flow.GraphID, userVersion uint64) flow.RunnerHandle {
	t.Helper()
	entry := vID(1)
	g := flow.NewGraph[rstate](graphID, flow.WithVersion(userVersion))
	task := flow.NewFuncTask(func(_ context.Context, in int) (int, error) { return in + 1, nil })
	sel := func(s rstate) int { return s.N }
	red := func(s *rstate, out int) error { s.N = out; return nil }
	if err := flow.AddVertex(g, entry, task, sel, red); err != nil {
		t.Fatalf("AddVertex: %v", err)
	}
	r, err := g.Compile(entry, entry)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return flow.NewRunnerHandle(r)
}

// TestRegistryAddResolveMultiVersion proves Add is keyed on BOTH GraphID and
// GraphVersion so two DIFFERENT versions of the same GraphID coexist and each
// resolves to its own handle, while an unregistered (id,version) resolves to
// (nil,false).
func TestRegistryAddResolveMultiVersion(t *testing.T) {
	t.Parallel()

	id := gID(7)
	hV1 := newHandle(t, id, 1)
	hV2 := newHandle(t, id, 2)
	if hV1.GraphVersion() == hV2.GraphVersion() {
		t.Fatalf("two userVersions produced the same GraphVersion %q; test cannot prove coexistence", hV1.GraphVersion())
	}

	reg := registry.New()
	if err := reg.Add(hV1); err != nil {
		t.Fatalf("Add v1: %v", err)
	}
	if err := reg.Add(hV2); err != nil {
		t.Fatalf("Add v2: %v", err)
	}

	tests := []struct {
		name    string
		id      flow.GraphID
		version string
		want    flow.RunnerHandle
		wantOK  bool
	}{
		{name: "resolve v1", id: id, version: hV1.GraphVersion(), want: hV1, wantOK: true},
		{name: "resolve v2", id: id, version: hV2.GraphVersion(), want: hV2, wantOK: true},
		{name: "unknown version", id: id, version: "no-such-version", want: nil, wantOK: false},
		{name: "unknown graph id", id: gID(99), version: hV1.GraphVersion(), want: nil, wantOK: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, ok := reg.Resolve(tt.id, tt.version)
			if ok != tt.wantOK {
				t.Fatalf("Resolve(%v,%q) ok = %v, want %v", tt.id, tt.version, ok, tt.wantOK)
			}
			if got != tt.want {
				t.Errorf("Resolve(%v,%q) handle = %v, want %v", tt.id, tt.version, got, tt.want)
			}
		})
	}
}

// TestRegistryAddDuplicate proves a second Add of the SAME (id,version) returns a
// typed *DuplicateRegistrationError carrying the offending id and version, while
// a different version of the same id is NOT a duplicate.
func TestRegistryAddDuplicate(t *testing.T) {
	t.Parallel()

	id := gID(3)
	h := newHandle(t, id, 1)
	other := newHandle(t, id, 2)

	reg := registry.New()
	if err := reg.Add(h); err != nil {
		t.Fatalf("Add #1: %v", err)
	}

	// A different version of the same id is fine.
	if err := reg.Add(other); err != nil {
		t.Fatalf("Add different version: %v", err)
	}

	// Re-adding the same (id,version) is a typed duplicate.
	err := reg.Add(h)
	var dup *registry.DuplicateRegistrationError
	if !errors.As(err, &dup) {
		t.Fatalf("Add duplicate error = %v, want *DuplicateRegistrationError", err)
	}
	if dup.GraphID != id {
		t.Errorf("DuplicateRegistrationError.GraphID = %v, want %v", dup.GraphID, id)
	}
	if dup.Version != h.GraphVersion() {
		t.Errorf("DuplicateRegistrationError.Version = %q, want %q", dup.Version, h.GraphVersion())
	}
	// The message names both the graph and the version for an operator log line.
	if msg := dup.Error(); !strings.Contains(msg, id.String()) || !strings.Contains(msg, h.GraphVersion()) {
		t.Errorf("DuplicateRegistrationError.Error() = %q, want it to name id %v and version %q", msg, id, h.GraphVersion())
	}
}

// TestRegistryManifest proves Manifest lists each served GraphID once with its
// sorted version list — the data for §18.3's GET /v1/graphs advertisement.
func TestRegistryManifest(t *testing.T) {
	t.Parallel()

	idA := gID(1)
	idB := gID(2)
	a1 := newHandle(t, idA, 1)
	a2 := newHandle(t, idA, 2)
	b1 := newHandle(t, idB, 1)

	reg := registry.New()
	for _, h := range []flow.RunnerHandle{a2, a1, b1} { // add out of order
		if err := reg.Add(h); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	man := reg.Manifest()
	if len(man) != 2 {
		t.Fatalf("Manifest len = %d, want 2", len(man))
	}

	byID := make(map[flow.GraphID][]string, len(man))
	for _, m := range man {
		byID[m.GraphID] = m.Versions
		// each GraphID's versions must be sorted ascending
		if !sort.StringsAreSorted(m.Versions) {
			t.Errorf("Manifest[%v].Versions not sorted: %v", m.GraphID, m.Versions)
		}
	}

	wantA := []string{a1.GraphVersion(), a2.GraphVersion()}
	sort.Strings(wantA)
	gotA := byID[idA]
	if len(gotA) != 2 {
		t.Fatalf("Manifest[idA] versions = %v, want 2", gotA)
	}
	for i := range wantA {
		if gotA[i] != wantA[i] {
			t.Errorf("Manifest[idA].Versions = %v, want %v", gotA, wantA)
			break
		}
	}

	gotB := byID[idB]
	if len(gotB) != 1 || gotB[0] != b1.GraphVersion() {
		t.Errorf("Manifest[idB].Versions = %v, want [%q]", gotB, b1.GraphVersion())
	}
}

// TestRegistryKeys proves Keys flattens every registered (GraphID,GraphVersion)
// into one GraphVersionKey per registration, in deterministic order, so a worker
// can Consume exactly the keys it serves (§18.5/§18.6). An empty registry yields
// an empty (non-nil) slice.
func TestRegistryKeys(t *testing.T) {
	t.Parallel()

	idA := gID(1)
	idB := gID(2)
	a1 := newHandle(t, idA, 1)
	a2 := newHandle(t, idA, 2)
	b1 := newHandle(t, idB, 1)

	reg := registry.New()
	for _, h := range []flow.RunnerHandle{a2, b1, a1} { // add out of order
		if err := reg.Add(h); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	want := []flow.GraphVersionKey{
		{GraphID: idA, GraphVersion: a1.GraphVersion()},
		{GraphID: idA, GraphVersion: a2.GraphVersion()},
		{GraphID: idB, GraphVersion: b1.GraphVersion()},
	}
	sort.Slice(want, func(i, j int) bool { return keyLess(want[i], want[j]) })

	got := reg.Keys()
	if len(got) != len(want) {
		t.Fatalf("Keys len = %d, want %d (%v)", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("Keys[%d] = %+v, want %+v", i, got[i], want[i])
		}
	}

	// Determinism: two calls return the same order.
	again := reg.Keys()
	for i := range got {
		if got[i] != again[i] {
			t.Errorf("Keys not deterministic at %d: %+v vs %+v", i, got[i], again[i])
		}
	}

	// Empty registry: a non-nil empty slice (a worker that serves nothing).
	if empty := registry.New().Keys(); empty == nil {
		t.Error("Keys() on empty registry = nil, want non-nil empty slice")
	} else if len(empty) != 0 {
		t.Errorf("Keys() on empty registry len = %d, want 0", len(empty))
	}
}

// keyLess orders GraphVersionKeys for the deterministic-order assertion: by
// GraphID bytes, then GraphVersion.
func keyLess(a, b flow.GraphVersionKey) bool {
	if a.GraphID != b.GraphID {
		return a.GraphID.String() < b.GraphID.String()
	}
	return a.GraphVersion < b.GraphVersion
}

// TestRegistryConcurrentAddResolve exercises Add and Resolve concurrently so the
// RWMutex is proven race-clean under -race. Each goroutine registers a distinct
// (id,version) then resolves it back.
func TestRegistryConcurrentAddResolve(t *testing.T) {
	t.Parallel()

	reg := registry.New()
	const n = 32
	handles := make([]flow.RunnerHandle, n)
	for i := range handles {
		handles[i] = newHandle(t, gID(byte(i+1)), 1)
	}

	var wg sync.WaitGroup
	for i := range handles {
		wg.Add(1)
		go func(h flow.RunnerHandle) {
			defer wg.Done()
			if err := reg.Add(h); err != nil {
				t.Errorf("concurrent Add: %v", err)
				return
			}
			got, ok := reg.Resolve(h.GraphID(), h.GraphVersion())
			if !ok || got != h {
				t.Errorf("Resolve after Add: got %v ok %v, want %v true", got, ok, h)
			}
		}(handles[i])
	}
	wg.Wait()

	if got := len(reg.Manifest()); got != n {
		t.Errorf("Manifest len after concurrent Add = %d, want %d", got, n)
	}
}
