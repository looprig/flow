package registry

import (
	"sort"
	"sync"

	"github.com/looprig/flow/pkg/flow"
)

// This file implements the (GraphID, GraphVersion) resolver (design §18.1): a
// PURE lookup from (GraphID, GraphVersion) to a flow.RunnerHandle. It neither
// executes the graph nor routes work across workers — routing is the control
// plane's job (§18.5), execution is the Runner's (§18.1). Keying on BOTH the
// GraphID and the GraphVersion lets ONE process serve MULTIPLE versions of a
// graph and resolve a resume to the matching one.
//
// Dependency inversion (CLAUDE.md): the registry depends on the flow.RunnerHandle
// INTERFACE, not on a concrete Runner[S]. It is therefore graph-agnostic — it
// stores handles for any S uniformly — and a new graph version requires zero edits
// here (open/closed). All access is guarded by an RWMutex so concurrent Add and
// Resolve are race-clean.

// key is the composite (GraphID, GraphVersion) map key (§18.1). Both fields are
// comparable value types (GraphID is a [16]byte array, version a string), so the
// struct is usable directly as a map key with no allocation.
type key struct {
	id      flow.GraphID
	version string
}

// Registry is the concurrency-safe, in-process resolver from (GraphID,
// GraphVersion) to a flow.RunnerHandle (§18.1). It is a pure lookup table: it does
// not execute graphs or route work. Construct one with New.
type Registry struct {
	mu      sync.RWMutex
	handles map[key]flow.RunnerHandle
}

// New returns an empty Registry ready to Add handles to.
func New() *Registry {
	return &Registry{handles: make(map[key]flow.RunnerHandle)}
}

// Add registers h under its (GraphID, GraphVersion). Keying on BOTH lets multiple
// versions of one graph coexist, so adding a different version of an already-
// registered GraphID succeeds. A duplicate — the SAME (id, version) already
// registered — is rejected with a typed *DuplicateRegistrationError (fail loudly,
// not a silent overwrite). It is concurrency-safe (write lock).
func (r *Registry) Add(h flow.RunnerHandle) error {
	k := key{id: h.GraphID(), version: h.GraphVersion()}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.handles[k]; ok {
		return &DuplicateRegistrationError{GraphID: k.id, Version: k.version}
	}
	r.handles[k] = h
	return nil
}

// Resolve returns the handle registered under the exact (id, version) and true,
// or (nil, false) if none is registered. It is an EXACT match — there is no
// fallback to another version (that policy belongs to the ingress/dispatcher,
// §18.3). It is concurrency-safe (read lock).
func (r *Registry) Resolve(id flow.GraphID, version string) (flow.RunnerHandle, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	h, ok := r.handles[key{id: id, version: version}]
	return h, ok
}

// GraphManifest is the per-GraphID advertisement of the versions this registry
// serves (§18.1): the data §18.3's GET /v1/graphs returns. Versions is sorted
// ascending for a stable, deterministic listing.
type GraphManifest struct {
	GraphID  flow.GraphID
	Versions []string
}

// Manifest returns one GraphManifest per distinct GraphID, each with its sorted
// list of served versions (§18.1, §18.3). The slice order across GraphIDs is not
// specified (a map iteration), but each entry's Versions is sorted. It is
// concurrency-safe (read lock) and returns freshly allocated slices so a caller
// cannot mutate the registry's internal state.
func (r *Registry) Manifest() []GraphManifest {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versionsByID := make(map[flow.GraphID][]string)
	for k := range r.handles {
		versionsByID[k.id] = append(versionsByID[k.id], k.version)
	}
	out := make([]GraphManifest, 0, len(versionsByID))
	for id, versions := range versionsByID {
		sort.Strings(versions)
		out = append(out, GraphManifest{GraphID: id, Versions: versions})
	}
	return out
}

// Keys returns one flow.GraphVersionKey per registered (GraphID, GraphVersion),
// in deterministic order (sorted by GraphID bytes then version), so flow.Serve can
// Consume EXACTLY the keys this registry serves (§18.5/§18.6 — registration is
// implicit via Consume). It is concurrency-safe (read lock) and returns a freshly
// allocated, non-nil slice (empty when nothing is registered) so a caller cannot
// mutate the registry's internal state. With Resolve, this satisfies flow.Resolver
// structurally — no adapter needed.
func (r *Registry) Keys() []flow.GraphVersionKey {
	r.mu.RLock()
	defer r.mu.RUnlock()

	keys := make([]flow.GraphVersionKey, 0, len(r.handles))
	for k := range r.handles {
		keys = append(keys, flow.GraphVersionKey{GraphID: k.id, GraphVersion: k.version})
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].GraphID != keys[j].GraphID {
			return keys[i].GraphID.String() < keys[j].GraphID.String()
		}
		return keys[i].GraphVersion < keys[j].GraphVersion
	})
	return keys
}

// DuplicateRegistrationError reports an Add of a (GraphID, Version) that is
// already registered (§18.1). Per CLAUDE.md every package-level failure is a
// concrete typed error so callers can errors.As to inspect it; it names both the
// GraphID and the Version so an operator can identify the offending registration
// from a log line. A different version of the same GraphID is NOT a duplicate.
type DuplicateRegistrationError struct {
	GraphID flow.GraphID
	Version string
}

// Error names the duplicated (GraphID, Version) registration.
func (e *DuplicateRegistrationError) Error() string {
	return "registry: graph " + e.GraphID.String() + " version " + e.Version + " already registered"
}
