package flow

import (
	"crypto/sha256"
	"encoding/hex"
	"slices"
	"strconv"
	"strings"
)

// This file computes the GraphVersion compatibility fingerprint (design §8.1) that
// Compile stamps onto a Runner. GraphID is identity (a pinned const, unchanged by
// edits); GraphVersion is the fingerprint that makes resuming an old checkpoint
// against a CHANGED graph fail loudly later (Resume compares it, §10.4):
//
//	GraphVersion = sha256( canonical( sorted VertexIDs, sorted edges (from,to),
//	                                  sorted conditional (from, sorted Targets),
//	                                  sorted error-route (from, handler),
//	                                  entry, finish ) )
//	             + ":" + userVersion
//
// TOPOLOGY changes (add/remove a vertex, rewire an edge, change a Targets set,
// add/remove an error-route, relabel entry/finish) move the sha256 prefix
// automatically. BEHAVIOR changes (task/selector/reducer/Pick are closures, not
// hashable) are deliberately invisible to the hash, so a caller bumps WithVersion
// to invalidate resume — that lands in the ":userVersion" suffix.
//
// The canonical form must be DETERMINISTIC, ORDER-INDEPENDENT, and STABLE across
// process restarts (it is the resume routing key). Every list is sorted by the
// canonical UUID String() form, and each section is delimited so two different
// topologies cannot collide by section-boundary ambiguity.

// Section delimiters for the canonical buffer. Each section is on its own line,
// prefixed with a single-char tag, and entries use a tag character that never
// appears in a UUID's String() form (only [0-9a-f-]): "->" for static edges, "?"
// for conditional, "!" for error-routes. A newline separates entries and a form
// feed ('\f', not a UUID char) separates SECTIONS, so e.g. an edge list and a
// condition list can never be confused even if one is empty.
const (
	versionSectionSep = "\f"
	versionEntrySep   = "\n"
	condTargetsSep    = ","
)

// graphVersion computes the §8.1 fingerprint for a compiled graph rooted at
// entry/finish: sha256 of the canonical topology form, hex-encoded, then a
// ":userVersion" suffix. It is called by Compile after validation succeeds, so
// every referenced endpoint is already known to exist.
func graphVersion[S any](g *Graph[S], entry, finish VertexID) string {
	sum := sha256.Sum256(canonicalForm(g, entry, finish))
	return hex.EncodeToString(sum[:]) + ":" + strconv.FormatUint(g.userVersion, 10)
}

// canonicalForm renders the graph's TOPOLOGY into a single deterministic byte
// buffer: five sorted sections (vertices, static edges, conditional edges,
// error-routes, roles) joined by a non-UUID section separator. It depends only on
// ids/edges/conds/error-routes/entry/finish — never on closures — so identical
// topologies with different behavior hash identically (§8.1).
func canonicalForm[S any](g *Graph[S], entry, finish VertexID) []byte {
	sections := []string{
		"V" + versionEntrySep + join(sortedVertexIDs(g)),
		"E" + versionEntrySep + join(sortedEdges(g)),
		"C" + versionEntrySep + join(sortedConditionals(g)),
		"X" + versionEntrySep + join(sortedErrorRoutes(g)),
		"R" + versionEntrySep + entry.String() + versionEntrySep + finish.String(),
	}
	return []byte(strings.Join(sections, versionSectionSep))
}

// join concatenates already-sorted canonical entries with the entry separator.
func join(entries []string) string { return strings.Join(entries, versionEntrySep) }

// sortedVertexIDs returns every vertex id as a canonical UUID string, sorted.
func sortedVertexIDs[S any](g *Graph[S]) []string {
	out := make([]string, 0, len(g.vertices))
	for id := range g.vertices {
		out = append(out, id.String())
	}
	slices.Sort(out)
	return out
}

// sortedEdges renders every static (from,to) pair as "from->to", sorted. A
// fan-out (multiple tos per from) yields multiple entries; the sort makes the set
// order-independent.
func sortedEdges[S any](g *Graph[S]) []string {
	var out []string
	for from, tos := range g.edges {
		for _, to := range tos {
			out = append(out, from.String()+"->"+to.String())
		}
	}
	slices.Sort(out)
	return out
}

// sortedConditionals renders each conditional edge as "from?t1,t2,..." with its
// Targets sorted, then sorts the whole list. Sorting Targets makes a declared
// target set order-independent; sorting the list makes the from order independent.
func sortedConditionals[S any](g *Graph[S]) []string {
	out := make([]string, 0, len(g.conds))
	for from, cond := range g.conds {
		targets := make([]string, 0, len(cond.Targets))
		for _, t := range cond.Targets {
			targets = append(targets, t.String())
		}
		slices.Sort(targets)
		out = append(out, from.String()+"?"+strings.Join(targets, condTargetsSep))
	}
	slices.Sort(out)
	return out
}

// sortedErrorRoutes renders each vertex carrying an error-route as
// "vertex!handler", sorted. A vertex with the default Pause-on-error policy
// (errorRoute nil) contributes nothing.
func sortedErrorRoutes[S any](g *Graph[S]) []string {
	var out []string
	for id, v := range g.vertices {
		if v.config.errorRoute == nil {
			continue
		}
		out = append(out, id.String()+"!"+v.config.errorRoute.handler.String())
	}
	slices.Sort(out)
	return out
}
