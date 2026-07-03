package nats

import (
	"testing"

	"github.com/looprig/flow/pkg/flow"
)

// This file unit-tests the PURE helpers of the JetStream control plane (§18.5):
// the version-routing token function (subjectFor) and the durable-consumer-name
// function (durableFor), plus option validation. These are untagged so they run
// in the default `go test` round (no server needed) — only the behavioral suite
// in controlplane_integration_test.go boots an embedded JetStream server.

// cpGVK builds a GraphVersionKey from a single GraphID byte and a version string
// so these tests need not mint UUIDs.
func cpGVK(b byte, version string) flow.GraphVersionKey {
	var id flow.GraphID
	id[0] = b
	return flow.GraphVersionKey{GraphID: id, GraphVersion: version}
}

// TestWorkSubjectFor proves the version-routing token is a deterministic,
// subject-safe hash of (GraphID, GraphVersion): the SAME key always maps to the
// SAME subject (so Submit and Consume route to each other), DIFFERENT keys map to
// DIFFERENT subjects (so versions are isolated), the arbitrary user version string
// never appears raw in the subject (sanitize-before-use), and the result is a
// single work-subject token under the work prefix with no wildcards/whitespace.
func TestWorkSubjectFor(t *testing.T) {
	t.Parallel()

	v1 := cpGVK(1, "v1")
	v2 := cpGVK(1, "v2")
	otherGraph := cpGVK(2, "v1")
	// A hostile version containing subject metacharacters and whitespace: it must
	// NOT leak into the subject token verbatim.
	hostile := cpGVK(1, "v1 with spaces.and.dots.>*")

	tests := []struct {
		name string
		a    flow.GraphVersionKey
		b    flow.GraphVersionKey
		same bool // do a and b hash to the same subject?
	}{
		{name: "identical keys collide", a: v1, b: v1, same: true},
		{name: "different version differs", a: v1, b: v2, same: false},
		{name: "different graph differs", a: v1, b: otherGraph, same: false},
		{name: "version vs graph not confused", a: cpGVK(1, "2"), b: cpGVK(2, "1"), same: false},
		{name: "hostile version is deterministic", a: hostile, b: hostile, same: true},
		{name: "hostile differs from clean", a: hostile, b: v1, same: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			sa := workSubjectFor(tt.a)
			sb := workSubjectFor(tt.b)

			if (sa == sb) != tt.same {
				t.Errorf("workSubjectFor(%v)=%q workSubjectFor(%v)=%q; same=%v want %v",
					tt.a, sa, tt.b, sb, sa == sb, tt.same)
			}
			for _, s := range []string{sa, sb} {
				if got, want := s[:len(workSubjectPrefix)], workSubjectPrefix; got != want {
					t.Errorf("subject %q does not start with prefix %q", s, want)
				}
				assertSubjectSafe(t, s)
			}
			// The raw version string must never appear in the subject.
			if containsRawVersion(sa, tt.a.GraphVersion) {
				t.Errorf("subject %q leaks raw version %q", sa, tt.a.GraphVersion)
			}
		})
	}
}

// TestDurableFor proves the durable consumer NAME is a deterministic,
// JetStream-name-safe function of the key that MATCHES the routing token: all
// workers serving the same key derive the same durable name (so they SHARE one
// durable consumer — the competing-consumers design), and the name contains no
// characters JetStream forbids in a Durable (whitespace, '.', '*', '>', slashes).
func TestDurableFor(t *testing.T) {
	t.Parallel()

	v1 := cpGVK(1, "v1")
	v2 := cpGVK(1, "v2")

	tests := []struct {
		name string
		a    flow.GraphVersionKey
		b    flow.GraphVersionKey
		same bool
	}{
		{name: "same key same durable", a: v1, b: v1, same: true},
		{name: "different key different durable", a: v1, b: v2, same: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			da := durableFor(tt.a)
			db := durableFor(tt.b)
			if (da == db) != tt.same {
				t.Errorf("durableFor(%v)=%q durableFor(%v)=%q; same=%v want %v",
					tt.a, da, tt.b, db, da == db, tt.same)
			}
			for _, d := range []string{da, db} {
				if len(d) == 0 {
					t.Fatalf("durable name is empty")
				}
				assertNameSafe(t, d)
			}
		})
	}
}

// TestControlPlaneConfigValidation proves the constructor's option/config seam is
// validated at the boundary: a positive MaxMsgSize and stream name pass, while a
// blank stream name is rejected (fail-secure config), and non-positive option
// values are ignored so the safe defaults stay in force.
func TestControlPlaneConfigValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		opts    []ControlPlaneOption
		wantErr bool
	}{
		{name: "defaults are valid", opts: nil, wantErr: false},
		{name: "explicit positive size", opts: []ControlPlaneOption{WithWorkMaxMsgSize(4096)}, wantErr: false},
		{name: "non-positive size ignored", opts: []ControlPlaneOption{WithWorkMaxMsgSize(0)}, wantErr: false},
		{name: "custom stream name", opts: []ControlPlaneOption{WithWorkStreamName("MY_WORK")}, wantErr: false},
		{name: "blank stream name rejected", opts: []ControlPlaneOption{WithWorkStreamName("   ")}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cfg := defaultControlPlaneConfig()
			for _, opt := range tt.opts {
				opt(&cfg)
			}
			err := cfg.validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

// assertSubjectSafe fails if s contains characters unsafe in a single-token NATS
// subject segment beyond the fixed prefix (whitespace or wildcards in the token).
func assertSubjectSafe(t *testing.T, s string) {
	t.Helper()
	token := s[len(workSubjectPrefix):]
	for _, r := range token {
		if r == ' ' || r == '\t' || r == '\n' || r == '.' || r == '*' || r == '>' {
			t.Errorf("subject token %q contains unsafe rune %q", token, r)
		}
	}
}

// assertNameSafe fails if d contains characters JetStream forbids in a consumer
// Durable name.
func assertNameSafe(t *testing.T, d string) {
	t.Helper()
	for _, r := range d {
		if r == ' ' || r == '\t' || r == '\n' || r == '.' || r == '*' || r == '>' || r == '/' || r == '\\' {
			t.Errorf("durable name %q contains forbidden rune %q", d, r)
		}
	}
}

// containsRawVersion reports whether the (non-trivial) raw version string appears
// verbatim in the subject — a leak of an unsanitized user value into a subject.
func containsRawVersion(subject, version string) bool {
	if len(version) < 3 { // too short to be a meaningful leak signal
		return false
	}
	for i := 0; i+len(version) <= len(subject); i++ {
		if subject[i:i+len(version)] == version {
			return true
		}
	}
	return false
}
