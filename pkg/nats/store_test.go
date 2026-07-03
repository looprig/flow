package nats

import (
	"errors"
	"strings"
	"testing"

	"github.com/looprig/flow/pkg/flow"
	"github.com/looprig/flow/pkg/uuid"
)

// These are PURE, server-free tests of the subject-mapping and id-validation seam
// that every store method calls before any I/O. Validating the id BEFORE it is
// concatenated into a NATS subject is a security boundary (CLAUDE.md: never
// concatenate an externally-supplied identifier raw into a path/key): a zero id
// must be rejected fail-secure, and a valid id must map to exactly one
// well-formed subject token under the stream's "flow.ckpt.>" prefix.

func TestSubjectFor(t *testing.T) {
	t.Parallel()
	// A fixed, known id so we can assert the EXACT subject string.
	known := flow.GraphRunID(uuid.MustParse("11111111-2222-3333-4444-555555555555"))

	tests := []struct {
		name    string
		id      flow.GraphRunID
		want    string
		wantErr bool
	}{
		{
			name:    "valid id maps to flow.ckpt.<uuid>",
			id:      known,
			want:    "flow.ckpt.11111111-2222-3333-4444-555555555555",
			wantErr: false,
		},
		{
			name:    "zero id rejected",
			id:      flow.GraphRunID{},
			want:    "",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := subjectFor(tt.id)
			if (err != nil) != tt.wantErr {
				t.Fatalf("subjectFor() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				var se *flow.StoreError
				if !errors.As(err, &se) {
					t.Fatalf("subjectFor() error = %v, want *flow.StoreError", err)
				}
				return
			}
			if got != tt.want {
				t.Errorf("subjectFor() = %q, want %q", got, tt.want)
			}
			// The subject must be a single token under the stream prefix: exactly
			// one '.' beyond "flow.ckpt." and no wildcards.
			if !strings.HasPrefix(got, subjectPrefix) {
				t.Errorf("subjectFor() = %q, want prefix %q", got, subjectPrefix)
			}
			token := strings.TrimPrefix(got, subjectPrefix)
			if strings.ContainsAny(token, ".*> ") {
				t.Errorf("subject token %q contains an illegal subject character", token)
			}
		})
	}
}
