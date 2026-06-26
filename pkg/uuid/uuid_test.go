package uuid

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// midValue is a fixed, deterministic non-zero UUID (bytes 1..16) with a known
// canonical string, used to exercise String/MarshalText/UnmarshalText against a
// value that is neither all-zero nor all-0xff.
var midValue = UUID{
	0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
	0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
}

const midValueStr = "01020304-0506-0708-090a-0b0c0d0e0f10"

func TestUUIDString(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		u    UUID
		want string
	}{
		{
			name: "all zero",
			u:    UUID{},
			want: "00000000-0000-0000-0000-000000000000",
		},
		{
			name: "all 0xff",
			u: UUID{
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			},
			want: "ffffffff-ffff-ffff-ffff-ffffffffffff",
		},
		{
			name: "mid value",
			u:    midValue,
			want: midValueStr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.u.String(); got != tt.want {
				t.Errorf("String() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestUUIDIsZero(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		u    UUID
		want bool
	}{
		{name: "zero value", u: UUID{}, want: true},
		{name: "non-zero first byte", u: UUID{1}, want: false},
		{name: "non-zero last byte", u: UUID{15: 1}, want: false},
		{name: "non-zero middle byte", u: UUID{8: 1}, want: false},
		{
			name: "all bytes set",
			u: UUID{
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
				0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := tt.u.IsZero(); got != tt.want {
				t.Errorf("IsZero() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestUUIDMarshalUnmarshalText(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name    string
		u       UUID
		wantStr string
	}{
		{
			name:    "zero",
			u:       UUID{},
			wantStr: "00000000-0000-0000-0000-000000000000",
		},
		{
			name:    "mid value",
			u:       midValue,
			wantStr: midValueStr,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			text, err := tt.u.MarshalText()
			if err != nil {
				t.Fatalf("MarshalText() err = %v", err)
			}
			if string(text) != tt.wantStr {
				t.Errorf("MarshalText() = %q, want %q", text, tt.wantStr)
			}
			var got UUID
			if err := got.UnmarshalText(text); err != nil {
				t.Fatalf("UnmarshalText(%q) err = %v", text, err)
			}
			if got != tt.u {
				t.Errorf("round-trip = %v, want %v", got, tt.u)
			}
		})
	}
}

// TestUUIDUnmarshalTextUppercase verifies decode is case-insensitive and
// normalized: an uppercase-hex literal decodes to the same bytes as the
// lowercase midValue (an encode-independent decode check), and String re-emits
// lowercase.
func TestUUIDUnmarshalTextUppercase(t *testing.T) {
	t.Parallel()
	const upper = "01020304-0506-0708-090A-0B0C0D0E0F10"
	var got UUID
	if err := got.UnmarshalText([]byte(upper)); err != nil {
		t.Fatalf("UnmarshalText(%q) err = %v", upper, err)
	}
	if got != midValue {
		t.Errorf("UnmarshalText(%q) = %v, want %v", upper, got, midValue)
	}
	if s := got.String(); s != midValueStr {
		t.Errorf("String() = %q, want lowercase %q", s, midValueStr)
	}
}

// TestUUIDUnmarshalTextErrors verifies that structurally invalid input returns
// errInvalidText and leaves the receiver unchanged (fail-secure: no partial
// write on failure).
func TestUUIDUnmarshalTextErrors(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		text string
	}{
		{name: "empty", text: ""},
		{name: "too short", text: "01020304-0506-0708-090a-0b0c0d0e0f1"},
		{name: "too long", text: "01020304-0506-0708-090a-0b0c0d0e0f100"},
		{name: "hyphen at wrong position", text: "0102030-40506-0708-090a-0b0c0d0e0f10"},
		{name: "non-hex digit in hex field", text: "0102030g-0506-0708-090a-0b0c0d0e0f10"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			// Start from a known non-zero value; failure must not mutate it.
			u := midValue
			err := u.UnmarshalText([]byte(tt.text))
			if err == nil {
				t.Fatalf("UnmarshalText(%q) err = nil, want error", tt.text)
			}
			if !errors.Is(err, errInvalidText) {
				t.Errorf("UnmarshalText(%q) err = %v, want errInvalidText", tt.text, err)
			}
			if u != midValue {
				t.Errorf("receiver mutated on failure = %v, want unchanged %v", u, midValue)
			}
		})
	}
}

// TestNew verifies the structural invariants of a crypto/rand v4 UUID. The
// payload is random, so we assert only RFC-4122 invariants and uniqueness, never
// specific byte values: version nibble 4, variant bits 10, non-zero, and that
// successive calls differ.
func TestNew(t *testing.T) {
	t.Parallel()
	const iterations = 100
	prev, err := New()
	if err != nil {
		t.Fatalf("New() err = %v", err)
	}
	for i := 0; i < iterations; i++ {
		u, err := New()
		if err != nil {
			t.Fatalf("New() iteration %d err = %v", i, err)
		}
		if got := u[6] >> 4; got != 0x4 {
			t.Errorf("iteration %d: version nibble = %#x, want 0x4", i, got)
		}
		if got := u[8] & 0xc0; got != 0x80 {
			t.Errorf("iteration %d: variant bits = %#x, want 0x80", i, got)
		}
		if u.IsZero() {
			t.Errorf("iteration %d: New() returned zero UUID", i)
		}
		if u == prev {
			t.Errorf("iteration %d: New() returned duplicate of previous %v", i, u)
		}
		prev = u
	}
}

// errReader is a reader that always fails, used to drive the GenerateError path.
type errReader struct{ err error }

func (r errReader) Read(_ []byte) (int, error) { return 0, r.err }

// TestNewGenerateError exercises the read-failure path via the newFromReader
// seam. Both an explicit reader error and a short read (fewer than 16 bytes,
// surfaced by io.ReadFull as io.ErrUnexpectedEOF) must yield a *GenerateError
// that unwraps to the underlying reader error.
func TestNewGenerateError(t *testing.T) {
	t.Parallel()
	sentinel := errors.New("boom")
	tests := []struct {
		name    string
		reader  io.Reader
		wantErr error
	}{
		{
			name:    "explicit reader error",
			reader:  errReader{err: sentinel},
			wantErr: sentinel,
		},
		{
			name:    "short read fewer than 16 bytes",
			reader:  bytes.NewReader([]byte{0x00, 0x01, 0x02}),
			wantErr: io.ErrUnexpectedEOF,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := newFromReader(tt.reader)
			if err == nil {
				t.Fatalf("newFromReader() err = nil, want error")
			}
			if !got.IsZero() {
				t.Errorf("newFromReader() = %v on error, want zero UUID", got)
			}
			var genErr *GenerateError
			if !errors.As(err, &genErr) {
				t.Fatalf("newFromReader() err = %v, want *GenerateError", err)
			}
			if !errors.Is(genErr.Err, tt.wantErr) {
				t.Errorf("GenerateError.Err = %v, want %v", genErr.Err, tt.wantErr)
			}
			if !errors.Is(errors.Unwrap(err), tt.wantErr) {
				t.Errorf("errors.Unwrap(err) = %v, want %v", errors.Unwrap(err), tt.wantErr)
			}
			if want := "uuid: generate: " + tt.wantErr.Error(); err.Error() != want {
				t.Errorf("Error() = %q, want %q", err.Error(), want)
			}
		})
	}
}
