package uuid

import (
	"encoding/hex"
	"errors"
)

// UUID is a 128-bit identifier stored as a raw 16-byte array.
type UUID [16]byte

// errInvalidText is returned when UnmarshalText receives text that is not a
// structurally valid 8-4-4-4-12 hyphenated encoding (wrong length, a hyphen off
// its fixed offset, or a non-hex digit in a hex field). A dedicated typed
// ParseError (and a public Parse) arrive in a later task; this leaf sentinel is
// sufficient until then.
var errInvalidText = errors.New("uuid: invalid text encoding")

// String returns the canonical lowercase 8-4-4-4-12 hyphenated hex encoding.
func (u UUID) String() string {
	// 32 hex chars + 4 hyphens.
	buf := make([]byte, 36)
	hex.Encode(buf[0:8], u[0:4])
	buf[8] = '-'
	hex.Encode(buf[9:13], u[4:6])
	buf[13] = '-'
	hex.Encode(buf[14:18], u[6:8])
	buf[18] = '-'
	hex.Encode(buf[19:23], u[8:10])
	buf[23] = '-'
	hex.Encode(buf[24:36], u[10:16])
	return string(buf)
}

// IsZero reports whether u is the zero UUID (absent / root).
func (u UUID) IsZero() bool { return u == UUID{} }

// MarshalText encodes u as its canonical 8-4-4-4-12 hyphenated form, so JSON
// (and any other encoding.TextMarshaler consumer) emits a readable string
// rather than a 16-int byte array.
func (u UUID) MarshalText() ([]byte, error) { return []byte(u.String()), nil }

// UnmarshalText parses the 8-4-4-4-12 hyphenated form back into the 16 bytes.
// The input must have hyphens at the fixed offsets and hex digits elsewhere;
// hex digits are case-insensitive (both 0x0a and 0x0A decode to 0x0a). The
// decoded value is normalized, so String/MarshalText always re-emit lowercase.
// Structurally invalid input returns errInvalidText and leaves the receiver
// unchanged (no partial write).
func (u *UUID) UnmarshalText(text []byte) error {
	if len(text) != 36 ||
		text[8] != '-' || text[13] != '-' || text[18] != '-' || text[23] != '-' {
		return errInvalidText
	}
	// Strip the hyphens into a contiguous 32-char hex buffer for decoding.
	var hexBuf [32]byte
	copy(hexBuf[0:8], text[0:8])
	copy(hexBuf[8:12], text[9:13])
	copy(hexBuf[12:16], text[14:18])
	copy(hexBuf[16:20], text[19:23])
	copy(hexBuf[20:32], text[24:36])
	var out UUID
	if _, err := hex.Decode(out[:], hexBuf[:]); err != nil {
		return errInvalidText
	}
	*u = out
	return nil
}
