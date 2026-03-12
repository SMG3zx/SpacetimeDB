package types

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/clockworklabs/spacetimedb-go/bsatn"
)

// Uuid is a 128-bit UUID stored as 16 raw bytes.
type Uuid [16]byte

// String returns the UUID in standard 8-4-4-4-12 hex format.
func (u Uuid) String() string {
	h := hex.EncodeToString(u[:])
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:32])
}

// UuidFromString parses a UUID in 8-4-4-4-12 format.
func UuidFromString(s string) (Uuid, error) {
	s = strings.ReplaceAll(s, "-", "")
	if len(s) != 32 {
		return Uuid{}, errors.New("uuid: invalid length")
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return Uuid{}, fmt.Errorf("uuid: %w", err)
	}
	var u Uuid
	copy(u[:], b)
	return u, nil
}

// Bytes returns the raw 16-byte representation.
func (u Uuid) Bytes() []byte {
	b := make([]byte, 16)
	copy(b, u[:])
	return b
}

// WriteBsatn encodes the Uuid as 16 raw bytes.
func (u Uuid) WriteBsatn(w *bsatn.Writer) {
	for _, b := range u {
		w.WriteU8(b)
	}
}

// ReadUuid decodes a Uuid from 16 raw bytes.
func ReadUuid(r *bsatn.Reader) (Uuid, error) {
	var u Uuid
	for i := range u {
		b, err := r.ReadU8()
		if err != nil {
			return Uuid{}, err
		}
		u[i] = b
	}
	return u, nil
}
