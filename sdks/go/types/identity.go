package types

import (
	"encoding/hex"
	"fmt"

	"github.com/clockworklabs/spacetimedb-go/bsatn"
)

// Identity is a 256-bit unique identifier for a SpacetimeDB user.
// Wire format: 32 raw bytes little-endian (u256).
type Identity [32]byte

// IsZero reports whether the Identity is the zero value.
func (id Identity) IsZero() bool { return id == Identity{} }

// Bytes returns the raw 32-byte representation.
func (id Identity) Bytes() []byte {
	b := make([]byte, 32)
	copy(b, id[:])
	return b
}

// String returns the identity as a hex string.
func (id Identity) String() string { return hex.EncodeToString(id[:]) }

// IdentityFromBytes creates an Identity from a 32-byte slice.
func IdentityFromBytes(b []byte) (Identity, error) {
	if len(b) != 32 {
		return Identity{}, fmt.Errorf("identity: expected 32 bytes, got %d", len(b))
	}
	var id Identity
	copy(id[:], b)
	return id, nil
}

// WriteBsatn encodes the Identity as 32 raw bytes.
func (id Identity) WriteBsatn(w *bsatn.Writer) {
	for _, b := range id {
		w.WriteU8(b)
	}
}

// ReadIdentity decodes an Identity from 32 raw bytes.
func ReadIdentity(r *bsatn.Reader) (Identity, error) {
	var id Identity
	for i := range id {
		b, err := r.ReadU8()
		if err != nil {
			return Identity{}, err
		}
		id[i] = b
	}
	return id, nil
}
