package types

import (
	"encoding/hex"
	"fmt"

	"github.com/clockworklabs/spacetimedb-go/bsatn"
)

// ConnectionId is a 128-bit identifier for a specific client connection.
type ConnectionId [16]byte

// IsZero reports whether the ConnectionId is the zero value.
func (c ConnectionId) IsZero() bool { return c == ConnectionId{} }

// Bytes returns the raw 16-byte representation.
func (c ConnectionId) Bytes() []byte {
	b := make([]byte, 16)
	copy(b, c[:])
	return b
}

// String returns the ConnectionId as a hex string.
func (c ConnectionId) String() string { return hex.EncodeToString(c[:]) }

// ConnectionIdFromBytes creates a ConnectionId from a 16-byte slice.
func ConnectionIdFromBytes(b []byte) (ConnectionId, error) {
	if len(b) != 16 {
		return ConnectionId{}, fmt.Errorf("connection_id: expected 16 bytes, got %d", len(b))
	}
	var c ConnectionId
	copy(c[:], b)
	return c, nil
}

// WriteBsatn encodes the ConnectionId as 16 raw bytes.
func (c ConnectionId) WriteBsatn(w *bsatn.Writer) {
	for _, b := range c {
		w.WriteU8(b)
	}
}

// ReadConnectionId decodes a ConnectionId from 16 raw bytes.
func ReadConnectionId(r *bsatn.Reader) (ConnectionId, error) {
	var c ConnectionId
	for i := range c {
		b, err := r.ReadU8()
		if err != nil {
			return ConnectionId{}, err
		}
		c[i] = b
	}
	return c, nil
}
