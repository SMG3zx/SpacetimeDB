package types

import (
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/clockworklabs/spacetimedb-go/bsatn"
)

// U256 is an unsigned 256-bit integer stored as 32 raw bytes, little-endian.
type U256 [32]byte

// IsZero reports whether the value is zero.
func (u U256) IsZero() bool { return u == U256{} }

// String returns the hex representation.
func (u U256) String() string { return "0x" + hex.EncodeToString(u[:]) }

// ToBigInt converts to a *big.Int.
func (u U256) ToBigInt() *big.Int {
	// bytes are LE, big.Int expects BE
	be := make([]byte, 32)
	for i := range be {
		be[i] = u[31-i]
	}
	return new(big.Int).SetBytes(be)
}

// WriteBsatn encodes U256 as 32 raw bytes.
func (u U256) WriteBsatn(w *bsatn.Writer) {
	w.WriteU256(u)
}

// ReadU256 decodes a U256 from 32 bytes.
func ReadU256(r *bsatn.Reader) (U256, error) {
	raw, err := r.ReadU256()
	if err != nil {
		return U256{}, fmt.Errorf("u256: %w", err)
	}
	return U256(raw), nil
}

// I256 is a signed 256-bit integer stored as 32 raw bytes, little-endian.
type I256 [32]byte

// IsZero reports whether the value is zero.
func (i I256) IsZero() bool { return i == I256{} }

// String returns the hex representation.
func (i I256) String() string { return "0x" + hex.EncodeToString(i[:]) }

// WriteBsatn encodes I256 as 32 raw bytes.
func (i I256) WriteBsatn(w *bsatn.Writer) {
	w.WriteI256([32]byte(i))
}

// ReadI256 decodes an I256 from 32 bytes.
func ReadI256(r *bsatn.Reader) (I256, error) {
	raw, err := r.ReadI256()
	if err != nil {
		return I256{}, fmt.Errorf("i256: %w", err)
	}
	return I256(raw), nil
}
