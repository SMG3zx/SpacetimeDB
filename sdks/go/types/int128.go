package types

import (
	"fmt"
	"math/big"

	"github.com/clockworklabs/spacetimedb-go/bsatn"
)

// U128 is an unsigned 128-bit integer stored as (Lo, Hi) uint64 pair.
// Wire format: Lo written first (little-endian), then Hi — 16 bytes total.
type U128 struct {
	Lo uint64
	Hi uint64
}

// IsZero reports whether the value is zero.
func (u U128) IsZero() bool { return u.Lo == 0 && u.Hi == 0 }

// String returns the decimal string representation.
func (u U128) String() string {
	b := new(big.Int).SetUint64(u.Hi)
	b.Lsh(b, 64)
	b.Or(b, new(big.Int).SetUint64(u.Lo))
	return b.String()
}

// WriteBsatn encodes U128 as 16 bytes (Lo first, Hi second), little-endian.
func (u U128) WriteBsatn(w *bsatn.Writer) {
	w.WriteU128(u.Lo, u.Hi)
}

// ReadU128 decodes a U128 from 16 bytes.
func ReadU128(r *bsatn.Reader) (U128, error) {
	lo, hi, err := r.ReadU128()
	if err != nil {
		return U128{}, fmt.Errorf("u128: %w", err)
	}
	return U128{Lo: lo, Hi: hi}, nil
}

// I128 is a signed 128-bit integer stored as (Lo uint64, Hi int64).
// Wire format: Lo written first, Hi second — 16 bytes total.
type I128 struct {
	Lo uint64
	Hi int64
}

// IsZero reports whether the value is zero.
func (i I128) IsZero() bool { return i.Lo == 0 && i.Hi == 0 }

// String returns the decimal string representation.
func (i I128) String() string {
	if i.Hi >= 0 {
		b := new(big.Int).SetInt64(i.Hi)
		b.Lsh(b, 64)
		b.Or(b, new(big.Int).SetUint64(i.Lo))
		return b.String()
	}
	// Negative: reconstruct via two's complement
	b := new(big.Int).SetInt64(i.Hi)
	b.Lsh(b, 64)
	b.Or(b, new(big.Int).SetUint64(i.Lo))
	return b.String()
}

// WriteBsatn encodes I128 as 16 bytes (Lo first, Hi second), little-endian.
func (i I128) WriteBsatn(w *bsatn.Writer) {
	w.WriteI128(i.Lo, i.Hi)
}

// ReadI128 decodes an I128 from 16 bytes.
func ReadI128(r *bsatn.Reader) (I128, error) {
	lo, hi, err := r.ReadI128()
	if err != nil {
		return I128{}, fmt.Errorf("i128: %w", err)
	}
	return I128{Lo: lo, Hi: hi}, nil
}
