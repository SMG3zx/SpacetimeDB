package bsatn

import (
	"encoding/binary"
	"errors"
	"math"
	"unicode/utf8"
	"unsafe"
)

// Errors returned by Reader methods.
var (
	ErrUnexpectedEOF = errors.New("bsatn: unexpected end of data")
	ErrInvalidBool   = errors.New("bsatn: invalid bool byte (must be 0 or 1)")
	ErrInvalidUTF8   = errors.New("bsatn: string is not valid UTF-8")
)

// Reader decodes values from a BSATN binary buffer.
// All integers are read little-endian.
type Reader struct {
	data []byte
	pos  int
}

// NewReader returns a Reader over data.
func NewReader(data []byte) *Reader {
	return &Reader{data: data}
}

// Reset re-initializes the Reader to decode from new data without allocating.
// This allows reuse of a package-level Reader in hot loops to avoid GC pressure
// in TinyGo WASM where every heap allocation is costly.
func (r *Reader) Reset(data []byte) {
	r.data = data
	r.pos = 0
}

// Remaining returns the number of bytes not yet consumed.
func (r *Reader) Remaining() int {
	return len(r.data) - r.pos
}

// IsEmpty reports whether all bytes have been consumed.
func (r *Reader) IsEmpty() bool {
	return r.pos >= len(r.data)
}

func (r *Reader) require(n int) error {
	if r.Remaining() < n {
		return ErrUnexpectedEOF
	}
	return nil
}

// ReadBool reads a single byte and returns it as a bool.
func (r *Reader) ReadBool() (bool, error) {
	if err := r.require(1); err != nil {
		return false, err
	}
	b := r.data[r.pos]
	r.pos++
	switch b {
	case 0:
		return false, nil
	case 1:
		return true, nil
	default:
		return false, ErrInvalidBool
	}
}

// ReadU8 reads an unsigned 8-bit integer.
func (r *Reader) ReadU8() (uint8, error) {
	if err := r.require(1); err != nil {
		return 0, err
	}
	v := r.data[r.pos]
	r.pos++
	return v, nil
}

// ReadI8 reads a signed 8-bit integer.
func (r *Reader) ReadI8() (int8, error) {
	v, err := r.ReadU8()
	return int8(v), err
}

// ReadU16 reads an unsigned 16-bit integer, little-endian.
func (r *Reader) ReadU16() (uint16, error) {
	if err := r.require(2); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint16(r.data[r.pos:])
	r.pos += 2
	return v, nil
}

// ReadI16 reads a signed 16-bit integer, little-endian.
func (r *Reader) ReadI16() (int16, error) {
	v, err := r.ReadU16()
	return int16(v), err
}

// ReadU32 reads an unsigned 32-bit integer, little-endian.
func (r *Reader) ReadU32() (uint32, error) {
	if err := r.require(4); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint32(r.data[r.pos:])
	r.pos += 4
	return v, nil
}

// ReadI32 reads a signed 32-bit integer, little-endian.
func (r *Reader) ReadI32() (int32, error) {
	v, err := r.ReadU32()
	return int32(v), err
}

// ReadU64 reads an unsigned 64-bit integer, little-endian.
func (r *Reader) ReadU64() (uint64, error) {
	if err := r.require(8); err != nil {
		return 0, err
	}
	v := binary.LittleEndian.Uint64(r.data[r.pos:])
	r.pos += 8
	return v, nil
}

// ReadI64 reads a signed 64-bit integer, little-endian.
func (r *Reader) ReadI64() (int64, error) {
	v, err := r.ReadU64()
	return int64(v), err
}

// ReadF32 reads a 32-bit float from its IEEE 754 bit pattern.
func (r *Reader) ReadF32() (float32, error) {
	bits, err := r.ReadU32()
	if err != nil {
		return 0, err
	}
	return math.Float32frombits(bits), nil
}

// ReadF64 reads a 64-bit float from its IEEE 754 bit pattern.
func (r *Reader) ReadF64() (float64, error) {
	bits, err := r.ReadU64()
	if err != nil {
		return 0, err
	}
	return math.Float64frombits(bits), nil
}

// ReadU128 reads a 128-bit unsigned integer as two u64 values.
// Returns (lo, hi) where lo is the lower 64 bits.
func (r *Reader) ReadU128() (lo, hi uint64, err error) {
	if lo, err = r.ReadU64(); err != nil {
		return
	}
	hi, err = r.ReadU64()
	return
}

// ReadI128 reads a 128-bit signed integer as (lo uint64, hi int64).
func (r *Reader) ReadI128() (lo uint64, hi int64, err error) {
	if lo, err = r.ReadU64(); err != nil {
		return
	}
	var hiU uint64
	hiU, err = r.ReadU64()
	hi = int64(hiU)
	return
}

// ReadU256 reads a 256-bit unsigned integer as 32 raw bytes (little-endian).
func (r *Reader) ReadU256() ([32]byte, error) {
	if err := r.require(32); err != nil {
		return [32]byte{}, err
	}
	var v [32]byte
	copy(v[:], r.data[r.pos:r.pos+32])
	r.pos += 32
	return v, nil
}

// ReadI256 reads a 256-bit signed integer as 32 raw bytes (little-endian).
func (r *Reader) ReadI256() ([32]byte, error) {
	return r.ReadU256()
}

// ReadString reads a u32 LE length-prefixed UTF-8 string.
func (r *Reader) ReadString() (string, error) {
	length, err := r.ReadU32()
	if err != nil {
		return "", err
	}
	if err := r.require(int(length)); err != nil {
		return "", err
	}
	b := r.data[r.pos : r.pos+int(length)]
	r.pos += int(length)
	if !utf8.Valid(b) {
		return "", ErrInvalidUTF8
	}
	return string(b), nil
}

// ReadBytes reads a u32 LE length-prefixed byte slice.
func (r *Reader) ReadBytes() ([]byte, error) {
	length, err := r.ReadU32()
	if err != nil {
		return nil, err
	}
	if err := r.require(int(length)); err != nil {
		return nil, err
	}
	b := make([]byte, length)
	copy(b, r.data[r.pos:r.pos+int(length)])
	r.pos += int(length)
	return b, nil
}

// ReadVariantTag reads the tag byte of a Sum type variant.
func (r *Reader) ReadVariantTag() (uint8, error) {
	return r.ReadU8()
}

// ReadArrayLen reads the element count prefix of an Array type.
func (r *Reader) ReadArrayLen() (uint32, error) {
	return r.ReadU32()
}

// Offset returns the current read position in the data buffer.
func (r *Reader) Offset() int {
	return r.pos
}

// Skip advances past n bytes without reading them.
func (r *Reader) Skip(n int) error {
	if err := r.require(n); err != nil {
		return err
	}
	r.pos += n
	return nil
}

// SkipString advances past a u32 length-prefixed string without allocating.
func (r *Reader) SkipString() error {
	length, err := r.ReadU32()
	if err != nil {
		return err
	}
	return r.Skip(int(length))
}

// RawSlice returns the underlying data slice from start to end.
// The returned slice aliases the reader's data and must not be modified.
func (r *Reader) RawSlice(start, end int) []byte {
	return r.data[start:end]
}

// ReadStringZeroCopy reads a length-prefixed UTF-8 string without allocating.
// The returned string aliases the reader's underlying buffer and is only valid
// until the buffer is reused. Use this in hot paths where the string value is
// not stored beyond the current scope.
func (r *Reader) ReadStringZeroCopy() (string, error) {
	length, err := r.ReadU32()
	if err != nil {
		return "", err
	}
	if err := r.require(int(length)); err != nil {
		return "", err
	}
	b := r.data[r.pos : r.pos+int(length)]
	r.pos += int(length)
	if !utf8.Valid(b) {
		return "", ErrInvalidUTF8
	}
	if len(b) == 0 {
		return "", nil
	}
	return unsafe.String(&b[0], len(b)), nil
}
