package bsatn

import (
	"encoding/binary"
	"math"
)

// Writer encodes values into BSATN binary format.
// All integers are written little-endian.
type Writer struct {
	buf []byte
}

// NewWriter returns a new Writer with an empty buffer.
func NewWriter() *Writer {
	return &Writer{}
}

// Bytes returns the encoded bytes accumulated so far.
func (w *Writer) Bytes() []byte {
	return w.buf
}

// Len returns the current number of bytes written.
func (w *Writer) Len() int {
	return len(w.buf)
}

// Reset clears the buffer so the Writer can be reused.
func (w *Writer) Reset() {
	w.buf = w.buf[:0]
}

// WriteBool writes a bool as a single byte: 0x00 = false, 0x01 = true.
func (w *Writer) WriteBool(v bool) {
	if v {
		w.buf = append(w.buf, 1)
	} else {
		w.buf = append(w.buf, 0)
	}
}

// WriteU8 writes an unsigned 8-bit integer.
func (w *Writer) WriteU8(v uint8) {
	w.buf = append(w.buf, v)
}

// WriteI8 writes a signed 8-bit integer.
func (w *Writer) WriteI8(v int8) {
	w.buf = append(w.buf, byte(v))
}

// WriteU16 writes an unsigned 16-bit integer, little-endian.
func (w *Writer) WriteU16(v uint16) {
	w.buf = binary.LittleEndian.AppendUint16(w.buf, v)
}

// WriteI16 writes a signed 16-bit integer, little-endian.
func (w *Writer) WriteI16(v int16) {
	w.buf = binary.LittleEndian.AppendUint16(w.buf, uint16(v))
}

// WriteU32 writes an unsigned 32-bit integer, little-endian.
func (w *Writer) WriteU32(v uint32) {
	w.buf = binary.LittleEndian.AppendUint32(w.buf, v)
}

// WriteI32 writes a signed 32-bit integer, little-endian.
func (w *Writer) WriteI32(v int32) {
	w.buf = binary.LittleEndian.AppendUint32(w.buf, uint32(v))
}

// WriteU64 writes an unsigned 64-bit integer, little-endian.
func (w *Writer) WriteU64(v uint64) {
	w.buf = binary.LittleEndian.AppendUint64(w.buf, v)
}

// WriteI64 writes a signed 64-bit integer, little-endian.
func (w *Writer) WriteI64(v int64) {
	w.buf = binary.LittleEndian.AppendUint64(w.buf, uint64(v))
}

// WriteF32 writes a 32-bit float as its IEEE 754 bit pattern, little-endian.
func (w *Writer) WriteF32(v float32) {
	w.buf = binary.LittleEndian.AppendUint32(w.buf, math.Float32bits(v))
}

// WriteF64 writes a 64-bit float as its IEEE 754 bit pattern, little-endian.
func (w *Writer) WriteF64(v float64) {
	w.buf = binary.LittleEndian.AppendUint64(w.buf, math.Float64bits(v))
}

// WriteU128 writes a 128-bit unsigned integer as 16 bytes little-endian.
// lo is the lower 64 bits (written first), hi is the upper 64 bits.
func (w *Writer) WriteU128(lo, hi uint64) {
	w.buf = binary.LittleEndian.AppendUint64(w.buf, lo)
	w.buf = binary.LittleEndian.AppendUint64(w.buf, hi)
}

// WriteI128 writes a 128-bit signed integer as 16 bytes little-endian.
// lo is the lower 64 bits (written first), hi is the upper 64 bits (signed).
func (w *Writer) WriteI128(lo uint64, hi int64) {
	w.buf = binary.LittleEndian.AppendUint64(w.buf, lo)
	w.buf = binary.LittleEndian.AppendUint64(w.buf, uint64(hi))
}

// WriteU256 writes a 256-bit unsigned integer as 32 bytes.
// v must be in little-endian byte order.
func (w *Writer) WriteU256(v [32]byte) {
	w.buf = append(w.buf, v[:]...)
}

// WriteI256 writes a 256-bit signed integer as 32 bytes.
// v must be in little-endian byte order.
func (w *Writer) WriteI256(v [32]byte) {
	w.buf = append(w.buf, v[:]...)
}

// WriteString writes a string as a u32 LE byte-length prefix followed by UTF-8 bytes.
func (w *Writer) WriteString(v string) {
	w.WriteU32(uint32(len(v)))
	w.buf = append(w.buf, v...)
}

// WriteBytes writes a byte slice as a u32 LE length prefix followed by raw bytes.
func (w *Writer) WriteBytes(v []byte) {
	w.WriteU32(uint32(len(v)))
	w.buf = append(w.buf, v...)
}

// WriteVariantTag writes the tag byte for a Sum type variant.
func (w *Writer) WriteVariantTag(tag uint8) {
	w.buf = append(w.buf, tag)
}

// WriteArrayLen writes the element count for an Array type.
func (w *Writer) WriteArrayLen(count uint32) {
	w.WriteU32(count)
}

// WriteRaw appends raw bytes to the buffer without any length prefix or encoding.
func (w *Writer) WriteRaw(v []byte) {
	w.buf = append(w.buf, v...)
}
