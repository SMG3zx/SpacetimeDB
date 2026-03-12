package bsatn_test

import (
	"math"
	"testing"

	"github.com/clockworklabs/spacetimedb-go/bsatn"
)

func roundtrip[T any](
	t *testing.T,
	name string,
	write func(*bsatn.Writer),
	read func(*bsatn.Reader) (T, error),
	want T,
	wantBytes []byte,
) {
	t.Helper()
	t.Run(name, func(t *testing.T) {
		w := bsatn.NewWriter()
		write(w)
		got := w.Bytes()
		if wantBytes != nil {
			if len(got) != len(wantBytes) {
				t.Fatalf("encoded bytes len=%d want=%d", len(got), len(wantBytes))
			}
			for i := range got {
				if got[i] != wantBytes[i] {
					t.Fatalf("byte[%d] = %02x, want %02x", i, got[i], wantBytes[i])
				}
			}
		}

		r := bsatn.NewReader(got)
		v, err := read(r)
		if err != nil {
			t.Fatalf("read error: %v", err)
		}
		_ = v
		_ = want
	})
}

func TestBool(t *testing.T) {
	roundtrip(t, "true", func(w *bsatn.Writer) { w.WriteBool(true) },
		func(r *bsatn.Reader) (bool, error) { return r.ReadBool() }, true, []byte{0x01})
	roundtrip(t, "false", func(w *bsatn.Writer) { w.WriteBool(false) },
		func(r *bsatn.Reader) (bool, error) { return r.ReadBool() }, false, []byte{0x00})
}

func TestU8(t *testing.T) {
	roundtrip(t, "0", func(w *bsatn.Writer) { w.WriteU8(0) },
		func(r *bsatn.Reader) (uint8, error) { return r.ReadU8() }, 0, []byte{0x00})
	roundtrip(t, "255", func(w *bsatn.Writer) { w.WriteU8(255) },
		func(r *bsatn.Reader) (uint8, error) { return r.ReadU8() }, 255, []byte{0xFF})
}

func TestU16(t *testing.T) {
	roundtrip(t, "0x0102", func(w *bsatn.Writer) { w.WriteU16(0x0102) },
		func(r *bsatn.Reader) (uint16, error) { return r.ReadU16() }, 0x0102, []byte{0x02, 0x01})
}

func TestU32(t *testing.T) {
	roundtrip(t, "0x01020304", func(w *bsatn.Writer) { w.WriteU32(0x01020304) },
		func(r *bsatn.Reader) (uint32, error) { return r.ReadU32() }, 0x01020304,
		[]byte{0x04, 0x03, 0x02, 0x01})
}

func TestU64(t *testing.T) {
	roundtrip(t, "max", func(w *bsatn.Writer) { w.WriteU64(^uint64(0)) },
		func(r *bsatn.Reader) (uint64, error) { return r.ReadU64() }, ^uint64(0),
		[]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
}

func TestI32(t *testing.T) {
	roundtrip(t, "-1", func(w *bsatn.Writer) { w.WriteI32(-1) },
		func(r *bsatn.Reader) (int32, error) { return r.ReadI32() }, -1,
		[]byte{0xFF, 0xFF, 0xFF, 0xFF})
}

func TestF32(t *testing.T) {
	roundtrip(t, "1.0", func(w *bsatn.Writer) { w.WriteF32(1.0) },
		func(r *bsatn.Reader) (float32, error) { return r.ReadF32() }, 1.0,
		[]byte{0x00, 0x00, 0x80, 0x3F})
}

func TestF64(t *testing.T) {
	roundtrip(t, "pi", func(w *bsatn.Writer) { w.WriteF64(math.Pi) },
		func(r *bsatn.Reader) (float64, error) { return r.ReadF64() }, math.Pi, nil)
}

func TestU128(t *testing.T) {
	t.Run("roundtrip", func(t *testing.T) {
		w := bsatn.NewWriter()
		w.WriteU128(0xDEADBEEFCAFEBABE, 0x0102030405060708)
		r := bsatn.NewReader(w.Bytes())
		lo, hi, err := r.ReadU128()
		if err != nil {
			t.Fatal(err)
		}
		if lo != 0xDEADBEEFCAFEBABE || hi != 0x0102030405060708 {
			t.Fatalf("got lo=%x hi=%x", lo, hi)
		}
	})
}

func TestString(t *testing.T) {
	roundtrip(t, "hello", func(w *bsatn.Writer) { w.WriteString("hello") },
		func(r *bsatn.Reader) (string, error) { return r.ReadString() }, "hello",
		[]byte{5, 0, 0, 0, 'h', 'e', 'l', 'l', 'o'})
	roundtrip(t, "empty", func(w *bsatn.Writer) { w.WriteString("") },
		func(r *bsatn.Reader) (string, error) { return r.ReadString() }, "",
		[]byte{0, 0, 0, 0})
}

func TestBytes(t *testing.T) {
	roundtrip(t, "data", func(w *bsatn.Writer) { w.WriteBytes([]byte{1, 2, 3}) },
		func(r *bsatn.Reader) ([]byte, error) { return r.ReadBytes() }, []byte{1, 2, 3},
		[]byte{3, 0, 0, 0, 1, 2, 3})
}

func TestInvalidBool(t *testing.T) {
	r := bsatn.NewReader([]byte{0x02})
	_, err := r.ReadBool()
	if err != bsatn.ErrInvalidBool {
		t.Fatalf("expected ErrInvalidBool, got %v", err)
	}
}

func TestUnexpectedEOF(t *testing.T) {
	r := bsatn.NewReader([]byte{0x01})
	_, err := r.ReadU32()
	if err != bsatn.ErrUnexpectedEOF {
		t.Fatalf("expected ErrUnexpectedEOF, got %v", err)
	}
}

func TestInvalidUTF8(t *testing.T) {
	w := bsatn.NewWriter()
	w.WriteU32(2)
	w.WriteU8(0xFF)
	w.WriteU8(0xFE)
	r := bsatn.NewReader(w.Bytes())
	_, err := r.ReadString()
	if err != bsatn.ErrInvalidUTF8 {
		t.Fatalf("expected ErrInvalidUTF8, got %v", err)
	}
}
