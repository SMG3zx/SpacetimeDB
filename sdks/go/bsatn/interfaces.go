package bsatn

// Serializable is implemented by types that can encode themselves to BSATN.
type Serializable interface {
	WriteBsatn(w *Writer)
}

// Codec provides paired read/write operations for a type T.
type Codec[T any] interface {
	Write(w *Writer, v T)
	Read(r *Reader) (T, error)
}

// Encode encodes a Serializable value and returns its BSATN bytes.
func Encode(v Serializable) []byte {
	w := NewWriter()
	v.WriteBsatn(w)
	return w.Bytes()
}

// Decode decodes a value from BSATN bytes using the provided read function.
func Decode[T any](data []byte, read func(*Reader) (T, error)) (T, error) {
	r := NewReader(data)
	return read(r)
}

// WriteOption encodes an optional value as a Sum type.
// Tag 0 = None (unit), tag 1 = Some(T).
func WriteOption[T any](w *Writer, v *T, writeElem func(*Writer, T)) {
	if v == nil {
		w.WriteVariantTag(0)
	} else {
		w.WriteVariantTag(1)
		writeElem(w, *v)
	}
}

// ReadOption decodes an optional value from a Sum type.
func ReadOption[T any](r *Reader, readElem func(*Reader) (T, error)) (*T, error) {
	tag, err := r.ReadVariantTag()
	if err != nil {
		return nil, err
	}
	switch tag {
	case 0:
		return nil, nil
	case 1:
		v, err := readElem(r)
		if err != nil {
			return nil, err
		}
		return &v, nil
	default:
		return nil, ErrUnexpectedEOF
	}
}

// WriteSlice encodes a slice as an Array type: u32 count followed by each element.
func WriteSlice[T any](w *Writer, s []T, writeElem func(*Writer, T)) {
	w.WriteArrayLen(uint32(len(s)))
	for _, v := range s {
		writeElem(w, v)
	}
}

// ReadSlice decodes a slice from an Array type.
func ReadSlice[T any](r *Reader, readElem func(*Reader) (T, error)) ([]T, error) {
	count, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	out := make([]T, count)
	for i := range out {
		out[i], err = readElem(r)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// WriteMap encodes a map as an array of key-value products.
func WriteMap[K comparable, V any](
	w *Writer,
	m map[K]V,
	writeKey func(*Writer, K),
	writeVal func(*Writer, V),
) {
	w.WriteArrayLen(uint32(len(m)))
	for k, v := range m {
		writeKey(w, k)
		writeVal(w, v)
	}
}

// ReadMap decodes a map from an array of key-value products.
func ReadMap[K comparable, V any](
	r *Reader,
	readKey func(*Reader) (K, error),
	readVal func(*Reader) (V, error),
) (map[K]V, error) {
	count, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	out := make(map[K]V, count)
	for i := uint32(0); i < count; i++ {
		k, err := readKey(r)
		if err != nil {
			return nil, err
		}
		v, err := readVal(r)
		if err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, nil
}
