package types

import (
	"fmt"

	"github.com/clockworklabs/spacetimedb-go/bsatn"
)

// AlgebraicType represents a SATS type at runtime.
// It is a sealed interface; the concrete variants are defined below.
type AlgebraicType interface {
	algebraicTypeTag() uint8
}

// --- Composite types ---

// SumType is a tagged union / discriminated union type.
type SumType struct {
	Variants []SumTypeVariant
}

func (SumType) algebraicTypeTag() uint8 { return 1 }

// SumTypeVariant is one variant of a SumType.
type SumTypeVariant struct {
	Name *string // optional
	Type AlgebraicType
}

// ProductType is a record / struct type.
type ProductType struct {
	Elements []ProductTypeElement
}

func (ProductType) algebraicTypeTag() uint8 { return 2 }

// ProductTypeElement is one field of a ProductType.
type ProductTypeElement struct {
	Name *string // optional
	Type AlgebraicType
}

// ArrayType is a homogeneous array type.
type ArrayType struct {
	ElemType AlgebraicType
}

func (ArrayType) algebraicTypeTag() uint8 { return 3 }

// RefType is a reference into a Typespace by index.
type RefType struct {
	Ref uint32
}

func (RefType) algebraicTypeTag() uint8 { return 0 }

// --- Primitive singleton types ---

type (
	StringAlgebraicType struct{}
	BoolAlgebraicType   struct{}
	I8AlgebraicType     struct{}
	U8AlgebraicType     struct{}
	I16AlgebraicType    struct{}
	U16AlgebraicType    struct{}
	I32AlgebraicType    struct{}
	U32AlgebraicType    struct{}
	I64AlgebraicType    struct{}
	U64AlgebraicType    struct{}
	I128AlgebraicType   struct{}
	U128AlgebraicType   struct{}
	I256AlgebraicType   struct{}
	U256AlgebraicType   struct{}
	F32AlgebraicType    struct{}
	F64AlgebraicType    struct{}
)

func (StringAlgebraicType) algebraicTypeTag() uint8 { return 4 }
func (BoolAlgebraicType) algebraicTypeTag() uint8   { return 5 }
func (I8AlgebraicType) algebraicTypeTag() uint8     { return 6 }
func (U8AlgebraicType) algebraicTypeTag() uint8     { return 7 }
func (I16AlgebraicType) algebraicTypeTag() uint8    { return 8 }
func (U16AlgebraicType) algebraicTypeTag() uint8    { return 9 }
func (I32AlgebraicType) algebraicTypeTag() uint8    { return 10 }
func (U32AlgebraicType) algebraicTypeTag() uint8    { return 11 }
func (I64AlgebraicType) algebraicTypeTag() uint8    { return 12 }
func (U64AlgebraicType) algebraicTypeTag() uint8    { return 13 }
func (I128AlgebraicType) algebraicTypeTag() uint8   { return 14 }
func (U128AlgebraicType) algebraicTypeTag() uint8   { return 15 }
func (I256AlgebraicType) algebraicTypeTag() uint8   { return 16 }
func (U256AlgebraicType) algebraicTypeTag() uint8   { return 17 }
func (F32AlgebraicType) algebraicTypeTag() uint8    { return 18 }
func (F64AlgebraicType) algebraicTypeTag() uint8    { return 19 }

// Convenient singletons.
var (
	AlgebraicString = StringAlgebraicType{}
	AlgebraicBool   = BoolAlgebraicType{}
	AlgebraicI8     = I8AlgebraicType{}
	AlgebraicU8     = U8AlgebraicType{}
	AlgebraicI16    = I16AlgebraicType{}
	AlgebraicU16    = U16AlgebraicType{}
	AlgebraicI32    = I32AlgebraicType{}
	AlgebraicU32    = U32AlgebraicType{}
	AlgebraicI64    = I64AlgebraicType{}
	AlgebraicU64    = U64AlgebraicType{}
	AlgebraicI128   = I128AlgebraicType{}
	AlgebraicU128   = U128AlgebraicType{}
	AlgebraicI256   = I256AlgebraicType{}
	AlgebraicU256   = U256AlgebraicType{}
	AlgebraicF32    = F32AlgebraicType{}
	AlgebraicF64    = F64AlgebraicType{}
)

// Special SpacetimeDB types recognized by the host.
// These are product/sum types with specific field names that give them semantic meaning.
var (
	// AlgebraicTimestamp is the AlgebraicType for a point in time (i64 microseconds since Unix epoch).
	AlgebraicTimestamp = ProductType{
		Elements: []ProductTypeElement{
			{Name: strPtr("__timestamp_micros_since_unix_epoch__"), Type: AlgebraicI64},
		},
	}

	// AlgebraicTimeDuration is the AlgebraicType for a time span (i64 microseconds).
	AlgebraicTimeDuration = ProductType{
		Elements: []ProductTypeElement{
			{Name: strPtr("__time_duration_micros__"), Type: AlgebraicI64},
		},
	}

	// AlgebraicScheduleAt is the AlgebraicType for a scheduled reducer's trigger time.
	// It is a sum of Interval (TimeDuration) and Time (Timestamp).
	AlgebraicScheduleAt = SumType{
		Variants: []SumTypeVariant{
			{Name: strPtr("Interval"), Type: AlgebraicTimeDuration},
			{Name: strPtr("Time"), Type: AlgebraicTimestamp},
		},
	}

	// AlgebraicBytes is the AlgebraicType for a byte array (Array<U8>).
	AlgebraicBytes = ArrayType{ElemType: AlgebraicU8}
)

func strPtr(s string) *string { return &s }

// --- Typespace ---

// Typespace holds a list of named algebraic types, indexed by AlgebraicTypeRef.
type Typespace struct {
	Types []AlgebraicType
}

// Resolve returns the AlgebraicType at the given ref index, or nil if out of bounds.
func (ts *Typespace) Resolve(ref uint32) AlgebraicType {
	if int(ref) >= len(ts.Types) {
		return nil
	}
	return ts.Types[ref]
}

// --- BSATN encode/decode for AlgebraicType ---

// WriteAlgebraicType encodes an AlgebraicType into a BSATN writer.
func WriteAlgebraicType(w *bsatn.Writer, t AlgebraicType) {
	w.WriteVariantTag(t.algebraicTypeTag())
	switch v := t.(type) {
	case RefType:
		w.WriteU32(v.Ref)
	case SumType:
		writeSumType(w, v)
	case ProductType:
		writeProductType(w, v)
	case ArrayType:
		WriteAlgebraicType(w, v.ElemType)
	// Primitives have no payload.
	}
}

func writeSumType(w *bsatn.Writer, s SumType) {
	w.WriteArrayLen(uint32(len(s.Variants)))
	for _, v := range s.Variants {
		writeOptionalString(w, v.Name)
		WriteAlgebraicType(w, v.Type)
	}
}

func writeProductType(w *bsatn.Writer, p ProductType) {
	w.WriteArrayLen(uint32(len(p.Elements)))
	for _, e := range p.Elements {
		writeOptionalString(w, e.Name)
		WriteAlgebraicType(w, e.Type)
	}
}

func writeOptionalString(w *bsatn.Writer, s *string) {
	if s == nil {
		w.WriteVariantTag(1) // None
	} else {
		w.WriteVariantTag(0) // Some
		w.WriteString(*s)
	}
}

// ReadAlgebraicType decodes an AlgebraicType from a BSATN reader.
func ReadAlgebraicType(r *bsatn.Reader) (AlgebraicType, error) {
	tag, err := r.ReadVariantTag()
	if err != nil {
		return nil, err
	}
	switch tag {
	case 0: // Ref
		ref, err := r.ReadU32()
		if err != nil {
			return nil, err
		}
		return RefType{Ref: ref}, nil
	case 1: // Sum
		s, err := readSumType(r)
		if err != nil {
			return nil, err
		}
		return s, nil
	case 2: // Product
		p, err := readProductType(r)
		if err != nil {
			return nil, err
		}
		return p, nil
	case 3: // Array
		elem, err := ReadAlgebraicType(r)
		if err != nil {
			return nil, err
		}
		return ArrayType{ElemType: elem}, nil
	case 4:
		return AlgebraicString, nil
	case 5:
		return AlgebraicBool, nil
	case 6:
		return AlgebraicI8, nil
	case 7:
		return AlgebraicU8, nil
	case 8:
		return AlgebraicI16, nil
	case 9:
		return AlgebraicU16, nil
	case 10:
		return AlgebraicI32, nil
	case 11:
		return AlgebraicU32, nil
	case 12:
		return AlgebraicI64, nil
	case 13:
		return AlgebraicU64, nil
	case 14:
		return AlgebraicI128, nil
	case 15:
		return AlgebraicU128, nil
	case 16:
		return AlgebraicI256, nil
	case 17:
		return AlgebraicU256, nil
	case 18:
		return AlgebraicF32, nil
	case 19:
		return AlgebraicF64, nil
	default:
		return nil, fmt.Errorf("types: unknown AlgebraicType tag %d", tag)
	}
}

func readSumType(r *bsatn.Reader) (SumType, error) {
	count, err := r.ReadArrayLen()
	if err != nil {
		return SumType{}, err
	}
	variants := make([]SumTypeVariant, count)
	for i := range variants {
		name, err := readOptionalString(r)
		if err != nil {
			return SumType{}, err
		}
		t, err := ReadAlgebraicType(r)
		if err != nil {
			return SumType{}, err
		}
		variants[i] = SumTypeVariant{Name: name, Type: t}
	}
	return SumType{Variants: variants}, nil
}

func readProductType(r *bsatn.Reader) (ProductType, error) {
	count, err := r.ReadArrayLen()
	if err != nil {
		return ProductType{}, err
	}
	elements := make([]ProductTypeElement, count)
	for i := range elements {
		name, err := readOptionalString(r)
		if err != nil {
			return ProductType{}, err
		}
		t, err := ReadAlgebraicType(r)
		if err != nil {
			return ProductType{}, err
		}
		elements[i] = ProductTypeElement{Name: name, Type: t}
	}
	return ProductType{Elements: elements}, nil
}

func readOptionalString(r *bsatn.Reader) (*string, error) {
	tag, err := r.ReadU8()
	if err != nil {
		return nil, err
	}
	if tag == 1 {
		return nil, nil // None
	}
	// tag == 0 means Some
	s, err := r.ReadString()
	if err != nil {
		return nil, err
	}
	return &s, nil
}
