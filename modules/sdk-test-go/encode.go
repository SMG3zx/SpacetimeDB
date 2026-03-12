package main

import (
	"fmt"

	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/types"
)

// ── SimpleEnum ───────────────────────────────────────────────────────────────

func encodeSimpleEnum(w *bsatn.Writer, v SimpleEnum) { w.WriteVariantTag(uint8(v)) }

func decodeSimpleEnum(r *bsatn.Reader) (SimpleEnum, error) {
	t, err := r.ReadVariantTag()
	return SimpleEnum(t), err
}

// ── EnumWithPayload ──────────────────────────────────────────────────────────

func encodeEnumWithPayload(w *bsatn.Writer, v EnumWithPayload) {
	w.WriteVariantTag(v.Tag)
	switch v.Tag {
	case EWPTagU8:
		w.WriteU8(v.U8Val)
	case EWPTagU16:
		w.WriteU16(v.U16Val)
	case EWPTagU32:
		w.WriteU32(v.U32Val)
	case EWPTagU64:
		w.WriteU64(v.U64Val)
	case EWPTagU128:
		v.U128Val.WriteBsatn(w)
	case EWPTagU256:
		v.U256Val.WriteBsatn(w)
	case EWPTagI8:
		w.WriteI8(v.I8Val)
	case EWPTagI16:
		w.WriteI16(v.I16Val)
	case EWPTagI32:
		w.WriteI32(v.I32Val)
	case EWPTagI64:
		w.WriteI64(v.I64Val)
	case EWPTagI128:
		v.I128Val.WriteBsatn(w)
	case EWPTagI256:
		v.I256Val.WriteBsatn(w)
	case EWPTagBool:
		w.WriteBool(v.BoolVal)
	case EWPTagF32:
		w.WriteF32(v.F32Val)
	case EWPTagF64:
		w.WriteF64(v.F64Val)
	case EWPTagStr:
		w.WriteString(v.StrVal)
	case EWPTagIdentity:
		v.IdVal.WriteBsatn(w)
	case EWPTagConnectionId:
		v.ConnVal.WriteBsatn(w)
	case EWPTagTimestamp:
		v.TsVal.WriteBsatn(w)
	case EWPTagUuid:
		v.UuidVal.WriteBsatn(w)
	case EWPTagBytes:
		w.WriteArrayLen(uint32(len(v.BytesVal)))
		for _, b := range v.BytesVal {
			w.WriteU8(b)
		}
	case EWPTagInts:
		w.WriteArrayLen(uint32(len(v.IntsVal)))
		for _, i := range v.IntsVal {
			w.WriteI32(i)
		}
	case EWPTagStrings:
		w.WriteArrayLen(uint32(len(v.StringsVal)))
		for _, s := range v.StringsVal {
			w.WriteString(s)
		}
	case EWPTagSimpleEnums:
		w.WriteArrayLen(uint32(len(v.SimpleEnumsVal)))
		for _, e := range v.SimpleEnumsVal {
			encodeSimpleEnum(w, e)
		}
	}
}

func decodeEnumWithPayload(r *bsatn.Reader) (EnumWithPayload, error) {
	tag, err := r.ReadVariantTag()
	if err != nil {
		return EnumWithPayload{}, err
	}
	v := EnumWithPayload{Tag: tag}
	switch tag {
	case EWPTagU8:
		v.U8Val, err = r.ReadU8()
	case EWPTagU16:
		v.U16Val, err = r.ReadU16()
	case EWPTagU32:
		v.U32Val, err = r.ReadU32()
	case EWPTagU64:
		v.U64Val, err = r.ReadU64()
	case EWPTagU128:
		v.U128Val, err = types.ReadU128(r)
	case EWPTagU256:
		v.U256Val, err = types.ReadU256(r)
	case EWPTagI8:
		v.I8Val, err = r.ReadI8()
	case EWPTagI16:
		v.I16Val, err = r.ReadI16()
	case EWPTagI32:
		v.I32Val, err = r.ReadI32()
	case EWPTagI64:
		v.I64Val, err = r.ReadI64()
	case EWPTagI128:
		v.I128Val, err = types.ReadI128(r)
	case EWPTagI256:
		v.I256Val, err = types.ReadI256(r)
	case EWPTagBool:
		v.BoolVal, err = r.ReadBool()
	case EWPTagF32:
		v.F32Val, err = r.ReadF32()
	case EWPTagF64:
		v.F64Val, err = r.ReadF64()
	case EWPTagStr:
		v.StrVal, err = r.ReadString()
	case EWPTagIdentity:
		v.IdVal, err = types.ReadIdentity(r)
	case EWPTagConnectionId:
		v.ConnVal, err = types.ReadConnectionId(r)
	case EWPTagTimestamp:
		v.TsVal, err = types.ReadTimestamp(r)
	case EWPTagUuid:
		v.UuidVal, err = types.ReadUuid(r)
	case EWPTagBytes:
		n, e := r.ReadArrayLen()
		if e != nil {
			return EnumWithPayload{}, e
		}
		v.BytesVal = make([]byte, n)
		for i := range v.BytesVal {
			v.BytesVal[i], err = r.ReadU8()
			if err != nil {
				return EnumWithPayload{}, err
			}
		}
	case EWPTagInts:
		n, e := r.ReadArrayLen()
		if e != nil {
			return EnumWithPayload{}, e
		}
		v.IntsVal = make([]int32, n)
		for i := range v.IntsVal {
			v.IntsVal[i], err = r.ReadI32()
			if err != nil {
				return EnumWithPayload{}, err
			}
		}
	case EWPTagStrings:
		n, e := r.ReadArrayLen()
		if e != nil {
			return EnumWithPayload{}, e
		}
		v.StringsVal = make([]string, n)
		for i := range v.StringsVal {
			v.StringsVal[i], err = r.ReadString()
			if err != nil {
				return EnumWithPayload{}, err
			}
		}
	case EWPTagSimpleEnums:
		n, e := r.ReadArrayLen()
		if e != nil {
			return EnumWithPayload{}, e
		}
		v.SimpleEnumsVal = make([]SimpleEnum, n)
		for i := range v.SimpleEnumsVal {
			v.SimpleEnumsVal[i], err = decodeSimpleEnum(r)
			if err != nil {
				return EnumWithPayload{}, err
			}
		}
	default:
		return EnumWithPayload{}, fmt.Errorf("unknown EnumWithPayload tag %d", tag)
	}
	if err != nil {
		return EnumWithPayload{}, err
	}
	return v, nil
}

// ── UnitStruct ───────────────────────────────────────────────────────────────

func encodeUnitStruct(_ *bsatn.Writer, _ UnitStruct) {}
func decodeUnitStruct(_ *bsatn.Reader) (UnitStruct, error) { return UnitStruct{}, nil }

// ── ByteStruct ───────────────────────────────────────────────────────────────

func encodeByteStruct(w *bsatn.Writer, v ByteStruct) { w.WriteU8(v.B) }
func decodeByteStruct(r *bsatn.Reader) (ByteStruct, error) {
	b, err := r.ReadU8()
	return ByteStruct{B: b}, err
}

// ── EveryPrimitiveStruct ─────────────────────────────────────────────────────

func encodeEveryPrimitiveStruct(w *bsatn.Writer, v EveryPrimitiveStruct) {
	w.WriteU8(v.A)
	w.WriteU16(v.B)
	w.WriteU32(v.C)
	w.WriteU64(v.D)
	v.E.WriteBsatn(w)
	v.F.WriteBsatn(w)
	w.WriteI8(v.G)
	w.WriteI16(v.H)
	w.WriteI32(v.I)
	w.WriteI64(v.J)
	v.K.WriteBsatn(w)
	v.L.WriteBsatn(w)
	w.WriteBool(v.M)
	w.WriteF32(v.N)
	w.WriteF64(v.O)
	w.WriteString(v.P)
	v.Q.WriteBsatn(w)
	v.R.WriteBsatn(w)
	v.S.WriteBsatn(w)
	v.T.WriteBsatn(w)
	v.U.WriteBsatn(w)
}

func decodeEveryPrimitiveStruct(r *bsatn.Reader) (EveryPrimitiveStruct, error) {
	var v EveryPrimitiveStruct
	var err error
	v.A, err = r.ReadU8()
	if err != nil {
		return v, err
	}
	v.B, err = r.ReadU16()
	if err != nil {
		return v, err
	}
	v.C, err = r.ReadU32()
	if err != nil {
		return v, err
	}
	v.D, err = r.ReadU64()
	if err != nil {
		return v, err
	}
	v.E, err = types.ReadU128(r)
	if err != nil {
		return v, err
	}
	v.F, err = types.ReadU256(r)
	if err != nil {
		return v, err
	}
	v.G, err = r.ReadI8()
	if err != nil {
		return v, err
	}
	v.H, err = r.ReadI16()
	if err != nil {
		return v, err
	}
	v.I, err = r.ReadI32()
	if err != nil {
		return v, err
	}
	v.J, err = r.ReadI64()
	if err != nil {
		return v, err
	}
	v.K, err = types.ReadI128(r)
	if err != nil {
		return v, err
	}
	v.L, err = types.ReadI256(r)
	if err != nil {
		return v, err
	}
	v.M, err = r.ReadBool()
	if err != nil {
		return v, err
	}
	v.N, err = r.ReadF32()
	if err != nil {
		return v, err
	}
	v.O, err = r.ReadF64()
	if err != nil {
		return v, err
	}
	v.P, err = r.ReadString()
	if err != nil {
		return v, err
	}
	v.Q, err = types.ReadIdentity(r)
	if err != nil {
		return v, err
	}
	v.R, err = types.ReadConnectionId(r)
	if err != nil {
		return v, err
	}
	v.S, err = types.ReadTimestamp(r)
	if err != nil {
		return v, err
	}
	v.T, err = types.ReadTimeDuration(r)
	if err != nil {
		return v, err
	}
	v.U, err = types.ReadUuid(r)
	return v, err
}

// ── EveryVecStruct ───────────────────────────────────────────────────────────

func encodeEveryVecStruct(w *bsatn.Writer, v EveryVecStruct) {
	writeVecU8(w, v.A)
	writeVecU16(w, v.B)
	writeVecU32(w, v.C)
	writeVecU64(w, v.D)
	writeVecU128(w, v.E)
	writeVecU256(w, v.F)
	writeVecI8(w, v.G)
	writeVecI16(w, v.H)
	writeVecI32(w, v.I)
	writeVecI64(w, v.J)
	writeVecI128(w, v.K)
	writeVecI256(w, v.L)
	writeVecBool(w, v.M)
	writeVecF32(w, v.N)
	writeVecF64(w, v.O)
	writeVecString(w, v.P)
	writeVecIdentity(w, v.Q)
	writeVecConnectionId(w, v.R)
	writeVecTimestamp(w, v.S)
	writeVecTimeDuration(w, v.T)
	writeVecUuid(w, v.U)
}

func decodeEveryVecStruct(r *bsatn.Reader) (EveryVecStruct, error) {
	var v EveryVecStruct
	var err error
	v.A, err = readVecU8(r)
	if err != nil {
		return v, err
	}
	v.B, err = readVecU16(r)
	if err != nil {
		return v, err
	}
	v.C, err = readVecU32(r)
	if err != nil {
		return v, err
	}
	v.D, err = readVecU64(r)
	if err != nil {
		return v, err
	}
	v.E, err = readVecU128(r)
	if err != nil {
		return v, err
	}
	v.F, err = readVecU256(r)
	if err != nil {
		return v, err
	}
	v.G, err = readVecI8(r)
	if err != nil {
		return v, err
	}
	v.H, err = readVecI16(r)
	if err != nil {
		return v, err
	}
	v.I, err = readVecI32(r)
	if err != nil {
		return v, err
	}
	v.J, err = readVecI64(r)
	if err != nil {
		return v, err
	}
	v.K, err = readVecI128(r)
	if err != nil {
		return v, err
	}
	v.L, err = readVecI256(r)
	if err != nil {
		return v, err
	}
	v.M, err = readVecBool(r)
	if err != nil {
		return v, err
	}
	v.N, err = readVecF32(r)
	if err != nil {
		return v, err
	}
	v.O, err = readVecF64(r)
	if err != nil {
		return v, err
	}
	v.P, err = readVecString(r)
	if err != nil {
		return v, err
	}
	v.Q, err = readVecIdentity(r)
	if err != nil {
		return v, err
	}
	v.R, err = readVecConnectionId(r)
	if err != nil {
		return v, err
	}
	v.S, err = readVecTimestamp(r)
	if err != nil {
		return v, err
	}
	v.T, err = readVecTimeDuration(r)
	if err != nil {
		return v, err
	}
	v.U, err = readVecUuid(r)
	return v, err
}

// ── Vec helper writers ───────────────────────────────────────────────────────

func writeVecU8(w *bsatn.Writer, v []uint8) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		w.WriteU8(x)
	}
}

func writeVecU16(w *bsatn.Writer, v []uint16) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		w.WriteU16(x)
	}
}

func writeVecU32(w *bsatn.Writer, v []uint32) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		w.WriteU32(x)
	}
}

func writeVecU64(w *bsatn.Writer, v []uint64) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		w.WriteU64(x)
	}
}

func writeVecU128(w *bsatn.Writer, v []types.U128) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		x.WriteBsatn(w)
	}
}

func writeVecU256(w *bsatn.Writer, v []types.U256) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		x.WriteBsatn(w)
	}
}

func writeVecI8(w *bsatn.Writer, v []int8) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		w.WriteI8(x)
	}
}

func writeVecI16(w *bsatn.Writer, v []int16) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		w.WriteI16(x)
	}
}

func writeVecI32(w *bsatn.Writer, v []int32) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		w.WriteI32(x)
	}
}

func writeVecI64(w *bsatn.Writer, v []int64) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		w.WriteI64(x)
	}
}

func writeVecI128(w *bsatn.Writer, v []types.I128) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		x.WriteBsatn(w)
	}
}

func writeVecI256(w *bsatn.Writer, v []types.I256) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		x.WriteBsatn(w)
	}
}

func writeVecBool(w *bsatn.Writer, v []bool) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		w.WriteBool(x)
	}
}

func writeVecF32(w *bsatn.Writer, v []float32) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		w.WriteF32(x)
	}
}

func writeVecF64(w *bsatn.Writer, v []float64) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		w.WriteF64(x)
	}
}

func writeVecString(w *bsatn.Writer, v []string) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		w.WriteString(x)
	}
}

func writeVecIdentity(w *bsatn.Writer, v []types.Identity) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		x.WriteBsatn(w)
	}
}

func writeVecConnectionId(w *bsatn.Writer, v []types.ConnectionId) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		x.WriteBsatn(w)
	}
}

func writeVecTimestamp(w *bsatn.Writer, v []types.Timestamp) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		x.WriteBsatn(w)
	}
}

func writeVecTimeDuration(w *bsatn.Writer, v []types.TimeDuration) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		x.WriteBsatn(w)
	}
}

func writeVecUuid(w *bsatn.Writer, v []types.Uuid) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		x.WriteBsatn(w)
	}
}

func writeVecSimpleEnum(w *bsatn.Writer, v []SimpleEnum) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		encodeSimpleEnum(w, x)
	}
}

func writeVecEnumWithPayload(w *bsatn.Writer, v []EnumWithPayload) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		encodeEnumWithPayload(w, x)
	}
}

func writeVecUnitStruct(w *bsatn.Writer, v []UnitStruct) {
	w.WriteArrayLen(uint32(len(v)))
}

func writeVecByteStruct(w *bsatn.Writer, v []ByteStruct) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		encodeByteStruct(w, x)
	}
}

func writeVecEveryPrimitiveStruct(w *bsatn.Writer, v []EveryPrimitiveStruct) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		encodeEveryPrimitiveStruct(w, x)
	}
}

func writeVecEveryVecStruct(w *bsatn.Writer, v []EveryVecStruct) {
	w.WriteArrayLen(uint32(len(v)))
	for _, x := range v {
		encodeEveryVecStruct(w, x)
	}
}

// ── Vec helper readers ───────────────────────────────────────────────────────

func readVecU8(r *bsatn.Reader) ([]uint8, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]uint8, n)
	for i := range v {
		v[i], err = r.ReadU8()
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecU16(r *bsatn.Reader) ([]uint16, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]uint16, n)
	for i := range v {
		v[i], err = r.ReadU16()
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecU32(r *bsatn.Reader) ([]uint32, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]uint32, n)
	for i := range v {
		v[i], err = r.ReadU32()
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecU64(r *bsatn.Reader) ([]uint64, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]uint64, n)
	for i := range v {
		v[i], err = r.ReadU64()
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecU128(r *bsatn.Reader) ([]types.U128, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]types.U128, n)
	for i := range v {
		v[i], err = types.ReadU128(r)
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecU256(r *bsatn.Reader) ([]types.U256, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]types.U256, n)
	for i := range v {
		v[i], err = types.ReadU256(r)
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecI8(r *bsatn.Reader) ([]int8, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]int8, n)
	for i := range v {
		v[i], err = r.ReadI8()
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecI16(r *bsatn.Reader) ([]int16, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]int16, n)
	for i := range v {
		v[i], err = r.ReadI16()
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecI32(r *bsatn.Reader) ([]int32, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]int32, n)
	for i := range v {
		v[i], err = r.ReadI32()
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecI64(r *bsatn.Reader) ([]int64, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]int64, n)
	for i := range v {
		v[i], err = r.ReadI64()
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecI128(r *bsatn.Reader) ([]types.I128, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]types.I128, n)
	for i := range v {
		v[i], err = types.ReadI128(r)
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecI256(r *bsatn.Reader) ([]types.I256, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]types.I256, n)
	for i := range v {
		v[i], err = types.ReadI256(r)
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecBool(r *bsatn.Reader) ([]bool, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]bool, n)
	for i := range v {
		v[i], err = r.ReadBool()
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecF32(r *bsatn.Reader) ([]float32, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]float32, n)
	for i := range v {
		v[i], err = r.ReadF32()
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecF64(r *bsatn.Reader) ([]float64, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]float64, n)
	for i := range v {
		v[i], err = r.ReadF64()
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecString(r *bsatn.Reader) ([]string, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]string, n)
	for i := range v {
		v[i], err = r.ReadString()
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecIdentity(r *bsatn.Reader) ([]types.Identity, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]types.Identity, n)
	for i := range v {
		v[i], err = types.ReadIdentity(r)
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecConnectionId(r *bsatn.Reader) ([]types.ConnectionId, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]types.ConnectionId, n)
	for i := range v {
		v[i], err = types.ReadConnectionId(r)
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecTimestamp(r *bsatn.Reader) ([]types.Timestamp, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]types.Timestamp, n)
	for i := range v {
		v[i], err = types.ReadTimestamp(r)
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecTimeDuration(r *bsatn.Reader) ([]types.TimeDuration, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]types.TimeDuration, n)
	for i := range v {
		v[i], err = types.ReadTimeDuration(r)
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecUuid(r *bsatn.Reader) ([]types.Uuid, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]types.Uuid, n)
	for i := range v {
		v[i], err = types.ReadUuid(r)
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecSimpleEnum(r *bsatn.Reader) ([]SimpleEnum, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]SimpleEnum, n)
	for i := range v {
		v[i], err = decodeSimpleEnum(r)
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecEnumWithPayload(r *bsatn.Reader) ([]EnumWithPayload, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]EnumWithPayload, n)
	for i := range v {
		v[i], err = decodeEnumWithPayload(r)
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecUnitStruct(r *bsatn.Reader) ([]UnitStruct, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	return make([]UnitStruct, n), nil
}

func readVecByteStruct(r *bsatn.Reader) ([]ByteStruct, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]ByteStruct, n)
	for i := range v {
		v[i], err = decodeByteStruct(r)
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecEveryPrimitiveStruct(r *bsatn.Reader) ([]EveryPrimitiveStruct, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]EveryPrimitiveStruct, n)
	for i := range v {
		v[i], err = decodeEveryPrimitiveStruct(r)
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

func readVecEveryVecStruct(r *bsatn.Reader) ([]EveryVecStruct, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	v := make([]EveryVecStruct, n)
	for i := range v {
		v[i], err = decodeEveryVecStruct(r)
		if err != nil {
			return nil, err
		}
	}
	return v, nil
}

// ── One* encode/decode ───────────────────────────────────────────────────────

func encodeOneU8(w *bsatn.Writer, v OneU8)   { w.WriteU8(v.N) }
func encodeOneU16(w *bsatn.Writer, v OneU16) { w.WriteU16(v.N) }
func encodeOneU32(w *bsatn.Writer, v OneU32) { w.WriteU32(v.N) }
func encodeOneU64(w *bsatn.Writer, v OneU64) { w.WriteU64(v.N) }
func encodeOneU128(w *bsatn.Writer, v OneU128) { v.N.WriteBsatn(w) }
func encodeOneU256(w *bsatn.Writer, v OneU256) { v.N.WriteBsatn(w) }
func encodeOneI8(w *bsatn.Writer, v OneI8)   { w.WriteI8(v.N) }
func encodeOneI16(w *bsatn.Writer, v OneI16) { w.WriteI16(v.N) }
func encodeOneI32(w *bsatn.Writer, v OneI32) { w.WriteI32(v.N) }
func encodeOneI64(w *bsatn.Writer, v OneI64) { w.WriteI64(v.N) }
func encodeOneI128(w *bsatn.Writer, v OneI128) { v.N.WriteBsatn(w) }
func encodeOneI256(w *bsatn.Writer, v OneI256) { v.N.WriteBsatn(w) }
func encodeOneBool(w *bsatn.Writer, v OneBool) { w.WriteBool(v.B) }
func encodeOneF32(w *bsatn.Writer, v OneF32)   { w.WriteF32(v.F) }
func encodeOneF64(w *bsatn.Writer, v OneF64)   { w.WriteF64(v.F) }
func encodeOneString(w *bsatn.Writer, v OneString) { w.WriteString(v.S) }
func encodeOneIdentity(w *bsatn.Writer, v OneIdentity) { v.I.WriteBsatn(w) }
func encodeOneConnectionId(w *bsatn.Writer, v OneConnectionId) { v.A.WriteBsatn(w) }
func encodeOneUuid(w *bsatn.Writer, v OneUuid) { v.U.WriteBsatn(w) }
func encodeOneTimestamp(w *bsatn.Writer, v OneTimestamp) { v.T.WriteBsatn(w) }
func encodeOneSimpleEnum(w *bsatn.Writer, v OneSimpleEnum) { encodeSimpleEnum(w, v.E) }
func encodeOneEnumWithPayload(w *bsatn.Writer, v OneEnumWithPayload) { encodeEnumWithPayload(w, v.E) }
func encodeOneUnitStruct(w *bsatn.Writer, v OneUnitStruct) { encodeUnitStruct(w, v.S) }
func encodeOneByteStruct(w *bsatn.Writer, v OneByteStruct) { encodeByteStruct(w, v.S) }
func encodeOneEveryPrimitiveStruct(w *bsatn.Writer, v OneEveryPrimitiveStruct) {
	encodeEveryPrimitiveStruct(w, v.S)
}
func encodeOneEveryVecStruct(w *bsatn.Writer, v OneEveryVecStruct) {
	encodeEveryVecStruct(w, v.S)
}

func decodeOneU8(r *bsatn.Reader) (OneU8, error) {
	n, err := r.ReadU8()
	return OneU8{N: n}, err
}
func decodeOneU16(r *bsatn.Reader) (OneU16, error) {
	n, err := r.ReadU16()
	return OneU16{N: n}, err
}
func decodeOneU32(r *bsatn.Reader) (OneU32, error) {
	n, err := r.ReadU32()
	return OneU32{N: n}, err
}
func decodeOneU64(r *bsatn.Reader) (OneU64, error) {
	n, err := r.ReadU64()
	return OneU64{N: n}, err
}
func decodeOneU128(r *bsatn.Reader) (OneU128, error) {
	n, err := types.ReadU128(r)
	return OneU128{N: n}, err
}
func decodeOneU256(r *bsatn.Reader) (OneU256, error) {
	n, err := types.ReadU256(r)
	return OneU256{N: n}, err
}
func decodeOneI8(r *bsatn.Reader) (OneI8, error) {
	n, err := r.ReadI8()
	return OneI8{N: n}, err
}
func decodeOneI16(r *bsatn.Reader) (OneI16, error) {
	n, err := r.ReadI16()
	return OneI16{N: n}, err
}
func decodeOneI32(r *bsatn.Reader) (OneI32, error) {
	n, err := r.ReadI32()
	return OneI32{N: n}, err
}
func decodeOneI64(r *bsatn.Reader) (OneI64, error) {
	n, err := r.ReadI64()
	return OneI64{N: n}, err
}
func decodeOneI128(r *bsatn.Reader) (OneI128, error) {
	n, err := types.ReadI128(r)
	return OneI128{N: n}, err
}
func decodeOneI256(r *bsatn.Reader) (OneI256, error) {
	n, err := types.ReadI256(r)
	return OneI256{N: n}, err
}
func decodeOneBool(r *bsatn.Reader) (OneBool, error) {
	b, err := r.ReadBool()
	return OneBool{B: b}, err
}
func decodeOneF32(r *bsatn.Reader) (OneF32, error) {
	f, err := r.ReadF32()
	return OneF32{F: f}, err
}
func decodeOneF64(r *bsatn.Reader) (OneF64, error) {
	f, err := r.ReadF64()
	return OneF64{F: f}, err
}
func decodeOneString(r *bsatn.Reader) (OneString, error) {
	s, err := r.ReadString()
	return OneString{S: s}, err
}
func decodeOneIdentity(r *bsatn.Reader) (OneIdentity, error) {
	i, err := types.ReadIdentity(r)
	return OneIdentity{I: i}, err
}
func decodeOneConnectionId(r *bsatn.Reader) (OneConnectionId, error) {
	a, err := types.ReadConnectionId(r)
	return OneConnectionId{A: a}, err
}
func decodeOneUuid(r *bsatn.Reader) (OneUuid, error) {
	u, err := types.ReadUuid(r)
	return OneUuid{U: u}, err
}
func decodeOneTimestamp(r *bsatn.Reader) (OneTimestamp, error) {
	t, err := types.ReadTimestamp(r)
	return OneTimestamp{T: t}, err
}
func decodeOneSimpleEnum(r *bsatn.Reader) (OneSimpleEnum, error) {
	e, err := decodeSimpleEnum(r)
	return OneSimpleEnum{E: e}, err
}
func decodeOneEnumWithPayload(r *bsatn.Reader) (OneEnumWithPayload, error) {
	e, err := decodeEnumWithPayload(r)
	return OneEnumWithPayload{E: e}, err
}
func decodeOneUnitStruct(r *bsatn.Reader) (OneUnitStruct, error) {
	s, err := decodeUnitStruct(r)
	return OneUnitStruct{S: s}, err
}
func decodeOneByteStruct(r *bsatn.Reader) (OneByteStruct, error) {
	s, err := decodeByteStruct(r)
	return OneByteStruct{S: s}, err
}
func decodeOneEveryPrimitiveStruct(r *bsatn.Reader) (OneEveryPrimitiveStruct, error) {
	s, err := decodeEveryPrimitiveStruct(r)
	return OneEveryPrimitiveStruct{S: s}, err
}
func decodeOneEveryVecStruct(r *bsatn.Reader) (OneEveryVecStruct, error) {
	s, err := decodeEveryVecStruct(r)
	return OneEveryVecStruct{S: s}, err
}

// ── Vec* encode/decode ───────────────────────────────────────────────────────

func encodeVecU8(w *bsatn.Writer, v VecU8)   { writeVecU8(w, v.N) }
func encodeVecU16(w *bsatn.Writer, v VecU16) { writeVecU16(w, v.N) }
func encodeVecU32(w *bsatn.Writer, v VecU32) { writeVecU32(w, v.N) }
func encodeVecU64(w *bsatn.Writer, v VecU64) { writeVecU64(w, v.N) }
func encodeVecU128(w *bsatn.Writer, v VecU128) { writeVecU128(w, v.N) }
func encodeVecU256(w *bsatn.Writer, v VecU256) { writeVecU256(w, v.N) }
func encodeVecI8(w *bsatn.Writer, v VecI8)   { writeVecI8(w, v.N) }
func encodeVecI16(w *bsatn.Writer, v VecI16) { writeVecI16(w, v.N) }
func encodeVecI32(w *bsatn.Writer, v VecI32) { writeVecI32(w, v.N) }
func encodeVecI64(w *bsatn.Writer, v VecI64) { writeVecI64(w, v.N) }
func encodeVecI128(w *bsatn.Writer, v VecI128) { writeVecI128(w, v.N) }
func encodeVecI256(w *bsatn.Writer, v VecI256) { writeVecI256(w, v.N) }
func encodeVecBool(w *bsatn.Writer, v VecBool) { writeVecBool(w, v.B) }
func encodeVecF32(w *bsatn.Writer, v VecF32)   { writeVecF32(w, v.F) }
func encodeVecF64(w *bsatn.Writer, v VecF64)   { writeVecF64(w, v.F) }
func encodeVecString(w *bsatn.Writer, v VecString) { writeVecString(w, v.S) }
func encodeVecIdentity(w *bsatn.Writer, v VecIdentity) { writeVecIdentity(w, v.I) }
func encodeVecConnectionId(w *bsatn.Writer, v VecConnectionId) { writeVecConnectionId(w, v.A) }
func encodeVecUuid(w *bsatn.Writer, v VecUuid) { writeVecUuid(w, v.U) }
func encodeVecTimestamp(w *bsatn.Writer, v VecTimestamp) { writeVecTimestamp(w, v.T) }
func encodeVecSimpleEnum(w *bsatn.Writer, v VecSimpleEnum) { writeVecSimpleEnum(w, v.E) }
func encodeVecEnumWithPayload(w *bsatn.Writer, v VecEnumWithPayload) {
	writeVecEnumWithPayload(w, v.E)
}
func encodeVecUnitStruct(w *bsatn.Writer, v VecUnitStruct) { writeVecUnitStruct(w, v.S) }
func encodeVecByteStruct(w *bsatn.Writer, v VecByteStruct) { writeVecByteStruct(w, v.S) }
func encodeVecEveryPrimitiveStruct(w *bsatn.Writer, v VecEveryPrimitiveStruct) {
	writeVecEveryPrimitiveStruct(w, v.S)
}
func encodeVecEveryVecStruct(w *bsatn.Writer, v VecEveryVecStruct) {
	writeVecEveryVecStruct(w, v.S)
}

func decodeVecU8(r *bsatn.Reader) (VecU8, error) {
	n, err := readVecU8(r)
	return VecU8{N: n}, err
}
func decodeVecU16(r *bsatn.Reader) (VecU16, error) {
	n, err := readVecU16(r)
	return VecU16{N: n}, err
}
func decodeVecU32(r *bsatn.Reader) (VecU32, error) {
	n, err := readVecU32(r)
	return VecU32{N: n}, err
}
func decodeVecU64(r *bsatn.Reader) (VecU64, error) {
	n, err := readVecU64(r)
	return VecU64{N: n}, err
}
func decodeVecU128(r *bsatn.Reader) (VecU128, error) {
	n, err := readVecU128(r)
	return VecU128{N: n}, err
}
func decodeVecU256(r *bsatn.Reader) (VecU256, error) {
	n, err := readVecU256(r)
	return VecU256{N: n}, err
}
func decodeVecI8(r *bsatn.Reader) (VecI8, error) {
	n, err := readVecI8(r)
	return VecI8{N: n}, err
}
func decodeVecI16(r *bsatn.Reader) (VecI16, error) {
	n, err := readVecI16(r)
	return VecI16{N: n}, err
}
func decodeVecI32(r *bsatn.Reader) (VecI32, error) {
	n, err := readVecI32(r)
	return VecI32{N: n}, err
}
func decodeVecI64(r *bsatn.Reader) (VecI64, error) {
	n, err := readVecI64(r)
	return VecI64{N: n}, err
}
func decodeVecI128(r *bsatn.Reader) (VecI128, error) {
	n, err := readVecI128(r)
	return VecI128{N: n}, err
}
func decodeVecI256(r *bsatn.Reader) (VecI256, error) {
	n, err := readVecI256(r)
	return VecI256{N: n}, err
}
func decodeVecBool(r *bsatn.Reader) (VecBool, error) {
	b, err := readVecBool(r)
	return VecBool{B: b}, err
}
func decodeVecF32(r *bsatn.Reader) (VecF32, error) {
	f, err := readVecF32(r)
	return VecF32{F: f}, err
}
func decodeVecF64(r *bsatn.Reader) (VecF64, error) {
	f, err := readVecF64(r)
	return VecF64{F: f}, err
}
func decodeVecString(r *bsatn.Reader) (VecString, error) {
	s, err := readVecString(r)
	return VecString{S: s}, err
}
func decodeVecIdentity(r *bsatn.Reader) (VecIdentity, error) {
	i, err := readVecIdentity(r)
	return VecIdentity{I: i}, err
}
func decodeVecConnectionId(r *bsatn.Reader) (VecConnectionId, error) {
	a, err := readVecConnectionId(r)
	return VecConnectionId{A: a}, err
}
func decodeVecUuid(r *bsatn.Reader) (VecUuid, error) {
	u, err := readVecUuid(r)
	return VecUuid{U: u}, err
}
func decodeVecTimestamp(r *bsatn.Reader) (VecTimestamp, error) {
	t, err := readVecTimestamp(r)
	return VecTimestamp{T: t}, err
}
func decodeVecSimpleEnum(r *bsatn.Reader) (VecSimpleEnum, error) {
	e, err := readVecSimpleEnum(r)
	return VecSimpleEnum{E: e}, err
}
func decodeVecEnumWithPayload(r *bsatn.Reader) (VecEnumWithPayload, error) {
	e, err := readVecEnumWithPayload(r)
	return VecEnumWithPayload{E: e}, err
}
func decodeVecUnitStruct(r *bsatn.Reader) (VecUnitStruct, error) {
	s, err := readVecUnitStruct(r)
	return VecUnitStruct{S: s}, err
}
func decodeVecByteStruct(r *bsatn.Reader) (VecByteStruct, error) {
	s, err := readVecByteStruct(r)
	return VecByteStruct{S: s}, err
}
func decodeVecEveryPrimitiveStruct(r *bsatn.Reader) (VecEveryPrimitiveStruct, error) {
	s, err := readVecEveryPrimitiveStruct(r)
	return VecEveryPrimitiveStruct{S: s}, err
}
func decodeVecEveryVecStruct(r *bsatn.Reader) (VecEveryVecStruct, error) {
	s, err := readVecEveryVecStruct(r)
	return VecEveryVecStruct{S: s}, err
}

// ── Option* encode/decode ────────────────────────────────────────────────────

func encodeOptionI32Row(w *bsatn.Writer, v OptionI32Row) {
	if v.N == nil {
		w.WriteVariantTag(0)
	} else {
		w.WriteVariantTag(1)
		w.WriteI32(*v.N)
	}
}
func decodeOptionI32Row(r *bsatn.Reader) (OptionI32Row, error) {
	t, err := r.ReadVariantTag()
	if err != nil || t == 0 {
		return OptionI32Row{}, err
	}
	n, err := r.ReadI32()
	return OptionI32Row{N: &n}, err
}

func encodeOptionStringRow(w *bsatn.Writer, v OptionStringRow) {
	if v.S == nil {
		w.WriteVariantTag(0)
	} else {
		w.WriteVariantTag(1)
		w.WriteString(*v.S)
	}
}
func decodeOptionStringRow(r *bsatn.Reader) (OptionStringRow, error) {
	t, err := r.ReadVariantTag()
	if err != nil || t == 0 {
		return OptionStringRow{}, err
	}
	s, err := r.ReadString()
	return OptionStringRow{S: &s}, err
}

func encodeOptionIdentityRow(w *bsatn.Writer, v OptionIdentityRow) {
	if v.I == nil {
		w.WriteVariantTag(0)
	} else {
		w.WriteVariantTag(1)
		v.I.WriteBsatn(w)
	}
}
func decodeOptionIdentityRow(r *bsatn.Reader) (OptionIdentityRow, error) {
	t, err := r.ReadVariantTag()
	if err != nil || t == 0 {
		return OptionIdentityRow{}, err
	}
	i, err := types.ReadIdentity(r)
	return OptionIdentityRow{I: &i}, err
}

func encodeOptionUuidRow(w *bsatn.Writer, v OptionUuidRow) {
	if v.U == nil {
		w.WriteVariantTag(0)
	} else {
		w.WriteVariantTag(1)
		v.U.WriteBsatn(w)
	}
}
func decodeOptionUuidRow(r *bsatn.Reader) (OptionUuidRow, error) {
	t, err := r.ReadVariantTag()
	if err != nil || t == 0 {
		return OptionUuidRow{}, err
	}
	u, err := types.ReadUuid(r)
	return OptionUuidRow{U: &u}, err
}

func encodeOptionSimpleEnumRow(w *bsatn.Writer, v OptionSimpleEnumRow) {
	if v.E == nil {
		w.WriteVariantTag(0)
	} else {
		w.WriteVariantTag(1)
		encodeSimpleEnum(w, *v.E)
	}
}
func decodeOptionSimpleEnumRow(r *bsatn.Reader) (OptionSimpleEnumRow, error) {
	t, err := r.ReadVariantTag()
	if err != nil || t == 0 {
		return OptionSimpleEnumRow{}, err
	}
	e, err := decodeSimpleEnum(r)
	return OptionSimpleEnumRow{E: &e}, err
}

func encodeOptionEveryPrimitiveStructRow(w *bsatn.Writer, v OptionEveryPrimitiveStructRow) {
	if v.S == nil {
		w.WriteVariantTag(0)
	} else {
		w.WriteVariantTag(1)
		encodeEveryPrimitiveStruct(w, *v.S)
	}
}
func decodeOptionEveryPrimitiveStructRow(r *bsatn.Reader) (OptionEveryPrimitiveStructRow, error) {
	t, err := r.ReadVariantTag()
	if err != nil || t == 0 {
		return OptionEveryPrimitiveStructRow{}, err
	}
	s, err := decodeEveryPrimitiveStruct(r)
	return OptionEveryPrimitiveStructRow{S: &s}, err
}

func encodeOptionVecOptionI32Row(w *bsatn.Writer, v OptionVecOptionI32Row) {
	if v.V == nil {
		w.WriteVariantTag(0)
	} else {
		w.WriteVariantTag(1)
		sl := *v.V
		w.WriteArrayLen(uint32(len(sl)))
		for _, elem := range sl {
			if elem == nil {
				w.WriteVariantTag(0)
			} else {
				w.WriteVariantTag(1)
				w.WriteI32(*elem)
			}
		}
	}
}
func decodeOptionVecOptionI32Row(r *bsatn.Reader) (OptionVecOptionI32Row, error) {
	t, err := r.ReadVariantTag()
	if err != nil || t == 0 {
		return OptionVecOptionI32Row{}, err
	}
	n, err := r.ReadArrayLen()
	if err != nil {
		return OptionVecOptionI32Row{}, err
	}
	sl := make([]*int32, n)
	for i := range sl {
		inner, err := r.ReadVariantTag()
		if err != nil {
			return OptionVecOptionI32Row{}, err
		}
		if inner == 1 {
			val, err := r.ReadI32()
			if err != nil {
				return OptionVecOptionI32Row{}, err
			}
			sl[i] = &val
		}
	}
	return OptionVecOptionI32Row{V: &sl}, nil
}

// ── Result* encode/decode ────────────────────────────────────────────────────

func encodeResultI32StringRow(w *bsatn.Writer, v ResultI32StringRow) {
	if v.IsOk {
		w.WriteVariantTag(0)
		w.WriteI32(v.OkVal)
	} else {
		w.WriteVariantTag(1)
		w.WriteString(v.ErrVal)
	}
}
func decodeResultI32StringRow(r *bsatn.Reader) (ResultI32StringRow, error) {
	t, err := r.ReadVariantTag()
	if err != nil {
		return ResultI32StringRow{}, err
	}
	if t == 0 {
		n, err := r.ReadI32()
		return ResultI32StringRow{IsOk: true, OkVal: n}, err
	}
	s, err := r.ReadString()
	return ResultI32StringRow{IsOk: false, ErrVal: s}, err
}

func encodeResultStringI32Row(w *bsatn.Writer, v ResultStringI32Row) {
	if v.IsOk {
		w.WriteVariantTag(0)
		w.WriteString(v.OkVal)
	} else {
		w.WriteVariantTag(1)
		w.WriteI32(v.ErrVal)
	}
}
func decodeResultStringI32Row(r *bsatn.Reader) (ResultStringI32Row, error) {
	t, err := r.ReadVariantTag()
	if err != nil {
		return ResultStringI32Row{}, err
	}
	if t == 0 {
		s, err := r.ReadString()
		return ResultStringI32Row{IsOk: true, OkVal: s}, err
	}
	n, err := r.ReadI32()
	return ResultStringI32Row{IsOk: false, ErrVal: n}, err
}

func encodeResultIdentityStringRow(w *bsatn.Writer, v ResultIdentityStringRow) {
	if v.IsOk {
		w.WriteVariantTag(0)
		v.OkVal.WriteBsatn(w)
	} else {
		w.WriteVariantTag(1)
		w.WriteString(v.ErrVal)
	}
}
func decodeResultIdentityStringRow(r *bsatn.Reader) (ResultIdentityStringRow, error) {
	t, err := r.ReadVariantTag()
	if err != nil {
		return ResultIdentityStringRow{}, err
	}
	if t == 0 {
		i, err := types.ReadIdentity(r)
		return ResultIdentityStringRow{IsOk: true, OkVal: i}, err
	}
	s, err := r.ReadString()
	return ResultIdentityStringRow{IsOk: false, ErrVal: s}, err
}

func encodeResultSimpleEnumI32Row(w *bsatn.Writer, v ResultSimpleEnumI32Row) {
	if v.IsOk {
		w.WriteVariantTag(0)
		encodeSimpleEnum(w, v.OkVal)
	} else {
		w.WriteVariantTag(1)
		w.WriteI32(v.ErrVal)
	}
}
func decodeResultSimpleEnumI32Row(r *bsatn.Reader) (ResultSimpleEnumI32Row, error) {
	t, err := r.ReadVariantTag()
	if err != nil {
		return ResultSimpleEnumI32Row{}, err
	}
	if t == 0 {
		e, err := decodeSimpleEnum(r)
		return ResultSimpleEnumI32Row{IsOk: true, OkVal: e}, err
	}
	n, err := r.ReadI32()
	return ResultSimpleEnumI32Row{IsOk: false, ErrVal: n}, err
}

func encodeResultEveryPrimitiveStructStringRow(w *bsatn.Writer, v ResultEveryPrimitiveStructStringRow) {
	if v.IsOk {
		w.WriteVariantTag(0)
		encodeEveryPrimitiveStruct(w, v.OkVal)
	} else {
		w.WriteVariantTag(1)
		w.WriteString(v.ErrVal)
	}
}
func decodeResultEveryPrimitiveStructStringRow(r *bsatn.Reader) (ResultEveryPrimitiveStructStringRow, error) {
	t, err := r.ReadVariantTag()
	if err != nil {
		return ResultEveryPrimitiveStructStringRow{}, err
	}
	if t == 0 {
		s, err := decodeEveryPrimitiveStruct(r)
		return ResultEveryPrimitiveStructStringRow{IsOk: true, OkVal: s}, err
	}
	s, err := r.ReadString()
	return ResultEveryPrimitiveStructStringRow{IsOk: false, ErrVal: s}, err
}

func encodeResultVecI32StringRow(w *bsatn.Writer, v ResultVecI32StringRow) {
	if v.IsOk {
		w.WriteVariantTag(0)
		writeVecI32(w, v.OkVal)
	} else {
		w.WriteVariantTag(1)
		w.WriteString(v.ErrVal)
	}
}
func decodeResultVecI32StringRow(r *bsatn.Reader) (ResultVecI32StringRow, error) {
	t, err := r.ReadVariantTag()
	if err != nil {
		return ResultVecI32StringRow{}, err
	}
	if t == 0 {
		v, err := readVecI32(r)
		return ResultVecI32StringRow{IsOk: true, OkVal: v}, err
	}
	s, err := r.ReadString()
	return ResultVecI32StringRow{IsOk: false, ErrVal: s}, err
}

// ── Unique* encode/decode ────────────────────────────────────────────────────

func encodeUniqueU8(w *bsatn.Writer, v UniqueU8)   { w.WriteU8(v.N); w.WriteI32(v.Data) }
func encodeUniqueU16(w *bsatn.Writer, v UniqueU16) { w.WriteU16(v.N); w.WriteI32(v.Data) }
func encodeUniqueU32(w *bsatn.Writer, v UniqueU32) { w.WriteU32(v.N); w.WriteI32(v.Data) }
func encodeUniqueU64(w *bsatn.Writer, v UniqueU64) { w.WriteU64(v.N); w.WriteI32(v.Data) }
func encodeUniqueU128(w *bsatn.Writer, v UniqueU128) { v.N.WriteBsatn(w); w.WriteI32(v.Data) }
func encodeUniqueU256(w *bsatn.Writer, v UniqueU256) { v.N.WriteBsatn(w); w.WriteI32(v.Data) }
func encodeUniqueI8(w *bsatn.Writer, v UniqueI8)   { w.WriteI8(v.N); w.WriteI32(v.Data) }
func encodeUniqueI16(w *bsatn.Writer, v UniqueI16) { w.WriteI16(v.N); w.WriteI32(v.Data) }
func encodeUniqueI32(w *bsatn.Writer, v UniqueI32) { w.WriteI32(v.N); w.WriteI32(v.Data) }
func encodeUniqueI64(w *bsatn.Writer, v UniqueI64) { w.WriteI64(v.N); w.WriteI32(v.Data) }
func encodeUniqueI128(w *bsatn.Writer, v UniqueI128) { v.N.WriteBsatn(w); w.WriteI32(v.Data) }
func encodeUniqueI256(w *bsatn.Writer, v UniqueI256) { v.N.WriteBsatn(w); w.WriteI32(v.Data) }
func encodeUniqueBool(w *bsatn.Writer, v UniqueBool) { w.WriteBool(v.B); w.WriteI32(v.Data) }
func encodeUniqueString(w *bsatn.Writer, v UniqueString) { w.WriteString(v.S); w.WriteI32(v.Data) }
func encodeUniqueIdentity(w *bsatn.Writer, v UniqueIdentity) {
	v.I.WriteBsatn(w)
	w.WriteI32(v.Data)
}
func encodeUniqueConnectionId(w *bsatn.Writer, v UniqueConnectionId) {
	v.A.WriteBsatn(w)
	w.WriteI32(v.Data)
}
func encodeUniqueUuid(w *bsatn.Writer, v UniqueUuid) { v.U.WriteBsatn(w); w.WriteI32(v.Data) }

func decodeUniqueU8(r *bsatn.Reader) (UniqueU8, error) {
	n, err := r.ReadU8()
	if err != nil {
		return UniqueU8{}, err
	}
	d, err := r.ReadI32()
	return UniqueU8{N: n, Data: d}, err
}
func decodeUniqueU16(r *bsatn.Reader) (UniqueU16, error) {
	n, err := r.ReadU16()
	if err != nil {
		return UniqueU16{}, err
	}
	d, err := r.ReadI32()
	return UniqueU16{N: n, Data: d}, err
}
func decodeUniqueU32(r *bsatn.Reader) (UniqueU32, error) {
	n, err := r.ReadU32()
	if err != nil {
		return UniqueU32{}, err
	}
	d, err := r.ReadI32()
	return UniqueU32{N: n, Data: d}, err
}
func decodeUniqueU64(r *bsatn.Reader) (UniqueU64, error) {
	n, err := r.ReadU64()
	if err != nil {
		return UniqueU64{}, err
	}
	d, err := r.ReadI32()
	return UniqueU64{N: n, Data: d}, err
}
func decodeUniqueU128(r *bsatn.Reader) (UniqueU128, error) {
	n, err := types.ReadU128(r)
	if err != nil {
		return UniqueU128{}, err
	}
	d, err := r.ReadI32()
	return UniqueU128{N: n, Data: d}, err
}
func decodeUniqueU256(r *bsatn.Reader) (UniqueU256, error) {
	n, err := types.ReadU256(r)
	if err != nil {
		return UniqueU256{}, err
	}
	d, err := r.ReadI32()
	return UniqueU256{N: n, Data: d}, err
}
func decodeUniqueI8(r *bsatn.Reader) (UniqueI8, error) {
	n, err := r.ReadI8()
	if err != nil {
		return UniqueI8{}, err
	}
	d, err := r.ReadI32()
	return UniqueI8{N: n, Data: d}, err
}
func decodeUniqueI16(r *bsatn.Reader) (UniqueI16, error) {
	n, err := r.ReadI16()
	if err != nil {
		return UniqueI16{}, err
	}
	d, err := r.ReadI32()
	return UniqueI16{N: n, Data: d}, err
}
func decodeUniqueI32(r *bsatn.Reader) (UniqueI32, error) {
	n, err := r.ReadI32()
	if err != nil {
		return UniqueI32{}, err
	}
	d, err := r.ReadI32()
	return UniqueI32{N: n, Data: d}, err
}
func decodeUniqueI64(r *bsatn.Reader) (UniqueI64, error) {
	n, err := r.ReadI64()
	if err != nil {
		return UniqueI64{}, err
	}
	d, err := r.ReadI32()
	return UniqueI64{N: n, Data: d}, err
}
func decodeUniqueI128(r *bsatn.Reader) (UniqueI128, error) {
	n, err := types.ReadI128(r)
	if err != nil {
		return UniqueI128{}, err
	}
	d, err := r.ReadI32()
	return UniqueI128{N: n, Data: d}, err
}
func decodeUniqueI256(r *bsatn.Reader) (UniqueI256, error) {
	n, err := types.ReadI256(r)
	if err != nil {
		return UniqueI256{}, err
	}
	d, err := r.ReadI32()
	return UniqueI256{N: n, Data: d}, err
}
func decodeUniqueBool(r *bsatn.Reader) (UniqueBool, error) {
	b, err := r.ReadBool()
	if err != nil {
		return UniqueBool{}, err
	}
	d, err := r.ReadI32()
	return UniqueBool{B: b, Data: d}, err
}
func decodeUniqueString(r *bsatn.Reader) (UniqueString, error) {
	s, err := r.ReadString()
	if err != nil {
		return UniqueString{}, err
	}
	d, err := r.ReadI32()
	return UniqueString{S: s, Data: d}, err
}
func decodeUniqueIdentity(r *bsatn.Reader) (UniqueIdentity, error) {
	i, err := types.ReadIdentity(r)
	if err != nil {
		return UniqueIdentity{}, err
	}
	d, err := r.ReadI32()
	return UniqueIdentity{I: i, Data: d}, err
}
func decodeUniqueConnectionId(r *bsatn.Reader) (UniqueConnectionId, error) {
	a, err := types.ReadConnectionId(r)
	if err != nil {
		return UniqueConnectionId{}, err
	}
	d, err := r.ReadI32()
	return UniqueConnectionId{A: a, Data: d}, err
}
func decodeUniqueUuid(r *bsatn.Reader) (UniqueUuid, error) {
	u, err := types.ReadUuid(r)
	if err != nil {
		return UniqueUuid{}, err
	}
	d, err := r.ReadI32()
	return UniqueUuid{U: u, Data: d}, err
}

// ── PK* encode/decode ─────────────────────────────────────────────────────────

func encodePkU8(w *bsatn.Writer, v PkU8)   { w.WriteU8(v.N); w.WriteI32(v.Data) }
func encodePkU16(w *bsatn.Writer, v PkU16) { w.WriteU16(v.N); w.WriteI32(v.Data) }
func encodePkU32(w *bsatn.Writer, v PkU32) { w.WriteU32(v.N); w.WriteI32(v.Data) }
func encodePkU32Two(w *bsatn.Writer, v PkU32Two) { w.WriteU32(v.N); w.WriteI32(v.Data) }
func encodePkU64(w *bsatn.Writer, v PkU64) { w.WriteU64(v.N); w.WriteI32(v.Data) }
func encodePkU128(w *bsatn.Writer, v PkU128) { v.N.WriteBsatn(w); w.WriteI32(v.Data) }
func encodePkU256(w *bsatn.Writer, v PkU256) { v.N.WriteBsatn(w); w.WriteI32(v.Data) }
func encodePkI8(w *bsatn.Writer, v PkI8)   { w.WriteI8(v.N); w.WriteI32(v.Data) }
func encodePkI16(w *bsatn.Writer, v PkI16) { w.WriteI16(v.N); w.WriteI32(v.Data) }
func encodePkI32(w *bsatn.Writer, v PkI32) { w.WriteI32(v.N); w.WriteI32(v.Data) }
func encodePkI64(w *bsatn.Writer, v PkI64) { w.WriteI64(v.N); w.WriteI32(v.Data) }
func encodePkI128(w *bsatn.Writer, v PkI128) { v.N.WriteBsatn(w); w.WriteI32(v.Data) }
func encodePkI256(w *bsatn.Writer, v PkI256) { v.N.WriteBsatn(w); w.WriteI32(v.Data) }
func encodePkBool(w *bsatn.Writer, v PkBool) { w.WriteBool(v.B); w.WriteI32(v.Data) }
func encodePkString(w *bsatn.Writer, v PkString) { w.WriteString(v.S); w.WriteI32(v.Data) }
func encodePkIdentity(w *bsatn.Writer, v PkIdentity) { v.I.WriteBsatn(w); w.WriteI32(v.Data) }
func encodePkConnectionId(w *bsatn.Writer, v PkConnectionId) {
	v.A.WriteBsatn(w)
	w.WriteI32(v.Data)
}
func encodePkUuid(w *bsatn.Writer, v PkUuid) { v.U.WriteBsatn(w); w.WriteI32(v.Data) }
func encodePkSimpleEnum(w *bsatn.Writer, v PkSimpleEnum) {
	encodeSimpleEnum(w, v.A)
	w.WriteI32(v.Data)
}

func decodePkU8(r *bsatn.Reader) (PkU8, error) {
	n, err := r.ReadU8()
	if err != nil {
		return PkU8{}, err
	}
	d, err := r.ReadI32()
	return PkU8{N: n, Data: d}, err
}
func decodePkU16(r *bsatn.Reader) (PkU16, error) {
	n, err := r.ReadU16()
	if err != nil {
		return PkU16{}, err
	}
	d, err := r.ReadI32()
	return PkU16{N: n, Data: d}, err
}
func decodePkU32(r *bsatn.Reader) (PkU32, error) {
	n, err := r.ReadU32()
	if err != nil {
		return PkU32{}, err
	}
	d, err := r.ReadI32()
	return PkU32{N: n, Data: d}, err
}
func decodePkU32Two(r *bsatn.Reader) (PkU32Two, error) {
	n, err := r.ReadU32()
	if err != nil {
		return PkU32Two{}, err
	}
	d, err := r.ReadI32()
	return PkU32Two{N: n, Data: d}, err
}
func decodePkU64(r *bsatn.Reader) (PkU64, error) {
	n, err := r.ReadU64()
	if err != nil {
		return PkU64{}, err
	}
	d, err := r.ReadI32()
	return PkU64{N: n, Data: d}, err
}
func decodePkU128(r *bsatn.Reader) (PkU128, error) {
	n, err := types.ReadU128(r)
	if err != nil {
		return PkU128{}, err
	}
	d, err := r.ReadI32()
	return PkU128{N: n, Data: d}, err
}
func decodePkU256(r *bsatn.Reader) (PkU256, error) {
	n, err := types.ReadU256(r)
	if err != nil {
		return PkU256{}, err
	}
	d, err := r.ReadI32()
	return PkU256{N: n, Data: d}, err
}
func decodePkI8(r *bsatn.Reader) (PkI8, error) {
	n, err := r.ReadI8()
	if err != nil {
		return PkI8{}, err
	}
	d, err := r.ReadI32()
	return PkI8{N: n, Data: d}, err
}
func decodePkI16(r *bsatn.Reader) (PkI16, error) {
	n, err := r.ReadI16()
	if err != nil {
		return PkI16{}, err
	}
	d, err := r.ReadI32()
	return PkI16{N: n, Data: d}, err
}
func decodePkI32(r *bsatn.Reader) (PkI32, error) {
	n, err := r.ReadI32()
	if err != nil {
		return PkI32{}, err
	}
	d, err := r.ReadI32()
	return PkI32{N: n, Data: d}, err
}
func decodePkI64(r *bsatn.Reader) (PkI64, error) {
	n, err := r.ReadI64()
	if err != nil {
		return PkI64{}, err
	}
	d, err := r.ReadI32()
	return PkI64{N: n, Data: d}, err
}
func decodePkI128(r *bsatn.Reader) (PkI128, error) {
	n, err := types.ReadI128(r)
	if err != nil {
		return PkI128{}, err
	}
	d, err := r.ReadI32()
	return PkI128{N: n, Data: d}, err
}
func decodePkI256(r *bsatn.Reader) (PkI256, error) {
	n, err := types.ReadI256(r)
	if err != nil {
		return PkI256{}, err
	}
	d, err := r.ReadI32()
	return PkI256{N: n, Data: d}, err
}
func decodePkBool(r *bsatn.Reader) (PkBool, error) {
	b, err := r.ReadBool()
	if err != nil {
		return PkBool{}, err
	}
	d, err := r.ReadI32()
	return PkBool{B: b, Data: d}, err
}
func decodePkString(r *bsatn.Reader) (PkString, error) {
	s, err := r.ReadString()
	if err != nil {
		return PkString{}, err
	}
	d, err := r.ReadI32()
	return PkString{S: s, Data: d}, err
}
func decodePkIdentity(r *bsatn.Reader) (PkIdentity, error) {
	i, err := types.ReadIdentity(r)
	if err != nil {
		return PkIdentity{}, err
	}
	d, err := r.ReadI32()
	return PkIdentity{I: i, Data: d}, err
}
func decodePkConnectionId(r *bsatn.Reader) (PkConnectionId, error) {
	a, err := types.ReadConnectionId(r)
	if err != nil {
		return PkConnectionId{}, err
	}
	d, err := r.ReadI32()
	return PkConnectionId{A: a, Data: d}, err
}
func decodePkUuid(r *bsatn.Reader) (PkUuid, error) {
	u, err := types.ReadUuid(r)
	if err != nil {
		return PkUuid{}, err
	}
	d, err := r.ReadI32()
	return PkUuid{U: u, Data: d}, err
}
func decodePkSimpleEnum(r *bsatn.Reader) (PkSimpleEnum, error) {
	a, err := decodeSimpleEnum(r)
	if err != nil {
		return PkSimpleEnum{}, err
	}
	d, err := r.ReadI32()
	return PkSimpleEnum{A: a, Data: d}, err
}

// ── Special table encode/decode ──────────────────────────────────────────────

func encodeLargeTable(w *bsatn.Writer, v LargeTable) {
	w.WriteU8(v.A)
	w.WriteU16(v.B)
	w.WriteU32(v.C)
	w.WriteU64(v.D)
	v.E.WriteBsatn(w)
	v.F.WriteBsatn(w)
	w.WriteI8(v.G)
	w.WriteI16(v.H)
	w.WriteI32(v.I)
	w.WriteI64(v.J)
	v.K.WriteBsatn(w)
	v.L.WriteBsatn(w)
	w.WriteBool(v.M)
	w.WriteF32(v.N)
	w.WriteF64(v.O)
	w.WriteString(v.P)
	encodeSimpleEnum(w, v.Q)
	encodeEnumWithPayload(w, v.R)
	encodeUnitStruct(w, v.S)
	encodeByteStruct(w, v.T)
	encodeEveryPrimitiveStruct(w, v.U)
	encodeEveryVecStruct(w, v.V)
}

func decodeLargeTable(r *bsatn.Reader) (LargeTable, error) {
	var v LargeTable
	var err error
	v.A, err = r.ReadU8()
	if err != nil {
		return v, err
	}
	v.B, err = r.ReadU16()
	if err != nil {
		return v, err
	}
	v.C, err = r.ReadU32()
	if err != nil {
		return v, err
	}
	v.D, err = r.ReadU64()
	if err != nil {
		return v, err
	}
	v.E, err = types.ReadU128(r)
	if err != nil {
		return v, err
	}
	v.F, err = types.ReadU256(r)
	if err != nil {
		return v, err
	}
	v.G, err = r.ReadI8()
	if err != nil {
		return v, err
	}
	v.H, err = r.ReadI16()
	if err != nil {
		return v, err
	}
	v.I, err = r.ReadI32()
	if err != nil {
		return v, err
	}
	v.J, err = r.ReadI64()
	if err != nil {
		return v, err
	}
	v.K, err = types.ReadI128(r)
	if err != nil {
		return v, err
	}
	v.L, err = types.ReadI256(r)
	if err != nil {
		return v, err
	}
	v.M, err = r.ReadBool()
	if err != nil {
		return v, err
	}
	v.N, err = r.ReadF32()
	if err != nil {
		return v, err
	}
	v.O, err = r.ReadF64()
	if err != nil {
		return v, err
	}
	v.P, err = r.ReadString()
	if err != nil {
		return v, err
	}
	v.Q, err = decodeSimpleEnum(r)
	if err != nil {
		return v, err
	}
	v.R, err = decodeEnumWithPayload(r)
	if err != nil {
		return v, err
	}
	v.S, err = decodeUnitStruct(r)
	if err != nil {
		return v, err
	}
	v.T, err = decodeByteStruct(r)
	if err != nil {
		return v, err
	}
	v.U, err = decodeEveryPrimitiveStruct(r)
	if err != nil {
		return v, err
	}
	v.V, err = decodeEveryVecStruct(r)
	return v, err
}

func encodeTableHoldsTable(w *bsatn.Writer, v TableHoldsTable) {
	encodeOneU8(w, v.A)
	encodeVecU8(w, v.B)
}

func decodeTableHoldsTable(r *bsatn.Reader) (TableHoldsTable, error) {
	a, err := decodeOneU8(r)
	if err != nil {
		return TableHoldsTable{}, err
	}
	b, err := decodeVecU8(r)
	return TableHoldsTable{A: a, B: b}, err
}

func encodeScheduledTable(w *bsatn.Writer, v ScheduledTable) {
	w.WriteU64(v.ScheduledId)
	v.ScheduledAt.WriteBsatn(w)
	w.WriteString(v.Text)
}

func decodeScheduledTable(r *bsatn.Reader) (ScheduledTable, error) {
	id, err := r.ReadU64()
	if err != nil {
		return ScheduledTable{}, err
	}
	at, err := types.ReadScheduleAt(r)
	if err != nil {
		return ScheduledTable{}, err
	}
	text, err := r.ReadString()
	return ScheduledTable{ScheduledId: id, ScheduledAt: at, Text: text}, err
}

func encodeIndexedTable(w *bsatn.Writer, v IndexedTable) {
	w.WriteU32(v.PlayerId)
}
func decodeIndexedTable(r *bsatn.Reader) (IndexedTable, error) {
	p, err := r.ReadU32()
	return IndexedTable{PlayerId: p}, err
}

func encodeIndexedTable2(w *bsatn.Writer, v IndexedTable2) {
	w.WriteU32(v.PlayerId)
	w.WriteF32(v.PlayerSnazz)
}
func decodeIndexedTable2(r *bsatn.Reader) (IndexedTable2, error) {
	p, err := r.ReadU32()
	if err != nil {
		return IndexedTable2{}, err
	}
	s, err := r.ReadF32()
	return IndexedTable2{PlayerId: p, PlayerSnazz: s}, err
}

func encodeBTreeU32Row(w *bsatn.Writer, v BTreeU32Row) {
	w.WriteU32(v.N)
	w.WriteI32(v.Data)
}
func decodeBTreeU32Row(r *bsatn.Reader) (BTreeU32Row, error) {
	n, err := r.ReadU32()
	if err != nil {
		return BTreeU32Row{}, err
	}
	d, err := r.ReadI32()
	return BTreeU32Row{N: n, Data: d}, err
}

func encodeUsersRow(w *bsatn.Writer, v UsersRow) {
	v.Identity.WriteBsatn(w)
	w.WriteString(v.Name)
}
func decodeUsersRow(r *bsatn.Reader) (UsersRow, error) {
	i, err := types.ReadIdentity(r)
	if err != nil {
		return UsersRow{}, err
	}
	n, err := r.ReadString()
	return UsersRow{Identity: i, Name: n}, err
}

func encodeIndexedSimpleEnumRow(w *bsatn.Writer, v IndexedSimpleEnumRow) {
	encodeSimpleEnum(w, v.N)
}
func decodeIndexedSimpleEnumRow(r *bsatn.Reader) (IndexedSimpleEnumRow, error) {
	n, err := decodeSimpleEnum(r)
	return IndexedSimpleEnumRow{N: n}, err
}
