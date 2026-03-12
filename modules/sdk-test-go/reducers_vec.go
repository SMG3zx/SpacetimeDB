package main

import (
	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go/types"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
)

func registerVecReducers() {
	regR("insert_vec_u8", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicU8}}}, insertVecU8)
	regR("insert_vec_u16", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicU16}}}, insertVecU16)
	regR("insert_vec_u32", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicU32}}}, insertVecU32)
	regR("insert_vec_u64", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicU64}}}, insertVecU64)
	regR("insert_vec_u128", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicU128}}}, insertVecU128)
	regR("insert_vec_u256", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicU256}}}, insertVecU256)
	regR("insert_vec_i8", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicI8}}}, insertVecI8)
	regR("insert_vec_i16", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicI16}}}, insertVecI16)
	regR("insert_vec_i32", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicI32}}}, insertVecI32)
	regR("insert_vec_i64", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicI64}}}, insertVecI64)
	regR("insert_vec_i128", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicI128}}}, insertVecI128)
	regR("insert_vec_i256", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicI256}}}, insertVecI256)
	regR("insert_vec_bool", []spacetimedb.ColumnDef{{Name: "b", Type: types.ArrayType{ElemType: types.AlgebraicBool}}}, insertVecBool)
	regR("insert_vec_f32", []spacetimedb.ColumnDef{{Name: "f", Type: types.ArrayType{ElemType: types.AlgebraicF32}}}, insertVecF32)
	regR("insert_vec_f64", []spacetimedb.ColumnDef{{Name: "f", Type: types.ArrayType{ElemType: types.AlgebraicF64}}}, insertVecF64)
	regR("insert_vec_string", []spacetimedb.ColumnDef{{Name: "s", Type: types.ArrayType{ElemType: types.AlgebraicString}}}, insertVecString)
	regR("insert_vec_identity", []spacetimedb.ColumnDef{{Name: "i", Type: types.ArrayType{ElemType: satIdentity}}}, insertVecIdentity)
	regR("insert_vec_connection_id", []spacetimedb.ColumnDef{{Name: "a", Type: types.ArrayType{ElemType: satConnectionId}}}, insertVecConnectionId)
	regR("insert_vec_uuid", []spacetimedb.ColumnDef{{Name: "u", Type: types.ArrayType{ElemType: satUuid}}}, insertVecUuid)
	regR("insert_vec_timestamp", []spacetimedb.ColumnDef{{Name: "t", Type: types.ArrayType{ElemType: types.AlgebraicTimestamp}}}, insertVecTimestamp)
	regR("insert_vec_simple_enum", []spacetimedb.ColumnDef{{Name: "e", Type: types.ArrayType{ElemType: satSimpleEnum}}}, insertVecSimpleEnum)
	regR("insert_vec_enum_with_payload", []spacetimedb.ColumnDef{{Name: "e", Type: types.ArrayType{ElemType: satEnumWithPayload}}}, insertVecEnumWithPayload)
	regR("insert_vec_unit_struct", []spacetimedb.ColumnDef{{Name: "s", Type: types.ArrayType{ElemType: satUnitStruct}}}, insertVecUnitStruct)
	regR("insert_vec_byte_struct", []spacetimedb.ColumnDef{{Name: "s", Type: types.ArrayType{ElemType: satByteStruct}}}, insertVecByteStruct)
	regR("insert_vec_every_primitive_struct", []spacetimedb.ColumnDef{{Name: "s", Type: types.ArrayType{ElemType: satEveryPrimitiveStruct}}}, insertVecEveryPrimitiveStruct)
	regR("insert_vec_every_vec_struct", []spacetimedb.ColumnDef{{Name: "s", Type: types.ArrayType{ElemType: satEveryVecStruct}}}, insertVecEveryVecStruct)
}

// ── Vec* reducer handlers ─────────────────────────────────────────────────────

func insertVecU8(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_u8", args)
	n, err := readVecU8(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_u8: " + err.Error())
	}
	if _, err := vecU8Table.Insert(VecU8{N: n}); err != nil {
		spacetimedb.LogPanic("insert_vec_u8: " + err.Error())
	}
}

func insertVecU16(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_u16", args)
	n, err := readVecU16(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_u16: " + err.Error())
	}
	if _, err := vecU16Table.Insert(VecU16{N: n}); err != nil {
		spacetimedb.LogPanic("insert_vec_u16: " + err.Error())
	}
}

func insertVecU32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_u32", args)
	n, err := readVecU32(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_u32: " + err.Error())
	}
	if _, err := vecU32Table.Insert(VecU32{N: n}); err != nil {
		spacetimedb.LogPanic("insert_vec_u32: " + err.Error())
	}
}

func insertVecU64(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_u64", args)
	n, err := readVecU64(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_u64: " + err.Error())
	}
	if _, err := vecU64Table.Insert(VecU64{N: n}); err != nil {
		spacetimedb.LogPanic("insert_vec_u64: " + err.Error())
	}
}

func insertVecU128(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_u128", args)
	n, err := readVecU128(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_u128: " + err.Error())
	}
	if _, err := vecU128Table.Insert(VecU128{N: n}); err != nil {
		spacetimedb.LogPanic("insert_vec_u128: " + err.Error())
	}
}

func insertVecU256(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_u256", args)
	n, err := readVecU256(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_u256: " + err.Error())
	}
	if _, err := vecU256Table.Insert(VecU256{N: n}); err != nil {
		spacetimedb.LogPanic("insert_vec_u256: " + err.Error())
	}
}

func insertVecI8(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_i8", args)
	n, err := readVecI8(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_i8: " + err.Error())
	}
	if _, err := vecI8Table.Insert(VecI8{N: n}); err != nil {
		spacetimedb.LogPanic("insert_vec_i8: " + err.Error())
	}
}

func insertVecI16(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_i16", args)
	n, err := readVecI16(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_i16: " + err.Error())
	}
	if _, err := vecI16Table.Insert(VecI16{N: n}); err != nil {
		spacetimedb.LogPanic("insert_vec_i16: " + err.Error())
	}
}

func insertVecI32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_i32", args)
	n, err := readVecI32(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_i32: " + err.Error())
	}
	if _, err := vecI32Table.Insert(VecI32{N: n}); err != nil {
		spacetimedb.LogPanic("insert_vec_i32: " + err.Error())
	}
}

func insertVecI64(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_i64", args)
	n, err := readVecI64(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_i64: " + err.Error())
	}
	if _, err := vecI64Table.Insert(VecI64{N: n}); err != nil {
		spacetimedb.LogPanic("insert_vec_i64: " + err.Error())
	}
}

func insertVecI128(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_i128", args)
	n, err := readVecI128(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_i128: " + err.Error())
	}
	if _, err := vecI128Table.Insert(VecI128{N: n}); err != nil {
		spacetimedb.LogPanic("insert_vec_i128: " + err.Error())
	}
}

func insertVecI256(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_i256", args)
	n, err := readVecI256(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_i256: " + err.Error())
	}
	if _, err := vecI256Table.Insert(VecI256{N: n}); err != nil {
		spacetimedb.LogPanic("insert_vec_i256: " + err.Error())
	}
}

func insertVecBool(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_bool", args)
	b, err := readVecBool(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_bool: " + err.Error())
	}
	if _, err := vecBoolTable.Insert(VecBool{B: b}); err != nil {
		spacetimedb.LogPanic("insert_vec_bool: " + err.Error())
	}
}

func insertVecF32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_f32", args)
	f, err := readVecF32(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_f32: " + err.Error())
	}
	if _, err := vecF32Table.Insert(VecF32{F: f}); err != nil {
		spacetimedb.LogPanic("insert_vec_f32: " + err.Error())
	}
}

func insertVecF64(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_f64", args)
	f, err := readVecF64(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_f64: " + err.Error())
	}
	if _, err := vecF64Table.Insert(VecF64{F: f}); err != nil {
		spacetimedb.LogPanic("insert_vec_f64: " + err.Error())
	}
}

func insertVecString(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_string", args)
	s, err := readVecString(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_string: " + err.Error())
	}
	if _, err := vecStringTable.Insert(VecString{S: s}); err != nil {
		spacetimedb.LogPanic("insert_vec_string: " + err.Error())
	}
}

func insertVecIdentity(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_identity", args)
	i, err := readVecIdentity(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_identity: " + err.Error())
	}
	if _, err := vecIdentityTable.Insert(VecIdentity{I: i}); err != nil {
		spacetimedb.LogPanic("insert_vec_identity: " + err.Error())
	}
}

func insertVecConnectionId(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_connection_id", args)
	a, err := readVecConnectionId(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_connection_id: " + err.Error())
	}
	if _, err := vecConnectionIdTable.Insert(VecConnectionId{A: a}); err != nil {
		spacetimedb.LogPanic("insert_vec_connection_id: " + err.Error())
	}
}

func insertVecUuid(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_uuid", args)
	u, err := readVecUuid(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_uuid: " + err.Error())
	}
	if _, err := vecUuidTable.Insert(VecUuid{U: u}); err != nil {
		spacetimedb.LogPanic("insert_vec_uuid: " + err.Error())
	}
}

func insertVecTimestamp(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_timestamp", args)
	t, err := readVecTimestamp(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_timestamp: " + err.Error())
	}
	if _, err := vecTimestampTable.Insert(VecTimestamp{T: t}); err != nil {
		spacetimedb.LogPanic("insert_vec_timestamp: " + err.Error())
	}
}

func insertVecSimpleEnum(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_simple_enum", args)
	e, err := readVecSimpleEnum(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_simple_enum: " + err.Error())
	}
	if _, err := vecSimpleEnumTable.Insert(VecSimpleEnum{E: e}); err != nil {
		spacetimedb.LogPanic("insert_vec_simple_enum: " + err.Error())
	}
}

func insertVecEnumWithPayload(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_enum_with_payload", args)
	e, err := readVecEnumWithPayload(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_enum_with_payload: " + err.Error())
	}
	if _, err := vecEnumWithPayloadTable.Insert(VecEnumWithPayload{E: e}); err != nil {
		spacetimedb.LogPanic("insert_vec_enum_with_payload: " + err.Error())
	}
}

func insertVecUnitStruct(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_unit_struct", args)
	s, err := readVecUnitStruct(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_unit_struct: " + err.Error())
	}
	if _, err := vecUnitStructTable.Insert(VecUnitStruct{S: s}); err != nil {
		spacetimedb.LogPanic("insert_vec_unit_struct: " + err.Error())
	}
}

func insertVecByteStruct(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_byte_struct", args)
	s, err := readVecByteStruct(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_byte_struct: " + err.Error())
	}
	if _, err := vecByteStructTable.Insert(VecByteStruct{S: s}); err != nil {
		spacetimedb.LogPanic("insert_vec_byte_struct: " + err.Error())
	}
}

func insertVecEveryPrimitiveStruct(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_every_primitive_struct", args)
	s, err := readVecEveryPrimitiveStruct(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_every_primitive_struct: " + err.Error())
	}
	if _, err := vecEveryPrimitiveStructTable.Insert(VecEveryPrimitiveStruct{S: s}); err != nil {
		spacetimedb.LogPanic("insert_vec_every_primitive_struct: " + err.Error())
	}
}

func insertVecEveryVecStruct(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_vec_every_vec_struct", args)
	s, err := readVecEveryVecStruct(r)
	if err != nil {
		spacetimedb.LogPanic("insert_vec_every_vec_struct: " + err.Error())
	}
	if _, err := vecEveryVecStructTable.Insert(VecEveryVecStruct{S: s}); err != nil {
		spacetimedb.LogPanic("insert_vec_every_vec_struct: " + err.Error())
	}
}
