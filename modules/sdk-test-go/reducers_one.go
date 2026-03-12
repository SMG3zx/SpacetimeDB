package main

import (
	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go/types"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
)

func registerOneReducers() {
	regR("insert_one_u8", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU8}}, insertOneU8)
	regR("insert_one_u16", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU16}}, insertOneU16)
	regR("insert_one_u32", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU32}}, insertOneU32)
	regR("insert_one_u64", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU64}}, insertOneU64)
	regR("insert_one_u128", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU128}}, insertOneU128)
	regR("insert_one_u256", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU256}}, insertOneU256)
	regR("insert_one_i8", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI8}}, insertOneI8)
	regR("insert_one_i16", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI16}}, insertOneI16)
	regR("insert_one_i32", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI32}}, insertOneI32)
	regR("insert_one_i64", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI64}}, insertOneI64)
	regR("insert_one_i128", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI128}}, insertOneI128)
	regR("insert_one_i256", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI256}}, insertOneI256)
	regR("insert_one_bool", []spacetimedb.ColumnDef{{Name: "b", Type: types.AlgebraicBool}}, insertOneBool)
	regR("insert_one_f32", []spacetimedb.ColumnDef{{Name: "f", Type: types.AlgebraicF32}}, insertOneF32)
	regR("insert_one_f64", []spacetimedb.ColumnDef{{Name: "f", Type: types.AlgebraicF64}}, insertOneF64)
	regR("insert_one_string", []spacetimedb.ColumnDef{{Name: "s", Type: types.AlgebraicString}}, insertOneString)
	regR("insert_one_identity", []spacetimedb.ColumnDef{{Name: "i", Type: satIdentity}}, insertOneIdentity)
	regR("insert_one_connection_id", []spacetimedb.ColumnDef{{Name: "a", Type: satConnectionId}}, insertOneConnectionId)
	regR("insert_one_uuid", []spacetimedb.ColumnDef{{Name: "u", Type: satUuid}}, insertOneUuid)
	regR("insert_one_timestamp", []spacetimedb.ColumnDef{{Name: "t", Type: types.AlgebraicTimestamp}}, insertOneTimestamp)
	regR("insert_one_simple_enum", []spacetimedb.ColumnDef{{Name: "e", Type: satSimpleEnum}}, insertOneSimpleEnum)
	regR("insert_one_enum_with_payload", []spacetimedb.ColumnDef{{Name: "e", Type: satEnumWithPayload}}, insertOneEnumWithPayload)
	regR("insert_one_unit_struct", []spacetimedb.ColumnDef{{Name: "s", Type: satUnitStruct}}, insertOneUnitStruct)
	regR("insert_one_byte_struct", []spacetimedb.ColumnDef{{Name: "s", Type: satByteStruct}}, insertOneByteStruct)
	regR("insert_one_every_primitive_struct", []spacetimedb.ColumnDef{{Name: "s", Type: satEveryPrimitiveStruct}}, insertOneEveryPrimitiveStruct)
	regR("insert_one_every_vec_struct", []spacetimedb.ColumnDef{{Name: "s", Type: satEveryVecStruct}}, insertOneEveryVecStruct)
}

// ── One* reducer handlers ─────────────────────────────────────────────────────

func insertOneU8(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_u8", args)
	n, err := r.ReadU8()
	if err != nil {
		spacetimedb.LogPanic("insert_one_u8: " + err.Error())
	}
	if _, err := oneU8Table.Insert(OneU8{N: n}); err != nil {
		spacetimedb.LogPanic("insert_one_u8: " + err.Error())
	}
}

func insertOneU16(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_u16", args)
	n, err := r.ReadU16()
	if err != nil {
		spacetimedb.LogPanic("insert_one_u16: " + err.Error())
	}
	if _, err := oneU16Table.Insert(OneU16{N: n}); err != nil {
		spacetimedb.LogPanic("insert_one_u16: " + err.Error())
	}
}

func insertOneU32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_u32", args)
	n, err := r.ReadU32()
	if err != nil {
		spacetimedb.LogPanic("insert_one_u32: " + err.Error())
	}
	if _, err := oneU32Table.Insert(OneU32{N: n}); err != nil {
		spacetimedb.LogPanic("insert_one_u32: " + err.Error())
	}
}

func insertOneU64(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_u64", args)
	n, err := r.ReadU64()
	if err != nil {
		spacetimedb.LogPanic("insert_one_u64: " + err.Error())
	}
	if _, err := oneU64Table.Insert(OneU64{N: n}); err != nil {
		spacetimedb.LogPanic("insert_one_u64: " + err.Error())
	}
}

func insertOneU128(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_u128", args)
	n, err := types.ReadU128(r)
	if err != nil {
		spacetimedb.LogPanic("insert_one_u128: " + err.Error())
	}
	if _, err := oneU128Table.Insert(OneU128{N: n}); err != nil {
		spacetimedb.LogPanic("insert_one_u128: " + err.Error())
	}
}

func insertOneU256(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_u256", args)
	n, err := types.ReadU256(r)
	if err != nil {
		spacetimedb.LogPanic("insert_one_u256: " + err.Error())
	}
	if _, err := oneU256Table.Insert(OneU256{N: n}); err != nil {
		spacetimedb.LogPanic("insert_one_u256: " + err.Error())
	}
}

func insertOneI8(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_i8", args)
	n, err := r.ReadI8()
	if err != nil {
		spacetimedb.LogPanic("insert_one_i8: " + err.Error())
	}
	if _, err := oneI8Table.Insert(OneI8{N: n}); err != nil {
		spacetimedb.LogPanic("insert_one_i8: " + err.Error())
	}
}

func insertOneI16(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_i16", args)
	n, err := r.ReadI16()
	if err != nil {
		spacetimedb.LogPanic("insert_one_i16: " + err.Error())
	}
	if _, err := oneI16Table.Insert(OneI16{N: n}); err != nil {
		spacetimedb.LogPanic("insert_one_i16: " + err.Error())
	}
}

func insertOneI32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_i32", args)
	n, err := r.ReadI32()
	if err != nil {
		spacetimedb.LogPanic("insert_one_i32: " + err.Error())
	}
	if _, err := oneI32Table.Insert(OneI32{N: n}); err != nil {
		spacetimedb.LogPanic("insert_one_i32: " + err.Error())
	}
}

func insertOneI64(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_i64", args)
	n, err := r.ReadI64()
	if err != nil {
		spacetimedb.LogPanic("insert_one_i64: " + err.Error())
	}
	if _, err := oneI64Table.Insert(OneI64{N: n}); err != nil {
		spacetimedb.LogPanic("insert_one_i64: " + err.Error())
	}
}

func insertOneI128(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_i128", args)
	n, err := types.ReadI128(r)
	if err != nil {
		spacetimedb.LogPanic("insert_one_i128: " + err.Error())
	}
	if _, err := oneI128Table.Insert(OneI128{N: n}); err != nil {
		spacetimedb.LogPanic("insert_one_i128: " + err.Error())
	}
}

func insertOneI256(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_i256", args)
	n, err := types.ReadI256(r)
	if err != nil {
		spacetimedb.LogPanic("insert_one_i256: " + err.Error())
	}
	if _, err := oneI256Table.Insert(OneI256{N: n}); err != nil {
		spacetimedb.LogPanic("insert_one_i256: " + err.Error())
	}
}

func insertOneBool(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_bool", args)
	b, err := r.ReadBool()
	if err != nil {
		spacetimedb.LogPanic("insert_one_bool: " + err.Error())
	}
	if _, err := oneBoolTable.Insert(OneBool{B: b}); err != nil {
		spacetimedb.LogPanic("insert_one_bool: " + err.Error())
	}
}

func insertOneF32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_f32", args)
	f, err := r.ReadF32()
	if err != nil {
		spacetimedb.LogPanic("insert_one_f32: " + err.Error())
	}
	if _, err := oneF32Table.Insert(OneF32{F: f}); err != nil {
		spacetimedb.LogPanic("insert_one_f32: " + err.Error())
	}
}

func insertOneF64(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_f64", args)
	f, err := r.ReadF64()
	if err != nil {
		spacetimedb.LogPanic("insert_one_f64: " + err.Error())
	}
	if _, err := oneF64Table.Insert(OneF64{F: f}); err != nil {
		spacetimedb.LogPanic("insert_one_f64: " + err.Error())
	}
}

func insertOneString(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_string", args)
	s, err := r.ReadString()
	if err != nil {
		spacetimedb.LogPanic("insert_one_string: " + err.Error())
	}
	if _, err := oneStringTable.Insert(OneString{S: s}); err != nil {
		spacetimedb.LogPanic("insert_one_string: " + err.Error())
	}
}

func insertOneIdentity(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_identity", args)
	i, err := types.ReadIdentity(r)
	if err != nil {
		spacetimedb.LogPanic("insert_one_identity: " + err.Error())
	}
	if _, err := oneIdentityTable.Insert(OneIdentity{I: i}); err != nil {
		spacetimedb.LogPanic("insert_one_identity: " + err.Error())
	}
}

func insertOneConnectionId(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_connection_id", args)
	a, err := types.ReadConnectionId(r)
	if err != nil {
		spacetimedb.LogPanic("insert_one_connection_id: " + err.Error())
	}
	if _, err := oneConnectionIdTable.Insert(OneConnectionId{A: a}); err != nil {
		spacetimedb.LogPanic("insert_one_connection_id: " + err.Error())
	}
}

func insertOneUuid(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_uuid", args)
	u, err := types.ReadUuid(r)
	if err != nil {
		spacetimedb.LogPanic("insert_one_uuid: " + err.Error())
	}
	if _, err := oneUuidTable.Insert(OneUuid{U: u}); err != nil {
		spacetimedb.LogPanic("insert_one_uuid: " + err.Error())
	}
}

func insertOneTimestamp(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_timestamp", args)
	t, err := types.ReadTimestamp(r)
	if err != nil {
		spacetimedb.LogPanic("insert_one_timestamp: " + err.Error())
	}
	if _, err := oneTimestampTable.Insert(OneTimestamp{T: t}); err != nil {
		spacetimedb.LogPanic("insert_one_timestamp: " + err.Error())
	}
}

func insertOneSimpleEnum(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_simple_enum", args)
	e, err := decodeSimpleEnum(r)
	if err != nil {
		spacetimedb.LogPanic("insert_one_simple_enum: " + err.Error())
	}
	if _, err := oneSimpleEnumTable.Insert(OneSimpleEnum{E: e}); err != nil {
		spacetimedb.LogPanic("insert_one_simple_enum: " + err.Error())
	}
}

func insertOneEnumWithPayload(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_enum_with_payload", args)
	e, err := decodeEnumWithPayload(r)
	if err != nil {
		spacetimedb.LogPanic("insert_one_enum_with_payload: " + err.Error())
	}
	if _, err := oneEnumWithPayloadTable.Insert(OneEnumWithPayload{E: e}); err != nil {
		spacetimedb.LogPanic("insert_one_enum_with_payload: " + err.Error())
	}
}

func insertOneUnitStruct(_ spacetimedb.ReducerContext, _ sys.BytesSource) {
	// UnitStruct has no fields, no args to read.
	if _, err := oneUnitStructTable.Insert(OneUnitStruct{S: UnitStruct{}}); err != nil {
		spacetimedb.LogPanic("insert_one_unit_struct: " + err.Error())
	}
}

func insertOneByteStruct(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_byte_struct", args)
	s, err := decodeByteStruct(r)
	if err != nil {
		spacetimedb.LogPanic("insert_one_byte_struct: " + err.Error())
	}
	if _, err := oneByteStructTable.Insert(OneByteStruct{S: s}); err != nil {
		spacetimedb.LogPanic("insert_one_byte_struct: " + err.Error())
	}
}

func insertOneEveryPrimitiveStruct(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_every_primitive_struct", args)
	s, err := decodeEveryPrimitiveStruct(r)
	if err != nil {
		spacetimedb.LogPanic("insert_one_every_primitive_struct: " + err.Error())
	}
	if _, err := oneEveryPrimitiveStructTable.Insert(OneEveryPrimitiveStruct{S: s}); err != nil {
		spacetimedb.LogPanic("insert_one_every_primitive_struct: " + err.Error())
	}
}

func insertOneEveryVecStruct(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_one_every_vec_struct", args)
	s, err := decodeEveryVecStruct(r)
	if err != nil {
		spacetimedb.LogPanic("insert_one_every_vec_struct: " + err.Error())
	}
	if _, err := oneEveryVecStructTable.Insert(OneEveryVecStruct{S: s}); err != nil {
		spacetimedb.LogPanic("insert_one_every_vec_struct: " + err.Error())
	}
}

