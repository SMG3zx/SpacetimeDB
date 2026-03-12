package main

import (
	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go/types"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
)

func registerPkReducers() {
	// Each PK table has 3 reducers (insert, update, delete), except PkSimpleEnum (only insert).
	// PK Update uses UniqueIndex.Update (generates update events).
	// PK Delete uses UniqueIndex.Delete (by key column).
	data32 := spacetimedb.ColumnDef{Name: "data", Type: types.AlgebraicI32}

	regR("insert_pk_u8", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU8}, data32}, insertPkU8)
	regR("update_pk_u8", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU8}, data32}, updatePkU8)
	regR("delete_pk_u8", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU8}}, deletePkU8)

	regR("insert_pk_u16", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU16}, data32}, insertPkU16)
	regR("update_pk_u16", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU16}, data32}, updatePkU16)
	regR("delete_pk_u16", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU16}}, deletePkU16)

	regR("insert_pk_u32", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU32}, data32}, insertPkU32)
	regR("update_pk_u32", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU32}, data32}, updatePkU32)
	regR("delete_pk_u32", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU32}}, deletePkU32)

	regR("insert_pk_u32_two", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU32}, data32}, insertPkU32Two)
	regR("update_pk_u32_two", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU32}, data32}, updatePkU32Two)
	regR("delete_pk_u32_two", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU32}}, deletePkU32Two)

	regR("insert_pk_u64", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU64}, data32}, insertPkU64)
	regR("update_pk_u64", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU64}, data32}, updatePkU64)
	regR("delete_pk_u64", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU64}}, deletePkU64)

	regR("insert_pk_u128", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU128}, data32}, insertPkU128)
	regR("update_pk_u128", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU128}, data32}, updatePkU128)
	regR("delete_pk_u128", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU128}}, deletePkU128)

	regR("insert_pk_u256", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU256}, data32}, insertPkU256)
	regR("update_pk_u256", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU256}, data32}, updatePkU256)
	regR("delete_pk_u256", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU256}}, deletePkU256)

	regR("insert_pk_i8", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI8}, data32}, insertPkI8)
	regR("update_pk_i8", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI8}, data32}, updatePkI8)
	regR("delete_pk_i8", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI8}}, deletePkI8)

	regR("insert_pk_i16", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI16}, data32}, insertPkI16)
	regR("update_pk_i16", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI16}, data32}, updatePkI16)
	regR("delete_pk_i16", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI16}}, deletePkI16)

	regR("insert_pk_i32", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI32}, data32}, insertPkI32)
	regR("update_pk_i32", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI32}, data32}, updatePkI32)
	regR("delete_pk_i32", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI32}}, deletePkI32)

	regR("insert_pk_i64", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI64}, data32}, insertPkI64)
	regR("update_pk_i64", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI64}, data32}, updatePkI64)
	regR("delete_pk_i64", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI64}}, deletePkI64)

	regR("insert_pk_i128", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI128}, data32}, insertPkI128)
	regR("update_pk_i128", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI128}, data32}, updatePkI128)
	regR("delete_pk_i128", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI128}}, deletePkI128)

	regR("insert_pk_i256", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI256}, data32}, insertPkI256)
	regR("update_pk_i256", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI256}, data32}, updatePkI256)
	regR("delete_pk_i256", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI256}}, deletePkI256)

	regR("insert_pk_bool", []spacetimedb.ColumnDef{{Name: "b", Type: types.AlgebraicBool}, data32}, insertPkBool)
	regR("update_pk_bool", []spacetimedb.ColumnDef{{Name: "b", Type: types.AlgebraicBool}, data32}, updatePkBool)
	regR("delete_pk_bool", []spacetimedb.ColumnDef{{Name: "b", Type: types.AlgebraicBool}}, deletePkBool)

	regR("insert_pk_string", []spacetimedb.ColumnDef{{Name: "s", Type: types.AlgebraicString}, data32}, insertPkString)
	regR("update_pk_string", []spacetimedb.ColumnDef{{Name: "s", Type: types.AlgebraicString}, data32}, updatePkString)
	regR("delete_pk_string", []spacetimedb.ColumnDef{{Name: "s", Type: types.AlgebraicString}}, deletePkString)

	regR("insert_pk_identity", []spacetimedb.ColumnDef{{Name: "i", Type: satIdentity}, data32}, insertPkIdentity)
	regR("update_pk_identity", []spacetimedb.ColumnDef{{Name: "i", Type: satIdentity}, data32}, updatePkIdentity)
	regR("delete_pk_identity", []spacetimedb.ColumnDef{{Name: "i", Type: satIdentity}}, deletePkIdentity)

	regR("insert_pk_connection_id", []spacetimedb.ColumnDef{{Name: "a", Type: satConnectionId}, data32}, insertPkConnectionId)
	regR("update_pk_connection_id", []spacetimedb.ColumnDef{{Name: "a", Type: satConnectionId}, data32}, updatePkConnectionId)
	regR("delete_pk_connection_id", []spacetimedb.ColumnDef{{Name: "a", Type: satConnectionId}}, deletePkConnectionId)

	regR("insert_pk_uuid", []spacetimedb.ColumnDef{{Name: "u", Type: satUuid}, data32}, insertPkUuid)
	regR("update_pk_uuid", []spacetimedb.ColumnDef{{Name: "u", Type: satUuid}, data32}, updatePkUuid)
	regR("delete_pk_uuid", []spacetimedb.ColumnDef{{Name: "u", Type: satUuid}}, deletePkUuid)

	// PkSimpleEnum: only insert (no update/delete in Rust source's define_tables! macro)
	regR("insert_pk_simple_enum", []spacetimedb.ColumnDef{{Name: "a", Type: satSimpleEnum}, data32}, insertPkSimpleEnum)
}

// ── PkU8 ──────────────────────────────────────────────────────────────────────

func insertPkU8(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_pk_u8", args)
	row, err := decodePkU8(r)
	if err != nil {
		spacetimedb.LogPanic("insert_pk_u8: " + err.Error())
	}
	if _, err := pkU8Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_pk_u8: " + err.Error())
	}
}

func updatePkU8(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_pk_u8", args)
	row, err := decodePkU8(r)
	if err != nil {
		spacetimedb.LogPanic("update_pk_u8: " + err.Error())
	}
	if _, err := pkU8NIdx.Update(row); err != nil {
		spacetimedb.LogPanic("update_pk_u8: " + err.Error())
	}
}

func deletePkU8(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_pk_u8", args)
	n, err := r.ReadU8()
	if err != nil {
		spacetimedb.LogPanic("delete_pk_u8: " + err.Error())
	}
	pkU8NIdx.Delete(n)
}

// ── PkU16 ─────────────────────────────────────────────────────────────────────

func insertPkU16(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_pk_u16", args)
	row, err := decodePkU16(r)
	if err != nil {
		spacetimedb.LogPanic("insert_pk_u16: " + err.Error())
	}
	if _, err := pkU16Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_pk_u16: " + err.Error())
	}
}

func updatePkU16(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_pk_u16", args)
	row, err := decodePkU16(r)
	if err != nil {
		spacetimedb.LogPanic("update_pk_u16: " + err.Error())
	}
	if _, err := pkU16NIdx.Update(row); err != nil {
		spacetimedb.LogPanic("update_pk_u16: " + err.Error())
	}
}

func deletePkU16(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_pk_u16", args)
	n, err := r.ReadU16()
	if err != nil {
		spacetimedb.LogPanic("delete_pk_u16: " + err.Error())
	}
	pkU16NIdx.Delete(n)
}

// ── PkU32 ─────────────────────────────────────────────────────────────────────

func insertPkU32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_pk_u32", args)
	row, err := decodePkU32(r)
	if err != nil {
		spacetimedb.LogPanic("insert_pk_u32: " + err.Error())
	}
	if _, err := pkU32Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_pk_u32: " + err.Error())
	}
}

func updatePkU32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_pk_u32", args)
	row, err := decodePkU32(r)
	if err != nil {
		spacetimedb.LogPanic("update_pk_u32: " + err.Error())
	}
	if _, err := pkU32NIdx.Update(row); err != nil {
		spacetimedb.LogPanic("update_pk_u32: " + err.Error())
	}
}

func deletePkU32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_pk_u32", args)
	n, err := r.ReadU32()
	if err != nil {
		spacetimedb.LogPanic("delete_pk_u32: " + err.Error())
	}
	pkU32NIdx.Delete(n)
}

// ── PkU32Two ──────────────────────────────────────────────────────────────────

func insertPkU32Two(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_pk_u32_two", args)
	row, err := decodePkU32Two(r)
	if err != nil {
		spacetimedb.LogPanic("insert_pk_u32_two: " + err.Error())
	}
	if _, err := pkU32TwoTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_pk_u32_two: " + err.Error())
	}
}

func updatePkU32Two(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_pk_u32_two", args)
	row, err := decodePkU32Two(r)
	if err != nil {
		spacetimedb.LogPanic("update_pk_u32_two: " + err.Error())
	}
	if _, err := pkU32TwoNIdx.Update(row); err != nil {
		spacetimedb.LogPanic("update_pk_u32_two: " + err.Error())
	}
}

func deletePkU32Two(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_pk_u32_two", args)
	n, err := r.ReadU32()
	if err != nil {
		spacetimedb.LogPanic("delete_pk_u32_two: " + err.Error())
	}
	pkU32TwoNIdx.Delete(n)
}

// ── PkU64 ─────────────────────────────────────────────────────────────────────

func insertPkU64(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_pk_u64", args)
	row, err := decodePkU64(r)
	if err != nil {
		spacetimedb.LogPanic("insert_pk_u64: " + err.Error())
	}
	if _, err := pkU64Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_pk_u64: " + err.Error())
	}
}

func updatePkU64(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_pk_u64", args)
	row, err := decodePkU64(r)
	if err != nil {
		spacetimedb.LogPanic("update_pk_u64: " + err.Error())
	}
	if _, err := pkU64NIdx.Update(row); err != nil {
		spacetimedb.LogPanic("update_pk_u64: " + err.Error())
	}
}

func deletePkU64(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_pk_u64", args)
	n, err := r.ReadU64()
	if err != nil {
		spacetimedb.LogPanic("delete_pk_u64: " + err.Error())
	}
	pkU64NIdx.Delete(n)
}

// ── PkU128 ────────────────────────────────────────────────────────────────────

func insertPkU128(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_pk_u128", args)
	row, err := decodePkU128(r)
	if err != nil {
		spacetimedb.LogPanic("insert_pk_u128: " + err.Error())
	}
	if _, err := pkU128Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_pk_u128: " + err.Error())
	}
}

func updatePkU128(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_pk_u128", args)
	row, err := decodePkU128(r)
	if err != nil {
		spacetimedb.LogPanic("update_pk_u128: " + err.Error())
	}
	if _, err := pkU128NIdx.Update(row); err != nil {
		spacetimedb.LogPanic("update_pk_u128: " + err.Error())
	}
}

func deletePkU128(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_pk_u128", args)
	n, err := types.ReadU128(r)
	if err != nil {
		spacetimedb.LogPanic("delete_pk_u128: " + err.Error())
	}
	pkU128NIdx.Delete(n)
}

// ── PkU256 ────────────────────────────────────────────────────────────────────

func insertPkU256(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_pk_u256", args)
	row, err := decodePkU256(r)
	if err != nil {
		spacetimedb.LogPanic("insert_pk_u256: " + err.Error())
	}
	if _, err := pkU256Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_pk_u256: " + err.Error())
	}
}

func updatePkU256(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_pk_u256", args)
	row, err := decodePkU256(r)
	if err != nil {
		spacetimedb.LogPanic("update_pk_u256: " + err.Error())
	}
	if _, err := pkU256NIdx.Update(row); err != nil {
		spacetimedb.LogPanic("update_pk_u256: " + err.Error())
	}
}

func deletePkU256(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_pk_u256", args)
	n, err := types.ReadU256(r)
	if err != nil {
		spacetimedb.LogPanic("delete_pk_u256: " + err.Error())
	}
	pkU256NIdx.Delete(n)
}

// ── PkI8 ──────────────────────────────────────────────────────────────────────

func insertPkI8(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_pk_i8", args)
	row, err := decodePkI8(r)
	if err != nil {
		spacetimedb.LogPanic("insert_pk_i8: " + err.Error())
	}
	if _, err := pkI8Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_pk_i8: " + err.Error())
	}
}

func updatePkI8(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_pk_i8", args)
	row, err := decodePkI8(r)
	if err != nil {
		spacetimedb.LogPanic("update_pk_i8: " + err.Error())
	}
	if _, err := pkI8NIdx.Update(row); err != nil {
		spacetimedb.LogPanic("update_pk_i8: " + err.Error())
	}
}

func deletePkI8(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_pk_i8", args)
	n, err := r.ReadI8()
	if err != nil {
		spacetimedb.LogPanic("delete_pk_i8: " + err.Error())
	}
	pkI8NIdx.Delete(n)
}

// ── PkI16 ─────────────────────────────────────────────────────────────────────

func insertPkI16(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_pk_i16", args)
	row, err := decodePkI16(r)
	if err != nil {
		spacetimedb.LogPanic("insert_pk_i16: " + err.Error())
	}
	if _, err := pkI16Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_pk_i16: " + err.Error())
	}
}

func updatePkI16(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_pk_i16", args)
	row, err := decodePkI16(r)
	if err != nil {
		spacetimedb.LogPanic("update_pk_i16: " + err.Error())
	}
	if _, err := pkI16NIdx.Update(row); err != nil {
		spacetimedb.LogPanic("update_pk_i16: " + err.Error())
	}
}

func deletePkI16(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_pk_i16", args)
	n, err := r.ReadI16()
	if err != nil {
		spacetimedb.LogPanic("delete_pk_i16: " + err.Error())
	}
	pkI16NIdx.Delete(n)
}

// ── PkI32 ─────────────────────────────────────────────────────────────────────

func insertPkI32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_pk_i32", args)
	row, err := decodePkI32(r)
	if err != nil {
		spacetimedb.LogPanic("insert_pk_i32: " + err.Error())
	}
	if _, err := pkI32Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_pk_i32: " + err.Error())
	}
}

func updatePkI32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_pk_i32", args)
	row, err := decodePkI32(r)
	if err != nil {
		spacetimedb.LogPanic("update_pk_i32: " + err.Error())
	}
	if _, err := pkI32NIdx.Update(row); err != nil {
		spacetimedb.LogPanic("update_pk_i32: " + err.Error())
	}
}

func deletePkI32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_pk_i32", args)
	n, err := r.ReadI32()
	if err != nil {
		spacetimedb.LogPanic("delete_pk_i32: " + err.Error())
	}
	pkI32NIdx.Delete(n)
}

// ── PkI64 ─────────────────────────────────────────────────────────────────────

func insertPkI64(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_pk_i64", args)
	row, err := decodePkI64(r)
	if err != nil {
		spacetimedb.LogPanic("insert_pk_i64: " + err.Error())
	}
	if _, err := pkI64Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_pk_i64: " + err.Error())
	}
}

func updatePkI64(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_pk_i64", args)
	row, err := decodePkI64(r)
	if err != nil {
		spacetimedb.LogPanic("update_pk_i64: " + err.Error())
	}
	if _, err := pkI64NIdx.Update(row); err != nil {
		spacetimedb.LogPanic("update_pk_i64: " + err.Error())
	}
}

func deletePkI64(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_pk_i64", args)
	n, err := r.ReadI64()
	if err != nil {
		spacetimedb.LogPanic("delete_pk_i64: " + err.Error())
	}
	pkI64NIdx.Delete(n)
}

// ── PkI128 ────────────────────────────────────────────────────────────────────

func insertPkI128(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_pk_i128", args)
	row, err := decodePkI128(r)
	if err != nil {
		spacetimedb.LogPanic("insert_pk_i128: " + err.Error())
	}
	if _, err := pkI128Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_pk_i128: " + err.Error())
	}
}

func updatePkI128(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_pk_i128", args)
	row, err := decodePkI128(r)
	if err != nil {
		spacetimedb.LogPanic("update_pk_i128: " + err.Error())
	}
	if _, err := pkI128NIdx.Update(row); err != nil {
		spacetimedb.LogPanic("update_pk_i128: " + err.Error())
	}
}

func deletePkI128(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_pk_i128", args)
	n, err := types.ReadI128(r)
	if err != nil {
		spacetimedb.LogPanic("delete_pk_i128: " + err.Error())
	}
	pkI128NIdx.Delete(n)
}

// ── PkI256 ────────────────────────────────────────────────────────────────────

func insertPkI256(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_pk_i256", args)
	row, err := decodePkI256(r)
	if err != nil {
		spacetimedb.LogPanic("insert_pk_i256: " + err.Error())
	}
	if _, err := pkI256Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_pk_i256: " + err.Error())
	}
}

func updatePkI256(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_pk_i256", args)
	row, err := decodePkI256(r)
	if err != nil {
		spacetimedb.LogPanic("update_pk_i256: " + err.Error())
	}
	if _, err := pkI256NIdx.Update(row); err != nil {
		spacetimedb.LogPanic("update_pk_i256: " + err.Error())
	}
}

func deletePkI256(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_pk_i256", args)
	n, err := types.ReadI256(r)
	if err != nil {
		spacetimedb.LogPanic("delete_pk_i256: " + err.Error())
	}
	pkI256NIdx.Delete(n)
}

// ── PkBool ────────────────────────────────────────────────────────────────────

func insertPkBool(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_pk_bool", args)
	row, err := decodePkBool(r)
	if err != nil {
		spacetimedb.LogPanic("insert_pk_bool: " + err.Error())
	}
	if _, err := pkBoolTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_pk_bool: " + err.Error())
	}
}

func updatePkBool(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_pk_bool", args)
	row, err := decodePkBool(r)
	if err != nil {
		spacetimedb.LogPanic("update_pk_bool: " + err.Error())
	}
	if _, err := pkBoolBIdx.Update(row); err != nil {
		spacetimedb.LogPanic("update_pk_bool: " + err.Error())
	}
}

func deletePkBool(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_pk_bool", args)
	b, err := r.ReadBool()
	if err != nil {
		spacetimedb.LogPanic("delete_pk_bool: " + err.Error())
	}
	pkBoolBIdx.Delete(b)
}

// ── PkString ──────────────────────────────────────────────────────────────────

func insertPkString(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_pk_string", args)
	row, err := decodePkString(r)
	if err != nil {
		spacetimedb.LogPanic("insert_pk_string: " + err.Error())
	}
	if _, err := pkStringTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_pk_string: " + err.Error())
	}
}

func updatePkString(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_pk_string", args)
	row, err := decodePkString(r)
	if err != nil {
		spacetimedb.LogPanic("update_pk_string: " + err.Error())
	}
	if _, err := pkStringSIdx.Update(row); err != nil {
		spacetimedb.LogPanic("update_pk_string: " + err.Error())
	}
}

func deletePkString(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_pk_string", args)
	s, err := r.ReadString()
	if err != nil {
		spacetimedb.LogPanic("delete_pk_string: " + err.Error())
	}
	pkStringSIdx.Delete(s)
}

// ── PkIdentity ────────────────────────────────────────────────────────────────

func insertPkIdentity(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_pk_identity", args)
	row, err := decodePkIdentity(r)
	if err != nil {
		spacetimedb.LogPanic("insert_pk_identity: " + err.Error())
	}
	if _, err := pkIdentityTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_pk_identity: " + err.Error())
	}
}

func updatePkIdentity(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_pk_identity", args)
	row, err := decodePkIdentity(r)
	if err != nil {
		spacetimedb.LogPanic("update_pk_identity: " + err.Error())
	}
	if _, err := pkIdentityIIdx.Update(row); err != nil {
		spacetimedb.LogPanic("update_pk_identity: " + err.Error())
	}
}

func deletePkIdentity(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_pk_identity", args)
	i, err := types.ReadIdentity(r)
	if err != nil {
		spacetimedb.LogPanic("delete_pk_identity: " + err.Error())
	}
	pkIdentityIIdx.Delete(i)
}

// ── PkConnectionId ────────────────────────────────────────────────────────────

func insertPkConnectionId(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_pk_connection_id", args)
	row, err := decodePkConnectionId(r)
	if err != nil {
		spacetimedb.LogPanic("insert_pk_connection_id: " + err.Error())
	}
	if _, err := pkConnectionIdTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_pk_connection_id: " + err.Error())
	}
}

func updatePkConnectionId(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_pk_connection_id", args)
	row, err := decodePkConnectionId(r)
	if err != nil {
		spacetimedb.LogPanic("update_pk_connection_id: " + err.Error())
	}
	if _, err := pkConnectionIdAIdx.Update(row); err != nil {
		spacetimedb.LogPanic("update_pk_connection_id: " + err.Error())
	}
}

func deletePkConnectionId(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_pk_connection_id", args)
	a, err := types.ReadConnectionId(r)
	if err != nil {
		spacetimedb.LogPanic("delete_pk_connection_id: " + err.Error())
	}
	pkConnectionIdAIdx.Delete(a)
}

// ── PkUuid ────────────────────────────────────────────────────────────────────

func insertPkUuid(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_pk_uuid", args)
	row, err := decodePkUuid(r)
	if err != nil {
		spacetimedb.LogPanic("insert_pk_uuid: " + err.Error())
	}
	if _, err := pkUuidTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_pk_uuid: " + err.Error())
	}
}

func updatePkUuid(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_pk_uuid", args)
	row, err := decodePkUuid(r)
	if err != nil {
		spacetimedb.LogPanic("update_pk_uuid: " + err.Error())
	}
	if _, err := pkUuidUIdx.Update(row); err != nil {
		spacetimedb.LogPanic("update_pk_uuid: " + err.Error())
	}
}

func deletePkUuid(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_pk_uuid", args)
	u, err := types.ReadUuid(r)
	if err != nil {
		spacetimedb.LogPanic("delete_pk_uuid: " + err.Error())
	}
	pkUuidUIdx.Delete(u)
}

// ── PkSimpleEnum (insert only) ────────────────────────────────────────────────

func insertPkSimpleEnum(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_pk_simple_enum", args)
	row, err := decodePkSimpleEnum(r)
	if err != nil {
		spacetimedb.LogPanic("insert_pk_simple_enum: " + err.Error())
	}
	if _, err := pkSimpleEnumTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_pk_simple_enum: " + err.Error())
	}
}
