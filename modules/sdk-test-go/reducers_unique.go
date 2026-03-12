package main

import (
	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go/types"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
)

func registerUniqueReducers() {
	// Each Unique table has 3 reducers: insert, update (delete+insert), delete_by_key.
	// Column names: numeric→"n", bool→"b", string→"s", identity→"i", conn_id→"a", uuid→"u".
	data32 := spacetimedb.ColumnDef{Name: "data", Type: types.AlgebraicI32}

	regR("insert_unique_u8", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU8}, data32}, insertUniqueU8)
	regR("update_unique_u8", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU8}, data32}, updateUniqueU8)
	regR("delete_unique_u8", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU8}}, deleteUniqueU8)

	regR("insert_unique_u16", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU16}, data32}, insertUniqueU16)
	regR("update_unique_u16", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU16}, data32}, updateUniqueU16)
	regR("delete_unique_u16", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU16}}, deleteUniqueU16)

	regR("insert_unique_u32", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU32}, data32}, insertUniqueU32)
	regR("update_unique_u32", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU32}, data32}, updateUniqueU32)
	regR("delete_unique_u32", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU32}}, deleteUniqueU32)

	regR("insert_unique_u64", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU64}, data32}, insertUniqueU64)
	regR("update_unique_u64", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU64}, data32}, updateUniqueU64)
	regR("delete_unique_u64", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU64}}, deleteUniqueU64)

	regR("insert_unique_u128", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU128}, data32}, insertUniqueU128)
	regR("update_unique_u128", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU128}, data32}, updateUniqueU128)
	regR("delete_unique_u128", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU128}}, deleteUniqueU128)

	regR("insert_unique_u256", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU256}, data32}, insertUniqueU256)
	regR("update_unique_u256", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU256}, data32}, updateUniqueU256)
	regR("delete_unique_u256", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU256}}, deleteUniqueU256)

	regR("insert_unique_i8", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI8}, data32}, insertUniqueI8)
	regR("update_unique_i8", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI8}, data32}, updateUniqueI8)
	regR("delete_unique_i8", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI8}}, deleteUniqueI8)

	regR("insert_unique_i16", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI16}, data32}, insertUniqueI16)
	regR("update_unique_i16", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI16}, data32}, updateUniqueI16)
	regR("delete_unique_i16", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI16}}, deleteUniqueI16)

	regR("insert_unique_i32", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI32}, data32}, insertUniqueI32)
	regR("update_unique_i32", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI32}, data32}, updateUniqueI32)
	regR("delete_unique_i32", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI32}}, deleteUniqueI32)

	regR("insert_unique_i64", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI64}, data32}, insertUniqueI64)
	regR("update_unique_i64", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI64}, data32}, updateUniqueI64)
	regR("delete_unique_i64", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI64}}, deleteUniqueI64)

	regR("insert_unique_i128", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI128}, data32}, insertUniqueI128)
	regR("update_unique_i128", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI128}, data32}, updateUniqueI128)
	regR("delete_unique_i128", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI128}}, deleteUniqueI128)

	regR("insert_unique_i256", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI256}, data32}, insertUniqueI256)
	regR("update_unique_i256", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI256}, data32}, updateUniqueI256)
	regR("delete_unique_i256", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI256}}, deleteUniqueI256)

	regR("insert_unique_bool", []spacetimedb.ColumnDef{{Name: "b", Type: types.AlgebraicBool}, data32}, insertUniqueBool)
	regR("update_unique_bool", []spacetimedb.ColumnDef{{Name: "b", Type: types.AlgebraicBool}, data32}, updateUniqueBool)
	regR("delete_unique_bool", []spacetimedb.ColumnDef{{Name: "b", Type: types.AlgebraicBool}}, deleteUniqueBool)

	regR("insert_unique_string", []spacetimedb.ColumnDef{{Name: "s", Type: types.AlgebraicString}, data32}, insertUniqueString)
	regR("update_unique_string", []spacetimedb.ColumnDef{{Name: "s", Type: types.AlgebraicString}, data32}, updateUniqueString)
	regR("delete_unique_string", []spacetimedb.ColumnDef{{Name: "s", Type: types.AlgebraicString}}, deleteUniqueString)

	regR("insert_unique_identity", []spacetimedb.ColumnDef{{Name: "i", Type: satIdentity}, data32}, insertUniqueIdentity)
	regR("update_unique_identity", []spacetimedb.ColumnDef{{Name: "i", Type: satIdentity}, data32}, updateUniqueIdentity)
	regR("delete_unique_identity", []spacetimedb.ColumnDef{{Name: "i", Type: satIdentity}}, deleteUniqueIdentity)

	regR("insert_unique_connection_id", []spacetimedb.ColumnDef{{Name: "a", Type: satConnectionId}, data32}, insertUniqueConnectionId)
	regR("update_unique_connection_id", []spacetimedb.ColumnDef{{Name: "a", Type: satConnectionId}, data32}, updateUniqueConnectionId)
	regR("delete_unique_connection_id", []spacetimedb.ColumnDef{{Name: "a", Type: satConnectionId}}, deleteUniqueConnectionId)

	regR("insert_unique_uuid", []spacetimedb.ColumnDef{{Name: "u", Type: satUuid}, data32}, insertUniqueUuid)
	regR("update_unique_uuid", []spacetimedb.ColumnDef{{Name: "u", Type: satUuid}, data32}, updateUniqueUuid)
	regR("delete_unique_uuid", []spacetimedb.ColumnDef{{Name: "u", Type: satUuid}}, deleteUniqueUuid)
}

// ── UniqueU8 ──────────────────────────────────────────────────────────────────

func insertUniqueU8(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_unique_u8", args)
	row, err := decodeUniqueU8(r)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_u8: " + err.Error())
	}
	if _, err := uniqueU8Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_unique_u8: " + err.Error())
	}
}

func updateUniqueU8(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_unique_u8", args)
	row, err := decodeUniqueU8(r)
	if err != nil {
		spacetimedb.LogPanic("update_unique_u8: " + err.Error())
	}
	uniqueU8NIdx.Delete(row.N)
	if _, err := uniqueU8Table.Insert(row); err != nil {
		spacetimedb.LogPanic("update_unique_u8: " + err.Error())
	}
}

func deleteUniqueU8(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_unique_u8", args)
	n, err := r.ReadU8()
	if err != nil {
		spacetimedb.LogPanic("delete_unique_u8: " + err.Error())
	}
	uniqueU8NIdx.Delete(n)
}

// ── UniqueU16 ─────────────────────────────────────────────────────────────────

func insertUniqueU16(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_unique_u16", args)
	row, err := decodeUniqueU16(r)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_u16: " + err.Error())
	}
	if _, err := uniqueU16Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_unique_u16: " + err.Error())
	}
}

func updateUniqueU16(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_unique_u16", args)
	row, err := decodeUniqueU16(r)
	if err != nil {
		spacetimedb.LogPanic("update_unique_u16: " + err.Error())
	}
	uniqueU16NIdx.Delete(row.N)
	if _, err := uniqueU16Table.Insert(row); err != nil {
		spacetimedb.LogPanic("update_unique_u16: " + err.Error())
	}
}

func deleteUniqueU16(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_unique_u16", args)
	n, err := r.ReadU16()
	if err != nil {
		spacetimedb.LogPanic("delete_unique_u16: " + err.Error())
	}
	uniqueU16NIdx.Delete(n)
}

// ── UniqueU32 ─────────────────────────────────────────────────────────────────

func insertUniqueU32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_unique_u32", args)
	row, err := decodeUniqueU32(r)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_u32: " + err.Error())
	}
	if _, err := uniqueU32Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_unique_u32: " + err.Error())
	}
}

func updateUniqueU32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_unique_u32", args)
	row, err := decodeUniqueU32(r)
	if err != nil {
		spacetimedb.LogPanic("update_unique_u32: " + err.Error())
	}
	uniqueU32NIdx.Delete(row.N)
	if _, err := uniqueU32Table.Insert(row); err != nil {
		spacetimedb.LogPanic("update_unique_u32: " + err.Error())
	}
}

func deleteUniqueU32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_unique_u32", args)
	n, err := r.ReadU32()
	if err != nil {
		spacetimedb.LogPanic("delete_unique_u32: " + err.Error())
	}
	uniqueU32NIdx.Delete(n)
}

// ── UniqueU64 ─────────────────────────────────────────────────────────────────

func insertUniqueU64(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_unique_u64", args)
	row, err := decodeUniqueU64(r)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_u64: " + err.Error())
	}
	if _, err := uniqueU64Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_unique_u64: " + err.Error())
	}
}

func updateUniqueU64(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_unique_u64", args)
	row, err := decodeUniqueU64(r)
	if err != nil {
		spacetimedb.LogPanic("update_unique_u64: " + err.Error())
	}
	uniqueU64NIdx.Delete(row.N)
	if _, err := uniqueU64Table.Insert(row); err != nil {
		spacetimedb.LogPanic("update_unique_u64: " + err.Error())
	}
}

func deleteUniqueU64(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_unique_u64", args)
	n, err := r.ReadU64()
	if err != nil {
		spacetimedb.LogPanic("delete_unique_u64: " + err.Error())
	}
	uniqueU64NIdx.Delete(n)
}

// ── UniqueU128 ────────────────────────────────────────────────────────────────

func insertUniqueU128(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_unique_u128", args)
	row, err := decodeUniqueU128(r)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_u128: " + err.Error())
	}
	if _, err := uniqueU128Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_unique_u128: " + err.Error())
	}
}

func updateUniqueU128(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_unique_u128", args)
	row, err := decodeUniqueU128(r)
	if err != nil {
		spacetimedb.LogPanic("update_unique_u128: " + err.Error())
	}
	uniqueU128NIdx.Delete(row.N)
	if _, err := uniqueU128Table.Insert(row); err != nil {
		spacetimedb.LogPanic("update_unique_u128: " + err.Error())
	}
}

func deleteUniqueU128(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_unique_u128", args)
	n, err := types.ReadU128(r)
	if err != nil {
		spacetimedb.LogPanic("delete_unique_u128: " + err.Error())
	}
	uniqueU128NIdx.Delete(n)
}

// ── UniqueU256 ────────────────────────────────────────────────────────────────

func insertUniqueU256(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_unique_u256", args)
	row, err := decodeUniqueU256(r)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_u256: " + err.Error())
	}
	if _, err := uniqueU256Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_unique_u256: " + err.Error())
	}
}

func updateUniqueU256(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_unique_u256", args)
	row, err := decodeUniqueU256(r)
	if err != nil {
		spacetimedb.LogPanic("update_unique_u256: " + err.Error())
	}
	uniqueU256NIdx.Delete(row.N)
	if _, err := uniqueU256Table.Insert(row); err != nil {
		spacetimedb.LogPanic("update_unique_u256: " + err.Error())
	}
}

func deleteUniqueU256(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_unique_u256", args)
	n, err := types.ReadU256(r)
	if err != nil {
		spacetimedb.LogPanic("delete_unique_u256: " + err.Error())
	}
	uniqueU256NIdx.Delete(n)
}

// ── UniqueI8 ──────────────────────────────────────────────────────────────────

func insertUniqueI8(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_unique_i8", args)
	row, err := decodeUniqueI8(r)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_i8: " + err.Error())
	}
	if _, err := uniqueI8Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_unique_i8: " + err.Error())
	}
}

func updateUniqueI8(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_unique_i8", args)
	row, err := decodeUniqueI8(r)
	if err != nil {
		spacetimedb.LogPanic("update_unique_i8: " + err.Error())
	}
	uniqueI8NIdx.Delete(row.N)
	if _, err := uniqueI8Table.Insert(row); err != nil {
		spacetimedb.LogPanic("update_unique_i8: " + err.Error())
	}
}

func deleteUniqueI8(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_unique_i8", args)
	n, err := r.ReadI8()
	if err != nil {
		spacetimedb.LogPanic("delete_unique_i8: " + err.Error())
	}
	uniqueI8NIdx.Delete(n)
}

// ── UniqueI16 ─────────────────────────────────────────────────────────────────

func insertUniqueI16(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_unique_i16", args)
	row, err := decodeUniqueI16(r)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_i16: " + err.Error())
	}
	if _, err := uniqueI16Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_unique_i16: " + err.Error())
	}
}

func updateUniqueI16(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_unique_i16", args)
	row, err := decodeUniqueI16(r)
	if err != nil {
		spacetimedb.LogPanic("update_unique_i16: " + err.Error())
	}
	uniqueI16NIdx.Delete(row.N)
	if _, err := uniqueI16Table.Insert(row); err != nil {
		spacetimedb.LogPanic("update_unique_i16: " + err.Error())
	}
}

func deleteUniqueI16(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_unique_i16", args)
	n, err := r.ReadI16()
	if err != nil {
		spacetimedb.LogPanic("delete_unique_i16: " + err.Error())
	}
	uniqueI16NIdx.Delete(n)
}

// ── UniqueI32 ─────────────────────────────────────────────────────────────────

func insertUniqueI32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_unique_i32", args)
	row, err := decodeUniqueI32(r)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_i32: " + err.Error())
	}
	if _, err := uniqueI32Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_unique_i32: " + err.Error())
	}
}

func updateUniqueI32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_unique_i32", args)
	row, err := decodeUniqueI32(r)
	if err != nil {
		spacetimedb.LogPanic("update_unique_i32: " + err.Error())
	}
	uniqueI32NIdx.Delete(row.N)
	if _, err := uniqueI32Table.Insert(row); err != nil {
		spacetimedb.LogPanic("update_unique_i32: " + err.Error())
	}
}

func deleteUniqueI32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_unique_i32", args)
	n, err := r.ReadI32()
	if err != nil {
		spacetimedb.LogPanic("delete_unique_i32: " + err.Error())
	}
	uniqueI32NIdx.Delete(n)
}

// ── UniqueI64 ─────────────────────────────────────────────────────────────────

func insertUniqueI64(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_unique_i64", args)
	row, err := decodeUniqueI64(r)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_i64: " + err.Error())
	}
	if _, err := uniqueI64Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_unique_i64: " + err.Error())
	}
}

func updateUniqueI64(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_unique_i64", args)
	row, err := decodeUniqueI64(r)
	if err != nil {
		spacetimedb.LogPanic("update_unique_i64: " + err.Error())
	}
	uniqueI64NIdx.Delete(row.N)
	if _, err := uniqueI64Table.Insert(row); err != nil {
		spacetimedb.LogPanic("update_unique_i64: " + err.Error())
	}
}

func deleteUniqueI64(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_unique_i64", args)
	n, err := r.ReadI64()
	if err != nil {
		spacetimedb.LogPanic("delete_unique_i64: " + err.Error())
	}
	uniqueI64NIdx.Delete(n)
}

// ── UniqueI128 ────────────────────────────────────────────────────────────────

func insertUniqueI128(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_unique_i128", args)
	row, err := decodeUniqueI128(r)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_i128: " + err.Error())
	}
	if _, err := uniqueI128Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_unique_i128: " + err.Error())
	}
}

func updateUniqueI128(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_unique_i128", args)
	row, err := decodeUniqueI128(r)
	if err != nil {
		spacetimedb.LogPanic("update_unique_i128: " + err.Error())
	}
	uniqueI128NIdx.Delete(row.N)
	if _, err := uniqueI128Table.Insert(row); err != nil {
		spacetimedb.LogPanic("update_unique_i128: " + err.Error())
	}
}

func deleteUniqueI128(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_unique_i128", args)
	n, err := types.ReadI128(r)
	if err != nil {
		spacetimedb.LogPanic("delete_unique_i128: " + err.Error())
	}
	uniqueI128NIdx.Delete(n)
}

// ── UniqueI256 ────────────────────────────────────────────────────────────────

func insertUniqueI256(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_unique_i256", args)
	row, err := decodeUniqueI256(r)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_i256: " + err.Error())
	}
	if _, err := uniqueI256Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_unique_i256: " + err.Error())
	}
}

func updateUniqueI256(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_unique_i256", args)
	row, err := decodeUniqueI256(r)
	if err != nil {
		spacetimedb.LogPanic("update_unique_i256: " + err.Error())
	}
	uniqueI256NIdx.Delete(row.N)
	if _, err := uniqueI256Table.Insert(row); err != nil {
		spacetimedb.LogPanic("update_unique_i256: " + err.Error())
	}
}

func deleteUniqueI256(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_unique_i256", args)
	n, err := types.ReadI256(r)
	if err != nil {
		spacetimedb.LogPanic("delete_unique_i256: " + err.Error())
	}
	uniqueI256NIdx.Delete(n)
}

// ── UniqueBool ────────────────────────────────────────────────────────────────

func insertUniqueBool(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_unique_bool", args)
	row, err := decodeUniqueBool(r)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_bool: " + err.Error())
	}
	if _, err := uniqueBoolTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_unique_bool: " + err.Error())
	}
}

func updateUniqueBool(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_unique_bool", args)
	row, err := decodeUniqueBool(r)
	if err != nil {
		spacetimedb.LogPanic("update_unique_bool: " + err.Error())
	}
	uniqueBoolBIdx.Delete(row.B)
	if _, err := uniqueBoolTable.Insert(row); err != nil {
		spacetimedb.LogPanic("update_unique_bool: " + err.Error())
	}
}

func deleteUniqueBool(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_unique_bool", args)
	b, err := r.ReadBool()
	if err != nil {
		spacetimedb.LogPanic("delete_unique_bool: " + err.Error())
	}
	uniqueBoolBIdx.Delete(b)
}

// ── UniqueString ──────────────────────────────────────────────────────────────

func insertUniqueString(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_unique_string", args)
	row, err := decodeUniqueString(r)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_string: " + err.Error())
	}
	if _, err := uniqueStringTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_unique_string: " + err.Error())
	}
}

func updateUniqueString(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_unique_string", args)
	row, err := decodeUniqueString(r)
	if err != nil {
		spacetimedb.LogPanic("update_unique_string: " + err.Error())
	}
	uniqueStringSIdx.Delete(row.S)
	if _, err := uniqueStringTable.Insert(row); err != nil {
		spacetimedb.LogPanic("update_unique_string: " + err.Error())
	}
}

func deleteUniqueString(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_unique_string", args)
	s, err := r.ReadString()
	if err != nil {
		spacetimedb.LogPanic("delete_unique_string: " + err.Error())
	}
	uniqueStringSIdx.Delete(s)
}

// ── UniqueIdentity ────────────────────────────────────────────────────────────

func insertUniqueIdentity(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_unique_identity", args)
	row, err := decodeUniqueIdentity(r)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_identity: " + err.Error())
	}
	if _, err := uniqueIdentityTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_unique_identity: " + err.Error())
	}
}

func updateUniqueIdentity(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_unique_identity", args)
	row, err := decodeUniqueIdentity(r)
	if err != nil {
		spacetimedb.LogPanic("update_unique_identity: " + err.Error())
	}
	uniqueIdentityIIdx.Delete(row.I)
	if _, err := uniqueIdentityTable.Insert(row); err != nil {
		spacetimedb.LogPanic("update_unique_identity: " + err.Error())
	}
}

func deleteUniqueIdentity(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_unique_identity", args)
	i, err := types.ReadIdentity(r)
	if err != nil {
		spacetimedb.LogPanic("delete_unique_identity: " + err.Error())
	}
	uniqueIdentityIIdx.Delete(i)
}

// ── UniqueConnectionId ────────────────────────────────────────────────────────

func insertUniqueConnectionId(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_unique_connection_id", args)
	row, err := decodeUniqueConnectionId(r)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_connection_id: " + err.Error())
	}
	if _, err := uniqueConnectionIdTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_unique_connection_id: " + err.Error())
	}
}

func updateUniqueConnectionId(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_unique_connection_id", args)
	row, err := decodeUniqueConnectionId(r)
	if err != nil {
		spacetimedb.LogPanic("update_unique_connection_id: " + err.Error())
	}
	uniqueConnectionIdAIdx.Delete(row.A)
	if _, err := uniqueConnectionIdTable.Insert(row); err != nil {
		spacetimedb.LogPanic("update_unique_connection_id: " + err.Error())
	}
}

func deleteUniqueConnectionId(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_unique_connection_id", args)
	a, err := types.ReadConnectionId(r)
	if err != nil {
		spacetimedb.LogPanic("delete_unique_connection_id: " + err.Error())
	}
	uniqueConnectionIdAIdx.Delete(a)
}

// ── UniqueUuid ────────────────────────────────────────────────────────────────

func insertUniqueUuid(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_unique_uuid", args)
	row, err := decodeUniqueUuid(r)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_uuid: " + err.Error())
	}
	if _, err := uniqueUuidTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_unique_uuid: " + err.Error())
	}
}

func updateUniqueUuid(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_unique_uuid", args)
	row, err := decodeUniqueUuid(r)
	if err != nil {
		spacetimedb.LogPanic("update_unique_uuid: " + err.Error())
	}
	uniqueUuidUIdx.Delete(row.U)
	if _, err := uniqueUuidTable.Insert(row); err != nil {
		spacetimedb.LogPanic("update_unique_uuid: " + err.Error())
	}
}

func deleteUniqueUuid(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_unique_uuid", args)
	u, err := types.ReadUuid(r)
	if err != nil {
		spacetimedb.LogPanic("delete_unique_uuid: " + err.Error())
	}
	uniqueUuidUIdx.Delete(u)
}
