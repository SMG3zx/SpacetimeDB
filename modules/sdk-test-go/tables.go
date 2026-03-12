package main

import (
	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go/types"
)

func registerAllTables() {
	// ── One* tables (26) ─────────────────────────────────────────────────────
	registerPublicTable("OneU8", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU8}})
	registerPublicTable("OneU16", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU16}})
	registerPublicTable("OneU32", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU32}})
	registerPublicTable("OneU64", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU64}})
	registerPublicTable("OneU128", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU128}})
	registerPublicTable("OneU256", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU256}})
	registerPublicTable("OneI8", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI8}})
	registerPublicTable("OneI16", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI16}})
	registerPublicTable("OneI32", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI32}})
	registerPublicTable("OneI64", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI64}})
	registerPublicTable("OneI128", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI128}})
	registerPublicTable("OneI256", []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicI256}})
	registerPublicTable("OneBool", []spacetimedb.ColumnDef{{Name: "b", Type: types.AlgebraicBool}})
	registerPublicTable("OneF32", []spacetimedb.ColumnDef{{Name: "f", Type: types.AlgebraicF32}})
	registerPublicTable("OneF64", []spacetimedb.ColumnDef{{Name: "f", Type: types.AlgebraicF64}})
	registerPublicTable("OneString", []spacetimedb.ColumnDef{{Name: "s", Type: types.AlgebraicString}})
	registerPublicTable("OneIdentity", []spacetimedb.ColumnDef{{Name: "i", Type: satIdentity}})
	registerPublicTable("OneConnectionId", []spacetimedb.ColumnDef{{Name: "a", Type: satConnectionId}})
	registerPublicTable("OneUuid", []spacetimedb.ColumnDef{{Name: "u", Type: satUuid}})
	registerPublicTable("OneTimestamp", []spacetimedb.ColumnDef{{Name: "t", Type: types.AlgebraicTimestamp}})
	registerPublicTable("OneSimpleEnum", []spacetimedb.ColumnDef{{Name: "e", Type: satSimpleEnum}})
	registerPublicTable("OneEnumWithPayload", []spacetimedb.ColumnDef{{Name: "e", Type: satEnumWithPayload}})
	registerPublicTable("OneUnitStruct", []spacetimedb.ColumnDef{{Name: "s", Type: satUnitStruct}})
	registerPublicTable("OneByteStruct", []spacetimedb.ColumnDef{{Name: "s", Type: satByteStruct}})
	registerPublicTable("OneEveryPrimitiveStruct", []spacetimedb.ColumnDef{{Name: "s", Type: satEveryPrimitiveStruct}})
	registerPublicTable("OneEveryVecStruct", []spacetimedb.ColumnDef{{Name: "s", Type: satEveryVecStruct}})

	// ── Vec* tables (26) ─────────────────────────────────────────────────────
	registerPublicTable("VecU8", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicU8}}})
	registerPublicTable("VecU16", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicU16}}})
	registerPublicTable("VecU32", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicU32}}})
	registerPublicTable("VecU64", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicU64}}})
	registerPublicTable("VecU128", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicU128}}})
	registerPublicTable("VecU256", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicU256}}})
	registerPublicTable("VecI8", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicI8}}})
	registerPublicTable("VecI16", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicI16}}})
	registerPublicTable("VecI32", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicI32}}})
	registerPublicTable("VecI64", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicI64}}})
	registerPublicTable("VecI128", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicI128}}})
	registerPublicTable("VecI256", []spacetimedb.ColumnDef{{Name: "n", Type: types.ArrayType{ElemType: types.AlgebraicI256}}})
	registerPublicTable("VecBool", []spacetimedb.ColumnDef{{Name: "b", Type: types.ArrayType{ElemType: types.AlgebraicBool}}})
	registerPublicTable("VecF32", []spacetimedb.ColumnDef{{Name: "f", Type: types.ArrayType{ElemType: types.AlgebraicF32}}})
	registerPublicTable("VecF64", []spacetimedb.ColumnDef{{Name: "f", Type: types.ArrayType{ElemType: types.AlgebraicF64}}})
	registerPublicTable("VecString", []spacetimedb.ColumnDef{{Name: "s", Type: types.ArrayType{ElemType: types.AlgebraicString}}})
	registerPublicTable("VecIdentity", []spacetimedb.ColumnDef{{Name: "i", Type: types.ArrayType{ElemType: satIdentity}}})
	registerPublicTable("VecConnectionId", []spacetimedb.ColumnDef{{Name: "a", Type: types.ArrayType{ElemType: satConnectionId}}})
	registerPublicTable("VecUuid", []spacetimedb.ColumnDef{{Name: "u", Type: types.ArrayType{ElemType: satUuid}}})
	registerPublicTable("VecTimestamp", []spacetimedb.ColumnDef{{Name: "t", Type: types.ArrayType{ElemType: types.AlgebraicTimestamp}}})
	registerPublicTable("VecSimpleEnum", []spacetimedb.ColumnDef{{Name: "e", Type: types.ArrayType{ElemType: satSimpleEnum}}})
	registerPublicTable("VecEnumWithPayload", []spacetimedb.ColumnDef{{Name: "e", Type: types.ArrayType{ElemType: satEnumWithPayload}}})
	registerPublicTable("VecUnitStruct", []spacetimedb.ColumnDef{{Name: "s", Type: types.ArrayType{ElemType: satUnitStruct}}})
	registerPublicTable("VecByteStruct", []spacetimedb.ColumnDef{{Name: "s", Type: types.ArrayType{ElemType: satByteStruct}}})
	registerPublicTable("VecEveryPrimitiveStruct", []spacetimedb.ColumnDef{{Name: "s", Type: types.ArrayType{ElemType: satEveryPrimitiveStruct}}})
	registerPublicTable("VecEveryVecStruct", []spacetimedb.ColumnDef{{Name: "s", Type: types.ArrayType{ElemType: satEveryVecStruct}}})

	// ── Option* tables (7) ───────────────────────────────────────────────────
	registerPublicTable("OptionI32", []spacetimedb.ColumnDef{{Name: "n", Type: satOptionI32}})
	registerPublicTable("OptionString", []spacetimedb.ColumnDef{{Name: "s", Type: satOptionString}})
	registerPublicTable("OptionIdentity", []spacetimedb.ColumnDef{{Name: "i", Type: satOptionIdentity}})
	registerPublicTable("OptionUuid", []spacetimedb.ColumnDef{{Name: "u", Type: satOptionUuid}})
	registerPublicTable("OptionSimpleEnum", []spacetimedb.ColumnDef{{Name: "e", Type: satOptionSimpleEnum}})
	registerPublicTable("OptionEveryPrimitiveStruct", []spacetimedb.ColumnDef{{Name: "s", Type: satOptionEveryPrimitiveStruct}})
	registerPublicTable("OptionVecOptionI32", []spacetimedb.ColumnDef{{Name: "v", Type: satOptionVecOptionI32}})

	// ── Result* tables (6) ───────────────────────────────────────────────────
	registerPublicTable("ResultI32String", []spacetimedb.ColumnDef{{Name: "r", Type: satResultI32String}})
	registerPublicTable("ResultStringI32", []spacetimedb.ColumnDef{{Name: "r", Type: satResultStringI32}})
	registerPublicTable("ResultIdentityString", []spacetimedb.ColumnDef{{Name: "r", Type: satResultIdentityString}})
	registerPublicTable("ResultSimpleEnumI32", []spacetimedb.ColumnDef{{Name: "r", Type: satResultSimpleEnumI32}})
	registerPublicTable("ResultEveryPrimitiveStructString", []spacetimedb.ColumnDef{{Name: "r", Type: satResultEveryPrimitiveStructString}})
	registerPublicTable("ResultVecI32String", []spacetimedb.ColumnDef{{Name: "r", Type: satResultVecI32String}})

	// ── Unique* tables (17) ──────────────────────────────────────────────────
	registerUniqueTable("UniqueU8", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicU8})
	registerUniqueTable("UniqueU16", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicU16})
	registerUniqueTable("UniqueU32", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicU32})
	registerUniqueTable("UniqueU64", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicU64})
	registerUniqueTable("UniqueU128", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicU128})
	registerUniqueTable("UniqueU256", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicU256})
	registerUniqueTable("UniqueI8", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicI8})
	registerUniqueTable("UniqueI16", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicI16})
	registerUniqueTable("UniqueI32", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicI32})
	registerUniqueTable("UniqueI64", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicI64})
	registerUniqueTable("UniqueI128", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicI128})
	registerUniqueTable("UniqueI256", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicI256})
	registerUniqueTable("UniqueBool", spacetimedb.ColumnDef{Name: "b", Type: types.AlgebraicBool})
	registerUniqueTable("UniqueString", spacetimedb.ColumnDef{Name: "s", Type: types.AlgebraicString})
	registerUniqueTable("UniqueIdentity", spacetimedb.ColumnDef{Name: "i", Type: satIdentity})
	registerUniqueTable("UniqueConnectionId", spacetimedb.ColumnDef{Name: "a", Type: satConnectionId})
	registerUniqueTable("UniqueUuid", spacetimedb.ColumnDef{Name: "u", Type: satUuid})

	// ── PK* tables (19) ──────────────────────────────────────────────────────
	registerPkTable("PkU8", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicU8})
	registerPkTable("PkU16", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicU16})
	registerPkTable("PkU32", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicU32})
	registerPkTable("PkU32Two", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicU32})
	registerPkTable("PkU64", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicU64})
	registerPkTable("PkU128", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicU128})
	registerPkTable("PkU256", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicU256})
	registerPkTable("PkI8", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicI8})
	registerPkTable("PkI16", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicI16})
	registerPkTable("PkI32", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicI32})
	registerPkTable("PkI64", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicI64})
	registerPkTable("PkI128", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicI128})
	registerPkTable("PkI256", spacetimedb.ColumnDef{Name: "n", Type: types.AlgebraicI256})
	registerPkTable("PkBool", spacetimedb.ColumnDef{Name: "b", Type: types.AlgebraicBool})
	registerPkTable("PkString", spacetimedb.ColumnDef{Name: "s", Type: types.AlgebraicString})
	registerPkTable("PkIdentity", spacetimedb.ColumnDef{Name: "i", Type: satIdentity})
	registerPkTable("PkConnectionId", spacetimedb.ColumnDef{Name: "a", Type: satConnectionId})
	registerPkTable("PkUuid", spacetimedb.ColumnDef{Name: "u", Type: satUuid})
	registerPkTable("PkSimpleEnum", spacetimedb.ColumnDef{Name: "a", Type: satSimpleEnum})

	// ── Special tables ───────────────────────────────────────────────────────

	// LargeTable: 22 columns, public, no PK/constraints.
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "LargeTable",
		Columns: []spacetimedb.ColumnDef{
			{Name: "a", Type: types.AlgebraicU8},
			{Name: "b", Type: types.AlgebraicU16},
			{Name: "c", Type: types.AlgebraicU32},
			{Name: "d", Type: types.AlgebraicU64},
			{Name: "e", Type: types.AlgebraicU128},
			{Name: "f", Type: types.AlgebraicU256},
			{Name: "g", Type: types.AlgebraicI8},
			{Name: "h", Type: types.AlgebraicI16},
			{Name: "i", Type: types.AlgebraicI32},
			{Name: "j", Type: types.AlgebraicI64},
			{Name: "k", Type: types.AlgebraicI128},
			{Name: "l", Type: types.AlgebraicI256},
			{Name: "m", Type: types.AlgebraicBool},
			{Name: "n", Type: types.AlgebraicF32},
			{Name: "o", Type: types.AlgebraicF64},
			{Name: "p", Type: types.AlgebraicString},
			{Name: "q", Type: satSimpleEnum},
			{Name: "r", Type: satEnumWithPayload},
			{Name: "s", Type: satUnitStruct},
			{Name: "t", Type: satByteStruct},
			{Name: "u", Type: satEveryPrimitiveStruct},
			{Name: "v", Type: satEveryVecStruct},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// TableHoldsTable: 2 columns, public.
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "TableHoldsTable",
		Columns: []spacetimedb.ColumnDef{
			{Name: "a", Type: types.ProductType{
				Elements: []types.ProductTypeElement{
					{Name: sptr("n"), Type: types.AlgebraicU8},
				},
			}},
			{Name: "b", Type: types.ProductType{
				Elements: []types.ProductTypeElement{
					{Name: sptr("n"), Type: types.ArrayType{ElemType: types.AlgebraicU8}},
				},
			}},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// ScheduledTable: PK auto_inc scheduled_id (u64), scheduled_at (ScheduleAt), text (String).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "ScheduledTable",
		Columns: []spacetimedb.ColumnDef{
			{Name: "scheduled_id", Type: types.AlgebraicU64},
			{Name: "scheduled_at", Type: types.AlgebraicScheduleAt},
			{Name: "text", Type: types.AlgebraicString},
		},
		PrimaryKey: []uint16{0},
		Sequences: []spacetimedb.SequenceDef{
			{Column: 0, Increment: 1},
		},
		Access: spacetimedb.TableAccessPublic,
	})
	spacetimedb.RegisterScheduleDef(spacetimedb.ScheduleDef{
		TableName:     "ScheduledTable",
		ScheduleAtCol: 1,
		ReducerName:   "send_scheduled_message",
	})

	// IndexedTable: player_id (u32), private, BTree on player_id.
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "IndexedTable",
		Columns: []spacetimedb.ColumnDef{
			{Name: "player_id", Type: types.AlgebraicU32},
		},
		Indexes: []spacetimedb.IndexDef{
			{AccessorName: sptr("player_id"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
		},
		Access: spacetimedb.TableAccessPrivate,
	})

	// IndexedTable2: player_id (u32), player_snazz (f32), private, composite BTree.
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "IndexedTable2",
		Columns: []spacetimedb.ColumnDef{
			{Name: "player_id", Type: types.AlgebraicU32},
			{Name: "player_snazz", Type: types.AlgebraicF32},
		},
		Indexes: []spacetimedb.IndexDef{
			{AccessorName: sptr("player_id_snazz_index"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0, 1}},
		},
		Access: spacetimedb.TableAccessPrivate,
	})

	// BTreeU32: n (u32), data (i32), public, BTree on n.
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "BTreeU32",
		Columns: []spacetimedb.ColumnDef{
			{Name: "n", Type: types.AlgebraicU32},
			{Name: "data", Type: types.AlgebraicI32},
		},
		Indexes: []spacetimedb.IndexDef{
			{AccessorName: sptr("n"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// Users: identity (Identity) PK, name (String), public.
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "Users",
		Columns: []spacetimedb.ColumnDef{
			{Name: "identity", Type: satIdentity},
			{Name: "name", Type: types.AlgebraicString},
		},
		PrimaryKey: []uint16{0},
		Access:     spacetimedb.TableAccessPublic,
	})

	// IndexedSimpleEnum: n (SimpleEnum), public, BTree on n.
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "IndexedSimpleEnum",
		Columns: []spacetimedb.ColumnDef{
			{Name: "n", Type: satSimpleEnum},
		},
		Indexes: []spacetimedb.IndexDef{
			{AccessorName: sptr("n"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// ── RLS policies ─────────────────────────────────────────────────────────
	spacetimedb.RegisterRLSDef(spacetimedb.RLSDef{SQL: "SELECT * FROM one_u_8"})
	spacetimedb.RegisterRLSDef(spacetimedb.RLSDef{SQL: "SELECT * FROM users WHERE identity = :sender"})
}

// registerUniqueTable registers a table with a unique constraint on column 0.
func registerUniqueTable(name string, uniqueCol spacetimedb.ColumnDef) {
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name:        name,
		Columns:     []spacetimedb.ColumnDef{uniqueCol, {Name: "data", Type: types.AlgebraicI32}},
		Constraints: []spacetimedb.ConstraintDef{{Columns: []uint16{0}}},
		Access:      spacetimedb.TableAccessPublic,
	})
}

// registerPkTable registers a table with a primary key on column 0.
func registerPkTable(name string, pkCol spacetimedb.ColumnDef) {
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name:       name,
		Columns:    []spacetimedb.ColumnDef{pkCol, {Name: "data", Type: types.AlgebraicI32}},
		PrimaryKey: []uint16{0},
		Access:     spacetimedb.TableAccessPublic,
	})
}
