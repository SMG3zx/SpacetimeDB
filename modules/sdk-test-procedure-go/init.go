package main

import (
	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go/types"
)

func init() {
	// ── Tables ────────────────────────────────────────────────────────────

	// MyTable: public, field (ReturnStruct).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "MyTable",
		Columns: []spacetimedb.ColumnDef{
			{Name: "field", Type: satReturnStruct},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// ScheduledProcTable: private, PK scheduled_id (auto_inc u64), scheduled_at,
	// reducer_ts (Timestamp), x (u8), y (u8); scheduled to call scheduled_proc.
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "ScheduledProcTable",
		Columns: []spacetimedb.ColumnDef{
			{Name: "scheduled_id", Type: types.AlgebraicU64},
			{Name: "scheduled_at", Type: types.AlgebraicScheduleAt},
			{Name: "reducer_ts", Type: types.AlgebraicTimestamp},
			{Name: "x", Type: types.AlgebraicU8},
			{Name: "y", Type: types.AlgebraicU8},
		},
		PrimaryKey: []uint16{0},
		Sequences: []spacetimedb.SequenceDef{
			{Column: 0, Increment: 1},
		},
		Access: spacetimedb.TableAccessPrivate,
	})
	spacetimedb.RegisterScheduleDef(spacetimedb.ScheduleDef{
		TableName:     "ScheduledProcTable",
		ScheduleAtCol: 1,
		ReducerName:   "scheduled_proc",
	})

	// ProcInsertsInto: public, reducer_ts, procedure_ts (Timestamp), x, y (u8).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "ProcInsertsInto",
		Columns: []spacetimedb.ColumnDef{
			{Name: "reducer_ts", Type: types.AlgebraicTimestamp},
			{Name: "procedure_ts", Type: types.AlgebraicTimestamp},
			{Name: "x", Type: types.AlgebraicU8},
			{Name: "y", Type: types.AlgebraicU8},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// PkUuid: public, u (Uuid), data (u8).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "PkUuid",
		Columns: []spacetimedb.ColumnDef{
			{Name: "u", Type: satUuid},
			{Name: "data", Type: types.AlgebraicU8},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// ── Reducer ───────────────────────────────────────────────────────────

	// 0: schedule_proc
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name:       "schedule_proc",
		Params:     []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(scheduleProcReducer)

	// ── Procedures ────────────────────────────────────────────────────────
	// Procedures are ordered; the index must match the handler registration order.

	// 0: return_primitive(lhs: u32, rhs: u32) -> u32
	spacetimedb.RegisterProcedureDef(spacetimedb.ProcedureDef{
		Name: "return_primitive",
		Params: []spacetimedb.ColumnDef{
			{Name: "lhs", Type: types.AlgebraicU32},
			{Name: "rhs", Type: types.AlgebraicU32},
		},
		ReturnType: types.AlgebraicU32,
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterProcedureHandler(procReturnPrimitive)

	// 1: return_struct(a: u32, b: String) -> ReturnStruct
	spacetimedb.RegisterProcedureDef(spacetimedb.ProcedureDef{
		Name: "return_struct",
		Params: []spacetimedb.ColumnDef{
			{Name: "a", Type: types.AlgebraicU32},
			{Name: "b", Type: types.AlgebraicString},
		},
		ReturnType: satReturnStruct,
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterProcedureHandler(procReturnStruct)

	// 2: return_enum_a(a: u32) -> ReturnEnum
	spacetimedb.RegisterProcedureDef(spacetimedb.ProcedureDef{
		Name: "return_enum_a",
		Params: []spacetimedb.ColumnDef{
			{Name: "a", Type: types.AlgebraicU32},
		},
		ReturnType: satReturnEnum,
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterProcedureHandler(procReturnEnumA)

	// 3: return_enum_b(b: String) -> ReturnEnum
	spacetimedb.RegisterProcedureDef(spacetimedb.ProcedureDef{
		Name: "return_enum_b",
		Params: []spacetimedb.ColumnDef{
			{Name: "b", Type: types.AlgebraicString},
		},
		ReturnType: satReturnEnum,
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterProcedureHandler(procReturnEnumB)

	// 4: will_panic() -> void
	spacetimedb.RegisterProcedureDef(spacetimedb.ProcedureDef{
		Name:       "will_panic",
		Params:     []spacetimedb.ColumnDef{},
		ReturnType: types.ProductType{},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterProcedureHandler(procWillPanic)

	// 5: read_my_schema() -> String
	spacetimedb.RegisterProcedureDef(spacetimedb.ProcedureDef{
		Name:       "read_my_schema",
		Params:     []spacetimedb.ColumnDef{},
		ReturnType: types.AlgebraicString,
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterProcedureHandler(procReadMySchema)

	// 6: invalid_request() -> String
	spacetimedb.RegisterProcedureDef(spacetimedb.ProcedureDef{
		Name:       "invalid_request",
		Params:     []spacetimedb.ColumnDef{},
		ReturnType: types.AlgebraicString,
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterProcedureHandler(procInvalidRequest)

	// 7: insert_with_tx_commit() -> void
	spacetimedb.RegisterProcedureDef(spacetimedb.ProcedureDef{
		Name:       "insert_with_tx_commit",
		Params:     []spacetimedb.ColumnDef{},
		ReturnType: types.ProductType{},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterProcedureHandler(procInsertWithTxCommit)

	// 8: insert_with_tx_rollback() -> void
	spacetimedb.RegisterProcedureDef(spacetimedb.ProcedureDef{
		Name:       "insert_with_tx_rollback",
		Params:     []spacetimedb.ColumnDef{},
		ReturnType: types.ProductType{},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterProcedureHandler(procInsertWithTxRollback)

	// 9: scheduled_proc(data: ScheduledProcTable) -> void
	spacetimedb.RegisterProcedureDef(spacetimedb.ProcedureDef{
		Name: "scheduled_proc",
		Params: []spacetimedb.ColumnDef{
			{Name: "data", Type: satScheduledProcTable},
		},
		ReturnType: types.ProductType{},
		Visibility: spacetimedb.ReducerVisibilityPrivate,
	})
	spacetimedb.RegisterProcedureHandler(procScheduledProc)

	// 10: sorted_uuids_insert() -> void
	spacetimedb.RegisterProcedureDef(spacetimedb.ProcedureDef{
		Name:       "sorted_uuids_insert",
		Params:     []spacetimedb.ColumnDef{},
		ReturnType: types.ProductType{},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterProcedureHandler(procSortedUuidsInsert)
}
