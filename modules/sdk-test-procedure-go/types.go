package main

import "github.com/clockworklabs/spacetimedb-go/types"

// ReturnStruct is a custom type returned by several procedures.
type ReturnStruct struct {
	A uint32
	B string
}

// ReturnEnum is a sum type returned by return_enum_a and return_enum_b.
type ReturnEnum struct {
	Tag  uint8 // ReturnEnumTagA or ReturnEnumTagB
	AVal uint32
	BVal string
}

const (
	ReturnEnumTagA uint8 = 0
	ReturnEnumTagB uint8 = 1
)

// MyTable is a row in the MyTable table (used in tx commit/rollback tests).
type MyTable struct {
	Field ReturnStruct
}

// ScheduledProcTable is a row in the ScheduledProcTable table.
// It is used to schedule calls to the scheduled_proc procedure.
type ScheduledProcTable struct {
	ScheduledId uint64
	ScheduledAt types.ScheduleAt
	ReducerTs   types.Timestamp
	X           uint8
	Y           uint8
}

// ProcInsertsInto is a row in the ProcInsertsInto table.
// Populated by the scheduled_proc procedure.
type ProcInsertsInto struct {
	ReducerTs   types.Timestamp
	ProcedureTs types.Timestamp
	X           uint8
	Y           uint8
}

// PkUuid is a row in the PkUuid table (used in sorted_uuids_insert).
type PkUuid struct {
	U    types.Uuid
	Data uint8
}
