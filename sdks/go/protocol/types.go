package protocol

import (
	"iter"

	"github.com/clockworklabs/spacetimedb-go/types"
)

// QuerySetId is a client-generated identifier for a subscription.
type QuerySetId struct {
	ID uint32
}

// QueryRows holds initial matching rows from a Subscribe response.
type QueryRows struct {
	Tables []SingleTableRows
}

// SingleTableRows holds all rows for one table in a QueryRows.
type SingleTableRows struct {
	Table string
	Rows  BsatnRowList
}

// BsatnRowList is a packed list of BSATN-encoded rows.
type BsatnRowList struct {
	SizeHint RowSizeHint
	RowsData []byte
}

// RowSizeHint describes row boundaries within BsatnRowList.RowsData.
type RowSizeHintKind uint8

const (
	RowSizeHintFixed   RowSizeHintKind = 0
	RowSizeHintOffsets RowSizeHintKind = 1
)

type RowSizeHint struct {
	Kind      RowSizeHintKind
	FixedSize uint16   // valid when Kind == RowSizeHintFixed
	Offsets   []uint64 // valid when Kind == RowSizeHintOffsets
}

// Rows returns an iterator over the raw BSATN bytes of each row.
func (l *BsatnRowList) Rows() iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		data := l.RowsData
		switch l.SizeHint.Kind {
		case RowSizeHintFixed:
			size := int(l.SizeHint.FixedSize)
			if size == 0 {
				return
			}
			for i := 0; i+size <= len(data); i += size {
				if !yield(data[i : i+size]) {
					return
				}
			}
		case RowSizeHintOffsets:
			offsets := l.SizeHint.Offsets
			for i, start := range offsets {
				var end int
				if i+1 < len(offsets) {
					end = int(offsets[i+1])
				} else {
					end = len(data)
				}
				if !yield(data[int(start):end]) {
					return
				}
			}
		}
	}
}

// Len returns the number of rows.
func (l *BsatnRowList) Len() int {
	switch l.SizeHint.Kind {
	case RowSizeHintFixed:
		if l.SizeHint.FixedSize == 0 {
			return 0
		}
		return len(l.RowsData) / int(l.SizeHint.FixedSize)
	case RowSizeHintOffsets:
		return len(l.SizeHint.Offsets)
	}
	return 0
}

// QuerySetUpdate carries row changes for one subscribed query set.
type QuerySetUpdate struct {
	QuerySetId QuerySetId
	Tables     []TableUpdate
}

// TableUpdate carries row changes for one table within a QuerySetUpdate.
type TableUpdate struct {
	TableName string
	Rows      []TableUpdateRows
}

// TableUpdateRows is either persistent (inserts+deletes) or event rows.
type TableUpdateRows struct {
	Kind            TableUpdateRowsKind
	PersistentTable *PersistentTableRows
	EventTable      *EventTableRows
}

type TableUpdateRowsKind uint8

const (
	TableUpdateRowsPersistent TableUpdateRowsKind = 0
	TableUpdateRowsEvent      TableUpdateRowsKind = 1
)

type PersistentTableRows struct {
	Inserts BsatnRowList
	Deletes BsatnRowList
}

type EventTableRows struct {
	Events BsatnRowList
}

// ReducerOutcome is the result of running a reducer.
type ReducerOutcome struct {
	Kind          ReducerOutcomeKind
	Ok            *ReducerOk
	ErrPayload    []byte  // typed BSATN error payload (Err variant)
	InternalError string  // human-readable error (InternalError variant)
}

type ReducerOutcomeKind uint8

const (
	ReducerOutcomeOk            ReducerOutcomeKind = 0
	ReducerOutcomeOkEmpty       ReducerOutcomeKind = 1
	ReducerOutcomeErr           ReducerOutcomeKind = 2
	ReducerOutcomeInternalError ReducerOutcomeKind = 3
)

type ReducerOk struct {
	RetValue        []byte
	TransactionUpdate TransactionUpdate
}

// ProcedureStatus is the result of running a procedure.
type ProcedureStatus struct {
	Kind          ProcedureStatusKind
	ReturnValue   []byte
	InternalError string
}

type ProcedureStatusKind uint8

const (
	ProcedureStatusReturned      ProcedureStatusKind = 0
	ProcedureStatusInternalError ProcedureStatusKind = 1
)

// TransactionUpdate notifies the client of committed transaction changes.
type TransactionUpdate struct {
	QuerySets []QuerySetUpdate
}

// Ensure types package is used (Identity/ConnectionId used in server_messages.go)
var _ = types.Identity{}
