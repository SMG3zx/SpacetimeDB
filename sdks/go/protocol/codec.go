package protocol

import "github.com/clockworklabs/spacetimedb-go/bsatn"

// --- BSATN read helpers ---

func readQuerySetId(r *bsatn.Reader) (QuerySetId, error) {
	id, err := r.ReadU32()
	return QuerySetId{ID: id}, err
}

func readBsatnRowList(r *bsatn.Reader) (BsatnRowList, error) {
	tag, err := r.ReadVariantTag()
	if err != nil {
		return BsatnRowList{}, err
	}
	var hint RowSizeHint
	switch tag {
	case 0: // FixedSize
		sz, err := r.ReadU16()
		if err != nil {
			return BsatnRowList{}, err
		}
		hint = RowSizeHint{Kind: RowSizeHintFixed, FixedSize: sz}
	case 1: // RowOffsets
		count, err := r.ReadArrayLen()
		if err != nil {
			return BsatnRowList{}, err
		}
		offsets := make([]uint64, count)
		for i := range offsets {
			offsets[i], err = r.ReadU64()
			if err != nil {
				return BsatnRowList{}, err
			}
		}
		hint = RowSizeHint{Kind: RowSizeHintOffsets, Offsets: offsets}
	default:
		return BsatnRowList{}, bsatn.ErrUnexpectedEOF
	}
	data, err := r.ReadBytes()
	if err != nil {
		return BsatnRowList{}, err
	}
	return BsatnRowList{SizeHint: hint, RowsData: data}, nil
}

func writeBsatnRowList(w *bsatn.Writer, l BsatnRowList) {
	switch l.SizeHint.Kind {
	case RowSizeHintFixed:
		w.WriteVariantTag(0)
		w.WriteU16(l.SizeHint.FixedSize)
	case RowSizeHintOffsets:
		w.WriteVariantTag(1)
		w.WriteArrayLen(uint32(len(l.SizeHint.Offsets)))
		for _, o := range l.SizeHint.Offsets {
			w.WriteU64(o)
		}
	}
	w.WriteBytes(l.RowsData)
}

func readSingleTableRows(r *bsatn.Reader) (SingleTableRows, error) {
	name, err := r.ReadString()
	if err != nil {
		return SingleTableRows{}, err
	}
	rows, err := readBsatnRowList(r)
	if err != nil {
		return SingleTableRows{}, err
	}
	return SingleTableRows{Table: name, Rows: rows}, nil
}

func readQueryRows(r *bsatn.Reader) (QueryRows, error) {
	tables, err := bsatn.ReadSlice(r, readSingleTableRows)
	if err != nil {
		return QueryRows{}, err
	}
	return QueryRows{Tables: tables}, nil
}

func readPersistentTableRows(r *bsatn.Reader) (PersistentTableRows, error) {
	ins, err := readBsatnRowList(r)
	if err != nil {
		return PersistentTableRows{}, err
	}
	del, err := readBsatnRowList(r)
	if err != nil {
		return PersistentTableRows{}, err
	}
	return PersistentTableRows{Inserts: ins, Deletes: del}, nil
}

func readEventTableRows(r *bsatn.Reader) (EventTableRows, error) {
	events, err := readBsatnRowList(r)
	if err != nil {
		return EventTableRows{}, err
	}
	return EventTableRows{Events: events}, nil
}

func readTableUpdateRows(r *bsatn.Reader) (TableUpdateRows, error) {
	tag, err := r.ReadVariantTag()
	if err != nil {
		return TableUpdateRows{}, err
	}
	switch tag {
	case 0:
		p, err := readPersistentTableRows(r)
		if err != nil {
			return TableUpdateRows{}, err
		}
		return TableUpdateRows{Kind: TableUpdateRowsPersistent, PersistentTable: &p}, nil
	case 1:
		e, err := readEventTableRows(r)
		if err != nil {
			return TableUpdateRows{}, err
		}
		return TableUpdateRows{Kind: TableUpdateRowsEvent, EventTable: &e}, nil
	default:
		return TableUpdateRows{}, bsatn.ErrUnexpectedEOF
	}
}

func readTableUpdate(r *bsatn.Reader) (TableUpdate, error) {
	name, err := r.ReadString()
	if err != nil {
		return TableUpdate{}, err
	}
	rows, err := bsatn.ReadSlice(r, readTableUpdateRows)
	if err != nil {
		return TableUpdate{}, err
	}
	return TableUpdate{TableName: name, Rows: rows}, nil
}

func readQuerySetUpdate(r *bsatn.Reader) (QuerySetUpdate, error) {
	qsid, err := readQuerySetId(r)
	if err != nil {
		return QuerySetUpdate{}, err
	}
	tables, err := bsatn.ReadSlice(r, readTableUpdate)
	if err != nil {
		return QuerySetUpdate{}, err
	}
	return QuerySetUpdate{QuerySetId: qsid, Tables: tables}, nil
}

func readTransactionUpdate(r *bsatn.Reader) (TransactionUpdate, error) {
	sets, err := bsatn.ReadSlice(r, readQuerySetUpdate)
	if err != nil {
		return TransactionUpdate{}, err
	}
	return TransactionUpdate{QuerySets: sets}, nil
}

func readReducerOk(r *bsatn.Reader) (ReducerOk, error) {
	retVal, err := r.ReadBytes()
	if err != nil {
		return ReducerOk{}, err
	}
	tx, err := readTransactionUpdate(r)
	if err != nil {
		return ReducerOk{}, err
	}
	return ReducerOk{RetValue: retVal, TransactionUpdate: tx}, nil
}

func readReducerOutcome(r *bsatn.Reader) (ReducerOutcome, error) {
	tag, err := r.ReadVariantTag()
	if err != nil {
		return ReducerOutcome{}, err
	}
	switch tag {
	case 0: // Ok
		ok, err := readReducerOk(r)
		if err != nil {
			return ReducerOutcome{}, err
		}
		return ReducerOutcome{Kind: ReducerOutcomeOk, Ok: &ok}, nil
	case 1: // OkEmpty
		return ReducerOutcome{Kind: ReducerOutcomeOkEmpty}, nil
	case 2: // Err
		payload, err := r.ReadBytes()
		if err != nil {
			return ReducerOutcome{}, err
		}
		return ReducerOutcome{Kind: ReducerOutcomeErr, ErrPayload: payload}, nil
	case 3: // InternalError
		msg, err := r.ReadString()
		if err != nil {
			return ReducerOutcome{}, err
		}
		return ReducerOutcome{Kind: ReducerOutcomeInternalError, InternalError: msg}, nil
	default:
		return ReducerOutcome{}, bsatn.ErrUnexpectedEOF
	}
}

func readProcedureStatus(r *bsatn.Reader) (ProcedureStatus, error) {
	tag, err := r.ReadVariantTag()
	if err != nil {
		return ProcedureStatus{}, err
	}
	switch tag {
	case 0: // Returned
		val, err := r.ReadBytes()
		if err != nil {
			return ProcedureStatus{}, err
		}
		return ProcedureStatus{Kind: ProcedureStatusReturned, ReturnValue: val}, nil
	case 1: // InternalError
		msg, err := r.ReadString()
		if err != nil {
			return ProcedureStatus{}, err
		}
		return ProcedureStatus{Kind: ProcedureStatusInternalError, InternalError: msg}, nil
	default:
		return ProcedureStatus{}, bsatn.ErrUnexpectedEOF
	}
}

// --- write helpers for QuerySetId ---

func writeQuerySetId(w *bsatn.Writer, q QuerySetId) {
	w.WriteU32(q.ID)
}
