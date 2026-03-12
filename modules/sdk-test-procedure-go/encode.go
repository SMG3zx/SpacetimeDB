package main

import (
	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/types"
)

func encodeReturnStruct(w *bsatn.Writer, v ReturnStruct) {
	w.WriteU32(v.A)
	w.WriteString(v.B)
}

func decodeReturnStruct(r *bsatn.Reader) (ReturnStruct, error) {
	a, err := r.ReadU32()
	if err != nil {
		return ReturnStruct{}, err
	}
	b, err := r.ReadString()
	if err != nil {
		return ReturnStruct{}, err
	}
	return ReturnStruct{A: a, B: b}, nil
}

func encodeReturnEnum(w *bsatn.Writer, v ReturnEnum) {
	w.WriteVariantTag(v.Tag)
	switch v.Tag {
	case ReturnEnumTagA:
		w.WriteU32(v.AVal)
	case ReturnEnumTagB:
		w.WriteString(v.BVal)
	}
}

func encodeMyTable(w *bsatn.Writer, v MyTable) {
	encodeReturnStruct(w, v.Field)
}

func decodeMyTable(r *bsatn.Reader) (MyTable, error) {
	f, err := decodeReturnStruct(r)
	if err != nil {
		return MyTable{}, err
	}
	return MyTable{Field: f}, nil
}

func encodeScheduledProcTable(w *bsatn.Writer, v ScheduledProcTable) {
	w.WriteU64(v.ScheduledId)
	v.ScheduledAt.WriteBsatn(w)
	v.ReducerTs.WriteBsatn(w)
	w.WriteU8(v.X)
	w.WriteU8(v.Y)
}

func decodeScheduledProcTable(r *bsatn.Reader) (ScheduledProcTable, error) {
	id, err := r.ReadU64()
	if err != nil {
		return ScheduledProcTable{}, err
	}
	sched, err := types.ReadScheduleAt(r)
	if err != nil {
		return ScheduledProcTable{}, err
	}
	ts, err := types.ReadTimestamp(r)
	if err != nil {
		return ScheduledProcTable{}, err
	}
	x, err := r.ReadU8()
	if err != nil {
		return ScheduledProcTable{}, err
	}
	y, err := r.ReadU8()
	if err != nil {
		return ScheduledProcTable{}, err
	}
	return ScheduledProcTable{ScheduledId: id, ScheduledAt: sched, ReducerTs: ts, X: x, Y: y}, nil
}

func encodeProcInsertsInto(w *bsatn.Writer, v ProcInsertsInto) {
	v.ReducerTs.WriteBsatn(w)
	v.ProcedureTs.WriteBsatn(w)
	w.WriteU8(v.X)
	w.WriteU8(v.Y)
}

func decodeProcInsertsInto(r *bsatn.Reader) (ProcInsertsInto, error) {
	reducerTs, err := types.ReadTimestamp(r)
	if err != nil {
		return ProcInsertsInto{}, err
	}
	procedureTs, err := types.ReadTimestamp(r)
	if err != nil {
		return ProcInsertsInto{}, err
	}
	x, err := r.ReadU8()
	if err != nil {
		return ProcInsertsInto{}, err
	}
	y, err := r.ReadU8()
	if err != nil {
		return ProcInsertsInto{}, err
	}
	return ProcInsertsInto{ReducerTs: reducerTs, ProcedureTs: procedureTs, X: x, Y: y}, nil
}

func encodePkUuid(w *bsatn.Writer, v PkUuid) {
	v.U.WriteBsatn(w)
	w.WriteU8(v.Data)
}

func decodePkUuid(r *bsatn.Reader) (PkUuid, error) {
	u, err := types.ReadUuid(r)
	if err != nil {
		return PkUuid{}, err
	}
	data, err := r.ReadU8()
	if err != nil {
		return PkUuid{}, err
	}
	return PkUuid{U: u, Data: data}, nil
}
