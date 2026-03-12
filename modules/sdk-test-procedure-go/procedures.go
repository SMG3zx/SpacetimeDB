package main

import (
	"fmt"
	"math/rand"

	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/types"
)

// writeResult encodes v to a BSATN writer and writes the bytes to the result sink.
func writeResult(resultSink sys.BytesSink, fn func(w *bsatn.Writer)) {
	w := bsatn.NewWriter()
	fn(w)
	if err := sys.WriteBytesToSink(resultSink, w.Bytes()); err != nil {
		spacetimedb.LogError("writeResult: " + err.Error())
	}
}

func procReturnPrimitive(_ spacetimedb.ProcedureContext, args sys.BytesSource, resultSink sys.BytesSink) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		spacetimedb.LogPanic("return_primitive: read args: " + err.Error())
	}
	r := bsatn.NewReader(data)
	lhs, err := r.ReadU32()
	if err != nil {
		spacetimedb.LogPanic("return_primitive: decode lhs: " + err.Error())
	}
	rhs, err := r.ReadU32()
	if err != nil {
		spacetimedb.LogPanic("return_primitive: decode rhs: " + err.Error())
	}
	writeResult(resultSink, func(w *bsatn.Writer) { w.WriteU32(lhs + rhs) })
}

func procReturnStruct(_ spacetimedb.ProcedureContext, args sys.BytesSource, resultSink sys.BytesSink) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		spacetimedb.LogPanic("return_struct: read args: " + err.Error())
	}
	r := bsatn.NewReader(data)
	a, err := r.ReadU32()
	if err != nil {
		spacetimedb.LogPanic("return_struct: decode a: " + err.Error())
	}
	b, err := r.ReadString()
	if err != nil {
		spacetimedb.LogPanic("return_struct: decode b: " + err.Error())
	}
	writeResult(resultSink, func(w *bsatn.Writer) {
		encodeReturnStruct(w, ReturnStruct{A: a, B: b})
	})
}

func procReturnEnumA(_ spacetimedb.ProcedureContext, args sys.BytesSource, resultSink sys.BytesSink) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		spacetimedb.LogPanic("return_enum_a: read args: " + err.Error())
	}
	r := bsatn.NewReader(data)
	a, err := r.ReadU32()
	if err != nil {
		spacetimedb.LogPanic("return_enum_a: decode a: " + err.Error())
	}
	writeResult(resultSink, func(w *bsatn.Writer) {
		encodeReturnEnum(w, ReturnEnum{Tag: ReturnEnumTagA, AVal: a})
	})
}

func procReturnEnumB(_ spacetimedb.ProcedureContext, args sys.BytesSource, resultSink sys.BytesSink) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		spacetimedb.LogPanic("return_enum_b: read args: " + err.Error())
	}
	r := bsatn.NewReader(data)
	b, err := r.ReadString()
	if err != nil {
		spacetimedb.LogPanic("return_enum_b: decode b: " + err.Error())
	}
	writeResult(resultSink, func(w *bsatn.Writer) {
		encodeReturnEnum(w, ReturnEnum{Tag: ReturnEnumTagB, BVal: b})
	})
}

func procWillPanic(_ spacetimedb.ProcedureContext, _ sys.BytesSource, _ sys.BytesSink) {
	spacetimedb.LogPanic("This procedure is expected to panic")
}

func procReadMySchema(ctx spacetimedb.ProcedureContext, _ sys.BytesSource, resultSink sys.BytesSink) {
	modId := types.Identity(sys.Identity())
	url := fmt.Sprintf("http://localhost:3000/v1/database/%s/schema?version=9", modId.String())
	resp, body, err := ctx.Http(spacetimedb.HttpRequest{
		Method:  spacetimedb.HttpMethodGet,
		URI:     url,
		Version: spacetimedb.HttpVersionHTTP11,
	}, nil)
	if err != nil {
		spacetimedb.LogPanic("read_my_schema: http request failed: " + err.Error())
	}
	_ = resp
	writeResult(resultSink, func(w *bsatn.Writer) { w.WriteString(string(body)) })
}

func procInvalidRequest(ctx spacetimedb.ProcedureContext, _ sys.BytesSource, resultSink sys.BytesSink) {
	_, body, err := ctx.Http(spacetimedb.HttpRequest{
		Method:  spacetimedb.HttpMethodGet,
		URI:     "http://foo.invalid/",
		Version: spacetimedb.HttpVersionHTTP11,
	}, nil)
	if err != nil {
		writeResult(resultSink, func(w *bsatn.Writer) { w.WriteString(err.Error()) })
		return
	}
	spacetimedb.LogPanic(fmt.Sprintf(
		"Got result from requesting `http://foo.invalid`... huh?\n%s", string(body),
	))
}

func procInsertWithTxCommit(ctx spacetimedb.ProcedureContext, _ sys.BytesSource, _ sys.BytesSink) {
	if err := ctx.WithTx(func(_ int64) error {
		_, err := myTableHandle.Insert(MyTable{Field: ReturnStruct{A: 42, B: "magic"}})
		return err
	}); err != nil {
		spacetimedb.LogPanic("insert_with_tx_commit: insert failed: " + err.Error())
	}
	// Assert there is 1 row.
	if err := ctx.WithTx(func(_ int64) error {
		count, err := myTableHandle.Count()
		if err != nil {
			return err
		}
		if count != 1 {
			spacetimedb.LogPanic(fmt.Sprintf("insert_with_tx_commit: expected 1 row, got %d", count))
		}
		return nil
	}); err != nil {
		spacetimedb.LogPanic("insert_with_tx_commit: count check failed: " + err.Error())
	}
}

func procInsertWithTxRollback(ctx spacetimedb.ProcedureContext, _ sys.BytesSource, _ sys.BytesSink) {
	// Insert then force rollback by returning an error.
	_ = ctx.WithTx(func(_ int64) error {
		if _, err := myTableHandle.Insert(MyTable{Field: ReturnStruct{A: 42, B: "magic"}}); err != nil {
			return err
		}
		return fmt.Errorf("rollback") // triggers abort
	})
	// Assert there are 0 rows.
	if err := ctx.WithTx(func(_ int64) error {
		count, err := myTableHandle.Count()
		if err != nil {
			return err
		}
		if count != 0 {
			spacetimedb.LogPanic(fmt.Sprintf("insert_with_tx_rollback: expected 0 rows, got %d", count))
		}
		return nil
	}); err != nil {
		spacetimedb.LogPanic("insert_with_tx_rollback: count check failed: " + err.Error())
	}
}

func procScheduledProc(ctx spacetimedb.ProcedureContext, args sys.BytesSource, _ sys.BytesSink) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		spacetimedb.LogPanic("scheduled_proc: read args: " + err.Error())
	}
	r := bsatn.NewReader(data)
	row, err := decodeScheduledProcTable(r)
	if err != nil {
		spacetimedb.LogPanic("scheduled_proc: decode arg: " + err.Error())
	}
	if err := ctx.WithTx(func(txMicros int64) error {
		_, err := procInsertsIntoHandle.Insert(ProcInsertsInto{
			ReducerTs:   row.ReducerTs,
			ProcedureTs: types.Timestamp{Microseconds: txMicros},
			X:           row.X,
			Y:           row.Y,
		})
		return err
	}); err != nil {
		spacetimedb.LogPanic("scheduled_proc: insert failed: " + err.Error())
	}
}

func procSortedUuidsInsert(ctx spacetimedb.ProcedureContext, _ sys.BytesSource, _ sys.BytesSink) {
	if err := ctx.WithTx(func(txMicros int64) error {
		ts := types.Timestamp{Microseconds: txMicros}
		for i := uint16(0); i < 1000; i++ {
			u := genUUIDv7(ts, ctx.Rng, i)
			if _, err := pkUuidHandle.Insert(PkUuid{U: u, Data: 0}); err != nil {
				return err
			}
		}
		var lastUuid *types.Uuid
		for row, err := range pkUuidHandle.Iter() {
			if err != nil {
				return err
			}
			if lastUuid != nil && !uuidLT(*lastUuid, row.U) {
				spacetimedb.LogPanic("UUIDs are not sorted correctly")
			}
			u := row.U
			lastUuid = &u
		}
		return nil
	}); err != nil {
		spacetimedb.LogPanic("sorted_uuids_insert: " + err.Error())
	}
}

// genUUIDv7 generates a UUID v7 with ms-precision timestamp + 12-bit sequence counter.
func genUUIDv7(ts types.Timestamp, rng *rand.Rand, seq uint16) types.Uuid {
	var u types.Uuid
	lo := rng.Uint64()
	hi := rng.Uint64()
	for i := 0; i < 8; i++ {
		u[i] = byte(lo >> (uint(i) * 8))
	}
	for i := 0; i < 8; i++ {
		u[8+i] = byte(hi >> (uint(i) * 8))
	}
	ms := uint64(ts.Microseconds / 1000)
	u[0] = byte(ms >> 40)
	u[1] = byte(ms >> 32)
	u[2] = byte(ms >> 24)
	u[3] = byte(ms >> 16)
	u[4] = byte(ms >> 8)
	u[5] = byte(ms)
	u[6] = 0x70 | byte(seq>>8)&0x0F
	u[7] = byte(seq)
	u[8] = (u[8] & 0x3F) | 0x80
	return u
}

// uuidLT returns true if a < b (byte-by-byte comparison).
func uuidLT(a, b types.Uuid) bool {
	for i := range a {
		if a[i] < b[i] {
			return true
		}
		if a[i] > b[i] {
			return false
		}
	}
	return false
}
