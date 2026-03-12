package main

import (
	"strconv"

	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
	"github.com/clockworklabs/spacetimedb-go/bsatn"
)

func emptyReducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {}

// argsReader is a package-level reusable Reader for decoding reducer arguments.
// Avoids per-call heap allocation of *bsatn.Reader (critical for TinyGo WASM GC).
var argsReader = bsatn.NewReader(nil)

// reuseReader resets the shared reader with new data and returns it.
// Drop-in replacement for reuseReader(data) with zero allocations.
func reuseReader(data []byte) *bsatn.Reader {
	argsReader.Reset(data)
	return argsReader
}

// ── Insert single row ─────────────────────────────────────────────────────────

func insertUnique0U32U64StrReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSourceReuse(args)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_0_u32_u64_str: " + err.Error())
	}
	r := reuseReader(data)
	id, age, name, err := decodeU32U64Str(r)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_0_u32_u64_str: decode: " + err.Error())
	}
	if _, err := unique0U32U64StrHandle.Insert(Unique0U32U64Str{Id: id, Age: age, Name: name}); err != nil {
		spacetimedb.LogPanic("insert_unique_0_u32_u64_str: insert: " + err.Error())
	}
}

func insertNoIndexU32U64StrReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSourceReuse(args)
	if err != nil {
		spacetimedb.LogPanic("insert_no_index_u32_u64_str: " + err.Error())
	}
	r := reuseReader(data)
	id, age, name, err := decodeU32U64Str(r)
	if err != nil {
		spacetimedb.LogPanic("insert_no_index_u32_u64_str: decode: " + err.Error())
	}
	if _, err := noIndexU32U64StrHandle.Insert(NoIndexU32U64Str{Id: id, Age: age, Name: name}); err != nil {
		spacetimedb.LogPanic("insert_no_index_u32_u64_str: insert: " + err.Error())
	}
}

func insertBtreeEachColumnU32U64StrReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSourceReuse(args)
	if err != nil {
		spacetimedb.LogPanic("insert_btree_each_column_u32_u64_str: " + err.Error())
	}
	r := reuseReader(data)
	id, age, name, err := decodeU32U64Str(r)
	if err != nil {
		spacetimedb.LogPanic("insert_btree_each_column_u32_u64_str: decode: " + err.Error())
	}
	if _, err := btreeEachColumnU32U64StrHandle.Insert(BtreeEachColumnU32U64Str{Id: id, Age: age, Name: name}); err != nil {
		spacetimedb.LogPanic("insert_btree_each_column_u32_u64_str: insert: " + err.Error())
	}
}

func insertUnique0U32U64U64Reducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSourceReuse(args)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_0_u32_u64_u64: " + err.Error())
	}
	r := reuseReader(data)
	id, x, y, err := decodeU32U64U64(r)
	if err != nil {
		spacetimedb.LogPanic("insert_unique_0_u32_u64_u64: decode: " + err.Error())
	}
	if _, err := unique0U32U64U64Handle.Insert(Unique0U32U64U64{Id: id, X: x, Y: y}); err != nil {
		spacetimedb.LogPanic("insert_unique_0_u32_u64_u64: insert: " + err.Error())
	}
}

func insertNoIndexU32U64U64Reducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSourceReuse(args)
	if err != nil {
		spacetimedb.LogPanic("insert_no_index_u32_u64_u64: " + err.Error())
	}
	r := reuseReader(data)
	id, x, y, err := decodeU32U64U64(r)
	if err != nil {
		spacetimedb.LogPanic("insert_no_index_u32_u64_u64: decode: " + err.Error())
	}
	if _, err := noIndexU32U64U64Handle.Insert(NoIndexU32U64U64{Id: id, X: x, Y: y}); err != nil {
		spacetimedb.LogPanic("insert_no_index_u32_u64_u64: insert: " + err.Error())
	}
}

func insertBtreeEachColumnU32U64U64Reducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSourceReuse(args)
	if err != nil {
		spacetimedb.LogPanic("insert_btree_each_column_u32_u64_u64: " + err.Error())
	}
	r := reuseReader(data)
	id, x, y, err := decodeU32U64U64(r)
	if err != nil {
		spacetimedb.LogPanic("insert_btree_each_column_u32_u64_u64: decode: " + err.Error())
	}
	if _, err := btreeEachColumnU32U64U64Handle.Insert(BtreeEachColumnU32U64U64{Id: id, X: x, Y: y}); err != nil {
		spacetimedb.LogPanic("insert_btree_each_column_u32_u64_u64: insert: " + err.Error())
	}
}

// ── Insert bulk ───────────────────────────────────────────────────────────────
//
// These reducers bypass TableHandle.Insert() to reuse a single bsatn.Writer
// across iterations. This avoids per-row allocations that exhaust TinyGo's
// limited WASM linear memory during bulk operations.

// bulkWriter is reused across bulk-insert iterations to avoid per-row allocation.
var bulkWriter = bsatn.NewWriter()

// skipU32U64U64 skips a {u32, u64, u64} row in the reader (4+8+8 = 20 bytes).
func skipU32U64U64(r *bsatn.Reader) error { return r.Skip(20) }

// skipU32U64Str skips a {u32, u64, string} row in the reader.
func skipU32U64Str(r *bsatn.Reader) error {
	if err := r.Skip(12); err != nil { // u32 + u64
		return err
	}
	return r.SkipString()
}

func insertBulkUnique0U32U64U64Reducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	n, _ := r.ReadArrayLen()
	tid, _ := sys.TableIdFromName("unique_0_u_32_u_64_u_64")
	for i := uint32(0); i < n; i++ {
		start := r.Offset()
		if skipU32U64U64(r) != nil {
			break
		}
		_, _ = sys.InsertBsatnReuse(tid, r.RawSlice(start, r.Offset()))
	}
}

func insertBulkNoIndexU32U64U64Reducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	n, _ := r.ReadArrayLen()
	tid, _ := sys.TableIdFromName("no_index_u_32_u_64_u_64")
	for i := uint32(0); i < n; i++ {
		start := r.Offset()
		if skipU32U64U64(r) != nil {
			break
		}
		_, _ = sys.InsertBsatnReuse(tid, r.RawSlice(start, r.Offset()))
	}
}

func insertBulkBtreeEachColumnU32U64U64Reducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	n, _ := r.ReadArrayLen()
	tid, _ := sys.TableIdFromName("btree_each_column_u_32_u_64_u_64")
	for i := uint32(0); i < n; i++ {
		start := r.Offset()
		if skipU32U64U64(r) != nil {
			break
		}
		_, _ = sys.InsertBsatnReuse(tid, r.RawSlice(start, r.Offset()))
	}
}

func insertBulkUnique0U32U64StrReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	n, _ := r.ReadArrayLen()
	tid, _ := sys.TableIdFromName("unique_0_u_32_u_64_str")
	for i := uint32(0); i < n; i++ {
		start := r.Offset()
		if skipU32U64Str(r) != nil {
			break
		}
		_, _ = sys.InsertBsatnReuse(tid, r.RawSlice(start, r.Offset()))
	}
}

func insertBulkNoIndexU32U64StrReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	n, _ := r.ReadArrayLen()
	tid, _ := sys.TableIdFromName("no_index_u_32_u_64_str")
	for i := uint32(0); i < n; i++ {
		start := r.Offset()
		if skipU32U64Str(r) != nil {
			break
		}
		_, _ = sys.InsertBsatnReuse(tid, r.RawSlice(start, r.Offset()))
	}
}

func insertBulkBtreeEachColumnU32U64StrReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	n, _ := r.ReadArrayLen()
	tid, _ := sys.TableIdFromName("btree_each_column_u_32_u_64_str")
	for i := uint32(0); i < n; i++ {
		start := r.Offset()
		if skipU32U64Str(r) != nil {
			break
		}
		_, _ = sys.InsertBsatnReuse(tid, r.RawSlice(start, r.Offset()))
	}
}

// ── Update bulk ───────────────────────────────────────────────────────────────

// updateU64Rows and updateStrRows are reusable slices for update_bulk reducers
// to avoid per-call allocations under TinyGo WASM.
var (
	updateU64Rows []Unique0U32U64U64
	updateStrRows []Unique0U32U64Str
)

func updateBulkUnique0U32U64U64Reducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	rowCount, _ := r.ReadU32()

	if cap(updateU64Rows) < int(rowCount) {
		updateU64Rows = make([]Unique0U32U64U64, 0, rowCount)
	}
	updateU64Rows = updateU64Rows[:0]
	for row, err := range unique0U32U64U64Handle.Iter() {
		if err != nil || uint32(len(updateU64Rows)) >= rowCount {
			break
		}
		updateU64Rows = append(updateU64Rows, row)
	}
	if uint32(len(updateU64Rows)) != rowCount {
		spacetimedb.LogPanic("update_bulk_unique_0_u32_u64_u64: expected " + strconv.FormatUint(uint64(rowCount), 10) + " rows, got " + strconv.Itoa(len(updateU64Rows)))
	}
	for _, row := range updateU64Rows {
		_, _ = unique0U32U64U64IdIdx.Update(Unique0U32U64U64{Id: row.Id, X: row.X + 1, Y: row.Y})
	}
}

func updateBulkUnique0U32U64StrReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	rowCount, _ := r.ReadU32()

	if cap(updateStrRows) < int(rowCount) {
		updateStrRows = make([]Unique0U32U64Str, 0, rowCount)
	}
	updateStrRows = updateStrRows[:0]
	for row, err := range unique0U32U64StrHandle.Iter() {
		if err != nil || uint32(len(updateStrRows)) >= rowCount {
			break
		}
		updateStrRows = append(updateStrRows, row)
	}
	if uint32(len(updateStrRows)) != rowCount {
		spacetimedb.LogPanic("update_bulk_unique_0_u32_u64_str: expected " + strconv.FormatUint(uint64(rowCount), 10) + " rows, got " + strconv.Itoa(len(updateStrRows)))
	}
	for _, row := range updateStrRows {
		_, _ = unique0U32U64StrIdIdx.Update(Unique0U32U64Str{Id: row.Id, Age: row.Age + 1, Name: row.Name})
	}
}

// ── Iterate ───────────────────────────────────────────────────────────────────

// iterReader is a package-level reusable Reader for iterate reducers.
// Avoids heap allocation of *Reader on every call (critical for TinyGo WASM GC).
var iterReader = bsatn.NewReader(nil)

func iterateUnique0U32U64StrReducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {
	tid, _ := sys.TableIdFromName("unique_0_u_32_u_64_str")
	iter, _ := sys.TableScanBsatn(tid)
	data, _ := sys.CollectIterReuse(iter)
	iterReader.Reset(data)
	for iterReader.Remaining() > 0 {
		_ = skipU32U64Str(iterReader)
	}
}

func iterateUnique0U32U64U64Reducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {
	tid, _ := sys.TableIdFromName("unique_0_u_32_u_64_u_64")
	iter, _ := sys.TableScanBsatn(tid)
	data, _ := sys.CollectIterReuse(iter)
	iterReader.Reset(data)
	for iterReader.Remaining() > 0 {
		_ = skipU32U64U64(iterReader)
	}
}

// ── Filter ────────────────────────────────────────────────────────────────────

func filterUnique0U32U64StrByIdReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	id, _ := r.ReadU32()
	_, _ = unique0U32U64StrIdIdx.Find(id)
}

func filterNoIndexU32U64StrByIdReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	id, _ := r.ReadU32()
	for row, err := range noIndexU32U64StrHandle.Iter() {
		if err != nil {
			break
		}
		if row.Id == id {
			_ = row
			break
		}
	}
}

func filterBtreeEachColumnU32U64StrByIdReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	id, _ := r.ReadU32()
	for _, err := range btreeStrIdIdx.Filter(id) {
		if err != nil {
			break
		}
	}
}

func filterUnique0U32U64StrByNameReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	name, _ := r.ReadString()
	for row, err := range unique0U32U64StrHandle.Iter() {
		if err != nil {
			break
		}
		if row.Name == name {
			_ = row
		}
	}
}

func filterNoIndexU32U64StrByNameReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	name, _ := r.ReadString()
	for row, err := range noIndexU32U64StrHandle.Iter() {
		if err != nil {
			break
		}
		if row.Name == name {
			_ = row
		}
	}
}

func filterBtreeEachColumnU32U64StrByNameReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	name, _ := r.ReadString()
	for _, err := range btreeStrNameIdx.Filter(name) {
		if err != nil {
			break
		}
	}
}

func filterUnique0U32U64U64ByIdReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	id, _ := r.ReadU32()
	_, _ = unique0U32U64U64IdIdx.Find(id)
}

func filterNoIndexU32U64U64ByIdReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	id, _ := r.ReadU32()
	for row, err := range noIndexU32U64U64Handle.Iter() {
		if err != nil {
			break
		}
		if row.Id == id {
			_ = row
			break
		}
	}
}

func filterBtreeEachColumnU32U64U64ByIdReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	id, _ := r.ReadU32()
	for _, err := range btreeU64IdIdx.Filter(id) {
		if err != nil {
			break
		}
	}
}

func filterUnique0U32U64U64ByXReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	x, _ := r.ReadU64()
	for row, err := range unique0U32U64U64Handle.Iter() {
		if err != nil {
			break
		}
		if row.X == x {
			_ = row
		}
	}
}

func filterNoIndexU32U64U64ByXReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	x, _ := r.ReadU64()
	for row, err := range noIndexU32U64U64Handle.Iter() {
		if err != nil {
			break
		}
		if row.X == x {
			_ = row
		}
	}
}

func filterBtreeEachColumnU32U64U64ByXReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	x, _ := r.ReadU64()
	for _, err := range btreeU64XIdx.Filter(x) {
		if err != nil {
			break
		}
	}
}

func filterUnique0U32U64U64ByYReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	y, _ := r.ReadU64()
	for row, err := range unique0U32U64U64Handle.Iter() {
		if err != nil {
			break
		}
		if row.Y == y {
			_ = row
		}
	}
}

func filterNoIndexU32U64U64ByYReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	y, _ := r.ReadU64()
	for row, err := range noIndexU32U64U64Handle.Iter() {
		if err != nil {
			break
		}
		if row.Y == y {
			_ = row
		}
	}
}

func filterBtreeEachColumnU32U64U64ByYReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	y, _ := r.ReadU64()
	for _, err := range btreeU64YIdx.Filter(y) {
		if err != nil {
			break
		}
	}
}

// ── Delete ────────────────────────────────────────────────────────────────────

func deleteUnique0U32U64StrByIdReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	id, _ := r.ReadU32()
	_, _ = unique0U32U64StrIdIdx.Delete(id)
}

func deleteUnique0U32U64U64ByIdReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	id, _ := r.ReadU32()
	_, _ = unique0U32U64U64IdIdx.Delete(id)
}

// ── Clear table (unimplemented) ───────────────────────────────────────────────

func clearTableUnimplementedReducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {
	spacetimedb.LogPanic("Modules currently have no interface to clear a table")
}

// ── Count ─────────────────────────────────────────────────────────────────────

func countUnique0U32U64StrReducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {
	count, _ := unique0U32U64StrHandle.Count()
	spacetimedb.LogInfo("COUNT: " + strconv.FormatUint(count, 10))
}

func countNoIndexU32U64StrReducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {
	count, _ := noIndexU32U64StrHandle.Count()
	spacetimedb.LogInfo("COUNT: " + strconv.FormatUint(count, 10))
}

func countBtreeEachColumnU32U64StrReducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {
	count, _ := btreeEachColumnU32U64StrHandle.Count()
	spacetimedb.LogInfo("COUNT: " + strconv.FormatUint(count, 10))
}

func countUnique0U32U64U64Reducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {
	count, _ := unique0U32U64U64Handle.Count()
	spacetimedb.LogInfo("COUNT: " + strconv.FormatUint(count, 10))
}

func countNoIndexU32U64U64Reducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {
	count, _ := noIndexU32U64U64Handle.Count()
	spacetimedb.LogInfo("COUNT: " + strconv.FormatUint(count, 10))
}

func countBtreeEachColumnU32U64U64Reducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {
	count, _ := btreeEachColumnU32U64U64Handle.Count()
	spacetimedb.LogInfo("COUNT: " + strconv.FormatUint(count, 10))
}

// ── Module-specific reducers ──────────────────────────────────────────────────

func fnWith1ArgsReducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {}

func fnWith32ArgsReducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {}

func printManyThingsReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	n, _ := r.ReadU32()
	for i := uint32(0); i < n; i++ {
		spacetimedb.LogInfo("hello again!")
	}
}
