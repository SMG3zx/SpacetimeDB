//go:build tinygo

// This file declares the raw WASM imports from the SpacetimeDB host ABI.
// Each function maps to a host-provided import via the //go:wasmimport directive.
// These are low-level FFI primitives — use the high-level wrappers in sys.go instead.
//
// The SpacetimeDB ABI is versioned via import module names (spacetime_10.0 through
// spacetime_10.4). All functions use unsafe.Pointer for WASM linear memory access
// and return uint32 error codes (0 = success, non-zero = Errno).

package sys

import "unsafe"

// ── spacetime_10.0 raw imports ────────────────────────────────────────────────
//
// Core datastore operations: table/index lookup, row iteration, insert, update,
// delete, console logging, timing, identity, and volatile scheduling.

// rawTableIdFromName resolves a table name (UTF-8 bytes) to its numeric TableId.
//go:wasmimport spacetime_10.0 table_id_from_name
//go:noescape
func rawTableIdFromName(namePtr unsafe.Pointer, nameLen uint32, out unsafe.Pointer) uint32

// rawIndexIdFromName resolves an index name (UTF-8 bytes) to its numeric IndexId.
//go:wasmimport spacetime_10.0 index_id_from_name
//go:noescape
func rawIndexIdFromName(namePtr unsafe.Pointer, nameLen uint32, out unsafe.Pointer) uint32

// rawDatastoreTableRowCount returns the number of rows in the given table.
//go:wasmimport spacetime_10.0 datastore_table_row_count
//go:noescape
func rawDatastoreTableRowCount(tableId TableId, out unsafe.Pointer) uint32

// rawDatastoreTableScanBsatn starts a full table scan, returning a RowIter handle.
//go:wasmimport spacetime_10.0 datastore_table_scan_bsatn
//go:noescape
func rawDatastoreTableScanBsatn(tableId TableId, out unsafe.Pointer) uint32

// rawDatastoreIndexScanRangeBsatn starts a range scan on a BTree index.
// prefix/rstart/rend are BSATN-encoded key values; pass NULL+0 for unused bounds.
//go:wasmimport spacetime_10.0 datastore_index_scan_range_bsatn
//go:noescape
func rawDatastoreIndexScanRangeBsatn(
	indexId IndexId,
	prefixPtr unsafe.Pointer, prefixLen uint32,
	prefixElems ColId,
	rstartPtr unsafe.Pointer, rstartLen uint32,
	rendPtr unsafe.Pointer, rendLen uint32,
	out unsafe.Pointer,
) uint32

// rawDatastoreDeleteByIndexScanRangeBsatn deletes rows matching an index range scan.
// Returns the number of deleted rows via the out pointer.
//go:wasmimport spacetime_10.0 datastore_delete_by_index_scan_range_bsatn
//go:noescape
func rawDatastoreDeleteByIndexScanRangeBsatn(
	indexId IndexId,
	prefixPtr unsafe.Pointer, prefixLen uint32,
	prefixElems ColId,
	rstartPtr unsafe.Pointer, rstartLen uint32,
	rendPtr unsafe.Pointer, rendLen uint32,
	out unsafe.Pointer,
) uint32

// rawDatastoreDeleteAllByEqBsatn deletes all rows equal to any row in rel
// (BSATN-encoded Vec<ProductValue>). Returns delete count via out.
//go:wasmimport spacetime_10.0 datastore_delete_all_by_eq_bsatn
//go:noescape
func rawDatastoreDeleteAllByEqBsatn(tableId TableId, relPtr unsafe.Pointer, relLen uint32, out unsafe.Pointer) uint32

// rawRowIterBsatnAdvance reads BSATN-encoded rows from iter into buf.
// Returns: -1 = exhausted (iter auto-closed), 0 = more data, >0 = error Errno.
// On BUFFER_TOO_SMALL, bufLenPtr is set to the required buffer size.
//go:wasmimport spacetime_10.0 row_iter_bsatn_advance
//go:noescape
func rawRowIterBsatnAdvance(iter RowIter, bufPtr unsafe.Pointer, bufLenPtr unsafe.Pointer) int32

// rawRowIterBsatnClose explicitly closes and deallocates a RowIter handle.
//go:wasmimport spacetime_10.0 row_iter_bsatn_close
//go:noescape
func rawRowIterBsatnClose(iter RowIter) uint32

// rawDatastoreInsertBsatn inserts a BSATN-encoded row into a table.
// rowPtr/rowLenPtr is an in/out buffer: the host may write back generated column
// values (e.g. auto-increment) and update rowLenPtr to the new length.
//go:wasmimport spacetime_10.0 datastore_insert_bsatn
//go:noescape
func rawDatastoreInsertBsatn(tableId TableId, rowPtr unsafe.Pointer, rowLenPtr unsafe.Pointer) uint32

// rawDatastoreUpdateBsatn updates a row identified by indexId in tableId.
// Like insert, the buffer is in/out for generated column values.
//go:wasmimport spacetime_10.0 datastore_update_bsatn
//go:noescape
func rawDatastoreUpdateBsatn(tableId TableId, indexId IndexId, rowPtr unsafe.Pointer, rowLenPtr unsafe.Pointer) uint32

// rawBytesSinkWrite writes bytes from buf to sink.
// bufLenPtr is in/out: on return it holds the number of bytes actually written.
//go:wasmimport spacetime_10.0 bytes_sink_write
//go:noescape
func rawBytesSinkWrite(sink BytesSink, bufPtr unsafe.Pointer, bufLenPtr unsafe.Pointer) uint32

// rawBytesSourceRead reads bytes from source into buf.
// Returns: -1 = exhausted, 0 = more data available, >0 = error Errno.
// bufLenPtr is in/out: set to available bytes on input, actual bytes read on output.
//go:wasmimport spacetime_10.0 bytes_source_read
//go:noescape
func rawBytesSourceRead(source BytesSource, bufPtr unsafe.Pointer, bufLenPtr unsafe.Pointer) int32

// rawConsoleLog writes a log message to the SpacetimeDB host console.
// target and filename may be NULL (zero-length) for omitted fields.
//go:wasmimport spacetime_10.0 console_log
//go:noescape
func rawConsoleLog(
	level uint32,
	targetPtr unsafe.Pointer, targetLen uint32,
	filenamePtr unsafe.Pointer, filenameLen uint32,
	lineNumber uint32,
	messagePtr unsafe.Pointer, messageLen uint32,
)

// rawConsoleTimerStart starts a named timing span and returns its handle.
//go:wasmimport spacetime_10.0 console_timer_start
//go:noescape
func rawConsoleTimerStart(namePtr unsafe.Pointer, nameLen uint32) uint32

// rawConsoleTimerEnd ends a timing span and logs its elapsed duration.
//go:wasmimport spacetime_10.0 console_timer_end
//go:noescape
func rawConsoleTimerEnd(timerId uint32) uint32

// rawIdentity writes the 32-byte module identity to the provided output buffer.
//go:wasmimport spacetime_10.0 identity
//go:noescape
func rawIdentity(outPtr unsafe.Pointer)

// rawVolatileNonatomicScheduleImmediate schedules a reducer call by name
// outside the current transaction. The call runs after the current tx commits.
//go:wasmimport spacetime_10.0 volatile_nonatomic_schedule_immediate
//go:noescape
func rawVolatileNonatomicScheduleImmediate(namePtr unsafe.Pointer, nameLen uint32, argsPtr unsafe.Pointer, argsLen uint32)

// ── spacetime_10.1 raw imports ────────────────────────────────────────────────
//
// Byte source length query — allows pre-allocating buffers to exact size.

// rawBytesSourceRemainingLength returns the number of unread bytes in source.
// Returns a negative value on error.
//go:wasmimport spacetime_10.1 bytes_source_remaining_length
//go:noescape
func rawBytesSourceRemainingLength(source BytesSource, out unsafe.Pointer) int32

// ── spacetime_10.2 raw imports ────────────────────────────────────────────────
//
// JWT credential lookup for authenticated callers.

// rawGetJwt retrieves the JWT payload for a connection, returning a BytesSource handle.
//go:wasmimport spacetime_10.2 get_jwt
//go:noescape
func rawGetJwt(connectionIdPtr unsafe.Pointer, bytesSourceIdOut unsafe.Pointer) uint32

// ── spacetime_10.3 raw imports ────────────────────────────────────────────────
//
// Procedure-specific operations: manual transaction management and HTTP client.

// rawProcedureStartMutTx begins a mutable transaction within a procedure.
// Writes the transaction timestamp (microseconds since Unix epoch) to microsOut.
//go:wasmimport spacetime_10.3 procedure_start_mut_tx
//go:noescape
func rawProcedureStartMutTx(microsOut unsafe.Pointer) uint32

// rawProcedureCommitMutTx commits the current procedure mutable transaction.
//go:wasmimport spacetime_10.3 procedure_commit_mut_tx
//go:noescape
func rawProcedureCommitMutTx() uint32

// rawProcedureAbortMutTx aborts the current procedure mutable transaction.
//go:wasmimport spacetime_10.3 procedure_abort_mut_tx
//go:noescape
func rawProcedureAbortMutTx() uint32

// rawProcedureHttpRequest makes an HTTP request from within a procedure.
// Returns two BytesSource handles via out: [0] = response metadata, [1] = response body.
//go:wasmimport spacetime_10.3 procedure_http_request
//go:noescape
func rawProcedureHttpRequest(
	requestPtr unsafe.Pointer, requestLen uint32,
	bodyPtr unsafe.Pointer, bodyLen uint32,
	out unsafe.Pointer,
) uint32

// ── spacetime_10.4 raw imports ────────────────────────────────────────────────
//
// Point index scan — efficient exact-match lookup on unique indexes.

// rawDatastoreIndexScanPointBsatn performs an exact-match lookup on a unique index.
// point is the BSATN-encoded key value. Returns a RowIter handle via out.
//go:wasmimport spacetime_10.4 datastore_index_scan_point_bsatn
//go:noescape
func rawDatastoreIndexScanPointBsatn(indexId IndexId, pointPtr unsafe.Pointer, pointLen uint32, out unsafe.Pointer) uint32

// rawDatastoreDeleteByIndexScanPointBsatn deletes rows matching an exact index key.
// Returns the number of deleted rows via out.
//go:wasmimport spacetime_10.4 datastore_delete_by_index_scan_point_bsatn
//go:noescape
func rawDatastoreDeleteByIndexScanPointBsatn(indexId IndexId, pointPtr unsafe.Pointer, pointLen uint32, out unsafe.Pointer) uint32
