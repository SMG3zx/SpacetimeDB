//go:build tinygo

package spacetimedb

import (
	"iter"

	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
)

// TableHandle provides Insert, Iter, Count, and Delete operations for a table.
// Generated module bindings create one TableHandle per table and embed type-specific
// accessor methods (FindBy*, DeleteBy*, Filter*) as wrapper types.
type TableHandle[Row any] struct {
	name    string
	tableId sys.TableId
	ready   bool
	encode  func(*bsatn.Writer, Row)
	decode  func(*bsatn.Reader) (Row, error)
}

// NewTableHandle creates a TableHandle for the named table.
// The table ID is resolved lazily on first use.
func NewTableHandle[Row any](
	name string,
	encode func(*bsatn.Writer, Row),
	decode func(*bsatn.Reader) (Row, error),
) *TableHandle[Row] {
	return &TableHandle[Row]{
		name:   name,
		encode: encode,
		decode: decode,
	}
}

// id returns the cached TableId, resolving it from the host on first call.
func (h *TableHandle[Row]) id() (sys.TableId, error) {
	if !h.ready {
		tid, err := sys.TableIdFromName(h.name)
		if err != nil {
			return 0, err
		}
		h.tableId = tid
		h.ready = true
	}
	return h.tableId, nil
}

// Count returns the number of rows in the table.
func (h *TableHandle[Row]) Count() (uint64, error) {
	tid, err := h.id()
	if err != nil {
		return 0, err
	}
	return sys.TableRowCount(tid)
}

// tableReuseWriter is a package-level writer reused by table Insert operations
// to avoid per-call heap allocations under TinyGo WASM.
var tableReuseWriter = bsatn.NewWriter()

// Insert encodes row as BSATN and inserts it into the table.
// The returned row reflects any auto-increment or generated column values.
func (h *TableHandle[Row]) Insert(row Row) (Row, error) {
	tid, err := h.id()
	if err != nil {
		return row, err
	}
	tableReuseWriter.Reset()
	h.encode(tableReuseWriter, row)
	out, err := sys.InsertBsatnReuse(tid, tableReuseWriter.Bytes())
	if err != nil {
		return row, err
	}
	// The host writes back only generated column values (e.g. auto-increment),
	// not the full row. When there are no generated columns, out is empty.
	if len(out) == 0 {
		return row, nil
	}
	tableReuseReader.Reset(out)
	return h.decode(tableReuseReader)
}

// Delete removes the row equal to row from the table.
// Returns true if the row was found and deleted.
func (h *TableHandle[Row]) Delete(row Row) (bool, error) {
	tid, err := h.id()
	if err != nil {
		return false, err
	}
	tableReuseWriter.Reset()
	h.encode(tableReuseWriter, row)
	// Wrap in a 1-element Vec<ProductValue> as required by the ABI.
	payload := encodeRowVec(tableReuseWriter.Bytes())
	deleted, err := sys.DeleteAllByEqBsatn(tid, payload)
	return deleted > 0, err
}

// Iter iterates over all rows in the table in an unspecified order.
// The iterator is safe to break early.
func (h *TableHandle[Row]) Iter() iter.Seq2[Row, error] {
	return func(yield func(Row, error) bool) {
		tid, err := h.id()
		if err != nil {
			var zero Row
			yield(zero, err)
			return
		}
		rowIter, err := sys.TableScanBsatn(tid)
		if err != nil {
			var zero Row
			yield(zero, err)
			return
		}
		iterRows(rowIter, h.decode, yield)
	}
}

// ── Internal helpers ──────────────────────────────────────────────────────────

// tableReuseReader is a package-level reader reused by iterRows to avoid
// per-call heap allocations under TinyGo WASM.
var tableReuseReader = bsatn.NewReader(nil)

// iterRows reads all rows from a RowIter, decoding each with decode and yielding
// via yield. Stops when yield returns false or the iterator is exhausted.
func iterRows[Row any](rowIter sys.RowIter, decode func(*bsatn.Reader) (Row, error), yield func(Row, error) bool) {
	data, err := sys.CollectIterReuse(rowIter)
	if err != nil {
		var zero Row
		yield(zero, err)
		return
	}
	tableReuseReader.Reset(data)
	for tableReuseReader.Remaining() > 0 {
		row, err := decode(tableReuseReader)
		if !yield(row, err) || err != nil {
			return
		}
	}
}

// deleteReuseWriter is a package-level writer reused by encodeRowVec
// to avoid per-call heap allocations under TinyGo WASM.
var deleteReuseWriter = bsatn.NewWriter()

// encodeRowVec wraps a single BSATN-encoded row in a Vec<ProductValue> envelope
// (4-byte LE count = 1, then the row bytes), as required by DeleteAllByEqBsatn.
func encodeRowVec(rowBytes []byte) []byte {
	deleteReuseWriter.Reset()
	deleteReuseWriter.WriteArrayLen(1)
	deleteReuseWriter.WriteRaw(rowBytes)
	return deleteReuseWriter.Bytes()
}
