//go:build tinygo

package spacetimedb

import (
	"iter"

	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
)

// UniqueIndex provides Find, Delete, and Update operations via a unique column index.
type UniqueIndex[Row any, Col any] struct {
	indexName string
	indexId   sys.IndexId
	ready     bool
	tableId   sys.TableId
	tableName string
	tableReady bool
	encodeCol func(*bsatn.Writer, Col)
	encodeRow func(*bsatn.Writer, Row)
	decodeRow func(*bsatn.Reader) (Row, error)
}

// NewUniqueIndex creates a UniqueIndex for the named index.
func NewUniqueIndex[Row any, Col any](
	tableName, indexName string,
	encodeCol func(*bsatn.Writer, Col),
	encodeRow func(*bsatn.Writer, Row),
	decodeRow func(*bsatn.Reader) (Row, error),
) *UniqueIndex[Row, Col] {
	return &UniqueIndex[Row, Col]{
		indexName: indexName,
		tableName: tableName,
		encodeCol: encodeCol,
		encodeRow: encodeRow,
		decodeRow: decodeRow,
	}
}

// iid returns the cached IndexId, resolving it from the host on first call.
func (idx *UniqueIndex[Row, Col]) iid() (sys.IndexId, error) {
	if !idx.ready {
		id, err := sys.IndexIdFromName(idx.indexName)
		if err != nil {
			return 0, err
		}
		idx.indexId = id
		idx.ready = true
	}
	return idx.indexId, nil
}

// tid returns the cached TableId, resolving it from the host on first call.
func (idx *UniqueIndex[Row, Col]) tid() (sys.TableId, error) {
	if !idx.tableReady {
		id, err := sys.TableIdFromName(idx.tableName)
		if err != nil {
			return 0, err
		}
		idx.tableId = id
		idx.tableReady = true
	}
	return idx.tableId, nil
}

// Find returns the row whose unique column equals col, or nil if not found.
// Uses the point index scan (spacetime_10.4) for efficient exact-match lookup.
func (idx *UniqueIndex[Row, Col]) Find(col Col) (*Row, error) {
	iid, err := idx.iid()
	if err != nil {
		return nil, err
	}
	reuseWriter.Reset()
	idx.encodeCol(reuseWriter, col)
	rowIter, err := sys.IndexScanPointBsatn(iid, reuseWriter.Bytes())
	if err != nil {
		return nil, err
	}
	var found *Row
	iterRows(rowIter, idx.decodeRow, func(row Row, e error) bool {
		if e != nil {
			err = e
			return false
		}
		r := row
		found = &r
		return false // stop after first match
	})
	if err != nil {
		return nil, err
	}
	return found, nil
}

// Delete removes the row whose unique column equals col.
// Returns true if a row was found and deleted.
// Uses the point index scan (spacetime_10.4) for efficient exact-match deletion.
func (idx *UniqueIndex[Row, Col]) Delete(col Col) (bool, error) {
	iid, err := idx.iid()
	if err != nil {
		return false, err
	}
	reuseWriter.Reset()
	idx.encodeCol(reuseWriter, col)
	deleted, err := sys.DeleteByIndexScanPointBsatn(iid, reuseWriter.Bytes())
	return deleted > 0, err
}

// reuseWriter is a package-level writer reused by hot-path operations
// to avoid per-call heap allocations under TinyGo WASM.
var reuseWriter = bsatn.NewWriter()

// reuseReader is a package-level reader reused by Update to decode generated
// column values without per-call heap allocations under TinyGo WASM.
var reuseReader = bsatn.NewReader(nil)

// Update replaces the row identified by the unique column value in row.
func (idx *UniqueIndex[Row, Col]) Update(row Row) (Row, error) {
	tid, err := idx.tid()
	if err != nil {
		return row, err
	}
	iid, err := idx.iid()
	if err != nil {
		return row, err
	}
	reuseWriter.Reset()
	idx.encodeRow(reuseWriter, row)
	out, err := sys.UpdateBsatnReuse(tid, iid, reuseWriter.Bytes())
	if err != nil {
		return row, err
	}
	// The host writes back only generated column values (e.g. auto-increment),
	// not the full row. When there are no generated columns, out is empty.
	if len(out) == 0 {
		return row, nil
	}
	reuseReader.Reset(out)
	return idx.decodeRow(reuseReader)
}

// BoundKind indicates whether a range bound is inclusive, exclusive, or absent.
type BoundKind uint8

const (
	BoundIncluded  BoundKind = 0 // value is included in the range
	BoundExcluded  BoundKind = 1 // value is excluded from the range
	BoundUnbounded BoundKind = 2 // no bound (open-ended)
)

// Bound represents one end of a range for BTree index scans.
// Use [NewBoundIncluded], [NewBoundExcluded], or [NewBoundUnbounded] to construct.
type Bound[Col any] struct {
	Kind  BoundKind
	Value Col // only meaningful when Kind != BoundUnbounded
}

// NewBoundIncluded returns a Bound that includes the given value (closed bound: <=/>= val).
func NewBoundIncluded[Col any](v Col) Bound[Col] { return Bound[Col]{Kind: BoundIncluded, Value: v} }

// NewBoundExcluded returns a Bound that excludes the given value (open bound: </>  val).
func NewBoundExcluded[Col any](v Col) Bound[Col] { return Bound[Col]{Kind: BoundExcluded, Value: v} }

// NewBoundUnbounded returns a Bound with no restriction on that end of the range.
func NewBoundUnbounded[Col any]() Bound[Col] { return Bound[Col]{Kind: BoundUnbounded} }

// boundWriter is a package-level writer reused by encodeBound to avoid
// per-call heap allocations under TinyGo WASM.
var boundWriter = bsatn.NewWriter()

// encodeBound encodes a Bound<Col> as BSATN: tag byte + encoded value (if bounded).
func encodeBound[Col any](b Bound[Col], encodeCol func(*bsatn.Writer, Col)) []byte {
	if b.Kind == BoundUnbounded {
		return nil // nil = Unbounded in the ABI
	}
	boundWriter.Reset()
	boundWriter.WriteVariantTag(uint8(b.Kind))
	encodeCol(boundWriter, b.Value)
	return boundWriter.Bytes()
}

// EncodeBound is the exported version of encodeBound. Generated multi-column
// BTree prefix queries use this to encode bounds for a trailing column whose
// type differs from the first (Col) column.
func EncodeBound[Col any](b Bound[Col], encodeCol func(*bsatn.Writer, Col)) []byte {
	return encodeBound(b, encodeCol)
}

// BTreeIndex provides Filter and range-scan operations over a btree-indexed column.
type BTreeIndex[Row any, Col any] struct {
	indexName string
	indexId   sys.IndexId
	ready     bool
	encodeCol func(*bsatn.Writer, Col)
	decodeRow func(*bsatn.Reader) (Row, error)
}

// NewBTreeIndex creates a BTreeIndex for the named index.
func NewBTreeIndex[Row any, Col any](
	indexName string,
	encodeCol func(*bsatn.Writer, Col),
	decodeRow func(*bsatn.Reader) (Row, error),
) *BTreeIndex[Row, Col] {
	return &BTreeIndex[Row, Col]{
		indexName: indexName,
		encodeCol: encodeCol,
		decodeRow: decodeRow,
	}
}

// iid returns the cached IndexId, resolving it from the host on first call.
func (idx *BTreeIndex[Row, Col]) iid() (sys.IndexId, error) {
	if !idx.ready {
		id, err := sys.IndexIdFromName(idx.indexName)
		if err != nil {
			return 0, err
		}
		idx.indexId = id
		idx.ready = true
	}
	return idx.indexId, nil
}

// Filter returns an iterator over all rows whose indexed column equals col.
func (idx *BTreeIndex[Row, Col]) Filter(col Col) iter.Seq2[Row, error] {
	return func(yield func(Row, error) bool) {
		iid, err := idx.iid()
		if err != nil {
			var zero Row
			yield(zero, err)
			return
		}
		reuseWriter.Reset()
		idx.encodeCol(reuseWriter, col)
		prefix := reuseWriter.Bytes()
		rowIter, err := sys.IndexScanRangeBsatn(iid, prefix, 1, nil, nil)
		if err != nil {
			var zero Row
			yield(zero, err)
			return
		}
		iterRows(rowIter, idx.decodeRow, yield)
	}
}

// FilterRange returns an iterator over all rows whose indexed column falls within the
// given range. Use [NewBoundIncluded], [NewBoundExcluded], or [NewBoundUnbounded] to
// construct the bounds.
//
// Example — rows where col is in [lo, hi):
//
//	for row, err := range idx.FilterRange(NewBoundIncluded(lo), NewBoundExcluded(hi)) { ... }
func (idx *BTreeIndex[Row, Col]) FilterRange(rstart, rend Bound[Col]) iter.Seq2[Row, error] {
	return func(yield func(Row, error) bool) {
		iid, err := idx.iid()
		if err != nil {
			var zero Row
			yield(zero, err)
			return
		}
		rstartBytes := encodeBound(rstart, idx.encodeCol)
		rendBytes := encodeBound(rend, idx.encodeCol)
		rowIter, err := sys.IndexScanRangeBsatn(iid, nil, 0, rstartBytes, rendBytes)
		if err != nil {
			var zero Row
			yield(zero, err)
			return
		}
		iterRows(rowIter, idx.decodeRow, yield)
	}
}

// FilterPrefixed performs a composite-prefix scan followed by an optional range bound on
// the trailing column. prefixBytes is the concatenated BSATN encoding of the leading
// key columns, prefixElems is the number of those columns, and rstart/rend constrain the
// next column beyond the prefix.
//
// This is the low-level building block for multi-column BTree index queries.
// stdbgen generates typed wrappers on top of this for composite indexes.
func (idx *BTreeIndex[Row, Col]) FilterPrefixed(prefixBytes []byte, prefixElems uint32, rstart, rend Bound[Col]) iter.Seq2[Row, error] {
	return func(yield func(Row, error) bool) {
		iid, err := idx.iid()
		if err != nil {
			var zero Row
			yield(zero, err)
			return
		}
		rstartBytes := encodeBound(rstart, idx.encodeCol)
		rendBytes := encodeBound(rend, idx.encodeCol)
		rowIter, err := sys.IndexScanRangeBsatn(iid, prefixBytes, prefixElems, rstartBytes, rendBytes)
		if err != nil {
			var zero Row
			yield(zero, err)
			return
		}
		iterRows(rowIter, idx.decodeRow, yield)
	}
}

// FilterPrefixedRaw is like FilterPrefixed but accepts pre-encoded BSATN bound bytes
// instead of typed Bound[Col] values. This allows multi-column composite indexes where
// the trailing column type differs from Col (the first-column type parameter).
//
// Pass nil for rstartBytes/rendBytes for unbounded ends.
func (idx *BTreeIndex[Row, Col]) FilterPrefixedRaw(prefixBytes []byte, prefixElems uint32, rstartBytes, rendBytes []byte) iter.Seq2[Row, error] {
	return func(yield func(Row, error) bool) {
		iid, err := idx.iid()
		if err != nil {
			var zero Row
			yield(zero, err)
			return
		}
		rowIter, err := sys.IndexScanRangeBsatn(iid, prefixBytes, prefixElems, rstartBytes, rendBytes)
		if err != nil {
			var zero Row
			yield(zero, err)
			return
		}
		iterRows(rowIter, idx.decodeRow, yield)
	}
}
