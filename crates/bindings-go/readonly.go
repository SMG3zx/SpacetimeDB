//go:build tinygo

package spacetimedb

import "iter"

// ── Read-only wrappers ────────────────────────────────────────────────────────
//
// View handlers must not mutate table data. These types wrap the mutable handles
// and expose only Count, Iter (table), Find (unique index), and Filter/FilterRange
// (BTree index), providing compile-time safety against accidental mutations.

// ReadOnlyTable wraps a TableHandle and exposes only Count and Iter.
// Use TableHandle.AsReadOnly to obtain one, typically inside a view handler.
type ReadOnlyTable[Row any] struct {
	h *TableHandle[Row]
}

// AsReadOnly returns a ReadOnlyTable that delegates Count and Iter to h.
func (h *TableHandle[Row]) AsReadOnly() ReadOnlyTable[Row] {
	return ReadOnlyTable[Row]{h: h}
}

// Count returns the number of rows in the table.
func (t ReadOnlyTable[Row]) Count() (uint64, error) {
	return t.h.Count()
}

// Iter iterates over all rows in the table in an unspecified order.
func (t ReadOnlyTable[Row]) Iter() iter.Seq2[Row, error] {
	return t.h.Iter()
}

// ReadOnlyUniqueIndex wraps a UniqueIndex and exposes only Find.
// Use UniqueIndex.AsReadOnly to obtain one, typically inside a view handler.
type ReadOnlyUniqueIndex[Row any, Col any] struct {
	idx *UniqueIndex[Row, Col]
}

// AsReadOnly returns a ReadOnlyUniqueIndex that delegates Find to idx.
func (idx *UniqueIndex[Row, Col]) AsReadOnly() ReadOnlyUniqueIndex[Row, Col] {
	return ReadOnlyUniqueIndex[Row, Col]{idx: idx}
}

// Find returns the row whose unique column equals col, or nil if not found.
func (r ReadOnlyUniqueIndex[Row, Col]) Find(col Col) (*Row, error) {
	return r.idx.Find(col)
}

// ReadOnlyBTreeIndex wraps a BTreeIndex and exposes only Filter and FilterRange.
// Use BTreeIndex.AsReadOnly to obtain one, typically inside a view handler.
type ReadOnlyBTreeIndex[Row any, Col any] struct {
	idx *BTreeIndex[Row, Col]
}

// AsReadOnly returns a ReadOnlyBTreeIndex that delegates Filter and FilterRange to idx.
func (idx *BTreeIndex[Row, Col]) AsReadOnly() ReadOnlyBTreeIndex[Row, Col] {
	return ReadOnlyBTreeIndex[Row, Col]{idx: idx}
}

// Filter returns an iterator over all rows whose indexed column equals col.
func (r ReadOnlyBTreeIndex[Row, Col]) Filter(col Col) iter.Seq2[Row, error] {
	return r.idx.Filter(col)
}

// FilterRange returns an iterator over all rows whose indexed column falls within [lo, hi].
func (r ReadOnlyBTreeIndex[Row, Col]) FilterRange(lo, hi Bound[Col]) iter.Seq2[Row, error] {
	return r.idx.FilterRange(lo, hi)
}

