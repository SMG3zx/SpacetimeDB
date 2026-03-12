package client

import (
	"encoding/hex"
	"iter"
	"sync"

	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/protocol"
)

// TableCache is a thread-safe in-memory store for BSATN-encoded rows of type Row.
//
// Rows are keyed by their raw BSATN bytes so that equality works even for
// float-containing types. Reference counting handles overlapping subscriptions.
type TableCache[Row any] struct {
	mu     sync.RWMutex
	rows   map[string]*rowEntry[Row] // key = hex(bsatn bytes)
	decode func(*bsatn.Reader) (Row, error)
}

type rowEntry[Row any] struct {
	row      Row
	key      string // hex-encoded BSATN bytes — canonical identity
	refCount uint32
}

// NewTableCache returns an empty cache using decode to deserialise each row.
func NewTableCache[Row any](decode func(*bsatn.Reader) (Row, error)) *TableCache[Row] {
	return &TableCache[Row]{
		rows:   make(map[string]*rowEntry[Row]),
		decode: decode,
	}
}

// Count returns the number of unique rows currently in the cache.
func (c *TableCache[Row]) Count() int {
	c.mu.RLock()
	n := len(c.rows)
	c.mu.RUnlock()
	return n
}

// Iter returns an iterator over all cached rows.
func (c *TableCache[Row]) Iter() iter.Seq[Row] {
	return func(yield func(Row) bool) {
		c.mu.RLock()
		entries := make([]*rowEntry[Row], 0, len(c.rows))
		for _, e := range c.rows {
			entries = append(entries, e)
		}
		c.mu.RUnlock()
		for _, e := range entries {
			if !yield(e.row) {
				return
			}
		}
	}
}

// ApplyInserts decodes each row in the BsatnRowList and inserts it into the cache.
// Returns the list of newly inserted rows (rows whose ref count just became 1).
func (c *TableCache[Row]) ApplyInserts(list *protocol.BsatnRowList) ([]Row, error) {
	var inserted []Row
	c.mu.Lock()
	defer c.mu.Unlock()
	for rawBytes := range list.Rows() {
		key := hex.EncodeToString(rawBytes)
		if e, ok := c.rows[key]; ok {
			e.refCount++
			continue
		}
		row, err := c.decode(bsatn.NewReader(rawBytes))
		if err != nil {
			return inserted, err
		}
		c.rows[key] = &rowEntry[Row]{row: row, key: key, refCount: 1}
		inserted = append(inserted, row)
	}
	return inserted, nil
}

// ApplyDeletes removes each row in the BsatnRowList from the cache.
// A row is only truly removed when its ref count reaches zero.
// Returns the list of deleted rows (rows fully removed from cache).
func (c *TableCache[Row]) ApplyDeletes(list *protocol.BsatnRowList) ([]Row, error) {
	var deleted []Row
	c.mu.Lock()
	defer c.mu.Unlock()
	for rawBytes := range list.Rows() {
		key := hex.EncodeToString(rawBytes)
		e, ok := c.rows[key]
		if !ok {
			continue
		}
		e.refCount--
		if e.refCount == 0 {
			deleted = append(deleted, e.row)
			delete(c.rows, key)
		}
	}
	return deleted, nil
}

// TableHandle combines a TableCache with callbacks and implements TableUpdateHandler.
// Generated module bindings use one TableHandle per table, registered with the connection.
type TableHandle[Row any] struct {
	*TableCache[Row]
	Callbacks *TableCallbacks[EventContext, Row]
}

// NewTableHandle creates a TableHandle with an empty cache and no callbacks registered.
func NewTableHandle[Row any](decode func(*bsatn.Reader) (Row, error)) *TableHandle[Row] {
	return &TableHandle[Row]{
		TableCache: NewTableCache[Row](decode),
		Callbacks:  NewTableCallbacks[EventContext, Row](),
	}
}

// OnInsert registers a callback for row inserts. Returns a CallbackId for removal.
func (h *TableHandle[Row]) OnInsert(fn func(Row, EventContext)) CallbackId {
	return h.Callbacks.OnInsert.Register(func(ctx EventContext, row Row) { fn(row, ctx) })
}

// OnDelete registers a callback for row deletes. Returns a CallbackId for removal.
func (h *TableHandle[Row]) OnDelete(fn func(Row, EventContext)) CallbackId {
	return h.Callbacks.OnDelete.Register(func(ctx EventContext, row Row) { fn(row, ctx) })
}

// OnUpdate registers a callback for row updates. Returns a CallbackId for removal.
func (h *TableHandle[Row]) OnUpdate(fn func(old, new Row, ctx EventContext)) CallbackId {
	return h.Callbacks.OnUpdate.Register(func(ctx EventContext, pair [2]Row) { fn(pair[0], pair[1], ctx) })
}

// ApplyInserts implements TableUpdateHandler: inserts rows and fires OnInsert callbacks.
func (h *TableHandle[Row]) ApplyInserts(rows *protocol.BsatnRowList) error {
	inserted, err := h.TableCache.ApplyInserts(rows)
	ctx := EventContext{} // populated by the connection when available
	for _, row := range inserted {
		h.Callbacks.OnInsert.Invoke(ctx, row)
	}
	return err
}

// ApplyDeletes implements TableUpdateHandler: removes rows and fires OnDelete callbacks.
func (h *TableHandle[Row]) ApplyDeletes(rows *protocol.BsatnRowList) error {
	deleted, err := h.TableCache.ApplyDeletes(rows)
	ctx := EventContext{}
	for _, row := range deleted {
		h.Callbacks.OnDelete.Invoke(ctx, row)
	}
	return err
}

// UniqueIndex maintains a secondary index mapping a column value to a row pointer.
// Safe for concurrent use.
type UniqueIndex[Row any, Col comparable] struct {
	mu      sync.RWMutex
	entries map[Col]*Row
	extract func(*Row) Col
}

// NewUniqueIndex returns an empty unique index.
func NewUniqueIndex[Row any, Col comparable](extract func(*Row) Col) *UniqueIndex[Row, Col] {
	return &UniqueIndex[Row, Col]{
		entries: make(map[Col]*Row),
		extract: extract,
	}
}

// Find returns the row with the given column value, if present.
func (idx *UniqueIndex[Row, Col]) Find(key Col) (*Row, bool) {
	idx.mu.RLock()
	r, ok := idx.entries[key]
	idx.mu.RUnlock()
	return r, ok
}

// Insert adds a row to the index.
func (idx *UniqueIndex[Row, Col]) Insert(row *Row) {
	key := idx.extract(row)
	idx.mu.Lock()
	idx.entries[key] = row
	idx.mu.Unlock()
}

// Remove deletes a row from the index.
func (idx *UniqueIndex[Row, Col]) Remove(row *Row) {
	key := idx.extract(row)
	idx.mu.Lock()
	delete(idx.entries, key)
	idx.mu.Unlock()
}
