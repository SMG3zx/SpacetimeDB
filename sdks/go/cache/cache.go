package cache

import (
	"sync"
	"sync/atomic"

	sdktypes "github.com/clockworklabs/spacetimedb/sdks/go/types"
)

// Store holds client-side table state and applies transactions atomically.
type Store struct {
	writeMu sync.Mutex
	state   atomic.Pointer[snapshot]
}

type snapshot struct {
	tables map[string]map[string][]byte
}

func newSnapshot() *snapshot {
	return &snapshot{tables: map[string]map[string][]byte{}}
}

func cloneSnapshot(src *snapshot) *snapshot {
	if src == nil {
		return newSnapshot()
	}

	next := &snapshot{tables: make(map[string]map[string][]byte, len(src.tables))}
	for tableName, rows := range src.tables {
		clonedRows := make(map[string][]byte, len(rows))
		for key, value := range rows {
			clonedRows[key] = cloneBytes(value)
		}
		next.tables[tableName] = clonedRows
	}

	return next
}

func cloneBytes(value []byte) []byte {
	if value == nil {
		return nil
	}
	cloned := make([]byte, len(value))
	copy(cloned, value)
	return cloned
}

func NewStore() *Store {
	store := &Store{}
	store.state.Store(newSnapshot())
	return store
}

// ApplyTransaction applies a transaction as a single atomic state update.
func (s *Store) ApplyTransaction(tx sdktypes.Transaction) {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	next := cloneSnapshot(s.state.Load())

	for _, tableMutation := range tx.Tables {
		rows, ok := next.tables[tableMutation.Table]
		if !ok {
			rows = map[string][]byte{}
			next.tables[tableMutation.Table] = rows
		}

		for _, key := range tableMutation.Deletes {
			delete(rows, key)
		}
		for _, row := range tableMutation.Inserts {
			rows[row.Key] = cloneBytes(row.Data)
		}
	}

	s.state.Store(next)
}

func (s *Store) Get(table, key string) ([]byte, bool) {
	current := s.state.Load()
	if current == nil {
		return nil, false
	}
	rows, ok := current.tables[table]
	if !ok {
		return nil, false
	}
	value, ok := rows[key]
	if !ok {
		return nil, false
	}
	return cloneBytes(value), true
}

// TableSnapshot returns a copy of all rows in one table keyed by row key.
func (s *Store) TableSnapshot(table string) map[string][]byte {
	current := s.state.Load()
	if current == nil {
		return map[string][]byte{}
	}

	rows, ok := current.tables[table]
	if !ok {
		return map[string][]byte{}
	}

	out := make(map[string][]byte, len(rows))
	for key, value := range rows {
		out[key] = cloneBytes(value)
	}
	return out
}

// Snapshot returns a full copy of the cache grouped by table and row key.
func (s *Store) Snapshot() map[string]map[string][]byte {
	current := s.state.Load()
	if current == nil {
		return map[string]map[string][]byte{}
	}

	out := make(map[string]map[string][]byte, len(current.tables))
	for tableName, rows := range current.tables {
		rowsCopy := make(map[string][]byte, len(rows))
		for key, value := range rows {
			rowsCopy[key] = cloneBytes(value)
		}
		out[tableName] = rowsCopy
	}

	return out
}
