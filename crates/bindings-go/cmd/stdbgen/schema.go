package main

// ── Schema types ──────────────────────────────────────────────────────────────

// Schema is the top-level schema definition loaded from stdb.yaml.
type Schema struct {
	Package    string       `yaml:"package"`
	Module     string       `yaml:"module"`
	Tables     []Table      `yaml:"tables"`
	Reducers   []Reducer    `yaml:"reducers"`
	Procedures []Procedure  `yaml:"procedures"`
	Views      []View       `yaml:"views"`
	Scheduled  []Scheduled  `yaml:"scheduled"`
	Lifecycle  Lifecycle    `yaml:"lifecycle"`
	Types      []TypeExport `yaml:"types"`
}

// Table describes a SpacetimeDB table.
type Table struct {
	Name          string        `yaml:"name"`
	Columns       []Column      `yaml:"columns"`
	PrimaryKey    []string      `yaml:"primary_key"`
	UniqueIndexes []UniqueIndex `yaml:"unique_indexes"`
	BTreeIndexes  []BTreeIndex  `yaml:"btree_indexes"`
	Access        string        `yaml:"access"` // "public" or "private" (default: "public")
	IsEvent       bool          `yaml:"is_event"`
}

// UniqueIndex describes a unique index on a table.
type UniqueIndex struct {
	Name    string   `yaml:"name"`
	Columns []string `yaml:"columns"`
}

// BTreeIndex describes a BTree index on a table.
type BTreeIndex struct {
	Name    string   `yaml:"name"`
	Columns []string `yaml:"columns"`
}

// Column describes a single column of a table (or reducer/procedure/view parameter).
type Column struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"`
}

// Reducer describes a SpacetimeDB reducer.
type Reducer struct {
	Name       string   `yaml:"name"`
	Params     []Column `yaml:"params"`
	Visibility string   `yaml:"visibility"` // "public" or "private" (default: "public")
}

// Procedure describes a SpacetimeDB stored procedure (can span transactions, make HTTP calls).
type Procedure struct {
	Name       string   `yaml:"name"`
	Params     []Column `yaml:"params"`
	ReturnType string   `yaml:"return_type"` // type name; empty = void
	Visibility string   `yaml:"visibility"`  // "public" or "private" (default: "public")
}

// View describes a SpacetimeDB view (read-only query, no transaction).
type View struct {
	Name        string   `yaml:"name"`
	Params      []Column `yaml:"params"`
	ReturnType  string   `yaml:"return_type"` // type name of the returned row
	IsPublic    bool     `yaml:"is_public"`
	IsAnonymous bool     `yaml:"is_anonymous"`
}

// Scheduled describes a scheduled reducer.
type Scheduled struct {
	Name  string `yaml:"name"`  // name of the reducer to schedule
	Table string `yaml:"table"` // name of the schedule-tracking table
}

// Lifecycle maps lifecycle events to reducer names.
type Lifecycle struct {
	OnInit       string `yaml:"on_init"`
	OnConnect    string `yaml:"on_connect"`
	OnDisconnect string `yaml:"on_disconnect"`
}

// TypeExport describes a named custom type to export into the module typespace.
// Exactly one of Product or Sum must be set.
type TypeExport struct {
	Name    string       `yaml:"name"`
	Product []Column     `yaml:"product"` // product type (struct)
	Sum     []SumVariant `yaml:"sum"`     // sum type (enum / tagged union)
	Scope   []string     `yaml:"scope"`   // optional namespace path
}

// SumVariant is one variant of a sum type.
type SumVariant struct {
	Name string `yaml:"name"`
	Type string `yaml:"type"` // inner type; empty = unit (no payload)
}

// ── Pre-computed code-gen helpers ─────────────────────────────────────────────

// PrefixColInfo holds the resolved type info for one column in a composite index.
type PrefixColInfo struct {
	Name       string // column name
	GoType     string // e.g., "types.Identity"
	EncodeStmt string // full statement for the prefix encoder, e.g., "sender.WriteBsatn(w)"
}

// PrefixFilterFunc describes one generated multi-column prefix-filter function.
type PrefixFilterFunc struct {
	FuncName           string // e.g., "FilterMessageBySenderAndSentRange"
	TableName          string // e.g., "Message"
	IdxVarName         string // e.g., "messageMessageSenderSentIdxBTreeIdx"
	PrefixCols         []PrefixColInfo
	TrailingType       string // Go type of the trailing (range) column
	TrailingEncodeExpr string // encodeCol closure, e.g. "func(w *bsatn.Writer, v types.Timestamp) { v.WriteBsatn(w) }"
	NumPrefix          uint32
}
