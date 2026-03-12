package main

import (
	"testing"
)

// ── titleCase ────────────────────────────────────────────────────────────────

func TestTitleCase(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"", ""},
		{"a", "A"},
		{"hello", "Hello"},
		{"Hello", "Hello"},
		{"hELLO", "HELLO"},
		{"x", "X"},
	}
	for _, tt := range tests {
		if got := titleCase(tt.in); got != tt.want {
			t.Errorf("titleCase(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// ── camelTitle ───────────────────────────────────────────────────────────────

func TestCamelTitle(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"my_index", "MyIndex"},
		{"sent_at", "SentAt"},
		{"id", "Id"},
		{"a_b_c", "ABC"},
		{"simple", "Simple"},
		{"", ""},
		{"already_Title", "AlreadyTitle"},
	}
	for _, tt := range tests {
		if got := camelTitle(tt.in); got != tt.want {
			t.Errorf("camelTitle(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// ── camelCols ────────────────────────────────────────────────────────────────

func TestCamelCols(t *testing.T) {
	tests := []struct {
		in   []string
		want string
	}{
		{[]string{"sent_at", "id"}, "SentAtId"},
		{[]string{"name"}, "Name"},
		{[]string{}, ""},
		{[]string{"a_b", "c_d", "e"}, "ABCDE"},
	}
	for _, tt := range tests {
		if got := camelCols(tt.in); got != tt.want {
			t.Errorf("camelCols(%v) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// ── lowerCase ────────────────────────────────────────────────────────────────

func TestLowerCase(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"", ""},
		{"A", "a"},
		{"Hello", "hello"},
		{"hello", "hello"},
		{"X", "x"},
		{"ABC", "aBC"},
	}
	for _, tt := range tests {
		if got := lowerCase(tt.in); got != tt.want {
			t.Errorf("lowerCase(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// ── tableAccess ──────────────────────────────────────────────────────────────

func TestTableAccess(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"private", "spacetimedb.TableAccessPrivate"},
		{"Private", "spacetimedb.TableAccessPrivate"},
		{"PRIVATE", "spacetimedb.TableAccessPrivate"},
		{"public", "spacetimedb.TableAccessPublic"},
		{"Public", "spacetimedb.TableAccessPublic"},
		{"", "spacetimedb.TableAccessPublic"},
		{"anything", "spacetimedb.TableAccessPublic"},
	}
	for _, tt := range tests {
		if got := tableAccess(tt.in); got != tt.want {
			t.Errorf("tableAccess(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// ── reducerVisibility ────────────────────────────────────────────────────────

func TestReducerVisibility(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"private", "spacetimedb.ReducerVisibilityPrivate"},
		{"Private", "spacetimedb.ReducerVisibilityPrivate"},
		{"PRIVATE", "spacetimedb.ReducerVisibilityPrivate"},
		{"public", "spacetimedb.ReducerVisibilityClientCallable"},
		{"", "spacetimedb.ReducerVisibilityClientCallable"},
		{"anything", "spacetimedb.ReducerVisibilityClientCallable"},
	}
	for _, tt := range tests {
		if got := reducerVisibility(tt.in); got != tt.want {
			t.Errorf("reducerVisibility(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// ── encodeExprOf ─────────────────────────────────────────────────────────────

func TestEncodeExprOf(t *testing.T) {
	tests := []struct {
		typ, col, want string
	}{
		// Standard write method
		{"U32", "count", "w.WriteU32(row.Count)"},
		{"String", "name", "w.WriteString(row.Name)"},
		{"Bool", "active", "w.WriteBool(row.Active)"},
		{"I64", "offset", "w.WriteI64(row.Offset)"},
		{"F32", "x_pos", "w.WriteF32(row.XPos)"},
		{"F64", "y_pos", "w.WriteF64(row.YPos)"},
		{"Bytes", "data", "w.WriteBytes(row.Data)"},
		{"I8", "small", "w.WriteI8(row.Small)"},
		{"U8", "byte_val", "w.WriteU8(row.ByteVal)"},
		{"I16", "short", "w.WriteI16(row.Short)"},
		{"U16", "ushort", "w.WriteU16(row.Ushort)"},
		{"U64", "big", "w.WriteU64(row.Big)"},
		// Special write (Identity, Timestamp)
		{"Identity", "sender", "row.Sender.WriteBsatn(w)"},
		{"Timestamp", "created_at", "row.CreatedAt.WriteBsatn(w)"},
		// Option<T> with standard inner
		{"Option<U32>", "age", "bsatn.WriteOption(w, row.Age, func(w *bsatn.Writer, v uint32) { w.WriteU32(v) })"},
		{"Option<String>", "bio", "bsatn.WriteOption(w, row.Bio, func(w *bsatn.Writer, v string) { w.WriteString(v) })"},
		// Option<T> with special inner
		{"Option<Identity>", "owner", "bsatn.WriteOption(w, row.Owner, func(w *bsatn.Writer, v types.Identity) { v.WriteBsatn(w) })"},
		{"Option<Timestamp>", "deleted_at", "bsatn.WriteOption(w, row.DeletedAt, func(w *bsatn.Writer, v types.Timestamp) { v.WriteBsatn(w) })"},
		// Unknown type falls back to String
		{"UnknownType", "field", "w.WriteString(row.Field)"},
	}
	for _, tt := range tests {
		if got := encodeExprOf(tt.typ, tt.col); got != tt.want {
			t.Errorf("encodeExprOf(%q, %q) =\n  %q\nwant\n  %q", tt.typ, tt.col, got, tt.want)
		}
	}
}

// ── colTypeOf ────────────────────────────────────────────────────────────────

func TestColTypeOf(t *testing.T) {
	cols := []Column{
		{Name: "id", Type: "U64"},
		{Name: "name", Type: "String"},
		{Name: "Score", Type: "F32"},
	}

	tests := []struct {
		colName, want string
	}{
		{"id", "U64"},
		{"ID", "U64"},     // case-insensitive
		{"name", "String"},
		{"Score", "F32"},
		{"score", "F32"},  // case-insensitive
		{"missing", "String"}, // fallback
	}
	for _, tt := range tests {
		if got := colTypeOf(cols, tt.colName); got != tt.want {
			t.Errorf("colTypeOf(cols, %q) = %q, want %q", tt.colName, got, tt.want)
		}
	}
}

// ── colIndex ─────────────────────────────────────────────────────────────────

func TestColIndex(t *testing.T) {
	cols := []Column{
		{Name: "id", Type: "U64"},
		{Name: "name", Type: "String"},
		{Name: "age", Type: "U32"},
	}

	tests := []struct {
		colName string
		want    int
	}{
		{"id", 0},
		{"ID", 0},       // case-insensitive
		{"name", 1},
		{"age", 2},
		{"AGE", 2},      // case-insensitive
		{"missing", -1}, // not found
	}
	for _, tt := range tests {
		if got := colIndex(cols, tt.colName); got != tt.want {
			t.Errorf("colIndex(cols, %q) = %d, want %d", tt.colName, got, tt.want)
		}
	}
}

// ── idxKeyGoTypeOf ───────────────────────────────────────────────────────────

func TestIdxKeyGoTypeOf(t *testing.T) {
	tc := map[string][]Column{
		"User": {
			{Name: "id", Type: "U64"},
			{Name: "name", Type: "String"},
			{Name: "sender", Type: "Identity"},
		},
	}

	tests := []struct {
		table string
		cols  []string
		want  string
	}{
		{"User", []string{"id"}, "uint64"},
		{"User", []string{"name"}, "string"},
		{"User", []string{"sender"}, "types.Identity"},
		{"User", []string{}, "string"},             // empty cols fallback
		{"User", []string{"missing"}, "string"},     // missing col fallback
		{"Missing", []string{"id"}, "string"},       // missing table fallback
	}
	for _, tt := range tests {
		if got := idxKeyGoTypeOf(tc, tt.table, tt.cols); got != tt.want {
			t.Errorf("idxKeyGoTypeOf(tc, %q, %v) = %q, want %q", tt.table, tt.cols, got, tt.want)
		}
	}
}

// ── idxKeyWriteOf ────────────────────────────────────────────────────────────

func TestIdxKeyWriteOf(t *testing.T) {
	tc := map[string][]Column{
		"User": {
			{Name: "id", Type: "U64"},
			{Name: "sender", Type: "Identity"},
			{Name: "created_at", Type: "Timestamp"},
		},
	}

	tests := []struct {
		table string
		cols  []string
		want  string
	}{
		// Standard type → w.WriteXxx(v)
		{"User", []string{"id"}, "func(w *bsatn.Writer, v uint64) { w.WriteU64(v) }"},
		// Special write type → v.WriteBsatn(w)
		{"User", []string{"sender"}, "func(w *bsatn.Writer, v types.Identity) { v.WriteBsatn(w) }"},
		{"User", []string{"created_at"}, "func(w *bsatn.Writer, v types.Timestamp) { v.WriteBsatn(w) }"},
		// Empty cols fallback
		{"User", []string{}, "func(w *bsatn.Writer, v string) { w.WriteString(v) }"},
		// Missing column fallback
		{"User", []string{"missing"}, "func(w *bsatn.Writer, v string) { w.WriteString(v) }"},
		// Missing table fallback
		{"Missing", []string{"id"}, "func(w *bsatn.Writer, v string) { w.WriteString(v) }"},
	}
	for _, tt := range tests {
		if got := idxKeyWriteOf(tc, tt.table, tt.cols); got != tt.want {
			t.Errorf("idxKeyWriteOf(tc, %q, %v) =\n  %q\nwant\n  %q", tt.table, tt.cols, got, tt.want)
		}
	}
}

// ── pkColIDsOf ───────────────────────────────────────────────────────────────

func TestPkColIDsOf(t *testing.T) {
	cols := []Column{
		{Name: "id", Type: "U64"},
		{Name: "name", Type: "String"},
		{Name: "age", Type: "U32"},
	}

	tests := []struct {
		pks  []string
		want string
	}{
		{[]string{"id"}, "0"},
		{[]string{"name"}, "1"},
		{[]string{"age"}, "2"},
		{[]string{"id", "name"}, "0, 1"},
		{[]string{"missing"}, "0"}, // not found → 0 with stderr warning
	}
	for _, tt := range tests {
		if got := pkColIDsOf("TestTable", tt.pks, cols); got != tt.want {
			t.Errorf("pkColIDsOf(\"TestTable\", %v, cols) = %q, want %q", tt.pks, got, tt.want)
		}
	}
}
