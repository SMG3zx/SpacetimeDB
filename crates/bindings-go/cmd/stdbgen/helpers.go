package main

import (
	"fmt"
	"os"
	"strings"
)

// ── Template helpers ──────────────────────────────────────────────────────────

// titleCase capitalizes the first character of s.
func titleCase(s string) string {
	if s == "" {
		return ""
	}
	return strings.ToUpper(s[:1]) + s[1:]
}

// camelCols joins column names into a single TitleCase identifier (e.g. ["sent_at","id"] → "SentAtId").
func camelCols(cols []string) string {
	var b strings.Builder
	for _, c := range cols {
		b.WriteString(camelTitle(c))
	}
	return b.String()
}

// camelTitle converts a snake_case string to TitleCase (e.g. "my_index" → "MyIndex").
func camelTitle(s string) string {
	parts := strings.Split(s, "_")
	var b strings.Builder
	for _, p := range parts {
		b.WriteString(titleCase(p))
	}
	return b.String()
}

// lowerCase lowercases the first character of s (used for Go unexported identifiers).
func lowerCase(s string) string {
	if s == "" {
		return ""
	}
	return strings.ToLower(s[:1]) + s[1:]
}

// tableAccess converts a YAML access string ("public"/"private") to the Go constant expression.
func tableAccess(access string) string {
	if strings.ToLower(access) == "private" {
		return "spacetimedb.TableAccessPrivate"
	}
	return "spacetimedb.TableAccessPublic"
}

// reducerVisibility converts a YAML visibility string to the Go constant expression.
func reducerVisibility(vis string) string {
	if strings.ToLower(vis) == "private" {
		return "spacetimedb.ReducerVisibilityPrivate"
	}
	return "spacetimedb.ReducerVisibilityClientCallable"
}

// encodeExprOf returns the Go statement that writes a column value to w.
func encodeExprOf(t string, colName string) string {
	// field access expression, e.g. "row.Identity"
	field := "row." + camelTitle(colName)
	if strings.HasPrefix(t, "Option<") && strings.HasSuffix(t, ">") {
		inner := t[7 : len(t)-1]
		gt := goTypeOf(inner)
		var innerExpr string
		if sw := specialWriteOf(inner); sw != "" {
			innerExpr = "v" + sw
		} else {
			innerExpr = writeMethodOf(inner) + "(v)"
		}
		return fmt.Sprintf("bsatn.WriteOption(w, %s, func(w *bsatn.Writer, v %s) { %s })", field, gt, innerExpr)
	}
	if sw := specialWriteOf(t); sw != "" {
		return field + sw
	}
	return writeMethodOf(t) + "(" + field + ")"
}

// idxKeyGoTypeOf returns the Go type for the first column of an index.
// tableColumns must be provided as context.
func idxKeyGoTypeOf(tableColumns map[string][]Column, tableName string, indexCols []string) string {
	if len(indexCols) == 0 {
		return "string"
	}
	cols := tableColumns[tableName]
	for _, col := range cols {
		if strings.EqualFold(col.Name, indexCols[0]) {
			return goTypeOf(col.Type)
		}
	}
	return "string"
}

// idxKeyWriteOf returns the encodeCol closure literal for the first column of an index.
func idxKeyWriteOf(tableColumns map[string][]Column, tableName string, indexCols []string) string {
	if len(indexCols) == 0 {
		return "func(w *bsatn.Writer, v string) { w.WriteString(v) }"
	}
	cols := tableColumns[tableName]
	for _, col := range cols {
		if strings.EqualFold(col.Name, indexCols[0]) {
			gt := goTypeOf(col.Type)
			if sw := specialWriteOf(col.Type); sw != "" {
				return fmt.Sprintf("func(w *bsatn.Writer, v %s) { v%s }", gt, sw)
			}
			wm := writeMethodOf(col.Type)
			return fmt.Sprintf("func(w *bsatn.Writer, v %s) { %s(v) }", gt, wm)
		}
	}
	return "func(w *bsatn.Writer, v string) { w.WriteString(v) }"
}

// colTypeOf returns the YAML type string for colName in columns, or "String" if not found.
func colTypeOf(columns []Column, colName string) string {
	for _, c := range columns {
		if strings.EqualFold(c.Name, colName) {
			return c.Type
		}
	}
	return "String"
}

// colIndex returns the 0-based index of colName in the columns slice, or -1.
func colIndex(columns []Column, colName string) int {
	for i, c := range columns {
		if strings.EqualFold(c.Name, colName) {
			return i
		}
	}
	return -1
}

// pkColIDsOf builds the uint16 col-ID list for a primary_key spec.
func pkColIDsOf(tableName string, pks []string, columns []Column) string {
	var ids []string
	for _, pk := range pks {
		idx := colIndex(columns, pk)
		if idx < 0 {
			fmt.Fprintf(os.Stderr, "stdbgen: table %s: primary_key column %q not found\n", tableName, pk)
			idx = 0
		}
		ids = append(ids, fmt.Sprintf("%d", idx))
	}
	return strings.Join(ids, ", ")
}
