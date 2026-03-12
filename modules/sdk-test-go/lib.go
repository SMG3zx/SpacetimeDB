//go:generate go run github.com/clockworklabs/spacetimedb-go-server/cmd/stdbgen

package main

import spacetimedb "github.com/clockworklabs/spacetimedb-go-server"

func sptr(s string) *string { return &s }

// registerPublicTable registers a simple public table.
func registerPublicTable(name string, cols []spacetimedb.ColumnDef) {
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name:    name,
		Columns: cols,
		Access:  spacetimedb.TableAccessPublic,
	})
}

func main() {}
