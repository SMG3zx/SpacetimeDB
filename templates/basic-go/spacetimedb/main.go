//go:generate go run github.com/clockworklabs/spacetimedb-go-server/cmd/stdbgen

// Package main is a SpacetimeDB module compiled to WebAssembly with TinyGo.
// Run `go generate` to regenerate the type-safe bindings from stdb.yaml.
// Build with: tinygo build -target wasm -o module.wasm ./
package main

import (
	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/types"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
)

// Person is a row in the Person table.
type Person struct {
	Name string
}

func encodePerson(w *bsatn.Writer, p Person) {
	w.WriteString(p.Name)
}

func decodePerson(r *bsatn.Reader) (Person, error) {
	name, err := r.ReadString()
	if err != nil {
		return Person{}, err
	}
	return Person{Name: name}, nil
}

var personTable = spacetimedb.NewTableHandle("Person", encodePerson, decodePerson)

func init() {
	// Register the Person table.
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "Person",
		Columns: []spacetimedb.ColumnDef{
			{Name: "name", Type: types.AlgebraicString},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// Register the Add reducer (must be registered in the same order as the handler).
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "Add",
		Params: []spacetimedb.ColumnDef{
			{Name: "name", Type: types.AlgebraicString},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(addReducer)

	// Register the SayHello reducer.
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name:       "SayHello",
		Params:     []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(sayHelloReducer)
}

func addReducer(ctx spacetimedb.ReducerContext, args sys.BytesSource) {
	// ctx.Sender holds the caller's identity; ctx.Timestamp is the call time.
	_ = ctx

	data, err := sys.ReadBytesSource(args)
	if err != nil {
		// panic() causes the host to roll back the transaction and log the message.
		panic("addReducer: failed to read args: " + err.Error())
	}
	r := bsatn.NewReader(data)
	name, err := r.ReadString()
	if err != nil {
		panic("addReducer: failed to decode name: " + err.Error())
	}
	if _, err = personTable.Insert(Person{Name: name}); err != nil {
		panic("addReducer: insert failed: " + err.Error())
	}
}

func sayHelloReducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {
	// Iterate over all rows in the Person table.
	for person, err := range personTable.Iter() {
		if err != nil {
			break
		}
		spacetimedb.LogInfo("Hello, " + person.Name + "!")
	}
	spacetimedb.LogInfo("Hello, World!")
}

// main is required by TinyGo but never called by the SpacetimeDB host.
func main() {}
