package main

import (
	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
)

// mustReadArgs reads all bytes from args and returns a BSATN reader.
// Panics (triggering transaction rollback) on read failure.
func mustReadArgs(name string, args sys.BytesSource) *bsatn.Reader {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		spacetimedb.LogPanic(name + ": read args: " + err.Error())
	}
	return bsatn.NewReader(data)
}

// regR registers a client-callable reducer def + handler pair.
func regR(name string, params []spacetimedb.ColumnDef, fn spacetimedb.ReducerHandler) {
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name:       name,
		Params:     params,
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(fn)
}

// regPrivR registers a private reducer def + handler pair.
func regPrivR(name string, params []spacetimedb.ColumnDef, fn spacetimedb.ReducerHandler) {
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name:       name,
		Params:     params,
		Visibility: spacetimedb.ReducerVisibilityPrivate,
	})
	spacetimedb.RegisterReducerHandler(fn)
}
