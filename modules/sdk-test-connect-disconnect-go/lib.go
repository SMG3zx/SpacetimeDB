package main

import (
	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/types"
)

func sptr(s string) *string { return &s }

var satIdentity = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("__identity__"), Type: types.AlgebraicU256},
	},
}

type Connected struct {
	Identity types.Identity
}

type Disconnected struct {
	Identity types.Identity
}

func encodeConnected(w *bsatn.Writer, v Connected) { v.Identity.WriteBsatn(w) }
func decodeConnected(r *bsatn.Reader) (Connected, error) {
	id, err := types.ReadIdentity(r)
	if err != nil {
		return Connected{}, err
	}
	return Connected{Identity: id}, nil
}

func encodeDisconnected(w *bsatn.Writer, v Disconnected) { v.Identity.WriteBsatn(w) }
func decodeDisconnected(r *bsatn.Reader) (Disconnected, error) {
	id, err := types.ReadIdentity(r)
	if err != nil {
		return Disconnected{}, err
	}
	return Disconnected{Identity: id}, nil
}

var connectedTable = spacetimedb.NewTableHandle("Connected", encodeConnected, decodeConnected)
var disconnectedTable = spacetimedb.NewTableHandle("Disconnected", encodeDisconnected, decodeDisconnected)

func init() {
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name:    "Connected",
		Columns: []spacetimedb.ColumnDef{{Name: "identity", Type: satIdentity}},
		Access:  spacetimedb.TableAccessPublic,
	})

	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name:    "Disconnected",
		Columns: []spacetimedb.ColumnDef{{Name: "identity", Type: satIdentity}},
		Access:  spacetimedb.TableAccessPublic,
	})

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name:       "identity_connected",
		Params:     []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityPrivate,
	})
	spacetimedb.RegisterLifecycleDef(spacetimedb.LifecycleDef{
		Kind:    spacetimedb.LifecycleOnConnect,
		Reducer: "identity_connected",
	})
	spacetimedb.RegisterReducerHandler(identityConnected)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name:       "identity_disconnected",
		Params:     []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityPrivate,
	})
	spacetimedb.RegisterLifecycleDef(spacetimedb.LifecycleDef{
		Kind:    spacetimedb.LifecycleOnDisconnect,
		Reducer: "identity_disconnected",
	})
	spacetimedb.RegisterReducerHandler(identityDisconnected)
}

func identityConnected(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	if _, err := connectedTable.Insert(Connected{Identity: ctx.Sender}); err != nil {
		spacetimedb.LogError("identity_connected: insert failed: " + err.Error())
	}
}

func identityDisconnected(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	if _, err := disconnectedTable.Insert(Disconnected{Identity: ctx.Sender}); err != nil {
		spacetimedb.LogError("identity_disconnected: insert failed: " + err.Error())
	}
}

func main() {}
