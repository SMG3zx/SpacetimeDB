package main

import (
	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go/types"
)

func init() {
	// ── Tables ────────────────────────────────────────────────────────────

	// Player: public, PK entity_id (auto_inc u64), unique identity (Identity).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "Player",
		Columns: []spacetimedb.ColumnDef{
			{Name: "entity_id", Type: types.AlgebraicU64},
			{Name: "identity", Type: satIdentity},
		},
		PrimaryKey: []uint16{0},
		Indexes: []spacetimedb.IndexDef{
			{AccessorName: sptr("entity_id"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
			{AccessorName: sptr("identity"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{1}},
		},
		Constraints: []spacetimedb.ConstraintDef{
			{Columns: []uint16{1}}, // unique identity
		},
		Sequences: []spacetimedb.SequenceDef{
			{Column: 0, Increment: 1},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// PlayerLevel: public, unique entity_id (u64), BTree level (u64).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "PlayerLevel",
		Columns: []spacetimedb.ColumnDef{
			{Name: "entity_id", Type: types.AlgebraicU64},
			{Name: "level", Type: types.AlgebraicU64},
		},
		Indexes: []spacetimedb.IndexDef{
			{AccessorName: sptr("entity_id"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
			{AccessorName: sptr("level"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{1}},
		},
		Constraints: []spacetimedb.ConstraintDef{
			{Columns: []uint16{0}}, // unique entity_id
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// PlayerLocation: private, unique entity_id (u64), BTree active (bool), x (i32), y (i32).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "PlayerLocation",
		Columns: []spacetimedb.ColumnDef{
			{Name: "entity_id", Type: types.AlgebraicU64},
			{Name: "active", Type: types.AlgebraicBool},
			{Name: "x", Type: types.AlgebraicI32},
			{Name: "y", Type: types.AlgebraicI32},
		},
		Indexes: []spacetimedb.IndexDef{
			{AccessorName: sptr("entity_id"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
			{AccessorName: sptr("active"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{1}},
		},
		Constraints: []spacetimedb.ConstraintDef{
			{Columns: []uint16{0}}, // unique entity_id
		},
		Access: spacetimedb.TableAccessPrivate,
	})

	// ── Reducers ──────────────────────────────────────────────────────────

	// 0: insert_player(identity: Identity, level: u64)
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "insert_player",
		Params: []spacetimedb.ColumnDef{
			{Name: "identity", Type: satIdentity},
			{Name: "level", Type: types.AlgebraicU64},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(insertPlayerReducer)

	// 1: delete_player(identity: Identity)
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "delete_player",
		Params: []spacetimedb.ColumnDef{
			{Name: "identity", Type: satIdentity},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(deletePlayerReducer)

	// 2: move_player(dx: i32, dy: i32)
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "move_player",
		Params: []spacetimedb.ColumnDef{
			{Name: "dx", Type: types.AlgebraicI32},
			{Name: "dy", Type: types.AlgebraicI32},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(movePlayerReducer)

	// ── Views ─────────────────────────────────────────────────────────────
	// Views are ordered; the authenticated and anonymous dispatch tables are separate.

	// 0 (authenticated): my_player() -> Option<Player>
	spacetimedb.RegisterViewDef(spacetimedb.ViewDef{
		Name:        "my_player",
		IsPublic:    true,
		IsAnonymous: false,
		Params:      []spacetimedb.ColumnDef{},
		ReturnType: types.SumType{
			Variants: []types.SumTypeVariant{
				{Name: sptr("none"), Type: types.ProductType{}},
				{Name: sptr("some"), Type: satPlayer},
			},
		},
	})
	spacetimedb.RegisterViewHandler(myPlayerView)

	// 1 (authenticated): my_player_and_level() -> Option<PlayerAndLevel>
	spacetimedb.RegisterViewDef(spacetimedb.ViewDef{
		Name:        "my_player_and_level",
		IsPublic:    true,
		IsAnonymous: false,
		Params:      []spacetimedb.ColumnDef{},
		ReturnType: types.SumType{
			Variants: []types.SumTypeVariant{
				{Name: sptr("none"), Type: types.ProductType{}},
				{Name: sptr("some"), Type: satPlayerAndLevel},
			},
		},
	})
	spacetimedb.RegisterViewHandler(myPlayerAndLevelView)

	// 2 (authenticated): nearby_players() -> Vec<PlayerLocation>
	spacetimedb.RegisterViewDef(spacetimedb.ViewDef{
		Name:        "nearby_players",
		IsPublic:    true,
		IsAnonymous: false,
		Params:      []spacetimedb.ColumnDef{},
		ReturnType:  satPlayerLocation,
	})
	spacetimedb.RegisterViewHandler(nearbyPlayersView)

	// 0 (anonymous): players_at_level_0() -> Vec<Player>
	spacetimedb.RegisterViewDef(spacetimedb.ViewDef{
		Name:        "players_at_level_0",
		IsPublic:    true,
		IsAnonymous: true,
		Params:      []spacetimedb.ColumnDef{},
		ReturnType:  satPlayer,
	})
	spacetimedb.RegisterViewAnonHandler(playersAtLevel0View)
}
