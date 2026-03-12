package main

import (
	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
	"github.com/clockworklabs/spacetimedb-go/bsatn"
)

func insertPlayerReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		spacetimedb.LogPanic("insert_player: read args: " + err.Error())
	}
	r := bsatn.NewReader(data)
	identity, err := decodeIdentity(r)
	if err != nil {
		spacetimedb.LogPanic("insert_player: decode identity: " + err.Error())
	}
	level, err := r.ReadU64()
	if err != nil {
		spacetimedb.LogPanic("insert_player: decode level: " + err.Error())
	}
	player, err := playerHandle.Insert(Player{EntityId: 0, Identity: identity})
	if err != nil {
		spacetimedb.LogPanic("insert_player: insert Player: " + err.Error())
	}
	if _, err := playerLevelHandle.Insert(PlayerLevel{EntityId: player.EntityId, Level: level}); err != nil {
		spacetimedb.LogPanic("insert_player: insert PlayerLevel: " + err.Error())
	}
}

func deletePlayerReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		spacetimedb.LogPanic("delete_player: read args: " + err.Error())
	}
	r := bsatn.NewReader(data)
	identity, err := decodeIdentity(r)
	if err != nil {
		spacetimedb.LogPanic("delete_player: decode identity: " + err.Error())
	}
	player, err := playerIdentityIdx.Find(identity)
	if err != nil {
		spacetimedb.LogPanic("delete_player: find Player: " + err.Error())
	}
	if player == nil {
		return
	}
	if _, err := playerEntityIdIdx.Delete(player.EntityId); err != nil {
		spacetimedb.LogPanic("delete_player: delete Player: " + err.Error())
	}
	if _, err := playerLevelEntityIdIdx.Delete(player.EntityId); err != nil {
		spacetimedb.LogPanic("delete_player: delete PlayerLevel: " + err.Error())
	}
}

func movePlayerReducer(ctx spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		spacetimedb.LogPanic("move_player: read args: " + err.Error())
	}
	r := bsatn.NewReader(data)
	dx, err := r.ReadI32()
	if err != nil {
		spacetimedb.LogPanic("move_player: decode dx: " + err.Error())
	}
	dy, err := r.ReadI32()
	if err != nil {
		spacetimedb.LogPanic("move_player: decode dy: " + err.Error())
	}

	// Find or create the player.
	myPlayer, err := playerIdentityIdx.Find(ctx.Sender)
	if err != nil {
		spacetimedb.LogPanic("move_player: find Player: " + err.Error())
	}
	if myPlayer == nil {
		p, err := playerHandle.Insert(Player{EntityId: 0, Identity: ctx.Sender})
		if err != nil {
			spacetimedb.LogPanic("move_player: insert Player: " + err.Error())
		}
		myPlayer = &p
	}

	// Find or update the player's location.
	loc, err := playerLocationEntityIdIdx.Find(myPlayer.EntityId)
	if err != nil {
		spacetimedb.LogPanic("move_player: find PlayerLocation: " + err.Error())
	}
	if loc != nil {
		newLoc := PlayerLocation{
			EntityId: loc.EntityId,
			Active:   loc.Active,
			X:        loc.X + dx,
			Y:        loc.Y + dy,
		}
		if _, err := playerLocationEntityIdIdx.Update(newLoc); err != nil {
			spacetimedb.LogPanic("move_player: update PlayerLocation: " + err.Error())
		}
	} else {
		if _, err := playerLocationHandle.Insert(PlayerLocation{
			EntityId: myPlayer.EntityId,
			Active:   true,
			X:        dx,
			Y:        dy,
		}); err != nil {
			spacetimedb.LogPanic("move_player: insert PlayerLocation: " + err.Error())
		}
	}
}
