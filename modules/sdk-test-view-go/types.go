package main

import "github.com/clockworklabs/spacetimedb-go/types"

// Player is a registered player with a unique identity.
type Player struct {
	EntityId uint64
	Identity types.Identity
}

// PlayerLevel stores the level for each player.
type PlayerLevel struct {
	EntityId uint64
	Level    uint64
}

// PlayerLocation stores the position for each player.
type PlayerLocation struct {
	EntityId uint64
	Active   bool
	X        int32
	Y        int32
}

// PlayerAndLevel is a view-only composite type returned by my_player_and_level.
type PlayerAndLevel struct {
	EntityId uint64
	Identity types.Identity
	Level    uint64
}
