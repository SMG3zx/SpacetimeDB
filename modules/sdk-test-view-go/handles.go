package main

import (
	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/types"
)

// ── Table Handles ─────────────────────────────────────────────────────────────

var playerHandle = spacetimedb.NewTableHandle("Player", encodePlayer, decodePlayer)
var playerLevelHandle = spacetimedb.NewTableHandle("PlayerLevel", encodePlayerLevel, decodePlayerLevel)
var playerLocationHandle = spacetimedb.NewTableHandle("PlayerLocation", encodePlayerLocation, decodePlayerLocation)

// ── Unique Indexes ────────────────────────────────────────────────────────────

// playerEntityIdIdx is the unique BTree index on Player.entity_id (the primary key).
var playerEntityIdIdx = spacetimedb.NewUniqueIndex[Player, uint64](
	"Player", "entity_id",
	func(w *bsatn.Writer, v uint64) { w.WriteU64(v) },
	encodePlayer, decodePlayer,
)

// playerIdentityIdx is the unique BTree index on Player.identity.
var playerIdentityIdx = spacetimedb.NewUniqueIndex[Player, types.Identity](
	"Player", "identity",
	encodeIdentity, encodePlayer, decodePlayer,
)

// playerLevelEntityIdIdx is the unique BTree index on PlayerLevel.entity_id.
var playerLevelEntityIdIdx = spacetimedb.NewUniqueIndex[PlayerLevel, uint64](
	"PlayerLevel", "entity_id",
	func(w *bsatn.Writer, v uint64) { w.WriteU64(v) },
	encodePlayerLevel, decodePlayerLevel,
)

// playerLocationEntityIdIdx is the unique BTree index on PlayerLocation.entity_id.
var playerLocationEntityIdIdx = spacetimedb.NewUniqueIndex[PlayerLocation, uint64](
	"PlayerLocation", "entity_id",
	func(w *bsatn.Writer, v uint64) { w.WriteU64(v) },
	encodePlayerLocation, decodePlayerLocation,
)

// ── BTree Indexes ─────────────────────────────────────────────────────────────

// playerLevelLevelIdx is the BTree index on PlayerLevel.level.
var playerLevelLevelIdx = spacetimedb.NewBTreeIndex[PlayerLevel, uint64](
	"level",
	func(w *bsatn.Writer, v uint64) { w.WriteU64(v) },
	decodePlayerLevel,
)

// playerLocationActiveIdx is the BTree index on PlayerLocation.active.
var playerLocationActiveIdx = spacetimedb.NewBTreeIndex[PlayerLocation, bool](
	"active",
	func(w *bsatn.Writer, v bool) { w.WriteBool(v) },
	decodePlayerLocation,
)
