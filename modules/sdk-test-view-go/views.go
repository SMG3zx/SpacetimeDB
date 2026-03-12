package main

import (
	"github.com/clockworklabs/spacetimedb-go-server/sys"
	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/types"
)

// myPlayerView returns the Player row for the calling identity (Option<Player>).
// Writes zero or one encoded Player rows to the rows sink.
func myPlayerView(sender types.Identity, _ *types.ConnectionId, _ sys.BytesSource, rows sys.BytesSink) {
	player, err := playerIdentityIdx.Find(sender)
	if err != nil || player == nil {
		return
	}
	w := bsatn.NewWriter()
	encodePlayer(w, *player)
	_ = sys.WriteBytesToSink(rows, w.Bytes())
}

// myPlayerAndLevelView returns a PlayerAndLevel composite for the calling identity.
// Writes zero or one encoded PlayerAndLevel rows to the rows sink.
func myPlayerAndLevelView(sender types.Identity, _ *types.ConnectionId, _ sys.BytesSource, rows sys.BytesSink) {
	player, err := playerIdentityIdx.Find(sender)
	if err != nil || player == nil {
		return
	}
	level, err := playerLevelEntityIdIdx.Find(player.EntityId)
	if err != nil || level == nil {
		return
	}
	w := bsatn.NewWriter()
	encodePlayerAndLevel(w, PlayerAndLevel{
		EntityId: player.EntityId,
		Identity: player.Identity,
		Level:    level.Level,
	})
	_ = sys.WriteBytesToSink(rows, w.Bytes())
}

// nearbyPlayersView returns all active PlayerLocation rows within 5 units of the caller.
// Writes zero or more encoded PlayerLocation rows to the rows sink.
func nearbyPlayersView(sender types.Identity, _ *types.ConnectionId, _ sys.BytesSource, rows sys.BytesSink) {
	myPlayer, err := playerIdentityIdx.Find(sender)
	if err != nil || myPlayer == nil {
		return
	}
	myLoc, err := playerLocationEntityIdIdx.Find(myPlayer.EntityId)
	if err != nil || myLoc == nil {
		return
	}
	for loc, err := range playerLocationActiveIdx.Filter(true) {
		if err != nil {
			return
		}
		if loc.EntityId == myLoc.EntityId {
			continue
		}
		dx := loc.X - myLoc.X
		if dx < 0 {
			dx = -dx
		}
		dy := loc.Y - myLoc.Y
		if dy < 0 {
			dy = -dy
		}
		if dx < 5 && dy < 5 {
			w := bsatn.NewWriter()
			encodePlayerLocation(w, loc)
			_ = sys.WriteBytesToSink(rows, w.Bytes())
		}
	}
}

// playersAtLevel0View returns all Player rows whose level is 0 (anonymous view).
// Writes zero or more encoded Player rows to the rows sink.
func playersAtLevel0View(_ sys.BytesSource, rows sys.BytesSink) {
	for lvl, err := range playerLevelLevelIdx.Filter(uint64(0)) {
		if err != nil {
			return
		}
		player, err := playerEntityIdIdx.Find(lvl.EntityId)
		if err != nil || player == nil {
			continue
		}
		w := bsatn.NewWriter()
		encodePlayer(w, *player)
		_ = sys.WriteBytesToSink(rows, w.Bytes())
	}
}
