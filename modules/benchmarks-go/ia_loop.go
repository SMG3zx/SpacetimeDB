package main

import (
	"strconv"

	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
)

func momentMilliseconds() uint64 {
	return 1
}

// calculateHash is a deterministic bit-mixing function used to update quad values
// in the enemy AI benchmark loop. The exact result doesn't need to match Rust's
// DefaultHasher — any consistent permutation works for the benchmark.
func calculateHash(v int64) int64 {
	u := uint64(v)
	u ^= u >> 30
	u *= 0xbf58476d1ce4e5b9
	u ^= u >> 27
	u *= 0x94d049bb133111eb
	u ^= u >> 31
	return int64(u)
}

const maxMoveTimestamps = 20

func moveAgent(agent *GameEnemyAiAgentState, currentTimeMs uint64) {
	entityId := agent.EntityId

	// Find + no-op update enemy state (benchmark workload).
	enemy, err := gameEnemyStateEntityIdIdx.Find(entityId)
	if err != nil || enemy == nil {
		spacetimedb.LogPanic("GameEnemyState Entity ID not found")
	}
	_, _ = gameEnemyStateEntityIdIdx.Update(*enemy)

	agent.NextActionTimestamp = currentTimeMs + 2000

	// Track movement history (cap at maxMoveTimestamps).
	agent.LastMoveTimestamps = append(agent.LastMoveTimestamps, currentTimeMs)
	if len(agent.LastMoveTimestamps) > maxMoveTimestamps {
		agent.LastMoveTimestamps = agent.LastMoveTimestamps[1:]
	}

	// Update targetable quad with a hash of the previous value.
	targetable, err := gameTargetableStateEntityIdIdx.Find(entityId)
	if err != nil || targetable == nil {
		spacetimedb.LogPanic("GameTargetableState Entity ID not found")
	}
	newHash := calculateHash(targetable.Quad)
	targetable.Quad = newHash
	_, _ = gameTargetableStateEntityIdIdx.Update(*targetable)

	// If the entity is alive, also update the live targetable state.
	liveTarget, err2 := gameLiveTargetableStateEntityIdIdx.Find(entityId)
	if err2 == nil && liveTarget != nil {
		_, _ = gameLiveTargetableStateEntityIdIdx.Delete(entityId)
		_, _ = gameLiveTargetableStateHandle.Insert(GameLiveTargetableState{
			EntityId: entityId,
			Quad:     newHash,
		})
	}

	// Update mobile entity location.
	mobile, err := gameMobileEntityStateEntityIdIdx.Find(entityId)
	if err != nil || mobile == nil {
		spacetimedb.LogPanic("GameMobileEntityState Entity ID not found")
	}
	newMobile := GameMobileEntityState{
		EntityId:  entityId,
		LocationX: mobile.LocationX + 1,
		LocationY: mobile.LocationY + 1,
		Timestamp: agent.NextActionTimestamp,
	}

	_, _ = gameEnemyAiAgentStateEntityIdIdx.Update(*agent)
	_, _ = gameMobileEntityStateEntityIdIdx.Update(newMobile)
}

func agentLoop(agent GameEnemyAiAgentState, currentTimeMs uint64) {
	entityId := agent.EntityId

	// Load coordinates as benchmark workload (not functionally used).
	gameMobileEntityStateEntityIdIdx.Find(entityId)

	agentEntity, err := gameEnemyStateEntityIdIdx.Find(entityId)
	if err != nil || agentEntity == nil {
		spacetimedb.LogPanic("GameEnemyState Entity ID not found")
	}

	// Load herd as benchmark workload.
	gameHerdCacheIdIdx.Find(agentEntity.HerdId)

	moveAgent(&agent, currentTimeMs)
}

// targetablesNearQuadBuf is a package-level buffer reused by getTargetablesNearQuad
// to avoid per-call heap allocations under TinyGo WASM.
var targetablesNearQuadBuf []GameTargetableState

func getTargetablesNearQuad(entityId, numPlayers uint64) []GameTargetableState {
	targetablesNearQuadBuf = targetablesNearQuadBuf[:0]
	for id := entityId; id < numPlayers; id++ {
		for t, err := range gameLiveTargetableStateQuadIdx.Filter(int64(id)) {
			if err != nil {
				break
			}
			targetable, err2 := gameTargetableStateEntityIdIdx.Find(t.EntityId)
			if err2 != nil || targetable == nil {
				spacetimedb.LogPanic("Identity not found")
			}
			targetablesNearQuadBuf = append(targetablesNearQuadBuf, *targetable)
		}
	}
	return targetablesNearQuadBuf
}

func insertBulkPosition(count uint32) {
	tid, _ := sys.TableIdFromName("position")
	for id := uint32(0); id < count; id++ {
		x := float32(id)
		y := float32(id + 5)
		z := float32(id * 5)
		p := Position{
			EntityId: id,
			X:        x,
			Y:        y,
			Z:        z,
			Vx:       x + 10.0,
			Vy:       y + 20.0,
			Vz:       z + 30.0,
		}
		bulkWriter.Reset()
		encodePosition(bulkWriter, p)
		_, _ = sys.InsertBsatnReuse(tid, bulkWriter.Bytes())
	}
	spacetimedb.LogInfo("INSERT POSITION: " + strconv.FormatUint(uint64(count), 10))
}

func insertBulkPositionReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	count, _ := r.ReadU32()
	insertBulkPosition(count)
}

func insertBulkVelocity(count uint32) {
	tid, _ := sys.TableIdFromName("velocity")
	for id := uint32(0); id < count; id++ {
		v := Velocity{
			EntityId: id,
			X:        float32(id),
			Y:        float32(id + 5),
			Z:        float32(id * 5),
		}
		bulkWriter.Reset()
		encodeVelocity(bulkWriter, v)
		_, _ = sys.InsertBsatnReuse(tid, bulkWriter.Bytes())
	}
	spacetimedb.LogInfo("INSERT VELOCITY: " + strconv.FormatUint(uint64(count), 10))
}

func insertBulkVelocityReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	count, _ := r.ReadU32()
	insertBulkVelocity(count)
}

func updatePositionAll(expected uint32) {
	count := 0
	for pos, err := range positionHandle.Iter() {
		if err != nil {
			break
		}
		pos.X += pos.Vx
		pos.Y += pos.Vy
		pos.Z += pos.Vz
		_, _ = positionEntityIdIdx.Update(pos)
		count++
	}
	spacetimedb.LogInfo("UPDATE POSITION ALL: " + strconv.FormatUint(uint64(expected), 10) + ", processed: " + strconv.Itoa(count))
}

func updatePositionAllReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	expected, _ := r.ReadU32()
	updatePositionAll(expected)
}

func updatePositionWithVelocity(expected uint32) {
	count := 0
	for vel, err := range velocityHandle.Iter() {
		if err != nil {
			break
		}
		pos, err2 := positionEntityIdIdx.Find(vel.EntityId)
		if err2 != nil || pos == nil {
			continue
		}
		pos.X += vel.X
		pos.Y += vel.Y
		pos.Z += vel.Z
		_, _ = positionEntityIdIdx.Update(*pos)
		count++
	}
	spacetimedb.LogInfo("UPDATE POSITION BY VELOCITY: " + strconv.FormatUint(uint64(expected), 10) + ", processed: " + strconv.Itoa(count))
}

func updatePositionWithVelocityReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	expected, _ := r.ReadU32()
	updatePositionWithVelocity(expected)
}

func insertWorld(players uint64) {
	tidAgent, _ := sys.TableIdFromName("game_enemy_ai_agent_state")
	tidLive, _ := sys.TableIdFromName("game_live_targetable_state")
	tidTarget, _ := sys.TableIdFromName("game_targetable_state")
	tidMobile, _ := sys.TableIdFromName("game_mobile_entity_state")
	tidEnemy, _ := sys.TableIdFromName("game_enemy_state")
	tidHerd, _ := sys.TableIdFromName("game_herd_cache")

	for i := uint64(0); i < players; i++ {
		id := i
		var nextActionTimestamp uint64
		if i&2 == 2 {
			nextActionTimestamp = momentMilliseconds() + 2000
		} else {
			nextActionTimestamp = momentMilliseconds()
		}

		bulkWriter.Reset()
		encodeGameEnemyAiAgentState(bulkWriter, GameEnemyAiAgentState{
			EntityId:            id,
			LastMoveTimestamps:  []uint64{id, 0, id * 2},
			NextActionTimestamp: nextActionTimestamp,
			Action:              AgentActionIdle,
		})
		_, _ = sys.InsertBsatnReuse(tidAgent, bulkWriter.Bytes())

		bulkWriter.Reset()
		encodeGameLiveTargetableState(bulkWriter, GameLiveTargetableState{
			EntityId: id,
			Quad:     int64(id),
		})
		_, _ = sys.InsertBsatnReuse(tidLive, bulkWriter.Bytes())

		bulkWriter.Reset()
		encodeGameTargetableState(bulkWriter, GameTargetableState{
			EntityId: id,
			Quad:     int64(id),
		})
		_, _ = sys.InsertBsatnReuse(tidTarget, bulkWriter.Bytes())

		bulkWriter.Reset()
		encodeGameMobileEntityState(bulkWriter, GameMobileEntityState{
			EntityId:  id,
			LocationX: int32(id),
			LocationY: int32(id),
			Timestamp: nextActionTimestamp,
		})
		_, _ = sys.InsertBsatnReuse(tidMobile, bulkWriter.Bytes())

		bulkWriter.Reset()
		encodeGameEnemyState(bulkWriter, GameEnemyState{
			EntityId: id,
			HerdId:   int32(id),
		})
		_, _ = sys.InsertBsatnReuse(tidEnemy, bulkWriter.Bytes())

		bulkWriter.Reset()
		encodeGameHerdCache(bulkWriter, GameHerdCache{
			Id:                int32(id),
			DimensionId:       uint32(id),
			CurrentPopulation: int32(id) * 2,
			MaxPopulation:     int32(id) * 4,
			SpawnEagerness:    float32(id),
			RoamingDistance:   int32(id),
			Location: SmallHexTile{
				X:         int32(id),
				Z:         int32(id),
				Dimension: uint32(id) * 2,
			},
		})
		_, _ = sys.InsertBsatnReuse(tidHerd, bulkWriter.Bytes())
	}
	spacetimedb.LogInfo("INSERT WORLD PLAYERS: " + strconv.FormatUint(players, 10))
}

func insertWorldReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	players, _ := r.ReadU64()
	insertWorld(players)
}

func gameLoopEnemyIa(players uint64) {
	count := 0
	currentTimeMs := momentMilliseconds()

	for agent, err := range gameEnemyAiAgentStateHandle.Iter() {
		if err != nil {
			break
		}
		agentTargetable, err2 := gameTargetableStateEntityIdIdx.Find(agent.EntityId)
		if err2 != nil || agentTargetable == nil {
			spacetimedb.LogPanic("No TargetableState for AgentState entity")
		}

		// Compute nearby targets as benchmark workload (result not used further).
		_ = getTargetablesNearQuad(agentTargetable.EntityId, players)

		agent.Action = AgentActionFighting
		agentLoop(agent, currentTimeMs)
		count++
	}
	spacetimedb.LogInfo("ENEMY IA LOOP PLAYERS: " + strconv.FormatUint(players, 10) + ", processed: " + strconv.Itoa(count))
}

func gameLoopEnemyIaReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	players, _ := r.ReadU64()
	gameLoopEnemyIa(players)
}

func initGameIaLoopReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	initialLoad, _ := r.ReadU32()
	l := newLoad(initialLoad)

	insertBulkPosition(l.biggestTable)
	insertBulkVelocity(l.bigTable)
	updatePositionAll(l.biggestTable)
	updatePositionWithVelocity(l.bigTable)
	insertWorld(uint64(l.numPlayers))
}

func runGameIaLoopReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	initialLoad, _ := r.ReadU32()
	l := newLoad(initialLoad)

	gameLoopEnemyIa(uint64(l.numPlayers))
}
