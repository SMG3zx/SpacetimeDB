package main

import "github.com/clockworklabs/spacetimedb-go/types"

// ── Synthetic types ───────────────────────────────────────────────────────────

type Unique0U32U64Str struct {
	Id   uint32
	Age  uint64
	Name string
}

type NoIndexU32U64Str struct {
	Id   uint32
	Age  uint64
	Name string
}

type BtreeEachColumnU32U64Str struct {
	Id   uint32
	Age  uint64
	Name string
}

type Unique0U32U64U64 struct {
	Id uint32
	X  uint64
	Y  uint64
}

type NoIndexU32U64U64 struct {
	Id uint32
	X  uint64
	Y  uint64
}

type BtreeEachColumnU32U64U64 struct {
	Id uint32
	X  uint64
	Y  uint64
}

// ── Circles types ─────────────────────────────────────────────────────────────

type Vector2 struct {
	X float32
	Y float32
}

type Entity struct {
	Id       uint32
	Position Vector2
	Mass     uint32
}

type Circle struct {
	EntityId      uint32
	PlayerId      uint32
	Direction     Vector2
	Magnitude     float32
	LastSplitTime types.Timestamp
}

type Food struct {
	EntityId uint32
}

// ── IA Loop types ─────────────────────────────────────────────────────────────

type AgentAction uint8

const (
	AgentActionInactive      AgentAction = 0
	AgentActionIdle          AgentAction = 1
	AgentActionEvading       AgentAction = 2
	AgentActionInvestigating AgentAction = 3
	AgentActionRetreating    AgentAction = 4
	AgentActionFighting      AgentAction = 5
)

type SmallHexTile struct {
	X         int32
	Z         int32
	Dimension uint32
}

type Velocity struct {
	EntityId uint32
	X        float32
	Y        float32
	Z        float32
}

type Position struct {
	EntityId uint32
	X        float32
	Y        float32
	Z        float32
	Vx       float32
	Vy       float32
	Vz       float32
}

type GameEnemyAiAgentState struct {
	EntityId              uint64
	LastMoveTimestamps    []uint64
	NextActionTimestamp   uint64
	Action                AgentAction
}

type GameTargetableState struct {
	EntityId uint64
	Quad     int64
}

type GameLiveTargetableState struct {
	EntityId uint64
	Quad     int64
}

type GameMobileEntityState struct {
	EntityId  uint64
	LocationX int32
	LocationY int32
	Timestamp uint64
}

type GameEnemyState struct {
	EntityId uint64
	HerdId   int32
}

type GameHerdCache struct {
	Id                int32
	DimensionId       uint32
	CurrentPopulation int32
	Location          SmallHexTile
	MaxPopulation     int32
	SpawnEagerness    float32
	RoamingDistance   int32
}
