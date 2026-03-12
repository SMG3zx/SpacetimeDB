//go:generate go run github.com/clockworklabs/spacetimedb-go-server/cmd/stdbgen

package main

import (
	"math"

	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/types"
)

// Game constants
const (
	StartPlayerMass                 int32   = 15
	StartPlayerSpeed                int32   = 10
	FoodMassMin                     int32   = 2
	FoodMassMax                     int32   = 4
	TargetFoodCount                 int     = 600
	MinimumSafeMassRatio            float32 = 0.85
	MinMassToSplit                  int32   = StartPlayerMass * 2
	MaxCirclesPerPlayer             int32   = 16
	SplitRecombineDelaySec          float32 = 5.0
	SplitGravPullBeforeRecombineSec float32 = 2.0
	AllowedSplitCircleOverlapPct    float32 = 0.9
	SelfCollisionSpeed              float32 = 0.05
)

// DbVector2 is a 2D vector for positions and directions.
type DbVector2 struct {
	X float32
	Y float32
}

func encodeDbVector2(w *bsatn.Writer, v DbVector2) {
	w.WriteF32(v.X)
	w.WriteF32(v.Y)
}

func decodeDbVector2(r *bsatn.Reader) (DbVector2, error) {
	x, err := r.ReadF32()
	if err != nil {
		return DbVector2{}, err
	}
	y, err := r.ReadF32()
	if err != nil {
		return DbVector2{}, err
	}
	return DbVector2{X: x, Y: y}, nil
}

func (v DbVector2) Add(o DbVector2) DbVector2    { return DbVector2{v.X + o.X, v.Y + o.Y} }
func (v DbVector2) Sub(o DbVector2) DbVector2    { return DbVector2{v.X - o.X, v.Y - o.Y} }
func (v DbVector2) Mul(s float32) DbVector2      { return DbVector2{v.X * s, v.Y * s} }
func (v DbVector2) SqrMagnitude() float32        { return v.X*v.X + v.Y*v.Y }
func (v DbVector2) Magnitude() float32           { return float32(math.Sqrt(float64(v.SqrMagnitude()))) }

func (v DbVector2) Div(s float32) DbVector2 {
	if s != 0 {
		return DbVector2{v.X / s, v.Y / s}
	}
	return DbVector2{}
}

func (v DbVector2) Normalized() DbVector2 { return v.Div(v.Magnitude()) }

// Row types

type Config struct {
	ID        int32
	WorldSize int64
}

type Entity struct {
	EntityID int32
	Position DbVector2
	Mass     int32
}

type Circle struct {
	EntityID      int32
	PlayerID      int32
	Direction     DbVector2
	Speed         float32
	LastSplitTime uint64
}

type Player struct {
	Identity types.Identity
	PlayerID int32
	Name     string
}

type Food struct {
	EntityID int32
}

type ConsumeEntityEvent struct {
	ConsumedEntityID int32
	ConsumerEntityID int32
}

// Encode/decode functions

func encodeConfig(w *bsatn.Writer, c Config) {
	w.WriteI32(c.ID)
	w.WriteI64(c.WorldSize)
}
func decodeConfig(r *bsatn.Reader) (Config, error) {
	id, err := r.ReadI32()
	if err != nil {
		return Config{}, err
	}
	ws, err := r.ReadI64()
	if err != nil {
		return Config{}, err
	}
	return Config{ID: id, WorldSize: ws}, nil
}

func encodeEntity(w *bsatn.Writer, e Entity) {
	w.WriteI32(e.EntityID)
	encodeDbVector2(w, e.Position)
	w.WriteI32(e.Mass)
}
func decodeEntity(r *bsatn.Reader) (Entity, error) {
	eid, err := r.ReadI32()
	if err != nil {
		return Entity{}, err
	}
	pos, err := decodeDbVector2(r)
	if err != nil {
		return Entity{}, err
	}
	mass, err := r.ReadI32()
	if err != nil {
		return Entity{}, err
	}
	return Entity{EntityID: eid, Position: pos, Mass: mass}, nil
}

func encodeCircle(w *bsatn.Writer, c Circle) {
	w.WriteI32(c.EntityID)
	w.WriteI32(c.PlayerID)
	encodeDbVector2(w, c.Direction)
	w.WriteF32(c.Speed)
	w.WriteU64(c.LastSplitTime)
}
func decodeCircle(r *bsatn.Reader) (Circle, error) {
	eid, err := r.ReadI32()
	if err != nil {
		return Circle{}, err
	}
	pid, err := r.ReadI32()
	if err != nil {
		return Circle{}, err
	}
	dir, err := decodeDbVector2(r)
	if err != nil {
		return Circle{}, err
	}
	spd, err := r.ReadF32()
	if err != nil {
		return Circle{}, err
	}
	lst, err := r.ReadU64()
	if err != nil {
		return Circle{}, err
	}
	return Circle{EntityID: eid, PlayerID: pid, Direction: dir, Speed: spd, LastSplitTime: lst}, nil
}

func encodePlayer(w *bsatn.Writer, p Player) {
	w.WriteBytes(p.Identity[:])
	w.WriteI32(p.PlayerID)
	w.WriteString(p.Name)
}
func decodePlayer(r *bsatn.Reader) (Player, error) {
	idBytes, err := r.ReadBytes(32)
	if err != nil {
		return Player{}, err
	}
	var identity types.Identity
	copy(identity[:], idBytes)
	pid, err := r.ReadI32()
	if err != nil {
		return Player{}, err
	}
	name, err := r.ReadString()
	if err != nil {
		return Player{}, err
	}
	return Player{Identity: identity, PlayerID: pid, Name: name}, nil
}

func encodeFood(w *bsatn.Writer, f Food) { w.WriteI32(f.EntityID) }
func decodeFood(r *bsatn.Reader) (Food, error) {
	eid, err := r.ReadI32()
	if err != nil {
		return Food{}, err
	}
	return Food{EntityID: eid}, nil
}

func encodeConsumeEntityEvent(w *bsatn.Writer, e ConsumeEntityEvent) {
	w.WriteI32(e.ConsumedEntityID)
	w.WriteI32(e.ConsumerEntityID)
}
func decodeConsumeEntityEvent(r *bsatn.Reader) (ConsumeEntityEvent, error) {
	consumed, err := r.ReadI32()
	if err != nil {
		return ConsumeEntityEvent{}, err
	}
	consumer, err := r.ReadI32()
	if err != nil {
		return ConsumeEntityEvent{}, err
	}
	return ConsumeEntityEvent{ConsumedEntityID: consumed, ConsumerEntityID: consumer}, nil
}

// Table handles

var (
	configTable           = spacetimedb.NewTableHandle("Config", encodeConfig, decodeConfig)
	entityTable           = spacetimedb.NewTableHandle("Entity", encodeEntity, decodeEntity)
	loggedOutEntityTable  = spacetimedb.NewTableHandle("LoggedOutEntity", encodeEntity, decodeEntity)
	circleTable           = spacetimedb.NewTableHandle("Circle", encodeCircle, decodeCircle)
	loggedOutCircleTable  = spacetimedb.NewTableHandle("LoggedOutCircle", encodeCircle, decodeCircle)
	playerTable           = spacetimedb.NewTableHandle("Player", encodePlayer, decodePlayer)
	loggedOutPlayerTable  = spacetimedb.NewTableHandle("LoggedOutPlayer", encodePlayer, decodePlayer)
	foodTable             = spacetimedb.NewTableHandle("Food", encodeFood, decodeFood)
	consumeEntityEvtTable = spacetimedb.NewTableHandle("ConsumeEntityEvent", encodeConsumeEntityEvent, decodeConsumeEntityEvent)
)

// Helper functions

func massToRadius(mass int32) float32 {
	return float32(math.Sqrt(float64(mass)))
}

func massToMaxMoveSpeed(mass int32) float32 {
	return 2.0 * float32(StartPlayerSpeed) / (1.0 + float32(math.Sqrt(float64(mass)/float64(StartPlayerMass))))
}

func isOverlapping(a, b Entity) bool {
	dx := a.Position.X - b.Position.X
	dy := a.Position.Y - b.Position.Y
	distSq := dx*dx + dy*dy
	ra := massToRadius(a.Mass)
	rb := massToRadius(b.Mass)
	maxR := ra
	if rb > ra {
		maxR = rb
	}
	return distSq <= maxR*maxR
}

func clampF32(v, lo, hi float32) float32 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func destroyEntity(entityID int32) {
	for f, err := range foodTable.Iter() {
		if err != nil {
			break
		}
		if f.EntityID == entityID {
			foodTable.Delete(f)
			break
		}
	}
	for c, err := range circleTable.Iter() {
		if err != nil {
			break
		}
		if c.EntityID == entityID {
			circleTable.Delete(c)
			break
		}
	}
	for e, err := range entityTable.Iter() {
		if err != nil {
			break
		}
		if e.EntityID == entityID {
			entityTable.Delete(e)
			break
		}
	}
}

func spawnCircleAt(ctx spacetimedb.ReducerContext, playerID int32, mass int32, position DbVector2) {
	entity, err := entityTable.Insert(Entity{EntityID: 0, Position: position, Mass: mass})
	if err != nil {
		spacetimedb.LogError("spawnCircleAt: insert entity failed: " + err.Error())
		return
	}
	circleTable.Insert(Circle{
		EntityID:      entity.EntityID,
		PlayerID:      playerID,
		Direction:     DbVector2{X: 0, Y: 1},
		Speed:         0,
		LastSplitTime: ctx.Timestamp,
	})
}

func spawnPlayerInitialCircle(ctx spacetimedb.ReducerContext, playerID int32) {
	var worldSize int64 = 1000
	for cfg, err := range configTable.Iter() {
		if err != nil {
			break
		}
		if cfg.ID == 0 {
			worldSize = cfg.WorldSize
			break
		}
	}
	r := massToRadius(StartPlayerMass)
	rng := ctx.Rng
	x := r + float32(rng.Float64())*float32(float64(worldSize)-2*float64(r))
	y := r + float32(rng.Float64())*float32(float64(worldSize)-2*float64(r))
	spawnCircleAt(ctx, playerID, StartPlayerMass, DbVector2{X: x, Y: y})
}

// Reducer implementations

func initReducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {
	spacetimedb.LogInfo("Initializing...")
	configTable.Insert(Config{ID: 0, WorldSize: 1000})
}

func connectReducer(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	// Check if player was previously logged out
	for player, err := range loggedOutPlayerTable.Iter() {
		if err != nil {
			break
		}
		if player.Identity == ctx.Sender {
			playerTable.Insert(player)
			loggedOutPlayerTable.Delete(player)
			for circle, err := range loggedOutCircleTable.Iter() {
				if err != nil {
					break
				}
				if circle.PlayerID == player.PlayerID {
					loggedOutCircleTable.Delete(circle)
					circleTable.Insert(circle)
					for entity, err := range loggedOutEntityTable.Iter() {
						if err != nil {
							break
						}
						if entity.EntityID == circle.EntityID {
							loggedOutEntityTable.Delete(entity)
							entityTable.Insert(entity)
							break
						}
					}
				}
			}
			return
		}
	}
	// New player
	playerTable.Insert(Player{Identity: ctx.Sender, PlayerID: 0, Name: ""})
}

func disconnectReducer(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	var foundPlayer *Player
	for player, err := range playerTable.Iter() {
		if err != nil {
			break
		}
		if player.Identity == ctx.Sender {
			foundPlayer = &player
			break
		}
	}
	if foundPlayer == nil {
		return
	}
	loggedOutPlayerTable.Insert(*foundPlayer)
	playerTable.Delete(*foundPlayer)
	for circle, err := range circleTable.Iter() {
		if err != nil {
			break
		}
		if circle.PlayerID == foundPlayer.PlayerID {
			for entity, err := range entityTable.Iter() {
				if err != nil {
					break
				}
				if entity.EntityID == circle.EntityID {
					loggedOutEntityTable.Insert(entity)
					entityTable.Delete(entity)
					break
				}
			}
			loggedOutCircleTable.Insert(circle)
			circleTable.Delete(circle)
		}
	}
}

func enterGameReducer(ctx spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		spacetimedb.LogError("enterGame: " + err.Error())
		return
	}
	rd := bsatn.NewReader(data)
	name, err := rd.ReadString()
	if err != nil {
		spacetimedb.LogError("enterGame: " + err.Error())
		return
	}
	spacetimedb.LogInfo("Creating player with name " + name)
	for player, err := range playerTable.Iter() {
		if err != nil {
			break
		}
		if player.Identity == ctx.Sender {
			player.Name = name
			playerTable.Delete(player)
			playerTable.Insert(player)
			spawnPlayerInitialCircle(ctx, player.PlayerID)
			return
		}
	}
}

func respawnReducer(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	for player, err := range playerTable.Iter() {
		if err != nil {
			break
		}
		if player.Identity == ctx.Sender {
			spawnPlayerInitialCircle(ctx, player.PlayerID)
			return
		}
	}
}

func suicideReducer(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	for player, err := range playerTable.Iter() {
		if err != nil {
			break
		}
		if player.Identity == ctx.Sender {
			for circle, err := range circleTable.Iter() {
				if err != nil {
					break
				}
				if circle.PlayerID == player.PlayerID {
					destroyEntity(circle.EntityID)
				}
			}
			return
		}
	}
}

func updatePlayerInputReducer(ctx spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		return
	}
	rd := bsatn.NewReader(data)
	direction, err := decodeDbVector2(rd)
	if err != nil {
		return
	}
	var playerID int32 = -1
	for player, err := range playerTable.Iter() {
		if err != nil {
			break
		}
		if player.Identity == ctx.Sender {
			playerID = player.PlayerID
			break
		}
	}
	if playerID == -1 {
		return
	}
	for circle, err := range circleTable.Iter() {
		if err != nil {
			break
		}
		if circle.PlayerID == playerID {
			circle.Direction = direction.Normalized()
			circle.Speed = clampF32(direction.Magnitude(), 0, 1)
			circleTable.Delete(circle)
			circleTable.Insert(circle)
		}
	}
}

func moveAllPlayersReducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {
	var worldSize int64 = 1000
	for cfg, err := range configTable.Iter() {
		if err != nil {
			break
		}
		if cfg.ID == 0 {
			worldSize = cfg.WorldSize
			break
		}
	}

	// Move circles based on direction
	for circle, err := range circleTable.Iter() {
		if err != nil {
			break
		}
		var circleEntity *Entity
		for e, err := range entityTable.Iter() {
			if err != nil {
				break
			}
			if e.EntityID == circle.EntityID {
				circleEntity = &e
				break
			}
		}
		if circleEntity == nil {
			continue
		}
		dir := circle.Direction.Mul(circle.Speed)
		newPos := circleEntity.Position.Add(dir.Mul(massToMaxMoveSpeed(circleEntity.Mass)))
		r := massToRadius(circleEntity.Mass)
		circleEntity.Position.X = clampF32(newPos.X, r, float32(worldSize)-r)
		circleEntity.Position.Y = clampF32(newPos.Y, r, float32(worldSize)-r)
		entityTable.Delete(*circleEntity)
		entityTable.Insert(*circleEntity)
	}

	// Check collisions
	var entities []Entity
	for e, err := range entityTable.Iter() {
		if err != nil {
			break
		}
		entities = append(entities, e)
	}
	for circle, err := range circleTable.Iter() {
		if err != nil {
			break
		}
		var circleEntity *Entity
		for i := range entities {
			if entities[i].EntityID == circle.EntityID {
				circleEntity = &entities[i]
				break
			}
		}
		if circleEntity == nil {
			continue
		}
		for _, other := range entities {
			if other.EntityID == circleEntity.EntityID {
				continue
			}
			if !isOverlapping(*circleEntity, other) {
				continue
			}
			var otherCircle *Circle
			for c, err := range circleTable.Iter() {
				if err != nil {
					break
				}
				if c.EntityID == other.EntityID {
					otherCircle = &c
					break
				}
			}
			if otherCircle != nil {
				if otherCircle.PlayerID != circle.PlayerID {
					massRatio := float32(other.Mass) / float32(circleEntity.Mass)
					if massRatio < MinimumSafeMassRatio {
						consumeEntityEvtTable.Insert(ConsumeEntityEvent{
							ConsumedEntityID: other.EntityID,
							ConsumerEntityID: circleEntity.EntityID,
						})
						circleEntity.Mass += other.Mass
						destroyEntity(other.EntityID)
						entityTable.Delete(*circleEntity)
						entityTable.Insert(*circleEntity)
					}
				}
			} else {
				consumeEntityEvtTable.Insert(ConsumeEntityEvent{
					ConsumedEntityID: other.EntityID,
					ConsumerEntityID: circleEntity.EntityID,
				})
				circleEntity.Mass += other.Mass
				destroyEntity(other.EntityID)
				entityTable.Delete(*circleEntity)
				entityTable.Insert(*circleEntity)
			}
		}
	}
}

func spawnFoodReducer(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	playerCount := 0
	for _, err := range playerTable.Iter() {
		if err != nil {
			break
		}
		playerCount++
	}
	if playerCount == 0 {
		return
	}

	var worldSize int64 = 1000
	for cfg, err := range configTable.Iter() {
		if err != nil {
			break
		}
		if cfg.ID == 0 {
			worldSize = cfg.WorldSize
			break
		}
	}

	foodCount := 0
	for _, err := range foodTable.Iter() {
		if err != nil {
			break
		}
		foodCount++
	}

	rng := ctx.Rng
	for foodCount < TargetFoodCount {
		foodMass := FoodMassMin + int32(rng.Int63n(int64(FoodMassMax-FoodMassMin)))
		r := massToRadius(foodMass)
		x := r + float32(rng.Float64())*float32(float64(worldSize)-2*float64(r))
		y := r + float32(rng.Float64())*float32(float64(worldSize)-2*float64(r))
		entity, err := entityTable.Insert(Entity{EntityID: 0, Position: DbVector2{X: x, Y: y}, Mass: foodMass})
		if err != nil {
			break
		}
		foodTable.Insert(Food{EntityID: entity.EntityID})
		foodCount++
	}
}

func circleDecayReducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {
	for circle, err := range circleTable.Iter() {
		if err != nil {
			break
		}
		for entity, err := range entityTable.Iter() {
			if err != nil {
				break
			}
			if entity.EntityID == circle.EntityID && entity.Mass > StartPlayerMass {
				entity.Mass = int32(float32(entity.Mass) * 0.99)
				entityTable.Delete(entity)
				entityTable.Insert(entity)
				break
			}
		}
	}
}

func circleRecombineReducer(ctx spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		return
	}
	rd := bsatn.NewReader(data)
	rd.ReadU64() // scheduled_id
	rd.ReadU64() // scheduled_at
	playerID, err := rd.ReadI32()
	if err != nil {
		return
	}
	var recombiningIDs []int32
	for circle, err := range circleTable.Iter() {
		if err != nil {
			break
		}
		if circle.PlayerID == playerID {
			timeSinceSplit := float32(ctx.Timestamp-circle.LastSplitTime) / 1_000_000.0
			if timeSinceSplit >= SplitRecombineDelaySec {
				recombiningIDs = append(recombiningIDs, circle.EntityID)
			}
		}
	}
	if len(recombiningIDs) <= 1 {
		return
	}
	baseID := recombiningIDs[0]
	for i := 1; i < len(recombiningIDs); i++ {
		consumeEntityEvtTable.Insert(ConsumeEntityEvent{
			ConsumedEntityID: recombiningIDs[i],
			ConsumerEntityID: baseID,
		})
	}
}

func consumeEntityReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		return
	}
	rd := bsatn.NewReader(data)
	rd.ReadU64() // scheduled_id
	rd.ReadU64() // scheduled_at
	consumedID, err := rd.ReadI32()
	if err != nil {
		return
	}
	consumerID, err := rd.ReadI32()
	if err != nil {
		return
	}
	var consumed, consumer *Entity
	for e, err := range entityTable.Iter() {
		if err != nil {
			break
		}
		if e.EntityID == consumedID {
			eCopy := e
			consumed = &eCopy
		}
		if e.EntityID == consumerID {
			eCopy := e
			consumer = &eCopy
		}
	}
	if consumed == nil || consumer == nil {
		return
	}
	consumeEntityEvtTable.Insert(ConsumeEntityEvent{
		ConsumedEntityID: consumed.EntityID,
		ConsumerEntityID: consumer.EntityID,
	})
	consumer.Mass += consumed.Mass
	destroyEntity(consumed.EntityID)
	entityTable.Delete(*consumer)
	entityTable.Insert(*consumer)
}

func playerSplitReducer(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	var playerID int32 = -1
	for player, err := range playerTable.Iter() {
		if err != nil {
			break
		}
		if player.Identity == ctx.Sender {
			playerID = player.PlayerID
			break
		}
	}
	if playerID == -1 {
		return
	}
	var circles []Circle
	for circle, err := range circleTable.Iter() {
		if err != nil {
			break
		}
		if circle.PlayerID == playerID {
			circles = append(circles, circle)
		}
	}
	circleCount := int32(len(circles))
	if circleCount >= MaxCirclesPerPlayer {
		return
	}
	for _, circle := range circles {
		var circleEntity *Entity
		for e, err := range entityTable.Iter() {
			if err != nil {
				break
			}
			if e.EntityID == circle.EntityID {
				circleEntity = &e
				break
			}
		}
		if circleEntity == nil {
			continue
		}
		if circleEntity.Mass >= MinMassToSplit*2 {
			halfMass := circleEntity.Mass / 2
			spawnCircleAt(ctx, circle.PlayerID, halfMass, circleEntity.Position.Add(circle.Direction))
			circleEntity.Mass -= halfMass
			entityTable.Delete(*circleEntity)
			entityTable.Insert(*circleEntity)
			circle.LastSplitTime = ctx.Timestamp
			circleTable.Delete(circle)
			circleTable.Insert(circle)
			circleCount++
			if circleCount >= MaxCirclesPerPlayer {
				break
			}
		}
	}
	spacetimedb.LogWarn("Player split!")
}

// Registration - handler order must match stdb.yaml reducer order

func init() {
	spacetimedb.RegisterReducerHandler(initReducer)
	spacetimedb.RegisterReducerHandler(connectReducer)
	spacetimedb.RegisterReducerHandler(disconnectReducer)
	spacetimedb.RegisterReducerHandler(enterGameReducer)
	spacetimedb.RegisterReducerHandler(respawnReducer)
	spacetimedb.RegisterReducerHandler(suicideReducer)
	spacetimedb.RegisterReducerHandler(updatePlayerInputReducer)
	spacetimedb.RegisterReducerHandler(moveAllPlayersReducer)
	spacetimedb.RegisterReducerHandler(spawnFoodReducer)
	spacetimedb.RegisterReducerHandler(circleDecayReducer)
	spacetimedb.RegisterReducerHandler(circleRecombineReducer)
	spacetimedb.RegisterReducerHandler(consumeEntityReducer)
	spacetimedb.RegisterReducerHandler(playerSplitReducer)
}

func main() {}
