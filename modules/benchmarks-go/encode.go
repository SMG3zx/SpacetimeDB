package main

import (
	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/types"
)

// ── Synthetic ─────────────────────────────────────────────────────────────────

func encodeU32U64Str(w *bsatn.Writer, id uint32, age uint64, name string) {
	w.WriteU32(id)
	w.WriteU64(age)
	w.WriteString(name)
}

func decodeU32U64Str(r *bsatn.Reader) (id uint32, age uint64, name string, err error) {
	if id, err = r.ReadU32(); err != nil {
		return
	}
	if age, err = r.ReadU64(); err != nil {
		return
	}
	name, err = r.ReadString()
	return
}

func encodeUnique0U32U64Str(w *bsatn.Writer, v Unique0U32U64Str) {
	encodeU32U64Str(w, v.Id, v.Age, v.Name)
}

func decodeUnique0U32U64Str(r *bsatn.Reader) (Unique0U32U64Str, error) {
	id, age, name, err := decodeU32U64Str(r)
	return Unique0U32U64Str{Id: id, Age: age, Name: name}, err
}

func encodeNoIndexU32U64Str(w *bsatn.Writer, v NoIndexU32U64Str) {
	encodeU32U64Str(w, v.Id, v.Age, v.Name)
}

func decodeNoIndexU32U64Str(r *bsatn.Reader) (NoIndexU32U64Str, error) {
	id, age, name, err := decodeU32U64Str(r)
	return NoIndexU32U64Str{Id: id, Age: age, Name: name}, err
}

func encodeBtreeEachColumnU32U64Str(w *bsatn.Writer, v BtreeEachColumnU32U64Str) {
	encodeU32U64Str(w, v.Id, v.Age, v.Name)
}

func decodeBtreeEachColumnU32U64Str(r *bsatn.Reader) (BtreeEachColumnU32U64Str, error) {
	id, age, name, err := decodeU32U64Str(r)
	return BtreeEachColumnU32U64Str{Id: id, Age: age, Name: name}, err
}

func encodeU32U64U64(w *bsatn.Writer, id uint32, x, y uint64) {
	w.WriteU32(id)
	w.WriteU64(x)
	w.WriteU64(y)
}

func decodeU32U64U64(r *bsatn.Reader) (id uint32, x, y uint64, err error) {
	if id, err = r.ReadU32(); err != nil {
		return
	}
	if x, err = r.ReadU64(); err != nil {
		return
	}
	y, err = r.ReadU64()
	return
}

func encodeUnique0U32U64U64(w *bsatn.Writer, v Unique0U32U64U64) {
	encodeU32U64U64(w, v.Id, v.X, v.Y)
}

func decodeUnique0U32U64U64(r *bsatn.Reader) (Unique0U32U64U64, error) {
	id, x, y, err := decodeU32U64U64(r)
	return Unique0U32U64U64{Id: id, X: x, Y: y}, err
}

func encodeNoIndexU32U64U64(w *bsatn.Writer, v NoIndexU32U64U64) {
	encodeU32U64U64(w, v.Id, v.X, v.Y)
}

func decodeNoIndexU32U64U64(r *bsatn.Reader) (NoIndexU32U64U64, error) {
	id, x, y, err := decodeU32U64U64(r)
	return NoIndexU32U64U64{Id: id, X: x, Y: y}, err
}

func encodeBtreeEachColumnU32U64U64(w *bsatn.Writer, v BtreeEachColumnU32U64U64) {
	encodeU32U64U64(w, v.Id, v.X, v.Y)
}

func decodeBtreeEachColumnU32U64U64(r *bsatn.Reader) (BtreeEachColumnU32U64U64, error) {
	id, x, y, err := decodeU32U64U64(r)
	return BtreeEachColumnU32U64U64{Id: id, X: x, Y: y}, err
}

// ── Circles ───────────────────────────────────────────────────────────────────

func encodeVector2(w *bsatn.Writer, v Vector2) {
	w.WriteF32(v.X)
	w.WriteF32(v.Y)
}

func decodeVector2(r *bsatn.Reader) (Vector2, error) {
	x, err := r.ReadF32()
	if err != nil {
		return Vector2{}, err
	}
	y, err := r.ReadF32()
	return Vector2{X: x, Y: y}, err
}

func encodeEntity(w *bsatn.Writer, v Entity) {
	w.WriteU32(v.Id)
	encodeVector2(w, v.Position)
	w.WriteU32(v.Mass)
}

func decodeEntity(r *bsatn.Reader) (Entity, error) {
	id, err := r.ReadU32()
	if err != nil {
		return Entity{}, err
	}
	pos, err := decodeVector2(r)
	if err != nil {
		return Entity{}, err
	}
	mass, err := r.ReadU32()
	return Entity{Id: id, Position: pos, Mass: mass}, err
}

func encodeCircle(w *bsatn.Writer, v Circle) {
	w.WriteU32(v.EntityId)
	w.WriteU32(v.PlayerId)
	encodeVector2(w, v.Direction)
	w.WriteF32(v.Magnitude)
	w.WriteI64(v.LastSplitTime.Microseconds)
}

func decodeCircle(r *bsatn.Reader) (Circle, error) {
	eid, err := r.ReadU32()
	if err != nil {
		return Circle{}, err
	}
	pid, err := r.ReadU32()
	if err != nil {
		return Circle{}, err
	}
	dir, err := decodeVector2(r)
	if err != nil {
		return Circle{}, err
	}
	mag, err := r.ReadF32()
	if err != nil {
		return Circle{}, err
	}
	us, err := r.ReadI64()
	return Circle{
		EntityId:      eid,
		PlayerId:      pid,
		Direction:     dir,
		Magnitude:     mag,
		LastSplitTime: types.Timestamp{Microseconds: us},
	}, err
}

func encodeFood(w *bsatn.Writer, v Food) {
	w.WriteU32(v.EntityId)
}

func decodeFood(r *bsatn.Reader) (Food, error) {
	eid, err := r.ReadU32()
	return Food{EntityId: eid}, err
}

// ── IA Loop ───────────────────────────────────────────────────────────────────

func encodeAgentAction(w *bsatn.Writer, v AgentAction) {
	w.WriteVariantTag(uint8(v))
}

func decodeAgentAction(r *bsatn.Reader) (AgentAction, error) {
	tag, err := r.ReadVariantTag()
	return AgentAction(tag), err
}

func encodeSmallHexTile(w *bsatn.Writer, v SmallHexTile) {
	w.WriteI32(v.X)
	w.WriteI32(v.Z)
	w.WriteU32(v.Dimension)
}

func decodeSmallHexTile(r *bsatn.Reader) (SmallHexTile, error) {
	x, err := r.ReadI32()
	if err != nil {
		return SmallHexTile{}, err
	}
	z, err := r.ReadI32()
	if err != nil {
		return SmallHexTile{}, err
	}
	dim, err := r.ReadU32()
	return SmallHexTile{X: x, Z: z, Dimension: dim}, err
}

func encodeVelocity(w *bsatn.Writer, v Velocity) {
	w.WriteU32(v.EntityId)
	w.WriteF32(v.X)
	w.WriteF32(v.Y)
	w.WriteF32(v.Z)
}

func decodeVelocity(r *bsatn.Reader) (Velocity, error) {
	eid, err := r.ReadU32()
	if err != nil {
		return Velocity{}, err
	}
	x, err := r.ReadF32()
	if err != nil {
		return Velocity{}, err
	}
	y, err := r.ReadF32()
	if err != nil {
		return Velocity{}, err
	}
	z, err := r.ReadF32()
	return Velocity{EntityId: eid, X: x, Y: y, Z: z}, err
}

func encodePosition(w *bsatn.Writer, v Position) {
	w.WriteU32(v.EntityId)
	w.WriteF32(v.X)
	w.WriteF32(v.Y)
	w.WriteF32(v.Z)
	w.WriteF32(v.Vx)
	w.WriteF32(v.Vy)
	w.WriteF32(v.Vz)
}

func decodePosition(r *bsatn.Reader) (Position, error) {
	eid, err := r.ReadU32()
	if err != nil {
		return Position{}, err
	}
	x, err := r.ReadF32()
	if err != nil {
		return Position{}, err
	}
	y, err := r.ReadF32()
	if err != nil {
		return Position{}, err
	}
	z, err := r.ReadF32()
	if err != nil {
		return Position{}, err
	}
	vx, err := r.ReadF32()
	if err != nil {
		return Position{}, err
	}
	vy, err := r.ReadF32()
	if err != nil {
		return Position{}, err
	}
	vz, err := r.ReadF32()
	return Position{EntityId: eid, X: x, Y: y, Z: z, Vx: vx, Vy: vy, Vz: vz}, err
}

func encodeVecU64(w *bsatn.Writer, v []uint64) {
	w.WriteArrayLen(uint32(len(v)))
	for _, elem := range v {
		w.WriteU64(elem)
	}
}

// decodeVecU64Buf is a package-level buffer reused by decodeVecU64 to avoid
// per-call heap allocations under TinyGo WASM.
var decodeVecU64Buf []uint64

func decodeVecU64(r *bsatn.Reader) ([]uint64, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, err
	}
	if cap(decodeVecU64Buf) < int(n) {
		decodeVecU64Buf = make([]uint64, n)
	}
	decodeVecU64Buf = decodeVecU64Buf[:n]
	for i := range decodeVecU64Buf {
		decodeVecU64Buf[i], err = r.ReadU64()
		if err != nil {
			return nil, err
		}
	}
	return decodeVecU64Buf, nil
}

func encodeGameEnemyAiAgentState(w *bsatn.Writer, v GameEnemyAiAgentState) {
	w.WriteU64(v.EntityId)
	encodeVecU64(w, v.LastMoveTimestamps)
	w.WriteU64(v.NextActionTimestamp)
	encodeAgentAction(w, v.Action)
}

func decodeGameEnemyAiAgentState(r *bsatn.Reader) (GameEnemyAiAgentState, error) {
	eid, err := r.ReadU64()
	if err != nil {
		return GameEnemyAiAgentState{}, err
	}
	lmt, err := decodeVecU64(r)
	if err != nil {
		return GameEnemyAiAgentState{}, err
	}
	nat, err := r.ReadU64()
	if err != nil {
		return GameEnemyAiAgentState{}, err
	}
	action, err := decodeAgentAction(r)
	return GameEnemyAiAgentState{
		EntityId:            eid,
		LastMoveTimestamps:  lmt,
		NextActionTimestamp: nat,
		Action:              action,
	}, err
}

func encodeGameTargetableState(w *bsatn.Writer, v GameTargetableState) {
	w.WriteU64(v.EntityId)
	w.WriteI64(v.Quad)
}

func decodeGameTargetableState(r *bsatn.Reader) (GameTargetableState, error) {
	eid, err := r.ReadU64()
	if err != nil {
		return GameTargetableState{}, err
	}
	quad, err := r.ReadI64()
	return GameTargetableState{EntityId: eid, Quad: quad}, err
}

func encodeGameLiveTargetableState(w *bsatn.Writer, v GameLiveTargetableState) {
	w.WriteU64(v.EntityId)
	w.WriteI64(v.Quad)
}

func decodeGameLiveTargetableState(r *bsatn.Reader) (GameLiveTargetableState, error) {
	eid, err := r.ReadU64()
	if err != nil {
		return GameLiveTargetableState{}, err
	}
	quad, err := r.ReadI64()
	return GameLiveTargetableState{EntityId: eid, Quad: quad}, err
}

func encodeGameMobileEntityState(w *bsatn.Writer, v GameMobileEntityState) {
	w.WriteU64(v.EntityId)
	w.WriteI32(v.LocationX)
	w.WriteI32(v.LocationY)
	w.WriteU64(v.Timestamp)
}

func decodeGameMobileEntityState(r *bsatn.Reader) (GameMobileEntityState, error) {
	eid, err := r.ReadU64()
	if err != nil {
		return GameMobileEntityState{}, err
	}
	lx, err := r.ReadI32()
	if err != nil {
		return GameMobileEntityState{}, err
	}
	ly, err := r.ReadI32()
	if err != nil {
		return GameMobileEntityState{}, err
	}
	ts, err := r.ReadU64()
	return GameMobileEntityState{EntityId: eid, LocationX: lx, LocationY: ly, Timestamp: ts}, err
}

func encodeGameEnemyState(w *bsatn.Writer, v GameEnemyState) {
	w.WriteU64(v.EntityId)
	w.WriteI32(v.HerdId)
}

func decodeGameEnemyState(r *bsatn.Reader) (GameEnemyState, error) {
	eid, err := r.ReadU64()
	if err != nil {
		return GameEnemyState{}, err
	}
	hid, err := r.ReadI32()
	return GameEnemyState{EntityId: eid, HerdId: hid}, err
}

func encodeGameHerdCache(w *bsatn.Writer, v GameHerdCache) {
	w.WriteI32(v.Id)
	w.WriteU32(v.DimensionId)
	w.WriteI32(v.CurrentPopulation)
	encodeSmallHexTile(w, v.Location)
	w.WriteI32(v.MaxPopulation)
	w.WriteF32(v.SpawnEagerness)
	w.WriteI32(v.RoamingDistance)
}

func decodeGameHerdCache(r *bsatn.Reader) (GameHerdCache, error) {
	id, err := r.ReadI32()
	if err != nil {
		return GameHerdCache{}, err
	}
	dimId, err := r.ReadU32()
	if err != nil {
		return GameHerdCache{}, err
	}
	curPop, err := r.ReadI32()
	if err != nil {
		return GameHerdCache{}, err
	}
	loc, err := decodeSmallHexTile(r)
	if err != nil {
		return GameHerdCache{}, err
	}
	maxPop, err := r.ReadI32()
	if err != nil {
		return GameHerdCache{}, err
	}
	spawnEag, err := r.ReadF32()
	if err != nil {
		return GameHerdCache{}, err
	}
	roamDist, err := r.ReadI32()
	return GameHerdCache{
		Id:                id,
		DimensionId:       dimId,
		CurrentPopulation: curPop,
		Location:          loc,
		MaxPopulation:     maxPop,
		SpawnEagerness:    spawnEag,
		RoamingDistance:   roamDist,
	}, err
}
