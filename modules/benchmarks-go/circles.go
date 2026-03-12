package main

import (
	"math"
	"strconv"

	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
)

func massToRadius(mass uint32) float32 {
	return float32(math.Sqrt(float64(mass)))
}

func isOverlapping(e1, e2 Entity) bool {
	r1 := massToRadius(e1.Mass)
	r2 := massToRadius(e2.Mass)
	dx := e1.Position.X - e2.Position.X
	dy := e1.Position.Y - e2.Position.Y
	dist := float32(math.Sqrt(float64(dx*dx + dy*dy)))
	maxR := r1
	if r2 > maxR {
		maxR = r2
	}
	return dist < maxR
}

func insertBulkEntity(count uint32) {
	tid, _ := sys.TableIdFromName("entity")
	for id := uint32(0); id < count; id++ {
		e := Entity{
			Id:       0, // auto_inc
			Position: Vector2{X: float32(id), Y: float32(id + 5)},
			Mass:     id * 5,
		}
		bulkWriter.Reset()
		encodeEntity(bulkWriter, e)
		_, _ = sys.InsertBsatnReuse(tid, bulkWriter.Bytes())
	}
	spacetimedb.LogInfo("INSERT ENTITY: " + strconv.FormatUint(uint64(count), 10))
}

func insertBulkEntityReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	count, _ := r.ReadU32()
	insertBulkEntity(count)
}

func insertBulkCircle(ctx spacetimedb.ReducerContext, count uint32) {
	tid, _ := sys.TableIdFromName("circle")
	for id := uint32(0); id < count; id++ {
		c := Circle{
			EntityId:      id,
			PlayerId:      id,
			Direction:     Vector2{X: float32(id), Y: float32(id + 5)},
			Magnitude:     float32(id * 5),
			LastSplitTime: ctx.Timestamp,
		}
		bulkWriter.Reset()
		encodeCircle(bulkWriter, c)
		_, _ = sys.InsertBsatnReuse(tid, bulkWriter.Bytes())
	}
	spacetimedb.LogInfo("INSERT CIRCLE: " + strconv.FormatUint(uint64(count), 10))
}

func insertBulkCircleReducer(ctx spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	count, _ := r.ReadU32()
	insertBulkCircle(ctx, count)
}

func insertBulkFood(count uint32) {
	tid, _ := sys.TableIdFromName("food")
	for id := uint32(1); id <= count; id++ {
		bulkWriter.Reset()
		encodeFood(bulkWriter, Food{EntityId: id})
		_, _ = sys.InsertBsatnReuse(tid, bulkWriter.Bytes())
	}
	spacetimedb.LogInfo("INSERT FOOD: " + strconv.FormatUint(uint64(count), 10))
}

func insertBulkFoodReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	count, _ := r.ReadU32()
	insertBulkFood(count)
}

func crossJoinAll(expected uint32) {
	count := 0
	for _, err1 := range circleHandle.Iter() {
		if err1 != nil {
			break
		}
		for _, err2 := range entityHandle.Iter() {
			if err2 != nil {
				break
			}
			for _, err3 := range foodHandle.Iter() {
				if err3 != nil {
					break
				}
				count++
			}
		}
	}
	spacetimedb.LogInfo("CROSS JOIN ALL: " + strconv.FormatUint(uint64(expected), 10) + ", processed: " + strconv.Itoa(count))
}

func crossJoinAllReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	expected, _ := r.ReadU32()
	crossJoinAll(expected)
}

func crossJoinCircleFood(expected uint32) {
	count := 0
	for circle, err := range circleHandle.Iter() {
		if err != nil {
			break
		}
		circleEntity, err2 := entityIdIdx.Find(circle.EntityId)
		if err2 != nil || circleEntity == nil {
			continue
		}
		for food, err3 := range foodHandle.Iter() {
			if err3 != nil {
				break
			}
			foodEntity, err4 := entityIdIdx.Find(food.EntityId)
			if err4 != nil || foodEntity == nil {
				spacetimedb.LogPanic("Entity not found: " + strconv.FormatUint(uint64(food.EntityId), 10))
			}
			count++
			_ = isOverlapping(*circleEntity, *foodEntity)
		}
	}
	spacetimedb.LogInfo("CROSS JOIN CIRCLE FOOD: " + strconv.FormatUint(uint64(expected), 10) + ", processed: " + strconv.Itoa(count))
}

func crossJoinCircleFoodReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	expected, _ := r.ReadU32()
	crossJoinCircleFood(expected)
}

func initGameCirclesReducer(ctx spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	initialLoad, _ := r.ReadU32()
	l := newLoad(initialLoad)

	insertBulkFood(l.initialLoad)
	insertBulkEntity(l.initialLoad)
	insertBulkCircle(ctx, l.smallTable)
}

func runGameCirclesReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, _ := sys.ReadBytesSourceReuse(args)
	r := reuseReader(data)
	initialLoad, _ := r.ReadU32()
	l := newLoad(initialLoad)

	crossJoinCircleFood(l.initialLoad * l.smallTable)
	crossJoinAll(l.initialLoad * l.initialLoad * l.smallTable)
}
