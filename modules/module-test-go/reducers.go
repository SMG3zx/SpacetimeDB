package main

import (
	"fmt"

	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/types"
)

// ── Reducer Implementations ──────────────────────────────────────────────────

func initReducer(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	_, err := repeatingTestArgTable.Insert(RepeatingTestArg{
		ScheduledId: 0,
		ScheduledAt: types.ScheduleAtInterval(types.TimeDuration{Nanoseconds: 1_000_000_000}),
		PrevTime:    ctx.Timestamp,
	})
	if err != nil {
		spacetimedb.LogError("init: insert RepeatingTestArg failed: " + err.Error())
	}
}

func repeatingTestReducer(ctx spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		spacetimedb.LogError("repeating_test: failed to read args: " + err.Error())
		return
	}
	r := bsatn.NewReader(data)
	arg, err := decodeRepeatingTestArg(r)
	if err != nil {
		spacetimedb.LogError("repeating_test: failed to decode arg: " + err.Error())
		return
	}
	deltaUs := ctx.Timestamp.Microseconds - arg.PrevTime.Microseconds
	spacetimedb.LogTrace(fmt.Sprintf("Timestamp: %d, Delta time: %d us", ctx.Timestamp.Microseconds, deltaUs))
}

func addReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		spacetimedb.LogError("add: failed to read args: " + err.Error())
		return
	}
	r := bsatn.NewReader(data)
	name, err := r.ReadString()
	if err != nil {
		spacetimedb.LogError("add: failed to decode name: " + err.Error())
		return
	}
	age, err := r.ReadU8()
	if err != nil {
		spacetimedb.LogError("add: failed to decode age: " + err.Error())
		return
	}
	_, err = personTable.Insert(Person{Id: 0, Name: name, Age: age})
	if err != nil {
		spacetimedb.LogError("add: insert failed: " + err.Error())
	}
}

func sayHelloReducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {
	for person, err := range personTable.Iter() {
		if err != nil {
			break
		}
		spacetimedb.LogInfo("Hello, " + person.Name + "!")
	}
	spacetimedb.LogInfo("Hello, World!")
}

func listOverAgeReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		spacetimedb.LogError("list_over_age: failed to read args: " + err.Error())
		return
	}
	r := bsatn.NewReader(data)
	age, err := r.ReadU8()
	if err != nil {
		spacetimedb.LogError("list_over_age: failed to decode age: " + err.Error())
		return
	}
	for person, err := range personAgeIndex.FilterRange(
		spacetimedb.NewBoundIncluded(age),
		spacetimedb.NewBoundUnbounded[uint8](),
	) {
		if err != nil {
			break
		}
		spacetimedb.LogInfo(fmt.Sprintf("%s has age %d >= %d", person.Name, person.Age, age))
	}
}

func logModuleIdentityReducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {
	modId := types.Identity(sys.Identity())
	spacetimedb.LogInfo("Module identity: " + modId.String())
}

func testReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		spacetimedb.LogError("test: failed to read args: " + err.Error())
		return
	}
	r := bsatn.NewReader(data)

	arg, err := decodeTestA(r)
	if err != nil {
		spacetimedb.LogError("test: failed to decode arg: " + err.Error())
		return
	}
	arg2, err := decodeTestB(r)
	if err != nil {
		spacetimedb.LogError("test: failed to decode arg2: " + err.Error())
		return
	}
	arg3, err := decodeTestC(r)
	if err != nil {
		spacetimedb.LogError("test: failed to decode arg3: " + err.Error())
		return
	}
	arg4, err := decodeTestF(r)
	if err != nil {
		spacetimedb.LogError("test: failed to decode arg4: " + err.Error())
		return
	}

	spacetimedb.LogInfo("BEGIN")
	spacetimedb.LogInfo("bar: " + arg2.Foo)

	switch arg3 {
	case TestCFoo:
		spacetimedb.LogInfo("Foo")
	case TestCBar:
		spacetimedb.LogInfo("Bar")
	}

	switch arg4.Variant {
	case TestFFooV:
		spacetimedb.LogInfo("Foo")
	case TestFBarV:
		spacetimedb.LogInfo("Bar")
	case TestFBazV:
		spacetimedb.LogInfo(arg4.BazVal)
	}

	for i := uint32(0); i < 1000; i++ {
		_, err := testATable.Insert(TestA{X: i + arg.X, Y: i + arg.Y, Z: "Yo"})
		if err != nil {
			spacetimedb.LogError("test: insert TestA failed: " + err.Error())
		}
	}

	rowCountBefore, _ := testATable.Count()
	spacetimedb.LogInfo(fmt.Sprintf("Row count before delete: %d", rowCountBefore))

	numDeleted := uint64(0)
	for x := uint32(5); x < 10; x++ {
		var toDelete []TestA
		for ta, err := range testAFooIndex.Filter(x) {
			if err != nil {
				break
			}
			toDelete = append(toDelete, ta)
		}
		for _, ta := range toDelete {
			deleted, err := testATable.Delete(ta)
			if err == nil && deleted {
				numDeleted++
			}
		}
	}

	rowCountAfter, _ := testATable.Count()
	if rowCountBefore != rowCountAfter+numDeleted {
		spacetimedb.LogError(fmt.Sprintf(
			"Started with %d rows, deleted %d, and wound up with %d rows... huh?",
			rowCountBefore, numDeleted, rowCountAfter,
		))
	}

	_, insertErr := testETable.Insert(TestE{Id: 0, Name: "Tyler"})
	if insertErr != nil {
		spacetimedb.LogInfo("Error: " + insertErr.Error())
	} else {
		spacetimedb.LogInfo("Inserted TestE Tyler")
	}

	spacetimedb.LogInfo(fmt.Sprintf("Row count after delete: %d", rowCountAfter))

	otherRowCount, _ := testATable.Count()
	spacetimedb.LogInfo(fmt.Sprintf("Row count filtered by condition: %d", otherRowCount))

	spacetimedb.LogInfo("MultiColumn")

	for i := int64(0); i < 1000; i++ {
		_, err := pointsTable.Insert(Point{X: i + int64(arg.X), Y: i + int64(arg.Y)})
		if err != nil {
			spacetimedb.LogError("test: insert Point failed: " + err.Error())
		}
	}

	multiRowCount := 0
	for point, err := range pointsTable.Iter() {
		if err != nil {
			break
		}
		if point.X >= 0 && point.Y <= 200 {
			multiRowCount++
		}
	}
	spacetimedb.LogInfo(fmt.Sprintf("Row count filtered by multi-column condition: %d", multiRowCount))

	spacetimedb.LogInfo("END")
}

func addPlayerReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		spacetimedb.LogError("add_player: failed to read args: " + err.Error())
		return
	}
	r := bsatn.NewReader(data)
	name, err := r.ReadString()
	if err != nil {
		spacetimedb.LogError("add_player: failed to decode name: " + err.Error())
		return
	}
	_, err = testETable.Insert(TestE{Id: 0, Name: name})
	if err != nil {
		spacetimedb.LogError("add_player: insert failed: " + err.Error())
	}
}

func deletePlayerReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		spacetimedb.LogError("delete_player: failed to read args: " + err.Error())
		return
	}
	r := bsatn.NewReader(data)
	id, err := r.ReadU64()
	if err != nil {
		spacetimedb.LogError("delete_player: failed to decode id: " + err.Error())
		return
	}
	found := false
	for row, err := range testETable.Iter() {
		if err != nil {
			break
		}
		if row.Id == id {
			_, _ = testETable.Delete(row)
			found = true
			break
		}
	}
	if !found {
		spacetimedb.LogError(fmt.Sprintf("delete_player: No TestE row with id %d", id))
	}
}

func deletePlayersByNameReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		spacetimedb.LogError("delete_players_by_name: failed to read args: " + err.Error())
		return
	}
	r := bsatn.NewReader(data)
	name, err := r.ReadString()
	if err != nil {
		spacetimedb.LogError("delete_players_by_name: failed to decode name: " + err.Error())
		return
	}

	var toDelete []TestE
	for row, err := range testENameIndex.Filter(name) {
		if err != nil {
			break
		}
		toDelete = append(toDelete, row)
	}

	if len(toDelete) == 0 {
		spacetimedb.LogError("delete_players_by_name: No TestE row with name " + name)
		return
	}

	numDeleted := 0
	for _, row := range toDelete {
		_, err := testETable.Delete(row)
		if err == nil {
			numDeleted++
		}
	}
	spacetimedb.LogInfo(fmt.Sprintf("Deleted %d player(s) with name %s", numDeleted, name))
}

func clientConnectedReducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {}

func addPrivateReducer(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	data, err := sys.ReadBytesSource(args)
	if err != nil {
		spacetimedb.LogError("add_private: failed to read args: " + err.Error())
		return
	}
	r := bsatn.NewReader(data)
	name, err := r.ReadString()
	if err != nil {
		spacetimedb.LogError("add_private: failed to decode name: " + err.Error())
		return
	}
	_, err = privateTableHandle.Insert(PrivateTable{Name: name})
	if err != nil {
		spacetimedb.LogError("add_private: insert failed: " + err.Error())
	}
}

func queryPrivateReducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {
	for row, err := range privateTableHandle.Iter() {
		if err != nil {
			break
		}
		spacetimedb.LogInfo("Private, " + row.Name + "!")
	}
	spacetimedb.LogInfo("Private, World!")
}

func testBtreeIndexArgsReducer(_ spacetimedb.ReducerContext, _ sys.BytesSource) {
	// Exercise BTree index filter/range operations to verify they compile and execute.
	testName := "String"

	for _, err := range testENameIndex.Filter(testName) {
		if err != nil {
			break
		}
	}
	for _, err := range testENameIndex.Filter("str") {
		if err != nil {
			break
		}
	}
	for _, err := range testENameIndex.FilterRange(
		spacetimedb.NewBoundIncluded(""),
		spacetimedb.NewBoundUnbounded[string](),
	) {
		if err != nil {
			break
		}
	}

	for _, err := range pointsMultiIndex.Filter(int64(0)) {
		if err != nil {
			break
		}
	}
	for _, err := range pointsMultiIndex.FilterRange(
		spacetimedb.NewBoundIncluded(int64(0)),
		spacetimedb.NewBoundIncluded(int64(3)),
	) {
		if err != nil {
			break
		}
	}
	for _, err := range pointsMultiIndex.FilterRange(
		spacetimedb.NewBoundIncluded(int64(0)),
		spacetimedb.NewBoundUnbounded[int64](),
	) {
		if err != nil {
			break
		}
	}
	for _, err := range pointsMultiIndex.FilterRange(
		spacetimedb.NewBoundUnbounded[int64](),
		spacetimedb.NewBoundIncluded(int64(3)),
	) {
		if err != nil {
			break
		}
	}
}

func assertCallerIdentityIsModuleIdentityReducer(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	caller := ctx.Sender
	owner := types.Identity(sys.Identity())
	if caller != owner {
		spacetimedb.LogError("Caller " + caller.String() + " is not the owner " + owner.String())
	} else {
		spacetimedb.LogInfo("Called by the owner " + owner.String())
	}
}

// ── View Implementations ──────────────────────────────────────────────────────

// myPlayerView returns the Player row for the calling identity, if any.
// Writes zero or one encoded Player rows to the rows sink.
func myPlayerView(sender types.Identity, _ *types.ConnectionId, _ sys.BytesSource, rows sys.BytesSink) {
	for player, err := range playerTable.Iter() {
		if err != nil {
			break
		}
		if player.Identity == sender {
			w := bsatn.NewWriter()
			encodePlayer(w, player)
			_ = sys.WriteBytesToSink(rows, w.Bytes())
			return
		}
	}
}
