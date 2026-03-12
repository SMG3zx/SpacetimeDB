package main

import (
	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
	"github.com/clockworklabs/spacetimedb-go/types"
)

func scheduleProcReducer(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	if _, err := scheduledProcTableHandle.Insert(ScheduledProcTable{
		ScheduledId: 0,
		ScheduledAt: types.ScheduleAtInterval(types.TimeDuration{Nanoseconds: 1_000_000_000}),
		ReducerTs:   ctx.Timestamp,
		X:           42,
		Y:           24,
	}); err != nil {
		spacetimedb.LogError("schedule_proc: insert failed: " + err.Error())
	}
}
