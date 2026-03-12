package main

import spacetimedb "github.com/clockworklabs/spacetimedb-go-server"

var myTableHandle = spacetimedb.NewTableHandle("MyTable", encodeMyTable, decodeMyTable)
var scheduledProcTableHandle = spacetimedb.NewTableHandle("ScheduledProcTable", encodeScheduledProcTable, decodeScheduledProcTable)
var procInsertsIntoHandle = spacetimedb.NewTableHandle("ProcInsertsInto", encodeProcInsertsInto, decodeProcInsertsInto)
var pkUuidHandle = spacetimedb.NewTableHandle("PkUuid", encodePkUuid, decodePkUuid)
