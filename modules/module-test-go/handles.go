package main

import (
	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go/bsatn"
)

// ── Table Handles ────────────────────────────────────────────────────────────

var personTable = spacetimedb.NewTableHandle("Person", encodePerson, decodePerson)
var removeTableHandle = spacetimedb.NewTableHandle("RemoveTable", encodeRemoveTable, decodeRemoveTable)
var testATable = spacetimedb.NewTableHandle("TestA", encodeTestA, decodeTestA)
var testDTable = spacetimedb.NewTableHandle("TestD", encodeTestD, decodeTestD)
var testETable = spacetimedb.NewTableHandle("TestE", encodeTestE, decodeTestE)
var testFTable = spacetimedb.NewTableHandle("TestF", encodeTestFoobar, decodeTestFoobar)
var privateTableHandle = spacetimedb.NewTableHandle("PrivateTable", encodePrivateTable, decodePrivateTable)
var pointsTable = spacetimedb.NewTableHandle("Point", encodePoint, decodePoint)
var pkMultiIdentityTable = spacetimedb.NewTableHandle("PkMultiIdentity", encodePkMultiIdentity, decodePkMultiIdentity)
var repeatingTestArgTable = spacetimedb.NewTableHandle("RepeatingTestArg", encodeRepeatingTestArg, decodeRepeatingTestArg)
var hasSpecialStuffTable = spacetimedb.NewTableHandle("HasSpecialStuff", encodeHasSpecialStuff, decodeHasSpecialStuff)
var playerTable = spacetimedb.NewTableHandle("Player", encodePlayer, decodePlayer)
var loggedOutPlayerTable = spacetimedb.NewTableHandle("LoggedOutPlayer", encodePlayer, decodePlayer)

// ── BTree Indexes ────────────────────────────────────────────────────────────

// personAgeIndex is a BTree index on Person.Age (accessor "age").
var personAgeIndex = spacetimedb.NewBTreeIndex[Person, uint8](
	"age",
	func(w *bsatn.Writer, v uint8) { w.WriteU8(v) },
	decodePerson,
)

// testAFooIndex is a BTree index on TestA.X (accessor "foo").
var testAFooIndex = spacetimedb.NewBTreeIndex[TestA, uint32](
	"foo",
	func(w *bsatn.Writer, v uint32) { w.WriteU32(v) },
	decodeTestA,
)

// testENameIndex is a BTree index on TestE.Name (accessor "name").
var testENameIndex = spacetimedb.NewBTreeIndex[TestE, string](
	"name",
	func(w *bsatn.Writer, v string) { w.WriteString(v) },
	decodeTestE,
)

// pointsMultiIndex is a BTree multi-column index on Point.X,Y (accessor "multi_column_index").
var pointsMultiIndex = spacetimedb.NewBTreeIndex[Point, int64](
	"multi_column_index",
	func(w *bsatn.Writer, v int64) { w.WriteI64(v) },
	decodePoint,
)
