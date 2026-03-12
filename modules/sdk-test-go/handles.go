package main

import (
	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/types"
)

// ── One* table handles ───────────────────────────────────────────────────────

var oneU8Table = spacetimedb.NewTableHandle("OneU8", encodeOneU8, decodeOneU8)
var oneU16Table = spacetimedb.NewTableHandle("OneU16", encodeOneU16, decodeOneU16)
var oneU32Table = spacetimedb.NewTableHandle("OneU32", encodeOneU32, decodeOneU32)
var oneU64Table = spacetimedb.NewTableHandle("OneU64", encodeOneU64, decodeOneU64)
var oneU128Table = spacetimedb.NewTableHandle("OneU128", encodeOneU128, decodeOneU128)
var oneU256Table = spacetimedb.NewTableHandle("OneU256", encodeOneU256, decodeOneU256)
var oneI8Table = spacetimedb.NewTableHandle("OneI8", encodeOneI8, decodeOneI8)
var oneI16Table = spacetimedb.NewTableHandle("OneI16", encodeOneI16, decodeOneI16)
var oneI32Table = spacetimedb.NewTableHandle("OneI32", encodeOneI32, decodeOneI32)
var oneI64Table = spacetimedb.NewTableHandle("OneI64", encodeOneI64, decodeOneI64)
var oneI128Table = spacetimedb.NewTableHandle("OneI128", encodeOneI128, decodeOneI128)
var oneI256Table = spacetimedb.NewTableHandle("OneI256", encodeOneI256, decodeOneI256)
var oneBoolTable = spacetimedb.NewTableHandle("OneBool", encodeOneBool, decodeOneBool)
var oneF32Table = spacetimedb.NewTableHandle("OneF32", encodeOneF32, decodeOneF32)
var oneF64Table = spacetimedb.NewTableHandle("OneF64", encodeOneF64, decodeOneF64)
var oneStringTable = spacetimedb.NewTableHandle("OneString", encodeOneString, decodeOneString)
var oneIdentityTable = spacetimedb.NewTableHandle("OneIdentity", encodeOneIdentity, decodeOneIdentity)
var oneConnectionIdTable = spacetimedb.NewTableHandle("OneConnectionId", encodeOneConnectionId, decodeOneConnectionId)
var oneUuidTable = spacetimedb.NewTableHandle("OneUuid", encodeOneUuid, decodeOneUuid)
var oneTimestampTable = spacetimedb.NewTableHandle("OneTimestamp", encodeOneTimestamp, decodeOneTimestamp)
var oneSimpleEnumTable = spacetimedb.NewTableHandle("OneSimpleEnum", encodeOneSimpleEnum, decodeOneSimpleEnum)
var oneEnumWithPayloadTable = spacetimedb.NewTableHandle("OneEnumWithPayload", encodeOneEnumWithPayload, decodeOneEnumWithPayload)
var oneUnitStructTable = spacetimedb.NewTableHandle("OneUnitStruct", encodeOneUnitStruct, decodeOneUnitStruct)
var oneByteStructTable = spacetimedb.NewTableHandle("OneByteStruct", encodeOneByteStruct, decodeOneByteStruct)
var oneEveryPrimitiveStructTable = spacetimedb.NewTableHandle("OneEveryPrimitiveStruct", encodeOneEveryPrimitiveStruct, decodeOneEveryPrimitiveStruct)
var oneEveryVecStructTable = spacetimedb.NewTableHandle("OneEveryVecStruct", encodeOneEveryVecStruct, decodeOneEveryVecStruct)

// ── Vec* table handles ───────────────────────────────────────────────────────

var vecU8Table = spacetimedb.NewTableHandle("VecU8", encodeVecU8, decodeVecU8)
var vecU16Table = spacetimedb.NewTableHandle("VecU16", encodeVecU16, decodeVecU16)
var vecU32Table = spacetimedb.NewTableHandle("VecU32", encodeVecU32, decodeVecU32)
var vecU64Table = spacetimedb.NewTableHandle("VecU64", encodeVecU64, decodeVecU64)
var vecU128Table = spacetimedb.NewTableHandle("VecU128", encodeVecU128, decodeVecU128)
var vecU256Table = spacetimedb.NewTableHandle("VecU256", encodeVecU256, decodeVecU256)
var vecI8Table = spacetimedb.NewTableHandle("VecI8", encodeVecI8, decodeVecI8)
var vecI16Table = spacetimedb.NewTableHandle("VecI16", encodeVecI16, decodeVecI16)
var vecI32Table = spacetimedb.NewTableHandle("VecI32", encodeVecI32, decodeVecI32)
var vecI64Table = spacetimedb.NewTableHandle("VecI64", encodeVecI64, decodeVecI64)
var vecI128Table = spacetimedb.NewTableHandle("VecI128", encodeVecI128, decodeVecI128)
var vecI256Table = spacetimedb.NewTableHandle("VecI256", encodeVecI256, decodeVecI256)
var vecBoolTable = spacetimedb.NewTableHandle("VecBool", encodeVecBool, decodeVecBool)
var vecF32Table = spacetimedb.NewTableHandle("VecF32", encodeVecF32, decodeVecF32)
var vecF64Table = spacetimedb.NewTableHandle("VecF64", encodeVecF64, decodeVecF64)
var vecStringTable = spacetimedb.NewTableHandle("VecString", encodeVecString, decodeVecString)
var vecIdentityTable = spacetimedb.NewTableHandle("VecIdentity", encodeVecIdentity, decodeVecIdentity)
var vecConnectionIdTable = spacetimedb.NewTableHandle("VecConnectionId", encodeVecConnectionId, decodeVecConnectionId)
var vecUuidTable = spacetimedb.NewTableHandle("VecUuid", encodeVecUuid, decodeVecUuid)
var vecTimestampTable = spacetimedb.NewTableHandle("VecTimestamp", encodeVecTimestamp, decodeVecTimestamp)
var vecSimpleEnumTable = spacetimedb.NewTableHandle("VecSimpleEnum", encodeVecSimpleEnum, decodeVecSimpleEnum)
var vecEnumWithPayloadTable = spacetimedb.NewTableHandle("VecEnumWithPayload", encodeVecEnumWithPayload, decodeVecEnumWithPayload)
var vecUnitStructTable = spacetimedb.NewTableHandle("VecUnitStruct", encodeVecUnitStruct, decodeVecUnitStruct)
var vecByteStructTable = spacetimedb.NewTableHandle("VecByteStruct", encodeVecByteStruct, decodeVecByteStruct)
var vecEveryPrimitiveStructTable = spacetimedb.NewTableHandle("VecEveryPrimitiveStruct", encodeVecEveryPrimitiveStruct, decodeVecEveryPrimitiveStruct)
var vecEveryVecStructTable = spacetimedb.NewTableHandle("VecEveryVecStruct", encodeVecEveryVecStruct, decodeVecEveryVecStruct)

// ── Option* table handles ────────────────────────────────────────────────────

var optionI32Table = spacetimedb.NewTableHandle("OptionI32", encodeOptionI32Row, decodeOptionI32Row)
var optionStringTable = spacetimedb.NewTableHandle("OptionString", encodeOptionStringRow, decodeOptionStringRow)
var optionIdentityTable = spacetimedb.NewTableHandle("OptionIdentity", encodeOptionIdentityRow, decodeOptionIdentityRow)
var optionUuidTable = spacetimedb.NewTableHandle("OptionUuid", encodeOptionUuidRow, decodeOptionUuidRow)
var optionSimpleEnumTable = spacetimedb.NewTableHandle("OptionSimpleEnum", encodeOptionSimpleEnumRow, decodeOptionSimpleEnumRow)
var optionEveryPrimitiveStructTable = spacetimedb.NewTableHandle("OptionEveryPrimitiveStruct", encodeOptionEveryPrimitiveStructRow, decodeOptionEveryPrimitiveStructRow)
var optionVecOptionI32Table = spacetimedb.NewTableHandle("OptionVecOptionI32", encodeOptionVecOptionI32Row, decodeOptionVecOptionI32Row)

// ── Result* table handles ────────────────────────────────────────────────────

var resultI32StringTable = spacetimedb.NewTableHandle("ResultI32String", encodeResultI32StringRow, decodeResultI32StringRow)
var resultStringI32Table = spacetimedb.NewTableHandle("ResultStringI32", encodeResultStringI32Row, decodeResultStringI32Row)
var resultIdentityStringTable = spacetimedb.NewTableHandle("ResultIdentityString", encodeResultIdentityStringRow, decodeResultIdentityStringRow)
var resultSimpleEnumI32Table = spacetimedb.NewTableHandle("ResultSimpleEnumI32", encodeResultSimpleEnumI32Row, decodeResultSimpleEnumI32Row)
var resultEveryPrimitiveStructStringTable = spacetimedb.NewTableHandle("ResultEveryPrimitiveStructString", encodeResultEveryPrimitiveStructStringRow, decodeResultEveryPrimitiveStructStringRow)
var resultVecI32StringTable = spacetimedb.NewTableHandle("ResultVecI32String", encodeResultVecI32StringRow, decodeResultVecI32StringRow)

// ── Unique* table handles and indexes ────────────────────────────────────────

var uniqueU8Table = spacetimedb.NewTableHandle("UniqueU8", encodeUniqueU8, decodeUniqueU8)
var uniqueU16Table = spacetimedb.NewTableHandle("UniqueU16", encodeUniqueU16, decodeUniqueU16)
var uniqueU32Table = spacetimedb.NewTableHandle("UniqueU32", encodeUniqueU32, decodeUniqueU32)
var uniqueU64Table = spacetimedb.NewTableHandle("UniqueU64", encodeUniqueU64, decodeUniqueU64)
var uniqueU128Table = spacetimedb.NewTableHandle("UniqueU128", encodeUniqueU128, decodeUniqueU128)
var uniqueU256Table = spacetimedb.NewTableHandle("UniqueU256", encodeUniqueU256, decodeUniqueU256)
var uniqueI8Table = spacetimedb.NewTableHandle("UniqueI8", encodeUniqueI8, decodeUniqueI8)
var uniqueI16Table = spacetimedb.NewTableHandle("UniqueI16", encodeUniqueI16, decodeUniqueI16)
var uniqueI32Table = spacetimedb.NewTableHandle("UniqueI32", encodeUniqueI32, decodeUniqueI32)
var uniqueI64Table = spacetimedb.NewTableHandle("UniqueI64", encodeUniqueI64, decodeUniqueI64)
var uniqueI128Table = spacetimedb.NewTableHandle("UniqueI128", encodeUniqueI128, decodeUniqueI128)
var uniqueI256Table = spacetimedb.NewTableHandle("UniqueI256", encodeUniqueI256, decodeUniqueI256)
var uniqueBoolTable = spacetimedb.NewTableHandle("UniqueBool", encodeUniqueBool, decodeUniqueBool)
var uniqueStringTable = spacetimedb.NewTableHandle("UniqueString", encodeUniqueString, decodeUniqueString)
var uniqueIdentityTable = spacetimedb.NewTableHandle("UniqueIdentity", encodeUniqueIdentity, decodeUniqueIdentity)
var uniqueConnectionIdTable = spacetimedb.NewTableHandle("UniqueConnectionId", encodeUniqueConnectionId, decodeUniqueConnectionId)
var uniqueUuidTable = spacetimedb.NewTableHandle("UniqueUuid", encodeUniqueUuid, decodeUniqueUuid)

// UniqueIndex on each Unique* table's unique column for delete/update operations
var uniqueU8NIdx = spacetimedb.NewUniqueIndex[UniqueU8, uint8]("UniqueU8", "n",
	func(w *bsatn.Writer, v uint8) { w.WriteU8(v) }, encodeUniqueU8, decodeUniqueU8)
var uniqueU16NIdx = spacetimedb.NewUniqueIndex[UniqueU16, uint16]("UniqueU16", "n",
	func(w *bsatn.Writer, v uint16) { w.WriteU16(v) }, encodeUniqueU16, decodeUniqueU16)
var uniqueU32NIdx = spacetimedb.NewUniqueIndex[UniqueU32, uint32]("UniqueU32", "n",
	func(w *bsatn.Writer, v uint32) { w.WriteU32(v) }, encodeUniqueU32, decodeUniqueU32)
var uniqueU64NIdx = spacetimedb.NewUniqueIndex[UniqueU64, uint64]("UniqueU64", "n",
	func(w *bsatn.Writer, v uint64) { w.WriteU64(v) }, encodeUniqueU64, decodeUniqueU64)
var uniqueU128NIdx = spacetimedb.NewUniqueIndex[UniqueU128, types.U128]("UniqueU128", "n",
	func(w *bsatn.Writer, v types.U128) { v.WriteBsatn(w) }, encodeUniqueU128, decodeUniqueU128)
var uniqueU256NIdx = spacetimedb.NewUniqueIndex[UniqueU256, types.U256]("UniqueU256", "n",
	func(w *bsatn.Writer, v types.U256) { v.WriteBsatn(w) }, encodeUniqueU256, decodeUniqueU256)
var uniqueI8NIdx = spacetimedb.NewUniqueIndex[UniqueI8, int8]("UniqueI8", "n",
	func(w *bsatn.Writer, v int8) { w.WriteI8(v) }, encodeUniqueI8, decodeUniqueI8)
var uniqueI16NIdx = spacetimedb.NewUniqueIndex[UniqueI16, int16]("UniqueI16", "n",
	func(w *bsatn.Writer, v int16) { w.WriteI16(v) }, encodeUniqueI16, decodeUniqueI16)
var uniqueI32NIdx = spacetimedb.NewUniqueIndex[UniqueI32, int32]("UniqueI32", "n",
	func(w *bsatn.Writer, v int32) { w.WriteI32(v) }, encodeUniqueI32, decodeUniqueI32)
var uniqueI64NIdx = spacetimedb.NewUniqueIndex[UniqueI64, int64]("UniqueI64", "n",
	func(w *bsatn.Writer, v int64) { w.WriteI64(v) }, encodeUniqueI64, decodeUniqueI64)
var uniqueI128NIdx = spacetimedb.NewUniqueIndex[UniqueI128, types.I128]("UniqueI128", "n",
	func(w *bsatn.Writer, v types.I128) { v.WriteBsatn(w) }, encodeUniqueI128, decodeUniqueI128)
var uniqueI256NIdx = spacetimedb.NewUniqueIndex[UniqueI256, types.I256]("UniqueI256", "n",
	func(w *bsatn.Writer, v types.I256) { v.WriteBsatn(w) }, encodeUniqueI256, decodeUniqueI256)
var uniqueBoolBIdx = spacetimedb.NewUniqueIndex[UniqueBool, bool]("UniqueBool", "b",
	func(w *bsatn.Writer, v bool) { w.WriteBool(v) }, encodeUniqueBool, decodeUniqueBool)
var uniqueStringSIdx = spacetimedb.NewUniqueIndex[UniqueString, string]("UniqueString", "s",
	func(w *bsatn.Writer, v string) { w.WriteString(v) }, encodeUniqueString, decodeUniqueString)
var uniqueIdentityIIdx = spacetimedb.NewUniqueIndex[UniqueIdentity, types.Identity]("UniqueIdentity", "i",
	func(w *bsatn.Writer, v types.Identity) { v.WriteBsatn(w) }, encodeUniqueIdentity, decodeUniqueIdentity)
var uniqueConnectionIdAIdx = spacetimedb.NewUniqueIndex[UniqueConnectionId, types.ConnectionId]("UniqueConnectionId", "a",
	func(w *bsatn.Writer, v types.ConnectionId) { v.WriteBsatn(w) }, encodeUniqueConnectionId, decodeUniqueConnectionId)
var uniqueUuidUIdx = spacetimedb.NewUniqueIndex[UniqueUuid, types.Uuid]("UniqueUuid", "u",
	func(w *bsatn.Writer, v types.Uuid) { v.WriteBsatn(w) }, encodeUniqueUuid, decodeUniqueUuid)

// ── PK* table handles and indexes ─────────────────────────────────────────────

var pkU8Table = spacetimedb.NewTableHandle("PkU8", encodePkU8, decodePkU8)
var pkU16Table = spacetimedb.NewTableHandle("PkU16", encodePkU16, decodePkU16)
var pkU32Table = spacetimedb.NewTableHandle("PkU32", encodePkU32, decodePkU32)
var pkU32TwoTable = spacetimedb.NewTableHandle("PkU32Two", encodePkU32Two, decodePkU32Two)
var pkU64Table = spacetimedb.NewTableHandle("PkU64", encodePkU64, decodePkU64)
var pkU128Table = spacetimedb.NewTableHandle("PkU128", encodePkU128, decodePkU128)
var pkU256Table = spacetimedb.NewTableHandle("PkU256", encodePkU256, decodePkU256)
var pkI8Table = spacetimedb.NewTableHandle("PkI8", encodePkI8, decodePkI8)
var pkI16Table = spacetimedb.NewTableHandle("PkI16", encodePkI16, decodePkI16)
var pkI32Table = spacetimedb.NewTableHandle("PkI32", encodePkI32, decodePkI32)
var pkI64Table = spacetimedb.NewTableHandle("PkI64", encodePkI64, decodePkI64)
var pkI128Table = spacetimedb.NewTableHandle("PkI128", encodePkI128, decodePkI128)
var pkI256Table = spacetimedb.NewTableHandle("PkI256", encodePkI256, decodePkI256)
var pkBoolTable = spacetimedb.NewTableHandle("PkBool", encodePkBool, decodePkBool)
var pkStringTable = spacetimedb.NewTableHandle("PkString", encodePkString, decodePkString)
var pkIdentityTable = spacetimedb.NewTableHandle("PkIdentity", encodePkIdentity, decodePkIdentity)
var pkConnectionIdTable = spacetimedb.NewTableHandle("PkConnectionId", encodePkConnectionId, decodePkConnectionId)
var pkUuidTable = spacetimedb.NewTableHandle("PkUuid", encodePkUuid, decodePkUuid)
var pkSimpleEnumTable = spacetimedb.NewTableHandle("PkSimpleEnum", encodePkSimpleEnum, decodePkSimpleEnum)

// UniqueIndex on each PK* table's PK column for update/delete operations
var pkU8NIdx = spacetimedb.NewUniqueIndex[PkU8, uint8]("PkU8", "n",
	func(w *bsatn.Writer, v uint8) { w.WriteU8(v) }, encodePkU8, decodePkU8)
var pkU16NIdx = spacetimedb.NewUniqueIndex[PkU16, uint16]("PkU16", "n",
	func(w *bsatn.Writer, v uint16) { w.WriteU16(v) }, encodePkU16, decodePkU16)
var pkU32NIdx = spacetimedb.NewUniqueIndex[PkU32, uint32]("PkU32", "n",
	func(w *bsatn.Writer, v uint32) { w.WriteU32(v) }, encodePkU32, decodePkU32)
var pkU32TwoNIdx = spacetimedb.NewUniqueIndex[PkU32Two, uint32]("PkU32Two", "n",
	func(w *bsatn.Writer, v uint32) { w.WriteU32(v) }, encodePkU32Two, decodePkU32Two)
var pkU64NIdx = spacetimedb.NewUniqueIndex[PkU64, uint64]("PkU64", "n",
	func(w *bsatn.Writer, v uint64) { w.WriteU64(v) }, encodePkU64, decodePkU64)
var pkU128NIdx = spacetimedb.NewUniqueIndex[PkU128, types.U128]("PkU128", "n",
	func(w *bsatn.Writer, v types.U128) { v.WriteBsatn(w) }, encodePkU128, decodePkU128)
var pkU256NIdx = spacetimedb.NewUniqueIndex[PkU256, types.U256]("PkU256", "n",
	func(w *bsatn.Writer, v types.U256) { v.WriteBsatn(w) }, encodePkU256, decodePkU256)
var pkI8NIdx = spacetimedb.NewUniqueIndex[PkI8, int8]("PkI8", "n",
	func(w *bsatn.Writer, v int8) { w.WriteI8(v) }, encodePkI8, decodePkI8)
var pkI16NIdx = spacetimedb.NewUniqueIndex[PkI16, int16]("PkI16", "n",
	func(w *bsatn.Writer, v int16) { w.WriteI16(v) }, encodePkI16, decodePkI16)
var pkI32NIdx = spacetimedb.NewUniqueIndex[PkI32, int32]("PkI32", "n",
	func(w *bsatn.Writer, v int32) { w.WriteI32(v) }, encodePkI32, decodePkI32)
var pkI64NIdx = spacetimedb.NewUniqueIndex[PkI64, int64]("PkI64", "n",
	func(w *bsatn.Writer, v int64) { w.WriteI64(v) }, encodePkI64, decodePkI64)
var pkI128NIdx = spacetimedb.NewUniqueIndex[PkI128, types.I128]("PkI128", "n",
	func(w *bsatn.Writer, v types.I128) { v.WriteBsatn(w) }, encodePkI128, decodePkI128)
var pkI256NIdx = spacetimedb.NewUniqueIndex[PkI256, types.I256]("PkI256", "n",
	func(w *bsatn.Writer, v types.I256) { v.WriteBsatn(w) }, encodePkI256, decodePkI256)
var pkBoolBIdx = spacetimedb.NewUniqueIndex[PkBool, bool]("PkBool", "b",
	func(w *bsatn.Writer, v bool) { w.WriteBool(v) }, encodePkBool, decodePkBool)
var pkStringSIdx = spacetimedb.NewUniqueIndex[PkString, string]("PkString", "s",
	func(w *bsatn.Writer, v string) { w.WriteString(v) }, encodePkString, decodePkString)
var pkIdentityIIdx = spacetimedb.NewUniqueIndex[PkIdentity, types.Identity]("PkIdentity", "i",
	func(w *bsatn.Writer, v types.Identity) { v.WriteBsatn(w) }, encodePkIdentity, decodePkIdentity)
var pkConnectionIdAIdx = spacetimedb.NewUniqueIndex[PkConnectionId, types.ConnectionId]("PkConnectionId", "a",
	func(w *bsatn.Writer, v types.ConnectionId) { v.WriteBsatn(w) }, encodePkConnectionId, decodePkConnectionId)
var pkUuidUIdx = spacetimedb.NewUniqueIndex[PkUuid, types.Uuid]("PkUuid", "u",
	func(w *bsatn.Writer, v types.Uuid) { v.WriteBsatn(w) }, encodePkUuid, decodePkUuid)
var pkSimpleEnumAIdx = spacetimedb.NewUniqueIndex[PkSimpleEnum, SimpleEnum]("PkSimpleEnum", "a",
	func(w *bsatn.Writer, v SimpleEnum) { encodeSimpleEnum(w, v) }, encodePkSimpleEnum, decodePkSimpleEnum)

// ── Special table handles ────────────────────────────────────────────────────

var largeTableHandle = spacetimedb.NewTableHandle("LargeTable", encodeLargeTable, decodeLargeTable)
var tableHoldsTableHandle = spacetimedb.NewTableHandle("TableHoldsTable", encodeTableHoldsTable, decodeTableHoldsTable)
var scheduledTableHandle = spacetimedb.NewTableHandle("ScheduledTable", encodeScheduledTable, decodeScheduledTable)
var indexedTableHandle = spacetimedb.NewTableHandle("IndexedTable", encodeIndexedTable, decodeIndexedTable)
var indexedTable2Handle = spacetimedb.NewTableHandle("IndexedTable2", encodeIndexedTable2, decodeIndexedTable2)
var btreeU32TableHandle = spacetimedb.NewTableHandle("BTreeU32", encodeBTreeU32Row, decodeBTreeU32Row)
var usersTableHandle = spacetimedb.NewTableHandle("Users", encodeUsersRow, decodeUsersRow)
var indexedSimpleEnumTableHandle = spacetimedb.NewTableHandle("IndexedSimpleEnum", encodeIndexedSimpleEnumRow, decodeIndexedSimpleEnumRow)

// BTree index on BTreeU32.n
var btreeU32NIdx = spacetimedb.NewBTreeIndex[BTreeU32Row, uint32]("n",
	func(w *bsatn.Writer, v uint32) { w.WriteU32(v) }, decodeBTreeU32Row)

// BTree index on IndexedSimpleEnum.n
var indexedSimpleEnumNIdx = spacetimedb.NewBTreeIndex[IndexedSimpleEnumRow, SimpleEnum]("n",
	func(w *bsatn.Writer, v SimpleEnum) { encodeSimpleEnum(w, v) }, decodeIndexedSimpleEnumRow)

// UniqueIndex on Users.identity (PK)
var usersIdentityIdx = spacetimedb.NewUniqueIndex[UsersRow, types.Identity]("Users", "identity",
	func(w *bsatn.Writer, v types.Identity) { v.WriteBsatn(w) }, encodeUsersRow, decodeUsersRow)
