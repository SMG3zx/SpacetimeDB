package main

import (
	"math/rand"
	"strconv"

	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
	"github.com/clockworklabs/spacetimedb-go/types"
)

// ── SATS type vars for special reducer params ─────────────────────────────────

// satRowNU32DataI32 is the SATS ProductType for {n: u32, data: i32} (BTreeU32/PkU32 rows).
var satRowNU32DataI32 = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("n"), Type: types.AlgebraicU32},
		{Name: sptr("data"), Type: types.AlgebraicI32},
	},
}

// satOneU8Param is the SATS ProductType for OneU8 used as a reducer param.
var satOneU8Param = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("n"), Type: types.AlgebraicU8},
	},
}

// satVecU8Param is the SATS ProductType for VecU8 used as a reducer param.
var satVecU8Param = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("n"), Type: types.ArrayType{ElemType: types.AlgebraicU8}},
	},
}

// largeTableParams are the 22 individual ColumnDefs for insert/delete_large_table.
var largeTableParams = []spacetimedb.ColumnDef{
	{Name: "a", Type: types.AlgebraicU8},
	{Name: "b", Type: types.AlgebraicU16},
	{Name: "c", Type: types.AlgebraicU32},
	{Name: "d", Type: types.AlgebraicU64},
	{Name: "e", Type: types.AlgebraicU128},
	{Name: "f", Type: types.AlgebraicU256},
	{Name: "g", Type: types.AlgebraicI8},
	{Name: "h", Type: types.AlgebraicI16},
	{Name: "i", Type: types.AlgebraicI32},
	{Name: "j", Type: types.AlgebraicI64},
	{Name: "k", Type: types.AlgebraicI128},
	{Name: "l", Type: types.AlgebraicI256},
	{Name: "m", Type: types.AlgebraicBool},
	{Name: "n", Type: types.AlgebraicF32},
	{Name: "o", Type: types.AlgebraicF64},
	{Name: "p", Type: types.AlgebraicString},
	{Name: "q", Type: satSimpleEnum},
	{Name: "r", Type: satEnumWithPayload},
	{Name: "s", Type: satUnitStruct},
	{Name: "t", Type: satByteStruct},
	{Name: "u", Type: satEveryPrimitiveStruct},
	{Name: "v", Type: satEveryVecStruct},
}

// ── Registration ──────────────────────────────────────────────────────────────

func registerSpecialReducers() {
	regR("update_pk_simple_enum", []spacetimedb.ColumnDef{
		{Name: "a", Type: satSimpleEnum},
		{Name: "data", Type: types.AlgebraicI32},
	}, updatePkSimpleEnum)

	regR("insert_into_btree_u32", []spacetimedb.ColumnDef{
		{Name: "rows", Type: types.ArrayType{ElemType: satRowNU32DataI32}},
	}, insertIntoBTreeU32)

	regR("delete_from_btree_u32", []spacetimedb.ColumnDef{
		{Name: "rows", Type: types.ArrayType{ElemType: satRowNU32DataI32}},
	}, deleteFromBTreeU32)

	regR("insert_into_pk_btree_u32", []spacetimedb.ColumnDef{
		{Name: "pk_u32", Type: types.ArrayType{ElemType: satRowNU32DataI32}},
		{Name: "bt_u32", Type: types.ArrayType{ElemType: satRowNU32DataI32}},
	}, insertIntoPkBTreeU32)

	regR("insert_unique_u32_update_pk_u32", []spacetimedb.ColumnDef{
		{Name: "n", Type: types.AlgebraicU32},
		{Name: "d_unique", Type: types.AlgebraicI32},
		{Name: "d_pk", Type: types.AlgebraicI32},
	}, insertUniqueU32UpdatePkU32)

	regR("delete_pk_u32_insert_pk_u32_two", []spacetimedb.ColumnDef{
		{Name: "n", Type: types.AlgebraicU32},
		{Name: "data", Type: types.AlgebraicI32},
	}, deletePkU32InsertPkU32Two)

	regR("insert_caller_one_identity", nil, insertCallerOneIdentity)
	regR("insert_caller_vec_identity", nil, insertCallerVecIdentity)
	regR("insert_caller_unique_identity", []spacetimedb.ColumnDef{
		{Name: "data", Type: types.AlgebraicI32},
	}, insertCallerUniqueIdentity)
	regR("insert_caller_pk_identity", []spacetimedb.ColumnDef{
		{Name: "data", Type: types.AlgebraicI32},
	}, insertCallerPkIdentity)

	regR("insert_caller_one_connection_id", nil, insertCallerOneConnectionId)
	regR("insert_caller_vec_connection_id", nil, insertCallerVecConnectionId)
	regR("insert_caller_unique_connection_id", []spacetimedb.ColumnDef{
		{Name: "data", Type: types.AlgebraicI32},
	}, insertCallerUniqueConnectionId)
	regR("insert_caller_pk_connection_id", []spacetimedb.ColumnDef{
		{Name: "data", Type: types.AlgebraicI32},
	}, insertCallerPkConnectionId)

	regR("insert_call_timestamp", nil, insertCallTimestamp)
	regR("insert_call_uuid_v4", nil, insertCallUuidV4)
	regR("insert_call_uuid_v7", nil, insertCallUuidV7)

	regR("insert_primitives_as_strings", []spacetimedb.ColumnDef{
		{Name: "s", Type: satEveryPrimitiveStruct},
	}, insertPrimitivesAsStrings)

	regR("insert_large_table", largeTableParams, insertLargeTable)
	regR("delete_large_table", largeTableParams, deleteLargeTable)

	regR("insert_table_holds_table", []spacetimedb.ColumnDef{
		{Name: "a", Type: satOneU8Param},
		{Name: "b", Type: satVecU8Param},
	}, insertTableHoldsTable)

	regR("no_op_succeeds", nil, noOpSucceeds)

	regPrivR("send_scheduled_message", []spacetimedb.ColumnDef{
		{Name: "arg", Type: satScheduledTable},
	}, sendScheduledMessage)

	regR("insert_user", []spacetimedb.ColumnDef{
		{Name: "name", Type: types.AlgebraicString},
		{Name: "identity", Type: satIdentity},
	}, insertUser)

	regR("insert_into_indexed_simple_enum", []spacetimedb.ColumnDef{
		{Name: "n", Type: satSimpleEnum},
	}, insertIntoIndexedSimpleEnum)

	regR("update_indexed_simple_enum", []spacetimedb.ColumnDef{
		{Name: "a", Type: satSimpleEnum},
		{Name: "b", Type: satSimpleEnum},
	}, updateIndexedSimpleEnum)

	regR("sorted_uuids_insert", nil, sortedUuidsInsert)
}

// ── Handlers ──────────────────────────────────────────────────────────────────

func updatePkSimpleEnum(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_pk_simple_enum", args)
	a, err := decodeSimpleEnum(r)
	if err != nil {
		spacetimedb.LogPanic("update_pk_simple_enum: " + err.Error())
	}
	data, err := r.ReadI32()
	if err != nil {
		spacetimedb.LogPanic("update_pk_simple_enum: " + err.Error())
	}
	row, err := pkSimpleEnumAIdx.Find(a)
	if err != nil {
		spacetimedb.LogPanic("update_pk_simple_enum: " + err.Error())
	}
	if row == nil {
		spacetimedb.LogPanic("update_pk_simple_enum: row not found")
	}
	row.Data = data
	if _, err := pkSimpleEnumAIdx.Update(*row); err != nil {
		spacetimedb.LogPanic("update_pk_simple_enum: " + err.Error())
	}
}

func insertIntoBTreeU32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_into_btree_u32", args)
	n, err := r.ReadArrayLen()
	if err != nil {
		spacetimedb.LogPanic("insert_into_btree_u32: " + err.Error())
	}
	for i := 0; i < int(n); i++ {
		row, err := decodeBTreeU32Row(r)
		if err != nil {
			spacetimedb.LogPanic("insert_into_btree_u32: " + err.Error())
		}
		if _, err := btreeU32TableHandle.Insert(row); err != nil {
			spacetimedb.LogPanic("insert_into_btree_u32: " + err.Error())
		}
	}
}

func deleteFromBTreeU32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_from_btree_u32", args)
	n, err := r.ReadArrayLen()
	if err != nil {
		spacetimedb.LogPanic("delete_from_btree_u32: " + err.Error())
	}
	for i := 0; i < int(n); i++ {
		row, err := decodeBTreeU32Row(r)
		if err != nil {
			spacetimedb.LogPanic("delete_from_btree_u32: " + err.Error())
		}
		if _, err := btreeU32TableHandle.Delete(row); err != nil {
			spacetimedb.LogPanic("delete_from_btree_u32: " + err.Error())
		}
	}
}

func insertIntoPkBTreeU32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_into_pk_btree_u32", args)
	nPk, err := r.ReadArrayLen()
	if err != nil {
		spacetimedb.LogPanic("insert_into_pk_btree_u32: " + err.Error())
	}
	for i := 0; i < int(nPk); i++ {
		row, err := decodePkU32(r)
		if err != nil {
			spacetimedb.LogPanic("insert_into_pk_btree_u32: " + err.Error())
		}
		if _, err := pkU32Table.Insert(row); err != nil {
			spacetimedb.LogPanic("insert_into_pk_btree_u32: " + err.Error())
		}
	}
	nBt, err := r.ReadArrayLen()
	if err != nil {
		spacetimedb.LogPanic("insert_into_pk_btree_u32: " + err.Error())
	}
	for i := 0; i < int(nBt); i++ {
		row, err := decodeBTreeU32Row(r)
		if err != nil {
			spacetimedb.LogPanic("insert_into_pk_btree_u32: " + err.Error())
		}
		if _, err := btreeU32TableHandle.Insert(row); err != nil {
			spacetimedb.LogPanic("insert_into_pk_btree_u32: " + err.Error())
		}
	}
}

func insertUniqueU32UpdatePkU32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_unique_u32_update_pk_u32", args)
	n, err := r.ReadU32()
	if err != nil {
		spacetimedb.LogPanic("insert_unique_u32_update_pk_u32: " + err.Error())
	}
	dUnique, err := r.ReadI32()
	if err != nil {
		spacetimedb.LogPanic("insert_unique_u32_update_pk_u32: " + err.Error())
	}
	dPk, err := r.ReadI32()
	if err != nil {
		spacetimedb.LogPanic("insert_unique_u32_update_pk_u32: " + err.Error())
	}
	if _, err := uniqueU32Table.Insert(UniqueU32{N: n, Data: dUnique}); err != nil {
		spacetimedb.LogPanic("insert_unique_u32_update_pk_u32: " + err.Error())
	}
	if _, err := pkU32NIdx.Update(PkU32{N: n, Data: dPk}); err != nil {
		spacetimedb.LogPanic("insert_unique_u32_update_pk_u32: " + err.Error())
	}
}

func deletePkU32InsertPkU32Two(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_pk_u32_insert_pk_u32_two", args)
	n, err := r.ReadU32()
	if err != nil {
		spacetimedb.LogPanic("delete_pk_u32_insert_pk_u32_two: " + err.Error())
	}
	data, err := r.ReadI32()
	if err != nil {
		spacetimedb.LogPanic("delete_pk_u32_insert_pk_u32_two: " + err.Error())
	}
	if _, err := pkU32TwoTable.Insert(PkU32Two{N: n, Data: data}); err != nil {
		spacetimedb.LogPanic("delete_pk_u32_insert_pk_u32_two: " + err.Error())
	}
	if _, err := pkU32Table.Delete(PkU32{N: n, Data: data}); err != nil {
		spacetimedb.LogPanic("delete_pk_u32_insert_pk_u32_two: " + err.Error())
	}
}

// ── Caller identity reducers ──────────────────────────────────────────────────

func insertCallerOneIdentity(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	if _, err := oneIdentityTable.Insert(OneIdentity{I: ctx.Sender}); err != nil {
		spacetimedb.LogPanic("insert_caller_one_identity: " + err.Error())
	}
}

func insertCallerVecIdentity(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	if _, err := vecIdentityTable.Insert(VecIdentity{I: []types.Identity{ctx.Sender}}); err != nil {
		spacetimedb.LogPanic("insert_caller_vec_identity: " + err.Error())
	}
}

func insertCallerUniqueIdentity(ctx spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_caller_unique_identity", args)
	data, err := r.ReadI32()
	if err != nil {
		spacetimedb.LogPanic("insert_caller_unique_identity: " + err.Error())
	}
	if _, err := uniqueIdentityTable.Insert(UniqueIdentity{I: ctx.Sender, Data: data}); err != nil {
		spacetimedb.LogPanic("insert_caller_unique_identity: " + err.Error())
	}
}

func insertCallerPkIdentity(ctx spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_caller_pk_identity", args)
	data, err := r.ReadI32()
	if err != nil {
		spacetimedb.LogPanic("insert_caller_pk_identity: " + err.Error())
	}
	if _, err := pkIdentityTable.Insert(PkIdentity{I: ctx.Sender, Data: data}); err != nil {
		spacetimedb.LogPanic("insert_caller_pk_identity: " + err.Error())
	}
}

// ── Caller connection_id reducers ─────────────────────────────────────────────

func insertCallerOneConnectionId(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	if ctx.ConnectionId == nil {
		spacetimedb.LogPanic("insert_caller_one_connection_id: no connection id")
	}
	if _, err := oneConnectionIdTable.Insert(OneConnectionId{A: *ctx.ConnectionId}); err != nil {
		spacetimedb.LogPanic("insert_caller_one_connection_id: " + err.Error())
	}
}

func insertCallerVecConnectionId(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	if ctx.ConnectionId == nil {
		spacetimedb.LogPanic("insert_caller_vec_connection_id: no connection id")
	}
	if _, err := vecConnectionIdTable.Insert(VecConnectionId{A: []types.ConnectionId{*ctx.ConnectionId}}); err != nil {
		spacetimedb.LogPanic("insert_caller_vec_connection_id: " + err.Error())
	}
}

func insertCallerUniqueConnectionId(ctx spacetimedb.ReducerContext, args sys.BytesSource) {
	if ctx.ConnectionId == nil {
		spacetimedb.LogPanic("insert_caller_unique_connection_id: no connection id")
	}
	r := mustReadArgs("insert_caller_unique_connection_id", args)
	data, err := r.ReadI32()
	if err != nil {
		spacetimedb.LogPanic("insert_caller_unique_connection_id: " + err.Error())
	}
	if _, err := uniqueConnectionIdTable.Insert(UniqueConnectionId{A: *ctx.ConnectionId, Data: data}); err != nil {
		spacetimedb.LogPanic("insert_caller_unique_connection_id: " + err.Error())
	}
}

func insertCallerPkConnectionId(ctx spacetimedb.ReducerContext, args sys.BytesSource) {
	if ctx.ConnectionId == nil {
		spacetimedb.LogPanic("insert_caller_pk_connection_id: no connection id")
	}
	r := mustReadArgs("insert_caller_pk_connection_id", args)
	data, err := r.ReadI32()
	if err != nil {
		spacetimedb.LogPanic("insert_caller_pk_connection_id: " + err.Error())
	}
	if _, err := pkConnectionIdTable.Insert(PkConnectionId{A: *ctx.ConnectionId, Data: data}); err != nil {
		spacetimedb.LogPanic("insert_caller_pk_connection_id: " + err.Error())
	}
}

// ── Timestamp / UUID reducers ─────────────────────────────────────────────────

func insertCallTimestamp(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	if _, err := oneTimestampTable.Insert(OneTimestamp{T: ctx.Timestamp}); err != nil {
		spacetimedb.LogPanic("insert_call_timestamp: " + err.Error())
	}
}

func insertCallUuidV4(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	u := genUUIDv4(ctx.Rng)
	if _, err := oneUuidTable.Insert(OneUuid{U: u}); err != nil {
		spacetimedb.LogPanic("insert_call_uuid_v4: " + err.Error())
	}
}

func insertCallUuidV7(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	u := genUUIDv7(ctx.Timestamp, ctx.Rng, 0)
	if _, err := oneUuidTable.Insert(OneUuid{U: u}); err != nil {
		spacetimedb.LogPanic("insert_call_uuid_v7: " + err.Error())
	}
}

// ── Primitives-as-strings reducer ─────────────────────────────────────────────

func insertPrimitivesAsStrings(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_primitives_as_strings", args)
	s, err := decodeEveryPrimitiveStruct(r)
	if err != nil {
		spacetimedb.LogPanic("insert_primitives_as_strings: " + err.Error())
	}
	strs := []string{
		strconv.FormatUint(uint64(s.A), 10),
		strconv.FormatUint(uint64(s.B), 10),
		strconv.FormatUint(uint64(s.C), 10),
		strconv.FormatUint(uint64(s.D), 10),
		s.E.String(),
		s.F.String(),
		strconv.FormatInt(int64(s.G), 10),
		strconv.FormatInt(int64(s.H), 10),
		strconv.FormatInt(int64(s.I), 10),
		strconv.FormatInt(int64(s.J), 10),
		s.K.String(),
		s.L.String(),
		strconv.FormatBool(s.M),
		strconv.FormatFloat(float64(s.N), 'g', -1, 32),
		strconv.FormatFloat(s.O, 'g', -1, 64),
		s.P,
		s.Q.String(),
		s.R.String(),
		s.S.String(),
		s.T.String(),
		s.U.String(),
	}
	if _, err := vecStringTable.Insert(VecString{S: strs}); err != nil {
		spacetimedb.LogPanic("insert_primitives_as_strings: " + err.Error())
	}
}

// ── Large table reducers ──────────────────────────────────────────────────────

func insertLargeTable(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_large_table", args)
	row, err := decodeLargeTable(r)
	if err != nil {
		spacetimedb.LogPanic("insert_large_table: " + err.Error())
	}
	if _, err := largeTableHandle.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_large_table: " + err.Error())
	}
}

func deleteLargeTable(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("delete_large_table", args)
	row, err := decodeLargeTable(r)
	if err != nil {
		spacetimedb.LogPanic("delete_large_table: " + err.Error())
	}
	if _, err := largeTableHandle.Delete(row); err != nil {
		spacetimedb.LogPanic("delete_large_table: " + err.Error())
	}
}

func insertTableHoldsTable(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_table_holds_table", args)
	row, err := decodeTableHoldsTable(r)
	if err != nil {
		spacetimedb.LogPanic("insert_table_holds_table: " + err.Error())
	}
	if _, err := tableHoldsTableHandle.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_table_holds_table: " + err.Error())
	}
}

func noOpSucceeds(_ spacetimedb.ReducerContext, _ sys.BytesSource) {}

// ── Scheduled reducer ─────────────────────────────────────────────────────────

func sendScheduledMessage(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("send_scheduled_message", args)
	if _, err := decodeScheduledTable(r); err != nil {
		spacetimedb.LogPanic("send_scheduled_message: " + err.Error())
	}
}

// ── Users and indexed reducers ────────────────────────────────────────────────

func insertUser(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_user", args)
	name, err := r.ReadString()
	if err != nil {
		spacetimedb.LogPanic("insert_user: " + err.Error())
	}
	identity, err := types.ReadIdentity(r)
	if err != nil {
		spacetimedb.LogPanic("insert_user: " + err.Error())
	}
	if _, err := usersTableHandle.Insert(UsersRow{Name: name, Identity: identity}); err != nil {
		spacetimedb.LogPanic("insert_user: " + err.Error())
	}
}

func insertIntoIndexedSimpleEnum(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_into_indexed_simple_enum", args)
	n, err := decodeSimpleEnum(r)
	if err != nil {
		spacetimedb.LogPanic("insert_into_indexed_simple_enum: " + err.Error())
	}
	if _, err := indexedSimpleEnumTableHandle.Insert(IndexedSimpleEnumRow{N: n}); err != nil {
		spacetimedb.LogPanic("insert_into_indexed_simple_enum: " + err.Error())
	}
}

func updateIndexedSimpleEnum(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("update_indexed_simple_enum", args)
	a, err := decodeSimpleEnum(r)
	if err != nil {
		spacetimedb.LogPanic("update_indexed_simple_enum: " + err.Error())
	}
	b, err := decodeSimpleEnum(r)
	if err != nil {
		spacetimedb.LogPanic("update_indexed_simple_enum: " + err.Error())
	}
	// Only delete+insert if a row with n=a exists.
	found := false
	for _, ferr := range indexedSimpleEnumNIdx.Filter(a) {
		if ferr != nil {
			spacetimedb.LogPanic("update_indexed_simple_enum: " + ferr.Error())
		}
		found = true
		break
	}
	if found {
		if _, err := indexedSimpleEnumTableHandle.Delete(IndexedSimpleEnumRow{N: a}); err != nil {
			spacetimedb.LogPanic("update_indexed_simple_enum: " + err.Error())
		}
		if _, err := indexedSimpleEnumTableHandle.Insert(IndexedSimpleEnumRow{N: b}); err != nil {
			spacetimedb.LogPanic("update_indexed_simple_enum: " + err.Error())
		}
	}
}

// ── Sorted UUIDs test reducer ─────────────────────────────────────────────────

func sortedUuidsInsert(ctx spacetimedb.ReducerContext, _ sys.BytesSource) {
	// Insert 1000 UUID v7s. seq is a monotonic 12-bit counter that ensures
	// strict ordering within the same millisecond timestamp.
	for i := 0; i < 1000; i++ {
		u := genUUIDv7(ctx.Timestamp, ctx.Rng, uint16(i))
		if _, err := pkUuidTable.Insert(PkUuid{U: u, Data: 0}); err != nil {
			spacetimedb.LogPanic("sorted_uuids_insert: " + err.Error())
		}
	}
	// Verify that the table scan returns rows in ascending UUID order.
	var lastUUID *types.Uuid
	for row, err := range pkUuidTable.Iter() {
		if err != nil {
			spacetimedb.LogPanic("sorted_uuids_insert: " + err.Error())
		}
		if lastUUID != nil && uuidGE(*lastUUID, row.U) {
			spacetimedb.LogPanic("sorted_uuids_insert: UUIDs are not sorted correctly")
		}
		u := row.U
		lastUUID = &u
	}
}

// ── UUID generation helpers ───────────────────────────────────────────────────

// genUUIDv4 generates a random UUID version 4 using the given PRNG.
func genUUIDv4(rng *rand.Rand) types.Uuid {
	var u types.Uuid
	lo := rng.Uint64()
	hi := rng.Uint64()
	for i := 0; i < 8; i++ {
		u[i] = byte(lo >> (uint(i) * 8))
	}
	for i := 0; i < 8; i++ {
		u[8+i] = byte(hi >> (uint(i) * 8))
	}
	u[6] = (u[6] & 0x0F) | 0x40 // version 4
	u[8] = (u[8] & 0x3F) | 0x80 // variant RFC 4122
	return u
}

// genUUIDv7 generates a time-ordered UUID version 7.
// The top 48 bits encode the Unix millisecond timestamp from ts.
// seq (12-bit counter) occupies the rand_a field to ensure strict ordering
// within the same millisecond when generating multiple UUIDs per reducer call.
func genUUIDv7(ts types.Timestamp, rng *rand.Rand, seq uint16) types.Uuid {
	var u types.Uuid
	lo := rng.Uint64()
	hi := rng.Uint64()
	for i := 0; i < 8; i++ {
		u[i] = byte(lo >> (uint(i) * 8))
	}
	for i := 0; i < 8; i++ {
		u[8+i] = byte(hi >> (uint(i) * 8))
	}
	// Overwrite bytes 0-7 with timestamp + version + seq.
	ms := uint64(ts.Microseconds / 1000)
	u[0] = byte(ms >> 40)
	u[1] = byte(ms >> 32)
	u[2] = byte(ms >> 24)
	u[3] = byte(ms >> 16)
	u[4] = byte(ms >> 8)
	u[5] = byte(ms)
	u[6] = 0x70 | byte(seq>>8)&0x0F // version 7, seq high 4 bits
	u[7] = byte(seq)                  // seq low 8 bits
	u[8] = (u[8] & 0x3F) | 0x80      // variant RFC 4122
	return u
}

// uuidGE reports whether a >= b in big-endian byte order (UUID comparison order).
func uuidGE(a, b types.Uuid) bool {
	for i := range a {
		if a[i] > b[i] {
			return true
		}
		if a[i] < b[i] {
			return false
		}
	}
	return true // equal
}
