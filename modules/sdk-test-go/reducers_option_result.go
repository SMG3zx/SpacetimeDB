package main

import (
	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
)

func registerOptionResultReducers() {
	// ── Option* reducers (7) ─────────────────────────────────────────────────
	regR("insert_option_i32", []spacetimedb.ColumnDef{{Name: "n", Type: satOptionI32}}, insertOptionI32)
	regR("insert_option_string", []spacetimedb.ColumnDef{{Name: "s", Type: satOptionString}}, insertOptionString)
	regR("insert_option_identity", []spacetimedb.ColumnDef{{Name: "i", Type: satOptionIdentity}}, insertOptionIdentity)
	regR("insert_option_uuid", []spacetimedb.ColumnDef{{Name: "u", Type: satOptionUuid}}, insertOptionUuid)
	regR("insert_option_simple_enum", []spacetimedb.ColumnDef{{Name: "e", Type: satOptionSimpleEnum}}, insertOptionSimpleEnum)
	regR("insert_option_every_primitive_struct", []spacetimedb.ColumnDef{{Name: "s", Type: satOptionEveryPrimitiveStruct}}, insertOptionEveryPrimitiveStruct)
	regR("insert_option_vec_option_i32", []spacetimedb.ColumnDef{{Name: "v", Type: satOptionVecOptionI32}}, insertOptionVecOptionI32)

	// ── Result* reducers (6) ─────────────────────────────────────────────────
	regR("insert_result_i32_string", []spacetimedb.ColumnDef{{Name: "r", Type: satResultI32String}}, insertResultI32String)
	regR("insert_result_string_i32", []spacetimedb.ColumnDef{{Name: "r", Type: satResultStringI32}}, insertResultStringI32)
	regR("insert_result_identity_string", []spacetimedb.ColumnDef{{Name: "r", Type: satResultIdentityString}}, insertResultIdentityString)
	regR("insert_result_simple_enum_i32", []spacetimedb.ColumnDef{{Name: "r", Type: satResultSimpleEnumI32}}, insertResultSimpleEnumI32)
	regR("insert_result_every_primitive_struct_string", []spacetimedb.ColumnDef{{Name: "r", Type: satResultEveryPrimitiveStructString}}, insertResultEveryPrimitiveStructString)
	regR("insert_result_vec_i32_string", []spacetimedb.ColumnDef{{Name: "r", Type: satResultVecI32String}}, insertResultVecI32String)
}

// ── Option* handlers ──────────────────────────────────────────────────────────

func insertOptionI32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_option_i32", args)
	row, err := decodeOptionI32Row(r)
	if err != nil {
		spacetimedb.LogPanic("insert_option_i32: " + err.Error())
	}
	if _, err := optionI32Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_option_i32: " + err.Error())
	}
}

func insertOptionString(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_option_string", args)
	row, err := decodeOptionStringRow(r)
	if err != nil {
		spacetimedb.LogPanic("insert_option_string: " + err.Error())
	}
	if _, err := optionStringTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_option_string: " + err.Error())
	}
}

func insertOptionIdentity(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_option_identity", args)
	row, err := decodeOptionIdentityRow(r)
	if err != nil {
		spacetimedb.LogPanic("insert_option_identity: " + err.Error())
	}
	if _, err := optionIdentityTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_option_identity: " + err.Error())
	}
}

func insertOptionUuid(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_option_uuid", args)
	row, err := decodeOptionUuidRow(r)
	if err != nil {
		spacetimedb.LogPanic("insert_option_uuid: " + err.Error())
	}
	if _, err := optionUuidTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_option_uuid: " + err.Error())
	}
}

func insertOptionSimpleEnum(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_option_simple_enum", args)
	row, err := decodeOptionSimpleEnumRow(r)
	if err != nil {
		spacetimedb.LogPanic("insert_option_simple_enum: " + err.Error())
	}
	if _, err := optionSimpleEnumTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_option_simple_enum: " + err.Error())
	}
}

func insertOptionEveryPrimitiveStruct(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_option_every_primitive_struct", args)
	row, err := decodeOptionEveryPrimitiveStructRow(r)
	if err != nil {
		spacetimedb.LogPanic("insert_option_every_primitive_struct: " + err.Error())
	}
	if _, err := optionEveryPrimitiveStructTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_option_every_primitive_struct: " + err.Error())
	}
}

func insertOptionVecOptionI32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_option_vec_option_i32", args)
	row, err := decodeOptionVecOptionI32Row(r)
	if err != nil {
		spacetimedb.LogPanic("insert_option_vec_option_i32: " + err.Error())
	}
	if _, err := optionVecOptionI32Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_option_vec_option_i32: " + err.Error())
	}
}

// ── Result* handlers ──────────────────────────────────────────────────────────

func insertResultI32String(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_result_i32_string", args)
	row, err := decodeResultI32StringRow(r)
	if err != nil {
		spacetimedb.LogPanic("insert_result_i32_string: " + err.Error())
	}
	if _, err := resultI32StringTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_result_i32_string: " + err.Error())
	}
}

func insertResultStringI32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_result_string_i32", args)
	row, err := decodeResultStringI32Row(r)
	if err != nil {
		spacetimedb.LogPanic("insert_result_string_i32: " + err.Error())
	}
	if _, err := resultStringI32Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_result_string_i32: " + err.Error())
	}
}

func insertResultIdentityString(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_result_identity_string", args)
	row, err := decodeResultIdentityStringRow(r)
	if err != nil {
		spacetimedb.LogPanic("insert_result_identity_string: " + err.Error())
	}
	if _, err := resultIdentityStringTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_result_identity_string: " + err.Error())
	}
}

func insertResultSimpleEnumI32(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_result_simple_enum_i32", args)
	row, err := decodeResultSimpleEnumI32Row(r)
	if err != nil {
		spacetimedb.LogPanic("insert_result_simple_enum_i32: " + err.Error())
	}
	if _, err := resultSimpleEnumI32Table.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_result_simple_enum_i32: " + err.Error())
	}
}

func insertResultEveryPrimitiveStructString(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_result_every_primitive_struct_string", args)
	row, err := decodeResultEveryPrimitiveStructStringRow(r)
	if err != nil {
		spacetimedb.LogPanic("insert_result_every_primitive_struct_string: " + err.Error())
	}
	if _, err := resultEveryPrimitiveStructStringTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_result_every_primitive_struct_string: " + err.Error())
	}
}

func insertResultVecI32String(_ spacetimedb.ReducerContext, args sys.BytesSource) {
	r := mustReadArgs("insert_result_vec_i32_string", args)
	row, err := decodeResultVecI32StringRow(r)
	if err != nil {
		spacetimedb.LogPanic("insert_result_vec_i32_string: " + err.Error())
	}
	if _, err := resultVecI32StringTable.Insert(row); err != nil {
		spacetimedb.LogPanic("insert_result_vec_i32_string: " + err.Error())
	}
}
