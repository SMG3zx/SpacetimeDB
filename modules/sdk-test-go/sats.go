package main

import "github.com/clockworklabs/spacetimedb-go/types"

// ── SATS type variables ──────────────────────────────────────────────────────

var satIdentity = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("__identity__"), Type: types.AlgebraicU256},
	},
}

var satConnectionId = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("__connection_id__"), Type: types.AlgebraicU128},
	},
}

var satUuid = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("__uuid__"), Type: types.AlgebraicU128},
	},
}

var satSimpleEnum = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("Zero"), Type: types.ProductType{}},
		{Name: sptr("One"), Type: types.ProductType{}},
		{Name: sptr("Two"), Type: types.ProductType{}},
	},
}

var satEnumWithPayload = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("U8"), Type: types.AlgebraicU8},
		{Name: sptr("U16"), Type: types.AlgebraicU16},
		{Name: sptr("U32"), Type: types.AlgebraicU32},
		{Name: sptr("U64"), Type: types.AlgebraicU64},
		{Name: sptr("U128"), Type: types.AlgebraicU128},
		{Name: sptr("U256"), Type: types.AlgebraicU256},
		{Name: sptr("I8"), Type: types.AlgebraicI8},
		{Name: sptr("I16"), Type: types.AlgebraicI16},
		{Name: sptr("I32"), Type: types.AlgebraicI32},
		{Name: sptr("I64"), Type: types.AlgebraicI64},
		{Name: sptr("I128"), Type: types.AlgebraicI128},
		{Name: sptr("I256"), Type: types.AlgebraicI256},
		{Name: sptr("Bool"), Type: types.AlgebraicBool},
		{Name: sptr("F32"), Type: types.AlgebraicF32},
		{Name: sptr("F64"), Type: types.AlgebraicF64},
		{Name: sptr("Str"), Type: types.AlgebraicString},
		{Name: sptr("Identity"), Type: satIdentity},
		{Name: sptr("ConnectionId"), Type: satConnectionId},
		{Name: sptr("Timestamp"), Type: types.AlgebraicTimestamp},
		{Name: sptr("Uuid"), Type: satUuid},
		{Name: sptr("Bytes"), Type: types.AlgebraicBytes},
		{Name: sptr("Ints"), Type: types.ArrayType{ElemType: types.AlgebraicI32}},
		{Name: sptr("Strings"), Type: types.ArrayType{ElemType: types.AlgebraicString}},
		{Name: sptr("SimpleEnums"), Type: types.ArrayType{ElemType: satSimpleEnum}},
	},
}

var satUnitStruct = types.ProductType{}

var satByteStruct = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("b"), Type: types.AlgebraicU8},
	},
}

var satEveryPrimitiveStruct = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("a"), Type: types.AlgebraicU8},
		{Name: sptr("b"), Type: types.AlgebraicU16},
		{Name: sptr("c"), Type: types.AlgebraicU32},
		{Name: sptr("d"), Type: types.AlgebraicU64},
		{Name: sptr("e"), Type: types.AlgebraicU128},
		{Name: sptr("f"), Type: types.AlgebraicU256},
		{Name: sptr("g"), Type: types.AlgebraicI8},
		{Name: sptr("h"), Type: types.AlgebraicI16},
		{Name: sptr("i"), Type: types.AlgebraicI32},
		{Name: sptr("j"), Type: types.AlgebraicI64},
		{Name: sptr("k"), Type: types.AlgebraicI128},
		{Name: sptr("l"), Type: types.AlgebraicI256},
		{Name: sptr("m"), Type: types.AlgebraicBool},
		{Name: sptr("n"), Type: types.AlgebraicF32},
		{Name: sptr("o"), Type: types.AlgebraicF64},
		{Name: sptr("p"), Type: types.AlgebraicString},
		{Name: sptr("q"), Type: satIdentity},
		{Name: sptr("r"), Type: satConnectionId},
		{Name: sptr("s"), Type: types.AlgebraicTimestamp},
		{Name: sptr("t"), Type: types.AlgebraicTimeDuration},
		{Name: sptr("u"), Type: satUuid},
	},
}

var satEveryVecStruct = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("a"), Type: types.ArrayType{ElemType: types.AlgebraicU8}},
		{Name: sptr("b"), Type: types.ArrayType{ElemType: types.AlgebraicU16}},
		{Name: sptr("c"), Type: types.ArrayType{ElemType: types.AlgebraicU32}},
		{Name: sptr("d"), Type: types.ArrayType{ElemType: types.AlgebraicU64}},
		{Name: sptr("e"), Type: types.ArrayType{ElemType: types.AlgebraicU128}},
		{Name: sptr("f"), Type: types.ArrayType{ElemType: types.AlgebraicU256}},
		{Name: sptr("g"), Type: types.ArrayType{ElemType: types.AlgebraicI8}},
		{Name: sptr("h"), Type: types.ArrayType{ElemType: types.AlgebraicI16}},
		{Name: sptr("i"), Type: types.ArrayType{ElemType: types.AlgebraicI32}},
		{Name: sptr("j"), Type: types.ArrayType{ElemType: types.AlgebraicI64}},
		{Name: sptr("k"), Type: types.ArrayType{ElemType: types.AlgebraicI128}},
		{Name: sptr("l"), Type: types.ArrayType{ElemType: types.AlgebraicI256}},
		{Name: sptr("m"), Type: types.ArrayType{ElemType: types.AlgebraicBool}},
		{Name: sptr("n"), Type: types.ArrayType{ElemType: types.AlgebraicF32}},
		{Name: sptr("o"), Type: types.ArrayType{ElemType: types.AlgebraicF64}},
		{Name: sptr("p"), Type: types.ArrayType{ElemType: types.AlgebraicString}},
		{Name: sptr("q"), Type: types.ArrayType{ElemType: satIdentity}},
		{Name: sptr("r"), Type: types.ArrayType{ElemType: satConnectionId}},
		{Name: sptr("s"), Type: types.ArrayType{ElemType: types.AlgebraicTimestamp}},
		{Name: sptr("t"), Type: types.ArrayType{ElemType: types.AlgebraicTimeDuration}},
		{Name: sptr("u"), Type: types.ArrayType{ElemType: satUuid}},
	},
}

// satOptionI32 is Option<i32>
var satOptionI32 = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("none"), Type: types.ProductType{}},
		{Name: sptr("some"), Type: types.AlgebraicI32},
	},
}

// satOptionString is Option<String>
var satOptionString = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("none"), Type: types.ProductType{}},
		{Name: sptr("some"), Type: types.AlgebraicString},
	},
}

// satOptionIdentity is Option<Identity>
var satOptionIdentity = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("none"), Type: types.ProductType{}},
		{Name: sptr("some"), Type: satIdentity},
	},
}

// satOptionUuid is Option<Uuid>
var satOptionUuid = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("none"), Type: types.ProductType{}},
		{Name: sptr("some"), Type: satUuid},
	},
}

// satOptionSimpleEnum is Option<SimpleEnum>
var satOptionSimpleEnum = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("none"), Type: types.ProductType{}},
		{Name: sptr("some"), Type: satSimpleEnum},
	},
}

// satOptionEveryPrimitiveStruct is Option<EveryPrimitiveStruct>
var satOptionEveryPrimitiveStruct = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("none"), Type: types.ProductType{}},
		{Name: sptr("some"), Type: satEveryPrimitiveStruct},
	},
}

// satOptionI32Inner is Option<i32> used inside Vec
var satOptionI32Inner = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("none"), Type: types.ProductType{}},
		{Name: sptr("some"), Type: types.AlgebraicI32},
	},
}

// satVecOptionI32 is Vec<Option<i32>>
var satVecOptionI32 = types.ArrayType{ElemType: satOptionI32Inner}

// satOptionVecOptionI32 is Option<Vec<Option<i32>>>
var satOptionVecOptionI32 = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("none"), Type: types.ProductType{}},
		{Name: sptr("some"), Type: satVecOptionI32},
	},
}

// Result SATS types: Ok=tag 0, Err=tag 1
var satResultI32String = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("ok"), Type: types.AlgebraicI32},
		{Name: sptr("err"), Type: types.AlgebraicString},
	},
}

var satResultStringI32 = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("ok"), Type: types.AlgebraicString},
		{Name: sptr("err"), Type: types.AlgebraicI32},
	},
}

var satResultIdentityString = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("ok"), Type: satIdentity},
		{Name: sptr("err"), Type: types.AlgebraicString},
	},
}

var satResultSimpleEnumI32 = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("ok"), Type: satSimpleEnum},
		{Name: sptr("err"), Type: types.AlgebraicI32},
	},
}

var satResultEveryPrimitiveStructString = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("ok"), Type: satEveryPrimitiveStruct},
		{Name: sptr("err"), Type: types.AlgebraicString},
	},
}

var satResultVecI32String = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("ok"), Type: types.ArrayType{ElemType: types.AlgebraicI32}},
		{Name: sptr("err"), Type: types.AlgebraicString},
	},
}

// satScheduledTable is the SATS type for the ScheduledTable row used as reducer param.
var satScheduledTable = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("scheduled_id"), Type: types.AlgebraicU64},
		{Name: sptr("scheduled_at"), Type: types.AlgebraicScheduleAt},
		{Name: sptr("text"), Type: types.AlgebraicString},
	},
}
