package main

import "github.com/clockworklabs/spacetimedb-go/types"

var satUuid = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("__uuid__"), Type: types.AlgebraicU128},
	},
}

var satReturnStruct = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("a"), Type: types.AlgebraicU32},
		{Name: sptr("b"), Type: types.AlgebraicString},
	},
}

var satReturnEnum = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("A"), Type: types.AlgebraicU32},
		{Name: sptr("B"), Type: types.AlgebraicString},
	},
}

var satScheduledProcTable = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("scheduled_id"), Type: types.AlgebraicU64},
		{Name: sptr("scheduled_at"), Type: types.AlgebraicScheduleAt},
		{Name: sptr("reducer_ts"), Type: types.AlgebraicTimestamp},
		{Name: sptr("x"), Type: types.AlgebraicU8},
		{Name: sptr("y"), Type: types.AlgebraicU8},
	},
}
