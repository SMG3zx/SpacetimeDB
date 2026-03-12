package main

import "github.com/clockworklabs/spacetimedb-go/types"

// ── SATS Type Variables ──────────────────────────────────────────────────────

// satIdentity is the SATS AlgebraicType for types.Identity (U256 wrapped in a product).
var satIdentity = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("__identity__"), Type: types.AlgebraicU256},
	},
}

// satConnectionId is the SATS AlgebraicType for types.ConnectionId (U128 wrapped in a product).
var satConnectionId = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("__connection_id__"), Type: types.AlgebraicU128},
	},
}

// satTestC is the SATS AlgebraicType for the TestC enum (Foo | Bar).
var satTestC = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("Foo"), Type: types.ProductType{}},
		{Name: sptr("Bar"), Type: types.ProductType{}},
	},
}

// satBaz is the SATS AlgebraicType for the Baz struct.
var satBaz = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("field"), Type: types.AlgebraicString},
	},
}

// satFoobar is the SATS AlgebraicType for the Foobar enum (Baz(Baz) | Bar | Har(u32)).
var satFoobar = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("Baz"), Type: satBaz},
		{Name: sptr("Bar"), Type: types.ProductType{}},
		{Name: sptr("Har"), Type: types.AlgebraicU32},
	},
}

// satTestF is the SATS AlgebraicType for the TestF enum (Foo | Bar | Baz(String)).
var satTestF = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("Foo"), Type: types.ProductType{}},
		{Name: sptr("Bar"), Type: types.ProductType{}},
		{Name: sptr("Baz"), Type: types.AlgebraicString},
	},
}

// satOptionTestC is the SATS AlgebraicType for Option<TestC>.
var satOptionTestC = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("none"), Type: types.ProductType{}},
		{Name: sptr("some"), Type: satTestC},
	},
}

// satTestA is the SATS AlgebraicType for the TestA struct (used in reducer params).
var satTestA = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("x"), Type: types.AlgebraicU32},
		{Name: sptr("y"), Type: types.AlgebraicU32},
		{Name: sptr("z"), Type: types.AlgebraicString},
	},
}

// satTestB is the SATS AlgebraicType for the TestB struct (used in reducer params).
var satTestB = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("foo"), Type: types.AlgebraicString},
	},
}
