package main

import "github.com/clockworklabs/spacetimedb-go/types"

var satIdentity = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("__identity__"), Type: types.AlgebraicU256},
	},
}

// satPlayer is the SATS product type for the Player table.
var satPlayer = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("entity_id"), Type: types.AlgebraicU64},
		{Name: sptr("identity"), Type: satIdentity},
	},
}

// satPlayerLevel is the SATS product type for the PlayerLevel table.
var satPlayerLevel = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("entity_id"), Type: types.AlgebraicU64},
		{Name: sptr("level"), Type: types.AlgebraicU64},
	},
}

// satPlayerLocation is the SATS product type for the PlayerLocation table.
var satPlayerLocation = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("entity_id"), Type: types.AlgebraicU64},
		{Name: sptr("active"), Type: types.AlgebraicBool},
		{Name: sptr("x"), Type: types.AlgebraicI32},
		{Name: sptr("y"), Type: types.AlgebraicI32},
	},
}

// satPlayerAndLevel is the SATS product type for the PlayerAndLevel view type.
var satPlayerAndLevel = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("entity_id"), Type: types.AlgebraicU64},
		{Name: sptr("identity"), Type: satIdentity},
		{Name: sptr("level"), Type: types.AlgebraicU64},
	},
}
