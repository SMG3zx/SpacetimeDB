package main

import "github.com/clockworklabs/spacetimedb-go/types"

// ── Synthetic SATS types ──────────────────────────────────────────────────────

// satU32U64Str is the shared product type for the u32/u64/String benchmark tables.
var satU32U64Str = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("id"), Type: types.AlgebraicU32},
		{Name: sptr("age"), Type: types.AlgebraicU64},
		{Name: sptr("name"), Type: types.AlgebraicString},
	},
}

// satU32U64U64 is the shared product type for the u32/u64/u64 benchmark tables.
var satU32U64U64 = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("id"), Type: types.AlgebraicU32},
		{Name: sptr("x"), Type: types.AlgebraicU64},
		{Name: sptr("y"), Type: types.AlgebraicU64},
	},
}

// ── Circles SATS types ────────────────────────────────────────────────────────

var satVector2 = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("x"), Type: types.AlgebraicF32},
		{Name: sptr("y"), Type: types.AlgebraicF32},
	},
}

var satEntity = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("id"), Type: types.AlgebraicU32},
		{Name: sptr("position"), Type: satVector2},
		{Name: sptr("mass"), Type: types.AlgebraicU32},
	},
}

var satCircle = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("entity_id"), Type: types.AlgebraicU32},
		{Name: sptr("player_id"), Type: types.AlgebraicU32},
		{Name: sptr("direction"), Type: satVector2},
		{Name: sptr("magnitude"), Type: types.AlgebraicF32},
		{Name: sptr("last_split_time"), Type: types.AlgebraicTimestamp},
	},
}

var satFood = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("entity_id"), Type: types.AlgebraicU32},
	},
}

// ── IA Loop SATS types ────────────────────────────────────────────────────────

var satAgentAction = types.SumType{
	Variants: []types.SumTypeVariant{
		{Name: sptr("Inactive"), Type: types.ProductType{}},
		{Name: sptr("Idle"), Type: types.ProductType{}},
		{Name: sptr("Evading"), Type: types.ProductType{}},
		{Name: sptr("Investigating"), Type: types.ProductType{}},
		{Name: sptr("Retreating"), Type: types.ProductType{}},
		{Name: sptr("Fighting"), Type: types.ProductType{}},
	},
}

var satSmallHexTile = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("x"), Type: types.AlgebraicI32},
		{Name: sptr("z"), Type: types.AlgebraicI32},
		{Name: sptr("dimension"), Type: types.AlgebraicU32},
	},
}

var satVelocity = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("entity_id"), Type: types.AlgebraicU32},
		{Name: sptr("x"), Type: types.AlgebraicF32},
		{Name: sptr("y"), Type: types.AlgebraicF32},
		{Name: sptr("z"), Type: types.AlgebraicF32},
	},
}

var satPosition = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("entity_id"), Type: types.AlgebraicU32},
		{Name: sptr("x"), Type: types.AlgebraicF32},
		{Name: sptr("y"), Type: types.AlgebraicF32},
		{Name: sptr("z"), Type: types.AlgebraicF32},
		{Name: sptr("vx"), Type: types.AlgebraicF32},
		{Name: sptr("vy"), Type: types.AlgebraicF32},
		{Name: sptr("vz"), Type: types.AlgebraicF32},
	},
}

var satGameEnemyAiAgentState = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("entity_id"), Type: types.AlgebraicU64},
		{Name: sptr("last_move_timestamps"), Type: types.ArrayType{ElemType: types.AlgebraicU64}},
		{Name: sptr("next_action_timestamp"), Type: types.AlgebraicU64},
		{Name: sptr("action"), Type: satAgentAction},
	},
}

var satGameTargetableState = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("entity_id"), Type: types.AlgebraicU64},
		{Name: sptr("quad"), Type: types.AlgebraicI64},
	},
}

var satGameLiveTargetableState = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("entity_id"), Type: types.AlgebraicU64},
		{Name: sptr("quad"), Type: types.AlgebraicI64},
	},
}

var satGameMobileEntityState = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("entity_id"), Type: types.AlgebraicU64},
		{Name: sptr("location_x"), Type: types.AlgebraicI32},
		{Name: sptr("location_y"), Type: types.AlgebraicI32},
		{Name: sptr("timestamp"), Type: types.AlgebraicU64},
	},
}

var satGameEnemyState = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("entity_id"), Type: types.AlgebraicU64},
		{Name: sptr("herd_id"), Type: types.AlgebraicI32},
	},
}

var satGameHerdCache = types.ProductType{
	Elements: []types.ProductTypeElement{
		{Name: sptr("id"), Type: types.AlgebraicI32},
		{Name: sptr("dimension_id"), Type: types.AlgebraicU32},
		{Name: sptr("current_population"), Type: types.AlgebraicI32},
		{Name: sptr("location"), Type: satSmallHexTile},
		{Name: sptr("max_population"), Type: types.AlgebraicI32},
		{Name: sptr("spawn_eagerness"), Type: types.AlgebraicF32},
		{Name: sptr("roaming_distance"), Type: types.AlgebraicI32},
	},
}
