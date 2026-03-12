package main

import (
	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go/types"
)

// numTables is the number of tables registered below.
// Custom types in the typespace are indexed starting at this offset.
const numTables = 17

// Pre-computed RefType references for custom types in the typespace.
// These are used in column defs and reducer params so the validator
// sees Ref types instead of inline product/sum types.
var (
	refVector2     = types.RefType{Ref: numTables + 0} // 17
	refAgentAction = types.RefType{Ref: numTables + 1} // 18
	refSmallHexTile = types.RefType{Ref: numTables + 2} // 19
	refU32U64Str   = types.RefType{Ref: numTables + 3} // 20
	refU32U64U64   = types.RefType{Ref: numTables + 4} // 21
)

func init() {
	// ── Synthetic Tables ───────────────────────────────────────────────────

	// unique_0_u32_u64_str: PK id (u32).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "unique_0_u32_u64_str",
		Columns: []spacetimedb.ColumnDef{
			{Name: "id", Type: types.AlgebraicU32},
			{Name: "age", Type: types.AlgebraicU64},
			{Name: "name", Type: types.AlgebraicString},
		},
		PrimaryKey: []uint16{0},
		Indexes: []spacetimedb.IndexDef{
			{SourceName: sptr("unique_0_u32_u64_str_id_idx_btree"), AccessorName: sptr("id"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
		},
		Constraints: []spacetimedb.ConstraintDef{
			{SourceName: sptr("unique_0_u32_u64_str_id_unique"), Columns: []uint16{0}},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// no_index_u32_u64_str: no indexes.
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "no_index_u32_u64_str",
		Columns: []spacetimedb.ColumnDef{
			{Name: "id", Type: types.AlgebraicU32},
			{Name: "age", Type: types.AlgebraicU64},
			{Name: "name", Type: types.AlgebraicString},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// btree_each_column_u32_u64_str: BTree on id, age, name.
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "btree_each_column_u32_u64_str",
		Columns: []spacetimedb.ColumnDef{
			{Name: "id", Type: types.AlgebraicU32},
			{Name: "age", Type: types.AlgebraicU64},
			{Name: "name", Type: types.AlgebraicString},
		},
		Indexes: []spacetimedb.IndexDef{
			{SourceName: sptr("btree_each_column_u32_u64_str_id_idx_btree"), AccessorName: sptr("id"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
			{SourceName: sptr("btree_each_column_u32_u64_str_age_idx_btree"), AccessorName: sptr("age"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{1}},
			{SourceName: sptr("btree_each_column_u32_u64_str_name_idx_btree"), AccessorName: sptr("name"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{2}},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// unique_0_u32_u64_u64: PK id (u32).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "unique_0_u32_u64_u64",
		Columns: []spacetimedb.ColumnDef{
			{Name: "id", Type: types.AlgebraicU32},
			{Name: "x", Type: types.AlgebraicU64},
			{Name: "y", Type: types.AlgebraicU64},
		},
		PrimaryKey: []uint16{0},
		Indexes: []spacetimedb.IndexDef{
			{SourceName: sptr("unique_0_u32_u64_u64_id_idx_btree"), AccessorName: sptr("id"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
		},
		Constraints: []spacetimedb.ConstraintDef{
			{SourceName: sptr("unique_0_u32_u64_u64_id_unique"), Columns: []uint16{0}},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// no_index_u32_u64_u64: no indexes.
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "no_index_u32_u64_u64",
		Columns: []spacetimedb.ColumnDef{
			{Name: "id", Type: types.AlgebraicU32},
			{Name: "x", Type: types.AlgebraicU64},
			{Name: "y", Type: types.AlgebraicU64},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// btree_each_column_u32_u64_u64: BTree on id, x, y.
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "btree_each_column_u32_u64_u64",
		Columns: []spacetimedb.ColumnDef{
			{Name: "id", Type: types.AlgebraicU32},
			{Name: "x", Type: types.AlgebraicU64},
			{Name: "y", Type: types.AlgebraicU64},
		},
		Indexes: []spacetimedb.IndexDef{
			{SourceName: sptr("btree_each_column_u32_u64_u64_id_idx_btree"), AccessorName: sptr("id"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
			{SourceName: sptr("btree_each_column_u32_u64_u64_x_idx_btree"), AccessorName: sptr("x"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{1}},
			{SourceName: sptr("btree_each_column_u32_u64_u64_y_idx_btree"), AccessorName: sptr("y"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{2}},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// ── Circles Tables ─────────────────────────────────────────────────────

	// Entity: PK id (auto_inc u32), position (Vector2), mass (u32).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "Entity",
		Columns: []spacetimedb.ColumnDef{
			{Name: "id", Type: types.AlgebraicU32},
			{Name: "position", Type: refVector2},
			{Name: "mass", Type: types.AlgebraicU32},
		},
		PrimaryKey: []uint16{0},
		Indexes: []spacetimedb.IndexDef{
			{SourceName: sptr("Entity_id_idx_btree"), AccessorName: sptr("id"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
		},
		Constraints: []spacetimedb.ConstraintDef{
			{SourceName: sptr("Entity_id_unique"), Columns: []uint16{0}},
		},
		Sequences: []spacetimedb.SequenceDef{
			{Column: 0, Increment: 1},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// Circle: PK entity_id (u32), BTree player_id (u32).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "Circle",
		Columns: []spacetimedb.ColumnDef{
			{Name: "entity_id", Type: types.AlgebraicU32},
			{Name: "player_id", Type: types.AlgebraicU32},
			{Name: "direction", Type: refVector2},
			{Name: "magnitude", Type: types.AlgebraicF32},
			{Name: "last_split_time", Type: types.AlgebraicTimestamp},
		},
		PrimaryKey: []uint16{0},
		Indexes: []spacetimedb.IndexDef{
			{SourceName: sptr("Circle_entity_id_idx_btree"), AccessorName: sptr("entity_id"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
			{SourceName: sptr("Circle_player_id_idx_btree"), AccessorName: sptr("player_id"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{1}},
		},
		Constraints: []spacetimedb.ConstraintDef{
			{SourceName: sptr("Circle_entity_id_unique"), Columns: []uint16{0}},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// Food: PK entity_id (u32).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "Food",
		Columns: []spacetimedb.ColumnDef{
			{Name: "entity_id", Type: types.AlgebraicU32},
		},
		PrimaryKey: []uint16{0},
		Indexes: []spacetimedb.IndexDef{
			{SourceName: sptr("Food_entity_id_idx_btree"), AccessorName: sptr("entity_id"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
		},
		Constraints: []spacetimedb.ConstraintDef{
			{SourceName: sptr("Food_entity_id_unique"), Columns: []uint16{0}},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// ── IA Loop Tables ─────────────────────────────────────────────────────

	// Velocity: PK entity_id (u32).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "Velocity",
		Columns: []spacetimedb.ColumnDef{
			{Name: "entity_id", Type: types.AlgebraicU32},
			{Name: "x", Type: types.AlgebraicF32},
			{Name: "y", Type: types.AlgebraicF32},
			{Name: "z", Type: types.AlgebraicF32},
		},
		PrimaryKey: []uint16{0},
		Indexes: []spacetimedb.IndexDef{
			{SourceName: sptr("Velocity_entity_id_idx_btree"), AccessorName: sptr("entity_id"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
		},
		Constraints: []spacetimedb.ConstraintDef{
			{SourceName: sptr("Velocity_entity_id_unique"), Columns: []uint16{0}},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// Position: PK entity_id (u32).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "Position",
		Columns: []spacetimedb.ColumnDef{
			{Name: "entity_id", Type: types.AlgebraicU32},
			{Name: "x", Type: types.AlgebraicF32},
			{Name: "y", Type: types.AlgebraicF32},
			{Name: "z", Type: types.AlgebraicF32},
			{Name: "vx", Type: types.AlgebraicF32},
			{Name: "vy", Type: types.AlgebraicF32},
			{Name: "vz", Type: types.AlgebraicF32},
		},
		PrimaryKey: []uint16{0},
		Indexes: []spacetimedb.IndexDef{
			{SourceName: sptr("Position_entity_id_idx_btree"), AccessorName: sptr("entity_id"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
		},
		Constraints: []spacetimedb.ConstraintDef{
			{SourceName: sptr("Position_entity_id_unique"), Columns: []uint16{0}},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// GameEnemyAiAgentState: PK entity_id (u64).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "GameEnemyAiAgentState",
		Columns: []spacetimedb.ColumnDef{
			{Name: "entity_id", Type: types.AlgebraicU64},
			{Name: "last_move_timestamps", Type: types.ArrayType{ElemType: types.AlgebraicU64}},
			{Name: "next_action_timestamp", Type: types.AlgebraicU64},
			{Name: "action", Type: refAgentAction},
		},
		PrimaryKey: []uint16{0},
		Indexes: []spacetimedb.IndexDef{
			{SourceName: sptr("GameEnemyAiAgentState_entity_id_idx_btree"), AccessorName: sptr("entity_id"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
		},
		Constraints: []spacetimedb.ConstraintDef{
			{SourceName: sptr("GameEnemyAiAgentState_entity_id_unique"), Columns: []uint16{0}},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// GameTargetableState: PK entity_id (u64).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "GameTargetableState",
		Columns: []spacetimedb.ColumnDef{
			{Name: "entity_id", Type: types.AlgebraicU64},
			{Name: "quad", Type: types.AlgebraicI64},
		},
		PrimaryKey: []uint16{0},
		Indexes: []spacetimedb.IndexDef{
			{SourceName: sptr("GameTargetableState_entity_id_idx_btree"), AccessorName: sptr("entity_id"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
		},
		Constraints: []spacetimedb.ConstraintDef{
			{SourceName: sptr("GameTargetableState_entity_id_unique"), Columns: []uint16{0}},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// GameLiveTargetableState: unique entity_id (u64), BTree quad (i64).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "GameLiveTargetableState",
		Columns: []spacetimedb.ColumnDef{
			{Name: "entity_id", Type: types.AlgebraicU64},
			{Name: "quad", Type: types.AlgebraicI64},
		},
		Indexes: []spacetimedb.IndexDef{
			{SourceName: sptr("GameLiveTargetableState_entity_id_idx_btree"), AccessorName: sptr("entity_id"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
			{SourceName: sptr("GameLiveTargetableState_quad_idx_btree"), AccessorName: sptr("quad"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{1}},
		},
		Constraints: []spacetimedb.ConstraintDef{
			{SourceName: sptr("GameLiveTargetableState_entity_id_unique"), Columns: []uint16{0}},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// GameMobileEntityState: PK entity_id (u64), BTree location_x (i32).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "GameMobileEntityState",
		Columns: []spacetimedb.ColumnDef{
			{Name: "entity_id", Type: types.AlgebraicU64},
			{Name: "location_x", Type: types.AlgebraicI32},
			{Name: "location_y", Type: types.AlgebraicI32},
			{Name: "timestamp", Type: types.AlgebraicU64},
		},
		PrimaryKey: []uint16{0},
		Indexes: []spacetimedb.IndexDef{
			{SourceName: sptr("GameMobileEntityState_entity_id_idx_btree"), AccessorName: sptr("entity_id"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
			{SourceName: sptr("GameMobileEntityState_location_x_idx_btree"), AccessorName: sptr("location_x"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{1}},
		},
		Constraints: []spacetimedb.ConstraintDef{
			{SourceName: sptr("GameMobileEntityState_entity_id_unique"), Columns: []uint16{0}},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// GameEnemyState: PK entity_id (u64).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "GameEnemyState",
		Columns: []spacetimedb.ColumnDef{
			{Name: "entity_id", Type: types.AlgebraicU64},
			{Name: "herd_id", Type: types.AlgebraicI32},
		},
		PrimaryKey: []uint16{0},
		Indexes: []spacetimedb.IndexDef{
			{SourceName: sptr("GameEnemyState_entity_id_idx_btree"), AccessorName: sptr("entity_id"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
		},
		Constraints: []spacetimedb.ConstraintDef{
			{SourceName: sptr("GameEnemyState_entity_id_unique"), Columns: []uint16{0}},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// GameHerdCache: PK id (i32).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "GameHerdCache",
		Columns: []spacetimedb.ColumnDef{
			{Name: "id", Type: types.AlgebraicI32},
			{Name: "dimension_id", Type: types.AlgebraicU32},
			{Name: "current_population", Type: types.AlgebraicI32},
			{Name: "location", Type: refSmallHexTile},
			{Name: "max_population", Type: types.AlgebraicI32},
			{Name: "spawn_eagerness", Type: types.AlgebraicF32},
			{Name: "roaming_distance", Type: types.AlgebraicI32},
		},
		PrimaryKey: []uint16{0},
		Indexes: []spacetimedb.IndexDef{
			{SourceName: sptr("GameHerdCache_id_idx_btree"), AccessorName: sptr("id"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
		},
		Constraints: []spacetimedb.ConstraintDef{
			{SourceName: sptr("GameHerdCache_id_unique"), Columns: []uint16{0}},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// ── Register custom types in typespace ────────────────────────────────
	// These must be registered AFTER all table registrations because the
	// typespace indices are offset by numTables.

	_ = spacetimedb.RegisterTypespaceType(satVector2)     // index 17
	_ = spacetimedb.RegisterTypespaceType(satAgentAction)  // index 18
	_ = spacetimedb.RegisterTypespaceType(satSmallHexTile) // index 19
	_ = spacetimedb.RegisterTypespaceType(satU32U64Str)    // index 20
	_ = spacetimedb.RegisterTypespaceType(satU32U64U64)    // index 21

	// ── Register named type exports for client code generation ────────────
	// Table row types (indices 0..numTables-1):
	// Fields not in alphabetical order need CustomOrdering: true.
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "unique_0_u32_u64_str", TypeRef: 0, CustomOrdering: true})       // id, age, name
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "no_index_u32_u64_str", TypeRef: 1, CustomOrdering: true})       // id, age, name
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "btree_each_column_u32_u64_str", TypeRef: 2, CustomOrdering: true}) // id, age, name
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "unique_0_u32_u64_u64", TypeRef: 3})                             // id, x, y (alphabetical)
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "no_index_u32_u64_u64", TypeRef: 4})                             // id, x, y (alphabetical)
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "btree_each_column_u32_u64_u64", TypeRef: 5})                    // id, x, y (alphabetical)
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "Entity", TypeRef: 6, CustomOrdering: true})                     // id, position, mass
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "Circle", TypeRef: 7, CustomOrdering: true})                     // entity_id, player_id, direction, ...
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "Food", TypeRef: 8})                                             // entity_id (single field)
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "Velocity", TypeRef: 9})                                         // entity_id, x, y, z (alphabetical)
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "Position", TypeRef: 10, CustomOrdering: true})                  // entity_id, x, y, z, vx, vy, vz
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "GameEnemyAiAgentState", TypeRef: 11, CustomOrdering: true})     // entity_id, ..., action
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "GameTargetableState", TypeRef: 12})                             // entity_id, quad (alphabetical)
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "GameLiveTargetableState", TypeRef: 13})                         // entity_id, quad (alphabetical)
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "GameMobileEntityState", TypeRef: 14})                           // entity_id, location_x, location_y, timestamp (alphabetical)
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "GameEnemyState", TypeRef: 15})                                  // entity_id, herd_id (alphabetical)
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "GameHerdCache", TypeRef: 16, CustomOrdering: true})             // id, dimension_id, current_population, ...
	// Custom types:
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "Vector2", TypeRef: numTables + 0})                              // x, y (alphabetical)
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "AgentAction", TypeRef: numTables + 1, CustomOrdering: true})    // Inactive, Idle, Evading, ... (not alphabetical)
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "SmallHexTile", TypeRef: numTables + 2, CustomOrdering: true})   // x, z, dimension (not alphabetical)
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "U32U64Str", TypeRef: numTables + 3, CustomOrdering: true})      // id, age, name (not alphabetical)
	spacetimedb.RegisterTypeDef(spacetimedb.TypeDef{Name: "U32U64U64", TypeRef: numTables + 4})                            // id, x, y (alphabetical)

	// ── Synthetic Reducers ─────────────────────────────────────────────────

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "empty", Params: []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(emptyReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "insert_unique_0_u32_u64_str",
		Params: []spacetimedb.ColumnDef{
			{Name: "id", Type: types.AlgebraicU32},
			{Name: "age", Type: types.AlgebraicU64},
			{Name: "name", Type: types.AlgebraicString},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(insertUnique0U32U64StrReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "insert_no_index_u32_u64_str",
		Params: []spacetimedb.ColumnDef{
			{Name: "id", Type: types.AlgebraicU32},
			{Name: "age", Type: types.AlgebraicU64},
			{Name: "name", Type: types.AlgebraicString},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(insertNoIndexU32U64StrReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "insert_btree_each_column_u32_u64_str",
		Params: []spacetimedb.ColumnDef{
			{Name: "id", Type: types.AlgebraicU32},
			{Name: "age", Type: types.AlgebraicU64},
			{Name: "name", Type: types.AlgebraicString},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(insertBtreeEachColumnU32U64StrReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "insert_unique_0_u32_u64_u64",
		Params: []spacetimedb.ColumnDef{
			{Name: "id", Type: types.AlgebraicU32},
			{Name: "x", Type: types.AlgebraicU64},
			{Name: "y", Type: types.AlgebraicU64},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(insertUnique0U32U64U64Reducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "insert_no_index_u32_u64_u64",
		Params: []spacetimedb.ColumnDef{
			{Name: "id", Type: types.AlgebraicU32},
			{Name: "x", Type: types.AlgebraicU64},
			{Name: "y", Type: types.AlgebraicU64},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(insertNoIndexU32U64U64Reducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "insert_btree_each_column_u32_u64_u64",
		Params: []spacetimedb.ColumnDef{
			{Name: "id", Type: types.AlgebraicU32},
			{Name: "x", Type: types.AlgebraicU64},
			{Name: "y", Type: types.AlgebraicU64},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(insertBtreeEachColumnU32U64U64Reducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "insert_bulk_unique_0_u32_u64_u64",
		Params: []spacetimedb.ColumnDef{
			{Name: "locs", Type: types.ArrayType{ElemType: refU32U64U64}},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(insertBulkUnique0U32U64U64Reducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "insert_bulk_no_index_u32_u64_u64",
		Params: []spacetimedb.ColumnDef{
			{Name: "locs", Type: types.ArrayType{ElemType: refU32U64U64}},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(insertBulkNoIndexU32U64U64Reducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "insert_bulk_btree_each_column_u32_u64_u64",
		Params: []spacetimedb.ColumnDef{
			{Name: "locs", Type: types.ArrayType{ElemType: refU32U64U64}},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(insertBulkBtreeEachColumnU32U64U64Reducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "insert_bulk_unique_0_u32_u64_str",
		Params: []spacetimedb.ColumnDef{
			{Name: "people", Type: types.ArrayType{ElemType: refU32U64Str}},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(insertBulkUnique0U32U64StrReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "insert_bulk_no_index_u32_u64_str",
		Params: []spacetimedb.ColumnDef{
			{Name: "people", Type: types.ArrayType{ElemType: refU32U64Str}},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(insertBulkNoIndexU32U64StrReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "insert_bulk_btree_each_column_u32_u64_str",
		Params: []spacetimedb.ColumnDef{
			{Name: "people", Type: types.ArrayType{ElemType: refU32U64Str}},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(insertBulkBtreeEachColumnU32U64StrReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "update_bulk_unique_0_u32_u64_u64",
		Params: []spacetimedb.ColumnDef{
			{Name: "row_count", Type: types.AlgebraicU32},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(updateBulkUnique0U32U64U64Reducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "update_bulk_unique_0_u32_u64_str",
		Params: []spacetimedb.ColumnDef{
			{Name: "row_count", Type: types.AlgebraicU32},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(updateBulkUnique0U32U64StrReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "iterate_unique_0_u32_u64_str", Params: []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(iterateUnique0U32U64StrReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "iterate_unique_0_u32_u64_u64", Params: []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(iterateUnique0U32U64U64Reducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "filter_unique_0_u32_u64_str_by_id",
		Params: []spacetimedb.ColumnDef{{Name: "id", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(filterUnique0U32U64StrByIdReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "filter_no_index_u32_u64_str_by_id",
		Params: []spacetimedb.ColumnDef{{Name: "id", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(filterNoIndexU32U64StrByIdReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "filter_btree_each_column_u32_u64_str_by_id",
		Params: []spacetimedb.ColumnDef{{Name: "id", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(filterBtreeEachColumnU32U64StrByIdReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "filter_unique_0_u32_u64_str_by_name",
		Params: []spacetimedb.ColumnDef{{Name: "name", Type: types.AlgebraicString}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(filterUnique0U32U64StrByNameReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "filter_no_index_u32_u64_str_by_name",
		Params: []spacetimedb.ColumnDef{{Name: "name", Type: types.AlgebraicString}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(filterNoIndexU32U64StrByNameReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "filter_btree_each_column_u32_u64_str_by_name",
		Params: []spacetimedb.ColumnDef{{Name: "name", Type: types.AlgebraicString}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(filterBtreeEachColumnU32U64StrByNameReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "filter_unique_0_u32_u64_u64_by_id",
		Params: []spacetimedb.ColumnDef{{Name: "id", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(filterUnique0U32U64U64ByIdReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "filter_no_index_u32_u64_u64_by_id",
		Params: []spacetimedb.ColumnDef{{Name: "id", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(filterNoIndexU32U64U64ByIdReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "filter_btree_each_column_u32_u64_u64_by_id",
		Params: []spacetimedb.ColumnDef{{Name: "id", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(filterBtreeEachColumnU32U64U64ByIdReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "filter_unique_0_u32_u64_u64_by_x",
		Params: []spacetimedb.ColumnDef{{Name: "x", Type: types.AlgebraicU64}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(filterUnique0U32U64U64ByXReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "filter_no_index_u32_u64_u64_by_x",
		Params: []spacetimedb.ColumnDef{{Name: "x", Type: types.AlgebraicU64}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(filterNoIndexU32U64U64ByXReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "filter_btree_each_column_u32_u64_u64_by_x",
		Params: []spacetimedb.ColumnDef{{Name: "x", Type: types.AlgebraicU64}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(filterBtreeEachColumnU32U64U64ByXReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "filter_unique_0_u32_u64_u64_by_y",
		Params: []spacetimedb.ColumnDef{{Name: "y", Type: types.AlgebraicU64}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(filterUnique0U32U64U64ByYReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "filter_no_index_u32_u64_u64_by_y",
		Params: []spacetimedb.ColumnDef{{Name: "y", Type: types.AlgebraicU64}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(filterNoIndexU32U64U64ByYReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "filter_btree_each_column_u32_u64_u64_by_y",
		Params: []spacetimedb.ColumnDef{{Name: "y", Type: types.AlgebraicU64}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(filterBtreeEachColumnU32U64U64ByYReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "delete_unique_0_u32_u64_str_by_id",
		Params: []spacetimedb.ColumnDef{{Name: "id", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(deleteUnique0U32U64StrByIdReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "delete_unique_0_u32_u64_u64_by_id",
		Params: []spacetimedb.ColumnDef{{Name: "id", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(deleteUnique0U32U64U64ByIdReducer)

	for _, name := range []string{
		"clear_table_unique_0_u32_u64_str",
		"clear_table_no_index_u32_u64_str",
		"clear_table_btree_each_column_u32_u64_str",
		"clear_table_unique_0_u32_u64_u64",
		"clear_table_no_index_u32_u64_u64",
		"clear_table_btree_each_column_u32_u64_u64",
	} {
		spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
			Name: name, Params: []spacetimedb.ColumnDef{},
			Visibility: spacetimedb.ReducerVisibilityClientCallable,
		})
		spacetimedb.RegisterReducerHandler(clearTableUnimplementedReducer)
	}

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "count_unique_0_u32_u64_str", Params: []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(countUnique0U32U64StrReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "count_no_index_u32_u64_str", Params: []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(countNoIndexU32U64StrReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "count_btree_each_column_u32_u64_str", Params: []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(countBtreeEachColumnU32U64StrReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "count_unique_0_u32_u64_u64", Params: []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(countUnique0U32U64U64Reducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "count_no_index_u32_u64_u64", Params: []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(countNoIndexU32U64U64Reducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "count_btree_each_column_u32_u64_u64", Params: []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(countBtreeEachColumnU32U64U64Reducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "fn_with_1_args",
		Params: []spacetimedb.ColumnDef{{Name: "arg", Type: types.AlgebraicString}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(fnWith1ArgsReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "fn_with_32_args",
		Params: []spacetimedb.ColumnDef{
			{Name: "arg1", Type: types.AlgebraicString},
			{Name: "arg2", Type: types.AlgebraicString},
			{Name: "arg3", Type: types.AlgebraicString},
			{Name: "arg4", Type: types.AlgebraicString},
			{Name: "arg5", Type: types.AlgebraicString},
			{Name: "arg6", Type: types.AlgebraicString},
			{Name: "arg7", Type: types.AlgebraicString},
			{Name: "arg8", Type: types.AlgebraicString},
			{Name: "arg9", Type: types.AlgebraicString},
			{Name: "arg10", Type: types.AlgebraicString},
			{Name: "arg11", Type: types.AlgebraicString},
			{Name: "arg12", Type: types.AlgebraicString},
			{Name: "arg13", Type: types.AlgebraicString},
			{Name: "arg14", Type: types.AlgebraicString},
			{Name: "arg15", Type: types.AlgebraicString},
			{Name: "arg16", Type: types.AlgebraicString},
			{Name: "arg17", Type: types.AlgebraicString},
			{Name: "arg18", Type: types.AlgebraicString},
			{Name: "arg19", Type: types.AlgebraicString},
			{Name: "arg20", Type: types.AlgebraicString},
			{Name: "arg21", Type: types.AlgebraicString},
			{Name: "arg22", Type: types.AlgebraicString},
			{Name: "arg23", Type: types.AlgebraicString},
			{Name: "arg24", Type: types.AlgebraicString},
			{Name: "arg25", Type: types.AlgebraicString},
			{Name: "arg26", Type: types.AlgebraicString},
			{Name: "arg27", Type: types.AlgebraicString},
			{Name: "arg28", Type: types.AlgebraicString},
			{Name: "arg29", Type: types.AlgebraicString},
			{Name: "arg30", Type: types.AlgebraicString},
			{Name: "arg31", Type: types.AlgebraicString},
			{Name: "arg32", Type: types.AlgebraicString},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(fnWith32ArgsReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "print_many_things",
		Params: []spacetimedb.ColumnDef{{Name: "n", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(printManyThingsReducer)

	// ── Circles Reducers ───────────────────────────────────────────────────

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "insert_bulk_entity",
		Params: []spacetimedb.ColumnDef{{Name: "count", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(insertBulkEntityReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "insert_bulk_circle",
		Params: []spacetimedb.ColumnDef{{Name: "count", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(insertBulkCircleReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "insert_bulk_food",
		Params: []spacetimedb.ColumnDef{{Name: "count", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(insertBulkFoodReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "cross_join_all",
		Params: []spacetimedb.ColumnDef{{Name: "expected", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(crossJoinAllReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "cross_join_circle_food",
		Params: []spacetimedb.ColumnDef{{Name: "expected", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(crossJoinCircleFoodReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "init_game_circles",
		Params: []spacetimedb.ColumnDef{{Name: "initial_load", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(initGameCirclesReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "run_game_circles",
		Params: []spacetimedb.ColumnDef{{Name: "initial_load", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(runGameCirclesReducer)

	// ── IA Loop Reducers ───────────────────────────────────────────────────

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "insert_bulk_position",
		Params: []spacetimedb.ColumnDef{{Name: "count", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(insertBulkPositionReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "insert_bulk_velocity",
		Params: []spacetimedb.ColumnDef{{Name: "count", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(insertBulkVelocityReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "update_position_all",
		Params: []spacetimedb.ColumnDef{{Name: "expected", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(updatePositionAllReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "update_position_with_velocity",
		Params: []spacetimedb.ColumnDef{{Name: "expected", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(updatePositionWithVelocityReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "insert_world",
		Params: []spacetimedb.ColumnDef{{Name: "players", Type: types.AlgebraicU64}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(insertWorldReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "game_loop_enemy_ia",
		Params: []spacetimedb.ColumnDef{{Name: "players", Type: types.AlgebraicU64}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(gameLoopEnemyIaReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "init_game_ia_loop",
		Params: []spacetimedb.ColumnDef{{Name: "initial_load", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(initGameIaLoopReducer)

	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "run_game_ia_loop",
		Params: []spacetimedb.ColumnDef{{Name: "initial_load", Type: types.AlgebraicU32}},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(runGameIaLoopReducer)
}
