package main

import (
	spacetimedb "github.com/clockworklabs/spacetimedb-go-server"
	"github.com/clockworklabs/spacetimedb-go/types"
)

// ── Module Registration ──────────────────────────────────────────────────────

func init() {
	// ── Tables ────────────────────────────────────────────────────────────

	// Person: public, PK id (auto_inc u32), name (String), age (u8); BTree "age" on age.
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "Person",
		Columns: []spacetimedb.ColumnDef{
			{Name: "id", Type: types.AlgebraicU32},
			{Name: "name", Type: types.AlgebraicString},
			{Name: "age", Type: types.AlgebraicU8},
		},
		PrimaryKey: []uint16{0},
		Indexes: []spacetimedb.IndexDef{
			{AccessorName: sptr("age"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{2}},
		},
		Sequences: []spacetimedb.SequenceDef{
			{Column: 0, Increment: 1},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// RemoveTable: private, id (u32).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "RemoveTable",
		Columns: []spacetimedb.ColumnDef{
			{Name: "id", Type: types.AlgebraicU32},
		},
		Access: spacetimedb.TableAccessPrivate,
	})

	// TestA: private, x (u32), y (u32), z (String); BTree "foo" on x.
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "TestA",
		Columns: []spacetimedb.ColumnDef{
			{Name: "x", Type: types.AlgebraicU32},
			{Name: "y", Type: types.AlgebraicU32},
			{Name: "z", Type: types.AlgebraicString},
		},
		Indexes: []spacetimedb.IndexDef{
			{AccessorName: sptr("foo"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0}},
		},
		Access: spacetimedb.TableAccessPrivate,
	})

	// TestD: public, test_c (Option<TestC>); default = Some(TestC::Foo) = [1, 0].
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "TestD",
		Columns: []spacetimedb.ColumnDef{
			{Name: "test_c", Type: satOptionTestC},
		},
		Access: spacetimedb.TableAccessPublic,
		DefaultValues: []spacetimedb.ColumnDefaultValue{
			{ColId: 0, Value: []byte{1, 0}}, // some(Foo)
		},
	})

	// TestE: private, PK id (auto_inc u64), name (String); BTree "name" on name.
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "TestE",
		Columns: []spacetimedb.ColumnDef{
			{Name: "id", Type: types.AlgebraicU64},
			{Name: "name", Type: types.AlgebraicString},
		},
		PrimaryKey: []uint16{0},
		Indexes: []spacetimedb.IndexDef{
			{AccessorName: sptr("name"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{1}},
		},
		Sequences: []spacetimedb.SequenceDef{
			{Column: 0, Increment: 1},
		},
		Access: spacetimedb.TableAccessPrivate,
	})

	// TestF (TestFoobar): public, field (Foobar).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "TestF",
		Columns: []spacetimedb.ColumnDef{
			{Name: "field", Type: satFoobar},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// PrivateTable: private, name (String).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "PrivateTable",
		Columns: []spacetimedb.ColumnDef{
			{Name: "name", Type: types.AlgebraicString},
		},
		Access: spacetimedb.TableAccessPrivate,
	})

	// Point: private, x (i64), y (i64); BTree multi-column "multi_column_index" on [x, y].
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "Point",
		Columns: []spacetimedb.ColumnDef{
			{Name: "x", Type: types.AlgebraicI64},
			{Name: "y", Type: types.AlgebraicI64},
		},
		Indexes: []spacetimedb.IndexDef{
			{AccessorName: sptr("multi_column_index"), Algorithm: spacetimedb.IndexAlgorithmBTree, Columns: []uint16{0, 1}},
		},
		Access: spacetimedb.TableAccessPrivate,
	})

	// PkMultiIdentity: private, PK id (u32), unique auto_inc other (u32).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "PkMultiIdentity",
		Columns: []spacetimedb.ColumnDef{
			{Name: "id", Type: types.AlgebraicU32},
			{Name: "other", Type: types.AlgebraicU32},
		},
		PrimaryKey: []uint16{0},
		Constraints: []spacetimedb.ConstraintDef{
			{Columns: []uint16{1}},
		},
		Sequences: []spacetimedb.SequenceDef{
			{Column: 1, Increment: 1},
		},
		Access: spacetimedb.TableAccessPrivate,
	})

	// RepeatingTestArg: private, PK scheduled_id (auto_inc u64), scheduled_at (ScheduleAt),
	// prev_time (Timestamp); scheduled to call repeating_test.
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "RepeatingTestArg",
		Columns: []spacetimedb.ColumnDef{
			{Name: "scheduled_id", Type: types.AlgebraicU64},
			{Name: "scheduled_at", Type: types.AlgebraicScheduleAt},
			{Name: "prev_time", Type: types.AlgebraicTimestamp},
		},
		PrimaryKey: []uint16{0},
		Sequences: []spacetimedb.SequenceDef{
			{Column: 0, Increment: 1},
		},
		Access: spacetimedb.TableAccessPrivate,
	})
	spacetimedb.RegisterScheduleDef(spacetimedb.ScheduleDef{
		TableName:     "RepeatingTestArg",
		ScheduleAtCol: 1,
		ReducerName:   "repeating_test",
	})

	// HasSpecialStuff: private, identity (Identity), connection_id (ConnectionId).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "HasSpecialStuff",
		Columns: []spacetimedb.ColumnDef{
			{Name: "identity", Type: satIdentity},
			{Name: "connection_id", Type: satConnectionId},
		},
		Access: spacetimedb.TableAccessPrivate,
	})

	// Player: public, PK identity (Identity), unique auto_inc player_id (u64), unique name (String).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "Player",
		Columns: []spacetimedb.ColumnDef{
			{Name: "identity", Type: satIdentity},
			{Name: "player_id", Type: types.AlgebraicU64},
			{Name: "name", Type: types.AlgebraicString},
		},
		PrimaryKey: []uint16{0},
		Constraints: []spacetimedb.ConstraintDef{
			{Columns: []uint16{1}},
			{Columns: []uint16{2}},
		},
		Sequences: []spacetimedb.SequenceDef{
			{Column: 1, Increment: 1},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// LoggedOutPlayer: public (same structure as Player).
	spacetimedb.RegisterTableDef(spacetimedb.TableDef{
		Name: "LoggedOutPlayer",
		Columns: []spacetimedb.ColumnDef{
			{Name: "identity", Type: satIdentity},
			{Name: "player_id", Type: types.AlgebraicU64},
			{Name: "name", Type: types.AlgebraicString},
		},
		PrimaryKey: []uint16{0},
		Constraints: []spacetimedb.ConstraintDef{
			{Columns: []uint16{1}},
			{Columns: []uint16{2}},
		},
		Sequences: []spacetimedb.SequenceDef{
			{Column: 1, Increment: 1},
		},
		Access: spacetimedb.TableAccessPublic,
	})

	// ── Reducers ──────────────────────────────────────────────────────────
	// Lifecycle/scheduled reducers are forced private by buildModuleDefBSATN.

	// 0: init — lifecycle Init
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name:       "init",
		Params:     []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityPrivate,
	})
	spacetimedb.RegisterLifecycleDef(spacetimedb.LifecycleDef{
		Kind:    spacetimedb.LifecycleInit,
		Reducer: "init",
	})
	spacetimedb.RegisterReducerHandler(initReducer)

	// 1: repeating_test — scheduled by RepeatingTestArg
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "repeating_test",
		Params: []spacetimedb.ColumnDef{
			{Name: "arg", Type: types.ProductType{
				Elements: []types.ProductTypeElement{
					{Name: sptr("scheduled_id"), Type: types.AlgebraicU64},
					{Name: sptr("scheduled_at"), Type: types.AlgebraicScheduleAt},
					{Name: sptr("prev_time"), Type: types.AlgebraicTimestamp},
				},
			}},
		},
		Visibility: spacetimedb.ReducerVisibilityPrivate,
	})
	spacetimedb.RegisterReducerHandler(repeatingTestReducer)

	// 2: add — insert a Person
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "add",
		Params: []spacetimedb.ColumnDef{
			{Name: "name", Type: types.AlgebraicString},
			{Name: "age", Type: types.AlgebraicU8},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(addReducer)

	// 3: say_hello — log all Person names
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name:       "say_hello",
		Params:     []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(sayHelloReducer)

	// 4: list_over_age — log persons with age >= threshold
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "list_over_age",
		Params: []spacetimedb.ColumnDef{
			{Name: "age", Type: types.AlgebraicU8},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(listOverAgeReducer)

	// 5: log_module_identity — log the module's own identity
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name:       "log_module_identity",
		Params:     []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(logModuleIdentityReducer)

	// 6: test — comprehensive test of tables, indexes, and custom types
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "test",
		Params: []spacetimedb.ColumnDef{
			{Name: "arg", Type: satTestA},
			{Name: "arg2", Type: satTestB},
			{Name: "arg3", Type: satTestC},
			{Name: "arg4", Type: satTestF},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(testReducer)

	// 7: add_player — insert a TestE row
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "add_player",
		Params: []spacetimedb.ColumnDef{
			{Name: "name", Type: types.AlgebraicString},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(addPlayerReducer)

	// 8: delete_player — delete a TestE row by id
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "delete_player",
		Params: []spacetimedb.ColumnDef{
			{Name: "id", Type: types.AlgebraicU64},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(deletePlayerReducer)

	// 9: delete_players_by_name — delete TestE rows by name
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "delete_players_by_name",
		Params: []spacetimedb.ColumnDef{
			{Name: "name", Type: types.AlgebraicString},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(deletePlayersByNameReducer)

	// 10: client_connected — lifecycle OnConnect
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name:       "client_connected",
		Params:     []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityPrivate,
	})
	spacetimedb.RegisterLifecycleDef(spacetimedb.LifecycleDef{
		Kind:    spacetimedb.LifecycleOnConnect,
		Reducer: "client_connected",
	})
	spacetimedb.RegisterReducerHandler(clientConnectedReducer)

	// 11: add_private — insert a PrivateTable row
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name: "add_private",
		Params: []spacetimedb.ColumnDef{
			{Name: "name", Type: types.AlgebraicString},
		},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(addPrivateReducer)

	// 12: query_private — log all PrivateTable rows
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name:       "query_private",
		Params:     []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(queryPrivateReducer)

	// 13: test_btree_index_args — exercises BTree filter/range operations
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name:       "test_btree_index_args",
		Params:     []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(testBtreeIndexArgsReducer)

	// 14: assert_caller_identity_is_module_identity — verify caller == module identity
	spacetimedb.RegisterReducerDef(spacetimedb.ReducerDef{
		Name:       "assert_caller_identity_is_module_identity",
		Params:     []spacetimedb.ColumnDef{},
		Visibility: spacetimedb.ReducerVisibilityClientCallable,
	})
	spacetimedb.RegisterReducerHandler(assertCallerIdentityIsModuleIdentityReducer)

	// ── View ──────────────────────────────────────────────────────────────

	// my_player: public view returning the Player row for the calling identity.
	spacetimedb.RegisterViewDef(spacetimedb.ViewDef{
		Name:        "my_player",
		IsPublic:    true,
		IsAnonymous: false,
		Params:      []spacetimedb.ColumnDef{},
		ReturnType: types.SumType{
			Variants: []types.SumTypeVariant{
				{Name: sptr("none"), Type: types.ProductType{}},
				{Name: sptr("some"), Type: types.ProductType{
					Elements: []types.ProductTypeElement{
						{Name: sptr("identity"), Type: satIdentity},
						{Name: sptr("player_id"), Type: types.AlgebraicU64},
						{Name: sptr("name"), Type: types.AlgebraicString},
					},
				}},
			},
		},
	})
	spacetimedb.RegisterViewHandler(myPlayerView)
}
