// regression-tests/client exercises the SpacetimeDB Go client SDK
// against the sdk-test Rust module.
//
// It connects to a local SpacetimeDB instance, subscribes to a subset of
// tables, calls reducers to insert rows, and verifies that the expected
// OnInsert/OnUpdate/OnDelete callbacks fire.
//
// Usage:
//
//	go run . [host] [db-name]
//
// Defaults: host=localhost:3000, db-name=sdk-test
//
// Prerequisites:
//  1. spacetime start
//  2. spacetime publish --server local sdk-test ./modules/sdk-test/
package main

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"github.com/clockworklabs/spacetimedb-go/client"
	"github.com/clockworklabs/spacetimedb-go/examples/regression-tests/client/module_bindings"
)

func main() {
	host := "localhost:3000"
	dbName := "sdk-test"
	if len(os.Args) > 1 {
		host = os.Args[1]
	}
	if len(os.Args) > 2 {
		dbName = os.Args[2]
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := client.NewDbConnectionBuilder().
		WithUri(host).
		WithModuleName(dbName).
		Build(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "connect: %v\n", err)
		os.Exit(1)
	}
	defer conn.Disconnect()

	tables := module_bindings.NewRemoteTables()
	reducers := module_bindings.NewRemoteReducers(conn)
	module_bindings.RegisterTables(conn, tables)

	// Track callbacks.
	var (
		u8Inserts      atomic.Int32
		u32Inserts     atomic.Int32
		stringInserts  atomic.Int32
		boolInserts    atomic.Int32
		pkU32Inserts   atomic.Int32
		pkU32Updates   atomic.Int32
		pkU32Deletes   atomic.Int32
	)

	tables.OneU8.OnInsert(func(row module_bindings.OneU8, _ client.EventContext) {
		u8Inserts.Add(1)
		fmt.Printf("[one_u8] insert: n=%d\n", row.N)
	})

	tables.OneU32.OnInsert(func(row module_bindings.OneU32, _ client.EventContext) {
		u32Inserts.Add(1)
		fmt.Printf("[one_u32] insert: n=%d\n", row.N)
	})

	tables.OneString.OnInsert(func(row module_bindings.OneString, _ client.EventContext) {
		stringInserts.Add(1)
		fmt.Printf("[one_string] insert: s=%q\n", row.S)
	})

	tables.OneBool.OnInsert(func(row module_bindings.OneBool, _ client.EventContext) {
		boolInserts.Add(1)
		fmt.Printf("[one_bool] insert: b=%v\n", row.B)
	})

	tables.OneSimpleEnum.OnInsert(func(row module_bindings.OneSimpleEnum, _ client.EventContext) {
		fmt.Printf("[one_simple_enum] insert: e=%d\n", row.E)
	})

	tables.VecU32.OnInsert(func(row module_bindings.VecU32, _ client.EventContext) {
		fmt.Printf("[vec_u32] insert: n=%v\n", row.N)
	})

	tables.OptionI32.OnInsert(func(row module_bindings.OptionI32, _ client.EventContext) {
		if row.N == nil {
			fmt.Printf("[option_i32] insert: n=nil\n")
		} else {
			fmt.Printf("[option_i32] insert: n=%d\n", *row.N)
		}
	})

	tables.PkU32.OnInsert(func(row module_bindings.PkU32, _ client.EventContext) {
		pkU32Inserts.Add(1)
		fmt.Printf("[pk_u32] insert: n=%d data=%d\n", row.N, row.Data)
	})

	tables.PkU32.OnUpdate(func(old, new module_bindings.PkU32, _ client.EventContext) {
		pkU32Updates.Add(1)
		fmt.Printf("[pk_u32] update: n=%d data=%d->%d\n", new.N, old.Data, new.Data)
	})

	tables.PkU32.OnDelete(func(row module_bindings.PkU32, _ client.EventContext) {
		pkU32Deletes.Add(1)
		fmt.Printf("[pk_u32] delete: n=%d data=%d\n", row.N, row.Data)
	})

	tables.UniqueU32.OnInsert(func(row module_bindings.UniqueU32, _ client.EventContext) {
		fmt.Printf("[unique_u32] insert: n=%d data=%d\n", row.N, row.Data)
	})

	errCh := conn.RunAsync(ctx)

	// Subscribe to the tables we care about.
	if _, err := conn.Subscribe([]string{
		"SELECT * FROM one_u_8",
		"SELECT * FROM one_u_32",
		"SELECT * FROM one_u_64",
		"SELECT * FROM one_string",
		"SELECT * FROM one_bool",
		"SELECT * FROM one_simple_enum",
		"SELECT * FROM vec_u_32",
		"SELECT * FROM option_i_32",
		"SELECT * FROM pk_u_32",
		"SELECT * FROM unique_u_32",
	}); err != nil {
		fmt.Fprintf(os.Stderr, "subscribe: %v\n", err)
		os.Exit(1)
	}

	// Allow subscription to be applied.
	time.Sleep(500 * time.Millisecond)

	fmt.Printf("Initial state: one_u8=%d one_u32=%d pk_u32=%d\n",
		tables.OneU8.Count(), tables.OneU32.Count(), tables.PkU32.Count())

	// --- Test: insert primitives ---
	if _, err := reducers.InsertOneU8(42); err != nil {
		fmt.Fprintf(os.Stderr, "insert_one_u8: %v\n", err)
	}
	if _, err := reducers.InsertOneU32(12345); err != nil {
		fmt.Fprintf(os.Stderr, "insert_one_u32: %v\n", err)
	}
	if _, err := reducers.InsertOneString("hello from Go"); err != nil {
		fmt.Fprintf(os.Stderr, "insert_one_string: %v\n", err)
	}
	if _, err := reducers.InsertOneBool(true); err != nil {
		fmt.Fprintf(os.Stderr, "insert_one_bool: %v\n", err)
	}

	// --- Test: insert enum ---
	if _, err := reducers.InsertOneSimpleEnum(module_bindings.SimpleEnumOne); err != nil {
		fmt.Fprintf(os.Stderr, "insert_one_simple_enum: %v\n", err)
	}

	// --- Test: insert vec ---
	if _, err := reducers.InsertVecU32([]uint32{1, 2, 3}); err != nil {
		fmt.Fprintf(os.Stderr, "insert_vec_u32: %v\n", err)
	}

	// --- Test: insert option ---
	v := int32(99)
	if _, err := reducers.InsertOptionI32(&v); err != nil {
		fmt.Fprintf(os.Stderr, "insert_option_i32 some: %v\n", err)
	}
	if _, err := reducers.InsertOptionI32(nil); err != nil {
		fmt.Fprintf(os.Stderr, "insert_option_i32 none: %v\n", err)
	}

	// --- Test: primary key insert/update/delete ---
	if _, err := reducers.InsertPkU32(1, 100); err != nil {
		fmt.Fprintf(os.Stderr, "insert_pk_u32: %v\n", err)
	}
	if _, err := reducers.UpdatePkU32(1, 200); err != nil {
		fmt.Fprintf(os.Stderr, "update_pk_u32: %v\n", err)
	}
	if _, err := reducers.DeletePkU32(1); err != nil {
		fmt.Fprintf(os.Stderr, "delete_pk_u32: %v\n", err)
	}

	// --- Test: unique index insert/update/delete ---
	if _, err := reducers.InsertUniqueU32(10, 1000); err != nil {
		fmt.Fprintf(os.Stderr, "insert_unique_u32: %v\n", err)
	}
	if _, err := reducers.UpdateUniqueU32(10, 2000); err != nil {
		fmt.Fprintf(os.Stderr, "update_unique_u32: %v\n", err)
	}
	if _, err := reducers.DeleteUniqueU32(10); err != nil {
		fmt.Fprintf(os.Stderr, "delete_unique_u32: %v\n", err)
	}

	// --- Test: no-op reducer ---
	if _, err := reducers.NoOpSucceeds(); err != nil {
		fmt.Fprintf(os.Stderr, "no_op_succeeds: %v\n", err)
	}

	// Allow callbacks to fire.
	time.Sleep(1 * time.Second)

	// Print summary.
	fmt.Printf("\n--- Callback summary ---\n")
	fmt.Printf("one_u8 inserts:    %d (expected 1)\n", u8Inserts.Load())
	fmt.Printf("one_u32 inserts:   %d (expected 1)\n", u32Inserts.Load())
	fmt.Printf("one_string inserts:%d (expected 1)\n", stringInserts.Load())
	fmt.Printf("one_bool inserts:  %d (expected 1)\n", boolInserts.Load())
	fmt.Printf("pk_u32 inserts:    %d (expected 1)\n", pkU32Inserts.Load())
	fmt.Printf("pk_u32 updates:    %d (expected 1)\n", pkU32Updates.Load())
	fmt.Printf("pk_u32 deletes:    %d (expected 1)\n", pkU32Deletes.Load())

	// Verify expected counts.
	failed := false
	check := func(name string, got, want int32) {
		if got != want {
			fmt.Fprintf(os.Stderr, "FAIL %s: got %d, want %d\n", name, got, want)
			failed = true
		}
	}
	check("one_u8 inserts", u8Inserts.Load(), 1)
	check("one_u32 inserts", u32Inserts.Load(), 1)
	check("one_string inserts", stringInserts.Load(), 1)
	check("one_bool inserts", boolInserts.Load(), 1)
	check("pk_u32 inserts", pkU32Inserts.Load(), 1)
	check("pk_u32 updates", pkU32Updates.Load(), 1)
	check("pk_u32 deletes", pkU32Deletes.Load(), 1)

	conn.Disconnect()

	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			fmt.Fprintf(os.Stderr, "connection error: %v\n", err)
			os.Exit(1)
		}
	case <-time.After(2 * time.Second):
	}

	if failed {
		fmt.Fprintln(os.Stderr, "Some checks failed.")
		os.Exit(1)
	}
	fmt.Println("All checks passed.")
}
