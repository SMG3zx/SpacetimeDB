// quickstart-chat demonstrates the SpacetimeDB Go client SDK.
//
// It connects to a local SpacetimeDB instance running the quickstart-chat
// Rust module, subscribes to the User and Message tables, and sends a message.
//
// Usage:
//
//	go run . [host] [db-name]
//
// Defaults: host=localhost:3000, db-name=quickstart-chat
//
// Prerequisites:
//  1. spacetime start
//  2. spacetime publish --server local quickstart-chat ./server/
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/clockworklabs/spacetimedb-go/client"
	"github.com/clockworklabs/spacetimedb-go/examples/quickstart-chat/module_bindings"
)

func main() {
	host := "localhost:3000"
	dbName := "quickstart-chat"
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

	// Register table callbacks before subscribing.
	tables.Message.OnInsert(func(msg module_bindings.Message, _ client.EventContext) {
		fmt.Printf("[message] %s: %s\n", msg.Sender, msg.Text)
	})
	tables.User.OnInsert(func(user module_bindings.User, _ client.EventContext) {
		name := "<anonymous>"
		if user.Name != nil {
			name = *user.Name
		}
		fmt.Printf("[user joined] %s (online=%v)\n", name, user.Online)
	})
	tables.User.OnUpdate(func(old, new module_bindings.User, _ client.EventContext) {
		oldName := "<anonymous>"
		if old.Name != nil {
			oldName = *old.Name
		}
		newName := "<anonymous>"
		if new.Name != nil {
			newName = *new.Name
		}
		fmt.Printf("[user updated] %s -> %s (online=%v)\n", oldName, newName, new.Online)
	})

	errCh := conn.RunAsync(ctx)

	// Subscribe to all rows.
	if _, err := conn.Subscribe([]string{"SELECT * FROM *"}); err != nil {
		fmt.Fprintf(os.Stderr, "subscribe: %v\n", err)
		os.Exit(1)
	}

	// Allow subscription to be applied.
	time.Sleep(500 * time.Millisecond)

	fmt.Printf("Users in cache: %d\n", tables.User.Count())
	fmt.Printf("Messages in cache: %d\n", tables.Message.Count())

	// Set our name.
	if _, err := reducers.SetName("GoUser"); err != nil {
		fmt.Fprintf(os.Stderr, "set_name: %v\n", err)
	}

	// Send a message.
	if _, err := reducers.SendMessage("Hello from Go!"); err != nil {
		fmt.Fprintf(os.Stderr, "send_message: %v\n", err)
	}

	// Allow callbacks to fire.
	time.Sleep(500 * time.Millisecond)

	conn.Disconnect()

	select {
	case err := <-errCh:
		if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
			fmt.Fprintf(os.Stderr, "connection error: %v\n", err)
			os.Exit(1)
		}
	case <-time.After(2 * time.Second):
	}

	fmt.Println("Done.")
}
