---
title: Go Reference
toc_max_heading_level: 6
slug: /clients/go
---

The SpacetimeDB client SDK for Go provides everything you need to build clients that connect to SpacetimeDB modules, subscribe to real-time data updates, and call reducers. It communicates over WebSocket using the BSATN binary protocol, and maintains an in-memory client cache of subscribed rows.

Before diving into the reference, you may want to review:

- [Generating Client Bindings](./00200-codegen.md) - How to generate Go bindings from your module
- [Connecting to SpacetimeDB](./00300-connection.md) - Establishing and managing connections
- [SDK API Reference](./00400-sdk-api.md) - Core concepts that apply across all SDKs

| Name                                                                  | Description                                                                                          |
| --------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------- |
| [Project setup](#project-setup)                                       | Configure your Go project to use the SpacetimeDB Go client SDK.                                      |
| [Generate module bindings](#generate-module-bindings)                 | Use the SpacetimeDB CLI to generate module-specific types and interfaces.                            |
| [`DbConnection` type](#type-dbconnection)                             | A connection to a remote database, constructed via a builder pattern.                                |
| [`EventContext` type](#type-eventcontext)                             | Context passed to row callbacks (OnInsert, OnDelete, OnUpdate).                                      |
| [`ReducerEventContext` type](#type-reducereventcontext)               | Context passed to reducer callbacks.                                                                 |
| [`SubscriptionEventContext` type](#type-subscriptioneventcontext)     | Context passed to subscription lifecycle callbacks.                                                  |
| [`ErrorContext` type](#type-errorcontext)                             | Context passed to error-related callbacks.                                                           |
| [Access the client cache](#access-the-client-cache)                   | Query subscribed rows locally, and register callbacks when rows change.                              |
| [Observe and invoke reducers](#observe-and-invoke-reducers)           | Send requests to run reducers and register callbacks for reducer results.                            |
| [Subscriptions](#subscriptions)                                       | Subscribe to SQL queries and receive real-time updates.                                              |
| [Identify a client](#identify-a-client)                               | Types for identifying users and client connections.                                                   |
| [Example usage](#example-usage)                                       | A complete working example using the Go client SDK.                                                  |

## Project setup

Create a new Go project and add the SpacetimeDB Go client SDK as a dependency:

```bash
mkdir my-spacetimedb-client
cd my-spacetimedb-client
go mod init my-spacetimedb-client
go get github.com/clockworklabs/spacetimedb-go@latest
```

This pulls in all the sub-packages you will need:

- `github.com/clockworklabs/spacetimedb-go/client` -- connection, cache, subscriptions
- `github.com/clockworklabs/spacetimedb-go/types` -- Identity, ConnectionId, Timestamp
- `github.com/clockworklabs/spacetimedb-go/bsatn` -- BSATN binary codec
- `github.com/clockworklabs/spacetimedb-go/protocol` -- WebSocket v2 message types

Your project layout should look like:

```
my-spacetimedb-client/
  go.mod
  go.sum
  main.go
  module_bindings/
    module_bindings.go
    types.go
```

## Generate module bindings

Each SpacetimeDB client depends on bindings specific to your module. Generate them using the SpacetimeDB CLI:

```bash
mkdir -p module_bindings
spacetime generate --lang go \
    --out-dir ./module_bindings \
    --module-path PATH-TO-MODULE-DIRECTORY
```

This creates Go files in `module_bindings/` containing:

- **`RemoteTables`** -- typed `TableHandle` fields for each table in your module, with `OnInsert`, `OnDelete`, `OnUpdate`, `Count`, and `Iter` methods.
- **`RemoteReducers`** -- typed methods to call each reducer, handling BSATN serialization automatically.
- **Type structs** -- Go structs matching each table's row type, with BSATN encode/decode functions.
- **`RegisterTables`** -- a helper that registers all table handlers with a `DbConnection`.
- **`NewRemoteTables`** / **`NewRemoteReducers`** -- constructors.

Import them in your client code:

```go
import (
    "github.com/clockworklabs/spacetimedb-go/client"
    "my-spacetimedb-client/module_bindings"
)
```

## Type `DbConnection`

A `DbConnection` represents an active WebSocket connection to a SpacetimeDB module. Create one using the builder pattern:

```go
conn, err := client.NewDbConnectionBuilder().
    WithUri("ws://localhost:3000").
    WithModuleName("my_module").
    WithToken(savedToken).
    OnConnect(func(identity types.Identity, connId types.ConnectionId, token string) {
        fmt.Println("Connected with identity:", identity)
        // Save the token for reconnection
        client.SaveToken("localhost:3000", "my_module", token)
    }).
    OnDisconnect(func(err error) {
        if err != nil {
            fmt.Println("Disconnected with error:", err)
        }
    }).
    OnConnectError(func(err error) {
        fmt.Println("Connection failed:", err)
    }).
    Build(ctx)
```

### Builder methods

| Method | Signature | Description |
| --- | --- | --- |
| `WithUri` | `WithUri(uri string) *DbConnectionBuilder` | Set the server URI (e.g. `"ws://localhost:3000"` or `"wss://example.com"`). Required. |
| `WithModuleName` | `WithModuleName(name string) *DbConnectionBuilder` | Set the module/database name. Required. |
| `WithToken` | `WithToken(token string) *DbConnectionBuilder` | Set the authentication token for reconnection. Optional. |
| `WithCompression` | `WithCompression(c protocol.Compression) *DbConnectionBuilder` | Set the compression algorithm (default: Brotli). Optional. |
| `OnConnect` | `OnConnect(fn func(Identity, ConnectionId, string)) *DbConnectionBuilder` | Callback fired after the initial connection handshake. |
| `OnDisconnect` | `OnDisconnect(fn func(error)) *DbConnectionBuilder` | Callback fired when the connection closes. |
| `OnConnectError` | `OnConnectError(fn func(error)) *DbConnectionBuilder` | Callback fired if the initial connection fails. |
| `Build` | `Build(ctx context.Context) (*DbConnection, error)` | Connect and block until the server acknowledges. Returns the ready connection. |

### Connection methods

| Method | Signature | Description |
| --- | --- | --- |
| `Identity` | `Identity() types.Identity` | Returns the client's identity. |
| `ConnectionId` | `ConnectionId() types.ConnectionId` | Returns the connection identifier. |
| `Token` | `Token() string` | Returns the authentication token from the server. |
| `IsActive` | `IsActive() bool` | Reports whether the connection is open. |
| `Disconnect` | `Disconnect() error` | Closes the WebSocket connection. |
| `RunBlocking` | `RunBlocking(ctx context.Context) error` | Runs the message loop until the connection closes or ctx is cancelled. |
| `RunAsync` | `RunAsync(ctx context.Context) <-chan error` | Starts the message loop in a background goroutine. Returns a channel that receives the terminal error. |
| `AdvanceOneMessage` | `AdvanceOneMessage(ctx context.Context) error` | Processes exactly one incoming message. Blocks until available. |
| `FrameTick` | `FrameTick() error` | Processes all currently queued messages without blocking. Useful in game loops. |
| `CallReducer` | `CallReducer(reducer string, argsBsatn []byte) (uint32, error)` | Sends a raw reducer call. Prefer the typed methods on `RemoteReducers`. |
| `Subscribe` | `Subscribe(queries []string) (uint32, error)` | Sends a subscription request. Prefer `SubscriptionBuilder`. |
| `OneOffQuery` | `OneOffQuery(ctx context.Context, query string) (*QueryResult, error)` | Executes a SQL query without creating a subscription. |

### Message loop

After building a connection, you must run the message loop to process incoming server messages (subscription updates, reducer results, etc.). Choose one of:

```go
// Option 1: Block the current goroutine
err := conn.RunBlocking(ctx)

// Option 2: Run in background
errCh := conn.RunAsync(ctx)
// ... do other work ...
err := <-errCh

// Option 3: Manual per-frame (game loops)
for conn.IsActive() {
    conn.FrameTick()
    // ... render frame ...
}
```

## Type `EventContext`

`EventContext` is passed to row callbacks (`OnInsert`, `OnDelete`, `OnUpdate`). It provides access to the client's identity and, when the change was triggered by a reducer, includes a `ReducerEvent` with metadata.

```go
type EventContext struct {
    Identity     types.Identity
    ConnectionId types.ConnectionId
    Db           any            // typed as *RemoteTables in codegen
    Reducers     any            // typed as *RemoteReducers in codegen
    Event        *ReducerEvent  // nil for subscription-initiated inserts
}
```

The `ReducerEvent` struct:

```go
type ReducerEvent struct {
    Timestamp          types.Timestamp
    CallerIdentity     types.Identity
    CallerConnectionId types.ConnectionId
    ReducerName        string
    Status             ReducerStatus  // Committed, Failed, or OutOfEnergy
    EnergyQuanta       int64
}
```

## Type `ReducerEventContext`

`ReducerEventContext` is passed to reducer-specific callbacks. It embeds `EventContext` and adds reducer-specific fields:

```go
type ReducerEventContext struct {
    EventContext
    ReducerName string
    Status      ReducerStatus
    Timestamp   types.Timestamp
}
```

`ReducerStatus` values:

| Constant | Description |
| --- | --- |
| `ReducerStatusCommitted` | The reducer committed successfully. |
| `ReducerStatusFailed` | The reducer returned an error. |
| `ReducerStatusOutOfEnergy` | The reducer ran out of energy. |

## Type `SubscriptionEventContext`

`SubscriptionEventContext` is passed to subscription lifecycle callbacks:

```go
type SubscriptionEventContext struct {
    Identity     types.Identity
    ConnectionId types.ConnectionId
    Db           any
    Reducers     any
    QuerySetId   uint32
}
```

## Type `ErrorContext`

`ErrorContext` is passed to error callbacks. Identity and ConnectionId may be nil if the error occurs before those are established:

```go
type ErrorContext struct {
    Identity     *types.Identity
    ConnectionId *types.ConnectionId
    Err          error
}
```

## Access the client cache

The client SDK maintains an in-memory cache of all rows matching your active subscriptions. Generated bindings provide a `TableHandle` for each table with typed access.

### Set up tables

```go
tables := module_bindings.NewRemoteTables()
reducers := module_bindings.NewRemoteReducers(conn)
module_bindings.RegisterTables(conn, tables)
```

### Count rows

```go
count := tables.User.Count()
fmt.Printf("There are %d users\n", count)
```

### Iterate over rows

`Iter` returns a Go iterator (`iter.Seq[Row]`) over all cached rows:

```go
for user := range tables.User.Iter() {
    fmt.Printf("User: %s (online=%v)\n", user.Name, user.Online)
}
```

### Callback `OnInsert`

Register a callback that fires whenever a new row is inserted into the client cache:

```go
callbackId := tables.User.OnInsert(func(user module_bindings.User, ctx client.EventContext) {
    fmt.Printf("New user: %s\n", user.Name)
})
```

The returned `CallbackId` can be used to remove the callback later:

```go
tables.User.Callbacks.OnInsert.Remove(callbackId)
```

### Callback `OnDelete`

Register a callback that fires whenever a row is removed from the client cache:

```go
tables.User.OnDelete(func(user module_bindings.User, ctx client.EventContext) {
    fmt.Printf("User removed: %s\n", user.Name)
})
```

### Callback `OnUpdate`

Register a callback that fires when a row with a primary key is updated (old row deleted, new row inserted):

```go
tables.User.OnUpdate(func(old, new module_bindings.User, ctx client.EventContext) {
    fmt.Printf("User %s updated: online %v -> %v\n", old.Name, old.Online, new.Online)
})
```

### Unique index `Find`

If your table has a unique column, the generated bindings include a `UniqueIndex` that allows O(1) lookup:

```go
idx := client.NewUniqueIndex[module_bindings.User, types.Identity](
    func(u *module_bindings.User) types.Identity { return u.Identity },
)

// Find a user by identity
user, ok := idx.Find(someIdentity)
if ok {
    fmt.Printf("Found user: %s\n", user.Name)
}
```

## Observe and invoke reducers

### Call a reducer

Generated `RemoteReducers` provide typed methods for each reducer. They serialize arguments to BSATN and send the call over the WebSocket:

```go
reducers := module_bindings.NewRemoteReducers(conn)

// Call a reducer with arguments
reqId, err := reducers.SendMessage("Hello from Go!")
if err != nil {
    log.Fatal("send_message failed:", err)
}

// Call a reducer with no arguments
reqId, err = reducers.SayHello()
```

Each method returns a `requestId` (uint32) and an error. The `requestId` can be used to correlate with reducer result notifications.

### Observe a reducer (low-level)

For raw reducer calls without generated bindings, use `CallReducer` directly:

```go
w := bsatn.NewWriter()
w.WriteString("Hello!")
reqId, err := conn.CallReducer("send_message", w.Bytes())
```

## Subscriptions

Subscriptions tell the server which table rows to replicate to the client cache. When subscribed rows change, the server pushes delta updates and the SDK fires the appropriate row callbacks.

### SubscriptionBuilder

Use `SubscriptionBuilder` for a fluent interface:

```go
sub, err := client.NewSubscriptionBuilder(conn).
    OnApplied(func(querySetId uint32) {
        fmt.Println("Subscription applied, querySetId:", querySetId)
        fmt.Printf("Users in cache: %d\n", tables.User.Count())
    }).
    OnError(func(querySetId uint32, errMsg string) {
        fmt.Println("Subscription error:", errMsg)
    }).
    Subscribe([]string{
        "SELECT * FROM user WHERE online = true",
        "SELECT * FROM message",
    })
if err != nil {
    log.Fatal("subscribe failed:", err)
}
```

### Subscribe to all tables

A convenience method subscribes to all rows in all tables:

```go
sub, err := client.NewSubscriptionBuilder(conn).
    OnApplied(func(querySetId uint32) {
        fmt.Println("All tables subscribed")
    }).
    SubscribeToAllTables()
```

This is equivalent to `Subscribe([]string{"SELECT * FROM *"})`.

### SubscriptionHandle

The `Subscribe` and `SubscribeToAllTables` methods return a `*SubscriptionHandle`:

| Method | Signature | Description |
| --- | --- | --- |
| `QuerySetId` | `QuerySetId() uint32` | Returns the query set identifier. |
| `IsActive` | `IsActive() bool` | Reports whether the subscription is in the Applied state. |
| `IsEnded` | `IsEnded() bool` | Reports whether the subscription has ended. |
| `Unsubscribe` | `Unsubscribe()` | Ends the subscription. |
| `UnsubscribeThen` | `UnsubscribeThen(onEnded func())` | Sets a callback and then ends the subscription. |

### Unsubscribe

```go
sub.Unsubscribe()

// Or with a callback
sub.UnsubscribeThen(func() {
    fmt.Println("Unsubscribed successfully")
})
```

## Identify a client

### `Identity`

`Identity` is a 256-bit (32-byte) unique identifier for a SpacetimeDB user. It persists across connections when the same token is used.

```go
type Identity [32]byte
```

| Method | Description |
| --- | --- |
| `IsZero() bool` | Reports whether the Identity is the zero value. |
| `Bytes() []byte` | Returns the raw 32-byte representation. |
| `String() string` | Returns the identity as a hex string. |

Create from bytes:

```go
id, err := types.IdentityFromBytes(rawBytes)
```

### `ConnectionId`

`ConnectionId` is a 128-bit (16-byte) identifier for a specific client connection. A new `ConnectionId` is assigned each time a client connects.

```go
type ConnectionId [16]byte
```

| Method | Description |
| --- | --- |
| `IsZero() bool` | Reports whether the ConnectionId is the zero value. |
| `Bytes() []byte` | Returns the raw 16-byte representation. |
| `String() string` | Returns the ConnectionId as a hex string. |

### `Timestamp`

`Timestamp` represents a point in time as microseconds since the Unix epoch.

```go
type Timestamp struct {
    Microseconds int64
}
```

| Method | Description |
| --- | --- |
| `ToTime() time.Time` | Converts to a Go `time.Time` (UTC). |
| `String() string` | Formats as RFC3339. |

Create from `time.Time`:

```go
ts := types.TimestampFromTime(time.Now())
```

### Token persistence

The SDK provides helpers to save and load authentication tokens to disk:

```go
// Save after connecting
client.SaveToken("localhost:3000", "my_module", conn.Token())

// Load before connecting
token, err := client.LoadToken("localhost:3000", "my_module")
if err != nil {
    log.Fatal(err)
}
```

Tokens are stored in `~/.spacetimedb/go_client_tokens/` with file permissions `0600`.

## Example usage

A complete working example that connects to a SpacetimeDB module, subscribes to tables, and calls reducers:

```go
package main

import (
    "context"
    "fmt"
    "os"
    "time"

    "github.com/clockworklabs/spacetimedb-go/client"
    "github.com/clockworklabs/spacetimedb-go/types"
    "my-spacetimedb-client/module_bindings"
)

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    // Load a previously saved token, if any.
    token, _ := client.LoadToken("localhost:3000", "my_module")

    // Connect to SpacetimeDB.
    conn, err := client.NewDbConnectionBuilder().
        WithUri("ws://localhost:3000").
        WithModuleName("my_module").
        WithToken(token).
        OnConnect(func(identity types.Identity, connId types.ConnectionId, tok string) {
            fmt.Println("Connected! Identity:", identity)
            // Persist the token so we keep the same identity next time.
            _ = client.SaveToken("localhost:3000", "my_module", tok)
        }).
        OnDisconnect(func(err error) {
            if err != nil {
                fmt.Fprintf(os.Stderr, "Disconnected: %v\n", err)
            }
        }).
        Build(ctx)
    if err != nil {
        fmt.Fprintf(os.Stderr, "Failed to connect: %v\n", err)
        os.Exit(1)
    }
    defer conn.Disconnect()

    // Set up typed table handles and reducer accessors.
    tables := module_bindings.NewRemoteTables()
    reducers := module_bindings.NewRemoteReducers(conn)
    module_bindings.RegisterTables(conn, tables)

    // Register callbacks before subscribing.
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
        fmt.Printf("[user updated] online: %v -> %v\n", old.Online, new.Online)
    })

    // Start processing messages in the background.
    errCh := conn.RunAsync(ctx)

    // Subscribe to all tables.
    _, err = client.NewSubscriptionBuilder(conn).
        OnApplied(func(querySetId uint32) {
            fmt.Printf("Subscribed! Users: %d, Messages: %d\n",
                tables.User.Count(), tables.Message.Count())
        }).
        SubscribeToAllTables()
    if err != nil {
        fmt.Fprintf(os.Stderr, "subscribe: %v\n", err)
        os.Exit(1)
    }

    // Give time for subscription to be applied.
    time.Sleep(500 * time.Millisecond)

    // Call some reducers.
    if _, err := reducers.SetName("GoUser"); err != nil {
        fmt.Fprintf(os.Stderr, "set_name: %v\n", err)
    }
    if _, err := reducers.SendMessage("Hello from Go!"); err != nil {
        fmt.Fprintf(os.Stderr, "send_message: %v\n", err)
    }

    // Let callbacks process.
    time.Sleep(500 * time.Millisecond)

    // Disconnect and wait for the message loop to exit.
    conn.Disconnect()
    select {
    case err := <-errCh:
        if err != nil {
            fmt.Fprintf(os.Stderr, "connection error: %v\n", err)
        }
    case <-time.After(2 * time.Second):
    }

    fmt.Println("Done.")
}
```
