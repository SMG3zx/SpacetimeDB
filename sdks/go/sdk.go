// Package spacetimedb is the SpacetimeDB Go client SDK.
//
// SpacetimeDB is a relational database with embedded application logic. Clients
// connect directly to the database over WebSocket and receive real-time updates
// when subscribed table data changes.
//
// # Quick start
//
//  1. Generate client bindings from your module:
//     spacetime generate --lang go --out-dir ./bindings --module-name my_module
//
//  2. Connect and subscribe:
//
//     conn, err := client.NewDbConnectionBuilder().
//         WithUri("ws://localhost:3000").
//         WithModuleName("my_module").
//         Build()
//
// Sub-packages:
//   - [github.com/clockworklabs/spacetimedb-go/bsatn] — BSATN binary codec
//   - [github.com/clockworklabs/spacetimedb-go/types] — SpacetimeDB types (Identity, Timestamp, …)
//   - [github.com/clockworklabs/spacetimedb-go/protocol] — WebSocket v2 message types
//   - [github.com/clockworklabs/spacetimedb-go/client] — Connection, cache, subscriptions
package spacetimedb
