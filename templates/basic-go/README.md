# SpacetimeDB Go Client + Go Server (Scaffold)

This template provides:
- A Go server scaffold in `spacetimedb/` with starter reducer-style functions (`Init`, `IdentityConnected`, `IdentityDisconnected`, `Add`, `SayHello`) and a `Person` model.
- A Go client in `client/`.

## Important

The Go server scaffold is not publishable in the current release.
Go-produced wasm currently imports WASI functions that the SpacetimeDB wasm host does not provide.
For publishable server modules, use Rust/C#/TypeScript/C++.

## Setup

1. Generate Go bindings:
   ```
   spacetime generate --lang go --out-dir client/module_bindings --module-path spacetimedb
   ```
2. Run the client:
   ```
   cd client
   go mod tidy
   go run .
   ```
