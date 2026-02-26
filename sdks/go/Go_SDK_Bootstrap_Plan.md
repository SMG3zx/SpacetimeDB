## Go SDK Bootstrap Plan (Modeled After C# + Rust, Full Parallel Track)

### Summary
Build the Go SDK in two tracks in parallel, but with shared contracts:
1. A handwritten Go runtime package (`sdks/go`) with Rust/C#-parity client behavior.
2. In-tree Rust codegen + CLI integration (`spacetime generate --lang go`) that emits typed module bindings against that runtime.

This matches how C# works today: runtime primitives live in SDK code, and `spacetime generate` emits module-specific typed wrappers.

### How C# Currently Does It (Reference Model)
- Runtime + event/subscription logic is handwritten in C#: [SpacetimeDBClient.cs](/c:/Users/OWNER/Downloads/SpacetimeDB/sdks/csharp/src/SpacetimeDBClient.cs), [AbstractEventHandler.cs](/c:/Users/OWNER/Downloads/SpacetimeDB/sdks/csharp/src/EventHandling/AbstractEventHandler.cs), and related SDK files.
- Generated module bindings come from Rust codegen backend: [csharp.rs](/c:/Users/OWNER/Downloads/SpacetimeDB/crates/codegen/src/csharp.rs).
- CLI wires language selection and output formatting: [generate.rs](/c:/Users/OWNER/Downloads/SpacetimeDB/crates/cli/src/subcommands/generate.rs).
- Client API message schema is generated from server message definitions using module-def flow (see script): [gen-client-api.sh](/c:/Users/OWNER/Downloads/SpacetimeDB/sdks/csharp/tools~/gen-client-api.sh), [get_ws_schema_v2.rs](/c:/Users/OWNER/Downloads/SpacetimeDB/crates/client-api-messages/examples/get_ws_schema_v2.rs).

### Public APIs / Interfaces to Add
- Runtime package (`sdks/go`):
1. `type DbConnectionBuilder struct { ... }`
2. `func (b *DbConnectionBuilder) WithURI/WithDatabaseName/WithToken/WithCompression/WithConfirmedReads/...`
3. `func (b *DbConnectionBuilder) Build(ctx context.Context) (*DbConnection, error)`
4. `type DbConnection struct { Db, Reducers, Procedures, SubscriptionBuilder, Disconnect, CallReducer, CallProcedure, OneOffQuery }`
5. Callback/event interfaces for connect, disconnect, subscription applied/error, table row events, reducer/procedure results.
- Generated bindings surface (per module):
1. `module_bindings/client.go` (typed connection wrapper)
2. `module_bindings/tables/*.go` (table accessors + typed row events)
3. `module_bindings/reducers/*.go` (typed reducer calls)
4. `module_bindings/procedures/*.go` (typed procedure calls)
5. `module_bindings/types/*.go` (schema types)

### Implementation Plan

#### Phase 1: Runtime skeleton in `sdks/go` (first executable vertical slice)
1. [ ] Create `sdks/go` module with package layout:
- `internal/protocol` (wire message structs + encode/decode)
- `internal/bsatn` (serialization helpers)
- `connection`, `subscription`, `cache`, `events`, `types`
2. [x] Implement WS connection lifecycle with protocol `v2.bsatn.spacetimedb` and auth/token flow.
3. [x] Implement request-id/query-id allocators and message routing.
4. [x] Implement client cache with atomic transaction application semantics.
5. [x] Implement reducer/procedure call paths and callback dispatch.
6. [x] Implement one-off query and subscription management.

#### Phase 2: Add Go backend to codegen crate
1. Add new backend file: `crates/codegen/src/go.rs`.
2. Register in codegen exports: [lib.rs](/c:/Users/OWNER/Downloads/SpacetimeDB/crates/codegen/src/lib.rs).
3. Generate outputs matching runtime interfaces from Phase 1:
- global client wrapper
- tables/views
- reducers/procedures
- type definitions
4. Use existing `Lang` trait contract (table/type/reducer/procedure/global generators).

#### Phase 3: CLI integration for `--lang go`
1. Extend language enum + parser + display name in [generate.rs](/c:/Users/OWNER/Downloads/SpacetimeDB/crates/cli/src/subcommands/generate.rs).
2. Add default output dir for Go (use `module_bindings` for parity with C# style).
3. Add auto-detection by `go.mod` in `detect_default_language`.
4. Add formatter integration hook (initially no-op; optional `gofmt` task later).
5. Ensure `dev` path can resolve/use Go generate targets via existing generate-entry pipeline.

#### Phase 4: Protocol schema generation support for Go runtime
1. Mirror C# `ClientApi` flow: generate Go wire types from `client-api-messages` schema source used by [get_ws_schema_v2.rs](/c:/Users/OWNER/Downloads/SpacetimeDB/crates/client-api-messages/examples/get_ws_schema_v2.rs).
2. Keep generated protocol types separate from module binding codegen to reduce churn.
3. Add regeneration script under `sdks/go/tools/`.

#### Phase 5: SDK parity hardening
1. Match Rust/C# capabilities:
- connect/disconnect lifecycle events
- subscriptions with overlap-safe cache behavior
- reducers + procedures (typed args/returns)
- one-off query
- compression options
- confirmed reads
2. Align error taxonomy and retry/reconnect semantics with existing SDK expectations.

### Test Cases and Scenarios

#### Codegen tests
1. Add snapshot test case in [codegen.rs](/c:/Users/OWNER/Downloads/SpacetimeDB/crates/codegen/tests/codegen.rs) for Go backend outputs.
2. Verify generated file naming/layout stability.

#### CLI tests
1. Parse/serde tests for `--lang go`.
2. Default out-dir and detection tests (`go.mod` presence).
3. Multi-entry generate config behavior with Go entries.

#### Runtime integration tests (Go)
1. Connect/auth/disconnect lifecycle.
2. Subscribe/unsubscribe correctness.
3. Overlapping subscription dedup behavior.
4. Reducer success/error callbacks.
5. Procedure returned/internal-error paths.
6. One-off query success/error.
7. Reconnection and stale callback cleanup.

### Assumptions and Defaults Chosen
1. You want **in-repo, Rust-driven codegen** (same model as C#), not an external generator.
2. Target is **capability parity with Rust/C#**, not a minimal transport client.
3. Initial Go SDK scope is backend/server Go usage (not browser/WASM-specific runtime constraints).
4. `spacetime generate --lang go` is required in first public milestone.
5. `init` templates for Go are out of first milestone unless needed to unblock onboarding.
