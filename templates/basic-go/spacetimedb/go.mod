module spacetimedb_module

go 1.23

require (
	github.com/clockworklabs/spacetimedb-go v0.0.0
	github.com/clockworklabs/spacetimedb-go-server v0.0.0
)

// Replace with the local SDK path. Update these to match your environment,
// or remove them once the SDK is published to the Go module proxy.
replace (
	github.com/clockworklabs/spacetimedb-go => SPACETIMEDB_GO_PATH
	github.com/clockworklabs/spacetimedb-go-server => SPACETIMEDB_GO_SERVER_PATH
)
