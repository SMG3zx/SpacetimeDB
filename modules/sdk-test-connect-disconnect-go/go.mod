module spacetimedb_sdk_test_connect_disconnect

go 1.23

require (
	github.com/clockworklabs/spacetimedb-go v0.0.0
	github.com/clockworklabs/spacetimedb-go-server v0.0.0
)

replace (
	github.com/clockworklabs/spacetimedb-go => ../../sdks/go
	github.com/clockworklabs/spacetimedb-go-server => ../../crates/bindings-go
)
