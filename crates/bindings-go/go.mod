module github.com/clockworklabs/spacetimedb-go-server

go 1.23

// The server SDK depends on the client SDK's bsatn and types packages.
require github.com/clockworklabs/spacetimedb-go v0.0.0

replace github.com/clockworklabs/spacetimedb-go => ../../sdks/go
