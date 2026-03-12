//go:generate go run github.com/clockworklabs/spacetimedb-go-server/cmd/stdbgen

package main

func sptr(s string) *string { return &s }

// main is required by TinyGo but never called by the SpacetimeDB host.
func main() {}
