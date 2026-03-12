//go:generate go run github.com/clockworklabs/spacetimedb-go-server/cmd/stdbgen
package main

func sptr(s string) *string { return &s }

func main() {}
