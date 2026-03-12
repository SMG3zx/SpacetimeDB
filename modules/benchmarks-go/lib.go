//go:generate go run github.com/clockworklabs/spacetimedb-go-server/cmd/stdbgen
package main

func sptr(s string) *string { return &s }

// load holds pre-computed table sizes derived from initial_load.
type load struct {
	initialLoad  uint32
	smallTable   uint32
	numPlayers   uint32
	bigTable     uint32
	biggestTable uint32
}

func newLoad(initialLoad uint32) load {
	return load{
		initialLoad:  initialLoad,
		smallTable:   initialLoad,
		numPlayers:   initialLoad,
		bigTable:     initialLoad * 50,
		biggestTable: initialLoad * 100,
	}
}

func main() {}
