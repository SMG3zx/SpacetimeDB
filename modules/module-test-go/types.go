package main

import "github.com/clockworklabs/spacetimedb-go/types"

// ── Go Struct Types ──────────────────────────────────────────────────────────

// Person is a row in the Person table.
type Person struct {
	Id   uint32
	Name string
	Age  uint8
}

// RemoveTable is a row in the RemoveTable table.
type RemoveTable struct {
	Id uint32
}

// TestA is a row in the TestA table.
type TestA struct {
	X uint32
	Y uint32
	Z string
}

// TestB is a helper struct used as a reducer parameter.
type TestB struct {
	Foo string
}

// TestC is an enum type (Foo=0, Bar=1).
type TestC uint8

const (
	TestCFoo TestC = 0
	TestCBar TestC = 1
)

// TestD is a row in the TestD table, holding an optional TestC.
type TestD struct {
	TestC *TestC // nil = None, non-nil = Some
}

// TestE is a row in the TestE table.
type TestE struct {
	Id   uint64
	Name string
}

// Baz is a helper struct used inside the Foobar enum.
type Baz struct {
	Field string
}

// FoobarVariant identifies which variant a Foobar value holds.
type FoobarVariant uint8

const (
	FoobarBazV FoobarVariant = 0
	FoobarBarV FoobarVariant = 1
	FoobarHarV FoobarVariant = 2
)

// Foobar is an enum with payload: Baz(Baz) | Bar | Har(u32).
type Foobar struct {
	Variant FoobarVariant
	BazVal  *Baz
	HarVal  uint32
}

// TestFoobar is a row in the TestF table.
type TestFoobar struct {
	Field Foobar
}

// TestFVariant identifies which variant a TestF value holds.
type TestFVariant uint8

const (
	TestFFooV TestFVariant = 0
	TestFBarV TestFVariant = 1
	TestFBazV TestFVariant = 2
)

// TestF is an enum with optional payload: Foo | Bar | Baz(String).
type TestF struct {
	Variant TestFVariant
	BazVal  string
}

// PrivateTable is a row in the PrivateTable table.
type PrivateTable struct {
	Name string
}

// Point is a row in the Point table.
type Point struct {
	X int64
	Y int64
}

// PkMultiIdentity is a row in the PkMultiIdentity table.
type PkMultiIdentity struct {
	Id    uint32
	Other uint32
}

// RepeatingTestArg is a row in the RepeatingTestArg scheduled table.
type RepeatingTestArg struct {
	ScheduledId uint64
	ScheduledAt types.ScheduleAt
	PrevTime    types.Timestamp
}

// HasSpecialStuff is a row in the HasSpecialStuff table.
type HasSpecialStuff struct {
	Identity     types.Identity
	ConnectionId types.ConnectionId
}

// Player is a row in both the Player and LoggedOutPlayer tables.
type Player struct {
	Identity types.Identity
	PlayerId uint64
	Name     string
}
