package cache

import (
	"bytes"
	"testing"

	sdktypes "github.com/clockworklabs/spacetimedb/sdks/go/types"
)

func TestApplyTransactionAtomicallyPublishesState(t *testing.T) {
	store := NewStore()

	store.ApplyTransaction(sdktypes.Transaction{Tables: []sdktypes.TableMutation{{
		Table: "users",
		Inserts: []sdktypes.Row{{
			Key:  "u1",
			Data: []byte("alice"),
		}},
	}}})

	value, ok := store.Get("users", "u1")
	if !ok {
		t.Fatalf("expected users/u1 to exist")
	}
	if !bytes.Equal(value, []byte("alice")) {
		t.Fatalf("unexpected value: %q", string(value))
	}

	store.ApplyTransaction(sdktypes.Transaction{Tables: []sdktypes.TableMutation{
		{
			Table:   "users",
			Deletes: []string{"u1"},
			Inserts: []sdktypes.Row{{
				Key:  "u2",
				Data: []byte("bob"),
			}},
		},
		{
			Table: "teams",
			Inserts: []sdktypes.Row{{
				Key:  "t1",
				Data: []byte("infra"),
			}},
		},
	}})

	if _, ok := store.Get("users", "u1"); ok {
		t.Fatalf("expected users/u1 to be deleted")
	}
	value, ok = store.Get("users", "u2")
	if !ok {
		t.Fatalf("expected users/u2 to exist")
	}
	if !bytes.Equal(value, []byte("bob")) {
		t.Fatalf("unexpected users/u2 value: %q", string(value))
	}
	team, ok := store.Get("teams", "t1")
	if !ok {
		t.Fatalf("expected teams/t1 to exist")
	}
	if !bytes.Equal(team, []byte("infra")) {
		t.Fatalf("unexpected teams/t1 value: %q", string(team))
	}
}

func TestReturnedDataIsDefensivelyCopied(t *testing.T) {
	store := NewStore()
	store.ApplyTransaction(sdktypes.Transaction{Tables: []sdktypes.TableMutation{{
		Table: "users",
		Inserts: []sdktypes.Row{{
			Key:  "u1",
			Data: []byte("alice"),
		}},
	}}})

	value, ok := store.Get("users", "u1")
	if !ok {
		t.Fatalf("expected users/u1 to exist")
	}
	value[0] = 'A'

	value2, ok := store.Get("users", "u1")
	if !ok {
		t.Fatalf("expected users/u1 to exist")
	}
	if !bytes.Equal(value2, []byte("alice")) {
		t.Fatalf("cache leaked mutable backing array: %q", string(value2))
	}

	table := store.TableSnapshot("users")
	table["u1"][0] = 'B'

	value3, ok := store.Get("users", "u1")
	if !ok {
		t.Fatalf("expected users/u1 to exist")
	}
	if !bytes.Equal(value3, []byte("alice")) {
		t.Fatalf("table snapshot leaked mutable backing array: %q", string(value3))
	}
}
