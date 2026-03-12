package client_test

import (
	"testing"

	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/client"
	"github.com/clockworklabs/spacetimedb-go/protocol"
)

type person struct {
	ID   uint32
	Name string
}

func encodePerson(p person) []byte {
	w := bsatn.NewWriter()
	w.WriteU32(p.ID)
	w.WriteString(p.Name)
	return w.Bytes()
}

func decodePerson(r *bsatn.Reader) (person, error) {
	id, err := r.ReadU32()
	if err != nil {
		return person{}, err
	}
	name, err := r.ReadString()
	if err != nil {
		return person{}, err
	}
	return person{ID: id, Name: name}, nil
}

func makeRowList(rows ...[]byte) protocol.BsatnRowList {
	if len(rows) == 0 {
		return protocol.BsatnRowList{}
	}
	// Use RowOffsets so variable-length rows work correctly.
	offsets := make([]uint64, len(rows))
	var data []byte
	var offset uint64
	for i, r := range rows {
		offsets[i] = offset
		data = append(data, r...)
		offset += uint64(len(r))
	}
	return protocol.BsatnRowList{
		SizeHint: protocol.RowSizeHint{Kind: protocol.RowSizeHintOffsets, Offsets: offsets},
		RowsData: data,
	}
}

func TestCacheInsertAndIter(t *testing.T) {
	c := client.NewTableCache(decodePerson)

	alice := encodePerson(person{1, "Alice"})
	bob := encodePerson(person{2, "Bob"})
	list := makeRowList(alice, bob)

	inserted, err := c.ApplyInserts(&list)
	if err != nil {
		t.Fatal(err)
	}
	if len(inserted) != 2 {
		t.Fatalf("expected 2 inserted, got %d", len(inserted))
	}
	if c.Count() != 2 {
		t.Fatalf("expected count 2, got %d", c.Count())
	}

	var names []string
	for p := range c.Iter() {
		names = append(names, p.Name)
	}
	if len(names) != 2 {
		t.Fatalf("iter returned %d rows", len(names))
	}
}

func TestCacheDelete(t *testing.T) {
	c := client.NewTableCache(decodePerson)
	alice := encodePerson(person{1, "Alice"})
	list := makeRowList(alice)

	c.ApplyInserts(&list)
	if c.Count() != 1 {
		t.Fatal("expected 1 row")
	}

	deleted, err := c.ApplyDeletes(&list)
	if err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 1 {
		t.Fatalf("expected 1 deleted, got %d", len(deleted))
	}
	if c.Count() != 0 {
		t.Fatal("expected empty cache after delete")
	}
}

func TestCacheRefCounting(t *testing.T) {
	c := client.NewTableCache(decodePerson)
	alice := encodePerson(person{1, "Alice"})
	list := makeRowList(alice)

	// Insert twice (simulate two overlapping subscriptions)
	c.ApplyInserts(&list)
	c.ApplyInserts(&list)
	if c.Count() != 1 {
		t.Fatal("ref counting broken: expected 1 unique row")
	}

	// First delete should not remove
	deleted, _ := c.ApplyDeletes(&list)
	if len(deleted) != 0 {
		t.Fatal("row should not be deleted yet (refcount > 0)")
	}
	if c.Count() != 1 {
		t.Fatal("row should still be in cache")
	}

	// Second delete removes
	deleted, _ = c.ApplyDeletes(&list)
	if len(deleted) != 1 {
		t.Fatal("row should be deleted now")
	}
	if c.Count() != 0 {
		t.Fatal("cache should be empty")
	}
}

func TestUniqueIndex(t *testing.T) {
	idx := client.NewUniqueIndex[person, uint32](func(p *person) uint32 { return p.ID })

	alice := &person{1, "Alice"}
	idx.Insert(alice)

	got, ok := idx.Find(1)
	if !ok || got.Name != "Alice" {
		t.Fatal("expected Alice at id=1")
	}

	idx.Remove(alice)
	_, ok = idx.Find(1)
	if ok {
		t.Fatal("expected id=1 to be gone")
	}
}
