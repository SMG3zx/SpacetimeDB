package types_test

import (
	"testing"
	"time"

	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/types"
)

func encode(f func(*bsatn.Writer)) []byte {
	w := bsatn.NewWriter()
	f(w)
	return w.Bytes()
}

func TestIdentityRoundtrip(t *testing.T) {
	want := types.Identity{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32}
	r := bsatn.NewReader(encode(func(w *bsatn.Writer) { want.WriteBsatn(w) }))
	got, err := types.ReadIdentity(r)
	if err != nil || got != want {
		t.Fatalf("got %v err %v", got, err)
	}
}

func TestIdentityString(t *testing.T) {
	id := types.Identity{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
		0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
		0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18,
		0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20}
	want := "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"
	if id.String() != want {
		t.Fatalf("got %s, want %s", id.String(), want)
	}
}

func TestConnectionIdRoundtrip(t *testing.T) {
	want := types.ConnectionId{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}
	r := bsatn.NewReader(encode(func(w *bsatn.Writer) { want.WriteBsatn(w) }))
	got, err := types.ReadConnectionId(r)
	if err != nil || got != want {
		t.Fatalf("got %v err %v", got, err)
	}
}

func TestTimestampRoundtrip(t *testing.T) {
	ts := types.TimestampFromTime(time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC))
	r := bsatn.NewReader(encode(func(w *bsatn.Writer) { ts.WriteBsatn(w) }))
	got, err := types.ReadTimestamp(r)
	if err != nil || got.Microseconds != ts.Microseconds {
		t.Fatalf("got %v err %v", got, err)
	}
}

func TestTimeDurationRoundtrip(t *testing.T) {
	d := types.TimeDurationFromDuration(5 * time.Second)
	r := bsatn.NewReader(encode(func(w *bsatn.Writer) { d.WriteBsatn(w) }))
	got, err := types.ReadTimeDuration(r)
	if err != nil || got.Nanoseconds != d.Nanoseconds {
		t.Fatalf("got %v err %v", got, err)
	}
}

func TestUuidRoundtrip(t *testing.T) {
	u, _ := types.UuidFromString("550e8400-e29b-41d4-a716-446655440000")
	r := bsatn.NewReader(encode(func(w *bsatn.Writer) { u.WriteBsatn(w) }))
	got, err := types.ReadUuid(r)
	if err != nil || got != u {
		t.Fatalf("got %v err %v", got, err)
	}
}

func TestU128Roundtrip(t *testing.T) {
	want := types.U128{Lo: 0xDEADBEEF, Hi: 0xCAFEBABE}
	r := bsatn.NewReader(encode(func(w *bsatn.Writer) { want.WriteBsatn(w) }))
	got, err := types.ReadU128(r)
	if err != nil || got != want {
		t.Fatalf("got %v err %v", got, err)
	}
}

func TestI128Roundtrip(t *testing.T) {
	want := types.I128{Lo: 42, Hi: -1}
	r := bsatn.NewReader(encode(func(w *bsatn.Writer) { want.WriteBsatn(w) }))
	got, err := types.ReadI128(r)
	if err != nil || got != want {
		t.Fatalf("got %v err %v", got, err)
	}
}

func TestU256Roundtrip(t *testing.T) {
	var want types.U256
	want[0] = 0xFF
	want[31] = 0x01
	r := bsatn.NewReader(encode(func(w *bsatn.Writer) { want.WriteBsatn(w) }))
	got, err := types.ReadU256(r)
	if err != nil || got != want {
		t.Fatalf("got %v err %v", got, err)
	}
}
