package main

import (
	"fmt"

	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/types"
)

// ── Encode / Decode Functions ────────────────────────────────────────────────

func encodePerson(w *bsatn.Writer, v Person) {
	w.WriteU32(v.Id)
	w.WriteString(v.Name)
	w.WriteU8(v.Age)
}

func decodePerson(r *bsatn.Reader) (Person, error) {
	id, err := r.ReadU32()
	if err != nil {
		return Person{}, err
	}
	name, err := r.ReadString()
	if err != nil {
		return Person{}, err
	}
	age, err := r.ReadU8()
	if err != nil {
		return Person{}, err
	}
	return Person{Id: id, Name: name, Age: age}, nil
}

func encodeRemoveTable(w *bsatn.Writer, v RemoveTable) {
	w.WriteU32(v.Id)
}

func decodeRemoveTable(r *bsatn.Reader) (RemoveTable, error) {
	id, err := r.ReadU32()
	if err != nil {
		return RemoveTable{}, err
	}
	return RemoveTable{Id: id}, nil
}

func encodeTestA(w *bsatn.Writer, v TestA) {
	w.WriteU32(v.X)
	w.WriteU32(v.Y)
	w.WriteString(v.Z)
}

func decodeTestA(r *bsatn.Reader) (TestA, error) {
	x, err := r.ReadU32()
	if err != nil {
		return TestA{}, err
	}
	y, err := r.ReadU32()
	if err != nil {
		return TestA{}, err
	}
	z, err := r.ReadString()
	if err != nil {
		return TestA{}, err
	}
	return TestA{X: x, Y: y, Z: z}, nil
}

func decodeTestB(r *bsatn.Reader) (TestB, error) {
	foo, err := r.ReadString()
	if err != nil {
		return TestB{}, err
	}
	return TestB{Foo: foo}, nil
}

func encodeTestC(w *bsatn.Writer, v TestC) {
	w.WriteVariantTag(uint8(v))
}

func decodeTestC(r *bsatn.Reader) (TestC, error) {
	tag, err := r.ReadVariantTag()
	if err != nil {
		return 0, err
	}
	return TestC(tag), nil
}

func encodeTestD(w *bsatn.Writer, v TestD) {
	if v.TestC == nil {
		w.WriteVariantTag(0) // none
	} else {
		w.WriteVariantTag(1) // some
		encodeTestC(w, *v.TestC)
	}
}

func decodeTestD(r *bsatn.Reader) (TestD, error) {
	tag, err := r.ReadVariantTag()
	if err != nil {
		return TestD{}, err
	}
	if tag == 0 {
		return TestD{}, nil
	}
	tc, err := decodeTestC(r)
	if err != nil {
		return TestD{}, err
	}
	return TestD{TestC: &tc}, nil
}

func encodeTestE(w *bsatn.Writer, v TestE) {
	w.WriteU64(v.Id)
	w.WriteString(v.Name)
}

func decodeTestE(r *bsatn.Reader) (TestE, error) {
	id, err := r.ReadU64()
	if err != nil {
		return TestE{}, err
	}
	name, err := r.ReadString()
	if err != nil {
		return TestE{}, err
	}
	return TestE{Id: id, Name: name}, nil
}

func encodeBaz(w *bsatn.Writer, v Baz) {
	w.WriteString(v.Field)
}

func decodeBaz(r *bsatn.Reader) (Baz, error) {
	field, err := r.ReadString()
	if err != nil {
		return Baz{}, err
	}
	return Baz{Field: field}, nil
}

func encodeFoobar(w *bsatn.Writer, v Foobar) {
	w.WriteVariantTag(uint8(v.Variant))
	switch v.Variant {
	case FoobarBazV:
		encodeBaz(w, *v.BazVal)
	case FoobarBarV:
		// unit variant, no payload
	case FoobarHarV:
		w.WriteU32(v.HarVal)
	}
}

func decodeFoobar(r *bsatn.Reader) (Foobar, error) {
	tag, err := r.ReadVariantTag()
	if err != nil {
		return Foobar{}, err
	}
	switch FoobarVariant(tag) {
	case FoobarBazV:
		baz, err := decodeBaz(r)
		if err != nil {
			return Foobar{}, err
		}
		return Foobar{Variant: FoobarBazV, BazVal: &baz}, nil
	case FoobarBarV:
		return Foobar{Variant: FoobarBarV}, nil
	case FoobarHarV:
		n, err := r.ReadU32()
		if err != nil {
			return Foobar{}, err
		}
		return Foobar{Variant: FoobarHarV, HarVal: n}, nil
	default:
		return Foobar{}, fmt.Errorf("unknown Foobar variant %d", tag)
	}
}

func encodeTestFoobar(w *bsatn.Writer, v TestFoobar) {
	encodeFoobar(w, v.Field)
}

func decodeTestFoobar(r *bsatn.Reader) (TestFoobar, error) {
	field, err := decodeFoobar(r)
	if err != nil {
		return TestFoobar{}, err
	}
	return TestFoobar{Field: field}, nil
}

func encodeTestF(w *bsatn.Writer, v TestF) {
	w.WriteVariantTag(uint8(v.Variant))
	if v.Variant == TestFBazV {
		w.WriteString(v.BazVal)
	}
}

func decodeTestF(r *bsatn.Reader) (TestF, error) {
	tag, err := r.ReadVariantTag()
	if err != nil {
		return TestF{}, err
	}
	switch TestFVariant(tag) {
	case TestFFooV, TestFBarV:
		return TestF{Variant: TestFVariant(tag)}, nil
	case TestFBazV:
		s, err := r.ReadString()
		if err != nil {
			return TestF{}, err
		}
		return TestF{Variant: TestFBazV, BazVal: s}, nil
	default:
		return TestF{}, fmt.Errorf("unknown TestF variant %d", tag)
	}
}

func encodePrivateTable(w *bsatn.Writer, v PrivateTable) {
	w.WriteString(v.Name)
}

func decodePrivateTable(r *bsatn.Reader) (PrivateTable, error) {
	name, err := r.ReadString()
	if err != nil {
		return PrivateTable{}, err
	}
	return PrivateTable{Name: name}, nil
}

func encodePoint(w *bsatn.Writer, v Point) {
	w.WriteI64(v.X)
	w.WriteI64(v.Y)
}

func decodePoint(r *bsatn.Reader) (Point, error) {
	x, err := r.ReadI64()
	if err != nil {
		return Point{}, err
	}
	y, err := r.ReadI64()
	if err != nil {
		return Point{}, err
	}
	return Point{X: x, Y: y}, nil
}

func encodePkMultiIdentity(w *bsatn.Writer, v PkMultiIdentity) {
	w.WriteU32(v.Id)
	w.WriteU32(v.Other)
}

func decodePkMultiIdentity(r *bsatn.Reader) (PkMultiIdentity, error) {
	id, err := r.ReadU32()
	if err != nil {
		return PkMultiIdentity{}, err
	}
	other, err := r.ReadU32()
	if err != nil {
		return PkMultiIdentity{}, err
	}
	return PkMultiIdentity{Id: id, Other: other}, nil
}

func encodeRepeatingTestArg(w *bsatn.Writer, v RepeatingTestArg) {
	w.WriteU64(v.ScheduledId)
	v.ScheduledAt.WriteBsatn(w)
	v.PrevTime.WriteBsatn(w)
}

func decodeRepeatingTestArg(r *bsatn.Reader) (RepeatingTestArg, error) {
	id, err := r.ReadU64()
	if err != nil {
		return RepeatingTestArg{}, err
	}
	sched, err := types.ReadScheduleAt(r)
	if err != nil {
		return RepeatingTestArg{}, err
	}
	ts, err := types.ReadTimestamp(r)
	if err != nil {
		return RepeatingTestArg{}, err
	}
	return RepeatingTestArg{ScheduledId: id, ScheduledAt: sched, PrevTime: ts}, nil
}

func encodeHasSpecialStuff(w *bsatn.Writer, v HasSpecialStuff) {
	v.Identity.WriteBsatn(w)
	v.ConnectionId.WriteBsatn(w)
}

func decodeHasSpecialStuff(r *bsatn.Reader) (HasSpecialStuff, error) {
	id, err := types.ReadIdentity(r)
	if err != nil {
		return HasSpecialStuff{}, err
	}
	connId, err := types.ReadConnectionId(r)
	if err != nil {
		return HasSpecialStuff{}, err
	}
	return HasSpecialStuff{Identity: id, ConnectionId: connId}, nil
}

func encodePlayer(w *bsatn.Writer, v Player) {
	v.Identity.WriteBsatn(w)
	w.WriteU64(v.PlayerId)
	w.WriteString(v.Name)
}

func decodePlayer(r *bsatn.Reader) (Player, error) {
	id, err := types.ReadIdentity(r)
	if err != nil {
		return Player{}, err
	}
	playerId, err := r.ReadU64()
	if err != nil {
		return Player{}, err
	}
	name, err := r.ReadString()
	if err != nil {
		return Player{}, err
	}
	return Player{Identity: id, PlayerId: playerId, Name: name}, nil
}
