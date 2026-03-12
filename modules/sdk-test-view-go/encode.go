package main

import (
	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/types"
)

// ── Player ────────────────────────────────────────────────────────────────────

func encodePlayer(w *bsatn.Writer, p Player) {
	w.WriteU64(p.EntityId)
	encodeIdentity(w, p.Identity)
}

func decodePlayer(r *bsatn.Reader) (Player, error) {
	entityId, err := r.ReadU64()
	if err != nil {
		return Player{}, err
	}
	identity, err := decodeIdentity(r)
	if err != nil {
		return Player{}, err
	}
	return Player{EntityId: entityId, Identity: identity}, nil
}

// ── PlayerLevel ───────────────────────────────────────────────────────────────

func encodePlayerLevel(w *bsatn.Writer, p PlayerLevel) {
	w.WriteU64(p.EntityId)
	w.WriteU64(p.Level)
}

func decodePlayerLevel(r *bsatn.Reader) (PlayerLevel, error) {
	entityId, err := r.ReadU64()
	if err != nil {
		return PlayerLevel{}, err
	}
	level, err := r.ReadU64()
	if err != nil {
		return PlayerLevel{}, err
	}
	return PlayerLevel{EntityId: entityId, Level: level}, nil
}

// ── PlayerLocation ────────────────────────────────────────────────────────────

func encodePlayerLocation(w *bsatn.Writer, p PlayerLocation) {
	w.WriteU64(p.EntityId)
	w.WriteBool(p.Active)
	w.WriteI32(p.X)
	w.WriteI32(p.Y)
}

func decodePlayerLocation(r *bsatn.Reader) (PlayerLocation, error) {
	entityId, err := r.ReadU64()
	if err != nil {
		return PlayerLocation{}, err
	}
	active, err := r.ReadBool()
	if err != nil {
		return PlayerLocation{}, err
	}
	x, err := r.ReadI32()
	if err != nil {
		return PlayerLocation{}, err
	}
	y, err := r.ReadI32()
	if err != nil {
		return PlayerLocation{}, err
	}
	return PlayerLocation{EntityId: entityId, Active: active, X: x, Y: y}, nil
}

// ── PlayerAndLevel ────────────────────────────────────────────────────────────

func encodePlayerAndLevel(w *bsatn.Writer, p PlayerAndLevel) {
	w.WriteU64(p.EntityId)
	encodeIdentity(w, p.Identity)
	w.WriteU64(p.Level)
}

// ── Identity ─────────────────────────────────────────────────────────────────

func encodeIdentity(w *bsatn.Writer, id types.Identity) {
	b := [32]byte(id)
	lo := uint64(b[0]) | uint64(b[1])<<8 | uint64(b[2])<<16 | uint64(b[3])<<24 |
		uint64(b[4])<<32 | uint64(b[5])<<40 | uint64(b[6])<<48 | uint64(b[7])<<56
	hi := uint64(b[8]) | uint64(b[9])<<8 | uint64(b[10])<<16 | uint64(b[11])<<24 |
		uint64(b[12])<<32 | uint64(b[13])<<40 | uint64(b[14])<<48 | uint64(b[15])<<56
	lo2 := uint64(b[16]) | uint64(b[17])<<8 | uint64(b[18])<<16 | uint64(b[19])<<24 |
		uint64(b[20])<<32 | uint64(b[21])<<40 | uint64(b[22])<<48 | uint64(b[23])<<56
	hi2 := uint64(b[24]) | uint64(b[25])<<8 | uint64(b[26])<<16 | uint64(b[27])<<24 |
		uint64(b[28])<<32 | uint64(b[29])<<40 | uint64(b[30])<<48 | uint64(b[31])<<56
	w.WriteU128(lo, hi)
	w.WriteU128(lo2, hi2)
}

func decodeIdentity(r *bsatn.Reader) (types.Identity, error) {
	lo, hi, err := r.ReadU128()
	if err != nil {
		return types.Identity{}, err
	}
	lo2, hi2, err := r.ReadU128()
	if err != nil {
		return types.Identity{}, err
	}
	var b [32]byte
	for i := 0; i < 8; i++ {
		b[i] = byte(lo >> (uint(i) * 8))
		b[8+i] = byte(hi >> (uint(i) * 8))
		b[16+i] = byte(lo2 >> (uint(i) * 8))
		b[24+i] = byte(hi2 >> (uint(i) * 8))
	}
	return types.Identity(b), nil
}
