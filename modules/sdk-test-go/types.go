package main

import "github.com/clockworklabs/spacetimedb-go/types"

// ── Custom Types ─────────────────────────────────────────────────────────────

type SimpleEnum uint8

const (
	SimpleEnumZero SimpleEnum = 0
	SimpleEnumOne  SimpleEnum = 1
	SimpleEnumTwo  SimpleEnum = 2
)

// EnumWithPayload is a sum type with 24 variants.
type EnumWithPayload struct {
	Tag     uint8
	U8Val   uint8
	U16Val  uint16
	U32Val  uint32
	U64Val  uint64
	U128Val types.U128
	U256Val types.U256
	I8Val   int8
	I16Val  int16
	I32Val  int32
	I64Val  int64
	I128Val types.I128
	I256Val types.I256
	BoolVal bool
	F32Val  float32
	F64Val  float64
	StrVal  string
	IdVal   types.Identity
	ConnVal types.ConnectionId
	TsVal   types.Timestamp
	UuidVal types.Uuid
	BytesVal       []byte
	IntsVal        []int32
	StringsVal     []string
	SimpleEnumsVal []SimpleEnum
}

const (
	EWPTagU8           uint8 = 0
	EWPTagU16          uint8 = 1
	EWPTagU32          uint8 = 2
	EWPTagU64          uint8 = 3
	EWPTagU128         uint8 = 4
	EWPTagU256         uint8 = 5
	EWPTagI8           uint8 = 6
	EWPTagI16          uint8 = 7
	EWPTagI32          uint8 = 8
	EWPTagI64          uint8 = 9
	EWPTagI128         uint8 = 10
	EWPTagI256         uint8 = 11
	EWPTagBool         uint8 = 12
	EWPTagF32          uint8 = 13
	EWPTagF64          uint8 = 14
	EWPTagStr          uint8 = 15
	EWPTagIdentity     uint8 = 16
	EWPTagConnectionId uint8 = 17
	EWPTagTimestamp    uint8 = 18
	EWPTagUuid         uint8 = 19
	EWPTagBytes        uint8 = 20
	EWPTagInts         uint8 = 21
	EWPTagStrings      uint8 = 22
	EWPTagSimpleEnums  uint8 = 23
)

type UnitStruct struct{}

type ByteStruct struct {
	B uint8
}

type EveryPrimitiveStruct struct {
	A uint8
	B uint16
	C uint32
	D uint64
	E types.U128
	F types.U256
	G int8
	H int16
	I int32
	J int64
	K types.I128
	L types.I256
	M bool
	N float32
	O float64
	P string
	Q types.Identity
	R types.ConnectionId
	S types.Timestamp
	T types.TimeDuration
	U types.Uuid
}

type EveryVecStruct struct {
	A []uint8
	B []uint16
	C []uint32
	D []uint64
	E []types.U128
	F []types.U256
	G []int8
	H []int16
	I []int32
	J []int64
	K []types.I128
	L []types.I256
	M []bool
	N []float32
	O []float64
	P []string
	Q []types.Identity
	R []types.ConnectionId
	S []types.Timestamp
	T []types.TimeDuration
	U []types.Uuid
}

// ── One* Row types ───────────────────────────────────────────────────────────

type OneU8 struct{ N uint8 }
type OneU16 struct{ N uint16 }
type OneU32 struct{ N uint32 }
type OneU64 struct{ N uint64 }
type OneU128 struct{ N types.U128 }
type OneU256 struct{ N types.U256 }
type OneI8 struct{ N int8 }
type OneI16 struct{ N int16 }
type OneI32 struct{ N int32 }
type OneI64 struct{ N int64 }
type OneI128 struct{ N types.I128 }
type OneI256 struct{ N types.I256 }
type OneBool struct{ B bool }
type OneF32 struct{ F float32 }
type OneF64 struct{ F float64 }
type OneString struct{ S string }
type OneIdentity struct{ I types.Identity }
type OneConnectionId struct{ A types.ConnectionId }
type OneUuid struct{ U types.Uuid }
type OneTimestamp struct{ T types.Timestamp }
type OneSimpleEnum struct{ E SimpleEnum }
type OneEnumWithPayload struct{ E EnumWithPayload }
type OneUnitStruct struct{ S UnitStruct }
type OneByteStruct struct{ S ByteStruct }
type OneEveryPrimitiveStruct struct{ S EveryPrimitiveStruct }
type OneEveryVecStruct struct{ S EveryVecStruct }

// ── Vec* Row types ───────────────────────────────────────────────────────────

type VecU8 struct{ N []uint8 }
type VecU16 struct{ N []uint16 }
type VecU32 struct{ N []uint32 }
type VecU64 struct{ N []uint64 }
type VecU128 struct{ N []types.U128 }
type VecU256 struct{ N []types.U256 }
type VecI8 struct{ N []int8 }
type VecI16 struct{ N []int16 }
type VecI32 struct{ N []int32 }
type VecI64 struct{ N []int64 }
type VecI128 struct{ N []types.I128 }
type VecI256 struct{ N []types.I256 }
type VecBool struct{ B []bool }
type VecF32 struct{ F []float32 }
type VecF64 struct{ F []float64 }
type VecString struct{ S []string }
type VecIdentity struct{ I []types.Identity }
type VecConnectionId struct{ A []types.ConnectionId }
type VecUuid struct{ U []types.Uuid }
type VecTimestamp struct{ T []types.Timestamp }
type VecSimpleEnum struct{ E []SimpleEnum }
type VecEnumWithPayload struct{ E []EnumWithPayload }
type VecUnitStruct struct{ S []UnitStruct }
type VecByteStruct struct{ S []ByteStruct }
type VecEveryPrimitiveStruct struct{ S []EveryPrimitiveStruct }
type VecEveryVecStruct struct{ S []EveryVecStruct }

// ── Option* Row types ────────────────────────────────────────────────────────

type OptionI32Row struct{ N *int32 }
type OptionStringRow struct{ S *string }
type OptionIdentityRow struct{ I *types.Identity }
type OptionUuidRow struct{ U *types.Uuid }
type OptionSimpleEnumRow struct{ E *SimpleEnum }
type OptionEveryPrimitiveStructRow struct{ S *EveryPrimitiveStruct }
type OptionVecOptionI32Row struct{ V *[]*int32 }

// ── Result* Row types ────────────────────────────────────────────────────────

type ResultI32StringRow struct {
	IsOk   bool
	OkVal  int32
	ErrVal string
}

type ResultStringI32Row struct {
	IsOk   bool
	OkVal  string
	ErrVal int32
}

type ResultIdentityStringRow struct {
	IsOk   bool
	OkVal  types.Identity
	ErrVal string
}

type ResultSimpleEnumI32Row struct {
	IsOk   bool
	OkVal  SimpleEnum
	ErrVal int32
}

type ResultEveryPrimitiveStructStringRow struct {
	IsOk   bool
	OkVal  EveryPrimitiveStruct
	ErrVal string
}

type ResultVecI32StringRow struct {
	IsOk   bool
	OkVal  []int32
	ErrVal string
}

// ── Unique* Row types ────────────────────────────────────────────────────────

type UniqueU8 struct {
	N    uint8
	Data int32
}
type UniqueU16 struct {
	N    uint16
	Data int32
}
type UniqueU32 struct {
	N    uint32
	Data int32
}
type UniqueU64 struct {
	N    uint64
	Data int32
}
type UniqueU128 struct {
	N    types.U128
	Data int32
}
type UniqueU256 struct {
	N    types.U256
	Data int32
}
type UniqueI8 struct {
	N    int8
	Data int32
}
type UniqueI16 struct {
	N    int16
	Data int32
}
type UniqueI32 struct {
	N    int32
	Data int32
}
type UniqueI64 struct {
	N    int64
	Data int32
}
type UniqueI128 struct {
	N    types.I128
	Data int32
}
type UniqueI256 struct {
	N    types.I256
	Data int32
}
type UniqueBool struct {
	B    bool
	Data int32
}
type UniqueString struct {
	S    string
	Data int32
}
type UniqueIdentity struct {
	I    types.Identity
	Data int32
}
type UniqueConnectionId struct {
	A    types.ConnectionId
	Data int32
}
type UniqueUuid struct {
	U    types.Uuid
	Data int32
}

// ── PK* Row types ─────────────────────────────────────────────────────────────

type PkU8 struct {
	N    uint8
	Data int32
}
type PkU16 struct {
	N    uint16
	Data int32
}
type PkU32 struct {
	N    uint32
	Data int32
}
type PkU32Two struct {
	N    uint32
	Data int32
}
type PkU64 struct {
	N    uint64
	Data int32
}
type PkU128 struct {
	N    types.U128
	Data int32
}
type PkU256 struct {
	N    types.U256
	Data int32
}
type PkI8 struct {
	N    int8
	Data int32
}
type PkI16 struct {
	N    int16
	Data int32
}
type PkI32 struct {
	N    int32
	Data int32
}
type PkI64 struct {
	N    int64
	Data int32
}
type PkI128 struct {
	N    types.I128
	Data int32
}
type PkI256 struct {
	N    types.I256
	Data int32
}
type PkBool struct {
	B    bool
	Data int32
}
type PkString struct {
	S    string
	Data int32
}
type PkIdentity struct {
	I    types.Identity
	Data int32
}
type PkConnectionId struct {
	A    types.ConnectionId
	Data int32
}
type PkUuid struct {
	U    types.Uuid
	Data int32
}
type PkSimpleEnum struct {
	A    SimpleEnum
	Data int32
}

// ── Special table Row types ──────────────────────────────────────────────────

type LargeTable struct {
	A uint8
	B uint16
	C uint32
	D uint64
	E types.U128
	F types.U256
	G int8
	H int16
	I int32
	J int64
	K types.I128
	L types.I256
	M bool
	N float32
	O float64
	P string
	Q SimpleEnum
	R EnumWithPayload
	S UnitStruct
	T ByteStruct
	U EveryPrimitiveStruct
	V EveryVecStruct
}

type TableHoldsTable struct {
	A OneU8
	B VecU8
}

type ScheduledTable struct {
	ScheduledId uint64
	ScheduledAt types.ScheduleAt
	Text        string
}

type IndexedTable struct {
	PlayerId uint32
}

type IndexedTable2 struct {
	PlayerId    uint32
	PlayerSnazz float32
}

type BTreeU32Row struct {
	N    uint32
	Data int32
}

type UsersRow struct {
	Identity types.Identity
	Name     string
}

type IndexedSimpleEnumRow struct {
	N SimpleEnum
}
