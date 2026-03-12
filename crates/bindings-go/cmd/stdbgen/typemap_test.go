package main

import (
	"strings"
	"testing"
)

// ── algebraicType ────────────────────────────────────────────────────────────

func TestAlgebraicType(t *testing.T) {
	// All direct map entries
	directTests := []struct {
		in, want string
	}{
		{"String", "types.AlgebraicString"},
		{"Bool", "types.AlgebraicBool"},
		{"I8", "types.AlgebraicI8"},
		{"U8", "types.AlgebraicU8"},
		{"I16", "types.AlgebraicI16"},
		{"U16", "types.AlgebraicU16"},
		{"I32", "types.AlgebraicI32"},
		{"U32", "types.AlgebraicU32"},
		{"I64", "types.AlgebraicI64"},
		{"U64", "types.AlgebraicU64"},
		{"I128", "types.AlgebraicI128"},
		{"U128", "types.AlgebraicU128"},
		{"F32", "types.AlgebraicF32"},
		{"F64", "types.AlgebraicF64"},
		{"Bytes", "types.AlgebraicBytes"},
		{"Identity", "algebraicIdentity"},
		{"Timestamp", "types.AlgebraicTimestamp"},
	}
	for _, tt := range directTests {
		if got := algebraicType(tt.in); got != tt.want {
			t.Errorf("algebraicType(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}

	// Option<T> wrapping
	optU32 := algebraicType("Option<U32>")
	if !strings.Contains(optU32, "SumType") {
		t.Errorf("algebraicType(\"Option<U32>\") should contain SumType, got %q", optU32)
	}
	if !strings.Contains(optU32, "some") || !strings.Contains(optU32, "none") {
		t.Errorf("algebraicType(\"Option<U32>\") should contain some/none variants, got %q", optU32)
	}
	if !strings.Contains(optU32, "types.AlgebraicU32") {
		t.Errorf("algebraicType(\"Option<U32>\") should contain inner type AlgebraicU32, got %q", optU32)
	}

	// Nested Option<Option<String>>
	nested := algebraicType("Option<Option<String>>")
	if !strings.Contains(nested, "SumType") {
		t.Errorf("algebraicType(\"Option<Option<String>>\") should contain SumType, got %q", nested)
	}

	// Unknown type falls back to AlgebraicString
	if got := algebraicType("UnknownType"); got != "types.AlgebraicString" {
		t.Errorf("algebraicType(\"UnknownType\") = %q, want \"types.AlgebraicString\"", got)
	}
}

// ── goTypeOf ─────────────────────────────────────────────────────────────────

func TestGoTypeOf(t *testing.T) {
	directTests := []struct {
		in, want string
	}{
		{"String", "string"},
		{"Bool", "bool"},
		{"I8", "int8"},
		{"U8", "uint8"},
		{"I16", "int16"},
		{"U16", "uint16"},
		{"I32", "int32"},
		{"U32", "uint32"},
		{"I64", "int64"},
		{"U64", "uint64"},
		{"F32", "float32"},
		{"F64", "float64"},
		{"Bytes", "[]byte"},
		{"Identity", "types.Identity"},
		{"Timestamp", "types.Timestamp"},
	}
	for _, tt := range directTests {
		if got := goTypeOf(tt.in); got != tt.want {
			t.Errorf("goTypeOf(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}

	// Option<T> → pointer
	if got := goTypeOf("Option<U32>"); got != "*uint32" {
		t.Errorf("goTypeOf(\"Option<U32>\") = %q, want \"*uint32\"", got)
	}
	if got := goTypeOf("Option<Identity>"); got != "*types.Identity" {
		t.Errorf("goTypeOf(\"Option<Identity>\") = %q, want \"*types.Identity\"", got)
	}

	// Nested Option<Option<String>> → **string
	if got := goTypeOf("Option<Option<String>>"); got != "**string" {
		t.Errorf("goTypeOf(\"Option<Option<String>>\") = %q, want \"**string\"", got)
	}

	// Unknown type fallback
	if got := goTypeOf("UnknownType"); got != "string" {
		t.Errorf("goTypeOf(\"UnknownType\") = %q, want \"string\"", got)
	}
}

// ── readMethodOf ─────────────────────────────────────────────────────────────

func TestReadMethodOf(t *testing.T) {
	directTests := []struct {
		in, want string
	}{
		{"String", "r.ReadString()"},
		{"Bool", "r.ReadBool()"},
		{"I8", "r.ReadI8()"},
		{"U8", "r.ReadU8()"},
		{"I16", "r.ReadI16()"},
		{"U16", "r.ReadU16()"},
		{"I32", "r.ReadI32()"},
		{"U32", "r.ReadU32()"},
		{"I64", "r.ReadI64()"},
		{"U64", "r.ReadU64()"},
		{"F32", "r.ReadF32()"},
		{"F64", "r.ReadF64()"},
		{"Bytes", "r.ReadBytes()"},
		{"Identity", "types.ReadIdentity(r)"},
		{"Timestamp", "types.ReadTimestamp(r)"},
	}
	for _, tt := range directTests {
		if got := readMethodOf(tt.in); got != tt.want {
			t.Errorf("readMethodOf(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}

	// Option<T>
	optU32 := readMethodOf("Option<U32>")
	if !strings.Contains(optU32, "bsatn.ReadOption") {
		t.Errorf("readMethodOf(\"Option<U32>\") should contain bsatn.ReadOption, got %q", optU32)
	}
	if !strings.Contains(optU32, "uint32") {
		t.Errorf("readMethodOf(\"Option<U32>\") should contain uint32, got %q", optU32)
	}
	if !strings.Contains(optU32, "r.ReadU32()") {
		t.Errorf("readMethodOf(\"Option<U32>\") should contain r.ReadU32(), got %q", optU32)
	}

	// Option<Identity>
	optId := readMethodOf("Option<Identity>")
	if !strings.Contains(optId, "types.Identity") {
		t.Errorf("readMethodOf(\"Option<Identity>\") should contain types.Identity, got %q", optId)
	}
	if !strings.Contains(optId, "types.ReadIdentity(r)") {
		t.Errorf("readMethodOf(\"Option<Identity>\") should contain types.ReadIdentity(r), got %q", optId)
	}

	// Unknown type fallback
	if got := readMethodOf("UnknownType"); got != "r.ReadString()" {
		t.Errorf("readMethodOf(\"UnknownType\") = %q, want \"r.ReadString()\"", got)
	}
}

// ── writeMethodOf ────────────────────────────────────────────────────────────

func TestWriteMethodOf(t *testing.T) {
	directTests := []struct {
		in, want string
	}{
		{"String", "w.WriteString"},
		{"Bool", "w.WriteBool"},
		{"I8", "w.WriteI8"},
		{"U8", "w.WriteU8"},
		{"I16", "w.WriteI16"},
		{"U16", "w.WriteU16"},
		{"I32", "w.WriteI32"},
		{"U32", "w.WriteU32"},
		{"I64", "w.WriteI64"},
		{"U64", "w.WriteU64"},
		{"F32", "w.WriteF32"},
		{"F64", "w.WriteF64"},
		{"Bytes", "w.WriteBytes"},
	}
	for _, tt := range directTests {
		if got := writeMethodOf(tt.in); got != tt.want {
			t.Errorf("writeMethodOf(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}

	// Types not in writeMethod map fallback to WriteString
	if got := writeMethodOf("Identity"); got != "w.WriteString" {
		t.Errorf("writeMethodOf(\"Identity\") = %q, want \"w.WriteString\"", got)
	}
	if got := writeMethodOf("UnknownType"); got != "w.WriteString" {
		t.Errorf("writeMethodOf(\"UnknownType\") = %q, want \"w.WriteString\"", got)
	}
}

// ── specialWriteOf ───────────────────────────────────────────────────────────

func TestSpecialWriteOf(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"Identity", ".WriteBsatn(w)"},
		{"Timestamp", ".WriteBsatn(w)"},
		{"U32", ""},
		{"String", ""},
		{"UnknownType", ""},
	}
	for _, tt := range tests {
		if got := specialWriteOf(tt.in); got != tt.want {
			t.Errorf("specialWriteOf(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// ── customAlgebraicTypeOf ────────────────────────────────────────────────────

func TestCustomAlgebraicTypeOf_SumType(t *testing.T) {
	te := TypeExport{
		Name: "Status",
		Sum: []SumVariant{
			{Name: "active", Type: "U32"},
			{Name: "inactive", Type: ""},
			{Name: "pending", Type: "String"},
		},
	}
	got := customAlgebraicTypeOf(te)

	if !strings.HasPrefix(got, "types.SumType{Variants: []types.SumTypeVariant{") {
		t.Errorf("customAlgebraicTypeOf sum type should start with SumType prefix, got %q", got)
	}
	if !strings.Contains(got, `"active"`) {
		t.Errorf("should contain active variant name, got %q", got)
	}
	if !strings.Contains(got, "types.AlgebraicU32") {
		t.Errorf("should contain AlgebraicU32 for active variant, got %q", got)
	}
	if !strings.Contains(got, `"inactive"`) {
		t.Errorf("should contain inactive variant name, got %q", got)
	}
	if !strings.Contains(got, "types.ProductType{}") {
		t.Errorf("should contain ProductType{} for unit variant, got %q", got)
	}
	if !strings.Contains(got, `"pending"`) {
		t.Errorf("should contain pending variant name, got %q", got)
	}
	if !strings.Contains(got, "types.AlgebraicString") {
		t.Errorf("should contain AlgebraicString for pending variant, got %q", got)
	}
}

func TestCustomAlgebraicTypeOf_ProductType(t *testing.T) {
	te := TypeExport{
		Name: "Point",
		Product: []Column{
			{Name: "x", Type: "F32"},
			{Name: "y", Type: "F32"},
		},
	}
	got := customAlgebraicTypeOf(te)

	if !strings.HasPrefix(got, "types.ProductType{Elements: []types.ProductTypeElement{") {
		t.Errorf("customAlgebraicTypeOf product type should start with ProductType prefix, got %q", got)
	}
	if !strings.Contains(got, `"x"`) {
		t.Errorf("should contain x field name, got %q", got)
	}
	if !strings.Contains(got, `"y"`) {
		t.Errorf("should contain y field name, got %q", got)
	}
	if !strings.Contains(got, "types.AlgebraicF32") {
		t.Errorf("should contain AlgebraicF32, got %q", got)
	}
}

// ── zeroValOf ────────────────────────────────────────────────────────────────

func TestZeroValOf(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"String", `""`},
		{"Bool", "false"},
		{"I8", "0"},
		{"U8", "0"},
		{"I16", "0"},
		{"U16", "0"},
		{"I32", "0"},
		{"U32", "0"},
		{"I64", "0"},
		{"U64", "0"},
		{"I128", "0"},
		{"U128", "0"},
		{"F32", "0.0"},
		{"F64", "0.0"},
		{"Bytes", "[]byte{}"},
		{"Identity", "types.Identity{}"},
		{"Timestamp", "types.Timestamp(0)"},
		{"Option<U32>", "nil"},
		{"Option<String>", "nil"},
		{"UnknownType", `""`},
	}
	for _, tt := range tests {
		if got := zeroValOf(tt.in); got != tt.want {
			t.Errorf("zeroValOf(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// ── isOptionType ─────────────────────────────────────────────────────────────

func TestIsOptionType(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"Option<U32>", true},
		{"Option<String>", true},
		{"Option<Identity>", true},
		{"U32", false},
		{"String", false},
		{"Option", false},
		{"Option<", false},
		{"<U32>", false},
	}
	for _, tt := range tests {
		if got := isOptionType(tt.in); got != tt.want {
			t.Errorf("isOptionType(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestCustomAlgebraicTypeOf_EmptyProduct(t *testing.T) {
	te := TypeExport{
		Name: "Empty",
	}
	got := customAlgebraicTypeOf(te)
	if !strings.Contains(got, "types.ProductType{Elements: []types.ProductTypeElement{") {
		t.Errorf("empty TypeExport should produce ProductType, got %q", got)
	}
}
