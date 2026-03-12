//go:build tinygo

package spacetimedb

import (
	"github.com/clockworklabs/spacetimedb-go/bsatn"
	"github.com/clockworklabs/spacetimedb-go/types"
	"github.com/clockworklabs/spacetimedb-go-server/sys"
)

// ── WASM export ───────────────────────────────────────────────────────────────

// __describe_module__ is called by the SpacetimeDB host to obtain the module's
// schema, encoded as a BSATN-serialized RawModuleDefV10.
//
//export __describe_module__
func describeModule(sink sys.BytesSink) {
	data := buildModuleDefBSATN()
	_ = sys.WriteBytesToSink(sink, data)
}

// ── BSATN serialization ───────────────────────────────────────────────────────

// buildModuleDefBSATN returns the BSATN encoding of RawModuleDef (the outer enum)
// wrapping a RawModuleDefV10 built from the current registries.
// The host deserializes this as RawModuleDef, so the V10 variant tag (2) must come first.
func buildModuleDefBSATN() []byte {
	w := bsatn.NewWriter()

	// Outer enum: RawModuleDef::V10 = variant tag 2
	w.WriteVariantTag(2)

	// Build the typespace: one ProductType per registered table, then any explicitly
	// registered custom types from RegisterTypespaceType.
	typespaceTypes := make([]types.AlgebraicType, 0, len(tableRegistry)+len(typespaceExtRegistry))
	for _, t := range tableRegistry {
		elems := make([]types.ProductTypeElement, len(t.Columns))
		for j, col := range t.Columns {
			name := col.Name
			elems[j] = types.ProductTypeElement{Name: &name, Type: col.Type}
		}
		typespaceTypes = append(typespaceTypes, types.ProductType{Elements: elems})
	}
	typespaceTypes = append(typespaceTypes, typespaceExtRegistry...)

	// Collect internal function names (lifecycle + schedule reducers)
	// and force their visibility to Private, matching the C# SDK behavior.
	internalFunctions := make(map[string]bool)
	for _, lc := range lifecycleRegistry {
		internalFunctions[lc.Reducer] = true
	}
	for _, s := range scheduleRegistry {
		internalFunctions[s.ReducerName] = true
	}
	for i := range reducerRegistry {
		if internalFunctions[reducerRegistry[i].Name] {
			reducerRegistry[i].Visibility = ReducerVisibilityPrivate
		}
	}
	for i := range procedureRegistry {
		if internalFunctions[procedureRegistry[i].Name] {
			procedureRegistry[i].Visibility = ReducerVisibilityPrivate
		}
	}

	// Count sections to emit.
	numSections := 3 // Typespace + Tables + Reducers
	if len(typeRegistry) > 0 {
		numSections++
	}
	if len(procedureRegistry) > 0 {
		numSections++
	}
	if len(viewRegistry) > 0 {
		numSections++
	}
	if len(scheduleRegistry) > 0 {
		numSections++
	}
	if len(lifecycleRegistry) > 0 {
		numSections++
	}
	if len(rlsRegistry) > 0 {
		numSections++
	}
	if caseConversionPolicy != nil {
		numSections++
	}
	if len(explicitNameRegistry) > 0 {
		numSections++
	}

	// RawModuleDefV10 is a product type: its only field is sections Vec<...>.
	// As a ProductValue, the fields are concatenated without any outer wrapper.
	w.WriteArrayLen(uint32(numSections))

	// Section: Typespace (variant tag 0).
	w.WriteVariantTag(0)
	w.WriteArrayLen(uint32(len(typespaceTypes)))
	for _, t := range typespaceTypes {
		types.WriteAlgebraicType(w, t)
	}

	// Section: Types (variant tag 1), only if any named types are registered.
	if len(typeRegistry) > 0 {
		w.WriteVariantTag(1)
		w.WriteArrayLen(uint32(len(typeRegistry)))
		for _, td := range typeRegistry {
			writeTypeDef(w, td)
		}
	}

	// Section: Tables (variant tag 2).
	w.WriteVariantTag(2)
	w.WriteArrayLen(uint32(len(tableRegistry)))
	for i, t := range tableRegistry {
		writeTableDef(w, t, uint32(i))
	}

	// Section: Reducers (variant tag 3).
	w.WriteVariantTag(3)
	w.WriteArrayLen(uint32(len(reducerRegistry)))
	for _, r := range reducerRegistry {
		writeReducerDef(w, r)
	}

	// Section: Procedures (variant tag 4), only if any are registered.
	if len(procedureRegistry) > 0 {
		w.WriteVariantTag(4)
		w.WriteArrayLen(uint32(len(procedureRegistry)))
		for _, p := range procedureRegistry {
			writeProcedureDef(w, p)
		}
	}

	// Section: Views (variant tag 5), only if any are registered.
	if len(viewRegistry) > 0 {
		w.WriteVariantTag(5)
		w.WriteArrayLen(uint32(len(viewRegistry)))
		for i, v := range viewRegistry {
			writeViewDef(w, v, uint32(i))
		}
	}

	// Section: Schedules (variant tag 6), only if any are registered.
	if len(scheduleRegistry) > 0 {
		w.WriteVariantTag(6)
		w.WriteArrayLen(uint32(len(scheduleRegistry)))
		for _, s := range scheduleRegistry {
			writeScheduleDef(w, s)
		}
	}

	// Section: LifeCycleReducers (variant tag 7), only if any are registered.
	if len(lifecycleRegistry) > 0 {
		w.WriteVariantTag(7)
		w.WriteArrayLen(uint32(len(lifecycleRegistry)))
		for _, lc := range lifecycleRegistry {
			writeLifecycleDef(w, lc)
		}
	}

	// Section: RowLevelSecurity (variant tag 8), only if any policies are registered.
	if len(rlsRegistry) > 0 {
		w.WriteVariantTag(8)
		w.WriteArrayLen(uint32(len(rlsRegistry)))
		for _, rls := range rlsRegistry {
			writeRLSDef(w, rls)
		}
	}

	// Section: CaseConversionPolicy (variant tag 9), only if explicitly set.
	if caseConversionPolicy != nil {
		w.WriteVariantTag(9)
		w.WriteVariantTag(uint8(*caseConversionPolicy))
	}

	// Section: ExplicitNames (variant tag 10), only if any mappings are registered.
	if len(explicitNameRegistry) > 0 {
		w.WriteVariantTag(10)
		w.WriteArrayLen(uint32(len(explicitNameRegistry)))
		for _, e := range explicitNameRegistry {
			writeExplicitNameEntry(w, e)
		}
	}

	return w.Bytes()
}

// writeTableDef serializes a RawTableDefV10 value (field order must match Rust struct).
func writeTableDef(w *bsatn.Writer, t TableDef, typeRef uint32) {
	// source_name: RawIdentifier (String)
	w.WriteString(t.Name)
	// product_type_ref: AlgebraicTypeRef (u32)
	w.WriteU32(typeRef)
	// primary_key: ColList (Array<u16>)
	w.WriteArrayLen(uint32(len(t.PrimaryKey)))
	for _, col := range t.PrimaryKey {
		w.WriteU16(col)
	}
	// indexes: Vec<RawIndexDefV10>
	w.WriteArrayLen(uint32(len(t.Indexes)))
	for _, idx := range t.Indexes {
		writeIndexDef(w, idx)
	}
	// constraints: Vec<RawConstraintDefV10>
	w.WriteArrayLen(uint32(len(t.Constraints)))
	for _, c := range t.Constraints {
		writeConstraintDef(w, c)
	}
	// sequences: Vec<RawSequenceDefV10>
	w.WriteArrayLen(uint32(len(t.Sequences)))
	for _, s := range t.Sequences {
		writeSequenceDef(w, s)
	}
	// table_type: TableType — always User (tag 1) for module-defined tables
	w.WriteVariantTag(1)
	// table_access: TableAccess
	w.WriteVariantTag(uint8(t.Access))
	// default_values: Vec<RawColumnDefaultValueV10>
	w.WriteArrayLen(uint32(len(t.DefaultValues)))
	for _, dv := range t.DefaultValues {
		w.WriteU16(dv.ColId)
		w.WriteArrayLen(uint32(len(dv.Value)))
		w.WriteRaw(dv.Value)
	}
	// is_event: bool
	w.WriteBool(t.IsEvent)
}

// writeIndexDef serializes a RawIndexDefV10 value.
func writeIndexDef(w *bsatn.Writer, idx IndexDef) {
	writeOptString(w, idx.SourceName)
	writeOptString(w, idx.AccessorName)
	switch idx.Algorithm {
	case IndexAlgorithmBTree:
		w.WriteVariantTag(0)
		w.WriteArrayLen(uint32(len(idx.Columns)))
		for _, col := range idx.Columns {
			w.WriteU16(col)
		}
	case IndexAlgorithmHash:
		w.WriteVariantTag(1)
		w.WriteArrayLen(uint32(len(idx.Columns)))
		for _, col := range idx.Columns {
			w.WriteU16(col)
		}
	case IndexAlgorithmDirect:
		w.WriteVariantTag(2)
		if len(idx.Columns) > 0 {
			w.WriteU16(idx.Columns[0])
		}
	}
}

// writeConstraintDef serializes a RawConstraintDefV10 value.
func writeConstraintDef(w *bsatn.Writer, c ConstraintDef) {
	writeOptString(w, c.SourceName)
	// data: RawConstraintDataV9 — only Unique variant (tag 0) is supported
	w.WriteVariantTag(0)
	// RawUniqueConstraintDataV9 { columns: ColList }
	w.WriteArrayLen(uint32(len(c.Columns)))
	for _, col := range c.Columns {
		w.WriteU16(col)
	}
}

// writeSequenceDef serializes a RawSequenceDefV10 value.
func writeSequenceDef(w *bsatn.Writer, s SequenceDef) {
	writeOptString(w, s.SourceName)
	// column: ColId (u16)
	w.WriteU16(s.Column)
	// start: Option<i128>
	if s.Start == nil {
		w.WriteVariantTag(1) // None
	} else {
		w.WriteVariantTag(0) // Some
		v := *s.Start
		w.WriteI128(uint64(v), int64(v)>>63)
	}
	// min_value: Option<i128> — None
	w.WriteVariantTag(1)
	// max_value: Option<i128> — None
	w.WriteVariantTag(1)
	// increment: i128
	w.WriteI128(uint64(s.Increment), int64(s.Increment)>>63)
}

// writeReducerDef serializes a RawReducerDefV10 value.
func writeReducerDef(w *bsatn.Writer, r ReducerDef) {
	// source_name: RawIdentifier
	w.WriteString(r.Name)
	// params: ProductType (inline — NOT registered in typespace)
	w.WriteArrayLen(uint32(len(r.Params)))
	for _, p := range r.Params {
		name := p.Name
		writeOptString(w, &name)
		types.WriteAlgebraicType(w, p.Type)
	}
	// visibility: FunctionVisibility
	w.WriteVariantTag(uint8(r.Visibility))
	// ok_return_type: AlgebraicType — empty ProductType (unit)
	types.WriteAlgebraicType(w, types.ProductType{})
	// err_return_type: AlgebraicType — String
	types.WriteAlgebraicType(w, types.AlgebraicString)
}

// writeTypeDef serializes a RawTypeDefV10 value.
func writeTypeDef(w *bsatn.Writer, td TypeDef) {
	// source_name: RawScopedTypeNameV10 { scope: Vec<String>, name: String }
	w.WriteArrayLen(uint32(len(td.Scope)))
	for _, s := range td.Scope {
		w.WriteString(s)
	}
	w.WriteString(td.Name)
	// ty: AlgebraicTypeRef (u32)
	w.WriteU32(td.TypeRef)
	// custom_ordering: bool
	w.WriteBool(td.CustomOrdering)
}

// writeScheduleDef serializes a RawScheduleDefV10 value.
func writeScheduleDef(w *bsatn.Writer, s ScheduleDef) {
	// source_name: Option<RawIdentifier>
	writeOptString(w, s.SourceName)
	// table_name: RawIdentifier
	w.WriteString(s.TableName)
	// schedule_at_col: ColId (u16)
	w.WriteU16(s.ScheduleAtCol)
	// function_name: RawIdentifier
	w.WriteString(s.ReducerName)
}

// writeLifecycleDef serializes a RawLifeCycleReducerDefV10 value.
func writeLifecycleDef(w *bsatn.Writer, lc LifecycleDef) {
	// lifecycle_spec: Lifecycle (SumType: Init=0, OnConnect=1, OnDisconnect=2)
	w.WriteVariantTag(uint8(lc.Kind))
	// function_name: RawIdentifier
	w.WriteString(lc.Reducer)
}

// writeExplicitNameEntry serializes an ExplicitNameEntry (ExplicitNameEntry enum variant).
func writeExplicitNameEntry(w *bsatn.Writer, e ExplicitNameEntry) {
	// variant tag: Table=0, Function=1, Index=2
	w.WriteVariantTag(uint8(e.Kind))
	// NameMapping { source_name, canonical_name }
	w.WriteString(e.SourceName)
	w.WriteString(e.CanonicalName)
}

// writeRLSDef serializes a RawRowLevelSecurityDefV10 value.
func writeRLSDef(w *bsatn.Writer, rls RLSDef) {
	// sql: RawSql (String)
	w.WriteString(rls.SQL)
}

// writeOptString encodes an Option<String> using SpacetimeDB BSATN convention:
// Some(s) → tag 0 + string bytes; None → tag 1.
func writeOptString(w *bsatn.Writer, s *string) {
	if s == nil {
		w.WriteVariantTag(1) // None
	} else {
		w.WriteVariantTag(0) // Some
		w.WriteString(*s)
	}
}
