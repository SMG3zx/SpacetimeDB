//go:build tinygo

package spacetimedb

import "github.com/clockworklabs/spacetimedb-go/types"

// ── Module-level registries ───────────────────────────────────────────────────

// Module-level registries accumulate definitions from generated init() code.
// At module load time, __describe_module__ serializes these registries into
// a BSATN-encoded RawModuleDefV10 for the SpacetimeDB host.
var (
	tableRegistry        []TableDef              // all tables in this module
	reducerRegistry      []ReducerDef            // all reducers (client-callable + internal)
	lifecycleRegistry    []LifecycleDef          // lifecycle hooks (init, connect, disconnect)
	scheduleRegistry     []ScheduleDef           // scheduled reducer registrations
	typeRegistry         []TypeDef               // named type exports for client codegen
	typespaceExtRegistry []types.AlgebraicType   // custom algebraic types in the typespace
	rlsRegistry          []RLSDef                // row-level security policies
	explicitNameRegistry []ExplicitNameEntry     // explicit source→canonical name mappings
	caseConversionPolicy *CaseConversionPolicy   // optional module-wide case conversion override
)

// RegisterTableDef adds a table descriptor to the module registry.
// Call this from package-level init() functions in generated bindings.
func RegisterTableDef(def TableDef) {
	tableRegistry = append(tableRegistry, def)
}

// RegisterReducerDef adds a reducer descriptor to the module registry.
// Call this from package-level init() functions in generated bindings.
func RegisterReducerDef(def ReducerDef) {
	reducerRegistry = append(reducerRegistry, def)
}

// RegisterLifecycleDef assigns a reducer to a lifecycle event.
func RegisterLifecycleDef(def LifecycleDef) {
	lifecycleRegistry = append(lifecycleRegistry, def)
}

// RegisterScheduleDef registers a scheduled reducer.
// The table referenced by def.TableName must have a ScheduleAt column at def.ScheduleAtCol.
func RegisterScheduleDef(def ScheduleDef) {
	scheduleRegistry = append(scheduleRegistry, def)
}

// SetCaseConversionPolicy sets the module-wide case conversion policy.
// Overrides the default (SnakeCase). Call from an init() function.
func SetCaseConversionPolicy(policy CaseConversionPolicy) {
	caseConversionPolicy = &policy
}

// RegisterExplicitName adds an explicit source->canonical name mapping for an entity.
func RegisterExplicitName(entry ExplicitNameEntry) {
	explicitNameRegistry = append(explicitNameRegistry, entry)
}

// RegisterRLSDef registers a row-level security policy.
// Only one RLS policy can be active per table; registering multiple policies is additive.
func RegisterRLSDef(def RLSDef) {
	rlsRegistry = append(rlsRegistry, def)
}

// RegisterTypeDef exports a named type for client code generation.
// The TypeRef must be a valid index into the module's typespace (assigned by the host
// from the Typespace section). Use this to give names to types referenced in table columns.
func RegisterTypeDef(def TypeDef) {
	typeRegistry = append(typeRegistry, def)
}

// RegisterTypespaceType appends an AlgebraicType to the module's typespace.
// The returned index can be used as the TypeRef in a TypeDef or as a types.RefType{Ref: n}
// in column/parameter definitions that reference this type.
// Indices start after the automatically-added table row types (0..len(tables)-1).
func RegisterTypespaceType(at types.AlgebraicType) uint32 {
	idx := uint32(len(typespaceExtRegistry))
	typespaceExtRegistry = append(typespaceExtRegistry, at)
	return idx
}
