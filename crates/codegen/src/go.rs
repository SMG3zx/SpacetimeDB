//! Go client SDK code generation for SpacetimeDB modules.
//!
//! The generated files use the package name `module_bindings` and import from
//! `github.com/clockworklabs/spacetimedb-go`.

use std::ops::Deref;

use super::code_indenter::{CodeIndenter, Indenter};
use super::util::{
    collect_case, is_reducer_invokable, iter_reducers, iter_tables,
    print_auto_generated_file_comment, type_ref_name,
};
use crate::{CodegenOptions, Lang, OutputFile};
use convert_case::{Case, Casing};
use spacetimedb_lib::sats::layout::PrimitiveType;
use spacetimedb_schema::def::{ModuleDef, ReducerDef, TableDef, TypeDef};
use spacetimedb_schema::schema::TableSchema;
use spacetimedb_schema::type_for_generate::{
    AlgebraicTypeDef, AlgebraicTypeUse, PlainEnumTypeDef, ProductTypeDef, SumTypeDef,
};

const INDENT: &str = "\t";
const SDK_MODULE: &str = "github.com/clockworklabs/spacetimedb-go";

/// The Go codegen target.  Package name is always `module_bindings`.
pub struct Go;

impl Lang for Go {
    fn generate_type_files(&self, module: &ModuleDef, typ: &TypeDef) -> Vec<OutputFile> {
        let name = collect_case(Case::Pascal, typ.accessor_name.name_segments());
        let code = match &module.typespace_for_generate()[typ.ty] {
            AlgebraicTypeDef::Product(prod) => gen_product_type(module, &name, prod),
            AlgebraicTypeDef::Sum(sum) => gen_sum_type(module, &name, sum),
            AlgebraicTypeDef::PlainEnum(plain_enum) => gen_plain_enum(&name, plain_enum),
        };
        let filename = format!("types/{name}.go");
        vec![OutputFile { filename, code }]
    }

    fn generate_table_file_from_schema(
        &self,
        module: &ModuleDef,
        tbl: &TableDef,
        _schema: TableSchema,
    ) -> OutputFile {
        let table_name = tbl.accessor_name.deref().to_case(Case::Pascal);
        let row_type = type_ref_name(module, tbl.product_type_ref);
        let code = gen_table_file(&table_name, &row_type);
        OutputFile {
            filename: format!("{}_table.go", tbl.accessor_name.deref().to_case(Case::Snake)),
            code,
        }
    }

    fn generate_reducer_file(&self, module: &ModuleDef, reducer: &ReducerDef) -> OutputFile {
        let name = reducer.accessor_name.deref().to_case(Case::Pascal);
        let snake = reducer.accessor_name.deref().to_case(Case::Snake);
        let code = gen_reducer_file(module, reducer, &name);
        OutputFile {
            filename: format!("reducers/{snake}.go"),
            code,
        }
    }

    fn generate_procedure_file(
        &self,
        module: &ModuleDef,
        procedure: &spacetimedb_schema::def::ProcedureDef,
    ) -> OutputFile {
        let name = procedure.accessor_name.deref().to_case(Case::Pascal);
        let snake = procedure.accessor_name.deref().to_case(Case::Snake);
        let code = gen_procedure_file(module, procedure, &name);
        OutputFile {
            filename: format!("procedures/{snake}.go"),
            code,
        }
    }

    fn generate_global_files(&self, module: &ModuleDef, options: &CodegenOptions) -> Vec<OutputFile> {
        let code = gen_module_bindings(module, options);
        vec![OutputFile {
            filename: "module_bindings.go".to_string(),
            code,
        }]
    }
}

// ---------------------------------------------------------------------------
// Type mapping helpers
// ---------------------------------------------------------------------------

/// Returns the Go type name for an AlgebraicTypeUse.
fn go_type_name(module: &ModuleDef, ty: &AlgebraicTypeUse) -> String {
    match ty {
        AlgebraicTypeUse::Unit => "struct{}".to_string(),
        AlgebraicTypeUse::Never => "struct{}".to_string(),
        AlgebraicTypeUse::Primitive(p) => primitive_go_type(*p).to_string(),
        AlgebraicTypeUse::String => "string".to_string(),
        AlgebraicTypeUse::Array(elem) => format!("[]{}", go_type_name(module, elem)),
        AlgebraicTypeUse::Option(inner) => format!("*{}", go_type_name(module, inner)),
        AlgebraicTypeUse::Ref(r) => type_ref_name(module, *r),
        AlgebraicTypeUse::Identity => "types.Identity".to_string(),
        AlgebraicTypeUse::ConnectionId => "types.ConnectionId".to_string(),
        AlgebraicTypeUse::Timestamp => "types.Timestamp".to_string(),
        AlgebraicTypeUse::TimeDuration => "types.TimeDuration".to_string(),
        AlgebraicTypeUse::Uuid => "types.Uuid".to_string(),
        AlgebraicTypeUse::ScheduleAt => "ScheduleAt".to_string(),
        AlgebraicTypeUse::Result { .. } => "[]byte".to_string(),
    }
}

fn primitive_go_type(p: PrimitiveType) -> &'static str {
    match p {
        PrimitiveType::Bool => "bool",
        PrimitiveType::I8 => "int8",
        PrimitiveType::U8 => "uint8",
        PrimitiveType::I16 => "int16",
        PrimitiveType::U16 => "uint16",
        PrimitiveType::I32 => "int32",
        PrimitiveType::U32 => "uint32",
        PrimitiveType::I64 => "int64",
        PrimitiveType::U64 => "uint64",
        PrimitiveType::I128 => "types.I128",
        PrimitiveType::U128 => "types.U128",
        PrimitiveType::I256 => "types.I256",
        PrimitiveType::U256 => "types.U256",
        PrimitiveType::F32 => "float32",
        PrimitiveType::F64 => "float64",
    }
}

/// Returns the BSATN write expression for a value of the given type.
fn bsatn_write_expr(module: &ModuleDef, ty: &AlgebraicTypeUse, expr: &str) -> String {
    match ty {
        AlgebraicTypeUse::Primitive(p) => {
            let method = primitive_write_method(*p);
            format!("w.{method}({expr})")
        }
        AlgebraicTypeUse::String => format!("w.WriteString({expr})"),
        AlgebraicTypeUse::Array(elem) => {
            let elem_write = bsatn_write_expr(module, elem, "_elem");
            format!(
                "bsatn.WriteSlice(w, {expr}, func(w *bsatn.Writer, _elem {}) {{ {} }})",
                go_type_name(module, elem),
                elem_write
            )
        }
        AlgebraicTypeUse::Option(inner) => {
            let inner_write = bsatn_write_expr(module, inner, "*_opt");
            format!(
                "bsatn.WriteOption(w, {expr}, func(w *bsatn.Writer, _opt *{}) {{ {} }})",
                go_type_name(module, inner),
                inner_write
            )
        }
        AlgebraicTypeUse::Ref(_)
        | AlgebraicTypeUse::Identity
        | AlgebraicTypeUse::ConnectionId
        | AlgebraicTypeUse::Timestamp
        | AlgebraicTypeUse::TimeDuration
        | AlgebraicTypeUse::Uuid
        | AlgebraicTypeUse::ScheduleAt => {
            format!("{expr}.WriteBsatn(w)")
        }
        AlgebraicTypeUse::Unit | AlgebraicTypeUse::Never => String::new(),
        AlgebraicTypeUse::Result { .. } => format!("w.WriteBytes({expr})"),
    }
}

fn primitive_write_method(p: PrimitiveType) -> &'static str {
    match p {
        PrimitiveType::Bool => "WriteBool",
        PrimitiveType::I8 => "WriteI8",
        PrimitiveType::U8 => "WriteU8",
        PrimitiveType::I16 => "WriteI16",
        PrimitiveType::U16 => "WriteU16",
        PrimitiveType::I32 => "WriteI32",
        PrimitiveType::U32 => "WriteU32",
        PrimitiveType::I64 => "WriteI64",
        PrimitiveType::U64 => "WriteU64",
        PrimitiveType::I128 => "WriteI128",
        PrimitiveType::U128 => "WriteU128",
        PrimitiveType::I256 => "WriteI256",
        PrimitiveType::U256 => "WriteU256",
        PrimitiveType::F32 => "WriteF32",
        PrimitiveType::F64 => "WriteF64",
    }
}

/// Returns a Go expression that reads a value of the given type from `r *bsatn.Reader`.
fn bsatn_read_expr(module: &ModuleDef, ty: &AlgebraicTypeUse) -> String {
    match ty {
        AlgebraicTypeUse::Primitive(p) => {
            let method = primitive_read_method(*p);
            format!("r.{method}()")
        }
        AlgebraicTypeUse::String => "r.ReadString()".to_string(),
        AlgebraicTypeUse::Array(elem) => {
            let elem_type = go_type_name(module, elem);
            let elem_read = bsatn_read_expr(module, elem);
            format!(
                "bsatn.ReadSlice[{elem_type}](r, func(r *bsatn.Reader) ({elem_type}, error) {{ return {elem_read} }})"
            )
        }
        AlgebraicTypeUse::Option(inner) => {
            let inner_type = go_type_name(module, inner);
            let inner_read = bsatn_read_expr(module, inner);
            format!(
                "bsatn.ReadOption[{inner_type}](r, func(r *bsatn.Reader) ({inner_type}, error) {{ return {inner_read} }})"
            )
        }
        AlgebraicTypeUse::Identity => "types.ReadIdentity(r)".to_string(),
        AlgebraicTypeUse::ConnectionId => "types.ReadConnectionId(r)".to_string(),
        AlgebraicTypeUse::Timestamp => "types.ReadTimestamp(r)".to_string(),
        AlgebraicTypeUse::TimeDuration => "types.ReadTimeDuration(r)".to_string(),
        AlgebraicTypeUse::Uuid => "types.ReadUuid(r)".to_string(),
        AlgebraicTypeUse::Ref(r) => {
            let type_name = type_ref_name(module, *r);
            format!("Read{type_name}(r)")
        }
        AlgebraicTypeUse::ScheduleAt => "ReadScheduleAt(r)".to_string(),
        AlgebraicTypeUse::Unit | AlgebraicTypeUse::Never => "struct{}{}, nil".to_string(),
        AlgebraicTypeUse::Result { .. } => "r.ReadBytes()".to_string(),
    }
}

fn primitive_read_method(p: PrimitiveType) -> &'static str {
    match p {
        PrimitiveType::Bool => "ReadBool",
        PrimitiveType::I8 => "ReadI8",
        PrimitiveType::U8 => "ReadU8",
        PrimitiveType::I16 => "ReadI16",
        PrimitiveType::U16 => "ReadU16",
        PrimitiveType::I32 => "ReadI32",
        PrimitiveType::U32 => "ReadU32",
        PrimitiveType::I64 => "ReadI64",
        PrimitiveType::U64 => "ReadU64",
        PrimitiveType::I128 => "ReadI128",
        PrimitiveType::U128 => "ReadU128",
        PrimitiveType::I256 => "ReadI256",
        PrimitiveType::U256 => "ReadU256",
        PrimitiveType::F32 => "ReadF32",
        PrimitiveType::F64 => "ReadF64",
    }
}

fn needs_types_import(ty: &AlgebraicTypeUse) -> bool {
    match ty {
        AlgebraicTypeUse::Primitive(
            PrimitiveType::I128
            | PrimitiveType::U128
            | PrimitiveType::I256
            | PrimitiveType::U256,
        )
        | AlgebraicTypeUse::Identity
        | AlgebraicTypeUse::ConnectionId
        | AlgebraicTypeUse::Timestamp
        | AlgebraicTypeUse::TimeDuration
        | AlgebraicTypeUse::Uuid => true,
        AlgebraicTypeUse::Array(elem) => needs_types_import(elem),
        AlgebraicTypeUse::Option(inner) => needs_types_import(inner),
        _ => false,
    }
}

fn product_needs_types_import(prod: &ProductTypeDef) -> bool {
    prod.elements.iter().any(|(_, ty)| needs_types_import(ty))
}

// ---------------------------------------------------------------------------
// File header
// ---------------------------------------------------------------------------

fn write_file_header(out: &mut Indenter) {
    print_auto_generated_file_comment(out);
    writeln!(out, "package module_bindings");
    writeln!(out);
}

// ---------------------------------------------------------------------------
// Product type (struct)
// ---------------------------------------------------------------------------

fn gen_product_type(module: &ModuleDef, name: &str, prod: &ProductTypeDef) -> String {
    let mut output = CodeIndenter::new(String::new(), INDENT);
    let out = &mut output;

    write_file_header(out);

    let bsatn_imp = format!("{SDK_MODULE}/bsatn");
    let types_imp = format!("{SDK_MODULE}/types");
    let mut imports: Vec<&str> = vec![&bsatn_imp];
    if product_needs_types_import(prod) {
        imports.push(&types_imp);
    }
    write_imports(out, &imports);

    // Struct definition
    writeln!(out, "type {name} struct {{");
    {
        let mut inner = out.indented(1);
        for (field_name, ty) in prod.elements.iter() {
            let go_name = field_name.deref().to_case(Case::Pascal);
            let go_ty = go_type_name(module, ty);
            writeln!(inner, "{go_name} {go_ty}");
        }
    }
    writeln!(out, "}}");
    writeln!(out);

    // WriteBsatn method
    writeln!(out, "func (v {name}) WriteBsatn(w *bsatn.Writer) {{");
    {
        let mut inner = out.indented(1);
        for (field_name, ty) in prod.elements.iter() {
            let go_name = field_name.deref().to_case(Case::Pascal);
            let write_expr = bsatn_write_expr(module, ty, &format!("v.{go_name}"));
            if !write_expr.is_empty() {
                writeln!(inner, "{write_expr}");
            }
        }
    }
    writeln!(out, "}}");
    writeln!(out);

    // ReadXxx function
    writeln!(out, "func Read{name}(r *bsatn.Reader) ({name}, error) {{");
    {
        let mut inner = out.indented(1);
        writeln!(inner, "var v {name}");
        writeln!(inner, "var err error");
        for (field_name, ty) in prod.elements.iter() {
            let go_name = field_name.deref().to_case(Case::Pascal);
            let read_expr = bsatn_read_expr(module, ty);
            writeln!(inner, "v.{go_name}, err = {read_expr}");
            writeln!(inner, "if err != nil {{ return v, err }}");
        }
        writeln!(inner, "return v, nil");
    }
    writeln!(out, "}}");

    output.into_inner()
}

// ---------------------------------------------------------------------------
// Sum type (interface + variant structs)
// ---------------------------------------------------------------------------

fn gen_sum_type(module: &ModuleDef, name: &str, sum: &SumTypeDef) -> String {
    let mut output = CodeIndenter::new(String::new(), INDENT);
    let out = &mut output;

    write_file_header(out);
    let bsatn_imp = format!("{SDK_MODULE}/bsatn");
    write_imports(out, &[&bsatn_imp, "fmt"]);

    // Sealed interface
    writeln!(out, "type {name} interface {{");
    {
        let mut inner = out.indented(1);
        writeln!(inner, "is{name}()");
        writeln!(inner, "WriteBsatn(w *bsatn.Writer)");
    }
    writeln!(out, "}}");
    writeln!(out);

    // One struct per variant
    for (i, (variant_name, variant_ty)) in sum.variants.iter().enumerate() {
        let vname = variant_name.deref().to_case(Case::Pascal);
        let full_vname = format!("{name}{vname}");
        let go_ty = go_type_name(module, variant_ty);
        let has_value = !matches!(variant_ty, AlgebraicTypeUse::Unit);

        if has_value {
            writeln!(out, "type {full_vname} struct {{ Value {go_ty} }}");
        } else {
            writeln!(out, "type {full_vname} struct{{}}");
        }
        writeln!(out, "func ({full_vname}) is{name}() {{}}");
        writeln!(out, "func (v {full_vname}) WriteBsatn(w *bsatn.Writer) {{");
        {
            let mut inner = out.indented(1);
            writeln!(inner, "w.WriteVariantTag({i})");
            if has_value {
                let write_expr = bsatn_write_expr(module, variant_ty, "v.Value");
                if !write_expr.is_empty() {
                    writeln!(inner, "{write_expr}");
                }
            }
        }
        writeln!(out, "}}");
        writeln!(out);
    }

    // Read function
    writeln!(out, "func Read{name}(r *bsatn.Reader) ({name}, error) {{");
    {
        let mut inner = out.indented(1);
        writeln!(inner, "tag, err := r.ReadVariantTag()");
        writeln!(inner, "if err != nil {{ return nil, err }}");
        writeln!(inner, "switch tag {{");
        for (i, (variant_name, variant_ty)) in sum.variants.iter().enumerate() {
            let vname = variant_name.deref().to_case(Case::Pascal);
            let full_vname = format!("{name}{vname}");
            let has_value = !matches!(variant_ty, AlgebraicTypeUse::Unit);
            writeln!(inner, "case {i}:");
            {
                let mut inner2 = inner.indented(1);
                if has_value {
                    let read_expr = bsatn_read_expr(module, variant_ty);
                    writeln!(inner2, "val, err := {read_expr}");
                    writeln!(inner2, "if err != nil {{ return nil, err }}");
                    writeln!(inner2, "return {full_vname}{{Value: val}}, nil");
                } else {
                    writeln!(inner2, "return {full_vname}{{}}, nil");
                }
            }
        }
        writeln!(
            inner,
            "default: return nil, fmt.Errorf(\"unknown {name} variant %d\", tag)"
        );
        writeln!(inner, "}}");
    }
    writeln!(out, "}}");

    output.into_inner()
}

// ---------------------------------------------------------------------------
// Plain enum (iota const)
// ---------------------------------------------------------------------------

fn gen_plain_enum(name: &str, plain_enum: &PlainEnumTypeDef) -> String {
    let mut output = CodeIndenter::new(String::new(), INDENT);
    let out = &mut output;

    write_file_header(out);
    let bsatn_imp = format!("{SDK_MODULE}/bsatn");
    write_imports(out, &[&bsatn_imp]);

    writeln!(out, "type {name} uint8");
    writeln!(out);
    writeln!(out, "const (");
    {
        let mut inner = out.indented(1);
        for (i, variant) in plain_enum.variants.iter().enumerate() {
            let vname = variant.deref().to_case(Case::Pascal);
            if i == 0 {
                writeln!(inner, "{name}{vname} {name} = iota");
            } else {
                writeln!(inner, "{name}{vname}");
            }
        }
    }
    writeln!(out, ")");
    writeln!(out);

    // WriteBsatn
    writeln!(out, "func (v {name}) WriteBsatn(w *bsatn.Writer) {{");
    {
        let mut inner = out.indented(1);
        writeln!(inner, "w.WriteVariantTag(uint8(v))");
    }
    writeln!(out, "}}");
    writeln!(out);

    // Read function
    writeln!(out, "func Read{name}(r *bsatn.Reader) ({name}, error) {{");
    {
        let mut inner = out.indented(1);
        writeln!(inner, "tag, err := r.ReadVariantTag()");
        writeln!(inner, "return {name}(tag), err");
    }
    writeln!(out, "}}");

    output.into_inner()
}

// ---------------------------------------------------------------------------
// Table file
// ---------------------------------------------------------------------------

fn gen_table_file(table_name: &str, row_type: &str) -> String {
    let mut output = CodeIndenter::new(String::new(), INDENT);
    let out = &mut output;

    write_file_header(out);
    let bsatn_imp = format!("{SDK_MODULE}/bsatn");
    let client_imp = format!("{SDK_MODULE}/client");
    write_imports(out, &[&bsatn_imp, &client_imp]);

    writeln!(out, "// {table_name}TableHandle provides access to the {table_name} table in the client cache.");
    writeln!(out, "type {table_name}TableHandle struct {{");
    {
        let mut inner = out.indented(1);
        writeln!(inner, "cache *client.TableCache[{row_type}]");
    }
    writeln!(out, "}}");
    writeln!(out);

    writeln!(out, "func (h *{table_name}TableHandle) Count() int {{");
    {
        let mut inner = out.indented(1);
        writeln!(inner, "return h.cache.Count()");
    }
    writeln!(out, "}}");
    writeln!(out);

    writeln!(out, "func (h *{table_name}TableHandle) Iter() func(func({row_type}) bool) {{");
    {
        let mut inner = out.indented(1);
        writeln!(inner, "return h.cache.Iter()");
    }
    writeln!(out, "}}");
    writeln!(out);

    writeln!(
        out,
        "func (h *{table_name}TableHandle) OnInsert(fn func(*client.EventContext, {row_type})) client.CallbackId {{"
    );
    {
        let mut inner = out.indented(1);
        writeln!(inner, "return h.cache.Callbacks.OnInsert.Register(fn)");
    }
    writeln!(out, "}}");
    writeln!(out);

    writeln!(
        out,
        "func (h *{table_name}TableHandle) RemoveOnInsert(id client.CallbackId) {{"
    );
    {
        let mut inner = out.indented(1);
        writeln!(inner, "h.cache.Callbacks.OnInsert.Remove(id)");
    }
    writeln!(out, "}}");
    writeln!(out);

    writeln!(
        out,
        "func (h *{table_name}TableHandle) OnDelete(fn func(*client.EventContext, {row_type})) client.CallbackId {{"
    );
    {
        let mut inner = out.indented(1);
        writeln!(inner, "return h.cache.Callbacks.OnDelete.Register(fn)");
    }
    writeln!(out, "}}");
    writeln!(out);

    writeln!(
        out,
        "func (h *{table_name}TableHandle) RemoveOnDelete(id client.CallbackId) {{"
    );
    {
        let mut inner = out.indented(1);
        writeln!(inner, "h.cache.Callbacks.OnDelete.Remove(id)");
    }
    writeln!(out, "}}");
    writeln!(out);

    writeln!(
        out,
        "func (h *{table_name}TableHandle) ApplyInserts(rows *bsatn.BsatnRowList) error {{"
    );
    {
        let mut inner = out.indented(1);
        writeln!(inner, "return h.cache.ApplyInsertsRaw(rows, Read{row_type})");
    }
    writeln!(out, "}}");
    writeln!(out);

    writeln!(
        out,
        "func (h *{table_name}TableHandle) ApplyDeletes(rows *bsatn.BsatnRowList) error {{"
    );
    {
        let mut inner = out.indented(1);
        writeln!(inner, "return h.cache.ApplyDeletesRaw(rows, Read{row_type})");
    }
    writeln!(out, "}}");

    output.into_inner()
}

// ---------------------------------------------------------------------------
// Reducer file
// ---------------------------------------------------------------------------

fn gen_reducer_file(module: &ModuleDef, reducer: &ReducerDef, name: &str) -> String {
    let mut output = CodeIndenter::new(String::new(), INDENT);
    let out = &mut output;

    write_file_header(out);
    let bsatn_imp = format!("{SDK_MODULE}/bsatn");
    let client_imp = format!("{SDK_MODULE}/client");
    write_imports(out, &[&bsatn_imp, &client_imp]);

    // Args struct
    writeln!(out, "// {name}Args holds the arguments for the {name} reducer.");
    writeln!(out, "type {name}Args struct {{");
    {
        let mut inner = out.indented(1);
        for (field_name, ty) in reducer.params_for_generate.elements.iter() {
            let go_name = field_name.deref().to_case(Case::Pascal);
            let go_ty = go_type_name(module, ty);
            writeln!(inner, "{go_name} {go_ty}");
        }
    }
    writeln!(out, "}}");
    writeln!(out);

    // WriteBsatn for args
    writeln!(out, "func (v {name}Args) WriteBsatn(w *bsatn.Writer) {{");
    {
        let mut inner = out.indented(1);
        for (field_name, ty) in reducer.params_for_generate.elements.iter() {
            let go_name = field_name.deref().to_case(Case::Pascal);
            let write_expr = bsatn_write_expr(module, ty, &format!("v.{go_name}"));
            if !write_expr.is_empty() {
                writeln!(inner, "{write_expr}");
            }
        }
    }
    writeln!(out, "}}");
    writeln!(out);

    if is_reducer_invokable(reducer) {
        let params = build_go_params(module, &reducer.params_for_generate);
        let args = build_go_args(&reducer.params_for_generate);
        let _sep = if params.is_empty() { "" } else { ", " };

        writeln!(out, "func (r *RemoteReducers) {name}({params}) error {{");
        {
            let mut inner = out.indented(1);
            writeln!(
                inner,
                "args, err := bsatn.Encode({name}Args{{{args}}})"
            );
            writeln!(inner, "if err != nil {{ return err }}");
            writeln!(
                inner,
                "_, err = r.conn.CallReducer(\"{}\", args)",
                reducer.name
            );
            writeln!(inner, "return err");
        }
        writeln!(out, "}}");
        writeln!(out);
    }

    // Callback registration
    writeln!(
        out,
        "func (r *RemoteReducers) On{name}(fn func(*client.ReducerEventContext, {name}Args)) client.CallbackId {{"
    );
    {
        let mut inner = out.indented(1);
        writeln!(inner, "return r.on{name}.Register(fn)");
    }
    writeln!(out, "}}");
    writeln!(out);

    writeln!(
        out,
        "func (r *RemoteReducers) RemoveOn{name}(id client.CallbackId) {{"
    );
    {
        let mut inner = out.indented(1);
        writeln!(inner, "r.on{name}.Remove(id)");
    }
    writeln!(out, "}}");

    output.into_inner()
}

// ---------------------------------------------------------------------------
// Procedure file
// ---------------------------------------------------------------------------

fn gen_procedure_file(
    module: &ModuleDef,
    procedure: &spacetimedb_schema::def::ProcedureDef,
    name: &str,
) -> String {
    let mut output = CodeIndenter::new(String::new(), INDENT);
    let out = &mut output;

    write_file_header(out);
    let bsatn_imp = format!("{SDK_MODULE}/bsatn");
    let client_imp = format!("{SDK_MODULE}/client");
    write_imports(out, &["context", &bsatn_imp, &client_imp]);

    let ret_ty = go_type_name(module, &procedure.return_type_for_generate);
    let zero_val = go_zero_value(&procedure.return_type_for_generate);

    // Args struct
    writeln!(out, "type {name}Args struct {{");
    {
        let mut inner = out.indented(1);
        for (field_name, ty) in procedure.params_for_generate.elements.iter() {
            let go_name = field_name.deref().to_case(Case::Pascal);
            let go_ty = go_type_name(module, ty);
            writeln!(inner, "{go_name} {go_ty}");
        }
    }
    writeln!(out, "}}");
    writeln!(out);

    writeln!(out, "func (v {name}Args) WriteBsatn(w *bsatn.Writer) {{");
    {
        let mut inner = out.indented(1);
        for (field_name, ty) in procedure.params_for_generate.elements.iter() {
            let go_name = field_name.deref().to_case(Case::Pascal);
            let write_expr = bsatn_write_expr(module, ty, &format!("v.{go_name}"));
            if !write_expr.is_empty() {
                writeln!(inner, "{write_expr}");
            }
        }
    }
    writeln!(out, "}}");
    writeln!(out);

    // Typed call on RemoteProcedures
    let params = build_go_params(module, &procedure.params_for_generate);
    let args = build_go_args(&procedure.params_for_generate);
    let ctx_sep = if params.is_empty() { "" } else { ", " };
    let ret_read = bsatn_read_expr(module, &procedure.return_type_for_generate);

    writeln!(
        out,
        "func (p *RemoteProcedures) {name}(ctx context.Context{ctx_sep}{params}) ({ret_ty}, error) {{"
    );
    {
        let mut inner = out.indented(1);
        writeln!(inner, "argsBytes, err := bsatn.Encode({name}Args{{{args}}})");
        writeln!(
            inner,
            "if err != nil {{ return {zero_val}, err }}"
        );
        writeln!(
            inner,
            "resultBytes, err := p.conn.CallProcedure(ctx, \"{}\", argsBytes)",
            procedure.name
        );
        writeln!(
            inner,
            "if err != nil {{ return {zero_val}, err }}"
        );
        writeln!(inner, "r := bsatn.NewReader(resultBytes)");
        writeln!(inner, "return {ret_read}");
    }
    writeln!(out, "}}");

    output.into_inner()
}

fn go_zero_value(ty: &AlgebraicTypeUse) -> &'static str {
    match ty {
        AlgebraicTypeUse::Primitive(PrimitiveType::Bool) => "false",
        AlgebraicTypeUse::String => "\"\"",
        AlgebraicTypeUse::Option(_) => "nil",
        AlgebraicTypeUse::Ref(_) => "nil",
        _ => "v",
    }
}

// ---------------------------------------------------------------------------
// Global module_bindings.go
// ---------------------------------------------------------------------------

fn gen_module_bindings(module: &ModuleDef, options: &CodegenOptions) -> String {
    let mut output = CodeIndenter::new(String::new(), INDENT);
    let out = &mut output;

    write_file_header(out);
    let bsatn_imp = format!("{SDK_MODULE}/bsatn");
    let client_imp = format!("{SDK_MODULE}/client");
    write_imports(out, &["context", &bsatn_imp, &client_imp]);

    // RemoteTables
    writeln!(
        out,
        "// RemoteTables provides typed access to all tables in the client cache."
    );
    writeln!(out, "type RemoteTables struct {{");
    {
        let mut inner = out.indented(1);
        for tbl in iter_tables(module, options.visibility) {
            let name = tbl.accessor_name.deref().to_case(Case::Pascal);
            writeln!(inner, "{name} *{name}TableHandle");
        }
    }
    writeln!(out, "}}");
    writeln!(out);

    // RemoteReducers
    writeln!(
        out,
        "// RemoteReducers provides typed methods to call module reducers."
    );
    writeln!(out, "type RemoteReducers struct {{");
    {
        let mut inner = out.indented(1);
        writeln!(inner, "conn *client.DbConnection");
        for reducer in iter_reducers(module, options.visibility) {
            let name = reducer.accessor_name.deref().to_case(Case::Pascal);
            writeln!(
                inner,
                "on{name} client.CallbackRegistry[*client.ReducerEventContext, {name}Args]"
            );
        }
    }
    writeln!(out, "}}");
    writeln!(out);

    // RemoteProcedures
    writeln!(
        out,
        "// RemoteProcedures provides typed methods to call module procedures."
    );
    writeln!(out, "type RemoteProcedures struct {{");
    {
        let mut inner = out.indented(1);
        writeln!(inner, "conn *client.DbConnection");
    }
    writeln!(out, "}}");
    writeln!(out);

    // RegisterTables
    writeln!(
        out,
        "// RegisterTables registers BSATN decoders for all module tables with the DbConnection."
    );
    writeln!(out, "func RegisterTables(conn *client.DbConnection, tables *RemoteTables) {{");
    {
        let mut inner = out.indented(1);
        for tbl in iter_tables(module, options.visibility) {
            let name = tbl.accessor_name.deref().to_case(Case::Pascal);
            let row_type = type_ref_name(module, tbl.product_type_ref);
            writeln!(inner, "tables.{name} = &{name}TableHandle{{");
            writeln!(inner, "\tcache: client.NewTableCache[{row_type}](),");
            writeln!(inner, "}}");
            writeln!(
                inner,
                "conn.RegisterTableHandler(\"{}\", tables.{name})",
                tbl.accessor_name.deref()
            );
        }
    }
    writeln!(out, "}}");
    writeln!(out);

    // NewDbConnection helper
    writeln!(out, "// NewDbConnection builds a SpacetimeDB connection for this module.");
    writeln!(
        out,
        "func NewDbConnection(ctx context.Context, uri, moduleName string) (*client.DbConnection, *RemoteTables, *RemoteReducers, error) {{"
    );
    {
        let mut inner = out.indented(1);
        writeln!(
            inner,
            "conn, err := client.NewDbConnectionBuilder().\n\t\tWithUri(uri).\n\t\tWithModuleName(moduleName).\n\t\tBuild(ctx)"
        );
        writeln!(inner, "if err != nil {{ return nil, nil, nil, err }}");
        writeln!(inner, "tables := &RemoteTables{{}}");
        writeln!(inner, "RegisterTables(conn, tables)");
        writeln!(inner, "reducers := &RemoteReducers{{conn: conn}}");
        writeln!(inner, "return conn, tables, reducers, nil");
    }
    writeln!(out, "}}");

    // Suppress unused imports in generated files that may have no tables/reducers
    writeln!(out, "var _ = bsatn.NewWriter");
    writeln!(out, "var _ context.Context = nil");

    output.into_inner()
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

fn write_imports(out: &mut Indenter, imports: &[&str]) {
    if imports.is_empty() {
        return;
    }
    if imports.len() == 1 {
        writeln!(out, "import \"{}\"", imports[0]);
    } else {
        writeln!(out, "import (");
        {
            let mut inner = out.indented(1);
            for imp in imports {
                writeln!(inner, "\"{}\"", imp);
            }
        }
        writeln!(out, ")");
    }
    writeln!(out);
}

fn build_go_params(module: &ModuleDef, prod: &ProductTypeDef) -> String {
    prod.elements
        .iter()
        .map(|(name, ty)| {
            format!(
                "{} {}",
                name.deref().to_case(Case::Camel),
                go_type_name(module, ty)
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn build_go_args(prod: &ProductTypeDef) -> String {
    prod.elements
        .iter()
        .map(|(name, _)| {
            let go_name = name.deref().to_case(Case::Pascal);
            let param = name.deref().to_case(Case::Camel);
            format!("{go_name}: {param}")
        })
        .collect::<Vec<_>>()
        .join(", ")
}
