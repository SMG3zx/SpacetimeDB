#![allow(unused_must_use)]

use std::fmt::{self, Write};
use std::ops::Deref;

use convert_case::{Case, Casing};
use spacetimedb_lib::sats::layout::PrimitiveType;
use spacetimedb_schema::def::{ModuleDef, ProcedureDef, ReducerDef, TableDef, TypeDef};
use spacetimedb_schema::identifier::Identifier;
use spacetimedb_schema::reducer_name::ReducerName;
use spacetimedb_schema::schema::TableSchema;
use spacetimedb_schema::type_for_generate::{
    AlgebraicTypeDef, AlgebraicTypeUse, PlainEnumTypeDef, ProductTypeDef, SumTypeDef,
};

use crate::util::{collect_case, iter_procedures, iter_reducers, iter_table_names_and_types, type_ref_name};
use crate::{CodegenOptions, Lang, OutputFile};

const INDENT: &str = "\t";

#[derive(Clone, Copy, Debug, Default)]
pub struct Go;

impl Lang for Go {
    fn generate_table_file_from_schema(
        &self,
        module: &ModuleDef,
        table: &TableDef,
        _schema: TableSchema,
    ) -> OutputFile {
        let mut output = String::new();
        print_go_header(&mut output, &[]);

        let table_type = table_type_name(&table.accessor_name);
        let row_type = type_ref_name(module, table.product_type_ref);
        let wire_name = table.name.deref();
        let accessor = table.accessor_name.deref();

        writeln!(output, "type {table_type} struct {{}}");
        writeln!(output);
        writeln!(output, "func New{table_type}() {table_type} {{");
        writeln!(output, "{INDENT}return {table_type}{{}}");
        writeln!(output, "}}");
        writeln!(output);
        writeln!(output, "func ({table_type}) Name() string {{ return \"{wire_name}\" }}");
        writeln!(
            output,
            "func ({table_type}) Accessor() string {{ return \"{accessor}\" }}"
        );
        writeln!(output);
        writeln!(output, "type {table_type}Row = {row_type}");

        OutputFile {
            filename: format!("tables_{}.go", table.accessor_name.deref().to_case(Case::Snake)),
            code: output,
        }
    }

    fn generate_type_files(&self, module: &ModuleDef, typ: &TypeDef) -> Vec<OutputFile> {
        let mut output = String::new();
        let ty_name = collect_case(Case::Pascal, typ.accessor_name.name_segments());
        let ty = &module.typespace_for_generate()[typ.ty];

        let mut imports = Vec::new();
        match ty {
            AlgebraicTypeDef::Product(product) => gather_imports_product(module, product, &mut imports),
            AlgebraicTypeDef::Sum(sum) => gather_imports_sum(module, sum, &mut imports),
            AlgebraicTypeDef::PlainEnum(_) => {}
        }
        imports.sort();
        imports.dedup();

        let import_refs = imports.iter().map(String::as_str).collect::<Vec<_>>();
        print_go_header(&mut output, &import_refs);

        match ty {
            AlgebraicTypeDef::Product(product) => write_product_type(module, &mut output, &ty_name, product).unwrap(),
            AlgebraicTypeDef::Sum(sum) => write_sum_type(&mut output, &ty_name, sum).unwrap(),
            AlgebraicTypeDef::PlainEnum(plain_enum) => {
                write_plain_enum_type(&mut output, &ty_name, plain_enum).unwrap()
            }
        }

        vec![OutputFile {
            filename: format!(
                "types_{}.go",
                typ.accessor_name
                    .name_segments()
                    .map(|s| s.deref())
                    .collect::<Vec<_>>()
                    .join("_")
            ),
            code: output,
        }]
    }

    fn generate_reducer_file(&self, _module: &ModuleDef, reducer: &ReducerDef) -> OutputFile {
        let mut output = String::new();
        print_go_header(
            &mut output,
            &["context", "github.com/clockworklabs/spacetimedb/sdks/go/connection"],
        );

        let method_name = reducer_method_name(&reducer.accessor_name);
        let reducer_name = reducer.name.deref();
        writeln!(
            output,
            "func (c *Client) {method_name}(ctx context.Context, args []byte, callback connection.ReducerResultCallback) (uint32, error) {{"
        );
        writeln!(
            output,
            "{INDENT}return c.CallReducer(ctx, \"{reducer_name}\", args, callback)"
        );
        writeln!(output, "}}");

        OutputFile {
            filename: format!("reducers_{}.go", reducer.accessor_name.deref().to_case(Case::Snake)),
            code: output,
        }
    }

    fn generate_procedure_file(&self, _module: &ModuleDef, procedure: &ProcedureDef) -> OutputFile {
        let mut output = String::new();
        print_go_header(
            &mut output,
            &["context", "github.com/clockworklabs/spacetimedb/sdks/go/connection"],
        );

        let method_name = procedure_method_name(&procedure.accessor_name);
        let procedure_name = procedure.name.deref();
        writeln!(
            output,
            "func (c *Client) {method_name}(ctx context.Context, args []byte, callback connection.ProcedureResultCallback) (uint32, error) {{"
        );
        writeln!(
            output,
            "{INDENT}return c.CallProcedure(ctx, \"{procedure_name}\", args, callback)"
        );
        writeln!(output, "}}");

        OutputFile {
            filename: format!("procedures_{}.go", procedure.accessor_name.deref().to_case(Case::Snake)),
            code: output,
        }
    }

    fn generate_global_files(&self, module: &ModuleDef, options: &CodegenOptions) -> Vec<OutputFile> {
        let mut client = String::new();
        print_go_header(
            &mut client,
            &[
                "context",
                "errors",
                "github.com/clockworklabs/spacetimedb/sdks/go/connection",
            ],
        );
        writeln!(client, "type Result[T any, E any] struct {{");
        writeln!(client, "{INDENT}Ok  *T");
        writeln!(client, "{INDENT}Err *E");
        writeln!(client, "}}");
        writeln!(client);
        writeln!(client, "type ScheduleAt struct {{");
        writeln!(client, "{INDENT}Tag   string `json:\"tag\"`");
        writeln!(client, "{INDENT}Value any    `json:\"value,omitempty\"`");
        writeln!(client, "}}");
        writeln!(client);
        writeln!(client, "type Client struct {{");
        writeln!(client, "{INDENT}conn *connection.Connection");
        writeln!(client, "}}");
        writeln!(client);
        writeln!(client, "func NewClient(conn *connection.Connection) *Client {{");
        writeln!(client, "{INDENT}return &Client{{conn: conn}}");
        writeln!(client, "}}");
        writeln!(client);
        writeln!(
            client,
            "func (c *Client) Connection() *connection.Connection {{ return c.conn }}"
        );
        writeln!(client);
        writeln!(
            client,
            "func (c *Client) CallReducer(ctx context.Context, reducer string, args []byte, callback connection.ReducerResultCallback) (uint32, error) {{"
        );
        writeln!(client, "{INDENT}if c == nil || c.conn == nil {{");
        writeln!(
            client,
            "{INDENT}{INDENT}return 0, errors.New(\"spacetimedb client is not connected\")"
        );
        writeln!(client, "{INDENT}}}");
        writeln!(client, "{INDENT}if ctx != nil {{");
        writeln!(client, "{INDENT}{INDENT}select {{");
        writeln!(client, "{INDENT}{INDENT}case <-ctx.Done():");
        writeln!(client, "{INDENT}{INDENT}{INDENT}return 0, ctx.Err()");
        writeln!(client, "{INDENT}{INDENT}default:");
        writeln!(client, "{INDENT}{INDENT}}}");
        writeln!(client, "{INDENT}}}");
        writeln!(client, "{INDENT}return c.conn.CallReducer(reducer, args, callback)");
        writeln!(client, "}}");
        writeln!(client);
        writeln!(
            client,
            "func (c *Client) CallProcedure(ctx context.Context, procedure string, args []byte, callback connection.ProcedureResultCallback) (uint32, error) {{"
        );
        writeln!(client, "{INDENT}if c == nil || c.conn == nil {{");
        writeln!(
            client,
            "{INDENT}{INDENT}return 0, errors.New(\"spacetimedb client is not connected\")"
        );
        writeln!(client, "{INDENT}}}");
        writeln!(client, "{INDENT}if ctx != nil {{");
        writeln!(client, "{INDENT}{INDENT}select {{");
        writeln!(client, "{INDENT}{INDENT}case <-ctx.Done():");
        writeln!(client, "{INDENT}{INDENT}{INDENT}return 0, ctx.Err()");
        writeln!(client, "{INDENT}{INDENT}default:");
        writeln!(client, "{INDENT}{INDENT}}}");
        writeln!(client, "{INDENT}}}");
        writeln!(client, "{INDENT}return c.conn.CallProcedure(procedure, args, callback)");
        writeln!(client, "}}");
        writeln!(client);
        writeln!(
            client,
            "func (c *Client) OneOffQuery(ctx context.Context, query string, callback connection.OneOffQueryResultCallback) (uint32, error) {{"
        );
        writeln!(client, "{INDENT}if c == nil || c.conn == nil {{");
        writeln!(
            client,
            "{INDENT}{INDENT}return 0, errors.New(\"spacetimedb client is not connected\")"
        );
        writeln!(client, "{INDENT}}}");
        writeln!(client, "{INDENT}if ctx != nil {{");
        writeln!(client, "{INDENT}{INDENT}select {{");
        writeln!(client, "{INDENT}{INDENT}case <-ctx.Done():");
        writeln!(client, "{INDENT}{INDENT}{INDENT}return 0, ctx.Err()");
        writeln!(client, "{INDENT}{INDENT}default:");
        writeln!(client, "{INDENT}{INDENT}}}");
        writeln!(client, "{INDENT}}}");
        writeln!(client, "{INDENT}return c.conn.OneOffQuery(query, callback)");
        writeln!(client, "}}");

        let mut schema = String::new();
        print_go_header(&mut schema, &[]);
        writeln!(schema, "var TableAccessors = []string{{");
        for (_, accessor_name, _) in iter_table_names_and_types(module, options.visibility) {
            writeln!(schema, "{INDENT}\"{accessor_name}\",");
        }
        writeln!(schema, "}}");
        writeln!(schema);
        writeln!(schema, "var ReducerNames = []string{{");
        for reducer in iter_reducers(module, options.visibility) {
            writeln!(schema, "{INDENT}\"{}\",", reducer.name);
        }
        writeln!(schema, "}}");
        writeln!(schema);
        writeln!(schema, "var ProcedureNames = []string{{");
        for procedure in iter_procedures(module, options.visibility) {
            writeln!(schema, "{INDENT}\"{}\",", procedure.name);
        }
        writeln!(schema, "}}");

        vec![
            OutputFile {
                filename: "client.go".to_string(),
                code: client,
            },
            OutputFile {
                filename: "schema.go".to_string(),
                code: schema,
            },
        ]
    }
}

fn print_go_header(out: &mut String, imports: &[&str]) {
    writeln!(
        out,
        "// THIS FILE IS AUTOMATICALLY GENERATED BY SPACETIMEDB. EDITS TO THIS FILE"
    );
    writeln!(
        out,
        "// WILL NOT BE SAVED. MODIFY TABLES IN YOUR MODULE SOURCE CODE INSTEAD."
    );
    writeln!(out);
    writeln!(out, "package module_bindings");
    if imports.is_empty() {
        writeln!(out);
        return;
    }

    writeln!(out);
    writeln!(out, "import (");
    for import in imports {
        writeln!(out, "{INDENT}\"{import}\"");
    }
    writeln!(out, ")");
    writeln!(out);
}

fn table_type_name(accessor_name: &Identifier) -> String {
    format!("{}Table", accessor_name.deref().to_case(Case::Pascal))
}

fn reducer_method_name(reducer_name: &ReducerName) -> String {
    format!("Call{}", reducer_name.deref().to_case(Case::Pascal))
}

fn procedure_method_name(procedure_name: &Identifier) -> String {
    format!("Call{}", procedure_name.deref().to_case(Case::Pascal))
}

fn write_product_type(module: &ModuleDef, out: &mut String, type_name: &str, product: &ProductTypeDef) -> fmt::Result {
    writeln!(out, "type {type_name} struct {{")?;
    for (idx, (field_name, field_ty)) in product.elements.iter().enumerate() {
        let go_field = go_exported_field_name(field_name, idx);
        write!(out, "{INDENT}{go_field} ")?;
        write_type(module, out, field_ty)?;
        writeln!(out, " `json:\"{}\"`", field_name.deref())?;
    }
    writeln!(out, "}}")
}

fn write_sum_type(out: &mut String, type_name: &str, sum: &SumTypeDef) -> fmt::Result {
    writeln!(out, "type {type_name}Tag string")?;
    writeln!(out)?;
    writeln!(out, "const (")?;
    for (variant_name, _) in &sum.variants {
        let tag_name = format!("{type_name}Tag{}", variant_name.deref().to_case(Case::Pascal));
        writeln!(out, "{INDENT}{tag_name} {type_name}Tag = \"{}\"", variant_name.deref())?;
    }
    writeln!(out, ")")?;
    writeln!(out)?;
    writeln!(out, "type {type_name} struct {{")?;
    writeln!(out, "{INDENT}Tag   {type_name}Tag `json:\"tag\"`")?;
    writeln!(out, "{INDENT}Value any         `json:\"value,omitempty\"`")?;
    writeln!(out, "}}")
}

fn write_plain_enum_type(out: &mut String, type_name: &str, plain_enum: &PlainEnumTypeDef) -> fmt::Result {
    writeln!(out, "type {type_name} uint8")?;
    writeln!(out)?;
    writeln!(out, "const (")?;
    for (idx, variant_name) in plain_enum.variants.iter().enumerate() {
        let name = format!("{type_name}{}", variant_name.deref().to_case(Case::Pascal));
        if idx == 0 {
            writeln!(out, "{INDENT}{name} {type_name} = iota")?;
        } else {
            writeln!(out, "{INDENT}{name}")?;
        }
    }
    writeln!(out, ")")
}

fn write_type<W: Write>(module: &ModuleDef, out: &mut W, ty: &AlgebraicTypeUse) -> fmt::Result {
    match ty {
        AlgebraicTypeUse::Unit => write!(out, "struct{{}}"),
        AlgebraicTypeUse::Never => write!(out, "any"),
        AlgebraicTypeUse::Identity | AlgebraicTypeUse::ConnectionId | AlgebraicTypeUse::Uuid => write!(out, "string"),
        AlgebraicTypeUse::Timestamp => write!(out, "time.Time"),
        AlgebraicTypeUse::TimeDuration => write!(out, "time.Duration"),
        AlgebraicTypeUse::ScheduleAt => write!(out, "ScheduleAt"),
        AlgebraicTypeUse::Option(inner) => {
            write!(out, "*")?;
            write_type(module, out, inner)
        }
        AlgebraicTypeUse::Result { ok_ty, err_ty } => {
            write!(out, "Result[")?;
            write_type(module, out, ok_ty)?;
            write!(out, ", ")?;
            write_type(module, out, err_ty)?;
            write!(out, "]")
        }
        AlgebraicTypeUse::Primitive(prim) => write!(
            out,
            "{}",
            match prim {
                PrimitiveType::Bool => "bool",
                PrimitiveType::I8 => "int8",
                PrimitiveType::U8 => "uint8",
                PrimitiveType::I16 => "int16",
                PrimitiveType::U16 => "uint16",
                PrimitiveType::I32 => "int32",
                PrimitiveType::U32 => "uint32",
                PrimitiveType::I64 => "int64",
                PrimitiveType::U64 => "uint64",
                PrimitiveType::I128 | PrimitiveType::U128 | PrimitiveType::I256 | PrimitiveType::U256 => "*big.Int",
                PrimitiveType::F32 => "float32",
                PrimitiveType::F64 => "float64",
            }
        ),
        AlgebraicTypeUse::String => write!(out, "string"),
        AlgebraicTypeUse::Array(inner) => {
            write!(out, "[]")?;
            write_type(module, out, inner)
        }
        AlgebraicTypeUse::Ref(r) => write!(out, "{}", type_ref_name(module, *r)),
    }
}

fn gather_imports_product(module: &ModuleDef, product: &ProductTypeDef, imports: &mut Vec<String>) {
    for (_, ty) in &product.elements {
        gather_imports_type(module, ty, imports);
    }
}

fn gather_imports_sum(module: &ModuleDef, sum: &SumTypeDef, imports: &mut Vec<String>) {
    for (_, ty) in &sum.variants {
        gather_imports_type(module, ty, imports);
    }
}

fn gather_imports_type(module: &ModuleDef, ty: &AlgebraicTypeUse, imports: &mut Vec<String>) {
    match ty {
        AlgebraicTypeUse::Timestamp | AlgebraicTypeUse::TimeDuration => imports.push("time".to_string()),
        AlgebraicTypeUse::Primitive(PrimitiveType::I128)
        | AlgebraicTypeUse::Primitive(PrimitiveType::U128)
        | AlgebraicTypeUse::Primitive(PrimitiveType::I256)
        | AlgebraicTypeUse::Primitive(PrimitiveType::U256) => imports.push("math/big".to_string()),
        AlgebraicTypeUse::Option(inner) | AlgebraicTypeUse::Array(inner) => gather_imports_type(module, inner, imports),
        AlgebraicTypeUse::Result { ok_ty, err_ty } => {
            gather_imports_type(module, ok_ty, imports);
            gather_imports_type(module, err_ty, imports);
        }
        AlgebraicTypeUse::Ref(r) => {
            // Nested references can contain types that require imports.
            match &module.typespace_for_generate()[*r] {
                AlgebraicTypeDef::Product(product) => gather_imports_product(module, product, imports),
                AlgebraicTypeDef::Sum(sum) => gather_imports_sum(module, sum, imports),
                AlgebraicTypeDef::PlainEnum(_) => {}
            }
        }
        AlgebraicTypeUse::Unit
        | AlgebraicTypeUse::Never
        | AlgebraicTypeUse::Identity
        | AlgebraicTypeUse::ConnectionId
        | AlgebraicTypeUse::ScheduleAt
        | AlgebraicTypeUse::Uuid
        | AlgebraicTypeUse::Primitive(_)
        | AlgebraicTypeUse::String => {}
    }
}

fn go_exported_field_name(field_name: &Identifier, index: usize) -> String {
    let mut name = field_name.deref().to_case(Case::Pascal);
    if name.is_empty() {
        name = format!("Field{index}");
    }
    if name.chars().next().map_or(true, |first| !first.is_ascii_alphabetic()) {
        name = format!("Field{name}");
    }
    if is_go_keyword(&name) {
        name.push('_');
    }
    name
}

fn is_go_keyword(name: &str) -> bool {
    matches!(
        name,
        "Break"
            | "Case"
            | "Chan"
            | "Const"
            | "Continue"
            | "Default"
            | "Defer"
            | "Else"
            | "Fallthrough"
            | "For"
            | "Func"
            | "Go"
            | "Goto"
            | "If"
            | "Import"
            | "Interface"
            | "Map"
            | "Package"
            | "Range"
            | "Return"
            | "Select"
            | "Struct"
            | "Switch"
            | "Type"
            | "Var"
    )
}
