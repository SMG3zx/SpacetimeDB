use crate::{CodegenOptions, Lang, OutputFile};
use spacetimedb_schema::def::{ModuleDef, ProcedureDef, ReducerDef, TableDef, TypeDef};
use spacetimedb_schema::schema::TableSchema;

/// Stub Go backend used while generation logic is implemented incrementally.
#[derive(Clone, Copy, Debug, Default)]
pub struct Go;

impl Lang for Go {
    fn generate_table_file_from_schema(&self, _module: &ModuleDef, table: &TableDef, _schema: TableSchema) -> OutputFile {
        OutputFile {
            filename: format!("{}_table.go", table.accessor_name),
            code: String::new(),
        }
    }

    fn generate_type_files(&self, _module: &ModuleDef, typ: &TypeDef) -> Vec<OutputFile> {
        vec![OutputFile {
            filename: format!("{}.go", typ.accessor_name),
            code: String::new(),
        }]
    }

    fn generate_reducer_file(&self, _module: &ModuleDef, reducer: &ReducerDef) -> OutputFile {
        OutputFile {
            filename: format!("{}_reducer.go", reducer.accessor_name),
            code: String::new(),
        }
    }

    fn generate_procedure_file(&self, _module: &ModuleDef, procedure: &ProcedureDef) -> OutputFile {
        OutputFile {
            filename: format!("{}_procedure.go", procedure.accessor_name),
            code: String::new(),
        }
    }

    fn generate_global_files(&self, _module: &ModuleDef, _options: &CodegenOptions) -> Vec<OutputFile> {
        vec![OutputFile {
            filename: "client.go".to_string(),
            code: String::new(),
        }]
    }
}
