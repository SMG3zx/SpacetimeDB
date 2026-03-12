#![allow(clippy::disallowed_macros)]
use spacetimedb_guard::ensure_binaries_built;
use spacetimedb_smoketests::{require_tinygo, workspace_root, Smoketest};
use std::process::Command;

/// Ensure that the CLI is able to create and compile a Go project using TinyGo.
/// This test does not depend on a running SpacetimeDB instance.
/// Skips if tinygo is not available in PATH.
#[test]
fn test_build_go_module() {
    require_tinygo!();

    let workspace = workspace_root();
    let cli_path = ensure_binaries_built();

    // Create temp directory for the project.
    let tmpdir = tempfile::tempdir().expect("Failed to create temp directory");

    // Initialize Go project via spacetime init.
    let output = Command::new(&cli_path)
        .args([
            "init",
            "--non-interactive",
            "--lang=go",
            "--project-path",
            tmpdir.path().to_str().unwrap(),
            "go-project",
        ])
        .output()
        .expect("Failed to run spacetime init");
    assert!(
        output.status.success(),
        "spacetime init --lang=go failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let server_path = tmpdir.path().join("spacetimedb");
    assert!(server_path.exists(), "spacetimedb/ directory was not created");
    assert!(server_path.join("go.mod").exists(), "go.mod was not created");
    assert!(server_path.join("main.go").exists(), "main.go was not created");

    // Add a replace directive so TinyGo can find the local server SDK.
    // In a real user scenario the module would be fetched from the registry.
    let go_mod_path = server_path.join("go.mod");
    let go_mod = std::fs::read_to_string(&go_mod_path).expect("Failed to read go.mod");
    let sdk_path = workspace.join("crates/bindings-go");
    let client_sdk_path = workspace.join("sdks/go");
    let updated = format!(
        "{}\nreplace github.com/clockworklabs/spacetimedb-go-server => {}\nreplace github.com/clockworklabs/spacetimedb-go => {}\n",
        go_mod,
        sdk_path.display(),
        client_sdk_path.display()
    );
    std::fs::write(&go_mod_path, updated).expect("Failed to write go.mod");

    // Compile the module to WASM using TinyGo (reactor model, no WASI imports).
    let output = Command::new("tinygo")
        .args([
            "build",
            "-target",
            "wasm-unknown",
            "-gc",
            "conservative",
            "-buildmode",
            "c-shared",
            "-o",
            server_path.join("module.wasm").to_str().unwrap(),
            "./",
        ])
        .current_dir(&server_path)
        .output()
        .expect("Failed to run tinygo build");
    assert!(
        output.status.success(),
        "tinygo build failed:\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );

    let wasm_path = server_path.join("module.wasm");
    assert!(wasm_path.exists(), "module.wasm was not produced");
    assert!(
        wasm_path.metadata().unwrap().len() > 0,
        "module.wasm is empty"
    );

    eprintln!(
        "Go module compiled successfully: {} bytes",
        wasm_path.metadata().unwrap().len()
    );
}

/// The Go module source used by the integration tests below.
/// Matches the basic-go template with Add and SayHello reducers.
const GO_MODULE_SOURCE: &str = include_str!("../../../../templates/basic-go/spacetimedb/main.go");

/// Publish a Go module (compiled via TinyGo) to a running server and exercise
/// its reducers and tables.  Skips if TinyGo is not available.
#[test]
fn test_go_module_reducers() {
    require_tinygo!();

    let mut test = Smoketest::builder().autopublish(false).build();

    test.publish_go_module_source("go-module", "go-module-reducers", GO_MODULE_SOURCE)
        .expect("Failed to publish Go module");

    // Insert a person via the Add reducer.
    // Note: SpacetimeDB's default CaseConversionPolicy converts "Add" to "add"
    // and "SayHello" to "say_hello" (snake_case).
    test.call("add", &["Alice"]).expect("add reducer failed");

    // Check the row was inserted. Table name "Person" becomes "person".
    test.assert_sql("SELECT name FROM person", " name\n---------\n \"Alice\"");

    // say_hello iterates the table and logs each person, then logs "Hello, World!".
    test.call("say_hello", &[]).expect("say_hello reducer failed");

    let logs = test.logs(10).expect("Failed to fetch logs");
    assert!(
        logs.iter().any(|l| l.contains("Hello, Alice!")),
        "Expected 'Hello, Alice!' in logs, got: {:?}",
        logs
    );
    assert!(
        logs.iter().any(|l| l.contains("Hello, World!")),
        "Expected 'Hello, World!' in logs, got: {:?}",
        logs
    );
}

/// Publish a Go module and verify that `spacetime generate --lang go` produces
/// the expected client-side binding files.
///
/// NOTE: Currently ignored because Go modules don't emit named TypeDef entries
/// in the typespace, which causes the codegen to panic on type_def_from_ref().
/// This requires a fix in either the Go SDK's __describe_module__ or the codegen.
#[test]
#[ignore]
fn test_go_codegen_output() {
    require_tinygo!();

    let mut test = Smoketest::builder().autopublish(false).build();

    test.publish_go_module_source("go-codegen", "go-codegen-module", GO_MODULE_SOURCE)
        .expect("Failed to publish Go module");

    // The WASM was placed at <project_dir>/go-codegen/spacetimedb/module.wasm
    // by publish_go_module_source.
    let wasm_path = test
        .project_dir
        .path()
        .join("go-codegen")
        .join("spacetimedb")
        .join("module.wasm");
    let wasm_path_str = wasm_path.to_str().unwrap().to_string();

    // Generate Go client bindings directly from the compiled WASM.
    let out_dir = tempfile::tempdir().expect("Failed to create temp directory");
    test.spacetime(&[
        "generate",
        "--lang",
        "go",
        "--out-dir",
        out_dir.path().to_str().unwrap(),
        "--bin-path",
        &wasm_path_str,
    ])
    .expect("spacetime generate failed");

    // Verify expected output files exist.
    let reducers_dir = out_dir.path().join("reducers");

    assert!(
        out_dir.path().join("person_table.go").exists(),
        "Expected person_table.go to be generated, got files: {:?}",
        std::fs::read_dir(out_dir.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect::<Vec<_>>()
    );
    assert!(
        reducers_dir.join("add.go").exists(),
        "Expected reducers/add.go to be generated"
    );
}
