package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestRun_Success tests run() with a valid schema file.
func TestRun_Success(t *testing.T) {
	tmpDir := t.TempDir()
	schemaFile := filepath.Join(tmpDir, "test.yaml")
	outFile := filepath.Join(tmpDir, "generated.go")

	yamlContent := `
package: testpkg
tables:
  - name: Item
    columns:
      - name: id
        type: U32
      - name: label
        type: String
    primary_key: [id]
reducers:
  - name: AddItem
    params:
      - name: label
        type: String
`
	if err := os.WriteFile(schemaFile, []byte(yamlContent), 0644); err != nil {
		t.Fatal(err)
	}

	code := run([]string{"-schema", schemaFile, "-out", outFile, "-pkg", "testpkg"})
	if code != 0 {
		t.Fatalf("run returned %d, want 0", code)
	}

	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "package testpkg") {
		t.Error("output should contain package testpkg")
	}
	if !strings.Contains(string(data), "type Item struct") {
		t.Error("output should contain Item struct")
	}
}

// TestRun_MissingSchemaFile tests run() when the schema file doesn't exist.
func TestRun_MissingSchemaFile(t *testing.T) {
	code := run([]string{"-schema", "/nonexistent/path.yaml"})
	if code == 0 {
		t.Fatal("expected non-zero exit code for missing schema file")
	}
}

// TestRun_InvalidYAML tests run() with malformed YAML.
func TestRun_InvalidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	schemaFile := filepath.Join(tmpDir, "bad.yaml")
	if err := os.WriteFile(schemaFile, []byte(":\n  :\n    - [invalid"), 0644); err != nil {
		t.Fatal(err)
	}

	code := run([]string{"-schema", schemaFile})
	if code == 0 {
		t.Fatal("expected non-zero exit code for invalid YAML")
	}
}

// TestRun_DefaultPackage tests that run() defaults package to "main" when not specified.
func TestRun_DefaultPackage(t *testing.T) {
	tmpDir := t.TempDir()
	schemaFile := filepath.Join(tmpDir, "test.yaml")
	outFile := filepath.Join(tmpDir, "generated.go")

	if err := os.WriteFile(schemaFile, []byte("tables: []\n"), 0644); err != nil {
		t.Fatal(err)
	}

	code := run([]string{"-schema", schemaFile, "-out", outFile})
	if code != 0 {
		t.Fatalf("run returned %d, want 0", code)
	}

	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "package main") {
		t.Error("should default to package main")
	}
}

// TestRun_PkgOverride tests that -pkg flag overrides schema package.
func TestRun_PkgOverride(t *testing.T) {
	tmpDir := t.TempDir()
	schemaFile := filepath.Join(tmpDir, "test.yaml")
	outFile := filepath.Join(tmpDir, "generated.go")

	if err := os.WriteFile(schemaFile, []byte("package: original\ntables: []\n"), 0644); err != nil {
		t.Fatal(err)
	}

	code := run([]string{"-schema", schemaFile, "-out", outFile, "-pkg", "override"})
	if code != 0 {
		t.Fatalf("run returned %d, want 0", code)
	}

	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "package override") {
		t.Error("should use overridden package name")
	}
}

// TestRun_UnwritableOutput tests run() when the output path is not writable.
func TestRun_UnwritableOutput(t *testing.T) {
	tmpDir := t.TempDir()
	schemaFile := filepath.Join(tmpDir, "test.yaml")
	if err := os.WriteFile(schemaFile, []byte("package: testpkg\ntables: []\n"), 0644); err != nil {
		t.Fatal(err)
	}

	code := run([]string{"-schema", schemaFile, "-out", "/nonexistent/dir/output.go"})
	if code == 0 {
		t.Fatal("expected non-zero exit code for unwritable output path")
	}
}

// TestRun_SchemaFromPackageField tests that schema package field is used when -pkg is not set.
func TestRun_SchemaFromPackageField(t *testing.T) {
	tmpDir := t.TempDir()
	schemaFile := filepath.Join(tmpDir, "test.yaml")
	outFile := filepath.Join(tmpDir, "generated.go")

	if err := os.WriteFile(schemaFile, []byte("package: mypkg\ntables: []\n"), 0644); err != nil {
		t.Fatal(err)
	}

	code := run([]string{"-schema", schemaFile, "-out", outFile})
	if code != 0 {
		t.Fatalf("run returned %d, want 0", code)
	}

	data, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "package mypkg") {
		t.Error("should use schema's package name")
	}
}

// TestRun_BadFlags tests run() with invalid flags.
func TestRun_BadFlags(t *testing.T) {
	code := run([]string{"-invalid-flag"})
	if code == 0 {
		t.Fatal("expected non-zero exit code for bad flags")
	}
}

// TestRun_WithTests tests the -tests flag generates a test file.
func TestRun_WithTests(t *testing.T) {
	tmpDir := t.TempDir()
	schemaFile := filepath.Join(tmpDir, "test.yaml")
	outFile := filepath.Join(tmpDir, "generated_stdb.go")

	yamlContent := `
package: testpkg
tables:
  - name: Player
    columns:
      - name: id
        type: U64
      - name: name
        type: String
`
	if err := os.WriteFile(schemaFile, []byte(yamlContent), 0644); err != nil {
		t.Fatal(err)
	}

	code := run([]string{"-schema", schemaFile, "-out", outFile, "-tests"})
	if code != 0 {
		t.Fatalf("run returned %d, want 0", code)
	}

	// Check main output
	if _, err := os.ReadFile(outFile); err != nil {
		t.Fatal("main output file not written")
	}

	// Check test output
	testFile := filepath.Join(tmpDir, "generated_stdb_test.go")
	testData, err := os.ReadFile(testFile)
	if err != nil {
		t.Fatal("test output file not written")
	}
	if !strings.Contains(string(testData), "TestPlayerEncodeDecode") {
		t.Error("test file should contain TestPlayerEncodeDecode")
	}
}

// TestRun_WithTestsNoTables tests -tests flag with no tables (should skip test generation).
func TestRun_WithTestsNoTables(t *testing.T) {
	tmpDir := t.TempDir()
	schemaFile := filepath.Join(tmpDir, "test.yaml")
	outFile := filepath.Join(tmpDir, "generated_stdb.go")

	if err := os.WriteFile(schemaFile, []byte("package: testpkg\ntables: []\n"), 0644); err != nil {
		t.Fatal(err)
	}

	code := run([]string{"-schema", schemaFile, "-out", outFile, "-tests"})
	if code != 0 {
		t.Fatalf("run returned %d, want 0", code)
	}

	// Test file should NOT be generated when there are no tables
	testFile := filepath.Join(tmpDir, "generated_stdb_test.go")
	if _, err := os.ReadFile(testFile); err == nil {
		t.Error("test file should not be generated when there are no tables")
	}
}

// TestRun_WithTestsUnwritable tests -tests flag when test output path is not writable.
func TestRun_WithTestsUnwritable(t *testing.T) {
	tmpDir := t.TempDir()
	schemaFile := filepath.Join(tmpDir, "test.yaml")

	yamlContent := `
package: testpkg
tables:
  - name: Item
    columns:
      - name: id
        type: U32
`
	if err := os.WriteFile(schemaFile, []byte(yamlContent), 0644); err != nil {
		t.Fatal(err)
	}

	// Create a subdirectory that we can make read-only to prevent test file writes.
	// The main output goes to a writable temp dir, but the test file derived from it
	// will be in the same dir. We make the dir read-only after writing the schema.
	restrictedDir := filepath.Join(tmpDir, "restricted")
	if err := os.MkdirAll(restrictedDir, 0755); err != nil {
		t.Fatal(err)
	}
	outFile := filepath.Join(restrictedDir, "generated_stdb.go")

	// First run without -tests to verify it works
	code := run([]string{"-schema", schemaFile, "-out", outFile})
	if code != 0 {
		t.Fatalf("run without -tests returned %d, want 0", code)
	}

	// Make directory read-only so test file write fails
	if err := os.Chmod(restrictedDir, 0444); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(restrictedDir, 0755) // restore for cleanup

	// Now run with -tests; main file write will also fail since dir is read-only
	code = run([]string{"-schema", schemaFile, "-out", outFile, "-tests"})
	if code == 0 {
		t.Fatal("expected non-zero exit code when output dir is read-only")
	}
}
