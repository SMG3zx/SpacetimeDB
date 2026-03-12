"""Smoketests for Go server modules."""
import unittest
import tempfile
import shutil
import subprocess
from pathlib import Path

from .. import run_cmd, STDB_DIR, spacetime, Smoketest


def _have_tinygo():
    try:
        run_cmd("tinygo", "version", capture_stderr=True, log=False)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False

HAVE_TINYGO = _have_tinygo()

def requires_tinygo(item):
    if HAVE_TINYGO:
        return item
    return unittest.skip("TinyGo not available")(item)


# Path to the Go server-side bindings and the shared Go SDK within the repo.
GO_SERVER_PATH = (STDB_DIR / "crates" / "bindings-go").absolute()
GO_SDK_PATH = (STDB_DIR / "sdks" / "go").absolute()
GO_TEMPLATE_PATH = STDB_DIR / "templates" / "basic-go" / "spacetimedb"

# The main.go from the Go template is a working module with Person table,
# Add reducer, and SayHello reducer.
GO_TEMPLATE_MAIN = open(GO_TEMPLATE_PATH / "main.go").read()


def _setup_go_module(tmpdir):
    """Initialise a Go module project using ``spacetime init`` and fix up
    the ``go.mod`` so that it uses the local SDK paths instead of the
    (potentially unpublished) Go module proxy versions.
    """

    spacetime(
        "init",
        "--non-interactive",
        "--lang=go",
        "--project-path",
        tmpdir,
        "go-test-project",
    )

    project_path = Path(tmpdir) / "spacetimedb"

    # Rewrite go.mod replace directives to point at the local checkout.
    go_mod = project_path / "go.mod"
    contents = go_mod.read_text()
    contents = contents.replace("SPACETIMEDB_GO_PATH", str(GO_SDK_PATH))
    contents = contents.replace("SPACETIMEDB_GO_SERVER_PATH", str(GO_SERVER_PATH))
    go_mod.write_text(contents)

    # Run go mod tidy to resolve dependencies.
    run_cmd("go", "mod", "tidy", cwd=project_path, capture_stderr=True)

    return project_path


def _write_main_go(project_path, code):
    """Write *code* to the project's main.go."""
    (project_path / "main.go").write_text(code)


@requires_tinygo
class GoModuleBuild(unittest.TestCase):
    """Test CLI can create and compile a Go project."""

    def test_build_go_module(self):
        """
        Ensure that the CLI can create and compile a Go project via
        ``spacetime init --lang=go`` followed by ``spacetime build``.
        This test does not depend on a running SpacetimeDB instance.
        Skips if TinyGo is not available.
        """

        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = _setup_go_module(tmpdir)

            # Verify the expected files were created.
            self.assertTrue(
                (project_path / "go.mod").exists(),
                "go.mod should exist after spacetime init --lang=go",
            )
            self.assertTrue(
                (project_path / "main.go").exists(),
                "main.go should exist after spacetime init --lang=go",
            )
            self.assertTrue(
                (project_path / "stdb.yaml").exists(),
                "stdb.yaml should exist after spacetime init --lang=go",
            )

            # Build the module.  spacetime build invokes TinyGo under the hood.
            try:
                spacetime("build", "--module-path", str(project_path), capture_stderr=True)
            except subprocess.CalledProcessError as e:
                print("stdout:", e.stdout)
                print("stderr:", e.stderr if hasattr(e, "stderr") else "(captured)")
                raise


@requires_tinygo
class GoModuleReducers(Smoketest):
    """Test Go module reducers work end-to-end.

    This test publishes a Go module, calls reducers via the CLI, and
    verifies the results through SQL queries and logs.
    """

    AUTOPUBLISH = False
    MODULE_CODE = ""  # We override setUpClass to handle Go projects.

    @classmethod
    def setUpClass(cls):
        # Create a temporary directory that persists for the whole test class.
        cls._tmpdir_obj = tempfile.TemporaryDirectory()
        cls._tmpdir = cls._tmpdir_obj.name

        cls.project_path = _setup_go_module(cls._tmpdir)

        # Write the template main.go (contains Person table, Add, SayHello).
        _write_main_go(cls.project_path, GO_TEMPLATE_MAIN)

        # Set up config path for the Smoketest helpers.
        cls.config_path = cls.project_path / "config.toml"
        cls.reset_config()

        # Build and publish.
        cls.publish_module(cls, capture_stderr=True)

    @classmethod
    def tearDownClass(cls):
        # Clean up the database.
        if hasattr(cls, "database_identity"):
            try:
                cls.spacetime("delete", "--yes", cls.database_identity)
            except Exception:
                pass
        cls._tmpdir_obj.cleanup()

    def test_go_module_reducers(self):
        """Call Add and SayHello reducers and verify SQL output and logs."""

        # Insert a person via the Add reducer.
        self.call("Add", "Alice")
        self.call("Add", "Bob")

        # Verify the Person table has the expected rows.
        sql_out = self.sql("SELECT * FROM Person")
        self.assertIn("Alice", sql_out)
        self.assertIn("Bob", sql_out)

        # Call the SayHello reducer which logs greetings.
        self.call("SayHello")

        # Verify logs contain the expected greeting messages.
        logs = self.logs(10)
        hello_logs = [l for l in logs if "Hello" in l]
        self.assertTrue(
            any("Hello, Alice!" in l for l in hello_logs),
            f"Expected 'Hello, Alice!' in logs, got: {hello_logs}",
        )
        self.assertTrue(
            any("Hello, Bob!" in l for l in hello_logs),
            f"Expected 'Hello, Bob!' in logs, got: {hello_logs}",
        )
        self.assertTrue(
            any("Hello, World!" in l for l in hello_logs),
            f"Expected 'Hello, World!' in logs, got: {hello_logs}",
        )


@requires_tinygo
class GoModuleCodegen(unittest.TestCase):
    """Test ``spacetime generate --lang go`` produces Go binding files."""

    def test_go_codegen_output(self):
        """
        Build a Go module and then run ``spacetime generate --lang go`` to
        verify that Go client binding files are created.
        """

        with tempfile.TemporaryDirectory() as tmpdir:
            project_path = _setup_go_module(tmpdir)

            # Build the module first (generate needs the compiled WASM).
            try:
                spacetime("build", "--module-path", str(project_path), capture_stderr=True)
            except subprocess.CalledProcessError as e:
                print("stdout:", e.stdout)
                print("stderr:", e.stderr if hasattr(e, "stderr") else "(captured)")
                raise

            # Generate Go client bindings.
            out_dir = Path(tmpdir) / "generated_bindings"
            out_dir.mkdir()

            spacetime(
                "generate",
                "--lang", "go",
                "--out-dir", str(out_dir),
                "--module-path", str(project_path),
                capture_stderr=True,
            )

            # Verify that at least one .go file was generated.
            go_files = list(out_dir.glob("*.go"))
            self.assertTrue(
                len(go_files) > 0,
                f"Expected .go files in {out_dir}, found: {list(out_dir.iterdir())}",
            )

            # Check that the generated files contain expected type/function names.
            all_content = ""
            for f in go_files:
                all_content += f.read_text()

            # The template module has a Person table, so we expect Person-related output.
            self.assertIn("Person", all_content,
                          "Generated Go bindings should reference the Person table")
