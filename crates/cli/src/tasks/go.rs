use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

/// Placeholder hook for formatting generated Go bindings.
/// Intentionally a no-op for now; we can run `gofmt` here in a follow-up.
pub(crate) fn gofmt(_project_dir: &Path, _generated_files: BTreeSet<PathBuf>) -> anyhow::Result<()> {
    Ok(())
}
