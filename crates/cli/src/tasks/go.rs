use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use anyhow::Context;
use itertools::Itertools;
use std::ffi::OsString;

use crate::detect::has_go_fmt;

pub(crate) fn gofmt(project_dir: &Path, generated_files: BTreeSet<PathBuf>) -> anyhow::Result<()> {
    if !has_go_fmt() {
        anyhow::bail!("gofmt is not installed. Please install Go and ensure `gofmt` is in PATH.");
    }

    let cwd = std::env::current_dir().context("Failed to retrieve current directory")?;
    let go_files = generated_files
        .into_iter()
        .filter(|f| f.extension().is_some_and(|ext| ext == "go"))
        .map(|f| if f.is_absolute() { f } else { cwd.join(f) })
        .map(|f| f.canonicalize().unwrap_or(f))
        .collect_vec();

    if go_files.is_empty() {
        return Ok(());
    }

    duct::cmd(
        "gofmt",
        std::iter::once(OsString::from("-w")).chain(go_files.into_iter().map_into()),
    )
    .dir(project_dir)
    .run()
    .context("Failed to run gofmt on generated Go files")?;

    Ok(())
}
