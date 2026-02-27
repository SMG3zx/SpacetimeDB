use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Context;
use itertools::Itertools;
use std::ffi::OsString;

use crate::detect::{has_go, has_go_fmt};

pub(crate) fn build_go(project_path: &Path, build_debug: bool) -> anyhow::Result<PathBuf> {
    if !has_go() {
        anyhow::bail!("Go compiler not found in PATH. Please install Go (1.21+ recommended).");
    }

    let output_dir = project_path.join("build");
    fs::create_dir_all(&output_dir).context("Failed to create Go build output directory")?;
    let output = output_dir.join("module.wasm");

    let mut args = vec!["build".to_string()];
    if !build_debug {
        args.push("-trimpath".to_string());
    }
    args.push("-o".to_string());
    args.push(output.to_string_lossy().to_string());
    args.push(".".to_string());

    let mut cmd = duct::cmd("go", args);

    cmd = cmd.dir(project_path).env("GOOS", "wasip1").env("GOARCH", "wasm");

    cmd.run().context(
        "Failed to build Go module for WASI. Ensure your module is a valid Go WASM target and uses package main.",
    )?;

    Ok(output)
}

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
