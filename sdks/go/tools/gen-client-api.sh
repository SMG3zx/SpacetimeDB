#!/usr/bin/env bash

set -ueo pipefail

SDK_PATH="$(dirname "$0")/.."
SDK_PATH="$(realpath "$SDK_PATH")"
STDB_PATH="$SDK_PATH/../.."

OUT_DIR="$SDK_PATH/internal/clientapi/.output"
DEST_DIR="$SDK_PATH/internal/clientapi"

rm -rf "$OUT_DIR"
mkdir -p "$OUT_DIR" "$DEST_DIR"

cargo run --manifest-path "$STDB_PATH/crates/client-api-messages/Cargo.toml" --example get_ws_schema_v2 | \
cargo run --manifest-path "$STDB_PATH/crates/cli/Cargo.toml" -- generate -l go \
  --module-def \
  -o "$OUT_DIR"

while IFS= read -r -d '' file; do
  sed -i 's/^package module_bindings$/package clientapi/' "$file"
done < <(find "$OUT_DIR" -type f -name "*.go" -print0)

find "$DEST_DIR" -mindepth 1 -not -name "doc.go" -exec rm -rf {} +
cp -R "$OUT_DIR"/. "$DEST_DIR"/
rm -rf "$OUT_DIR"
