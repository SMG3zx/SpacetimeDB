#!/usr/bin/env bash

set -ueo pipefail

SDK_PATH="$(dirname "$0")/.."
SDK_PATH="$(realpath "$SDK_PATH")"
STDB_PATH="$SDK_PATH/../.."

OUT_DIR="$SDK_PATH/internal/clientapi/.output"
DEST_DIR="$SDK_PATH/internal/clientapi"

mkdir -p "$OUT_DIR" "$DEST_DIR"

cargo run --manifest-path "$STDB_PATH/crates/client-api-messages/Cargo.toml" --example get_ws_schema_v2 | \
cargo run --manifest-path "$STDB_PATH/crates/cli/Cargo.toml" -- generate -l go \
  --module-def \
  -o "$OUT_DIR"

for file in "$OUT_DIR"/*.go; do
  [ -f "$file" ] || continue
  sed -i 's/^package module_bindings$/package clientapi/' "$file"
done

find "$DEST_DIR" -maxdepth 1 -name "*.go" -type f ! -name "doc.go" -delete
cp "$OUT_DIR"/*.go "$DEST_DIR"/
rm -rf "$OUT_DIR"
