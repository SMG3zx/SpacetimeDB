@echo off
setlocal

set "SDK_PATH=%~dp0.."
set "STDB_PATH=%SDK_PATH%\..\.."
set "OUT_DIR=%SDK_PATH%\internal\clientapi\.output"
set "DEST_DIR=%SDK_PATH%\internal\clientapi"

if not exist "%OUT_DIR%" mkdir "%OUT_DIR%"
if not exist "%DEST_DIR%" mkdir "%DEST_DIR%"

cargo run --manifest-path "%STDB_PATH%\crates\client-api-messages\Cargo.toml" --example get_ws_schema_v2 | ^
cargo run --manifest-path "%STDB_PATH%\crates\cli\Cargo.toml" -- generate -l go ^
  --module-def ^
  -o "%OUT_DIR%"

for %%f in ("%OUT_DIR%\*.go") do (
  powershell -NoProfile -Command "(Get-Content '%%~ff') -replace '^package module_bindings$','package clientapi' | Set-Content '%%~ff'"
)

for %%f in ("%DEST_DIR%\*.go") do (
  if /I not "%%~nxf"=="doc.go" del /q "%%~ff"
)
copy /y "%OUT_DIR%\*.go" "%DEST_DIR%\" >nul
rmdir /s /q "%OUT_DIR%"

endlocal
