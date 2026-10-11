#!/usr/bin/env bash
# Packages the crate as `cargo publish` would, inspects the package, builds a
# consumer against the unpacked package outside the repository, and runs it
# against a fresh database:
#
#   scripts/release/smoke-rust.sh
set -euo pipefail
root=$(git rev-parse --show-toplevel)
source "$root/scripts/release/smoke-lib.sh"

cd "$root/postgremq-rs"
version=$(awk -F'"' '/^version = / { print $2; exit }' Cargo.toml)
cargo package --locked --allow-dirty --list > "$SMOKE_WORK/files"
for required in build.rs Cargo.toml README.md LICENSE src/lib.rs migrations/000001_initial_schema.up.sql; do
  grep -qx "$required" "$SMOKE_WORK/files" || { echo "smoke-rust: package lacks $required" >&2; exit 1; }
done
if grep -q '^tests/' "$SMOKE_WORK/files"; then
  echo "smoke-rust: package includes tests/" >&2; exit 1
fi
cargo package --locked --allow-dirty
mkdir -p "$SMOKE_WORK/crate"
tar -xzf "target/package/postgremq-$version.crate" -C "$SMOKE_WORK/crate"

mkdir -p "$SMOKE_WORK/app/src"
cp "$root/scripts/release/smoke/rust/src/main.rs" "$SMOKE_WORK/app/src/"
cat > "$SMOKE_WORK/app/Cargo.toml" <<TOML
[package]
name = "postgremq-smoke"
version = "0.0.0"
edition = "2024"
publish = false

[dependencies]
postgremq = { path = "$SMOKE_WORK/crate/postgremq-$version" }
serde_json = "1"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
TOML
cd "$SMOKE_WORK/app"
smoke_database pmq_smoke_rust
CARGO_TARGET_DIR="$root/postgremq-rs/target/smoke" cargo run --quiet
