//! Embeds the schema migrations this crate bundles.
//!
//! The pin is `[package.metadata.postgremq] mq-schema` in Cargo.toml: a schema
//! version, the number of the last migration to bundle. Migrations 1..N are
//! embedded. `make sync` keeps the pin at the last migration (CI checks it),
//! so a commit adding a migration also changes this crate and releases it.
//! `migrations/` is a symlink to `../mq/migrations`, which `cargo package`
//! follows.

use std::error::Error;
use std::fmt::Write as _;
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let manifest_dir = Path::new(&manifest_dir);
    let dir = manifest_dir.join("migrations");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=migrations");

    let pin = mq_schema_pin(&std::fs::read_to_string(manifest_dir.join("Cargo.toml"))?)?;
    let mut migrations = Vec::new();
    for entry in std::fs::read_dir(&dir)? {
        let name = entry?.file_name().to_string_lossy().into_owned();
        let Some(stem) = name.strip_suffix(".up.sql") else {
            continue;
        };
        let Some((number, _)) = stem.split_once('_') else {
            continue;
        };
        if let Ok(number) = number.parse::<u64>() {
            migrations.push((number, name));
        }
    }
    migrations.sort();
    if let Some(pair) = migrations.windows(2).find(|pair| pair[0].0 == pair[1].0) {
        return Err(format!(
            "two migrations numbered {}: {} and {}",
            pair[0].0, pair[0].1, pair[1].1
        )
        .into());
    }
    if !migrations.iter().any(|(number, _)| *number == pin) {
        return Err(format!(
            "Cargo.toml pins mq schema {pin}, but {} has no migration {pin}",
            dir.display()
        )
        .into());
    }
    migrations.retain(|(number, _)| *number <= pin);

    let mut out = String::new();
    writeln!(
        out,
        "/// The pinned mq schema version (Cargo.toml `[package.metadata.postgremq] mq-schema`)."
    )?;
    writeln!(out, "#[cfg(test)]\nconst MQ_SCHEMA: u64 = {pin};")?;
    writeln!(out, "/// The embedded migrations, in version order.")?;
    writeln!(out, "const MIGRATIONS: &[Migration] = &[")?;
    for (number, name) in &migrations {
        let path = dir.join(name);
        writeln!(
            out,
            "    Migration {{ version: {number}, sql: include_str!({:?}) }},",
            path.display().to_string()
        )?;
    }
    writeln!(out, "];")?;
    std::fs::write(
        Path::new(&std::env::var("OUT_DIR")?).join("migrations.rs"),
        out,
    )?;
    Ok(())
}

/// The `mq-schema` value of `[package.metadata.postgremq]`. Strict on
/// purpose: the line must be exactly `mq-schema = <number>`, so this and the
/// release checks (scripts/release/verify_release.py) read the same pin.
fn mq_schema_pin(manifest: &str) -> Result<u64, Box<dyn Error>> {
    let mut in_table = false;
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_table = line == "[package.metadata.postgremq]";
        } else if in_table && line.starts_with("mq-schema") {
            return line
                .strip_prefix("mq-schema = ")
                .and_then(|value| value.parse::<u64>().ok())
                .filter(|pin| *pin > 0)
                .ok_or_else(|| {
                    format!("Cargo.toml: write the pin exactly as mq-schema = <migration number>, not `{line}`").into()
                });
        }
    }
    Err("Cargo.toml: missing [package.metadata.postgremq] mq-schema = <migration number>".into())
}
