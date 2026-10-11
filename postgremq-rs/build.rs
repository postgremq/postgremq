//! Embeds the schema migrations this crate bundles: every
//! `migrations/<number>_<mq version>.up.sql`, in version order.
//!
//! `migrations/` is a copy of mq/migrations under numbered names, which
//! `make sync` keeps in step with it (CI checks it), so the commit that changes
//! the schema also changes this crate and releases it. Do not edit it here.

use std::error::Error;
use std::fmt::Write as _;
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR")?;
    let dir = Path::new(&manifest_dir).join("migrations");
    println!("cargo:rerun-if-changed=migrations");

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
    if migrations.is_empty() {
        return Err(format!("{} has no migrations", dir.display()).into());
    }
    if let Some(pair) = migrations.windows(2).find(|pair| pair[0].0 == pair[1].0) {
        return Err(format!(
            "two migrations numbered {}: {} and {}",
            pair[0].0, pair[0].1, pair[1].1
        )
        .into());
    }

    let mut out = String::new();
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
