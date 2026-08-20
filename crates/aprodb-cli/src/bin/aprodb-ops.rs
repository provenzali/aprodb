use std::{
    collections::{BTreeMap, VecDeque},
    env, fs,
    path::{Path, PathBuf},
    process::ExitCode,
};

use aprodb::{LegacyImportOptions, import_0_1};
use aprodb_engine::{EncryptionConfig, Engine, EngineConfig, REPAIR_DERIVED_CONFIRMATION};
use serde::Deserialize;

const USAGE: &str = "usage:\n  aprodb-ops verify DATA [--keyring FILE]\n  aprodb-ops verify-backup BACKUP\n  aprodb-ops restore BACKUP DEST [--keyring FILE]\n  aprodb-ops repair SOURCE DEST REBUILD_DERIVED_ON_SEPARATE_COPY [--keyring FILE]\n  aprodb-ops rekey SOURCE DEST --destination-keyring FILE [--source-keyring FILE]\n  aprodb-ops import-0.1 SOURCE PRESERVED DEST TENANT NAMESPACE COLLECTION PARTITION [--destination-keyring FILE] [--max-records N] [--max-stored-bytes N] [--max-source-bytes N] [--batch-operations N]";

fn main() -> ExitCode {
    match run(env::args().skip(1).collect()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("aprodb-ops: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run(arguments: Vec<String>) -> Result<(), String> {
    let mut arguments = VecDeque::from(arguments);
    let command = arguments.pop_front().ok_or(USAGE)?;
    match command.as_str() {
        "verify" => {
            let data = required_path(&mut arguments, "DATA")?;
            let flags = flags(arguments, &["--keyring"])?;
            let engine = Engine::open(engine_config(data, flags.get("--keyring"))?)
                .map_err(|error| error.to_string())?;
            print_json(&engine.verify().map_err(|error| error.to_string())?)
        }
        "verify-backup" => {
            let backup = required_path(&mut arguments, "BACKUP")?;
            ensure_empty(arguments)?;
            print_json(&Engine::verify_backup(backup).map_err(|error| error.to_string())?)
        }
        "restore" => {
            let backup = required_path(&mut arguments, "BACKUP")?;
            let destination = required_path(&mut arguments, "DEST")?;
            let flags = flags(arguments, &["--keyring"])?;
            let config = engine_config(destination.clone(), flags.get("--keyring"))?;
            print_json(
                &Engine::restore_backup(backup, destination, config)
                    .map_err(|error| error.to_string())?,
            )
        }
        "repair" => {
            let source = required_path(&mut arguments, "SOURCE")?;
            let destination = required_path(&mut arguments, "DEST")?;
            let confirmation = required(&mut arguments, "CONFIRMATION")?;
            if confirmation != REPAIR_DERIVED_CONFIRMATION {
                return Err(format!(
                    "repair requires exact confirmation {REPAIR_DERIVED_CONFIRMATION}"
                ));
            }
            let flags = flags(arguments, &["--keyring"])?;
            let engine = Engine::open(engine_config(source, flags.get("--keyring"))?)
                .map_err(|error| error.to_string())?;
            print_json(
                &engine
                    .repair_derived_to_copy(destination, &confirmation)
                    .map_err(|error| error.to_string())?,
            )
        }
        "rekey" => {
            let source = required_path(&mut arguments, "SOURCE")?;
            let destination = required_path(&mut arguments, "DEST")?;
            let flags = flags(arguments, &["--source-keyring", "--destination-keyring"])?;
            let destination_keyring = flags
                .get("--destination-keyring")
                .ok_or("--destination-keyring is required")?;
            let engine = Engine::open(engine_config(source, flags.get("--source-keyring"))?)
                .map_err(|error| error.to_string())?;
            print_json(
                &engine
                    .rekey_to_copy(destination, load_keyring(Path::new(destination_keyring))?)
                    .map_err(|error| error.to_string())?,
            )
        }
        "import-0.1" => {
            let source = required_path(&mut arguments, "SOURCE")?;
            let preserved = required_path(&mut arguments, "PRESERVED")?;
            let destination = required_path(&mut arguments, "DEST")?;
            let tenant = required(&mut arguments, "TENANT")?.into_bytes();
            let namespace = required(&mut arguments, "NAMESPACE")?.into_bytes();
            let collection = required(&mut arguments, "COLLECTION")?.into_bytes();
            let partition = required(&mut arguments, "PARTITION")?.into_bytes();
            let flags = flags(
                arguments,
                &[
                    "--destination-keyring",
                    "--max-records",
                    "--max-stored-bytes",
                    "--max-source-bytes",
                    "--batch-operations",
                ],
            )?;
            let mut destination_config =
                engine_config(destination, flags.get("--destination-keyring"))?;
            destination_config.durability = aprodb_engine::Durability::Durable;
            print_json(
                &import_0_1(LegacyImportOptions {
                    source,
                    source_copy: preserved,
                    destination: destination_config,
                    tenant,
                    namespace,
                    collection,
                    partition,
                    max_records: numeric_flag(&flags, "--max-records", 1_000_000)?,
                    max_stored_bytes: numeric_flag(
                        &flags,
                        "--max-stored-bytes",
                        4 * 1024 * 1024 * 1024,
                    )?,
                    max_source_bytes: numeric_flag(
                        &flags,
                        "--max-source-bytes",
                        16 * 1024 * 1024 * 1024u64,
                    )?,
                    batch_operations: numeric_flag(&flags, "--batch-operations", 256)?,
                })
                .map_err(|error| error.to_string())?,
            )
        }
        "--help" | "-h" => Err(USAGE.into()),
        _ => Err(format!("unknown command: {command}\n{USAGE}")),
    }
}

fn required(arguments: &mut VecDeque<String>, name: &str) -> Result<String, String> {
    arguments
        .pop_front()
        .ok_or_else(|| format!("missing argument {name}"))
}

fn required_path(arguments: &mut VecDeque<String>, name: &str) -> Result<PathBuf, String> {
    required(arguments, name).map(PathBuf::from)
}

fn ensure_empty(arguments: VecDeque<String>) -> Result<(), String> {
    if arguments.is_empty() {
        Ok(())
    } else {
        Err(format!("unexpected argument: {}", arguments[0]))
    }
}

fn flags(
    mut arguments: VecDeque<String>,
    allowed: &[&str],
) -> Result<BTreeMap<String, String>, String> {
    let mut parsed = BTreeMap::new();
    while let Some(name) = arguments.pop_front() {
        if !allowed.contains(&name.as_str()) {
            return Err(format!("unknown option: {name}"));
        }
        let value = required(&mut arguments, &format!("value for {name}"))?;
        if parsed.insert(name.clone(), value).is_some() {
            return Err(format!("duplicate option: {name}"));
        }
    }
    Ok(parsed)
}

fn numeric_flag<T>(flags: &BTreeMap<String, String>, name: &str, default: T) -> Result<T, String>
where
    T: std::str::FromStr,
{
    flags.get(name).map_or(Ok(default), |value| {
        value.parse().map_err(|_| format!("invalid {name}"))
    })
}

fn engine_config(path: PathBuf, keyring: Option<&String>) -> Result<EngineConfig, String> {
    let mut config = EngineConfig::new(path);
    if let Some(keyring) = keyring {
        config.encryption = Some(load_keyring(Path::new(keyring))?);
    }
    Ok(config)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyringFile {
    active_key_id: String,
    keys: BTreeMap<String, String>,
}

fn load_keyring(path: &Path) -> Result<EncryptionConfig, String> {
    let metadata =
        fs::symlink_metadata(path).map_err(|error| format!("keyring metadata error: {error}"))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > 64 * 1024 {
        return Err("keyring must be a regular file within 64 KiB".into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err("keyring must be accessible only by the owner".into());
        }
    }
    let bytes = fs::read(path).map_err(|error| format!("keyring read error: {error}"))?;
    if bytes.len() > 64 * 1024 {
        return Err("keyring grew beyond 64 KiB while reading".into());
    }
    let file: KeyringFile =
        serde_json::from_slice(&bytes).map_err(|error| format!("keyring JSON error: {error}"))?;
    let mut keys = BTreeMap::new();
    for (id, encoded) in file.keys {
        if encoded.len() != 64 {
            return Err(format!("key {id} does not contain 32 hex bytes"));
        }
        let mut key = [0u8; 32];
        hex::decode_to_slice(encoded.as_bytes(), &mut key)
            .map_err(|_| format!("key {id} is not valid hex"))?;
        keys.insert(id, key);
    }
    EncryptionConfig::new(file.active_key_id, keys).map_err(|error| error.to_string())
}

fn print_json(value: &impl serde::Serialize) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string(value).map_err(|error| error.to_string())?
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_rejects_unknown_or_duplicate_flags() {
        assert!(run(vec!["verify".into()]).is_err());
        assert!(flags(VecDeque::from(["--bad".into(), "x".into()]), &[]).is_err());
        assert!(
            flags(
                VecDeque::from([
                    "--keyring".into(),
                    "a".into(),
                    "--keyring".into(),
                    "b".into(),
                ]),
                &["--keyring"],
            )
            .is_err()
        );
        assert_eq!(
            REPAIR_DERIVED_CONFIRMATION,
            "REBUILD_DERIVED_ON_SEPARATE_COPY"
        );
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
