use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};

use aprodb_engine::{
    AproError as EngineError, AtomicMutation, Durability as EngineDurability, Engine, EngineConfig,
    Payload, PutRequest, RecordIdentity, VerificationReport,
};
use serde::{Deserialize, Serialize};

use crate::{Config, Database, Value};

const LEGACY_FILES: [&str; 2] = ["aprodb.snapshot", "aprodb.wal"];

#[derive(Clone, Debug)]
pub struct LegacyImportOptions {
    pub source: PathBuf,
    pub source_copy: PathBuf,
    pub destination: EngineConfig,
    pub tenant: Vec<u8>,
    pub namespace: Vec<u8>,
    pub collection: Vec<u8>,
    pub partition: Vec<u8>,
    pub max_records: usize,
    pub max_stored_bytes: usize,
    pub max_source_bytes: u64,
    pub batch_operations: usize,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LegacySourceFile {
    pub name: String,
    pub bytes: u64,
    pub blake3: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct LegacyImportReport {
    pub source: PathBuf,
    pub preserved_copy: PathBuf,
    pub destination: PathBuf,
    pub source_files: Vec<LegacySourceFile>,
    pub records_imported: usize,
    pub verification: VerificationReport,
}

pub fn import_0_1(mut options: LegacyImportOptions) -> aprodb_engine::Result<LegacyImportReport> {
    if options.max_records == 0
        || options.max_stored_bytes == 0
        || options.max_source_bytes == 0
        || options.batch_operations == 0
        || options.batch_operations > options.destination.limits.max_batch_operations
    {
        return Err(EngineError::InvalidInput(
            "invalid import 0.1 parameters or limits".into(),
        ));
    }
    let source = fs::canonicalize(&options.source).map_err(storage_error)?;
    if !source.is_dir() {
        return Err(EngineError::InvalidInput(
            "0.1 source path is not a directory".into(),
        ));
    }
    let preserved_copy = resolve_new_path(&options.source_copy)?;
    let destination = resolve_new_path(&options.destination.path)?;
    if preserved_copy.starts_with(&source) || destination.starts_with(&source) {
        return Err(EngineError::InvalidInput(
            "import copy and destination must be located outside the 0.1 source directory".into(),
        ));
    }
    if preserved_copy == destination {
        return Err(EngineError::InvalidInput(
            "preserved copy and import destination must not be the same path".into(),
        ));
    }
    fs::create_dir(&preserved_copy).map_err(storage_error)?;
    let raw = preserved_copy.join("raw");
    let reader = preserved_copy.join("reader-copy");
    fs::create_dir(&raw).map_err(storage_error)?;
    fs::create_dir(&reader).map_err(storage_error)?;

    let mut source_files = Vec::new();
    let mut total_source_bytes = 0u64;
    for name in LEGACY_FILES {
        let source_file = source.join(name);
        if !source_file.exists() {
            continue;
        }
        let file = copy_live_source(&source_file, &raw.join(name), options.max_source_bytes)?;
        total_source_bytes = total_source_bytes.checked_add(file.bytes).ok_or_else(|| {
            EngineError::ResourceLimit("0.1 source size exceeds u64 maximum limit".into())
        })?;
        if total_source_bytes > options.max_source_bytes {
            return Err(EngineError::ResourceLimit(format!(
                "0.1 source size exceeds the limit of {} bytes",
                options.max_source_bytes
            )));
        }
        copy_known_file(&raw.join(name), &reader.join(name), &file)?;
        source_files.push(file);
    }
    if source_files.is_empty() {
        return Err(EngineError::IncompatibleFormat(
            "no aprodb.snapshot or aprodb.wal files found in 0.1 source".into(),
        ));
    }
    write_legacy_manifest(&preserved_copy, &source_files)?;

    let legacy = Database::open(Config::new(&reader)).map_err(legacy_error)?;
    let rows = legacy
        .export_for_migration(options.max_records, options.max_stored_bytes)
        .map_err(legacy_error)?;
    drop(legacy);

    let destination_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            EngineError::InvalidInput("import destination directory name is not valid UTF-8".into())
        })?;
    let work = destination
        .parent()
        .expect("path resolved with parent")
        .join(format!(".{destination_name}.aprodb-importing"));
    if work.exists() {
        return Err(EngineError::InvalidInput(format!(
            "import working directory already exists at path: {}",
            work.display()
        )));
    }
    options.destination.path = work.clone();
    let engine = Engine::open(options.destination)?;
    let mut records_imported = 0usize;
    for chunk in rows.chunks(options.batch_operations) {
        let mutations = chunk
            .iter()
            .map(|(key, value)| {
                let identity = RecordIdentity::new(
                    options.tenant.clone(),
                    options.namespace.clone(),
                    options.collection.clone(),
                    options.partition.clone(),
                    key.as_bytes().to_vec(),
                )?;
                let (payload, content_type) = legacy_payload(value.clone());
                let mut put = PutRequest::new(identity, payload);
                put.content_type = content_type.into();
                Ok(AtomicMutation::Put(put))
            })
            .collect::<aprodb_engine::Result<Vec<_>>>()?;
        engine.atomic_batch(mutations, EngineDurability::Durable)?;
        records_imported = records_imported.saturating_add(chunk.len());
    }
    let verification = engine.verify()?;
    drop(engine);
    fs::rename(&work, &destination).map_err(storage_error)?;

    Ok(LegacyImportReport {
        source,
        preserved_copy,
        destination,
        source_files,
        records_imported,
        verification,
    })
}

fn legacy_payload(value: Value) -> (Payload, &'static str) {
    match value {
        Value::Bytes(value) => (Payload::Bytes(value), "application/octet-stream"),
        Value::Text(value) => (Payload::Text(value), "text/plain; charset=utf-8"),
        Value::Integer(value) => (Payload::Integer(value), "application/x-aprodb-integer"),
        Value::Float(value) => (Payload::Float(value), "application/x-aprodb-float"),
        Value::Vector(value) => (Payload::Vector(value), "application/x-aprodb-vector-f32"),
    }
}

fn resolve_new_path(path: &Path) -> aprodb_engine::Result<PathBuf> {
    if path.exists() {
        return Err(EngineError::InvalidInput(format!(
            "destination path already exists: {}",
            path.display()
        )));
    }
    let name = path.file_name().ok_or_else(|| {
        EngineError::InvalidInput("destination path does not contain a valid name".into())
    })?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let parent = fs::canonicalize(parent).map_err(storage_error)?;
    Ok(parent.join(name))
}

fn copy_live_source(
    source: &Path,
    destination: &Path,
    maximum_bytes: u64,
) -> aprodb_engine::Result<LegacySourceFile> {
    let metadata = fs::symlink_metadata(source).map_err(storage_error)?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(EngineError::InvalidInput(format!(
            "0.1 source file is not a regular file at path: {}",
            source.display()
        )));
    }
    if metadata.len() > maximum_bytes {
        return Err(EngineError::ResourceLimit(format!(
            "0.1 source file size exceeds limit of {maximum_bytes} bytes"
        )));
    }
    let (bytes, hash) = stream_copy(source, destination, maximum_bytes)?;
    let (observed_bytes, observed_hash) = hash_file(source, maximum_bytes)?;
    if bytes != observed_bytes || hash != observed_hash || bytes != metadata.len() {
        return Err(EngineError::Conflict(
            "0.1 source changed during copy; please stop all writers and retry".into(),
        ));
    }
    Ok(LegacySourceFile {
        name: source
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| {
                EngineError::InvalidInput("0.1 source filename is not valid UTF-8".into())
            })?
            .into(),
        bytes,
        blake3: hash,
    })
}

fn copy_known_file(
    source: &Path,
    destination: &Path,
    expected: &LegacySourceFile,
) -> aprodb_engine::Result<()> {
    let (bytes, hash) = stream_copy(source, destination, expected.bytes)?;
    if bytes != expected.bytes || hash != expected.blake3 {
        return Err(EngineError::Corrupt(
            "0.1 reader copy does not match preserved copy; data integrity failure".into(),
        ));
    }
    Ok(())
}

fn stream_copy(
    source: &Path,
    destination: &Path,
    maximum_bytes: u64,
) -> aprodb_engine::Result<(u64, String)> {
    let mut input = File::open(source).map_err(storage_error)?;
    let mut output = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(destination)
        .map_err(storage_error)?;
    let mut hasher = blake3::Hasher::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = input.read(&mut buffer).map_err(storage_error)?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(read as u64)
            .ok_or_else(|| EngineError::ResourceLimit("0.1 file exceeds u64".into()))?;
        if bytes > maximum_bytes {
            return Err(EngineError::ResourceLimit(format!(
                "0.1 file exceeds {maximum_bytes} bytes"
            )));
        }
        output.write_all(&buffer[..read]).map_err(storage_error)?;
        hasher.update(&buffer[..read]);
    }
    output.sync_all().map_err(storage_error)?;
    Ok((bytes, hasher.finalize().to_hex().to_string()))
}

fn hash_file(path: &Path, maximum_bytes: u64) -> aprodb_engine::Result<(u64, String)> {
    let mut input = File::open(path).map_err(storage_error)?;
    let mut hasher = blake3::Hasher::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = input.read(&mut buffer).map_err(storage_error)?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(read as u64)
            .ok_or_else(|| EngineError::ResourceLimit("0.1 file exceeds u64".into()))?;
        if bytes > maximum_bytes {
            return Err(EngineError::ResourceLimit(format!(
                "0.1 file exceeds {maximum_bytes} bytes"
            )));
        }
        hasher.update(&buffer[..read]);
    }
    Ok((bytes, hasher.finalize().to_hex().to_string()))
}

fn write_legacy_manifest(root: &Path, files: &[LegacySourceFile]) -> aprodb_engine::Result<()> {
    let bytes = serde_json::to_vec_pretty(files)
        .map_err(|error| EngineError::Storage(format!("0.1 copy manifest write error: {error}")))?;
    let mut manifest = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(root.join("legacy-manifest.json"))
        .map_err(storage_error)?;
    manifest.write_all(&bytes).map_err(storage_error)?;
    manifest.sync_all().map_err(storage_error)
}

fn legacy_error(error: crate::AproError) -> EngineError {
    EngineError::IncompatibleFormat(format!("AProDB 0.1 read error encountered: {error}"))
}

fn storage_error(error: std::io::Error) -> EngineError {
    EngineError::Storage(error.to_string())
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
