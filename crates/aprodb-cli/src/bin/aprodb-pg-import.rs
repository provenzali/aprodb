// Copyright 2026 Andrea Provenzali and AProDB contributors
// SPDX-License-Identifier: AGPL-3.0-only

use std::{
    collections::{BTreeMap, BTreeSet},
    env, fs,
    io::{self, BufRead, BufReader, Read},
    path::{Path, PathBuf},
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use aprodb_engine::{AtomicMutation, Engine, EngineConfig, ExpectedVersion, PutRequest};
use aprodb_types::{CollectionPolicy, Durability, Payload, RecordIdentity};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

const PARTITIONS: usize = 16;
const DEFAULT_BATCH_OPERATIONS: usize = 256;
const DEFAULT_MAX_DATA_BYTES: u64 = 64 * 1024 * 1024 * 1024;
const DEFAULT_MIN_FREE_DISK_BYTES: u64 = 8 * 1024 * 1024 * 1024;
const DEFAULT_MAX_COMPACTION_TEMPORARY_BYTES: u64 = 16 * 1024 * 1024 * 1024;
const MAX_TOTAL_BUFFERED_BATCH_BYTES: usize = 32 * 1024 * 1024;
const MAX_INPUT_FRAME_BYTES: usize = 17 * 1024 * 1024;
type ImportResult<T> = Result<T, Box<dyn std::error::Error>>;
type SourcePrimaryKey = BTreeMap<String, Vec<u8>>;

enum InputFrame {
    Manifest {
        database: String,
        tables: u64,
    },
    Table {
        schema: String,
        table: String,
        primary_key: Vec<String>,
        estimated_rows: Option<u64>,
    },
    Row {
        ctid: Option<String>,
        tableoid: Option<String>,
        row: Box<RawValue>,
    },
    End {
        rows: Option<u64>,
    },
    Complete {
        tables: u64,
    },
}

#[derive(Debug, Deserialize)]
struct FrameKind {
    kind: String,
}

#[derive(Debug, Deserialize)]
struct TableFrame {
    schema: String,
    table: String,
    #[serde(default)]
    primary_key: Vec<String>,
    estimated_rows: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct RowFrame {
    ctid: Option<String>,
    tableoid: Option<String>,
    row: Box<RawValue>,
}

#[derive(Debug, Deserialize)]
struct EndFrame {
    rows: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ManifestFrame {
    database: String,
    tables: u64,
}

#[derive(Debug, Deserialize)]
struct CompleteFrame {
    tables: u64,
}

#[derive(Clone, Debug)]
struct TableContext {
    schema: String,
    table: String,
    collection: Vec<u8>,
    primary_key: Vec<String>,
    rows: u64,
    logical_bytes: u64,
}

#[derive(Default)]
struct PartitionBuffer {
    mutations: Vec<AtomicMutation>,
    estimated_bytes: usize,
}

#[derive(Debug, Serialize)]
struct ImportSummary {
    tables: usize,
    rows: u64,
    logical_bytes: u64,
    batches: u64,
    elapsed_ms: u128,
    verified_heads: usize,
    verified_events: usize,
    reopened_heads: usize,
    reopened_events: usize,
}

#[derive(Clone, Debug)]
struct Options {
    data_dir: PathBuf,
    tenant: Vec<u8>,
    namespace: Vec<u8>,
    durability: Durability,
    batch_operations: usize,
    progress_every: u64,
    max_data_bytes: u64,
    min_free_disk_bytes: u64,
    max_compaction_temporary_bytes: u64,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("aprodb-pg-import: {error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let options = parse_options(env::args().skip(1))?;
    let summary = publish_import(BufReader::new(io::stdin().lock()), &options)?;
    println!("{}", serde_json::to_string(&summary)?);
    Ok(())
}

fn usage() -> &'static str {
    "usage: aprodb-pg-import --data-dir PATH [--tenant NAME] [--namespace NAME] \
     [--durability durable|relaxed] [--batch-operations N] [--progress-every N] \
     [--max-data-bytes N] [--min-free-disk-bytes N] \
     [--max-compaction-temporary-bytes N]"
}

fn parse_options(arguments: impl Iterator<Item = String>) -> Result<Options, String> {
    let mut values = arguments.collect::<Vec<_>>().into_iter();
    let mut data_dir = None;
    let mut tenant = b"commit".to_vec();
    let mut namespace = b"emeroteca".to_vec();
    let mut durability = Durability::Durable;
    let mut batch_operations = DEFAULT_BATCH_OPERATIONS;
    let mut progress_every = 100_000;
    let mut max_data_bytes = DEFAULT_MAX_DATA_BYTES;
    let mut min_free_disk_bytes = DEFAULT_MIN_FREE_DISK_BYTES;
    let mut max_compaction_temporary_bytes = DEFAULT_MAX_COMPACTION_TEMPORARY_BYTES;
    while let Some(flag) = values.next() {
        let mut value = || {
            values
                .next()
                .ok_or_else(|| format!("missing value for {flag}"))
        };
        match flag.as_str() {
            "--data-dir" => data_dir = Some(PathBuf::from(value()?)),
            "--tenant" => tenant = value()?.into_bytes(),
            "--namespace" => namespace = value()?.into_bytes(),
            "--durability" => {
                durability = match value()?.as_str() {
                    "durable" => Durability::Durable,
                    "relaxed" => Durability::Relaxed,
                    _ => return Err("durability must be durable or relaxed".into()),
                };
            }
            "--batch-operations" => {
                batch_operations = value()?
                    .parse()
                    .map_err(|_| "batch operations must be a positive integer")?;
            }
            "--progress-every" => {
                progress_every = value()?
                    .parse()
                    .map_err(|_| "progress interval must be a positive integer")?;
            }
            "--max-data-bytes" => {
                max_data_bytes = value()?
                    .parse()
                    .map_err(|_| "maximum data bytes must be a positive integer")?;
            }
            "--min-free-disk-bytes" => {
                min_free_disk_bytes = value()?
                    .parse()
                    .map_err(|_| "minimum free disk bytes must be a positive integer")?;
            }
            "--max-compaction-temporary-bytes" => {
                max_compaction_temporary_bytes = value()?
                    .parse()
                    .map_err(|_| "compaction temporary bytes must be a positive integer")?;
            }
            "--help" | "-h" => return Err(usage().into()),
            _ => return Err(format!("unknown argument {flag}; {}", usage())),
        }
    }
    if batch_operations == 0
        || progress_every == 0
        || max_data_bytes == 0
        || min_free_disk_bytes == 0
        || max_compaction_temporary_bytes == 0
    {
        return Err("batch, progress, and disk limits must be positive".into());
    }
    Ok(Options {
        data_dir: data_dir.ok_or_else(|| usage().to_string())?,
        tenant,
        namespace,
        durability,
        batch_operations,
        progress_every,
        max_data_bytes,
        min_free_disk_bytes,
        max_compaction_temporary_bytes,
    })
}

fn ensure_new_or_empty_directory(path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if path.exists() && fs::read_dir(path)?.next().is_some() {
        return Err(format!("destination is not empty: {}", path.display()).into());
    }
    Ok(())
}

fn publish_import(
    reader: impl BufRead,
    options: &Options,
) -> Result<ImportSummary, Box<dyn std::error::Error>> {
    let target = &options.data_dir;
    if target.exists() {
        return Err(format!(
            "destination must not already exist for atomic publication: {}",
            target.display()
        )
        .into());
    }
    let parent = target
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;
    let file_name = target
        .file_name()
        .ok_or_else(|| format!("destination has no directory name: {}", target.display()))?;
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let staging = parent.join(format!(
        ".{}.importing-{}-{nonce}",
        file_name.to_string_lossy(),
        std::process::id()
    ));
    fs::create_dir(&staging)?;

    let mut staging_options = options.clone();
    staging_options.data_dir = staging.clone();
    eprintln!("import staging directory: {}", staging.display());
    let summary = import_reader(reader, &staging_options).map_err(|error| {
        format!(
            "import failed; incomplete staging directory preserved at {}: {error}",
            staging.display()
        )
    })?;
    fs::rename(&staging, target)?;
    eprintln!("published imported database at {}", target.display());
    Ok(summary)
}

fn import_reader(
    mut reader: impl BufRead,
    options: &Options,
) -> Result<ImportSummary, Box<dyn std::error::Error>> {
    ensure_new_or_empty_directory(&options.data_dir)?;
    let started = Instant::now();
    let path = options.data_dir.clone();
    let engine = Engine::open(import_engine_config(options, &path))?;
    let mut current: Option<TableContext> = None;
    let mut seen_tables = BTreeSet::new();
    let mut expected_tables = None;
    let mut complete = false;
    let mut buffers = std::array::from_fn(|_| PartitionBuffer::default());
    let mut total_rows = 0u64;
    let mut total_bytes = 0u64;
    let mut batches = 0u64;

    let mut line = String::new();
    let mut line_number = 0usize;
    loop {
        line.clear();
        let bytes = (&mut reader)
            .take(u64::try_from(MAX_INPUT_FRAME_BYTES.saturating_add(1)).unwrap_or(u64::MAX))
            .read_line(&mut line)?;
        if bytes == 0 {
            break;
        }
        line_number = line_number.saturating_add(1);
        if bytes > MAX_INPUT_FRAME_BYTES {
            return Err(format!(
                "JSONL frame at line {line_number} exceeds the {MAX_INPUT_FRAME_BYTES}-byte limit"
            )
            .into());
        }
        if line.trim().is_empty() {
            continue;
        }
        let frame = parse_input_frame(&line)
            .map_err(|error| format!("invalid JSONL frame at line {line_number}: {error}"))?;
        if complete {
            return Err(format!("frame received after completion at line {line_number}").into());
        }
        match frame {
            InputFrame::Manifest { database, tables } => {
                if expected_tables.replace(tables).is_some() || !seen_tables.is_empty() {
                    return Err("duplicate or out-of-order manifest frame".into());
                }
                validate_source_name("database", &database)?;
                eprintln!("import manifest: database {database}, {tables} tables");
            }
            InputFrame::Table {
                schema,
                table,
                primary_key,
                estimated_rows,
            } => {
                if expected_tables.is_none() {
                    return Err("table header received before the manifest".into());
                }
                flush_all(&engine, &mut buffers, options.durability, &mut batches)?;
                if let Some(previous) = current.as_ref() {
                    return Err(format!(
                        "missing end frame for {}.{}",
                        previous.schema, previous.table
                    )
                    .into());
                }
                validate_source_name("schema", &schema)?;
                validate_source_name("table", &table)?;
                let source_name = format!("{schema}.{table}");
                if !seen_tables.insert(source_name.clone()) {
                    return Err(format!("duplicate table header: {source_name}").into());
                }
                let collection = source_name.as_bytes().to_vec();
                let identity = RecordIdentity::new(
                    options.tenant.clone(),
                    options.namespace.clone(),
                    collection.clone(),
                    b"p00".to_vec(),
                    b"catalog".to_vec(),
                )?;
                engine.configure_collection(&identity, CollectionPolicy::default())?;
                if let Some(estimated) = estimated_rows {
                    eprintln!("starting {source_name}; estimated rows: {estimated}");
                } else {
                    eprintln!("starting {source_name}");
                }
                current = Some(TableContext {
                    schema,
                    table,
                    collection,
                    primary_key,
                    rows: 0,
                    logical_bytes: 0,
                });
            }
            InputFrame::Row {
                ctid,
                tableoid,
                row,
            } => {
                let table = current
                    .as_mut()
                    .ok_or("row frame received before a table header")?;
                let (identity, metadata) =
                    row_identity(options, table, ctid.as_deref(), tableoid.as_deref(), &row)?;
                let payload_bytes = row.get().as_bytes().to_vec();
                let estimated_bytes = payload_bytes.len().saturating_add(512);
                let bucket = partition_bucket(&identity);
                let buffered_bytes = buffers
                    .iter()
                    .map(|buffer| buffer.estimated_bytes)
                    .fold(0usize, usize::saturating_add);
                if buffered_bytes.saturating_add(estimated_bytes) > MAX_TOTAL_BUFFERED_BATCH_BYTES
                    && let Some((largest, _)) = buffers
                        .iter()
                        .enumerate()
                        .filter(|(_, buffer)| !buffer.mutations.is_empty())
                        .max_by_key(|(_, buffer)| buffer.estimated_bytes)
                {
                    flush_partition(
                        &engine,
                        &mut buffers[largest],
                        options.durability,
                        &mut batches,
                    )?;
                }
                if !buffers[bucket].mutations.is_empty()
                    && (buffers[bucket].mutations.len() >= options.batch_operations
                        || buffers[bucket]
                            .estimated_bytes
                            .saturating_add(estimated_bytes)
                            > MAX_TOTAL_BUFFERED_BATCH_BYTES)
                {
                    flush_partition(
                        &engine,
                        &mut buffers[bucket],
                        options.durability,
                        &mut batches,
                    )?;
                }
                let mut request = PutRequest::new(
                    identity,
                    Payload::Document {
                        schema_version: 1,
                        bytes: payload_bytes,
                    },
                );
                request.content_type = "application/json".into();
                request.metadata = metadata;
                request.expected = ExpectedVersion::Missing;
                buffers[bucket].mutations.push(AtomicMutation::Put(request));
                buffers[bucket].estimated_bytes = buffers[bucket]
                    .estimated_bytes
                    .saturating_add(estimated_bytes);
                table.rows = table.rows.saturating_add(1);
                table.logical_bytes = table
                    .logical_bytes
                    .saturating_add(u64::try_from(row.get().len()).unwrap_or(u64::MAX));
                total_rows = total_rows.saturating_add(1);
                total_bytes =
                    total_bytes.saturating_add(u64::try_from(row.get().len()).unwrap_or(u64::MAX));
                if total_rows.is_multiple_of(options.progress_every) {
                    eprintln!("import progress: {total_rows} rows, {total_bytes} logical bytes");
                }
            }
            InputFrame::End { rows } => {
                flush_all(&engine, &mut buffers, options.durability, &mut batches)?;
                let table = current.take().ok_or("end frame without an active table")?;
                if let Some(expected) = rows
                    && table.rows != expected
                {
                    return Err(format!(
                        "row count mismatch for {}.{}: expected {expected}, imported {}",
                        table.schema, table.table, table.rows
                    )
                    .into());
                }
                eprintln!(
                    "imported {}.{}: {} rows, {} logical bytes",
                    table.schema, table.table, table.rows, table.logical_bytes
                );
            }
            InputFrame::Complete { tables } => {
                if current.is_some() {
                    return Err("complete frame received before the current table ended".into());
                }
                let expected = expected_tables.ok_or("complete frame received before manifest")?;
                let imported = u64::try_from(seen_tables.len()).unwrap_or(u64::MAX);
                if tables != expected || imported != expected {
                    return Err(format!(
                        "table count mismatch: manifest {expected}, complete {tables}, imported {imported}"
                    )
                    .into());
                }
                complete = true;
            }
        }
    }
    flush_all(&engine, &mut buffers, options.durability, &mut batches)?;
    if let Some(table) = current.take() {
        return Err(format!("missing end frame for {}.{}", table.schema, table.table).into());
    }
    if !complete {
        return Err("input ended without a complete frame".into());
    }
    let verified = engine.verify()?;
    drop(engine);
    let reopened = Engine::open(import_engine_config(options, &path))?;
    let reopened_report = reopened.verify()?;
    drop(reopened);
    Ok(ImportSummary {
        tables: seen_tables.len(),
        rows: total_rows,
        logical_bytes: total_bytes,
        batches,
        elapsed_ms: started.elapsed().as_millis(),
        verified_heads: verified.heads_checked,
        verified_events: verified.events_checked,
        reopened_heads: reopened_report.heads_checked,
        reopened_events: reopened_report.events_checked,
    })
}

fn import_engine_config(options: &Options, path: &Path) -> EngineConfig {
    let mut config = EngineConfig::new(path);
    config.max_data_bytes = Some(options.max_data_bytes);
    config.min_free_disk_bytes = options.min_free_disk_bytes;
    config.max_compaction_temporary_bytes = options.max_compaction_temporary_bytes;
    config
}

fn parse_input_frame(line: &str) -> Result<InputFrame, serde_json::Error> {
    let kind: FrameKind = serde_json::from_str(line)?;
    match kind.kind.as_str() {
        "manifest" => {
            let frame: ManifestFrame = serde_json::from_str(line)?;
            Ok(InputFrame::Manifest {
                database: frame.database,
                tables: frame.tables,
            })
        }
        "table" => {
            let frame: TableFrame = serde_json::from_str(line)?;
            Ok(InputFrame::Table {
                schema: frame.schema,
                table: frame.table,
                primary_key: frame.primary_key,
                estimated_rows: frame.estimated_rows,
            })
        }
        "row" => {
            let frame: RowFrame = serde_json::from_str(line)?;
            Ok(InputFrame::Row {
                ctid: frame.ctid,
                tableoid: frame.tableoid,
                row: frame.row,
            })
        }
        "end" => {
            let frame: EndFrame = serde_json::from_str(line)?;
            Ok(InputFrame::End { rows: frame.rows })
        }
        "complete" => {
            let frame: CompleteFrame = serde_json::from_str(line)?;
            Ok(InputFrame::Complete {
                tables: frame.tables,
            })
        }
        _ => Err(<serde_json::Error as serde::de::Error>::custom(format!(
            "unknown frame kind {}",
            kind.kind
        ))),
    }
}

fn validate_source_name(kind: &str, value: &str) -> Result<(), Box<dyn std::error::Error>> {
    if value.is_empty() || value.len() > 255 || value.bytes().any(|byte| byte == 0) {
        return Err(format!("{kind} must contain 1..255 non-NUL bytes").into());
    }
    Ok(())
}

fn row_identity(
    options: &Options,
    table: &TableContext,
    ctid: Option<&str>,
    tableoid: Option<&str>,
    row: &RawValue,
) -> ImportResult<(RecordIdentity, SourcePrimaryKey)> {
    let fields: BTreeMap<String, Box<RawValue>> = serde_json::from_str(row.get())?;
    let mut hasher = blake3::Hasher::new();
    update_hash(&mut hasher, table.schema.as_bytes());
    update_hash(&mut hasher, table.table.as_bytes());
    if table.primary_key.is_empty() {
        let tableoid = tableoid.ok_or_else(|| {
            format!(
                "table {}.{} has no primary key and the row has no tableoid",
                table.schema, table.table
            )
        })?;
        let ctid = ctid.ok_or_else(|| {
            format!(
                "table {}.{} has no primary key and the row has no ctid",
                table.schema, table.table
            )
        })?;
        update_hash(&mut hasher, b"tableoid");
        update_hash(&mut hasher, tableoid.as_bytes());
        update_hash(&mut hasher, b"ctid");
        update_hash(&mut hasher, ctid.as_bytes());
    } else {
        for column in &table.primary_key {
            let value = fields.get(column).ok_or_else(|| {
                format!(
                    "primary-key column {column} is absent from {}.{} row",
                    table.schema, table.table
                )
            })?;
            update_hash(&mut hasher, column.as_bytes());
            update_hash(&mut hasher, value.get().as_bytes());
        }
    }
    let hash = hasher.finalize();
    let bucket = usize::from(hash.as_bytes()[0]) % PARTITIONS;
    let identity = RecordIdentity::new(
        options.tenant.clone(),
        options.namespace.clone(),
        table.collection.clone(),
        format!("p{bucket:02x}").into_bytes(),
        hash.to_hex().as_bytes().to_vec(),
    )?;
    let mut metadata = BTreeMap::from([
        ("source_schema".into(), table.schema.as_bytes().to_vec()),
        ("source_table".into(), table.table.as_bytes().to_vec()),
        (
            "source_primary_key".into(),
            serde_json::to_vec(&table.primary_key)?,
        ),
    ]);
    if let Some(ctid) = ctid {
        metadata.insert("source_ctid".into(), ctid.as_bytes().to_vec());
    }
    if let Some(tableoid) = tableoid {
        metadata.insert("source_tableoid".into(), tableoid.as_bytes().to_vec());
    }
    Ok((identity, metadata))
}

fn update_hash(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&(value.len() as u64).to_be_bytes());
    hasher.update(value);
}

fn partition_bucket(identity: &RecordIdentity) -> usize {
    std::str::from_utf8(&identity.partition[1..])
        .ok()
        .and_then(|value| usize::from_str_radix(value, 16).ok())
        .unwrap_or_default()
        & (PARTITIONS - 1)
}

fn flush_all(
    engine: &Engine,
    buffers: &mut [PartitionBuffer; PARTITIONS],
    durability: Durability,
    batches: &mut u64,
) -> Result<(), Box<dyn std::error::Error>> {
    for buffer in buffers {
        flush_partition(engine, buffer, durability, batches)?;
    }
    Ok(())
}

fn flush_partition(
    engine: &Engine,
    buffer: &mut PartitionBuffer,
    durability: Durability,
    batches: &mut u64,
) -> Result<(), Box<dyn std::error::Error>> {
    if buffer.mutations.is_empty() {
        return Ok(());
    }
    let mutations = std::mem::take(&mut buffer.mutations);
    buffer.estimated_bytes = 0;
    engine.atomic_batch(mutations, durability)?;
    *batches = batches.saturating_add(1);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::io::Cursor;

    use aprodb_engine::{Engine, EngineConfig};
    use aprodb_types::Payload;
    use serde_json::value::RawValue;

    use super::{
        DEFAULT_MAX_COMPACTION_TEMPORARY_BYTES, DEFAULT_MAX_DATA_BYTES,
        DEFAULT_MIN_FREE_DISK_BYTES, MAX_INPUT_FRAME_BYTES, Options, TableContext, import_reader,
        publish_import, row_identity,
    };

    fn test_options(data_dir: std::path::PathBuf) -> Options {
        Options {
            data_dir,
            tenant: b"commit".to_vec(),
            namespace: b"emeroteca".to_vec(),
            durability: aprodb_types::Durability::Durable,
            batch_operations: 2,
            progress_every: 10,
            max_data_bytes: DEFAULT_MAX_DATA_BYTES,
            min_free_disk_bytes: DEFAULT_MIN_FREE_DISK_BYTES,
            max_compaction_temporary_bytes: DEFAULT_MAX_COMPACTION_TEMPORARY_BYTES,
        }
    }

    #[test]
    fn imports_primary_key_and_ctid_rows_without_losing_json_numbers() {
        let directory = tempfile::tempdir().unwrap();
        let data = directory.path().join("data");
        let input = concat!(
            "{\"kind\":\"manifest\",\"database\":\"emeroteca\",\"tables\":2}\n",
            "{\"kind\":\"table\",\"schema\":\"public\",\"table\":\"items\",",
            "\"primary_key\":[\"id\"],\"estimated_rows\":2}\n",
            "{\"kind\":\"row\",\"ctid\":\"(0,1)\",\"row\":",
            "{\"id\":90071992547409931234567890,\"name\":\"alpha\"}}\n",
            "{\"kind\":\"row\",\"ctid\":\"(0,2)\",\"row\":",
            "{\"id\":2,\"name\":\"beta\"}}\n",
            "{\"kind\":\"end\",\"rows\":2}\n",
            "{\"kind\":\"table\",\"schema\":\"public\",\"table\":\"logs\",",
            "\"primary_key\":[],\"estimated_rows\":1}\n",
            "{\"kind\":\"row\",\"ctid\":\"(4,7)\",\"tableoid\":\"public.logs\",\"row\":",
            "{\"message\":\"hello\"}}\n",
            "{\"kind\":\"end\",\"rows\":1}\n",
            "{\"kind\":\"complete\",\"tables\":2}\n",
        );
        let options = test_options(data.clone());
        let summary = import_reader(Cursor::new(input), &options).unwrap();
        assert_eq!(summary.tables, 2);
        assert_eq!(summary.rows, 3);
        assert_eq!(summary.verified_heads, 3);
        assert_eq!(summary.reopened_heads, 3);

        let engine = Engine::open(EngineConfig::new(&data)).unwrap();
        let table = TableContext {
            schema: "public".into(),
            table: "items".into(),
            collection: b"public.items".to_vec(),
            primary_key: vec!["id".into()],
            rows: 0,
            logical_bytes: 0,
        };
        let row =
            RawValue::from_string("{\"id\":90071992547409931234567890,\"name\":\"alpha\"}".into())
                .unwrap();
        let (identity, _) = row_identity(&options, &table, Some("(0,1)"), None, &row).unwrap();
        let stored = engine.get(&identity).unwrap().unwrap();
        assert!(matches!(
            stored.payload,
            Some(Payload::Document { bytes, .. })
                if bytes == br#"{"id":90071992547409931234567890,"name":"alpha"}"#
        ));
    }

    #[test]
    fn rejects_a_truncated_export() {
        let directory = tempfile::tempdir().unwrap();
        let input = concat!(
            "{\"kind\":\"manifest\",\"database\":\"emeroteca\",\"tables\":1}\n",
            "{\"kind\":\"table\",\"schema\":\"public\",\"table\":\"items\",",
            "\"primary_key\":[\"id\"],\"estimated_rows\":1}\n",
            "{\"kind\":\"row\",\"ctid\":\"(0,1)\",\"row\":{\"id\":1}}\n",
            "{\"kind\":\"end\",\"rows\":1}\n",
        );

        let error = import_reader(
            Cursor::new(input),
            &test_options(directory.path().join("truncated")),
        )
        .unwrap_err();
        assert!(error.to_string().contains("without a complete frame"));
    }

    #[test]
    fn rejects_an_inconsistent_table_count() {
        let directory = tempfile::tempdir().unwrap();
        let input = concat!(
            "{\"kind\":\"manifest\",\"database\":\"emeroteca\",\"tables\":2}\n",
            "{\"kind\":\"table\",\"schema\":\"public\",\"table\":\"items\",",
            "\"primary_key\":[\"id\"],\"estimated_rows\":0}\n",
            "{\"kind\":\"end\",\"rows\":0}\n",
            "{\"kind\":\"complete\",\"tables\":1}\n",
        );

        let error = import_reader(
            Cursor::new(input),
            &test_options(directory.path().join("mismatch")),
        )
        .unwrap_err();
        assert!(error.to_string().contains("table count mismatch"));
    }

    #[test]
    fn publishes_only_a_complete_import() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("published");
        let input = concat!(
            "{\"kind\":\"manifest\",\"database\":\"emeroteca\",\"tables\":0}\n",
            "{\"kind\":\"complete\",\"tables\":0}\n",
        );

        let summary = publish_import(Cursor::new(input), &test_options(target.clone())).unwrap();
        assert_eq!(summary.tables, 0);
        assert!(target.is_dir());
    }

    #[test]
    fn keeps_a_truncated_import_out_of_the_publish_target() {
        let directory = tempfile::tempdir().unwrap();
        let target = directory.path().join("published");
        let input = "{\"kind\":\"manifest\",\"database\":\"emeroteca\",\"tables\":0}\n";

        let error = publish_import(Cursor::new(input), &test_options(target.clone())).unwrap_err();
        assert!(error.to_string().contains("staging directory preserved"));
        assert!(!target.exists());
        assert_eq!(
            std::fs::read_dir(directory.path())
                .unwrap()
                .filter_map(Result::ok)
                .filter(|entry| entry.file_name().to_string_lossy().contains(".importing-"))
                .count(),
            1
        );
    }

    #[test]
    fn rejects_an_oversized_input_frame_before_parsing() {
        let directory = tempfile::tempdir().unwrap();
        let input = format!("{}\n", "x".repeat(MAX_INPUT_FRAME_BYTES));

        let error = import_reader(
            Cursor::new(input),
            &test_options(directory.path().join("oversized")),
        )
        .unwrap_err();
        assert!(error.to_string().contains("exceeds the"));
    }

    #[test]
    fn tableoid_disambiguates_ctids_without_a_primary_key() {
        let directory = tempfile::tempdir().unwrap();
        let options = test_options(directory.path().join("identities"));
        let table = TableContext {
            schema: "public".into(),
            table: "partitioned_logs".into(),
            collection: b"public.partitioned_logs".to_vec(),
            primary_key: Vec::new(),
            rows: 0,
            logical_bytes: 0,
        };
        let row = RawValue::from_string("{\"message\":\"same\"}".into()).unwrap();

        let (first, _) =
            row_identity(&options, &table, Some("(0,1)"), Some("logs_2025"), &row).unwrap();
        let (second, _) =
            row_identity(&options, &table, Some("(0,1)"), Some("logs_2026"), &row).unwrap();
        assert_ne!(first, second);
        assert!(row_identity(&options, &table, Some("(0,1)"), None, &row).is_err());
    }
}
