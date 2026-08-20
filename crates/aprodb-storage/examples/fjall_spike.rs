use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    time::Instant,
};

use aprodb_storage::{
    BackendCompression, CommitMode, FjallBackend, FjallOptions, StorageBackend, StorageBatch,
    StorageSpace,
};
use serde::Serialize;

const RECORDS: usize = 2_000;
const UPDATE_ROUNDS: usize = 2;
const PAYLOAD_BYTES: usize = 1024;
const BATCH_SIZE: usize = 100;

#[derive(Clone, Copy)]
struct Variant {
    name: &'static str,
    aprodb_zstd: bool,
    backend_lz4: bool,
}

#[derive(Clone, Copy)]
enum Profile {
    Compressible,
    Random,
}

impl Profile {
    const fn name(self) -> &'static str {
        match self {
            Self::Compressible => "compressible",
            Self::Random => "random",
        }
    }
}

#[derive(Serialize)]
struct Measurement {
    variant: &'static str,
    profile: &'static str,
    records: usize,
    mutations: usize,
    logical_payload_bytes: u64,
    encoded_payload_bytes: u64,
    minimal_event_bytes: u64,
    delta_event_bytes: u64,
    self_contained_event_bytes: u64,
    submitted_storage_bytes: u64,
    process_io_written_bytes: u64,
    process_io_write_amplification: f64,
    observed_file_growth_bytes: u64,
    observed_write_amplification_proxy: f64,
    payload_ratio: f64,
    change_log_to_payload_ratio: f64,
    durable_p50_us: u128,
    durable_p95_us: u128,
    durable_p99_us: u128,
    throughput_mutations_per_second: f64,
    space_after_reopen_bytes: u64,
    recovery_ms: u128,
    journal_fragments_after_reopen: usize,
    write_buffer_bytes_after_reopen: u64,
    table_count_after_reopen: usize,
    compaction_disk_bytes_before: u64,
    compaction_disk_bytes_after: u64,
    compaction_tables_before: usize,
    compaction_tables_after: usize,
    physical_compaction_metrics_available: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let variants = [
        Variant {
            name: "aprodb_zstd_only",
            aprodb_zstd: true,
            backend_lz4: false,
        },
        Variant {
            name: "backend_lz4_only",
            aprodb_zstd: false,
            backend_lz4: true,
        },
        Variant {
            name: "both",
            aprodb_zstd: true,
            backend_lz4: true,
        },
        Variant {
            name: "none",
            aprodb_zstd: false,
            backend_lz4: false,
        },
    ];
    let laboratory = tempfile::tempdir()?;
    let mut measurements = Vec::new();
    for profile in [Profile::Compressible, Profile::Random] {
        for variant in variants {
            measurements.push(run_variant(laboratory.path(), profile, variant)?);
        }
    }
    println!("{}", serde_json::to_string_pretty(&measurements)?);
    Ok(())
}

fn run_variant(
    laboratory: &Path,
    profile: Profile,
    variant: Variant,
) -> Result<Measurement, Box<dyn std::error::Error>> {
    let path = laboratory.join(format!("{}-{}", profile.name(), variant.name));
    let backend_compression = if variant.backend_lz4 {
        BackendCompression::Lz4
    } else {
        BackendCompression::None
    };
    let options = FjallOptions {
        cache_bytes: 8 * 1024 * 1024,
        max_journal_bytes: 64 * 1024 * 1024,
        worker_threads: 2,
        max_memtable_bytes: 1024 * 1024,
        payload_compression: backend_compression,
        metadata_compression: backend_compression,
        journal_compression: backend_compression,
        ..FjallOptions::default()
    };
    let process_io_before = process_total_written_bytes()?;
    let backend = FjallBackend::open(&path, options.clone())?;
    let mut previous_files = file_sizes(&path)?;
    let mut observed_file_growth_bytes = 0u64;
    let mut logical_payload_bytes = 0u64;
    let mut encoded_payload_bytes = 0u64;
    let mut minimal_event_bytes = 0u64;
    let mut submitted_storage_bytes = 0u64;
    let mut durable_latencies = Vec::new();
    let started = Instant::now();

    for round in 0..UPDATE_ROUNDS {
        for batch_start in (0..RECORDS).step_by(BATCH_SIZE) {
            let batch_end = (batch_start + BATCH_SIZE).min(RECORDS);
            let mut batch = StorageBatch::with_capacity((batch_end - batch_start) * 3 + 1);
            for record in batch_start..batch_end {
                let raw = payload(profile, record, round);
                let encoded = encode_payload(&raw, variant.aprodb_zstd)?;
                let head_key = head_key(record);
                let version_key = version_key(record, round);
                let event_key = event_key(round * RECORDS + record + 1);
                let event = minimal_event(record, round);
                logical_payload_bytes += raw.len() as u64;
                encoded_payload_bytes += encoded.len() as u64;
                minimal_event_bytes += event.len() as u64;
                batch.put(StorageSpace::Versions, version_key, encoded);
                batch.put(
                    StorageSpace::Records,
                    head_key,
                    (round as u64).to_be_bytes().to_vec(),
                );
                batch.put(StorageSpace::Events, event_key, event);
            }
            batch.put(
                StorageSpace::Catalog,
                b"spike-sequence".to_vec(),
                ((round * RECORDS + batch_end) as u64)
                    .to_be_bytes()
                    .to_vec(),
            );
            submitted_storage_bytes += batch.bytes() as u64;
            let commit_started = Instant::now();
            backend.commit(batch, CommitMode::Durable)?;
            durable_latencies.push(commit_started.elapsed().as_micros());
            let current_files = file_sizes(&path)?;
            observed_file_growth_bytes = observed_file_growth_bytes
                .saturating_add(positive_file_growth(&previous_files, &current_files));
            previous_files = current_files;
        }
    }
    let elapsed = started.elapsed();
    let compaction = backend.major_compact()?;
    backend.persist(CommitMode::Durable)?;
    drop(backend);
    let process_io_written_bytes = process_total_written_bytes()?.saturating_sub(process_io_before);

    let recovery_started = Instant::now();
    let reopened = FjallBackend::open(&path, options)?;
    let recovery_ms = recovery_started.elapsed().as_millis();
    for record in (0..RECORDS).step_by((RECORDS / 16).max(1)) {
        let head = reopened
            .get(StorageSpace::Records, &head_key(record))?
            .ok_or("missing head")?;
        if head != ((UPDATE_ROUNDS - 1) as u64).to_be_bytes() {
            return Err("head with incorrect version".into());
        }
        let value = reopened
            .get(
                StorageSpace::Versions,
                &version_key(record, UPDATE_ROUNDS - 1),
            )?
            .ok_or("missing version")?;
        let decoded = decode_payload(&value)?;
        if decoded != payload(profile, record, UPDATE_ROUNDS - 1) {
            return Err("retrieved payload differs".into());
        }
    }
    let stats = reopened.stats()?;
    durable_latencies.sort_unstable();
    let mutations = RECORDS * UPDATE_ROUNDS;
    let delta_event_bytes = mutations as u64 * 16;
    Ok(Measurement {
        variant: variant.name,
        profile: profile.name(),
        records: RECORDS,
        mutations,
        logical_payload_bytes,
        encoded_payload_bytes,
        minimal_event_bytes,
        delta_event_bytes,
        self_contained_event_bytes: minimal_event_bytes.saturating_add(encoded_payload_bytes),
        submitted_storage_bytes,
        process_io_written_bytes,
        process_io_write_amplification: ratio(process_io_written_bytes, submitted_storage_bytes),
        observed_file_growth_bytes,
        observed_write_amplification_proxy: ratio(
            observed_file_growth_bytes,
            submitted_storage_bytes,
        ),
        payload_ratio: ratio(encoded_payload_bytes, logical_payload_bytes),
        change_log_to_payload_ratio: ratio(minimal_event_bytes, logical_payload_bytes),
        durable_p50_us: percentile(&durable_latencies, 50),
        durable_p95_us: percentile(&durable_latencies, 95),
        durable_p99_us: percentile(&durable_latencies, 99),
        throughput_mutations_per_second: mutations as f64 / elapsed.as_secs_f64(),
        space_after_reopen_bytes: stats.disk_bytes,
        recovery_ms,
        journal_fragments_after_reopen: stats.journal_fragments,
        write_buffer_bytes_after_reopen: stats.write_buffer_bytes,
        table_count_after_reopen: stats.table_count,
        compaction_disk_bytes_before: compaction.disk_bytes_before,
        compaction_disk_bytes_after: compaction.disk_bytes_after,
        compaction_tables_before: compaction.table_count_before,
        compaction_tables_after: compaction.table_count_after,
        physical_compaction_metrics_available: true,
    })
}

fn process_total_written_bytes() -> Result<u64, Box<dyn std::error::Error>> {
    let pid = sysinfo::get_current_pid()?;
    let mut system = sysinfo::System::new();
    system.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::Some(&[pid]),
        true,
        sysinfo::ProcessRefreshKind::nothing().with_disk_usage(),
    );
    system
        .process(pid)
        .map(|process| process.disk_usage().total_written_bytes)
        .ok_or_else(|| "process I/O counters are unavailable".into())
}

fn payload(profile: Profile, record: usize, round: usize) -> Vec<u8> {
    match profile {
        Profile::Compressible => {
            let mut bytes = vec![b'a' + (record % 7) as u8; PAYLOAD_BYTES];
            bytes[..8].copy_from_slice(&(record as u64).to_be_bytes());
            bytes[8..16].copy_from_slice(&(round as u64).to_be_bytes());
            bytes
        }
        Profile::Random => {
            let mut state = (record as u64)
                .wrapping_mul(0x9E37_79B9_7F4A_7C15)
                .wrapping_add(round as u64 + 1);
            (0..PAYLOAD_BYTES)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state as u8
                })
                .collect()
        }
    }
}

fn encode_payload(raw: &[u8], compress: bool) -> std::io::Result<Vec<u8>> {
    if compress {
        let candidate = zstd::bulk::compress(raw, 1)?;
        if candidate.len() + 9 < raw.len() {
            let mut encoded = Vec::with_capacity(candidate.len() + 5);
            encoded.push(1);
            encoded.extend_from_slice(&(raw.len() as u32).to_le_bytes());
            encoded.extend_from_slice(&candidate);
            return Ok(encoded);
        }
    }
    let mut encoded = Vec::with_capacity(raw.len() + 5);
    encoded.push(0);
    encoded.extend_from_slice(&(raw.len() as u32).to_le_bytes());
    encoded.extend_from_slice(raw);
    Ok(encoded)
}

fn decode_payload(encoded: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    if encoded.len() < 5 {
        return Err("incomplete spike payload".into());
    }
    let logical_len = u32::from_le_bytes(encoded[1..5].try_into()?) as usize;
    let bytes = match encoded[0] {
        0 => encoded[5..].to_vec(),
        1 => zstd::bulk::decompress(&encoded[5..], logical_len)?,
        _ => return Err("unknown spike codec".into()),
    };
    if bytes.len() != logical_len {
        return Err("inconsistent spike length".into());
    }
    Ok(bytes)
}

fn head_key(record: usize) -> Vec<u8> {
    format!("record/{record:08}").into_bytes()
}

fn version_key(record: usize, round: usize) -> Vec<u8> {
    format!("record/{record:08}/version/{round:04}").into_bytes()
}

fn event_key(sequence: usize) -> Vec<u8> {
    (sequence as u64).to_be_bytes().to_vec()
}

fn minimal_event(record: usize, round: usize) -> Vec<u8> {
    let mut event = head_key(record);
    event.extend_from_slice(&(round as u64).to_be_bytes());
    event
}

fn percentile(values: &[u128], percentile: usize) -> u128 {
    if values.is_empty() {
        return 0;
    }
    let index = (values.len() - 1) * percentile / 100;
    values[index]
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn file_sizes(root: &Path) -> Result<HashMap<PathBuf, u64>, std::io::Error> {
    let mut files = HashMap::new();
    collect_file_sizes(root, root, &mut files)?;
    Ok(files)
}

fn collect_file_sizes(
    root: &Path,
    current: &Path,
    files: &mut HashMap<PathBuf, u64>,
) -> Result<(), std::io::Error> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            collect_file_sizes(root, &entry.path(), files)?;
        } else {
            let relative = entry.path().strip_prefix(root).unwrap().to_path_buf();
            files.insert(relative, metadata.len());
        }
    }
    Ok(())
}

fn positive_file_growth(previous: &HashMap<PathBuf, u64>, current: &HashMap<PathBuf, u64>) -> u64 {
    current
        .iter()
        .map(|(path, size)| size.saturating_sub(previous.get(path).copied().unwrap_or(0)))
        .sum()
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
