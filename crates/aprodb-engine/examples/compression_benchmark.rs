use std::time::Instant;

use aprodb_engine::{AtomicMutation, Engine, EngineConfig, PutRequest};
use aprodb_storage::BackendCompression;
use aprodb_types::{CompressionPolicy, CompressionTierPolicy, Durability, Payload, RecordIdentity};
use serde::Serialize;

const RECORDS: usize = 256;
const BATCH_SIZE: usize = 16;
const PAYLOAD_BYTES: usize = 4096;

#[derive(Clone, Copy)]
struct Variant {
    name: &'static str,
    logical_zstd: bool,
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
    payload_bytes: usize,
    logical_payload_bytes: u64,
    stored_payload_bytes: u64,
    logical_payload_ratio: f64,
    disk_bytes_before_compaction: u64,
    disk_bytes_after_compaction: u64,
    durable_batch_p50_us: u128,
    durable_batch_p95_us: u128,
    durable_batch_p99_us: u128,
    throughput_records_per_second: f64,
    process_cpu_ms: u64,
    process_io_read_bytes: u64,
    process_io_written_bytes: u64,
    process_resident_end_bytes: u64,
    compression_micros: u64,
    zstd_records: u64,
    raw_records: u64,
    adaptive_fallbacks: u64,
    recovery_ms: u128,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let variants = [
        Variant {
            name: "aprodb_zstd_only",
            logical_zstd: true,
            backend_lz4: false,
        },
        Variant {
            name: "backend_lz4_only",
            logical_zstd: false,
            backend_lz4: true,
        },
        Variant {
            name: "both",
            logical_zstd: true,
            backend_lz4: true,
        },
        Variant {
            name: "none",
            logical_zstd: false,
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
    laboratory: &std::path::Path,
    profile: Profile,
    variant: Variant,
) -> Result<Measurement, Box<dyn std::error::Error>> {
    let path = laboratory.join(format!("{}-{}", profile.name(), variant.name));
    let physical = if variant.backend_lz4 {
        BackendCompression::Lz4
    } else {
        BackendCompression::None
    };
    let mut config = EngineConfig::new(&path);
    config.storage.payload_compression = physical;
    config.storage.metadata_compression = physical;
    config.storage.surface_compression = physical;
    config.storage.journal_compression = physical;
    let engine = Engine::open(config.clone())?;
    let collection = identity(0);
    if !variant.logical_zstd {
        let raw = CompressionTierPolicy::raw();
        engine.configure_compression_policy(
            &collection,
            CompressionPolicy {
                surface: raw.clone(),
                hot: raw.clone(),
                warm: raw.clone(),
                cold: raw.clone(),
                archive: raw,
                skip_content_type_prefixes: Vec::new(),
            },
        )?;
    }

    let mut system = sysinfo::System::new();
    let process_before = process_sample(&mut system)?;
    let mut latencies = Vec::new();
    let started = Instant::now();
    for batch_start in (0..RECORDS).step_by(BATCH_SIZE) {
        let mutations = (batch_start..(batch_start + BATCH_SIZE).min(RECORDS))
            .map(|record| {
                AtomicMutation::Put(PutRequest::new(
                    identity(record),
                    Payload::Bytes(payload(profile, record)),
                ))
            })
            .collect();
        let commit_started = Instant::now();
        engine.atomic_batch(mutations, Durability::Durable)?;
        latencies.push(commit_started.elapsed().as_micros());
    }
    let elapsed = started.elapsed();
    engine.sync()?;
    let disk_bytes_before_compaction = engine.stats()?.disk_bytes;
    let compaction = engine.major_compact()?;
    let compression = engine.compression_stats();
    let process_after = process_sample(&mut system)?;
    engine.verify()?;
    drop(engine);

    let recovery_started = Instant::now();
    let reopened = Engine::open(config)?;
    let recovery_ms = recovery_started.elapsed().as_millis();
    reopened
        .get(&identity(RECORDS - 1))?
        .ok_or_else(|| std::io::Error::other("record missing after reopen"))?;
    reopened.verify()?;

    latencies.sort_unstable();
    Ok(Measurement {
        variant: variant.name,
        profile: profile.name(),
        records: RECORDS,
        payload_bytes: PAYLOAD_BYTES,
        logical_payload_bytes: compression.logical_bytes,
        stored_payload_bytes: compression.stored_payload_bytes,
        logical_payload_ratio: ratio(compression.stored_payload_bytes, compression.logical_bytes),
        disk_bytes_before_compaction,
        disk_bytes_after_compaction: compaction.disk_bytes_after,
        durable_batch_p50_us: percentile(&latencies, 50),
        durable_batch_p95_us: percentile(&latencies, 95),
        durable_batch_p99_us: percentile(&latencies, 99),
        throughput_records_per_second: RECORDS as f64 / elapsed.as_secs_f64(),
        process_cpu_ms: process_after.cpu_ms.saturating_sub(process_before.cpu_ms),
        process_io_read_bytes: process_after
            .read_bytes
            .saturating_sub(process_before.read_bytes),
        process_io_written_bytes: process_after
            .written_bytes
            .saturating_sub(process_before.written_bytes),
        process_resident_end_bytes: process_after.resident_bytes,
        compression_micros: compression.compression_micros,
        zstd_records: compression.zstandard_payloads,
        raw_records: compression.raw_payloads,
        adaptive_fallbacks: compression.adaptive_fallbacks,
        recovery_ms,
    })
}

struct ProcessSample {
    cpu_ms: u64,
    read_bytes: u64,
    written_bytes: u64,
    resident_bytes: u64,
}

fn process_sample(
    system: &mut sysinfo::System,
) -> Result<ProcessSample, Box<dyn std::error::Error>> {
    let pid = sysinfo::get_current_pid()?;
    system.refresh_processes_specifics(
        sysinfo::ProcessesToUpdate::Some(&[pid]),
        true,
        sysinfo::ProcessRefreshKind::nothing()
            .with_cpu()
            .with_memory()
            .with_disk_usage(),
    );
    let process = system
        .process(pid)
        .ok_or_else(|| std::io::Error::other("process counters not available"))?;
    let disk = process.disk_usage();
    Ok(ProcessSample {
        cpu_ms: process.accumulated_cpu_time(),
        read_bytes: disk.total_read_bytes,
        written_bytes: disk.total_written_bytes,
        resident_bytes: process.memory(),
    })
}

fn identity(record: usize) -> RecordIdentity {
    RecordIdentity::new(
        "benchmark",
        "compression",
        "payloads",
        "partition",
        format!("record-{record:08}"),
    )
    .expect("constant benchmark identity")
}

fn payload(profile: Profile, record: usize) -> Vec<u8> {
    match profile {
        Profile::Compressible => {
            let mut bytes = vec![b'a' + (record % 7) as u8; PAYLOAD_BYTES];
            bytes[..8].copy_from_slice(&(record as u64).to_le_bytes());
            bytes
        }
        Profile::Random => {
            let mut state = (record as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            (0..PAYLOAD_BYTES)
                .map(|_| {
                    state ^= state << 7;
                    state ^= state >> 9;
                    state ^= state << 8;
                    state as u8
                })
                .collect()
        }
    }
}

fn percentile(sorted: &[u128], percentile: usize) -> u128 {
    let index = sorted.len().saturating_sub(1).saturating_mul(percentile) / 100;
    sorted[index]
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
