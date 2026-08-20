use std::{env, net::SocketAddr, path::PathBuf, process::ExitCode};

use aprodb_client::{BlockingClient, ClientConfig, tls_client_config};
use aprodb_types::{
    CompressionMode, CompressionPolicy, CompressionTierPolicy, Durability, RecordIdentity,
    SurfaceDefinition, SurfaceFormat, SurfaceKind,
};

const USAGE: &str = "usage: aprodb-cli <health|stats|cache-stats|compression-stats|compute-stats|audit AFTER|- LIMIT|backup NAME|compression-policy TENANT NAMESPACE COLLECTION|set-compression TENANT NAMESPACE COLLECTION raw|zstd|verify|compact|expire|shutdown|explain TENANT NAMESPACE COLLECTION PARTITION KEY|create-surface ID work|read TENANT NAMESPACE COLLECTION STATES records|json MAX_RECORDS MAX_BYTES RETAINED|build-surface ID MAX_EVENTS|rebuild-surface ID> [--address HOST:PORT] [--tls-ca PEM --tls-server-name NAME [--tls-cert PEM --tls-key PEM]]\nrequires APRODB_ADMIN_TOKEN";

#[derive(Clone, Debug, Eq, PartialEq)]
enum Command {
    Health,
    Stats,
    Verify,
    Compact,
    Shutdown,
    CacheStats,
    CompressionStats,
    ComputeStats,
    Audit { after: Option<u64>, limit: usize },
    Backup(String),
    CompressionPolicy(RecordIdentity),
    SetCompression(RecordIdentity, CompressionPolicy),
    Expire,
    Explain(RecordIdentity),
    CreateSurface(SurfaceDefinition),
    BuildSurface { id: String, max_events: usize },
    RebuildSurface(String),
}

#[derive(Debug)]
struct CliOptions {
    command: Command,
    address: SocketAddr,
    tls_ca: Option<PathBuf>,
    tls_server_name: String,
    tls_certificate: Option<PathBuf>,
    tls_private_key: Option<PathBuf>,
}

fn parse(arguments: impl IntoIterator<Item = String>) -> Result<CliOptions, String> {
    let mut arguments = arguments.into_iter();
    let command = match arguments.next().as_deref() {
        Some("health") => Command::Health,
        Some("stats") => Command::Stats,
        Some("verify") => Command::Verify,
        Some("compact") => Command::Compact,
        Some("shutdown") => Command::Shutdown,
        Some("cache-stats") => Command::CacheStats,
        Some("compression-stats") => Command::CompressionStats,
        Some("compute-stats") => Command::ComputeStats,
        Some("audit") => {
            let first = required(&mut arguments, "AFTER")?;
            let after = if first == "-" {
                None
            } else if let Ok(limit) = first.parse::<usize>() {
                Some(u64::try_from(limit).map_err(|_| "AFTER exceeds u64".to_string())?)
            } else {
                return Err("AFTER must be '-' or a number".into());
            };
            Command::Audit {
                after,
                limit: required_usize(&mut arguments, "LIMIT")?,
            }
        }
        Some("backup") => Command::Backup(required(&mut arguments, "NAME")?),
        Some("compression-policy") => {
            Command::CompressionPolicy(required_collection(&mut arguments)?)
        }
        Some("set-compression") => {
            let collection = required_collection(&mut arguments)?;
            let mode = required(&mut arguments, "MODE")?;
            let mut policy = CompressionPolicy::default();
            if mode == "raw" {
                policy.hot = CompressionTierPolicy::raw();
                policy.warm = CompressionTierPolicy::raw();
                policy.cold = CompressionTierPolicy::raw();
                policy.archive = CompressionTierPolicy::raw();
            } else if mode != "zstd" {
                return Err("MODE must be 'raw' or 'zstd'".into());
            }
            Command::SetCompression(collection, policy)
        }
        Some("expire") => Command::Expire,
        Some("explain") => Command::Explain(
            RecordIdentity::new(
                required(&mut arguments, "TENANT")?,
                required(&mut arguments, "NAMESPACE")?,
                required(&mut arguments, "COLLECTION")?,
                required(&mut arguments, "PARTITION")?,
                required(&mut arguments, "KEY")?,
            )
            .map_err(|error| error.to_string())?,
        ),
        Some("create-surface") => {
            let id = required(&mut arguments, "ID")?;
            let kind = match required(&mut arguments, "KIND")?.as_str() {
                "work" => SurfaceKind::Work,
                "read" => SurfaceKind::Read,
                _ => return Err("KIND must be 'work' or 'read'".into()),
            };
            let source_tenant = required(&mut arguments, "TENANT")?.into_bytes();
            let source_namespace = required(&mut arguments, "NAMESPACE")?.into_bytes();
            let source_collection = required(&mut arguments, "COLLECTION")?.into_bytes();
            let workflow_states = required(&mut arguments, "STATES")?
                .split(',')
                .map(str::to_owned)
                .collect::<Vec<_>>();
            let format = match required(&mut arguments, "FORMAT")?.as_str() {
                "records" => SurfaceFormat::AprodbRecords,
                "json" => SurfaceFormat::Json,
                _ => return Err("FORMAT must be 'records' or 'json'".into()),
            };
            Command::CreateSurface(SurfaceDefinition {
                id,
                kind,
                source_tenant,
                source_namespace,
                source_collection,
                workflow_states,
                format,
                max_records: required_usize(&mut arguments, "MAX_RECORDS")?,
                max_bytes: required_usize(&mut arguments, "MAX_BYTES")?,
                retained_generations: required_usize(&mut arguments, "RETAINED")?,
            })
        }
        Some("build-surface") => Command::BuildSurface {
            id: required(&mut arguments, "ID")?,
            max_events: required_usize(&mut arguments, "MAX_EVENTS")?,
        },
        Some("rebuild-surface") => Command::RebuildSurface(required(&mut arguments, "ID")?),
        Some("--help" | "-h") | None => return Err(USAGE.into()),
        Some(command) => return Err(format!("unknown command: {command}")),
    };
    let mut address = "127.0.0.1:7644"
        .parse()
        .expect("indirizzo amministrativo costante");
    let mut tls_ca = None;
    let mut tls_server_name = "localhost".to_string();
    let mut tls_certificate = None;
    let mut tls_private_key = None;
    while let Some(argument) = arguments.next() {
        match argument.as_str() {
            "--address" => {
                address = arguments
                    .next()
                    .ok_or("missing value for --address")?
                    .parse()
                    .map_err(|_| "invalid --address value".to_string())?;
            }
            "--tls-ca" => {
                tls_ca = Some(PathBuf::from(required(&mut arguments, "TLS_CA")?));
            }
            "--tls-server-name" => tls_server_name = required(&mut arguments, "TLS_NAME")?,
            "--tls-cert" => {
                tls_certificate = Some(PathBuf::from(required(&mut arguments, "TLS_CERT")?));
            }
            "--tls-key" => {
                tls_private_key = Some(PathBuf::from(required(&mut arguments, "TLS_KEY")?));
            }
            _ => return Err(format!("unknown argument: {argument}")),
        }
    }
    if tls_certificate.is_some() != tls_private_key.is_some() {
        return Err("--tls-cert and --tls-key must be provided together".into());
    }
    if tls_certificate.is_some() && tls_ca.is_none() {
        return Err("mTLS identity requires --tls-ca".into());
    }
    Ok(CliOptions {
        command,
        address,
        tls_ca,
        tls_server_name,
        tls_certificate,
        tls_private_key,
    })
}

fn required(arguments: &mut impl Iterator<Item = String>, name: &str) -> Result<String, String> {
    arguments
        .next()
        .ok_or_else(|| format!("missing argument: {name}"))
}

fn required_usize(
    arguments: &mut impl Iterator<Item = String>,
    name: &str,
) -> Result<usize, String> {
    required(arguments, name)?
        .parse()
        .map_err(|_| format!("invalid argument: {name}"))
}

fn required_collection(
    arguments: &mut impl Iterator<Item = String>,
) -> Result<RecordIdentity, String> {
    RecordIdentity::new(
        required(arguments, "TENANT")?,
        required(arguments, "NAMESPACE")?,
        required(arguments, "COLLECTION")?,
        "_",
        "_",
    )
    .map_err(|error| error.to_string())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("aprodb-cli: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), String> {
    let options = parse(env::args().skip(1))?;
    let token = env::var_os("APRODB_ADMIN_TOKEN")
        .ok_or("Missing APRODB_ADMIN_TOKEN environment variable")?
        .to_string_lossy()
        .into_owned();
    let mut config = ClientConfig::admin(token);
    if let Some(ca) = options.tls_ca.as_ref() {
        let ca = std::fs::read(ca)
            .map_err(|error| format!("Error reading TLS CA certificate: {error}"))?;
        let certificate = options
            .tls_certificate
            .as_ref()
            .map(std::fs::read)
            .transpose()
            .map_err(|error| format!("Error reading TLS client certificate: {error}"))?;
        let private_key = options
            .tls_private_key
            .as_ref()
            .map(std::fs::read)
            .transpose()
            .map_err(|error| format!("Error reading TLS client private key: {error}"))?;
        config.tls = Some(
            tls_client_config(
                &ca,
                options.tls_server_name,
                certificate.as_deref().zip(private_key.as_deref()),
            )
            .map_err(|error| error.to_string())?,
        );
    }
    let client =
        BlockingClient::connect_tcp(options.address, config).map_err(|error| error.to_string())?;
    match options.command {
        Command::Health => println!("health_status={}", client.health().map_err(client_error)?),
        Command::Stats => {
            let stats = client.stats().map_err(client_error)?;
            println!(
                "disk_bytes={} write_buffer_bytes={} active_connections={} inflight_requests={} total_requests={} rejected_requests={} auth_failures={}",
                stats.disk_bytes,
                stats.write_buffer_bytes,
                stats.active_connections,
                stats.inflight_requests,
                stats.total_requests,
                stats.rejected_requests,
                stats.auth_failures
            );
        }
        Command::Verify => {
            client.verify().map_err(client_error)?;
            println!("verification=successful");
        }
        Command::Compact => {
            client.compact().map_err(client_error)?;
            println!("compaction=successful");
        }
        Command::Shutdown => {
            client.shutdown().map_err(client_error)?;
            println!("shutdown_request=accepted");
        }
        Command::CacheStats => {
            let stats = client.cache_stats().map_err(client_error)?;
            println!(
                "metadata_bytes={}/{} object_bytes={}/{} compressed_bytes={}/{} negative_bytes={}/{} object_hits={} object_misses={} object_evictions={} object_rejections={}",
                stats.metadata.resident_bytes,
                stats.metadata.budget_bytes,
                stats.objects.resident_bytes,
                stats.objects.budget_bytes,
                stats.compressed.resident_bytes,
                stats.compressed.budget_bytes,
                stats.negative.resident_bytes,
                stats.negative.budget_bytes,
                stats.objects.hits,
                stats.objects.misses,
                stats.objects.evictions,
                stats.objects.rejections
            );
        }
        Command::CompressionStats => {
            let stats = client.compression_stats().map_err(client_error)?;
            println!(
                "logical_bytes={} stored_bytes={} raw_records={} zstd_records={} dictionary_records={} fallbacks={} skipped={} compress_us={} decompress_us={} failures={} channels={} scratch_bytes={}/{}",
                stats.logical_bytes,
                stats.stored_bytes,
                stats.raw_records,
                stats.zstd_records,
                stats.dictionary_records,
                stats.incompressible_fallbacks,
                stats.content_type_skips,
                stats.compress_micros,
                stats.decompress_micros,
                stats.codec_failures,
                stats.channels,
                stats.scratch_in_use_bytes,
                stats.scratch_budget_bytes
            );
        }
        Command::ComputeStats => {
            let stats = client.compute_stats().map_err(client_error)?;
            println!(
                "backend={} requests={} cpu={} accelerator={} fallbacks={} timeouts={} queue_rejections={} accelerator_failures={} circuit_rejections={} batches={} batched_requests={} inflight_bytes={} peak_inflight_bytes={} vram_bytes={}/{} vram_entries={} vram_hits={} vram_misses={} vram_evictions={} upload_bytes={} readback_bytes={} transfer_us={} kernel_us={} device_resets={}",
                stats.accelerator_name.as_deref().unwrap_or("cpu-only"),
                stats.requests,
                stats.cpu_runs,
                stats.accelerator_runs,
                stats.cpu_fallbacks,
                stats.request_timeouts,
                stats.queue_rejections,
                stats.accelerator_failures,
                stats.circuit_open_rejections,
                stats.micro_batches,
                stats.micro_batched_requests,
                stats.inflight_bytes,
                stats.peak_inflight_bytes,
                stats.vram_resident_bytes,
                stats.vram_budget_bytes,
                stats.vram_entries,
                stats.vram_hits,
                stats.vram_misses,
                stats.vram_evictions,
                stats.upload_bytes,
                stats.readback_bytes,
                stats.transfer_micros,
                stats.kernel_micros,
                stats.device_resets
            );
        }
        Command::Audit { after, limit } => {
            let page = client.audit(after, limit).map_err(client_error)?;
            for event in page.events {
                println!(
                    "sequence={} request_id={} principal={} operation={} outcome={:?} target_hash={} error_class={}",
                    event.sequence,
                    event.request_id,
                    event.principal,
                    event.operation,
                    event.outcome,
                    event.target_hash.is_some(),
                    event.error_class.as_deref().unwrap_or("-")
                );
            }
            println!(
                "next_sequence={}",
                page.next_sequence
                    .map_or_else(|| "-".into(), |value| value.to_string())
            );
        }
        Command::Backup(name) => {
            let backup = client.backup(name).map_err(client_error)?;
            println!(
                "backup={} generation={} files={} bytes={} logical_bytes={} encrypted={}",
                backup.name,
                backup.catalog_generation,
                backup.files,
                backup.bytes,
                backup.logical_bytes,
                backup.encrypted
            );
        }
        Command::CompressionPolicy(collection) => {
            let policy = client
                .compression_policy(collection)
                .map_err(client_error)?;
            println!(
                "surface={} hot={} warm={} cold={} archive={} skip_prefixes={}",
                compression_mode_name(policy.surface.mode),
                compression_mode_name(policy.hot.mode),
                compression_mode_name(policy.warm.mode),
                compression_mode_name(policy.cold.mode),
                compression_mode_name(policy.archive.mode),
                policy.skip_content_type_prefixes.join(",")
            );
        }
        Command::SetCompression(collection, policy) => {
            client
                .configure_compression(collection, policy)
                .map_err(client_error)?;
            println!("compression_policy=updated_successfully");
        }
        Command::Expire => {
            let report = client
                .expire(1024, Durability::Durable)
                .map_err(client_error)?;
            println!(
                "ttl_scanned_entries={} ttl_expired_entries={} ttl_stale_entries={}",
                report.scanned, report.expired, report.stale_entries
            );
        }
        Command::Explain(identity) => {
            let placement = client.explain_placement(identity).map_err(client_error)?;
            println!(
                "version={}:{}:{} score={} freshness={} urgency={} current={} recommended={} storage_class={} pinned={} cache_resident={} physical_tiering={} reasons={}",
                placement.canonical_version.epoch,
                placement.canonical_version.shard_id,
                placement.canonical_version.sequence,
                placement.radial_score_millis,
                placement.freshness_millis,
                placement.urgency_millis,
                placement.current_layer,
                placement.recommended_layer,
                placement.storage_class,
                placement.pinned,
                placement.object_cache_resident,
                placement.physical_tiering_supported,
                placement.reasons.join(" | ")
            );
        }
        Command::CreateSurface(definition) => {
            let id = definition.id.clone();
            client.create_surface(definition).map_err(client_error)?;
            println!("surface_creation_successful={id}");
        }
        Command::BuildSurface { id, max_events } => {
            let report = client
                .build_surface(&id, max_events, Durability::Durable)
                .map_err(client_error)?;
            println!(
                "surface={} generation={} events={} records={} bytes={}",
                report.projection_id,
                report.generation,
                report.events_applied,
                report.record_count,
                report.serialized_bytes
            );
        }
        Command::RebuildSurface(id) => {
            let report = client
                .rebuild_surface(&id, Durability::Durable)
                .map_err(client_error)?;
            println!(
                "surface={} generation={} records={} bytes={}",
                report.projection_id,
                report.generation,
                report.record_count,
                report.serialized_bytes
            );
        }
    }
    Ok(())
}

fn client_error(error: aprodb_client::ClientError) -> String {
    error.to_string()
}

const fn compression_mode_name(mode: CompressionMode) -> &'static str {
    match mode {
        CompressionMode::Raw => "raw",
        CompressionMode::AdaptiveZstandard => "zstd",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_handles_command_and_address() {
        let options =
            parse(["verify".into(), "--address".into(), "127.0.0.1:9999".into()]).unwrap();
        assert_eq!(options.command, Command::Verify);
        assert_eq!(options.address.port(), 9999);
        assert!(parse(Vec::new()).is_err());
        assert!(matches!(
            parse([
                "explain".into(),
                "t".into(),
                "n".into(),
                "c".into(),
                "p".into(),
                "k".into(),
            ])
            .unwrap()
            .command,
            Command::Explain(_)
        ));
        assert!(matches!(
            parse([
                "create-surface".into(),
                "work".into(),
                "work".into(),
                "t".into(),
                "n".into(),
                "c".into(),
                "pending,ready".into(),
                "records".into(),
                "10".into(),
                "4096".into(),
                "2".into(),
            ])
            .unwrap()
            .command,
            Command::CreateSurface(_)
        ));
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
