use std::{
    collections::{BTreeMap, HashMap},
    env,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::ExitCode,
    sync::Arc,
    time::Duration,
};

use aprodb_engine::{EncryptionConfig, Engine, EngineConfig};
use aprodb_server::{Server, ServerConfig, TenantQuota, tls_server_config};
use serde::Deserialize;
use sysinfo::System;

const USAGE: &str = "usage: aprodb-server --data-dir PATH [--data-listen ADDR] [--admin-listen ADDR] [--data-local NAME] [--admin-local NAME] [--backup-root PATH] [--max-data-bytes N] [--min-free-disk-bytes N] [--max-compaction-temporary-bytes N] [--tls-cert PEM --tls-key PEM [--tls-client-ca PEM]] [--encryption-keyring JSON] [--tenant-quotas JSON] [--admin-principal ID] [--compute-cpu-threads N] [--compute-queue-depth N] [--compute-queue-bytes N] [--compute-max-batch-rows N] [--compute-max-batch-bytes N] [--compute-timeout-ms N] [--compute-micro-batch-ms N] [--gpu-vram-bytes N]\nrequires APRODB_DATA_TOKEN and APRODB_ADMIN_TOKEN";

#[derive(Debug)]
struct Options {
    data_dir: PathBuf,
    data_listen: Option<SocketAddr>,
    admin_listen: Option<SocketAddr>,
    data_local: Option<String>,
    admin_local: Option<String>,
    max_frame_bytes: usize,
    max_connections: usize,
    max_inflight_per_connection: usize,
    max_inflight_global: usize,
    response_queue_depth: usize,
    backpressure_retry_after: Duration,
    idle_timeout: Duration,
    drain_timeout: Duration,
    allow_plaintext_non_loopback: bool,
    memory_budget_bytes: Option<usize>,
    compute_cpu_threads: Option<usize>,
    compute_queue_depth: Option<usize>,
    compute_queue_bytes: Option<usize>,
    compute_max_batch_rows: Option<usize>,
    compute_max_batch_bytes: Option<usize>,
    compute_timeout: Option<Duration>,
    compute_micro_batch_wait: Option<Duration>,
    gpu_vram_bytes: Option<usize>,
    tls_certificate: Option<PathBuf>,
    tls_private_key: Option<PathBuf>,
    tls_client_ca: Option<PathBuf>,
    encryption_keyring: Option<PathBuf>,
    tenant_quotas: Option<PathBuf>,
    admin_principal: String,
    backup_root: Option<PathBuf>,
    max_data_bytes: Option<u64>,
    min_free_disk_bytes: Option<u64>,
    max_compaction_temporary_bytes: Option<u64>,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::new(),
            data_listen: Some("127.0.0.1:7643".parse().expect("indirizzo costante")),
            admin_listen: Some("127.0.0.1:7644".parse().expect("indirizzo costante")),
            data_local: None,
            admin_local: None,
            max_frame_bytes: aprodb_proto::DEFAULT_MAX_FRAME_BYTES,
            max_connections: 128,
            max_inflight_per_connection: 32,
            max_inflight_global: 256,
            response_queue_depth: 64,
            backpressure_retry_after: Duration::from_millis(10),
            idle_timeout: Duration::from_secs(60),
            drain_timeout: Duration::from_secs(30),
            allow_plaintext_non_loopback: false,
            memory_budget_bytes: None,
            compute_cpu_threads: None,
            compute_queue_depth: None,
            compute_queue_bytes: None,
            compute_max_batch_rows: None,
            compute_max_batch_bytes: None,
            compute_timeout: None,
            compute_micro_batch_wait: None,
            gpu_vram_bytes: None,
            tls_certificate: None,
            tls_private_key: None,
            tls_client_ca: None,
            encryption_keyring: None,
            tenant_quotas: None,
            admin_principal: "local-admin".into(),
            backup_root: None,
            max_data_bytes: None,
            min_free_disk_bytes: None,
            max_compaction_temporary_bytes: None,
        }
    }
}

fn parse_options(arguments: impl IntoIterator<Item = String>) -> Result<Options, String> {
    let mut options = Options::default();
    let mut arguments = arguments.into_iter();
    while let Some(argument) = arguments.next() {
        let value = |arguments: &mut dyn Iterator<Item = String>| {
            arguments
                .next()
                .ok_or_else(|| format!("missing value for {argument}"))
        };
        match argument.as_str() {
            "--data-dir" => options.data_dir = PathBuf::from(value(&mut arguments)?),
            "--data-listen" => {
                options.data_listen = Some(
                    value(&mut arguments)?
                        .parse()
                        .map_err(|_| "invalid --data-listen argument".to_string())?,
                );
            }
            "--admin-listen" => {
                options.admin_listen = Some(
                    value(&mut arguments)?
                        .parse()
                        .map_err(|_| "invalid --admin-listen argument".to_string())?,
                );
            }
            "--no-data-tcp" => options.data_listen = None,
            "--no-admin-tcp" => options.admin_listen = None,
            "--data-local" => options.data_local = Some(value(&mut arguments)?),
            "--admin-local" => options.admin_local = Some(value(&mut arguments)?),
            "--max-frame-bytes" => {
                options.max_frame_bytes = parse_usize(&argument, value(&mut arguments)?)?;
            }
            "--max-connections" => {
                options.max_connections = parse_usize(&argument, value(&mut arguments)?)?;
            }
            "--max-inflight-per-connection" => {
                options.max_inflight_per_connection =
                    parse_usize(&argument, value(&mut arguments)?)?;
            }
            "--max-inflight-global" => {
                options.max_inflight_global = parse_usize(&argument, value(&mut arguments)?)?;
            }
            "--response-queue-depth" => {
                options.response_queue_depth = parse_usize(&argument, value(&mut arguments)?)?;
            }
            "--backpressure-retry-ms" => {
                options.backpressure_retry_after =
                    Duration::from_millis(parse_u64(&argument, value(&mut arguments)?)?);
            }
            "--idle-timeout-ms" => {
                options.idle_timeout =
                    Duration::from_millis(parse_u64(&argument, value(&mut arguments)?)?);
            }
            "--drain-timeout-ms" => {
                options.drain_timeout =
                    Duration::from_millis(parse_u64(&argument, value(&mut arguments)?)?);
            }
            "--allow-plaintext-non-loopback" => options.allow_plaintext_non_loopback = true,
            "--memory-budget-bytes" => {
                options.memory_budget_bytes = Some(parse_usize(&argument, value(&mut arguments)?)?);
            }
            "--compute-cpu-threads" => {
                options.compute_cpu_threads = Some(parse_usize(&argument, value(&mut arguments)?)?);
            }
            "--compute-queue-depth" => {
                options.compute_queue_depth = Some(parse_usize(&argument, value(&mut arguments)?)?);
            }
            "--compute-queue-bytes" => {
                options.compute_queue_bytes = Some(parse_usize(&argument, value(&mut arguments)?)?);
            }
            "--compute-max-batch-rows" => {
                options.compute_max_batch_rows =
                    Some(parse_usize(&argument, value(&mut arguments)?)?);
            }
            "--compute-max-batch-bytes" => {
                options.compute_max_batch_bytes =
                    Some(parse_usize(&argument, value(&mut arguments)?)?);
            }
            "--compute-timeout-ms" => {
                options.compute_timeout = Some(Duration::from_millis(parse_u64(
                    &argument,
                    value(&mut arguments)?,
                )?));
            }
            "--compute-micro-batch-ms" => {
                options.compute_micro_batch_wait = Some(Duration::from_millis(parse_u64(
                    &argument,
                    value(&mut arguments)?,
                )?));
            }
            "--gpu-vram-bytes" => {
                options.gpu_vram_bytes = Some(parse_usize(&argument, value(&mut arguments)?)?);
            }
            "--tls-cert" => options.tls_certificate = Some(PathBuf::from(value(&mut arguments)?)),
            "--tls-key" => options.tls_private_key = Some(PathBuf::from(value(&mut arguments)?)),
            "--tls-client-ca" => {
                options.tls_client_ca = Some(PathBuf::from(value(&mut arguments)?))
            }
            "--encryption-keyring" => {
                options.encryption_keyring = Some(PathBuf::from(value(&mut arguments)?));
            }
            "--tenant-quotas" => {
                options.tenant_quotas = Some(PathBuf::from(value(&mut arguments)?));
            }
            "--admin-principal" => options.admin_principal = value(&mut arguments)?,
            "--backup-root" => options.backup_root = Some(PathBuf::from(value(&mut arguments)?)),
            "--max-data-bytes" => {
                options.max_data_bytes = Some(parse_u64(&argument, value(&mut arguments)?)?);
            }
            "--min-free-disk-bytes" => {
                options.min_free_disk_bytes = Some(parse_u64(&argument, value(&mut arguments)?)?);
            }
            "--max-compaction-temporary-bytes" => {
                options.max_compaction_temporary_bytes =
                    Some(parse_u64(&argument, value(&mut arguments)?)?);
            }
            "--help" | "-h" => return Err(USAGE.into()),
            _ => return Err(format!("unknown argument: {argument}")),
        }
    }
    if options.data_dir.as_os_str().is_empty() {
        return Err("--data-dir is required".into());
    }
    if options.tls_certificate.is_some() != options.tls_private_key.is_some() {
        return Err("--tls-cert and --tls-key must be provided together".into());
    }
    if options.tls_client_ca.is_some() && options.tls_certificate.is_none() {
        return Err("--tls-client-ca requires --tls-cert and --tls-key".into());
    }
    Ok(options)
}

fn parse_usize(name: &str, value: String) -> Result<usize, String> {
    value.parse().map_err(|_| format!("{name} is invalid"))
}

fn parse_u64(name: &str, value: String) -> Result<u64, String> {
    value.parse().map_err(|_| format!("{name} is invalid"))
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("aprodb-server: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), String> {
    let mut options = parse_options(env::args().skip(1))?;
    if options.encryption_keyring.is_none() {
        options.encryption_keyring =
            env::var_os("APRODB_ENCRYPTION_KEYRING_FILE").map(PathBuf::from);
    }
    let data_token = env::var_os("APRODB_DATA_TOKEN")
        .ok_or("missing APRODB_DATA_TOKEN")?
        .to_string_lossy()
        .into_owned();
    let admin_token = env::var_os("APRODB_ADMIN_TOKEN")
        .ok_or("missing APRODB_ADMIN_TOKEN")?
        .to_string_lossy()
        .into_owned();
    let memory = detect_memory_limit(options.memory_budget_bytes)?;
    let mut engine_config = EngineConfig::new(&options.data_dir);
    if let Some(path) = &options.encryption_keyring {
        engine_config.encryption = Some(load_encryption_keyring(path)?);
    }
    let encryption_enabled = engine_config.encryption.is_some();
    engine_config
        .apply_memory_budget(memory.effective_bytes)
        .map_err(|error| error.to_string())?;
    if let Some(value) = options.compute_cpu_threads {
        engine_config.compute.cpu_threads = value;
    }
    if let Some(value) = options.compute_queue_depth {
        engine_config.compute.queue_depth = value;
    }
    if let Some(value) = options.compute_queue_bytes {
        engine_config.compute.queue_byte_budget = value;
    }
    if let Some(value) = options.compute_max_batch_rows {
        engine_config.compute.max_batch_rows = value;
    }
    if let Some(value) = options.compute_max_batch_bytes {
        engine_config.compute.max_batch_bytes = value;
    }
    if let Some(value) = options.compute_timeout {
        engine_config.compute.request_timeout = value;
    }
    if let Some(value) = options.compute_micro_batch_wait {
        engine_config.compute.micro_batch_max_wait = value;
    }
    if let Some(value) = options.gpu_vram_bytes {
        engine_config.compute.vram_budget_bytes = value;
    }
    if let Some(value) = options.max_data_bytes {
        engine_config.max_data_bytes = Some(value);
    }
    if let Some(value) = options.min_free_disk_bytes {
        engine_config.min_free_disk_bytes = value;
    }
    if let Some(value) = options.max_compaction_temporary_bytes {
        engine_config.max_compaction_temporary_bytes = value;
    }
    let engine = Arc::new(Engine::open(engine_config).map_err(|error| error.to_string())?);
    let mut config =
        ServerConfig::loopback(data_token, admin_token).map_err(|error| error.to_string())?;
    config.data_tcp = options.data_listen;
    config.admin_tcp = options.admin_listen;
    config.local_data = options.data_local;
    config.local_admin = options.admin_local;
    config.max_frame_bytes = options.max_frame_bytes;
    config.max_connections = options.max_connections;
    config.max_inflight_per_connection = options.max_inflight_per_connection;
    config.max_inflight_global = options.max_inflight_global;
    config.response_queue_depth = options.response_queue_depth;
    config.backpressure_retry_after = options.backpressure_retry_after;
    config.idle_timeout = options.idle_timeout;
    config.drain_timeout = options.drain_timeout;
    config.allow_plaintext_non_loopback = options.allow_plaintext_non_loopback;
    config.admin_principal = options.admin_principal;
    config.backup_root = options.backup_root;
    if let Some(path) = &options.tenant_quotas {
        config.tenant_quotas = load_tenant_quotas(path)?;
    }
    if let (Some(certificate), Some(private_key)) =
        (&options.tls_certificate, &options.tls_private_key)
    {
        let certificate = read_bounded_file(certificate, 4 * 1024 * 1024, false)?;
        let private_key = read_bounded_file(private_key, 1024 * 1024, true)?;
        let client_ca = options
            .tls_client_ca
            .as_ref()
            .map(|path| read_bounded_file(path, 4 * 1024 * 1024, false))
            .transpose()?;
        config.tls = Some(
            tls_server_config(&certificate, &private_key, client_ca.as_deref())
                .map_err(|error| error.to_string())?,
        );
    }
    let tls_enabled = config.tls.is_some();
    let handle = Server::start(engine, config)
        .await
        .map_err(|error| error.to_string())?;
    eprintln!(
        "AProDB server started: data_tcp={:?}, admin_tcp={:?}, data_local={:?}, admin_local={:?}, tls_enabled={}, at_rest_encryption_enabled={}, memory_effective_bytes={}, memory_physical_bytes={}, memory_container_bytes={:?}, memory_configured_bytes={:?}",
        handle.data_tcp,
        handle.admin_tcp,
        handle.local_data,
        handle.local_admin,
        tls_enabled,
        encryption_enabled,
        memory.effective_bytes,
        memory.physical_bytes,
        memory.container_bytes,
        memory.configured_bytes,
    );
    tokio::select! {
        result = tokio::signal::ctrl_c() => result.map_err(|error| error.to_string())?,
        () = handle.wait_for_shutdown() => {},
    }
    handle.shutdown().await.map_err(|error| error.to_string())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EncryptionKeyringFile {
    active_key_id: String,
    keys: BTreeMap<String, String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TenantQuotasFile {
    tenants: HashMap<String, TenantQuota>,
}

fn load_encryption_keyring(path: &Path) -> Result<EncryptionConfig, String> {
    let bytes = read_bounded_file(path, 64 * 1024, true)?;
    let file: EncryptionKeyringFile =
        serde_json::from_slice(&bytes).map_err(|error| format!("invalid keyring JSON: {error}"))?;
    let mut keys = BTreeMap::new();
    for (key_id, encoded) in file.keys {
        if encoded.len() != 64 {
            return Err(format!("key {key_id} must contain exactly 32 hex bytes"));
        }
        let mut key = [0u8; 32];
        hex::decode_to_slice(encoded.as_bytes(), &mut key)
            .map_err(|_| format!("key {key_id} is not valid hex"))?;
        keys.insert(key_id, key);
    }
    EncryptionConfig::new(file.active_key_id, keys).map_err(|error| error.to_string())
}

fn load_tenant_quotas(path: &Path) -> Result<HashMap<Vec<u8>, TenantQuota>, String> {
    let bytes = read_bounded_file(path, 1024 * 1024, false)?;
    let file: TenantQuotasFile = serde_json::from_slice(&bytes)
        .map_err(|error| format!("invalid tenant quotas JSON: {error}"))?;
    Ok(file
        .tenants
        .into_iter()
        .map(|(tenant, quota)| (tenant.into_bytes(), quota))
        .collect())
}

fn read_bounded_file(path: &Path, maximum_bytes: u64, secret: bool) -> Result<Vec<u8>, String> {
    let metadata = std::fs::symlink_metadata(path)
        .map_err(|error| format!("unable to read metadata for {}: {error}", path.display()))?;
    if metadata.file_type().is_symlink() || !metadata.is_file() {
        return Err(format!("{} must be a regular file", path.display()));
    }
    if metadata.len() > maximum_bytes {
        return Err(format!(
            "{} exceeds the limit of {} bytes",
            path.display(),
            maximum_bytes
        ));
    }
    #[cfg(unix)]
    if secret {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(format!(
                "{} must be readable/writable only by owner",
                path.display()
            ));
        }
    }
    #[cfg(not(unix))]
    let _ = secret;
    let bytes = std::fs::read(path)
        .map_err(|error| format!("unable to read {}: {error}", path.display()))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > maximum_bytes {
        return Err(format!(
            "{} grew beyond limit during reading",
            path.display()
        ));
    }
    Ok(bytes)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct MemoryLimit {
    physical_bytes: u64,
    container_bytes: Option<u64>,
    configured_bytes: Option<usize>,
    effective_bytes: usize,
}

fn detect_memory_limit(configured_bytes: Option<usize>) -> Result<MemoryLimit, String> {
    let mut system = System::new();
    system.refresh_memory();
    let physical_bytes = system.total_memory();
    let container_bytes = system
        .cgroup_limits()
        .map(|limits| limits.total_memory)
        .filter(|limit| *limit > 0);
    choose_memory_limit(configured_bytes, physical_bytes, container_bytes)
}

fn choose_memory_limit(
    configured_bytes: Option<usize>,
    physical_bytes: u64,
    container_bytes: Option<u64>,
) -> Result<MemoryLimit, String> {
    let detected_ceiling = [
        (physical_bytes > 0).then_some(physical_bytes),
        container_bytes.filter(|limit| *limit > 0),
    ]
    .into_iter()
    .flatten()
    .min();
    let ceiling = detected_ceiling
        .map(|bytes| usize::try_from(bytes).unwrap_or(usize::MAX))
        .or(configured_bytes)
        .ok_or("memory limit not detectable: specify --memory-budget-bytes")?;
    let requested = configured_bytes.unwrap_or(ceiling / 2);
    let effective_bytes = requested.min(ceiling);
    Ok(MemoryLimit {
        physical_bytes,
        container_bytes,
        configured_bytes,
        effective_bytes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_requires_data_directory_and_parses_limits() {
        let parsed = parse_options([
            "--data-dir".into(),
            "db".into(),
            "--max-connections".into(),
            "7".into(),
            "--no-admin-tcp".into(),
        ])
        .unwrap();
        assert_eq!(parsed.data_dir, PathBuf::from("db"));
        assert_eq!(parsed.max_connections, 7);
        assert!(parsed.admin_listen.is_none());
        assert!(parse_options(Vec::new()).is_err());
    }

    #[test]
    fn memory_limit_uses_minimum_and_defaults_to_half() {
        let automatic = choose_memory_limit(None, 16_000, Some(8_000)).unwrap();
        assert_eq!(automatic.effective_bytes, 4_000);
        let configured = choose_memory_limit(Some(12_000), 16_000, Some(8_000)).unwrap();
        assert_eq!(configured.effective_bytes, 8_000);
        assert!(choose_memory_limit(None, 0, None).is_err());
    }

    #[test]
    fn keyring_loader_validates_exact_keys_without_exposing_material() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("keyring.json");
        let material = "11".repeat(32);
        std::fs::write(
            &path,
            format!(r#"{{"active_key_id":"primary","keys":{{"primary":"{material}"}}}}"#),
        )
        .unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        }
        let keyring = load_encryption_keyring(&path).unwrap();
        let rendered = format!("{keyring:?}");
        assert!(rendered.contains("primary"));
        assert!(!rendered.contains(&material));
    }

    #[test]
    fn tls_arguments_must_be_complete() {
        assert!(
            parse_options([
                "--data-dir".into(),
                "db".into(),
                "--tls-cert".into(),
                "cert.pem".into(),
            ])
            .is_err()
        );
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
