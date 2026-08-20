use std::{
    collections::{HashMap, HashSet},
    fmt,
    io::Cursor,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    path::PathBuf,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use aprodb_engine::{
    AproError, AtomicMutation as EngineMutation, CacheMetrics, ClaimRequest, ComputeExecution,
    ComputePreference, CostEstimate, DeleteRequest, Durability, Engine,
    ExpectedVersion as EngineExpected, LeaseProof, PutRequest, SurfaceDefinition, SurfaceFormat,
    SurfaceKind, VectorMetric, VectorSearchRequest, WorkflowScope,
};
use aprodb_proto::{
    AtomicMutation, BuildSurfaceOperation, ClientHello, DeleteOperation, EndpointRole, ErrorCode,
    ExpectedMode, ExpectedVersion, ExpirationStats, LeaseOperation, PutOperation, Request,
    Response, ServerHello, Stats, WireAuditEvent, WireBackupInfo, WireCacheMetrics, WireCacheStats,
    WireCompressionDictionary, WireCompressionMode, WireCompressionPolicy, WireCompressionStats,
    WireCompressionTierPolicy, WireComputeExecution, WireComputePreference, WireComputeStats,
    WireCostEstimate, WireDurability, WirePlacement, WireRecord, WireSurfaceBuild,
    WireSurfaceDefinition, WireSurfaceFormat, WireSurfaceGeneration, WireSurfaceKind,
    WireVectorHit, WireVectorMetric, WireVectorSearchResult, decode_limited, encode_limited,
    identity_from_wire, request,
};
use aprodb_types::{
    AuditCursor, AuditOutcome, CompressionMode, CompressionPolicy, CompressionTierPolicy, Payload,
    RadialLayer, RecordIdentity,
};
use futures_util::{SinkExt, StreamExt, stream::SplitSink};
use rustls::{RootCertStore, server::WebPkiClientVerifier};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpListener,
    sync::{Mutex, Semaphore, mpsc, watch},
    task::{JoinHandle, JoinSet},
    time::timeout,
};
use tokio_rustls::TlsAcceptor;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[derive(Clone)]
pub struct SecretToken(Vec<u8>);

impl SecretToken {
    pub fn new(token: impl Into<Vec<u8>>) -> Result<Self, ServerError> {
        let token = token.into();
        if token.len() < 16 || token.len() > 4096 {
            return Err(ServerError::InvalidConfig(
                "token must be between 16 and 4096 bytes in length".into(),
            ));
        }
        Ok(Self(token))
    }

    fn matches(&self, candidate: &[u8]) -> bool {
        self.0.len() == candidate.len() && bool::from(self.0.ct_eq(candidate))
    }
}

impl fmt::Debug for SecretToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretToken([REDACTED])")
    }
}

pub fn tls_server_config(
    certificate_pem: &[u8],
    private_key_pem: &[u8],
    client_ca_pem: Option<&[u8]>,
) -> Result<Arc<rustls::ServerConfig>, ServerError> {
    let certificates = rustls_pemfile::certs(&mut Cursor::new(certificate_pem))
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|error| ServerError::InvalidConfig(format!("TLS certificate: {error}")))?;
    if certificates.is_empty() {
        return Err(ServerError::InvalidConfig(
            "TLS certificate chain cannot be empty".into(),
        ));
    }
    let private_key = rustls_pemfile::private_key(&mut Cursor::new(private_key_pem))
        .map_err(|error| ServerError::InvalidConfig(format!("TLS private key: {error}")))?
        .ok_or_else(|| ServerError::InvalidConfig("TLS private key is missing".into()))?;
    let builder = rustls::ServerConfig::builder();
    let config = if let Some(client_ca_pem) = client_ca_pem {
        let mut roots = RootCertStore::empty();
        let client_roots = rustls_pemfile::certs(&mut Cursor::new(client_ca_pem))
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(|error| ServerError::InvalidConfig(format!("TLS client CA: {error}")))?;
        if client_roots.is_empty() {
            return Err(ServerError::InvalidConfig(
                "TLS client CA cannot be empty".into(),
            ));
        }
        for certificate in client_roots {
            roots.add(certificate).map_err(|error| {
                ServerError::InvalidConfig(format!("TLS client CA is invalid: {error}"))
            })?;
        }
        let verifier = WebPkiClientVerifier::builder(Arc::new(roots))
            .build()
            .map_err(|error| {
                ServerError::InvalidConfig(format!(
                    "mTLS client certificate verifier error: {error}"
                ))
            })?;
        builder
            .with_client_cert_verifier(verifier)
            .with_single_cert(certificates, private_key)
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)
    }
    .map_err(|error| ServerError::InvalidConfig(format!("TLS server identity: {error}")))?;
    Ok(Arc::new(config))
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantQuota {
    pub max_inflight: usize,
    pub max_requests_per_second: usize,
    pub max_request_bytes: usize,
    pub max_vector_work_items: usize,
}

impl TenantQuota {
    fn validate(&self) -> Result<(), ServerError> {
        for (name, value) in [
            ("max_inflight", self.max_inflight),
            ("max_requests_per_second", self.max_requests_per_second),
            ("max_request_bytes", self.max_request_bytes),
            ("max_vector_work_items", self.max_vector_work_items),
        ] {
            if value == 0 {
                return Err(ServerError::InvalidConfig(format!(
                    "tenant quota {name} must be greater than zero"
                )));
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct ServerConfig {
    pub data_tcp: Option<SocketAddr>,
    pub admin_tcp: Option<SocketAddr>,
    pub local_data: Option<String>,
    pub local_admin: Option<String>,
    pub data_token: SecretToken,
    pub admin_token: SecretToken,
    pub admin_principal: String,
    pub max_frame_bytes: usize,
    pub max_connections: usize,
    pub max_inflight_per_connection: usize,
    pub max_inflight_global: usize,
    pub response_queue_depth: usize,
    pub backpressure_retry_after: Duration,
    pub idle_timeout: Duration,
    pub drain_timeout: Duration,
    pub allow_plaintext_non_loopback: bool,
    pub tenant_quotas: HashMap<Vec<u8>, TenantQuota>,
    pub tls: Option<Arc<rustls::ServerConfig>>,
    pub backup_root: Option<PathBuf>,
}

impl ServerConfig {
    pub fn loopback(
        data_token: impl Into<Vec<u8>>,
        admin_token: impl Into<Vec<u8>>,
    ) -> Result<Self, ServerError> {
        Ok(Self {
            data_tcp: Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)),
            admin_tcp: Some(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)),
            local_data: None,
            local_admin: None,
            data_token: SecretToken::new(data_token)?,
            admin_token: SecretToken::new(admin_token)?,
            admin_principal: "local-admin".into(),
            max_frame_bytes: aprodb_proto::DEFAULT_MAX_FRAME_BYTES,
            max_connections: 128,
            max_inflight_per_connection: 32,
            max_inflight_global: 256,
            response_queue_depth: 64,
            backpressure_retry_after: Duration::from_millis(10),
            idle_timeout: Duration::from_secs(60),
            drain_timeout: Duration::from_secs(30),
            allow_plaintext_non_loopback: false,
            tenant_quotas: HashMap::new(),
            tls: None,
            backup_root: None,
        })
    }

    fn validate(&self) -> Result<(), ServerError> {
        if self.admin_principal.is_empty() || self.admin_principal.len() > 128 {
            return Err(ServerError::InvalidConfig(
                "admin_principal must be between 1 and 128 bytes".into(),
            ));
        }
        if self.data_tcp.is_none()
            && self.admin_tcp.is_none()
            && self.local_data.is_none()
            && self.local_admin.is_none()
        {
            return Err(ServerError::InvalidConfig(
                "at least one server endpoint must be configured".into(),
            ));
        }
        for (name, value) in [
            ("max_connections", self.max_connections),
            (
                "max_inflight_per_connection",
                self.max_inflight_per_connection,
            ),
            ("max_inflight_global", self.max_inflight_global),
            ("response_queue_depth", self.response_queue_depth),
        ] {
            if value == 0 {
                return Err(ServerError::InvalidConfig(format!(
                    "{name} must be greater than zero"
                )));
            }
        }
        if self.max_inflight_per_connection > self.max_inflight_global {
            return Err(ServerError::InvalidConfig(
                "inflight requests per connection exceed the global limit".into(),
            ));
        }
        if self.response_queue_depth < self.max_inflight_per_connection {
            return Err(ServerError::InvalidConfig(
                "response queue depth is less than inflight requests per connection".into(),
            ));
        }
        if self.idle_timeout.is_zero()
            || self.drain_timeout.is_zero()
            || self.backpressure_retry_after.is_zero()
        {
            return Err(ServerError::InvalidConfig(
                "server timeout values must be greater than zero".into(),
            ));
        }
        let hello = ClientHello::new(EndpointRole::Data, Vec::new(), self.max_frame_bytes);
        hello
            .validate()
            .map_err(|error| ServerError::InvalidConfig(error.to_string()))?;
        if self.data_token.0 == self.admin_token.0 {
            return Err(ServerError::InvalidConfig(
                "data and admin tokens must be different".into(),
            ));
        }
        for (tenant, quota) in &self.tenant_quotas {
            if tenant.is_empty() || tenant.len() > 128 {
                return Err(ServerError::InvalidConfig(
                    "tenant quota identifier must be between 1 and 128 bytes in length".into(),
                ));
            }
            quota.validate()?;
        }
        if !self.allow_plaintext_non_loopback && self.tls.is_none() {
            for address in [self.data_tcp, self.admin_tcp].into_iter().flatten() {
                if !address.ip().is_loopback() {
                    return Err(ServerError::InvalidConfig(format!(
                        "non-loopback TCP plaintext connections are prohibited: {address}"
                    )));
                }
            }
        }
        #[cfg(windows)]
        for name in [&self.local_data, &self.local_admin].into_iter().flatten() {
            if !name.starts_with(r"\\.\pipe\") {
                return Err(ServerError::InvalidConfig(
                    "Windows named pipe must begin with \\\\.\\pipe\\".into(),
                ));
            }
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    #[error("invalid server configuration: {0}")]
    InvalidConfig(String),
    #[error("server I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("server task error: {0}")]
    Task(String),
}

#[derive(Default)]
struct Metrics {
    active_connections: AtomicU64,
    inflight_requests: AtomicU64,
    total_requests: AtomicU64,
    rejected_requests: AtomicU64,
    auth_failures: AtomicU64,
}

impl Metrics {
    fn snapshot(&self, engine: &Engine) -> aprodb_types::Result<Stats> {
        let storage = engine.stats()?;
        Ok(Stats {
            disk_bytes: storage.disk_bytes,
            write_buffer_bytes: storage.write_buffer_bytes,
            journal_fragments: storage.journal_fragments as u64,
            table_count: storage.table_count as u64,
            completed_compactions: storage.completed_compactions as u64,
            active_connections: self.active_connections.load(Ordering::Acquire),
            inflight_requests: self.inflight_requests.load(Ordering::Acquire),
            total_requests: self.total_requests.load(Ordering::Acquire),
            rejected_requests: self.rejected_requests.load(Ordering::Acquire),
            auth_failures: self.auth_failures.load(Ordering::Acquire),
        })
    }
}

struct State {
    engine: Arc<Engine>,
    config: Arc<ServerConfig>,
    metrics: Arc<Metrics>,
    global_inflight: Arc<Semaphore>,
    shutdown: watch::Sender<bool>,
    tenant_quota_runtime: Arc<TenantQuotaRuntime>,
}

#[derive(Default)]
struct TenantQuotaUsage {
    window_second: u64,
    requests_in_window: usize,
    inflight: usize,
}

#[derive(Default)]
struct TenantQuotaRuntime {
    usage: StdMutex<HashMap<Vec<u8>, TenantQuotaUsage>>,
}

struct TenantQuotaPermit {
    runtime: Arc<TenantQuotaRuntime>,
    tenant: Vec<u8>,
}

impl Drop for TenantQuotaPermit {
    fn drop(&mut self) {
        if let Ok(mut usage) = self.runtime.usage.lock()
            && let Some(tenant) = usage.get_mut(&self.tenant)
        {
            tenant.inflight = tenant.inflight.saturating_sub(1);
        }
    }
}

impl TenantQuotaRuntime {
    fn try_acquire(
        self: &Arc<Self>,
        tenant: &[u8],
        quota: &TenantQuota,
        request: &Request,
        request_bytes: usize,
    ) -> std::result::Result<TenantQuotaPermit, (ErrorCode, &'static str)> {
        if request_bytes > quota.max_request_bytes {
            return Err((
                ErrorCode::ResourceLimit,
                "tenant request byte quota exceeded",
            ));
        }
        if let Some(request::Operation::VectorSearch(operation)) = request.operation.as_ref() {
            let scan = usize::try_from(operation.max_scan_records).unwrap_or(usize::MAX);
            let work = operation.query.len().saturating_mul(scan);
            if work > quota.max_vector_work_items {
                return Err((ErrorCode::ResourceLimit, "tenant compute quota exceeded"));
            }
        }
        let second = now_unix_ms().unwrap_or(u64::MAX) / 1_000;
        let mut usage = self
            .usage
            .lock()
            .map_err(|_| (ErrorCode::Internal, "tenant quota counter not available"))?;
        let tenant_usage = usage.entry(tenant.to_vec()).or_default();
        if tenant_usage.window_second != second {
            tenant_usage.window_second = second;
            tenant_usage.requests_in_window = 0;
        }
        if tenant_usage.requests_in_window >= quota.max_requests_per_second {
            return Err((
                ErrorCode::Backpressure,
                "tenant requests per second quota reached",
            ));
        }
        if tenant_usage.inflight >= quota.max_inflight {
            return Err((
                ErrorCode::Backpressure,
                "tenant inflight requests quota reached",
            ));
        }
        tenant_usage.requests_in_window += 1;
        tenant_usage.inflight += 1;
        Ok(TenantQuotaPermit {
            runtime: Arc::clone(self),
            tenant: tenant.to_vec(),
        })
    }
}

pub struct Server;

impl Server {
    pub async fn start(
        engine: Arc<Engine>,
        config: ServerConfig,
    ) -> Result<ServerHandle, ServerError> {
        config.validate()?;
        let config = Arc::new(config);
        let (shutdown, _) = watch::channel(false);
        let state = Arc::new(State {
            engine,
            config: Arc::clone(&config),
            metrics: Arc::new(Metrics::default()),
            global_inflight: Arc::new(Semaphore::new(config.max_inflight_global)),
            shutdown: shutdown.clone(),
            tenant_quota_runtime: Arc::new(TenantQuotaRuntime::default()),
        });
        let connection_limit = Arc::new(Semaphore::new(config.max_connections));
        let mut joins = Vec::new();
        let mut data_tcp = None;
        let mut admin_tcp = None;

        if let Some(address) = config.data_tcp {
            let listener = TcpListener::bind(address).await?;
            data_tcp = Some(listener.local_addr()?);
            joins.push(tokio::spawn(run_tcp_listener(
                listener,
                EndpointRole::Data,
                Arc::clone(&state),
                Arc::clone(&connection_limit),
                shutdown.subscribe(),
            )));
        }
        if let Some(address) = config.admin_tcp {
            let listener = TcpListener::bind(address).await?;
            admin_tcp = Some(listener.local_addr()?);
            joins.push(tokio::spawn(run_tcp_listener(
                listener,
                EndpointRole::Admin,
                Arc::clone(&state),
                Arc::clone(&connection_limit),
                shutdown.subscribe(),
            )));
        }

        #[cfg(windows)]
        for (name, role) in [
            (&config.local_data, EndpointRole::Data),
            (&config.local_admin, EndpointRole::Admin),
        ] {
            if let Some(name) = name {
                let mut options = tokio::net::windows::named_pipe::ServerOptions::new();
                options.first_pipe_instance(true);
                let first_pipe = options.create(name)?;
                joins.push(tokio::spawn(run_windows_pipe_listener(
                    name.clone(),
                    first_pipe,
                    role,
                    Arc::clone(&state),
                    Arc::clone(&connection_limit),
                    shutdown.subscribe(),
                )));
            }
        }

        #[cfg(unix)]
        for (path, role) in [
            (&config.local_data, EndpointRole::Data),
            (&config.local_admin, EndpointRole::Admin),
        ] {
            if let Some(path) = path {
                let listener = tokio::net::UnixListener::bind(path)?;
                joins.push(tokio::spawn(run_unix_listener(
                    listener,
                    std::path::PathBuf::from(path),
                    role,
                    Arc::clone(&state),
                    Arc::clone(&connection_limit),
                    shutdown.subscribe(),
                )));
            }
        }

        Ok(ServerHandle {
            data_tcp,
            admin_tcp,
            local_data: config.local_data.clone(),
            local_admin: config.local_admin.clone(),
            shutdown,
            joins,
            drain_timeout: config.drain_timeout,
        })
    }
}

pub struct ServerHandle {
    pub data_tcp: Option<SocketAddr>,
    pub admin_tcp: Option<SocketAddr>,
    pub local_data: Option<String>,
    pub local_admin: Option<String>,
    shutdown: watch::Sender<bool>,
    joins: Vec<JoinHandle<Result<(), ServerError>>>,
    drain_timeout: Duration,
}

impl ServerHandle {
    pub async fn wait_for_shutdown(&self) {
        let mut shutdown = self.shutdown.subscribe();
        while !*shutdown.borrow() && shutdown.changed().await.is_ok() {}
    }

    pub async fn shutdown(mut self) -> Result<(), ServerError> {
        self.shutdown.send_replace(true);
        for mut join in self.joins.drain(..) {
            match timeout(self.drain_timeout, &mut join).await {
                Ok(result) => result.map_err(|error| ServerError::Task(error.to_string()))??,
                Err(_) => {
                    join.abort();
                    return Err(ServerError::Task(format!(
                        "drain exceeded {:?}",
                        self.drain_timeout
                    )));
                }
            }
        }
        Ok(())
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
    }
}

async fn run_tcp_listener(
    listener: TcpListener,
    role: EndpointRole,
    state: Arc<State>,
    connection_limit: Arc<Semaphore>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServerError> {
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                stream.set_nodelay(true)?;
                if let Some(tls) = state.config.tls.clone() {
                    spawn_tls_connection(
                        &mut connections,
                        stream,
                        role,
                        Arc::clone(&state),
                        Arc::clone(&connection_limit),
                        shutdown.clone(),
                        tls,
                    );
                } else {
                    spawn_connection(
                        &mut connections,
                        stream,
                        role,
                        Arc::clone(&state),
                        Arc::clone(&connection_limit),
                        shutdown.clone(),
                    );
                }
            }
        }
    }
    while connections.join_next().await.is_some() {}
    Ok(())
}

#[cfg(unix)]
async fn run_unix_listener(
    listener: tokio::net::UnixListener,
    path: std::path::PathBuf,
    role: EndpointRole,
    state: Arc<State>,
    connection_limit: Arc<Semaphore>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServerError> {
    let _cleanup = UnixSocketCleanup(path);
    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                spawn_connection(
                    &mut connections,
                    stream,
                    role,
                    Arc::clone(&state),
                    Arc::clone(&connection_limit),
                    shutdown.clone(),
                );
            }
        }
    }
    while connections.join_next().await.is_some() {}
    Ok(())
}

#[cfg(unix)]
struct UnixSocketCleanup(std::path::PathBuf);

#[cfg(unix)]
impl Drop for UnixSocketCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

#[cfg(windows)]
async fn run_windows_pipe_listener(
    name: String,
    mut pipe: tokio::net::windows::named_pipe::NamedPipeServer,
    role: EndpointRole,
    state: Arc<State>,
    connection_limit: Arc<Semaphore>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServerError> {
    use tokio::net::windows::named_pipe::ServerOptions;

    let mut connections = JoinSet::new();
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            connected = pipe.connect() => {
                connected?;
                let next_pipe = ServerOptions::new().create(&name)?;
                spawn_connection(
                    &mut connections,
                    pipe,
                    role,
                    Arc::clone(&state),
                    Arc::clone(&connection_limit),
                    shutdown.clone(),
                );
                pipe = next_pipe;
            }
        }
    }
    while connections.join_next().await.is_some() {}
    Ok(())
}

fn spawn_connection<S>(
    connections: &mut JoinSet<()>,
    stream: S,
    role: EndpointRole,
    state: Arc<State>,
    connection_limit: Arc<Semaphore>,
    shutdown: watch::Receiver<bool>,
) where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let Ok(permit) = connection_limit.try_acquire_owned() else {
        state
            .metrics
            .rejected_requests
            .fetch_add(1, Ordering::Relaxed);
        return;
    };
    connections.spawn(async move {
        let _permit = permit;
        let _active = ActiveConnection::new(Arc::clone(&state.metrics));
        let _ = handle_connection(stream, role, state, shutdown).await;
    });
}

fn spawn_tls_connection(
    connections: &mut JoinSet<()>,
    stream: tokio::net::TcpStream,
    role: EndpointRole,
    state: Arc<State>,
    connection_limit: Arc<Semaphore>,
    shutdown: watch::Receiver<bool>,
    tls: Arc<rustls::ServerConfig>,
) {
    let Ok(permit) = connection_limit.try_acquire_owned() else {
        state
            .metrics
            .rejected_requests
            .fetch_add(1, Ordering::Relaxed);
        return;
    };
    connections.spawn(async move {
        let _permit = permit;
        let _active = ActiveConnection::new(Arc::clone(&state.metrics));
        let acceptor = TlsAcceptor::from(tls);
        match timeout(state.config.idle_timeout, acceptor.accept(stream)).await {
            Ok(Ok(stream)) => {
                let _ = handle_connection(stream, role, state, shutdown).await;
            }
            Ok(Err(_)) | Err(_) => {
                state.metrics.auth_failures.fetch_add(1, Ordering::Relaxed); // Increment authentication failures metric when TLS accept fails.
            }
        }
    });
}

struct ActiveConnection(Arc<Metrics>); // Tracks active connections for metrics purposes.

impl ActiveConnection {
    fn new(metrics: Arc<Metrics>) -> Self {
        metrics.active_connections.fetch_add(1, Ordering::AcqRel); // Increment active connection count
        Self(metrics)
    }
}

impl Drop for ActiveConnection {
    fn drop(&mut self) {
        self.0.active_connections.fetch_sub(1, Ordering::AcqRel); // Decrement active connection count on drop
    }
}

async fn handle_connection<S>(
    stream: S,
    endpoint_role: EndpointRole,
    state: Arc<State>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), ServerError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(state.config.max_frame_bytes) // Set maximum frame size from config
        .new_codec();
    let mut framed = Framed::new(stream, codec);
    let frame = match timeout(state.config.idle_timeout, framed.next()).await {
        // Wait for initial frame within idle timeout
        Ok(Some(Ok(frame))) => frame,
        Ok(Some(Err(error))) => return Err(ServerError::Io(error)),
        Ok(None) | Err(_) => return Ok(()),
    };
    let hello = match decode_limited::<ClientHello>(&frame, state.config.max_frame_bytes) {
        // Decode and validate ClientHello message
        Ok(hello) => hello,
        Err(error) => {
            send_hello_rejection(&mut framed, ErrorCode::InvalidRequest, error.to_string()).await?; // Reject invalid ClientHello
            return Ok(());
        }
    };
    let role = match hello.validate() {
        Ok(role) => role,
        Err(error) => {
            send_hello_rejection(&mut framed, ErrorCode::Incompatible, error.to_string()).await?; // Reject incompatible ClientHello version
            return Ok(());
        }
    };
    if role != endpoint_role {
        // Ensure client role matches expected endpoint role
        state.metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
        send_hello_rejection(
            &mut framed,
            ErrorCode::Unauthorized,
            "role not authorized on this endpoint",
        )
        .await?;
        return Ok(());
    }
    let authorized = match endpoint_role {
        EndpointRole::Data => state.config.data_token.matches(&hello.auth_token),
        EndpointRole::Admin => state.config.admin_token.matches(&hello.auth_token),
    };
    if !authorized {
        // Check token authorization for specified role
        state.metrics.auth_failures.fetch_add(1, Ordering::Relaxed);
        send_hello_rejection(
            &mut framed,
            ErrorCode::Unauthenticated,
            "invalid credentials provided",
        )
        .await?;
        return Ok(());
    }
    let negotiated_max = (hello.max_frame_bytes as usize).min(state.config.max_frame_bytes); // Negotiate max frame size to use
    framed
        .send(encode_limited(
            &ServerHello::accepted(negotiated_max), // Send acceptance with negotiated max frame size
            state.config.max_frame_bytes,
        )?)
        .await?;

    let (sink, mut stream) = framed.split(); // Split framing into sink and stream for reading and writing
    let (responses, receiver) = mpsc::channel(state.config.response_queue_depth); // Channel for sending response messages
    let writer = tokio::spawn(response_writer(sink, receiver, negotiated_max)); // Spawn task for writing responses
    let connection_inflight = Arc::new(Semaphore::new(state.config.max_inflight_per_connection)); // Semaphore to limit concurrent inflight requests per connection
    let request_ids = Arc::new(Mutex::new(HashSet::new())); // Track active request IDs to avoid duplicates
    let mut requests = JoinSet::new(); // Set to manage concurrent request tasks

    loop {
        tokio::select! {
            changed = shutdown.changed() => { // Check for shutdown signal
                if changed.is_err() || *shutdown.borrow() { break; }
            }
            next = timeout(state.config.idle_timeout, stream.next()) => { // Wait for next request frame with idle timeout
                let frame = match next {
                    Ok(Some(Ok(frame))) => frame,
                    Ok(Some(Err(_))) | Ok(None) | Err(_) => break, // Close connection on read errors or timeout
                };
                let request_bytes = frame.len();
                let request = match decode_limited::<Request>(&frame, negotiated_max) {
                    Ok(request) => request,
                    Err(error) => { // Reject malformed requests
                        state.metrics.rejected_requests.fetch_add(1, Ordering::Relaxed);
                        let _ = responses.send(Response::error(
                            0,
                            ErrorCode::InvalidRequest,
                            error.to_string(),
                        )).await;
                        continue;
                    }
                };
                if let Err(error) = request.validate() {
                    state.metrics.rejected_requests.fetch_add(1, Ordering::Relaxed); // Reject requests failing validation
                    let _ = responses.send(Response::error(
                        request.request_id,
                        ErrorCode::InvalidRequest,
                        error.to_string(),
                    )).await;
                    continue;
                }
                if !role_allows(endpoint_role, request.operation.as_ref().expect("validata")) { // Check if operation is permitted for endpoint role
                    state.metrics.rejected_requests.fetch_add(1, Ordering::Relaxed);
                    let _ = responses.send(Response::error(
                        request.request_id,
                        ErrorCode::Unauthorized,
                        "operation not allowed on this endpoint",
                    )).await;
                    continue;
                }
                if request.deadline_unix_ms == 0 || deadline_expired(request.deadline_unix_ms) { // Reject requests with expired or missing deadlines
                    state.metrics.rejected_requests.fetch_add(1, Ordering::Relaxed);
                    let _ = responses.send(Response::error(
                        request.request_id,
                        ErrorCode::DeadlineExceeded,
                        "deadline expired before admission",
                    )).await;
                    continue;
                }
                // Enforce tenant quota limits for data endpoints
                let tenant_quota_permit = if endpoint_role == EndpointRole::Data {
                    if let Some(quota) = state.config.tenant_quotas.get(&request.tenant) {
                        match state.tenant_quota_runtime.try_acquire(
                            &request.tenant,
                            quota,
                            &request,
                            request_bytes,
                        ) {
                            Ok(permit) => Some(permit),
                            Err((code, message)) => {
                                state.metrics.rejected_requests.fetch_add(1, Ordering::Relaxed);
                                let response = if code == ErrorCode::Backpressure {
                                    Response::backpressure(
                                        request.request_id,
                                        message,
                                        retry_after_ms(state.config.backpressure_retry_after),
                                    )
                                } else {
                                    Response::error(request.request_id, code, message)
                                };
                                let _ = responses.send(response).await;
                                continue;
                            }
                        }
                    } else {
                        None
                    }
                } else {
                    None
                };
                // Acquire permit to enforce inflight request limit per connection
                let connection_permit = match Arc::clone(&connection_inflight).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        state.metrics.rejected_requests.fetch_add(1, Ordering::Relaxed);
                        let _ = responses.send(Response::backpressure(
                            request.request_id,
                            "connection inflight limit reached",
                            retry_after_ms(state.config.backpressure_retry_after),
                        )).await;
                        continue;
                    }
                };
                // Acquire permit to enforce global inflight request limit
                let global_permit = match Arc::clone(&state.global_inflight).try_acquire_owned() {
                    Ok(permit) => permit,
                    Err(_) => {
                        state.metrics.rejected_requests.fetch_add(1, Ordering::Relaxed);
                        let _ = responses.send(Response::backpressure(
                            request.request_id,
                            "global inflight limit reached",
                            retry_after_ms(state.config.backpressure_retry_after),
                        )).await;
                        continue;
                    }
                };
                {
                    // Ensure request ID is unique among inflight requests
                    let mut ids = request_ids.lock().await;
                    if !ids.insert(request.request_id) {
                        drop(ids);
                        let _ = responses.send(Response::error(
                            request.request_id,
                            ErrorCode::InvalidRequest,
                            "duplicate request_id in flight",
                        )).await;
                        continue;
                    }
                }
                // Spawn a task to process the request asynchronously
                let task_state = Arc::clone(&state);
                let task_responses = responses.clone();
                let task_ids = Arc::clone(&request_ids);
                requests.spawn(async move {
                    // Holds permits to enforce limits until request completes
                    let _tenant_quota_permit = tenant_quota_permit;
                    let _connection_permit = connection_permit;
                    let _global_permit = global_permit;
                    let _inflight = InflightRequest::new(Arc::clone(&task_state.metrics));
                    let request_id = request.request_id;
                    let dispatch_state = Arc::clone(&task_state);
                    // Run dispatch logic in blocking thread
                    let result = tokio::task::spawn_blocking(move || {
                        dispatch(&dispatch_state, request)
                    }).await;
                    let (response, should_shutdown) = match result {
                        Ok(result) => result,
                        Err(error) => (
                            Response::error(request_id, ErrorCode::Internal, error.to_string()),
                            false,
                        ),
                    };
                    // Send response back
                    let _ = task_responses.send(response).await;
                    // Remove request ID from active set
                    task_ids.lock().await.remove(&request_id);
                    if should_shutdown {
                        task_state.shutdown.send_replace(true); // Signal server shutdown if requested
                    }
                });
            }
        }
    }
    // Wait for all spawned request tasks to complete
    while requests.join_next().await.is_some() {}
    drop(responses); // Close the response channel
    // Await response writer task and propagate errors
    writer
        .await
        .map_err(|error| ServerError::Task(error.to_string()))??;
    Ok(())
}

async fn send_hello_rejection<S>(
    framed: &mut Framed<S, LengthDelimitedCodec>,
    code: ErrorCode,
    message: impl Into<String>,
) -> Result<(), ServerError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let maximum = framed.codec().max_frame_length();
    let hello = ServerHello::rejected(code, message);
    framed.send(encode_limited(&hello, maximum)?).await?;
    Ok(())
}

async fn response_writer<S>(
    mut sink: SplitSink<Framed<S, LengthDelimitedCodec>, bytes::Bytes>,
    mut responses: mpsc::Receiver<Response>,
    maximum: usize,
) -> Result<(), ServerError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    while let Some(response) = responses.recv().await {
        let request_id = response.request_id;
        let bytes = match encode_limited(&response, maximum) {
            Ok(bytes) => bytes,
            Err(error) => encode_limited(
                &Response::error(request_id, ErrorCode::ResourceLimit, error.to_string()),
                maximum,
            )?,
        };
        sink.send(bytes).await?;
    }
    sink.close().await?;
    Ok(())
}

struct InflightRequest(Arc<Metrics>);

impl InflightRequest {
    fn new(metrics: Arc<Metrics>) -> Self {
        metrics.total_requests.fetch_add(1, Ordering::AcqRel);
        metrics.inflight_requests.fetch_add(1, Ordering::AcqRel);
        Self(metrics)
    }
}

impl Drop for InflightRequest {
    fn drop(&mut self) {
        self.0.inflight_requests.fetch_sub(1, Ordering::AcqRel);
    }
}

fn role_allows(role: EndpointRole, operation: &request::Operation) -> bool {
    match role {
        EndpointRole::Data => operation.is_data(),
        EndpointRole::Admin => !operation.is_data(),
    }
}

fn deadline_expired(deadline_unix_ms: u64) -> bool {
    now_unix_ms().is_none_or(|now| now >= deadline_unix_ms)
}

fn now_unix_ms() -> Option<u64> {
    let duration = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    u64::try_from(duration.as_millis()).ok()
}

#[derive(Clone, Copy)]
struct AdminAudit {
    operation: &'static str,
    target_hash: [u8; 32],
}

fn admin_audit(
    operation: &request::Operation,
    tenant: &[u8],
    namespace: &[u8],
) -> Option<AdminAudit> {
    let (name, target): (&'static str, &[u8]) = match operation {
        request::Operation::Compact(_) => ("compact", b"storage"),
        request::Operation::Shutdown(_) => ("shutdown", b"server"),
        request::Operation::Expire(_) => ("expire", b"ttl"),
        request::Operation::CreateSurface(operation) => (
            "create_surface",
            operation
                .definition
                .as_ref()
                .map_or(b"missing".as_slice(), |value| value.id.as_bytes()),
        ),
        request::Operation::BuildSurface(operation) => {
            ("build_surface", operation.projection_id.as_bytes())
        }
        request::Operation::RebuildSurface(operation) => {
            ("rebuild_surface", operation.projection_id.as_bytes())
        }
        request::Operation::ConfigureCompression(operation) => {
            ("configure_compression", operation.collection.as_slice())
        }
        request::Operation::TrainDictionary(operation) => {
            ("train_dictionary", operation.collection.as_slice())
        }
        request::Operation::Backup(operation) => ("backup", operation.name.as_bytes()),
        _ => return None,
    };
    let mut hasher = blake3::Hasher::new();
    for part in [tenant, namespace, name.as_bytes(), target] {
        hasher.update(&(part.len() as u64).to_be_bytes());
        hasher.update(part);
    }
    Some(AdminAudit {
        operation: name,
        target_hash: *hasher.finalize().as_bytes(),
    })
}

fn dispatch(state: &State, request: Request) -> (Response, bool) {
    let request_id = request.request_id;
    let durability = match WireDurability::try_from(request.durability) {
        Ok(durability) => Durability::from(durability),
        Err(_) => {
            return (
                Response::error(request_id, ErrorCode::InvalidRequest, "unknown durability"),
                false,
            );
        }
    };
    let operation = request.operation.expect("validated request");
    let audit = admin_audit(&operation, &request.tenant, &request.namespace);
    if let Some(audit) = audit
        && let Err(error) = state.engine.append_audit_event(
            request_id,
            &state.config.admin_principal,
            audit.operation,
            AuditOutcome::Attempted,
            Some(audit.target_hash),
            None,
        )
    {
        return (error_response(state, request_id, &error), false);
    }
    let should_shutdown = matches!(&operation, request::Operation::Shutdown(_));
    let result = match operation {
        request::Operation::Put(operation) => dispatch_put(
            &state.engine,
            request.tenant,
            request.namespace,
            operation,
            durability,
        )
        .map(|receipt| {
            let mut response = Response::ok(request_id);
            response.receipts.push(receipt.into());
            response
        }),
        request::Operation::Get(operation) => operation
            .key
            .ok_or_else(|| AproError::InvalidInput("missing Get key".into()))
            .and_then(|key| {
                identity_from_wire(request.tenant, request.namespace, key)
                    .map_err(|error| AproError::InvalidInput(error.to_string()))
            })
            .and_then(|identity| state.engine.get(&identity))
            .map(|record| {
                let mut response = Response::ok(request_id);
                response.record = record.as_ref().map(WireRecord::from);
                response
            }),
        request::Operation::Delete(operation) => dispatch_delete(
            &state.engine,
            request.tenant,
            request.namespace,
            operation,
            durability,
        )
        .map(|receipt| {
            let mut response = Response::ok(request_id);
            response.receipts.push(receipt.into());
            response
        }),
        request::Operation::AtomicBatch(operation) => operation
            .mutations
            .into_iter()
            .map(|mutation| {
                wire_mutation(request.tenant.clone(), request.namespace.clone(), mutation)
            })
            .collect::<aprodb_types::Result<Vec<_>>>()
            .and_then(|mutations| state.engine.atomic_batch(mutations, durability))
            .map(|receipts| {
                let mut response = Response::ok(request_id);
                response.receipts = receipts.into_iter().map(Into::into).collect();
                response
            }),
        request::Operation::Sync(_) => state.engine.sync().map(|()| Response::ok(request_id)),
        request::Operation::Health(_) => state.engine.stats().map(|_| {
            let mut response = Response::ok(request_id);
            response.healthy = true;
            response
        }),
        request::Operation::Stats(_) => state.metrics.snapshot(&state.engine).map(|stats| {
            let mut response = Response::ok(request_id);
            response.stats = Some(stats);
            response
        }),
        request::Operation::Verify(_) => state.engine.verify().map(|_| Response::ok(request_id)),
        request::Operation::Compact(_) => state
            .engine
            .major_compact()
            .map(|_| Response::ok(request_id)),
        request::Operation::Shutdown(_) => Ok(Response::ok(request_id)),
        request::Operation::ExplainPlacement(operation) => operation
            .key
            .ok_or_else(|| AproError::InvalidInput("ExplainPlacement key missing".into()))
            .and_then(|key| {
                identity_from_wire(request.tenant, request.namespace, key)
                    .map_err(|error| AproError::InvalidInput(error.to_string()))
            })
            .and_then(|identity| {
                now_unix_ms()
                    .ok_or_else(|| AproError::InvalidInput("UTC clock unavailable".into()))
                    .and_then(|now| state.engine.explain_placement(&identity, now))
            })
            .map(|placement| {
                let mut response = Response::ok(request_id);
                response.placement = Some(WirePlacement {
                    canonical_version: Some(placement.canonical_version.into()),
                    radial_score_millis: u32::from(placement.radial_score_millis),
                    freshness_millis: u32::from(placement.freshness_millis),
                    urgency_millis: u32::from(placement.urgency_millis),
                    current_layer: radial_layer_name(placement.current_layer).into(),
                    recommended_layer: radial_layer_name(placement.recommended_layer).into(),
                    storage_class: placement.storage_class,
                    pinned: placement.pinned,
                    object_cache_resident: placement.object_cache_resident,
                    physical_tiering_supported: placement.physical_tiering_supported,
                    reasons: placement.reasons,
                });
                response
            }),
        request::Operation::CacheStats(_) => {
            let stats = state.engine.cache_stats();
            Ok({
                let mut response = Response::ok(request_id);
                response.cache_stats = Some(WireCacheStats {
                    metadata: Some(wire_cache_metrics(stats.metadata)),
                    objects: Some(wire_cache_metrics(stats.objects)),
                    negative: Some(wire_cache_metrics(stats.negative)),
                    compressed: Some(wire_cache_metrics(stats.compressed)),
                });
                response
            })
        }
        request::Operation::Expire(operation) => usize::try_from(operation.limit)
            .map_err(|_| AproError::ResourceLimit("Expire limit exceeds usize".into()))
            .and_then(|limit| state.engine.expire_due(limit, durability))
            .map(|report| {
                let mut response = Response::ok(request_id);
                response.expiration = Some(ExpirationStats {
                    scanned: u64::try_from(report.scanned).unwrap_or(u64::MAX),
                    expired: u64::try_from(report.expired).unwrap_or(u64::MAX),
                    stale_entries: u64::try_from(report.stale_entries).unwrap_or(u64::MAX),
                });
                response
            }),
        request::Operation::Append(operation) => dispatch_append(
            &state.engine,
            request.tenant,
            request.namespace,
            operation,
            durability,
        )
        .map(|receipt| {
            let mut response = Response::ok(request_id);
            response.receipts.push(receipt.into());
            response
        }),
        request::Operation::Claim(operation) => operation
            .scope
            .ok_or_else(|| AproError::InvalidInput("missing Claim scope".into()))
            .and_then(|scope| {
                Ok(ClaimRequest {
                    scope: WorkflowScope::new(
                        request.tenant,
                        request.namespace,
                        scope.collection,
                        scope.partition,
                    )?,
                    max_records: usize::try_from(operation.max_records).map_err(|_| {
                        AproError::ResourceLimit("Claim max_records exceeds usize".into())
                    })?,
                    lease_duration: Duration::from_millis(operation.lease_duration_ms),
                    idempotency_key_hash: optional_hash(operation.idempotency_key_hash)?,
                    durability,
                })
            })
            .and_then(|claim| state.engine.claim(claim))
            .map(|claimed| {
                let mut response = Response::ok(request_id);
                response.server_time_unix_ms = claimed.first().map_or_else(
                    || now_unix_ms().unwrap_or(0),
                    |item| item.server_time_unix_ms,
                );
                for item in claimed {
                    response.receipts.push(item.receipt.into());
                    response
                        .claimed_records
                        .push(WireRecord::from(&item.record));
                }
                response
            }),
        request::Operation::Heartbeat(operation) => dispatch_lease_operation(
            &state.engine,
            request_id,
            request.tenant,
            request.namespace,
            operation,
            durability,
            LeaseAction::Heartbeat,
        ),
        request::Operation::Complete(operation) => dispatch_lease_operation(
            &state.engine,
            request_id,
            request.tenant,
            request.namespace,
            operation,
            durability,
            LeaseAction::Complete,
        ),
        request::Operation::Fail(operation) => operation
            .lease
            .ok_or_else(|| AproError::InvalidInput("missing Fail lease".into()))
            .and_then(|lease| {
                dispatch_lease_operation(
                    &state.engine,
                    request_id,
                    request.tenant,
                    request.namespace,
                    lease,
                    durability,
                    LeaseAction::Fail(operation.permanent),
                )
            }),
        request::Operation::Publish(operation) => operation
            .key
            .ok_or_else(|| AproError::InvalidInput("missing Publish key".into()))
            .and_then(|key| {
                identity_from_wire(request.tenant, request.namespace, key)
                    .map_err(|error| AproError::InvalidInput(error.to_string()))
            })
            .and_then(|identity| {
                state.engine.publish(
                    &identity,
                    optional_hash(operation.idempotency_key_hash)?,
                    durability,
                )
            })
            .map(|result| workflow_response(request_id, result)),
        request::Operation::SubscribeChanges(operation) => usize::try_from(operation.limit)
            .map_err(|_| AproError::ResourceLimit("change-stream limit exceeds usize".into()))
            .and_then(|limit| {
                state
                    .engine
                    .changes(operation.shard, operation.after_sequence, limit)
            })
            .and_then(|events| {
                let watermark = events
                    .last()
                    .map_or(operation.after_sequence, |event| event.version.sequence);
                let encoded = events
                    .into_iter()
                    .filter(|event| {
                        event.tenant == request.tenant
                            && event.namespace == request.namespace
                            && event.collection == operation.collection
                    })
                    .map(|event| {
                        aprodb_types::encode_logical(aprodb_types::LogicalFrameKind::Change, &event)
                    })
                    .collect::<aprodb_types::Result<Vec<_>>>()?;
                let mut response = Response::ok(request_id);
                response.change_events = encoded;
                response.change_watermark = watermark;
                Ok(response)
            }),
        request::Operation::GetSurface(operation) => state
            .engine
            .surface_definition(&operation.projection_id)
            .and_then(|definition| {
                let definition = definition
                    .ok_or_else(|| AproError::InvalidInput("surface not found".into()))?;
                if definition.source_tenant != request.tenant
                    || definition.source_namespace != request.namespace
                {
                    return Err(AproError::InvalidInput(
                        "surface outside requested scope".into(),
                    ));
                }
                state.engine.read_surface(&operation.projection_id)
            })
            .map(|surface| {
                let mut response = Response::ok(request_id);
                response.surface = surface.map(wire_surface_generation);
                response
            }),
        request::Operation::CreateSurface(operation) => operation
            .definition
            .ok_or_else(|| AproError::InvalidInput("surface definition missing".into()))
            .and_then(engine_surface_definition)
            .and_then(|definition| state.engine.create_surface(definition))
            .map(|()| Response::ok(request_id)),
        request::Operation::BuildSurface(operation) => {
            dispatch_surface_build(&state.engine, operation, durability, false).map(|build| {
                let mut response = Response::ok(request_id);
                response.surface_build = Some(build);
                response
            })
        }
        request::Operation::RebuildSurface(operation) => {
            dispatch_surface_build(&state.engine, operation, durability, true).map(|build| {
                let mut response = Response::ok(request_id);
                response.surface_build = Some(build);
                response
            })
        }
        request::Operation::CompressionStats(_) => {
            let stats = state.engine.compression_stats();
            Ok({
                let mut response = Response::ok(request_id);
                response.compression_stats = Some(WireCompressionStats {
                    logical_bytes: stats.logical_bytes,
                    stored_bytes: stats.stored_payload_bytes,
                    raw_records: stats.raw_payloads,
                    zstd_records: stats.zstandard_payloads,
                    dictionary_records: stats.dictionary_payloads,
                    incompressible_fallbacks: stats.adaptive_fallbacks,
                    content_type_skips: stats.skipped_content_types,
                    compress_micros: stats.compression_micros,
                    decompress_micros: stats.decompression_micros,
                    codec_failures: stats.failures,
                    channels: u64::try_from(stats.channels).unwrap_or(u64::MAX),
                    scratch_budget_bytes: u64::try_from(stats.scratch_budget_bytes)
                        .unwrap_or(u64::MAX),
                    scratch_in_use_bytes: u64::try_from(stats.scratch_inflight_bytes)
                        .unwrap_or(u64::MAX),
                });
                response
            })
        }
        request::Operation::CompressionPolicy(operation) => {
            collection_identity(request.tenant, request.namespace, operation.collection)
                .and_then(|identity| state.engine.compression_policy(&identity))
                .and_then(wire_compression_policy)
                .map(|policy| {
                    let mut response = Response::ok(request_id);
                    response.compression_policy = Some(policy);
                    response
                })
        }
        request::Operation::ConfigureCompression(operation) => {
            let policy = operation
                .policy
                .ok_or_else(|| AproError::InvalidInput("compression policy missing".into()))
                .and_then(compression_policy_from_wire);
            collection_identity(request.tenant, request.namespace, operation.collection)
                .and_then(|identity| policy.map(|policy| (identity, policy)))
                .and_then(|(identity, policy)| {
                    state.engine.configure_compression_policy(&identity, policy)
                })
                .map(|()| Response::ok(request_id))
        }
        request::Operation::TrainDictionary(operation) => {
            let aprodb_proto::TrainDictionaryOperation {
                collection,
                schema,
                training_samples,
                validation_samples,
                max_dictionary_bytes,
                minimum_validation_gain_bytes,
            } = operation;
            let training = training_samples
                .into_iter()
                .map(Payload::try_from)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| AproError::InvalidInput(error.to_string()));
            let validation = validation_samples
                .into_iter()
                .map(Payload::try_from)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|error| AproError::InvalidInput(error.to_string()));
            let max_dictionary_bytes = usize::try_from(max_dictionary_bytes)
                .map_err(|_| AproError::ResourceLimit("dictionary size exceeds usize".into()));
            let minimum_validation_gain_bytes = usize::try_from(minimum_validation_gain_bytes)
                .map_err(|_| AproError::ResourceLimit("dictionary gain exceeds usize".into()));
            collection_identity(request.tenant, request.namespace, collection)
                .and_then(|identity| {
                    Ok((
                        identity,
                        training?,
                        validation?,
                        max_dictionary_bytes?,
                        minimum_validation_gain_bytes?,
                    ))
                })
                .and_then(
                    |(identity, training, validation, max_bytes, minimum_gain)| {
                        state.engine.train_and_activate_dictionary(
                            &identity,
                            schema,
                            &training,
                            &validation,
                            max_bytes,
                            minimum_gain,
                        )
                    },
                )
                .map(|dictionary| {
                    let mut response = Response::ok(request_id);
                    response.compression_dictionary = Some(WireCompressionDictionary {
                        id: dictionary.id,
                        schema: dictionary.schema,
                        bytes: u64::try_from(dictionary.bytes.len()).unwrap_or(u64::MAX),
                        checksum: dictionary.checksum,
                        created_at_unix_ms: dictionary.created_at_unix_ms,
                        validation_raw_bytes: dictionary.validation_raw_bytes,
                        validation_without_dictionary_bytes: dictionary
                            .validation_without_dictionary_bytes,
                        validation_with_dictionary_bytes: dictionary
                            .validation_with_dictionary_bytes,
                    });
                    response
                })
        }
        request::Operation::ComputeStats(_) => {
            let metrics = state.engine.compute_metrics();
            let accelerator = state.engine.accelerator_stats().unwrap_or_default();
            Ok({
                let mut response = Response::ok(request_id);
                response.compute_stats = Some(WireComputeStats {
                    requests: metrics.requests,
                    cpu_runs: metrics.cpu_runs,
                    accelerator_runs: metrics.accelerator_runs,
                    cpu_fallbacks: metrics.cpu_fallbacks,
                    queue_rejections: metrics.queue_rejections,
                    accelerator_failures: metrics.accelerator_failures,
                    request_timeouts: metrics.request_timeouts,
                    circuit_open_rejections: metrics.circuit_open_rejections,
                    micro_batches: metrics.micro_batches,
                    micro_batched_requests: metrics.micro_batched_requests,
                    inflight_bytes: u64::try_from(metrics.inflight_bytes).unwrap_or(u64::MAX),
                    peak_inflight_bytes: u64::try_from(metrics.peak_inflight_bytes)
                        .unwrap_or(u64::MAX),
                    accelerator_name: state.engine.accelerator_name(),
                    vram_budget_bytes: u64::try_from(accelerator.vram_budget_bytes)
                        .unwrap_or(u64::MAX),
                    vram_resident_bytes: u64::try_from(accelerator.vram_resident_bytes)
                        .unwrap_or(u64::MAX),
                    vram_entries: u64::try_from(accelerator.vram_entries).unwrap_or(u64::MAX),
                    vram_hits: accelerator.vram_hits,
                    vram_misses: accelerator.vram_misses,
                    vram_evictions: accelerator.vram_evictions,
                    upload_bytes: accelerator.upload_bytes,
                    readback_bytes: accelerator.readback_bytes,
                    transfer_micros: accelerator.transfer_micros,
                    kernel_micros: accelerator.kernel_micros,
                    device_resets: accelerator.device_resets,
                });
                response
            })
        }
        request::Operation::AuditList(operation) => usize::try_from(operation.limit)
            .map_err(|_| AproError::ResourceLimit("AuditList limit exceeds usize".into()))
            .and_then(|limit| {
                state.engine.read_audit(
                    operation
                        .after_sequence
                        .map(|sequence| AuditCursor { sequence }),
                    limit,
                )
            })
            .map(|page| {
                let mut response = Response::ok(request_id);
                response.audit_events = page
                    .events
                    .into_iter()
                    .map(|event| WireAuditEvent {
                        sequence: event.sequence,
                        event_id: event.event_id.to_vec(),
                        at_unix_ms: event.at_unix_ms,
                        request_id: event.request_id,
                        principal: event.principal,
                        operation: event.operation,
                        outcome: audit_outcome_name(event.outcome).into(),
                        target_hash: event.target_hash.map(|value| value.to_vec()),
                        error_class: event.error_class,
                    })
                    .collect();
                response.audit_next_sequence = page.next.map(|cursor| cursor.sequence);
                response
            }),
        request::Operation::Backup(operation) => state
            .config
            .backup_root
            .as_ref()
            .ok_or_else(|| AproError::Unsupported("online backup not configured".into()))
            .and_then(|root| state.engine.create_backup(root.join(&operation.name)))
            .map(|backup| {
                let mut response = Response::ok(request_id);
                response.backup = Some(WireBackupInfo {
                    name: operation.name,
                    catalog_generation: backup.manifest.catalog_generation,
                    files: u64::try_from(backup.manifest.files.len()).unwrap_or(u64::MAX),
                    bytes: backup.manifest.files.iter().map(|file| file.bytes).sum(),
                    logical_bytes: backup.manifest.logical_bytes,
                    encrypted: backup.manifest.encrypted,
                });
                response
            }),
        request::Operation::VectorSearch(operation) => {
            let metric = match WireVectorMetric::try_from(operation.metric) {
                Ok(WireVectorMetric::Dot) => Ok(VectorMetric::Dot),
                Ok(WireVectorMetric::Cosine) => Ok(VectorMetric::Cosine),
                Err(_) => Err(AproError::InvalidInput("unknown vector metric".into())),
            };
            let preference = match WireComputePreference::try_from(operation.preference) {
                Ok(WireComputePreference::Cpu) => Ok(ComputePreference::Cpu),
                Ok(WireComputePreference::Accelerator) => Ok(ComputePreference::Accelerator),
                Ok(WireComputePreference::Auto) => Ok(ComputePreference::Auto),
                Err(_) => Err(AproError::InvalidInput("unknown compute preference".into())),
            };
            usize::try_from(operation.limit)
                .map_err(|_| AproError::ResourceLimit("VectorSearch limit exceeds usize".into()))
                .and_then(|limit| {
                    Ok((
                        limit,
                        usize::try_from(operation.max_scan_records).map_err(|_| {
                            AproError::ResourceLimit("max_scan_records exceeds usize".into())
                        })?,
                        metric?,
                        preference?,
                    ))
                })
                .and_then(|(limit, max_scan_records, metric, preference)| {
                    state.engine.vector_exact(VectorSearchRequest {
                        tenant: request.tenant,
                        namespace: request.namespace,
                        collection: operation.collection,
                        query: operation.query,
                        metric,
                        limit,
                        max_scan_records,
                        preference,
                    })
                })
                .map(|result| {
                    let mut response = Response::ok(request_id);
                    response.vector_search = Some(WireVectorSearchResult {
                        hits: result
                            .hits
                            .into_iter()
                            .map(|hit| WireVectorHit {
                                partition: hit.identity.partition,
                                key: hit.identity.key,
                                version: Some(hit.version.into()),
                                score: hit.score,
                            })
                            .collect(),
                        scanned_records: u64::try_from(result.scanned_records).unwrap_or(u64::MAX),
                        vector_candidates: u64::try_from(result.vector_candidates)
                            .unwrap_or(u64::MAX),
                        execution: wire_compute_execution(result.execution) as i32,
                        accelerator: result.accelerator,
                        estimate: Some(wire_cost_estimate(result.estimate)),
                        fallback_reason: result.fallback_reason,
                    });
                    response
                })
        }
    };
    if let Some(audit) = audit {
        let (outcome, error_class) = match &result {
            Ok(_) => (AuditOutcome::Succeeded, None),
            Err(error) => (AuditOutcome::Failed, Some(error_class(error))),
        };
        if let Err(error) = state.engine.append_audit_event(
            request_id,
            &state.config.admin_principal,
            audit.operation,
            outcome,
            Some(audit.target_hash),
            error_class,
        ) {
            return (error_response(state, request_id, &error), false);
        }
    }
    match result {
        Ok(response) => (response, should_shutdown),
        Err(error) => (error_response(state, request_id, &error), false),
    }
}

fn dispatch_put(
    engine: &Engine,
    tenant: Vec<u8>,
    namespace: Vec<u8>,
    operation: PutOperation,
    durability: Durability,
) -> aprodb_types::Result<aprodb_types::MutationReceipt> {
    engine.put_with_durability(
        put_request_from_wire(tenant, namespace, operation)?,
        durability,
    )
}

fn collection_identity(
    tenant: Vec<u8>,
    namespace: Vec<u8>,
    collection: Vec<u8>,
) -> aprodb_types::Result<RecordIdentity> {
    RecordIdentity::new(tenant, namespace, collection, b"_".to_vec(), b"_".to_vec())
}

fn wire_compression_policy(
    policy: CompressionPolicy,
) -> aprodb_types::Result<WireCompressionPolicy> {
    Ok(WireCompressionPolicy {
        surface: Some(wire_compression_tier(policy.surface)?),
        hot: Some(wire_compression_tier(policy.hot)?),
        warm: Some(wire_compression_tier(policy.warm)?),
        cold: Some(wire_compression_tier(policy.cold)?),
        archive: Some(wire_compression_tier(policy.archive)?),
        skip_content_type_prefixes: policy.skip_content_type_prefixes,
    })
}

fn wire_compression_tier(
    policy: CompressionTierPolicy,
) -> aprodb_types::Result<WireCompressionTierPolicy> {
    Ok(WireCompressionTierPolicy {
        mode: match policy.mode {
            CompressionMode::Raw => WireCompressionMode::Raw,
            CompressionMode::AdaptiveZstandard => WireCompressionMode::AdaptiveZstandard,
        } as i32,
        zstd_level: policy.zstd_level,
        min_input_bytes: u64::try_from(policy.min_input_bytes)
            .map_err(|_| AproError::ResourceLimit("compression min_input exceeds u64".into()))?,
        min_savings_bytes: u64::try_from(policy.min_savings_bytes)
            .map_err(|_| AproError::ResourceLimit("compression min_savings exceeds u64".into()))?,
        dictionary_id: policy.dictionary_id,
    })
}

fn compression_policy_from_wire(
    policy: WireCompressionPolicy,
) -> aprodb_types::Result<CompressionPolicy> {
    Ok(CompressionPolicy {
        surface: compression_tier_from_wire(policy.surface, "surface")?,
        hot: compression_tier_from_wire(policy.hot, "hot")?,
        warm: compression_tier_from_wire(policy.warm, "warm")?,
        cold: compression_tier_from_wire(policy.cold, "cold")?,
        archive: compression_tier_from_wire(policy.archive, "archive")?,
        skip_content_type_prefixes: policy.skip_content_type_prefixes,
    })
}

fn compression_tier_from_wire(
    policy: Option<WireCompressionTierPolicy>,
    name: &str,
) -> aprodb_types::Result<CompressionTierPolicy> {
    let policy = policy
        .ok_or_else(|| AproError::InvalidInput(format!("compression tier {name} missing")))?;
    Ok(CompressionTierPolicy {
        mode: match WireCompressionMode::try_from(policy.mode) {
            Ok(WireCompressionMode::Raw) => CompressionMode::Raw,
            Ok(WireCompressionMode::AdaptiveZstandard) => CompressionMode::AdaptiveZstandard,
            Err(_) => {
                return Err(AproError::InvalidInput(format!(
                    "unknown compression mode {name}"
                )));
            }
        },
        zstd_level: policy.zstd_level,
        min_input_bytes: usize::try_from(policy.min_input_bytes)
            .map_err(|_| AproError::ResourceLimit(format!("min_input {name} exceeds usize")))?,
        min_savings_bytes: usize::try_from(policy.min_savings_bytes)
            .map_err(|_| AproError::ResourceLimit(format!("min_savings {name} exceeds usize")))?,
        dictionary_id: policy.dictionary_id,
    })
}

const fn wire_compute_execution(execution: ComputeExecution) -> WireComputeExecution {
    match execution {
        ComputeExecution::Cpu => WireComputeExecution::Cpu,
        ComputeExecution::Accelerator => WireComputeExecution::Accelerator,
        ComputeExecution::CpuFallback => WireComputeExecution::CpuFallback,
    }
}

const fn wire_cost_estimate(estimate: CostEstimate) -> WireCostEstimate {
    WireCostEstimate {
        transfer_in_micros: estimate.transfer_in_micros,
        queue_wait_micros: estimate.queue_wait_micros,
        launch_micros: estimate.launch_micros,
        accelerator_compute_micros: estimate.accelerator_compute_micros,
        transfer_out_micros: estimate.transfer_out_micros,
        synchronization_micros: estimate.synchronization_micros,
        risk_margin_micros: estimate.risk_margin_micros,
        accelerator_total_micros: estimate.accelerator_total_micros,
        cpu_compute_micros: estimate.cpu_compute_micros,
        vram_cache_hit: estimate.vram_cache_hit,
    }
}

fn dispatch_append(
    engine: &Engine,
    tenant: Vec<u8>,
    namespace: Vec<u8>,
    operation: PutOperation,
    durability: Durability,
) -> aprodb_types::Result<aprodb_types::MutationReceipt> {
    engine.append(
        put_request_from_wire(tenant, namespace, operation)?,
        durability,
    )
}

fn put_request_from_wire(
    tenant: Vec<u8>,
    namespace: Vec<u8>,
    operation: PutOperation,
) -> aprodb_types::Result<PutRequest> {
    let identity = identity_from_wire(
        tenant,
        namespace,
        operation
            .key
            .ok_or_else(|| AproError::InvalidInput("missing Put key".into()))?,
    )
    .map_err(|error| AproError::InvalidInput(error.to_string()))?;
    let payload = operation
        .payload
        .ok_or_else(|| AproError::InvalidInput("missing Put payload".into()))?
        .try_into()
        .map_err(|error: aprodb_proto::ProtocolError| AproError::InvalidInput(error.to_string()))?;
    let mut put = PutRequest::new(identity, payload);
    put.content_type = operation.content_type;
    put.metadata = operation.metadata;
    put.expires_at_unix_ms = operation.expires_at_unix_ms;
    put.expected = engine_expected(operation.expected)?;
    put.delta = (!operation.delta.is_empty()).then_some(operation.delta);
    put.idempotency_key_hash = optional_hash(operation.idempotency_key_hash)?;
    Ok(put)
}

fn dispatch_delete(
    engine: &Engine,
    tenant: Vec<u8>,
    namespace: Vec<u8>,
    operation: DeleteOperation,
    durability: Durability,
) -> aprodb_types::Result<aprodb_types::MutationReceipt> {
    let identity = identity_from_wire(
        tenant,
        namespace,
        operation
            .key
            .ok_or_else(|| AproError::InvalidInput("missing Delete key".into()))?,
    )
    .map_err(|error| AproError::InvalidInput(error.to_string()))?;
    let delete = DeleteRequest {
        identity,
        expected: engine_expected(operation.expected)?,
        idempotency_key_hash: optional_hash(operation.idempotency_key_hash)?,
        delta: (!operation.delta.is_empty()).then_some(operation.delta),
    };
    engine
        .atomic_batch(vec![EngineMutation::Delete(delete)], durability)
        .map(|mut receipts| receipts.remove(0))
}

fn wire_mutation(
    tenant: Vec<u8>,
    namespace: Vec<u8>,
    mutation: AtomicMutation,
) -> aprodb_types::Result<EngineMutation> {
    match mutation.kind {
        Some(aprodb_proto::atomic_mutation::Kind::Put(operation)) => {
            let identity = identity_from_wire(
                tenant,
                namespace,
                operation
                    .key
                    .ok_or_else(|| AproError::InvalidInput("missing Put key".into()))?,
            )
            .map_err(|error| AproError::InvalidInput(error.to_string()))?;
            let payload = operation
                .payload
                .ok_or_else(|| AproError::InvalidInput("missing Put payload".into()))?
                .try_into()
                .map_err(|error: aprodb_proto::ProtocolError| {
                    AproError::InvalidInput(error.to_string())
                })?;
            let mut put = PutRequest::new(identity, payload);
            put.content_type = operation.content_type;
            put.metadata = operation.metadata;
            put.expires_at_unix_ms = operation.expires_at_unix_ms;
            put.expected = engine_expected(operation.expected)?;
            put.delta = (!operation.delta.is_empty()).then_some(operation.delta);
            put.idempotency_key_hash = optional_hash(operation.idempotency_key_hash)?;
            Ok(EngineMutation::Put(put))
        }
        Some(aprodb_proto::atomic_mutation::Kind::Delete(operation)) => {
            let identity = identity_from_wire(
                tenant,
                namespace,
                operation
                    .key
                    .ok_or_else(|| AproError::InvalidInput("missing Delete key".into()))?,
            )
            .map_err(|error| AproError::InvalidInput(error.to_string()))?;
            Ok(EngineMutation::Delete(DeleteRequest {
                identity,
                expected: engine_expected(operation.expected)?,
                idempotency_key_hash: optional_hash(operation.idempotency_key_hash)?,
                delta: (!operation.delta.is_empty()).then_some(operation.delta),
            }))
        }
        None => Err(AproError::InvalidInput("empty batch mutation".into())),
    }
}

fn optional_hash(bytes: Vec<u8>) -> aprodb_types::Result<Option<[u8; 32]>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    bytes
        .try_into()
        .map(Some)
        .map_err(|_| AproError::InvalidInput("idempotency hash not 32 bytes long".into()))
}

#[derive(Clone, Copy)]
enum LeaseAction {
    Heartbeat,
    Complete,
    Fail(bool),
}

fn dispatch_lease_operation(
    engine: &Engine,
    request_id: u64,
    tenant: Vec<u8>,
    namespace: Vec<u8>,
    operation: LeaseOperation,
    durability: Durability,
    action: LeaseAction,
) -> aprodb_types::Result<Response> {
    let identity = identity_from_wire(
        tenant,
        namespace,
        operation
            .key
            .ok_or_else(|| AproError::InvalidInput("missing lease key".into()))?,
    )
    .map_err(|error| AproError::InvalidInput(error.to_string()))?;
    let lease = LeaseProof {
        lease_id: operation
            .lease_id
            .try_into()
            .map_err(|_| AproError::InvalidInput("lease ID not 16 bytes long".into()))?,
        fencing_token: operation.fencing_token,
    };
    let idempotency = optional_hash(operation.idempotency_key_hash)?;
    let result = match action {
        LeaseAction::Heartbeat => engine.heartbeat(
            &identity,
            lease,
            Duration::from_millis(operation.extension_ms),
            idempotency,
            durability,
        ),
        LeaseAction::Complete => engine.complete(&identity, lease, idempotency, durability),
        LeaseAction::Fail(permanent) => {
            engine.fail(&identity, lease, permanent, idempotency, durability)
        }
    }?;
    Ok(workflow_response(request_id, result))
}

fn workflow_response(request_id: u64, result: aprodb_engine::WorkflowMutationResult) -> Response {
    let mut response = Response::ok(request_id);
    response.record = Some(WireRecord::from(&result.record));
    response.receipts.push(result.receipt.into());
    response
}

fn engine_surface_definition(
    definition: WireSurfaceDefinition,
) -> aprodb_types::Result<SurfaceDefinition> {
    let kind = match WireSurfaceKind::try_from(definition.kind) {
        Ok(WireSurfaceKind::Work) => SurfaceKind::Work,
        Ok(WireSurfaceKind::Read) => SurfaceKind::Read,
        Err(_) => return Err(AproError::InvalidInput("unknown surface kind".into())),
    };
    let format = match WireSurfaceFormat::try_from(definition.format) {
        Ok(WireSurfaceFormat::AprodbRecords) => SurfaceFormat::AprodbRecords,
        Ok(WireSurfaceFormat::Json) => SurfaceFormat::Json,
        Err(_) => return Err(AproError::InvalidInput("unknown surface format".into())),
    };
    Ok(SurfaceDefinition {
        id: definition.id,
        kind,
        source_tenant: definition.source_tenant,
        source_namespace: definition.source_namespace,
        source_collection: definition.source_collection,
        workflow_states: definition.workflow_states,
        format,
        max_records: usize::try_from(definition.max_records).map_err(|_| {
            AproError::ResourceLimit("surface max_records exceeds usize limit".into())
        })?,
        max_bytes: usize::try_from(definition.max_bytes).map_err(|_| {
            AproError::ResourceLimit("surface max_bytes exceeds usize limit".into())
        })?,
        retained_generations: usize::try_from(definition.retained_generations).map_err(|_| {
            AproError::ResourceLimit("surface retained_generations exceeds usize limit".into())
        })?,
    })
}

fn wire_surface_generation(surface: aprodb_engine::SurfaceRead) -> WireSurfaceGeneration {
    let generation = surface.generation;
    WireSurfaceGeneration {
        projection_id: generation.projection_id,
        generation: generation.generation,
        source_watermarks: generation.source_watermarks,
        format: match generation.format {
            SurfaceFormat::AprodbRecords => WireSurfaceFormat::AprodbRecords as i32,
            SurfaceFormat::Json => WireSurfaceFormat::Json as i32,
        },
        record_count: u64::try_from(generation.record_count).unwrap_or(u64::MAX),
        serialized: generation.serialized,
        created_at_unix_ms: generation.created_at_unix_ms,
        stale_by_sequences: surface.stale_by_sequences,
        complete: surface.complete,
        errors: surface.errors,
    }
}

fn dispatch_surface_build(
    engine: &Engine,
    operation: BuildSurfaceOperation,
    durability: Durability,
    rebuild: bool,
) -> aprodb_types::Result<WireSurfaceBuild> {
    let report = if rebuild {
        engine.rebuild_surface(&operation.projection_id, durability)?
    } else {
        engine.build_surface_incremental(
            &operation.projection_id,
            usize::try_from(operation.max_events)
                .map_err(|_| AproError::ResourceLimit("max_events exceeds usize limit".into()))?,
            durability,
        )?
    };
    Ok(WireSurfaceBuild {
        projection_id: report.projection_id,
        generation: report.generation,
        events_applied: u64::try_from(report.events_applied).unwrap_or(u64::MAX),
        source_watermarks: report.source_watermarks,
        record_count: u64::try_from(report.record_count).unwrap_or(u64::MAX),
        serialized_bytes: u64::try_from(report.serialized_bytes).unwrap_or(u64::MAX),
    })
}

fn engine_expected(expected: Option<ExpectedVersion>) -> aprodb_types::Result<EngineExpected> {
    let Some(expected) = expected else {
        return Ok(EngineExpected::Any);
    };
    match ExpectedMode::try_from(expected.mode) {
        Ok(ExpectedMode::Any) => Ok(EngineExpected::Any),
        Ok(ExpectedMode::Missing) => Ok(EngineExpected::Missing),
        Ok(ExpectedMode::Exact) => expected
            .version
            .map(|version| EngineExpected::Exact(version.into()))
            .ok_or_else(|| AproError::InvalidInput("expected Exact mode without version".into())),
        Err(_) => Err(AproError::InvalidInput("unknown expected mode".into())),
    }
}

fn error_code(error: &AproError) -> ErrorCode {
    match error {
        AproError::InvalidInput(_) => ErrorCode::InvalidRequest,
        AproError::ResourceLimit(_) => ErrorCode::ResourceLimit,
        AproError::Conflict(_) => ErrorCode::Conflict,
        AproError::Corrupt(_) | AproError::Encryption(_) => ErrorCode::Corrupt,
        AproError::IncompatibleFormat(_) => ErrorCode::Incompatible,
        AproError::Storage(_) | AproError::DataDirectoryLocked(_) => ErrorCode::Storage,
        AproError::Backpressure(_) => ErrorCode::Backpressure,
        AproError::ChangeLogGap(_) => ErrorCode::ChangeLogGap,
        AproError::Unsupported(_) => ErrorCode::Unsupported,
        AproError::Compute(_) => ErrorCode::Internal,
    }
}

const fn error_class(error: &AproError) -> &'static str {
    match error {
        AproError::InvalidInput(_) => "invalid_request",
        AproError::ResourceLimit(_) => "resource_limit",
        AproError::Conflict(_) => "conflict",
        AproError::Corrupt(_) => "corrupt",
        AproError::Encryption(_) => "encryption",
        AproError::IncompatibleFormat(_) => "incompatible_format",
        AproError::Storage(_) => "storage",
        AproError::DataDirectoryLocked(_) => "data_directory_locked",
        AproError::Backpressure(_) => "backpressure",
        AproError::ChangeLogGap(_) => "change_log_gap",
        AproError::Unsupported(_) => "unsupported",
        AproError::Compute(_) => "compute",
    }
}

const fn audit_outcome_name(outcome: AuditOutcome) -> &'static str {
    match outcome {
        AuditOutcome::Attempted => "attempted",
        AuditOutcome::Succeeded => "succeeded",
        AuditOutcome::Failed => "failed",
    }
}

const fn radial_layer_name(layer: RadialLayer) -> &'static str {
    match layer {
        RadialLayer::Surface => "surface",
        RadialLayer::Hot => "hot",
        RadialLayer::Warm => "warm",
        RadialLayer::Cold => "cold",
        RadialLayer::Archive => "archive",
    }
}

fn wire_cache_metrics(metrics: CacheMetrics) -> WireCacheMetrics {
    WireCacheMetrics {
        budget_bytes: u64::try_from(metrics.budget_bytes).unwrap_or(u64::MAX),
        resident_bytes: u64::try_from(metrics.resident_bytes).unwrap_or(u64::MAX),
        entries: u64::try_from(metrics.entries).unwrap_or(u64::MAX),
        hits: metrics.hits,
        misses: metrics.misses,
        admissions: metrics.admissions,
        rejections: metrics.rejections,
        evictions: metrics.evictions,
    }
}

fn error_response(state: &State, request_id: u64, error: &AproError) -> Response {
    if matches!(error, AproError::Backpressure(_)) {
        Response::backpressure(
            request_id,
            error.to_string(),
            retry_after_ms(state.config.backpressure_retry_after),
        )
    } else {
        Response::error(request_id, error_code(error), error.to_string())
    }
}

fn retry_after_ms(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis())
        .unwrap_or(u64::MAX)
        .max(1)
}

impl From<aprodb_proto::ProtocolError> for ServerError {
    fn from(error: aprodb_proto::ProtocolError) -> Self {
        Self::Task(error.to_string())
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
