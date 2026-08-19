use std::{
    collections::BTreeMap,
    fmt,
    io::Cursor,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use aprodb_proto::{
    AtomicBatchOperation, AtomicMutation, AuditListOperation, BackupOperation,
    BuildSurfaceOperation, CacheStatsOperation, ClaimOperation, ClientHello, CompactOperation,
    CompressionPolicyOperation, CompressionStatsOperation, ComputeStatsOperation,
    ConfigureCompressionOperation, CreateSurfaceOperation, DeleteOperation, EndpointRole,
    ErrorCode, ExpectedMode, ExpectedVersion, ExpireOperation, ExplainPlacementOperation,
    FailOperation, GetOperation, GetSurfaceOperation, HealthOperation, Key, LeaseOperation,
    PublishOperation, PutOperation, Request, Response, ServerHello, ShutdownOperation, Stats,
    StatsOperation, SubscribeChangesOperation, SyncOperation, TrainDictionaryOperation,
    VectorSearchOperation, VerifyOperation, WireCompressionMode, WireCompressionPolicy,
    WireCompressionTierPolicy, WireComputeExecution, WireComputePreference, WireDurability,
    WirePayload, WireSurfaceDefinition, WireSurfaceFormat, WireSurfaceKind, WireVectorMetric,
    WorkflowScopeOperation, decode_limited, encode_limited, request,
};
use aprodb_types::{
    AuditEvent, AuditOutcome, ChangeEvent, ClaimedRecord, CompressionMode, CompressionPolicy,
    CompressionTierPolicy, Durability, LeaseProof, LogicalFrameKind, MutationReceipt, Payload,
    RecordEnvelope, RecordIdentity, SurfaceBuildReport, SurfaceDefinition, SurfaceFormat,
    SurfaceGeneration, SurfaceKind, SurfaceRead, Version, WorkflowScope, decode_logical,
};
pub use aprodb_types::{ComputeExecution, ComputePreference, CostEstimate, VectorMetric};
use futures_util::{SinkExt, StreamExt};
use rustls::{RootCertStore, pki_types::ServerName};
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    runtime::Runtime,
    sync::{mpsc, oneshot},
    time::{Instant, timeout, timeout_at},
};
use tokio_rustls::TlsConnector;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[derive(Debug, thiserror::Error)]
pub enum ClientError {
    #[error("I/O client: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocollo: {0}")]
    Protocol(#[from] aprodb_proto::ProtocolError),
    #[error("handshake rifiutato ({code:?}): {message}")]
    Handshake { code: ErrorCode, message: String },
    #[error("server ({code:?}): {message}")]
    Server {
        code: ErrorCode,
        message: String,
        retry_after: Option<Duration>,
    },
    #[error("connessione chiusa")]
    Disconnected,
    #[error("deadline client scaduta")]
    DeadlineExceeded,
    #[error("coda client chiusa")]
    QueueClosed,
    #[error("runtime client: {0}")]
    Runtime(String),
    #[error("risposta non valida: {0}")]
    InvalidResponse(String),
    #[error("TLS client: {0}")]
    Tls(String),
}

pub type Result<T> = std::result::Result<T, ClientError>;

#[derive(Clone)]
pub struct ClientTlsConfig {
    config: Arc<rustls::ClientConfig>,
    server_name: String,
}

impl fmt::Debug for ClientTlsConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ClientTlsConfig")
            .field("server_name", &self.server_name)
            .field("credentials", &"[REDACTED]")
            .finish()
    }
}

pub fn tls_client_config(
    root_ca_pem: &[u8],
    server_name: impl Into<String>,
    client_identity_pem: Option<(&[u8], &[u8])>,
) -> Result<ClientTlsConfig> {
    let mut roots = RootCertStore::empty();
    let certificates = rustls_pemfile::certs(&mut Cursor::new(root_ca_pem))
        .collect::<std::io::Result<Vec<_>>>()
        .map_err(|error| ClientError::Tls(format!("CA server: {error}")))?;
    if certificates.is_empty() {
        return Err(ClientError::Tls("CA server vuota".into()));
    }
    for certificate in certificates {
        roots
            .add(certificate)
            .map_err(|error| ClientError::Tls(format!("CA server non valida: {error}")))?;
    }
    let builder = rustls::ClientConfig::builder().with_root_certificates(roots);
    let config = if let Some((certificate_pem, private_key_pem)) = client_identity_pem {
        let certificates = rustls_pemfile::certs(&mut Cursor::new(certificate_pem))
            .collect::<std::io::Result<Vec<_>>>()
            .map_err(|error| ClientError::Tls(format!("certificato client: {error}")))?;
        if certificates.is_empty() {
            return Err(ClientError::Tls("catena certificati client vuota".into()));
        }
        let private_key = rustls_pemfile::private_key(&mut Cursor::new(private_key_pem))
            .map_err(|error| ClientError::Tls(format!("chiave privata client: {error}")))?
            .ok_or_else(|| ClientError::Tls("chiave privata client assente".into()))?;
        builder
            .with_client_auth_cert(certificates, private_key)
            .map_err(|error| ClientError::Tls(format!("identità client: {error}")))?
    } else {
        builder.with_no_client_auth()
    };
    let server_name = server_name.into();
    ServerName::try_from(server_name.clone())
        .map_err(|error| ClientError::Tls(format!("nome server non valido: {error}")))?;
    Ok(ClientTlsConfig {
        config: Arc::new(config),
        server_name,
    })
}

#[derive(Clone, Debug)]
pub struct ClientConfig {
    pub role: EndpointRole,
    pub auth_token: Vec<u8>,
    pub max_frame_bytes: usize,
    pub queue_depth: usize,
    pub request_timeout: Duration,
    pub handshake_timeout: Duration,
    pub tls: Option<ClientTlsConfig>,
}

impl ClientConfig {
    #[must_use]
    pub fn data(auth_token: impl Into<Vec<u8>>) -> Self {
        Self {
            role: EndpointRole::Data,
            auth_token: auth_token.into(),
            max_frame_bytes: aprodb_proto::DEFAULT_MAX_FRAME_BYTES,
            queue_depth: 128,
            request_timeout: Duration::from_secs(30),
            handshake_timeout: Duration::from_secs(5),
            tls: None,
        }
    }

    #[must_use]
    pub fn admin(auth_token: impl Into<Vec<u8>>) -> Self {
        Self {
            role: EndpointRole::Admin,
            ..Self::data(auth_token)
        }
    }

    fn validate(&self) -> Result<()> {
        if self.queue_depth == 0 {
            return Err(ClientError::InvalidResponse(
                "queue_depth client deve essere positivo".into(),
            ));
        }
        if self.request_timeout.is_zero() || self.handshake_timeout.is_zero() {
            return Err(ClientError::InvalidResponse(
                "timeout client deve essere positivo".into(),
            ));
        }
        let hello = ClientHello::new(self.role, self.auth_token.clone(), self.max_frame_bytes);
        hello.validate()?;
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum Expected {
    #[default]
    Any,
    Missing,
    Exact(Version),
}

#[derive(Clone, Debug)]
pub struct PutOptions {
    pub content_type: String,
    pub metadata: BTreeMap<String, Vec<u8>>,
    pub expires_at_unix_ms: Option<u64>,
    pub expected: Expected,
    pub delta: Option<Vec<u8>>,
    pub idempotency_key_hash: Option<[u8; 32]>,
}

impl Default for PutOptions {
    fn default() -> Self {
        Self {
            content_type: "application/octet-stream".into(),
            metadata: BTreeMap::new(),
            expires_at_unix_ms: None,
            expected: Expected::Any,
            delta: None,
            idempotency_key_hash: None,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct DeleteOptions {
    pub expected: Expected,
    pub delta: Option<Vec<u8>>,
    pub idempotency_key_hash: Option<[u8; 32]>,
}

#[derive(Clone, Debug)]
pub enum Mutation {
    Put {
        identity: RecordIdentity,
        payload: Payload,
        options: PutOptions,
    },
    Delete {
        identity: RecordIdentity,
        options: DeleteOptions,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Placement {
    pub canonical_version: Version,
    pub radial_score_millis: u16,
    pub freshness_millis: u16,
    pub urgency_millis: u16,
    pub current_layer: String,
    pub recommended_layer: String,
    pub storage_class: String,
    pub pinned: bool,
    pub object_cache_resident: bool,
    pub physical_tiering_supported: bool,
    pub reasons: Vec<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClientCacheMetrics {
    pub budget_bytes: u64,
    pub resident_bytes: u64,
    pub entries: u64,
    pub hits: u64,
    pub misses: u64,
    pub admissions: u64,
    pub rejections: u64,
    pub evictions: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClientCacheStats {
    pub metadata: ClientCacheMetrics,
    pub objects: ClientCacheMetrics,
    pub negative: ClientCacheMetrics,
    pub compressed: ClientCacheMetrics,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClientCompressionStats {
    pub logical_bytes: u64,
    pub stored_bytes: u64,
    pub raw_records: u64,
    pub zstd_records: u64,
    pub dictionary_records: u64,
    pub incompressible_fallbacks: u64,
    pub content_type_skips: u64,
    pub compress_micros: u64,
    pub decompress_micros: u64,
    pub codec_failures: u64,
    pub channels: u64,
    pub scratch_budget_bytes: u64,
    pub scratch_in_use_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientCompressionDictionary {
    pub id: u64,
    pub schema: String,
    pub bytes: u64,
    pub checksum: u32,
    pub created_at_unix_ms: u64,
    pub validation_raw_bytes: u64,
    pub validation_without_dictionary_bytes: u64,
    pub validation_with_dictionary_bytes: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ClientVectorSearchHit {
    pub identity: RecordIdentity,
    pub version: Version,
    pub score: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ClientVectorSearchResult {
    pub hits: Vec<ClientVectorSearchHit>,
    pub scanned_records: usize,
    pub vector_candidates: usize,
    pub execution: ComputeExecution,
    pub accelerator: Option<String>,
    pub estimate: CostEstimate,
    pub fallback_reason: Option<String>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClientComputeStats {
    pub requests: u64,
    pub cpu_runs: u64,
    pub accelerator_runs: u64,
    pub cpu_fallbacks: u64,
    pub queue_rejections: u64,
    pub accelerator_failures: u64,
    pub request_timeouts: u64,
    pub circuit_open_rejections: u64,
    pub micro_batches: u64,
    pub micro_batched_requests: u64,
    pub inflight_bytes: u64,
    pub peak_inflight_bytes: u64,
    pub accelerator_name: Option<String>,
    pub vram_budget_bytes: u64,
    pub vram_resident_bytes: u64,
    pub vram_entries: u64,
    pub vram_hits: u64,
    pub vram_misses: u64,
    pub vram_evictions: u64,
    pub upload_bytes: u64,
    pub readback_bytes: u64,
    pub transfer_micros: u64,
    pub kernel_micros: u64,
    pub device_resets: u64,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ClientExpirationReport {
    pub scanned: u64,
    pub expired: u64,
    pub stale_entries: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ClientWorkflowResult {
    pub record: RecordEnvelope,
    pub receipt: MutationReceipt,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ChangePage {
    pub events: Vec<ChangeEvent>,
    pub watermark: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditPage {
    pub events: Vec<AuditEvent>,
    pub next_sequence: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientBackupInfo {
    pub name: String,
    pub catalog_generation: u64,
    pub files: u64,
    pub bytes: u64,
    pub logical_bytes: u64,
    pub encrypted: bool,
}

impl Mutation {
    fn identity(&self) -> &RecordIdentity {
        match self {
            Self::Put { identity, .. } | Self::Delete { identity, .. } => identity,
        }
    }
}

struct Command {
    request: Request,
    response: oneshot::Sender<Result<Response>>,
}

#[derive(Clone, Copy)]
enum LeaseMutation {
    Heartbeat,
    Complete,
    Fail(bool),
}

#[derive(Clone)]
pub struct AsyncClient {
    commands: mpsc::Sender<Command>,
    next_request_id: Arc<AtomicU64>,
    request_timeout: Duration,
}

impl AsyncClient {
    pub async fn connect_tcp(address: SocketAddr, config: ClientConfig) -> Result<Self> {
        config.validate()?;
        let stream = timeout(config.handshake_timeout, TcpStream::connect(address))
            .await
            .map_err(|_| ClientError::DeadlineExceeded)??;
        stream.set_nodelay(true)?;
        if let Some(tls) = config.tls.clone() {
            let server_name = ServerName::try_from(tls.server_name.clone())
                .map_err(|error| ClientError::Tls(format!("nome server non valido: {error}")))?;
            let stream = timeout(
                config.handshake_timeout,
                TlsConnector::from(tls.config).connect(server_name, stream),
            )
            .await
            .map_err(|_| ClientError::DeadlineExceeded)?
            .map_err(|error| ClientError::Tls(error.to_string()))?;
            Self::connect_stream(stream, config).await
        } else {
            Self::connect_stream(stream, config).await
        }
    }

    #[cfg(unix)]
    pub async fn connect_local(
        path: impl AsRef<std::path::Path>,
        config: ClientConfig,
    ) -> Result<Self> {
        if config.tls.is_some() {
            return Err(ClientError::Tls(
                "TLS è supportato solo per il trasporto TCP".into(),
            ));
        }
        let stream = timeout(
            config.handshake_timeout,
            tokio::net::UnixStream::connect(path),
        )
        .await
        .map_err(|_| ClientError::DeadlineExceeded)??;
        Self::connect_stream(stream, config).await
    }

    #[cfg(windows)]
    pub async fn connect_local(name: &str, config: ClientConfig) -> Result<Self> {
        if config.tls.is_some() {
            return Err(ClientError::Tls(
                "TLS è supportato solo per il trasporto TCP".into(),
            ));
        }
        let stream = tokio::net::windows::named_pipe::ClientOptions::new().open(name)?;
        Self::connect_stream(stream, config).await
    }

    async fn connect_stream<S>(stream: S, config: ClientConfig) -> Result<Self>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        config.validate()?;
        let codec = LengthDelimitedCodec::builder()
            .max_frame_length(config.max_frame_bytes)
            .new_codec();
        let mut framed = Framed::new(stream, codec);
        let hello = ClientHello::new(
            config.role,
            config.auth_token.clone(),
            config.max_frame_bytes,
        );
        let bytes = encode_limited(&hello, config.max_frame_bytes)?;
        timeout(config.handshake_timeout, framed.send(bytes))
            .await
            .map_err(|_| ClientError::DeadlineExceeded)??;
        let frame = timeout(config.handshake_timeout, framed.next())
            .await
            .map_err(|_| ClientError::DeadlineExceeded)?
            .ok_or(ClientError::Disconnected)??;
        let server_hello: ServerHello = decode_limited(&frame, config.max_frame_bytes)?;
        if !server_hello.accepted {
            return Err(ClientError::Handshake {
                code: ErrorCode::try_from(server_hello.error_code).unwrap_or(ErrorCode::Internal),
                message: server_hello.error_message,
            });
        }
        if server_hello.protocol_major != aprodb_proto::PROTOCOL_MAJOR {
            return Err(ClientError::Handshake {
                code: ErrorCode::Incompatible,
                message: "protocol major server incompatibile".into(),
            });
        }
        let negotiated_max = (server_hello.max_frame_bytes as usize).min(config.max_frame_bytes);
        let (commands, receiver) = mpsc::channel(config.queue_depth);
        tokio::spawn(connection_actor(framed, receiver, negotiated_max));
        Ok(Self {
            commands,
            next_request_id: Arc::new(AtomicU64::new(1)),
            request_timeout: config.request_timeout,
        })
    }

    async fn request(&self, mut request: Request) -> Result<Response> {
        let caller_deadline = Instant::now()
            .checked_add(self.request_timeout)
            .ok_or_else(|| ClientError::Runtime("deadline monotona non rappresentabile".into()))?;
        request.request_id = self.next_request_id.fetch_add(1, Ordering::Relaxed);
        request.deadline_unix_ms = unix_ms_after(self.request_timeout)?;
        request.validate()?;
        let (response, receiver) = oneshot::channel();
        timeout_at(
            caller_deadline,
            self.commands.send(Command { request, response }),
        )
        .await
        .map_err(|_| ClientError::DeadlineExceeded)?
        .map_err(|_| ClientError::QueueClosed)?;
        let response = timeout_at(caller_deadline, receiver)
            .await
            .map_err(|_| ClientError::DeadlineExceeded)?
            .map_err(|_| ClientError::Disconnected)??;
        if !response.is_ok() {
            return Err(ClientError::Server {
                code: ErrorCode::try_from(response.error_code).unwrap_or(ErrorCode::Internal),
                message: response.error_message,
                retry_after: (response.retry_after_ms != 0)
                    .then(|| Duration::from_millis(response.retry_after_ms)),
            });
        }
        Ok(response)
    }

    pub async fn put(
        &self,
        identity: RecordIdentity,
        payload: Payload,
        options: PutOptions,
        durability: Durability,
    ) -> Result<MutationReceipt> {
        let (tenant, namespace, key) = split_identity(identity);
        let response = self
            .request(Request {
                request_id: 0,
                deadline_unix_ms: 0,
                tenant,
                namespace,
                durability: wire_durability(durability),
                operation: Some(request::Operation::Put(PutOperation {
                    key: Some(key),
                    payload: Some(WirePayload::from(&payload)),
                    content_type: options.content_type,
                    metadata: options.metadata,
                    expires_at_unix_ms: options.expires_at_unix_ms,
                    expected: Some(wire_expected(options.expected)),
                    delta: options.delta.unwrap_or_default(),
                    idempotency_key_hash: options
                        .idempotency_key_hash
                        .map_or_else(Vec::new, |hash| hash.to_vec()),
                })),
            })
            .await?;
        one_receipt(response)
    }

    pub async fn get(&self, identity: RecordIdentity) -> Result<Option<RecordEnvelope>> {
        let (tenant, namespace, key) = split_identity(identity);
        let response = self
            .request(Request {
                request_id: 0,
                deadline_unix_ms: 0,
                tenant,
                namespace,
                durability: WireDurability::Durable as i32,
                operation: Some(request::Operation::Get(GetOperation { key: Some(key) })),
            })
            .await?;
        response
            .record
            .map(TryInto::try_into)
            .transpose()
            .map_err(ClientError::from)
    }

    pub async fn delete(
        &self,
        identity: RecordIdentity,
        options: DeleteOptions,
        durability: Durability,
    ) -> Result<MutationReceipt> {
        let (tenant, namespace, key) = split_identity(identity);
        let response = self
            .request(Request {
                request_id: 0,
                deadline_unix_ms: 0,
                tenant,
                namespace,
                durability: wire_durability(durability),
                operation: Some(request::Operation::Delete(DeleteOperation {
                    key: Some(key),
                    expected: Some(wire_expected(options.expected)),
                    delta: options.delta.unwrap_or_default(),
                    idempotency_key_hash: options
                        .idempotency_key_hash
                        .map_or_else(Vec::new, |hash| hash.to_vec()),
                })),
            })
            .await?;
        one_receipt(response)
    }

    pub async fn compare_and_swap(
        &self,
        identity: RecordIdentity,
        payload: Payload,
        expected: Version,
        mut options: PutOptions,
        durability: Durability,
    ) -> Result<MutationReceipt> {
        options.expected = Expected::Exact(expected);
        self.put(identity, payload, options, durability).await
    }

    pub async fn atomic_batch(
        &self,
        mutations: Vec<Mutation>,
        durability: Durability,
    ) -> Result<Vec<MutationReceipt>> {
        let first = mutations
            .first()
            .ok_or_else(|| ClientError::InvalidResponse("AtomicBatch client vuoto".into()))?;
        let tenant = first.identity().tenant.clone();
        let namespace = first.identity().namespace.clone();
        if mutations.iter().any(|mutation| {
            mutation.identity().tenant != tenant || mutation.identity().namespace != namespace
        }) {
            return Err(ClientError::InvalidResponse(
                "AtomicBatch deve condividere tenant e namespace".into(),
            ));
        }
        let wire_mutations = mutations.into_iter().map(wire_mutation).collect::<Vec<_>>();
        let response = self
            .request(Request {
                request_id: 0,
                deadline_unix_ms: 0,
                tenant,
                namespace,
                durability: wire_durability(durability),
                operation: Some(request::Operation::AtomicBatch(AtomicBatchOperation {
                    mutations: wire_mutations,
                })),
            })
            .await?;
        response
            .receipts
            .into_iter()
            .map(|receipt| receipt.try_into().map_err(ClientError::from))
            .collect()
    }

    pub async fn append(
        &self,
        identity: RecordIdentity,
        payload: Payload,
        options: PutOptions,
        durability: Durability,
    ) -> Result<MutationReceipt> {
        let (tenant, namespace, key) = split_identity(identity);
        let response = self
            .request(Request {
                request_id: 0,
                deadline_unix_ms: 0,
                tenant,
                namespace,
                durability: wire_durability(durability),
                operation: Some(request::Operation::Append(wire_put_operation(
                    key, payload, options,
                ))),
            })
            .await?;
        one_receipt(response)
    }

    pub async fn claim(
        &self,
        scope: WorkflowScope,
        max_records: usize,
        lease_duration: Duration,
        idempotency_key_hash: Option<[u8; 32]>,
        durability: Durability,
    ) -> Result<Vec<ClaimedRecord>> {
        let max_records = u64::try_from(max_records)
            .map_err(|_| ClientError::InvalidResponse("max_records Claim oltre u64".into()))?;
        let lease_duration_ms = duration_ms(lease_duration, "durata lease")?;
        let response = self
            .request(Request {
                request_id: 0,
                deadline_unix_ms: 0,
                tenant: scope.tenant,
                namespace: scope.namespace,
                durability: wire_durability(durability),
                operation: Some(request::Operation::Claim(ClaimOperation {
                    scope: Some(WorkflowScopeOperation {
                        collection: scope.collection,
                        partition: scope.partition,
                    }),
                    max_records,
                    lease_duration_ms,
                    idempotency_key_hash: wire_hash(idempotency_key_hash),
                })),
            })
            .await?;
        claimed_records(response)
    }

    pub async fn heartbeat(
        &self,
        identity: RecordIdentity,
        lease: LeaseProof,
        extension: Duration,
        idempotency_key_hash: Option<[u8; 32]>,
        durability: Durability,
    ) -> Result<ClientWorkflowResult> {
        self.lease_mutation(
            identity,
            lease,
            extension,
            idempotency_key_hash,
            durability,
            LeaseMutation::Heartbeat,
        )
        .await
    }

    pub async fn complete(
        &self,
        identity: RecordIdentity,
        lease: LeaseProof,
        idempotency_key_hash: Option<[u8; 32]>,
        durability: Durability,
    ) -> Result<ClientWorkflowResult> {
        self.lease_mutation(
            identity,
            lease,
            Duration::ZERO,
            idempotency_key_hash,
            durability,
            LeaseMutation::Complete,
        )
        .await
    }

    pub async fn fail(
        &self,
        identity: RecordIdentity,
        lease: LeaseProof,
        permanent: bool,
        idempotency_key_hash: Option<[u8; 32]>,
        durability: Durability,
    ) -> Result<ClientWorkflowResult> {
        self.lease_mutation(
            identity,
            lease,
            Duration::ZERO,
            idempotency_key_hash,
            durability,
            LeaseMutation::Fail(permanent),
        )
        .await
    }

    pub async fn publish(
        &self,
        identity: RecordIdentity,
        idempotency_key_hash: Option<[u8; 32]>,
        durability: Durability,
    ) -> Result<ClientWorkflowResult> {
        let (tenant, namespace, key) = split_identity(identity);
        let response = self
            .request(Request {
                request_id: 0,
                deadline_unix_ms: 0,
                tenant,
                namespace,
                durability: wire_durability(durability),
                operation: Some(request::Operation::Publish(PublishOperation {
                    key: Some(key),
                    idempotency_key_hash: wire_hash(idempotency_key_hash),
                })),
            })
            .await?;
        one_workflow_result(response)
    }

    pub async fn subscribe_changes(
        &self,
        tenant: Vec<u8>,
        namespace: Vec<u8>,
        collection: Vec<u8>,
        shard: u32,
        after_sequence: u64,
        limit: usize,
    ) -> Result<ChangePage> {
        let limit = u64::try_from(limit)
            .map_err(|_| ClientError::InvalidResponse("limite change stream oltre u64".into()))?;
        let response = self
            .request(Request {
                request_id: 0,
                deadline_unix_ms: 0,
                tenant,
                namespace,
                durability: WireDurability::Durable as i32,
                operation: Some(request::Operation::SubscribeChanges(
                    SubscribeChangesOperation {
                        collection,
                        shard,
                        after_sequence,
                        limit,
                    },
                )),
            })
            .await?;
        let events = response
            .change_events
            .iter()
            .map(|bytes| {
                decode_logical(LogicalFrameKind::Change, bytes)
                    .map_err(|error| ClientError::InvalidResponse(error.to_string()))
            })
            .collect::<Result<Vec<ChangeEvent>>>()?;
        Ok(ChangePage {
            events,
            watermark: response.change_watermark,
        })
    }

    pub async fn create_surface(&self, definition: SurfaceDefinition) -> Result<()> {
        self.request(admin_request(request::Operation::CreateSurface(
            CreateSurfaceOperation {
                definition: Some(wire_surface_definition(definition)?),
            },
        )))
        .await
        .map(|_| ())
    }

    pub async fn build_surface(
        &self,
        projection_id: impl Into<String>,
        max_events: usize,
        durability: Durability,
    ) -> Result<SurfaceBuildReport> {
        self.surface_build(projection_id, max_events, durability, false)
            .await
    }

    pub async fn rebuild_surface(
        &self,
        projection_id: impl Into<String>,
        durability: Durability,
    ) -> Result<SurfaceBuildReport> {
        self.surface_build(projection_id, 0, durability, true).await
    }

    pub async fn get_surface(
        &self,
        tenant: Vec<u8>,
        namespace: Vec<u8>,
        projection_id: impl Into<String>,
    ) -> Result<Option<SurfaceRead>> {
        let response = self
            .request(Request {
                request_id: 0,
                deadline_unix_ms: 0,
                tenant,
                namespace,
                durability: WireDurability::Durable as i32,
                operation: Some(request::Operation::GetSurface(GetSurfaceOperation {
                    projection_id: projection_id.into(),
                })),
            })
            .await?;
        response.surface.map(surface_generation).transpose()
    }

    async fn lease_mutation(
        &self,
        identity: RecordIdentity,
        lease: LeaseProof,
        extension: Duration,
        idempotency_key_hash: Option<[u8; 32]>,
        durability: Durability,
        mutation: LeaseMutation,
    ) -> Result<ClientWorkflowResult> {
        let (tenant, namespace, key) = split_identity(identity);
        let lease = LeaseOperation {
            key: Some(key),
            lease_id: lease.lease_id.to_vec(),
            fencing_token: lease.fencing_token,
            extension_ms: duration_ms(extension, "estensione lease")?,
            idempotency_key_hash: wire_hash(idempotency_key_hash),
        };
        let operation = match mutation {
            LeaseMutation::Heartbeat => request::Operation::Heartbeat(lease),
            LeaseMutation::Complete => request::Operation::Complete(lease),
            LeaseMutation::Fail(permanent) => request::Operation::Fail(FailOperation {
                lease: Some(lease),
                permanent,
            }),
        };
        let response = self
            .request(Request {
                request_id: 0,
                deadline_unix_ms: 0,
                tenant,
                namespace,
                durability: wire_durability(durability),
                operation: Some(operation),
            })
            .await?;
        one_workflow_result(response)
    }

    async fn surface_build(
        &self,
        projection_id: impl Into<String>,
        max_events: usize,
        durability: Durability,
        rebuild: bool,
    ) -> Result<SurfaceBuildReport> {
        let operation = BuildSurfaceOperation {
            projection_id: projection_id.into(),
            max_events: u64::try_from(max_events).map_err(|_| {
                ClientError::InvalidResponse("max_events superficie oltre u64".into())
            })?,
        };
        let operation = if rebuild {
            request::Operation::RebuildSurface(operation)
        } else {
            request::Operation::BuildSurface(operation)
        };
        let response = self
            .request(Request {
                durability: wire_durability(durability),
                ..admin_request(operation)
            })
            .await?;
        response
            .surface_build
            .map(surface_build_report)
            .transpose()?
            .ok_or_else(|| ClientError::InvalidResponse("report superficie assente".into()))
    }

    pub async fn sync(&self) -> Result<()> {
        self.request(admin_or_data_request(request::Operation::Sync(
            SyncOperation {},
        )))
        .await
        .map(|_| ())
    }

    pub async fn health(&self) -> Result<bool> {
        self.request(admin_request(request::Operation::Health(
            HealthOperation {},
        )))
        .await
        .map(|response| response.healthy)
    }

    pub async fn stats(&self) -> Result<Stats> {
        self.request(admin_request(request::Operation::Stats(StatsOperation {})))
            .await?
            .stats
            .ok_or_else(|| ClientError::InvalidResponse("stats assenti".into()))
    }

    pub async fn verify(&self) -> Result<()> {
        self.request(admin_request(request::Operation::Verify(
            VerifyOperation {},
        )))
        .await
        .map(|_| ())
    }

    pub async fn compact(&self) -> Result<()> {
        self.request(admin_request(request::Operation::Compact(
            CompactOperation {},
        )))
        .await
        .map(|_| ())
    }

    pub async fn audit(&self, after_sequence: Option<u64>, limit: usize) -> Result<AuditPage> {
        let response = self
            .request(admin_request(request::Operation::AuditList(
                AuditListOperation {
                    after_sequence,
                    limit: u64::try_from(limit).map_err(|_| {
                        ClientError::InvalidResponse("limit audit oltre u64".into())
                    })?,
                },
            )))
            .await?;
        let events = response
            .audit_events
            .into_iter()
            .map(|event| {
                let event_id: [u8; 16] = event.event_id.try_into().map_err(|_| {
                    ClientError::InvalidResponse("event_id audit non valido".into())
                })?;
                let target_hash = event
                    .target_hash
                    .map(|value| {
                        value.try_into().map_err(|_| {
                            ClientError::InvalidResponse("target_hash audit non valido".into())
                        })
                    })
                    .transpose()?;
                let outcome = match event.outcome.as_str() {
                    "attempted" => AuditOutcome::Attempted,
                    "succeeded" => AuditOutcome::Succeeded,
                    "failed" => AuditOutcome::Failed,
                    _ => {
                        return Err(ClientError::InvalidResponse(
                            "outcome audit sconosciuto".into(),
                        ));
                    }
                };
                Ok(AuditEvent {
                    format_version: 1,
                    sequence: event.sequence,
                    event_id,
                    at_unix_ms: event.at_unix_ms,
                    request_id: event.request_id,
                    principal: event.principal,
                    operation: event.operation,
                    outcome,
                    target_hash,
                    error_class: event.error_class,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(AuditPage {
            events,
            next_sequence: response.audit_next_sequence,
        })
    }

    pub async fn backup(&self, name: impl Into<String>) -> Result<ClientBackupInfo> {
        self.request(admin_request(request::Operation::Backup(BackupOperation {
            name: name.into(),
        })))
        .await?
        .backup
        .map(|backup| ClientBackupInfo {
            name: backup.name,
            catalog_generation: backup.catalog_generation,
            files: backup.files,
            bytes: backup.bytes,
            logical_bytes: backup.logical_bytes,
            encrypted: backup.encrypted,
        })
        .ok_or_else(|| ClientError::InvalidResponse("informazioni backup assenti".into()))
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.request(admin_request(request::Operation::Shutdown(
            ShutdownOperation {},
        )))
        .await
        .map(|_| ())
    }

    pub async fn explain_placement(&self, identity: RecordIdentity) -> Result<Placement> {
        let (tenant, namespace, key) = split_identity(identity);
        let response = self
            .request(Request {
                request_id: 0,
                deadline_unix_ms: 0,
                tenant,
                namespace,
                durability: WireDurability::Durable as i32,
                operation: Some(request::Operation::ExplainPlacement(
                    ExplainPlacementOperation { key: Some(key) },
                )),
            })
            .await?;
        let placement = response
            .placement
            .ok_or_else(|| ClientError::InvalidResponse("placement assente".into()))?;
        Ok(Placement {
            canonical_version: placement
                .canonical_version
                .ok_or_else(|| ClientError::InvalidResponse("versione placement assente".into()))?
                .into(),
            radial_score_millis: u16::try_from(placement.radial_score_millis)
                .map_err(|_| ClientError::InvalidResponse("radial score oltre u16".into()))?,
            freshness_millis: u16::try_from(placement.freshness_millis)
                .map_err(|_| ClientError::InvalidResponse("freshness oltre u16".into()))?,
            urgency_millis: u16::try_from(placement.urgency_millis)
                .map_err(|_| ClientError::InvalidResponse("urgenza oltre u16".into()))?,
            current_layer: placement.current_layer,
            recommended_layer: placement.recommended_layer,
            storage_class: placement.storage_class,
            pinned: placement.pinned,
            object_cache_resident: placement.object_cache_resident,
            physical_tiering_supported: placement.physical_tiering_supported,
            reasons: placement.reasons,
        })
    }

    pub async fn cache_stats(&self) -> Result<ClientCacheStats> {
        let response = self
            .request(admin_request(request::Operation::CacheStats(
                CacheStatsOperation {},
            )))
            .await?;
        let stats = response
            .cache_stats
            .ok_or_else(|| ClientError::InvalidResponse("cache stats assenti".into()))?;
        Ok(ClientCacheStats {
            metadata: client_cache_metrics(stats.metadata, "metadata")?,
            objects: client_cache_metrics(stats.objects, "objects")?,
            negative: client_cache_metrics(stats.negative, "negative")?,
            compressed: client_cache_metrics(stats.compressed, "compressed")?,
        })
    }

    pub async fn compression_stats(&self) -> Result<ClientCompressionStats> {
        let response = self
            .request(admin_request(request::Operation::CompressionStats(
                CompressionStatsOperation {},
            )))
            .await?;
        let stats = response
            .compression_stats
            .ok_or_else(|| ClientError::InvalidResponse("compression stats assenti".into()))?;
        Ok(ClientCompressionStats {
            logical_bytes: stats.logical_bytes,
            stored_bytes: stats.stored_bytes,
            raw_records: stats.raw_records,
            zstd_records: stats.zstd_records,
            dictionary_records: stats.dictionary_records,
            incompressible_fallbacks: stats.incompressible_fallbacks,
            content_type_skips: stats.content_type_skips,
            compress_micros: stats.compress_micros,
            decompress_micros: stats.decompress_micros,
            codec_failures: stats.codec_failures,
            channels: stats.channels,
            scratch_budget_bytes: stats.scratch_budget_bytes,
            scratch_in_use_bytes: stats.scratch_in_use_bytes,
        })
    }

    pub async fn compression_policy(
        &self,
        collection: RecordIdentity,
    ) -> Result<CompressionPolicy> {
        let response = self
            .request(Request {
                tenant: collection.tenant.clone(),
                namespace: collection.namespace.clone(),
                operation: Some(request::Operation::CompressionPolicy(
                    CompressionPolicyOperation {
                        collection: collection.collection,
                    },
                )),
                ..admin_request(request::Operation::CompressionStats(
                    CompressionStatsOperation {},
                ))
            })
            .await?;
        compression_policy_from_wire(
            response
                .compression_policy
                .ok_or_else(|| ClientError::InvalidResponse("compression policy assente".into()))?,
        )
    }

    pub async fn configure_compression(
        &self,
        collection: RecordIdentity,
        policy: CompressionPolicy,
    ) -> Result<()> {
        self.request(Request {
            tenant: collection.tenant.clone(),
            namespace: collection.namespace.clone(),
            operation: Some(request::Operation::ConfigureCompression(
                ConfigureCompressionOperation {
                    collection: collection.collection,
                    policy: Some(wire_compression_policy(policy)?),
                },
            )),
            ..admin_request(request::Operation::CompressionStats(
                CompressionStatsOperation {},
            ))
        })
        .await?;
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn train_dictionary(
        &self,
        collection: RecordIdentity,
        schema: impl Into<String>,
        training_samples: &[Payload],
        validation_samples: &[Payload],
        max_dictionary_bytes: usize,
        minimum_validation_gain_bytes: usize,
    ) -> Result<ClientCompressionDictionary> {
        let response = self
            .request(Request {
                tenant: collection.tenant.clone(),
                namespace: collection.namespace.clone(),
                operation: Some(request::Operation::TrainDictionary(
                    TrainDictionaryOperation {
                        collection: collection.collection,
                        schema: schema.into(),
                        training_samples: training_samples.iter().map(WirePayload::from).collect(),
                        validation_samples: validation_samples
                            .iter()
                            .map(WirePayload::from)
                            .collect(),
                        max_dictionary_bytes: u64::try_from(max_dictionary_bytes).map_err(
                            |_| {
                                ClientError::InvalidResponse(
                                    "dimensione dizionario oltre u64".into(),
                                )
                            },
                        )?,
                        minimum_validation_gain_bytes: u64::try_from(minimum_validation_gain_bytes)
                            .map_err(|_| {
                                ClientError::InvalidResponse("gain dizionario oltre u64".into())
                            })?,
                    },
                )),
                ..admin_request(request::Operation::CompressionStats(
                    CompressionStatsOperation {},
                ))
            })
            .await?;
        let dictionary = response.compression_dictionary.ok_or_else(|| {
            ClientError::InvalidResponse("metadati dizionario compressione assenti".into())
        })?;
        Ok(ClientCompressionDictionary {
            id: dictionary.id,
            schema: dictionary.schema,
            bytes: dictionary.bytes,
            checksum: dictionary.checksum,
            created_at_unix_ms: dictionary.created_at_unix_ms,
            validation_raw_bytes: dictionary.validation_raw_bytes,
            validation_without_dictionary_bytes: dictionary.validation_without_dictionary_bytes,
            validation_with_dictionary_bytes: dictionary.validation_with_dictionary_bytes,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn vector_exact(
        &self,
        tenant: Vec<u8>,
        namespace: Vec<u8>,
        collection: Vec<u8>,
        query: Vec<f32>,
        metric: VectorMetric,
        limit: usize,
        max_scan_records: usize,
        preference: ComputePreference,
    ) -> Result<ClientVectorSearchResult> {
        let response = self
            .request(Request {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                operation: Some(request::Operation::VectorSearch(VectorSearchOperation {
                    collection: collection.clone(),
                    query,
                    metric: match metric {
                        VectorMetric::Dot => WireVectorMetric::Dot,
                        VectorMetric::Cosine => WireVectorMetric::Cosine,
                    } as i32,
                    limit: u64::try_from(limit)
                        .map_err(|_| ClientError::InvalidResponse("limit oltre u64".into()))?,
                    max_scan_records: u64::try_from(max_scan_records).map_err(|_| {
                        ClientError::InvalidResponse("max_scan_records oltre u64".into())
                    })?,
                    preference: match preference {
                        ComputePreference::Cpu => WireComputePreference::Cpu,
                        ComputePreference::Accelerator => WireComputePreference::Accelerator,
                        ComputePreference::Auto => WireComputePreference::Auto,
                    } as i32,
                })),
                ..admin_or_data_request(request::Operation::Sync(SyncOperation {}))
            })
            .await?;
        let result = response
            .vector_search
            .ok_or_else(|| ClientError::InvalidResponse("risultato VectorSearch assente".into()))?;
        let execution = match WireComputeExecution::try_from(result.execution) {
            Ok(WireComputeExecution::Cpu) => ComputeExecution::Cpu,
            Ok(WireComputeExecution::Accelerator) => ComputeExecution::Accelerator,
            Ok(WireComputeExecution::CpuFallback) => ComputeExecution::CpuFallback,
            Err(_) => {
                return Err(ClientError::InvalidResponse(
                    "compute execution sconosciuta".into(),
                ));
            }
        };
        let hits = result
            .hits
            .into_iter()
            .map(|hit| {
                Ok(ClientVectorSearchHit {
                    identity: RecordIdentity::new(
                        tenant.clone(),
                        namespace.clone(),
                        collection.clone(),
                        hit.partition,
                        hit.key,
                    )
                    .map_err(|error| ClientError::InvalidResponse(error.to_string()))?,
                    version: hit
                        .version
                        .ok_or_else(|| {
                            ClientError::InvalidResponse("version hit vettoriale assente".into())
                        })?
                        .into(),
                    score: hit.score,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(ClientVectorSearchResult {
            hits,
            scanned_records: usize::try_from(result.scanned_records)
                .map_err(|_| ClientError::InvalidResponse("scanned_records oltre usize".into()))?,
            vector_candidates: usize::try_from(result.vector_candidates).map_err(|_| {
                ClientError::InvalidResponse("vector_candidates oltre usize".into())
            })?,
            execution,
            accelerator: result.accelerator,
            estimate: cost_estimate_from_wire(result.estimate)?,
            fallback_reason: result.fallback_reason,
        })
    }

    pub async fn compute_stats(&self) -> Result<ClientComputeStats> {
        let response = self
            .request(admin_request(request::Operation::ComputeStats(
                ComputeStatsOperation {},
            )))
            .await?;
        let stats = response
            .compute_stats
            .ok_or_else(|| ClientError::InvalidResponse("compute stats assenti".into()))?;
        Ok(ClientComputeStats {
            requests: stats.requests,
            cpu_runs: stats.cpu_runs,
            accelerator_runs: stats.accelerator_runs,
            cpu_fallbacks: stats.cpu_fallbacks,
            queue_rejections: stats.queue_rejections,
            accelerator_failures: stats.accelerator_failures,
            request_timeouts: stats.request_timeouts,
            circuit_open_rejections: stats.circuit_open_rejections,
            micro_batches: stats.micro_batches,
            micro_batched_requests: stats.micro_batched_requests,
            inflight_bytes: stats.inflight_bytes,
            peak_inflight_bytes: stats.peak_inflight_bytes,
            accelerator_name: stats.accelerator_name,
            vram_budget_bytes: stats.vram_budget_bytes,
            vram_resident_bytes: stats.vram_resident_bytes,
            vram_entries: stats.vram_entries,
            vram_hits: stats.vram_hits,
            vram_misses: stats.vram_misses,
            vram_evictions: stats.vram_evictions,
            upload_bytes: stats.upload_bytes,
            readback_bytes: stats.readback_bytes,
            transfer_micros: stats.transfer_micros,
            kernel_micros: stats.kernel_micros,
            device_resets: stats.device_resets,
        })
    }

    pub async fn expire(
        &self,
        limit: usize,
        durability: Durability,
    ) -> Result<ClientExpirationReport> {
        let limit = u64::try_from(limit)
            .map_err(|_| ClientError::InvalidResponse("limite Expire oltre u64".into()))?;
        let response = self
            .request(Request {
                durability: wire_durability(durability),
                operation: Some(request::Operation::Expire(ExpireOperation { limit })),
                ..admin_request(request::Operation::Expire(ExpireOperation { limit }))
            })
            .await?;
        let expiration = response
            .expiration
            .ok_or_else(|| ClientError::InvalidResponse("expiration stats assenti".into()))?;
        Ok(ClientExpirationReport {
            scanned: expiration.scanned,
            expired: expiration.expired,
            stale_entries: expiration.stale_entries,
        })
    }
}

async fn connection_actor<S>(
    mut framed: Framed<S, LengthDelimitedCodec>,
    mut commands: mpsc::Receiver<Command>,
    max_frame_bytes: usize,
) where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut pending = BTreeMap::<u64, oneshot::Sender<Result<Response>>>::new();
    loop {
        tokio::select! {
            command = commands.recv() => {
                let Some(command) = command else { break; };
                let request_id = command.request.request_id;
                if pending.insert(request_id, command.response).is_some() {
                    if let Some(response) = pending.remove(&request_id) {
                        let _ = response.send(Err(ClientError::InvalidResponse(
                            "request_id duplicato nel client".into(),
                        )));
                    }
                    continue;
                }
                match encode_limited(&command.request, max_frame_bytes) {
                    Ok(bytes) => {
                        if let Err(error) = framed.send(bytes).await {
                            if let Some(response) = pending.remove(&request_id) {
                                let _ = response.send(Err(ClientError::Io(error)));
                            }
                            break;
                        }
                    }
                    Err(error) => {
                        if let Some(response) = pending.remove(&request_id) {
                            let _ = response.send(Err(ClientError::Protocol(error)));
                        }
                    }
                }
            }
            frame = framed.next() => {
                match frame {
                    Some(Ok(bytes)) => match decode_limited::<Response>(&bytes, max_frame_bytes) {
                        Ok(response) => {
                            if let Some(sender) = pending.remove(&response.request_id) {
                                let _ = sender.send(Ok(response));
                            }
                        }
                        Err(error) => {
                            fail_pending(&mut pending, || ClientError::Protocol(
                                aprodb_proto::ProtocolError::Invalid(error.to_string())
                            ));
                            break;
                        }
                    },
                    Some(Err(error)) => {
                        fail_pending(&mut pending, || ClientError::Io(std::io::Error::new(
                            error.kind(), error.to_string()
                        )));
                        break;
                    }
                    None => break,
                }
            }
        }
    }
    fail_pending(&mut pending, || ClientError::Disconnected);
}

fn fail_pending(
    pending: &mut BTreeMap<u64, oneshot::Sender<Result<Response>>>,
    error: impl Fn() -> ClientError,
) {
    for (_, sender) in std::mem::take(pending) {
        let _ = sender.send(Err(error()));
    }
}

fn wire_mutation(mutation: Mutation) -> AtomicMutation {
    let kind = match mutation {
        Mutation::Put {
            identity,
            payload,
            options,
        } => {
            let (_, _, key) = split_identity(identity);
            aprodb_proto::atomic_mutation::Kind::Put(PutOperation {
                key: Some(key),
                payload: Some(WirePayload::from(&payload)),
                content_type: options.content_type,
                metadata: options.metadata,
                expires_at_unix_ms: options.expires_at_unix_ms,
                expected: Some(wire_expected(options.expected)),
                delta: options.delta.unwrap_or_default(),
                idempotency_key_hash: options
                    .idempotency_key_hash
                    .map_or_else(Vec::new, |hash| hash.to_vec()),
            })
        }
        Mutation::Delete { identity, options } => {
            let (_, _, key) = split_identity(identity);
            aprodb_proto::atomic_mutation::Kind::Delete(DeleteOperation {
                key: Some(key),
                expected: Some(wire_expected(options.expected)),
                delta: options.delta.unwrap_or_default(),
                idempotency_key_hash: options
                    .idempotency_key_hash
                    .map_or_else(Vec::new, |hash| hash.to_vec()),
            })
        }
    };
    AtomicMutation { kind: Some(kind) }
}

fn wire_put_operation(key: Key, payload: Payload, options: PutOptions) -> PutOperation {
    PutOperation {
        key: Some(key),
        payload: Some(WirePayload::from(&payload)),
        content_type: options.content_type,
        metadata: options.metadata,
        expires_at_unix_ms: options.expires_at_unix_ms,
        expected: Some(wire_expected(options.expected)),
        delta: options.delta.unwrap_or_default(),
        idempotency_key_hash: wire_hash(options.idempotency_key_hash),
    }
}

fn split_identity(identity: RecordIdentity) -> (Vec<u8>, Vec<u8>, Key) {
    (
        identity.tenant,
        identity.namespace,
        Key {
            collection: identity.collection,
            partition: identity.partition,
            key: identity.key,
        },
    )
}

fn wire_expected(expected: Expected) -> ExpectedVersion {
    match expected {
        Expected::Any => ExpectedVersion {
            mode: ExpectedMode::Any as i32,
            version: None,
        },
        Expected::Missing => ExpectedVersion {
            mode: ExpectedMode::Missing as i32,
            version: None,
        },
        Expected::Exact(version) => ExpectedVersion {
            mode: ExpectedMode::Exact as i32,
            version: Some(version.into()),
        },
    }
}

fn wire_durability(durability: Durability) -> i32 {
    WireDurability::from(durability) as i32
}

fn wire_hash(hash: Option<[u8; 32]>) -> Vec<u8> {
    hash.map_or_else(Vec::new, |hash| hash.to_vec())
}

fn duration_ms(duration: Duration, name: &str) -> Result<u64> {
    u64::try_from(duration.as_millis())
        .map_err(|_| ClientError::InvalidResponse(format!("{name} oltre u64 millisecondi")))
}

fn wire_surface_definition(definition: SurfaceDefinition) -> Result<WireSurfaceDefinition> {
    Ok(WireSurfaceDefinition {
        id: definition.id,
        kind: match definition.kind {
            SurfaceKind::Work => WireSurfaceKind::Work as i32,
            SurfaceKind::Read => WireSurfaceKind::Read as i32,
        },
        source_tenant: definition.source_tenant,
        source_namespace: definition.source_namespace,
        source_collection: definition.source_collection,
        workflow_states: definition.workflow_states,
        format: match definition.format {
            SurfaceFormat::AprodbRecords => WireSurfaceFormat::AprodbRecords as i32,
            SurfaceFormat::Json => WireSurfaceFormat::Json as i32,
        },
        max_records: u64::try_from(definition.max_records)
            .map_err(|_| ClientError::InvalidResponse("max_records superficie oltre u64".into()))?,
        max_bytes: u64::try_from(definition.max_bytes)
            .map_err(|_| ClientError::InvalidResponse("max_bytes superficie oltre u64".into()))?,
        retained_generations: u64::try_from(definition.retained_generations).map_err(|_| {
            ClientError::InvalidResponse("retained_generations superficie oltre u64".into())
        })?,
    })
}

fn admin_or_data_request(operation: request::Operation) -> Request {
    Request {
        request_id: 0,
        deadline_unix_ms: 0,
        tenant: b"system".to_vec(),
        namespace: b"system".to_vec(),
        durability: WireDurability::Durable as i32,
        operation: Some(operation),
    }
}

fn admin_request(operation: request::Operation) -> Request {
    Request {
        tenant: Vec::new(),
        namespace: Vec::new(),
        ..admin_or_data_request(operation)
    }
}

fn one_receipt(response: Response) -> Result<MutationReceipt> {
    if response.receipts.len() != 1 {
        return Err(ClientError::InvalidResponse(format!(
            "atteso un receipt, ricevuti {}",
            response.receipts.len()
        )));
    }
    response
        .receipts
        .into_iter()
        .next()
        .expect("lunghezza verificata")
        .try_into()
        .map_err(ClientError::from)
}

fn one_workflow_result(mut response: Response) -> Result<ClientWorkflowResult> {
    let record = response
        .record
        .take()
        .ok_or_else(|| ClientError::InvalidResponse("record workflow assente".into()))?
        .try_into()
        .map_err(ClientError::from)?;
    let receipt = one_receipt(response)?;
    Ok(ClientWorkflowResult { record, receipt })
}

fn claimed_records(response: Response) -> Result<Vec<ClaimedRecord>> {
    if response.claimed_records.len() != response.receipts.len() {
        return Err(ClientError::InvalidResponse(format!(
            "Claim ha {} record e {} receipt",
            response.claimed_records.len(),
            response.receipts.len()
        )));
    }
    let server_time_unix_ms = response.server_time_unix_ms;
    let retry_after_ms = response.retry_after_ms;
    response
        .claimed_records
        .into_iter()
        .zip(response.receipts)
        .map(|(record, receipt)| {
            let record: RecordEnvelope = record.try_into().map_err(ClientError::from)?;
            let lease_id = record.workflow.lease_id.ok_or_else(|| {
                ClientError::InvalidResponse("Claim senza lease id nel record".into())
            })?;
            let lease_deadline_unix_ms =
                record.workflow.lease_deadline_unix_ms.ok_or_else(|| {
                    ClientError::InvalidResponse("Claim senza deadline lease nel record".into())
                })?;
            Ok(ClaimedRecord {
                lease: LeaseProof {
                    lease_id,
                    fencing_token: record.workflow.fencing_token,
                },
                receipt: receipt.try_into().map_err(ClientError::from)?,
                record,
                lease_deadline_unix_ms,
                server_time_unix_ms,
                retry_after_ms,
            })
        })
        .collect()
}

fn surface_generation(surface: aprodb_proto::WireSurfaceGeneration) -> Result<SurfaceRead> {
    let format = match WireSurfaceFormat::try_from(surface.format) {
        Ok(WireSurfaceFormat::AprodbRecords) => SurfaceFormat::AprodbRecords,
        Ok(WireSurfaceFormat::Json) => SurfaceFormat::Json,
        Err(_) => {
            return Err(ClientError::InvalidResponse(
                "formato superficie sconosciuto".into(),
            ));
        }
    };
    Ok(SurfaceRead {
        generation: SurfaceGeneration {
            projection_id: surface.projection_id,
            generation: surface.generation,
            source_watermarks: surface.source_watermarks,
            format,
            record_count: usize::try_from(surface.record_count).map_err(|_| {
                ClientError::InvalidResponse("record_count superficie oltre usize".into())
            })?,
            serialized: surface.serialized,
            created_at_unix_ms: surface.created_at_unix_ms,
        },
        stale_by_sequences: surface.stale_by_sequences,
        complete: surface.complete,
        errors: surface.errors,
    })
}

fn surface_build_report(build: aprodb_proto::WireSurfaceBuild) -> Result<SurfaceBuildReport> {
    Ok(SurfaceBuildReport {
        projection_id: build.projection_id,
        generation: build.generation,
        events_applied: usize::try_from(build.events_applied).map_err(|_| {
            ClientError::InvalidResponse("events_applied superficie oltre usize".into())
        })?,
        source_watermarks: build.source_watermarks,
        record_count: usize::try_from(build.record_count).map_err(|_| {
            ClientError::InvalidResponse("record_count superficie oltre usize".into())
        })?,
        serialized_bytes: usize::try_from(build.serialized_bytes).map_err(|_| {
            ClientError::InvalidResponse("serialized_bytes superficie oltre usize".into())
        })?,
    })
}

fn client_cache_metrics(
    metrics: Option<aprodb_proto::WireCacheMetrics>,
    name: &str,
) -> Result<ClientCacheMetrics> {
    let metrics = metrics
        .ok_or_else(|| ClientError::InvalidResponse(format!("cache metrics {name} assenti")))?;
    Ok(ClientCacheMetrics {
        budget_bytes: metrics.budget_bytes,
        resident_bytes: metrics.resident_bytes,
        entries: metrics.entries,
        hits: metrics.hits,
        misses: metrics.misses,
        admissions: metrics.admissions,
        rejections: metrics.rejections,
        evictions: metrics.evictions,
    })
}

fn wire_compression_policy(policy: CompressionPolicy) -> Result<WireCompressionPolicy> {
    Ok(WireCompressionPolicy {
        surface: Some(wire_compression_tier(policy.surface)?),
        hot: Some(wire_compression_tier(policy.hot)?),
        warm: Some(wire_compression_tier(policy.warm)?),
        cold: Some(wire_compression_tier(policy.cold)?),
        archive: Some(wire_compression_tier(policy.archive)?),
        skip_content_type_prefixes: policy.skip_content_type_prefixes,
    })
}

fn wire_compression_tier(policy: CompressionTierPolicy) -> Result<WireCompressionTierPolicy> {
    Ok(WireCompressionTierPolicy {
        mode: match policy.mode {
            CompressionMode::Raw => WireCompressionMode::Raw,
            CompressionMode::AdaptiveZstandard => WireCompressionMode::AdaptiveZstandard,
        } as i32,
        zstd_level: policy.zstd_level,
        min_input_bytes: u64::try_from(policy.min_input_bytes)
            .map_err(|_| ClientError::InvalidResponse("min_input oltre u64".into()))?,
        min_savings_bytes: u64::try_from(policy.min_savings_bytes)
            .map_err(|_| ClientError::InvalidResponse("min_savings oltre u64".into()))?,
        dictionary_id: policy.dictionary_id,
    })
}

fn compression_policy_from_wire(policy: WireCompressionPolicy) -> Result<CompressionPolicy> {
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
) -> Result<CompressionTierPolicy> {
    let policy = policy
        .ok_or_else(|| ClientError::InvalidResponse(format!("compression tier {name} assente")))?;
    Ok(CompressionTierPolicy {
        mode: match WireCompressionMode::try_from(policy.mode) {
            Ok(WireCompressionMode::Raw) => CompressionMode::Raw,
            Ok(WireCompressionMode::AdaptiveZstandard) => CompressionMode::AdaptiveZstandard,
            Err(_) => {
                return Err(ClientError::InvalidResponse(format!(
                    "compression mode {name} sconosciuto"
                )));
            }
        },
        zstd_level: policy.zstd_level,
        min_input_bytes: usize::try_from(policy.min_input_bytes)
            .map_err(|_| ClientError::InvalidResponse(format!("min_input {name} oltre usize")))?,
        min_savings_bytes: usize::try_from(policy.min_savings_bytes)
            .map_err(|_| ClientError::InvalidResponse(format!("min_savings {name} oltre usize")))?,
        dictionary_id: policy.dictionary_id,
    })
}

fn cost_estimate_from_wire(
    estimate: Option<aprodb_proto::WireCostEstimate>,
) -> Result<CostEstimate> {
    let estimate =
        estimate.ok_or_else(|| ClientError::InvalidResponse("cost estimate assente".into()))?;
    Ok(CostEstimate {
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
    })
}

fn unix_ms_after(duration: Duration) -> Result<u64> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| ClientError::Runtime(error.to_string()))?;
    let deadline = now
        .checked_add(duration)
        .ok_or_else(|| ClientError::Runtime("deadline oltre durata rappresentabile".into()))?;
    u64::try_from(deadline.as_millis())
        .map_err(|_| ClientError::Runtime("deadline oltre u64".into()))
}

pub struct BlockingClient {
    runtime: Runtime,
    inner: AsyncClient,
}

impl BlockingClient {
    pub fn connect_tcp(address: SocketAddr, config: ClientConfig) -> Result<Self> {
        let runtime = Runtime::new().map_err(|error| ClientError::Runtime(error.to_string()))?;
        let inner = runtime.block_on(AsyncClient::connect_tcp(address, config))?;
        Ok(Self { runtime, inner })
    }

    pub fn put(
        &self,
        identity: RecordIdentity,
        payload: Payload,
        options: PutOptions,
        durability: Durability,
    ) -> Result<MutationReceipt> {
        self.runtime
            .block_on(self.inner.put(identity, payload, options, durability))
    }

    pub fn get(&self, identity: RecordIdentity) -> Result<Option<RecordEnvelope>> {
        self.runtime.block_on(self.inner.get(identity))
    }

    pub fn delete(
        &self,
        identity: RecordIdentity,
        options: DeleteOptions,
        durability: Durability,
    ) -> Result<MutationReceipt> {
        self.runtime
            .block_on(self.inner.delete(identity, options, durability))
    }

    pub fn compare_and_swap(
        &self,
        identity: RecordIdentity,
        payload: Payload,
        expected: Version,
        options: PutOptions,
        durability: Durability,
    ) -> Result<MutationReceipt> {
        self.runtime.block_on(
            self.inner
                .compare_and_swap(identity, payload, expected, options, durability),
        )
    }

    pub fn atomic_batch(
        &self,
        mutations: Vec<Mutation>,
        durability: Durability,
    ) -> Result<Vec<MutationReceipt>> {
        self.runtime
            .block_on(self.inner.atomic_batch(mutations, durability))
    }

    pub fn append(
        &self,
        identity: RecordIdentity,
        payload: Payload,
        options: PutOptions,
        durability: Durability,
    ) -> Result<MutationReceipt> {
        self.runtime
            .block_on(self.inner.append(identity, payload, options, durability))
    }

    pub fn claim(
        &self,
        scope: WorkflowScope,
        max_records: usize,
        lease_duration: Duration,
        idempotency_key_hash: Option<[u8; 32]>,
        durability: Durability,
    ) -> Result<Vec<ClaimedRecord>> {
        self.runtime.block_on(self.inner.claim(
            scope,
            max_records,
            lease_duration,
            idempotency_key_hash,
            durability,
        ))
    }

    pub fn heartbeat(
        &self,
        identity: RecordIdentity,
        lease: LeaseProof,
        extension: Duration,
        idempotency_key_hash: Option<[u8; 32]>,
        durability: Durability,
    ) -> Result<ClientWorkflowResult> {
        self.runtime.block_on(self.inner.heartbeat(
            identity,
            lease,
            extension,
            idempotency_key_hash,
            durability,
        ))
    }

    pub fn complete(
        &self,
        identity: RecordIdentity,
        lease: LeaseProof,
        idempotency_key_hash: Option<[u8; 32]>,
        durability: Durability,
    ) -> Result<ClientWorkflowResult> {
        self.runtime.block_on(self.inner.complete(
            identity,
            lease,
            idempotency_key_hash,
            durability,
        ))
    }

    pub fn fail(
        &self,
        identity: RecordIdentity,
        lease: LeaseProof,
        permanent: bool,
        idempotency_key_hash: Option<[u8; 32]>,
        durability: Durability,
    ) -> Result<ClientWorkflowResult> {
        self.runtime.block_on(self.inner.fail(
            identity,
            lease,
            permanent,
            idempotency_key_hash,
            durability,
        ))
    }

    pub fn publish(
        &self,
        identity: RecordIdentity,
        idempotency_key_hash: Option<[u8; 32]>,
        durability: Durability,
    ) -> Result<ClientWorkflowResult> {
        self.runtime.block_on(
            self.inner
                .publish(identity, idempotency_key_hash, durability),
        )
    }

    pub fn subscribe_changes(
        &self,
        tenant: Vec<u8>,
        namespace: Vec<u8>,
        collection: Vec<u8>,
        shard: u32,
        after_sequence: u64,
        limit: usize,
    ) -> Result<ChangePage> {
        self.runtime.block_on(self.inner.subscribe_changes(
            tenant,
            namespace,
            collection,
            shard,
            after_sequence,
            limit,
        ))
    }

    pub fn create_surface(&self, definition: SurfaceDefinition) -> Result<()> {
        self.runtime.block_on(self.inner.create_surface(definition))
    }

    pub fn build_surface(
        &self,
        projection_id: impl Into<String>,
        max_events: usize,
        durability: Durability,
    ) -> Result<SurfaceBuildReport> {
        self.runtime.block_on(
            self.inner
                .build_surface(projection_id, max_events, durability),
        )
    }

    pub fn rebuild_surface(
        &self,
        projection_id: impl Into<String>,
        durability: Durability,
    ) -> Result<SurfaceBuildReport> {
        self.runtime
            .block_on(self.inner.rebuild_surface(projection_id, durability))
    }

    pub fn get_surface(
        &self,
        tenant: Vec<u8>,
        namespace: Vec<u8>,
        projection_id: impl Into<String>,
    ) -> Result<Option<SurfaceRead>> {
        self.runtime
            .block_on(self.inner.get_surface(tenant, namespace, projection_id))
    }

    pub fn sync(&self) -> Result<()> {
        self.runtime.block_on(self.inner.sync())
    }

    pub fn health(&self) -> Result<bool> {
        self.runtime.block_on(self.inner.health())
    }

    pub fn stats(&self) -> Result<Stats> {
        self.runtime.block_on(self.inner.stats())
    }

    pub fn verify(&self) -> Result<()> {
        self.runtime.block_on(self.inner.verify())
    }

    pub fn compact(&self) -> Result<()> {
        self.runtime.block_on(self.inner.compact())
    }

    pub fn audit(&self, after_sequence: Option<u64>, limit: usize) -> Result<AuditPage> {
        self.runtime
            .block_on(self.inner.audit(after_sequence, limit))
    }

    pub fn backup(&self, name: impl Into<String>) -> Result<ClientBackupInfo> {
        self.runtime.block_on(self.inner.backup(name))
    }

    pub fn shutdown(&self) -> Result<()> {
        self.runtime.block_on(self.inner.shutdown())
    }

    pub fn explain_placement(&self, identity: RecordIdentity) -> Result<Placement> {
        self.runtime
            .block_on(self.inner.explain_placement(identity))
    }

    pub fn cache_stats(&self) -> Result<ClientCacheStats> {
        self.runtime.block_on(self.inner.cache_stats())
    }

    pub fn compression_stats(&self) -> Result<ClientCompressionStats> {
        self.runtime.block_on(self.inner.compression_stats())
    }

    pub fn compression_policy(&self, collection: RecordIdentity) -> Result<CompressionPolicy> {
        self.runtime
            .block_on(self.inner.compression_policy(collection))
    }

    pub fn configure_compression(
        &self,
        collection: RecordIdentity,
        policy: CompressionPolicy,
    ) -> Result<()> {
        self.runtime
            .block_on(self.inner.configure_compression(collection, policy))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn train_dictionary(
        &self,
        collection: RecordIdentity,
        schema: impl Into<String>,
        training_samples: &[Payload],
        validation_samples: &[Payload],
        max_dictionary_bytes: usize,
        minimum_validation_gain_bytes: usize,
    ) -> Result<ClientCompressionDictionary> {
        self.runtime.block_on(self.inner.train_dictionary(
            collection,
            schema,
            training_samples,
            validation_samples,
            max_dictionary_bytes,
            minimum_validation_gain_bytes,
        ))
    }

    #[allow(clippy::too_many_arguments)]
    pub fn vector_exact(
        &self,
        tenant: Vec<u8>,
        namespace: Vec<u8>,
        collection: Vec<u8>,
        query: Vec<f32>,
        metric: VectorMetric,
        limit: usize,
        max_scan_records: usize,
        preference: ComputePreference,
    ) -> Result<ClientVectorSearchResult> {
        self.runtime.block_on(self.inner.vector_exact(
            tenant,
            namespace,
            collection,
            query,
            metric,
            limit,
            max_scan_records,
            preference,
        ))
    }

    pub fn compute_stats(&self) -> Result<ClientComputeStats> {
        self.runtime.block_on(self.inner.compute_stats())
    }

    pub fn expire(&self, limit: usize, durability: Durability) -> Result<ClientExpirationReport> {
        self.runtime.block_on(self.inner.expire(limit, durability))
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: Apache-2.0
