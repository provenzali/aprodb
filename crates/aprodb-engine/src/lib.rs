use std::{
    collections::{BTreeMap, HashMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, RecvTimeoutError, SyncSender, TrySendError},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use aprodb_compute::ComputeScheduler;
use aprodb_storage::{CommitMode, EncryptedBackend, FjallBackend, StorageBatch, StorageSpace};
use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

mod cache;
pub use cache::CacheMetrics;
use cache::{BudgetCache, CacheAdmission};
mod compression;
use compression::CompressionManager;
pub use compression::CompressionStats;

pub use aprodb_compute::{
    AcceleratorStats, ColumnarF32Batch, ComputeExecution, ComputePreference, ComputeRequest,
    CostEstimate, CpuReference, ProjectionDescriptor, SchedulerConfig, SchedulerMetrics, ScoredRow,
    VectorMetric, exact_top_k,
};
pub use aprodb_storage::{
    BackendCapabilities, BackendStats, CompactionReport, EncryptionConfig, FaultInjector,
    FaultPoint, FjallOptions, STORAGE_SPACE_COUNT, StorageBackend,
};
pub use aprodb_types::{
    AproError, AuditCursor, AuditEvent, AuditOutcome, AuditState, CatalogState, ChangeBody,
    ChangeEvent, ChangeOperation, ClaimedRecord, CollectionPolicy, CompressionCatalog,
    CompressionDictionary, CompressionMode, CompressionPolicy, CompressionTierPolicy, Durability,
    EventRetentionMode, HeadPointer, IdempotencyExpiryEntry, IdempotencyRecord, LeaseProof, Limits,
    LogicalFrameKind, MutationReceipt, Payload, PlacementExplanation, RadialDescriptor,
    RadialLayer, RadialPolicy, RadialState, RecordEnvelope, RecordIdentity, Result,
    StorageClassDescriptor, StorageMedium, SurfaceBuildReport, SurfaceDefinition, SurfaceFormat,
    SurfaceGeneration, SurfaceKind, SurfacePointer, SurfaceRead, TtlEntry, Version,
    WorkflowDescriptor, WorkflowIndexEntry, WorkflowScope, decode_logical, encode_logical,
};

const CATALOG_KEY: &[u8] = b"catalog-state";
const RADIAL_STATE_KEY: &[u8] = b"state";
const COMPRESSION_CATALOG_KEY: &[u8] = b"catalog";
const IDEMPOTENCY_FINGERPRINT_VERSION: &[u8] = b"aprodb-idempotency-v1";
const ENCRYPTION_MARKER_FILE: &str = "APRODB_ENCRYPTION";
const ENCRYPTION_MARKER: &[u8] = b"APRODB\nat-rest=xchacha20poly1305-v1\n";
const AUDIT_STATE_KEY: &[u8] = b"state";
const AUDIT_EVENT_PREFIX: &[u8] = b"event:";

#[derive(Clone, Debug)]
pub struct EngineConfig {
    pub path: PathBuf,
    pub shards: u32,
    pub durability: Durability,
    pub limits: Limits,
    pub storage: FjallOptions,
    pub group_commit_window: Duration,
    pub group_commit_max_bytes: usize,
    pub memory_budget_bytes: usize,
    pub metadata_cache_bytes: usize,
    pub object_cache_bytes: usize,
    pub compressed_cache_bytes: usize,
    pub negative_cache_bytes: usize,
    pub negative_cache_ttl: Duration,
    pub idempotency_retention: Duration,
    pub max_lease_duration: Duration,
    pub lease_recovery_safety_margin: Duration,
    pub max_claim_batch: usize,
    pub max_workflow_attempts: u32,
    pub max_active_leases: usize,
    pub max_surface_records: usize,
    pub max_surfaces: usize,
    pub max_surface_generation_bytes: usize,
    pub max_retained_surface_generations: usize,
    pub compression_channels: usize,
    pub compression_scratch_bytes: usize,
    pub max_dictionary_bytes: usize,
    pub max_dictionaries: usize,
    pub max_dictionary_training_samples: usize,
    pub max_dictionary_training_bytes: usize,
    pub compute: SchedulerConfig,
    pub encryption: Option<EncryptionConfig>,
    pub max_data_bytes: Option<u64>,
    pub min_free_disk_bytes: u64,
    pub max_compaction_temporary_bytes: u64,
}

impl EngineConfig {
    #[must_use]
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let mut config = Self {
            path: path.into(),
            shards: 16,
            durability: Durability::Durable,
            limits: Limits::default(),
            storage: FjallOptions::default(),
            group_commit_window: Duration::ZERO,
            group_commit_max_bytes: 4 * 1024 * 1024,
            memory_budget_bytes: 256 * 1024 * 1024,
            metadata_cache_bytes: 8 * 1024 * 1024,
            object_cache_bytes: 32 * 1024 * 1024,
            compressed_cache_bytes: 16 * 1024 * 1024,
            negative_cache_bytes: 2 * 1024 * 1024,
            negative_cache_ttl: Duration::from_secs(2),
            idempotency_retention: Duration::from_secs(24 * 60 * 60),
            max_lease_duration: Duration::from_secs(15 * 60),
            lease_recovery_safety_margin: Duration::from_secs(5),
            max_claim_batch: 128,
            max_workflow_attempts: 5,
            max_active_leases: 100_000,
            max_surface_records: 100_000,
            max_surfaces: 256,
            max_surface_generation_bytes: 64 * 1024 * 1024,
            max_retained_surface_generations: 16,
            compression_channels: std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1)
                .next_power_of_two()
                .min(16),
            compression_scratch_bytes: 32 * 1024 * 1024,
            max_dictionary_bytes: 128 * 1024,
            max_dictionaries: 1024,
            max_dictionary_training_samples: 4096,
            max_dictionary_training_bytes: 64 * 1024 * 1024,
            compute: SchedulerConfig::default(),
            encryption: None,
            max_data_bytes: None,
            min_free_disk_bytes: 0,
            max_compaction_temporary_bytes: 64 * 1024 * 1024 * 1024,
        };
        config
            .apply_memory_budget(256 * 1024 * 1024)
            .expect("the default budget is valid");
        config
    }

    pub fn apply_memory_budget(&mut self, budget_bytes: usize) -> Result<()> {
        const MINIMUM: usize = 128 * 1024 * 1024;
        if budget_bytes < MINIMUM {
            return Err(AproError::InvalidInput(format!(
                "memory budget {budget_bytes} is below the minimum {MINIMUM}"
            )));
        }
        self.memory_budget_bytes = budget_bytes;
        self.storage.cache_bytes = u64::try_from(percent(budget_bytes, 15)).unwrap_or(u64::MAX);
        self.storage.max_memtable_bytes =
            u64::try_from((percent(budget_bytes, 10) / STORAGE_SPACE_COUNT).max(1024 * 1024))
                .unwrap_or(u64::MAX);
        self.limits.max_inflight_bytes = percent(budget_bytes, 10);
        self.metadata_cache_bytes = percent(budget_bytes, 20);
        self.object_cache_bytes = percent(budget_bytes, 20);
        self.compressed_cache_bytes = percent(budget_bytes, 8);
        self.negative_cache_bytes = percent(budget_bytes, 2);
        self.compression_scratch_bytes = percent(budget_bytes, 12);
        self.compute.queue_byte_budget = percent(budget_bytes, 3);
        self.compute.max_batch_bytes = self
            .compute
            .max_batch_bytes
            .min(self.compute.queue_byte_budget);
        self.limits.max_batch_bytes = self
            .limits
            .max_batch_bytes
            .min(self.limits.max_inflight_bytes);
        self.limits.max_record_bytes = self
            .limits
            .max_record_bytes
            .min(self.limits.max_batch_bytes / 2);
        self.max_surface_generation_bytes = self
            .max_surface_generation_bytes
            .min(self.limits.max_batch_bytes);
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ExpectedVersion {
    Any,
    Missing,
    Exact(Version),
}

#[derive(Clone, Debug, Serialize)]
pub struct PutRequest {
    pub identity: RecordIdentity,
    pub payload: Payload,
    pub content_type: String,
    pub metadata: BTreeMap<String, Vec<u8>>,
    pub expires_at_unix_ms: Option<u64>,
    pub idempotency_key_hash: Option<[u8; 32]>,
    pub expected: ExpectedVersion,
    pub delta: Option<Vec<u8>>,
    operation: ChangeOperation,
    workflow_override: Option<WorkflowDescriptor>,
}

impl PutRequest {
    #[must_use]
    pub fn new(identity: RecordIdentity, payload: Payload) -> Self {
        Self {
            identity,
            payload,
            content_type: "application/octet-stream".into(),
            metadata: BTreeMap::new(),
            expires_at_unix_ms: None,
            idempotency_key_hash: None,
            expected: ExpectedVersion::Any,
            delta: None,
            operation: ChangeOperation::Put,
            workflow_override: None,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct DeleteRequest {
    pub identity: RecordIdentity,
    pub expected: ExpectedVersion,
    pub idempotency_key_hash: Option<[u8; 32]>,
    pub delta: Option<Vec<u8>>,
}

impl DeleteRequest {
    #[must_use]
    pub fn new(identity: RecordIdentity) -> Self {
        Self {
            identity,
            expected: ExpectedVersion::Any,
            idempotency_key_hash: None,
            delta: None,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub enum AtomicMutation {
    Put(PutRequest),
    Delete(DeleteRequest),
}

#[derive(Clone, Debug, Serialize)]
pub struct ClaimRequest {
    pub scope: WorkflowScope,
    pub max_records: usize,
    pub lease_duration: Duration,
    pub idempotency_key_hash: Option<[u8; 32]>,
    pub durability: Durability,
}

#[derive(Clone, Debug)]
pub struct WorkflowMutationResult {
    pub record: RecordEnvelope,
    pub receipt: MutationReceipt,
}

impl AtomicMutation {
    fn identity(&self) -> &RecordIdentity {
        match self {
            Self::Put(request) => &request.identity,
            Self::Delete(request) => &request.identity,
        }
    }

    const fn expected(&self) -> ExpectedVersion {
        match self {
            Self::Put(request) => request.expected,
            Self::Delete(request) => request.expected,
        }
    }

    const fn idempotency_key_hash(&self) -> Option<[u8; 32]> {
        match self {
            Self::Put(request) => request.idempotency_key_hash,
            Self::Delete(request) => request.idempotency_key_hash,
        }
    }
}

#[derive(Clone, Debug)]
struct IdempotencyContext {
    scope: Vec<u8>,
    key_hash: [u8; 32],
    request_fingerprint: [u8; 32],
}

struct WorkflowCommit {
    current: RecordEnvelope,
    workflow: WorkflowDescriptor,
    operation: ChangeOperation,
    idempotency_key_hash: Option<[u8; 32]>,
    durability: Durability,
    idempotency: Option<IdempotencyContext>,
}

#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct VerificationReport {
    pub heads_checked: usize,
    pub events_checked: usize,
    pub surfaces_checked: usize,
    pub dictionaries_checked: usize,
    pub audit_events_checked: usize,
    pub max_sequence_by_shard: BTreeMap<u32, u64>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct CheckpointInfo {
    pub path: PathBuf,
    pub entries: usize,
    pub logical_bytes: u64,
    pub durable_watermarks: BTreeMap<u32, u64>,
    pub catalog_generation: u64,
    pub backend: String,
    pub logical_format: u32,
    pub software_version: String,
    pub encryption_key_ids: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackupFile {
    pub relative_path: String,
    pub bytes: u64,
    pub blake3: String,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackupManifest {
    pub manifest_version: u32,
    pub software_version: String,
    pub logical_format: u32,
    pub backend: String,
    pub created_at_unix_ms: u64,
    pub catalog_generation: u64,
    pub durable_watermarks: BTreeMap<u32, u64>,
    pub entries: usize,
    pub logical_bytes: u64,
    pub encrypted: bool,
    pub encryption_key_ids: Vec<String>,
    pub verification: VerificationReport,
    pub files: Vec<BackupFile>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct BackupInfo {
    pub path: PathBuf,
    pub manifest: BackupManifest,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RestoreReport {
    pub path: PathBuf,
    pub files_restored: usize,
    pub bytes_restored: u64,
    pub verification: VerificationReport,
}

pub const REPAIR_DERIVED_CONFIRMATION: &str = "REBUILD_DERIVED_ON_SEPARATE_COPY";

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RepairReport {
    pub destination: PathBuf,
    pub heads_scanned: usize,
    pub radial_rebuilt: usize,
    pub ttl_rebuilt: usize,
    pub workflow_rebuilt: usize,
    pub idempotency_expiry_rebuilt: usize,
    pub surfaces_rebuilt: usize,
    pub records_lost: usize,
    pub records_doubtful: usize,
    pub radial_hints_reset: usize,
    pub verification: VerificationReport,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AuditPage {
    pub events: Vec<AuditEvent>,
    pub next: Option<AuditCursor>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GarbageCollectionReport {
    pub safe_watermark: u64,
    pub events_deleted: usize,
    pub versions_deleted: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CacheStats {
    pub metadata: CacheMetrics,
    pub objects: CacheMetrics,
    pub compressed: CacheMetrics,
    pub negative: CacheMetrics,
}

#[derive(Clone)]
struct CompressedCacheEntry {
    version: Version,
    bytes: Vec<u8>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ExpirationReport {
    pub scanned: usize,
    pub expired: usize,
    pub stale_entries: usize,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct IdempotencyPurgeReport {
    pub scanned: usize,
    pub records_deleted: usize,
    pub stale_entries: usize,
}

#[derive(Clone, Debug)]
pub struct VectorSearchRequest {
    pub tenant: Vec<u8>,
    pub namespace: Vec<u8>,
    pub collection: Vec<u8>,
    pub query: Vec<f32>,
    pub metric: VectorMetric,
    pub limit: usize,
    pub max_scan_records: usize,
    pub preference: ComputePreference,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VectorSearchHit {
    pub identity: RecordIdentity,
    pub version: Version,
    pub score: f32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VectorSearchResult {
    pub hits: Vec<VectorSearchHit>,
    pub scanned_records: usize,
    pub vector_candidates: usize,
    pub execution: ComputeExecution,
    pub accelerator: Option<String>,
    pub estimate: CostEstimate,
    pub fallback_reason: Option<String>,
}

pub struct Engine {
    config: EngineConfig,
    backend: Arc<dyn StorageBackend>,
    catalog: RwLock<CatalogState>,
    shard_writers: Vec<Mutex<()>>,
    durable_watermarks: Vec<AtomicU64>,
    inflight_bytes: AtomicUsize,
    catalog_writer: Mutex<()>,
    group_commit: Option<GroupCommitter>,
    poisoned: Arc<AtomicBool>,
    radial_state: RwLock<RadialState>,
    compression_catalog: RwLock<CompressionCatalog>,
    compression_manager: CompressionManager,
    compute_scheduler: ComputeScheduler,
    compression_writer: Mutex<()>,
    dictionaries: RwLock<HashMap<u64, Arc<CompressionDictionary>>>,
    metadata_cache: BudgetCache<RadialDescriptor>,
    object_cache: BudgetCache<RecordEnvelope>,
    compressed_cache: BudgetCache<CompressedCacheEntry>,
    negative_cache: BudgetCache<u64>,
    surface_writer: Mutex<()>,
    lease_deadlines: Mutex<HashMap<(RecordIdentity, [u8; 16]), Instant>>,
    audit_writer: Mutex<()>,
    audit_sequence: AtomicU64,
}

impl Engine {
    pub fn open(config: EngineConfig) -> Result<Self> {
        validate_config(&config)?;
        if config.path.join("aprodb.wal").exists() || config.path.join("aprodb.snapshot").exists() {
            return Err(AproError::IncompatibleFormat(
                "AProDB 0.1 directory detected: use one-shot import only on a duplicate copy"
                    .into(),
            ));
        }
        let backend = open_storage_backend(&config, &config.path)?;
        Self::with_backend(config, backend)
    }

    pub fn with_backend(config: EngineConfig, backend: Arc<dyn StorageBackend>) -> Result<Self> {
        validate_config(&config)?;
        let catalog = match backend.get(StorageSpace::Catalog, CATALOG_KEY)? {
            Some(bytes) => {
                let catalog: CatalogState = decode_logical(LogicalFrameKind::Catalog, &bytes)?;
                if catalog.format_version != 1 {
                    return Err(AproError::IncompatibleFormat(format!(
                        "unsupported logical catalog version {}",
                        catalog.format_version
                    )));
                }
                if catalog.backend != backend.name() {
                    return Err(AproError::IncompatibleFormat(format!(
                        "catalog backend '{}' does not match adapter '{}'",
                        catalog.backend,
                        backend.name()
                    )));
                }
                if catalog.shard_sequences.len() != config.shards as usize {
                    return Err(AproError::IncompatibleFormat(format!(
                        "catalog contains {} shards, but configuration specifies {}",
                        catalog.shard_sequences.len(),
                        config.shards
                    )));
                }
                catalog
            }
            None => {
                let catalog = CatalogState::empty(backend.name(), config.shards);
                let mut batch = StorageBatch::with_capacity(1);
                batch.put(
                    StorageSpace::Catalog,
                    CATALOG_KEY.to_vec(),
                    encode_logical(LogicalFrameKind::Catalog, &catalog)?,
                );
                backend.commit(batch, CommitMode::Durable)?;
                catalog
            }
        };
        let durable_watermarks = (0..config.shards)
            .map(|shard| {
                AtomicU64::new(catalog.durable_watermarks.get(&shard).copied().unwrap_or(0))
            })
            .collect();
        let radial_state = match backend.get(StorageSpace::Radial, RADIAL_STATE_KEY)? {
            Some(bytes) => {
                let state: RadialState = decode_logical(LogicalFrameKind::RadialState, &bytes)?;
                if state.format_version != 1 {
                    return Err(AproError::IncompatibleFormat(format!(
                        "unsupported radial state version {}",
                        state.format_version
                    )));
                }
                state
            }
            None => RadialState::default(),
        };
        let compression_catalog =
            match backend.get(StorageSpace::Compression, COMPRESSION_CATALOG_KEY)? {
                Some(bytes) => {
                    let catalog: CompressionCatalog =
                        decode_logical(LogicalFrameKind::CompressionCatalog, &bytes)?;
                    if catalog.format_version != 1 {
                        return Err(AproError::IncompatibleFormat(format!(
                            "unsupported compression catalog version {}",
                            catalog.format_version
                        )));
                    }
                    catalog
                }
                None => {
                    let catalog = CompressionCatalog::default();
                    let mut batch = StorageBatch::with_capacity(1);
                    batch.put(
                        StorageSpace::Compression,
                        COMPRESSION_CATALOG_KEY.to_vec(),
                        encode_logical(LogicalFrameKind::CompressionCatalog, &catalog)?,
                    );
                    backend.commit(batch, CommitMode::Durable)?;
                    catalog
                }
            };
        let audit_sequence = match backend.get(StorageSpace::Audit, AUDIT_STATE_KEY)? {
            Some(bytes) => {
                let state: AuditState = decode_logical(LogicalFrameKind::AuditState, &bytes)?;
                if state.format_version != 1 {
                    return Err(AproError::IncompatibleFormat(format!(
                        "unsupported audit state version {}",
                        state.format_version
                    )));
                }
                state.last_sequence
            }
            None => 0,
        };
        let poisoned = Arc::new(AtomicBool::new(false));
        let group_commit = if config.group_commit_window.is_zero() {
            None
        } else {
            Some(GroupCommitter::new(
                Arc::clone(&backend),
                config.group_commit_window,
                config.group_commit_max_bytes,
                config.limits.max_queue_depth,
                Arc::clone(&poisoned),
            )?)
        };
        let metadata_cache_bytes = config.metadata_cache_bytes;
        let object_cache_bytes = config.object_cache_bytes;
        let compressed_cache_bytes = config.compressed_cache_bytes;
        let negative_cache_bytes = config.negative_cache_bytes;
        let compression_manager = CompressionManager::new(
            config.compression_channels,
            config.compression_scratch_bytes,
        )?;
        let compute_scheduler = ComputeScheduler::new(config.compute.clone())?;
        Ok(Self {
            shard_writers: (0..config.shards).map(|_| Mutex::new(())).collect(),
            durable_watermarks,
            config,
            backend,
            catalog: RwLock::new(catalog),
            inflight_bytes: AtomicUsize::new(0),
            catalog_writer: Mutex::new(()),
            group_commit,
            poisoned,
            radial_state: RwLock::new(radial_state),
            compression_catalog: RwLock::new(compression_catalog),
            compression_manager,
            compute_scheduler,
            compression_writer: Mutex::new(()),
            dictionaries: RwLock::new(HashMap::new()),
            metadata_cache: BudgetCache::new(metadata_cache_bytes),
            object_cache: BudgetCache::new(object_cache_bytes),
            compressed_cache: BudgetCache::new(compressed_cache_bytes),
            negative_cache: BudgetCache::new(negative_cache_bytes),
            surface_writer: Mutex::new(()),
            lease_deadlines: Mutex::new(HashMap::new()),
            audit_writer: Mutex::new(()),
            audit_sequence: AtomicU64::new(audit_sequence),
        })
    }

    #[must_use]
    pub fn capabilities(&self) -> BackendCapabilities {
        self.backend.capabilities()
    }

    pub fn configure_collection(
        &self,
        identity: &RecordIdentity,
        policy: CollectionPolicy,
    ) -> Result<()> {
        self.ensure_healthy()?;
        identity.validate(&self.config.limits)?;
        if policy.max_self_contained_event_bytes > self.config.limits.max_record_bytes {
            return Err(AproError::ResourceLimit(
                "SelfContained event size limit exceeds maximum record size limit".into(),
            ));
        }
        let _catalog_writer = self.catalog_writer.lock();
        let mut catalog = self.catalog.write();
        let mut updated = catalog.clone();
        updated.generation = updated.generation.saturating_add(1);
        updated
            .collections
            .insert(identity.collection_key(), policy);
        let mut batch = StorageBatch::with_capacity(1);
        batch.put(
            StorageSpace::Catalog,
            CATALOG_KEY.to_vec(),
            encode_logical(LogicalFrameKind::Catalog, &updated)?,
        );
        self.commit_primary(batch, CommitMode::Durable)?;
        *catalog = updated;
        Ok(())
    }

    pub fn configure_compression_policy(
        &self,
        collection: &RecordIdentity,
        policy: CompressionPolicy,
    ) -> Result<()> {
        self.ensure_healthy()?;
        collection.validate(&self.config.limits)?;
        validate_compression_policy(&policy, &self.config)?;
        for dictionary_id in compression_policy_dictionary_ids(&policy) {
            let dictionary = self.load_dictionary(dictionary_id)?;
            if dictionary.tenant != collection.tenant
                || dictionary.namespace != collection.namespace
                || dictionary.collection != collection.collection
            {
                return Err(AproError::InvalidInput(format!(
                    "dictionary {dictionary_id} belongs to a different collection"
                )));
            }
        }
        let _writer = self.compression_writer.lock();
        let mut updated = self.compression_catalog.read().clone();
        updated.generation = updated.generation.saturating_add(1);
        updated.policies.insert(collection.collection_key(), policy);
        let mut batch = StorageBatch::with_capacity(1);
        batch.put(
            StorageSpace::Compression,
            COMPRESSION_CATALOG_KEY.to_vec(),
            encode_logical(LogicalFrameKind::CompressionCatalog, &updated)?,
        );
        self.commit_primary(batch, CommitMode::Durable)?;
        *self.compression_catalog.write() = updated;
        Ok(())
    }

    #[must_use]
    pub fn compression_stats(&self) -> CompressionStats {
        self.compression_manager.stats()
    }

    pub fn compression_policy(&self, collection: &RecordIdentity) -> Result<CompressionPolicy> {
        self.ensure_healthy()?;
        collection.validate(&self.config.limits)?;
        Ok(self
            .compression_catalog
            .read()
            .policies
            .get(&collection.collection_key())
            .cloned()
            .unwrap_or_default())
    }

    pub fn train_and_activate_dictionary(
        &self,
        collection: &RecordIdentity,
        schema: impl Into<String>,
        training_samples: &[Payload],
        validation_samples: &[Payload],
        max_dictionary_bytes: usize,
        minimum_validation_gain_bytes: usize,
    ) -> Result<CompressionDictionary> {
        self.ensure_healthy()?;
        collection.validate(&self.config.limits)?;
        let schema = schema.into();
        if schema.is_empty() || schema.len() > 255 {
            return Err(AproError::InvalidInput(
                "dictionary schema must contain 1..255 bytes".into(),
            ));
        }
        if training_samples.len() < 8
            || training_samples.len() > self.config.max_dictionary_training_samples
            || validation_samples.is_empty()
            || validation_samples.len() > self.config.max_dictionary_training_samples
            || max_dictionary_bytes < 1024
            || max_dictionary_bytes > self.config.max_dictionary_bytes
        {
            return Err(AproError::ResourceLimit(
                "samples or dictionary size out of configured limits".into(),
            ));
        }
        let training = encode_dictionary_samples(training_samples)?;
        let validation = encode_dictionary_samples(validation_samples)?;
        let training_bytes = training
            .iter()
            .chain(&validation)
            .map(Vec::len)
            .try_fold(0usize, |total, len| total.checked_add(len))
            .ok_or_else(|| AproError::ResourceLimit("training bytes exceed usize".into()))?;
        if training_bytes > self.config.max_dictionary_training_bytes {
            return Err(AproError::ResourceLimit(format!(
                "dictionary training uses {training_bytes} bytes, maximum {}",
                self.config.max_dictionary_training_bytes
            )));
        }
        let dictionary_bytes = zstd::dict::from_samples(&training, max_dictionary_bytes)
            .map_err(|error| AproError::InvalidInput(format!("training Zstandard: {error}")))?;
        if dictionary_bytes.is_empty() || dictionary_bytes.len() > max_dictionary_bytes {
            return Err(AproError::Corrupt(
                "Zstandard trainer produced an invalid size".into(),
            ));
        }
        let level = CompressionPolicy::default().warm.zstd_level;
        let mut raw_bytes = 0usize;
        let mut without_dictionary_bytes = 0usize;
        let mut with_dictionary_bytes = 0usize;
        for (index, sample) in validation.iter().enumerate() {
            raw_bytes = raw_bytes.saturating_add(sample.len());
            without_dictionary_bytes = without_dictionary_bytes.saturating_add(
                self.compression_manager
                    .compressed_size(sample, level, None, index as u64)?,
            );
            with_dictionary_bytes =
                with_dictionary_bytes.saturating_add(self.compression_manager.compressed_size(
                    sample,
                    level,
                    Some(&dictionary_bytes),
                    index as u64,
                )?);
        }
        if with_dictionary_bytes.saturating_add(minimum_validation_gain_bytes)
            >= without_dictionary_bytes
        {
            return Err(AproError::InvalidInput(format!(
                "dictionary not published: {with_dictionary_bytes} bytes with dictionary, {without_dictionary_bytes} without"
            )));
        }

        let _writer = self.compression_writer.lock();
        let mut catalog = self.compression_catalog.read().clone();
        if usize::try_from(catalog.next_dictionary_id.saturating_sub(1)).unwrap_or(usize::MAX)
            >= self.config.max_dictionaries
        {
            return Err(AproError::ResourceLimit(
                "configured dictionary limit reached".into(),
            ));
        }
        let id = catalog.next_dictionary_id;
        catalog.next_dictionary_id = catalog
            .next_dictionary_id
            .checked_add(1)
            .ok_or_else(|| AproError::ResourceLimit("dictionary id exhausted".into()))?;
        let dictionary = CompressionDictionary {
            id,
            tenant: collection.tenant.clone(),
            namespace: collection.namespace.clone(),
            collection: collection.collection.clone(),
            schema,
            checksum: crc32fast::hash(&dictionary_bytes),
            bytes: dictionary_bytes,
            created_at_unix_ms: now_unix_ms()?,
            validation_raw_bytes: u64::try_from(raw_bytes).unwrap_or(u64::MAX),
            validation_without_dictionary_bytes: u64::try_from(without_dictionary_bytes)
                .unwrap_or(u64::MAX),
            validation_with_dictionary_bytes: u64::try_from(with_dictionary_bytes)
                .unwrap_or(u64::MAX),
        };
        let collection_key = collection.collection_key();
        let policy = catalog.policies.entry(collection_key).or_default();
        for tier in [
            &mut policy.hot,
            &mut policy.warm,
            &mut policy.cold,
            &mut policy.archive,
        ] {
            if tier.mode == CompressionMode::AdaptiveZstandard {
                tier.dictionary_id = Some(id);
            }
        }
        catalog.generation = catalog.generation.saturating_add(1);
        let mut batch = StorageBatch::with_capacity(2);
        batch.put(
            StorageSpace::Compression,
            compression_dictionary_key(id),
            encode_logical(LogicalFrameKind::CompressionDictionary, &dictionary)?,
        );
        batch.put(
            StorageSpace::Compression,
            COMPRESSION_CATALOG_KEY.to_vec(),
            encode_logical(LogicalFrameKind::CompressionCatalog, &catalog)?,
        );
        self.commit_primary(batch, CommitMode::Durable)?;
        self.dictionaries
            .write()
            .insert(id, Arc::new(dictionary.clone()));
        *self.compression_catalog.write() = catalog;
        Ok(dictionary)
    }

    pub fn configure_radial_policy(
        &self,
        collection: &RecordIdentity,
        policy: RadialPolicy,
    ) -> Result<()> {
        self.ensure_healthy()?;
        collection.validate(&self.config.limits)?;
        validate_radial_policy(&policy)?;
        let _catalog_writer = self.catalog_writer.lock();
        let mut updated = self.radial_state.read().clone();
        updated.generation = updated.generation.saturating_add(1);
        updated.policies.insert(collection.collection_key(), policy);
        let mut batch = StorageBatch::with_capacity(1);
        batch.put(
            StorageSpace::Radial,
            RADIAL_STATE_KEY.to_vec(),
            encode_logical(LogicalFrameKind::RadialState, &updated)?,
        );
        self.commit_primary(batch, CommitMode::Durable)?;
        *self.radial_state.write() = updated;
        Ok(())
    }

    pub fn register_storage_class(&self, descriptor: StorageClassDescriptor) -> Result<()> {
        self.ensure_healthy()?;
        validate_storage_class(&descriptor)?;
        if descriptor
            .path
            .as_ref()
            .is_some_and(|path| Path::new(path) != self.config.path)
        {
            return Err(AproError::Unsupported(
                "Fjall does not expose physical tiering across directories; use logical classes/projections"
                    .into(),
            ));
        }
        let _catalog_writer = self.catalog_writer.lock();
        let mut updated = self.radial_state.read().clone();
        updated.generation = updated.generation.saturating_add(1);
        updated
            .storage_classes
            .insert(descriptor.name.clone(), descriptor);
        let mut batch = StorageBatch::with_capacity(1);
        batch.put(
            StorageSpace::Radial,
            RADIAL_STATE_KEY.to_vec(),
            encode_logical(LogicalFrameKind::RadialState, &updated)?,
        );
        self.commit_primary(batch, CommitMode::Durable)?;
        *self.radial_state.write() = updated;
        Ok(())
    }

    #[must_use]
    pub fn storage_classes(&self) -> Vec<StorageClassDescriptor> {
        self.radial_state
            .read()
            .storage_classes
            .values()
            .cloned()
            .collect()
    }

    pub fn set_radial_signals(
        &self,
        identity: &RecordIdentity,
        urgency_millis: u16,
        pin_until_unix_ms: Option<u64>,
        reconstruction_cost_micros: u64,
    ) -> Result<()> {
        self.ensure_healthy()?;
        identity.validate(&self.config.limits)?;
        if urgency_millis > 1000 {
            return Err(AproError::InvalidInput(
                "radial urgency exceeds 1000".into(),
            ));
        }
        let shard = self.shard_for_partition(&identity.partition_key());
        let _writer = self.shard_writers[shard as usize].lock();
        let head = self
            .current_head(identity)?
            .ok_or_else(|| AproError::InvalidInput("record not found".into()))?;
        if head.tombstone {
            return Err(AproError::InvalidInput("record deleted".into()));
        }
        let mut descriptor = self
            .load_radial_descriptor(identity)?
            .ok_or_else(|| AproError::Corrupt("radial descriptor for record missing".into()))?;
        if descriptor.canonical_version != head.version {
            return Err(AproError::Corrupt(
                "radial descriptor refers to obsolete version".into(),
            ));
        }
        descriptor.urgency_millis = urgency_millis;
        descriptor.admin_pin_until_unix_ms = pin_until_unix_ms;
        descriptor.reconstruction_cost_micros = reconstruction_cost_micros;
        descriptor.last_decision = "administrative signals updated".into();
        let mut batch = StorageBatch::with_capacity(1);
        batch.put(
            StorageSpace::Radial,
            radial_key(identity),
            encode_logical(LogicalFrameKind::Radial, &descriptor)?,
        );
        self.commit_primary(batch, CommitMode::Durable)?;
        self.metadata_cache.invalidate(identity);
        Ok(())
    }

    pub fn explain_placement(
        &self,
        identity: &RecordIdentity,
        at_unix_ms: u64,
    ) -> Result<PlacementExplanation> {
        self.ensure_healthy()?;
        identity.validate(&self.config.limits)?;
        let descriptor = self
            .load_radial_descriptor(identity)?
            .ok_or_else(|| AproError::InvalidInput("radial descriptor not present".into()))?;
        let policy = self
            .radial_state
            .read()
            .policies
            .get(&identity.collection_key())
            .cloned()
            .unwrap_or_default();
        let freshness = freshness_millis(
            descriptor.updated_at_unix_ms,
            at_unix_ms,
            policy.freshness_half_life_ms,
        );
        let score = radial_score_millis(freshness, descriptor.urgency_millis, &policy);
        let pinned = descriptor
            .admin_pin_until_unix_ms
            .is_some_and(|until| until > at_unix_ms);
        let expired = descriptor
            .deadline_unix_ms
            .is_some_and(|deadline| deadline <= at_unix_ms);
        let mut recommended_layer = if expired {
            RadialLayer::Archive
        } else if pinned || score >= policy.promotion_threshold_millis {
            RadialLayer::Hot
        } else if score <= policy.demotion_threshold_millis {
            RadialLayer::Cold
        } else {
            RadialLayer::Warm
        };
        let mut reasons = vec![format!(
            "freshness={freshness}/1000, urgency={}/1000",
            descriptor.urgency_millis
        )];
        if pinned {
            reasons.push("administrative pin active".into());
        }
        if expired {
            reasons.push("TTL expired".into());
        }
        if recommended_layer != descriptor.layer
            && at_unix_ms.saturating_sub(descriptor.layer_since_unix_ms)
                < policy.minimum_residency_ms
            && !pinned
            && !expired
        {
            reasons.push("minimum residency prevents immediate migration".into());
            recommended_layer = descriptor.layer;
        }
        Ok(PlacementExplanation {
            canonical_version: descriptor.canonical_version,
            radial_score_millis: score,
            freshness_millis: freshness,
            urgency_millis: descriptor.urgency_millis,
            current_layer: descriptor.layer,
            recommended_layer,
            storage_class: descriptor.storage_class,
            pinned,
            object_cache_resident: self.object_cache.is_resident(identity, at_unix_ms),
            physical_tiering_supported: self.backend.capabilities().physical_storage_tiering,
            reasons,
        })
    }

    #[must_use]
    pub fn cache_stats(&self) -> CacheStats {
        CacheStats {
            metadata: self.metadata_cache.metrics(),
            objects: self.object_cache.metrics(),
            compressed: self.compressed_cache.metrics(),
            negative: self.negative_cache.metrics(),
        }
    }

    pub fn create_surface(&self, definition: SurfaceDefinition) -> Result<()> {
        self.ensure_healthy()?;
        validate_surface_definition(&definition, &self.config)?;
        self.ensure_surface_retention(&definition)?;
        let _surface_writer = self.surface_writer.lock();
        if let Some(existing) = self.load_surface_definition(&definition.id)? {
            return if existing == definition {
                Ok(())
            } else {
                Err(AproError::Conflict(format!(
                    "projection id {} already configured differently",
                    definition.id
                )))
            };
        }
        if self
            .backend
            .scan_prefix(
                StorageSpace::Surfaces,
                b"d",
                self.config.max_surfaces.saturating_add(1),
            )?
            .len()
            >= self.config.max_surfaces
        {
            return Err(AproError::ResourceLimit(format!(
                "limit of {} surfaces reached",
                self.config.max_surfaces
            )));
        }
        let _catalog_writer = self.catalog_writer.lock();
        let mut catalog = self.catalog.read().clone();
        let collection_key = surface_collection_key(&definition);
        let consumer = surface_consumer_name(&definition.id);
        let policy = catalog
            .collections
            .entry(collection_key.clone())
            .or_default();
        if !policy
            .required_consumers
            .iter()
            .any(|name| name == &consumer)
        {
            policy.required_consumers.push(consumer.clone());
            policy.required_consumers.sort();
        }
        let source_watermarks = (0..self.config.shards).map(|shard| (shard, 0)).collect();
        let pointer = SurfacePointer {
            projection_id: definition.id.clone(),
            current_generation: None,
            next_generation: 1,
            source_watermarks,
            retained_generations: Vec::new(),
        };
        for shard in 0..self.config.shards {
            catalog.consumer_watermarks.insert(
                consumer_watermark_key(&collection_key, shard, &consumer)?,
                0,
            );
        }
        catalog.generation = catalog.generation.saturating_add(1);
        let mut batch = StorageBatch::with_capacity(3);
        batch.put(
            StorageSpace::Surfaces,
            surface_definition_key(&definition.id),
            encode_logical(LogicalFrameKind::SurfaceDefinition, &definition)?,
        );
        batch.put(
            StorageSpace::Surfaces,
            surface_pointer_key(&definition.id),
            encode_logical(LogicalFrameKind::SurfacePointer, &pointer)?,
        );
        batch.put(
            StorageSpace::Catalog,
            CATALOG_KEY.to_vec(),
            encode_logical(LogicalFrameKind::Catalog, &catalog)?,
        );
        self.commit_primary(batch, CommitMode::Durable)?;
        *self.catalog.write() = catalog;
        Ok(())
    }

    pub fn build_surface_incremental(
        &self,
        projection_id: &str,
        max_events: usize,
        durability: Durability,
    ) -> Result<SurfaceBuildReport> {
        self.ensure_healthy()?;
        if max_events == 0 || max_events > self.config.limits.max_queue_depth {
            return Err(AproError::ResourceLimit(format!(
                "surface builder requires 1..{} events",
                self.config.limits.max_queue_depth
            )));
        }
        let _surface_writer = self.surface_writer.lock();
        let definition = self
            .load_surface_definition(projection_id)?
            .ok_or_else(|| AproError::InvalidInput("projection not configured".into()))?;
        let pointer = self.load_surface_pointer(projection_id)?;
        let mut records = self.surface_records(&definition, &pointer)?;
        let mut watermarks = pointer.source_watermarks.clone();
        let mut events_applied = 0usize;
        let mut remaining = max_events;
        for shard in 0..self.config.shards {
            if remaining == 0 {
                break;
            }
            let after = watermarks.get(&shard).copied().unwrap_or(0);
            let events = self.changes(shard, after, remaining)?;
            if let Some(last) = events.last() {
                watermarks.insert(shard, last.version.sequence);
            }
            remaining = remaining.saturating_sub(events.len().min(remaining));
            for event in events {
                if event_matches_surface(&event, &definition) {
                    self.apply_surface_event(&definition, &mut records, &event)?;
                    events_applied = events_applied.saturating_add(1);
                }
            }
        }
        if events_applied == 0
            && pointer.current_generation.is_some()
            && watermarks == pointer.source_watermarks
        {
            let generation = self
                .get_surface(projection_id)?
                .ok_or_else(|| AproError::Corrupt("surface pointer without generation".into()))?;
            return Ok(SurfaceBuildReport {
                projection_id: projection_id.into(),
                generation: generation.generation,
                events_applied: 0,
                source_watermarks: generation.source_watermarks,
                record_count: generation.record_count,
                serialized_bytes: generation.serialized.len(),
            });
        }
        self.publish_surface_generation(
            &definition,
            pointer,
            records,
            watermarks,
            events_applied,
            durability,
        )
    }

    pub fn rebuild_surface(
        &self,
        projection_id: &str,
        durability: Durability,
    ) -> Result<SurfaceBuildReport> {
        self.ensure_healthy()?;
        let _surface_writer = self.surface_writer.lock();
        let definition = self
            .load_surface_definition(projection_id)?
            .ok_or_else(|| AproError::InvalidInput("projection not configured".into()))?;
        let pointer = self.load_surface_pointer(projection_id)?;
        let _shards: Vec<_> = self.shard_writers.iter().map(Mutex::lock).collect();
        let watermarks = self.catalog.read().shard_sequences.clone();
        let prefix = surface_record_prefix(&definition)?;
        let rows = self.backend.scan_prefix(
            StorageSpace::Records,
            &prefix,
            self.config.max_surface_records.saturating_add(1),
        )?;
        if rows.len() > self.config.max_surface_records {
            return Err(AproError::ResourceLimit(format!(
                "rebuild exceeds the scan limit of {} records",
                self.config.max_surface_records
            )));
        }
        let now = now_unix_ms()?;
        let mut records = BTreeMap::new();
        for (_, bytes) in rows {
            let head: HeadPointer = decode_logical(LogicalFrameKind::Head, &bytes)?;
            if head.tombstone {
                continue;
            }
            let record = self.get_version(&head.identity, head.version)?;
            if surface_accepts(&definition, &record, now) {
                records.insert(record.identity.clone(), record);
            }
        }
        drop(_shards);
        self.publish_surface_generation(&definition, pointer, records, watermarks, 0, durability)
    }

    pub fn get_surface(&self, projection_id: &str) -> Result<Option<SurfaceGeneration>> {
        self.ensure_healthy()?;
        if self.load_surface_definition(projection_id)?.is_none() {
            return Ok(None);
        }
        let pointer = self.load_surface_pointer(projection_id)?;
        pointer
            .current_generation
            .map(|generation| self.load_surface_generation(projection_id, generation))
            .transpose()
    }

    pub fn read_surface(&self, projection_id: &str) -> Result<Option<SurfaceRead>> {
        let Some(generation) = self.get_surface(projection_id)? else {
            return Ok(None);
        };
        let catalog = self.catalog.read();
        let stale_by_sequences = generation
            .source_watermarks
            .iter()
            .map(|(shard, watermark)| {
                (
                    *shard,
                    catalog
                        .shard_sequences
                        .get(shard)
                        .copied()
                        .unwrap_or(0)
                        .saturating_sub(*watermark),
                )
            })
            .collect::<BTreeMap<_, _>>();
        let complete = stale_by_sequences.values().all(|stale| *stale == 0);
        Ok(Some(SurfaceRead {
            generation,
            stale_by_sequences,
            complete,
            errors: Vec::new(),
        }))
    }

    pub fn surface_definition(&self, projection_id: &str) -> Result<Option<SurfaceDefinition>> {
        self.ensure_healthy()?;
        self.load_surface_definition(projection_id)
    }

    pub fn expire_due(&self, limit: usize, durability: Durability) -> Result<ExpirationReport> {
        self.expire_due_at(now_unix_ms()?, limit, durability)
    }

    pub fn expire_due_at(
        &self,
        now_unix_ms: u64,
        limit: usize,
        durability: Durability,
    ) -> Result<ExpirationReport> {
        self.ensure_healthy()?;
        if limit == 0 || limit > self.config.limits.max_queue_depth {
            return Err(AproError::ResourceLimit(format!(
                "TTL limit must be between 1 and {}",
                self.config.limits.max_queue_depth
            )));
        }
        let rows = self.backend.scan_range(
            StorageSpace::Ttl,
            &[0; 8],
            &ttl_upper_key(now_unix_ms),
            limit,
        )?;
        let mut report = ExpirationReport {
            scanned: rows.len(),
            ..ExpirationReport::default()
        };
        for (key, bytes) in rows {
            let entry: TtlEntry = decode_logical(LogicalFrameKind::Ttl, &bytes)?;
            if entry.expires_at_unix_ms > now_unix_ms
                || key != ttl_key(entry.expires_at_unix_ms, &entry.identity)
            {
                return Err(AproError::Corrupt("inconsistent TTL index".into()));
            }
            let shard = self.shard_for_partition(&entry.identity.partition_key());
            let _writer = self.shard_writers[shard as usize].lock();
            let current_index = self.backend.get(StorageSpace::Ttl, &key)?;
            if current_index.as_deref() != Some(bytes.as_slice()) {
                report.stale_entries += 1;
                continue;
            }
            let head = self.current_head(&entry.identity)?;
            if head
                .as_ref()
                .is_some_and(|head| !head.tombstone && head.version == entry.version)
            {
                let retention_mode = self
                    .catalog
                    .read()
                    .collections
                    .get(&entry.identity.collection_key())
                    .map_or(EventRetentionMode::VersionRef, |policy| {
                        policy.retention_mode
                    });
                if retention_mode == EventRetentionMode::Delta {
                    return Err(AproError::Unsupported(
                        "TTL expiration on Delta collection requires a declared delta generator"
                            .into(),
                    ));
                }
                self.commit_mutations(
                    shard,
                    vec![AtomicMutation::Delete(DeleteRequest {
                        identity: entry.identity.clone(),
                        expected: ExpectedVersion::Exact(entry.version),
                        idempotency_key_hash: None,
                        delta: None,
                    })],
                    durability,
                    None,
                )?;
                report.expired += 1;
            } else {
                let mut cleanup = StorageBatch::with_capacity(1);
                cleanup.delete(StorageSpace::Ttl, key);
                self.commit_primary(cleanup, CommitMode::Durable)?;
                report.stale_entries += 1;
            }
        }
        Ok(report)
    }

    pub fn purge_expired_idempotency(
        &self,
        now_unix_ms: u64,
        limit: usize,
    ) -> Result<IdempotencyPurgeReport> {
        self.ensure_healthy()?;
        if limit == 0 || limit > self.config.limits.max_batch_operations / 2 {
            return Err(AproError::ResourceLimit(format!(
                "idempotency purge must be between 1 and {}",
                self.config.limits.max_batch_operations / 2
            )));
        }
        let mut end = now_unix_ms.to_be_bytes().to_vec();
        end.push(u8::MAX);
        let rows =
            self.backend
                .scan_range(StorageSpace::IdempotencyExpiry, &[0; 8], &end, limit)?;
        let _catalog_writer = self.catalog_writer.lock();
        let mut report = IdempotencyPurgeReport {
            scanned: rows.len(),
            ..IdempotencyPurgeReport::default()
        };
        let mut batch = StorageBatch::with_capacity(rows.len() * 2);
        for (expiry_key, bytes) in rows {
            let expiry: IdempotencyExpiryEntry =
                decode_logical(LogicalFrameKind::IdempotencyExpiry, &bytes)?;
            if expiry.expires_at_unix_ms > now_unix_ms {
                return Err(AproError::Corrupt(
                    "idempotency index out of time range".into(),
                ));
            }
            let current = self
                .backend
                .get(StorageSpace::Idempotency, &expiry.lookup_key)?
                .map(|bytes| {
                    decode_logical::<IdempotencyRecord>(LogicalFrameKind::Idempotency, &bytes)
                })
                .transpose()?;
            if current
                .as_ref()
                .is_some_and(|record| record.expires_at_unix_ms == expiry.expires_at_unix_ms)
            {
                batch.delete(StorageSpace::Idempotency, expiry.lookup_key);
                report.records_deleted += 1;
            } else {
                report.stale_entries += 1;
            }
            batch.delete(StorageSpace::IdempotencyExpiry, expiry_key);
        }
        if !batch.is_empty() {
            self.commit_primary(batch, CommitMode::Durable)?;
        }
        Ok(report)
    }

    pub fn put(&self, request: PutRequest) -> Result<MutationReceipt> {
        self.atomic_batch(vec![AtomicMutation::Put(request)], self.config.durability)
            .map(|mut receipts| receipts.remove(0))
    }

    pub fn append(
        &self,
        mut request: PutRequest,
        durability: Durability,
    ) -> Result<MutationReceipt> {
        request.expected = ExpectedVersion::Missing;
        request.operation = ChangeOperation::Append;
        request.workflow_override = Some(WorkflowDescriptor {
            state: "pending".into(),
            attempt: 0,
            lease_id: None,
            fencing_token: 0,
            lease_deadline_unix_ms: None,
        });
        self.atomic_batch(vec![AtomicMutation::Put(request)], durability)
            .map(|mut receipts| receipts.remove(0))
    }

    pub fn claim(&self, request: ClaimRequest) -> Result<Vec<ClaimedRecord>> {
        self.ensure_healthy()?;
        request.scope.validate(&self.config.limits)?;
        let claim_limit = self
            .config
            .max_claim_batch
            .min(self.config.limits.max_batch_operations);
        if request.max_records == 0 || request.max_records > claim_limit {
            return Err(AproError::ResourceLimit(format!(
                "claim must request between 1 and {} records",
                claim_limit
            )));
        }
        if request.lease_duration.is_zero()
            || request.lease_duration > self.config.max_lease_duration
        {
            return Err(AproError::InvalidInput(
                "lease duration is zero or exceeds the maximum configured".into(),
            ));
        }
        self.ensure_workflow_retention(&request.scope.collection_key())?;
        let now = now_unix_ms()?;
        let idempotency = request
            .idempotency_key_hash
            .map(|hash| idempotency_context_for(&request.scope.partition_key(), hash, &request))
            .transpose()?;
        let shard = self.shard_for_partition(&request.scope.partition_key());
        let _writer = self.shard_writers[shard as usize].lock();
        if let Some(context) = &idempotency
            && let Some(receipts) = self.lookup_idempotency(context, now)?
        {
            return self.claimed_from_receipts(receipts, now);
        }

        {
            let instant_now = Instant::now();
            let mut deadlines = self.lease_deadlines.lock();
            deadlines.retain(|_, deadline| *deadline > instant_now);
            if deadlines.len().saturating_add(request.max_records) > self.config.max_active_leases {
                return Err(AproError::Backpressure(
                    "active lease limit reached in process".into(),
                ));
            }
        }

        let mut candidates = self.workflow_candidates(&request.scope, "pending", now)?;
        candidates.extend(self.workflow_candidates(&request.scope, "leased", now)?);
        let mut seen = HashSet::new();
        candidates.retain(|record| seen.insert(record.identity.clone()));
        candidates.truncate(request.max_records);
        if candidates.is_empty() {
            if let Some(context) = &idempotency {
                self.persist_idempotency_outcome(context, Vec::new(), now, request.durability)?;
            }
            return Ok(Vec::new());
        }

        let lease_ms = duration_millis(request.lease_duration)?;
        let deadline = now
            .checked_add(lease_ms)
            .ok_or_else(|| AproError::ResourceLimit("lease deadline exceeds u64".into()))?;
        let mut lease_ids = Vec::with_capacity(candidates.len());
        let mutations = candidates
            .into_iter()
            .map(|record| {
                let mut lease_id = [0_u8; 16];
                getrandom::fill(&mut lease_id)
                    .map_err(|error| AproError::Storage(format!("RNG lease: {error}")))?;
                let fencing_token = record
                    .workflow
                    .fencing_token
                    .checked_add(1)
                    .ok_or_else(|| AproError::ResourceLimit("fencing token exhausted".into()))?;
                let workflow = WorkflowDescriptor {
                    state: "leased".into(),
                    attempt: record.workflow.attempt.saturating_add(1),
                    lease_id: Some(lease_id),
                    fencing_token,
                    lease_deadline_unix_ms: Some(deadline),
                };
                lease_ids.push((record.identity.clone(), lease_id));
                Ok(AtomicMutation::Put(workflow_put_request(
                    record,
                    workflow,
                    ChangeOperation::Claim,
                    request.idempotency_key_hash,
                )?))
            })
            .collect::<Result<Vec<_>>>()?;
        let receipts = self.commit_mutations(shard, mutations, request.durability, idempotency)?;
        let instant_deadline = Instant::now()
            .checked_add(request.lease_duration)
            .ok_or_else(|| {
                AproError::ResourceLimit("monotonic lease deadline exceeds Instant".into())
            })?;
        {
            let mut deadlines = self.lease_deadlines.lock();
            for (identity, lease_id) in lease_ids {
                deadlines.insert((identity, lease_id), instant_deadline);
            }
        }
        self.claimed_from_receipts(receipts, now)
    }

    pub fn heartbeat(
        &self,
        identity: &RecordIdentity,
        lease: LeaseProof,
        extension: Duration,
        idempotency_key_hash: Option<[u8; 32]>,
        durability: Durability,
    ) -> Result<WorkflowMutationResult> {
        if extension.is_zero() || extension > self.config.max_lease_duration {
            return Err(AproError::InvalidInput(
                "lease extension is zero or exceeds the configured maximum".into(),
            ));
        }
        let now = now_unix_ms()?;
        let context = idempotency_key_hash
            .map(|hash| {
                idempotency_context_for(
                    &identity.partition_key(),
                    hash,
                    &(
                        "heartbeat",
                        identity,
                        lease,
                        duration_millis(extension)?,
                        durability,
                    ),
                )
            })
            .transpose()?;
        let shard = self.shard_for_partition(&identity.partition_key());
        let _writer = self.shard_writers[shard as usize].lock();
        if let Some(context) = &context
            && let Some(receipts) = self.lookup_idempotency(context, now)?
        {
            return self.single_workflow_replay(receipts);
        }
        let current = self.active_lease_record(identity, lease, now)?;
        let deadline = now
            .checked_add(duration_millis(extension)?)
            .ok_or_else(|| AproError::ResourceLimit("lease deadline exceeds u64".into()))?;
        let mut workflow = current.workflow.clone();
        workflow.lease_deadline_unix_ms = Some(deadline);
        let receipt = self.commit_workflow_record_locked(
            shard,
            WorkflowCommit {
                current,
                workflow,
                operation: ChangeOperation::Heartbeat,
                idempotency_key_hash,
                durability,
                idempotency: context,
            },
        )?;
        let instant_deadline = Instant::now().checked_add(extension).ok_or_else(|| {
            AproError::ResourceLimit("monotonic lease deadline exceeds Instant".into())
        })?;
        self.lease_deadlines
            .lock()
            .insert((identity.clone(), lease.lease_id), instant_deadline);
        self.workflow_result(receipt)
    }

    pub fn complete(
        &self,
        identity: &RecordIdentity,
        lease: LeaseProof,
        idempotency_key_hash: Option<[u8; 32]>,
        durability: Durability,
    ) -> Result<WorkflowMutationResult> {
        let now = now_unix_ms()?;
        let context = idempotency_key_hash
            .map(|hash| {
                idempotency_context_for(
                    &identity.partition_key(),
                    hash,
                    &("complete", identity, lease, durability),
                )
            })
            .transpose()?;
        let shard = self.shard_for_partition(&identity.partition_key());
        let _writer = self.shard_writers[shard as usize].lock();
        if let Some(context) = &context
            && let Some(receipts) = self.lookup_idempotency(context, now)?
        {
            return self.single_workflow_replay(receipts);
        }
        let current = self.active_lease_record(identity, lease, now)?;
        let workflow = cleared_workflow(&current.workflow, "completed");
        let receipt = self.commit_workflow_record_locked(
            shard,
            WorkflowCommit {
                current,
                workflow,
                operation: ChangeOperation::Complete,
                idempotency_key_hash,
                durability,
                idempotency: context,
            },
        )?;
        self.lease_deadlines
            .lock()
            .remove(&(identity.clone(), lease.lease_id));
        self.workflow_result(receipt)
    }

    pub fn fail(
        &self,
        identity: &RecordIdentity,
        lease: LeaseProof,
        permanent: bool,
        idempotency_key_hash: Option<[u8; 32]>,
        durability: Durability,
    ) -> Result<WorkflowMutationResult> {
        let now = now_unix_ms()?;
        let context = idempotency_key_hash
            .map(|hash| {
                idempotency_context_for(
                    &identity.partition_key(),
                    hash,
                    &("fail", identity, lease, permanent, durability),
                )
            })
            .transpose()?;
        let shard = self.shard_for_partition(&identity.partition_key());
        let _writer = self.shard_writers[shard as usize].lock();
        if let Some(context) = &context
            && let Some(receipts) = self.lookup_idempotency(context, now)?
        {
            return self.single_workflow_replay(receipts);
        }
        let current = self.active_lease_record(identity, lease, now)?;
        let state = if permanent || current.workflow.attempt >= self.config.max_workflow_attempts {
            "dead_letter"
        } else {
            "pending"
        };
        let workflow = cleared_workflow(&current.workflow, state);
        let receipt = self.commit_workflow_record_locked(
            shard,
            WorkflowCommit {
                current,
                workflow,
                operation: ChangeOperation::Fail,
                idempotency_key_hash,
                durability,
                idempotency: context,
            },
        )?;
        self.lease_deadlines
            .lock()
            .remove(&(identity.clone(), lease.lease_id));
        self.workflow_result(receipt)
    }

    pub fn publish(
        &self,
        identity: &RecordIdentity,
        idempotency_key_hash: Option<[u8; 32]>,
        durability: Durability,
    ) -> Result<WorkflowMutationResult> {
        let now = now_unix_ms()?;
        let context = idempotency_key_hash
            .map(|hash| {
                idempotency_context_for(
                    &identity.partition_key(),
                    hash,
                    &("publish", identity, durability),
                )
            })
            .transpose()?;
        let shard = self.shard_for_partition(&identity.partition_key());
        let _writer = self.shard_writers[shard as usize].lock();
        if let Some(context) = &context
            && let Some(receipts) = self.lookup_idempotency(context, now)?
        {
            return self.single_workflow_replay(receipts);
        }
        let current = self
            .get(identity)?
            .ok_or_else(|| AproError::InvalidInput("record to publish not found".into()))?;
        if current.workflow.state == "published" {
            let receipt = self.receipt_for_version(current.version, durability)?;
            if let Some(context) = &context {
                self.persist_idempotency_outcome(context, vec![receipt.clone()], now, durability)?;
            }
            return self.workflow_result(receipt);
        }
        if current.workflow.state != "completed" {
            return Err(AproError::Conflict(format!(
                "Publish requires state 'completed', current state {}",
                current.workflow.state
            )));
        }
        let workflow = cleared_workflow(&current.workflow, "published");
        let receipt = self.commit_workflow_record_locked(
            shard,
            WorkflowCommit {
                current,
                workflow,
                operation: ChangeOperation::Publish,
                idempotency_key_hash,
                durability,
                idempotency: context,
            },
        )?;
        self.workflow_result(receipt)
    }

    pub fn acknowledge_consumer(
        &self,
        collection: &RecordIdentity,
        consumer: &str,
        shard: u32,
        sequence: u64,
    ) -> Result<()> {
        self.ensure_healthy()?;
        if consumer.is_empty() || consumer.len() > 255 {
            return Err(AproError::InvalidInput(
                "consumer is empty or exceeds 255 bytes".into(),
            ));
        }
        if shard >= self.config.shards {
            return Err(AproError::InvalidInput("shard is out of range".into()));
        }
        let _catalog_writer = self.catalog_writer.lock();
        let mut updated = self.catalog.read().clone();
        let collection_key = collection.collection_key();
        let policy = updated
            .collections
            .get(&collection_key)
            .ok_or_else(|| AproError::InvalidInput("collection is not configured".into()))?;
        if !policy
            .required_consumers
            .iter()
            .any(|name| name == consumer)
        {
            return Err(AproError::InvalidInput(format!(
                "required consumer not declared: {consumer}"
            )));
        }
        let latest = updated.shard_sequences.get(&shard).copied().unwrap_or(0);
        if sequence > latest {
            return Err(AproError::InvalidInput(format!(
                "watermark {sequence} exceeds sequence {latest}"
            )));
        }
        let key = consumer_watermark_key(&collection_key, shard, consumer)?;
        let current = updated.consumer_watermarks.get(&key).copied().unwrap_or(0);
        if sequence < current {
            return Err(AproError::Conflict(format!(
                "consumer watermark not monotonic: {sequence} < {current}"
            )));
        }
        updated.consumer_watermarks.insert(key, sequence);
        updated.generation = updated.generation.saturating_add(1);
        let mut batch = StorageBatch::with_capacity(1);
        batch.put(
            StorageSpace::Catalog,
            CATALOG_KEY.to_vec(),
            encode_logical(LogicalFrameKind::Catalog, &updated)?,
        );
        self.commit_primary(batch, CommitMode::Durable)?;
        *self.catalog.write() = updated;
        Ok(())
    }

    pub fn garbage_collect_collection(
        &self,
        collection: &RecordIdentity,
        shard: u32,
        max_events: usize,
    ) -> Result<GarbageCollectionReport> {
        self.ensure_healthy()?;
        if max_events == 0 || max_events > self.config.limits.max_batch_operations / 2 {
            return Err(AproError::ResourceLimit(format!(
                "GC events must be between 1 and {}",
                self.config.limits.max_batch_operations / 2
            )));
        }
        if shard >= self.config.shards {
            return Err(AproError::InvalidInput("shard is out of range".into()));
        }
        let collection_key = collection.collection_key();
        let catalog = self.catalog.read().clone();
        let policy = catalog
            .collections
            .get(&collection_key)
            .ok_or_else(|| AproError::InvalidInput("collection is not configured".into()))?;
        if policy.required_consumers.is_empty() {
            return Err(AproError::Unsupported(
                "Event GC requires at least one mandatory consumer".into(),
            ));
        }
        let mut safe_watermark = u64::MAX;
        for consumer in &policy.required_consumers {
            let key = consumer_watermark_key(&collection_key, shard, consumer)?;
            safe_watermark =
                safe_watermark.min(catalog.consumer_watermarks.get(&key).copied().unwrap_or(0));
        }
        if safe_watermark == 0 {
            return Ok(GarbageCollectionReport {
                safe_watermark,
                events_deleted: 0,
                versions_deleted: 0,
            });
        }

        let _writer = self.shard_writers[shard as usize].lock();
        let start = event_key(shard, 1);
        let end = event_key(shard, safe_watermark);
        let rows = self.backend.scan_range(
            StorageSpace::Events,
            &start,
            &end,
            max_events.saturating_add(self.config.limits.max_batch_operations),
        )?;
        let matching: Vec<(Vec<u8>, ChangeEvent)> = rows
            .into_iter()
            .map(|(key, bytes)| {
                decode_logical(LogicalFrameKind::Change, &bytes).map(|event| (key, event))
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .filter(|(_, event)| event_matches_collection(event, collection))
            .collect();
        let events: Vec<_> = truncate_pairs_without_splitting_batch(matching, max_events);
        if events.is_empty() {
            return Ok(GarbageCollectionReport {
                safe_watermark,
                events_deleted: 0,
                versions_deleted: 0,
            });
        }
        let mut batch = StorageBatch::with_capacity(events.len() * 2);
        let mut versions_deleted = 0usize;
        for (event_storage_key, event) in &events {
            batch.delete(StorageSpace::Events, event_storage_key.clone());
            let identity = identity_from_event(event)?;
            let current = self.current_head(&identity)?;
            if current.as_ref().map(|head| head.version) != Some(event.version) {
                batch.delete(
                    StorageSpace::Versions,
                    version_key(&identity, event.version),
                );
                versions_deleted += 1;
            }
        }
        self.commit_primary(batch, CommitMode::Durable)?;
        Ok(GarbageCollectionReport {
            safe_watermark,
            events_deleted: events.len(),
            versions_deleted,
        })
    }

    pub fn put_with_durability(
        &self,
        request: PutRequest,
        durability: Durability,
    ) -> Result<MutationReceipt> {
        self.atomic_batch(vec![AtomicMutation::Put(request)], durability)
            .map(|mut receipts| receipts.remove(0))
    }

    pub fn compare_and_swap(
        &self,
        mut request: PutRequest,
        expected: Version,
        durability: Durability,
    ) -> Result<MutationReceipt> {
        request.expected = ExpectedVersion::Exact(expected);
        self.atomic_batch(vec![AtomicMutation::Put(request)], durability)
            .map(|mut receipts| receipts.remove(0))
    }

    pub fn delete(&self, request: DeleteRequest) -> Result<MutationReceipt> {
        self.atomic_batch(
            vec![AtomicMutation::Delete(request)],
            self.config.durability,
        )
        .map(|mut receipts| receipts.remove(0))
    }

    pub fn atomic_batch(
        &self,
        mutations: Vec<AtomicMutation>,
        durability: Durability,
    ) -> Result<Vec<MutationReceipt>> {
        self.ensure_healthy()?;
        if mutations.is_empty() {
            return Ok(Vec::new());
        }
        if mutations.len() > self.config.limits.max_batch_operations {
            return Err(AproError::ResourceLimit(format!(
                "batch with {} operations, maximum {}",
                mutations.len(),
                self.config.limits.max_batch_operations
            )));
        }
        let first_partition = mutations[0].identity().partition_key();
        let mut identities = HashSet::with_capacity(mutations.len());
        let mut estimated_bytes = 0usize;
        for mutation in &mutations {
            let identity = mutation.identity();
            identity.validate(&self.config.limits)?;
            if identity.partition_key() != first_partition {
                return Err(AproError::InvalidInput(
                    "AtomicBatch requires single partition".into(),
                ));
            }
            if !identities.insert(identity.clone()) {
                return Err(AproError::InvalidInput(
                    "AtomicBatch does not accept multiple mutations of the same identity".into(),
                ));
            }
            estimated_bytes = estimated_bytes.saturating_add(identity.storage_key().len());
            if let AtomicMutation::Put(request) = mutation {
                request.payload.validate()?;
                estimated_bytes =
                    estimated_bytes.saturating_add(estimate_payload(&request.payload));
            }
        }
        if estimated_bytes > self.config.limits.max_batch_bytes {
            return Err(AproError::ResourceLimit(format!(
                "batch estimated at {estimated_bytes} bytes, maximum {}",
                self.config.limits.max_batch_bytes
            )));
        }
        self.enforce_write_disk_budget(estimated_bytes)?;
        let idempotency = idempotency_context(&mutations)?;
        let _inflight = self.acquire_inflight(estimated_bytes)?;
        let shard = self.shard_for_partition(&first_partition);
        let _writer = self.shard_writers[shard as usize].lock();
        self.commit_mutations(shard, mutations, durability, idempotency)
    }

    pub fn get(&self, identity: &RecordIdentity) -> Result<Option<RecordEnvelope>> {
        self.ensure_healthy()?;
        identity.validate(&self.config.limits)?;
        let now = now_unix_ms()?;
        if let Some(record) = self.object_cache.get(identity, now) {
            if !record
                .expires_at_unix_ms
                .is_some_and(|expires| expires <= now)
            {
                return Ok(Some(record));
            }
            self.object_cache.invalidate(identity);
        }
        let catalog_generation = self.catalog.read().generation;
        if self.negative_cache.get(identity, now) == Some(catalog_generation) {
            return Ok(None);
        }
        let Some(head_bytes) = self
            .backend
            .get(StorageSpace::Records, &identity.storage_key())?
        else {
            self.insert_negative_cache(identity.clone(), catalog_generation, now);
            return Ok(None);
        };
        let head: HeadPointer = decode_logical(LogicalFrameKind::Head, &head_bytes)?;
        if head.identity != *identity {
            return Err(AproError::Corrupt(
                "head is associated with a different identity".into(),
            ));
        }
        if head.tombstone {
            self.insert_negative_cache(identity.clone(), catalog_generation, now);
            return Ok(None);
        }
        let record = self.get_version(identity, head.version)?;
        if record
            .expires_at_unix_ms
            .is_some_and(|expires| expires <= now)
        {
            self.insert_negative_cache(identity.clone(), catalog_generation, now);
            return Ok(None);
        }
        let mut descriptor = self.load_radial_descriptor(identity)?.ok_or_else(|| {
            AproError::Corrupt("current record is missing a radial descriptor".into())
        })?;
        if descriptor.canonical_version != record.version {
            return Err(AproError::Corrupt(
                "record and radial descriptor diverge".into(),
            ));
        }
        descriptor.access_frequency_estimate =
            descriptor.access_frequency_estimate.saturating_add(1);
        descriptor.last_access_sampled_unix_ms = Some(now);
        let policy = self
            .radial_state
            .read()
            .policies
            .get(&identity.collection_key())
            .cloned()
            .unwrap_or_default();
        let freshness = freshness_millis(
            descriptor.updated_at_unix_ms,
            now,
            policy.freshness_half_life_ms,
        );
        let score = radial_score_millis(freshness, descriptor.urgency_millis, &policy);
        let descriptor_bytes = estimate_radial_descriptor(&descriptor);
        self.metadata_cache.insert(
            identity.clone(),
            descriptor.clone(),
            CacheAdmission {
                bytes: descriptor_bytes,
                radial_score_millis: score,
                pinned_until_unix_ms: descriptor.admin_pin_until_unix_ms,
                expires_at_unix_ms: None,
                now_unix_ms: now,
            },
        );
        self.object_cache.insert(
            identity.clone(),
            record.clone(),
            CacheAdmission {
                bytes: estimate_record(&record),
                radial_score_millis: score,
                pinned_until_unix_ms: descriptor.admin_pin_until_unix_ms,
                expires_at_unix_ms: record.expires_at_unix_ms,
                now_unix_ms: now,
            },
        );
        Ok(Some(record))
    }

    pub fn current_version(&self, identity: &RecordIdentity) -> Result<Option<Version>> {
        self.ensure_healthy()?;
        let Some(head_bytes) = self
            .backend
            .get(StorageSpace::Records, &identity.storage_key())?
        else {
            return Ok(None);
        };
        let head: HeadPointer = decode_logical(LogicalFrameKind::Head, &head_bytes)?;
        Ok(Some(head.version))
    }

    pub fn get_version(
        &self,
        identity: &RecordIdentity,
        version: Version,
    ) -> Result<RecordEnvelope> {
        self.ensure_healthy()?;
        let key = version_key(identity, version);
        let now = now_unix_ms()?;
        let cached = self
            .compressed_cache
            .get(identity, now)
            .filter(|entry| entry.version == version);
        let cache_hit = cached.is_some();
        let bytes = match cached {
            Some(entry) => entry.bytes,
            None => self
                .backend
                .get(StorageSpace::Versions, &key)?
                .ok_or_else(|| AproError::Corrupt("missing immutable version".into()))?,
        };
        let dictionary = self
            .compression_manager
            .stored_dictionary_id(&bytes)?
            .map(|id| self.load_dictionary(id))
            .transpose()?;
        let record = self.compression_manager.decode_record(
            &bytes,
            dictionary.as_deref(),
            &self.config.limits,
            xxh3_64(&key),
        )?;
        if record.identity != *identity || record.version != version {
            return Err(AproError::Corrupt(
                "immutable version does not match the reference".into(),
            ));
        }
        if !cache_hit
            && self
                .current_head(identity)?
                .is_some_and(|head| head.version == version)
        {
            self.compressed_cache.insert(
                identity.clone(),
                CompressedCacheEntry {
                    version,
                    bytes: bytes.clone(),
                },
                CacheAdmission {
                    bytes: bytes.len(),
                    radial_score_millis: 500,
                    pinned_until_unix_ms: None,
                    expires_at_unix_ms: record.expires_at_unix_ms,
                    now_unix_ms: now,
                },
            );
        }
        Ok(record)
    }

    pub fn changes(
        &self,
        shard: u32,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<ChangeEvent>> {
        self.ensure_healthy()?;
        if shard >= self.config.shards {
            // Check if the shard is out of range
            return Err(AproError::InvalidInput("shard out of range".into()));
        }
        if limit == 0 {
            return Ok(Vec::new());
        }
        if limit > self.config.limits.max_queue_depth {
            return Err(AproError::ResourceLimit(format!(
                "change stream limit {limit} exceeds maximum of {}",
                self.config.limits.max_queue_depth
            )));
        }
        let latest = self
            .catalog
            .read()
            .shard_sequences
            .get(&shard)
            .copied()
            .unwrap_or(0);
        if after_sequence > latest {
            return Err(AproError::InvalidInput(format!(
                "change stream watermark {after_sequence} exceeds latest sequence {latest}"
            )));
        }
        let scan_limit = limit.saturating_add(self.config.limits.max_batch_operations);
        let start = event_key(shard, after_sequence.saturating_add(1));
        let end = event_key(shard, u64::MAX);
        let encoded = self
            .backend
            .scan_range(StorageSpace::Events, &start, &end, scan_limit)?;
        let events: Vec<ChangeEvent> = encoded
            .into_iter()
            .map(|(_, bytes)| decode_logical(LogicalFrameKind::Change, &bytes))
            .collect::<Result<_>>()?;
        let expected = after_sequence.saturating_add(1);
        if expected <= latest
            && events.first().map(|event| event.version.sequence) != Some(expected)
        {
            return Err(AproError::ChangeLogGap(format!(
                "requested sequence {expected}, first available event sequence {:?}, latest {latest}",
                events.first().map(|event| event.version.sequence)
            )));
        }
        if after_sequence > 0 {
            let previous = self
                .backend
                .get(StorageSpace::Events, &event_key(shard, after_sequence))?
                .map(|bytes| decode_logical::<ChangeEvent>(LogicalFrameKind::Change, &bytes))
                .transpose()?;
            if previous
                .as_ref()
                .zip(events.first())
                .is_some_and(|(previous, next)| previous.batch_id == next.batch_id)
            {
                return Err(AproError::InvalidInput(
                    "watermark is in the middle of an AtomicBatch".into(),
                ));
            }
        }
        Ok(truncate_without_splitting_batch(events, limit))
    }

    pub fn sync(&self) -> Result<()> {
        self.ensure_healthy()?;
        let _catalog_writer = self.catalog_writer.lock();
        let mut updated = self.catalog.read().clone();
        updated.generation = updated.generation.saturating_add(1); // Increment catalog generation for sync consistency
        updated.durable_watermarks = updated.shard_sequences.clone();
        let mut batch = StorageBatch::with_capacity(1);
        batch.put(
            StorageSpace::Catalog,
            CATALOG_KEY.to_vec(),
            encode_logical(LogicalFrameKind::Catalog, &updated)?,
        );
        self.commit_primary(batch, CommitMode::Durable)?;
        for (shard, sequence) in &updated.shard_sequences {
            self.durable_watermarks[*shard as usize].store(*sequence, Ordering::Release);
        }
        *self.catalog.write() = updated;
        Ok(())
    }

    pub fn stats(&self) -> Result<BackendStats> {
        self.backend.stats()
    }

    pub fn vector_exact(&self, request: VectorSearchRequest) -> Result<VectorSearchResult> {
        self.ensure_healthy()?;
        if request.limit == 0
            || request.max_scan_records == 0
            || request.max_scan_records > self.config.compute.max_batch_rows
            || request.query.is_empty()
            || request.query.iter().any(|value| !value.is_finite())
        {
            return Err(AproError::InvalidInput(
                "Invalid VectorExact limits or query".into(),
            ));
        }
        let scope = RecordIdentity::new(
            request.tenant.clone(),
            request.namespace.clone(),
            request.collection.clone(),
            b"_".to_vec(),
            b"_".to_vec(),
        )?;
        scope.validate(&self.config.limits)?;
        let scan_limit = request
            .max_scan_records
            .checked_add(1)
            .ok_or_else(|| AproError::ResourceLimit("vector scan limit exhausted".into()))?;
        // A brief all-shard barrier gives the derived columnar projection one
        // exact catalog generation. The locks are released before compute.
        let shard_guards: Vec<_> = self.shard_writers.iter().map(Mutex::lock).collect();
        let heads = self.backend.scan_prefix(
            StorageSpace::Records,
            &record_collection_prefix(&scope),
            scan_limit,
        )?;
        if heads.len() > request.max_scan_records {
            return Err(AproError::ResourceLimit(format!(
                "VectorExact requires more than {} records; please explicitly increase max_scan_records",
                request.max_scan_records
            )));
        }
        let scanned_records = heads.len();
        let mut candidates = Vec::new();
        for (_, bytes) in heads {
            let head: HeadPointer = decode_logical(LogicalFrameKind::Head, &bytes)?;
            if head.tombstone {
                continue;
            }
            let Some(record) = self.get(&head.identity)? else {
                continue;
            };
            if let Some(Payload::Vector(vector)) = record.payload
                && vector.len() == request.query.len()
            {
                candidates.push((record.identity, record.version, vector));
            }
        }
        if candidates.is_empty() {
            return Ok(VectorSearchResult {
                hits: Vec::new(),
                scanned_records,
                vector_candidates: 0,
                execution: ComputeExecution::Cpu,
                accelerator: None,
                estimate: CostEstimate::default(),
                fallback_reason: None,
            });
        }
        let rows = candidates
            .iter()
            .map(|(_, _, vector)| Some(vector.clone()))
            .collect::<Vec<_>>();
        let batch = ColumnarF32Batch::from_rows(&rows, request.query.len())?;
        if batch.byte_len() > self.config.compute.max_batch_bytes {
            return Err(AproError::ResourceLimit(format!(
                "VectorExact batch size {} bytes exceeds the maximum allowed {} bytes",
                batch.byte_len(),
                self.config.compute.max_batch_bytes
            )));
        }
        let source_watermark = self.catalog.read().generation;
        drop(shard_guards);
        let schema_version = u32::try_from(request.query.len())
            .map_err(|_| AproError::ResourceLimit("vector dimension exceeds u32 limit".into()))?;
        let result = self.compute_scheduler.execute(ComputeRequest {
            batch,
            query: request.query,
            metric: request.metric,
            limit: request.limit,
            preference: request.preference,
            projection: Some(ProjectionDescriptor {
                projection_id: format!("vector:{:016x}", xxh3_64(&scope.collection_key())),
                source_watermark,
                schema_version,
            }),
        })?;
        let hits = result
            .rows
            .into_iter()
            .map(|scored| {
                let (identity, version, _) = candidates.get(scored.row).ok_or_else(|| {
                    AproError::Compute("VectorExact row is outside the candidate set".into())
                })?;
                Ok(VectorSearchHit {
                    identity: identity.clone(),
                    version: *version,
                    score: scored.score,
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(VectorSearchResult {
            hits,
            scanned_records,
            vector_candidates: candidates.len(),
            execution: result.execution,
            accelerator: result.accelerator,
            estimate: result.estimate,
            fallback_reason: result.fallback_reason,
        })
    }

    #[must_use]
    pub fn compute_metrics(&self) -> SchedulerMetrics {
        self.compute_scheduler.metrics()
    }

    #[must_use]
    pub fn accelerator_stats(&self) -> Option<AcceleratorStats> {
        self.compute_scheduler.accelerator_stats()
    }

    #[must_use]
    pub fn accelerator_name(&self) -> Option<String> {
        self.compute_scheduler.accelerator_name()
    }

    pub fn append_audit_event(
        &self,
        request_id: u64,
        principal: &str,
        operation: &str,
        outcome: AuditOutcome,
        target_hash: Option<[u8; 32]>,
        error_class: Option<&str>,
    ) -> Result<AuditEvent> {
        self.ensure_healthy()?;
        if principal.is_empty()
            || principal.len() > 128
            || operation.is_empty()
            || operation.len() > 128
            || error_class.is_some_and(|value| value.is_empty() || value.len() > 128)
        {
            return Err(AproError::InvalidInput(
                "audit fields exceed allowed length limits".into(),
            ));
        }
        let _writer = self.audit_writer.lock();
        let sequence = self
            .audit_sequence
            .load(Ordering::Acquire)
            .checked_add(1)
            .ok_or_else(|| AproError::ResourceLimit("audit sequence exhausted".into()))?;
        let mut event_id = [0u8; 16];
        getrandom::fill(&mut event_id)
            .map_err(|error| AproError::Storage(format!("audit RNG error: {error}")))?;
        let event = AuditEvent {
            format_version: 1,
            sequence,
            event_id,
            at_unix_ms: now_unix_ms()?,
            request_id,
            principal: principal.into(),
            operation: operation.into(),
            outcome,
            target_hash,
            error_class: error_class.map(str::to_owned),
        };
        let mut batch = StorageBatch::with_capacity(2);
        batch.put(
            StorageSpace::Audit,
            AUDIT_STATE_KEY.to_vec(),
            encode_logical(
                LogicalFrameKind::AuditState,
                &AuditState {
                    format_version: 1,
                    last_sequence: sequence,
                },
            )?,
        );
        batch.put(
            StorageSpace::Audit,
            audit_key(sequence),
            encode_logical(LogicalFrameKind::Audit, &event)?,
        );
        self.commit_primary(batch, CommitMode::Durable)?;
        self.audit_sequence.store(sequence, Ordering::Release);
        Ok(event)
    }

    pub fn read_audit(&self, after: Option<AuditCursor>, limit: usize) -> Result<AuditPage> {
        self.ensure_healthy()?;
        if limit == 0 || limit > self.config.limits.max_batch_operations {
            return Err(AproError::ResourceLimit(format!(
                "audit page must contain between 1 and {} events",
                self.config.limits.max_batch_operations
            )));
        }
        let start = after.map_or_else(|| audit_key(1), |cursor| audit_key(cursor.sequence));
        let mut rows = self.backend.scan_range(
            StorageSpace::Audit,
            &start,
            &audit_key(u64::MAX),
            limit.saturating_add(usize::from(after.is_some())),
        )?;
        if after.is_some() && rows.first().is_some_and(|(key, _)| *key == start) {
            rows.remove(0);
        }
        rows.truncate(limit);
        let mut events = Vec::with_capacity(rows.len());
        for (key, bytes) in rows {
            let event: AuditEvent = decode_logical(LogicalFrameKind::Audit, &bytes)?;
            validate_audit_event(&event)?;
            if key != audit_key(event.sequence) {
                return Err(AproError::Corrupt(
                    "audit event associated with a mismatched key".into(),
                ));
            }
            events.push(event);
        }
        let next = events.last().map(|event| AuditCursor {
            sequence: event.sequence,
        });
        Ok(AuditPage { events, next })
    }

    pub fn major_compact(&self) -> Result<CompactionReport> {
        self.ensure_healthy()?;
        let stats = self.backend.stats()?;
        let temporary_estimate = stats.disk_bytes;
        if temporary_estimate > self.config.max_compaction_temporary_bytes {
            return Err(AproError::Backpressure(format!(
                "compaction requires approximately {temporary_estimate} bytes temporary storage, limit is {}",
                self.config.max_compaction_temporary_bytes
            )));
        }
        if self.config.path.exists() {
            ensure_available_space(
                &self.config.path,
                temporary_estimate.saturating_add(self.config.min_free_disk_bytes),
                "compaction",
            )?;
        }
        let _shards: Vec<_> = self.shard_writers.iter().map(Mutex::lock).collect();
        let _catalog_writer = self.catalog_writer.lock();
        self.backend.major_compact()
    }

    pub fn create_checkpoint(&self, destination: impl AsRef<Path>) -> Result<CheckpointInfo> {
        self.create_checkpoint_with_encryption(destination.as_ref(), self.config.encryption.clone())
    }

    pub fn rekey_to_copy(
        &self,
        destination: impl AsRef<Path>,
        encryption: EncryptionConfig,
    ) -> Result<CheckpointInfo> {
        let destination = destination.as_ref();
        let checkpoint =
            self.create_checkpoint_with_encryption(destination, Some(encryption.clone()))?;
        let mut config = self.config.clone();
        config.path = destination.to_path_buf();
        config.encryption = Some(encryption);
        let copy = Engine::open(config)?;
        copy.verify()?;
        drop(copy);
        Ok(checkpoint)
    }

    fn create_checkpoint_with_encryption(
        &self,
        destination: &Path,
        encryption: Option<EncryptionConfig>,
    ) -> Result<CheckpointInfo> {
        self.ensure_healthy()?;
        if destination.exists() {
            return Err(AproError::InvalidInput(format!(
                "checkpoint already exists at: {}",
                destination.display()
            )));
        }
        let parent = destination.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent).map_err(io_storage_error)?;
        let checkpoint_estimate = self.backend.stats()?.disk_bytes;
        ensure_available_space(
            parent,
            checkpoint_estimate.saturating_add(self.config.min_free_disk_bytes),
            "checkpoint",
        )?;
        let _shards: Vec<_> = self.shard_writers.iter().map(Mutex::lock).collect();
        let _catalog_writer = self.catalog_writer.lock();

        let mut catalog = self.catalog.read().clone();
        catalog.generation = catalog.generation.saturating_add(1);
        catalog.durable_watermarks = catalog.shard_sequences.clone();
        let mut catalog_batch = StorageBatch::with_capacity(1);
        catalog_batch.put(
            StorageSpace::Catalog,
            CATALOG_KEY.to_vec(),
            encode_logical(LogicalFrameKind::Catalog, &catalog)?,
        );
        self.commit_primary(catalog_batch, CommitMode::Durable)?;
        *self.catalog.write() = catalog.clone();
        for (shard, sequence) in &catalog.durable_watermarks {
            self.durable_watermarks[*shard as usize].store(*sequence, Ordering::Release);
        }

        let mut checkpoint_config = self.config.clone();
        checkpoint_config.encryption = encryption;
        let checkpoint = open_storage_backend(&checkpoint_config, destination)?;
        let mut entries = 0usize;
        let mut logical_bytes = 0u64;
        for space in [
            StorageSpace::Records,
            StorageSpace::Versions,
            StorageSpace::Events,
            StorageSpace::Idempotency,
            StorageSpace::Catalog,
            StorageSpace::Radial,
            StorageSpace::Ttl,
            StorageSpace::Workflow,
            StorageSpace::IdempotencyExpiry,
            StorageSpace::Surfaces,
            StorageSpace::Compression,
            StorageSpace::Audit,
        ] {
            self.copy_space_to(checkpoint.as_ref(), space, &mut entries, &mut logical_bytes)?;
        }
        checkpoint.persist(CommitMode::Durable)?;
        drop(checkpoint);
        Ok(CheckpointInfo {
            path: destination.to_path_buf(),
            entries,
            logical_bytes,
            durable_watermarks: catalog.durable_watermarks,
            catalog_generation: catalog.generation,
            backend: self.backend.name().into(),
            logical_format: catalog.format_version,
            software_version: env!("CARGO_PKG_VERSION").into(),
            encryption_key_ids: checkpoint_config
                .encryption
                .as_ref()
                .map_or_else(Vec::new, EncryptionConfig::key_ids),
        })
    }

    pub fn create_backup(&self, destination: impl AsRef<Path>) -> Result<BackupInfo> {
        self.ensure_healthy()?;
        let destination = destination.as_ref();
        if destination.exists() {
            return Err(AproError::InvalidInput(format!(
                "backup already exists: {}",
                destination.display()
            )));
        }
        fs::create_dir_all(destination).map_err(io_storage_error)?;
        let data_path = destination.join("data");
        let checkpoint = self.create_checkpoint(&data_path)?;
        let mut verification_config = self.config.clone();
        verification_config.path = data_path.clone();
        let verification_engine = Engine::open(verification_config)?;
        let verification = verification_engine.verify()?;
        let verified_catalog = verification_engine.catalog.read().clone();
        drop(verification_engine);
        if verified_catalog.generation != checkpoint.catalog_generation
            || verified_catalog.durable_watermarks != checkpoint.durable_watermarks
        {
            return Err(AproError::Corrupt(
                "checkpoint reopened with divergent catalog generation or watermark".into(),
            ));
        }
        let files = inventory_files(&data_path)?;
        let manifest = BackupManifest {
            manifest_version: 1,
            software_version: checkpoint.software_version,
            logical_format: checkpoint.logical_format,
            backend: checkpoint.backend,
            created_at_unix_ms: now_unix_ms()?,
            catalog_generation: checkpoint.catalog_generation,
            durable_watermarks: checkpoint.durable_watermarks,
            entries: checkpoint.entries,
            logical_bytes: checkpoint.logical_bytes,
            encrypted: self.config.encryption.is_some(),
            encryption_key_ids: checkpoint.encryption_key_ids,
            verification,
            files,
        };
        write_backup_manifest(destination, &manifest)?;
        let verified_manifest = Self::verify_backup(destination)?;
        Ok(BackupInfo {
            path: destination.to_path_buf(),
            manifest: verified_manifest,
        })
    }

    pub fn verify_backup(path: impl AsRef<Path>) -> Result<BackupManifest> {
        let path = path.as_ref();
        let bytes = fs::read(path.join("backup-manifest.json")).map_err(io_storage_error)?;
        if bytes.len() > 16 * 1024 * 1024 {
            return Err(AproError::ResourceLimit(
                "backup manifest exceeds 16 MiB".into(),
            ));
        }
        let manifest: BackupManifest = serde_json::from_slice(&bytes)
            .map_err(|error| AproError::Corrupt(format!("manifest backup JSON: {error}")))?;
        if manifest.manifest_version != 1
            || manifest.logical_format != 1
            || manifest.backend != "fjall-3.1.8"
            || manifest.files.len() > 1_000_000
        {
            return Err(AproError::IncompatibleFormat(
                "unsupported backup manifest version, backend, or number of files".into(),
            ));
        }
        let observed = inventory_files(&path.join("data"))?;
        if observed != manifest.files {
            return Err(AproError::Corrupt(
                "backup inventory or checksum differs from manifest".into(),
            ));
        }
        Ok(manifest)
    }

    pub fn restore_backup(
        backup: impl AsRef<Path>,
        destination: impl AsRef<Path>,
        mut config: EngineConfig,
    ) -> Result<RestoreReport> {
        let backup = backup.as_ref();
        let destination = destination.as_ref();
        if destination.exists() {
            return Err(AproError::InvalidInput(format!(
                "restore destination already exists: {}",
                destination.display()
            )));
        }
        let manifest = Self::verify_backup(backup)?;
        if manifest.encrypted != config.encryption.is_some() {
            return Err(AproError::IncompatibleFormat(
                "restore encryption configuration differs from the backup".into(),
            ));
        }
        if manifest.encrypted {
            let available = config
                .encryption
                .as_ref()
                .map(EncryptionConfig::key_ids)
                .unwrap_or_default();
            if manifest
                .encryption_key_ids
                .iter()
                .any(|required| !available.contains(required))
            {
                return Err(AproError::Encryption(
                    "restore keyring does not contain all backup key IDs".into(),
                ));
            }
        }
        let destination_parent = destination.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(destination_parent).map_err(io_storage_error)?;
        let restore_bytes = manifest.files.iter().map(|file| file.bytes).sum::<u64>();
        ensure_available_space(
            destination_parent,
            restore_bytes.saturating_add(config.min_free_disk_bytes),
            "restore",
        )?;
        copy_inventory(&backup.join("data"), destination, &manifest.files)?;
        config.path = destination.to_path_buf();
        let restored = Engine::open(config)?;
        let catalog = restored.catalog.read().clone();
        if catalog.generation != manifest.catalog_generation
            || catalog.durable_watermarks != manifest.durable_watermarks
        {
            return Err(AproError::Corrupt(
                "restore with divergent catalog generation or watermarks".into(),
            ));
        }
        let verification = restored.verify()?;
        drop(restored);
        Ok(RestoreReport {
            path: destination.to_path_buf(),
            files_restored: manifest.files.len(),
            bytes_restored: manifest.files.iter().map(|file| file.bytes).sum(),
            verification,
        })
    }

    pub fn repair_derived_to_copy(
        &self,
        destination: impl AsRef<Path>,
        confirmation: &str,
    ) -> Result<RepairReport> {
        self.ensure_healthy()?;
        if confirmation != REPAIR_DERIVED_CONFIRMATION {
            return Err(AproError::InvalidInput(format!(
                "repair requires the exact confirmation string {REPAIR_DERIVED_CONFIRMATION}"
            )));
        }
        let destination = destination.as_ref();
        self.create_checkpoint(destination)?;
        let mut config = self.config.clone();
        config.path = destination.to_path_buf();
        let repaired = Engine::open(config)?;
        let mut report = repaired.rebuild_derived_indexes()?;
        let definitions = repaired.backend.scan_prefix(
            StorageSpace::Surfaces,
            b"d",
            repaired.config.max_surfaces.saturating_add(1),
        )?;
        if definitions.len() > repaired.config.max_surfaces {
            return Err(AproError::ResourceLimit(
                "number of surfaces exceeds repair limit".into(),
            ));
        }
        for (_, bytes) in definitions {
            let definition: SurfaceDefinition =
                decode_logical(LogicalFrameKind::SurfaceDefinition, &bytes)?;
            repaired.rebuild_surface(&definition.id, Durability::Durable)?;
            report.surfaces_rebuilt = report.surfaces_rebuilt.saturating_add(1);
        }
        report.verification = repaired.verify()?;
        drop(repaired);
        Ok(report)
    }

    pub fn verify(&self) -> Result<VerificationReport> {
        self.ensure_healthy()?;
        let records_end = vec![u8::MAX; self.config.limits.max_storage_key_bytes];
        let heads_checked =
            self.visit_space_range(StorageSpace::Records, &[], &records_end, |_, bytes| {
                let head: HeadPointer = decode_logical(LogicalFrameKind::Head, bytes)?;
                let record = self.get_version(&head.identity, head.version)?;
                if record.tombstone != head.tombstone {
                    return Err(AproError::Corrupt(
                        "head and version differ on tombstone flag".into(),
                    ));
                }
                let radial = self.load_radial_descriptor(&head.identity)?;
                if head.tombstone {
                    if radial.is_some() {
                        return Err(AproError::Corrupt(
                            "tombstone with current radial descriptor present".into(),
                        ));
                    }
                } else if radial
                    .as_ref()
                    .map(|descriptor| descriptor.canonical_version)
                    != Some(head.version)
                {
                    return Err(AproError::Corrupt(
                        "radial descriptor missing or on incorrect version".into(),
                    ));
                }
                if let Some(expires_at) = record.expires_at_unix_ms {
                    let entry = self
                        .backend
                        .get(StorageSpace::Ttl, &ttl_key(expires_at, &head.identity))?
                        .ok_or_else(|| AproError::Corrupt("missing TTL index".into()))?;
                    let entry: TtlEntry = decode_logical(LogicalFrameKind::Ttl, &entry)?;
                    if entry.identity != head.identity || entry.version != head.version {
                        return Err(AproError::Corrupt(
                            "TTL index refers to incorrect record/version".into(),
                        ));
                    }
                }
                if !record.tombstone {
                    let workflow = self
                        .backend
                        .get(StorageSpace::Workflow, &workflow_key(&record)?)?
                        .ok_or_else(|| AproError::Corrupt("missing workflow index".into()))?;
                    let workflow: WorkflowIndexEntry =
                        decode_logical(LogicalFrameKind::Workflow, &workflow)?;
                    if workflow.identity != head.identity || workflow.version != head.version {
                        return Err(AproError::Corrupt(
                            "workflow index refers to incorrect record/version".into(),
                        ));
                    }
                }
                Ok(())
            })?;
        let catalog = self.catalog.read().clone();
        let mut events_checked = 0usize;
        let mut max_sequence_by_shard = BTreeMap::new();
        for shard in 0..self.config.shards {
            let mut observed = 0;
            events_checked = events_checked.saturating_add(self.visit_space_range(
                StorageSpace::Events,
                &event_key(shard, 0),
                &event_key(shard, u64::MAX),
                |_, bytes| {
                    let event: ChangeEvent = decode_logical(LogicalFrameKind::Change, bytes)?;
                    if let ChangeBody::VersionRef { identity, version } = &event.body {
                        self.get_version(identity, *version)?;
                    }
                    if event.version.shard_id != shard || event.version.sequence <= observed {
                        return Err(AproError::Corrupt(format!(
                            "event in shard {shard} out of order or routed elsewhere"
                        )));
                    }
                    observed = event.version.sequence;
                    Ok(())
                },
            )?);
            let declared = catalog.shard_sequences.get(&shard).copied().unwrap_or(0);
            if observed != 0 && observed != declared {
                return Err(AproError::Corrupt(format!(
                    "shard {shard}: catalog has {declared}, last event is {observed}"
                )));
            }
            max_sequence_by_shard.insert(shard, observed);
        }
        let definitions = self.backend.scan_prefix(
            StorageSpace::Surfaces,
            b"d",
            self.config.max_surfaces.saturating_add(1),
        )?;
        if definitions.len() > self.config.max_surfaces {
            return Err(AproError::ResourceLimit(
                "number of surfaces exceeds verifiable limit".into(),
            ));
        }
        for (_, bytes) in &definitions {
            let definition: SurfaceDefinition =
                decode_logical(LogicalFrameKind::SurfaceDefinition, bytes)?;
            validate_surface_definition(&definition, &self.config)?;
            let pointer = self.load_surface_pointer(&definition.id)?;
            if pointer.retained_generations.len() > definition.retained_generations
                || pointer
                    .current_generation
                    .is_some_and(|current| !pointer.retained_generations.contains(&current))
            {
                return Err(AproError::Corrupt(
                    "surface retention or current generation inconsistent".into(),
                ));
            }
            for generation in &pointer.retained_generations {
                self.load_surface_generation(&definition.id, *generation)?;
            }
            if pointer.current_generation.is_some() {
                self.surface_records(&definition, &pointer)?;
            }
            let consumer = surface_consumer_name(&definition.id);
            let collection_key = surface_collection_key(&definition);
            for (shard, sequence) in &pointer.source_watermarks {
                if *sequence > catalog.shard_sequences.get(shard).copied().unwrap_or(0)
                    || catalog
                        .consumer_watermarks
                        .get(&consumer_watermark_key(&collection_key, *shard, &consumer)?)
                        .copied()
                        != Some(*sequence)
                {
                    return Err(AproError::Corrupt(
                        "surface watermark and catalog diverge".into(),
                    ));
                }
            }
        }
        let dictionaries = self.backend.scan_prefix(
            StorageSpace::Compression,
            b"dictionary:",
            self.config.max_dictionaries.saturating_add(1),
        )?;
        if dictionaries.len() > self.config.max_dictionaries {
            return Err(AproError::ResourceLimit(
                "number of dictionaries exceeds verifiable limit".into(),
            ));
        }
        for (key, bytes) in &dictionaries {
            let dictionary: CompressionDictionary =
                decode_logical(LogicalFrameKind::CompressionDictionary, bytes)?;
            if key != &compression_dictionary_key(dictionary.id)
                || dictionary.id == 0
                || dictionary.bytes.is_empty()
                || dictionary.bytes.len() > self.config.max_dictionary_bytes
                || crc32fast::hash(&dictionary.bytes) != dictionary.checksum
            {
                return Err(AproError::Corrupt(
                    "dictionary with inconsistent key, size, or checksum".into(),
                ));
            }
        }
        let mut expected_audit_sequence = 1u64;
        let audit_events_checked = self.visit_space_range(
            StorageSpace::Audit,
            &audit_key(1),
            &audit_key(u64::MAX),
            |key, bytes| {
                let event: AuditEvent = decode_logical(LogicalFrameKind::Audit, bytes)?;
                validate_audit_event(&event)?;
                if event.sequence != expected_audit_sequence || key != audit_key(event.sequence) {
                    return Err(AproError::Corrupt(
                        "inconsistent audit event sequence or key".into(),
                    ));
                }
                expected_audit_sequence = expected_audit_sequence.saturating_add(1);
                Ok(())
            },
        )?;
        if u64::try_from(audit_events_checked).unwrap_or(u64::MAX)
            != self.audit_sequence.load(Ordering::Acquire)
        {
            return Err(AproError::Corrupt(
                "audit state and event count diverge".into(),
            ));
        }
        Ok(VerificationReport {
            heads_checked,
            events_checked,
            surfaces_checked: definitions.len(),
            dictionaries_checked: dictionaries.len(),
            audit_events_checked,
            max_sequence_by_shard,
        })
    }

    fn rebuild_derived_indexes(&self) -> Result<RepairReport> {
        self.clear_derived_space(StorageSpace::Radial, Some(RADIAL_STATE_KEY))?;
        self.clear_derived_space(StorageSpace::Ttl, None)?;
        self.clear_derived_space(StorageSpace::Workflow, None)?;
        self.clear_derived_space(StorageSpace::IdempotencyExpiry, None)?;

        let mut radial_rebuilt = 0usize;
        let mut ttl_rebuilt = 0usize;
        let mut workflow_rebuilt = 0usize;
        let mut pending = StorageBatch::with_capacity(self.config.limits.max_batch_operations);
        let end = vec![u8::MAX; self.config.limits.max_storage_key_bytes];
        let heads_scanned =
            self.visit_space_range(StorageSpace::Records, &[], &end, |_, bytes| {
                let head: HeadPointer = decode_logical(LogicalFrameKind::Head, bytes)?;
                let record = self.get_version(&head.identity, head.version)?;
                if record.tombstone != head.tombstone {
                    return Err(AproError::Corrupt(
                        "repair: head and version mismatch".into(),
                    ));
                }
                if !record.tombstone {
                    let stored_bytes = self
                        .backend
                        .get(
                            StorageSpace::Versions,
                            &version_key(&record.identity, record.version),
                        )?
                        .ok_or_else(|| {
                            AproError::Corrupt("repair: canonical version missing".into())
                        })?
                        .len();
                    let radial_policy = self
                        .radial_state
                        .read()
                        .policies
                        .get(&record.identity.collection_key())
                        .cloned()
                        .unwrap_or_default();
                    let descriptor = RadialDescriptor {
                        identity: record.identity.clone(),
                        canonical_version: record.version,
                        created_at_unix_ms: record.created_at_unix_ms,
                        updated_at_unix_ms: record.updated_at_unix_ms,
                        access_frequency_estimate: 0,
                        last_access_sampled_unix_ms: None,
                        freshness_half_life_ms: radial_policy.freshness_half_life_ms,
                        urgency_millis: 0,
                        deadline_unix_ms: record.expires_at_unix_ms,
                        workflow_state: record.workflow.state.clone(),
                        projection_watermarks: BTreeMap::new(),
                        reconstruction_cost_micros: 0,
                        logical_bytes: u64::try_from(estimate_record(&record)).unwrap_or(u64::MAX),
                        physical_bytes: u64::try_from(stored_bytes).unwrap_or(u64::MAX),
                        storage_class: "primary".into(),
                        admin_pin_until_unix_ms: None,
                        layer: RadialLayer::Warm,
                        layer_since_unix_ms: record.updated_at_unix_ms,
                        last_decision: "repair derivato: hint radiali reimpostati".into(),
                    };
                    pending.put(
                        StorageSpace::Radial,
                        radial_key(&record.identity),
                        encode_logical(LogicalFrameKind::Radial, &descriptor)?,
                    );
                    radial_rebuilt = radial_rebuilt.saturating_add(1);
                    let workflow = WorkflowIndexEntry {
                        identity: record.identity.clone(),
                        version: record.version,
                        state: record.workflow.state.clone(),
                        available_at_unix_ms: workflow_available_at(&record.workflow),
                    };
                    pending.put(
                        StorageSpace::Workflow,
                        workflow_key(&record)?,
                        encode_logical(LogicalFrameKind::Workflow, &workflow)?,
                    );
                    workflow_rebuilt = workflow_rebuilt.saturating_add(1);
                    if let Some(expires_at_unix_ms) = record.expires_at_unix_ms {
                        let ttl = TtlEntry {
                            identity: record.identity.clone(),
                            version: record.version,
                            expires_at_unix_ms,
                        };
                        pending.put(
                            StorageSpace::Ttl,
                            ttl_key(expires_at_unix_ms, &record.identity),
                            encode_logical(LogicalFrameKind::Ttl, &ttl)?,
                        );
                        ttl_rebuilt = ttl_rebuilt.saturating_add(1);
                    }
                    if pending.len() >= self.config.limits.max_batch_operations {
                        let batch = std::mem::replace(
                            &mut pending,
                            StorageBatch::with_capacity(self.config.limits.max_batch_operations),
                        );
                        self.commit_primary(batch, CommitMode::Durable)?;
                    }
                }
                Ok(())
            })?;
        if !pending.is_empty() {
            self.commit_primary(pending, CommitMode::Durable)?;
        }

        let mut idempotency_expiry_rebuilt = 0usize;
        let mut pending = StorageBatch::with_capacity(self.config.limits.max_batch_operations);
        self.visit_space_range(StorageSpace::Idempotency, &[], &end, |key, bytes| {
            let record: IdempotencyRecord = decode_logical(LogicalFrameKind::Idempotency, bytes)?;
            let expiry = IdempotencyExpiryEntry {
                lookup_key: key.to_vec(),
                expires_at_unix_ms: record.expires_at_unix_ms,
            };
            pending.put(
                StorageSpace::IdempotencyExpiry,
                idempotency_expiry_key(record.expires_at_unix_ms, key),
                encode_logical(LogicalFrameKind::IdempotencyExpiry, &expiry)?,
            );
            idempotency_expiry_rebuilt = idempotency_expiry_rebuilt.saturating_add(1);
            if pending.len() >= self.config.limits.max_batch_operations {
                let batch = std::mem::replace(
                    &mut pending,
                    StorageBatch::with_capacity(self.config.limits.max_batch_operations),
                );
                self.commit_primary(batch, CommitMode::Durable)?;
            }
            Ok(())
        })?;
        if !pending.is_empty() {
            self.commit_primary(pending, CommitMode::Durable)?;
        }
        Ok(RepairReport {
            destination: self.config.path.clone(),
            heads_scanned,
            radial_rebuilt,
            ttl_rebuilt,
            workflow_rebuilt,
            idempotency_expiry_rebuilt,
            surfaces_rebuilt: 0,
            records_lost: 0,
            records_doubtful: 0,
            radial_hints_reset: radial_rebuilt,
            verification: VerificationReport::default(),
        })
    }

    fn clear_derived_space(&self, space: StorageSpace, preserve_key: Option<&[u8]>) -> Result<()> {
        let end = vec![u8::MAX; self.config.limits.max_storage_key_bytes];
        loop {
            let rows = self.backend.scan_range(
                space,
                &[],
                &end,
                self.config.limits.max_batch_operations,
            )?;
            let mut batch = StorageBatch::with_capacity(rows.len());
            for (key, _) in rows {
                if preserve_key != Some(key.as_slice()) {
                    batch.delete(space, key);
                }
            }
            if batch.is_empty() {
                break;
            }
            self.commit_primary(batch, CommitMode::Durable)?;
        }
        Ok(())
    }

    fn ensure_workflow_retention(&self, collection_key: &[u8]) -> Result<()> {
        if self
            .catalog
            .read()
            .collections
            .get(collection_key)
            .is_some_and(|policy| policy.retention_mode == EventRetentionMode::Delta)
        {
            return Err(AproError::Unsupported(
                "workflow on a Delta collection requires declared transition deltas".into(),
            ));
        }
        Ok(())
    }

    fn ensure_surface_retention(&self, definition: &SurfaceDefinition) -> Result<()> {
        if self
            .catalog
            .read()
            .collections
            .get(&surface_collection_key(definition))
            .is_some_and(|policy| policy.retention_mode == EventRetentionMode::Delta)
        {
            return Err(AproError::Unsupported(
                "generic builder does not support a Delta source without a declared applier".into(),
            ));
        }
        Ok(())
    }

    fn load_dictionary(&self, id: u64) -> Result<Arc<CompressionDictionary>> {
        if id == 0 {
            return Err(AproError::Corrupt("dictionary ID zero".into()));
        }
        if let Some(dictionary) = self.dictionaries.read().get(&id).cloned() {
            return Ok(dictionary);
        }
        let bytes = self
            .backend
            .get(StorageSpace::Compression, &compression_dictionary_key(id))?
            .ok_or_else(|| AproError::Corrupt(format!("dictionary {id} missing")))?;
        let dictionary: CompressionDictionary =
            decode_logical(LogicalFrameKind::CompressionDictionary, &bytes)?;
        if dictionary.id != id
            || dictionary.bytes.is_empty()
            || dictionary.bytes.len() > self.config.max_dictionary_bytes
            || crc32fast::hash(&dictionary.bytes) != dictionary.checksum
        {
            return Err(AproError::Corrupt(format!(
                "dictionary {id} is invalid or has a checksum mismatch"
            )));
        }
        let dictionary = Arc::new(dictionary);
        self.dictionaries
            .write()
            .insert(id, Arc::clone(&dictionary));
        Ok(dictionary)
    }

    fn load_surface_definition(&self, projection_id: &str) -> Result<Option<SurfaceDefinition>> {
        self.backend
            .get(
                StorageSpace::Surfaces,
                &surface_definition_key(projection_id),
            )?
            .map(|bytes| {
                decode_logical::<SurfaceDefinition>(LogicalFrameKind::SurfaceDefinition, &bytes)
            })
            .transpose()
    }

    fn load_surface_pointer(&self, projection_id: &str) -> Result<SurfacePointer> {
        let bytes = self
            .backend
            .get(StorageSpace::Surfaces, &surface_pointer_key(projection_id))?
            .ok_or_else(|| AproError::Corrupt("definition without surface pointer".into()))?;
        let pointer: SurfacePointer = decode_logical(LogicalFrameKind::SurfacePointer, &bytes)?;
        if pointer.projection_id != projection_id {
            return Err(AproError::Corrupt(
                "pointer associated with different projection".into(),
            ));
        }
        Ok(pointer)
    }

    fn load_surface_generation(
        &self,
        projection_id: &str,
        generation: u64,
    ) -> Result<SurfaceGeneration> {
        let bytes = self
            .backend
            .get(
                StorageSpace::Surfaces,
                &surface_generation_key(projection_id, generation),
            )?
            .ok_or_else(|| AproError::Corrupt("surface generation missing".into()))?;
        let value: SurfaceGeneration = decode_logical(LogicalFrameKind::SurfaceGeneration, &bytes)?;
        if value.projection_id != projection_id || value.generation != generation {
            return Err(AproError::Corrupt(
                "surface generation associated with a different id".into(),
            ));
        }
        Ok(value)
    }

    fn surface_records(
        &self,
        definition: &SurfaceDefinition,
        pointer: &SurfacePointer,
    ) -> Result<BTreeMap<RecordIdentity, RecordEnvelope>> {
        let Some(generation) = pointer.current_generation else {
            return Ok(BTreeMap::new());
        };
        let generation = self.load_surface_generation(&definition.id, generation)?;
        if generation.format != definition.format
            || generation.source_watermarks != pointer.source_watermarks
        {
            return Err(AproError::Corrupt(
                "pointer and surface generation diverge".into(),
            ));
        }
        let records = decode_surface_payload(generation.format, &generation.serialized)?;
        if records.len() != generation.record_count {
            return Err(AproError::Corrupt(
                "surface record count inconsistent".into(),
            ));
        }
        Ok(records
            .into_iter()
            .map(|record| (record.identity.clone(), record))
            .collect())
    }

    fn apply_surface_event(
        &self,
        definition: &SurfaceDefinition,
        records: &mut BTreeMap<RecordIdentity, RecordEnvelope>,
        event: &ChangeEvent,
    ) -> Result<()> {
        let identity = identity_from_event(event)?;
        if event.operation == ChangeOperation::Delete {
            records.remove(&identity);
            return Ok(());
        }
        let record = match &event.body {
            ChangeBody::VersionRef { identity, version } => self.get_version(identity, *version)?,
            ChangeBody::SelfContained { record } => (**record).clone(),
            ChangeBody::Delta { .. } => {
                return Err(AproError::Unsupported(
                    "generic surface cannot apply a domain delta".into(),
                ));
            }
        };
        if record.identity != identity || record.version != event.version {
            return Err(AproError::Corrupt(
                "surface event and source record diverge".into(),
            ));
        }
        if surface_accepts(definition, &record, now_unix_ms()?) {
            records.insert(identity, record);
        } else {
            records.remove(&identity);
        }
        Ok(())
    }

    fn publish_surface_generation(
        &self,
        definition: &SurfaceDefinition,
        mut pointer: SurfacePointer,
        records: BTreeMap<RecordIdentity, RecordEnvelope>,
        source_watermarks: BTreeMap<u32, u64>,
        events_applied: usize,
        _durability: Durability,
    ) -> Result<SurfaceBuildReport> {
        let (record_count, serialized) = serialize_bounded_surface(definition, records)?;
        let generation_id = pointer.next_generation;
        pointer.next_generation = pointer
            .next_generation
            .checked_add(1)
            .ok_or_else(|| AproError::ResourceLimit("exhausted surface generation".into()))?;
        pointer.current_generation = Some(generation_id);
        pointer.source_watermarks = source_watermarks.clone();
        pointer.retained_generations.push(generation_id);
        let generation = SurfaceGeneration {
            projection_id: definition.id.clone(),
            generation: generation_id,
            source_watermarks: source_watermarks.clone(),
            format: definition.format,
            record_count,
            serialized,
            created_at_unix_ms: now_unix_ms()?,
        };
        let _catalog_writer = self.catalog_writer.lock();
        let mut catalog = self.catalog.read().clone();
        let collection_key = surface_collection_key(definition);
        let consumer = surface_consumer_name(&definition.id);
        for (shard, sequence) in &source_watermarks {
            let latest = catalog.shard_sequences.get(shard).copied().unwrap_or(0);
            if *sequence > latest {
                return Err(AproError::Conflict(format!(
                    "surface watermark {sequence} beyond latest {latest} for shard {shard}"
                )));
            }
            let key = consumer_watermark_key(&collection_key, *shard, &consumer)?;
            let previous = catalog.consumer_watermarks.get(&key).copied().unwrap_or(0);
            if *sequence < previous {
                return Err(AproError::Conflict(
                    "non-monotonic surface watermark".into(),
                ));
            }
            catalog.consumer_watermarks.insert(key, *sequence);
        }
        catalog.generation = catalog.generation.saturating_add(1);
        let mut batch =
            StorageBatch::with_capacity(pointer.retained_generations.len().saturating_add(3));
        let keep = definition
            .retained_generations
            .min(self.config.max_retained_surface_generations);
        while pointer.retained_generations.len() > keep {
            let removed = pointer.retained_generations.remove(0);
            batch.delete(
                StorageSpace::Surfaces,
                surface_generation_key(&definition.id, removed),
            );
        }
        batch.put(
            StorageSpace::Surfaces,
            surface_generation_key(&definition.id, generation_id),
            encode_logical(LogicalFrameKind::SurfaceGeneration, &generation)?,
        );
        batch.put(
            StorageSpace::Surfaces,
            surface_pointer_key(&definition.id),
            encode_logical(LogicalFrameKind::SurfacePointer, &pointer)?,
        );
        batch.put(
            StorageSpace::Catalog,
            CATALOG_KEY.to_vec(),
            encode_logical(LogicalFrameKind::Catalog, &catalog)?,
        );
        self.commit_primary(batch, CommitMode::Durable)?;
        *self.catalog.write() = catalog;
        Ok(SurfaceBuildReport {
            projection_id: definition.id.clone(),
            generation: generation_id,
            events_applied,
            source_watermarks,
            record_count,
            serialized_bytes: generation.serialized.len(),
        })
    }

    fn workflow_candidates(
        &self,
        scope: &WorkflowScope,
        state: &str,
        now_unix_ms: u64,
    ) -> Result<Vec<RecordEnvelope>> {
        let rows = self.backend.scan_prefix(
            StorageSpace::Workflow,
            &workflow_prefix(&scope.partition_key(), state)?,
            self.config.limits.max_queue_depth,
        )?;
        let mut records = Vec::new();
        for (_, bytes) in rows {
            let entry: WorkflowIndexEntry = decode_logical(LogicalFrameKind::Workflow, &bytes)?;
            if entry.state != state {
                return Err(AproError::Corrupt(
                    "workflow index with inconsistent state".into(),
                ));
            }
            let Some(head) = self.current_head(&entry.identity)? else {
                continue;
            };
            if head.tombstone || head.version != entry.version {
                continue;
            }
            let record = self.get_version(&entry.identity, entry.version)?;
            if record.workflow.state != state
                || record
                    .expires_at_unix_ms
                    .is_some_and(|expires| expires <= now_unix_ms)
            {
                continue;
            }
            if state == "leased" && !self.lease_expired(&record, now_unix_ms)? {
                continue;
            }
            records.push(record);
        }
        Ok(records)
    }

    fn lease_expired(&self, record: &RecordEnvelope, now_unix_ms: u64) -> Result<bool> {
        let lease_id = record
            .workflow
            .lease_id
            .ok_or_else(|| AproError::Corrupt("leased record without lease ID".into()))?;
        if let Some(deadline) = self
            .lease_deadlines
            .lock()
            .get(&(record.identity.clone(), lease_id))
            .copied()
        {
            return Ok(Instant::now() >= deadline);
        }
        let persisted = record
            .workflow
            .lease_deadline_unix_ms
            .ok_or_else(|| AproError::Corrupt("leased record without deadline".into()))?;
        Ok(now_unix_ms
            >= persisted.saturating_add(duration_millis(self.config.lease_recovery_safety_margin)?))
    }

    fn active_lease_record(
        &self,
        identity: &RecordIdentity,
        lease: LeaseProof,
        now_unix_ms: u64,
    ) -> Result<RecordEnvelope> {
        let record = self
            .get(identity)?
            .ok_or_else(|| AproError::InvalidInput("leased record not present".into()))?;
        if record.workflow.state != "leased"
            || record.workflow.lease_id != Some(lease.lease_id)
            || record.workflow.fencing_token != lease.fencing_token
        {
            return Err(AproError::Conflict(
                "lease ID or fencing token outdated".into(),
            ));
        }
        if self.lease_expired(&record, now_unix_ms)? {
            return Err(AproError::Conflict("lease expired".into()));
        }
        Ok(record)
    }

    fn commit_workflow_record_locked(
        &self,
        shard: u32,
        command: WorkflowCommit,
    ) -> Result<MutationReceipt> {
        self.ensure_workflow_retention(&command.current.identity.collection_key())?;
        let request = workflow_put_request(
            command.current,
            command.workflow,
            command.operation,
            command.idempotency_key_hash,
        )?;
        self.commit_mutations(
            shard,
            vec![AtomicMutation::Put(request)],
            command.durability,
            command.idempotency,
        )
        .map(|mut receipts| receipts.remove(0))
    }

    fn single_workflow_replay(
        &self,
        mut receipts: Vec<MutationReceipt>,
    ) -> Result<WorkflowMutationResult> {
        if receipts.len() != 1 {
            return Err(AproError::Corrupt(
                "idempotent workflow result has no receipt".into(),
            ));
        }
        self.workflow_result(receipts.remove(0))
    }

    fn workflow_result(&self, receipt: MutationReceipt) -> Result<WorkflowMutationResult> {
        let event_bytes = self
            .backend
            .get(
                StorageSpace::Events,
                &event_key(receipt.version.shard_id, receipt.version.sequence),
            )?
            .ok_or_else(|| AproError::Corrupt("just written workflow event missing".into()))?;
        let event: ChangeEvent = decode_logical(LogicalFrameKind::Change, &event_bytes)?;
        let identity = identity_from_event(&event)?;
        let record = self.get_version(&identity, receipt.version)?;
        Ok(WorkflowMutationResult { record, receipt })
    }

    fn receipt_for_version(
        &self,
        version: Version,
        durability: Durability,
    ) -> Result<MutationReceipt> {
        if durability == Durability::Durable
            && self.durable_watermarks[version.shard_id as usize].load(Ordering::Acquire)
                < version.sequence
        {
            self.sync()?;
        }
        let batch_id = self
            .backend
            .get(
                StorageSpace::Events,
                &event_key(version.shard_id, version.sequence),
            )?
            .map(|bytes| decode_logical::<ChangeEvent>(LogicalFrameKind::Change, &bytes))
            .transpose()?
            .map_or_else(
                || batch_id(version.epoch, version.shard_id, version.sequence),
                |event| event.batch_id,
            );
        Ok(MutationReceipt {
            version,
            durability,
            durable_watermark: self.durable_watermarks[version.shard_id as usize]
                .load(Ordering::Acquire),
            batch_id,
        })
    }

    fn lookup_idempotency(
        &self,
        context: &IdempotencyContext,
        now_unix_ms: u64,
    ) -> Result<Option<Vec<MutationReceipt>>> {
        let lookup_key = idempotency_lookup_key(&context.scope, &context.key_hash);
        let Some(bytes) = self.backend.get(StorageSpace::Idempotency, &lookup_key)? else {
            return Ok(None);
        };
        let record: IdempotencyRecord = decode_logical(LogicalFrameKind::Idempotency, &bytes)?;
        if record.scope != context.scope || record.key_hash != context.key_hash {
            return Err(AproError::Corrupt(
                "idempotency record is associated with a different key".into(),
            ));
        }
        if record.expires_at_unix_ms <= now_unix_ms {
            return Ok(None);
        }
        if record.request_fingerprint != context.request_fingerprint {
            return Err(AproError::Conflict(
                "idempotency key reused for a different request".into(),
            ));
        }
        Ok(Some(record.receipts))
    }

    fn persist_idempotency_outcome(
        &self,
        context: &IdempotencyContext,
        receipts: Vec<MutationReceipt>,
        now_unix_ms: u64,
        durability: Durability,
    ) -> Result<()> {
        let _catalog_writer = self.catalog_writer.lock();
        if self.lookup_idempotency(context, now_unix_ms)?.is_some() {
            return Ok(());
        }
        let lookup_key = idempotency_lookup_key(&context.scope, &context.key_hash);
        let previous = self
            .backend
            .get(StorageSpace::Idempotency, &lookup_key)?
            .map(|bytes| decode_logical::<IdempotencyRecord>(LogicalFrameKind::Idempotency, &bytes))
            .transpose()?;
        let expires_at_unix_ms = now_unix_ms
            .checked_add(duration_millis(self.config.idempotency_retention)?)
            .ok_or_else(|| {
                AproError::ResourceLimit("idempotency expiration exceeds u64 limit".into())
            })?;
        let record = IdempotencyRecord {
            scope: context.scope.clone(),
            key_hash: context.key_hash,
            request_fingerprint: context.request_fingerprint,
            receipts,
            expires_at_unix_ms,
        };
        let expiry = IdempotencyExpiryEntry {
            lookup_key: lookup_key.clone(),
            expires_at_unix_ms,
        };
        let mut batch = StorageBatch::with_capacity(3);
        if let Some(previous) = previous {
            batch.delete(
                StorageSpace::IdempotencyExpiry,
                idempotency_expiry_key(previous.expires_at_unix_ms, &lookup_key),
            );
        }
        batch.put(
            StorageSpace::Idempotency,
            lookup_key.clone(),
            encode_logical(LogicalFrameKind::Idempotency, &record)?,
        );
        batch.put(
            StorageSpace::IdempotencyExpiry,
            idempotency_expiry_key(expires_at_unix_ms, &lookup_key),
            encode_logical(LogicalFrameKind::IdempotencyExpiry, &expiry)?,
        );
        self.commit_primary(
            batch,
            if durability == Durability::Durable {
                CommitMode::Durable
            } else {
                CommitMode::Relaxed
            },
        )
    }

    fn claimed_from_receipts(
        &self,
        receipts: Vec<MutationReceipt>,
        server_time_unix_ms: u64,
    ) -> Result<Vec<ClaimedRecord>> {
        let mut claimed = Vec::with_capacity(receipts.len());
        for receipt in receipts {
            let event_bytes = self
                .backend
                .get(
                    StorageSpace::Events,
                    &event_key(receipt.version.shard_id, receipt.version.sequence),
                )?
                .ok_or_else(|| AproError::Corrupt("missing Claim event".into()))?;
            let event: ChangeEvent = decode_logical(LogicalFrameKind::Change, &event_bytes)?;
            let identity = identity_from_event(&event)?;
            let record = self.get_version(&identity, receipt.version)?;
            let lease_id = record
                .workflow
                .lease_id
                .ok_or_else(|| AproError::Corrupt("Claim result missing lease id".into()))?;
            let lease_deadline_unix_ms = record
                .workflow
                .lease_deadline_unix_ms
                .ok_or_else(|| AproError::Corrupt("Claim result missing deadline".into()))?;
            if lease_deadline_unix_ms > server_time_unix_ms {
                let remaining = Duration::from_millis(
                    lease_deadline_unix_ms.saturating_sub(server_time_unix_ms),
                );
                if let Some(deadline) = Instant::now().checked_add(remaining) {
                    self.lease_deadlines
                        .lock()
                        .insert((identity.clone(), lease_id), deadline);
                }
            }
            claimed.push(ClaimedRecord {
                receipt,
                lease: LeaseProof {
                    lease_id,
                    fencing_token: record.workflow.fencing_token,
                },
                lease_deadline_unix_ms,
                server_time_unix_ms,
                retry_after_ms: 0,
                record,
            });
        }
        Ok(claimed)
    }

    fn commit_mutations(
        &self,
        shard: u32,
        mutations: Vec<AtomicMutation>,
        durability: Durability,
        idempotency: Option<IdempotencyContext>,
    ) -> Result<Vec<MutationReceipt>> {
        let catalog_writer = self.catalog_writer.lock();
        let now = now_unix_ms()?;
        let mut previous_idempotency_expiry = None;
        if let Some(context) = &idempotency {
            let lookup_key = idempotency_lookup_key(&context.scope, &context.key_hash);
            if let Some(bytes) = self.backend.get(StorageSpace::Idempotency, &lookup_key)? {
                let existing: IdempotencyRecord =
                    decode_logical(LogicalFrameKind::Idempotency, &bytes)?;
                if existing.scope != context.scope || existing.key_hash != context.key_hash {
                    return Err(AproError::Corrupt(
                        "idempotency record is associated with a different key".into(),
                    ));
                }
                if existing.expires_at_unix_ms > now {
                    if existing.request_fingerprint != context.request_fingerprint {
                        return Err(AproError::Conflict(
                            "idempotency key reused for a different request".into(),
                        ));
                    }
                    return Ok(existing.receipts);
                }
                previous_idempotency_expiry = Some(existing.expires_at_unix_ms);
            }
        }
        let mut catalog = self.catalog.read().clone();
        let mut next_sequence = catalog.shard_sequences.get(&shard).copied().unwrap_or(0);
        let first_sequence = next_sequence
            .checked_add(1)
            .ok_or_else(|| AproError::ResourceLimit("sequence exhausted".into()))?;
        let batch_id = batch_id(catalog.epoch, shard, first_sequence);
        let mut storage = StorageBatch::with_capacity(mutations.len() * 8 + 4);
        let mut versions = Vec::with_capacity(mutations.len());

        for mutation in mutations {
            let identity = mutation.identity().clone();
            let current = self.current_head(&identity)?;
            validate_expected(
                mutation.expected(),
                current.as_ref().map(|head| head.version),
            )?;
            let previous_record = current
                .as_ref()
                .map(|head| self.get_version(&identity, head.version))
                .transpose()?;
            let previous_radial = self.load_radial_descriptor(&identity)?;
            next_sequence = next_sequence
                .checked_add(1)
                .ok_or_else(|| AproError::ResourceLimit("sequence exhausted".into()))?;
            let version = Version {
                epoch: catalog.epoch,
                shard_id: shard,
                sequence: next_sequence,
            };
            let collection_key = identity.collection_key();
            let policy = catalog
                .collections
                .entry(collection_key)
                .or_default()
                .clone();

            let (mut record, operation, idempotency_key_hash, delta) = match mutation {
                AtomicMutation::Put(request) => {
                    let created_at = previous_record
                        .as_ref()
                        .map_or(now, |record| record.created_at_unix_ms);
                    let record = RecordEnvelope {
                        identity: identity.clone(),
                        payload: Some(request.payload),
                        content_type: request.content_type,
                        version,
                        created_at_unix_ms: created_at,
                        updated_at_unix_ms: now,
                        expires_at_unix_ms: request.expires_at_unix_ms,
                        metadata: request.metadata,
                        workflow: request.workflow_override.unwrap_or_else(|| {
                            previous_record
                                .as_ref()
                                .map_or_else(WorkflowDescriptor::default, |record| {
                                    record.workflow.clone()
                                })
                        }),
                        idempotency_key_hash: request.idempotency_key_hash,
                        dictionary_id: None,
                        tombstone: false,
                    };
                    (
                        record,
                        request.operation,
                        request.idempotency_key_hash,
                        request.delta,
                    )
                }
                AtomicMutation::Delete(request) => {
                    let record = RecordEnvelope {
                        identity: identity.clone(),
                        payload: None,
                        content_type: previous_record
                            .as_ref()
                            .map_or_else(String::new, |record| record.content_type.clone()),
                        version,
                        created_at_unix_ms: previous_record
                            .as_ref()
                            .map_or(now, |record| record.created_at_unix_ms),
                        updated_at_unix_ms: now,
                        expires_at_unix_ms: None,
                        metadata: BTreeMap::new(),
                        workflow: previous_record
                            .as_ref()
                            .map_or_else(WorkflowDescriptor::default, |record| {
                                record.workflow.clone()
                            }),
                        idempotency_key_hash: request.idempotency_key_hash,
                        dictionary_id: None,
                        tombstone: true,
                    };
                    (
                        record,
                        ChangeOperation::Delete,
                        request.idempotency_key_hash,
                        request.delta,
                    )
                }
            };
            record.validate(&self.config.limits)?;
            let logical_record_bytes = encode_logical(LogicalFrameKind::Record, &record)?;
            if logical_record_bytes.len() > self.config.limits.max_record_bytes {
                return Err(AproError::ResourceLimit(format!(
                    "serialized record of {} bytes, maximum {}",
                    logical_record_bytes.len(),
                    self.config.limits.max_record_bytes
                )));
            }
            let layer = previous_radial
                .as_ref()
                .map_or(RadialLayer::Warm, |descriptor| descriptor.layer);
            let compression_policy = self
                .compression_catalog
                .read()
                .policies
                .get(&identity.collection_key())
                .cloned()
                .unwrap_or_default();
            let tier_policy = compression_tier_policy(&compression_policy, layer);
            let skip_content_type = compression_policy
                .skip_content_type_prefixes
                .iter()
                .any(|prefix| record.content_type.starts_with(prefix));
            let dictionary = tier_policy
                .dictionary_id
                .map(|id| self.load_dictionary(id))
                .transpose()?
                .filter(|dictionary| {
                    dictionary.tenant == identity.tenant
                        && dictionary.namespace == identity.namespace
                        && dictionary.collection == identity.collection
                        && (dictionary.schema == "*" || dictionary.schema == record.content_type)
                });
            let record_bytes = self.compression_manager.encode_record(
                &mut record,
                &tier_policy,
                skip_content_type,
                dictionary.as_deref(),
                xxh3_64(&version_key(&identity, version)),
            )?;
            if record_bytes.len() > self.config.limits.max_record_bytes {
                return Err(AproError::ResourceLimit(format!(
                    "compressed framed record of {} bytes, maximum {}",
                    record_bytes.len(),
                    self.config.limits.max_record_bytes
                )));
            }
            let head = HeadPointer {
                identity: identity.clone(),
                version,
                tombstone: record.tombstone,
            };
            let body = match policy.retention_mode {
                EventRetentionMode::VersionRef => ChangeBody::VersionRef {
                    identity: identity.clone(),
                    version,
                },
                EventRetentionMode::Delta => {
                    let bytes = delta.ok_or_else(|| {
                        AproError::InvalidInput(
                            "Delta collection requires a self-contained delta".into(),
                        )
                    })?;
                    ChangeBody::Delta { bytes }
                }
                EventRetentionMode::SelfContained => {
                    let self_contained_record_bytes =
                        encode_logical(LogicalFrameKind::Record, &record)?;
                    if self_contained_record_bytes.len() > policy.max_self_contained_event_bytes {
                        return Err(AproError::ResourceLimit(format!(
                            "SelfContained event of {} bytes, maximum {}",
                            self_contained_record_bytes.len(),
                            policy.max_self_contained_event_bytes
                        )));
                    }
                    ChangeBody::SelfContained {
                        record: Box::new(record.clone()),
                    }
                }
            };
            let event = ChangeEvent {
                tenant: identity.tenant.clone(),
                namespace: identity.namespace.clone(),
                collection: identity.collection.clone(),
                partition: identity.partition.clone(),
                version,
                operation,
                key: identity.key.clone(),
                previous_version: current.as_ref().map(|head| head.version),
                batch_id,
                idempotency_key_hash,
                body,
            };
            if let Some(previous) = previous_record.as_ref().filter(|record| !record.tombstone) {
                storage.delete(StorageSpace::Workflow, workflow_key(previous)?);
            }
            if !record.tombstone {
                let workflow_entry = WorkflowIndexEntry {
                    identity: identity.clone(),
                    version,
                    state: record.workflow.state.clone(),
                    available_at_unix_ms: workflow_available_at(&record.workflow),
                };
                storage.put(
                    StorageSpace::Workflow,
                    workflow_key(&record)?,
                    encode_logical(LogicalFrameKind::Workflow, &workflow_entry)?,
                );
            }
            if let Some(expires_at) = previous_record
                .as_ref()
                .and_then(|record| record.expires_at_unix_ms)
            {
                storage.delete(StorageSpace::Ttl, ttl_key(expires_at, &identity));
            }
            if let Some(expires_at) = record.expires_at_unix_ms {
                let ttl_entry = TtlEntry {
                    identity: identity.clone(),
                    version,
                    expires_at_unix_ms: expires_at,
                };
                storage.put(
                    StorageSpace::Ttl,
                    ttl_key(expires_at, &identity),
                    encode_logical(LogicalFrameKind::Ttl, &ttl_entry)?,
                );
            }
            if record.tombstone {
                storage.delete(StorageSpace::Radial, radial_key(&identity));
            } else {
                let radial_policy = self
                    .radial_state
                    .read()
                    .policies
                    .get(&identity.collection_key())
                    .cloned()
                    .unwrap_or_default();
                let descriptor = RadialDescriptor {
                    identity: identity.clone(),
                    canonical_version: version,
                    created_at_unix_ms: record.created_at_unix_ms,
                    updated_at_unix_ms: record.updated_at_unix_ms,
                    access_frequency_estimate: previous_radial
                        .as_ref()
                        .map_or(0, |descriptor| descriptor.access_frequency_estimate),
                    last_access_sampled_unix_ms: previous_radial
                        .as_ref()
                        .and_then(|descriptor| descriptor.last_access_sampled_unix_ms),
                    freshness_half_life_ms: radial_policy.freshness_half_life_ms,
                    urgency_millis: previous_radial
                        .as_ref()
                        .map_or(0, |descriptor| descriptor.urgency_millis),
                    deadline_unix_ms: record.expires_at_unix_ms,
                    workflow_state: record.workflow.state.clone(),
                    projection_watermarks: previous_radial
                        .as_ref()
                        .map_or_else(BTreeMap::new, |descriptor| {
                            descriptor.projection_watermarks.clone()
                        }),
                    reconstruction_cost_micros: previous_radial
                        .as_ref()
                        .map_or(0, |descriptor| descriptor.reconstruction_cost_micros),
                    logical_bytes: u64::try_from(estimate_record(&record)).unwrap_or(u64::MAX),
                    physical_bytes: u64::try_from(record_bytes.len()).unwrap_or(u64::MAX),
                    storage_class: previous_radial.as_ref().map_or_else(
                        || "primary".into(),
                        |descriptor| descriptor.storage_class.clone(),
                    ),
                    admin_pin_until_unix_ms: previous_radial
                        .as_ref()
                        .and_then(|descriptor| descriptor.admin_pin_until_unix_ms),
                    layer: previous_radial
                        .as_ref()
                        .map_or(RadialLayer::Warm, |descriptor| descriptor.layer),
                    layer_since_unix_ms: previous_radial
                        .as_ref()
                        .map_or(now, |descriptor| descriptor.layer_since_unix_ms),
                    last_decision: "canonical mutation: descriptor updated".into(),
                };
                storage.put(
                    StorageSpace::Radial,
                    radial_key(&identity),
                    encode_logical(LogicalFrameKind::Radial, &descriptor)?,
                );
            }
            storage.put(
                StorageSpace::Versions,
                version_key(&identity, version),
                record_bytes,
            );
            storage.put(
                StorageSpace::Records,
                identity.storage_key(),
                encode_logical(LogicalFrameKind::Head, &head)?,
            );
            storage.put(
                StorageSpace::Events,
                event_key(shard, next_sequence),
                encode_logical(LogicalFrameKind::Change, &event)?,
            );
            versions.push(version);
            self.invalidate_derived(&identity);
        }
        catalog.generation = catalog.generation.saturating_add(1);
        catalog.shard_sequences.insert(shard, next_sequence);
        if durability == Durability::Durable {
            catalog.durable_watermarks.insert(shard, next_sequence);
        }
        storage.put(
            StorageSpace::Catalog,
            CATALOG_KEY.to_vec(),
            encode_logical(LogicalFrameKind::Catalog, &catalog)?,
        );

        let receipt_durable_watermark = if durability == Durability::Durable {
            next_sequence
        } else {
            self.durable_watermarks[shard as usize].load(Ordering::Acquire)
        };
        let receipts: Vec<_> = versions
            .iter()
            .copied()
            .map(|version| MutationReceipt {
                version,
                durability,
                durable_watermark: receipt_durable_watermark,
                batch_id,
            })
            .collect();
        if let Some(context) = &idempotency {
            let lookup_key = idempotency_lookup_key(&context.scope, &context.key_hash);
            if let Some(previous_expiry) = previous_idempotency_expiry {
                storage.delete(
                    StorageSpace::IdempotencyExpiry,
                    idempotency_expiry_key(previous_expiry, &lookup_key),
                );
            }
            let retention_ms = duration_millis(self.config.idempotency_retention)?;
            let expires_at_unix_ms = now.checked_add(retention_ms).ok_or_else(|| {
                AproError::ResourceLimit("idempotency expiry exceeds u64 limit".into())
            })?;
            let record = IdempotencyRecord {
                scope: context.scope.clone(),
                key_hash: context.key_hash,
                request_fingerprint: context.request_fingerprint,
                receipts: receipts.clone(),
                expires_at_unix_ms,
            };
            let expiry = IdempotencyExpiryEntry {
                lookup_key: lookup_key.clone(),
                expires_at_unix_ms,
            };
            storage.put(
                StorageSpace::Idempotency,
                lookup_key.clone(),
                encode_logical(LogicalFrameKind::Idempotency, &record)?,
            );
            storage.put(
                StorageSpace::IdempotencyExpiry,
                idempotency_expiry_key(expires_at_unix_ms, &lookup_key),
                encode_logical(LogicalFrameKind::IdempotencyExpiry, &expiry)?,
            );
        }

        let mode = match (durability, self.group_commit.is_some()) {
            (Durability::Durable, true) => CommitMode::Buffered,
            (Durability::Durable, false) => CommitMode::Durable,
            (Durability::Relaxed, _) => CommitMode::Relaxed,
        };
        let commit_bytes = storage.bytes();
        self.commit_primary(storage, mode)?;
        *self.catalog.write() = catalog;
        drop(catalog_writer);
        if durability == Durability::Durable {
            if let Some(group_commit) = &self.group_commit {
                group_commit.wait_for_persist(commit_bytes)?;
            }
            self.durable_watermarks[shard as usize].store(next_sequence, Ordering::Release);
        }
        Ok(receipts)
    }

    fn current_head(&self, identity: &RecordIdentity) -> Result<Option<HeadPointer>> {
        self.backend
            .get(StorageSpace::Records, &identity.storage_key())?
            .map(|bytes| decode_logical(LogicalFrameKind::Head, &bytes))
            .transpose()
    }

    fn load_radial_descriptor(
        &self,
        identity: &RecordIdentity,
    ) -> Result<Option<RadialDescriptor>> {
        let now = now_unix_ms()?;
        if let Some(descriptor) = self.metadata_cache.get(identity, now) {
            return Ok(Some(descriptor));
        }
        let Some(bytes) = self
            .backend
            .get(StorageSpace::Radial, &radial_key(identity))?
        else {
            return Ok(None);
        };
        let descriptor: RadialDescriptor = decode_logical(LogicalFrameKind::Radial, &bytes)?;
        if descriptor.identity != *identity {
            return Err(AproError::Corrupt(
                "radial descriptor is associated with a different identity".into(),
            ));
        }
        let policy = self
            .radial_state
            .read()
            .policies
            .get(&identity.collection_key())
            .cloned()
            .unwrap_or_default();
        let freshness = freshness_millis(
            descriptor.updated_at_unix_ms,
            now,
            policy.freshness_half_life_ms,
        );
        let score = radial_score_millis(freshness, descriptor.urgency_millis, &policy);
        self.metadata_cache.insert(
            identity.clone(),
            descriptor.clone(),
            CacheAdmission {
                bytes: estimate_radial_descriptor(&descriptor),
                radial_score_millis: score,
                pinned_until_unix_ms: descriptor.admin_pin_until_unix_ms,
                expires_at_unix_ms: None,
                now_unix_ms: now,
            },
        );
        Ok(Some(descriptor))
    }

    fn insert_negative_cache(
        &self,
        identity: RecordIdentity,
        catalog_generation: u64,
        now_unix_ms: u64,
    ) {
        let ttl_ms = u64::try_from(self.config.negative_cache_ttl.as_millis()).unwrap_or(u64::MAX);
        let expires = now_unix_ms.saturating_add(ttl_ms);
        let bytes = identity.storage_key().len().saturating_add(16);
        self.negative_cache.insert(
            identity,
            catalog_generation,
            CacheAdmission {
                bytes,
                radial_score_millis: 0,
                pinned_until_unix_ms: None,
                expires_at_unix_ms: Some(expires),
                now_unix_ms,
            },
        );
    }

    fn invalidate_derived(&self, identity: &RecordIdentity) {
        self.metadata_cache.invalidate(identity);
        self.object_cache.invalidate(identity);
        self.compressed_cache.invalidate(identity);
        self.negative_cache.invalidate(identity);
    }

    fn commit_primary(&self, batch: StorageBatch, mode: CommitMode) -> Result<()> {
        match self.backend.commit(batch, mode) {
            Ok(()) => Ok(()),
            Err(error) => {
                self.poisoned.store(true, Ordering::Release);
                Err(error)
            }
        }
    }

    fn acquire_inflight(&self, bytes: usize) -> Result<InflightGuard<'_>> {
        let result =
            self.inflight_bytes
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    current
                        .checked_add(bytes)
                        .filter(|next| *next <= self.config.limits.max_inflight_bytes)
                });
        if result.is_err() {
            return Err(AproError::Backpressure(format!(
                "inflight budget of {} bytes exhausted",
                self.config.limits.max_inflight_bytes
            )));
        }
        Ok(InflightGuard {
            counter: &self.inflight_bytes,
            bytes,
        })
    }

    fn enforce_write_disk_budget(&self, logical_bytes: usize) -> Result<()> {
        let stats = self.backend.stats()?;
        let additional = u64::try_from(logical_bytes)
            .unwrap_or(u64::MAX)
            .saturating_mul(2);
        if let Some(maximum) = self.config.max_data_bytes
            && stats.disk_bytes.saturating_add(additional) > maximum
        {
            return Err(AproError::ResourceLimit(format!(
                "data quota of {maximum} bytes exceeded"
            )));
        }
        if self.config.min_free_disk_bytes > 0 {
            ensure_available_space(
                &self.config.path,
                self.config.min_free_disk_bytes.saturating_add(additional),
                "write",
            )?;
        }
        Ok(())
    }

    fn shard_for_partition(&self, partition_key: &[u8]) -> u32 {
        (xxh3_64(partition_key) as u32) & (self.config.shards - 1)
    }

    fn copy_space_to(
        &self,
        checkpoint: &dyn StorageBackend,
        space: StorageSpace,
        entries: &mut usize,
        logical_bytes: &mut u64,
    ) -> Result<()> {
        let page_size = self.config.limits.max_queue_depth;
        let mut start = Vec::new();
        let end = vec![u8::MAX; self.config.limits.max_storage_key_bytes];
        let mut first_page = true;
        loop {
            let requested = page_size.saturating_add(usize::from(!first_page));
            let mut rows = self.backend.scan_range(space, &start, &end, requested)?;
            if !first_page && rows.first().is_some_and(|(key, _)| *key == start) {
                rows.remove(0);
            }
            if rows.is_empty() {
                break;
            }
            let last_key = rows.last().map(|(key, _)| key.clone()).unwrap_or_default();
            let full_page = rows.len() == page_size;
            let mut batch = StorageBatch::with_capacity(rows.len());
            for (key, value) in rows {
                *logical_bytes = logical_bytes
                    .saturating_add(key.len() as u64)
                    .saturating_add(value.len() as u64);
                *entries += 1;
                batch.put(space, key, value);
            }
            checkpoint.commit(batch, CommitMode::Buffered)?;
            if !full_page {
                break;
            }
            start = last_key;
            first_page = false;
        }
        Ok(())
    }

    fn visit_space_range(
        &self,
        space: StorageSpace,
        start: &[u8],
        end_inclusive: &[u8],
        mut visitor: impl FnMut(&[u8], &[u8]) -> Result<()>,
    ) -> Result<usize> {
        let page_size = self.config.limits.max_queue_depth;
        let mut cursor = start.to_vec();
        let mut first_page = true;
        let mut visited = 0usize;
        loop {
            let requested = page_size.saturating_add(usize::from(!first_page));
            let mut rows = self
                .backend
                .scan_range(space, &cursor, end_inclusive, requested)?;
            if !first_page && rows.first().is_some_and(|(key, _)| *key == cursor) {
                rows.remove(0);
            }
            if rows.is_empty() {
                break;
            }
            let last_key = rows.last().map(|(key, _)| key.clone()).unwrap_or_default();
            if !first_page && last_key <= cursor {
                return Err(AproError::Corrupt(
                    "verify scan does not advance between pages".into(),
                ));
            }
            let full_page = rows.len() == page_size;
            for (key, value) in &rows {
                visitor(key, value)?;
                visited = visited
                    .checked_add(1)
                    .ok_or_else(|| AproError::ResourceLimit("verify count exhausted".into()))?;
            }
            if !full_page {
                break;
            }
            cursor = last_key;
            first_page = false;
        }
        Ok(visited)
    }

    fn ensure_healthy(&self) -> Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(AproError::Storage(
                "engine halted after a persistence error".into(),
            ));
        }
        Ok(())
    }
}

struct PersistRequest {
    bytes: usize,
    response: SyncSender<std::result::Result<(), String>>,
}

struct GroupCommitter {
    sender: Option<SyncSender<PersistRequest>>,
    worker: Option<JoinHandle<()>>,
}

impl GroupCommitter {
    fn new(
        backend: Arc<dyn StorageBackend>,
        window: Duration,
        max_bytes: usize,
        queue_depth: usize,
        poisoned: Arc<AtomicBool>,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::sync_channel::<PersistRequest>(queue_depth);
        let worker = thread::Builder::new()
            .name("aprodb-group-commit".into())
            .spawn(move || {
                while let Ok(first) = receiver.recv() {
                    let started = Instant::now();
                    let mut bytes = first.bytes;
                    let mut requests = vec![first];
                    while bytes < max_bytes {
                        let remaining = window.saturating_sub(started.elapsed());
                        if remaining.is_zero() {
                            break;
                        }
                        match receiver.recv_timeout(remaining) {
                            Ok(request) => {
                                bytes = bytes.saturating_add(request.bytes);
                                requests.push(request);
                            }
                            Err(RecvTimeoutError::Timeout | RecvTimeoutError::Disconnected) => {
                                break;
                            }
                        }
                    }
                    let result = backend.persist(CommitMode::Durable);
                    if result.is_err() {
                        poisoned.store(true, Ordering::Release);
                    }
                    let message = result.err().map(|error| error.to_string());
                    for request in requests {
                        let response = match &message {
                            Some(message) => Err(message.clone()),
                            None => Ok(()),
                        };
                        let _ = request.response.send(response);
                    }
                    if poisoned.load(Ordering::Acquire) {
                        break;
                    }
                }
            })
            .map_err(|error| AproError::Storage(format!("thread group commit: {error}")))?;
        Ok(Self {
            sender: Some(sender),
            worker: Some(worker),
        })
    }

    fn wait_for_persist(&self, bytes: usize) -> Result<()> {
        let (response_tx, response_rx) = mpsc::sync_channel(1);
        let request = PersistRequest {
            bytes,
            response: response_tx,
        };
        match self
            .sender
            .as_ref()
            .ok_or_else(|| AproError::Storage("group commit stopped".into()))?
            .try_send(request)
        {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                return Err(AproError::Backpressure("group commit queue full".into()));
            }
            Err(TrySendError::Disconnected(_)) => {
                return Err(AproError::Storage("group commit unavailable".into()));
            }
        }
        response_rx
            .recv()
            .map_err(|_| AproError::Storage("group commit response interrupted".into()))?
            .map_err(AproError::Storage)
    }
}

impl Drop for GroupCommitter {
    fn drop(&mut self) {
        self.sender.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

struct InflightGuard<'a> {
    counter: &'a AtomicUsize,
    bytes: usize,
}

impl Drop for InflightGuard<'_> {
    fn drop(&mut self) {
        self.counter.fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

fn inventory_files(root: &Path) -> Result<Vec<BackupFile>> {
    if !root.is_dir() {
        return Err(AproError::Corrupt(format!(
            "backup data directory missing: {}",
            root.display()
        )));
    }
    let mut directories = vec![root.to_path_buf()];
    let mut files = Vec::new();
    while let Some(directory) = directories.pop() {
        for entry in fs::read_dir(&directory).map_err(io_storage_error)? {
            let entry = entry.map_err(io_storage_error)?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).map_err(io_storage_error)?;
            if metadata.file_type().is_symlink() {
                return Err(AproError::Corrupt(
                    "a backup must not contain symbolic links".into(),
                ));
            }
            let relative = path
                .strip_prefix(root)
                .map_err(|error| AproError::Corrupt(error.to_string()))?;
            let relative_path = normalized_safe_relative_path(relative)?;
            if metadata.is_dir() {
                directories.push(path);
            } else if metadata.is_file() {
                if files.len() >= 1_000_000 {
                    return Err(AproError::ResourceLimit(
                        "backup contains more than one million files".into(),
                    ));
                }
                let (bytes, checksum) = hash_file(&path)?;
                files.push(BackupFile {
                    relative_path,
                    bytes,
                    blake3: checksum,
                });
            } else {
                return Err(AproError::Corrupt(
                    "non-regular filesystem entry type found in backup".into(),
                ));
            }
        }
    }
    files.sort_by(|left, right| left.relative_path.cmp(&right.relative_path));
    Ok(files)
}

fn normalized_safe_relative_path(path: &Path) -> Result<String> {
    use std::path::Component;

    let mut components = Vec::new();
    for component in path.components() {
        match component {
            Component::Normal(value) => {
                let value = value
                    .to_str()
                    .ok_or_else(|| AproError::IncompatibleFormat("backup path not UTF-8".into()))?;
                if value.is_empty() || value.contains(':') {
                    return Err(AproError::Corrupt("unsafe backup path component".into()));
                }
                components.push(value);
            }
            _ => {
                return Err(AproError::Corrupt(
                    "backup path is absolute or contains traversal".into(),
                ));
            }
        }
    }
    if components.is_empty() || components.len() > 64 {
        return Err(AproError::ResourceLimit("invalid backup path depth".into()));
    }
    Ok(components.join("/"))
}

fn safe_path_from_manifest(relative: &str) -> Result<PathBuf> {
    let path = Path::new(relative);
    let normalized = normalized_safe_relative_path(path)?;
    if normalized != relative.replace('\\', "/") || relative.contains('\\') {
        return Err(AproError::Corrupt("manifest path is not canonical".into()));
    }
    Ok(path.to_path_buf())
}

fn hash_file(path: &Path) -> Result<(u64, String)> {
    let mut file = File::open(path).map_err(io_storage_error)?;
    let mut hasher = blake3::Hasher::new();
    let mut bytes = 0u64;
    let mut buffer = [0u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).map_err(io_storage_error)?;
        if read == 0 {
            break;
        }
        bytes = bytes
            .checked_add(read as u64)
            .ok_or_else(|| AproError::ResourceLimit("backup file size exceeds u64".into()))?;
        hasher.update(&buffer[..read]);
    }
    Ok((bytes, hasher.finalize().to_hex().to_string()))
}

fn write_backup_manifest(root: &Path, manifest: &BackupManifest) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|error| AproError::Storage(format!("manifest serialization: {error}")))?;
    if bytes.len() > 16 * 1024 * 1024 {
        return Err(AproError::ResourceLimit(
            "backup manifest exceeds 16 MiB".into(),
        ));
    }
    let temporary = root.join("backup-manifest.json.tmp");
    let final_path = root.join("backup-manifest.json");
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)
        .map_err(io_storage_error)?;
    file.write_all(&bytes)
        .and_then(|()| file.sync_all())
        .map_err(io_storage_error)?;
    drop(file);
    fs::rename(temporary, final_path).map_err(io_storage_error)
}

fn copy_inventory(source: &Path, destination: &Path, files: &[BackupFile]) -> Result<()> {
    fs::create_dir(destination).map_err(io_storage_error)?;
    for expected in files {
        let relative = safe_path_from_manifest(&expected.relative_path)?;
        let source_path = source.join(&relative);
        let destination_path = destination.join(&relative);
        if let Some(parent) = destination_path.parent() {
            fs::create_dir_all(parent).map_err(io_storage_error)?;
        }
        let mut input = File::open(&source_path).map_err(io_storage_error)?;
        let mut output = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&destination_path)
            .map_err(io_storage_error)?;
        let mut hasher = blake3::Hasher::new();
        let mut bytes = 0u64;
        let mut buffer = [0u8; 64 * 1024];
        loop {
            let read = input.read(&mut buffer).map_err(io_storage_error)?;
            if read == 0 {
                break;
            }
            output
                .write_all(&buffer[..read])
                .map_err(io_storage_error)?;
            hasher.update(&buffer[..read]);
            bytes = bytes
                .checked_add(read as u64)
                .ok_or_else(|| AproError::ResourceLimit("restore file size exceeds u64".into()))?;
        }
        output.sync_all().map_err(io_storage_error)?;
        if bytes != expected.bytes || hasher.finalize().to_hex().as_str() != expected.blake3 {
            return Err(AproError::Corrupt(format!(
                "backup file changed during restore: {}",
                expected.relative_path
            )));
        }
    }
    Ok(())
}

fn io_storage_error(error: std::io::Error) -> AproError {
    AproError::Storage(error.to_string())
}

fn ensure_available_space(path: &Path, required: u64, operation: &str) -> Result<()> {
    if required == 0 {
        return Ok(());
    }
    let available = fs2::available_space(path).map_err(io_storage_error)?;
    if available < required {
        return Err(AproError::Backpressure(format!(
            "insufficient free space for {operation}: available {available}, required {required} bytes"
        )));
    }
    Ok(())
}

fn open_storage_backend(config: &EngineConfig, path: &Path) -> Result<Arc<dyn StorageBackend>> {
    prepare_encryption_marker(path, config.encryption.as_ref())?;
    let raw: Arc<dyn StorageBackend> = Arc::new(FjallBackend::open(path, config.storage.clone())?);
    match &config.encryption {
        Some(encryption) => Ok(Arc::new(EncryptedBackend::new(raw, encryption.clone())?)),
        None => Ok(raw),
    }
}

fn prepare_encryption_marker(path: &Path, encryption: Option<&EncryptionConfig>) -> Result<()> {
    fs::create_dir_all(path).map_err(|error| AproError::Storage(error.to_string()))?;
    let marker_path = path.join(ENCRYPTION_MARKER_FILE);
    match File::open(&marker_path) {
        Ok(mut marker) => {
            let mut contents = Vec::new();
            marker
                .read_to_end(&mut contents)
                .map_err(|error| AproError::Storage(error.to_string()))?;
            if contents != ENCRYPTION_MARKER {
                return Err(AproError::IncompatibleFormat(
                    "unrecognized at-rest encryption marker".into(),
                ));
            }
            if encryption.is_none() {
                return Err(AproError::IncompatibleFormat(
                    "encrypted directory: please provide the at-rest keyring".into(),
                ));
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if encryption.is_none() {
                return Ok(());
            }
            if path.join("aprodb.wal").exists() || path.join("aprodb.snapshot").exists() {
                return Err(AproError::IncompatibleFormat(
                    "encryption cannot be enabled automatically on 0.1 data".into(),
                ));
            }
            let backend_has_data = path
                .join("backend")
                .read_dir()
                .map(|mut entries| entries.next().is_some())
                .unwrap_or(false);
            if path.join("APRODB_FORMAT").exists() || backend_has_data {
                return Err(AproError::IncompatibleFormat(
                    "unencrypted 1.x directory: use rekey-to-copy, not an in-place upgrade".into(),
                ));
            }
            let temporary = path.join("APRODB_ENCRYPTION.tmp");
            let mut marker = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .map_err(|error| AproError::Storage(error.to_string()))?;
            marker
                .write_all(ENCRYPTION_MARKER)
                .and_then(|()| marker.sync_all())
                .map_err(|error| AproError::Storage(error.to_string()))?;
            drop(marker);
            fs::rename(&temporary, &marker_path)
                .map_err(|error| AproError::Storage(error.to_string()))
        }
        Err(error) => Err(AproError::Storage(error.to_string())),
    }
}

fn validate_config(config: &EngineConfig) -> Result<()> {
    if config.max_data_bytes == Some(0) || config.max_compaction_temporary_bytes == 0 {
        return Err(AproError::InvalidInput(
            "disk quotas configured to zero".into(),
        ));
    }
    if config.shards == 0 || !config.shards.is_power_of_two() {
        return Err(AproError::InvalidInput(
            "the shard count must be a power of two".into(),
        ));
    }
    if config.limits.max_batch_operations == 0
        || config.limits.max_batch_bytes == 0
        || config.limits.max_inflight_bytes == 0
        || config.limits.max_queue_depth == 0
    {
        return Err(AproError::InvalidInput(
            "batch, in-flight, and queue limits must be positive".into(),
        ));
    }
    if config.limits.max_queue_depth < config.limits.max_batch_operations {
        return Err(AproError::InvalidInput(
            "queue must support at least one maximum AtomicBatch".into(),
        ));
    }
    let required_storage_operations = config
        .limits
        .max_batch_operations
        .checked_mul(8)
        .and_then(|value| value.checked_add(4))
        .ok_or_else(|| AproError::InvalidInput("batch limit too large".into()))?;
    if config.storage.max_batch_operations < required_storage_operations {
        return Err(AproError::InvalidInput(format!(
            "storage allows {} operations, but at least {required_storage_operations} are required",
            config.storage.max_batch_operations
        )));
    }
    if config.storage.max_batch_bytes < config.limits.max_batch_bytes {
        return Err(AproError::InvalidInput(
            "the storage byte limit is lower than the engine batch limit".into(),
        ));
    }
    if !config.group_commit_window.is_zero() && config.group_commit_max_bytes == 0 {
        return Err(AproError::InvalidInput(
            "group commit requires a positive byte limit".into(),
        ));
    }
    if config.group_commit_window > Duration::from_secs(60) {
        return Err(AproError::InvalidInput(
            "group commit window exceeds 60 seconds".into(),
        ));
    }
    if config.negative_cache_bytes > 0 && config.negative_cache_ttl.is_zero() {
        return Err(AproError::InvalidInput(
            "negative cache enabled with zero TTL".into(),
        ));
    }
    if config.idempotency_retention.is_zero()
        || config.idempotency_retention > Duration::from_secs(365 * 24 * 60 * 60)
    {
        return Err(AproError::InvalidInput(
            "idempotency retention must be between one millisecond and 365 days".into(),
        ));
    }
    if config.max_lease_duration.is_zero()
        || config.max_lease_duration > Duration::from_secs(24 * 60 * 60)
    {
        return Err(AproError::InvalidInput(
            "maximum lease duration must be between one millisecond and 24 hours".into(),
        ));
    }
    if config.lease_recovery_safety_margin > config.max_lease_duration {
        return Err(AproError::InvalidInput(
            "lease recovery margin greater than maximum duration".into(),
        ));
    }
    if config.max_claim_batch == 0
        || config.max_active_leases < config.max_claim_batch
        || config.max_workflow_attempts == 0
    {
        return Err(AproError::InvalidInput(
            "claim/lease limits are inconsistent with the batch limit".into(),
        ));
    }
    if config.max_surface_records == 0
        || config.max_surfaces == 0
        || config.max_surfaces > config.limits.max_queue_depth
        || config.max_surface_generation_bytes == 0
        || config.max_surface_generation_bytes > config.limits.max_batch_bytes
        || config.max_retained_surface_generations == 0
        || config.max_retained_surface_generations > 64
    {
        return Err(AproError::InvalidInput("invalid surface limits".into()));
    }
    if config.compression_channels == 0
        || !config.compression_channels.is_power_of_two()
        || config.compression_channels > 64
        || config.compression_scratch_bytes == 0
        || config.max_dictionary_bytes < 1024
        || config.max_dictionary_bytes > 1024 * 1024
        || config.max_dictionaries == 0
        || config.max_dictionary_training_samples < 8
        || config.max_dictionary_training_bytes < config.max_dictionary_bytes
    {
        return Err(AproError::InvalidInput(
            "invalid codec or dictionary limits".into(),
        ));
    }
    config.compute.validate()?;
    if config.compute.max_batch_bytes > config.compute.queue_byte_budget
        || config.compute.max_batch_bytes > config.limits.max_inflight_bytes
    {
        return Err(AproError::InvalidInput(
            "compute batch exceeds engine's queue or inflight byte budget".into(),
        ));
    }
    let memtables = usize::try_from(config.storage.max_memtable_bytes)
        .unwrap_or(usize::MAX)
        .saturating_mul(STORAGE_SPACE_COUNT);
    let storage_cache = usize::try_from(config.storage.cache_bytes).unwrap_or(usize::MAX);
    let reserved = memtables
        .saturating_add(storage_cache)
        .saturating_add(config.limits.max_inflight_bytes)
        .saturating_add(config.metadata_cache_bytes)
        .saturating_add(config.object_cache_bytes)
        .saturating_add(config.compressed_cache_bytes)
        .saturating_add(config.compression_scratch_bytes)
        .saturating_add(config.compute.queue_byte_budget)
        .saturating_add(config.negative_cache_bytes);
    if reserved > config.memory_budget_bytes {
        return Err(AproError::InvalidInput(format!(
            "memory budget {0} less than configured reserved memory {reserved}",
            config.memory_budget_bytes
        )));
    }
    Ok(())
}

const fn percent(value: usize, percentage: usize) -> usize {
    value.saturating_mul(percentage) / 100
}

fn validate_radial_policy(policy: &RadialPolicy) -> Result<()> {
    if policy.freshness_half_life_ms == 0 {
        return Err(AproError::InvalidInput(
            "freshness half-life must be positive".into(),
        ));
    }
    if u32::from(policy.freshness_weight_millis) + u32::from(policy.urgency_weight_millis) != 1000 {
        return Err(AproError::InvalidInput(
            "radial weights must sum to 1000".into(),
        ));
    }
    if policy.promotion_threshold_millis > 1000
        || policy.demotion_threshold_millis > 1000
        || policy.promotion_threshold_millis <= policy.demotion_threshold_millis
    {
        return Err(AproError::InvalidInput(
            "radial thresholds invalid or lack hysteresis".into(),
        ));
    }
    Ok(())
}

fn validate_storage_class(descriptor: &StorageClassDescriptor) -> Result<()> {
    if descriptor.name.is_empty() || descriptor.name.len() > 128 {
        return Err(AproError::InvalidInput(
            "storage class name must be 1..128 bytes".into(),
        ));
    }
    if descriptor.budget_bytes == 0 {
        return Err(AproError::InvalidInput(
            "storage class budget must be positive".into(),
        ));
    }
    Ok(())
}

fn validate_compression_policy(policy: &CompressionPolicy, config: &EngineConfig) -> Result<()> {
    if policy.surface.mode != CompressionMode::Raw || policy.surface.dictionary_id.is_some() {
        return Err(AproError::Unsupported(
            "pre-serialized surfaces use Raw in Milestone 5".into(),
        ));
    }
    for tier in [&policy.hot, &policy.warm, &policy.cold, &policy.archive] {
        match tier.mode {
            CompressionMode::Raw => {
                if tier.dictionary_id.is_some() {
                    return Err(AproError::InvalidInput(
                        "a Raw policy cannot reference a dictionary".into(),
                    ));
                }
            }
            CompressionMode::AdaptiveZstandard => {
                if !(-7..=22).contains(&tier.zstd_level)
                    || tier.min_input_bytes > config.limits.max_record_bytes
                    || tier.min_savings_bytes > config.limits.max_record_bytes
                {
                    return Err(AproError::InvalidInput(
                        "Zstandard level or thresholds out of bounds".into(),
                    ));
                }
            }
        }
    }
    if policy.skip_content_type_prefixes.len() > 128
        || policy
            .skip_content_type_prefixes
            .iter()
            .any(|prefix| prefix.is_empty() || prefix.len() > 255 || !prefix.is_ascii())
    {
        return Err(AproError::InvalidInput(
            "list of non-compressible content types invalid".into(),
        ));
    }
    Ok(())
}

fn compression_policy_dictionary_ids(policy: &CompressionPolicy) -> Vec<u64> {
    let mut ids = [
        policy.hot.dictionary_id,
        policy.warm.dictionary_id,
        policy.cold.dictionary_id,
        policy.archive.dictionary_id,
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>();
    ids.sort_unstable();
    ids.dedup();
    ids
}

fn compression_tier_policy(
    policy: &CompressionPolicy,
    layer: RadialLayer,
) -> CompressionTierPolicy {
    match layer {
        RadialLayer::Surface => policy.surface.clone(),
        RadialLayer::Hot => policy.hot.clone(),
        RadialLayer::Warm => policy.warm.clone(),
        RadialLayer::Cold => policy.cold.clone(),
        RadialLayer::Archive => policy.archive.clone(),
    }
}

fn encode_dictionary_samples(samples: &[Payload]) -> Result<Vec<Vec<u8>>> {
    samples
        .iter()
        .map(|payload| {
            payload.validate()?;
            bincode::serde::encode_to_vec(payload, bincode::config::standard())
                .map_err(|error| AproError::InvalidInput(format!("sample encoding: {error}")))
        })
        .collect()
}

fn compression_dictionary_key(id: u64) -> Vec<u8> {
    let mut key = b"dictionary:".to_vec();
    key.extend_from_slice(&id.to_be_bytes());
    key
}

fn audit_key(sequence: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(AUDIT_EVENT_PREFIX.len() + 8);
    key.extend_from_slice(AUDIT_EVENT_PREFIX);
    key.extend_from_slice(&sequence.to_be_bytes());
    key
}

fn validate_audit_event(event: &AuditEvent) -> Result<()> {
    if event.format_version != 1
        || event.sequence == 0
        || event.principal.is_empty()
        || event.principal.len() > 128
        || event.operation.is_empty()
        || event.operation.len() > 128
        || event
            .error_class
            .as_ref()
            .is_some_and(|value| value.is_empty() || value.len() > 128)
    {
        return Err(AproError::Corrupt("invalid audit event".into()));
    }
    Ok(())
}

fn validate_expected(expected: ExpectedVersion, current: Option<Version>) -> Result<()> {
    match expected {
        ExpectedVersion::Any => Ok(()),
        ExpectedVersion::Missing if current.is_none() => Ok(()),
        ExpectedVersion::Exact(version) if current == Some(version) => Ok(()),
        _ => Err(AproError::Conflict(format!(
            "expected version {expected:?}, current {current:?}"
        ))),
    }
}

fn version_key(identity: &RecordIdentity, version: Version) -> Vec<u8> {
    let mut key = identity.storage_key();
    key.extend_from_slice(&version.storage_suffix());
    key
}

fn record_collection_prefix(identity: &RecordIdentity) -> Vec<u8> {
    let collection = identity.collection_key();
    let mut prefix = Vec::with_capacity(1 + collection.len());
    prefix.push(1);
    prefix.extend_from_slice(&collection);
    prefix
}

fn radial_key(identity: &RecordIdentity) -> Vec<u8> {
    let mut key = Vec::with_capacity(1 + identity.storage_key().len());
    key.push(b'r');
    key.extend_from_slice(&identity.storage_key());
    key
}

fn ttl_key(expires_at_unix_ms: u64, identity: &RecordIdentity) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + identity.storage_key().len());
    key.extend_from_slice(&expires_at_unix_ms.to_be_bytes());
    key.extend_from_slice(&identity.storage_key());
    key
}

fn ttl_upper_key(expires_at_unix_ms: u64) -> Vec<u8> {
    let mut key = Vec::with_capacity(9);
    key.extend_from_slice(&expires_at_unix_ms.to_be_bytes());
    key.push(u8::MAX);
    key
}

fn idempotency_context(mutations: &[AtomicMutation]) -> Result<Option<IdempotencyContext>> {
    let Some(first_hash) = mutations
        .first()
        .and_then(AtomicMutation::idempotency_key_hash)
    else {
        if mutations
            .iter()
            .skip(1)
            .any(|mutation| mutation.idempotency_key_hash().is_some())
        {
            return Err(AproError::InvalidInput(
                "idempotent AtomicBatch requires the same key on every mutation".into(),
            ));
        }
        return Ok(None);
    };
    if mutations
        .iter()
        .any(|mutation| mutation.idempotency_key_hash() != Some(first_hash))
    {
        return Err(AproError::InvalidInput(
            "idempotent AtomicBatch requires the same key on every mutation".into(),
        ));
    }
    let encoded = bincode::serde::encode_to_vec(mutations, bincode::config::standard())
        .map_err(|error| AproError::InvalidInput(format!("fingerprint idempotenza: {error}")))?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(IDEMPOTENCY_FINGERPRINT_VERSION);
    hasher.update(&encoded);
    Ok(Some(IdempotencyContext {
        scope: mutations[0].identity().partition_key(),
        key_hash: first_hash,
        request_fingerprint: *hasher.finalize().as_bytes(),
    }))
}

fn idempotency_context_for<T: Serialize>(
    scope: &[u8],
    key_hash: [u8; 32],
    request: &T,
) -> Result<IdempotencyContext> {
    let encoded = bincode::serde::encode_to_vec(request, bincode::config::standard())
        .map_err(|error| AproError::InvalidInput(format!("fingerprint idempotenza: {error}")))?;
    let mut hasher = blake3::Hasher::new();
    hasher.update(IDEMPOTENCY_FINGERPRINT_VERSION);
    hasher.update(&encoded);
    Ok(IdempotencyContext {
        scope: scope.to_vec(),
        key_hash,
        request_fingerprint: *hasher.finalize().as_bytes(),
    })
}

fn idempotency_lookup_key(scope: &[u8], key_hash: &[u8; 32]) -> Vec<u8> {
    let mut key = Vec::with_capacity(scope.len() + key_hash.len() + 1);
    key.push(b'i');
    key.extend_from_slice(scope);
    key.extend_from_slice(key_hash);
    key
}

fn idempotency_expiry_key(expires_at_unix_ms: u64, lookup_key: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + lookup_key.len());
    key.extend_from_slice(&expires_at_unix_ms.to_be_bytes());
    key.extend_from_slice(lookup_key);
    key
}

fn duration_millis(duration: Duration) -> Result<u64> {
    u64::try_from(duration.as_millis())
        .map_err(|_| AproError::ResourceLimit("duration exceeds u64 milliseconds".into()))
}

fn workflow_available_at(workflow: &WorkflowDescriptor) -> u64 {
    if workflow.state == "leased" {
        workflow.lease_deadline_unix_ms.unwrap_or(0)
    } else {
        0
    }
}

fn workflow_prefix(scope: &[u8], state: &str) -> Result<Vec<u8>> {
    let state_len = u16::try_from(state.len())
        .map_err(|_| AproError::InvalidInput("workflow state too long".into()))?;
    let mut key = Vec::with_capacity(1 + scope.len() + 2 + state.len());
    key.push(b'w');
    key.extend_from_slice(scope);
    key.extend_from_slice(&state_len.to_be_bytes());
    key.extend_from_slice(state.as_bytes());
    Ok(key)
}

fn workflow_key(record: &RecordEnvelope) -> Result<Vec<u8>> {
    let mut key = workflow_prefix(&record.identity.partition_key(), &record.workflow.state)?;
    key.extend_from_slice(&workflow_available_at(&record.workflow).to_be_bytes());
    key.extend_from_slice(&record.identity.storage_key());
    Ok(key)
}

fn workflow_put_request(
    record: RecordEnvelope,
    workflow: WorkflowDescriptor,
    operation: ChangeOperation,
    idempotency_key_hash: Option<[u8; 32]>,
) -> Result<PutRequest> {
    let payload = record
        .payload
        .ok_or_else(|| AproError::Conflict("workflow not applicable to a tombstone".into()))?;
    Ok(PutRequest {
        identity: record.identity,
        payload,
        content_type: record.content_type,
        metadata: record.metadata,
        expires_at_unix_ms: record.expires_at_unix_ms,
        idempotency_key_hash,
        expected: ExpectedVersion::Exact(record.version),
        delta: None,
        operation,
        workflow_override: Some(workflow),
    })
}

fn cleared_workflow(previous: &WorkflowDescriptor, state: &str) -> WorkflowDescriptor {
    WorkflowDescriptor {
        state: state.into(),
        attempt: previous.attempt,
        lease_id: None,
        fencing_token: previous.fencing_token,
        lease_deadline_unix_ms: None,
    }
}

fn validate_surface_definition(
    definition: &SurfaceDefinition,
    config: &EngineConfig,
) -> Result<()> {
    if definition.id.is_empty()
        || definition.id.len() > 128
        || !definition
            .id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(AproError::InvalidInput(
            "projection id must use 1..128 safe ASCII characters".into(),
        ));
    }
    RecordIdentity {
        tenant: definition.source_tenant.clone(),
        namespace: definition.source_namespace.clone(),
        collection: definition.source_collection.clone(),
        partition: b"surface-validation".to_vec(),
        key: b"surface-validation".to_vec(),
    }
    .validate(&config.limits)?;
    if definition.workflow_states.is_empty()
        || definition
            .workflow_states
            .iter()
            .any(|state| state.is_empty() || state.len() > 255)
    {
        return Err(AproError::InvalidInput(
            "surface requires at least one valid workflow state".into(),
        ));
    }
    if definition.max_records == 0 || definition.max_records > config.max_surface_records {
        return Err(AproError::ResourceLimit(format!(
            "surface max_records must be between 1 and {}",
            config.max_surface_records
        )));
    }
    if definition.max_bytes < 64
        || definition.max_bytes > config.max_surface_generation_bytes.saturating_sub(4096)
    {
        return Err(AproError::ResourceLimit(format!(
            "surface max_bytes must be between 64 and {}",
            config.max_surface_generation_bytes.saturating_sub(4096)
        )));
    }
    if definition.retained_generations == 0
        || definition.retained_generations > config.max_retained_surface_generations
    {
        return Err(AproError::ResourceLimit(format!(
            "generation retention must be between 1 and {}",
            config.max_retained_surface_generations
        )));
    }
    Ok(())
}

fn surface_collection_key(definition: &SurfaceDefinition) -> Vec<u8> {
    let mut output = Vec::new();
    for component in [
        &definition.source_tenant,
        &definition.source_namespace,
        &definition.source_collection,
    ] {
        let len = u16::try_from(component.len()).unwrap_or(u16::MAX);
        output.extend_from_slice(&len.to_be_bytes());
        output.extend_from_slice(component);
    }
    output
}

fn surface_record_prefix(definition: &SurfaceDefinition) -> Result<Vec<u8>> {
    let mut output = vec![1];
    for component in [
        &definition.source_tenant,
        &definition.source_namespace,
        &definition.source_collection,
    ] {
        let len = u16::try_from(component.len())
            .map_err(|_| AproError::ResourceLimit("surface component exceeds u16".into()))?;
        output.extend_from_slice(&len.to_be_bytes());
        output.extend_from_slice(component);
    }
    Ok(output)
}

fn surface_consumer_name(projection_id: &str) -> String {
    format!("surface:{projection_id}")
}

fn surface_storage_key(prefix: u8, projection_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(3 + projection_id.len());
    key.push(prefix);
    key.extend_from_slice(
        &u16::try_from(projection_id.len())
            .unwrap_or(u16::MAX)
            .to_be_bytes(),
    );
    key.extend_from_slice(projection_id.as_bytes());
    key
}

fn surface_definition_key(projection_id: &str) -> Vec<u8> {
    surface_storage_key(b'd', projection_id)
}

fn surface_pointer_key(projection_id: &str) -> Vec<u8> {
    surface_storage_key(b'p', projection_id)
}

fn surface_generation_key(projection_id: &str, generation: u64) -> Vec<u8> {
    let mut key = surface_storage_key(b'g', projection_id);
    key.extend_from_slice(&generation.to_be_bytes());
    key
}

fn event_matches_surface(event: &ChangeEvent, definition: &SurfaceDefinition) -> bool {
    event.tenant == definition.source_tenant
        && event.namespace == definition.source_namespace
        && event.collection == definition.source_collection
}

fn surface_accepts(
    definition: &SurfaceDefinition,
    record: &RecordEnvelope,
    now_unix_ms: u64,
) -> bool {
    !record.tombstone
        && !record
            .expires_at_unix_ms
            .is_some_and(|expires| expires <= now_unix_ms)
        && definition
            .workflow_states
            .iter()
            .any(|state| state == &record.workflow.state)
}

fn encode_surface_payload(format: SurfaceFormat, records: &[RecordEnvelope]) -> Result<Vec<u8>> {
    match format {
        SurfaceFormat::AprodbRecords => encode_logical(LogicalFrameKind::SurfacePayload, &records),
        SurfaceFormat::Json => serde_json::to_vec(records)
            .map_err(|error| AproError::InvalidInput(format!("JSON serialization: {error}"))),
    }
}

fn decode_surface_payload(format: SurfaceFormat, serialized: &[u8]) -> Result<Vec<RecordEnvelope>> {
    match format {
        SurfaceFormat::AprodbRecords => {
            decode_logical(LogicalFrameKind::SurfacePayload, serialized)
        }
        SurfaceFormat::Json => serde_json::from_slice(serialized)
            .map_err(|error| AproError::Corrupt(format!("JSON surface: {error}"))),
    }
}

fn serialize_bounded_surface(
    definition: &SurfaceDefinition,
    records: BTreeMap<RecordIdentity, RecordEnvelope>,
) -> Result<(usize, Vec<u8>)> {
    let mut records: Vec<_> = records.into_values().take(definition.max_records).collect();
    let encoded = encode_surface_payload(definition.format, &records)?;
    if encoded.len() <= definition.max_bytes {
        return Ok((records.len(), encoded));
    }
    let mut low = 0usize;
    let mut high = records.len();
    let mut best = encode_surface_payload(definition.format, &[])?;
    if best.len() > definition.max_bytes {
        return Err(AproError::ResourceLimit(
            "even the empty surface exceeds max_bytes".into(),
        ));
    }
    while low < high {
        let middle = low + (high - low).div_ceil(2);
        let candidate = encode_surface_payload(definition.format, &records[..middle])?;
        if candidate.len() <= definition.max_bytes {
            low = middle;
            best = candidate;
        } else {
            high = middle - 1;
        }
    }
    records.truncate(low);
    Ok((records.len(), best))
}

fn event_key(shard: u32, sequence: u64) -> [u8; 12] {
    let mut key = [0u8; 12];
    key[..4].copy_from_slice(&shard.to_be_bytes());
    key[4..].copy_from_slice(&sequence.to_be_bytes());
    key
}

fn batch_id(epoch: u64, shard: u32, first_sequence: u64) -> [u8; 20] {
    let mut id = [0u8; 20];
    id[..8].copy_from_slice(&epoch.to_be_bytes());
    id[8..12].copy_from_slice(&shard.to_be_bytes());
    id[12..].copy_from_slice(&first_sequence.to_be_bytes());
    id
}

fn consumer_watermark_key(collection_key: &[u8], shard: u32, consumer: &str) -> Result<Vec<u8>> {
    let consumer_len = u16::try_from(consumer.len())
        .map_err(|_| AproError::InvalidInput("consumer name too long".into()))?;
    let mut key = Vec::with_capacity(collection_key.len() + 6 + consumer.len());
    key.extend_from_slice(collection_key);
    key.extend_from_slice(&shard.to_be_bytes());
    key.extend_from_slice(&consumer_len.to_be_bytes());
    key.extend_from_slice(consumer.as_bytes());
    Ok(key)
}

fn event_matches_collection(event: &ChangeEvent, collection: &RecordIdentity) -> bool {
    event.tenant == collection.tenant
        && event.namespace == collection.namespace
        && event.collection == collection.collection
}

fn identity_from_event(event: &ChangeEvent) -> Result<RecordIdentity> {
    RecordIdentity::new(
        event.tenant.clone(),
        event.namespace.clone(),
        event.collection.clone(),
        event.partition.clone(),
        event.key.clone(),
    )
}

fn truncate_pairs_without_splitting_batch(
    events: Vec<(Vec<u8>, ChangeEvent)>,
    limit: usize,
) -> Vec<(Vec<u8>, ChangeEvent)> {
    if events.len() <= limit {
        return events;
    }
    let boundary_batch = events[limit - 1].1.batch_id;
    let end = events
        .iter()
        .enumerate()
        .skip(limit)
        .find_map(|(index, (_, event))| (event.batch_id != boundary_batch).then_some(index))
        .unwrap_or(events.len());
    events.into_iter().take(end).collect()
}

fn truncate_without_splitting_batch(events: Vec<ChangeEvent>, limit: usize) -> Vec<ChangeEvent> {
    if events.len() <= limit {
        return events;
    }
    let boundary_batch = events[limit - 1].batch_id;
    let end = events
        .iter()
        .enumerate()
        .skip(limit)
        .find_map(|(index, event)| (event.batch_id != boundary_batch).then_some(index))
        .unwrap_or(events.len());
    events.into_iter().take(end).collect()
}

fn estimate_payload(payload: &Payload) -> usize {
    match payload {
        Payload::Bytes(bytes) | Payload::Document { bytes, .. } => bytes.len(),
        Payload::Text(text) => text.len(),
        Payload::Integer(_) | Payload::Float(_) | Payload::Timestamp(_) => 8,
        Payload::Boolean(_) => 1,
        Payload::Vector(values) => values.len().saturating_mul(size_of::<f32>()),
        Payload::BlobRef { id, .. } => id.len().saturating_add(8),
    }
}

fn estimate_record(record: &RecordEnvelope) -> usize {
    record
        .identity
        .storage_key()
        .len()
        .saturating_add(record.payload.as_ref().map_or(0, estimate_payload))
        .saturating_add(record.content_type.len())
        .saturating_add(
            record
                .metadata
                .iter()
                .map(|(key, value)| key.len().saturating_add(value.len()))
                .sum::<usize>(),
        )
        .saturating_add(record.workflow.state.len())
        .saturating_add(128)
}

fn estimate_radial_descriptor(descriptor: &RadialDescriptor) -> usize {
    descriptor
        .identity
        .storage_key()
        .len()
        .saturating_add(descriptor.workflow_state.len())
        .saturating_add(descriptor.storage_class.len())
        .saturating_add(descriptor.last_decision.len())
        .saturating_add(
            descriptor
                .projection_watermarks
                .keys()
                .map(String::len)
                .sum::<usize>(),
        )
        .saturating_add(192)
}

fn freshness_millis(updated_at: u64, at: u64, half_life_ms: u64) -> u16 {
    if at <= updated_at {
        return 1000;
    }
    let age = at.saturating_sub(updated_at) as f64;
    let half_life = half_life_ms.max(1) as f64;
    let freshness = 2_f64.powf(-age / half_life).clamp(0.0, 1.0);
    (freshness * 1000.0).round() as u16
}

fn radial_score_millis(freshness: u16, urgency: u16, policy: &RadialPolicy) -> u16 {
    let weighted = u32::from(freshness) * u32::from(policy.freshness_weight_millis)
        + u32::from(urgency.min(1000)) * u32::from(policy.urgency_weight_millis);
    u16::try_from((weighted / 1000).min(1000)).unwrap_or(1000)
}

fn now_unix_ms() -> Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|error| AproError::InvalidInput(format!("clock before Unix epoch: {error}")))?
        .as_millis();
    u64::try_from(millis).map_err(|_| AproError::ResourceLimit("timestamp exceeds u64".into()))
}

#[cfg(test)]
mod tests {
    use std::{
        collections::HashSet,
        sync::{
            Arc, Barrier,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use super::{
        AtomicMutation, ClaimRequest, DeleteRequest, Engine, EngineConfig, EventRetentionMode,
        ExpectedVersion, Payload, PutRequest, REPAIR_DERIVED_CONFIRMATION, RadialLayer,
        RadialPolicy, RecordIdentity, StorageClassDescriptor, StorageMedium, SurfaceDefinition,
        SurfaceFormat, SurfaceKind, VectorSearchRequest, WorkflowScope, compression_dictionary_key,
        decode_surface_payload, now_unix_ms, percent, validate_config, version_key, workflow_key,
    };
    use crate::{AproError, AuditOutcome, ChangeBody, CollectionPolicy, Durability, Result};
    use aprodb_compute::{ComputeExecution, ComputePreference, VectorMetric};
    use aprodb_storage::{
        BackendCapabilities, BackendStats, CommitMode, EncryptionConfig, FaultInjector, FaultPoint,
        FjallBackend, FjallOptions, StorageBackend, StorageBatch, StorageSpace,
    };
    use aprodb_types::{
        CompressionCodec, CompressionDictionary, CompressionPolicy, CompressionTierPolicy,
        LogicalFrameKind, StoredRecordEnvelope, decode_logical, encode_logical,
    };
    use tempfile::tempdir;

    fn identity(partition: &str, key: &str) -> RecordIdentity {
        RecordIdentity::new("tenant", "namespace", "collection", partition, key).unwrap()
    }

    fn stored_record(
        engine: &Engine,
        identity: &RecordIdentity,
        version: super::Version,
    ) -> StoredRecordEnvelope {
        let bytes = engine
            .backend
            .get(StorageSpace::Versions, &version_key(identity, version))
            .unwrap()
            .unwrap();
        decode_logical(LogicalFrameKind::StoredRecord, &bytes).unwrap()
    }

    #[test]
    fn put_get_cas_delete_and_reopen() {
        let directory = tempdir().unwrap();
        let config = EngineConfig::new(directory.path());
        let first_version;
        {
            let engine = Engine::open(config.clone()).unwrap();
            let id = identity("p", "key");
            let first = engine
                .put(PutRequest::new(id.clone(), Payload::Text("one".into())))
                .unwrap();
            first_version = first.version;
            assert_eq!(
                engine.get(&id).unwrap().unwrap().payload,
                Some(Payload::Text("one".into()))
            );
            let stale = super::Version {
                sequence: first.version.sequence + 1,
                ..first.version
            };
            assert!(matches!(
                engine.compare_and_swap(
                    PutRequest::new(id.clone(), Payload::Text("bad".into())),
                    stale,
                    Durability::Durable
                ),
                Err(AproError::Conflict(_))
            ));
            engine
                .compare_and_swap(
                    PutRequest::new(id.clone(), Payload::Text("two".into())),
                    first.version,
                    Durability::Durable,
                )
                .unwrap();
            engine.delete(DeleteRequest::new(id.clone())).unwrap();
            assert_eq!(engine.get(&id).unwrap(), None);
            engine.verify().unwrap();
        }
        let engine = Engine::open(config).unwrap();
        let id = identity("p", "key");
        assert_eq!(engine.get(&id).unwrap(), None);
        assert_eq!(
            engine.get_version(&id, first_version).unwrap().payload,
            Some(Payload::Text("one".into()))
        );
    }

    #[test]
    fn atomic_batch_is_partition_scoped_and_events_are_not_split() {
        let directory = tempdir().unwrap();
        let engine = Engine::open(EngineConfig::new(directory.path())).unwrap();
        let a = identity("same", "a");
        let b = identity("same", "b");
        let receipts = engine
            .atomic_batch(
                vec![
                    AtomicMutation::Put(PutRequest::new(a.clone(), Payload::Integer(1))),
                    AtomicMutation::Put(PutRequest::new(b.clone(), Payload::Integer(2))),
                ],
                Durability::Durable,
            )
            .unwrap();
        assert_eq!(receipts.len(), 2);
        let events = engine.changes(receipts[0].version.shard_id, 0, 1).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].batch_id, events[1].batch_id);
        assert!(
            engine
                .atomic_batch(
                    vec![
                        AtomicMutation::Delete(DeleteRequest::new(a)),
                        AtomicMutation::Delete(DeleteRequest::new(identity("other", "b"))),
                    ],
                    Durability::Durable,
                )
                .is_err()
        );
    }

    #[test]
    fn retention_modes_return_exact_historical_data_after_reopen() {
        for mode in [
            EventRetentionMode::Delta,
            EventRetentionMode::VersionRef,
            EventRetentionMode::SelfContained,
        ] {
            let directory = tempdir().unwrap();
            let mut config = EngineConfig::new(directory.path());
            config.durability = Durability::Relaxed;
            config.storage.max_memtable_bytes = 1024 * 1024;
            let id = identity("p", "key");
            let mut shard = None;
            let mut versions = Vec::new();
            {
                let engine = Engine::open(config.clone()).unwrap();
                engine
                    .configure_collection(
                        &id,
                        CollectionPolicy {
                            retention_mode: mode,
                            required_consumers: vec!["projection".into()],
                            ..Default::default()
                        },
                    )
                    .unwrap();
                for value in 0..300u64 {
                    let mut payload = vec![b'x'; 4096];
                    payload[..8].copy_from_slice(&value.to_be_bytes());
                    let mut request = PutRequest::new(id.clone(), Payload::Bytes(payload));
                    request.delta = Some(value.to_be_bytes().to_vec());
                    let receipt = engine.put(request).unwrap();
                    shard = Some(receipt.version.shard_id);
                    versions.push(receipt.version);
                }
                engine.sync().unwrap();
            }
            let engine = Engine::open(config.clone()).unwrap();
            let shard = shard.unwrap();
            let compacted = engine.major_compact().unwrap();
            assert!(compacted.table_count_after > 0);
            let events = engine.changes(shard, 0, 512).unwrap();
            assert_eq!(events.len(), 300);
            for (expected, event) in (0..300u64).zip(&events) {
                match &event.body {
                    ChangeBody::Delta { bytes } => {
                        assert_eq!(bytes, &expected.to_be_bytes());
                    }
                    ChangeBody::VersionRef { identity, version } => {
                        let Some(Payload::Bytes(payload)) =
                            engine.get_version(identity, *version).unwrap().payload
                        else {
                            panic!("historical payload is not binary");
                        };
                        assert_eq!(&payload[..8], &expected.to_be_bytes());
                    }
                    ChangeBody::SelfContained { record } => {
                        let Some(Payload::Bytes(payload)) = &record.payload else {
                            panic!("event payload is not binary");
                        };
                        assert_eq!(&payload[..8], &expected.to_be_bytes());
                    }
                }
            }
            assert_eq!(
                engine
                    .garbage_collect_collection(&id, shard, 512)
                    .unwrap()
                    .events_deleted,
                0
            );
            engine
                .acknowledge_consumer(&id, "projection", shard, events[298].version.sequence)
                .unwrap();
            let gc = engine.garbage_collect_collection(&id, shard, 512).unwrap();
            assert_eq!(gc.events_deleted, 299);
            assert_eq!(gc.versions_deleted, 299);
            engine.major_compact().unwrap();
            drop(engine);

            let reopened = Engine::open(config).unwrap();
            let Some(Payload::Bytes(current)) = reopened.get(&id).unwrap().unwrap().payload else {
                panic!("current payload is not binary");
            };
            assert_eq!(&current[..8], &299u64.to_be_bytes());
            assert!(reopened.get_version(&id, versions[0]).is_err());
            assert!(matches!(
                reopened.changes(shard, 0, 512),
                Err(AproError::ChangeLogGap(_))
            ));
            let retained = reopened
                .changes(shard, events[298].version.sequence, 512)
                .unwrap();
            assert_eq!(retained.len(), 1);
            assert_eq!(retained[0].version, versions[299]);
        }
    }

    #[test]
    fn expected_missing_and_limits_are_enforced() {
        let directory = tempdir().unwrap();
        let mut config = EngineConfig::new(directory.path());
        config.limits.max_batch_operations = 1;
        let engine = Engine::open(config).unwrap();
        let id = identity("p", "key");
        let mut request = PutRequest::new(id.clone(), Payload::Boolean(true));
        request.expected = ExpectedVersion::Missing;
        engine.put(request.clone()).unwrap();
        assert!(matches!(engine.put(request), Err(AproError::Conflict(_))));
        assert!(matches!(
            engine.atomic_batch(
                vec![
                    AtomicMutation::Delete(DeleteRequest::new(id.clone())),
                    AtomicMutation::Delete(DeleteRequest::new(identity("p", "other"))),
                ],
                Durability::Relaxed,
            ),
            Err(AproError::ResourceLimit(_))
        ));
    }

    #[test]
    fn ttl_index_update_expiration_and_reopen_are_consistent() {
        let directory = tempdir().unwrap();
        let engine = Engine::open(EngineConfig::new(directory.path())).unwrap();
        let id = identity("ttl", "key");
        let first_expiry = now_unix_ms().unwrap().saturating_add(60_000);
        let mut first = PutRequest::new(id.clone(), Payload::Text("first".into()));
        first.expires_at_unix_ms = Some(first_expiry);
        engine.put(first).unwrap();
        assert!(engine.get(&id).unwrap().is_some());
        assert_eq!(
            engine
                .expire_due_at(first_expiry.saturating_sub(1), 8, Durability::Durable)
                .unwrap()
                .scanned,
            0
        );

        let second_expiry = first_expiry.saturating_add(60_000);
        let mut second = PutRequest::new(id.clone(), Payload::Text("second".into()));
        second.expires_at_unix_ms = Some(second_expiry);
        engine.put(second).unwrap();
        assert_eq!(
            engine
                .expire_due_at(first_expiry, 8, Durability::Durable)
                .unwrap()
                .scanned,
            0
        );
        let report = engine
            .expire_due_at(second_expiry, 8, Durability::Durable)
            .unwrap();
        assert_eq!(report.expired, 1);
        assert!(engine.get(&id).unwrap().is_none());
        engine.verify().unwrap();
        drop(engine);

        let reopened = Engine::open(EngineConfig::new(directory.path())).unwrap();
        assert!(reopened.get(&id).unwrap().is_none());
        assert_eq!(
            reopened
                .expire_due_at(second_expiry, 8, Durability::Durable)
                .unwrap()
                .scanned,
            0
        );
        reopened.verify().unwrap();
    }

    #[test]
    fn memory_budget_is_partitioned_and_rejects_unsafe_minimums() {
        let directory = tempdir().unwrap();
        let mut config = EngineConfig::new(directory.path());
        assert!(matches!(
            config.apply_memory_budget(127 * 1024 * 1024),
            Err(AproError::InvalidInput(_))
        ));

        let budget = 512 * 1024 * 1024;
        config.apply_memory_budget(budget).unwrap();
        assert_eq!(config.memory_budget_bytes, budget);
        assert_eq!(config.object_cache_bytes, percent(budget, 20));
        assert_eq!(config.metadata_cache_bytes, percent(budget, 20));
        assert_eq!(config.negative_cache_bytes, percent(budget, 2));
        assert_eq!(config.limits.max_inflight_bytes, percent(budget, 10));
        validate_config(&config).unwrap();
    }

    #[test]
    fn radial_policy_pin_storage_classes_and_explain_survive_restart() {
        let directory = tempdir().unwrap();
        let engine = Engine::open(EngineConfig::new(directory.path())).unwrap();
        let id = identity("radial", "key");
        engine
            .configure_radial_policy(
                &id,
                RadialPolicy {
                    freshness_half_life_ms: 1_000,
                    freshness_weight_millis: 700,
                    urgency_weight_millis: 300,
                    promotion_threshold_millis: 700,
                    demotion_threshold_millis: 350,
                    minimum_residency_ms: 0,
                },
            )
            .unwrap();
        let receipt = engine
            .put(PutRequest::new(id.clone(), Payload::Integer(1)))
            .unwrap();
        let at = now_unix_ms().unwrap().saturating_add(2_000);
        let object_cache_before = engine.cache_stats().objects;
        let cold = engine.explain_placement(&id, at).unwrap();
        assert_eq!(cold.canonical_version, receipt.version);
        assert_eq!(cold.recommended_layer, RadialLayer::Cold);
        assert!(!cold.physical_tiering_supported);

        engine
            .set_radial_signals(&id, 1_000, Some(at.saturating_add(10_000)), 50_000)
            .unwrap();
        let pinned = engine.explain_placement(&id, at).unwrap();
        assert!(pinned.pinned);
        assert_eq!(pinned.recommended_layer, RadialLayer::Hot);
        assert_eq!(engine.cache_stats().objects, object_cache_before);

        engine
            .register_storage_class(StorageClassDescriptor {
                name: "logical-cold".into(),
                medium: StorageMedium::Hdd,
                budget_bytes: 1024 * 1024,
                priority: 10,
                path: None,
            })
            .unwrap();
        assert!(matches!(
            engine.register_storage_class(StorageClassDescriptor {
                name: "other-device".into(),
                medium: StorageMedium::Ssd,
                budget_bytes: 1024,
                priority: 1,
                path: Some(
                    directory
                        .path()
                        .join("other")
                        .to_string_lossy()
                        .into_owned()
                ),
            }),
            Err(AproError::Unsupported(_))
        ));
        drop(engine);

        let reopened = Engine::open(EngineConfig::new(directory.path())).unwrap();
        assert!(
            reopened
                .storage_classes()
                .iter()
                .any(|storage| storage.name == "logical-cold")
        );
        assert!(reopened.explain_placement(&id, at).unwrap().pinned);
        reopened.verify().unwrap();
    }

    #[test]
    fn separate_caches_are_bounded_and_negative_entries_are_invalidated() {
        let directory = tempdir().unwrap();
        let mut config = EngineConfig::new(directory.path());
        config.object_cache_bytes = 16 * 1024;
        config.metadata_cache_bytes = 8 * 1024;
        config.negative_cache_bytes = 4 * 1024;
        let engine = Engine::open(config).unwrap();
        let missing = identity("cache", "missing");
        assert!(engine.get(&missing).unwrap().is_none());
        assert!(engine.get(&missing).unwrap().is_none());
        assert!(engine.cache_stats().negative.hits >= 1);
        engine
            .put(PutRequest::new(missing.clone(), Payload::Boolean(true)))
            .unwrap();
        assert_eq!(
            engine.get(&missing).unwrap().unwrap().payload,
            Some(Payload::Boolean(true))
        );

        let payload = vec![0x5A; 512];
        let mut identities = Vec::new();
        for index in 0..128 {
            let id = identity("cache", &format!("key-{index:03}"));
            engine
                .put(PutRequest::new(id.clone(), Payload::Bytes(payload.clone())))
                .unwrap();
            identities.push(id);
        }
        for id in &identities {
            assert_eq!(
                engine.get(id).unwrap().unwrap().payload,
                Some(Payload::Bytes(payload.clone()))
            );
        }
        let stats = engine.cache_stats();
        assert!(stats.objects.resident_bytes <= stats.objects.budget_bytes);
        assert!(stats.metadata.resident_bytes <= stats.metadata.budget_bytes);
        assert!(stats.objects.rejections + stats.objects.evictions > 0);
        engine.major_compact().unwrap();
        drop(engine);

        let reopened = Engine::open(EngineConfig::new(directory.path())).unwrap();
        for id in identities {
            assert!(reopened.get(&id).unwrap().is_some());
        }
        reopened.verify().unwrap();
    }

    #[test]
    fn idempotency_replays_exact_receipts_survives_restart_and_expires() {
        let directory = tempdir().unwrap();
        let config = EngineConfig::new(directory.path());
        let id = identity("idempotency", "key");
        let mut request = PutRequest::new(id.clone(), Payload::Integer(7));
        request.idempotency_key_hash = Some([0x11; 32]);

        let engine = Engine::open(config.clone()).unwrap();
        let first = engine.put(request.clone()).unwrap();
        let replay = engine.put(request.clone()).unwrap();
        assert_eq!(replay, first); // Ensure idempotent replay returns the same result
        assert_eq!(engine.current_version(&id).unwrap(), Some(first.version));
        let mut conflicting = request.clone();
        conflicting.payload = Payload::Integer(8);
        assert!(matches!(
            engine.put(conflicting),
            Err(AproError::Conflict(_))
        )); // Confirm conflicting put requests result in a conflict error
        drop(engine);

        let reopened = Engine::open(config).unwrap();
        assert_eq!(reopened.put(request.clone()).unwrap(), first);
        let purge = reopened.purge_expired_idempotency(u64::MAX, 16).unwrap();
        assert_eq!(purge.records_deleted, 1);
        let after_retention = reopened.put(request).unwrap();
        assert!(after_retention.version.sequence > first.version.sequence);
        reopened.verify().unwrap();
    }

    #[test]
    fn workflow_fencing_idempotency_and_restart_preserve_state_machine() {
        let directory = tempdir().unwrap();
        let mut config = EngineConfig::new(directory.path());
        config.lease_recovery_safety_margin = Duration::ZERO;
        let id = identity("workflow", "job");
        let scope = WorkflowScope::new("tenant", "namespace", "collection", "workflow").unwrap();
        let engine = Engine::open(config.clone()).unwrap();
        let append = engine
            .append(
                PutRequest::new(id.clone(), Payload::Text("job".into())),
                Durability::Durable,
            )
            .unwrap();
        assert_eq!(engine.get(&id).unwrap().unwrap().workflow.state, "pending");

        let first_request = ClaimRequest {
            scope: scope.clone(),
            max_records: 1,
            lease_duration: Duration::from_secs(60),
            idempotency_key_hash: Some([0x21; 32]),
            durability: Durability::Durable,
        };
        let first = engine.claim(first_request.clone()).unwrap();
        let replay = engine.claim(first_request).unwrap();
        assert_eq!(replay[0].record.version, first[0].record.version); // Confirm repeated claim returns same record version
        assert_eq!(replay[0].lease, first[0].lease);
        assert!(first[0].record.version.sequence > append.version.sequence);

        let heartbeat = engine
            .heartbeat(
                &id,
                first[0].lease,
                Duration::from_secs(60),
                Some([0x22; 32]),
                Durability::Durable,
            )
            .unwrap();
        let heartbeat_replay = engine
            .heartbeat(
                &id,
                first[0].lease,
                Duration::from_secs(60),
                Some([0x22; 32]),
                Durability::Durable,
            )
            .unwrap();
        assert_eq!(heartbeat_replay.receipt, heartbeat.receipt); // Verify heartbeat idempotency preserves receipt

        let failed = engine
            .fail(
                &id,
                first[0].lease,
                false,
                Some([0x23; 32]),
                Durability::Durable,
            )
            .unwrap();
        let failed_replay = engine
            .fail(
                &id,
                first[0].lease,
                false,
                Some([0x23; 32]),
                Durability::Durable,
            )
            .unwrap();
        assert_eq!(failed_replay.receipt, failed.receipt); // Confirm failure replay returns matching receipt
        assert_eq!(failed.record.workflow.state, "pending"); // Workflow state remains 'pending' after failure

        let second = engine
            .claim(ClaimRequest {
                scope,
                max_records: 1,
                lease_duration: Duration::from_secs(60),
                idempotency_key_hash: Some([0x24; 32]),
                durability: Durability::Durable,
            })
            .unwrap();
        assert!(second[0].lease.fencing_token > first[0].lease.fencing_token); // Ensure fencing token increments with subsequent claims
        drop(engine);

        let reopened = Engine::open(config).unwrap();
        assert!(matches!(
            reopened.complete(&id, first[0].lease, None, Durability::Durable),
            Err(AproError::Conflict(_))
        )); // Prevent completing a job with a stale lease after restart
        let completed = reopened
            .complete(&id, second[0].lease, Some([0x25; 32]), Durability::Durable)
            .unwrap();
        let completed_replay = reopened
            .complete(&id, second[0].lease, Some([0x25; 32]), Durability::Durable)
            .unwrap();
        assert_eq!(completed_replay.receipt, completed.receipt); // Confirm idempotent replay of complete returns matching receipt
        assert_eq!(completed.record.workflow.state, "completed"); // Workflow state updated to 'completed' after completing the job

        let published = reopened
            .publish(&id, Some([0x26; 32]), Durability::Durable)
            .unwrap();
        let published_replay = reopened
            .publish(&id, Some([0x26; 32]), Durability::Durable)
            .unwrap();
        assert_eq!(published_replay.receipt, published.receipt); // Confirm idempotent replay of publish returns matching receipt
        assert_eq!(published.record.workflow.state, "published"); // Workflow state updated to 'published' after publishing
        assert_eq!(
            reopened
                .publish(&id, None, Durability::Durable)
                .unwrap()
                .record
                .version,
            published.record.version
        );
        reopened.verify().unwrap();
    }

    #[test]
    fn concurrent_claims_do_not_return_the_same_job() {
        let directory = tempdir().unwrap();
        let engine = Arc::new(Engine::open(EngineConfig::new(directory.path())).unwrap());
        let scope = WorkflowScope::new("tenant", "namespace", "collection", "queue").unwrap();
        for key in ["one", "two"] {
            engine
                .append(
                    PutRequest::new(identity("queue", key), Payload::Text(key.into())),
                    Durability::Durable,
                )
                .unwrap();
        }
        let barrier = Arc::new(Barrier::new(3));
        let mut workers = Vec::new();
        for _ in 0..2 {
            let engine = Arc::clone(&engine);
            let scope = scope.clone();
            let barrier = Arc::clone(&barrier);
            workers.push(std::thread::spawn(move || {
                barrier.wait();
                engine
                    .claim(ClaimRequest {
                        scope,
                        max_records: 1,
                        lease_duration: Duration::from_secs(60),
                        idempotency_key_hash: None,
                        durability: Durability::Durable,
                    })
                    .unwrap()
                    .remove(0)
                    .record
                    .identity
                    .key
            }));
        }
        barrier.wait();
        let claimed: HashSet<_> = workers
            .into_iter()
            .map(|worker| worker.join().unwrap())
            .collect();
        assert_eq!(claimed.len(), 2); // Ensure concurrent claims return distinct jobs without duplication
    }

    #[test]
    fn work_and_read_surfaces_advance_incrementally_and_survive_restart() {
        let directory = tempdir().unwrap();
        let config = EngineConfig::new(directory.path());
        let engine = Engine::open(config.clone()).unwrap();
        let work = SurfaceDefinition {
            id: "work-queue".into(),
            kind: SurfaceKind::Work,
            source_tenant: b"tenant".to_vec(),
            source_namespace: b"namespace".to_vec(),
            source_collection: b"collection".to_vec(),
            workflow_states: vec!["pending".into()],
            format: SurfaceFormat::AprodbRecords,
            max_records: 32,
            max_bytes: 1024 * 1024,
            retained_generations: 2,
        };
        let read = SurfaceDefinition {
            id: "published-feed".into(),
            kind: SurfaceKind::Read,
            workflow_states: vec!["published".into()],
            format: SurfaceFormat::Json,
            ..work.clone()
        };
        engine.create_surface(work.clone()).unwrap();
        engine.create_surface(read.clone()).unwrap();
        let first = identity("surface", "first");
        let second = identity("surface", "second");
        for id in [&first, &second] {
            engine
                .append(
                    PutRequest::new(id.clone(), Payload::Text("job".into())),
                    Durability::Durable,
                )
                .unwrap();
        }
        let initial_work = engine
            .build_surface_incremental(&work.id, 64, Durability::Durable)
            .unwrap();
        assert_eq!(initial_work.record_count, 2);
        let first_generation = engine.get_surface(&work.id).unwrap().unwrap();
        assert_eq!(
            decode_surface_payload(first_generation.format, &first_generation.serialized)
                .unwrap()
                .len(),
            2
        );

        let scope = WorkflowScope::new("tenant", "namespace", "collection", "surface").unwrap();
        let claimed = engine
            .claim(ClaimRequest {
                scope,
                max_records: 1,
                lease_duration: Duration::from_secs(60),
                idempotency_key_hash: None,
                durability: Durability::Durable,
            })
            .unwrap();
        let leased_id = claimed[0].record.identity.clone();
        let stale_work = engine.read_surface(&work.id).unwrap().unwrap();
        assert!(!stale_work.complete);
        assert!(
            stale_work
                .stale_by_sequences
                .values()
                .any(|stale| *stale > 0)
        );
        let reduced_work = engine
            .build_surface_incremental(&work.id, 64, Durability::Durable)
            .unwrap();
        assert_eq!(reduced_work.record_count, 1);
        assert!(reduced_work.generation > initial_work.generation);
        assert!(engine.read_surface(&work.id).unwrap().unwrap().complete);

        engine
            .complete(&leased_id, claimed[0].lease, None, Durability::Durable)
            .unwrap();
        engine
            .publish(&leased_id, None, Durability::Durable)
            .unwrap();
        let published = engine
            .build_surface_incremental(&read.id, 64, Durability::Durable)
            .unwrap();
        assert_eq!(published.record_count, 1);
        let read_generation = engine.get_surface(&read.id).unwrap().unwrap();
        let read_records =
            decode_surface_payload(read_generation.format, &read_generation.serialized).unwrap();
        assert_eq!(read_records[0].identity, leased_id);
        assert_eq!(read_records[0].workflow.state, "published");
        drop(engine);

        let reopened = Engine::open(config).unwrap();
        assert_eq!(
            reopened.get_surface(&read.id).unwrap().unwrap(),
            read_generation
        );
        let unchanged = reopened
            .build_surface_incremental(&read.id, 64, Durability::Durable)
            .unwrap();
        assert_eq!(unchanged.generation, read_generation.generation);
        reopened.verify().unwrap();
    }

    #[test]
    fn surface_gap_requires_explicit_rebuild() {
        // Test that a surface gap requires an explicit rebuild to resolve it.
        let directory = tempdir().unwrap();
        let engine = Engine::open(EngineConfig::new(directory.path())).unwrap();
        let id = identity("surface-gap", "item");
        engine
            .configure_collection(
                &id,
                CollectionPolicy {
                    retention_mode: EventRetentionMode::VersionRef,
                    required_consumers: vec!["old-consumer".into()],
                    max_self_contained_event_bytes: 1024 * 1024,
                },
            )
            .unwrap();
        let mut versions = Vec::new();
        for value in 0..4 {
            versions.push(
                engine
                    .put(PutRequest::new(id.clone(), Payload::Integer(value)))
                    .unwrap()
                    .version,
            );
        }
        let shard = versions[0].shard_id;
        engine
            .acknowledge_consumer(&id, "old-consumer", shard, versions[2].sequence)
            .unwrap();
        assert_eq!(
            engine
                .garbage_collect_collection(&id, shard, 16)
                .unwrap()
                .events_deleted,
            3
        );
        let definition = SurfaceDefinition {
            id: "gap-rebuild".into(),
            kind: SurfaceKind::Read,
            source_tenant: id.tenant.clone(),
            source_namespace: id.namespace.clone(),
            source_collection: id.collection.clone(),
            workflow_states: vec!["ready".into()],
            format: SurfaceFormat::Json,
            max_records: 16,
            max_bytes: 64 * 1024,
            retained_generations: 2,
        };
        engine.create_surface(definition.clone()).unwrap();
        assert!(matches!(
            engine.build_surface_incremental(&definition.id, 16, Durability::Durable),
            Err(AproError::ChangeLogGap(_))
        ));
        let rebuilt = engine
            .rebuild_surface(&definition.id, Durability::Durable)
            .unwrap();
        assert_eq!(rebuilt.record_count, 1);
        assert_eq!(rebuilt.source_watermarks[&shard], versions[3].sequence);
        let generation = engine.get_surface(&definition.id).unwrap().unwrap();
        assert_eq!(generation.source_watermarks, rebuilt.source_watermarks);
    }

    #[test]
    #[ignore = "capacity gate test: writes 129 MiB and is executed explicitly for Milestone 3"]
    fn canonical_dataset_can_exceed_the_configured_memory_budget() {
        // Test that dataset size can exceed configured memory budget without errors.
        const BUDGET_BYTES: usize = 128 * 1024 * 1024;
        const PAYLOAD_BYTES: usize = 128 * 1024;
        const RECORDS: usize = 1_032;
        const BATCH_RECORDS: usize = 48;

        fn payload(index: usize) -> Vec<u8> {
            // Generate deterministic payload of specified size for given index.
            let mut state = (index as u64 + 1).wrapping_mul(0x9E37_79B9_7F4A_7C15);
            (0..PAYLOAD_BYTES)
                .map(|_| {
                    state ^= state << 13;
                    state ^= state >> 7;
                    state ^= state << 17;
                    state as u8
                })
                .collect()
        }

        const { assert!(RECORDS * PAYLOAD_BYTES > BUDGET_BYTES) };
        let directory = tempdir().unwrap();
        let mut config = EngineConfig::new(directory.path());
        config.apply_memory_budget(BUDGET_BYTES).unwrap();
        let engine = Engine::open(config.clone()).unwrap();
        for batch_start in (0..RECORDS).step_by(BATCH_RECORDS) {
            let batch_end = (batch_start + BATCH_RECORDS).min(RECORDS);
            let mutations = (batch_start..batch_end)
                .map(|index| {
                    AtomicMutation::Put(PutRequest::new(
                        identity("larger-than-budget", &format!("key-{index:04}")),
                        Payload::Bytes(payload(index)),
                    ))
                })
                .collect();
            engine.atomic_batch(mutations, Durability::Relaxed).unwrap();
        }
        engine.sync().unwrap();
        engine.major_compact().unwrap();
        drop(engine);

        let reopened = Engine::open(config).unwrap();
        for index in [0, RECORDS / 2, RECORDS - 1] {
            let id = identity("larger-than-budget", &format!("key-{index:04}"));
            assert_eq!(
                reopened.get(&id).unwrap().unwrap().payload,
                Some(Payload::Bytes(payload(index)))
            );
        }
        let cache = reopened.cache_stats();
        assert!(cache.objects.resident_bytes <= cache.objects.budget_bytes);
        reopened.verify().unwrap();
    }

    struct CountingBackend {
        // Storage backend wrapper that counts commit and persist operations.
        inner: FjallBackend,
        buffered_commits: AtomicUsize,
        durable_commits: AtomicUsize,
        persists: AtomicUsize,
    }

    impl CountingBackend {
        fn new(path: &std::path::Path) -> Self {
            // Initialize CountingBackend with inner FjallBackend at path.
            Self {
                inner: FjallBackend::open(path, FjallOptions::default()).unwrap(),
                buffered_commits: AtomicUsize::new(0),
                durable_commits: AtomicUsize::new(0),
                persists: AtomicUsize::new(0),
            }
        }

        fn reset(&self) {
            // Reset counts of buffered commits, durable commits, and persists to zero.
            self.buffered_commits.store(0, Ordering::Release);
            self.durable_commits.store(0, Ordering::Release);
            self.persists.store(0, Ordering::Release);
        }
    }

    impl StorageBackend for CountingBackend {
        fn name(&self) -> &'static str {
            self.inner.name()
        }

        fn capabilities(&self) -> BackendCapabilities {
            self.inner.capabilities()
        }

        fn get(&self, space: StorageSpace, key: &[u8]) -> Result<Option<Vec<u8>>> {
            self.inner.get(space, key)
        }

        fn commit(&self, batch: StorageBatch, mode: CommitMode) -> Result<()> {
            match mode {
                CommitMode::Buffered | CommitMode::Relaxed => {
                    self.buffered_commits.fetch_add(1, Ordering::AcqRel);
                }
                CommitMode::Durable => {
                    self.durable_commits.fetch_add(1, Ordering::AcqRel);
                }
            }
            self.inner.commit(batch, mode)
        }

        fn persist(&self, mode: CommitMode) -> Result<()> {
            self.persists.fetch_add(1, Ordering::AcqRel);
            self.inner.persist(mode)
        }

        fn scan_prefix(
            &self,
            space: StorageSpace,
            prefix: &[u8],
            limit: usize,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            self.inner.scan_prefix(space, prefix, limit)
        }

        fn scan_range(
            &self,
            space: StorageSpace,
            start: &[u8],
            end_inclusive: &[u8],
            limit: usize,
        ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
            self.inner.scan_range(space, start, end_inclusive, limit)
        }

        fn stats(&self) -> Result<BackendStats> {
            self.inner.stats()
        }
    }

    #[test]
    fn zero_window_syncs_each_request_and_nonzero_window_groups() {
        // Verify zero commit window syncs each request individually, non-zero groups requests.
        let zero_directory = tempdir().unwrap();
        let zero_backend = Arc::new(CountingBackend::new(zero_directory.path()));
        let zero_engine = Engine::with_backend(
            EngineConfig::new(zero_directory.path()),
            zero_backend.clone(),
        )
        .unwrap();
        zero_backend.reset();
        zero_engine
            .put(PutRequest::new(
                identity("zero", "key"),
                Payload::Integer(1),
            ))
            .unwrap();
        assert_eq!(zero_backend.durable_commits.load(Ordering::Acquire), 1);
        assert_eq!(zero_backend.persists.load(Ordering::Acquire), 0);

        let grouped_directory = tempdir().unwrap();
        let grouped_backend = Arc::new(CountingBackend::new(grouped_directory.path()));
        let mut config = EngineConfig::new(grouped_directory.path());
        config.group_commit_window = std::time::Duration::from_secs(1);
        let grouped_engine =
            Arc::new(Engine::with_backend(config, grouped_backend.clone()).unwrap());
        grouped_backend.reset();

        let first = identity("partition-a", "a");
        let mut suffix = 0u32;
        let second = loop {
            let candidate = identity(&format!("partition-{suffix}"), "b");
            if grouped_engine.shard_for_partition(&candidate.partition_key())
                != grouped_engine.shard_for_partition(&first.partition_key())
            {
                break candidate;
            }
            suffix += 1;
        };
        let barrier = Arc::new(Barrier::new(3));
        let workers: Vec<_> = [first, second]
            .into_iter()
            .map(|id| {
                let engine = Arc::clone(&grouped_engine);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    engine
                        .put(PutRequest::new(id, Payload::Integer(1)))
                        .unwrap();
                })
            })
            .collect();
        barrier.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(grouped_backend.buffered_commits.load(Ordering::Acquire), 2);
        assert_eq!(grouped_backend.durable_commits.load(Ordering::Acquire), 0);
        assert_eq!(grouped_backend.persists.load(Ordering::Acquire), 1);
    }

    #[test]
    fn logical_compression_is_adaptive_skippable_and_recoverable() {
        // Test logical compression behavior: adaptive, skippable, and recoverable.
        let directory = tempdir().unwrap();
        let config = EngineConfig::new(directory.path());
        let compressed_id = identity("compression", "compressible");
        let raw_id = identity("compression", "random");
        let skipped_id = identity("compression", "image");
        let compressed_version;
        {
            let engine = Engine::open(config.clone()).unwrap();
            let compressed_payload = Payload::Bytes(vec![b'a'; 32 * 1024]);
            let receipt = engine
                .put(PutRequest::new(
                    compressed_id.clone(),
                    compressed_payload.clone(),
                ))
                .unwrap();
            compressed_version = receipt.version;
            let stored = stored_record(&engine, &compressed_id, receipt.version);
            let stored_payload = stored.payload.unwrap();
            assert_eq!(stored_payload.codec, CompressionCodec::Zstandard);
            assert!(stored_payload.bytes.len() < stored_payload.logical_bytes as usize);
            assert_eq!(
                engine.get(&compressed_id).unwrap().unwrap().payload,
                Some(compressed_payload)
            );

            let mut seed = 0x9E37_79B9_7F4A_7C15_u64;
            let random = (0..16 * 1024)
                .map(|_| {
                    seed ^= seed << 7;
                    seed ^= seed >> 9;
                    seed ^= seed << 8;
                    seed as u8
                })
                .collect::<Vec<_>>();
            let receipt = engine
                .put(PutRequest::new(raw_id.clone(), Payload::Bytes(random)))
                .unwrap();
            assert_eq!(
                stored_record(&engine, &raw_id, receipt.version)
                    .payload
                    .unwrap()
                    .codec,
                CompressionCodec::Raw
            );

            let mut image = PutRequest::new(skipped_id.clone(), Payload::Bytes(vec![0; 8192]));
            image.content_type = "image/png".into();
            let receipt = engine.put(image).unwrap();
            assert_eq!(
                stored_record(&engine, &skipped_id, receipt.version)
                    .payload
                    .unwrap()
                    .codec,
                CompressionCodec::Raw
            );
            let stats = engine.compression_stats();
            assert!(stats.zstandard_payloads >= 1);
            assert!(stats.adaptive_fallbacks >= 1);
            assert!(stats.skipped_content_types >= 1);
        }
        let reopened = Engine::open(config).unwrap();
        assert_eq!(
            reopened
                .get_version(&compressed_id, compressed_version)
                .unwrap()
                .payload,
            Some(Payload::Bytes(vec![b'a'; 32 * 1024]))
        );
        reopened.verify().unwrap();
    }

    #[test]
    fn vector_exact_scans_a_bounded_collection_and_orders_ties() {
        // The 'vector_exact' function scans a bounded collection and orders ties deterministically.
        let directory = tempdir().unwrap();
        let config = EngineConfig::new(directory.path());
        {
            let engine = Engine::open(config.clone()).unwrap();
            for (key, vector) in [
                ("a", vec![1.0, 0.0]),
                ("b", vec![1.0, 0.0]),
                ("c", vec![0.0, 1.0]),
            ] {
                engine
                    .put(PutRequest::new(
                        identity("vectors", key),
                        Payload::Vector(vector),
                    ))
                    .unwrap();
            }
            engine
                .put(PutRequest::new(
                    identity("vectors", "other-width"),
                    Payload::Vector(vec![1.0, 0.0, 0.0]),
                ))
                .unwrap();
            engine
                .put(PutRequest::new(
                    identity("vectors", "text"),
                    Payload::Text("not a vector".into()),
                ))
                .unwrap();
            let result = engine
                .vector_exact(VectorSearchRequest {
                    tenant: b"tenant".to_vec(),
                    namespace: b"namespace".to_vec(),
                    collection: b"collection".to_vec(),
                    query: vec![1.0, 0.0],
                    metric: VectorMetric::Dot,
                    limit: 3,
                    max_scan_records: 16,
                    preference: ComputePreference::Cpu,
                })
                .unwrap();
            assert_eq!(result.execution, ComputeExecution::Cpu);
            assert_eq!(result.scanned_records, 5);
            assert_eq!(result.vector_candidates, 3);
            assert_eq!(
                result
                    .hits
                    .iter()
                    .map(|hit| hit.identity.key.as_slice())
                    .collect::<Vec<_>>(),
                vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()]
            );
            assert!(matches!(
                engine.vector_exact(VectorSearchRequest {
                    tenant: b"tenant".to_vec(),
                    namespace: b"namespace".to_vec(),
                    collection: b"collection".to_vec(),
                    query: vec![1.0, 0.0],
                    metric: VectorMetric::Dot,
                    limit: 1,
                    max_scan_records: 2,
                    preference: ComputePreference::Auto,
                }),
                Err(AproError::ResourceLimit(_))
            ));
        }
        let reopened = Engine::open(config).unwrap();
        let result = reopened
            .vector_exact(VectorSearchRequest {
                tenant: b"tenant".to_vec(),
                namespace: b"namespace".to_vec(),
                collection: b"collection".to_vec(),
                query: vec![1.0, 0.0],
                metric: VectorMetric::Cosine,
                limit: 1,
                max_scan_records: 16,
                preference: ComputePreference::Auto,
            })
            .unwrap();
        assert_eq!(result.hits[0].identity.key, b"a");
        #[cfg(not(feature = "gpu"))]
        assert_eq!(result.execution, ComputeExecution::CpuFallback);
    }

    #[test]
    fn compression_policy_and_cache_budgets_are_independent() {
        // Verify that compression policies and cache budgets operate independently without interference.
        let directory = tempdir().unwrap();
        let engine = Engine::open(EngineConfig::new(directory.path())).unwrap();
        let id = identity("compression-policy", "key");
        let policy = CompressionPolicy {
            hot: CompressionTierPolicy::raw(),
            warm: CompressionTierPolicy::raw(),
            cold: CompressionTierPolicy::raw(),
            archive: CompressionTierPolicy::raw(),
            ..CompressionPolicy::default()
        };
        engine
            .configure_compression_policy(&id, policy.clone())
            .unwrap();
        assert_eq!(engine.compression_policy(&id).unwrap(), policy);
        let receipt = engine
            .put(PutRequest::new(
                id.clone(),
                Payload::Bytes(vec![b'z'; 4096]),
            ))
            .unwrap();
        assert_eq!(
            stored_record(&engine, &id, receipt.version)
                .payload
                .unwrap()
                .codec,
            CompressionCodec::Raw
        );
        let before = engine.cache_stats();
        engine.get(&id).unwrap().unwrap();
        engine.get(&id).unwrap().unwrap();
        let after = engine.cache_stats();
        assert_eq!(
            after.compressed.budget_bytes,
            before.compressed.budget_bytes
        );
        assert!(after.compressed.admissions > before.compressed.admissions);
        assert!(after.objects.hits > before.objects.hits);
        assert_ne!(after.compressed.budget_bytes, after.objects.budget_bytes);
    }

    #[test]
    fn dictionary_is_validated_persisted_and_required_for_exact_decode() {
        // This test validates that dictionaries are validated, persisted correctly, and required for exact decoding.
        let directory = tempdir().unwrap();
        let config = EngineConfig::new(directory.path());
        let id = identity("dictionary", "key");
        let training = (0..32)
            .map(|index| {
                Payload::Text(format!(
                    "type=invoice;customer=regional-{index:04};currency=EUR;status=pending;line=widget-alpha;warehouse=rome;tax=standard"
                ))
            })
            .collect::<Vec<_>>();
        let validation = (100..108)
            .map(|index| {
                Payload::Text(format!(
                    "type=invoice;customer=regional-{index:04};currency=EUR;status=pending;line=widget-beta;warehouse=rome;tax=standard"
                ))
            })
            .collect::<Vec<_>>();
        let version;
        let dictionary_id;
        let dictionary_frame;
        {
            let engine = Engine::open(config.clone()).unwrap();
            let dictionary = engine
                .train_and_activate_dictionary(
                    &id,
                    "application/invoice",
                    &training,
                    &validation,
                    2048,
                    0,
                )
                .unwrap();
            dictionary_id = dictionary.id;
            assert!(
                dictionary.validation_with_dictionary_bytes
                    < dictionary.validation_without_dictionary_bytes
            );
            let mut request = PutRequest::new(
                id.clone(),
                Payload::Text(
                    "type=invoice;customer=regional-9999;currency=EUR;status=pending;line=widget-gamma;warehouse=rome;tax=standard"
                        .into(),
                ),
            );
            request.content_type = "application/invoice".into();
            let mut policy = engine.compression_policy(&id).unwrap();
            for tier in [
                &mut policy.hot,
                &mut policy.warm,
                &mut policy.cold,
                &mut policy.archive,
            ] {
                tier.min_input_bytes = 1;
                tier.min_savings_bytes = 0;
            }
            engine.configure_compression_policy(&id, policy).unwrap();
            version = engine.put(request).unwrap().version;
            let stored = stored_record(&engine, &id, version);
            let stored_payload = stored.payload.unwrap();
            assert_eq!(stored_payload.codec, CompressionCodec::Zstandard);
            assert_eq!(stored_payload.dictionary_id, Some(dictionary_id));
            dictionary_frame = engine
                .backend
                .get(
                    StorageSpace::Compression,
                    &compression_dictionary_key(dictionary_id),
                )
                .unwrap()
                .unwrap();
        }
        {
            let reopened = Engine::open(config.clone()).unwrap();
            assert_eq!(reopened.get_version(&id, version).unwrap().version, version);
            let mut dictionary: CompressionDictionary =
                decode_logical(LogicalFrameKind::CompressionDictionary, &dictionary_frame).unwrap();
            dictionary.bytes[0] ^= 0xFF;
            let mut batch = StorageBatch::with_capacity(1);
            batch.put(
                StorageSpace::Compression,
                compression_dictionary_key(dictionary_id),
                encode_logical(LogicalFrameKind::CompressionDictionary, &dictionary).unwrap(),
            );
            reopened.backend.commit(batch, CommitMode::Durable).unwrap();
        }
        {
            let reopened = Engine::open(config.clone()).unwrap();
            assert!(matches!(
                reopened.get_version(&id, version),
                Err(AproError::Corrupt(_))
            ));
            let mut batch = StorageBatch::with_capacity(1);
            batch.put(
                StorageSpace::Compression,
                compression_dictionary_key(dictionary_id),
                dictionary_frame,
            );
            reopened.backend.commit(batch, CommitMode::Durable).unwrap();
        }
        {
            let reopened = Engine::open(config.clone()).unwrap();
            assert_eq!(reopened.get_version(&id, version).unwrap().version, version);
            let mut batch = StorageBatch::with_capacity(1);
            batch.delete(
                StorageSpace::Compression,
                compression_dictionary_key(dictionary_id),
            );
            reopened.backend.commit(batch, CommitMode::Durable).unwrap();
        }
        let reopened = Engine::open(config).unwrap();
        assert!(matches!(
            reopened.get_version(&id, version),
            Err(AproError::Corrupt(_))
        ));
    }

    #[test]
    fn compression_scratch_budget_applies_backpressure_without_publication() {
        // Ensure that compression scratch space budget enforces backpressure correctly and prevents publication under budget constraints.
        let directory = tempdir().unwrap();
        let mut config = EngineConfig::new(directory.path());
        config.compression_scratch_bytes = 1024;
        let engine = Engine::open(config).unwrap();
        let id = identity("scratch", "key");
        assert!(matches!(
            engine.put(PutRequest::new(
                id.clone(),
                Payload::Bytes(vec![b's'; 4096])
            )),
            Err(AproError::Backpressure(_))
        ));
        assert_eq!(engine.get(&id).unwrap(), None);
    }

    #[test]
    fn legacy_aprc_record_remains_readable_but_new_writes_use_aprx() {
        let directory = tempdir().unwrap();
        let config = EngineConfig::new(directory.path());
        let id = identity("legacy-aprc", "key");
        let version;
        let expected;
        {
            let engine = Engine::open(config.clone()).unwrap();
            version = engine
                .put(PutRequest::new(
                    id.clone(),
                    Payload::Text("legacy logical record".into()),
                ))
                .unwrap()
                .version;
            let current_frame = engine
                .backend
                .get(StorageSpace::Versions, &version_key(&id, version))
                .unwrap()
                .unwrap();
            assert!(current_frame.starts_with(b"APRX"));
            expected = engine.get_version(&id, version).unwrap();
            let mut batch = StorageBatch::with_capacity(1);
            batch.put(
                StorageSpace::Versions,
                version_key(&id, version),
                encode_logical(LogicalFrameKind::Record, &expected).unwrap(),
            );
            engine.backend.commit(batch, CommitMode::Durable).unwrap();
        }
        let reopened = Engine::open(config).unwrap();
        assert_eq!(reopened.get_version(&id, version).unwrap(), expected);
        reopened.verify().unwrap();
    }

    #[derive(Default)]
    struct ToggleFault {
        point: AtomicUsize,
    }

    impl ToggleFault {
        fn arm(&self, point: FaultPoint) {
            let code = match point {
                FaultPoint::BeforeOpen => 4,
                FaultPoint::BeforeCommit => 1,
                FaultPoint::AfterCommitBeforeReturn => 2,
                FaultPoint::BeforePersist => 3,
            };
            self.point.store(code, Ordering::Release);
        }
    }

    impl FaultInjector for ToggleFault {
        fn check(&self, point: FaultPoint) -> Result<()> {
            let code = match point {
                FaultPoint::BeforeOpen => 4,
                FaultPoint::BeforeCommit => 1,
                FaultPoint::AfterCommitBeforeReturn => 2,
                FaultPoint::BeforePersist => 3,
            };
            if self
                .point
                .compare_exchange(code, 0, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                return Err(AproError::Storage(format!("fault {point:?}")));
            }
            Ok(())
        }
    }

    #[test]
    fn write_faults_fail_closed_and_reopen_recovers_known_state() {
        let before_directory = tempdir().unwrap();
        let faults = Arc::new(ToggleFault::default());
        let backend = Arc::new(
            FjallBackend::open_with_faults(
                before_directory.path(),
                FjallOptions::default(),
                faults.clone(),
            )
            .unwrap(),
        );
        let engine =
            Engine::with_backend(EngineConfig::new(before_directory.path()), backend).unwrap();
        let id = identity("fault", "key");

        faults.arm(FaultPoint::BeforeCommit);
        assert!(
            engine
                .put(PutRequest::new(id.clone(), Payload::Integer(1)))
                .is_err()
        );
        assert!(engine.get(&id).is_err());
        drop(engine);
        let reopened = Engine::open(EngineConfig::new(before_directory.path())).unwrap();
        assert_eq!(reopened.get(&id).unwrap(), None);
        drop(reopened);

        let after_directory = tempdir().unwrap();
        let after_faults = Arc::new(ToggleFault::default());
        let backend = Arc::new(
            FjallBackend::open_with_faults(
                after_directory.path(),
                FjallOptions::default(),
                after_faults.clone(),
            )
            .unwrap(),
        );
        let engine =
            Engine::with_backend(EngineConfig::new(after_directory.path()), backend).unwrap();
        after_faults.arm(FaultPoint::AfterCommitBeforeReturn);
        assert!(
            engine
                .put(PutRequest::new(id.clone(), Payload::Integer(2)))
                .is_err()
        );
        assert!(engine.get(&id).is_err());
        assert!(
            engine
                .put(PutRequest::new(id.clone(), Payload::Integer(3)))
                .is_err()
        );
        drop(engine);

        let engine = Engine::open(EngineConfig::new(after_directory.path())).unwrap();
        let committed = engine.get(&id).unwrap().unwrap();
        assert_eq!(committed.payload, Some(Payload::Integer(2)));
        let next = engine
            .put(PutRequest::new(id, Payload::Integer(3)))
            .unwrap();
        assert!(next.version.sequence > committed.version.sequence);
    }

    #[test]
    fn logical_checkpoint_reopens_as_an_independent_database() {
        let directory = tempdir().unwrap();
        let checkpoint = directory.path().join("checkpoint-1");
        let engine = Engine::open(EngineConfig::new(directory.path().join("source"))).unwrap();
        let id = identity("checkpoint", "key");
        engine
            .put(PutRequest::new(id.clone(), Payload::Text("durable".into())))
            .unwrap();
        let info = engine.create_checkpoint(&checkpoint).unwrap();
        assert!(info.entries >= 4);
        assert!(info.logical_bytes > 0);
        assert!(engine.create_checkpoint(&checkpoint).is_err());

        let restored = Engine::open(EngineConfig::new(&checkpoint)).unwrap();
        assert_eq!(
            restored.get(&id).unwrap().unwrap().payload,
            Some(Payload::Text("durable".into()))
        );
        restored.verify().unwrap();
    }

    #[test]
    fn encrypted_directory_requires_the_keyring_and_checkpoint_stays_encrypted() {
        let directory = tempfile::tempdir().unwrap();
        let data = directory.path().join("encrypted");
        let checkpoint = directory.path().join("encrypted-checkpoint");
        let mut config = EngineConfig::new(&data);
        config.encryption = Some(EncryptionConfig::single("key-1", [9; 32]).unwrap());
        let engine = Engine::open(config.clone()).unwrap();
        let id = identity("encrypted", "secret");
        engine
            .put(PutRequest::new(
                id.clone(),
                Payload::Text("payload riservato".into()),
            ))
            .unwrap();
        engine.create_checkpoint(&checkpoint).unwrap();
        drop(engine);

        assert!(matches!(
            Engine::open(EngineConfig::new(&data)),
            Err(AproError::IncompatibleFormat(_))
        ));
        let mut wrong = EngineConfig::new(&data);
        wrong.encryption = Some(EncryptionConfig::single("key-1", [8; 32]).unwrap());
        assert!(matches!(Engine::open(wrong), Err(AproError::Encryption(_))));

        let reopened = Engine::open(config).unwrap();
        assert_eq!(
            reopened.get(&id).unwrap().unwrap().payload,
            Some(Payload::Text("payload riservato".into()))
        );
        drop(reopened);
        let mut checkpoint_config = EngineConfig::new(&checkpoint);
        checkpoint_config.encryption = Some(EncryptionConfig::single("key-1", [9; 32]).unwrap());
        let checkpoint_engine = Engine::open(checkpoint_config).unwrap();
        assert_eq!(
            checkpoint_engine.get(&id).unwrap().unwrap().payload,
            Some(Payload::Text("payload riservato".into()))
        );
        checkpoint_engine.verify().unwrap();
    }

    #[test]
    fn rekey_writes_a_verified_independent_copy_and_preserves_source_key() {
        let directory = tempfile::tempdir().unwrap();
        let source = directory.path().join("source-old-key");
        let destination = directory.path().join("copy-new-key");
        let old_encryption = EncryptionConfig::single("old", [3; 32]).unwrap();
        let new_encryption = EncryptionConfig::single("new", [4; 32]).unwrap();
        let mut source_config = EngineConfig::new(&source);
        source_config.encryption = Some(old_encryption.clone());
        let engine = Engine::open(source_config.clone()).unwrap();
        let id = identity("rekey", "key");
        engine
            .put(PutRequest::new(id.clone(), Payload::Text("secret".into())))
            .unwrap();
        let checkpoint = engine
            .rekey_to_copy(&destination, new_encryption.clone())
            .unwrap();
        assert_eq!(checkpoint.encryption_key_ids, vec!["new"]);
        drop(engine);

        let mut destination_config = EngineConfig::new(&destination);
        destination_config.encryption = Some(new_encryption);
        let rekeyed = Engine::open(destination_config).unwrap();
        assert_eq!(
            rekeyed.get(&id).unwrap().unwrap().payload,
            Some(Payload::Text("secret".into()))
        );
        rekeyed.verify().unwrap();
        drop(rekeyed);

        let mut wrong_destination = EngineConfig::new(&destination);
        wrong_destination.encryption = Some(old_encryption);
        assert!(matches!(
            Engine::open(wrong_destination),
            Err(AproError::Encryption(_))
        ));
        assert!(
            Engine::open(source_config)
                .unwrap()
                .get(&id)
                .unwrap()
                .is_some()
        );
    }

    #[test]
    fn disk_quota_and_free_space_reserve_fail_before_mutation() {
        let quota_directory = tempfile::tempdir().unwrap();
        let mut quota_config = EngineConfig::new(quota_directory.path());
        quota_config.max_data_bytes = Some(1);
        let quota_engine = Engine::open(quota_config).unwrap();
        let id = identity("disk", "quota");
        assert!(matches!(
            quota_engine.put(PutRequest::new(id.clone(), Payload::Bytes(vec![1; 64]))),
            Err(AproError::ResourceLimit(_))
        ));
        assert!(quota_engine.get(&id).unwrap().is_none());

        let reserve_directory = tempfile::tempdir().unwrap();
        let mut reserve_config = EngineConfig::new(reserve_directory.path());
        reserve_config.min_free_disk_bytes = u64::MAX;
        let reserve_engine = Engine::open(reserve_config).unwrap();
        assert!(matches!(
            reserve_engine.put(PutRequest::new(id.clone(), Payload::Integer(1))),
            Err(AproError::Backpressure(_))
        ));
        assert!(reserve_engine.get(&id).unwrap().is_none());
    }

    #[test]
    fn verified_backup_restores_separately_and_detects_tampering() {
        let directory = tempfile::tempdir().unwrap();
        let data = directory.path().join("source");
        let backup = directory.path().join("backup");
        let tampered_backup = directory.path().join("tampered-backup");
        let restore = directory.path().join("restore");
        let engine = Engine::open(EngineConfig::new(&data)).unwrap();
        let id = identity("backup", "key");
        engine
            .put(PutRequest::new(
                id.clone(),
                Payload::Text("backup-value".into()),
            ))
            .unwrap();
        let info = engine.create_backup(&backup).unwrap();
        assert!(!info.manifest.files.is_empty());
        assert_eq!(info.manifest.verification.heads_checked, 1);
        let report = Engine::restore_backup(
            &backup,
            &restore,
            EngineConfig::new(directory.path().join("ignored")),
        )
        .unwrap();
        assert_eq!(report.verification.heads_checked, 1);
        let restored = Engine::open(EngineConfig::new(&restore)).unwrap();
        assert_eq!(
            restored.get(&id).unwrap().unwrap().payload,
            Some(Payload::Text("backup-value".into()))
        );
        drop(restored);
        assert!(Engine::restore_backup(&backup, &restore, EngineConfig::new("ignored")).is_err());

        engine.create_backup(&tampered_backup).unwrap();
        let manifest = Engine::verify_backup(&tampered_backup).unwrap();
        let victim = manifest.files.iter().find(|file| file.bytes > 0).unwrap();
        let victim_path = tampered_backup.join("data").join(&victim.relative_path);
        let mut bytes = std::fs::read(&victim_path).unwrap();
        bytes[0] ^= 1;
        std::fs::write(victim_path, bytes).unwrap();
        assert!(matches!(
            Engine::verify_backup(&tampered_backup),
            Err(AproError::Corrupt(_))
        ));
    }

    #[test]
    fn repair_is_explicit_copy_only_and_rebuilds_derived_indexes() {
        let directory = tempfile::tempdir().unwrap();
        let data = directory.path().join("source");
        let repaired_path = directory.path().join("repaired");
        let config = EngineConfig::new(&data);
        let backend = Arc::new(FjallBackend::open(&data, config.storage.clone()).unwrap());
        let engine = Engine::with_backend(config.clone(), backend.clone()).unwrap();
        let id = identity("repair", "key");
        engine
            .put(PutRequest::new(
                id.clone(),
                Payload::Text("canonical".into()),
            ))
            .unwrap();
        let record = engine.get(&id).unwrap().unwrap();
        let mut damage = StorageBatch::with_capacity(1);
        damage.delete(StorageSpace::Workflow, workflow_key(&record).unwrap());
        backend.commit(damage, CommitMode::Durable).unwrap();
        assert!(matches!(engine.verify(), Err(AproError::Corrupt(_))));
        assert!(
            engine
                .repair_derived_to_copy(&repaired_path, "yes")
                .is_err()
        );
        let report = engine
            .repair_derived_to_copy(&repaired_path, REPAIR_DERIVED_CONFIRMATION)
            .unwrap();
        assert_eq!(report.records_lost, 0);
        assert_eq!(report.records_doubtful, 0);
        assert_eq!(report.workflow_rebuilt, 1);
        assert_eq!(report.radial_hints_reset, 1);
        assert_eq!(report.verification.heads_checked, 1);
        assert!(matches!(engine.verify(), Err(AproError::Corrupt(_))));
        let repaired = Engine::open(EngineConfig::new(repaired_path)).unwrap();
        assert_eq!(
            repaired.get(&id).unwrap().unwrap().payload,
            Some(Payload::Text("canonical".into()))
        );
        repaired.verify().unwrap();
    }

    #[test]
    fn audit_is_durable_bounded_and_contains_no_target_plaintext() {
        let directory = tempfile::tempdir().unwrap();
        let engine = Engine::open(EngineConfig::new(directory.path())).unwrap();
        engine
            .append_audit_event(
                7,
                "admin-service",
                "configure_compression",
                AuditOutcome::Attempted,
                Some(blake3::hash(b"tenant/namespace/secret-key").into()),
                None,
            )
            .unwrap();
        engine
            .append_audit_event(
                7,
                "admin-service",
                "configure_compression",
                AuditOutcome::Succeeded,
                Some(blake3::hash(b"tenant/namespace/secret-key").into()),
                None,
            )
            .unwrap();
        let first = engine.read_audit(None, 1).unwrap();
        assert_eq!(first.events.len(), 1);
        assert_eq!(first.events[0].outcome, AuditOutcome::Attempted);
        let second = engine.read_audit(first.next, 2).unwrap();
        assert_eq!(second.events.len(), 1);
        assert_eq!(second.events[0].outcome, AuditOutcome::Succeeded);
        assert!(engine.read_audit(None, 0).is_err());
        assert_eq!(engine.verify().unwrap().audit_events_checked, 2);
        drop(engine);
        let reopened = Engine::open(EngineConfig::new(directory.path())).unwrap();
        let events = reopened.read_audit(None, 2).unwrap().events;
        assert_eq!(events.len(), 2);
        assert!(
            events
                .iter()
                .all(|event| !event.principal.contains("secret"))
        );
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
