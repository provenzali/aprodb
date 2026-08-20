use std::collections::BTreeMap;

use serde::{Deserialize, Serialize, de::DeserializeOwned};

const LOGICAL_FORMAT_VERSION: u8 = 1;
const FRAME_HEADER_LEN: usize = 13;
const MAX_LOGICAL_FRAME_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum AproError {
    #[error("invalid input: {0}")]
    InvalidInput(String),
    #[error("resource limit exceeded: {0}")]
    ResourceLimit(String),
    #[error("version conflict: {0}")]
    Conflict(String),
    #[error("persistent data corrupted: {0}")]
    Corrupt(String),
    #[error("incompatible format: {0}")]
    IncompatibleFormat(String),
    #[error("storage: {0}")]
    Storage(String),
    #[error("data directory already in use: {0}")]
    DataDirectoryLocked(String),
    #[error("backpressure: {0}")]
    Backpressure(String),
    #[error("gap in change log: {0}")]
    ChangeLogGap(String),
    #[error("operation not supported: {0}")]
    Unsupported(String),
    #[error("compute: {0}")]
    Compute(String),
    #[error("encryption: {0}")]
    Encryption(String),
}

pub type Result<T> = std::result::Result<T, AproError>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum VectorMetric {
    Dot,
    Cosine,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComputePreference {
    Cpu,
    Accelerator,
    Auto,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ComputeExecution {
    Cpu,
    Accelerator,
    CpuFallback,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct CostEstimate {
    pub transfer_in_micros: u64,
    pub queue_wait_micros: u64,
    pub launch_micros: u64,
    pub accelerator_compute_micros: u64,
    pub transfer_out_micros: u64,
    pub synchronization_micros: u64,
    pub risk_margin_micros: u64,
    pub accelerator_total_micros: u64,
    pub cpu_compute_micros: u64,
    pub vram_cache_hit: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    Durable,
    Relaxed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventRetentionMode {
    Delta,
    VersionRef,
    SelfContained,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Version {
    pub epoch: u64,
    pub shard_id: u32,
    pub sequence: u64,
}

impl Version {
    #[must_use]
    pub fn storage_suffix(self) -> [u8; 20] {
        let mut bytes = [0u8; 20];
        bytes[..8].copy_from_slice(&self.epoch.to_be_bytes());
        bytes[8..12].copy_from_slice(&self.shard_id.to_be_bytes());
        bytes[12..].copy_from_slice(&self.sequence.to_be_bytes());
        bytes
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct RecordIdentity {
    pub tenant: Vec<u8>,
    pub namespace: Vec<u8>,
    pub collection: Vec<u8>,
    pub partition: Vec<u8>,
    pub key: Vec<u8>,
}

impl RecordIdentity {
    pub fn new(
        tenant: impl Into<Vec<u8>>,
        namespace: impl Into<Vec<u8>>,
        collection: impl Into<Vec<u8>>,
        partition: impl Into<Vec<u8>>,
        key: impl Into<Vec<u8>>,
    ) -> Result<Self> {
        let identity = Self {
            tenant: tenant.into(),
            namespace: namespace.into(),
            collection: collection.into(),
            partition: partition.into(),
            key: key.into(),
        };
        identity.validate(&Limits::default())?;
        Ok(identity)
    }

    pub fn validate(&self, limits: &Limits) -> Result<()> {
        for (name, component) in [
            ("tenant", &self.tenant),
            ("namespace", &self.namespace),
            ("collection", &self.collection),
            ("partition", &self.partition),
            ("key", &self.key),
        ] {
            if component.is_empty() {
                return Err(AproError::InvalidInput(format!("{name} cannot be empty")));
            }
            if component.len() > limits.max_key_component_bytes {
                return Err(AproError::ResourceLimit(format!(
                    "{name} exceeds {} bytes",
                    limits.max_key_component_bytes
                )));
            }
            if component.len() > u16::MAX as usize {
                return Err(AproError::ResourceLimit(format!(
                    "{name} exceeds logical format"
                )));
            }
        }
        if self.storage_key().len() > limits.max_storage_key_bytes {
            return Err(AproError::ResourceLimit(format!(
                "serialized identity exceeds {} byte",
                limits.max_storage_key_bytes
            )));
        }
        Ok(())
    }

    #[must_use]
    pub fn storage_key(&self) -> Vec<u8> {
        let components = [
            self.tenant.as_slice(),
            self.namespace.as_slice(),
            self.collection.as_slice(),
            self.partition.as_slice(),
            self.key.as_slice(),
        ];
        let capacity = 1 + components.iter().map(|part| 2 + part.len()).sum::<usize>();
        let mut output = Vec::with_capacity(capacity);
        output.push(LOGICAL_FORMAT_VERSION);
        for component in components {
            let len = u16::try_from(component.len()).unwrap_or(u16::MAX);
            output.extend_from_slice(&len.to_be_bytes());
            output.extend_from_slice(component);
        }
        output
    }

    #[must_use]
    pub fn collection_key(&self) -> Vec<u8> {
        let mut output = Vec::new();
        for component in [&self.tenant, &self.namespace, &self.collection] {
            let len = u16::try_from(component.len()).unwrap_or(u16::MAX);
            output.extend_from_slice(&len.to_be_bytes());
            output.extend_from_slice(component);
        }
        output
    }

    #[must_use]
    pub fn partition_key(&self) -> Vec<u8> {
        let mut output = self.collection_key();
        let len = u16::try_from(self.partition.len()).unwrap_or(u16::MAX);
        output.extend_from_slice(&len.to_be_bytes());
        output.extend_from_slice(&self.partition);
        output
    }
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct WorkflowScope {
    pub tenant: Vec<u8>,
    pub namespace: Vec<u8>,
    pub collection: Vec<u8>,
    pub partition: Vec<u8>,
}

impl WorkflowScope {
    pub fn new(
        tenant: impl Into<Vec<u8>>,
        namespace: impl Into<Vec<u8>>,
        collection: impl Into<Vec<u8>>,
        partition: impl Into<Vec<u8>>,
    ) -> Result<Self> {
        let scope = Self {
            tenant: tenant.into(),
            namespace: namespace.into(),
            collection: collection.into(),
            partition: partition.into(),
        };
        scope.validate(&Limits::default())?;
        Ok(scope)
    }

    pub fn validate(&self, limits: &Limits) -> Result<()> {
        RecordIdentity {
            tenant: self.tenant.clone(),
            namespace: self.namespace.clone(),
            collection: self.collection.clone(),
            partition: self.partition.clone(),
            key: b"scope-validation".to_vec(),
        }
        .validate(limits)
    }

    pub fn identity(&self, key: impl Into<Vec<u8>>) -> Result<RecordIdentity> {
        RecordIdentity::new(
            self.tenant.clone(),
            self.namespace.clone(),
            self.collection.clone(),
            self.partition.clone(),
            key,
        )
    }

    #[must_use]
    pub fn collection_key(&self) -> Vec<u8> {
        let mut output = Vec::new();
        for component in [&self.tenant, &self.namespace, &self.collection] {
            let len = u16::try_from(component.len()).unwrap_or(u16::MAX);
            output.extend_from_slice(&len.to_be_bytes());
            output.extend_from_slice(component);
        }
        output
    }

    #[must_use]
    pub fn partition_key(&self) -> Vec<u8> {
        let mut output = self.collection_key();
        let len = u16::try_from(self.partition.len()).unwrap_or(u16::MAX);
        output.extend_from_slice(&len.to_be_bytes());
        output.extend_from_slice(&self.partition);
        output
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Payload {
    Bytes(Vec<u8>),
    Text(String),
    Integer(i64),
    Float(f64),
    Boolean(bool),
    Timestamp(i64),
    Vector(Vec<f32>),
    Document { schema_version: u32, bytes: Vec<u8> },
    BlobRef { id: Vec<u8>, logical_bytes: u64 },
}

impl Payload {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Float(value) if !value.is_finite() => {
                Err(AproError::InvalidInput("float must be finite".into()))
            }
            Self::Vector(values) if values.is_empty() => {
                Err(AproError::InvalidInput("vector cannot be empty".into()))
            }
            Self::Vector(values) if values.iter().any(|value| !value.is_finite()) => Err(
                AproError::InvalidInput("vector must contain only finite floats".into()),
            ),
            Self::Document {
                schema_version: 0, ..
            } => Err(AproError::InvalidInput(
                "schema version must be positive".into(),
            )),
            Self::BlobRef { id, .. } if id.is_empty() => {
                Err(AproError::InvalidInput("empty blob id".into()))
            }
            _ => Ok(()),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct WorkflowDescriptor {
    pub state: String,
    pub attempt: u32,
    pub lease_id: Option<[u8; 16]>,
    pub fencing_token: u64,
    pub lease_deadline_unix_ms: Option<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkflowIndexEntry {
    pub identity: RecordIdentity,
    pub version: Version,
    pub state: String,
    pub available_at_unix_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct LeaseProof {
    pub lease_id: [u8; 16],
    pub fencing_token: u64,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClaimedRecord {
    pub record: RecordEnvelope,
    pub receipt: MutationReceipt,
    pub lease: LeaseProof,
    pub lease_deadline_unix_ms: u64,
    pub server_time_unix_ms: u64,
    pub retry_after_ms: u64,
}

impl Default for WorkflowDescriptor {
    fn default() -> Self {
        Self {
            state: "ready".into(),
            attempt: 0,
            lease_id: None,
            fencing_token: 0,
            lease_deadline_unix_ms: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RecordEnvelope {
    pub identity: RecordIdentity,
    pub payload: Option<Payload>,
    pub content_type: String,
    pub version: Version,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
    pub expires_at_unix_ms: Option<u64>,
    pub metadata: BTreeMap<String, Vec<u8>>,
    pub workflow: WorkflowDescriptor,
    pub idempotency_key_hash: Option<[u8; 32]>,
    pub dictionary_id: Option<u64>,
    pub tombstone: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompressionCodec {
    Raw,
    Zstandard,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CompressionMode {
    Raw,
    AdaptiveZstandard,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompressionTierPolicy {
    pub mode: CompressionMode,
    pub zstd_level: i32,
    pub min_input_bytes: usize,
    pub min_savings_bytes: usize,
    pub dictionary_id: Option<u64>,
}

impl CompressionTierPolicy {
    #[must_use]
    pub const fn raw() -> Self {
        Self {
            mode: CompressionMode::Raw,
            zstd_level: 0,
            min_input_bytes: 0,
            min_savings_bytes: 0,
            dictionary_id: None,
        }
    }

    #[must_use]
    pub const fn adaptive(
        zstd_level: i32,
        min_input_bytes: usize,
        min_savings_bytes: usize,
    ) -> Self {
        Self {
            mode: CompressionMode::AdaptiveZstandard,
            zstd_level,
            min_input_bytes,
            min_savings_bytes,
            dictionary_id: None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompressionPolicy {
    pub surface: CompressionTierPolicy,
    pub hot: CompressionTierPolicy,
    pub warm: CompressionTierPolicy,
    pub cold: CompressionTierPolicy,
    pub archive: CompressionTierPolicy,
    pub skip_content_type_prefixes: Vec<String>,
}

impl Default for CompressionPolicy {
    fn default() -> Self {
        Self {
            surface: CompressionTierPolicy::raw(),
            hot: CompressionTierPolicy::adaptive(1, 256, 16),
            warm: CompressionTierPolicy::adaptive(3, 256, 16),
            cold: CompressionTierPolicy::adaptive(6, 512, 32),
            archive: CompressionTierPolicy::adaptive(9, 1024, 64),
            skip_content_type_prefixes: vec![
                "image/".into(),
                "audio/".into(),
                "video/".into(),
                "application/zip".into(),
                "application/gzip".into(),
                "application/zstd".into(),
            ],
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StoredPayload {
    pub codec_version: u8,
    pub codec: CompressionCodec,
    pub dictionary_id: Option<u64>,
    pub logical_bytes: u64,
    pub logical_checksum: u32,
    pub bytes: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct StoredRecordEnvelope {
    pub identity: RecordIdentity,
    pub payload: Option<StoredPayload>,
    pub content_type: String,
    pub version: Version,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
    pub expires_at_unix_ms: Option<u64>,
    pub metadata: BTreeMap<String, Vec<u8>>,
    pub workflow: WorkflowDescriptor,
    pub idempotency_key_hash: Option<[u8; 32]>,
    pub tombstone: bool,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompressionDictionary {
    pub id: u64,
    pub tenant: Vec<u8>,
    pub namespace: Vec<u8>,
    pub collection: Vec<u8>,
    pub schema: String,
    pub bytes: Vec<u8>,
    pub checksum: u32,
    pub created_at_unix_ms: u64,
    pub validation_raw_bytes: u64,
    pub validation_without_dictionary_bytes: u64,
    pub validation_with_dictionary_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CompressionCatalog {
    pub format_version: u32,
    pub generation: u64,
    pub next_dictionary_id: u64,
    pub policies: BTreeMap<Vec<u8>, CompressionPolicy>,
}

impl Default for CompressionCatalog {
    fn default() -> Self {
        Self {
            format_version: 1,
            generation: 1,
            next_dictionary_id: 1,
            policies: BTreeMap::new(),
        }
    }
}

impl RecordEnvelope {
    pub fn validate(&self, limits: &Limits) -> Result<()> {
        self.identity.validate(limits)?;
        if self.tombstone != self.payload.is_none() {
            return Err(AproError::InvalidInput(
                "tombstone and payload presence mismatch".into(),
            ));
        }
        if let Some(payload) = &self.payload {
            payload.validate()?;
        }
        let metadata_bytes = self
            .metadata
            .iter()
            .map(|(key, value)| key.len().saturating_add(value.len()))
            .sum::<usize>();
        if metadata_bytes > limits.max_metadata_bytes {
            return Err(AproError::ResourceLimit(format!(
                "metadata exceeds {} byte",
                limits.max_metadata_bytes
            )));
        }
        if self.workflow.state.is_empty() || self.workflow.state.len() > 255 {
            return Err(AproError::InvalidInput(
                "workflow state must contain 1..255 bytes".into(),
            ));
        }
        if self.workflow.state == "leased" {
            if self.workflow.lease_id.is_none()
                || self.workflow.lease_deadline_unix_ms.is_none()
                || self.workflow.fencing_token == 0
            {
                return Err(AproError::InvalidInput(
                    "leased state requires lease id, fencing token, and deadline".into(),
                ));
            }
        } else if self.workflow.lease_id.is_some() || self.workflow.lease_deadline_unix_ms.is_some()
        {
            return Err(AproError::InvalidInput(
                "a non-leased state cannot hold an active lease".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct HeadPointer {
    pub identity: RecordIdentity,
    pub version: Version,
    pub tombstone: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ChangeOperation {
    Put,
    Delete,
    Append,
    Claim,
    Heartbeat,
    Complete,
    Fail,
    Publish,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ChangeBody {
    Delta {
        bytes: Vec<u8>,
    },
    VersionRef {
        identity: RecordIdentity,
        version: Version,
    },
    SelfContained {
        record: Box<RecordEnvelope>,
    },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ChangeEvent {
    pub tenant: Vec<u8>,
    pub namespace: Vec<u8>,
    pub collection: Vec<u8>,
    pub partition: Vec<u8>,
    pub version: Version,
    pub operation: ChangeOperation,
    pub key: Vec<u8>,
    pub previous_version: Option<Version>,
    pub batch_id: [u8; 20],
    pub idempotency_key_hash: Option<[u8; 32]>,
    pub body: ChangeBody,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CollectionPolicy {
    pub retention_mode: EventRetentionMode,
    pub required_consumers: Vec<String>,
    pub max_self_contained_event_bytes: usize,
}

impl Default for CollectionPolicy {
    fn default() -> Self {
        Self {
            retention_mode: EventRetentionMode::VersionRef,
            required_consumers: Vec::new(),
            max_self_contained_event_bytes: 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct CatalogState {
    pub format_version: u32,
    pub generation: u64,
    pub backend: String,
    pub epoch: u64,
    pub shard_sequences: BTreeMap<u32, u64>,
    pub durable_watermarks: BTreeMap<u32, u64>,
    pub collections: BTreeMap<Vec<u8>, CollectionPolicy>,
    pub consumer_watermarks: BTreeMap<Vec<u8>, u64>,
}

impl CatalogState {
    #[must_use]
    pub fn empty(backend: impl Into<String>, shard_count: u32) -> Self {
        Self {
            format_version: 1,
            generation: 1,
            backend: backend.into(),
            epoch: 1,
            shard_sequences: (0..shard_count).map(|shard| (shard, 0)).collect(),
            durable_watermarks: (0..shard_count).map(|shard| (shard, 0)).collect(),
            collections: BTreeMap::new(),
            consumer_watermarks: BTreeMap::new(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MutationReceipt {
    pub version: Version,
    pub durability: Durability,
    pub durable_watermark: u64,
    pub batch_id: [u8; 20],
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdempotencyRecord {
    pub scope: Vec<u8>,
    pub key_hash: [u8; 32],
    pub request_fingerprint: [u8; 32],
    pub receipts: Vec<MutationReceipt>,
    pub expires_at_unix_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct IdempotencyExpiryEntry {
    pub lookup_key: Vec<u8>,
    pub expires_at_unix_ms: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AuditOutcome {
    Attempted,
    Succeeded,
    Failed,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuditEvent {
    pub format_version: u32,
    pub sequence: u64,
    pub event_id: [u8; 16],
    pub at_unix_ms: u64,
    pub request_id: u64,
    pub principal: String,
    pub operation: String,
    pub outcome: AuditOutcome,
    pub target_hash: Option<[u8; 32]>,
    pub error_class: Option<String>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuditCursor {
    pub sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct AuditState {
    pub format_version: u32,
    pub last_sequence: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SurfaceKind {
    Work,
    Read,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SurfaceFormat {
    AprodbRecords,
    Json,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SurfaceDefinition {
    pub id: String,
    pub kind: SurfaceKind,
    pub source_tenant: Vec<u8>,
    pub source_namespace: Vec<u8>,
    pub source_collection: Vec<u8>,
    pub workflow_states: Vec<String>,
    pub format: SurfaceFormat,
    pub max_records: usize,
    pub max_bytes: usize,
    pub retained_generations: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SurfacePointer {
    pub projection_id: String,
    pub current_generation: Option<u64>,
    pub next_generation: u64,
    pub source_watermarks: BTreeMap<u32, u64>,
    pub retained_generations: Vec<u64>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SurfaceGeneration {
    pub projection_id: String,
    pub generation: u64,
    pub source_watermarks: BTreeMap<u32, u64>,
    pub format: SurfaceFormat,
    pub record_count: usize,
    pub serialized: Vec<u8>,
    pub created_at_unix_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SurfaceRead {
    pub generation: SurfaceGeneration,
    pub stale_by_sequences: BTreeMap<u32, u64>,
    pub complete: bool,
    pub errors: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SurfaceBuildReport {
    pub projection_id: String,
    pub generation: u64,
    pub events_applied: usize,
    pub source_watermarks: BTreeMap<u32, u64>,
    pub record_count: usize,
    pub serialized_bytes: usize,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RadialLayer {
    Surface,
    Hot,
    Warm,
    Cold,
    Archive,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageMedium {
    Logical,
    Nvme,
    Ssd,
    Hdd,
    Memory,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RadialPolicy {
    pub freshness_half_life_ms: u64,
    pub freshness_weight_millis: u16,
    pub urgency_weight_millis: u16,
    pub promotion_threshold_millis: u16,
    pub demotion_threshold_millis: u16,
    pub minimum_residency_ms: u64,
}

impl Default for RadialPolicy {
    fn default() -> Self {
        Self {
            freshness_half_life_ms: 60 * 60 * 1000,
            freshness_weight_millis: 700,
            urgency_weight_millis: 300,
            promotion_threshold_millis: 700,
            demotion_threshold_millis: 350,
            minimum_residency_ms: 15 * 60 * 1000,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StorageClassDescriptor {
    pub name: String,
    pub medium: StorageMedium,
    pub budget_bytes: u64,
    pub priority: u16,
    pub path: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RadialState {
    pub format_version: u32,
    pub generation: u64,
    pub policies: BTreeMap<Vec<u8>, RadialPolicy>,
    pub storage_classes: BTreeMap<String, StorageClassDescriptor>,
}

impl Default for RadialState {
    fn default() -> Self {
        let primary = StorageClassDescriptor {
            name: "primary".into(),
            medium: StorageMedium::Logical,
            budget_bytes: u64::MAX,
            priority: 0,
            path: None,
        };
        Self {
            format_version: 1,
            generation: 1,
            policies: BTreeMap::new(),
            storage_classes: [(primary.name.clone(), primary)].into_iter().collect(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RadialDescriptor {
    pub identity: RecordIdentity,
    pub canonical_version: Version,
    pub created_at_unix_ms: u64,
    pub updated_at_unix_ms: u64,
    pub access_frequency_estimate: u64,
    pub last_access_sampled_unix_ms: Option<u64>,
    pub freshness_half_life_ms: u64,
    pub urgency_millis: u16,
    pub deadline_unix_ms: Option<u64>,
    pub workflow_state: String,
    pub projection_watermarks: BTreeMap<String, u64>,
    pub reconstruction_cost_micros: u64,
    pub logical_bytes: u64,
    pub physical_bytes: u64,
    pub storage_class: String,
    pub admin_pin_until_unix_ms: Option<u64>,
    pub layer: RadialLayer,
    pub layer_since_unix_ms: u64,
    pub last_decision: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TtlEntry {
    pub identity: RecordIdentity,
    pub version: Version,
    pub expires_at_unix_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PlacementExplanation {
    pub canonical_version: Version,
    pub radial_score_millis: u16,
    pub freshness_millis: u16,
    pub urgency_millis: u16,
    pub current_layer: RadialLayer,
    pub recommended_layer: RadialLayer,
    pub storage_class: String,
    pub pinned: bool,
    pub object_cache_resident: bool,
    pub physical_tiering_supported: bool,
    pub reasons: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Limits {
    pub max_key_component_bytes: usize,
    pub max_storage_key_bytes: usize,
    pub max_record_bytes: usize,
    pub max_metadata_bytes: usize,
    pub max_batch_operations: usize,
    pub max_batch_bytes: usize,
    pub max_inflight_bytes: usize,
    pub max_queue_depth: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_key_component_bytes: 4 * 1024,
            max_storage_key_bytes: 32 * 1024,
            max_record_bytes: 16 * 1024 * 1024,
            max_metadata_bytes: 64 * 1024,
            max_batch_operations: 1024,
            max_batch_bytes: 64 * 1024 * 1024,
            max_inflight_bytes: 64 * 1024 * 1024,
            max_queue_depth: 1024,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LogicalFrameKind {
    Record,
    StoredRecord,
    Head,
    Change,
    Catalog,
    Radial,
    RadialState,
    Ttl,
    Workflow,
    Idempotency,
    IdempotencyExpiry,
    SurfaceDefinition,
    SurfacePointer,
    SurfaceGeneration,
    SurfacePayload,
    CompressionCatalog,
    CompressionDictionary,
    Audit,
    AuditState,
}

impl LogicalFrameKind {
    const fn magic(self) -> [u8; 4] {
        match self {
            Self::Record => *b"APRC",
            Self::StoredRecord => *b"APRX",
            Self::Head => *b"APH1",
            Self::Change => *b"APCE",
            Self::Catalog => *b"APCT",
            Self::Radial => *b"APRD",
            Self::RadialState => *b"APRS",
            Self::Ttl => *b"APTL",
            Self::Workflow => *b"APWF",
            Self::Idempotency => *b"APID",
            Self::IdempotencyExpiry => *b"APIE",
            Self::SurfaceDefinition => *b"APSD",
            Self::SurfacePointer => *b"APSP",
            Self::SurfaceGeneration => *b"APSG",
            Self::SurfacePayload => *b"APSY",
            Self::CompressionCatalog => *b"APCC",
            Self::CompressionDictionary => *b"APCD",
            Self::Audit => *b"APAU",
            Self::AuditState => *b"APAS",
        }
    }
}

pub fn encode_logical<T: Serialize>(kind: LogicalFrameKind, value: &T) -> Result<Vec<u8>> {
    let payload = bincode::serde::encode_to_vec(value, bincode::config::standard())
        .map_err(|error| AproError::InvalidInput(format!("codifica logica: {error}")))?;
    if payload.len() > MAX_LOGICAL_FRAME_BYTES {
        return Err(AproError::ResourceLimit(format!(
            "logical frame exceeds {MAX_LOGICAL_FRAME_BYTES} byte"
        )));
    }
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| AproError::ResourceLimit("logical frame over 4 GiB".into()))?;
    let mut output = Vec::with_capacity(FRAME_HEADER_LEN + payload.len());
    output.extend_from_slice(&kind.magic());
    output.push(LOGICAL_FORMAT_VERSION);
    output.extend_from_slice(&payload_len.to_le_bytes());
    output.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    output.extend_from_slice(&payload);
    Ok(output)
}

pub fn decode_logical<T: DeserializeOwned>(kind: LogicalFrameKind, bytes: &[u8]) -> Result<T> {
    if bytes.len() < FRAME_HEADER_LEN {
        return Err(AproError::Corrupt("logical frame incomplete".into()));
    }
    if bytes[..4] != kind.magic() {
        return Err(AproError::Corrupt("logical frame magic invalid".into()));
    }
    if bytes[4] != LOGICAL_FORMAT_VERSION {
        return Err(AproError::IncompatibleFormat(format!(
            "logical version {} not supported",
            bytes[4]
        )));
    }
    let payload_len = u32::from_le_bytes(
        bytes[5..9]
            .try_into()
            .map_err(|_| AproError::Corrupt("logical length incomplete".into()))?,
    ) as usize;
    if payload_len > MAX_LOGICAL_FRAME_BYTES || bytes.len() != FRAME_HEADER_LEN + payload_len {
        return Err(AproError::Corrupt(
            "logical frame length inconsistent".into(),
        ));
    }
    let checksum = u32::from_le_bytes(
        bytes[9..13]
            .try_into()
            .map_err(|_| AproError::Corrupt("logical checksum incomplete".into()))?,
    );
    let payload = &bytes[FRAME_HEADER_LEN..];
    if crc32fast::hash(payload) != checksum {
        return Err(AproError::Corrupt("logical frame checksum invalid".into()));
    }
    let (value, consumed) = bincode::serde::decode_from_slice(payload, bincode::config::standard())
        .map_err(|error| AproError::Corrupt(format!("logical decoding: {error}")))?;
    if consumed != payload.len() {
        return Err(AproError::Corrupt(
            "leftover bytes after logical frame".into(),
        ));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::{
        CatalogState, ChangeBody, ChangeEvent, ChangeOperation, EventRetentionMode, Limits,
        LogicalFrameKind, Payload, RecordEnvelope, RecordIdentity, Version, WorkflowDescriptor,
        decode_logical, encode_logical,
    };
    use std::collections::BTreeMap;

    #[test]
    fn identity_key_preserves_component_boundaries() {
        let left = RecordIdentity::new("a", "bc", "c", "p", "k").unwrap();
        let right = RecordIdentity::new("ab", "c", "c", "p", "k").unwrap();
        assert_ne!(left.storage_key(), right.storage_key());
        left.validate(&Limits::default()).unwrap();
    }

    #[test]
    fn version_storage_suffix_sorts_lexicographically() {
        let older = Version {
            epoch: 1,
            shard_id: 7,
            sequence: 9,
        };
        let newer = Version {
            epoch: 1,
            shard_id: 7,
            sequence: 10,
        };
        assert!(older.storage_suffix() < newer.storage_suffix());
    }

    #[test]
    fn logical_frames_round_trip_and_detect_corruption() {
        let identity = RecordIdentity::new("t", "n", "c", "p", b"key").unwrap();
        let version = Version {
            epoch: 1,
            shard_id: 2,
            sequence: 3,
        };
        let record = RecordEnvelope {
            identity: identity.clone(),
            payload: Some(Payload::Text("hello".into())),
            content_type: "text/plain".into(),
            version,
            created_at_unix_ms: 10,
            updated_at_unix_ms: 11,
            expires_at_unix_ms: None,
            metadata: BTreeMap::new(),
            workflow: WorkflowDescriptor::default(),
            idempotency_key_hash: None,
            dictionary_id: None,
            tombstone: false,
        };
        let encoded = encode_logical(LogicalFrameKind::Record, &record).unwrap();
        assert_eq!(
            decode_logical::<RecordEnvelope>(LogicalFrameKind::Record, &encoded).unwrap(),
            record
        );
        let mut corrupt = encoded;
        *corrupt.last_mut().unwrap() ^= 0x80;
        assert!(decode_logical::<RecordEnvelope>(LogicalFrameKind::Record, &corrupt).is_err());

        let event = ChangeEvent {
            tenant: b"t".to_vec(),
            namespace: b"n".to_vec(),
            collection: b"c".to_vec(),
            partition: b"p".to_vec(),
            version,
            operation: ChangeOperation::Put,
            key: b"key".to_vec(),
            previous_version: None,
            batch_id: [7; 20],
            idempotency_key_hash: None,
            body: ChangeBody::VersionRef { identity, version },
        };
        let encoded = encode_logical(LogicalFrameKind::Change, &event).unwrap();
        assert_eq!(
            decode_logical::<ChangeEvent>(LogicalFrameKind::Change, &encoded).unwrap(),
            event
        );

        let mut catalog = CatalogState::empty("fjall", 4);
        catalog.collections.insert(
            b"collection".to_vec(),
            super::CollectionPolicy {
                retention_mode: EventRetentionMode::VersionRef,
                ..Default::default()
            },
        );
        let encoded = encode_logical(LogicalFrameKind::Catalog, &catalog).unwrap();
        assert_eq!(
            decode_logical::<CatalogState>(LogicalFrameKind::Catalog, &encoded).unwrap(),
            catalog
        );
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: Apache-2.0
