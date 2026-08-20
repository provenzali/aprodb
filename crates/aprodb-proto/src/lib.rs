use std::collections::BTreeMap;

use bytes::{BufMut, Bytes, BytesMut};
use prost::{Enumeration, Message};

pub const PROTOCOL_MAGIC: &[u8] = b"APRODB\0";
pub const PROTOCOL_MAJOR: u32 = 1;
pub const PROTOCOL_MINOR: u32 = 0;
pub const DEFAULT_MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;
pub const MIN_FRAME_BYTES: usize = 1024;
pub const MAX_BATCH_MUTATIONS: usize = 1024;
pub const FRAME_LENGTH_BYTES: usize = 4;
pub const SERVER_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, thiserror::Error)]
pub enum ProtocolError {
    #[error("frame of {actual} bytes exceeds the limit {maximum}")]
    FrameTooLarge { actual: usize, maximum: usize },
    #[error("invalid protobuf message: {0}")]
    Decode(#[from] prost::DecodeError),
    #[error("invalid protocol message: {0}")]
    Invalid(String),
}

pub type Result<T> = std::result::Result<T, ProtocolError>;

#[derive(Clone, PartialEq, Message)]
pub struct ClientHello {
    #[prost(bytes = "vec", tag = "1")]
    pub magic: Vec<u8>,
    #[prost(uint32, tag = "2")]
    pub protocol_major: u32,
    #[prost(uint32, tag = "3")]
    pub protocol_minor: u32,
    #[prost(enumeration = "EndpointRole", tag = "4")]
    pub role: i32,
    #[prost(bytes = "vec", tag = "5")]
    pub auth_token: Vec<u8>,
    #[prost(uint32, tag = "6")]
    pub max_frame_bytes: u32,
}

impl ClientHello {
    #[must_use]
    pub fn new(role: EndpointRole, auth_token: Vec<u8>, max_frame_bytes: usize) -> Self {
        Self {
            magic: PROTOCOL_MAGIC.to_vec(),
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: PROTOCOL_MINOR,
            role: role as i32,
            auth_token,
            max_frame_bytes: max_frame_bytes.min(u32::MAX as usize) as u32,
        }
    }

    pub fn validate(&self) -> Result<EndpointRole> {
        if self.magic != PROTOCOL_MAGIC {
            return Err(ProtocolError::Invalid("invalid handshake magic".into()));
        }
        if self.protocol_major != PROTOCOL_MAJOR {
            return Err(ProtocolError::Invalid(format!(
                "unsupported protocol major version {}",
                self.protocol_major
            )));
        }
        let max_frame = self.max_frame_bytes as usize;
        if !(MIN_FRAME_BYTES..=DEFAULT_MAX_FRAME_BYTES).contains(&max_frame) {
            return Err(ProtocolError::Invalid(format!(
                "max frame {max_frame} out of range"
            )));
        }
        EndpointRole::try_from(self.role)
            .map_err(|_| ProtocolError::Invalid("unknown handshake role".into()))
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct ServerHello {
    #[prost(bool, tag = "1")]
    pub accepted: bool,
    #[prost(uint32, tag = "2")]
    pub protocol_major: u32,
    #[prost(uint32, tag = "3")]
    pub protocol_minor: u32,
    #[prost(string, tag = "4")]
    pub server_version: String,
    #[prost(uint32, tag = "5")]
    pub max_frame_bytes: u32,
    #[prost(enumeration = "ErrorCode", tag = "6")]
    pub error_code: i32,
    #[prost(string, tag = "7")]
    pub error_message: String,
}

impl ServerHello {
    #[must_use]
    pub fn accepted(max_frame_bytes: usize) -> Self {
        Self {
            accepted: true,
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: PROTOCOL_MINOR,
            server_version: SERVER_VERSION.into(),
            max_frame_bytes: max_frame_bytes.min(u32::MAX as usize) as u32,
            error_code: ErrorCode::None as i32,
            error_message: String::new(),
        }
    }

    #[must_use]
    pub fn rejected(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            accepted: false,
            protocol_major: PROTOCOL_MAJOR,
            protocol_minor: PROTOCOL_MINOR,
            server_version: SERVER_VERSION.into(),
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES as u32,
            error_code: code as i32,
            error_message: message.into(),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Enumeration)]
#[repr(i32)]
pub enum EndpointRole {
    Data = 0,
    Admin = 1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Enumeration)]
#[repr(i32)]
pub enum WireDurability {
    Durable = 0,
    Relaxed = 1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Enumeration)]
#[repr(i32)]
pub enum ExpectedMode {
    Any = 0,
    Missing = 1,
    Exact = 2,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Enumeration)]
#[repr(i32)]
pub enum WireSurfaceKind {
    Work = 0,
    Read = 1,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Enumeration)]
#[repr(i32)]
pub enum WireSurfaceFormat {
    AprodbRecords = 0,
    Json = 1,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Enumeration)]
#[repr(i32)]
pub enum WireCompressionMode {
    Raw = 0,
    AdaptiveZstandard = 1,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Enumeration)]
#[repr(i32)]
pub enum WireVectorMetric {
    Dot = 0,
    Cosine = 1,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Enumeration)]
#[repr(i32)]
pub enum WireComputePreference {
    Cpu = 0,
    Accelerator = 1,
    Auto = 2,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Enumeration)]
#[repr(i32)]
pub enum WireComputeExecution {
    Cpu = 0,
    Accelerator = 1,
    CpuFallback = 2,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Enumeration)]
#[repr(i32)]
pub enum ResponseStatus {
    Ok = 0,
    Error = 1,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Enumeration)]
#[repr(i32)]
pub enum ErrorCode {
    None = 0,
    InvalidRequest = 1,
    Unauthenticated = 2,
    Unauthorized = 3,
    NotFound = 4,
    Conflict = 5,
    ResourceLimit = 6,
    Backpressure = 7,
    DeadlineExceeded = 8,
    Storage = 9,
    Corrupt = 10,
    Incompatible = 11,
    Unsupported = 12,
    Internal = 13,
    ChangeLogGap = 14,
}

#[derive(Clone, PartialEq, Message)]
pub struct WireVersion {
    #[prost(uint64, tag = "1")]
    pub epoch: u64,
    #[prost(uint32, tag = "2")]
    pub shard_id: u32,
    #[prost(uint64, tag = "3")]
    pub sequence: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct Key {
    #[prost(bytes = "vec", tag = "1")]
    pub collection: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub partition: Vec<u8>,
    #[prost(bytes = "vec", tag = "3")]
    pub key: Vec<u8>,
}

impl Key {
    pub fn validate(&self) -> Result<()> {
        for (name, value) in [
            ("collection", &self.collection),
            ("partition", &self.partition),
            ("key", &self.key),
        ] {
            if value.is_empty() {
                return Err(ProtocolError::Invalid(format!("{name} is empty")));
            }
            if value.len() > u16::MAX as usize {
                return Err(ProtocolError::Invalid(format!("{name} is too long")));
            }
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct ExpectedVersion {
    #[prost(enumeration = "ExpectedMode", tag = "1")]
    pub mode: i32,
    #[prost(message, optional, tag = "2")]
    pub version: Option<WireVersion>,
}

#[derive(Clone, PartialEq, Message)]
pub struct VectorPayload {
    #[prost(float, repeated, tag = "1")]
    pub values: Vec<f32>,
}

#[derive(Clone, PartialEq, Message)]
pub struct DocumentPayload {
    #[prost(uint32, tag = "1")]
    pub schema_version: u32,
    #[prost(bytes = "vec", tag = "2")]
    pub bytes: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct BlobReference {
    #[prost(bytes = "vec", tag = "1")]
    pub id: Vec<u8>,
    #[prost(uint64, tag = "2")]
    pub logical_bytes: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct TimestampPayload {
    #[prost(sint64, tag = "1")]
    pub unix_ms: i64,
}

#[derive(Clone, PartialEq, Message)]
pub struct WirePayload {
    #[prost(oneof = "wire_payload::Kind", tags = "1, 2, 3, 4, 5, 6, 7, 8, 9")]
    pub kind: Option<wire_payload::Kind>,
}

pub mod wire_payload {
    use super::{BlobReference, DocumentPayload, TimestampPayload, VectorPayload};
    use prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Kind {
        #[prost(bytes, tag = "1")]
        BytesValue(Vec<u8>),
        #[prost(string, tag = "2")]
        TextValue(String),
        #[prost(sint64, tag = "3")]
        IntegerValue(i64),
        #[prost(double, tag = "4")]
        FloatValue(f64),
        #[prost(bool, tag = "5")]
        BooleanValue(bool),
        #[prost(message, tag = "6")]
        Timestamp(TimestampPayload),
        #[prost(message, tag = "7")]
        Vector(VectorPayload),
        #[prost(message, tag = "8")]
        Document(DocumentPayload),
        #[prost(message, tag = "9")]
        BlobRef(BlobReference),
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct PutOperation {
    #[prost(message, optional, tag = "1")]
    pub key: Option<Key>,
    #[prost(message, optional, tag = "2")]
    pub payload: Option<WirePayload>,
    #[prost(string, tag = "3")]
    pub content_type: String,
    #[prost(btree_map = "string, bytes", tag = "4")]
    pub metadata: BTreeMap<String, Vec<u8>>,
    #[prost(uint64, optional, tag = "5")]
    pub expires_at_unix_ms: Option<u64>,
    #[prost(message, optional, tag = "6")]
    pub expected: Option<ExpectedVersion>,
    #[prost(bytes = "vec", tag = "7")]
    pub delta: Vec<u8>,
    #[prost(bytes = "vec", tag = "8")]
    pub idempotency_key_hash: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetOperation {
    #[prost(message, optional, tag = "1")]
    pub key: Option<Key>,
}

#[derive(Clone, PartialEq, Message)]
pub struct DeleteOperation {
    #[prost(message, optional, tag = "1")]
    pub key: Option<Key>,
    #[prost(message, optional, tag = "2")]
    pub expected: Option<ExpectedVersion>,
    #[prost(bytes = "vec", tag = "3")]
    pub delta: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    pub idempotency_key_hash: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct AtomicMutation {
    #[prost(oneof = "atomic_mutation::Kind", tags = "1, 2")]
    pub kind: Option<atomic_mutation::Kind>,
}

pub mod atomic_mutation {
    use super::{DeleteOperation, PutOperation};
    use prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Kind {
        #[prost(message, tag = "1")]
        Put(PutOperation),
        #[prost(message, tag = "2")]
        Delete(DeleteOperation),
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct AtomicBatchOperation {
    #[prost(message, repeated, tag = "1")]
    pub mutations: Vec<AtomicMutation>,
}

macro_rules! empty_message {
    ($($name:ident),+ $(,)?) => {
        $(#[derive(Clone, PartialEq, Message)] pub struct $name {})+
    };
}

empty_message!(
    SyncOperation,
    HealthOperation,
    StatsOperation,
    VerifyOperation,
    CompactOperation,
    ShutdownOperation,
    CacheStatsOperation,
    CompressionStatsOperation,
    ComputeStatsOperation,
);

#[derive(Clone, PartialEq, Message)]
pub struct AuditListOperation {
    #[prost(uint64, optional, tag = "1")]
    pub after_sequence: Option<u64>,
    #[prost(uint64, tag = "2")]
    pub limit: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct BackupOperation {
    #[prost(string, tag = "1")]
    pub name: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct WireCompressionTierPolicy {
    #[prost(enumeration = "WireCompressionMode", tag = "1")]
    pub mode: i32,
    #[prost(sint32, tag = "2")]
    pub zstd_level: i32,
    #[prost(uint64, tag = "3")]
    pub min_input_bytes: u64,
    #[prost(uint64, tag = "4")]
    pub min_savings_bytes: u64,
    #[prost(uint64, optional, tag = "5")]
    pub dictionary_id: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
pub struct WireCompressionPolicy {
    #[prost(message, optional, tag = "1")]
    pub surface: Option<WireCompressionTierPolicy>,
    #[prost(message, optional, tag = "2")]
    pub hot: Option<WireCompressionTierPolicy>,
    #[prost(message, optional, tag = "3")]
    pub warm: Option<WireCompressionTierPolicy>,
    #[prost(message, optional, tag = "4")]
    pub cold: Option<WireCompressionTierPolicy>,
    #[prost(message, optional, tag = "5")]
    pub archive: Option<WireCompressionTierPolicy>,
    #[prost(string, repeated, tag = "6")]
    pub skip_content_type_prefixes: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct CompressionPolicyOperation {
    #[prost(bytes = "vec", tag = "1")]
    pub collection: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ConfigureCompressionOperation {
    #[prost(bytes = "vec", tag = "1")]
    pub collection: Vec<u8>,
    #[prost(message, optional, tag = "2")]
    pub policy: Option<WireCompressionPolicy>,
}

#[derive(Clone, PartialEq, Message)]
pub struct TrainDictionaryOperation {
    #[prost(bytes = "vec", tag = "1")]
    pub collection: Vec<u8>,
    #[prost(string, tag = "2")]
    pub schema: String,
    #[prost(message, repeated, tag = "3")]
    pub training_samples: Vec<WirePayload>,
    #[prost(message, repeated, tag = "4")]
    pub validation_samples: Vec<WirePayload>,
    #[prost(uint64, tag = "5")]
    pub max_dictionary_bytes: u64,
    #[prost(uint64, tag = "6")]
    pub minimum_validation_gain_bytes: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct VectorSearchOperation {
    #[prost(bytes = "vec", tag = "1")]
    pub collection: Vec<u8>,
    #[prost(float, repeated, tag = "2")]
    pub query: Vec<f32>,
    #[prost(enumeration = "WireVectorMetric", tag = "3")]
    pub metric: i32,
    #[prost(uint64, tag = "4")]
    pub limit: u64,
    #[prost(uint64, tag = "5")]
    pub max_scan_records: u64,
    #[prost(enumeration = "WireComputePreference", tag = "6")]
    pub preference: i32,
}

#[derive(Clone, PartialEq, Message)]
pub struct ExplainPlacementOperation {
    #[prost(message, optional, tag = "1")]
    pub key: Option<Key>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ExpireOperation {
    #[prost(uint64, tag = "1")]
    pub limit: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct WorkflowScopeOperation {
    #[prost(bytes = "vec", tag = "1")]
    pub collection: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub partition: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct ClaimOperation {
    #[prost(message, optional, tag = "1")]
    pub scope: Option<WorkflowScopeOperation>,
    #[prost(uint64, tag = "2")]
    pub max_records: u64,
    #[prost(uint64, tag = "3")]
    pub lease_duration_ms: u64,
    #[prost(bytes = "vec", tag = "4")]
    pub idempotency_key_hash: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct LeaseOperation {
    #[prost(message, optional, tag = "1")]
    pub key: Option<Key>,
    #[prost(bytes = "vec", tag = "2")]
    pub lease_id: Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub fencing_token: u64,
    #[prost(uint64, tag = "4")]
    pub extension_ms: u64,
    #[prost(bytes = "vec", tag = "5")]
    pub idempotency_key_hash: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct FailOperation {
    #[prost(message, optional, tag = "1")]
    pub lease: Option<LeaseOperation>,
    #[prost(bool, tag = "2")]
    pub permanent: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct PublishOperation {
    #[prost(message, optional, tag = "1")]
    pub key: Option<Key>,
    #[prost(bytes = "vec", tag = "2")]
    pub idempotency_key_hash: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct SubscribeChangesOperation {
    #[prost(bytes = "vec", tag = "1")]
    pub collection: Vec<u8>,
    #[prost(uint32, tag = "2")]
    pub shard: u32,
    #[prost(uint64, tag = "3")]
    pub after_sequence: u64,
    #[prost(uint64, tag = "4")]
    pub limit: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct WireSurfaceDefinition {
    #[prost(string, tag = "1")]
    pub id: String,
    #[prost(enumeration = "WireSurfaceKind", tag = "2")]
    pub kind: i32,
    #[prost(bytes = "vec", tag = "3")]
    pub source_tenant: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    pub source_namespace: Vec<u8>,
    #[prost(bytes = "vec", tag = "5")]
    pub source_collection: Vec<u8>,
    #[prost(string, repeated, tag = "6")]
    pub workflow_states: Vec<String>,
    #[prost(enumeration = "WireSurfaceFormat", tag = "7")]
    pub format: i32,
    #[prost(uint64, tag = "8")]
    pub max_records: u64,
    #[prost(uint64, tag = "9")]
    pub max_bytes: u64,
    #[prost(uint64, tag = "10")]
    pub retained_generations: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct CreateSurfaceOperation {
    #[prost(message, optional, tag = "1")]
    pub definition: Option<WireSurfaceDefinition>,
}

#[derive(Clone, PartialEq, Message)]
pub struct BuildSurfaceOperation {
    #[prost(string, tag = "1")]
    pub projection_id: String,
    #[prost(uint64, tag = "2")]
    pub max_events: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct GetSurfaceOperation {
    #[prost(string, tag = "1")]
    pub projection_id: String,
}

#[derive(Clone, PartialEq, Message)]
pub struct Request {
    #[prost(uint64, tag = "1")]
    pub request_id: u64,
    #[prost(uint64, tag = "2")]
    pub deadline_unix_ms: u64,
    #[prost(bytes = "vec", tag = "3")]
    pub tenant: Vec<u8>,
    #[prost(bytes = "vec", tag = "4")]
    pub namespace: Vec<u8>,
    #[prost(enumeration = "WireDurability", tag = "5")]
    pub durability: i32,
    #[prost(
        oneof = "request::Operation",
        tags = "10, 11, 12, 13, 14, 20, 21, 22, 23, 24, 25, 26, 27, 30, 31, 32, 33, 34, 35, 36, 37, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50"
    )]
    pub operation: Option<request::Operation>,
}

impl Request {
    pub fn validate(&self) -> Result<()> {
        if self.request_id == 0 {
            return Err(ProtocolError::Invalid("request_id is zero".into()));
        }
        WireDurability::try_from(self.durability)
            .map_err(|_| ProtocolError::Invalid("unknown durability".into()))?;
        let operation = self
            .operation
            .as_ref()
            .ok_or_else(|| ProtocolError::Invalid("operation missing".into()))?;
        if operation.is_data() || operation.requires_identity() {
            if self.tenant.is_empty() || self.namespace.is_empty() {
                return Err(ProtocolError::Invalid(
                    "tenant or namespace is empty".into(),
                ));
            }
            if self.tenant.len() > u16::MAX as usize || self.namespace.len() > u16::MAX as usize {
                return Err(ProtocolError::Invalid(
                    "tenant or namespace too long".into(),
                ));
            }
        }
        operation.validate()
    }
}

pub mod request {
    use super::{
        AtomicBatchOperation, AuditListOperation, BackupOperation, BuildSurfaceOperation,
        CacheStatsOperation, ClaimOperation, CompactOperation, CompressionPolicyOperation,
        CompressionStatsOperation, ComputeStatsOperation, ConfigureCompressionOperation,
        CreateSurfaceOperation, DeleteOperation, ExpireOperation, ExplainPlacementOperation,
        FailOperation, GetOperation, GetSurfaceOperation, HealthOperation, LeaseOperation,
        MAX_BATCH_MUTATIONS, ProtocolError, PublishOperation, PutOperation, Result,
        ShutdownOperation, StatsOperation, SubscribeChangesOperation, SyncOperation,
        TrainDictionaryOperation, VectorSearchOperation, VerifyOperation,
    };
    use prost::Oneof;

    #[derive(Clone, PartialEq, Oneof)]
    pub enum Operation {
        #[prost(message, tag = "10")]
        Put(PutOperation),
        #[prost(message, tag = "11")]
        Get(GetOperation),
        #[prost(message, tag = "12")]
        Delete(DeleteOperation),
        #[prost(message, tag = "13")]
        AtomicBatch(AtomicBatchOperation),
        #[prost(message, tag = "14")]
        Sync(SyncOperation),
        #[prost(message, tag = "20")]
        Health(HealthOperation),
        #[prost(message, tag = "21")]
        Stats(StatsOperation),
        #[prost(message, tag = "22")]
        Verify(VerifyOperation),
        #[prost(message, tag = "23")]
        Compact(CompactOperation),
        #[prost(message, tag = "24")]
        Shutdown(ShutdownOperation),
        #[prost(message, tag = "25")]
        ExplainPlacement(ExplainPlacementOperation),
        #[prost(message, tag = "26")]
        CacheStats(CacheStatsOperation),
        #[prost(message, tag = "27")]
        Expire(ExpireOperation),
        #[prost(message, tag = "30")]
        Append(PutOperation),
        #[prost(message, tag = "31")]
        Claim(ClaimOperation),
        #[prost(message, tag = "32")]
        Heartbeat(LeaseOperation),
        #[prost(message, tag = "33")]
        Complete(LeaseOperation),
        #[prost(message, tag = "34")]
        Fail(FailOperation),
        #[prost(message, tag = "35")]
        Publish(PublishOperation),
        #[prost(message, tag = "36")]
        SubscribeChanges(SubscribeChangesOperation),
        #[prost(message, tag = "37")]
        GetSurface(GetSurfaceOperation),
        #[prost(message, tag = "40")]
        CreateSurface(CreateSurfaceOperation),
        #[prost(message, tag = "41")]
        BuildSurface(BuildSurfaceOperation),
        #[prost(message, tag = "42")]
        RebuildSurface(BuildSurfaceOperation),
        #[prost(message, tag = "43")]
        CompressionStats(CompressionStatsOperation),
        #[prost(message, tag = "44")]
        CompressionPolicy(CompressionPolicyOperation),
        #[prost(message, tag = "45")]
        ConfigureCompression(ConfigureCompressionOperation),
        #[prost(message, tag = "46")]
        TrainDictionary(TrainDictionaryOperation),
        #[prost(message, tag = "47")]
        ComputeStats(ComputeStatsOperation),
        #[prost(message, tag = "48")]
        AuditList(AuditListOperation),
        #[prost(message, tag = "49")]
        Backup(BackupOperation),
        #[prost(message, tag = "50")]
        VectorSearch(VectorSearchOperation),
    }

    impl Operation {
        #[must_use]
        pub const fn is_data(&self) -> bool {
            matches!(
                self,
                Self::Put(_)
                    | Self::Get(_)
                    | Self::Delete(_)
                    | Self::AtomicBatch(_)
                    | Self::Sync(_)
                    | Self::Append(_)
                    | Self::Claim(_)
                    | Self::Heartbeat(_)
                    | Self::Complete(_)
                    | Self::Fail(_)
                    | Self::Publish(_)
                    | Self::SubscribeChanges(_)
                    | Self::GetSurface(_)
                    | Self::VectorSearch(_)
            )
        }

        #[must_use]
        pub const fn requires_identity(&self) -> bool {
            matches!(
                self,
                Self::ExplainPlacement(_)
                    | Self::CompressionPolicy(_)
                    | Self::ConfigureCompression(_)
                    | Self::TrainDictionary(_)
            )
        }

        pub fn validate(&self) -> Result<()> {
            match self {
                Self::Put(operation) => {
                    operation
                        .key
                        .as_ref()
                        .ok_or_else(|| ProtocolError::Invalid("Put operation key missing".into()))?
                        .validate()?;
                    if operation.payload.is_none() {
                        return Err(ProtocolError::Invalid(
                            "Put operation payload missing".into(),
                        ));
                    }
                    validate_idempotency_hash(&operation.idempotency_key_hash)?;
                }
                Self::Get(operation) => operation
                    .key
                    .as_ref()
                    .ok_or_else(|| ProtocolError::Invalid("Get operation key missing".into()))?
                    .validate()?,
                Self::Delete(operation) => {
                    operation
                        .key
                        .as_ref()
                        .ok_or_else(|| {
                            ProtocolError::Invalid("Delete operation key missing".into())
                        })?
                        .validate()?;
                    validate_idempotency_hash(&operation.idempotency_key_hash)?;
                }
                Self::AtomicBatch(operation) => {
                    if operation.mutations.is_empty()
                        || operation.mutations.len() > MAX_BATCH_MUTATIONS
                    {
                        return Err(ProtocolError::Invalid(format!(
                            "AtomicBatch with {} mutations",
                            operation.mutations.len()
                        )));
                    }
                    for mutation in &operation.mutations {
                        match mutation.kind.as_ref() {
                            Some(super::atomic_mutation::Kind::Put(put)) => {
                                Self::Put(put.clone()).validate()?;
                            }
                            Some(super::atomic_mutation::Kind::Delete(delete)) => {
                                Self::Delete(delete.clone()).validate()?;
                            }
                            None => {
                                return Err(ProtocolError::Invalid("empty batch mutation".into()));
                            }
                        }
                    }
                }
                Self::Sync(_)
                | Self::Health(_)
                | Self::Stats(_)
                | Self::Verify(_)
                | Self::Compact(_)
                | Self::Shutdown(_)
                | Self::CacheStats(_)
                | Self::CompressionStats(_) => {}
                Self::ComputeStats(_) => {}
                Self::AuditList(operation) => {
                    if operation.limit == 0 {
                        return Err(ProtocolError::Invalid("AuditList limit is zero".into()));
                    }
                }
                Self::Backup(operation) => {
                    if operation.name.is_empty()
                        || operation.name.len() > 128
                        || !operation.name.bytes().all(|byte| {
                            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.')
                        })
                    {
                        return Err(ProtocolError::Invalid("unsafe backup name".into()));
                    }
                }
                Self::ExplainPlacement(operation) => operation
                    .key
                    .as_ref()
                    .ok_or_else(|| {
                        ProtocolError::Invalid("ExplainPlacement operation key missing".into())
                    })?
                    .validate()?,
                Self::Expire(operation) => {
                    if operation.limit == 0 {
                        return Err(ProtocolError::Invalid("Expire limit is zero".into()));
                    }
                }
                Self::Append(operation) => Self::Put(operation.clone()).validate()?,
                Self::Claim(operation) => {
                    let scope = operation
                        .scope
                        .as_ref()
                        .ok_or_else(|| ProtocolError::Invalid("Claim scope missing".into()))?;
                    if scope.collection.is_empty()
                        || scope.partition.is_empty()
                        || operation.max_records == 0
                        || operation.lease_duration_ms == 0
                    {
                        return Err(ProtocolError::Invalid("invalid Claim".into()));
                    }
                    validate_idempotency_hash(&operation.idempotency_key_hash)?;
                }
                Self::Heartbeat(operation) => validate_lease(operation, true)?,
                Self::Complete(operation) => validate_lease(operation, false)?,
                Self::Fail(operation) => validate_lease(
                    operation.lease.as_ref().ok_or_else(|| {
                        ProtocolError::Invalid("missing lease in Fail operation".into())
                    })?,
                    false,
                )?,
                Self::Publish(operation) => {
                    operation
                        .key
                        .as_ref()
                        .ok_or_else(|| {
                            ProtocolError::Invalid("Publish operation key missing".into())
                        })?
                        .validate()?;
                    validate_idempotency_hash(&operation.idempotency_key_hash)?;
                }
                Self::SubscribeChanges(operation) => {
                    if operation.collection.is_empty() || operation.limit == 0 {
                        return Err(ProtocolError::Invalid(
                            "SubscribeChanges collection or limit invalid".into(),
                        ));
                    }
                }
                Self::GetSurface(operation) => validate_projection_id(&operation.projection_id)?,
                Self::CreateSurface(operation) => {
                    let definition = operation.definition.as_ref().ok_or_else(|| {
                        ProtocolError::Invalid("surface definition missing".into())
                    })?;
                    validate_projection_id(&definition.id)?;
                    if definition.source_tenant.is_empty()
                        || definition.source_namespace.is_empty()
                        || definition.source_collection.is_empty()
                        || definition.workflow_states.is_empty()
                        || definition.max_records == 0
                        || definition.max_bytes == 0
                        || definition.retained_generations == 0
                    {
                        return Err(ProtocolError::Invalid(
                            "incomplete surface definition".into(),
                        ));
                    }
                    super::WireSurfaceKind::try_from(definition.kind)
                        .map_err(|_| ProtocolError::Invalid("unknown surface kind".into()))?;
                    super::WireSurfaceFormat::try_from(definition.format)
                        .map_err(|_| ProtocolError::Invalid("unknown surface format".into()))?;
                }
                Self::BuildSurface(operation) | Self::RebuildSurface(operation) => {
                    validate_projection_id(&operation.projection_id)?;
                    if matches!(self, Self::BuildSurface(_)) && operation.max_events == 0 {
                        return Err(ProtocolError::Invalid("surface max_events is zero".into()));
                    }
                }
                Self::CompressionPolicy(operation) => {
                    validate_collection(&operation.collection)?;
                }
                Self::ConfigureCompression(operation) => {
                    validate_collection(&operation.collection)?;
                    let policy = operation.policy.as_ref().ok_or_else(|| {
                        ProtocolError::Invalid("missing compression policy".into())
                    })?;
                    validate_compression_policy(policy)?;
                }
                Self::TrainDictionary(operation) => {
                    validate_collection(&operation.collection)?;
                    if operation.schema.is_empty()
                        || operation.training_samples.is_empty()
                        || operation.validation_samples.is_empty()
                        || operation.max_dictionary_bytes == 0
                    {
                        return Err(ProtocolError::Invalid(
                            "incomplete train dictionary request".into(),
                        ));
                    }
                }
                Self::VectorSearch(operation) => {
                    validate_collection(&operation.collection)?;
                    if operation.query.is_empty()
                        || operation.query.iter().any(|value| !value.is_finite())
                        || operation.limit == 0
                        || operation.max_scan_records == 0
                    {
                        return Err(ProtocolError::Invalid(
                            "invalid VectorSearch request".into(),
                        ));
                    }
                    super::WireVectorMetric::try_from(operation.metric)
                        .map_err(|_| ProtocolError::Invalid("unknown vector metric".into()))?;
                    super::WireComputePreference::try_from(operation.preference)
                        .map_err(|_| ProtocolError::Invalid("unknown compute preference".into()))?;
                }
            }
            Ok(())
        }
    }

    fn validate_idempotency_hash(hash: &[u8]) -> Result<()> {
        if hash.is_empty() || hash.len() == 32 {
            Ok(())
        } else {
            Err(ProtocolError::Invalid(
                "idempotency hash must be empty or exactly 32 bytes".into(),
            ))
        }
    }

    fn validate_lease(operation: &LeaseOperation, extension_required: bool) -> Result<()> {
        operation
            .key
            .as_ref()
            .ok_or_else(|| ProtocolError::Invalid("lease operation key missing".into()))?
            .validate()?;
        if operation.lease_id.len() != 16
            || operation.fencing_token == 0
            || (extension_required && operation.extension_ms == 0)
        {
            return Err(ProtocolError::Invalid("invalid lease proof".into()));
        }
        validate_idempotency_hash(&operation.idempotency_key_hash)
    }

    fn validate_projection_id(id: &str) -> Result<()> {
        if id.is_empty() || id.len() > 128 {
            Err(ProtocolError::Invalid("invalid projection id".into()))
        } else {
            Ok(())
        }
    }

    fn validate_collection(collection: &[u8]) -> Result<()> {
        if collection.is_empty() || collection.len() > u16::MAX as usize {
            Err(ProtocolError::Invalid("invalid collection".into()))
        } else {
            Ok(())
        }
    }

    fn validate_compression_policy(policy: &super::WireCompressionPolicy) -> Result<()> {
        for tier in [
            policy.surface.as_ref(),
            policy.hot.as_ref(),
            policy.warm.as_ref(),
            policy.cold.as_ref(),
            policy.archive.as_ref(),
        ] {
            let tier = tier
                .ok_or_else(|| ProtocolError::Invalid("missing tier compression policy".into()))?;
            super::WireCompressionMode::try_from(tier.mode)
                .map_err(|_| ProtocolError::Invalid("unknown compression mode".into()))?;
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq, Message)]
pub struct WireRecord {
    #[prost(bytes = "vec", tag = "1")]
    pub tenant: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub namespace: Vec<u8>,
    #[prost(message, optional, tag = "3")]
    pub key: Option<Key>,
    #[prost(message, optional, tag = "4")]
    pub payload: Option<WirePayload>,
    #[prost(string, tag = "5")]
    pub content_type: String,
    #[prost(message, optional, tag = "6")]
    pub version: Option<WireVersion>,
    #[prost(uint64, tag = "7")]
    pub created_at_unix_ms: u64,
    #[prost(uint64, tag = "8")]
    pub updated_at_unix_ms: u64,
    #[prost(uint64, optional, tag = "9")]
    pub expires_at_unix_ms: Option<u64>,
    #[prost(btree_map = "string, bytes", tag = "10")]
    pub metadata: BTreeMap<String, Vec<u8>>,
    #[prost(bool, tag = "11")]
    pub tombstone: bool,
    #[prost(message, optional, tag = "12")]
    pub workflow: Option<WireWorkflow>,
    #[prost(bytes = "vec", tag = "13")]
    pub idempotency_key_hash: Vec<u8>,
    #[prost(uint64, optional, tag = "14")]
    pub dictionary_id: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
pub struct WireWorkflow {
    #[prost(string, tag = "1")]
    pub state: String,
    #[prost(uint32, tag = "2")]
    pub attempt: u32,
    #[prost(bytes = "vec", tag = "3")]
    pub lease_id: Vec<u8>,
    #[prost(uint64, tag = "4")]
    pub fencing_token: u64,
    #[prost(uint64, optional, tag = "5")]
    pub lease_deadline_unix_ms: Option<u64>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Receipt {
    #[prost(message, optional, tag = "1")]
    pub version: Option<WireVersion>,
    #[prost(enumeration = "WireDurability", tag = "2")]
    pub durability: i32,
    #[prost(uint64, tag = "3")]
    pub durable_watermark: u64,
    #[prost(bytes = "vec", tag = "4")]
    pub batch_id: Vec<u8>,
}

#[derive(Clone, PartialEq, Message)]
pub struct Stats {
    #[prost(uint64, tag = "1")]
    pub disk_bytes: u64,
    #[prost(uint64, tag = "2")]
    pub write_buffer_bytes: u64,
    #[prost(uint64, tag = "3")]
    pub journal_fragments: u64,
    #[prost(uint64, tag = "4")]
    pub table_count: u64,
    #[prost(uint64, tag = "5")]
    pub completed_compactions: u64,
    #[prost(uint64, tag = "10")]
    pub active_connections: u64,
    #[prost(uint64, tag = "11")]
    pub inflight_requests: u64,
    #[prost(uint64, tag = "12")]
    pub total_requests: u64,
    #[prost(uint64, tag = "13")]
    pub rejected_requests: u64,
    #[prost(uint64, tag = "14")]
    pub auth_failures: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct WirePlacement {
    #[prost(message, optional, tag = "1")]
    pub canonical_version: Option<WireVersion>,
    #[prost(uint32, tag = "2")]
    pub radial_score_millis: u32,
    #[prost(uint32, tag = "3")]
    pub freshness_millis: u32,
    #[prost(uint32, tag = "4")]
    pub urgency_millis: u32,
    #[prost(string, tag = "5")]
    pub current_layer: String,
    #[prost(string, tag = "6")]
    pub recommended_layer: String,
    #[prost(string, tag = "7")]
    pub storage_class: String,
    #[prost(bool, tag = "8")]
    pub pinned: bool,
    #[prost(bool, tag = "9")]
    pub object_cache_resident: bool,
    #[prost(bool, tag = "10")]
    pub physical_tiering_supported: bool,
    #[prost(string, repeated, tag = "11")]
    pub reasons: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct WireCacheMetrics {
    #[prost(uint64, tag = "1")]
    pub budget_bytes: u64,
    #[prost(uint64, tag = "2")]
    pub resident_bytes: u64,
    #[prost(uint64, tag = "3")]
    pub entries: u64,
    #[prost(uint64, tag = "4")]
    pub hits: u64,
    #[prost(uint64, tag = "5")]
    pub misses: u64,
    #[prost(uint64, tag = "6")]
    pub admissions: u64,
    #[prost(uint64, tag = "7")]
    pub rejections: u64,
    #[prost(uint64, tag = "8")]
    pub evictions: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct WireCacheStats {
    #[prost(message, optional, tag = "1")]
    pub metadata: Option<WireCacheMetrics>,
    #[prost(message, optional, tag = "2")]
    pub objects: Option<WireCacheMetrics>,
    #[prost(message, optional, tag = "3")]
    pub negative: Option<WireCacheMetrics>,
    #[prost(message, optional, tag = "4")]
    pub compressed: Option<WireCacheMetrics>,
}

#[derive(Clone, PartialEq, Message)]
pub struct WireCompressionStats {
    #[prost(uint64, tag = "1")]
    pub logical_bytes: u64,
    #[prost(uint64, tag = "2")]
    pub stored_bytes: u64,
    #[prost(uint64, tag = "3")]
    pub raw_records: u64,
    #[prost(uint64, tag = "4")]
    pub zstd_records: u64,
    #[prost(uint64, tag = "5")]
    pub dictionary_records: u64,
    #[prost(uint64, tag = "6")]
    pub incompressible_fallbacks: u64,
    #[prost(uint64, tag = "7")]
    pub content_type_skips: u64,
    #[prost(uint64, tag = "8")]
    pub compress_micros: u64,
    #[prost(uint64, tag = "9")]
    pub decompress_micros: u64,
    #[prost(uint64, tag = "10")]
    pub codec_failures: u64,
    #[prost(uint64, tag = "11")]
    pub channels: u64,
    #[prost(uint64, tag = "12")]
    pub scratch_budget_bytes: u64,
    #[prost(uint64, tag = "13")]
    pub scratch_in_use_bytes: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct WireCompressionDictionary {
    #[prost(uint64, tag = "1")]
    pub id: u64,
    #[prost(string, tag = "2")]
    pub schema: String,
    #[prost(uint64, tag = "3")]
    pub bytes: u64,
    #[prost(uint32, tag = "4")]
    pub checksum: u32,
    #[prost(uint64, tag = "5")]
    pub created_at_unix_ms: u64,
    #[prost(uint64, tag = "6")]
    pub validation_raw_bytes: u64,
    #[prost(uint64, tag = "7")]
    pub validation_without_dictionary_bytes: u64,
    #[prost(uint64, tag = "8")]
    pub validation_with_dictionary_bytes: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct WireCostEstimate {
    #[prost(uint64, tag = "1")]
    pub transfer_in_micros: u64,
    #[prost(uint64, tag = "2")]
    pub queue_wait_micros: u64,
    #[prost(uint64, tag = "3")]
    pub launch_micros: u64,
    #[prost(uint64, tag = "4")]
    pub accelerator_compute_micros: u64,
    #[prost(uint64, tag = "5")]
    pub transfer_out_micros: u64,
    #[prost(uint64, tag = "6")]
    pub synchronization_micros: u64,
    #[prost(uint64, tag = "7")]
    pub risk_margin_micros: u64,
    #[prost(uint64, tag = "8")]
    pub accelerator_total_micros: u64,
    #[prost(uint64, tag = "9")]
    pub cpu_compute_micros: u64,
    #[prost(bool, tag = "10")]
    pub vram_cache_hit: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct WireVectorHit {
    #[prost(bytes = "vec", tag = "1")]
    pub partition: Vec<u8>,
    #[prost(bytes = "vec", tag = "2")]
    pub key: Vec<u8>,
    #[prost(message, optional, tag = "3")]
    pub version: Option<WireVersion>,
    #[prost(float, tag = "4")]
    pub score: f32,
}

#[derive(Clone, PartialEq, Message)]
pub struct WireVectorSearchResult {
    #[prost(message, repeated, tag = "1")]
    pub hits: Vec<WireVectorHit>,
    #[prost(uint64, tag = "2")]
    pub scanned_records: u64,
    #[prost(uint64, tag = "3")]
    pub vector_candidates: u64,
    #[prost(enumeration = "WireComputeExecution", tag = "4")]
    pub execution: i32,
    #[prost(string, optional, tag = "5")]
    pub accelerator: Option<String>,
    #[prost(message, optional, tag = "6")]
    pub estimate: Option<WireCostEstimate>,
    #[prost(string, optional, tag = "7")]
    pub fallback_reason: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct WireComputeStats {
    #[prost(uint64, tag = "1")]
    pub requests: u64,
    #[prost(uint64, tag = "2")]
    pub cpu_runs: u64,
    #[prost(uint64, tag = "3")]
    pub accelerator_runs: u64,
    #[prost(uint64, tag = "4")]
    pub cpu_fallbacks: u64,
    #[prost(uint64, tag = "5")]
    pub queue_rejections: u64,
    #[prost(uint64, tag = "6")]
    pub accelerator_failures: u64,
    #[prost(uint64, tag = "7")]
    pub request_timeouts: u64,
    #[prost(uint64, tag = "8")]
    pub circuit_open_rejections: u64,
    #[prost(uint64, tag = "9")]
    pub micro_batches: u64,
    #[prost(uint64, tag = "10")]
    pub micro_batched_requests: u64,
    #[prost(uint64, tag = "11")]
    pub inflight_bytes: u64,
    #[prost(uint64, tag = "12")]
    pub peak_inflight_bytes: u64,
    #[prost(string, optional, tag = "13")]
    pub accelerator_name: Option<String>,
    #[prost(uint64, tag = "20")]
    pub vram_budget_bytes: u64,
    #[prost(uint64, tag = "21")]
    pub vram_resident_bytes: u64,
    #[prost(uint64, tag = "22")]
    pub vram_entries: u64,
    #[prost(uint64, tag = "23")]
    pub vram_hits: u64,
    #[prost(uint64, tag = "24")]
    pub vram_misses: u64,
    #[prost(uint64, tag = "25")]
    pub vram_evictions: u64,
    #[prost(uint64, tag = "26")]
    pub upload_bytes: u64,
    #[prost(uint64, tag = "27")]
    pub readback_bytes: u64,
    #[prost(uint64, tag = "28")]
    pub transfer_micros: u64,
    #[prost(uint64, tag = "29")]
    pub kernel_micros: u64,
    #[prost(uint64, tag = "30")]
    pub device_resets: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct WireAuditEvent {
    #[prost(uint64, tag = "1")]
    pub sequence: u64,
    #[prost(bytes = "vec", tag = "2")]
    pub event_id: Vec<u8>,
    #[prost(uint64, tag = "3")]
    pub at_unix_ms: u64,
    #[prost(uint64, tag = "4")]
    pub request_id: u64,
    #[prost(string, tag = "5")]
    pub principal: String,
    #[prost(string, tag = "6")]
    pub operation: String,
    #[prost(string, tag = "7")]
    pub outcome: String,
    #[prost(bytes = "vec", optional, tag = "8")]
    pub target_hash: Option<Vec<u8>>,
    #[prost(string, optional, tag = "9")]
    pub error_class: Option<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct WireBackupInfo {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(uint64, tag = "2")]
    pub catalog_generation: u64,
    #[prost(uint64, tag = "3")]
    pub files: u64,
    #[prost(uint64, tag = "4")]
    pub bytes: u64,
    #[prost(uint64, tag = "5")]
    pub logical_bytes: u64,
    #[prost(bool, tag = "6")]
    pub encrypted: bool,
}

#[derive(Clone, PartialEq, Message)]
pub struct ExpirationStats {
    #[prost(uint64, tag = "1")]
    pub scanned: u64,
    #[prost(uint64, tag = "2")]
    pub expired: u64,
    #[prost(uint64, tag = "3")]
    pub stale_entries: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct WireSurfaceGeneration {
    #[prost(string, tag = "1")]
    pub projection_id: String,
    #[prost(uint64, tag = "2")]
    pub generation: u64,
    #[prost(btree_map = "uint32, uint64", tag = "3")]
    pub source_watermarks: BTreeMap<u32, u64>,
    #[prost(enumeration = "WireSurfaceFormat", tag = "4")]
    pub format: i32,
    #[prost(uint64, tag = "5")]
    pub record_count: u64,
    #[prost(bytes = "vec", tag = "6")]
    pub serialized: Vec<u8>,
    #[prost(uint64, tag = "7")]
    pub created_at_unix_ms: u64,
    #[prost(btree_map = "uint32, uint64", tag = "8")]
    pub stale_by_sequences: BTreeMap<u32, u64>,
    #[prost(bool, tag = "9")]
    pub complete: bool,
    #[prost(string, repeated, tag = "10")]
    pub errors: Vec<String>,
}

#[derive(Clone, PartialEq, Message)]
pub struct WireSurfaceBuild {
    #[prost(string, tag = "1")]
    pub projection_id: String,
    #[prost(uint64, tag = "2")]
    pub generation: u64,
    #[prost(uint64, tag = "3")]
    pub events_applied: u64,
    #[prost(btree_map = "uint32, uint64", tag = "4")]
    pub source_watermarks: BTreeMap<u32, u64>,
    #[prost(uint64, tag = "5")]
    pub record_count: u64,
    #[prost(uint64, tag = "6")]
    pub serialized_bytes: u64,
}

#[derive(Clone, PartialEq, Message)]
pub struct Response {
    #[prost(uint64, tag = "1")]
    pub request_id: u64,
    #[prost(enumeration = "ResponseStatus", tag = "2")]
    pub status: i32,
    #[prost(enumeration = "ErrorCode", tag = "3")]
    pub error_code: i32,
    #[prost(string, tag = "4")]
    pub error_message: String,
    #[prost(string, tag = "5")]
    pub server_version: String,
    #[prost(message, optional, tag = "6")]
    pub record: Option<WireRecord>,
    #[prost(message, repeated, tag = "7")]
    pub receipts: Vec<Receipt>,
    #[prost(message, optional, tag = "8")]
    pub stats: Option<Stats>,
    #[prost(bool, tag = "9")]
    pub healthy: bool,
    #[prost(uint64, tag = "10")]
    pub retry_after_ms: u64,
    #[prost(message, optional, tag = "11")]
    pub placement: Option<WirePlacement>,
    #[prost(message, optional, tag = "12")]
    pub cache_stats: Option<WireCacheStats>,
    #[prost(message, optional, tag = "13")]
    pub expiration: Option<ExpirationStats>,
    #[prost(bytes = "vec", repeated, tag = "14")]
    pub change_events: Vec<Vec<u8>>,
    #[prost(uint64, tag = "15")]
    pub change_watermark: u64,
    #[prost(message, optional, tag = "16")]
    pub surface: Option<WireSurfaceGeneration>,
    #[prost(message, optional, tag = "17")]
    pub surface_build: Option<WireSurfaceBuild>,
    #[prost(message, repeated, tag = "18")]
    pub claimed_records: Vec<WireRecord>,
    #[prost(uint64, tag = "19")]
    pub server_time_unix_ms: u64,
    #[prost(message, optional, tag = "20")]
    pub compression_stats: Option<WireCompressionStats>,
    #[prost(message, optional, tag = "21")]
    pub compression_policy: Option<WireCompressionPolicy>,
    #[prost(message, optional, tag = "22")]
    pub compression_dictionary: Option<WireCompressionDictionary>,
    #[prost(message, optional, tag = "23")]
    pub vector_search: Option<WireVectorSearchResult>,
    #[prost(message, optional, tag = "24")]
    pub compute_stats: Option<WireComputeStats>,
    #[prost(message, repeated, tag = "25")]
    pub audit_events: Vec<WireAuditEvent>,
    #[prost(uint64, optional, tag = "26")]
    pub audit_next_sequence: Option<u64>,
    #[prost(message, optional, tag = "27")]
    pub backup: Option<WireBackupInfo>,
}

impl Response {
    #[must_use]
    pub fn ok(request_id: u64) -> Self {
        Self {
            request_id,
            status: ResponseStatus::Ok as i32,
            error_code: ErrorCode::None as i32,
            error_message: String::new(),
            server_version: SERVER_VERSION.into(),
            record: None,
            receipts: Vec::new(),
            stats: None,
            healthy: true,
            retry_after_ms: 0,
            placement: None,
            cache_stats: None,
            expiration: None,
            change_events: Vec::new(),
            change_watermark: 0,
            surface: None,
            surface_build: None,
            claimed_records: Vec::new(),
            server_time_unix_ms: 0,
            compression_stats: None,
            compression_policy: None,
            compression_dictionary: None,
            vector_search: None,
            compute_stats: None,
            audit_events: Vec::new(),
            audit_next_sequence: None,
            backup: None,
        }
    }

    #[must_use]
    pub fn error(request_id: u64, code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            request_id,
            status: ResponseStatus::Error as i32,
            error_code: code as i32,
            error_message: message.into(),
            server_version: SERVER_VERSION.into(),
            record: None,
            receipts: Vec::new(),
            stats: None,
            healthy: false,
            retry_after_ms: 0,
            placement: None,
            cache_stats: None,
            expiration: None,
            change_events: Vec::new(),
            change_watermark: 0,
            surface: None,
            surface_build: None,
            claimed_records: Vec::new(),
            server_time_unix_ms: 0,
            compression_stats: None,
            compression_policy: None,
            compression_dictionary: None,
            vector_search: None,
            compute_stats: None,
            audit_events: Vec::new(),
            audit_next_sequence: None,
            backup: None,
        }
    }

    #[must_use]
    pub fn backpressure(request_id: u64, message: impl Into<String>, retry_after_ms: u64) -> Self {
        let mut response = Self::error(request_id, ErrorCode::Backpressure, message);
        response.retry_after_ms = retry_after_ms.max(1);
        response
    }

    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.status == ResponseStatus::Ok as i32
    }
}

impl From<aprodb_types::Version> for WireVersion {
    fn from(version: aprodb_types::Version) -> Self {
        Self {
            epoch: version.epoch,
            shard_id: version.shard_id,
            sequence: version.sequence,
        }
    }
}

impl From<WireVersion> for aprodb_types::Version {
    fn from(version: WireVersion) -> Self {
        Self {
            epoch: version.epoch,
            shard_id: version.shard_id,
            sequence: version.sequence,
        }
    }
}

impl From<aprodb_types::Durability> for WireDurability {
    fn from(durability: aprodb_types::Durability) -> Self {
        match durability {
            aprodb_types::Durability::Durable => Self::Durable,
            aprodb_types::Durability::Relaxed => Self::Relaxed,
        }
    }
}

impl From<WireDurability> for aprodb_types::Durability {
    fn from(durability: WireDurability) -> Self {
        match durability {
            WireDurability::Durable => Self::Durable,
            WireDurability::Relaxed => Self::Relaxed,
        }
    }
}

impl From<&aprodb_types::Payload> for WirePayload {
    fn from(payload: &aprodb_types::Payload) -> Self {
        use aprodb_types::Payload;
        let kind = match payload {
            Payload::Bytes(bytes) => wire_payload::Kind::BytesValue(bytes.clone()),
            Payload::Text(text) => wire_payload::Kind::TextValue(text.clone()),
            Payload::Integer(value) => wire_payload::Kind::IntegerValue(*value),
            Payload::Float(value) => wire_payload::Kind::FloatValue(*value),
            Payload::Boolean(value) => wire_payload::Kind::BooleanValue(*value),
            Payload::Timestamp(unix_ms) => {
                wire_payload::Kind::Timestamp(TimestampPayload { unix_ms: *unix_ms })
            }
            Payload::Vector(values) => wire_payload::Kind::Vector(VectorPayload {
                values: values.clone(),
            }),
            Payload::Document {
                schema_version,
                bytes,
            } => wire_payload::Kind::Document(DocumentPayload {
                schema_version: *schema_version,
                bytes: bytes.clone(),
            }),
            Payload::BlobRef { id, logical_bytes } => wire_payload::Kind::BlobRef(BlobReference {
                id: id.clone(),
                logical_bytes: *logical_bytes,
            }),
        };
        Self { kind: Some(kind) }
    }
}

impl TryFrom<WirePayload> for aprodb_types::Payload {
    type Error = ProtocolError;

    fn try_from(payload: WirePayload) -> Result<Self> {
        let payload = match payload
            .kind
            .ok_or_else(|| ProtocolError::Invalid("payload wire vuoto".into()))?
        {
            wire_payload::Kind::BytesValue(bytes) => Self::Bytes(bytes),
            wire_payload::Kind::TextValue(text) => Self::Text(text),
            wire_payload::Kind::IntegerValue(value) => Self::Integer(value),
            wire_payload::Kind::FloatValue(value) => Self::Float(value),
            wire_payload::Kind::BooleanValue(value) => Self::Boolean(value),
            wire_payload::Kind::Timestamp(value) => Self::Timestamp(value.unix_ms),
            wire_payload::Kind::Vector(value) => Self::Vector(value.values),
            wire_payload::Kind::Document(value) => Self::Document {
                schema_version: value.schema_version,
                bytes: value.bytes,
            },
            wire_payload::Kind::BlobRef(value) => Self::BlobRef {
                id: value.id,
                logical_bytes: value.logical_bytes,
            },
        };
        payload
            .validate()
            .map_err(|error| ProtocolError::Invalid(error.to_string()))?;
        Ok(payload)
    }
}

pub fn identity_from_wire(
    tenant: Vec<u8>,
    namespace: Vec<u8>,
    key: Key,
) -> Result<aprodb_types::RecordIdentity> {
    key.validate()?;
    aprodb_types::RecordIdentity::new(tenant, namespace, key.collection, key.partition, key.key)
        .map_err(|error| ProtocolError::Invalid(error.to_string()))
}

impl From<&aprodb_types::RecordEnvelope> for WireRecord {
    fn from(record: &aprodb_types::RecordEnvelope) -> Self {
        let workflow = &record.workflow;
        Self {
            tenant: record.identity.tenant.clone(),
            namespace: record.identity.namespace.clone(),
            key: Some(Key {
                collection: record.identity.collection.clone(),
                partition: record.identity.partition.clone(),
                key: record.identity.key.clone(),
            }),
            payload: record.payload.as_ref().map(WirePayload::from),
            content_type: record.content_type.clone(),
            version: Some(record.version.into()),
            created_at_unix_ms: record.created_at_unix_ms,
            updated_at_unix_ms: record.updated_at_unix_ms,
            expires_at_unix_ms: record.expires_at_unix_ms,
            metadata: record.metadata.clone(),
            tombstone: record.tombstone,
            workflow: Some(WireWorkflow {
                state: workflow.state.clone(),
                attempt: workflow.attempt,
                lease_id: workflow.lease_id.map_or_else(Vec::new, |id| id.to_vec()),
                fencing_token: workflow.fencing_token,
                lease_deadline_unix_ms: workflow.lease_deadline_unix_ms,
            }),
            idempotency_key_hash: record
                .idempotency_key_hash
                .map_or_else(Vec::new, |hash| hash.to_vec()),
            dictionary_id: record.dictionary_id,
        }
    }
}

impl TryFrom<WireRecord> for aprodb_types::RecordEnvelope {
    type Error = ProtocolError;

    fn try_from(record: WireRecord) -> Result<Self> {
        let identity = identity_from_wire(
            record.tenant,
            record.namespace,
            record
                .key
                .ok_or_else(|| ProtocolError::Invalid("record without key".into()))?,
        )?;
        let version = record
            .version
            .ok_or_else(|| ProtocolError::Invalid("record without version".into()))?
            .into();
        let workflow = record
            .workflow
            .ok_or_else(|| ProtocolError::Invalid("record without workflow".into()))?;
        let lease_id = fixed_optional::<16>(workflow.lease_id, "lease ID")?;
        let idempotency_key_hash =
            fixed_optional::<32>(record.idempotency_key_hash, "idempotency key hash")?;
        let envelope = Self {
            identity,
            payload: record.payload.map(TryInto::try_into).transpose()?,
            content_type: record.content_type,
            version,
            created_at_unix_ms: record.created_at_unix_ms,
            updated_at_unix_ms: record.updated_at_unix_ms,
            expires_at_unix_ms: record.expires_at_unix_ms,
            metadata: record.metadata,
            workflow: aprodb_types::WorkflowDescriptor {
                state: workflow.state,
                attempt: workflow.attempt,
                lease_id,
                fencing_token: workflow.fencing_token,
                lease_deadline_unix_ms: workflow.lease_deadline_unix_ms,
            },
            idempotency_key_hash,
            dictionary_id: record.dictionary_id,
            tombstone: record.tombstone,
        };
        envelope
            .validate(&aprodb_types::Limits::default())
            .map_err(|error| ProtocolError::Invalid(error.to_string()))?;
        Ok(envelope)
    }
}

fn fixed_optional<const N: usize>(bytes: Vec<u8>, name: &str) -> Result<Option<[u8; N]>> {
    if bytes.is_empty() {
        return Ok(None);
    }
    bytes
        .try_into()
        .map(Some)
        .map_err(|_: Vec<u8>| ProtocolError::Invalid(format!("{name} has incorrect length")))
}

impl From<aprodb_types::MutationReceipt> for Receipt {
    fn from(receipt: aprodb_types::MutationReceipt) -> Self {
        Self {
            version: Some(receipt.version.into()),
            durability: WireDurability::from(receipt.durability) as i32,
            durable_watermark: receipt.durable_watermark,
            batch_id: receipt.batch_id.to_vec(),
        }
    }
}

impl TryFrom<Receipt> for aprodb_types::MutationReceipt {
    type Error = ProtocolError;

    fn try_from(receipt: Receipt) -> Result<Self> {
        let durability = WireDurability::try_from(receipt.durability)
            .map_err(|_| ProtocolError::Invalid("unknown receipt durability".into()))?
            .into();
        let batch_id = receipt
            .batch_id
            .try_into()
            .map_err(|_: Vec<u8>| ProtocolError::Invalid("batch id not 20 bytes long".into()))?;
        Ok(Self {
            version: receipt
                .version
                .ok_or_else(|| ProtocolError::Invalid("receipt without version".into()))?
                .into(),
            durability,
            durable_watermark: receipt.durable_watermark,
            batch_id,
        })
    }
}

pub fn encode_limited<M: Message>(message: &M, maximum: usize) -> Result<Bytes> {
    let encoded_len = message.encoded_len();
    if encoded_len > maximum {
        return Err(ProtocolError::FrameTooLarge {
            actual: encoded_len,
            maximum,
        });
    }
    let mut bytes = BytesMut::with_capacity(encoded_len);
    message
        .encode(&mut bytes)
        .map_err(|error| ProtocolError::Invalid(error.to_string()))?;
    Ok(bytes.freeze())
}

pub fn decode_limited<M: Message + Default>(bytes: &[u8], maximum: usize) -> Result<M> {
    if bytes.len() > maximum {
        return Err(ProtocolError::FrameTooLarge {
            actual: bytes.len(),
            maximum,
        });
    }
    Ok(M::decode(bytes)?)
}

pub fn encode_frame<M: Message>(message: &M, maximum: usize) -> Result<Bytes> {
    let payload = encode_limited(message, maximum)?;
    let payload_len = u32::try_from(payload.len())
        .map_err(|_| ProtocolError::Invalid("frame size exceeds u32 capacity".into()))?;
    let mut frame = BytesMut::with_capacity(FRAME_LENGTH_BYTES + payload.len());
    frame.put_u32(payload_len);
    frame.extend_from_slice(&payload);
    Ok(frame.freeze())
}

pub fn decode_frame<M: Message + Default>(frame: &[u8], maximum: usize) -> Result<M> {
    if frame.len() < FRAME_LENGTH_BYTES {
        return Err(ProtocolError::Invalid("incomplete frame prefix".into()));
    }
    let declared = u32::from_be_bytes(frame[..FRAME_LENGTH_BYTES].try_into().expect("4 byte"));
    let payload = &frame[FRAME_LENGTH_BYTES..];
    if declared as usize != payload.len() {
        return Err(ProtocolError::Invalid(format!(
            "frame declares {declared} bytes but contains {}",
            payload.len()
        )));
    }
    decode_limited(payload, maximum)
}

#[cfg(test)]
mod tests {
    use super::{
        ClientHello, DEFAULT_MAX_FRAME_BYTES, EndpointRole, ErrorCode, PROTOCOL_MAGIC,
        ProtocolError, Request, Response, SyncOperation, WireDurability, decode_limited,
        encode_limited, request,
    };
    use proptest::prelude::*;

    #[test]
    fn hello_and_request_round_trip() {
        let hello = ClientHello::new(
            EndpointRole::Data,
            b"0123456789abcdef".to_vec(),
            DEFAULT_MAX_FRAME_BYTES,
        );
        let encoded = encode_limited(&hello, DEFAULT_MAX_FRAME_BYTES).unwrap();
        let decoded: ClientHello = decode_limited(&encoded, DEFAULT_MAX_FRAME_BYTES).unwrap();
        assert_eq!(decoded.magic, PROTOCOL_MAGIC);
        assert_eq!(decoded.validate().unwrap(), EndpointRole::Data);

        let request = Request {
            request_id: 7,
            deadline_unix_ms: 0,
            tenant: b"tenant".to_vec(),
            namespace: b"namespace".to_vec(),
            durability: WireDurability::Durable as i32,
            operation: Some(request::Operation::Sync(SyncOperation {})),
        };
        request.validate().unwrap();
        let encoded = encode_limited(&request, 1024).unwrap();
        let decoded: Request = decode_limited(&encoded, 1024).unwrap();
        assert_eq!(decoded.request_id, 7);
    }

    #[test]
    fn bounds_and_unknown_operations_are_rejected() {
        let response = Response::error(9, ErrorCode::Backpressure, "busy");
        assert!(matches!(
            encode_limited(&response, 1),
            Err(ProtocolError::FrameTooLarge { .. })
        ));
        let request = Request {
            request_id: 1,
            deadline_unix_ms: 0,
            tenant: vec![],
            namespace: vec![],
            durability: WireDurability::Durable as i32,
            operation: None,
        };
        assert!(request.validate().is_err());
    }

    proptest! {
        #[test]
        fn protobuf_decoder_never_panics(bytes in proptest::collection::vec(any::<u8>(), 0..8192)) {
            let _ = decode_limited::<Request>(&bytes, 8192);
            let _ = decode_limited::<Response>(&bytes, 8192);
            let _ = decode_limited::<ClientHello>(&bytes, 8192);
        }
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: Apache-2.0
