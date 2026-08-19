use std::{
    collections::{BTreeMap, HashSet},
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use aprodb_types::{AproError, Result};
use chacha20poly1305::{
    XChaCha20Poly1305, XNonce,
    aead::{Aead, KeyInit, Payload},
};
use fjall::{
    CompressionType, Database, Keyspace, KeyspaceCreateOptions, OwnedWriteBatch, PersistMode,
    config::CompressionPolicy,
};
use fs4::fs_std::FileExt;

const FORMAT_MARKER: &[u8] = b"APRODB\nlogical=1\nbackend=fjall-3\n";
const FORMAT_FILE: &str = "APRODB_FORMAT";
const LOCK_FILE: &str = ".aprodb.lock";
const ENCRYPTED_MAGIC: &[u8; 4] = b"APEN";
const ENCRYPTED_VERSION: u8 = 1;
const ENCRYPTED_HEADER_LEN: usize = 40;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum StorageSpace {
    Records,
    Versions,
    Events,
    Catalog,
    Idempotency,
    Radial,
    Ttl,
    Workflow,
    IdempotencyExpiry,
    Surfaces,
    Compression,
    Audit,
}

pub const STORAGE_SPACE_COUNT: usize = 12;

impl StorageSpace {
    const fn name(self) -> &'static str {
        match self {
            Self::Records => "records",
            Self::Versions => "versions",
            Self::Events => "events",
            Self::Catalog => "catalog",
            Self::Idempotency => "idempotency",
            Self::Radial => "radial",
            Self::Ttl => "ttl",
            Self::Workflow => "workflow",
            Self::IdempotencyExpiry => "idempotency-expiry",
            Self::Surfaces => "surfaces",
            Self::Compression => "compression",
            Self::Audit => "audit",
        }
    }

    const fn code(self) -> u8 {
        match self {
            Self::Records => 1,
            Self::Versions => 2,
            Self::Events => 3,
            Self::Catalog => 4,
            Self::Idempotency => 5,
            Self::Radial => 6,
            Self::Ttl => 7,
            Self::Workflow => 8,
            Self::IdempotencyExpiry => 9,
            Self::Surfaces => 10,
            Self::Compression => 11,
            Self::Audit => 12,
        }
    }
}

#[derive(Clone, Eq, PartialEq)]
pub struct EncryptionConfig {
    active_key_id: String,
    keys: BTreeMap<String, [u8; 32]>,
}

impl std::fmt::Debug for EncryptionConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("EncryptionConfig")
            .field("active_key_id", &self.active_key_id)
            .field("key_ids", &self.keys.keys().collect::<Vec<_>>())
            .field("key_material", &"[REDACTED]")
            .finish()
    }
}

impl EncryptionConfig {
    pub fn new(active_key_id: impl Into<String>, keys: BTreeMap<String, [u8; 32]>) -> Result<Self> {
        let config = Self {
            active_key_id: active_key_id.into(),
            keys,
        };
        config.validate()?;
        Ok(config)
    }

    pub fn single(key_id: impl Into<String>, key: [u8; 32]) -> Result<Self> {
        let key_id = key_id.into();
        Self::new(key_id.clone(), BTreeMap::from([(key_id, key)]))
    }

    #[must_use]
    pub fn active_key_id(&self) -> &str {
        &self.active_key_id
    }

    #[must_use]
    pub fn key_ids(&self) -> Vec<String> {
        self.keys.keys().cloned().collect()
    }

    fn validate(&self) -> Result<()> {
        if self.keys.is_empty() || self.keys.len() > 16 {
            return Err(AproError::InvalidInput(
                "keyring cifratura deve contenere 1..16 chiavi".into(),
            ));
        }
        for key_id in self.keys.keys() {
            if key_id.is_empty()
                || key_id.len() > u8::MAX as usize
                || !key_id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
            {
                return Err(AproError::InvalidInput(
                    "key id deve usare 1..255 caratteri ASCII alfanumerici, '-', '_' o '.'".into(),
                ));
            }
        }
        if !self.keys.contains_key(&self.active_key_id) {
            return Err(AproError::InvalidInput(
                "active key id assente dal keyring".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommitMode {
    Buffered,
    Relaxed,
    Durable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BackendCompression {
    None,
    Lz4,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BackendCapabilities {
    pub atomic_cross_keyspace_batch: bool,
    pub durable_sync_all: bool,
    pub relaxed_os_buffer: bool,
    pub consistent_snapshots: bool,
    pub prefix_scan: bool,
    pub range_scan: bool,
    pub per_keyspace_compression: bool,
    pub native_checkpoint: bool,
    pub logical_checkpoint: bool,
    pub dataset_larger_than_ram: bool,
    pub bounded_write_buffer: bool,
    pub physical_compaction_control: bool,
    pub physical_storage_tiering: bool,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BackendStats {
    pub disk_bytes: u64,
    pub write_buffer_bytes: u64,
    pub journal_fragments: usize,
    pub table_count: usize,
    pub active_compactions: usize,
    pub completed_compactions: usize,
    pub compaction_micros: u64,
    pub outstanding_flushes: usize,
    pub keyspace_bytes: Vec<(StorageSpace, u64)>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompactionReport {
    pub disk_bytes_before: u64,
    pub disk_bytes_after: u64,
    pub table_count_before: usize,
    pub table_count_after: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum BatchOperation {
    Put {
        space: StorageSpace,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        space: StorageSpace,
        key: Vec<u8>,
    },
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StorageBatch {
    operations: Vec<BatchOperation>,
    bytes: usize,
}

impl StorageBatch {
    #[must_use]
    pub fn with_capacity(operations: usize) -> Self {
        Self {
            operations: Vec::with_capacity(operations),
            bytes: 0,
        }
    }

    pub fn put(&mut self, space: StorageSpace, key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) {
        let key = key.into();
        let value = value.into();
        self.bytes = self
            .bytes
            .saturating_add(key.len())
            .saturating_add(value.len());
        self.operations
            .push(BatchOperation::Put { space, key, value });
    }

    pub fn delete(&mut self, space: StorageSpace, key: impl Into<Vec<u8>>) {
        let key = key.into();
        self.bytes = self.bytes.saturating_add(key.len());
        self.operations.push(BatchOperation::Delete { space, key });
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.operations.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    #[must_use]
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    #[must_use]
    pub fn operations(&self) -> &[BatchOperation] {
        &self.operations
    }
}

pub trait FaultInjector: Send + Sync {
    fn check(&self, point: FaultPoint) -> Result<()>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultPoint {
    BeforeOpen,
    BeforeCommit,
    AfterCommitBeforeReturn,
    BeforePersist,
}

#[derive(Default)]
pub struct NoFaults;

impl FaultInjector for NoFaults {
    fn check(&self, _point: FaultPoint) -> Result<()> {
        Ok(())
    }
}

pub trait StorageBackend: Send + Sync {
    fn name(&self) -> &'static str;
    fn capabilities(&self) -> BackendCapabilities;
    fn get(&self, space: StorageSpace, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn commit(&self, batch: StorageBatch, mode: CommitMode) -> Result<()>;
    fn persist(&self, mode: CommitMode) -> Result<()>;
    fn scan_prefix(
        &self,
        space: StorageSpace,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    fn scan_range(
        &self,
        space: StorageSpace,
        start: &[u8],
        end_inclusive: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>>;
    fn stats(&self) -> Result<BackendStats>;
    fn major_compact(&self) -> Result<CompactionReport> {
        Err(AproError::Unsupported(
            "il backend non offre compattazione fisica esplicita".into(),
        ))
    }
}

pub struct EncryptedBackend {
    inner: Arc<dyn StorageBackend>,
    config: EncryptionConfig,
}

impl EncryptedBackend {
    pub fn new(inner: Arc<dyn StorageBackend>, config: EncryptionConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self { inner, config })
    }

    fn encrypt(&self, space: StorageSpace, key: &[u8], plaintext: &[u8]) -> Result<Vec<u8>> {
        let key_id = self.config.active_key_id.as_bytes();
        let key_material = self
            .config
            .keys
            .get(&self.config.active_key_id)
            .expect("configurazione validata");
        let cipher = XChaCha20Poly1305::new(key_material.into());
        let mut nonce = [0u8; 24];
        getrandom::fill(&mut nonce)
            .map_err(|error| AproError::Encryption(format!("generazione nonce: {error}")))?;
        let aad = encryption_aad(space, key, key_id);
        let ciphertext = cipher
            .encrypt(
                XNonce::from_slice(&nonce),
                Payload {
                    msg: plaintext,
                    aad: &aad,
                },
            )
            .map_err(|_| AproError::Encryption("AEAD encrypt fallita".into()))?;
        let plaintext_len = u64::try_from(plaintext.len())
            .map_err(|_| AproError::ResourceLimit("plaintext oltre u64".into()))?;
        let mut output = Vec::with_capacity(
            ENCRYPTED_HEADER_LEN
                .saturating_add(key_id.len())
                .saturating_add(ciphertext.len()),
        );
        output.extend_from_slice(ENCRYPTED_MAGIC);
        output.push(ENCRYPTED_VERSION);
        output.push(space.code());
        output.push(key_id.len() as u8);
        output.push(0);
        output.extend_from_slice(&plaintext_len.to_be_bytes());
        output.extend_from_slice(&nonce);
        output.extend_from_slice(key_id);
        output.extend_from_slice(&ciphertext);
        Ok(output)
    }

    fn decrypt(&self, space: StorageSpace, key: &[u8], encrypted: &[u8]) -> Result<Vec<u8>> {
        if encrypted.len() < ENCRYPTED_HEADER_LEN
            || &encrypted[..4] != ENCRYPTED_MAGIC
            || encrypted[4] != ENCRYPTED_VERSION
            || encrypted[5] != space.code()
            || encrypted[7] != 0
        {
            return Err(AproError::Encryption(
                "frame at-rest assente, corrotto o di versione errata".into(),
            ));
        }
        let key_id_len = encrypted[6] as usize;
        let ciphertext_offset = ENCRYPTED_HEADER_LEN
            .checked_add(key_id_len)
            .ok_or_else(|| AproError::Encryption("lunghezza key id eccessiva".into()))?;
        if encrypted.len() < ciphertext_offset.saturating_add(16) {
            return Err(AproError::Encryption(
                "frame at-rest troncato prima del tag AEAD".into(),
            ));
        }
        let plaintext_len = usize::try_from(u64::from_be_bytes(
            encrypted[8..16].try_into().expect("header verificato"),
        ))
        .map_err(|_| AproError::ResourceLimit("plaintext cifrato oltre usize".into()))?;
        if encrypted.len() - ciphertext_offset != plaintext_len.saturating_add(16) {
            return Err(AproError::Encryption(
                "lunghezza frame at-rest incoerente".into(),
            ));
        }
        let key_id = std::str::from_utf8(&encrypted[ENCRYPTED_HEADER_LEN..ciphertext_offset])
            .map_err(|_| AproError::Encryption("key id at-rest non UTF-8".into()))?;
        let key_material = self.config.keys.get(key_id).ok_or_else(|| {
            AproError::Encryption(format!("key id '{key_id}' assente dal keyring"))
        })?;
        let cipher = XChaCha20Poly1305::new(key_material.into());
        let nonce = XNonce::from_slice(&encrypted[16..40]);
        let aad = encryption_aad(space, key, key_id.as_bytes());
        let plaintext = cipher
            .decrypt(
                nonce,
                Payload {
                    msg: &encrypted[ciphertext_offset..],
                    aad: &aad,
                },
            )
            .map_err(|_| AproError::Encryption("autenticazione AEAD del valore fallita".into()))?;
        if plaintext.len() != plaintext_len {
            return Err(AproError::Encryption(
                "lunghezza plaintext at-rest incoerente".into(),
            ));
        }
        Ok(plaintext)
    }
}

impl StorageBackend for EncryptedBackend {
    fn name(&self) -> &'static str {
        self.inner.name()
    }

    fn capabilities(&self) -> BackendCapabilities {
        self.inner.capabilities()
    }

    fn get(&self, space: StorageSpace, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.inner
            .get(space, key)?
            .map(|value| self.decrypt(space, key, &value))
            .transpose()
    }

    fn commit(&self, batch: StorageBatch, mode: CommitMode) -> Result<()> {
        let mut encrypted = StorageBatch::with_capacity(batch.len());
        for operation in batch.operations {
            match operation {
                BatchOperation::Put { space, key, value } => {
                    let value = self.encrypt(space, &key, &value)?;
                    encrypted.put(space, key, value);
                }
                BatchOperation::Delete { space, key } => encrypted.delete(space, key),
            }
        }
        self.inner.commit(encrypted, mode)
    }

    fn persist(&self, mode: CommitMode) -> Result<()> {
        self.inner.persist(mode)
    }

    fn scan_prefix(
        &self,
        space: StorageSpace,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner
            .scan_prefix(space, prefix, limit)?
            .into_iter()
            .map(|(key, value)| self.decrypt(space, &key, &value).map(|value| (key, value)))
            .collect()
    }

    fn scan_range(
        &self,
        space: StorageSpace,
        start: &[u8],
        end_inclusive: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.inner
            .scan_range(space, start, end_inclusive, limit)?
            .into_iter()
            .map(|(key, value)| self.decrypt(space, &key, &value).map(|value| (key, value)))
            .collect()
    }

    fn stats(&self) -> Result<BackendStats> {
        self.inner.stats()
    }

    fn major_compact(&self) -> Result<CompactionReport> {
        self.inner.major_compact()
    }
}

fn encryption_aad(space: StorageSpace, key: &[u8], key_id: &[u8]) -> Vec<u8> {
    let mut aad = Vec::with_capacity(2 + key_id.len() + key.len());
    aad.push(ENCRYPTED_VERSION);
    aad.push(space.code());
    aad.extend_from_slice(key_id);
    aad.extend_from_slice(key);
    aad
}

#[derive(Clone, Debug)]
pub struct FjallOptions {
    pub cache_bytes: u64,
    pub max_journal_bytes: u64,
    pub worker_threads: usize,
    pub max_memtable_bytes: u64,
    pub max_batch_operations: usize,
    pub max_batch_bytes: usize,
    pub maintenance_timeout: Duration,
    pub payload_compression: BackendCompression,
    pub metadata_compression: BackendCompression,
    pub surface_compression: BackendCompression,
    pub journal_compression: BackendCompression,
}

impl Default for FjallOptions {
    fn default() -> Self {
        Self {
            cache_bytes: 32 * 1024 * 1024,
            max_journal_bytes: 512 * 1024 * 1024,
            worker_threads: std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1)
                .min(4),
            max_memtable_bytes: 16 * 1024 * 1024,
            max_batch_operations: 16_384,
            max_batch_bytes: 64 * 1024 * 1024,
            maintenance_timeout: Duration::from_secs(30),
            payload_compression: BackendCompression::None,
            metadata_compression: BackendCompression::Lz4,
            surface_compression: BackendCompression::Lz4,
            journal_compression: BackendCompression::Lz4,
        }
    }
}

struct Keyspaces {
    records: Keyspace,
    versions: Keyspace,
    events: Keyspace,
    catalog: Keyspace,
    idempotency: Keyspace,
    radial: Keyspace,
    ttl: Keyspace,
    workflow: Keyspace,
    idempotency_expiry: Keyspace,
    surfaces: Keyspace,
    compression: Keyspace,
    audit: Keyspace,
}

impl Keyspaces {
    fn get(&self, space: StorageSpace) -> &Keyspace {
        match space {
            StorageSpace::Records => &self.records,
            StorageSpace::Versions => &self.versions,
            StorageSpace::Events => &self.events,
            StorageSpace::Catalog => &self.catalog,
            StorageSpace::Idempotency => &self.idempotency,
            StorageSpace::Radial => &self.radial,
            StorageSpace::Ttl => &self.ttl,
            StorageSpace::Workflow => &self.workflow,
            StorageSpace::IdempotencyExpiry => &self.idempotency_expiry,
            StorageSpace::Surfaces => &self.surfaces,
            StorageSpace::Compression => &self.compression,
            StorageSpace::Audit => &self.audit,
        }
    }

    fn all(&self) -> [(StorageSpace, &Keyspace); STORAGE_SPACE_COUNT] {
        [
            (StorageSpace::Records, &self.records),
            (StorageSpace::Versions, &self.versions),
            (StorageSpace::Events, &self.events),
            (StorageSpace::Catalog, &self.catalog),
            (StorageSpace::Idempotency, &self.idempotency),
            (StorageSpace::Radial, &self.radial),
            (StorageSpace::Ttl, &self.ttl),
            (StorageSpace::Workflow, &self.workflow),
            (StorageSpace::IdempotencyExpiry, &self.idempotency_expiry),
            (StorageSpace::Surfaces, &self.surfaces),
            (StorageSpace::Compression, &self.compression),
            (StorageSpace::Audit, &self.audit),
        ]
    }
}

pub struct FjallBackend {
    root: PathBuf,
    _lock: DirectoryLock,
    database: Database,
    keyspaces: Keyspaces,
    options: FjallOptions,
    faults: Arc<dyn FaultInjector>,
    poisoned: AtomicBool,
}

impl FjallBackend {
    pub fn open(root: impl AsRef<Path>, options: FjallOptions) -> Result<Self> {
        Self::open_with_faults(root, options, Arc::new(NoFaults))
    }

    pub fn open_with_faults(
        root: impl AsRef<Path>,
        options: FjallOptions,
        faults: Arc<dyn FaultInjector>,
    ) -> Result<Self> {
        validate_options(&options)?;
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(storage_error)?;
        let lock = acquire_lock(&root)?;
        validate_or_create_marker(&root)?;

        let backend_path = root.join("backend");
        faults.check(FaultPoint::BeforeOpen)?;
        let database = Database::builder(&backend_path)
            .cache_size(options.cache_bytes)
            .max_journaling_size(options.max_journal_bytes)
            .worker_threads(options.worker_threads)
            .journal_compression(compression_type(options.journal_compression))
            .open()
            .map_err(storage_error)?;

        let payload_options =
            || keyspace_options(options.payload_compression, options.max_memtable_bytes);
        let metadata_options =
            || keyspace_options(options.metadata_compression, options.max_memtable_bytes);
        let surface_options =
            || keyspace_options(options.surface_compression, options.max_memtable_bytes);
        let keyspaces = Keyspaces {
            records: database
                .keyspace(StorageSpace::Records.name(), payload_options)
                .map_err(storage_error)?,
            versions: database
                .keyspace(StorageSpace::Versions.name(), payload_options)
                .map_err(storage_error)?,
            events: database
                .keyspace(StorageSpace::Events.name(), metadata_options)
                .map_err(storage_error)?,
            catalog: database
                .keyspace(StorageSpace::Catalog.name(), metadata_options)
                .map_err(storage_error)?,
            idempotency: database
                .keyspace(StorageSpace::Idempotency.name(), metadata_options)
                .map_err(storage_error)?,
            radial: database
                .keyspace(StorageSpace::Radial.name(), metadata_options)
                .map_err(storage_error)?,
            ttl: database
                .keyspace(StorageSpace::Ttl.name(), metadata_options)
                .map_err(storage_error)?,
            workflow: database
                .keyspace(StorageSpace::Workflow.name(), metadata_options)
                .map_err(storage_error)?,
            idempotency_expiry: database
                .keyspace(StorageSpace::IdempotencyExpiry.name(), metadata_options)
                .map_err(storage_error)?,
            surfaces: database
                .keyspace(StorageSpace::Surfaces.name(), surface_options)
                .map_err(storage_error)?,
            compression: database
                .keyspace(StorageSpace::Compression.name(), metadata_options)
                .map_err(storage_error)?,
            audit: database
                .keyspace(StorageSpace::Audit.name(), metadata_options)
                .map_err(storage_error)?,
        };
        Ok(Self {
            root,
            _lock: lock,
            database,
            keyspaces,
            options,
            faults,
            poisoned: AtomicBool::new(false),
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }
}

impl StorageBackend for FjallBackend {
    fn name(&self) -> &'static str {
        "fjall-3.1.8"
    }

    fn capabilities(&self) -> BackendCapabilities {
        BackendCapabilities {
            atomic_cross_keyspace_batch: true,
            durable_sync_all: true,
            relaxed_os_buffer: true,
            consistent_snapshots: true,
            prefix_scan: true,
            range_scan: true,
            per_keyspace_compression: true,
            native_checkpoint: false,
            logical_checkpoint: true,
            dataset_larger_than_ram: true,
            bounded_write_buffer: true,
            physical_compaction_control: true,
            physical_storage_tiering: false,
        }
    }

    fn get(&self, space: StorageSpace, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.keyspaces
            .get(space)
            .get(key)
            .map(|value| value.map(|value| value.to_vec()))
            .map_err(storage_error)
    }

    fn commit(&self, batch: StorageBatch, mode: CommitMode) -> Result<()> {
        if batch.len() > self.options.max_batch_operations {
            return Err(AproError::ResourceLimit(format!(
                "batch storage con {} operazioni, massimo {}",
                batch.len(),
                self.options.max_batch_operations
            )));
        }
        if batch.bytes() > self.options.max_batch_bytes {
            return Err(AproError::ResourceLimit(format!(
                "batch storage di {} byte, massimo {}",
                batch.bytes(),
                self.options.max_batch_bytes
            )));
        }
        if batch.is_empty() {
            return Ok(());
        }
        if self.poisoned.load(Ordering::Acquire) {
            return Err(AproError::Storage(
                "backend Fjall arrestato dopo un errore di scrittura; riaprire il database".into(),
            ));
        }
        let result = (|| {
            self.faults.check(FaultPoint::BeforeCommit)?;
            let mut fjall_batch =
                OwnedWriteBatch::with_capacity(self.database.clone(), batch.len());
            for operation in batch.operations {
                match operation {
                    BatchOperation::Put { space, key, value } => {
                        fjall_batch.insert(self.keyspaces.get(space), key, value);
                    }
                    BatchOperation::Delete { space, key } => {
                        fjall_batch.remove(self.keyspaces.get(space), key);
                    }
                }
            }
            fjall_batch
                .durability(Some(to_persist_mode(mode)))
                .commit()
                .map_err(storage_error)?;
            self.faults.check(FaultPoint::AfterCommitBeforeReturn)
        })();
        if result.is_err() {
            self.poisoned.store(true, Ordering::Release);
        }
        result
    }

    fn persist(&self, mode: CommitMode) -> Result<()> {
        if self.poisoned.load(Ordering::Acquire) {
            return Err(AproError::Storage(
                "backend Fjall arrestato dopo un errore di scrittura; riaprire il database".into(),
            ));
        }
        let result = self.faults.check(FaultPoint::BeforePersist).and_then(|()| {
            self.database
                .persist(to_persist_mode(mode))
                .map_err(storage_error)
        });
        if result.is_err() {
            self.poisoned.store(true, Ordering::Release);
        }
        result
    }

    fn scan_prefix(
        &self,
        space: StorageSpace,
        prefix: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.keyspaces
            .get(space)
            .prefix(prefix)
            .take(limit)
            .map(|item| {
                item.into_inner()
                    .map(|(key, value)| (key.to_vec(), value.to_vec()))
                    .map_err(storage_error)
            })
            .collect()
    }

    fn scan_range(
        &self,
        space: StorageSpace,
        start: &[u8],
        end_inclusive: &[u8],
        limit: usize,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        self.keyspaces
            .get(space)
            .range(start..=end_inclusive)
            .take(limit)
            .map(|item| {
                item.into_inner()
                    .map(|(key, value)| (key.to_vec(), value.to_vec()))
                    .map_err(storage_error)
            })
            .collect()
    }

    fn stats(&self) -> Result<BackendStats> {
        Ok(BackendStats {
            disk_bytes: self.database.disk_space().map_err(storage_error)?,
            write_buffer_bytes: self.database.write_buffer_size(),
            journal_fragments: self.database.journal_count(),
            table_count: self
                .keyspaces
                .all()
                .into_iter()
                .map(|(_, keyspace)| keyspace.table_count())
                .sum(),
            active_compactions: self.database.active_compactions(),
            completed_compactions: self.database.compactions_completed(),
            compaction_micros: self.database.time_compacting().as_micros() as u64,
            outstanding_flushes: self.database.outstanding_flushes(),
            keyspace_bytes: self
                .keyspaces
                .all()
                .into_iter()
                .map(|(space, keyspace)| (space, keyspace.disk_space()))
                .collect(),
        })
    }

    fn major_compact(&self) -> Result<CompactionReport> {
        let before = self.stats()?;
        for (_, keyspace) in self.keyspaces.all() {
            keyspace.rotate_memtable_and_wait().map_err(storage_error)?;
        }
        let deadline = Instant::now() + self.options.maintenance_timeout;
        while self.database.write_buffer_size() != 0 || self.database.outstanding_flushes() != 0 {
            if Instant::now() >= deadline {
                return Err(AproError::Storage(format!(
                    "timeout flush Fjall dopo {:?}",
                    self.options.maintenance_timeout
                )));
            }
            std::thread::yield_now();
        }
        for (_, keyspace) in self.keyspaces.all() {
            keyspace.major_compact().map_err(storage_error)?;
        }
        let after = self.stats()?;
        Ok(CompactionReport {
            disk_bytes_before: before.disk_bytes,
            disk_bytes_after: after.disk_bytes,
            table_count_before: before.table_count,
            table_count_after: after.table_count,
        })
    }
}

fn validate_options(options: &FjallOptions) -> Result<()> {
    if options.cache_bytes == 0 {
        return Err(AproError::InvalidInput(
            "cache Fjall deve avere un budget positivo".into(),
        ));
    }
    if options.max_journal_bytes < 64 * 1024 * 1024 {
        return Err(AproError::InvalidInput(
            "max journal Fjall deve essere almeno 64 MiB".into(),
        ));
    }
    if options.worker_threads == 0 {
        return Err(AproError::InvalidInput(
            "Fjall richiede almeno un worker".into(),
        ));
    }
    if options.max_memtable_bytes < 1024 * 1024 {
        return Err(AproError::InvalidInput(
            "memtable Fjall deve essere almeno 1 MiB".into(),
        ));
    }
    if options.maintenance_timeout.is_zero() {
        return Err(AproError::InvalidInput(
            "timeout manutenzione Fjall deve essere positivo".into(),
        ));
    }
    Ok(())
}

struct DirectoryLock {
    file: File,
    canonical_root: PathBuf,
}

impl Drop for DirectoryLock {
    fn drop(&mut self) {
        if let Ok(mut roots) = locked_roots().lock() {
            roots.remove(&self.canonical_root);
        }
        let _ = FileExt::unlock(&self.file);
    }
}

fn locked_roots() -> &'static Mutex<HashSet<PathBuf>> {
    static ROOTS: OnceLock<Mutex<HashSet<PathBuf>>> = OnceLock::new();
    ROOTS.get_or_init(|| Mutex::new(HashSet::new()))
}

fn acquire_lock(root: &Path) -> Result<DirectoryLock> {
    let canonical_root = fs::canonicalize(root).map_err(storage_error)?;
    let lock_path = root.join(LOCK_FILE);
    let lock = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&lock_path)
        .map_err(storage_error)?;
    let acquired = lock
        .try_lock_exclusive()
        .map_err(|error| AproError::DataDirectoryLocked(format!("{}: {error}", root.display())))?;
    if !acquired {
        return Err(AproError::DataDirectoryLocked(root.display().to_string()));
    }
    let mut roots = locked_roots()
        .lock()
        .map_err(|_| AproError::Storage("registro lock directory avvelenato".into()))?;
    if !roots.insert(canonical_root.clone()) {
        return Err(AproError::DataDirectoryLocked(root.display().to_string()));
    }
    drop(roots);
    Ok(DirectoryLock {
        file: lock,
        canonical_root,
    })
}

fn validate_or_create_marker(root: &Path) -> Result<()> {
    let marker_path = root.join(FORMAT_FILE);
    match File::open(&marker_path) {
        Ok(mut marker) => {
            let mut contents = Vec::new();
            marker.read_to_end(&mut contents).map_err(storage_error)?;
            if contents != FORMAT_MARKER {
                return Err(AproError::IncompatibleFormat(format!(
                    "marker non riconosciuto in {}",
                    marker_path.display()
                )));
            }
            Ok(())
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let legacy_wal = root.join("aprodb.wal");
            let legacy_snapshot = root.join("aprodb.snapshot");
            if legacy_wal.exists() || legacy_snapshot.exists() {
                return Err(AproError::IncompatibleFormat(
                    "directory AProDB 0.1: usare import esplicito o una directory 1.x vuota".into(),
                ));
            }
            let backend_path = root.join("backend");
            if backend_path.exists()
                && backend_path
                    .read_dir()
                    .map_err(storage_error)?
                    .next()
                    .is_some()
            {
                return Err(AproError::IncompatibleFormat(
                    "backend senza marker AProDB; apertura automatica rifiutata".into(),
                ));
            }
            let temporary = root.join("APRODB_FORMAT.tmp");
            let mut marker = OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&temporary)
                .map_err(storage_error)?;
            marker.write_all(FORMAT_MARKER).map_err(storage_error)?;
            marker.sync_all().map_err(storage_error)?;
            drop(marker);
            fs::rename(&temporary, &marker_path).map_err(storage_error)
        }
        Err(error) => Err(storage_error(error)),
    }
}

fn keyspace_options(
    compression: BackendCompression,
    max_memtable_bytes: u64,
) -> KeyspaceCreateOptions {
    let compression = compression_type(compression);
    KeyspaceCreateOptions::default()
        .data_block_compression_policy(CompressionPolicy::all(compression))
        .index_block_compression_policy(CompressionPolicy::all(compression))
        .max_memtable_size(max_memtable_bytes)
}

const fn compression_type(compression: BackendCompression) -> CompressionType {
    match compression {
        BackendCompression::None => CompressionType::None,
        BackendCompression::Lz4 => CompressionType::Lz4,
    }
}

const fn to_persist_mode(mode: CommitMode) -> PersistMode {
    match mode {
        CommitMode::Buffered | CommitMode::Relaxed => PersistMode::Buffer,
        CommitMode::Durable => PersistMode::SyncAll,
    }
}

fn storage_error(error: impl std::fmt::Display) -> AproError {
    AproError::Storage(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::{
        io::{BufRead, BufReader, Write},
        net::{TcpListener, TcpStream},
        process::{Command, Stdio},
        sync::Arc,
    };

    use super::{
        CommitMode, EncryptedBackend, EncryptionConfig, FjallBackend, FjallOptions, StorageBackend,
        StorageBatch, StorageSpace,
    };
    use aprodb_types::AproError;
    use tempfile::tempdir;

    #[test]
    fn rejects_legacy_directory_and_second_open() {
        let legacy = tempdir().unwrap();
        std::fs::write(legacy.path().join("aprodb.wal"), b"legacy").unwrap();
        assert!(matches!(
            FjallBackend::open(legacy.path(), FjallOptions::default()),
            Err(AproError::IncompatibleFormat(_))
        ));

        let current = tempdir().unwrap();
        let first = FjallBackend::open(current.path(), FjallOptions::default()).unwrap();
        assert!(matches!(
            FjallBackend::open(current.path(), FjallOptions::default()),
            Err(AproError::DataDirectoryLocked(_))
        ));
        drop(first);
        FjallBackend::open(current.path(), FjallOptions::default()).unwrap();
    }

    #[test]
    fn encrypted_backend_authenticates_space_key_and_key_id() {
        let directory = tempdir().unwrap();
        let raw = Arc::new(FjallBackend::open(directory.path(), FjallOptions::default()).unwrap());
        let raw_backend: Arc<dyn StorageBackend> = raw.clone();
        let encrypted = EncryptedBackend::new(
            raw_backend,
            EncryptionConfig::single("primary-1", [7; 32]).unwrap(),
        )
        .unwrap();
        let mut batch = StorageBatch::with_capacity(1);
        batch.put(
            StorageSpace::Catalog,
            b"key".to_vec(),
            b"plaintext".to_vec(),
        );
        encrypted.commit(batch, CommitMode::Durable).unwrap();
        let physical = raw.get(StorageSpace::Catalog, b"key").unwrap().unwrap();
        assert!(physical.starts_with(b"APEN"));
        assert!(!physical.windows(9).any(|window| window == b"plaintext"));
        assert_eq!(
            encrypted.get(StorageSpace::Catalog, b"key").unwrap(),
            Some(b"plaintext".to_vec())
        );

        let wrong = EncryptedBackend::new(
            raw.clone(),
            EncryptionConfig::single("primary-1", [8; 32]).unwrap(),
        )
        .unwrap();
        assert!(matches!(
            wrong.get(StorageSpace::Catalog, b"key"),
            Err(AproError::Encryption(_))
        ));

        let mut tampered = physical;
        let last = tampered.len() - 1;
        tampered[last] ^= 1;
        let mut batch = StorageBatch::with_capacity(1);
        batch.put(StorageSpace::Catalog, b"key".to_vec(), tampered);
        raw.commit(batch, CommitMode::Durable).unwrap();
        assert!(matches!(
            encrypted.get(StorageSpace::Catalog, b"key"),
            Err(AproError::Encryption(_))
        ));
    }

    #[test]
    fn cross_process_lock_is_exclusive() {
        let directory = tempdir().unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "tests::cross_process_lock_helper", "--nocapture"])
            .env("APRODB_LOCK_HELPER_PATH", directory.path())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        let release_address = loop {
            line.clear();
            assert_ne!(output.read_line(&mut line).unwrap(), 0);
            if let Some((_, address)) = line.trim().split_once("APRODB_LOCK_READY:") {
                break address.to_owned();
            }
        };
        let second = FjallBackend::open(directory.path(), FjallOptions::default());
        TcpStream::connect(release_address).unwrap();
        assert!(child.wait().unwrap().success());
        assert!(matches!(second, Err(AproError::DataDirectoryLocked(_))));
    }

    #[test]
    fn cross_process_lock_helper() {
        let Ok(path) = std::env::var("APRODB_LOCK_HELPER_PATH") else {
            return;
        };
        let _backend = FjallBackend::open(path, FjallOptions::default()).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        println!("APRODB_LOCK_READY:{}", listener.local_addr().unwrap());
        std::io::stdout().flush().unwrap();
        listener.accept().unwrap();
    }

    struct OpenFault;

    impl super::FaultInjector for OpenFault {
        fn check(&self, point: super::FaultPoint) -> aprodb_types::Result<()> {
            if point == super::FaultPoint::BeforeOpen {
                return Err(AproError::Storage("fault apertura backend".into()));
            }
            Ok(())
        }
    }

    #[test]
    fn failed_backend_open_releases_the_directory_lock() {
        let directory = tempdir().unwrap();
        assert!(
            FjallBackend::open_with_faults(
                directory.path(),
                FjallOptions::default(),
                Arc::new(OpenFault),
            )
            .is_err()
        );
        FjallBackend::open(directory.path(), FjallOptions::default()).unwrap();
    }

    #[test]
    fn durable_commit_survives_writer_process_kill() {
        let directory = tempdir().unwrap();
        let mut child = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::durable_commit_process_helper",
                "--nocapture",
            ])
            .env("APRODB_DURABLE_HELPER_PATH", directory.path())
            .stdout(Stdio::piped())
            .spawn()
            .unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let mut line = String::new();
        loop {
            line.clear();
            assert_ne!(output.read_line(&mut line).unwrap(), 0);
            if line.contains("APRODB_DURABLE_ACK") {
                break;
            }
        }
        child.kill().unwrap();
        child.wait().unwrap();

        let reopened = FjallBackend::open(directory.path(), FjallOptions::default()).unwrap();
        assert_eq!(
            reopened.get(StorageSpace::Records, b"durable-key").unwrap(),
            Some(b"durable-value".to_vec())
        );
    }

    #[test]
    fn durable_commit_process_helper() {
        let Ok(path) = std::env::var("APRODB_DURABLE_HELPER_PATH") else {
            return;
        };
        let backend = FjallBackend::open(path, FjallOptions::default()).unwrap();
        let mut batch = StorageBatch::with_capacity(1);
        batch.put(
            StorageSpace::Records,
            b"durable-key".to_vec(),
            b"durable-value".to_vec(),
        );
        backend.commit(batch, CommitMode::Durable).unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        println!("APRODB_DURABLE_ACK");
        std::io::stdout().flush().unwrap();
        listener.accept().unwrap();
    }

    #[test]
    fn atomic_batch_reopens_with_all_keyspaces() {
        let directory = tempdir().unwrap();
        {
            let backend = FjallBackend::open(directory.path(), FjallOptions::default()).unwrap();
            let mut batch = StorageBatch::with_capacity(3);
            batch.put(StorageSpace::Records, b"head".to_vec(), b"record".to_vec());
            batch.put(StorageSpace::Events, b"event".to_vec(), b"change".to_vec());
            batch.put(
                StorageSpace::Catalog,
                b"catalog".to_vec(),
                b"state".to_vec(),
            );
            backend.commit(batch, CommitMode::Durable).unwrap();
        }
        let backend = FjallBackend::open(directory.path(), FjallOptions::default()).unwrap();
        assert_eq!(
            backend.get(StorageSpace::Records, b"head").unwrap(),
            Some(b"record".to_vec())
        );
        assert_eq!(
            backend.get(StorageSpace::Events, b"event").unwrap(),
            Some(b"change".to_vec())
        );
        assert_eq!(
            backend.get(StorageSpace::Catalog, b"catalog").unwrap(),
            Some(b"state".to_vec())
        );
    }

    #[test]
    fn scans_are_bounded() {
        let directory = tempdir().unwrap();
        let backend = FjallBackend::open(directory.path(), FjallOptions::default()).unwrap();
        let mut batch = StorageBatch::default();
        for key in [b"a:1", b"a:2", b"a:3", b"b:1"] {
            batch.put(StorageSpace::Records, key.to_vec(), key.to_vec());
        }
        backend.commit(batch, CommitMode::Relaxed).unwrap();
        let rows = backend
            .scan_prefix(StorageSpace::Records, b"a:", 2)
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, b"a:1");
        assert_eq!(rows[1].0, b"a:2");
    }

    #[test]
    fn explicit_compaction_flushes_and_preserves_data() {
        let directory = tempdir().unwrap();
        let options = FjallOptions {
            max_memtable_bytes: 1024 * 1024,
            ..FjallOptions::default()
        };
        {
            let backend = FjallBackend::open(directory.path(), options.clone()).unwrap();
            let mut batch = StorageBatch::with_capacity(1024);
            for key in 0u32..1024 {
                batch.put(
                    StorageSpace::Versions,
                    key.to_be_bytes().to_vec(),
                    vec![key as u8; 4096],
                );
            }
            backend.commit(batch, CommitMode::Durable).unwrap();
            assert!(backend.capabilities().physical_compaction_control);
            let report = backend.major_compact().unwrap();
            assert!(report.disk_bytes_after > 0);
            assert!(
                report.table_count_after > 0,
                "report compattazione inatteso: {report:?}"
            );
            assert_eq!(
                backend
                    .get(StorageSpace::Versions, &1023u32.to_be_bytes())
                    .unwrap(),
                Some(vec![255; 4096])
            );
        }
        let reopened = FjallBackend::open(directory.path(), options).unwrap();
        assert_eq!(
            reopened
                .get(StorageSpace::Versions, &1023u32.to_be_bytes())
                .unwrap(),
            Some(vec![255; 4096])
        );
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
