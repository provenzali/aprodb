use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use parking_lot::{Mutex, RwLock};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::xxh3_64;

use crate::{
    AproError, Result, Value,
    compression::{CompressionChannel, StoredValue},
    compute::score_vectors_cpu,
    record::{Operation, Record},
    snapshot,
    wal::Wal,
};

#[cfg(feature = "gpu")]
use crate::compute::GpuExecutor;

const DEFAULT_GPU_MIN_WORK: usize = 16 * 1024 * 1024;
const MAX_KEY_BYTES: usize = 1024 * 1024;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Durability {
    Relaxed,
    #[default]
    SyncData,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ComputeBackend {
    #[default]
    Auto,
    Cpu,
    Gpu,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Metric {
    Dot,
    #[default]
    Cosine,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub path: PathBuf,
    pub shards: usize,
    pub durability: Durability,
    /// Numero minimo di componenti (`vettori × dimensione`) prima di tentare la GPU in `Auto`.
    pub gpu_min_work: usize,
    pub compression_level: i32,
    pub compression_min_size: usize,
    pub compression_channels: usize,
}

impl Config {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        let parallelism = std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(4);
        Self {
            path: path.into(),
            shards: (parallelism * 4).next_power_of_two().clamp(16, 256),
            durability: Durability::SyncData,
            gpu_min_work: DEFAULT_GPU_MIN_WORK,
            compression_level: 1,
            compression_min_size: 32,
            compression_channels: parallelism.next_power_of_two().clamp(2, 32),
        }
    }
}

#[derive(Clone, Debug)]
struct Entry {
    sequence: u64,
    value: Option<StoredValue>,
}

#[derive(Clone, Debug, Serialize)]
pub struct SearchHit {
    pub key: String,
    pub score: f32,
}

#[derive(Clone, Debug, Serialize)]
pub struct SearchResult {
    pub hits: Vec<SearchHit>,
    pub backend: ComputeBackend,
    pub accelerator: Option<String>,
    pub candidates: usize,
}

#[derive(Clone, Debug, Serialize)]
pub struct DatabaseStats {
    pub live_keys: usize,
    pub tombstones: usize,
    pub shards: usize,
    pub sequence: u64,
    pub wal_bytes: u64,
    pub rayon_threads: usize,
    pub gpu_compiled: bool,
    pub compressed_values: usize,
    pub raw_values: usize,
    pub logical_value_bytes: u64,
    pub stored_value_bytes: u64,
    pub compression_ratio: f64,
    pub compression_channels: usize,
}

#[cfg(feature = "gpu")]
enum GpuState {
    Uninitialized,
    Ready(GpuExecutor),
    Unavailable(String),
}

pub struct Database {
    config: Config,
    shards: Vec<RwLock<HashMap<String, Entry>>>,
    wal: Mutex<Wal>,
    sequence: AtomicU64,
    write_gate: RwLock<()>,
    compression: Vec<Mutex<CompressionChannel>>,
    #[cfg(feature = "gpu")]
    gpu: Mutex<GpuState>,
}

impl Database {
    pub fn open(config: Config) -> Result<Self> {
        validate_config(&config)?;
        fs::create_dir_all(&config.path)?;
        let snapshot_records = snapshot::load(&snapshot_path(&config.path))?;
        let (wal, wal_records) = Wal::open(&wal_path(&config.path), config.durability)?;
        let compression: Vec<_> = (0..config.compression_channels)
            .map(|_| {
                CompressionChannel::new(config.compression_level, config.compression_min_size)
                    .map(Mutex::new)
            })
            .collect::<Result<_>>()?;
        let database = Self {
            shards: (0..config.shards)
                .map(|_| RwLock::new(HashMap::new()))
                .collect(),
            config,
            wal: Mutex::new(wal),
            sequence: AtomicU64::new(0),
            write_gate: RwLock::new(()),
            compression,
            #[cfg(feature = "gpu")]
            gpu: Mutex::new(GpuState::Uninitialized),
        };

        let mut max_sequence = 0;
        for record in snapshot_records.into_iter().chain(wal_records) {
            max_sequence = max_sequence.max(record.sequence);
            database.apply_record(&record);
        }
        database.sequence.store(max_sequence, Ordering::Release);
        Ok(database)
    }

    pub fn put(&self, key: impl Into<String>, value: Value) -> Result<u64> {
        self.put_batch(vec![(key.into(), value)])
            .map(|mut sequences| sequences.remove(0))
    }

    pub fn put_batch(&self, entries: Vec<(String, Value)>) -> Result<Vec<u64>> {
        if entries.is_empty() {
            return Ok(Vec::new());
        }
        for (key, value) in &entries {
            validate_key(key)?;
            value.validate()?;
        }

        let _gate = self.write_gate.read();
        let count = entries.len() as u64;
        let first = self.sequence.fetch_add(count, Ordering::AcqRel) + 1;
        let records: Vec<Record> = entries
            .into_par_iter()
            .enumerate()
            .map(|(offset, (key, value))| {
                let stored = self.compress_value(&key, &value)?;
                Ok(Record {
                    sequence: first + offset as u64,
                    key,
                    operation: Operation::Put(stored),
                })
            })
            .collect::<Result<_>>()?;
        self.wal.lock().append_batch(&records)?;
        records
            .par_iter()
            .for_each(|record| self.apply_record(record));
        Ok(records.iter().map(|record| record.sequence).collect())
    }

    pub fn get(&self, key: &str) -> Result<Option<Value>> {
        let shard = self.shards[self.shard_index(key)].read();
        let stored = shard.get(key).and_then(|entry| entry.value.clone());
        drop(shard);
        stored
            .map(|value| self.decompress_value(key, &value))
            .transpose()
    }

    pub fn get_batch(&self, keys: &[String]) -> Result<Vec<Option<Value>>> {
        keys.par_iter().map(|key| self.get(key)).collect()
    }

    pub fn delete(&self, key: &str) -> Result<bool> {
        validate_key(key)?;
        let _gate = self.write_gate.read();
        let existed = self.shards[self.shard_index(key)]
            .read()
            .get(key)
            .is_some_and(|entry| entry.value.is_some());
        let sequence = self.sequence.fetch_add(1, Ordering::AcqRel) + 1;
        let record = Record {
            sequence,
            key: key.to_owned(),
            operation: Operation::Delete,
        };
        self.wal
            .lock()
            .append_batch(std::slice::from_ref(&record))?;
        self.apply_record(&record);
        Ok(existed)
    }

    pub fn scan_prefix(&self, prefix: &str, limit: usize) -> Result<Vec<(String, Value)>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let stored_rows: Vec<_> = self
            .shards
            .par_iter()
            .map(|shard| {
                shard
                    .read()
                    .iter()
                    .filter_map(|(key, entry)| {
                        if key.starts_with(prefix) {
                            entry.value.clone().map(|value| (key.clone(), value))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .reduce(Vec::new, |mut left, mut right| {
                left.append(&mut right);
                left
            });
        let mut rows: Vec<_> = stored_rows
            .into_par_iter()
            .map(|(key, stored)| {
                let value = self.decompress_value(&key, &stored)?;
                Ok((key, value))
            })
            .collect::<Result<_>>()?;
        rows.par_sort_unstable_by(|left, right| left.0.cmp(&right.0));
        rows.truncate(limit);
        Ok(rows)
    }

    /// Bounded, point-in-time export used only by the explicit 0.1 importer.
    pub fn export_for_migration(
        &self,
        max_records: usize,
        max_stored_bytes: usize,
    ) -> Result<Vec<(String, Value)>> {
        if max_records == 0 || max_stored_bytes == 0 {
            return Err(AproError::InvalidValue(
                "i limiti export migrazione devono essere positivi".into(),
            ));
        }
        let _gate = self.write_gate.write();
        let mut stored_rows = Vec::new();
        let mut stored_bytes = 0usize;
        for shard in &self.shards {
            let shard = shard.read();
            for (key, entry) in shard.iter() {
                let Some(value) = entry.value.as_ref() else {
                    continue;
                };
                if stored_rows.len() >= max_records {
                    return Err(AproError::InvalidValue(format!(
                        "export migrazione oltre {max_records} record"
                    )));
                }
                stored_bytes = stored_bytes
                    .checked_add(key.len())
                    .and_then(|bytes| bytes.checked_add(value.stored_len()))
                    .ok_or_else(|| {
                        AproError::InvalidValue("byte export migrazione oltre usize".into())
                    })?;
                if stored_bytes > max_stored_bytes {
                    return Err(AproError::InvalidValue(format!(
                        "export migrazione oltre {max_stored_bytes} byte memorizzati"
                    )));
                }
                stored_rows.push((key.clone(), value.clone()));
            }
        }
        let mut rows = stored_rows
            .into_iter()
            .map(|(key, stored)| {
                let value = self.decompress_value(&key, &stored)?;
                Ok((key, value))
            })
            .collect::<Result<Vec<_>>>()?;
        rows.sort_unstable_by(|left, right| left.0.cmp(&right.0));
        Ok(rows)
    }

    pub fn vector_search(
        &self,
        query: &[f32],
        limit: usize,
        metric: Metric,
        backend: ComputeBackend,
    ) -> Result<SearchResult> {
        validate_query(query)?;
        if limit == 0 {
            return Ok(SearchResult {
                hits: Vec::new(),
                backend: ComputeBackend::Cpu,
                accelerator: None,
                candidates: 0,
            });
        }

        let stored_candidates: Vec<(String, StoredValue)> = self
            .shards
            .par_iter()
            .map(|shard| {
                shard
                    .read()
                    .iter()
                    .filter_map(|(key, entry)| match &entry.value {
                        Some(stored) if stored.is_vector() => Some((key.clone(), stored.clone())),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
            })
            .reduce(Vec::new, |mut left, mut right| {
                left.append(&mut right);
                left
            });
        let candidates: Vec<(String, Vec<f32>)> = stored_candidates
            .into_par_iter()
            .filter_map(|(key, stored)| match self.decompress_value(&key, &stored) {
                Ok(Value::Vector(vector)) if vector.len() == query.len() => Some(Ok((key, vector))),
                Ok(Value::Vector(_)) => None,
                Ok(_) => Some(Err(AproError::Corrupt(
                    "metadato vettoriale incoerente".into(),
                ))),
                Err(error) => Some(Err(error)),
            })
            .collect::<Result<_>>()?;
        let vectors: Vec<Vec<f32>> = candidates
            .iter()
            .map(|(_, vector)| vector.clone())
            .collect();

        let (scores, used, accelerator) = match backend {
            ComputeBackend::Cpu => (
                score_vectors_cpu(&vectors, query, metric),
                ComputeBackend::Cpu,
                None,
            ),
            ComputeBackend::Gpu => {
                let (scores, name) = self.score_vectors_gpu(&vectors, query, metric)?;
                (scores, ComputeBackend::Gpu, Some(name))
            }
            ComputeBackend::Auto
                if vectors.len().saturating_mul(query.len()) >= self.config.gpu_min_work =>
            {
                match self.score_vectors_gpu(&vectors, query, metric) {
                    Ok((scores, name)) => (scores, ComputeBackend::Gpu, Some(name)),
                    Err(_) => (
                        score_vectors_cpu(&vectors, query, metric),
                        ComputeBackend::Cpu,
                        None,
                    ),
                }
            }
            ComputeBackend::Auto => (
                score_vectors_cpu(&vectors, query, metric),
                ComputeBackend::Cpu,
                None,
            ),
        };

        let mut hits: Vec<_> = candidates
            .into_iter()
            .zip(scores)
            .map(|((key, _), score)| SearchHit { key, score })
            .collect();
        hits.par_sort_unstable_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.key.cmp(&right.key))
        });
        let candidate_count = hits.len();
        hits.truncate(limit);
        Ok(SearchResult {
            hits,
            backend: used,
            accelerator,
            candidates: candidate_count,
        })
    }

    pub fn snapshot(&self) -> Result<usize> {
        let _gate = self.write_gate.write();
        let records: Vec<Record> = self
            .shards
            .iter()
            .flat_map(|shard| {
                shard
                    .read()
                    .iter()
                    .filter_map(|(key, entry)| {
                        entry.value.clone().map(|value| Record {
                            sequence: entry.sequence,
                            key: key.clone(),
                            operation: Operation::Put(value),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .collect();
        snapshot::save(&snapshot_path(&self.config.path), &records)?;
        for shard in &self.shards {
            shard.write().retain(|_, entry| entry.value.is_some());
        }
        Ok(records.len())
    }

    pub fn sync(&self) -> Result<()> {
        self.wal.lock().sync()
    }

    pub fn stats(&self) -> Result<DatabaseStats> {
        let (live_keys, tombstones, compressed, raw, logical_bytes, stored_bytes) = self
            .shards
            .par_iter()
            .map(|shard| {
                shard.read().values().fold(
                    (0usize, 0usize, 0usize, 0usize, 0u64, 0u64),
                    |(live, dead, compressed, raw, logical, stored), entry| match &entry.value {
                        Some(value) => (
                            live + 1,
                            dead,
                            compressed + usize::from(value.is_compressed()),
                            raw + usize::from(!value.is_compressed()),
                            logical + value.logical_len() as u64,
                            stored + value.stored_len() as u64,
                        ),
                        None => (live, dead + 1, compressed, raw, logical, stored),
                    },
                )
            })
            .reduce(
                || (0, 0, 0, 0, 0, 0),
                |a, b| {
                    (
                        a.0 + b.0,
                        a.1 + b.1,
                        a.2 + b.2,
                        a.3 + b.3,
                        a.4 + b.4,
                        a.5 + b.5,
                    )
                },
            );
        Ok(DatabaseStats {
            live_keys,
            tombstones,
            shards: self.shards.len(),
            sequence: self.sequence.load(Ordering::Acquire),
            wal_bytes: fs::metadata(wal_path(&self.config.path))?.len(),
            rayon_threads: rayon::current_num_threads(),
            gpu_compiled: cfg!(feature = "gpu"),
            compressed_values: compressed,
            raw_values: raw,
            logical_value_bytes: logical_bytes,
            stored_value_bytes: stored_bytes,
            compression_ratio: if logical_bytes == 0 {
                1.0
            } else {
                stored_bytes as f64 / logical_bytes as f64
            },
            compression_channels: self.compression.len(),
        })
    }

    pub fn initialize_gpu(&self) -> Result<String> {
        #[cfg(feature = "gpu")]
        {
            let mut state = self.gpu.lock();
            match &*state {
                GpuState::Ready(executor) => return Ok(executor.adapter_name().to_owned()),
                GpuState::Unavailable(error) => {
                    return Err(AproError::GpuUnavailable(error.clone()));
                }
                GpuState::Uninitialized => {}
            }
            match GpuExecutor::new() {
                Ok(executor) => {
                    let name = executor.adapter_name().to_owned();
                    *state = GpuState::Ready(executor);
                    Ok(name)
                }
                Err(error) => {
                    let message = error.to_string();
                    *state = GpuState::Unavailable(message.clone());
                    Err(AproError::GpuUnavailable(message))
                }
            }
        }
        #[cfg(not(feature = "gpu"))]
        {
            Err(AproError::GpuUnavailable(
                "binario compilato senza la feature `gpu`".into(),
            ))
        }
    }

    fn apply_record(&self, record: &Record) {
        let mut shard = self.shards[self.shard_index(&record.key)].write();
        if shard
            .get(&record.key)
            .is_some_and(|current| current.sequence > record.sequence)
        {
            return;
        }
        let value = match &record.operation {
            Operation::Put(value) => Some(value.clone()),
            Operation::Delete => None,
        };
        shard.insert(
            record.key.clone(),
            Entry {
                sequence: record.sequence,
                value,
            },
        );
    }

    fn shard_index(&self, key: &str) -> usize {
        xxh3_64(key.as_bytes()) as usize & (self.shards.len() - 1)
    }

    fn compression_index(&self, key: &str) -> usize {
        xxh3_64(key.as_bytes()) as usize & (self.compression.len() - 1)
    }

    fn compress_value(&self, key: &str, value: &Value) -> Result<StoredValue> {
        self.compression[self.compression_index(key)]
            .lock()
            .compress(value)
    }

    fn decompress_value(&self, key: &str, value: &StoredValue) -> Result<Value> {
        self.compression[self.compression_index(key)]
            .lock()
            .decompress(value)
    }

    #[cfg(feature = "gpu")]
    fn score_vectors_gpu(
        &self,
        vectors: &[Vec<f32>],
        query: &[f32],
        metric: Metric,
    ) -> Result<(Vec<f32>, String)> {
        self.initialize_gpu()?;
        let state = self.gpu.lock();
        let GpuState::Ready(executor) = &*state else {
            return Err(AproError::GpuUnavailable("inizializzazione fallita".into()));
        };
        let scores = executor.score(vectors, query, metric)?;
        Ok((scores, executor.adapter_name().to_owned()))
    }

    #[cfg(not(feature = "gpu"))]
    fn score_vectors_gpu(
        &self,
        _vectors: &[Vec<f32>],
        _query: &[f32],
        _metric: Metric,
    ) -> Result<(Vec<f32>, String)> {
        Err(AproError::GpuUnavailable(
            "binario compilato senza la feature `gpu`".into(),
        ))
    }
}

fn validate_config(config: &Config) -> Result<()> {
    if config.shards == 0 || !config.shards.is_power_of_two() {
        return Err(AproError::InvalidValue(
            "il numero di shard deve essere una potenza di due".into(),
        ));
    }
    if config.compression_channels == 0 || !config.compression_channels.is_power_of_two() {
        return Err(AproError::InvalidValue(
            "i canali di compressione devono essere una potenza di due".into(),
        ));
    }
    if config.compression_channels > 64 {
        return Err(AproError::InvalidValue(
            "sono ammessi al massimo 64 canali di compressione".into(),
        ));
    }
    Ok(())
}

fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() {
        return Err(AproError::InvalidKey(
            "la chiave non può essere vuota".into(),
        ));
    }
    if key.len() > MAX_KEY_BYTES {
        return Err(AproError::InvalidKey(format!(
            "la chiave supera {MAX_KEY_BYTES} byte"
        )));
    }
    Ok(())
}

fn validate_query(query: &[f32]) -> Result<()> {
    if query.is_empty() {
        return Err(AproError::InvalidVector("query vuota".into()));
    }
    if query.iter().any(|value| !value.is_finite()) {
        return Err(AproError::InvalidVector(
            "la query contiene numeri non finiti".into(),
        ));
    }
    Ok(())
}

fn wal_path(root: &Path) -> PathBuf {
    root.join("aprodb.wal")
}

fn snapshot_path(root: &Path) -> PathBuf {
    root.join("aprodb.snapshot")
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
