use std::{
    sync::atomic::{AtomicU64, AtomicUsize, Ordering},
    time::Instant,
};

use aprodb_types::{
    AproError, CompressionCodec, CompressionDictionary, CompressionMode, CompressionTierPolicy,
    Limits, LogicalFrameKind, Payload, RecordEnvelope, Result, StoredPayload, StoredRecordEnvelope,
    decode_logical, encode_logical,
};
use parking_lot::Mutex;

const CODEC_VERSION: u8 = 1;

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompressionStats {
    pub logical_bytes: u64,
    pub stored_payload_bytes: u64,
    pub raw_payloads: u64,
    pub zstandard_payloads: u64,
    pub dictionary_payloads: u64,
    pub adaptive_fallbacks: u64,
    pub skipped_content_types: u64,
    pub compression_micros: u64,
    pub decompression_micros: u64,
    pub failures: u64,
    pub channels: usize,
    pub scratch_budget_bytes: usize,
    pub scratch_inflight_bytes: usize,
}

struct CodecChannel {
    compressor: zstd::bulk::Compressor<'static>,
    decompressor: zstd::bulk::Decompressor<'static>,
}

impl CodecChannel {
    fn new() -> Result<Self> {
        Ok(Self {
            compressor: zstd::bulk::Compressor::new(1).map_err(|error| {
                AproError::Storage(format!("Zstandard initialization error: {error}"))
            })?,
            decompressor: zstd::bulk::Decompressor::new().map_err(|error| {
                AproError::Storage(format!("inizializzazione Zstandard: {error}"))
            })?,
        })
    }
}

pub(crate) struct CompressionManager {
    channels: Vec<Mutex<CodecChannel>>,
    scratch_budget_bytes: usize,
    scratch_inflight_bytes: AtomicUsize,
    logical_bytes: AtomicU64,
    stored_payload_bytes: AtomicU64,
    raw_payloads: AtomicU64,
    zstandard_payloads: AtomicU64,
    dictionary_payloads: AtomicU64,
    adaptive_fallbacks: AtomicU64,
    skipped_content_types: AtomicU64,
    compression_micros: AtomicU64,
    decompression_micros: AtomicU64,
    failures: AtomicU64,
}

impl CompressionManager {
    pub(crate) fn new(channels: usize, scratch_budget_bytes: usize) -> Result<Self> {
        if channels == 0 || !channels.is_power_of_two() || channels > 64 {
            return Err(AproError::InvalidInput(
                "codec channels must be a power of two between 1 and 64".into(),
            ));
        }
        if scratch_budget_bytes == 0 {
            return Err(AproError::InvalidInput(
                "codec scratch budget must be positive".into(),
            ));
        }
        Ok(Self {
            channels: (0..channels)
                .map(|_| CodecChannel::new().map(Mutex::new))
                .collect::<Result<_>>()?,
            scratch_budget_bytes,
            scratch_inflight_bytes: AtomicUsize::new(0),
            logical_bytes: AtomicU64::new(0),
            stored_payload_bytes: AtomicU64::new(0),
            raw_payloads: AtomicU64::new(0),
            zstandard_payloads: AtomicU64::new(0),
            dictionary_payloads: AtomicU64::new(0),
            adaptive_fallbacks: AtomicU64::new(0),
            skipped_content_types: AtomicU64::new(0),
            compression_micros: AtomicU64::new(0),
            decompression_micros: AtomicU64::new(0),
            failures: AtomicU64::new(0),
        })
    }

    pub(crate) fn encode_record(
        &self,
        record: &mut RecordEnvelope,
        policy: &CompressionTierPolicy,
        skip_content_type: bool,
        dictionary: Option<&CompressionDictionary>,
        channel_hash: u64,
    ) -> Result<Vec<u8>> {
        let payload = record
            .payload
            .as_ref()
            .map(|payload| {
                self.encode_payload(payload, policy, skip_content_type, dictionary, channel_hash)
            })
            .transpose()?;
        record.dictionary_id = payload.as_ref().and_then(|payload| payload.dictionary_id);
        let stored = StoredRecordEnvelope {
            identity: record.identity.clone(),
            payload,
            content_type: record.content_type.clone(),
            version: record.version,
            created_at_unix_ms: record.created_at_unix_ms,
            updated_at_unix_ms: record.updated_at_unix_ms,
            expires_at_unix_ms: record.expires_at_unix_ms,
            metadata: record.metadata.clone(),
            workflow: record.workflow.clone(),
            idempotency_key_hash: record.idempotency_key_hash,
            tombstone: record.tombstone,
        };
        encode_logical(LogicalFrameKind::StoredRecord, &stored)
    }

    pub(crate) fn decode_record(
        &self,
        bytes: &[u8],
        dictionary: Option<&CompressionDictionary>,
        limits: &Limits,
        channel_hash: u64,
    ) -> Result<RecordEnvelope> {
        if bytes.starts_with(b"APRC") {
            let record: RecordEnvelope = decode_logical(LogicalFrameKind::Record, bytes)?;
            record.validate(limits)?;
            return Ok(record);
        }
        let stored: StoredRecordEnvelope = decode_logical(LogicalFrameKind::StoredRecord, bytes)?;
        if stored.tombstone != stored.payload.is_none() {
            return Err(AproError::Corrupt(
                "stored record with inconsistent tombstone/payload".into(),
            ));
        }
        let dictionary_id = stored
            .payload
            .as_ref()
            .and_then(|payload| payload.dictionary_id);
        let payload = stored
            .payload
            .as_ref()
            .map(|payload| self.decode_payload(payload, dictionary, channel_hash))
            .transpose()?;
        let record = RecordEnvelope {
            identity: stored.identity,
            payload,
            content_type: stored.content_type,
            version: stored.version,
            created_at_unix_ms: stored.created_at_unix_ms,
            updated_at_unix_ms: stored.updated_at_unix_ms,
            expires_at_unix_ms: stored.expires_at_unix_ms,
            metadata: stored.metadata,
            workflow: stored.workflow,
            idempotency_key_hash: stored.idempotency_key_hash,
            dictionary_id,
            tombstone: stored.tombstone,
        };
        record.validate(limits)?;
        Ok(record)
    }

    pub(crate) fn stored_dictionary_id(&self, bytes: &[u8]) -> Result<Option<u64>> {
        if bytes.starts_with(b"APRC") {
            return Ok(None);
        }
        let stored: StoredRecordEnvelope = decode_logical(LogicalFrameKind::StoredRecord, bytes)?;
        Ok(stored
            .payload
            .as_ref()
            .and_then(|payload| payload.dictionary_id))
    }

    pub(crate) fn compressed_size(
        &self,
        bytes: &[u8],
        level: i32,
        dictionary: Option<&[u8]>,
        channel_hash: u64,
    ) -> Result<usize> {
        let _scratch = self.acquire_scratch(scratch_requirement(bytes.len()))?;
        let mut channel = self.channels[self.channel_index(channel_hash)].lock();
        channel
            .compressor
            .set_dictionary(level, dictionary.unwrap_or_default())
            .map_err(|error| AproError::InvalidInput(format!("Zstandard dictionary: {error}")))?;
        channel
            .compressor
            .compress(bytes)
            .map(|bytes| bytes.len())
            .map_err(|error| AproError::InvalidInput(format!("compressione Zstandard: {error}")))
    }

    #[must_use]
    pub(crate) fn stats(&self) -> CompressionStats {
        CompressionStats {
            logical_bytes: self.logical_bytes.load(Ordering::Acquire),
            stored_payload_bytes: self.stored_payload_bytes.load(Ordering::Acquire),
            raw_payloads: self.raw_payloads.load(Ordering::Acquire),
            zstandard_payloads: self.zstandard_payloads.load(Ordering::Acquire),
            dictionary_payloads: self.dictionary_payloads.load(Ordering::Acquire),
            adaptive_fallbacks: self.adaptive_fallbacks.load(Ordering::Acquire),
            skipped_content_types: self.skipped_content_types.load(Ordering::Acquire),
            compression_micros: self.compression_micros.load(Ordering::Acquire),
            decompression_micros: self.decompression_micros.load(Ordering::Acquire),
            failures: self.failures.load(Ordering::Acquire),
            channels: self.channels.len(),
            scratch_budget_bytes: self.scratch_budget_bytes,
            scratch_inflight_bytes: self.scratch_inflight_bytes.load(Ordering::Acquire),
        }
    }

    fn encode_payload(
        &self,
        payload: &Payload,
        policy: &CompressionTierPolicy,
        skip_content_type: bool,
        dictionary: Option<&CompressionDictionary>,
        channel_hash: u64,
    ) -> Result<StoredPayload> {
        let started = Instant::now();
        let raw = bincode::serde::encode_to_vec(payload, bincode::config::standard())
            .map_err(|error| AproError::InvalidInput(format!("payload encoding: {error}")))?;
        let logical_bytes = u64::try_from(raw.len())
            .map_err(|_| AproError::ResourceLimit("logical payload size exceeds u64".into()))?;
        let _scratch = self.acquire_scratch(scratch_requirement(raw.len()))?;
        let mut selected_dictionary_id = None;
        let mut codec = CompressionCodec::Raw;
        let mut stored_bytes = raw.clone();
        if skip_content_type {
            self.skipped_content_types.fetch_add(1, Ordering::Relaxed);
        } else if policy.mode == CompressionMode::AdaptiveZstandard
            && raw.len() >= policy.min_input_bytes
        {
            let dictionary_bytes = dictionary.map(|dictionary| dictionary.bytes.as_slice());
            let candidate = (|| {
                let mut channel = self.channels[self.channel_index(channel_hash)].lock();
                channel
                    .compressor
                    .set_dictionary(policy.zstd_level, dictionary_bytes.unwrap_or_default())
                    .map_err(|error| {
                        AproError::InvalidInput(format!("Zstandard dictionary error: {error}"))
                    })?;
                channel.compressor.compress(&raw).map_err(|error| {
                    AproError::Storage(format!("Zstandard compression error: {error}"))
                })
            })();
            let candidate = candidate.inspect_err(|_| {
                self.failures.fetch_add(1, Ordering::Relaxed);
            })?;
            if candidate.len().saturating_add(policy.min_savings_bytes) < raw.len() {
                codec = CompressionCodec::Zstandard;
                stored_bytes = candidate;
                selected_dictionary_id = dictionary.map(|dictionary| dictionary.id);
                self.zstandard_payloads.fetch_add(1, Ordering::Relaxed);
                if selected_dictionary_id.is_some() {
                    self.dictionary_payloads.fetch_add(1, Ordering::Relaxed);
                }
            } else {
                self.adaptive_fallbacks.fetch_add(1, Ordering::Relaxed);
            }
        }
        if codec == CompressionCodec::Raw {
            self.raw_payloads.fetch_add(1, Ordering::Relaxed);
        }
        self.logical_bytes
            .fetch_add(logical_bytes, Ordering::Relaxed);
        self.stored_payload_bytes.fetch_add(
            u64::try_from(stored_bytes.len()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.compression_micros.fetch_add(
            u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        Ok(StoredPayload {
            codec_version: CODEC_VERSION,
            codec,
            dictionary_id: selected_dictionary_id,
            logical_bytes,
            logical_checksum: crc32fast::hash(&raw),
            bytes: stored_bytes,
        })
    }

    fn decode_payload(
        &self,
        stored: &StoredPayload,
        dictionary: Option<&CompressionDictionary>,
        channel_hash: u64,
    ) -> Result<Payload> {
        let started = Instant::now();
        if stored.codec_version != CODEC_VERSION {
            return Err(AproError::IncompatibleFormat(format!(
                "payload codec version {} not supported",
                stored.codec_version
            )));
        }
        let logical_bytes = usize::try_from(stored.logical_bytes)
            .map_err(|_| AproError::Corrupt("payload length exceeds usize".into()))?;
        let _scratch = self.acquire_scratch(scratch_requirement(logical_bytes))?;
        let raw = match stored.codec {
            CompressionCodec::Raw => {
                if stored.dictionary_id.is_some() || stored.bytes.len() != logical_bytes {
                    return Err(AproError::Corrupt(
                        "Raw payload with dictionary or inconsistent length".into(),
                    ));
                }
                stored.bytes.clone()
            }
            CompressionCodec::Zstandard => {
                let dictionary_bytes = match (stored.dictionary_id, dictionary) {
                    (Some(expected), Some(dictionary)) if dictionary.id == expected => {
                        Some(dictionary.bytes.as_slice())
                    }
                    (Some(expected), _) => {
                        return Err(AproError::Corrupt(format!(
                            "dictionary {expected} requested by payload not available"
                        )));
                    }
                    (None, _) => None,
                };
                let result = (|| {
                    let mut channel = self.channels[self.channel_index(channel_hash)].lock();
                    channel
                        .decompressor
                        .set_dictionary(dictionary_bytes.unwrap_or_default())
                        .map_err(|error| {
                            AproError::Corrupt(format!("Zstandard dictionary: {error}"))
                        })?;
                    channel
                        .decompressor
                        .decompress(&stored.bytes, logical_bytes)
                        .map_err(|error| AproError::Corrupt(format!("Zstandard payload: {error}")))
                })();
                result.inspect_err(|_| {
                    self.failures.fetch_add(1, Ordering::Relaxed);
                })?
            }
        };
        if raw.len() != logical_bytes || crc32fast::hash(&raw) != stored.logical_checksum {
            return Err(AproError::Corrupt(
                "invalid length or checksum of decompressed payload".into(),
            ));
        }
        let (payload, consumed): (Payload, usize) =
            bincode::serde::decode_from_slice(&raw, bincode::config::standard())
                .map_err(|error| AproError::Corrupt(format!("payload decoding: {error}")))?;
        if consumed != raw.len() {
            return Err(AproError::Corrupt(
                "residual bytes after decompressed payload".into(),
            ));
        }
        self.decompression_micros.fetch_add(
            u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        Ok(payload)
    }

    fn channel_index(&self, hash: u64) -> usize {
        hash as usize & (self.channels.len() - 1)
    }

    fn acquire_scratch(&self, bytes: usize) -> Result<ScratchGuard<'_>> {
        let mut current = self.scratch_inflight_bytes.load(Ordering::Acquire);
        loop {
            let next = current
                .checked_add(bytes)
                .ok_or_else(|| AproError::ResourceLimit("codec scratch counter exceeded".into()))?;
            if next > self.scratch_budget_bytes {
                return Err(AproError::Backpressure(format!(
                    "codec scratch {next} over budget {}",
                    self.scratch_budget_bytes
                )));
            }
            match self.scratch_inflight_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    return Ok(ScratchGuard {
                        manager: self,
                        bytes,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }
}

struct ScratchGuard<'a> {
    manager: &'a CompressionManager,
    bytes: usize,
}

impl Drop for ScratchGuard<'_> {
    fn drop(&mut self) {
        self.manager
            .scratch_inflight_bytes
            .fetch_sub(self.bytes, Ordering::AcqRel);
    }
}

fn scratch_requirement(logical_bytes: usize) -> usize {
    logical_bytes.saturating_mul(2).saturating_add(64 * 1024)
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
