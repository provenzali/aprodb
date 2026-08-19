use std::sync::Arc;

use crate::{AproError, Result, Value};

const STORED_FORMAT_VERSION: u8 = 1;
const CODEC_RAW: u8 = 0;
const CODEC_ZSTD: u8 = 1;
const STORED_HEADER_LEN: usize = 8;
const MIN_GAIN_BYTES: usize = 8;
const MAX_LOGICAL_BYTES: usize = 256 * 1024 * 1024;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ValueKind {
    Bytes = 1,
    Text = 2,
    Integer = 3,
    Float = 4,
    Vector = 5,
}

#[derive(Clone, Debug)]
pub(crate) struct StoredValue {
    codec: u8,
    kind: ValueKind,
    logical_len: u32,
    payload: Arc<[u8]>,
}

impl StoredValue {
    pub(crate) fn is_vector(&self) -> bool {
        self.kind == ValueKind::Vector
    }

    pub(crate) fn is_compressed(&self) -> bool {
        self.codec == CODEC_ZSTD
    }

    pub(crate) fn logical_len(&self) -> usize {
        self.logical_len as usize
    }

    pub(crate) fn stored_len(&self) -> usize {
        STORED_HEADER_LEN + self.payload.len()
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(self.stored_len());
        output.push(STORED_FORMAT_VERSION);
        output.push(self.codec);
        output.push(self.kind as u8);
        output.push(0);
        output.extend_from_slice(&self.logical_len.to_le_bytes());
        output.extend_from_slice(&self.payload);
        output
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < STORED_HEADER_LEN {
            return Err(AproError::Corrupt(
                "header del valore memorizzato incompleto".into(),
            ));
        }
        if bytes[0] != STORED_FORMAT_VERSION {
            return Err(AproError::Corrupt(format!(
                "versione del valore memorizzato non supportata: {}",
                bytes[0]
            )));
        }
        let codec = bytes[1];
        if codec != CODEC_RAW && codec != CODEC_ZSTD {
            return Err(AproError::Corrupt(format!(
                "codec di compressione sconosciuto: {codec}"
            )));
        }
        let kind = ValueKind::from_tag(bytes[2])?;
        let logical_len = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        if logical_len as usize > MAX_LOGICAL_BYTES {
            return Err(AproError::Corrupt(
                "valore logico oltre il limite massimo".into(),
            ));
        }
        let payload: Arc<[u8]> = bytes[STORED_HEADER_LEN..].into();
        if codec == CODEC_RAW && payload.len() != logical_len as usize {
            return Err(AproError::Corrupt(
                "lunghezza del valore raw incoerente".into(),
            ));
        }
        Ok(Self {
            codec,
            kind,
            logical_len,
            payload,
        })
    }
}

impl ValueKind {
    fn of(value: &Value) -> Self {
        match value {
            Value::Bytes(_) => Self::Bytes,
            Value::Text(_) => Self::Text,
            Value::Integer(_) => Self::Integer,
            Value::Float(_) => Self::Float,
            Value::Vector(_) => Self::Vector,
        }
    }

    fn from_tag(tag: u8) -> Result<Self> {
        match tag {
            1 => Ok(Self::Bytes),
            2 => Ok(Self::Text),
            3 => Ok(Self::Integer),
            4 => Ok(Self::Float),
            5 => Ok(Self::Vector),
            _ => Err(AproError::Corrupt(format!(
                "tipo del valore memorizzato sconosciuto: {tag}"
            ))),
        }
    }
}

pub(crate) struct CompressionChannel {
    compressor: zstd::bulk::Compressor<'static>,
    decompressor: zstd::bulk::Decompressor<'static>,
    min_size: usize,
}

impl CompressionChannel {
    pub(crate) fn new(level: i32, min_size: usize) -> Result<Self> {
        let compressor = zstd::bulk::Compressor::new(level)
            .map_err(|error| AproError::InvalidValue(format!("Zstd: {error}")))?;
        let decompressor = zstd::bulk::Decompressor::new()
            .map_err(|error| AproError::InvalidValue(format!("Zstd: {error}")))?;
        Ok(Self {
            compressor,
            decompressor,
            min_size,
        })
    }

    pub(crate) fn compress(&mut self, value: &Value) -> Result<StoredValue> {
        let raw = value.encode();
        if raw.len() > MAX_LOGICAL_BYTES {
            return Err(AproError::InvalidValue(format!(
                "valore oltre il limite di {MAX_LOGICAL_BYTES} byte"
            )));
        }
        let logical_len: u32 = raw
            .len()
            .try_into()
            .map_err(|_| AproError::InvalidValue("valore oltre 4 GiB".into()))?;
        let compressed = if raw.len() >= self.min_size {
            Some(
                self.compressor
                    .compress(&raw)
                    .map_err(|error| AproError::InvalidValue(format!("Zstd: {error}")))?,
            )
        } else {
            None
        };
        let (codec, payload): (u8, Arc<[u8]>) = match compressed {
            Some(candidate) if candidate.len() + MIN_GAIN_BYTES < raw.len() => {
                (CODEC_ZSTD, candidate.into())
            }
            _ => (CODEC_RAW, raw.into()),
        };
        Ok(StoredValue {
            codec,
            kind: ValueKind::of(value),
            logical_len,
            payload,
        })
    }

    pub(crate) fn decompress(&mut self, stored: &StoredValue) -> Result<Value> {
        let bytes = match stored.codec {
            CODEC_RAW => stored.payload.to_vec(),
            CODEC_ZSTD => self
                .decompressor
                .decompress(&stored.payload, stored.logical_len as usize)
                .map_err(|error| AproError::Corrupt(format!("frame Zstd non valido: {error}")))?,
            codec => {
                return Err(AproError::Corrupt(format!(
                    "codec di compressione sconosciuto: {codec}"
                )));
            }
        };
        if bytes.len() != stored.logical_len as usize {
            return Err(AproError::Corrupt(
                "lunghezza dopo decompressione incoerente".into(),
            ));
        }
        let value = Value::decode(&bytes)?;
        if ValueKind::of(&value) != stored.kind {
            return Err(AproError::Corrupt(
                "tipo dichiarato e tipo decompresso non coincidono".into(),
            ));
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::CompressionChannel;
    use crate::Value;

    #[test]
    fn adaptive_compression_round_trip() {
        let mut channel = CompressionChannel::new(1, 16).unwrap();
        let compressible = Value::Text("abc".repeat(1_000));
        let stored = channel.compress(&compressible).unwrap();
        assert!(stored.is_compressed());
        assert!(stored.stored_len() < stored.logical_len());
        assert_eq!(channel.decompress(&stored).unwrap(), compressible);

        let small = Value::Integer(42);
        let stored = channel.compress(&small).unwrap();
        assert!(!stored.is_compressed());
        assert_eq!(channel.decompress(&stored).unwrap(), small);
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
