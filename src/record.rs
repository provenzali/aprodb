use std::io::{Read, Write};

use crate::{AproError, Result, compression::StoredValue};

const FRAME_MAGIC: [u8; 4] = *b"APRF";
const HEADER_LEN: usize = 12;
const MAX_FRAME_SIZE: usize = 256 * 1024 * 1024;

#[derive(Clone, Debug)]
pub(crate) enum Operation {
    Put(StoredValue),
    Delete,
}

#[derive(Clone, Debug)]
pub(crate) struct Record {
    pub sequence: u64,
    pub key: String,
    pub operation: Operation,
}

pub(crate) enum FrameRead {
    Record(Record),
    Eof,
    Truncated,
}

pub(crate) fn write_frame(mut writer: impl Write, record: &Record) -> Result<usize> {
    let payload = encode_record(record)?;
    if payload.len() > MAX_FRAME_SIZE {
        return Err(AproError::InvalidValue(format!(
            "record oltre il limite di {MAX_FRAME_SIZE} byte"
        )));
    }
    let mut header = [0u8; HEADER_LEN];
    header[..4].copy_from_slice(&FRAME_MAGIC);
    header[4..8].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    header[8..12].copy_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    writer.write_all(&header)?;
    writer.write_all(&payload)?;
    Ok(HEADER_LEN + payload.len())
}

pub(crate) fn read_frame(mut reader: impl Read) -> Result<FrameRead> {
    let mut header = [0u8; HEADER_LEN];
    let mut read = 0;
    while read < header.len() {
        match reader.read(&mut header[read..])? {
            0 if read == 0 => return Ok(FrameRead::Eof),
            0 => return Ok(FrameRead::Truncated),
            count => read += count,
        }
    }

    if header[..4] != FRAME_MAGIC {
        return Err(AproError::Corrupt("magic del frame non valido".into()));
    }
    let payload_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
    if payload_len == 0 || payload_len > MAX_FRAME_SIZE {
        return Err(AproError::Corrupt(format!(
            "lunghezza frame non valida: {payload_len}"
        )));
    }
    let checksum = u32::from_le_bytes(header[8..12].try_into().unwrap());
    let mut payload = vec![0u8; payload_len];
    let mut read = 0;
    while read < payload.len() {
        match reader.read(&mut payload[read..])? {
            0 => return Ok(FrameRead::Truncated),
            count => read += count,
        }
    }
    if crc32fast::hash(&payload) != checksum {
        return Err(AproError::Corrupt("checksum del frame non valido".into()));
    }
    Ok(FrameRead::Record(decode_record(&payload)?))
}

fn encode_record(record: &Record) -> Result<Vec<u8>> {
    let key = record.key.as_bytes();
    let key_len: u32 = key
        .len()
        .try_into()
        .map_err(|_| AproError::InvalidKey("chiave troppo lunga".into()))?;
    let (kind, value) = match &record.operation {
        Operation::Put(value) => (1u8, value.encode()),
        Operation::Delete => (2u8, Vec::new()),
    };
    let value_len: u32 = value
        .len()
        .try_into()
        .map_err(|_| AproError::InvalidValue("valore troppo grande".into()))?;

    let mut out = Vec::with_capacity(17 + key.len() + value.len());
    out.extend_from_slice(&record.sequence.to_le_bytes());
    out.push(kind);
    out.extend_from_slice(&key_len.to_le_bytes());
    out.extend_from_slice(&value_len.to_le_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&value);
    Ok(out)
}

fn decode_record(payload: &[u8]) -> Result<Record> {
    if payload.len() < 17 {
        return Err(AproError::Corrupt("record incompleto".into()));
    }
    let sequence = u64::from_le_bytes(payload[..8].try_into().unwrap());
    let kind = payload[8];
    let key_len = u32::from_le_bytes(payload[9..13].try_into().unwrap()) as usize;
    let value_len = u32::from_le_bytes(payload[13..17].try_into().unwrap()) as usize;
    let expected = 17usize
        .checked_add(key_len)
        .and_then(|n| n.checked_add(value_len))
        .ok_or_else(|| AproError::Corrupt("lunghezza record eccessiva".into()))?;
    if payload.len() != expected {
        return Err(AproError::Corrupt("lunghezza record incoerente".into()));
    }
    let key = String::from_utf8(payload[17..17 + key_len].to_vec())
        .map_err(|_| AproError::Corrupt("chiave UTF-8 non valida".into()))?;
    let value_bytes = &payload[17 + key_len..];
    let operation = match kind {
        1 => Operation::Put(StoredValue::decode(value_bytes)?),
        2 if value_bytes.is_empty() => Operation::Delete,
        2 => return Err(AproError::Corrupt("delete con payload inatteso".into())),
        _ => {
            return Err(AproError::Corrupt(format!(
                "operazione sconosciuta: {kind}"
            )));
        }
    };
    Ok(Record {
        sequence,
        key,
        operation,
    })
}

#[cfg(test)]
mod tests {
    use super::{FrameRead, Operation, Record, read_frame, write_frame};
    use crate::{Value, compression::CompressionChannel};

    #[test]
    fn v0_1_put_frame_matches_golden_bytes() {
        let mut compression = CompressionChannel::new(1, 32).unwrap();
        let stored = compression.compress(&Value::Text("ciao".into())).unwrap();
        let record = Record {
            sequence: 7,
            key: "key".into(),
            operation: Operation::Put(stored),
        };
        let mut bytes = Vec::new();
        write_frame(&mut bytes, &record).unwrap();
        let expected =
            hex::decode(include_str!("../tests/golden/v0_1_put_frame.hex").trim()).unwrap();
        assert_eq!(bytes, expected);
        match read_frame(bytes.as_slice()).unwrap() {
            FrameRead::Record(decoded) => {
                assert_eq!(decoded.sequence, 7);
                assert_eq!(decoded.key, "key");
                assert!(matches!(decoded.operation, Operation::Put(_)));
            }
            FrameRead::Eof | FrameRead::Truncated => panic!("frame golden non leggibile"),
        }
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
