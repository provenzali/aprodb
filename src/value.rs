use serde::{Deserialize, Serialize};

use crate::{AproError, Result};

const TAG_BYTES: u8 = 1;
const TAG_TEXT: u8 = 2;
const TAG_INTEGER: u8 = 3;
const TAG_FLOAT: u8 = 4;
const TAG_VECTOR: u8 = 5;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
pub enum Value {
    Bytes(Vec<u8>),
    Text(String),
    Integer(i64),
    Float(f64),
    Vector(Vec<f32>),
}

impl Value {
    pub(crate) fn validate(&self) -> Result<()> {
        match self {
            Self::Vector(values) => {
                if values.is_empty() {
                    return Err(AproError::InvalidVector(
                        "la dimensione deve essere maggiore di zero".into(),
                    ));
                }
                if values.iter().any(|value| !value.is_finite()) {
                    return Err(AproError::InvalidVector(
                        "sono ammessi solo numeri finiti".into(),
                    ));
                }
            }
            Self::Float(value) if !value.is_finite() => {
                return Err(AproError::InvalidValue(
                    "sono ammessi solo numeri finiti".into(),
                ));
            }
            _ => {}
        }
        Ok(())
    }

    pub(crate) fn encode(&self) -> Vec<u8> {
        match self {
            Self::Bytes(value) => {
                let mut out = Vec::with_capacity(1 + value.len());
                out.push(TAG_BYTES);
                out.extend_from_slice(value);
                out
            }
            Self::Text(value) => {
                let mut out = Vec::with_capacity(1 + value.len());
                out.push(TAG_TEXT);
                out.extend_from_slice(value.as_bytes());
                out
            }
            Self::Integer(value) => {
                let mut out = Vec::with_capacity(9);
                out.push(TAG_INTEGER);
                out.extend_from_slice(&value.to_le_bytes());
                out
            }
            Self::Float(value) => {
                let mut out = Vec::with_capacity(9);
                out.push(TAG_FLOAT);
                out.extend_from_slice(&value.to_le_bytes());
                out
            }
            Self::Vector(values) => {
                let mut out = Vec::with_capacity(5 + values.len() * 4);
                out.push(TAG_VECTOR);
                out.extend_from_slice(&(values.len() as u32).to_le_bytes());
                for value in values {
                    out.extend_from_slice(&value.to_le_bytes());
                }
                out
            }
        }
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self> {
        let Some((&tag, body)) = bytes.split_first() else {
            return Err(AproError::Corrupt("valore vuoto".into()));
        };

        let value = match tag {
            TAG_BYTES => Self::Bytes(body.to_vec()),
            TAG_TEXT => Self::Text(
                String::from_utf8(body.to_vec())
                    .map_err(|_| AproError::Corrupt("testo UTF-8 non valido".into()))?,
            ),
            TAG_INTEGER => Self::Integer(i64::from_le_bytes(array8(body, "intero")?)),
            TAG_FLOAT => Self::Float(f64::from_le_bytes(array8(body, "float")?)),
            TAG_VECTOR => {
                if body.len() < 4 {
                    return Err(AproError::Corrupt("header vettore incompleto".into()));
                }
                let count = u32::from_le_bytes(body[..4].try_into().unwrap()) as usize;
                let expected = count
                    .checked_mul(4)
                    .and_then(|n| n.checked_add(4))
                    .ok_or_else(|| AproError::Corrupt("dimensione vettore eccessiva".into()))?;
                if body.len() != expected {
                    return Err(AproError::Corrupt("dimensione vettore incoerente".into()));
                }
                let values = body[4..]
                    .chunks_exact(4)
                    .map(|chunk| f32::from_le_bytes(chunk.try_into().unwrap()))
                    .collect();
                Self::Vector(values)
            }
            _ => {
                return Err(AproError::Corrupt(format!(
                    "tipo valore sconosciuto: {tag}"
                )));
            }
        };
        value.validate()?;
        Ok(value)
    }
}

fn array8(bytes: &[u8], kind: &str) -> Result<[u8; 8]> {
    bytes
        .try_into()
        .map_err(|_| AproError::Corrupt(format!("{kind} di lunghezza non valida")))
}

#[cfg(test)]
mod tests {
    use super::Value;

    #[test]
    fn value_round_trip() {
        let values = [
            Value::Bytes(vec![0, 1, 255]),
            Value::Text("ciao 🚀".into()),
            Value::Integer(-42),
            Value::Float(3.25),
            Value::Vector(vec![1.0, -2.0, 4.5]),
        ];
        for value in values {
            assert_eq!(Value::decode(&value.encode()).unwrap(), value);
        }
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
