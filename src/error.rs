use std::io;

#[derive(Debug, thiserror::Error)]
pub enum AproError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("persistent data corrupted: {0}")]
    Corrupt(String),

    #[error("invalid key: {0}")]
    InvalidKey(String),

    #[error("invalid value: {0}")]
    InvalidValue(String),

    #[error("invalid vector: {0}")]
    InvalidVector(String),

    #[error("GPU unavailable: {0}")]
    GpuUnavailable(String),

    #[error("GPU error: {0}")]
    Gpu(String),
}

pub type Result<T> = std::result::Result<T, AproError>;
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
