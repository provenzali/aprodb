use std::io;

#[derive(Debug, thiserror::Error)]
pub enum AproError {
    #[error("errore I/O: {0}")]
    Io(#[from] io::Error),

    #[error("dati persistenti corrotti: {0}")]
    Corrupt(String),

    #[error("chiave non valida: {0}")]
    InvalidKey(String),

    #[error("valore non valido: {0}")]
    InvalidValue(String),

    #[error("vettore non valido: {0}")]
    InvalidVector(String),

    #[error("GPU non disponibile: {0}")]
    GpuUnavailable(String),

    #[error("errore GPU: {0}")]
    Gpu(String),
}

pub type Result<T> = std::result::Result<T, AproError>;
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
