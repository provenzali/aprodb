//! AProDB: motore key-value tipizzato con WAL, sharding e ricerca vettoriale parallela.

mod compression;
mod compute;
mod engine;
mod error;
mod migration;
mod record;
mod snapshot;
mod value;
mod wal;

pub use engine::{
    ComputeBackend, Config, Database, DatabaseStats, Durability, Metric, SearchHit, SearchResult,
};
pub use error::{AproError, Result};
pub use migration::{LegacyImportOptions, LegacyImportReport, LegacySourceFile, import_0_1};
pub use value::Value;

/// API canonica 1.x in costruzione. L'API 0.1 resta disponibile alla radice
/// finché la migrazione del facade non è completata.
pub mod v1 {
    pub use aprodb_engine::*;
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
