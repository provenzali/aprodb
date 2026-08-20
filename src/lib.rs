//! AProDB: typed key-value engine with WAL, sharding, and parallel vector search.

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

/// Canonical API 1.x under development. The 0.1 API remains available at the root
/// until the facade migration is complete.
pub mod v1 {
    pub use aprodb_engine::*;
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
