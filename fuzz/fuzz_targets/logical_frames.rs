#![cfg_attr(not(windows), no_main)]

use aprodb_types::{
    AuditEvent, AuditState, CatalogState, ChangeEvent, CompressionCatalog, CompressionDictionary, HeadPointer,
    IdempotencyExpiryEntry, IdempotencyRecord, LogicalFrameKind, RadialDescriptor, RadialState,
    RecordEnvelope, StoredRecordEnvelope, SurfaceDefinition, SurfaceGeneration, SurfacePointer,
    TtlEntry, WorkflowIndexEntry, decode_logical,
};
use aprodb_proto::{ClientHello, Request, Response, ServerHello, decode_limited};
fn exercise(bytes: &[u8]) {
    let _ = decode_logical::<RecordEnvelope>(LogicalFrameKind::Record, bytes);
    let _ = decode_logical::<HeadPointer>(LogicalFrameKind::Head, bytes);
    let _ = decode_logical::<ChangeEvent>(LogicalFrameKind::Change, bytes);
    let _ = decode_logical::<CatalogState>(LogicalFrameKind::Catalog, bytes);
    let _ = decode_logical::<RadialDescriptor>(LogicalFrameKind::Radial, bytes);
    let _ = decode_logical::<RadialState>(LogicalFrameKind::RadialState, bytes);
    let _ = decode_logical::<TtlEntry>(LogicalFrameKind::Ttl, bytes);
    let _ = decode_logical::<WorkflowIndexEntry>(LogicalFrameKind::Workflow, bytes);
    let _ = decode_logical::<IdempotencyRecord>(LogicalFrameKind::Idempotency, bytes);
    let _ = decode_logical::<IdempotencyExpiryEntry>(LogicalFrameKind::IdempotencyExpiry, bytes);
    let _ = decode_logical::<SurfaceDefinition>(LogicalFrameKind::SurfaceDefinition, bytes);
    let _ = decode_logical::<SurfacePointer>(LogicalFrameKind::SurfacePointer, bytes);
    let _ = decode_logical::<SurfaceGeneration>(LogicalFrameKind::SurfaceGeneration, bytes);
    let _ = decode_logical::<Vec<RecordEnvelope>>(LogicalFrameKind::SurfacePayload, bytes);
    let _ = decode_logical::<StoredRecordEnvelope>(LogicalFrameKind::StoredRecord, bytes);
    let _ = decode_logical::<CompressionCatalog>(LogicalFrameKind::CompressionCatalog, bytes);
    let _ = decode_logical::<CompressionDictionary>(LogicalFrameKind::CompressionDictionary, bytes);
    let _ = decode_logical::<AuditEvent>(LogicalFrameKind::Audit, bytes);
    let _ = decode_logical::<AuditState>(LogicalFrameKind::AuditState, bytes);
    let _ = decode_limited::<ClientHello>(bytes, aprodb_proto::DEFAULT_MAX_FRAME_BYTES);
    let _ = decode_limited::<ServerHello>(bytes, aprodb_proto::DEFAULT_MAX_FRAME_BYTES);
    let _ = decode_limited::<Request>(bytes, aprodb_proto::DEFAULT_MAX_FRAME_BYTES);
    let _ = decode_limited::<Response>(bytes, aprodb_proto::DEFAULT_MAX_FRAME_BYTES);
}

#[cfg(not(windows))]
libfuzzer_sys::fuzz_target!(|bytes: &[u8]| exercise(bytes));

#[cfg(windows)]
fn main() {
    for bytes in [&[][..], b"APRC", &[0xff; 64]] {
        exercise(bytes);
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
