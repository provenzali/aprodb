use std::collections::BTreeMap;

use aprodb_types::{
    AuditEvent, AuditOutcome, AuditState, CatalogState, ChangeBody, ChangeEvent, ChangeOperation,
    CompressionCatalog, CompressionCodec, CompressionDictionary, Durability, HeadPointer,
    IdempotencyExpiryEntry, IdempotencyRecord, LogicalFrameKind, MutationReceipt, Payload,
    RadialDescriptor, RadialLayer, RecordEnvelope, RecordIdentity, StoredPayload,
    StoredRecordEnvelope, SurfaceDefinition, SurfaceFormat, SurfaceGeneration, SurfaceKind,
    SurfacePointer, TtlEntry, Version, WorkflowDescriptor, WorkflowIndexEntry, encode_logical,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let identity = RecordIdentity::new("tenant", "namespace", "collection", "partition", "key")?;
    let version = Version {
        epoch: 1,
        shard_id: 2,
        sequence: 3,
    };
    let record = RecordEnvelope {
        identity: identity.clone(),
        payload: Some(Payload::Text("ciao".into())),
        content_type: "text/plain".into(),
        version,
        created_at_unix_ms: 10,
        updated_at_unix_ms: 11,
        expires_at_unix_ms: None,
        metadata: BTreeMap::new(),
        workflow: WorkflowDescriptor::default(),
        idempotency_key_hash: None,
        dictionary_id: None,
        tombstone: false,
    };
    let head = HeadPointer {
        identity: identity.clone(),
        version,
        tombstone: false,
    };
    let event = ChangeEvent {
        tenant: identity.tenant.clone(),
        namespace: identity.namespace.clone(),
        collection: identity.collection.clone(),
        partition: identity.partition.clone(),
        version,
        operation: ChangeOperation::Put,
        key: identity.key.clone(),
        previous_version: None,
        batch_id: [7; 20],
        idempotency_key_hash: None,
        body: ChangeBody::VersionRef {
            identity: identity.clone(),
            version,
        },
    };
    let catalog = CatalogState::empty("fjall-3.1.8", 2);
    let radial = RadialDescriptor {
        identity: identity.clone(),
        canonical_version: version,
        created_at_unix_ms: 10,
        updated_at_unix_ms: 11,
        access_frequency_estimate: 4,
        last_access_sampled_unix_ms: Some(12),
        freshness_half_life_ms: 3_600_000,
        urgency_millis: 250,
        deadline_unix_ms: Some(100),
        workflow_state: "ready".into(),
        projection_watermarks: BTreeMap::new(),
        reconstruction_cost_micros: 50,
        logical_bytes: 128,
        physical_bytes: 96,
        storage_class: "primary".into(),
        admin_pin_until_unix_ms: None,
        layer: RadialLayer::Warm,
        layer_since_unix_ms: 11,
        last_decision: "golden".into(),
    };
    let ttl = TtlEntry {
        identity: identity.clone(),
        version,
        expires_at_unix_ms: 100,
    };
    let workflow = WorkflowIndexEntry {
        identity: identity.clone(),
        version,
        state: "pending".into(),
        available_at_unix_ms: 0,
    };
    let receipt = MutationReceipt {
        version,
        durability: Durability::Durable,
        durable_watermark: 3,
        batch_id: [7; 20],
    };
    let idempotency = IdempotencyRecord {
        scope: vec![1, 2, 3],
        key_hash: [8; 32],
        request_fingerprint: [9; 32],
        receipts: vec![receipt],
        expires_at_unix_ms: 500,
    };
    let idempotency_expiry = IdempotencyExpiryEntry {
        lookup_key: vec![4, 5, 6],
        expires_at_unix_ms: 500,
    };
    let surface_definition = SurfaceDefinition {
        id: "feed".into(),
        kind: SurfaceKind::Read,
        source_tenant: b"tenant".to_vec(),
        source_namespace: b"namespace".to_vec(),
        source_collection: b"collection".to_vec(),
        workflow_states: vec!["published".into()],
        format: SurfaceFormat::Json,
        max_records: 100,
        max_bytes: 4096,
        retained_generations: 2,
    };
    let surface_pointer = SurfacePointer {
        projection_id: "feed".into(),
        current_generation: Some(1),
        next_generation: 2,
        source_watermarks: [(2, 3)].into_iter().collect(),
        retained_generations: vec![1],
    };
    let surface_generation = SurfaceGeneration {
        projection_id: "feed".into(),
        generation: 1,
        source_watermarks: [(2, 3)].into_iter().collect(),
        format: SurfaceFormat::Json,
        record_count: 1,
        serialized: b"[]".to_vec(),
        created_at_unix_ms: 20,
    };
    let stored_record = StoredRecordEnvelope {
        identity: identity.clone(),
        payload: Some(StoredPayload {
            codec_version: 1,
            codec: CompressionCodec::Raw,
            dictionary_id: None,
            logical_bytes: 5,
            logical_checksum: 0x1234_5678,
            bytes: b"ciao!".to_vec(),
        }),
        content_type: "text/plain".into(),
        version,
        created_at_unix_ms: 10,
        updated_at_unix_ms: 11,
        expires_at_unix_ms: None,
        metadata: BTreeMap::new(),
        workflow: WorkflowDescriptor::default(),
        idempotency_key_hash: None,
        tombstone: false,
    };
    let compression_catalog = CompressionCatalog {
        generation: 1,
        ..CompressionCatalog::default()
    };
    let compression_dictionary = CompressionDictionary {
        id: 7,
        tenant: identity.tenant.clone(),
        namespace: identity.namespace.clone(),
        collection: identity.collection.clone(),
        schema: "text/plain".into(),
        bytes: vec![1, 2, 3, 4],
        checksum: 0xAABB_CCDD,
        created_at_unix_ms: 12,
        validation_raw_bytes: 100,
        validation_without_dictionary_bytes: 80,
        validation_with_dictionary_bytes: 60,
    };
    let audit = AuditEvent {
        format_version: 1,
        sequence: 9,
        event_id: [8; 16],
        at_unix_ms: 12,
        request_id: 44,
        principal: "operator".into(),
        operation: "compact".into(),
        outcome: AuditOutcome::Succeeded,
        target_hash: Some([7; 32]),
        error_class: None,
    };
    let audit_state = AuditState {
        format_version: 1,
        last_sequence: 9,
    };

    for (name, kind, bytes) in [
        (
            "record",
            LogicalFrameKind::Record,
            encode_logical(LogicalFrameKind::Record, &record)?,
        ),
        (
            "head",
            LogicalFrameKind::Head,
            encode_logical(LogicalFrameKind::Head, &head)?,
        ),
        (
            "change",
            LogicalFrameKind::Change,
            encode_logical(LogicalFrameKind::Change, &event)?,
        ),
        (
            "catalog",
            LogicalFrameKind::Catalog,
            encode_logical(LogicalFrameKind::Catalog, &catalog)?,
        ),
        (
            "radial",
            LogicalFrameKind::Radial,
            encode_logical(LogicalFrameKind::Radial, &radial)?,
        ),
        (
            "ttl",
            LogicalFrameKind::Ttl,
            encode_logical(LogicalFrameKind::Ttl, &ttl)?,
        ),
        (
            "workflow",
            LogicalFrameKind::Workflow,
            encode_logical(LogicalFrameKind::Workflow, &workflow)?,
        ),
        (
            "idempotency",
            LogicalFrameKind::Idempotency,
            encode_logical(LogicalFrameKind::Idempotency, &idempotency)?,
        ),
        (
            "idempotency_expiry",
            LogicalFrameKind::IdempotencyExpiry,
            encode_logical(LogicalFrameKind::IdempotencyExpiry, &idempotency_expiry)?,
        ),
        (
            "surface_definition",
            LogicalFrameKind::SurfaceDefinition,
            encode_logical(LogicalFrameKind::SurfaceDefinition, &surface_definition)?,
        ),
        (
            "surface_pointer",
            LogicalFrameKind::SurfacePointer,
            encode_logical(LogicalFrameKind::SurfacePointer, &surface_pointer)?,
        ),
        (
            "surface_generation",
            LogicalFrameKind::SurfaceGeneration,
            encode_logical(LogicalFrameKind::SurfaceGeneration, &surface_generation)?,
        ),
        (
            "surface_payload",
            LogicalFrameKind::SurfacePayload,
            encode_logical(LogicalFrameKind::SurfacePayload, &vec![record])?,
        ),
        (
            "stored_record",
            LogicalFrameKind::StoredRecord,
            encode_logical(LogicalFrameKind::StoredRecord, &stored_record)?,
        ),
        (
            "compression_catalog",
            LogicalFrameKind::CompressionCatalog,
            encode_logical(LogicalFrameKind::CompressionCatalog, &compression_catalog)?,
        ),
        (
            "compression_dictionary",
            LogicalFrameKind::CompressionDictionary,
            encode_logical(
                LogicalFrameKind::CompressionDictionary,
                &compression_dictionary,
            )?,
        ),
        (
            "audit",
            LogicalFrameKind::Audit,
            encode_logical(LogicalFrameKind::Audit, &audit)?,
        ),
        (
            "audit_state",
            LogicalFrameKind::AuditState,
            encode_logical(LogicalFrameKind::AuditState, &audit_state)?,
        ),
    ] {
        println!("{name}:{kind:?}:{}", hex::encode(bytes));
    }
    Ok(())
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: Apache-2.0
