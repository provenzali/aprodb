use std::collections::BTreeMap;

use aprodb_types::{
    AuditEvent, AuditOutcome, AuditState, CatalogState, ChangeBody, ChangeEvent, ChangeOperation,
    CompressionCatalog, CompressionCodec, CompressionDictionary, Durability, HeadPointer,
    IdempotencyExpiryEntry, IdempotencyRecord, LogicalFrameKind, MutationReceipt, Payload,
    RadialDescriptor, RadialLayer, RecordEnvelope, RecordIdentity, StoredPayload,
    StoredRecordEnvelope, SurfaceDefinition, SurfaceFormat, SurfaceGeneration, SurfaceKind,
    SurfacePointer, TtlEntry, Version, WorkflowDescriptor, WorkflowIndexEntry, decode_logical,
    encode_logical,
};
use proptest::prelude::*;

#[test]
fn logical_v1_frames_match_goldens() {
    let identity =
        RecordIdentity::new("tenant", "namespace", "collection", "partition", "key").unwrap();
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

    assert_golden(
        LogicalFrameKind::Record,
        &record,
        include_str!("golden/record_v1.hex"),
    );
    assert_golden(
        LogicalFrameKind::Head,
        &head,
        include_str!("golden/head_v1.hex"),
    );
    assert_golden(
        LogicalFrameKind::Change,
        &event,
        include_str!("golden/change_v1.hex"),
    );
    assert_golden(
        LogicalFrameKind::Catalog,
        &catalog,
        include_str!("golden/catalog_v1.hex"),
    );
    assert_golden(
        LogicalFrameKind::Radial,
        &radial,
        include_str!("golden/radial_v1.hex"),
    );
    assert_golden(
        LogicalFrameKind::Ttl,
        &ttl,
        include_str!("golden/ttl_v1.hex"),
    );
    assert_golden(
        LogicalFrameKind::Workflow,
        &workflow,
        include_str!("golden/workflow_v1.hex"),
    );
    assert_golden(
        LogicalFrameKind::Idempotency,
        &idempotency,
        include_str!("golden/idempotency_v1.hex"),
    );
    assert_golden(
        LogicalFrameKind::IdempotencyExpiry,
        &idempotency_expiry,
        include_str!("golden/idempotency_expiry_v1.hex"),
    );
    assert_golden(
        LogicalFrameKind::SurfaceDefinition,
        &surface_definition,
        include_str!("golden/surface_definition_v1.hex"),
    );
    assert_golden(
        LogicalFrameKind::SurfacePointer,
        &surface_pointer,
        include_str!("golden/surface_pointer_v1.hex"),
    );
    assert_golden(
        LogicalFrameKind::SurfaceGeneration,
        &surface_generation,
        include_str!("golden/surface_generation_v1.hex"),
    );
    assert_golden(
        LogicalFrameKind::SurfacePayload,
        &vec![record],
        include_str!("golden/surface_payload_v1.hex"),
    );
    assert_golden(
        LogicalFrameKind::StoredRecord,
        &stored_record,
        include_str!("golden/stored_record_v1.hex"),
    );
    assert_golden(
        LogicalFrameKind::CompressionCatalog,
        &compression_catalog,
        include_str!("golden/compression_catalog_v1.hex"),
    );
    assert_golden(
        LogicalFrameKind::CompressionDictionary,
        &compression_dictionary,
        include_str!("golden/compression_dictionary_v1.hex"),
    );
    assert_golden(
        LogicalFrameKind::Audit,
        &audit,
        include_str!("golden/audit_v1.hex"),
    );
    assert_golden(
        LogicalFrameKind::AuditState,
        &audit_state,
        include_str!("golden/audit_state_v1.hex"),
    );
}

fn assert_golden<T>(kind: LogicalFrameKind, value: &T, expected_hex: &str)
where
    T: serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
{
    let encoded = encode_logical(kind, value).unwrap();
    assert_eq!(encoded, hex::decode(expected_hex.trim()).unwrap());
    let decoded = decode_logical::<T>(kind, &encoded).unwrap();
    assert_eq!(&decoded, value);
}

proptest! {
    #[test]
    fn record_codec_round_trips_arbitrary_binary_payload(
        tenant in proptest::collection::vec(any::<u8>(), 1..32),
        namespace in proptest::collection::vec(any::<u8>(), 1..32),
        collection in proptest::collection::vec(any::<u8>(), 1..32),
        partition in proptest::collection::vec(any::<u8>(), 1..32),
        key in proptest::collection::vec(any::<u8>(), 1..128),
        payload in proptest::collection::vec(any::<u8>(), 0..4096),
        sequence in any::<u64>(),
    ) {
        let identity = RecordIdentity::new(tenant, namespace, collection, partition, key).unwrap();
        let record = RecordEnvelope {
            identity,
            payload: Some(Payload::Bytes(payload)),
            content_type: "application/octet-stream".into(),
            version: Version { epoch: 1, shard_id: 0, sequence },
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
            expires_at_unix_ms: None,
            metadata: BTreeMap::new(),
            workflow: WorkflowDescriptor::default(),
            idempotency_key_hash: None,
            dictionary_id: None,
            tombstone: false,
        };
        let encoded = encode_logical(LogicalFrameKind::Record, &record).unwrap();
        prop_assert_eq!(
            decode_logical::<RecordEnvelope>(LogicalFrameKind::Record, &encoded).unwrap(),
            record
        );
    }

    #[test]
    fn stored_record_codec_round_trips_bounded_payload(
        payload in proptest::collection::vec(any::<u8>(), 0..4096),
        checksum in any::<u32>(),
        sequence in any::<u64>(),
    ) {
        let stored = StoredRecordEnvelope {
            identity: RecordIdentity::new("tenant", "namespace", "collection", "partition", "key").unwrap(),
            payload: Some(StoredPayload {
                codec_version: 1,
                codec: CompressionCodec::Raw,
                dictionary_id: None,
                logical_bytes: payload.len() as u64,
                logical_checksum: checksum,
                bytes: payload,
            }),
            content_type: "application/octet-stream".into(),
            version: Version { epoch: 1, shard_id: 0, sequence },
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
            expires_at_unix_ms: None,
            metadata: BTreeMap::new(),
            workflow: WorkflowDescriptor::default(),
            idempotency_key_hash: None,
            tombstone: false,
        };
        let encoded = encode_logical(LogicalFrameKind::StoredRecord, &stored).unwrap();
        prop_assert_eq!(
            decode_logical::<StoredRecordEnvelope>(LogicalFrameKind::StoredRecord, &encoded).unwrap(),
            stored
        );
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: Apache-2.0
