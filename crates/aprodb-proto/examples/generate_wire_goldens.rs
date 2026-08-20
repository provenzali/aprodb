use std::collections::BTreeMap;

use aprodb_proto::{
    AuditListOperation, ClaimOperation, ClientHello, EndpointRole, ExpectedMode, ExpectedVersion,
    Key, PutOperation, Receipt, Request, Response, VectorSearchOperation, WireAuditEvent,
    WireComputeExecution, WireComputePreference, WireCostEstimate, WireDurability, WirePayload,
    WireSurfaceFormat, WireSurfaceGeneration, WireVectorHit, WireVectorMetric,
    WireVectorSearchResult, WireVersion, WorkflowScopeOperation, encode_frame, request,
    wire_payload,
};

fn main() {
    let hello = ClientHello::new(EndpointRole::Data, Vec::new(), 4096);
    let request = Request {
        request_id: 42,
        deadline_unix_ms: 1_700_000_000_000,
        tenant: b"tenant".to_vec(),
        namespace: b"namespace".to_vec(),
        durability: WireDurability::Durable as i32,
        operation: Some(request::Operation::Put(PutOperation {
            key: Some(Key {
                collection: b"objects".to_vec(),
                partition: b"p".to_vec(),
                key: b"k".to_vec(),
            }),
            payload: Some(WirePayload {
                kind: Some(wire_payload::Kind::TextValue("hello".into())),
            }),
            content_type: "text/plain".into(),
            metadata: BTreeMap::new(),
            expires_at_unix_ms: None,
            expected: Some(ExpectedVersion {
                mode: ExpectedMode::Missing as i32,
                version: None,
            }),
            delta: Vec::new(),
            idempotency_key_hash: Vec::new(),
        })),
    };
    println!("hello={}", hex::encode(encode_frame(&hello, 4096).unwrap()));
    println!(
        "request={}",
        hex::encode(encode_frame(&request, 4096).unwrap())
    );
    let mut response = Response::ok(42);
    response.receipts.push(Receipt {
        version: Some(WireVersion {
            epoch: 1,
            shard_id: 3,
            sequence: 9,
        }),
        durability: WireDurability::Durable as i32,
        durable_watermark: 9,
        batch_id: vec![0xAB; 16],
    });
    println!(
        "response={}",
        hex::encode(encode_frame(&response, 4096).unwrap())
    );
    let claim = Request {
        request_id: 43,
        deadline_unix_ms: 1_700_000_000_000,
        tenant: b"tenant".to_vec(),
        namespace: b"namespace".to_vec(),
        durability: WireDurability::Durable as i32,
        operation: Some(request::Operation::Claim(ClaimOperation {
            scope: Some(WorkflowScopeOperation {
                collection: b"objects".to_vec(),
                partition: b"p".to_vec(),
            }),
            max_records: 2,
            lease_duration_ms: 30_000,
            idempotency_key_hash: vec![0xCD; 32],
        })),
    };
    println!("claim={}", hex::encode(encode_frame(&claim, 4096).unwrap()));
    let mut surface = Response::ok(43);
    surface.surface = Some(WireSurfaceGeneration {
        projection_id: "read-surface".into(),
        generation: 2,
        source_watermarks: BTreeMap::from([(3, 9)]),
        format: WireSurfaceFormat::Json as i32,
        record_count: 1,
        serialized: br#"[{"key":"k"}]"#.to_vec(),
        created_at_unix_ms: 1_700_000_000_123,
        stale_by_sequences: BTreeMap::from([(3, 2)]),
        complete: false,
        errors: Vec::new(),
    });
    println!(
        "surface={}",
        hex::encode(encode_frame(&surface, 4096).unwrap())
    );
    let vector = Request {
        request_id: 44,
        deadline_unix_ms: 1_700_000_000_000,
        tenant: b"tenant".to_vec(),
        namespace: b"namespace".to_vec(),
        durability: WireDurability::Relaxed as i32,
        operation: Some(request::Operation::VectorSearch(VectorSearchOperation {
            collection: b"vectors".to_vec(),
            query: vec![1.0, 0.5],
            metric: WireVectorMetric::Cosine as i32,
            limit: 2,
            max_scan_records: 100,
            preference: WireComputePreference::Auto as i32,
        })),
    };
    println!(
        "vector={}",
        hex::encode(encode_frame(&vector, 4096).unwrap())
    );
    let mut vector_response = Response::ok(44);
    vector_response.vector_search = Some(WireVectorSearchResult {
        hits: vec![WireVectorHit {
            partition: b"p".to_vec(),
            key: b"v1".to_vec(),
            version: Some(WireVersion {
                epoch: 1,
                shard_id: 3,
                sequence: 10,
            }),
            score: 0.75,
        }],
        scanned_records: 5,
        vector_candidates: 4,
        execution: WireComputeExecution::CpuFallback as i32,
        accelerator: None,
        estimate: Some(WireCostEstimate {
            transfer_in_micros: 10,
            queue_wait_micros: 2,
            launch_micros: 3,
            accelerator_compute_micros: 4,
            transfer_out_micros: 5,
            synchronization_micros: 6,
            risk_margin_micros: 7,
            accelerator_total_micros: 37,
            cpu_compute_micros: 40,
            vram_cache_hit: false,
        }),
        fallback_reason: Some("circuit open".into()),
    });
    println!(
        "vector_response={}",
        hex::encode(encode_frame(&vector_response, 4096).unwrap())
    );
    let audit_request = Request {
        request_id: 77,
        deadline_unix_ms: 1_700_000_000_000,
        tenant: Vec::new(),
        namespace: Vec::new(),
        durability: WireDurability::Durable as i32,
        operation: Some(request::Operation::AuditList(AuditListOperation {
            after_sequence: Some(8),
            limit: 16,
        })),
    };
    println!(
        "audit_request={}",
        hex::encode(encode_frame(&audit_request, 4096).unwrap())
    );
    let mut audit_response = Response::ok(77);
    audit_response.audit_events.push(WireAuditEvent {
        sequence: 9,
        event_id: vec![8; 16],
        at_unix_ms: 12,
        request_id: 44,
        principal: "operator".into(),
        operation: "compact".into(),
        outcome: "succeeded".into(),
        target_hash: Some(vec![7; 32]),
        error_class: None,
    });
    audit_response.audit_next_sequence = Some(9);
    println!(
        "audit_response={}",
        hex::encode(encode_frame(&audit_response, 4096).unwrap())
    );
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: Apache-2.0
