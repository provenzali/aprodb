use std::collections::BTreeMap;

use aprodb_proto::{
    AuditListOperation, ClaimOperation, ClientHello, EndpointRole, ExpectedMode, ExpectedVersion,
    Key, PutOperation, Receipt, Request, Response, VectorSearchOperation, WireAuditEvent,
    WireComputeExecution, WireComputePreference, WireCostEstimate, WireDurability, WirePayload,
    WireSurfaceFormat, WireSurfaceGeneration, WireVectorHit, WireVectorMetric,
    WireVectorSearchResult, WireVersion, WorkflowScopeOperation, decode_frame, encode_frame,
    request, wire_payload,
};

fn canonical_audit_request() -> Request {
    Request {
        request_id: 77,
        deadline_unix_ms: 1_700_000_000_000,
        tenant: Vec::new(),
        namespace: Vec::new(),
        durability: WireDurability::Durable as i32,
        operation: Some(request::Operation::AuditList(AuditListOperation {
            after_sequence: Some(8),
            limit: 16,
        })),
    }
}

fn canonical_audit_response() -> Response {
    let mut response = Response::ok(77);
    response.audit_events.push(WireAuditEvent {
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
    response.audit_next_sequence = Some(9);
    response
}

fn canonical_request() -> Request {
    Request {
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
    }
}

fn canonical_response() -> Response {
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
    response
}

fn canonical_claim_request() -> Request {
    Request {
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
    }
}

fn canonical_surface_response() -> Response {
    let mut response = Response::ok(43);
    response.surface = Some(WireSurfaceGeneration {
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
    response
}

fn canonical_vector_request() -> Request {
    Request {
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
    }
}

fn canonical_vector_response() -> Response {
    let mut response = Response::ok(44);
    response.vector_search = Some(WireVectorSearchResult {
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
    response
}

#[test]
fn framed_v1_messages_match_goldens() {
    let hello = ClientHello::new(EndpointRole::Data, Vec::new(), 4096);
    let hello_bytes = encode_frame(&hello, 4096).unwrap();
    let expected_hello = hex::decode(include_str!("golden/client_hello_v1.hex").trim()).unwrap();
    assert_eq!(hello_bytes, expected_hello);
    let decoded_hello: ClientHello = decode_frame(&hello_bytes, 4096).unwrap();
    assert_eq!(decoded_hello, hello);

    let request = canonical_request();
    let request_bytes = encode_frame(&request, 4096).unwrap();
    let expected_request = hex::decode(include_str!("golden/put_request_v1.hex").trim()).unwrap();
    assert_eq!(request_bytes, expected_request);
    let decoded_request: Request = decode_frame(&request_bytes, 4096).unwrap();
    assert_eq!(decoded_request, request);

    let response = canonical_response();
    let response_bytes = encode_frame(&response, 4096).unwrap();
    let expected_response =
        hex::decode(include_str!("golden/durable_response_v1.hex").trim()).unwrap();
    assert_eq!(response_bytes, expected_response);
    let decoded_response: Response = decode_frame(&response_bytes, 4096).unwrap();
    assert_eq!(decoded_response, response);

    let claim = canonical_claim_request();
    let claim_bytes = encode_frame(&claim, 4096).unwrap();
    let expected_claim = hex::decode(include_str!("golden/claim_request_v1.hex").trim()).unwrap();
    assert_eq!(claim_bytes, expected_claim);
    assert_eq!(decode_frame::<Request>(&claim_bytes, 4096).unwrap(), claim);

    let surface = canonical_surface_response();
    let surface_bytes = encode_frame(&surface, 4096).unwrap();
    let expected_surface =
        hex::decode(include_str!("golden/surface_response_v1.hex").trim()).unwrap();
    assert_eq!(surface_bytes, expected_surface);
    assert_eq!(
        decode_frame::<Response>(&surface_bytes, 4096).unwrap(),
        surface
    );

    let vector = canonical_vector_request();
    let vector_bytes = encode_frame(&vector, 4096).unwrap();
    let expected_vector = hex::decode(include_str!("golden/vector_request_v1.hex").trim()).unwrap();
    assert_eq!(vector_bytes, expected_vector);
    assert_eq!(
        decode_frame::<Request>(&vector_bytes, 4096).unwrap(),
        vector
    );

    let vector_response = canonical_vector_response();
    let vector_response_bytes = encode_frame(&vector_response, 4096).unwrap();
    let expected_vector_response =
        hex::decode(include_str!("golden/vector_response_v1.hex").trim()).unwrap();
    assert_eq!(vector_response_bytes, expected_vector_response);
    assert_eq!(
        decode_frame::<Response>(&vector_response_bytes, 4096).unwrap(),
        vector_response
    );

    let audit_request = canonical_audit_request();
    let audit_request_bytes = encode_frame(&audit_request, 4096).unwrap();
    assert_eq!(
        audit_request_bytes,
        hex::decode(include_str!("golden/audit_request_v1.hex").trim()).unwrap()
    );
    assert_eq!(
        decode_frame::<Request>(&audit_request_bytes, 4096).unwrap(),
        audit_request
    );
    let audit_response = canonical_audit_response();
    let audit_response_bytes = encode_frame(&audit_response, 4096).unwrap();
    assert_eq!(
        audit_response_bytes,
        hex::decode(include_str!("golden/audit_response_v1.hex").trim()).unwrap()
    );
    assert_eq!(
        decode_frame::<Response>(&audit_response_bytes, 4096).unwrap(),
        audit_response
    );
}

#[test]
fn framed_decoder_rejects_length_mismatch() {
    let mut frame = encode_frame(&canonical_request(), 4096).unwrap().to_vec();
    frame[3] = frame[3].saturating_add(1);
    assert!(decode_frame::<Request>(&frame, 4096).is_err());
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: Apache-2.0
