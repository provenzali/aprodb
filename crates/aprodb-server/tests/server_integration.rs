use std::{sync::Arc, time::Duration};

use aprodb_client::{
    AsyncClient, ClientConfig, ClientError, ComputeExecution, ComputePreference, DeleteOptions,
    Expected, Mutation, PutOptions, VectorMetric, tls_client_config,
};
use aprodb_engine::{Engine, EngineConfig};
use aprodb_proto::{
    ClientHello, EndpointRole, ErrorCode, GetOperation, Key, Request, Response, ServerHello,
    WireDurability, decode_limited, encode_limited, request,
};
use aprodb_server::{Server, ServerConfig, TenantQuota, tls_server_config};
use aprodb_types::{
    AuditOutcome, ChangeOperation, CompressionPolicy, Durability, LeaseProof, Payload,
    RecordIdentity, SurfaceDefinition, SurfaceFormat, SurfaceKind, WorkflowScope,
};
use futures_util::{SinkExt, StreamExt};
use tokio::net::TcpStream;
use tokio_util::codec::{Framed, LengthDelimitedCodec};

const DATA_TOKEN: &[u8] = b"data-token-for-tests-0001";
const ADMIN_TOKEN: &[u8] = b"admin-token-for-tests-001";

struct TestTlsIdentity {
    ca: String,
    server_certificate: String,
    server_key: String,
    client_certificate: String,
    client_key: String,
}

fn test_tls_identity() -> TestTlsIdentity {
    use rcgen::{
        BasicConstraints, CertificateParams, DistinguishedName, DnType, ExtendedKeyUsagePurpose,
        IsCa, KeyPair,
    };

    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let mut ca_name = DistinguishedName::new();
    ca_name.push(DnType::CommonName, "AProDB test CA");
    ca_params.distinguished_name = ca_name;
    let ca_key = KeyPair::generate().unwrap();
    let ca = ca_params.self_signed(&ca_key).unwrap();

    let mut server_params = CertificateParams::new(vec!["localhost".into()]).unwrap();
    server_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let server_key = KeyPair::generate().unwrap();
    let server = server_params.signed_by(&server_key, &ca, &ca_key).unwrap();

    let mut client_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    client_params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let client_key = KeyPair::generate().unwrap();
    let client = client_params.signed_by(&client_key, &ca, &ca_key).unwrap();

    TestTlsIdentity {
        ca: ca.pem(),
        server_certificate: server.pem(),
        server_key: server_key.serialize_pem(),
        client_certificate: client.pem(),
        client_key: client_key.serialize_pem(),
    }
}

fn identity(key: &str) -> RecordIdentity {
    RecordIdentity::new("tenant", "namespace", "collection", "partition", key).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admin_mutation_is_durably_audited_and_data_role_cannot_read_audit() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(EngineConfig::new(directory.path())).unwrap());
    let mut config = ServerConfig::loopback(DATA_TOKEN, ADMIN_TOKEN).unwrap();
    config.admin_principal = "operator-test".into();
    let handle = Server::start(Arc::clone(&engine), config).await.unwrap();
    let data = AsyncClient::connect_tcp(handle.data_tcp.unwrap(), ClientConfig::data(DATA_TOKEN))
        .await
        .unwrap();
    let admin =
        AsyncClient::connect_tcp(handle.admin_tcp.unwrap(), ClientConfig::admin(ADMIN_TOKEN))
            .await
            .unwrap();

    admin.compact().await.unwrap();
    let page = admin.audit(None, 8).await.unwrap();
    assert_eq!(page.events.len(), 2);
    assert_eq!(page.events[0].outcome, AuditOutcome::Attempted);
    assert_eq!(page.events[1].outcome, AuditOutcome::Succeeded);
    assert!(page.events.iter().all(|event| {
        event.operation == "compact"
            && event.principal == "operator-test"
            && event.target_hash.is_some()
    }));

    let unauthorized = data.audit(None, 1).await.unwrap_err();
    assert!(matches!(
        unauthorized,
        ClientError::Server {
            code: ErrorCode::Unauthorized,
            ..
        }
    ));

    drop(data);
    drop(admin);
    handle.shutdown().await.unwrap();
    drop(engine);
    let reopened = Engine::open(EngineConfig::new(directory.path())).unwrap();
    assert_eq!(reopened.read_audit(None, 8).unwrap().events.len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tenant_quota_rejects_oversized_requests_and_compute_before_execution() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(EngineConfig::new(directory.path())).unwrap());
    let mut config = ServerConfig::loopback(DATA_TOKEN, ADMIN_TOKEN).unwrap();
    config.tenant_quotas.insert(
        b"tenant".to_vec(),
        TenantQuota {
            max_inflight: 4,
            max_requests_per_second: 1_000,
            max_request_bytes: 512,
            max_vector_work_items: 64,
        },
    );
    let handle = Server::start(engine, config).await.unwrap();
    let data = AsyncClient::connect_tcp(handle.data_tcp.unwrap(), ClientConfig::data(DATA_TOKEN))
        .await
        .unwrap();

    let oversized = data
        .put(
            identity("oversized"),
            Payload::Text("x".repeat(1_024)),
            PutOptions::default(),
            Durability::Durable,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        oversized,
        ClientError::Server {
            code: ErrorCode::ResourceLimit,
            ..
        }
    ));

    let compute = data
        .vector_exact(
            b"tenant".to_vec(),
            b"namespace".to_vec(),
            b"collection".to_vec(),
            vec![1.0; 16],
            VectorMetric::Dot,
            1,
            16,
            ComputePreference::Cpu,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        compute,
        ClientError::Server {
            code: ErrorCode::ResourceLimit,
            ..
        }
    ));

    drop(data);
    handle.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mtls_authenticates_server_and_client_and_rejects_anonymous_peer() {
    let tls = test_tls_identity();
    let directory = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(EngineConfig::new(directory.path())).unwrap());
    let mut config = ServerConfig::loopback(DATA_TOKEN, ADMIN_TOKEN).unwrap();
    config.tls = Some(
        tls_server_config(
            tls.server_certificate.as_bytes(),
            tls.server_key.as_bytes(),
            Some(tls.ca.as_bytes()),
        )
        .unwrap(),
    );
    let handle = Server::start(engine, config).await.unwrap();
    let address = handle.data_tcp.unwrap();

    let mut anonymous = ClientConfig::data(DATA_TOKEN);
    anonymous.tls = Some(tls_client_config(tls.ca.as_bytes(), "localhost", None).unwrap());
    assert!(AsyncClient::connect_tcp(address, anonymous).await.is_err());

    let mut authenticated = ClientConfig::data(DATA_TOKEN);
    authenticated.tls = Some(
        tls_client_config(
            tls.ca.as_bytes(),
            "localhost",
            Some((tls.client_certificate.as_bytes(), tls.client_key.as_bytes())),
        )
        .unwrap(),
    );
    let client = AsyncClient::connect_tcp(address, authenticated)
        .await
        .unwrap();
    client
        .put(
            identity("mtls"),
            Payload::Text("secured".into()),
            PutOptions::default(),
            Durability::Durable,
        )
        .await
        .unwrap();
    drop(client);
    handle.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn online_backup_is_verified_bounded_to_server_root_and_audited() {
    let data_directory = tempfile::tempdir().unwrap();
    let backup_directory = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(EngineConfig::new(data_directory.path())).unwrap());
    let mut config = ServerConfig::loopback(DATA_TOKEN, ADMIN_TOKEN).unwrap();
    config.backup_root = Some(backup_directory.path().to_path_buf());
    let handle = Server::start(engine, config).await.unwrap();
    let data = AsyncClient::connect_tcp(handle.data_tcp.unwrap(), ClientConfig::data(DATA_TOKEN))
        .await
        .unwrap();
    let admin =
        AsyncClient::connect_tcp(handle.admin_tcp.unwrap(), ClientConfig::admin(ADMIN_TOKEN))
            .await
            .unwrap();
    data.put(
        identity("backup-online"),
        Payload::Text("durable".into()),
        PutOptions::default(),
        Durability::Durable,
    )
    .await
    .unwrap();

    let backup = admin.backup("daily-001").await.unwrap();
    assert_eq!(backup.name, "daily-001");
    assert!(backup.files > 0);
    let manifest = Engine::verify_backup(backup_directory.path().join("daily-001")).unwrap();
    assert_eq!(manifest.catalog_generation, backup.catalog_generation);
    let audit = admin.audit(None, 8).await.unwrap();
    assert_eq!(audit.events.len(), 2);
    assert!(audit.events.iter().all(|event| event.operation == "backup"));

    drop(data);
    drop(admin);
    handle.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn vector_compute_is_bounded_observable_and_role_separated() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(EngineConfig::new(directory.path())).unwrap());
    let config = ServerConfig::loopback(DATA_TOKEN, ADMIN_TOKEN).unwrap();
    let handle = Server::start(engine, config).await.unwrap();
    let data = AsyncClient::connect_tcp(handle.data_tcp.unwrap(), ClientConfig::data(DATA_TOKEN))
        .await
        .unwrap();
    let admin =
        AsyncClient::connect_tcp(handle.admin_tcp.unwrap(), ClientConfig::admin(ADMIN_TOKEN))
            .await
            .unwrap();

    for (key, vector) in [
        ("north", vec![1.0, 0.0]),
        ("diagonal", vec![1.0, 1.0]),
        ("west", vec![-1.0, 0.0]),
    ] {
        data.put(
            identity(key),
            Payload::Vector(vector),
            PutOptions::default(),
            Durability::Durable,
        )
        .await
        .unwrap();
    }

    let result = data
        .vector_exact(
            b"tenant".to_vec(),
            b"namespace".to_vec(),
            b"collection".to_vec(),
            vec![1.0, 0.0],
            VectorMetric::Cosine,
            2,
            8,
            ComputePreference::Cpu,
        )
        .await
        .unwrap();
    assert_eq!(result.execution, ComputeExecution::Cpu);
    assert_eq!(result.hits.len(), 2);
    assert_eq!(result.hits[0].identity.key, b"north");
    assert_eq!(result.scanned_records, 3);
    assert_eq!(result.vector_candidates, 3);

    let stats = admin.compute_stats().await.unwrap();
    assert_eq!(stats.requests, 1);
    assert_eq!(stats.cpu_runs, 1);
    let denied = data.compute_stats().await.unwrap_err();
    assert!(matches!(
        denied,
        ClientError::Server {
            code: ErrorCode::Unauthorized,
            ..
        }
    ));
    let denied = admin
        .vector_exact(
            b"tenant".to_vec(),
            b"namespace".to_vec(),
            b"collection".to_vec(),
            vec![1.0, 0.0],
            VectorMetric::Dot,
            1,
            8,
            ComputePreference::Cpu,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        denied,
        ClientError::Server {
            code: ErrorCode::Unauthorized,
            ..
        }
    ));

    drop(data);
    drop(admin);
    handle.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tcp_client_server_is_bounded_versioned_and_role_separated() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(EngineConfig::new(directory.path())).unwrap());
    let config = ServerConfig::loopback(DATA_TOKEN, ADMIN_TOKEN).unwrap();
    let handle = Server::start(engine, config).await.unwrap();
    let data_address = handle.data_tcp.unwrap();
    let admin_address = handle.admin_tcp.unwrap();

    let wrong =
        match AsyncClient::connect_tcp(data_address, ClientConfig::data(b"wrong-token-for-tests"))
            .await
        {
            Ok(_) => panic!("handshake accepted an invalid token"),
            Err(error) => error,
        };
    assert!(matches!(
        wrong,
        ClientError::Handshake {
            code: ErrorCode::Unauthenticated,
            ..
        }
    ));

    let data = AsyncClient::connect_tcp(data_address, ClientConfig::data(DATA_TOKEN))
        .await
        .unwrap();
    let admin = AsyncClient::connect_tcp(admin_address, ClientConfig::admin(ADMIN_TOKEN))
        .await
        .unwrap();

    let first = data
        .put(
            identity("one"),
            Payload::Text("first".into()),
            PutOptions {
                expected: Expected::Missing,
                ..PutOptions::default()
            },
            Durability::Durable,
        )
        .await
        .unwrap();
    let record = data.get(identity("one")).await.unwrap().unwrap();
    assert_eq!(record.payload, Some(Payload::Text("first".into())));
    assert_eq!(record.version, first.version);

    let second = data
        .compare_and_swap(
            identity("one"),
            Payload::Text("second".into()),
            first.version,
            PutOptions::default(),
            Durability::Relaxed,
        )
        .await
        .unwrap();
    assert!(second.version > first.version);

    let (left, right) = tokio::join!(
        data.put(
            identity("parallel-a"),
            Payload::Integer(10),
            PutOptions::default(),
            Durability::Durable,
        ),
        data.put(
            identity("parallel-b"),
            Payload::Integer(20),
            PutOptions::default(),
            Durability::Durable,
        )
    );
    left.unwrap();
    right.unwrap();

    let receipts = data
        .atomic_batch(
            vec![
                Mutation::Put {
                    identity: identity("batch-a"),
                    payload: Payload::Boolean(true),
                    options: PutOptions::default(),
                },
                Mutation::Put {
                    identity: identity("batch-b"),
                    payload: Payload::Boolean(false),
                    options: PutOptions::default(),
                },
            ],
            Durability::Durable,
        )
        .await
        .unwrap();
    assert_eq!(receipts.len(), 2);

    data.delete(
        identity("one"),
        DeleteOptions {
            expected: Expected::Exact(second.version),
            ..DeleteOptions::default()
        },
        Durability::Durable,
    )
    .await
    .unwrap();
    assert!(data.get(identity("one")).await.unwrap().is_none());

    let denied = data.health().await.unwrap_err();
    assert!(matches!(
        denied,
        ClientError::Server {
            code: ErrorCode::Unauthorized,
            ..
        }
    ));
    assert!(admin.health().await.unwrap());
    let placement = admin
        .explain_placement(identity("parallel-a"))
        .await
        .unwrap();
    assert_eq!(placement.storage_class, "primary");
    assert!(!placement.physical_tiering_supported);
    let cache_stats = admin.cache_stats().await.unwrap();
    assert!(cache_stats.objects.resident_bytes <= cache_stats.objects.budget_bytes);
    assert!(cache_stats.compressed.resident_bytes <= cache_stats.compressed.budget_bytes);
    let policy = CompressionPolicy::default();
    admin
        .configure_compression(identity("compression-policy"), policy.clone())
        .await
        .unwrap();
    assert_eq!(
        admin
            .compression_policy(identity("compression-policy"))
            .await
            .unwrap(),
        policy
    );
    let training = (0..16)
        .map(|index| {
            Payload::Text(format!(
                "type=invoice;customer=regional-{index:04};currency=EUR;status=pending;line=widget-alpha;warehouse=rome;tax=standard"
            ))
        })
        .collect::<Vec<_>>();
    let validation = (100..104)
        .map(|index| {
            Payload::Text(format!(
                "type=invoice;customer=regional-{index:04};currency=EUR;status=pending;line=widget-beta;warehouse=rome;tax=standard"
            ))
        })
        .collect::<Vec<_>>();
    let dictionary = admin
        .train_dictionary(
            identity("compression-policy"),
            "application/invoice",
            &training,
            &validation,
            2048,
            0,
        )
        .await
        .unwrap();
    assert!(
        dictionary.validation_with_dictionary_bytes
            < dictionary.validation_without_dictionary_bytes
    );
    let compression = admin.compression_stats().await.unwrap();
    assert!(compression.channels.is_power_of_two());
    assert_eq!(compression.scratch_in_use_bytes, 0);

    data.put(
        identity("expired"),
        Payload::Text("ttl".into()),
        PutOptions {
            expires_at_unix_ms: Some(1),
            ..PutOptions::default()
        },
        Durability::Durable,
    )
    .await
    .unwrap();
    assert!(data.get(identity("expired")).await.unwrap().is_none());
    let expiration = admin.expire(16, Durability::Durable).await.unwrap();
    assert_eq!(expiration.expired, 1);
    admin.verify().await.unwrap();
    let stats = admin.stats().await.unwrap();
    assert!(stats.total_requests >= 10);

    admin.shutdown().await.unwrap();
    handle.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workflow_changes_and_surfaces_are_usable_over_tcp() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(EngineConfig::new(directory.path())).unwrap());
    let config = ServerConfig::loopback(DATA_TOKEN, ADMIN_TOKEN).unwrap();
    let handle = Server::start(engine, config).await.unwrap();
    let data = AsyncClient::connect_tcp(handle.data_tcp.unwrap(), ClientConfig::data(DATA_TOKEN))
        .await
        .unwrap();
    let admin =
        AsyncClient::connect_tcp(handle.admin_tcp.unwrap(), ClientConfig::admin(ADMIN_TOKEN))
            .await
            .unwrap();

    let work = SurfaceDefinition {
        id: "tcp-work".into(),
        kind: SurfaceKind::Work,
        source_tenant: b"tenant".to_vec(),
        source_namespace: b"namespace".to_vec(),
        source_collection: b"collection".to_vec(),
        workflow_states: vec!["pending".into()],
        format: SurfaceFormat::AprodbRecords,
        max_records: 32,
        max_bytes: 1024 * 1024,
        retained_generations: 2,
    };
    let read = SurfaceDefinition {
        id: "tcp-read".into(),
        kind: SurfaceKind::Read,
        workflow_states: vec!["published".into()],
        format: SurfaceFormat::Json,
        ..work.clone()
    };
    admin.create_surface(work.clone()).await.unwrap();
    admin.create_surface(read.clone()).await.unwrap();

    let mut append_options = PutOptions {
        expected: Expected::Missing,
        idempotency_key_hash: Some([0x41; 32]),
        ..PutOptions::default()
    };
    let appended = data
        .append(
            identity("workflow-job"),
            Payload::Text("payload".into()),
            append_options.clone(),
            Durability::Durable,
        )
        .await
        .unwrap();
    let replay = data
        .append(
            identity("workflow-job"),
            Payload::Text("payload".into()),
            append_options.clone(),
            Durability::Durable,
        )
        .await
        .unwrap();
    assert_eq!(replay, appended);
    append_options.idempotency_key_hash = Some([0x42; 32]);

    let changes = data
        .subscribe_changes(
            b"tenant".to_vec(),
            b"namespace".to_vec(),
            b"collection".to_vec(),
            appended.version.shard_id,
            0,
            64,
        )
        .await
        .unwrap();
    assert!(
        changes
            .events
            .iter()
            .any(|event| event.operation == ChangeOperation::Append)
    );
    assert!(changes.watermark >= appended.version.sequence);

    let initial_work = admin
        .build_surface(&work.id, 64, Durability::Durable)
        .await
        .unwrap();
    assert_eq!(initial_work.record_count, 1);
    let scope = WorkflowScope::new("tenant", "namespace", "collection", "partition").unwrap();
    let claimed = data
        .claim(
            scope.clone(),
            1,
            Duration::from_secs(60),
            Some([0x43; 32]),
            Durability::Durable,
        )
        .await
        .unwrap();
    let claim_replay = data
        .claim(
            scope.clone(),
            1,
            Duration::from_secs(60),
            Some([0x43; 32]),
            Durability::Durable,
        )
        .await
        .unwrap();
    assert_eq!(claimed.len(), 1);
    assert_eq!(claim_replay.len(), 1);
    assert_eq!(claim_replay[0].record, claimed[0].record);
    assert_eq!(claim_replay[0].receipt, claimed[0].receipt);
    assert_eq!(claim_replay[0].lease, claimed[0].lease);
    assert_eq!(
        claim_replay[0].lease_deadline_unix_ms,
        claimed[0].lease_deadline_unix_ms
    );
    assert!(claim_replay[0].server_time_unix_ms >= claimed[0].server_time_unix_ms);
    assert_eq!(claimed[0].record.workflow.state, "leased");

    let heartbeat = data
        .heartbeat(
            identity("workflow-job"),
            claimed[0].lease,
            Duration::from_secs(30),
            Some([0x44; 32]),
            Durability::Durable,
        )
        .await
        .unwrap();
    let heartbeat_replay = data
        .heartbeat(
            identity("workflow-job"),
            claimed[0].lease,
            Duration::from_secs(30),
            Some([0x44; 32]),
            Durability::Durable,
        )
        .await
        .unwrap();
    assert_eq!(heartbeat_replay, heartbeat);
    let stale = data
        .complete(
            identity("workflow-job"),
            LeaseProof {
                fencing_token: claimed[0].lease.fencing_token.saturating_add(1),
                ..claimed[0].lease
            },
            None,
            Durability::Durable,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        stale,
        ClientError::Server {
            code: ErrorCode::Conflict,
            ..
        }
    ));

    let failed = data
        .fail(
            identity("workflow-job"),
            claimed[0].lease,
            false,
            Some([0x45; 32]),
            Durability::Durable,
        )
        .await
        .unwrap();
    assert_eq!(failed.record.workflow.state, "pending");
    let reclaimed = data
        .claim(
            scope,
            1,
            Duration::from_secs(60),
            Some([0x46; 32]),
            Durability::Durable,
        )
        .await
        .unwrap();
    let completed = data
        .complete(
            identity("workflow-job"),
            reclaimed[0].lease,
            Some([0x47; 32]),
            Durability::Durable,
        )
        .await
        .unwrap();
    assert_eq!(completed.record.workflow.state, "completed");
    let published = data
        .publish(
            identity("workflow-job"),
            Some([0x48; 32]),
            Durability::Durable,
        )
        .await
        .unwrap();
    assert_eq!(published.record.workflow.state, "published");

    let work_build = admin
        .build_surface(&work.id, 64, Durability::Durable)
        .await
        .unwrap();
    let read_build = admin
        .build_surface(&read.id, 64, Durability::Durable)
        .await
        .unwrap();
    assert_eq!(work_build.record_count, 0);
    assert_eq!(read_build.record_count, 1);
    let work_generation = data
        .get_surface(b"tenant".to_vec(), b"namespace".to_vec(), &work.id)
        .await
        .unwrap()
        .unwrap();
    let read_generation = data
        .get_surface(b"tenant".to_vec(), b"namespace".to_vec(), &read.id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(work_generation.generation.record_count, 0);
    assert_eq!(read_generation.generation.record_count, 1);
    assert!(work_generation.complete);
    assert!(read_generation.complete);
    assert!(
        work_generation
            .stale_by_sequences
            .values()
            .all(|stale| *stale == 0)
    );
    admin.verify().await.unwrap();

    admin.shutdown().await.unwrap();
    handle.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn expired_deadline_is_rejected_before_dispatch() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(EngineConfig::new(directory.path())).unwrap());
    let config = ServerConfig::loopback(DATA_TOKEN, ADMIN_TOKEN).unwrap();
    let maximum = config.max_frame_bytes;
    let handle = Server::start(engine, config).await.unwrap();
    let stream = TcpStream::connect(handle.data_tcp.unwrap()).await.unwrap();
    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(maximum)
        .new_codec();
    let mut framed = Framed::new(stream, codec);
    framed
        .send(
            encode_limited(
                &ClientHello::new(EndpointRole::Data, DATA_TOKEN.to_vec(), maximum),
                maximum,
            )
            .unwrap(),
        )
        .await
        .unwrap();
    let hello: ServerHello =
        decode_limited(&framed.next().await.unwrap().unwrap(), maximum).unwrap();
    assert!(hello.accepted);

    let request = Request {
        request_id: 41,
        deadline_unix_ms: 1,
        tenant: b"tenant".to_vec(),
        namespace: b"namespace".to_vec(),
        durability: WireDurability::Durable as i32,
        operation: Some(request::Operation::Get(GetOperation {
            key: Some(Key {
                collection: b"collection".to_vec(),
                partition: b"partition".to_vec(),
                key: b"key".to_vec(),
            }),
        })),
    };
    framed
        .send(encode_limited(&request, maximum).unwrap())
        .await
        .unwrap();
    let response: Response =
        decode_limited(&framed.next().await.unwrap().unwrap(), maximum).unwrap();
    assert_eq!(response.request_id, 41);
    assert_eq!(
        ErrorCode::try_from(response.error_code).unwrap(),
        ErrorCode::DeadlineExceeded
    );

    drop(framed);
    handle.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn inflight_limit_applies_backpressure_without_an_unbounded_queue() {
    let directory = tempfile::tempdir().unwrap();
    let mut engine_config = EngineConfig::new(directory.path());
    engine_config.group_commit_window = Duration::from_millis(500);
    let engine = Arc::new(Engine::open(engine_config).unwrap());
    let mut config = ServerConfig::loopback(DATA_TOKEN, ADMIN_TOKEN).unwrap();
    config.admin_tcp = None;
    config.max_inflight_per_connection = 1;
    config.max_inflight_global = 1;
    config.response_queue_depth = 1;
    let handle = Server::start(engine, config).await.unwrap();
    let data = AsyncClient::connect_tcp(handle.data_tcp.unwrap(), ClientConfig::data(DATA_TOKEN))
        .await
        .unwrap();

    let (first, second) = tokio::join!(
        data.put(
            identity("bounded-a"),
            Payload::Integer(1),
            PutOptions::default(),
            Durability::Durable,
        ),
        data.put(
            identity("bounded-b"),
            Payload::Integer(2),
            PutOptions::default(),
            Durability::Durable,
        )
    );
    let successes = usize::from(first.is_ok()) + usize::from(second.is_ok());
    let backpressure = [first.as_ref().err(), second.as_ref().err()]
        .into_iter()
        .flatten()
        .filter(|error| {
            matches!(
                error,
                ClientError::Server {
                    code: ErrorCode::Backpressure,
                    ..
                }
            )
        })
        .count();
    assert_eq!(successes, 1);
    assert_eq!(backpressure, 1);
    let retry_hint_present = [first.as_ref().err(), second.as_ref().err()]
        .into_iter()
        .flatten()
        .any(|error| {
            matches!(
                error,
                ClientError::Server {
                    code: ErrorCode::Backpressure,
                    retry_after: Some(_),
                    ..
                }
            )
        });
    assert!(retry_hint_present);

    drop(data);
    handle.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn negotiated_frame_limit_rejects_oversized_request_in_client() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(EngineConfig::new(directory.path())).unwrap());
    let mut config = ServerConfig::loopback(DATA_TOKEN, ADMIN_TOKEN).unwrap();
    config.admin_tcp = None;
    config.max_frame_bytes = aprodb_proto::MIN_FRAME_BYTES;
    let handle = Server::start(engine, config).await.unwrap();
    let mut client_config = ClientConfig::data(DATA_TOKEN);
    client_config.max_frame_bytes = aprodb_proto::MIN_FRAME_BYTES;
    let data = AsyncClient::connect_tcp(handle.data_tcp.unwrap(), client_config)
        .await
        .unwrap();

    let error = data
        .put(
            identity("large"),
            Payload::Bytes(vec![0_u8; aprodb_proto::MIN_FRAME_BYTES * 2]),
            PutOptions::default(),
            Durability::Durable,
        )
        .await
        .unwrap_err();
    assert!(matches!(error, ClientError::Protocol(_)));

    drop(data);
    handle.shutdown().await.unwrap();
}

#[cfg(windows)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn windows_named_pipe_is_ready_when_start_returns() {
    let directory = tempfile::tempdir().unwrap();
    let engine = Arc::new(Engine::open(EngineConfig::new(directory.path())).unwrap());
    let pipe = format!(r"\\.\pipe\aprodb-test-{}", std::process::id());
    let mut config = ServerConfig::loopback(DATA_TOKEN, ADMIN_TOKEN).unwrap();
    config.data_tcp = None;
    config.admin_tcp = None;
    config.local_data = Some(pipe.clone());
    let handle = Server::start(engine, config).await.unwrap();

    let client = AsyncClient::connect_local(&pipe, ClientConfig::data(DATA_TOKEN))
        .await
        .unwrap();
    client
        .put(
            identity("local"),
            Payload::Text("pipe".into()),
            PutOptions::default(),
            Durability::Durable,
        )
        .await
        .unwrap();
    assert_eq!(
        client
            .get(identity("local"))
            .await
            .unwrap()
            .unwrap()
            .payload,
        Some(Payload::Text("pipe".into()))
    );

    drop(client);
    handle.shutdown().await.unwrap();
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unix_socket_accepts_data_and_is_removed_on_shutdown() {
    let directory = tempfile::tempdir().unwrap();
    let socket = directory.path().join("aprodb.sock");
    let engine = Arc::new(Engine::open(EngineConfig::new(directory.path().join("data"))).unwrap());
    let mut config = ServerConfig::loopback(DATA_TOKEN, ADMIN_TOKEN).unwrap();
    config.data_tcp = None;
    config.admin_tcp = None;
    config.local_data = Some(socket.to_string_lossy().into_owned());
    let handle = Server::start(engine, config).await.unwrap();

    let client = AsyncClient::connect_local(&socket, ClientConfig::data(DATA_TOKEN))
        .await
        .unwrap();
    client
        .put(
            identity("local"),
            Payload::Text("socket".into()),
            PutOptions::default(),
            Durability::Durable,
        )
        .await
        .unwrap();
    drop(client);
    handle.shutdown().await.unwrap();
    assert!(!socket.exists());
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
