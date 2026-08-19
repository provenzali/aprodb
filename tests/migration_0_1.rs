use std::path::Path;

use aprodb::v1::{AproError, Engine, EngineConfig, Payload, RecordIdentity};
use aprodb::{Config, Database, LegacyImportOptions, Value, import_0_1};

fn digest(path: &Path) -> Option<String> {
    path.exists().then(|| {
        blake3::hash(&std::fs::read(path).unwrap())
            .to_hex()
            .to_string()
    })
}

fn identity(key: &str) -> RecordIdentity {
    RecordIdentity::new("legacy", "import", "default", "p0", key).unwrap()
}

#[test]
fn one_shot_import_preserves_source_copy_and_maps_live_0_1_values() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("legacy-source");
    let preserved = directory.path().join("legacy-preserved");
    let destination = directory.path().join("aprodb-1");
    {
        let database = Database::open(Config::new(&source)).unwrap();
        database.put("text", Value::Text("ciao".into())).unwrap();
        database.put("integer", Value::Integer(42)).unwrap();
        database
            .put("vector", Value::Vector(vec![1.0, 2.0]))
            .unwrap();
        database.put("gone", Value::Bytes(vec![1, 2])).unwrap();
        database.snapshot().unwrap();
        database.delete("gone").unwrap();
        database.put("float", Value::Float(3.5)).unwrap();
        database.sync().unwrap();
    }
    let wal_before = digest(&source.join("aprodb.wal"));
    let snapshot_before = digest(&source.join("aprodb.snapshot"));
    assert!(matches!(
        Engine::open(EngineConfig::new(&source)),
        Err(AproError::IncompatibleFormat(_))
    ));

    let destination_config = EngineConfig::new(&destination);
    let report = import_0_1(LegacyImportOptions {
        source: source.clone(),
        source_copy: preserved.clone(),
        destination: destination_config.clone(),
        tenant: b"legacy".to_vec(),
        namespace: b"import".to_vec(),
        collection: b"default".to_vec(),
        partition: b"p0".to_vec(),
        max_records: 100,
        max_stored_bytes: 1024 * 1024,
        max_source_bytes: 16 * 1024 * 1024,
        batch_operations: 16,
    })
    .unwrap();
    assert_eq!(report.records_imported, 4);
    assert_eq!(wal_before, digest(&source.join("aprodb.wal")));
    assert_eq!(snapshot_before, digest(&source.join("aprodb.snapshot")));
    assert!(preserved.join("raw/aprodb.wal").is_file());
    assert!(preserved.join("raw/aprodb.snapshot").is_file());
    assert!(preserved.join("legacy-manifest.json").is_file());

    let imported = Engine::open(destination_config).unwrap();
    assert_eq!(
        imported.get(&identity("text")).unwrap().unwrap().payload,
        Some(Payload::Text("ciao".into()))
    );
    assert_eq!(
        imported.get(&identity("integer")).unwrap().unwrap().payload,
        Some(Payload::Integer(42))
    );
    assert_eq!(
        imported.get(&identity("float")).unwrap().unwrap().payload,
        Some(Payload::Float(3.5))
    );
    assert_eq!(
        imported.get(&identity("vector")).unwrap().unwrap().payload,
        Some(Payload::Vector(vec![1.0, 2.0]))
    );
    assert!(imported.get(&identity("gone")).unwrap().is_none());
    imported.verify().unwrap();
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
