use aprodb_engine::{
    AtomicMutation, Durability, EncryptionConfig, Engine, EngineConfig, Payload, PutRequest,
    RecordIdentity,
};

fn identity(index: usize) -> RecordIdentity {
    RecordIdentity::new(
        "long-test",
        "operability",
        "records",
        "p0",
        format!("key-{index:06}"),
    )
    .unwrap()
}

#[test]
#[ignore = "long operability gate: 2k durable writes, four verified restore cycles and rekey"]
fn repeated_encrypted_backup_restore_and_rekey_remain_consistent() {
    let directory = tempfile::tempdir().unwrap();
    let source = directory.path().join("source");
    let old_key = EncryptionConfig::single("old", [21; 32]).unwrap();
    let mut source_config = EngineConfig::new(&source);
    source_config.encryption = Some(old_key.clone());
    let engine = Engine::open(source_config.clone()).unwrap();

    for batch in 0..64 {
        let mutations = (0..32)
            .map(|offset| {
                let index = batch * 32 + offset;
                AtomicMutation::Put(PutRequest::new(
                    identity(index),
                    Payload::Bytes(vec![(index % 251) as u8; 1024]),
                ))
            })
            .collect();
        engine.atomic_batch(mutations, Durability::Durable).unwrap();
        if batch % 16 == 15 {
            let generation = batch / 16;
            let backup = directory.path().join(format!("backup-{generation}"));
            let restore = directory.path().join(format!("restore-{generation}"));
            engine.create_backup(&backup).unwrap();
            let mut restore_config = EngineConfig::new(&restore);
            restore_config.encryption = Some(old_key.clone());
            Engine::restore_backup(&backup, &restore, restore_config).unwrap();
            let mut reopened_config = EngineConfig::new(&restore);
            reopened_config.encryption = Some(old_key.clone());
            let restored = Engine::open(reopened_config).unwrap();
            restored.verify().unwrap();
            assert!(restored.get(&identity(batch * 32)).unwrap().is_some());
        }
    }
    engine.verify().unwrap();
    let rekeyed_path = directory.path().join("rekeyed");
    let new_key = EncryptionConfig::single("new", [22; 32]).unwrap();
    engine
        .rekey_to_copy(&rekeyed_path, new_key.clone())
        .unwrap();
    drop(engine);

    let mut rekeyed_config = EngineConfig::new(&rekeyed_path);
    rekeyed_config.encryption = Some(new_key);
    let rekeyed = Engine::open(rekeyed_config).unwrap();
    rekeyed.verify().unwrap();
    assert!(rekeyed.get(&identity(0)).unwrap().is_some());
    assert!(rekeyed.get(&identity(2047)).unwrap().is_some());
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
