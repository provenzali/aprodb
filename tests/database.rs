use std::{fs::OpenOptions, io::Write, sync::Arc, thread};

use aprodb::{ComputeBackend, Config, Database, Metric, Value};
use tempfile::tempdir;

#[test]
fn values_survive_reopen_and_delete() {
    let temp = tempdir().unwrap();
    {
        let db = Database::open(Config::new(temp.path())).unwrap();
        db.put("text", Value::Text("ciao".into())).unwrap();
        db.put("number", Value::Integer(42)).unwrap();
        db.put("gone", Value::Bytes(vec![1, 2, 3])).unwrap();
        assert!(db.delete("gone").unwrap());
    }
    let db = Database::open(Config::new(temp.path())).unwrap();
    assert_eq!(db.get("text").unwrap(), Some(Value::Text("ciao".into())));
    assert_eq!(db.get("number").unwrap(), Some(Value::Integer(42)));
    assert_eq!(db.get("gone").unwrap(), None);
}

#[test]
fn batch_prefix_snapshot_and_recovery_work() {
    let temp = tempdir().unwrap();
    let db = Database::open(Config::new(temp.path())).unwrap();
    let batch = (0..100)
        .map(|index| (format!("user:{index:03}"), Value::Integer(index)))
        .collect();
    assert_eq!(db.put_batch(batch).unwrap().len(), 100);
    let rows = db.scan_prefix("user:0", 10).unwrap();
    assert_eq!(rows.len(), 10);
    assert_eq!(rows[0].0, "user:000");
    assert_eq!(db.snapshot().unwrap(), 100);
    drop(db);

    let db = Database::open(Config::new(temp.path())).unwrap();
    assert_eq!(db.stats().unwrap().live_keys, 100);
    assert_eq!(db.get("user:099").unwrap(), Some(Value::Integer(99)));
}

#[test]
fn incomplete_wal_tail_is_repaired() {
    let temp = tempdir().unwrap();
    {
        let db = Database::open(Config::new(temp.path())).unwrap();
        db.put("safe", Value::Text("yes".into())).unwrap();
    }
    let wal = temp.path().join("aprodb.wal");
    OpenOptions::new()
        .append(true)
        .open(&wal)
        .unwrap()
        .write_all(b"APRF\x20\x00")
        .unwrap();
    let before = std::fs::metadata(&wal).unwrap().len();
    let db = Database::open(Config::new(temp.path())).unwrap();
    assert_eq!(db.get("safe").unwrap(), Some(Value::Text("yes".into())));
    assert!(std::fs::metadata(wal).unwrap().len() < before);
}

#[test]
fn concurrent_writes_use_independent_shards() {
    let temp = tempdir().unwrap();
    let db = Arc::new(Database::open(Config::new(temp.path())).unwrap());
    let workers: Vec<_> = (0..8)
        .map(|worker| {
            let db = Arc::clone(&db);
            thread::spawn(move || {
                for item in 0..50 {
                    db.put(format!("worker:{worker}:{item}"), Value::Integer(item))
                        .unwrap();
                }
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(db.stats().unwrap().live_keys, 400);
}

#[test]
fn cpu_vector_search_orders_cosine_similarity() {
    let temp = tempdir().unwrap();
    let mut config = Config::new(temp.path());
    config.gpu_min_work = usize::MAX;
    let db = Database::open(config).unwrap();
    db.put("x", Value::Vector(vec![1.0, 0.0])).unwrap();
    db.put("y", Value::Vector(vec![0.0, 1.0])).unwrap();
    db.put("xy", Value::Vector(vec![0.7, 0.7])).unwrap();
    db.put("wrong-dimension", Value::Vector(vec![1.0, 0.0, 0.0]))
        .unwrap();
    let result = db
        .vector_search(&[1.0, 0.0], 3, Metric::Cosine, ComputeBackend::Cpu)
        .unwrap();
    assert_eq!(result.hits[0].key, "x");
    assert_eq!(result.candidates, 3);
    assert_eq!(result.backend, ComputeBackend::Cpu);
    let automatic = db
        .vector_search(&[1.0, 0.0], 3, Metric::Cosine, ComputeBackend::Auto)
        .unwrap();
    assert_eq!(automatic.backend, ComputeBackend::Cpu);
}

#[cfg(feature = "gpu")]
#[test]
#[ignore = "richiede un adapter GPU compatibile"]
fn gpu_matches_cpu() {
    let temp = tempdir().unwrap();
    let db = Database::open(Config::new(temp.path())).unwrap();
    for index in 0..512 {
        db.put(
            format!("v:{index}"),
            Value::Vector(vec![index as f32, 1.0, 0.5]),
        )
        .unwrap();
    }
    let cpu = db
        .vector_search(&[2.0, 1.0, 0.5], 20, Metric::Dot, ComputeBackend::Cpu)
        .unwrap();
    let gpu = db
        .vector_search(&[2.0, 1.0, 0.5], 20, Metric::Dot, ComputeBackend::Gpu)
        .unwrap();
    assert_eq!(
        cpu.hits.iter().map(|hit| &hit.key).collect::<Vec<_>>(),
        gpu.hits.iter().map(|hit| &hit.key).collect::<Vec<_>>()
    );
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
