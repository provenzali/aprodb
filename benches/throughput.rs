use std::time::Instant;

use aprodb::{ComputeBackend, Config, Database, Durability, Metric, Value};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let temp = tempfile::tempdir()?;
    let mut config = Config::new(temp.path());
    config.durability = Durability::Relaxed;
    let db = Database::open(config)?;
    let items = 50_000usize;
    let dimension = 64usize;
    let batch = (0..items)
        .map(|row| {
            let vector = (0..dimension)
                .map(|column| ((row * 31 + column * 17) % 1000) as f32 / 1000.0)
                .collect();
            (format!("bench:{row:08}"), Value::Vector(vector))
        })
        .collect();

    let start = Instant::now();
    db.put_batch(batch)?;
    let ingest = start.elapsed();
    let query = vec![0.5; dimension];
    let start = Instant::now();
    let cpu = db.vector_search(&query, 10, Metric::Cosine, ComputeBackend::Cpu)?;
    let cpu_search = start.elapsed();
    let start = Instant::now();
    let gpu_cold = db.vector_search(&query, 10, Metric::Cosine, ComputeBackend::Gpu)?;
    let gpu_cold_search = start.elapsed();
    let start = Instant::now();
    let gpu_warm = db.vector_search(&query, 10, Metric::Cosine, ComputeBackend::Gpu)?;
    let gpu_warm_search = start.elapsed();

    assert_eq!(
        cpu.hits.iter().map(|hit| &hit.key).collect::<Vec<_>>(),
        gpu_warm.hits.iter().map(|hit| &hit.key).collect::<Vec<_>>()
    );

    println!(
        "ingest={:.0} ops/s cpu={:.2?} gpu_cold={:.2?} gpu_warm={:.2?} adapter={} candidates={}",
        items as f64 / ingest.as_secs_f64(),
        cpu_search,
        gpu_cold_search,
        gpu_warm_search,
        gpu_cold.accelerator.as_deref().unwrap_or("unknown"),
        cpu.candidates
    );
    Ok(())
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
