use std::time::{Duration, Instant};

use aprodb_compute::{
    AcceleratorBackend, ColumnarF32Batch, ComputeBackend, CpuPool, ProjectionDescriptor, ScoredRow,
    VectorMetric, WgpuBackend, WgpuConfig, scores_equivalent,
};

const SAMPLES: usize = 9;
const TOP_K: usize = 20;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cpu = CpuPool::new(
        std::thread::available_parallelism()
            .map(usize::from)
            .unwrap_or(1)
            .min(16),
    )?;
    let gpu = WgpuBackend::new(WgpuConfig {
        vram_budget_bytes: 256 * 1024 * 1024,
        timeout: Duration::from_secs(60),
    })?;

    println!("backend_initial={}", gpu.name());
    println!(
        "rows\twidth\tbytes\tcpu_p50_us\tcpu_p95_us\tcpu_p99_us\tgpu_cold_us\tgpu_warm_p50_us\tgpu_warm_p95_us\tgpu_warm_p99_us\tcpu_rows_s\tgpu_rows_s\ttransfer_us\tkernel_us\tvram_hits"
    );
    for (case, (rows, width)) in [(1_024, 64), (8_192, 64), (65_536, 64), (65_536, 256)]
        .into_iter()
        .enumerate()
    {
        let batch = deterministic_batch(rows, width)?;
        let query = deterministic_query(width);
        let projection = ProjectionDescriptor {
            projection_id: format!("compute-crossover-{case}"),
            source_watermark: 1,
            schema_version: u32::try_from(width)?,
        };

        let (cpu_reference, mut cpu_samples) = measure(SAMPLES, || {
            let scores = cpu.score_vectors(&batch, &query, VectorMetric::Cosine)?;
            Ok::<_, aprodb_types::AproError>(top_k(scores, TOP_K))
        })?;
        let before = gpu.stats();
        let cold_started = Instant::now();
        let cold = top_k(
            gpu.score_accelerated(&batch, &query, VectorMetric::Cosine, Some(&projection))?,
            TOP_K,
        );
        let cold_us = micros(cold_started.elapsed());
        if !scores_equivalent(&cpu_reference, &cold, 1e-4) {
            return Err("ranking GPU freddo non equivalente alla CPU".into());
        }
        let (warm_reference, mut warm_samples) = measure(SAMPLES, || {
            let scores =
                gpu.score_accelerated(&batch, &query, VectorMetric::Cosine, Some(&projection))?;
            Ok::<_, aprodb_types::AproError>(top_k(scores, TOP_K))
        })?;
        if !scores_equivalent(&cpu_reference, &warm_reference, 1e-4) {
            return Err("ranking GPU caldo non equivalente alla CPU".into());
        }
        let after = gpu.stats();
        cpu_samples.sort_unstable();
        warm_samples.sort_unstable();
        let cpu_p50 = percentile(&cpu_samples, 50);
        let gpu_p50 = percentile(&warm_samples, 50);
        println!(
            "{rows}\t{width}\t{}\t{}\t{}\t{}\t{cold_us}\t{}\t{}\t{}\t{:.2}\t{:.2}\t{}\t{}\t{}",
            batch.byte_len(),
            cpu_p50,
            percentile(&cpu_samples, 95),
            percentile(&cpu_samples, 99),
            gpu_p50,
            percentile(&warm_samples, 95),
            percentile(&warm_samples, 99),
            rows_per_second(rows, cpu_p50),
            rows_per_second(rows, gpu_p50),
            after.transfer_micros.saturating_sub(before.transfer_micros),
            after.kernel_micros.saturating_sub(before.kernel_micros),
            after.vram_hits.saturating_sub(before.vram_hits),
        );
        gpu.invalidate_projection(&projection.projection_id);
    }
    println!("backend_final={}", gpu.name());
    Ok(())
}

fn deterministic_batch(rows: usize, width: usize) -> aprodb_types::Result<ColumnarF32Batch> {
    let rows = (0..rows)
        .map(|row| {
            Some(
                (0..width)
                    .map(|column| ((row * 17 + column * 31 + 7) % 257) as f32 / 257.0)
                    .collect(),
            )
        })
        .collect::<Vec<_>>();
    ColumnarF32Batch::from_rows(&rows, width)
}

fn deterministic_query(width: usize) -> Vec<f32> {
    (0..width)
        .map(|column| ((column * 13 + 3) % 127) as f32 / 127.0)
        .collect()
}

fn measure<T, E>(
    samples: usize,
    mut operation: impl FnMut() -> Result<T, E>,
) -> Result<(T, Vec<u64>), E> {
    let mut durations = Vec::with_capacity(samples);
    let mut last = None;
    for _ in 0..samples {
        let started = Instant::now();
        last = Some(operation()?);
        durations.push(micros(started.elapsed()));
    }
    Ok((last.expect("almeno un campione"), durations))
}

fn top_k(scores: Vec<Option<f32>>, limit: usize) -> Vec<ScoredRow> {
    let mut rows = scores
        .into_iter()
        .enumerate()
        .filter_map(|(row, score)| score.map(|score| ScoredRow { row, score }))
        .collect::<Vec<_>>();
    rows.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.row.cmp(&right.row))
    });
    rows.truncate(limit);
    rows
}

fn percentile(sorted: &[u64], percentile: usize) -> u64 {
    let index = sorted
        .len()
        .saturating_mul(percentile)
        .div_ceil(100)
        .saturating_sub(1)
        .min(sorted.len().saturating_sub(1));
    sorted[index]
}

fn micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

fn rows_per_second(rows: usize, micros: u64) -> f64 {
    if micros == 0 {
        return f64::INFINITY;
    }
    rows as f64 * 1_000_000.0 / micros as f64
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
