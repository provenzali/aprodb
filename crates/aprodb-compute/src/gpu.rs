use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
        mpsc,
    },
    time::{Duration, Instant},
};

use aprodb_types::{AproError, Result};
use bytemuck::{Pod, Zeroable};
use parking_lot::Mutex;
use wgpu::util::DeviceExt;

use crate::{
    AcceleratorBackend, AcceleratorStats, ColumnarF32Batch, ProjectionDescriptor, VectorMetric,
};

const SHADER: &str = r#"
struct Params {
    count: u32,
    dimension: u32,
    metric: u32,
    _padding: u32,
}

@group(0) @binding(0) var<storage, read> vectors: array<f32>;
@group(0) @binding(1) var<storage, read> query: array<f32>;
@group(0) @binding(2) var<storage, read_write> scores: array<f32>;
@group(0) @binding(3) var<uniform> params: Params;

@compute @workgroup_size(128)
fn main(@builtin(global_invocation_id) id: vec3<u32>) {
    let row = id.x;
    if (row >= params.count) {
        return;
    }
    var dot = 0.0;
    var norm_vector = 0.0;
    var norm_query = 0.0;
    let offset = row * params.dimension;
    for (var column = 0u; column < params.dimension; column += 1u) {
        let left = vectors[offset + column];
        let right = query[column];
        dot += left * right;
        if (params.metric == 1u) {
            norm_vector += left * left;
            norm_query += right * right;
        }
    }
    if (params.metric == 1u) {
        let denominator = sqrt(norm_vector * norm_query);
        scores[row] = select(0.0, dot / denominator, denominator > 0.0);
    } else {
        scores[row] = dot;
    }
}
"#;

#[repr(C)]
#[derive(Clone, Copy, Pod, Zeroable)]
struct Params {
    count: u32,
    dimension: u32,
    metric: u32,
    padding: u32,
}

#[derive(Clone, Debug)]
pub struct WgpuConfig {
    pub vram_budget_bytes: usize,
    pub timeout: Duration,
}

impl WgpuConfig {
    fn validate(&self) -> Result<()> {
        if self.vram_budget_bytes == 0
            || self.timeout.is_zero()
            || self.timeout > Duration::from_secs(300)
        {
            return Err(AproError::InvalidInput("invalid wgpu configuration".into()));
        }
        Ok(())
    }
}

struct CachedProjection {
    buffer: Arc<wgpu::Buffer>,
    rows: usize,
    width: usize,
    bytes: usize,
    last_used: u64,
}

struct GpuContext {
    device: wgpu::Device,
    queue: wgpu::Queue,
    pipeline: wgpu::ComputePipeline,
    adapter_name: String,
    cache: HashMap<ProjectionDescriptor, CachedProjection>,
    resident_bytes: usize,
    access_clock: u64,
}

#[derive(Default)]
struct GpuCounters {
    resident_bytes: AtomicUsize,
    entries: AtomicUsize,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    upload_bytes: AtomicU64,
    readback_bytes: AtomicU64,
    transfer_micros: AtomicU64,
    kernel_micros: AtomicU64,
    device_resets: AtomicU64,
}

pub struct WgpuBackend {
    config: WgpuConfig,
    context: Mutex<Option<GpuContext>>,
    counters: GpuCounters,
}

impl WgpuBackend {
    pub fn new(config: WgpuConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            config,
            context: Mutex::new(None),
            counters: GpuCounters::default(),
        })
    }

    fn initialize() -> Result<GpuContext> {
        pollster::block_on(async {
            let instance = wgpu::Instance::default();
            let adapter = instance
                .request_adapter(&wgpu::RequestAdapterOptions {
                    power_preference: wgpu::PowerPreference::HighPerformance,
                    compatible_surface: None,
                    force_fallback_adapter: false,
                    ..Default::default()
                })
                .await
                .map_err(|error| AproError::Compute(format!("adapter wgpu: {error}")))?;
            let adapter_name = adapter.get_info().name;
            let (device, queue) = adapter
                .request_device(&wgpu::DeviceDescriptor {
                    label: Some("aprodb-v1-device"),
                    ..Default::default()
                })
                .await
                .map_err(|error| AproError::Compute(format!("device wgpu: {error}")))?;
            let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
                label: Some("aprodb-v1-vector-exact"),
                source: wgpu::ShaderSource::Wgsl(SHADER.into()),
            });
            let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
                label: Some("aprodb-v1-vector-exact"),
                layout: None,
                module: &shader,
                entry_point: Some("main"),
                compilation_options: Default::default(),
                cache: None,
            });
            Ok(GpuContext {
                device,
                queue,
                pipeline,
                adapter_name,
                cache: HashMap::new(),
                resident_bytes: 0,
                access_clock: 0,
            })
        })
    }

    fn reset(&self, context: &mut Option<GpuContext>) {
        *context = None;
        self.counters.resident_bytes.store(0, Ordering::Relaxed);
        self.counters.entries.store(0, Ordering::Relaxed);
        self.counters.device_resets.fetch_add(1, Ordering::Relaxed);
    }

    fn score_with_context(
        &self,
        context: &mut GpuContext,
        batch: &ColumnarF32Batch,
        query: &[f32],
        metric: VectorMetric,
        projection: Option<&ProjectionDescriptor>,
    ) -> Result<Vec<Option<f32>>> {
        if batch.rows() == 0 {
            return Ok(Vec::new());
        }
        let count = u32::try_from(batch.rows())
            .map_err(|_| AproError::ResourceLimit("too many vectors for wgpu".into()))?;
        let dimension = u32::try_from(batch.width())
            .map_err(|_| AproError::ResourceLimit("vector dimension exceeds u32".into()))?;
        let values_bytes = batch.values().len().saturating_mul(size_of::<f32>());
        let max_storage = context.device.limits().max_storage_buffer_binding_size as usize;
        if values_bytes > max_storage {
            return Err(AproError::ResourceLimit(format!(
                "vector buffer {values_bytes} exceeds wgpu limit {max_storage}"
            )));
        }

        let transfer_started = Instant::now();
        let vectors_buffer = if let Some(projection) = projection {
            context.access_clock = context.access_clock.saturating_add(1);
            if let Some(cached) = context.cache.get_mut(projection) {
                if cached.rows == batch.rows() && cached.width == batch.width() {
                    cached.last_used = context.access_clock;
                    self.counters.hits.fetch_add(1, Ordering::Relaxed);
                    Arc::clone(&cached.buffer)
                } else {
                    context.cache.remove(projection);
                    self.counters.misses.fetch_add(1, Ordering::Relaxed);
                    self.upload_projection(context, batch, projection, values_bytes)
                }
            } else {
                self.counters.misses.fetch_add(1, Ordering::Relaxed);
                self.upload_projection(context, batch, projection, values_bytes)
            }
        } else {
            self.counters.misses.fetch_add(1, Ordering::Relaxed);
            self.counters
                .upload_bytes
                .fetch_add(values_bytes as u64, Ordering::Relaxed);
            Arc::new(
                context
                    .device
                    .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                        label: Some("aprodb-v1-vectors-transient"),
                        contents: bytemuck::cast_slice(batch.values()),
                        usage: wgpu::BufferUsages::STORAGE,
                    }),
            )
        };
        let query_bytes = query.len().saturating_mul(size_of::<f32>());
        self.counters
            .upload_bytes
            .fetch_add(query_bytes as u64, Ordering::Relaxed);
        let query_buffer = context
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("aprodb-v1-query"),
                contents: bytemuck::cast_slice(query),
                usage: wgpu::BufferUsages::STORAGE,
            });
        let params = Params {
            count,
            dimension,
            metric: u32::from(metric == VectorMetric::Cosine),
            padding: 0,
        };
        let params_buffer = context
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("aprodb-v1-vector-params"),
                contents: bytemuck::bytes_of(&params),
                usage: wgpu::BufferUsages::UNIFORM,
            });
        self.counters.transfer_micros.fetch_add(
            duration_micros(transfer_started.elapsed()),
            Ordering::Relaxed,
        );

        let output_size = u64::from(count) * size_of::<f32>() as u64;
        let output_buffer = context.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("aprodb-v1-vector-scores"),
            size: output_size,
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
            mapped_at_creation: false,
        });
        let readback_buffer = context.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("aprodb-v1-vector-readback"),
            size: output_size,
            usage: wgpu::BufferUsages::COPY_DST | wgpu::BufferUsages::MAP_READ,
            mapped_at_creation: false,
        });
        let bind_group = context
            .device
            .create_bind_group(&wgpu::BindGroupDescriptor {
                label: Some("aprodb-v1-vector-bindings"),
                layout: &context.pipeline.get_bind_group_layout(0),
                entries: &[
                    wgpu::BindGroupEntry {
                        binding: 0,
                        resource: vectors_buffer.as_entire_binding(),
                    },
                    wgpu::BindGroupEntry {
                        binding: 1,
                        resource: query_buffer.as_entire_binding(),
                    },
                    wgpu::BindGroupEntry {
                        binding: 2,
                        resource: output_buffer.as_entire_binding(),
                    },
                    wgpu::BindGroupEntry {
                        binding: 3,
                        resource: params_buffer.as_entire_binding(),
                    },
                ],
            });
        let kernel_started = Instant::now();
        let mut encoder = context
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("aprodb-v1-vector-commands"),
            });
        {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("aprodb-v1-vector-pass"),
                timestamp_writes: None,
            });
            pass.set_pipeline(&context.pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            pass.dispatch_workgroups(count.div_ceil(128), 1, 1);
        }
        encoder.copy_buffer_to_buffer(&output_buffer, 0, &readback_buffer, 0, output_size);
        let submission = context.queue.submit([encoder.finish()]);
        let (sender, receiver) = mpsc::sync_channel(1);
        readback_buffer.map_async(wgpu::MapMode::Read, .., move |result| {
            let _ = sender.send(result);
        });
        context
            .device
            .poll(wgpu::PollType::Wait {
                submission_index: Some(submission),
                timeout: Some(self.config.timeout),
            })
            .map_err(|error| AproError::Compute(format!("poll wgpu: {error}")))?;
        receiver
            .recv_timeout(self.config.timeout)
            .map_err(|_| AproError::Compute("timeout readback wgpu".into()))?
            .map_err(|error| AproError::Compute(format!("map readback wgpu: {error}")))?;
        self.counters
            .kernel_micros
            .fetch_add(duration_micros(kernel_started.elapsed()), Ordering::Relaxed);
        self.counters
            .readback_bytes
            .fetch_add(output_size, Ordering::Relaxed);
        let view = readback_buffer
            .get_mapped_range(..)
            .map_err(|error| AproError::Compute(format!("mapped range wgpu: {error}")))?;
        let scores = bytemuck::cast_slice::<u8, f32>(&view)
            .iter()
            .copied()
            .enumerate()
            .map(|(row, score)| {
                if batch.is_valid(row) {
                    if score.is_finite() {
                        Ok(Some(score))
                    } else {
                        Err(AproError::Compute(
                            "wgpu produced a non-finite score".into(),
                        ))
                    }
                } else {
                    Ok(None)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        drop(view);
        readback_buffer.unmap();
        Ok(scores)
    }

    fn upload_projection(
        &self,
        context: &mut GpuContext,
        batch: &ColumnarF32Batch,
        projection: &ProjectionDescriptor,
        bytes: usize,
    ) -> Arc<wgpu::Buffer> {
        let buffer = Arc::new(context.device.create_buffer_init(
            &wgpu::util::BufferInitDescriptor {
                label: Some("aprodb-v1-vram-projection"),
                contents: bytemuck::cast_slice(batch.values()),
                usage: wgpu::BufferUsages::STORAGE,
            },
        ));
        self.counters
            .upload_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        context.cache.retain(|descriptor, entry| {
            let retain = descriptor.projection_id != projection.projection_id;
            if !retain {
                context.resident_bytes = context.resident_bytes.saturating_sub(entry.bytes);
            }
            retain
        });
        if bytes <= self.config.vram_budget_bytes {
            while context.resident_bytes.saturating_add(bytes) > self.config.vram_budget_bytes {
                let victim = context
                    .cache
                    .iter()
                    .min_by_key(|(_, entry)| entry.last_used)
                    .map(|(descriptor, _)| descriptor.clone());
                let Some(victim) = victim else {
                    break;
                };
                if let Some(entry) = context.cache.remove(&victim) {
                    context.resident_bytes = context.resident_bytes.saturating_sub(entry.bytes);
                    self.counters.evictions.fetch_add(1, Ordering::Relaxed);
                }
            }
            context.resident_bytes = context.resident_bytes.saturating_add(bytes);
            context.cache.insert(
                projection.clone(),
                CachedProjection {
                    buffer: Arc::clone(&buffer),
                    rows: batch.rows(),
                    width: batch.width(),
                    bytes,
                    last_used: context.access_clock,
                },
            );
            self.counters
                .resident_bytes
                .store(context.resident_bytes, Ordering::Relaxed);
            self.counters
                .entries
                .store(context.cache.len(), Ordering::Relaxed);
        }
        buffer
    }
}

impl AcceleratorBackend for WgpuBackend {
    fn name(&self) -> String {
        self.context.lock().as_ref().map_or_else(
            || "wgpu (lazy)".into(),
            |context| context.adapter_name.clone(),
        )
    }

    fn is_cached(&self, projection: &ProjectionDescriptor) -> bool {
        self.context
            .lock()
            .as_ref()
            .is_some_and(|context| context.cache.contains_key(projection))
    }

    fn score_accelerated(
        &self,
        batch: &ColumnarF32Batch,
        query: &[f32],
        metric: VectorMetric,
        projection: Option<&ProjectionDescriptor>,
    ) -> Result<Vec<Option<f32>>> {
        let mut guard = self.context.lock();
        if guard.is_none() {
            *guard = Some(Self::initialize()?);
        }
        let result = self.score_with_context(
            guard.as_mut().expect("initialized context"),
            batch,
            query,
            metric,
            projection,
        );
        if result.is_err() {
            self.reset(&mut guard);
        }
        result
    }

    fn invalidate_projection(&self, projection_id: &str) {
        let mut guard = self.context.lock();
        if let Some(context) = guard.as_mut() {
            context.cache.retain(|descriptor, entry| {
                let retain = descriptor.projection_id != projection_id;
                if !retain {
                    context.resident_bytes = context.resident_bytes.saturating_sub(entry.bytes);
                }
                retain
            });
            self.counters
                .resident_bytes
                .store(context.resident_bytes, Ordering::Relaxed);
            self.counters
                .entries
                .store(context.cache.len(), Ordering::Relaxed);
        }
    }

    fn stats(&self) -> AcceleratorStats {
        AcceleratorStats {
            vram_budget_bytes: self.config.vram_budget_bytes,
            vram_resident_bytes: self.counters.resident_bytes.load(Ordering::Relaxed),
            vram_entries: self.counters.entries.load(Ordering::Relaxed),
            vram_hits: self.counters.hits.load(Ordering::Relaxed),
            vram_misses: self.counters.misses.load(Ordering::Relaxed),
            vram_evictions: self.counters.evictions.load(Ordering::Relaxed),
            upload_bytes: self.counters.upload_bytes.load(Ordering::Relaxed),
            readback_bytes: self.counters.readback_bytes.load(Ordering::Relaxed),
            transfer_micros: self.counters.transfer_micros.load(Ordering::Relaxed),
            kernel_micros: self.counters.kernel_micros.load(Ordering::Relaxed),
            device_resets: self.counters.device_resets.load(Ordering::Relaxed),
        }
    }
}

fn duration_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ComputeBackend, CpuReference, ScoredRow, exact_top_k, scores_equivalent};

    #[test]
    fn wgpu_matches_cpu_when_an_adapter_is_available() {
        let backend = WgpuBackend::new(WgpuConfig {
            vram_budget_bytes: 8 * 1024 * 1024,
            timeout: Duration::from_secs(30),
        })
        .unwrap();
        let rows = (0..512)
            .map(|row| {
                Some(
                    (0..64)
                        .map(|column| ((row * 17 + column * 31) % 101) as f32 / 101.0)
                        .collect::<Vec<_>>(),
                )
            })
            .collect::<Vec<_>>();
        let batch = ColumnarF32Batch::from_rows(&rows, 64).unwrap();
        let query = (0..64)
            .map(|column| ((column * 13) % 47) as f32 / 47.0)
            .collect::<Vec<_>>();
        let gpu_scores = match backend.score_accelerated(
            &batch,
            &query,
            VectorMetric::Cosine,
            Some(&ProjectionDescriptor {
                projection_id: "gpu-equivalence".into(),
                source_watermark: 1,
                schema_version: 1,
            }),
        ) {
            Ok(scores) => scores,
            Err(error) if error.to_string().contains("adapter wgpu") => return,
            Err(error) => panic!("backend wgpu disponibile ma fallito: {error}"),
        };
        let cpu_scores = CpuReference
            .score_vectors(&batch, &query, VectorMetric::Cosine)
            .unwrap();
        let top = |scores: Vec<Option<f32>>| {
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
            rows.truncate(20);
            rows
        };
        let cpu_top = exact_top_k(&CpuReference, &batch, &query, VectorMetric::Cosine, 20).unwrap();
        assert!(scores_equivalent(&cpu_top, &top(gpu_scores), 1e-4));
        assert!(scores_equivalent(&cpu_top, &top(cpu_scores), 0.0));
        let projection = ProjectionDescriptor {
            projection_id: "gpu-equivalence".into(),
            source_watermark: 1,
            schema_version: 1,
        };
        backend
            .score_accelerated(&batch, &query, VectorMetric::Cosine, Some(&projection))
            .unwrap();
        assert!(backend.stats().vram_hits >= 1);
        backend.invalidate_projection("gpu-equivalence");
        assert!(!backend.is_cached(&projection));
        let misses = backend.stats().vram_misses;
        let after_invalidation = backend
            .score_accelerated(&batch, &query, VectorMetric::Cosine, Some(&projection))
            .unwrap();
        assert!(backend.stats().vram_misses > misses);
        assert!(scores_equivalent(&cpu_top, &top(after_invalidation), 1e-4));
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
