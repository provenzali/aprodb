use std::{sync::mpsc, time::Duration};

use bytemuck::{Pod, Zeroable};
use wgpu::util::DeviceExt;

use crate::{AproError, Metric, Result};

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

pub(crate) struct GpuExecutor {
    device: wgpu::Device,
    queue: wgpu::Queue,
    pipeline: wgpu::ComputePipeline,
    adapter_name: String,
}

impl GpuExecutor {
    pub(crate) fn new() -> Result<Self> {
        pollster::block_on(Self::new_async())
    }

    async fn new_async() -> Result<Self> {
        let instance = wgpu::Instance::default();
        let adapter = instance
            .request_adapter(&wgpu::RequestAdapterOptions {
                power_preference: wgpu::PowerPreference::HighPerformance,
                compatible_surface: None,
                force_fallback_adapter: false,
                ..Default::default()
            })
            .await
            .map_err(|error| AproError::GpuUnavailable(error.to_string()))?;
        let adapter_name = adapter.get_info().name;
        let (device, queue) = adapter
            .request_device(&wgpu::DeviceDescriptor {
                label: Some("aprodb-device"),
                ..Default::default()
            })
            .await
            .map_err(|error| AproError::GpuUnavailable(error.to_string()))?;
        let shader = device.create_shader_module(wgpu::ShaderModuleDescriptor {
            label: Some("aprodb-vector-score"),
            source: wgpu::ShaderSource::Wgsl(SHADER.into()),
        });
        let pipeline = device.create_compute_pipeline(&wgpu::ComputePipelineDescriptor {
            label: Some("aprodb-vector-score"),
            layout: None,
            module: &shader,
            entry_point: Some("main"),
            compilation_options: Default::default(),
            cache: None,
        });
        Ok(Self {
            device,
            queue,
            pipeline,
            adapter_name,
        })
    }

    pub(crate) fn adapter_name(&self) -> &str {
        &self.adapter_name
    }

    pub(crate) fn score(
        &self,
        vectors: &[Vec<f32>],
        query: &[f32],
        metric: Metric,
    ) -> Result<Vec<f32>> {
        if vectors.is_empty() {
            return Ok(Vec::new());
        }
        let count: u32 = vectors
            .len()
            .try_into()
            .map_err(|_| AproError::Gpu("troppi vettori per un dispatch GPU".into()))?;
        let dimension: u32 = query
            .len()
            .try_into()
            .map_err(|_| AproError::Gpu("dimensione eccessiva per la GPU".into()))?;
        let flat: Vec<f32> = vectors.iter().flatten().copied().collect();
        let params = Params {
            count,
            dimension,
            metric: u32::from(metric == Metric::Cosine),
            padding: 0,
        };

        let vectors_buffer = self
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("aprodb-vectors"),
                contents: bytemuck::cast_slice(&flat),
                usage: wgpu::BufferUsages::STORAGE,
            });
        let query_buffer = self
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("aprodb-query"),
                contents: bytemuck::cast_slice(query),
                usage: wgpu::BufferUsages::STORAGE,
            });
        let params_buffer = self
            .device
            .create_buffer_init(&wgpu::util::BufferInitDescriptor {
                label: Some("aprodb-params"),
                contents: bytemuck::bytes_of(&params),
                usage: wgpu::BufferUsages::UNIFORM,
            });
        let output_size = u64::from(count) * std::mem::size_of::<f32>() as u64;
        let output_buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("aprodb-scores"),
            size: output_size,
            usage: wgpu::BufferUsages::STORAGE | wgpu::BufferUsages::COPY_SRC,
            mapped_at_creation: false,
        });
        let readback_buffer = self.device.create_buffer(&wgpu::BufferDescriptor {
            label: Some("aprodb-readback"),
            size: output_size,
            usage: wgpu::BufferUsages::COPY_DST | wgpu::BufferUsages::MAP_READ,
            mapped_at_creation: false,
        });

        let layout = self.pipeline.get_bind_group_layout(0);
        let bind_group = self.device.create_bind_group(&wgpu::BindGroupDescriptor {
            label: Some("aprodb-vector-bindings"),
            layout: &layout,
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
        let mut encoder = self
            .device
            .create_command_encoder(&wgpu::CommandEncoderDescriptor {
                label: Some("aprodb-vector-commands"),
            });
        {
            let mut pass = encoder.begin_compute_pass(&wgpu::ComputePassDescriptor {
                label: Some("aprodb-vector-pass"),
                timestamp_writes: None,
            });
            pass.set_pipeline(&self.pipeline);
            pass.set_bind_group(0, &bind_group, &[]);
            pass.dispatch_workgroups(count.div_ceil(128), 1, 1);
        }
        encoder.copy_buffer_to_buffer(&output_buffer, 0, &readback_buffer, 0, output_size);
        let submission = self.queue.submit([encoder.finish()]);

        let (sender, receiver) = mpsc::sync_channel(1);
        readback_buffer.map_async(wgpu::MapMode::Read, .., move |result| {
            let _ = sender.send(result);
        });
        self.device
            .poll(wgpu::PollType::Wait {
                submission_index: Some(submission),
                timeout: Some(Duration::from_secs(30)),
            })
            .map_err(|error| AproError::Gpu(error.to_string()))?;
        receiver
            .recv_timeout(Duration::from_secs(30))
            .map_err(|error| AproError::Gpu(format!("timeout readback: {error}")))?
            .map_err(|error| AproError::Gpu(error.to_string()))?;

        let view = readback_buffer
            .get_mapped_range(..)
            .map_err(|error| AproError::Gpu(error.to_string()))?;
        let result = bytemuck::cast_slice::<u8, f32>(&view).to_vec();
        drop(view);
        readback_buffer.unmap();
        Ok(result)
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
