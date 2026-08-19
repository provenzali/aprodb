mod cpu;

#[cfg(feature = "gpu")]
mod gpu;

pub(crate) use cpu::score_vectors_cpu;

#[cfg(feature = "gpu")]
pub(crate) use gpu::GpuExecutor;
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
