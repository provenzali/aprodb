use std::{
    cmp::Ordering,
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering as AtomicOrdering},
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

use aprodb_types::{AproError, Result};
pub use aprodb_types::{ComputeExecution, ComputePreference, CostEstimate, VectorMetric};
use crossbeam_channel::{Receiver, Sender, TrySendError, bounded};
use parking_lot::Mutex;
use rayon::{ThreadPool, ThreadPoolBuilder, prelude::*};

#[cfg(feature = "gpu")]
mod gpu;

#[cfg(feature = "gpu")]
pub use gpu::{WgpuBackend, WgpuConfig};

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ProjectionDescriptor {
    pub projection_id: String,
    pub source_watermark: u64,
    pub schema_version: u32,
}

impl ProjectionDescriptor {
    pub fn validate(&self) -> Result<()> {
        if self.projection_id.is_empty() || self.projection_id.len() > 128 {
            return Err(AproError::InvalidInput(
                "compute projection ID must be between 1 and 128 bytes long".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ColumnarLayout {
    pub rows: usize,
    pub width: usize,
    pub value_alignment_bytes: usize,
    pub validity_word_bits: usize,
    pub value_bytes: usize,
    pub validity_bytes: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ColumnarF32Batch {
    values: Vec<f32>,
    validity: Vec<u32>,
    rows: usize,
    width: usize,
}

impl ColumnarF32Batch {
    pub fn from_rows(rows: &[Option<Vec<f32>>], width: usize) -> Result<Self> {
        if width == 0 {
            return Err(AproError::InvalidInput(
                "columnar width must be positive".into(),
            ));
        }
        let value_count = rows
            .len()
            .checked_mul(width)
            .ok_or_else(|| AproError::ResourceLimit("columnar batch too large".into()))?;
        let mut values = Vec::with_capacity(value_count);
        let mut validity = vec![0u32; rows.len().div_ceil(32)];
        for (row_index, row) in rows.iter().enumerate() {
            match row {
                Some(row) => {
                    if row.len() != width || row.iter().any(|value| !value.is_finite()) {
                        return Err(AproError::InvalidInput(
                            "columnar row is either non-finite or has an incorrect size".into(),
                        ));
                    }
                    values.extend_from_slice(row);
                    validity[row_index / 32] |= 1u32 << (row_index % 32);
                }
                None => values.resize(values.len() + width, 0.0),
            }
        }
        Ok(Self {
            values,
            validity,
            rows: rows.len(),
            width,
        })
    }

    fn concatenate(batches: &[&Self]) -> Result<Self> {
        let width = batches.first().map_or(1, |batch| batch.width);
        if batches.iter().any(|batch| batch.width != width) {
            return Err(AproError::InvalidInput(
                "micro-batches have incompatible widths".into(),
            ));
        }
        let rows = batches.iter().try_fold(0usize, |total, batch| {
            total.checked_add(batch.rows).ok_or_else(|| {
                AproError::ResourceLimit("micro-batch rows exceed usize limit".into())
            })
        })?;
        let mut materialized = Vec::with_capacity(rows);
        for batch in batches {
            for row in 0..batch.rows {
                materialized.push(batch.row(row).map(<[f32]>::to_vec));
            }
        }
        Self::from_rows(&materialized, width)
    }

    #[must_use]
    pub const fn rows(&self) -> usize {
        self.rows
    }

    #[must_use]
    pub const fn width(&self) -> usize {
        self.width
    }

    #[must_use]
    pub fn is_valid(&self, row: usize) -> bool {
        row < self.rows && self.validity[row / 32] & (1u32 << (row % 32)) != 0
    }

    #[must_use]
    pub fn row(&self, row: usize) -> Option<&[f32]> {
        if !self.is_valid(row) {
            return None;
        }
        let start = row * self.width;
        Some(&self.values[start..start + self.width])
    }

    #[must_use]
    pub fn values(&self) -> &[f32] {
        &self.values
    }

    #[must_use]
    pub fn validity(&self) -> &[u32] {
        &self.validity
    }

    #[must_use]
    pub fn byte_len(&self) -> usize {
        self.values
            .len()
            .saturating_mul(size_of::<f32>())
            .saturating_add(self.validity.len().saturating_mul(size_of::<u32>()))
    }

    #[must_use]
    pub fn layout(&self) -> ColumnarLayout {
        ColumnarLayout {
            rows: self.rows,
            width: self.width,
            value_alignment_bytes: align_of::<f32>(),
            validity_word_bits: u32::BITS as usize,
            value_bytes: self.values.len().saturating_mul(size_of::<f32>()),
            validity_bytes: self.validity.len().saturating_mul(size_of::<u32>()),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct ScoredRow {
    pub row: usize,
    pub score: f32,
}

pub trait ComputeBackend: Send + Sync {
    fn name(&self) -> &str;
    fn score_vectors(
        &self,
        batch: &ColumnarF32Batch,
        query: &[f32],
        metric: VectorMetric,
    ) -> Result<Vec<Option<f32>>>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct CpuReference;

impl ComputeBackend for CpuReference {
    fn name(&self) -> &str {
        "cpu-reference"
    }

    fn score_vectors(
        &self,
        batch: &ColumnarF32Batch,
        query: &[f32],
        metric: VectorMetric,
    ) -> Result<Vec<Option<f32>>> {
        validate_query(batch, query)?;
        (0..batch.rows())
            .into_par_iter()
            .map(|row| {
                batch
                    .row(row)
                    .map(|vector| score(vector, query, metric))
                    .transpose()
            })
            .collect()
    }
}

pub struct CpuPool {
    pool: ThreadPool,
    name: String,
}

impl CpuPool {
    pub fn new(threads: usize) -> Result<Self> {
        if threads == 0 || threads > 256 {
            return Err(AproError::InvalidInput(
                "CPU thread pool must have between 1 and 256 threads".into(),
            ));
        }
        let pool = ThreadPoolBuilder::new()
            .num_threads(threads)
            .thread_name(|index| format!("aprodb-compute-cpu-{index}"))
            .build()
            .map_err(|error| AproError::Compute(format!("CPU thread pool error: {error}")))?;
        Ok(Self {
            pool,
            name: format!("cpu-pool-{threads}"),
        })
    }
}

impl ComputeBackend for CpuPool {
    fn name(&self) -> &str {
        &self.name
    }

    fn score_vectors(
        &self,
        batch: &ColumnarF32Batch,
        query: &[f32],
        metric: VectorMetric,
    ) -> Result<Vec<Option<f32>>> {
        validate_query(batch, query)?;
        self.pool.install(|| {
            (0..batch.rows())
                .into_par_iter()
                .map(|row| {
                    batch
                        .row(row)
                        .map(|vector| score(vector, query, metric))
                        .transpose()
                })
                .collect()
        })
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AcceleratorStats {
    pub vram_budget_bytes: usize,
    pub vram_resident_bytes: usize,
    pub vram_entries: usize,
    pub vram_hits: u64,
    pub vram_misses: u64,
    pub vram_evictions: u64,
    pub upload_bytes: u64,
    pub readback_bytes: u64,
    pub transfer_micros: u64,
    pub kernel_micros: u64,
    pub device_resets: u64,
}

pub trait AcceleratorBackend: Send + Sync {
    fn name(&self) -> String;
    fn is_cached(&self, projection: &ProjectionDescriptor) -> bool;
    fn score_accelerated(
        &self,
        batch: &ColumnarF32Batch,
        query: &[f32],
        metric: VectorMetric,
        projection: Option<&ProjectionDescriptor>,
    ) -> Result<Vec<Option<f32>>>;
    fn invalidate_projection(&self, projection_id: &str);
    fn stats(&self) -> AcceleratorStats;
}

#[derive(Clone, Debug)]
pub struct ComputeRequest {
    pub batch: ColumnarF32Batch,
    pub query: Vec<f32>,
    pub metric: VectorMetric,
    pub limit: usize,
    pub preference: ComputePreference,
    pub projection: Option<ProjectionDescriptor>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ComputeResult {
    pub rows: Vec<ScoredRow>,
    pub execution: ComputeExecution,
    pub accelerator: Option<String>,
    pub estimate: CostEstimate,
    pub fallback_reason: Option<String>,
}

#[derive(Clone, Copy, Debug)]
pub struct CostModel {
    pub cpu_nanos_per_element: u64,
    pub accelerator_nanos_per_element: u64,
    pub transfer_nanos_per_byte: u64,
    pub launch_micros: u64,
    pub synchronization_micros: u64,
    pub risk_margin_micros: u64,
}

impl Default for CostModel {
    fn default() -> Self {
        Self {
            cpu_nanos_per_element: 8,
            accelerator_nanos_per_element: 1,
            transfer_nanos_per_byte: 1,
            launch_micros: 250,
            synchronization_micros: 100,
            risk_margin_micros: 250,
        }
    }
}

impl CostModel {
    #[must_use]
    pub fn estimate(
        &self,
        batch: &ColumnarF32Batch,
        query_bytes: usize,
        queue_wait: Duration,
        cache_hit: bool,
    ) -> CostEstimate {
        let elements = batch.rows.saturating_mul(batch.width) as u64;
        let transfer_in_bytes = if cache_hit {
            query_bytes
        } else {
            batch.byte_len().saturating_add(query_bytes)
        };
        let transfer_in_micros = nanos_to_micros(
            (transfer_in_bytes as u64).saturating_mul(self.transfer_nanos_per_byte),
        );
        let accelerator_compute_micros =
            nanos_to_micros(elements.saturating_mul(self.accelerator_nanos_per_element));
        let transfer_out_micros = nanos_to_micros(
            (batch.rows.saturating_mul(size_of::<f32>()) as u64)
                .saturating_mul(self.transfer_nanos_per_byte),
        );
        let queue_wait_micros = duration_micros(queue_wait);
        let accelerator_total_micros = transfer_in_micros
            .saturating_add(queue_wait_micros)
            .saturating_add(self.launch_micros)
            .saturating_add(accelerator_compute_micros)
            .saturating_add(transfer_out_micros)
            .saturating_add(self.synchronization_micros)
            .saturating_add(self.risk_margin_micros);
        CostEstimate {
            transfer_in_micros,
            queue_wait_micros,
            launch_micros: self.launch_micros,
            accelerator_compute_micros,
            transfer_out_micros,
            synchronization_micros: self.synchronization_micros,
            risk_margin_micros: self.risk_margin_micros,
            accelerator_total_micros,
            cpu_compute_micros: nanos_to_micros(
                elements.saturating_mul(self.cpu_nanos_per_element),
            ),
            vram_cache_hit: cache_hit,
        }
    }
}

#[derive(Clone, Debug)]
pub struct SchedulerConfig {
    pub cpu_threads: usize,
    pub queue_depth: usize,
    pub queue_byte_budget: usize,
    pub accelerator_workers: usize,
    pub max_batch_rows: usize,
    pub max_batch_bytes: usize,
    pub micro_batch_max_wait: Duration,
    pub request_timeout: Duration,
    pub failure_threshold: u32,
    pub cooldown: Duration,
    pub vram_budget_bytes: usize,
    pub model: CostModel,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            cpu_threads: std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(1)
                .min(16),
            queue_depth: 64,
            queue_byte_budget: 64 * 1024 * 1024,
            accelerator_workers: 1,
            max_batch_rows: 1_000_000,
            max_batch_bytes: 256 * 1024 * 1024,
            micro_batch_max_wait: Duration::from_millis(1),
            request_timeout: Duration::from_secs(30),
            failure_threshold: 3,
            cooldown: Duration::from_secs(30),
            vram_budget_bytes: 256 * 1024 * 1024,
            model: CostModel::default(),
        }
    }
}

impl SchedulerConfig {
    pub fn validate(&self) -> Result<()> {
        if self.cpu_threads == 0
            || self.cpu_threads > 256
            || self.queue_depth == 0
            || self.queue_byte_budget == 0
            || self.accelerator_workers == 0
            || self.accelerator_workers > 8
            || self.max_batch_rows == 0
            || self.max_batch_bytes == 0
            || self.request_timeout.is_zero()
            || self.request_timeout > Duration::from_secs(300)
            || self.micro_batch_max_wait > self.request_timeout
            || self.failure_threshold == 0
            || self.cooldown.is_zero()
            || self.vram_budget_bytes == 0
        {
            return Err(AproError::InvalidInput(
                "invalid compute scheduler configuration".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SchedulerMetrics {
    pub requests: u64,
    pub cpu_runs: u64,
    pub accelerator_runs: u64,
    pub cpu_fallbacks: u64,
    pub queue_rejections: u64,
    pub accelerator_failures: u64,
    pub request_timeouts: u64,
    pub circuit_open_rejections: u64,
    pub micro_batches: u64,
    pub micro_batched_requests: u64,
    pub inflight_bytes: usize,
    pub peak_inflight_bytes: usize,
}

#[derive(Default)]
struct SchedulerCounters {
    requests: AtomicU64,
    cpu_runs: AtomicU64,
    accelerator_runs: AtomicU64,
    cpu_fallbacks: AtomicU64,
    queue_rejections: AtomicU64,
    accelerator_failures: AtomicU64,
    request_timeouts: AtomicU64,
    circuit_open_rejections: AtomicU64,
    micro_batches: AtomicU64,
    micro_batched_requests: AtomicU64,
    inflight_bytes: AtomicUsize,
    peak_inflight_bytes: AtomicUsize,
}

impl SchedulerCounters {
    fn snapshot(&self) -> SchedulerMetrics {
        SchedulerMetrics {
            requests: self.requests.load(AtomicOrdering::Relaxed),
            cpu_runs: self.cpu_runs.load(AtomicOrdering::Relaxed),
            accelerator_runs: self.accelerator_runs.load(AtomicOrdering::Relaxed),
            cpu_fallbacks: self.cpu_fallbacks.load(AtomicOrdering::Relaxed),
            queue_rejections: self.queue_rejections.load(AtomicOrdering::Relaxed),
            accelerator_failures: self.accelerator_failures.load(AtomicOrdering::Relaxed),
            request_timeouts: self.request_timeouts.load(AtomicOrdering::Relaxed),
            circuit_open_rejections: self.circuit_open_rejections.load(AtomicOrdering::Relaxed),
            micro_batches: self.micro_batches.load(AtomicOrdering::Relaxed),
            micro_batched_requests: self.micro_batched_requests.load(AtomicOrdering::Relaxed),
            inflight_bytes: self.inflight_bytes.load(AtomicOrdering::Relaxed),
            peak_inflight_bytes: self.peak_inflight_bytes.load(AtomicOrdering::Relaxed),
        }
    }
}

struct CircuitState {
    consecutive_failures: u32,
    open_until: Option<Instant>,
}

impl CircuitState {
    const fn new() -> Self {
        Self {
            consecutive_failures: 0,
            open_until: None,
        }
    }

    fn allows(&mut self, now: Instant) -> bool {
        match self.open_until {
            Some(until) if now < until => false,
            Some(_) => {
                self.open_until = None;
                self.consecutive_failures = 0;
                true
            }
            None => true,
        }
    }

    fn success(&mut self) {
        self.consecutive_failures = 0;
        self.open_until = None;
    }

    fn failure(&mut self, now: Instant, threshold: u32, cooldown: Duration) {
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);
        if self.consecutive_failures >= threshold {
            self.open_until = now.checked_add(cooldown);
        }
    }
}

struct ScheduledRequest {
    request: Arc<ComputeRequest>,
    submitted: Instant,
    response: Sender<Result<ComputeResult>>,
    reserved_bytes: usize,
    counters: Arc<SchedulerCounters>,
}

impl Drop for ScheduledRequest {
    fn drop(&mut self) {
        self.counters
            .inflight_bytes
            .fetch_sub(self.reserved_bytes, AtomicOrdering::AcqRel);
    }
}

pub struct ComputeScheduler {
    config: SchedulerConfig,
    cpu: Arc<dyn ComputeBackend>,
    accelerator: Option<Arc<dyn AcceleratorBackend>>,
    sender: Option<Sender<ScheduledRequest>>,
    workers: Vec<JoinHandle<()>>,
    counters: Arc<SchedulerCounters>,
}

impl ComputeScheduler {
    pub fn new(config: SchedulerConfig) -> Result<Self> {
        config.validate()?;
        let cpu: Arc<dyn ComputeBackend> = Arc::new(CpuPool::new(config.cpu_threads)?);
        #[cfg(feature = "gpu")]
        let accelerator: Option<Arc<dyn AcceleratorBackend>> =
            Some(Arc::new(WgpuBackend::new(WgpuConfig {
                vram_budget_bytes: config.vram_budget_bytes,
                timeout: config.request_timeout,
            })?));
        #[cfg(not(feature = "gpu"))]
        let accelerator = None;
        Self::with_backends(config, cpu, accelerator)
    }

    pub fn with_backends(
        config: SchedulerConfig,
        cpu: Arc<dyn ComputeBackend>,
        accelerator: Option<Arc<dyn AcceleratorBackend>>,
    ) -> Result<Self> {
        config.validate()?;
        let (sender, receiver) = bounded(config.queue_depth);
        let counters = Arc::new(SchedulerCounters::default());
        let circuit = Arc::new(Mutex::new(CircuitState::new()));
        let mut workers = Vec::with_capacity(config.accelerator_workers);
        for index in 0..config.accelerator_workers {
            let receiver = receiver.clone();
            let cpu = Arc::clone(&cpu);
            let accelerator = accelerator.as_ref().map(Arc::clone);
            let counters = Arc::clone(&counters);
            let circuit = Arc::clone(&circuit);
            let worker_config = config.clone();
            workers.push(
                thread::Builder::new()
                    .name(format!("aprodb-compute-accelerator-{index}"))
                    .spawn(move || {
                        worker_loop(receiver, cpu, accelerator, counters, circuit, worker_config);
                    })
                    .map_err(|error| {
                        AproError::Compute(format!("thread scheduler accelerator: {error}"))
                    })?,
            );
        }
        Ok(Self {
            config,
            cpu,
            accelerator,
            sender: Some(sender),
            workers,
            counters,
        })
    }

    pub fn execute(&self, request: ComputeRequest) -> Result<ComputeResult> {
        validate_request(&request, &self.config)?;
        self.counters.requests.fetch_add(1, AtomicOrdering::Relaxed);
        if request.preference == ComputePreference::Cpu {
            return run_cpu(
                self.cpu.as_ref(),
                &request,
                ComputeExecution::Cpu,
                None,
                CostEstimate::default(),
                &self.counters,
            );
        }
        if self.accelerator.is_none() {
            return run_cpu(
                self.cpu.as_ref(),
                &request,
                ComputeExecution::CpuFallback,
                Some("CPU-only build or accelerator not configured".into()),
                CostEstimate::default(),
                &self.counters,
            );
        }
        let reserved_bytes = request
            .batch
            .byte_len()
            .saturating_add(request.query.len().saturating_mul(size_of::<f32>()));
        if !reserve_inflight(
            &self.counters,
            reserved_bytes,
            self.config.queue_byte_budget,
        ) {
            self.counters
                .queue_rejections
                .fetch_add(1, AtomicOrdering::Relaxed);
            return run_cpu(
                self.cpu.as_ref(),
                &request,
                ComputeExecution::CpuFallback,
                Some("accelerator byte budget exhausted".into()),
                CostEstimate::default(),
                &self.counters,
            );
        }
        let request = Arc::new(request);
        let (response_sender, response_receiver) = bounded(1);
        let scheduled = ScheduledRequest {
            request: Arc::clone(&request),
            submitted: Instant::now(),
            response: response_sender,
            reserved_bytes,
            counters: Arc::clone(&self.counters),
        };
        let fallback_request = Arc::clone(&scheduled.request);
        match self
            .sender
            .as_ref()
            .ok_or_else(|| AproError::Compute("scheduler stopped".into()))?
            .try_send(scheduled)
        {
            Ok(()) => match response_receiver.recv_timeout(self.config.request_timeout) {
                Ok(result) => result,
                Err(_) => {
                    self.counters
                        .request_timeouts
                        .fetch_add(1, AtomicOrdering::Relaxed);
                    run_cpu(
                        self.cpu.as_ref(),
                        &fallback_request,
                        ComputeExecution::CpuFallback,
                        Some("accelerator scheduler timeout".into()),
                        CostEstimate::default(),
                        &self.counters,
                    )
                }
            },
            Err(TrySendError::Full(scheduled)) => {
                self.counters
                    .queue_rejections
                    .fetch_add(1, AtomicOrdering::Relaxed);
                run_cpu(
                    self.cpu.as_ref(),
                    &scheduled.request,
                    ComputeExecution::CpuFallback,
                    Some("accelerator queue full".into()),
                    CostEstimate::default(),
                    &self.counters,
                )
            }
            Err(TrySendError::Disconnected(_)) => {
                Err(AproError::Compute("accelerator worker unavailable".into()))
            }
        }
    }

    #[must_use]
    pub fn metrics(&self) -> SchedulerMetrics {
        self.counters.snapshot()
    }

    #[must_use]
    pub fn accelerator_stats(&self) -> Option<AcceleratorStats> {
        self.accelerator.as_ref().map(|backend| backend.stats())
    }

    #[must_use]
    pub fn accelerator_name(&self) -> Option<String> {
        self.accelerator.as_ref().map(|backend| backend.name())
    }

    pub fn invalidate_projection(&self, projection_id: &str) {
        if let Some(accelerator) = &self.accelerator {
            accelerator.invalidate_projection(projection_id);
        }
    }
}

impl Drop for ComputeScheduler {
    fn drop(&mut self) {
        self.sender.take();
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

fn worker_loop(
    receiver: Receiver<ScheduledRequest>,
    cpu: Arc<dyn ComputeBackend>,
    accelerator: Option<Arc<dyn AcceleratorBackend>>,
    counters: Arc<SchedulerCounters>,
    circuit: Arc<Mutex<CircuitState>>,
    config: SchedulerConfig,
) {
    let mut pending = VecDeque::new();
    loop {
        let first = match pending.pop_front().or_else(|| receiver.recv().ok()) {
            Some(request) => request,
            None => return,
        };
        let deadline = Instant::now()
            .checked_add(config.micro_batch_max_wait)
            .unwrap_or_else(Instant::now);
        let mut rows = first.request.batch.rows();
        let mut bytes = first.request.batch.byte_len();
        let mut batch = vec![first];
        while rows < config.max_batch_rows && bytes < config.max_batch_bytes {
            let next = match receiver.recv_deadline(deadline) {
                Ok(next) => next,
                Err(_) => break,
            };
            if compatible(&batch[0].request, &next.request)
                && rows.saturating_add(next.request.batch.rows()) <= config.max_batch_rows
                && bytes.saturating_add(next.request.batch.byte_len()) <= config.max_batch_bytes
            {
                rows = rows.saturating_add(next.request.batch.rows());
                bytes = bytes.saturating_add(next.request.batch.byte_len());
                batch.push(next);
            } else {
                pending.push_back(next);
                break;
            }
        }
        counters.micro_batches.fetch_add(1, AtomicOrdering::Relaxed);
        counters
            .micro_batched_requests
            .fetch_add(batch.len() as u64, AtomicOrdering::Relaxed);
        execute_micro_batch(
            &batch,
            cpu.as_ref(),
            accelerator.as_deref(),
            &counters,
            &circuit,
            &config,
        );
    }
}

fn execute_micro_batch(
    requests: &[ScheduledRequest],
    cpu: &dyn ComputeBackend,
    accelerator: Option<&dyn AcceleratorBackend>,
    counters: &SchedulerCounters,
    circuit: &Mutex<CircuitState>,
    config: &SchedulerConfig,
) {
    let batches = requests
        .iter()
        .map(|request| &request.request.batch)
        .collect::<Vec<_>>();
    let combined = match ColumnarF32Batch::concatenate(&batches) {
        Ok(batch) => batch,
        Err(error) => {
            send_same_error(requests, error);
            return;
        }
    };
    let query = &requests[0].request.query;
    let metric = requests[0].request.metric;
    let projection = if requests.len() == 1 {
        requests[0].request.projection.as_ref()
    } else {
        None
    };
    let cache_hit = accelerator
        .zip(projection)
        .is_some_and(|(backend, projection)| backend.is_cached(projection));
    let queue_wait = requests[0].submitted.elapsed();
    let estimate = config.model.estimate(
        &combined,
        query.len().saturating_mul(size_of::<f32>()),
        queue_wait,
        cache_hit,
    );
    let forced = requests
        .iter()
        .any(|request| request.request.preference == ComputePreference::Accelerator);
    let mut fallback_reason = None;
    let use_accelerator = if accelerator.is_none() {
        fallback_reason = Some("accelerator feature or device is unavailable".into());
        false
    } else if !circuit.lock().allows(Instant::now()) {
        counters
            .circuit_open_rejections
            .fetch_add(1, AtomicOrdering::Relaxed);
        fallback_reason = Some("accelerator circuit breaker is cooling down".into());
        false
    } else if !forced && estimate.accelerator_total_micros >= estimate.cpu_compute_micros {
        fallback_reason = Some("total accelerator cost is not less than CPU".into());
        false
    } else {
        true
    };

    if use_accelerator {
        let backend = accelerator.expect("presence verified");
        match backend.score_accelerated(&combined, query, metric, projection) {
            Ok(scores) if scores.len() == combined.rows() => {
                circuit.lock().success();
                counters
                    .accelerator_runs
                    .fetch_add(requests.len() as u64, AtomicOrdering::Relaxed);
                distribute_results(
                    requests,
                    &scores,
                    ComputeExecution::Accelerator,
                    Some(backend.name()),
                    estimate,
                    None,
                );
                return;
            }
            Ok(_) => {
                fallback_reason = Some("accelerator returned incorrect number of rows".into());
            }
            Err(error) => {
                fallback_reason = Some(error.to_string());
            }
        }
        counters
            .accelerator_failures
            .fetch_add(1, AtomicOrdering::Relaxed);
        circuit
            .lock()
            .failure(Instant::now(), config.failure_threshold, config.cooldown);
    }

    match cpu.score_vectors(&combined, query, metric) {
        Ok(scores) => {
            counters
                .cpu_fallbacks
                .fetch_add(requests.len() as u64, AtomicOrdering::Relaxed);
            distribute_results(
                requests,
                &scores,
                ComputeExecution::CpuFallback,
                None,
                estimate,
                fallback_reason,
            );
        }
        Err(error) => send_same_error(requests, error),
    }
}

fn distribute_results(
    requests: &[ScheduledRequest],
    scores: &[Option<f32>],
    execution: ComputeExecution,
    accelerator: Option<String>,
    estimate: CostEstimate,
    fallback_reason: Option<String>,
) {
    let mut offset = 0usize;
    for request in requests {
        let end = offset.saturating_add(request.request.batch.rows());
        let result = if end <= scores.len() {
            Ok(ComputeResult {
                rows: top_k_from_scores(&scores[offset..end], request.request.limit),
                execution,
                accelerator: accelerator.clone(),
                estimate,
                fallback_reason: fallback_reason.clone(),
            })
        } else {
            Err(AproError::Compute(
                "invalid micro-batch result splitting".into(),
            ))
        };
        let _ = request.response.send(result);
        offset = end;
    }
}

fn send_same_error(requests: &[ScheduledRequest], error: AproError) {
    let message = error.to_string();
    for request in requests {
        let _ = request
            .response
            .send(Err(AproError::Compute(message.clone())));
    }
}

fn compatible(left: &ComputeRequest, right: &ComputeRequest) -> bool {
    left.metric == right.metric
        && left.batch.width() == right.batch.width()
        && left.query.len() == right.query.len()
        && left
            .query
            .iter()
            .zip(&right.query)
            .all(|(left, right)| left.to_bits() == right.to_bits())
}

fn validate_request(request: &ComputeRequest, config: &SchedulerConfig) -> Result<()> {
    validate_query(&request.batch, &request.query)?;
    if request.batch.rows() > config.max_batch_rows
        || request.batch.byte_len() > config.max_batch_bytes
    {
        return Err(AproError::ResourceLimit(
            "compute batch exceeds the configured limits".into(),
        ));
    }
    if let Some(projection) = &request.projection {
        projection.validate()?;
    }
    Ok(())
}

fn run_cpu(
    cpu: &dyn ComputeBackend,
    request: &ComputeRequest,
    execution: ComputeExecution,
    fallback_reason: Option<String>,
    estimate: CostEstimate,
    counters: &SchedulerCounters,
) -> Result<ComputeResult> {
    let scores = cpu.score_vectors(&request.batch, &request.query, request.metric)?;
    match execution {
        ComputeExecution::Cpu => {
            counters.cpu_runs.fetch_add(1, AtomicOrdering::Relaxed);
        }
        ComputeExecution::CpuFallback => {
            counters.cpu_fallbacks.fetch_add(1, AtomicOrdering::Relaxed);
        }
        ComputeExecution::Accelerator => {}
    }
    Ok(ComputeResult {
        rows: top_k_from_scores(&scores, request.limit),
        execution,
        accelerator: None,
        estimate,
        fallback_reason,
    })
}

fn reserve_inflight(counters: &SchedulerCounters, bytes: usize, budget: usize) -> bool {
    let mut current = counters.inflight_bytes.load(AtomicOrdering::Acquire);
    loop {
        let Some(updated) = current.checked_add(bytes) else {
            return false;
        };
        if updated > budget {
            return false;
        }
        match counters.inflight_bytes.compare_exchange_weak(
            current,
            updated,
            AtomicOrdering::AcqRel,
            AtomicOrdering::Acquire,
        ) {
            Ok(_) => {
                counters
                    .peak_inflight_bytes
                    .fetch_max(updated, AtomicOrdering::Relaxed);
                return true;
            }
            Err(observed) => current = observed,
        }
    }
}

pub fn exact_top_k(
    backend: &dyn ComputeBackend,
    batch: &ColumnarF32Batch,
    query: &[f32],
    metric: VectorMetric,
    limit: usize,
) -> Result<Vec<ScoredRow>> {
    Ok(top_k_from_scores(
        &backend.score_vectors(batch, query, metric)?,
        limit,
    ))
}

fn top_k_from_scores(scores: &[Option<f32>], limit: usize) -> Vec<ScoredRow> {
    let mut rows = scores
        .iter()
        .copied()
        .enumerate()
        .filter_map(|(row, score)| score.map(|score| ScoredRow { row, score }))
        .collect::<Vec<_>>();
    rows.par_sort_unstable_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.row.cmp(&right.row))
    });
    rows.truncate(limit);
    rows
}

pub fn scores_equivalent(left: &[ScoredRow], right: &[ScoredRow], tolerance: f32) -> bool {
    tolerance.is_finite()
        && tolerance >= 0.0
        && left.len() == right.len()
        && left.iter().zip(right).all(|(left, right)| {
            left.row == right.row
                && (left.score - right.score).abs()
                    <= tolerance * left.score.abs().max(right.score.abs()).max(1.0)
        })
}

fn validate_query(batch: &ColumnarF32Batch, query: &[f32]) -> Result<()> {
    if query.len() != batch.width() || query.iter().any(|value| !value.is_finite()) {
        return Err(AproError::InvalidInput(
            "query contains non-finite values or has an incorrect size".into(),
        ));
    }
    Ok(())
}

fn score(vector: &[f32], query: &[f32], metric: VectorMetric) -> Result<f32> {
    let (dot, vector_norm, query_norm) = vector.iter().zip(query).fold(
        (0.0f64, 0.0f64, 0.0f64),
        |(dot, vector_norm, query_norm), (left, right)| {
            let left = f64::from(*left);
            let right = f64::from(*right);
            (
                dot + left * right,
                vector_norm + left * left,
                query_norm + right * right,
            )
        },
    );
    let score = match metric {
        VectorMetric::Dot => dot,
        VectorMetric::Cosine => {
            let denominator = (vector_norm * query_norm).sqrt();
            if denominator.total_cmp(&0.0) == Ordering::Greater {
                dot / denominator
            } else {
                0.0
            }
        }
    };
    if !score.is_finite() || score.abs() > f64::from(f32::MAX) {
        return Err(AproError::InvalidInput(
            "vector score cannot be represented as f32".into(),
        ));
    }
    Ok(score as f32)
}

const fn nanos_to_micros(nanos: u64) -> u64 {
    nanos.saturating_add(999) / 1000
}

fn duration_micros(duration: Duration) -> u64 {
    u64::try_from(duration.as_micros()).unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Barrier, atomic::AtomicUsize, mpsc};

    use super::*;

    #[test]
    fn columnar_layout_preserves_validity_and_cpu_ranking() {
        let batch = ColumnarF32Batch::from_rows(
            &[
                Some(vec![1.0, 0.0]),
                None,
                Some(vec![0.7, 0.7]),
                Some(vec![0.0, 1.0]),
            ],
            2,
        )
        .unwrap();
        assert_eq!(batch.values(), &[1.0, 0.0, 0.0, 0.0, 0.7, 0.7, 0.0, 1.0]);
        assert_eq!(batch.layout().value_alignment_bytes, 4);
        assert_eq!(batch.layout().validity_word_bits, 32);
        assert!(!batch.is_valid(1));
        let top = exact_top_k(&CpuReference, &batch, &[1.0, 0.0], VectorMetric::Cosine, 3).unwrap();
        assert_eq!(
            top.iter().map(|row| row.row).collect::<Vec<_>>(),
            vec![0, 2, 3]
        );
    }

    #[test]
    fn cpu_semantics_reject_non_finite_and_order_ties_by_row() {
        assert!(ColumnarF32Batch::from_rows(&[Some(vec![f32::NAN])], 1).is_err());
        let batch =
            ColumnarF32Batch::from_rows(&[Some(vec![1.0, 0.0]), Some(vec![1.0, 0.0])], 2).unwrap();
        let top = exact_top_k(&CpuReference, &batch, &[1.0, 0.0], VectorMetric::Dot, 2).unwrap();
        assert_eq!(top[0].row, 0);
        assert_eq!(top[1].row, 1);
    }

    struct MockAccelerator {
        calls: AtomicUsize,
        failures: AtomicUsize,
    }

    struct OomAccelerator;

    impl AcceleratorBackend for OomAccelerator {
        fn name(&self) -> String {
            "mock-oom".into()
        }

        fn is_cached(&self, _projection: &ProjectionDescriptor) -> bool {
            false
        }

        fn score_accelerated(
            &self,
            _batch: &ColumnarF32Batch,
            _query: &[f32],
            _metric: VectorMetric,
            _projection: Option<&ProjectionDescriptor>,
        ) -> Result<Vec<Option<f32>>> {
            Err(AproError::ResourceLimit(
                "Injected GPU out-of-memory error".into(),
            ))
        }

        fn invalidate_projection(&self, _projection_id: &str) {}

        fn stats(&self) -> AcceleratorStats {
            AcceleratorStats::default()
        }
    }

    struct BlockingAccelerator {
        entered: Arc<Barrier>,
        release: Arc<Barrier>,
    }

    impl AcceleratorBackend for BlockingAccelerator {
        fn name(&self) -> String {
            "mock-blocking".into()
        }

        fn is_cached(&self, _projection: &ProjectionDescriptor) -> bool {
            false
        }

        fn score_accelerated(
            &self,
            batch: &ColumnarF32Batch,
            query: &[f32],
            metric: VectorMetric,
            _projection: Option<&ProjectionDescriptor>,
        ) -> Result<Vec<Option<f32>>> {
            self.entered.wait();
            self.release.wait();
            CpuReference.score_vectors(batch, query, metric)
        }

        fn invalidate_projection(&self, _projection_id: &str) {}

        fn stats(&self) -> AcceleratorStats {
            AcceleratorStats::default()
        }
    }

    impl AcceleratorBackend for MockAccelerator {
        fn name(&self) -> String {
            "mock-accelerator".into()
        }

        fn is_cached(&self, _projection: &ProjectionDescriptor) -> bool {
            false
        }

        fn score_accelerated(
            &self,
            batch: &ColumnarF32Batch,
            query: &[f32],
            metric: VectorMetric,
            _projection: Option<&ProjectionDescriptor>,
        ) -> Result<Vec<Option<f32>>> {
            self.calls.fetch_add(1, AtomicOrdering::SeqCst);
            if self
                .failures
                .fetch_update(
                    AtomicOrdering::SeqCst,
                    AtomicOrdering::SeqCst,
                    |remaining| remaining.checked_sub(1),
                )
                .is_ok()
            {
                return Err(AproError::Compute("Injected accelerator failure".into()));
            }
            CpuReference.score_vectors(batch, query, metric)
        }

        fn invalidate_projection(&self, _projection_id: &str) {}

        fn stats(&self) -> AcceleratorStats {
            AcceleratorStats::default()
        }
    }

    fn accelerator_model() -> CostModel {
        CostModel {
            cpu_nanos_per_element: 1_000,
            accelerator_nanos_per_element: 1,
            transfer_nanos_per_byte: 0,
            launch_micros: 0,
            synchronization_micros: 0,
            risk_margin_micros: 0,
        }
    }

    fn request(preference: ComputePreference) -> ComputeRequest {
        ComputeRequest {
            batch: ColumnarF32Batch::from_rows(&[Some(vec![1.0, 0.0]), Some(vec![0.0, 1.0])], 2)
                .unwrap(),
            query: vec![1.0, 0.0],
            metric: VectorMetric::Dot,
            limit: 2,
            preference,
            projection: None,
        }
    }

    #[test]
    fn scheduler_uses_cost_model_and_falls_back_after_fault() {
        let accelerator = Arc::new(MockAccelerator {
            calls: AtomicUsize::new(0),
            failures: AtomicUsize::new(1),
        });
        let config = SchedulerConfig {
            micro_batch_max_wait: Duration::ZERO,
            failure_threshold: 1,
            model: accelerator_model(),
            ..SchedulerConfig::default()
        };
        let scheduler = ComputeScheduler::with_backends(
            config,
            Arc::new(CpuReference),
            Some(accelerator.clone()),
        )
        .unwrap();
        let first = scheduler
            .execute(request(ComputePreference::Accelerator))
            .unwrap();
        assert_eq!(first.execution, ComputeExecution::CpuFallback);
        let second = scheduler
            .execute(request(ComputePreference::Accelerator))
            .unwrap();
        assert_eq!(second.execution, ComputeExecution::CpuFallback);
        assert_eq!(accelerator.calls.load(AtomicOrdering::SeqCst), 1);
        assert!(second.fallback_reason.unwrap().contains("cooling down"));
    }

    #[test]
    fn circuit_cooldown_is_tested_with_explicit_instants() {
        let now = Instant::now();
        let mut circuit = CircuitState::new();
        circuit.failure(now, 1, Duration::from_secs(5));
        assert!(!circuit.allows(now + Duration::from_secs(4)));
        assert!(circuit.allows(now + Duration::from_secs(5)));
    }

    #[test]
    fn accelerator_queue_byte_budget_rejects_before_admission() {
        let accelerator = Arc::new(MockAccelerator {
            calls: AtomicUsize::new(0),
            failures: AtomicUsize::new(0),
        });
        let scheduler = ComputeScheduler::with_backends(
            SchedulerConfig {
                queue_byte_budget: 1,
                model: accelerator_model(),
                ..SchedulerConfig::default()
            },
            Arc::new(CpuReference),
            Some(accelerator.clone()),
        )
        .unwrap();
        let result = scheduler
            .execute(request(ComputePreference::Accelerator))
            .unwrap();
        assert_eq!(result.execution, ComputeExecution::CpuFallback);
        assert!(
            result
                .fallback_reason
                .unwrap()
                .contains("byte budget exhausted")
        );
        assert_eq!(accelerator.calls.load(AtomicOrdering::SeqCst), 0);
        let metrics = scheduler.metrics();
        assert_eq!(metrics.queue_rejections, 1);
        assert_eq!(metrics.inflight_bytes, 0);
        assert_eq!(metrics.peak_inflight_bytes, 0);
    }

    #[test]
    fn accelerator_oom_is_isolated_and_retried_on_cpu() {
        let scheduler = ComputeScheduler::with_backends(
            SchedulerConfig {
                micro_batch_max_wait: Duration::ZERO,
                failure_threshold: 1,
                model: accelerator_model(),
                ..SchedulerConfig::default()
            },
            Arc::new(CpuReference),
            Some(Arc::new(OomAccelerator)),
        )
        .unwrap();
        let result = scheduler
            .execute(request(ComputePreference::Accelerator))
            .unwrap();
        assert_eq!(result.execution, ComputeExecution::CpuFallback);
        assert!(result.fallback_reason.unwrap().contains("out-of-memory"));
        assert_eq!(scheduler.metrics().accelerator_failures, 1);
    }

    #[test]
    fn request_timeout_returns_cpu_without_waiting_for_accelerator() {
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        let scheduler = Arc::new(
            ComputeScheduler::with_backends(
                SchedulerConfig {
                    micro_batch_max_wait: Duration::ZERO,
                    request_timeout: Duration::from_millis(20),
                    model: accelerator_model(),
                    ..SchedulerConfig::default()
                },
                Arc::new(CpuReference),
                Some(Arc::new(BlockingAccelerator {
                    entered: Arc::clone(&entered),
                    release: Arc::clone(&release),
                })),
            )
            .unwrap(),
        );
        let caller_scheduler = Arc::clone(&scheduler);
        let (sender, receiver) = mpsc::sync_channel(1);
        let caller = thread::spawn(move || {
            let result = caller_scheduler.execute(request(ComputePreference::Accelerator));
            sender.send(result).unwrap();
        });
        entered.wait();
        let result = receiver
            .recv_timeout(Duration::from_secs(1))
            .unwrap()
            .unwrap();
        assert_eq!(result.execution, ComputeExecution::CpuFallback);
        assert!(result.fallback_reason.unwrap().contains("timeout"));
        assert_eq!(scheduler.metrics().request_timeouts, 1);
        release.wait();
        caller.join().unwrap();
    }

    #[test]
    fn concurrent_compatible_requests_are_micro_batched() {
        let callers = 4;
        let accelerator = Arc::new(MockAccelerator {
            calls: AtomicUsize::new(0),
            failures: AtomicUsize::new(0),
        });
        let scheduler = Arc::new(
            ComputeScheduler::with_backends(
                SchedulerConfig {
                    micro_batch_max_wait: Duration::from_millis(50),
                    model: accelerator_model(),
                    ..SchedulerConfig::default()
                },
                Arc::new(CpuReference),
                Some(accelerator.clone()),
            )
            .unwrap(),
        );
        let start = Arc::new(Barrier::new(callers + 1));
        let workers = (0..callers)
            .map(|_| {
                let scheduler = Arc::clone(&scheduler);
                let start = Arc::clone(&start);
                thread::spawn(move || {
                    start.wait();
                    scheduler
                        .execute(request(ComputePreference::Accelerator))
                        .unwrap()
                })
            })
            .collect::<Vec<_>>();
        start.wait();
        for worker in workers {
            assert_eq!(
                worker.join().unwrap().execution,
                ComputeExecution::Accelerator
            );
        }
        assert!(accelerator.calls.load(AtomicOrdering::SeqCst) < callers);
        assert_eq!(scheduler.metrics().micro_batched_requests, callers as u64);
    }
}
// SPDX-FileCopyrightText: 2026 Andrea Provenzali
// SPDX-License-Identifier: AGPL-3.0-only
