# ADR-0003 — Heterogeneous compute with CPU reference and wgpu

- Status: accepted for Milestone 6, experimental hardware calibration
- Date: August 19, 2026
- Scope: exact/top-k vector single-node

## Context

The correct path must work without a GPU. The accelerator is derivative, volatile, and fallible; a decision based on batch size alone does not account for transfers, queue, initialization, readback, or risk. The VRAM content cannot serve as the authoritative source nor be reused after a mutation.

## Decision

- `CpuPool` implements reference semantics for dot product and cosine on contiguous columnar batches with validity bitmap. Incomplete inputs are rejected, the null vector has cosine zero, and ties are resolved by row index.
- The scheduler uses a limited queue and byte budget, micro-batching with a maximum wait, a separate CPU pool, timeout, circuit breaker, and CPU fallback. `Auto` compares CPU cost and the sum of transfer-in, queue wait, launch, GPU compute, transfer-out, synchronization, and risk margin.
- The `gpu` feature uses wgpu/WGSL. Readback uses `map_async` and a timeout; an error resets the derivative context, trips the circuit breaker according to a threshold, and does not modify storage or state.
- The VRAM cache is LRU and limited. The key contains projection id, global source generation, and schema version. VectorExact builds the batch under a short barrier across all shards, captures the generation, and releases locks before computation; a subsequent mutation forces a new key.
- `VectorExact` is read-only. There is not yet a GPU-derived publication; when added, it will need to recheck version and watermark before commit.

## Evidence

Deterministic CPU tests cover layout, nulls, ranking, ties, limits, queue budget, micro-batch, timeout/fault, and cooldown. The wgpu test compares top-20 results with CPU within a relative tolerance of `1e-4`, verifies cache hit, invalidation, and VRAM cache rebuild. The TCP test covers vector Put, VectorExact CPU, metrics, and role separation. Golden wire protects request and response.

The benchmark suite in [`benchmarks/compute`](../../benchmarks/compute/RESULTS.md) measures CPU, cold GPU, and hot GPU performance, including transfers and top-k. On the local system, the crossover is not monotonic: the model remains configurable and the data does not constitute an SLA.

## Consequences and limitations

- ExactFlat scans up to `max_scan_records`; it is not an ANN index and briefly blocks mutations while capturing the generation.
- wgpu does not expose portable pinned host memory: this implementation does not maintain an application-level pinned pool and uses internal staging buffers limited by the batch/queue budget. The default backend uses a GPU worker; larger configurations remain limited, but access to the device is serialized.
- The initial estimate is configurable but does not yet self-calibrate. The operator can force CPU or accelerator; the accelerator request also falls back to CPU if the device fails, because the operation is read-only and semantically safe.
- ANN indexes, GPU filters/aggregations, CUDA/HIP, VRAM persistence, or acceleration guarantees are not implemented. Storage, recovery, and protocol remain complete in the `--no-default-features` binary.