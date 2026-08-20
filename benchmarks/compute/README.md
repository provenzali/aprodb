# Milestone 6 compute benchmark

The lab measures the vector exact/top-k operator on both CPU and wgpu using the same columnar batches and verifies the ranking within a relative tolerance of `1e-4`.

```powershell
cargo run --release -p aprodb-compute --features gpu --example compute_crossover
```

For each dimension, it runs nine CPU samples, one cold GPU request, and nine requests with the projection already present in VRAM. GPU end-to-end latency includes upload, dispatch, synchronization, readback, and top-k; the counters also separate transfer time and kernel execution time. The data are deterministic. Local results do not constitute an SLA, and the crossover point must be recalibrated for the target hardware.

The verified results are in [RESULTS.md](RESULTS.md).
