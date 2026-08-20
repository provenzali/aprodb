# Local results — 19 August 2026

Command: `cargo run --release -p aprodb-compute --features gpu --example compute_crossover`.
Hardware: Intel Core i5-1340P and Intel Iris Xe; Windows 11 10.0.26200; Rust 1.97.1.
Nine samples for the hot path.
All top-20 GPU rankings matched the CPU within relative tolerance `1e-4`.

|Rows × width|Batch bytes|CPU p50/p95/p99 μs|Cold GPU μs|Hot GPU p50/p95/p99 μs|CPU rows/s|GPU rows/s|
|---:|---:|---:|---:|---:|---:|---:|
|1,024 × 64|262,272|824 / 1,089 / 1,089|590,567|1,152 / 3,551 / 3,551|1,242,718|888,889|
|8,192 × 64|2,098,176|2,215 / 2,844 / 2,844|2,604|1,960 / 2,584 / 2,584|3,698,420|4,179,592|
|65,536 × 64|16,785,408|12,621 / 13,669 / 13,669|16,158|11,209 / 14,159 / 14,159|5,192,615|5,846,730|
|65,536 × 256|67,117,056|16,149 / 21,243 / 21,243|154,014|17,685 / 20,602 / 20,602|4,058,208|3,705,739|

Across the ten dispatches per size (one cold and nine hot), the wgpu counters recorded transfer/kernel
times of 2,184/29,859 μs, 1,436/11,240 μs, 5,400/32,307 μs, and 17,982/209,618 μs, respectively,
with nine VRAM hits per size.
The end-to-end latency reported in the table also includes synchronization, readback, and top-k.

The local crossover is not monotonic: the hot GPU outperforms the CPU at the two intermediate sizes,
but not for the small batch or the 65,536 × 256 batch.
The cost model must therefore be calibrated for the adapter and batch shape; the cold run confirms
that choosing GPU based solely on the number of elements is incorrect.
These results are diagnostic and not intended as an SLA or product comparison.
