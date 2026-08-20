# Compression benchmark for Milestone 5

This lab measures the actual embedded execution path of AProDB, not a network server. It compares four configurations for the same keyspace and durability: AProDB logical Zstandard, Fjall physical LZ4, both, and neither.

## Execution

```powershell
cargo run --release -p aprodb-engine --example compression_benchmark
```

Each variant writes 256 records of 4 KiB each, in 16 durable atomic batches, then performs sync, compaction, verification, reopening, and a read. The workload is repeated with compressible and deterministically pseudorandom payloads.

The p50/p95/p99 latencies are per batch; throughput is measured in records/s. Process I/O counters on Windows include all process I/O; memory usage is reported as the resident set size at the end of the measured interval, not the peak. Fjall files start with a 64 MiB preallocation: at this scale, `disk_bytes_before_compaction` is not useful for comparing payloads, while logical/encoded bytes and process I/O remain comparable. A single local run does not justify competitive claims.

The verified results are in [RESULTS.md](RESULTS.md). The decision is in [ADR-0002](../../docs/adr/0002-logical-compression.md).