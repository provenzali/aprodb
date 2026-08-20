# Fjall 3.1.8 storage spike

This laboratory covers the quantitative criteria of Milestone 0.5. It is not a competitive benchmark and does not measure the client/server protocol.

## Execution

```powershell
cargo run --release -p aprodb-storage --example fjall_spike
```

The program creates independent temporary directories for eight workloads: four compression policies
(`aprodb_zstd_only`, `backend_lz4_only`, `both`, `none`) on compressible and pseudo-random payloads.
Each workload writes 2,000 records, executes two versions per record in batches of 100 and uses
`SyncAll` for each batch.

Each mutation writes an immutable version, a head, a minimal event with reference to the version, and the catalog watermark. At the end, it forces flush and major compaction via the Fjall API, reopens the database, and verifies a sample of exact versions.

## Metrics and limits

- Durable latencies are p50/p95/p99 of the 40 batches; throughput counts mutations, not individual physical operations.
- `process_io_written_bytes` comes from process counters. On Windows, this includes all process I/O: it is a comparative proxy of amplification, not a physical byte counter natively attributed by Fjall.
- `submitted_storage_bytes` is the sum of keys and values delivered to the backend; the I/O ratio uses this value as the denominator.
- The `minimal_event_bytes` cost is a synthetic lower bound. Full AProDB logical frames also include identity, sequence, batch ID, and checksum.
- A single local run is insufficient for regressions or claims of superiority. The Milestone 5 benchmark adds repetitions, warm-up, and tier policy.

The verified results are in [RESULTS.md](RESULTS.md). The decision is recorded in [ADR-0001](../../docs/adr/0001-fjall-backend.md).
