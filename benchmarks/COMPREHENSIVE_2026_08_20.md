# Comprehensive validation and stress report — 2026-08-20

AProDB remains a public beta. The measurements below are reproducible validation evidence, not an SLA or a claim of universal superiority.

## Test matrix

| Area | Workload or invariant | Result | Evidence |
| --- | --- | --- | --- |
| Build | format, CPU clippy, CPU tests | PASS | local CI and workspace gates |
| Functional storage | put/get/delete, batch, prefix scan, reopen | PASS | `tests/database.rs` |
| Concurrency | eight writer threads and server in-flight limits | PASS | database and server integration tests |
| Durability | durable/relaxed WAL semantics and recovery | PASS | engine/storage tests |
| Fault handling | incomplete WAL tail repair and reopen | PASS | database recovery tests |
| Backup/restore | encrypted restore and repeated rekey cycles | PASS | long operability gate, 72.4 s |
| Compression | raw/Zstandard/backend compression paths | PASS | compression laboratory |
| Migration | 0.1 import validation and rejection limits | PASS | migration tests |
| Compute | CPU exact vector search and ranking equivalence | PASS | compute tests |
| GPU compute | wgpu CPU/GPU equivalence and crossover | PASS on Tesla T4 | Vast report |
| Comparative DB | AProDB, SQLite, PostgreSQL, MariaDB, Redis | PASS | `comparative/VAST_2026_08_20.md` |
| MySQL | standalone MySQL server | NOT RUN | two disposable Vast provisioning attempts were unavailable; no local server was installed |
| Live Supabase | sanitized production-shaped import | NOT RUN | endpoint unavailable; no data guessed |
| Stress: 100k | random, 1 KiB values | PASS | 108.5 MB physical |
| Stress: 500k | random, 1 KiB values | PASS | 542.5 MB physical |
| Stress: 1M | random, 1 KiB values | PASS | 1.085 GB physical |
| Stress: 2M | random, 2 KiB values | PASS | 4.218 GB physical; ingest 20.1k/s, read 25.7k/s, p99 390.9 µs |

The stress sequence reached two million records and 4.218 GB of physical data without a crash, corruption, timeout, or recovery failure. It did not exhaust the host memory or disk; therefore this is a tested operating point, not the maximum capacity. The first intentional failure boundary remains the configured resource limits (record/frame size, scan/change-stream limits, vector batch limits, and server queue limits), which return structured resource-limit errors instead of attempting unbounded allocation.

The `--all-targets --include-ignored` Windows command also exposed a toolchain-only failure when a GPU-disabled throughput benchmark was run without the GPU feature. It is not a database failure; the ordinary CPU workspace gate passes.

## AProDB: advantages

- Very fast point reads on the tested in-process workload, with deterministic bounded batches and queues.
- Strong compression on repetitive payloads; the 50k cloud workload used 7.08 MB versus 31–46 MB for the SQL alternatives.
- Single-node recovery, WAL tail repair, checkpoints, backup/restore and explicit durability modes are tested.
- One implementation covers embedded use and the central server/client path.
- Exact CPU reference compute plus optional GPU fallback, with equivalence checks.
- Resource limits and protocol frame limits are explicit and observable.

## AProDB: disadvantages and open risks

- Still beta and not a replacement for a mature SQL ecosystem: no general SQL, joins, replication, or distributed consensus.
- The active working set is memory-oriented; very large datasets and eviction behavior need more long-running, out-of-RAM testing.
- The comparative benchmark is mostly one process/one connection per backend; network-server and multi-client fairness need a separate YCSB-style campaign.
- MySQL, live Supabase-shaped data, and larger-than-RAM cloud runs are still missing.
- GPU cold-start and transfer costs can dominate; GPU is not automatically faster.
- The storage backend and physical compaction behavior remain important operational dependencies.

## Next benchmark tranche

Provision a disposable Linux host and add standalone MySQL, YCSB-like read/update/scan mixes, 16/64/256 concurrent clients, crash injection during durable commits, and a dataset exceeding RAM. Keep each backend isolated, record p50/p95/p99, CPU/RAM/I/O/space, and destroy the host after artifact retrieval.

The final MySQL attempt on 2026-08-20 was terminated while the selected Vast offer remained in `loading`; no chargeable instance was left running. This is an infrastructure gap, not a performance result.
