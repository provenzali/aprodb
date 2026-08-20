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
| MySQL | standalone MySQL server | PASS | MySQL 8.0.46 completed the common ingest/read/scan workload; update-heavy YCSB is still separate |
| Live Supabase | sanitized production-shaped import | NOT RUN | endpoint unavailable; no data guessed |
| Stress: 100k | random, 1 KiB values | PASS | 108.5 MB physical |
| Stress: 500k | random, 1 KiB values | PASS | 542.5 MB physical |
| Stress: 1M | random, 1 KiB values | PASS | 1.085 GB physical |
| Stress: 2M | random, 2 KiB values | PASS | 4.218 GB physical; ingest 20.1k/s, read 25.7k/s, p99 390.9 µs |
| MySQL 8.0.46 | random, 50k × 512 B | PASS | 33.6k ingest/s, 30.8k read/s, p99 50.6 µs, 38.42 MB |
| Redis 6.0.16 | random, 50k × 512 B | PASS | 243k ingest/s, 29.9k read/s, p99 43.9 µs, 40.43 MB |
| AProDB updates | random, 50k + 10k updates | PASS | 102–105k updates/s, 1.76–1.78M reads/s, p99 0.9 µs |
| MySQL updates | random, 50k + 10k updates | PASS | 25.6–29.8k updates/s, 30.2–33.2k reads/s, p99 49–54 µs |
| Redis updates | random, 50k + 10k updates | PASS | 384–481k updates/s, 26.0–28.2k reads/s, p99 49–54 µs |

The stress sequence reached two million records and 4.218 GB of physical data without a crash, corruption, timeout, or recovery failure. It did not exhaust the host memory or disk; therefore this is a tested operating point, not the maximum capacity. The first intentional failure boundary remains the configured resource limits (record/frame size, scan/change-stream limits, vector batch limits, and server queue limits), which return structured resource-limit errors instead of attempting unbounded allocation.

The `--all-targets --include-ignored` Windows command also exposed a toolchain-only failure when a GPU-disabled throughput benchmark was run without the GPU feature. It is not a database failure; the ordinary CPU workspace gate passes.

## MySQL and Redis follow-up

On one disposable Linux host (48 GB RAM, 40 GB workspace disk), MySQL Community 8.0.46 and Redis 6.0.16 were compared with AProDB using the same 50,000-record ingest/read/ordered-scan workload. Median MySQL results were 37.9k/31.5k ops/s (compressible) and 33.9k/30.8k ops/s (random), with 46.8–50.6 µs read p99 and 38.42 MB table-plus-index space. Redis results were 294k/28.5k ops/s (compressible) and 305k/28.7k ops/s (random), with roughly 45–49 µs p99 and 40.43 MB used dataset space. AProDB remained at approximately 100–110k ingest/s and 0.9–2.1 µs read p99 on the same host.

The benchmark process itself peaked at approximately 75 MB RSS; daemon memory for MySQL and Redis is not included in that process-level figure. The 2M-record AProDB run is the largest completed stress point. A true out-of-RAM run requires a larger disposable host and is intentionally not forced on a 48 GB/40 GB machine.

The common comparative workload is an ingest/read/ordered-scan mix, not the full YCSB update-heavy suite. No YCSB harness was added to the product because that would be benchmark tooling rather than a storage feature; it remains a clearly labelled follow-up.

The bounded update phase is now implemented in the comparative harness (`--updates`), using upsert semantics for AProDB, SQLite, PostgreSQL and MySQL/MariaDB and SET semantics for Redis. A 10,000-update run completed on all three compared backends above. A true out-of-RAM run was not forced: available Vast offers did not provide the required RAM/disk combination reliably, and the disposable 48 GB/30 GB host was deliberately kept below OOM.

## Write-path ablation laboratory

A temporary uncommitted laboratory isolated the main write-path costs on 50,000 durable 512-byte records. With a batch size of 500, switching from durable to relaxed mode improved throughput from roughly 49–50k to 54–60k records/s. Disabling logical compression changed throughput only slightly: compression was neutral to mildly beneficial for repetitive data because it reduced bytes written, while random data paid a small CPU cost. Batch size was the strongest factor: durable compressed throughput rose from about 24–27k records/s at 50-record batches to 49–59k at 500 and 58–59k at 5,000. The largest isolated bottlenecks are therefore per-batch durability/flush overhead and too-small batches, not hashing or compression itself.

## Hash-token laboratory

A temporary, uncommitted BLAKE3 laboratory tested 50,000 durable 512-byte records. With unique payloads, hashing plus content-addressed writes took 948.6 ms versus 839.7 ms for direct writes (about 13% slower). With 100 repeated payloads, deduplication reduced the write phase to 75.3 ms versus 844.2 ms (about 11.2x faster) because only 100 payloads were persisted. This validates a conditional design: content tokens help when duplicate payloads are common, but add overhead for unique data. GPU hashing was not added; for individual records transfer overhead would likely dominate, and a GPU path should be evaluated only for large batches.

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

Add a dedicated YCSB-like read/update/scan harness, 16/64/256 concurrent clients, crash injection during durable commits, and a dataset exceeding RAM. Keep each backend isolated, record p50/p95/p99, CPU/RAM/I/O/space, and destroy the host after artifact retrieval.

The earlier MySQL provisioning attempts on 2026-08-20 were terminated while offers remained in `loading`; the later MySQL 8.0.46 host completed the common workload successfully. No chargeable instance was left running after the campaign.
