# AProDB software manual

## 1. Overview

AProDB is an embedded key-value database written in Rust.
It keeps the working set in memory, logs changes to disk, and offers parallel vector search.
It is designed as a high-performance experimental foundation with a typed API—not yet as a production-ready distributed service.

Main features include:

- UTF-8 keys and typed values;
- concurrent access through sharding;
- `put`/`get`/`delete`, batch lookup, and prefix scan;
- write-ahead log with CRC32 and truncated tail recovery;
- consistent snapshots;
- adaptive Zstandard compression in RAM, WAL, and snapshot;
- dot product and cosine similarity on `f32` vectors;
- CPU parallelism via Rayon;
- GPU compute shader via `wgpu`, with automatic CPU fallback;
- Rust library and command-line interface.

## 2. Status and limitations of version 0.1

This version is a single-process, single-node MVP.
It does not implement SQL, multi-key transactions, replication, authentication, network protocol, or distributed consensus.
The active dataset must fit in RAM.
The WAL is not yet automatically compacted and can grow; snapshots reduce startup time and clean tombstones in memory, but retain the WAL as a complete recovery history.

## 3. Requirements

- Rust stable with Cargo;
- a C-compatible compiler, used by the reference Zstandard library;
- a platform supported by `wgpu` for the GPU feature;
- updated graphics drivers to enable acceleration;
- no mandatory GPU: `--no-default-features` produces a CPU-only binary.

On this Windows workstation, the project uses the target `x86_64-pc-windows-gnu`; Rustup and WinLibs UCRT are already installed. A new shell automatically inherits both from the user's `PATH`.

## 4. Compilation

Complete build:

```powershell
cargo build --release
```

CPU-only build:

```powershell
cargo build --release --no-default-features
```

Tests:

```powershell
cargo test --all-features
```

Recommended static checks:

```powershell
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
```

## 5. Data model

Each element is identified by a non-empty UTF-8 key.
`Value` supports:

| Variant | Rust type | Typical use |
|---|---|---|
| `Bytes` | `Vec<u8>` | binary payloads |
| `Text` | `String` | UTF-8 text |
| `Integer` | `i64` | counters and identifiers |
| `Float` | `f64` | scalar numeric values |
| `Vector` | `Vec<f32>` | embeddings and numeric features |

Float and vector components must be finite; `NaN` and infinite values are rejected.

## 6. Use as a Rust library

Minimal example:

```rust
use aprodb::{Config, Database, Value};

let db = Database::open(Config::new("./data"))?;
db.put("user:42", Value::Text("Ada".into()))?;
assert_eq!(db.get("user:42")?, Some(Value::Text("Ada".into())));
db.sync()?;
# Ok::<(), aprodb::AproError>(())
```

Batch writes and reads:

```rust
let writes = vec![
    ("a".to_string(), Value::Integer(1)),
    ("b".to_string(), Value::Integer(2)),
];
db.put_batch(writes)?;
let values = db.get_batch(&["a".into(), "b".into()])?;
```

Vector search:

```rust
use aprodb::{ComputeBackend, Metric};

db.put("doc:1", Value::Vector(vec![1.0, 0.0, 0.0]))?;
db.put("doc:2", Value::Vector(vec![0.0, 1.0, 0.0]))?;
let result = db.vector_search(
    &[0.9, 0.1, 0.0],
    10,
    Metric::Cosine,
    ComputeBackend::Auto,
)?;
```

`SearchResult.backend` indicates whether the operation used the CPU or GPU; `accelerator` contains the adapter name when available.

## 7. Configuration

`Config::new(path)` selects safe defaults. The modifiable fields are:

- `path`: directory containing WAL and snapshot;
- `shards`: power of two; more shards reduce contention but increase overhead;
- `durability`: `SyncData` synchronizes every single operation or batch, `Relaxed` favors
  throughput and latency;
- `gpu_min_work`: minimum component count (`vectors × dimensions`) before `Auto` tries the GPU;
  default 16,777,216.
- `compression_level`: Zstandard level, default `1` to prioritize speed;
- `compression_min_size`: minimum threshold for trying Zstd, default 32 bytes;
- `compression_channels`: independent compression/decompression settings, power of two, default
  limited to 32 and maximum 64.

`SyncData` is the default. For bulk ingestion, using `put_batch` is much more efficient than many `put` calls, because it performs a single WAL synchronization per batch.

## 8. Integrated compression

Each value passes through a `CompressionChannel` on input and output. The internal format contains version, codec, logical type, and original length. Above `compression_min_size`, AProDB tries Zstandard; however, it only keeps the compressed result if it saves at least eight bytes. Small, incompressible, or already compressed payloads remain raw and do not undergo artificial expansion.

The default level is Zstd 1, chosen to limit latency. Higher levels can further reduce space but consume more CPU. There is no ideal compressor for every data distribution: adaptivity is part of the project, not an exception.

Channels are independent context pools, selected by key hash. A single context would serialize all operations; one context for each of many shards would use too much memory. The configurable pool offers controlled parallelism. `put_batch` compresses multiple values in parallel before serial append to the WAL.

The compressed bytes are stored directly in the working set in RAM and reused by the WAL and snapshots. On reading, Zstd errors, lengths differing from the declared value, or inconsistent types cause a corruption error.

`stats` exposes:

- `compressed_values` and `raw_values`;
- `logical_value_bytes` and `stored_value_bytes`;
- `compression_ratio`, where values below 1 indicate savings;
- `compression_channels`.

## 9. Persistence and recovery

The data directory contains:

- `aprodb.wal`: append-only log of mutations;
- `aprodb.snapshot`: a consistent image of live keys;
- `aprodb.snapshot.tmp`: possible temporary file during creation; not read as a valid database.

Write procedure:

1. validation of key and value;
2. assignment of the sequence number;
3. appending the record to the WAL with CRC32;
4. optional `sync_data` step depending on durability;
5. application to the in-memory shard.

At startup, AProDB loads the snapshot and WAL. An incomplete final WAL frame, typically caused by a shutdown during a write, is ignored, and the file is truncated to the last intact frame. An incorrect checksum in a complete frame is instead reported as corruption.

Snapshot creation temporarily blocks new writes, but not reads. The WAL remains the authoritative source for recovery.

## 10. Parallelism

- Sharding limits locks to keys within the same shard.
- `get_batch`, `put_batch`, prefix scan, vector collection, CPU scoring, and sorting use Rayon.
- Compression and decompression use a sharded pool of Zstd contexts without a global lock.
- The record sequence guarantees *last assigned write wins* even when threads finish out of order.
- The snapshot uses a global write gate only to obtain a consistent boundary.

## 11. GPU acceleration

The GPU is lazily initialized on first use. A WGSL compute shader assigns a logical thread to each vector and computes:

- `Dot`: sum of products of components;
- `Cosine`: dot product divided by the product of the norms.

Policies:

- `Cpu`: always uses Rayon;
- `Gpu`: requires a GPU and returns an error if not available;
- `Auto`: tries the GPU above `gpu_min_work`; if initialization or dispatch fails, falls back to CPU.

The GPU is only beneficial when the computational workload justifies the overhead of upload, dispatch, and readback. Key-value lookups intentionally remain on the CPU/RAM.

## 12. CLI

General syntax:

```text
aprodb [--path DIRECTORY] [--relaxed] <COMMAND>
```

Global options:

- `--path`: data directory; default `.aprodb`;
- `--relaxed`: avoids `sync_data` at every operation and favors throughput by accepting a higher risk in case of crash.

The CLI writes JSON results to standard output and reports errors with a non-zero exit code.

### `put`

```powershell
aprodb put greeting "hello world"
aprodb put visits 42 --kind integer
aprodb put temperature 21.5 --kind float
aprodb put embedding "0.1,0.2,0.3" --kind vector
aprodb put signature "00ff10" --kind bytes
```

Formats for `--kind`: `text` (default), `bytes` (hexadecimal), `integer`, `float`, `vector` with comma-separated components.
The command returns the assigned key and sequence.

### `get` and `delete`

```powershell
aprodb get greeting
aprodb delete greeting
```

`get` returns the typed value or `null`.
`delete` returns `deleted: true` only if the key was active.

### `scan`

```powershell
aprodb scan "user:" --limit 100
```

Performs a parallel prefix scan, sorts the keys, and limits the result.
An empty prefix scans all active keys.

### `vector-search`

```powershell
aprodb vector-search "0.9,0.1,0" --limit 10 --metric cosine --backend auto
```

- `--metric`: `cosine` (default) or `dot`;
- `--backend`: `auto` (default), `cpu`, or `gpu`;
- only the keys with `Value::Vector` of the same dimension as the query are considered.

The result reports hit, score, number of candidates, actual backend, and accelerator name.

### `stats`, `snapshot` and `gpu-info`

```powershell
aprodb stats
aprodb snapshot
aprodb gpu-info
```

`stats` displays keys, tombstones, sequence, WAL bytes, threads, compression, and availability of the GPU feature. `snapshot` saves the live keys and cleans up the tombstones in RAM. `gpu-info` initializes the backend and returns the selected adapter or an explicit error.

### `demo`

```powershell
aprodb --relaxed demo --items 10000 --dimension 128 --backend auto
```

Generates deterministic vectors with the prefix `demo:vector:`, performs batch ingest and a top-5 search, then displays timings and throughput. Writes to the chosen directory: use a dedicated path if you do not want to mix demo data with application data.

### Help

```powershell
aprodb --help
aprodb vector-search --help
```

## 13. Maintenance and operational security

- Perform backups of the data directory only after `sync()` or when the process is stopped.
- Do not manually modify WAL or snapshot files.
- Do not share the same directory between multiple processes: cross-process locking is not yet implemented.
- Monitor `wal_bytes` and plan for future WAL compaction.
- Use `Relaxed` only if you accept the possible loss of the most recent writes in case of crash or power failure.
- Keep external copies: WAL and snapshots do not replace a proper backup strategy.

## 14. Code structure

- `src/engine.rs`: public API, sharding, concurrency, and orchestration;
- `src/value.rs`: value types and binary format;
- `src/compression.rs`: `StoredValue`, raw/Zstd selection, and context pool;
- `src/record.rs`: persistent frames and checksum;
- `src/wal.rs`: append and recovery;
- `src/snapshot.rs`: snapshot reading and writing;
- `src/compute/cpu.rs`: parallel CPU scoring;
- `src/compute/gpu.rs`: `wgpu` initialization, shader, and readback;
- `src/main.rs`: CLI;
- `tests/`: end-to-end behavior and persistence.

## 15. Troubleshooting

**GPU not available**: use `Auto` or `Cpu`; check driver and `wgpu` backend support.

**Checksum error**: work on a copy of the directory, keep the original files, and restore from backup; do not automatically delete the reported frame.

**Very large WAL**: snapshots improve state loading, but version 0.1 does not yet rewrite the WAL. Plan sufficient disk space.

**Poor ingest performance**: prefer `put_batch`; consider `Relaxed` only if its durability risk is acceptable.

**First GPU query slow**: lazy initialization of the adapter and pipeline occurs during the first request. Reuse the same `Database` instance; subsequent requests use the pipeline already created. In `Auto` mode, tune `gpu_min_work` based on benchmarks on your hardware.

## 16. Reproducible benchmark

```powershell
cargo bench --bench throughput
```

The benchmark creates a temporary directory, inserts 50,000 vectors with 64 components, and measures
the CPU path, the first GPU request, and the warm GPU path while also verifying that the rankings match.
On the Intel Iris Xe workstation used during development: about 61,120 insert/s in batch relaxed,
CPU 71.77 ms, cold GPU 534.40 ms and hot GPU 98.98 ms.
These are local measurements, not SLA values; they justify the conservative default value for `gpu_min_work`.

## 17. Benchmark against external databases

The `benchmarks/comparative` crate compares AProDB with SQLite, PostgreSQL, MySQL, MariaDB, and Redis/Valkey using identical keys and payloads. It is independent of the main crate: SQL and Redis drivers are not included in applications that depend on AProDB.

### Protocol

- two 50,000-record profiles: repetitive/compressible and pseudorandom;
- 512-byte binary payload and batch size of 500;
- a durable commit per batch;
- 50,000 point lookups against the hot dataset;
- 20 ordered prefix/range scans, limited to 1,000 rows;
- three repetitions, compared by median;
- automatic verification of lengths and row counts.

Full execution, after creating `aprodb_bench` on the servers:

```powershell
cargo run --release --manifest-path benchmarks/comparative/Cargo.toml -- `
  --backends aprodb,sqlite,postgres,mysql,mariadb,redis `
  --profiles compressible,random `
  --records 50000 --reads 50000 --payload-bytes 512 `
  --batch-size 500 --runs 3 --scan-repeats 20 --scan-limit 1000
```

For an embedded test without a server:

```powershell
cargo run --release --manifest-path benchmarks/comparative/Cargo.toml -- `
  --backends aprodb,sqlite --profiles compressible,random
```

The runner accepts custom URLs with `--postgres-url`, `--mysql-url`, `--mariadb-url`, and `--redis-url`. It writes an incremental JSON report under the `--workdir`; a backend error does not erase previously completed tests.

### Local results as of 19 August 2026

On an Intel Core i5-1340P with 16 threads and NVMe SSD, all 30 tests passed verification. Main medians:

|Profile|Engine|Ingest ops/s|Lookup ops/s|Lookup p99 μs|Scan ops/s|MiB space|
|---|---|---:|---:|---:|---:|---:|
|compressible|AProDB|43,215|161,677|10.8|400.4|6.76|
|compressible|SQLite|6,157|13,107|223.6|2,455.9|29.61|
|compressible|PostgreSQL|32,157|974|1,630.2|310.2|31.10|
|compressible|MySQL|15,915|1,870|969.2|608.9|44.00|
|compressible|MariaDB|55,200|2,290|838.1|634.6|44.00|
|random|AProDB|28,091|395,376|3.1|387.1|27.32|
|random|SQLite|6,032|13,559|230.0|2,565.3|29.61|
|random|PostgreSQL|32,918|966|1,610.8|277.5|31.10|
|random|MySQL|13,431|1,791|984.1|551.8|44.00|
|random|MariaDB|36,106|2,146|896.5|677.3|44.00|

On this workload, AProDB leads in point lookups and space usage on compressible data. It does not lead in durable ingestion: MariaDB wins in both profiles, and PostgreSQL surpasses AProDB on random data. SQLite dominates scans because it uses the ordered primary key, while AProDB's `scan_prefix` currently scans all shards. This highlights ordered indexes as a concrete priority.

The embedded vs. client-server comparison must be interpreted correctly. AProDB and SQLite do not use network communication; PostgreSQL, MySQL, MariaDB, and Redis/Valkey use a local TCP connection. AProDB keeps the active dataset in RAM. SQL space does not include global redo/WAL, and Redis/Valkey space is `used_memory_dataset`, not AOF/RDB bytes. The test does not measure maximum capacity, concurrent clients, joins, replication, or fault recovery, and is not an SLA. Methodology and full tables are in `benchmarks/comparative/RESULTS.md`.

## 18. Engine capacity and choice

| Characteristic | AProDB 0.1 | SQLite | PostgreSQL/MySQL/MariaDB |
|---|---|---|---|
| Model | Typed KV and vectors | Embedded relational SQL | Client/server relational SQL |
| Hot KV lookup | Very fast in-process | Fast in-process | Includes protocol and SQL |
| Per-value compression | Integrated adaptive Zstd | Not in core | Depends on engine/configuration |
| Prefix/range index | Not yet; shard scan | B-tree | B-tree and advanced indexes |
| General ACID transactions | Not yet | Yes | Yes |
| Multi-process/network access | Not yet | Multi-process on file, no core server | Yes |
| Query, join, and constraints | No | Yes | Yes |
| GPU vector search | Integrated | Not in core | Via specific extensions/products |
| Replication and high availability | No | Not in core | Yes, with varying capabilities |
| Operational maturity | Prototype/MVP | Very high | Very high |

Use AProDB 0.1 when the case is embedded, single-process, key-value or vector, and you accept the explicit MVP limitations. Use SQLite when SQL and local transactions on a single file are required. Use PostgreSQL, MySQL, or MariaDB when you need concurrent users, network access, full transactions, multiple indexes, replication, and mature operational tooling.

## 19. Target architecture

The normative specification for radial AProDB is in [paper.md](paper.md).
It defines the target contracts and the criteria required to declare each milestone complete. This manual distinguishes the 0.1 prototype from the 1.x capabilities that are actually implemented.

The approved direction includes:

- central service for multiprocess access and exclusive embedded mode;
- CPU as the complete reference implementation and GPU as an optional accelerator;
- storage contract with Fjall as the first candidate, redb/RocksDB as fallback, and native backend only if necessary;
- atomic AProDB catalog and logical change log with record, distinct from the backend's physical WAL; events do not duplicate the full payload by default;
- retention of events selectable per collection among Delta, VersionRef, and SelfContained, without long-lived MVCC snapshots;
- single Durable mode with configurable group commit window;
- atomicity within a partition, CAS, idempotency, claim, lease, and fencing;
- separate caches, and initial radial score based on freshness, workflow, and pin; other signals only after measurement;
- incremental, reconstructible, and watermarked work and read surfaces;
- adaptive compression of logical payload with Zstandard and Raw fallback, coordinated per keyspace with backend physical compression;
- support for datasets larger than RAM, storage classes, and limited compaction;
- versioned binary protocol, quotas, backpressure, recovery, backup, and observability.

The GPU component remains in the original roadmap: it has not been postponed. Compute interfaces and layouts are prepared by the foundations; correctness remains available also on CPU-only.

The [implementation matrix](docs/requirements-matrix.md) connects normative requirements to verifiable gates. Any paper requirement not recorded there as implemented and verified remains a design target, not an available feature.

## 20. Repository and source distribution

AProDB is published in the canonical public repository [provenzali/aprodb](https://github.com/provenzali/aprodb). The local `main` branch uses that repository as `origin`; publication events and verification are recorded in the diary.

The project is explicitly in **beta testing** and is not production-ready. The distribution identifies Andrea Provenzali as the original creator and author of the specification by name, account `@provenzali`, and ORCID `0009-0009-9677-9840`; tax code, date of birth, nationality, and email are not included in public files.

The Milestone 0 publication baseline records:

- owner `provenzali`, repository `aprodb`, and public visibility;
- an audit of secrets, large files, and local artifacts;
- Core `AGPL-3.0-only`; client, protocol, and public types `Apache-2.0`;
- branch `main` and a verified baseline commit;
- local CPU-only tests followed by GitHub CPU-only CI;
- no GPU dependency on hosted runners;
- no force-push or modification of repositories unrelated to AProDB.

The `.gitignore` excludes Rust build artifacts, runtime data, WAL, snapshots, local databases, sensitive configurations, and logs. Cargo.lock is part of the reproducible distribution and must not be ignored.

The binding map of components is in `LICENSING.md`. Distributions retain `NOTICE`; contributions use DCO and inherit the license of the modified component. The client does not depend on the AGPL compute implementation: the shared types required for the wire are in `aprodb-types`.

## 21. Experimental canonical engine 1.x — Milestone 1

The facade retains the 0.1 API at the root and exposes the new vertical via `aprodb::v1`. This path can be used as an embedded library for local experimentation; for multiple processes, use the server described in the next section, which remains the sole owner of the data directory.

Minimal example:

```rust
use aprodb::v1::{Engine, EngineConfig, Payload, PutRequest, RecordIdentity};

fn main() -> Result<(), aprodb::v1::AproError> {
    let config = EngineConfig::new("./data-v1");
    let engine = Engine::open(config)?;
    let id = RecordIdentity::new(
        "tenant", "namespace", "objects", "partition-a", "key-1"
    )?;


    let receipt = engine.put(PutRequest::new(
        id.clone(), Payload::Text("hello".into())
    ))?;
    let record = engine.get(&id)?.expect("record exists");
    assert_eq!(record.version, receipt.version);
    Ok(())
}
```

### Configuration and limits

`EngineConfig` requires a data directory, a shard count that is a power of two, and positive, consistent budgets for keys, records, batches, in-flight memory, code, and storage. The directory is marked as logical format 1.x and locked exclusively, including across processes. A 0.1 directory containing `aprodb.wal` or legacy snapshots is rejected with `IncompatibleFormat`; there is no automatic import. The explicit, copy-only one-shot import is described in section 27.

The default configuration uses Fjall 3.1.8 with canonical payloads and no additional physical compression, and LZ4 for metadata, change log, and surfaces; cache and memtable are limited, and maintenance has timeouts. Canonical logical compression is described in section 25. `group_commit_window = 0` enforces a `SyncAll` for every Durable request. With a positive window, requests are queued on a bounded channel and receive the receipt only after the group's persistence; `group_commit_max_bytes` can close the group early.

### Operations and consistency

Put, Get, Delete, CompareAndSwap, and AtomicBatch are available within a single partition. Each mutation writes as part of the same atomic batch:

- the immutable version of the record;
- the head pointer;
- the change event with sequence and batch id;
- the versioned catalog and watermarks.

`Durable` acknowledges the request only after `SyncAll` or after the persistence of the group commit. `Relaxed` guarantees visibility and logical order, but not survival after a power loss; `Engine::sync()` brings the catalog and watermark to the Durable state.

Each collection can use Delta, VersionRef, or SelfContained. Delta requires self-sufficient data provided by the request. VersionRef always reads the immutable version indicated by the event, never the current value. SelfContained requires explicit policy and respects the configured size limit. The GC removes obsolete events and versions only up to the minimum watermark of required consumers, and always preserves the current version.

### Recovery, checkpoint, and maintenance

Reopening reconstructs state from Fjall and validates the format, backend, shards, and catalog. `verify()` checks that each head resolves the exact version and that the sequences of events are consistent. `create_checkpoint(destination)` logically stops writers, makes the catalog durable, and copies keyspaces in paged fashion into a new directory; it does not overwrite an existing destination directory.

`major_compact()` forces flush and compaction through Fjall API with timeout, without interpreting physical files. `stats()` reports available space, write buffer, journal, tables, flush, and compaction counters. After any commit or persist error, the backend and engine enter a fail-closed state: new operations are rejected until closed and reopened, as the physical outcome may be ambiguous.

### Real limits

Idempotency key, workflow, and projections are available in the vertical of section 24; GPU and operability are described in sections 26 and 27. The Fjall spike, the ADR, and upstream risks are documented in `benchmarks/storage-spike` and `docs/adr/0001-fjall-backend.md`.

## 22. Experimental multiprocess server — Milestone 2

`aprodb-server` is the central 1.x process. It opens and exclusively locks the data directory; application processes must use `aprodb-client` and must not open the same directory. By default, TCP uses `127.0.0.1:7643` for data operations and `127.0.0.1:7644` for administration.

### Startup and authentication

Data and admin tokens must be distinct and contain between 16 and 4096 bytes. They are read from `APRODB_DATA_TOKEN` and `APRODB_ADMIN_TOKEN`, not from arguments, and are not printed by configuration types or startup logs.

```powershell
$env:APRODB_DATA_TOKEN = "replace-with-data-token"
$env:APRODB_ADMIN_TOKEN = "replace-with-admin-token"
cargo run -p aprodb-server -- --data-dir .\aprodb-data
```

Endpoints can be changed with `--data-listen` and `--admin-listen` or disabled with `--no-data-tcp` and `--no-admin-tcp`. `--data-local` and `--admin-local` enable Windows named pipes (e.g. `\\.\pipe\aprodb-data`) or Unix domain sockets. The server creates the first named pipe before declaring itself started.

TCP plaintext is rejected on non-loopback addresses. The explicit `--allow-plaintext-non-loopback` option removes only this security restriction and is not recommended for untrusted networks; TLS/mTLS is described in section 27. Environment variables remain visible to the process account according to operating system rules; use a service account and appropriate ACLs.

### Protocol and Rust client

The wire format is Protobuf, framed with a big-endian `u32` length prefix. The handshake checks for magic `APRODB`, major protocol 1, role, token, and maximum size. The default limit is 8 MiB, and the client applies the lowest negotiated value. The canonical messages and enums are also described in `crates/aprodb-proto/proto/aprodb.proto`; golden and property tests safeguard the format.

`AsyncClient` multiplexes multiple request IDs over a limited connection and correlates responses, even if out of order. `BlockingClient` offers the same interface for synchronous programs. Put, Get, Delete, CompareAndSwap, and AtomicBatch are available within a partition, along with workflow, change stream, surfaces, Sync, and administrative commands. The receipt preserves version, shard, sequence, applied durability, and the engine's durable watermark. Durable and Relaxed have the same semantics as described in section 21.

The client deadline covers both time spent in the queue and waiting for a response. The server refuses already expired deadlines before admission; a storage operation that has already been admitted is not interrupted midway, to avoid ambiguous outcomes. Persistent idempotency keys make explicit retries by the caller safe; the client does not yet perform automatic retries.

### Limits, backpressure, and shutdown

Frame size, connections, in-flight requests per connection and globally, response queue, idle timeout, and drain timeout all have configurable limits. The main options are `--max-frame-bytes`, `--max-connections`, `--max-inflight-per-connection`, `--max-inflight-global`, `--response-queue-depth`, `--idle-timeout-ms`, `--drain-timeout-ms`, and `--backpressure-retry-ms`. When the in-flight limit is exceeded, the server returns `Backpressure` with a positive `retry_after`, without creating an unbounded queue.

The data role cannot perform Health, Stats, Verify, Compact, or Shutdown; the admin role cannot read or modify records. The administrative CLI uses only TCP:

```powershell
$env:APRODB_ADMIN_TOKEN = "replace-with-admin-token"
cargo run -p aprodb-cli -- health
cargo run -p aprodb-cli -- stats
cargo run -p aprodb-cli -- verify
cargo run -p aprodb-cli -- compact
cargo run -p aprodb-cli -- shutdown
```

`Stats` exposes on-disk and write-buffer byte counts, plus counters for connections, in-flight requests, total requests, rejections, and failed authentications. Shutdown stops accepting new requests, completes those already admitted, closes responses, and waits for the admitted work to drain; Ctrl+C uses the same path. Tenant quotas, audit, and TLS are described in Milestone 7; export to an external metrics system is not yet available.

## 23. Experimental radial engine and storage capacity — Milestone 3

Milestone 3 keeps Fjall as the owner of WAL, manifest, segments, Bloom, flush, and compaction. AProDB does not interpret or duplicate these formats: `stats()` reports the figures provided by the backend, and `major_compact()` uses only its public API. The canonical records remain in storage, and there is no `HashMap` containing the entire 1.x dataset.

### Memory and cache budget

At startup, the server detects the physical memory and, when available, the cgroup limit. Without an override, it uses half of the lesser value; `--memory-budget-bytes N` requires a value of at least 128 MiB and is still capped by the detected ceiling. The startup log reports the effective, physical, container, and configured memory budgets in bytes.

`EngineConfig::apply_memory_budget` divides the budget among cache storage, memtable, inflight, metadata cache, object cache, compressed cache, scratch codec, and negative cache, leaving some headroom unreserved. Validation rejects any configuration where the sum of reserves exceeds the budget. The four AProDB caches are sharded and independent; the object cache uses weighted admission based on frequency, score, size, and pin, while misses have a short TTL. Scans, checkpoints, verification, and compaction bypass the object cache. `cache_stats()` and the following admin command expose budget, resident bytes, hits, misses, admissions, rejections, and evictions:

```powershell
cargo run -p aprodb-cli -- cache-stats
```

The dedicated capacity test writes 129 MiB of pseudorandom payloads with an engine budget of 128 MiB, performs sync, compaction, reopen, and exact reads. It is marked `ignored` in the regular suite because it lasts about 80 seconds and writes more than 129 MiB; the explicit gate is:

```powershell
cargo test -p aprodb-engine --no-default-features `
  canonical_dataset_can_exceed_the_configured_memory_budget -- --ignored
```

This test demonstrates exceeding the configured budget; it is not a benchmark that exhausts physical RAM nor a capacity SLA.

### TTL

`PutRequest::expires_at_unix_ms` sets an absolute UTC expiration. Version, head, change event, radial descriptor, and TTL index are updated in the same batch. `Get` never returns an expired record, even before physical cleanup. `expire_due(limit, durability)` examines a limited number of entries and deletes only if key and version still match, so an old index cannot delete a newer update. The CLI performs a durable sweep of up to 1024 entries:

```powershell
cargo run -p aprodb-cli -- expire
```

There is not yet an automatic TTL cycle in the daemon. For collections with `Delta` retention, expiry is rejected until a self-sufficient delta has been declared as available; `VersionRef` and `SelfContained` retain the usual guarantees of the change log. UTC time does not determine write order: version and sequence remain authoritative.

### Radial descriptor, policy, and storage class

Each Put creates or updates a `RadialDescriptor` atomically with canonical version, timestamp, deadline, size, workflow/projection status, reconstruction cost, class, layer, and motivation. Policies for collection, storage class, and generation are versioned and restored on reopen. The initial score uses freshness and urgency; separate thresholds, minimum permanence, and expiring pins limit oscillations. The score guides placement decisions but does not affect correctness.

`explain_placement` returns version, score, signals, current and recommended layer, class, pin, cache residency, physical tiering capability, and motivations. It is read-only with respect to the object cache. From the administrative client:

```powershell
cargo run -p aprodb-cli -- explain tenant namespace collection partition key
```

On the current Fjall backend, `physical_storage_tiering` is `false`: multiple classes without a path remain logical labels, while registering an alternative physical path fails with `Unsupported`. No file migrations, I/O priorities, or media controls unsupported by the backend are simulated. The derived surfaces of Milestone 4 achieve reconstructable placement without moving the canonical record.

## 24. Workflow, change stream, and experimental surfaces — Milestone 4

Milestone 4 is available in embedded mode via `aprodb::v1::Engine` and on the data plane via `AsyncClient` and `BlockingClient`. The worker semantics are at-least-once: AProDB makes database transitions atomic, but does not guarantee exactly-once for external effects.

### Persistent idempotency

Put, Delete, AtomicBatch, Append, Claim, Heartbeat, Complete, Fail, and Publish accept an optional 32-byte idempotency hash. The caller must compute the hash from an opaque key without sending the original secret. The scope is the partition; the engine saves a fingerprint of the request and receipt in the same batch as the canonical mutation. An identical retry within `EngineConfig::idempotency_retention` returns the same version, receipt, and lease; reusing the same hash for different parameters returns `Conflict`.

The default retention period is 24 hours. `purge_expired_idempotency(now, limit)` removes records and expiration indices with a limited batch; there is not yet a periodic sweep in the daemon. An expired record cannot be reused as a valid result. The durability of recording coincides with that of the mutation; a Durable outcome is recognized only after persistence.

### Workflow and fencing

`Append` creates a new record with status `pending`. `Claim` operates within a `WorkflowScope` (tenant/namespace/collection/partition), selects a limited batch of eligible records, and atomically moves them to `leased`. Each result includes the record/version, receipt, a random 128-bit lease ID, a monotonically increasing fencing token, UTC deadline, server time, and retry metadata. The default limit is 128 records, the maximum lease duration is 15 minutes, and the total number of active leases per process is limited.

`Heartbeat` requires the current lease ID and fencing token, and assigns a new deadline based on server time. `Complete` moves the record to `completed`; `Fail(false)` returns it to `pending`, unless `max_workflow_attempts` has been reached, while `Fail(true)` immediately moves it to `dead_letter`. `Publish` only accepts `completed` and produces `published`; repeating it on an already published record is an idempotent no-op. Expired leases or obsolete attempts return `Conflict` and do not modify the record.

During processing, a monotonic `Instant` is used to ensure the validity of the lease. After a restart, the persisted UTC deadline and configurable `lease_recovery_safety_margin` apply; an expired leased record becomes eligible again, and the next claim increments the fencing token. The record, workflow index, change event, idempotency, and catalog are all updated in the same batch storage operation. `Delta` collections are rejected by the generic workflow until a self-sufficient delta for transitions is declared.

Abbreviated asynchronous client example:

```rust
use std::time::Duration;
use aprodb_client::{AsyncClient, PutOptions};
use aprodb_types::{Durability, Payload, WorkflowScope};

let receipt = client.append(
    identity.clone(),
    Payload::Text("job".into()),
    PutOptions {
        idempotency_key_hash: Some([1; 32]),
        ..PutOptions::default()
    },
    Durability::Durable,
).await?;

let claimed = client.claim(
    WorkflowScope::new("tenant", "namespace", "jobs", "partition-a")?,
    16,
    Duration::from_secs(60),
    Some([2; 32]),
    Durability::Durable,
).await?;
if let Some(job) = claimed.first() {
    client.complete(
        job.record.identity.clone(), job.lease, Some([3; 32]),
        Durability::Durable,
    ).await?;
}
# let _ = receipt;
# Ok::<(), Box<dyn std::error::Error>>(())
```

### Change stream

`subscribe_changes(tenant, namespace, collection, shard, after_sequence, limit)` returns a page of `ChangeEvent` and the global watermark of the shard. The watermark may advance even if the filtered page is empty, because other collections share the shard: the consumer must save the watermark from the response, not infer it from the last filtered event. An AtomicBatch is not split; a cursor in the middle of a batch is rejected.

If the GC has deleted the requested sequence, the server returns `ChangeLogGap`. `VersionRef` always points to the exact immutable version; `SelfContained` contains the record only according to policy and limit; `Delta` remains self-sufficient for the declared consumer. The protocol uses limited-size frames: the consumer must choose pages proportionate to the negotiated frame size. This API is pull/paged; push notifications and non-Rust bindings are not yet available.

### Work/Read surfaces

A persistent `SurfaceDefinition` contains an id, type `Work` or `Read`, a source collection, the allowed workflow states, format, record/byte limits, and number of retained generations. The order is total and deterministic according to `RecordIdentity`. The implemented formats are the AProDB binary record frame and pre-serialized JSON. The builder limits output first by record and then by byte; it does not allocate an unbounded queue.

`create_surface` is administrative and idempotent only for the same definition. It also registers a mandatory consumer per shard, so GC cannot remove the required versions. `build_surface(id, max_events, durability)` reads from the next watermark, applies insert/update/remove, serializes an immutable generation, and publishes in the same batch the generation, pointer, consumer watermark, and catalog. Publication is always made Durable, even if the requested parameter is Relaxed, because a non-durable watermark cannot authorize GC. There is not yet a periodic builder: it must be invoked through an administrative call.

`get_surface` uses the data plane and returns the generation, timestamp, per-shard watermarks, staleness in sequence, a `complete` flag, and errors. `complete` is true when the watermarks reach the current sequences; a build with an exhausted event budget can publish a valid but stale generation. `rebuild_surface` is administrative, scans the canonical state under shard lock, and should be used explicitly after `ChangeLogGap`, schema change, or derived damage. It does not use long-lived MVCC snapshots.

Administrative example:

```powershell
cargo run -p aprodb-cli -- create-surface pending-work work tenant namespace jobs pending records 1000 8388608 2
cargo run -p aprodb-cli -- build-surface pending-work 4096
cargo run -p aprodb-cli -- rebuild-surface pending-work
```

The data client reads the surface with `get_surface(tenant, namespace, projection_id)`. The server checks that the tenant and namespace match the source. Older generations beyond retention are removed atomically during publication; there is not yet administrative rollback to a previous generation.

### Recovery, verification, and limitations

Workflow index, idempotency, definitions, pointers, generations, and surface payloads belong to the logical checkpoint and are reopened without rebuilding already Durable state. `verify()` checks workflow indexes, version references, retained generations, and consistency between pointer and watermark in the catalog. A gap is not hidden: incremental build fails and requires explicit rebuild.

The current surfaces are a minimal declarative increment: a collection, workflow status filter, sort by identity, and output as record/JSON. Filters on generic indexes, time windows, field selection/transformation, MessagePack/Protobuf/Arrow, dependencies between projections, automatic scheduler, and rollback remain unimplemented. Aggregate metrics for claim/lease/build are not yet exported; receipt, build report, watermark, staleness, and server Stats are the observable surfaces currently available. Quotas per tenant, audit, TLS, and encryption are described in section 27.

## 25. Canonical logical compression — Milestone 5

The new 1.x records use the logical frame `APRX` v1. The record keeps metadata and workflow separate from the payload; the serialized payload contains codec version, `Raw` or `Zstandard`, logical length, CRC32, and optional dictionary id. A read verifies the frame, length, checksum, dictionary, and decompression before reconstructing `Payload`. Experimental `APRC` frames produced before Milestone 5 remain readable; new writers do not produce them.

### Collection and tier policy

`CompressionPolicy` has a distinct policy for `Surface`, `Hot`, `Warm`, `Cold`, and `Archive`. Each canonical tier sets mode, Zstandard level, minimum input threshold, minimum savings, and dictionary id. `Surface` is Raw in this milestone because its payload is already serialized and can use separate physical compression of the keyspace. Default prefixes skip image, audio, video, zip, gzip, and zstd. If the Zstandard candidate does not exceed the minimum savings, the record remains Raw without expansion.

The policy is persisted durably in the compression catalog and applies to new versions; it does not retroactively rewrite existing ones. The tier used is that of the previous radial descriptor, or Warm for a new key. Administrative examples:

```powershell
cargo run -p aprodb-cli -- compression-policy tenant namespace objects
cargo run -p aprodb-cli -- set-compression tenant namespace objects raw
cargo run -p aprodb-cli -- set-compression tenant namespace objects zstd
cargo run -p aprodb-cli -- compression-stats
```

`set-compression raw|zstd` applies the uniform CLI profile to the four canonical tiers. For specific levels, thresholds, skip lists, and dictionary ids, use `AsyncClient::configure_compression` or `Engine::configure_compression_policy` with a complete `CompressionPolicy`.

### Pool, memory and cache

`EngineConfig::compression_channels` must be a power of two between 1 and 64; the default is the power of two corresponding to the available parallelism, with a maximum of 16. Compressor and decompressor contexts are reused. The total scratch space is limited by `compression_scratch_bytes`; a request that cannot reserve this will receive `Backpressure` before the commit. `apply_memory_budget` by default allocates 12% to scratch and 8% to the compressed cache.

The object cache contains decoded records; the compressed cache retains the frame of the current version. They have separate budgets, admissions, hit/miss tracking, evictions, and version invalidation. `cache-stats` displays both. Scans and maintenance continue to bypass the object cache; historical versions are not held indefinitely in cache.

`compression-stats` displays logical/encoded bytes, Raw/Zstandard/with-dictionary records, incompressible fallbacks, skipped content-types, codec microseconds, errors, channels, and current/budgeted scratch. These are counters of codec attempts, not only of successfully committed operations.

### Dictionaries

`train_and_activate_dictionary` and `AsyncClient::train_dictionary` require at least eight training samples, a separate validation set, schema, maximum size, and minimum gain. The number of samples, total bytes, dictionary size, and number of dictionaries are limited by `EngineConfig`. The dictionary is published only if it reduces the total validation set size compared to the same level without a dictionary. The updated dictionary and catalog are committed in the same durable batch.

Each version records its exact dictionary id; a read operation never uses the current dictionary in place of the recorded one. The dictionary bytes, checksum, and validation statistics are part of checkpoint and recovery. A missing or corrupt dictionary returns `Corrupt`, not a partial value. There is no dictionary garbage collection yet: dictionaries are retained conservatively until a complete version reachability test is available.

### Physical compression and benchmark

The default prevents double compression on the canonical keyspace; Fjall retains LZ4 for metadata, change logs, and surfaces. Physical options remain configurable per keyspace, but enabling them together with Zstandard requires measurement of the actual workload. The reproducible four-mode matrix, with ratio, durable latency, throughput, CPU, RAM, I/O, space, compaction, and recovery, is in `benchmarks/compression`. The local run is small and does not define an SLA.

External blobs are not transformed by the canonical codec: `BlobReference` remains a reference. Compression and storage of blob bytes will require a separate policy when the blob store is implemented. TLS, at-rest encryption, backup, and copy-only tooling are described in section 27.

## 26. Heterogeneous compute — Milestone 6

`Engine::vector_exact` and `AsyncClient::vector_exact` perform exact top-k search on all `Payload::Vector` in the collection that have the same dimension as the query. Dot product and cosine similarity are implemented. The CPU is the semantic authority: NaN/infinite input values are rejected, the cosine of a zero vector is zero, and ties are ordered by row, i.e., by identity in the order of canonical scan. CPU and GPU declare a relative tolerance of `1e-4` on float results.

Asynchronous client example:

```rust
use aprodb_client::{ComputePreference, VectorMetric};

let result = client.vector_exact(
    b"tenant".to_vec(), b"namespace".to_vec(), b"embeddings".to_vec(),
    vec![0.9, 0.1, 0.0], VectorMetric::Cosine,
    10, 100_000, ComputePreference::Auto,
).await?;
for hit in result.hits {
    println!("{:?} {}", hit.identity.key, hit.score);
}
# Ok::<(), aprodb_client::ClientError>(())
```

`max_scan_records` is mandatory and limits the records examined, not just compatible vectors. If the collection exceeds it, the request fails with `ResourceLimit` instead of returning a partial result. The columnar batch must also adhere to `compute.max_batch_rows` and `max_batch_bytes`. Non-vector records and vectors of different dimensions are ignored. ExactFlat is O(N): there is no ANN index yet.

### Consistency, scheduler, and fallback

The scan briefly acquires all shard orderers, builds a coherent projection, and captures the global generation; locks are released before computation. The result represents that generation and can be superseded by a concurrent mutation afterward, as with a normal read. VRAM cache uses projection id, generation, and schema version: it never reads a buffer from a previous generation.

`ComputePreference::Cpu` forces the dedicated CPU pool. `Auto` chooses the accelerator only when the estimate `transfer_in + queue_wait + launch + gpu_compute + transfer_out + sync + risk` is lower than for the CPU. `Accelerator` skips the cost comparison but retains the safe CPU fallback. Absence of GPU, exhausted queue/byte budget, timeout, driver error, or circuit breaker do not compromise storage and result in `CpuFallback` with a reason in the response.

The queue, in-flight bytes, workers, batch, pending micro-batch, timeout, and VRAM all have limits in `EngineConfig::compute`. The server exposes overrides with `--compute-cpu-threads`, `--compute-queue-depth`, `--compute-queue-bytes`, `--compute-max-batch-rows`, `--compute-max-batch-bytes`, `--compute-timeout-ms`, `--compute-micro-batch-ms`, and `--gpu-vram-bytes`. Inconsistent values prevent startup. The automatic memory budget also reserves memory for the compute queue.

### GPU, metrics and benchmarks

The default server feature `gpu` uses wgpu and initializes the adapter/device/pipeline only on the first accelerated request. `--no-default-features` removes wgpu and maintains the entire semantics via CPU. VRAM only retains derived buffers with LRU eviction; different schema/generation, invalidation, or device reset require a new upload. Asynchronous readback, polling, and response waiting are limited by timeout. No canonical data is stored in VRAM.

The `compute_stats` admin endpoint and the CLI expose CPU/accelerator requests, fallback, rejections, timeouts, batches, in-flight bytes, adapter name, VRAM usage/hits/misses/evictions, upload/readback bytes, transfer/kernel times, and reset:

```powershell
cargo run -p aprodb-cli -- compute-stats
```

The reproducible CPU/GPU benchmark, including transfers and top-k, is in `benchmarks/compute`. On the local system, a warm GPU was faster only for some intermediate batch shapes: acceleration is not guaranteed, and the model must be calibrated on real hardware. ANN, GPU filters/aggregations, CUDA/HIP, auto-tuning, and publication of GPU-mutated projections are not available.

## 27. Operability and security — Milestone 7

Milestone 7 is available on the 1.x track and remains experimental. All procedures that can change format or rebuild data operate in a new directory: AProDB does not perform restore, repair, rekey, upgrade, or import in-place. An existing destination directory is always rejected.

### At-rest encryption and keyring

`EngineConfig::encryption` enables XChaCha20-Poly1305 for the values of all keyspaces. The AAD binds the ciphertext, keyspace, key ID, and storage key; nonce and tag are verified on every read. An incorrect key, a moved frame, or tampered data returns `Encryption`/`Corrupt` without cleartext fallback. An encrypted database cannot be opened without all the keys required for backup or records.

The server accepts `--encryption-keyring FILE` or `APRODB_ENCRYPTION_KEYRING_FILE`. The JSON file is limited to 64 KiB:

```json
{
  "active_key_id": "primary-2026",
  "keys": {
    "primary-2026": "<64 hex characters, 32 bytes>"
  }
}
```

A maximum of 16 keys are allowed. The key material does not appear in `Debug`, logs, manifest, or audit. On Unix, the loader requires owner-only permissions; on Windows, the operator must apply an equivalent ACL. PEM files, the keyring, `.env`, and keys are excluded from the repository. The names of Fjall physical keys are not hidden: encrypting filenames and access patterns requires volume encryption.

Key rotation is explicit and copy-only:

```powershell
cargo run -p aprodb-cli --bin aprodb-ops -- rekey .\data-old .\data-new `
  --source-keyring .\old-keyring.json --destination-keyring .\new-keyring.json
```

The copy is re-opened and verified with the new keyring; the source retains the previous key and is not modified.

### TLS and mTLS

TCP can use Rustls with PEM chain and key:

```powershell
cargo run -p aprodb-server -- --data-dir .\aprodb-data `
  --tls-cert .\server-cert.pem --tls-key .\server-key.pem
```

`--tls-client-ca .\client-ca.pem` makes a valid client certificate mandatory. The admin CLI uses `--tls-ca`, `--tls-server-name`, and, for mTLS, `--tls-cert`/`--tls-key`. TLS timeouts and application handshake are limited. Named pipes and Unix sockets remain local and do not use TLS. Data and admin tokens continue to be verified inside the TLS channel.

### Audit and quotas

Compact, shutdown, expiry, creation/build/rebuild surface, compression configuration, dictionary training, and backup produce a Durable `Attempted` event before the action and a `Succeeded` or `Failed` event after the outcome.
Each event contains sequence, event id, timestamp, request id, principal, operation, outcome, optional BLAKE3 hash of the target, and error class; it does not contain token, record key, or payload.
Audit event reading is paginated and restricted to admin users:

```powershell
cargo run -p aprodb-cli -- audit - 100
cargo run -p aprodb-cli -- audit 200 100
```

`--admin-principal` assigns the registered identity. Audit is included in checkpoint, backup, recovery, and `verify`.

`--tenant-quotas FILE` loads a size-limited JSON file with this form:

```json
{
  "tenants": {
    "tenant-a": {
      "max_inflight": 8,
      "max_requests_per_second": 500,
      "max_request_bytes": 1048576,
      "max_vector_work_items": 10000000
    }
  }
}
```

Quotas are checked before dispatch. Exceeding the byte or compute-work quota returns `ResourceLimit`; rate or in-flight limits return `Backpressure` with retry-after. The per-second quota window is fixed, maintained in memory, and does not constitute billing. `--max-data-bytes`, `--min-free-disk-bytes`, and `--max-compaction-temporary-bytes` protect the disk. Over-quota writes fail before mutation; compaction, checkpoint, and restore check the space estimate before starting.

### Backup, restore, verify, and repair

With `--backup-root PATH`, the server accepts only safe ASCII backup names and resolves them under that root:

```powershell
cargo run -p aprodb-cli -- backup daily-001
```

Backup creates a consistent checkpoint, reopens it, runs `verify`, inventories files and bytes with BLAKE3, and publishes `backup-manifest.json` with catalog generation, watermark, backend, format, and key id. A simple copy is not considered a successful backup. Verification and restore are performed offline:

```powershell
cargo run -p aprodb-cli --bin aprodb-ops -- verify-backup .\backups\daily-001
cargo run -p aprodb-cli --bin aprodb-ops -- restore .\backups\daily-001 .\restored `
  --keyring .\keyring.json
cargo run -p aprodb-cli --bin aprodb-ops -- verify .\restored --keyring .\keyring.json
```

`verify` scans all records, versions/events, TTL, workflow, radial index, surfaces, dictionaries, and audit.
It does not repair.
The only allowed reconstruction concerns derived state and requires both copying and literal confirmation:

```powershell
cargo run -p aprodb-cli --bin aprodb-ops -- repair .\source .\repaired `
  REBUILD_DERIVED_ON_SEPARATE_COPY --keyring .\keyring.json
```

The JSON report distinguishes lost or questionable records from reconstructed indexes.
Corruption of the canonical record or catalog requires restore; it is neither hidden nor deleted.
Interruptions leave the partial copy for diagnosis and allow retry to a new destination.

### Import AProDB 0.1 and upgrade

The 1.x engine always rejects directories with `aprodb.wal` or `aprodb.snapshot`.
Import is offline and requires source, preserved copy, destination, and identity mapping:

```powershell
cargo run -p aprodb-cli --bin aprodb-ops -- import-0.1 `
  .\legacy .\legacy-preserved .\aprodb-1 legacy import default p0 `
  --max-records 1000000 --max-stored-bytes 4294967296 `
  --max-source-bytes 17179869184 --batch-operations 256
```

The command copies the snapshot and WAL into `raw` with checksums, uses a second copy for the 0.1 reader (which can only truncate an incomplete WAL tail), exports within record/byte limits, writes Durable batches in a work directory, verifies the result, and renames it.
Deletes already applied are not imported; bytes, text, i64, f64, and vector f32 retain their type.
Sequence, timestamp, compression, and 0.1 shard layout have no equivalent and are regenerated.
The source is rechecked during the copy and must be offline.

The writer supports logical format 1; unknown future formats are rejected.
Until a specific migration exists, backup/restore and copy-and-verify are the only upgrade/rollback plan.

### PostgreSQL validation import (public beta)

`aprodb-pg-import` is a bounded, one-way validation and migration tool for creating a new AProDB
directory from PostgreSQL base-table rows. The companion
`scripts/import_postgres_over_ssh.ps1` wrapper runs the exporter in a read-only,
repeatable-read transaction and streams JSONL to the local importer. Build a release binary for
large trials and start with `-RowLimit` on a heterogeneous sample before approving a full copy.

The stream contains explicit manifest, table, row, end-of-table, and completion frames. Exact JSON
numbers are not converted through floating point. A table maps to one AProDB collection; primary-key
identity is hashed with BLAKE3, while a table without a primary key uses snapshot `tableoid` plus
`ctid` and is therefore suitable only for a one-time copy. The importer uses 16 partitions, bounded
batches, and Durable writes by default. Canonical records and logical change events are committed
atomically without duplicating the full payload in the event by default.

The destination remains in a sibling `.importing-*` directory until the complete stream has been
accepted, the logical database has been verified, the engine has been reopened, and verification has
passed again. Publication is then a same-volume directory rename. Truncated input, inconsistent
counts, an existing destination, budget exhaustion, failed verification, or failed reopen return an
error without publishing the partial database. Interrupted staging data is retained for diagnosis;
the beta importer cannot resume it and a retry must use a new destination.

The completion summary reports tables, rows, logical bytes, committed batches, import duration,
verified heads/events, and reopened heads/events. Duration is operational evidence, not a benchmark
unless the binary, host, storage, source, and competing load are controlled and recorded. Default
limits are a 17 MiB input frame, 32 MiB of buffered batches, 64 GiB of database data, 16 GiB of
temporary compaction space, and an 8 GiB free-space reserve. Increase them only after checking the
destination filesystem and the load imposed by a long source snapshot.

The importer does not reproduce SQL indexes, constraints, triggers, views, sequences, permissions,
or query semantics, and it is not change data capture. See
[docs/postgresql-import.md](docs/postgresql-import.md) for the command, mapping, safety procedure,
and the complete list of limitations.

### Operational gates and limits

The long test `operability_long` is ignored in the quick suite because it performs 2,048 encrypted
Durable writes, four backup/restore cycles, and one rekey operation.
It should be run using:

```powershell
cargo test -p aprodb-engine --no-default-features --test operability_long -- `
  --ignored --exact repeated_encrypted_backup_restore_and_rekey_remain_consistent
cargo package -p aprodb-types --allow-dirty
cargo package --workspace --allow-dirty --no-verify
```

The single `aprodb-types` package verification passes, and the full workspace can be packaged with
`--no-verify`. Full workspace package verification currently fails because unpublished internal
path-and-version dependencies are resolved through crates.io; this does not affect GitHub publication.

No KMS, online restore, automatic canonical repair, fine-grained RBAC for collections, remote audit, metrics exporter, or replication are available. TLS, encryption, and backup are experimental application mechanisms and require periodic restore procedures, ACL management, and external key storage. Replication for Milestone 8 remains outside the initial implementation.
