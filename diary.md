# AProDB technical diary

This file records the main decisions and how the engine parts cooperate. It is not a changelog of every line: it describes the procedures that matter to understand, verify, and evolve the database.

## 2026-08-18 — Project start

The workspace was empty. A new Rust crate was created called **AProDB**, short for *Adaptive Parallel Object Database*. The goal of the first version is a truly compilable and persistent MVP, not an immediate substitute for PostgreSQL or Redis in production.

### Chosen architecture

- The active dataset lives in RAM and is divided into a number of shards equal to a power of two.
- `xxh3_64` associates each key with a shard; each shard has its own `RwLock<HashMap<...>>`.
- Reads on different shards do not block each other. Batch operations and scans use Rayon.
- Each modification is first serialized in the WAL and then applied to the shard. The WAL uses frames with magic number, length, and CRC32.
- Each record has an increasing sequence number. The shard only accepts the latest version, so two concurrent writes on the same key cannot revive a previous version.
- Deletions are temporarily retained as tombstones. This is necessary to correctly order concurrent put and delete operations.
- The snapshot acquires an exclusive gate on writes, saves only the live keys, and then removes the tombstones from RAM.

### Central types and procedures

- `Value`: represents bytes, UTF-8 text, integers, floats, and `f32` vectors; features binary encoding and validation.
- `Record`: contains sequence, key, and `Put`/`Delete` operation.
- `Wal::open`: reads valid frames, recovers records, and safely truncates only an incomplete tail.
- `Wal::append_batch`: records an entire batch and applies the durability policy only once.
- `Database::put_batch`: validates, assigns sequences, persists, and applies in parallel.
- `Database::vector_search`: collects in parallel the vectors of the required size, chooses CPU or GPU, computes the scores, and returns the top results.
- `GpuExecutor`: maintains device, queue, and compute pipeline; the WGSL shader executes dot product or cosine similarity with one work item per vector.

### Important rule on GPU usage

A GPU does not accelerate the lookup of a single key: the cost of transferring and synchronizing data would outweigh the computation cost. AProDB uses the GPU for batch vector searches and, in `auto` mode, waits for a configurable threshold; for small loads, it uses Rayon on the CPU.

### State at the end of the initial phase

At this stage, value format, records with checksum, WAL, snapshot, sharded engine, batch API, and CPU/GPU path were present. CLI, integration testing, benchmarking, and full verification were added in subsequent phases.

## 2026-08-18 — Integrated per-channel compression

A compressed storage layer has been added between `Value` and `Record`. The practical choice is **Zstandard level 1**: there is no universally optimal algorithm for every possible input, but Zstd combines a good compression ratio with fast encoding/decoding and a stable format. The low level prioritizes database latency.

### Ingestion procedure

1. `Database::put_batch` validates all values.
2. Rayon distributes the values to workers.
3. The key hash selects one of the `CompressionChannel` channels.
4. The channel encodes the `Value` and tries Zstd when the payload exceeds the configured threshold.
5. If the result does not save at least eight bytes, the raw payload is kept. In this way, small, random, or already compressed data do not expand unnecessarily.
6. `StoredValue` records format version, codec, logical type, original length, and payload.
7. The same `StoredValue` is written to the WAL and inserted into RAM; it is not compressed twice.

### Read path

`get`, batch get, prefix scan, and vector search clone the cheap reference `Arc<[u8]>`, release the shard's lock, and use the channel determined by the key. The decoder checks the length and type after decompression before rebuilding `Value`. Decompression errors are treated as data corruption, not as missing values.

### Why multiple channels

A single Zstd context protected by a mutex would be a bottleneck. AProDB creates a configurable and limited number of compressor/decompressor pairs, normally a power of two close to the core count and at most 32. This allows simultaneous work without multiplying contexts for all the shards, which could consume too much memory. Different keys are deterministically distributed across the channels.

### Observability

`Database::stats` includes compressed/raw values, logical bytes, bytes actually stored, compression ratio, and number of channels. The ratio measures the internal value format and excludes WAL frame overhead.

### CPU/GPU tuning

The release benchmark on Intel Iris Xe, using 50,000 vectors × 64 components, measured CPU 71.77 ms, cold
GPU 534.40 ms and hot GPU 98.98 ms.
The old threshold based only on the number of vectors would have chosen the slower GPU.
`Auto` now uses the total `number of vectors × dimension` work, with a default threshold of 16,777,216
components.
The GPU can still be forced; the threshold is configurable and must be calibrated on real hardware.

### Toolchain verification

The first Windows GNU build stopped before project compilation because `dlltool.exe` was missing.
WinLibs UCRT was installed via `winget`; afterwards, formatting, all-target compilation, Clippy, CPU suite, GPU suite, and CPU-only build all completed correctly.

## 2026-08-18 — CLI, tests, and measurements

The CLI was completed with `put`, `get`, `delete`, `scan`, `vector-search`, `stats`, `snapshot`,
`gpu-info` and `demo`.
A smoke test wrote highly compressible text, reopened the database between CLI processes, verified
the round trip, ran a search on Intel Iris Xe, and observed `compression_ratio = 0.0119` for
that synthetic payload.

The automated suite covers value encoding, adaptive compression, persistence, delete, batch, snapshot, repair of an incomplete WAL tail, concurrency, and CPU ranking.
The separate GPU test compares top-20 CPU/GPU on 512 vectors and passed on Intel Iris Xe.

Clippy required explicitly calling `.truncate(false)` when opening the WAL.
This change documents that opening must preserve the existing history; it does not change the intention of the code.

### Final outcome of the verification

- `cargo fmt --check`: passed;
- `cargo clippy --all-targets --all-features -- -D warnings`: passed;
- all-features suite: 7 tests passed, with the separate GPU test excluded as expected;
- forced GPU test on Intel Iris Xe: passed;
- `--no-default-features` suite: 7 tests passed;
- multiprocess CLI smoke test: passed;
- release benchmark: approximately 61,120 inserts/s; CPU 71.77 ms; cold GPU 534.40 ms; hot GPU
  98.98 ms on 50,000 × 64.

The database is explicitly a single-process MVP. The main future activities are cross-process locking, WAL compaction/rotation, crash testing with fault injection, decoder fuzzing, multi-key transactions, and an optional network protocol.

## 2026-08-19 — Multi-database comparative harness

The independent crate `benchmarks/comparative` has been added. It remains separate from the engine's `Cargo.toml`, so SQL drivers do not become dependencies of AProDB. The runner uses the same deterministic sequence of keys and payloads for AProDB, SQLite, PostgreSQL, MySQL, and MariaDB, and saves an incremental `report.json` after each test.

Loads have two profiles: `compressible` simulates repetitive fields in documents and logs, while `random` generates high-entropy pseudo-random bytes. This distinction serves to measure both the benefit and the upper bound cost of adaptive compression, without presenting a single dataset favorable to Zstd.

Each test includes batch ingest, hot dataset random lookup, ordered range/prefix scan, and physical space measurement. Commits occur once per batch with active durability: `SyncData` for AProDB, `synchronous=FULL` and WAL for SQLite, and durable server settings for SQL databases. Latency p50/p95/p99 is collected with an HDR histogram. The runner checks the length and number of records during measurements; a correctness error invalidates the individual test and is recorded in the report.

The comparison notes an essential architectural limitation: AProDB and SQLite are embedded in the same benchmark process, while PostgreSQL, MySQL, and MariaDB receive queries via a TCP connection on the loopback. The measurements therefore describe the current experience of each API and do not isolate the indexing algorithm alone. The SQL measurement also covers table and index storage but does not include global server WAL or redo files.

### Laboratory execution

Official portable distributions were used: PostgreSQL 18.6, MySQL Community 26.7.0, and MariaDB 12.3.2. The published hashes of MySQL and MariaDB were identical; for the PostgreSQL archive, the local SHA-256 `fbe23da234ee31547bf8a36d29dfd81e82b849df2d2b78d2eecb43d360252f8c` was recorded. Clusters were created under `target/bench-lab`, bound to `127.0.0.1` on ports 55432, 53306, and 53307, without permanent Windows services.

PostgreSQL was run with `fsync=on`, `synchronous_commit=on`, and `full_page_writes=on`; MySQL and MariaDB with `innodb_flush_log_at_trx_commit=1`. The binlog was disabled because the test did not measure replication. AProDB used `SyncData`; SQLite used WAL and `synchronous=FULL`. At the end of the test, the three servers were shut down in an orderly fashion and the ports were verified as closed.

The first attempt to download MariaDB via the REST redirect produced an empty file: it was discarded before extraction and replaced with the official archive, then verified via SHA-256. The first PostgreSQL readiness check omitted `-U postgres`, so the server rejected the non-existent Windows role `andre`; the logs identified the problem and the correct check confirmed that the server was already healthy.

### Result

The 30 final release-build trials completed without error.
On compressible payloads, AProDB medians are 43,215 ingest/s, 161,677 lookup/s, p99 10.8 µs,
400.4 scan/s and 6.76 MiB of physical space.
On random payloads: 28,091 ingest/s, 395,376 lookup/s, p99 3.1 µs, 387.1 scan/s and 27.32 MiB.

Internal compression reduces 24.46 MiB of logical data to 4.28 MiB on repetitive data; on random data it
recognizes that Zstd would worsen the result and retains all raw values.
The result also sets out two technical priorities: an index ordered for prefix scans and
optimizations of the compressed ingest.
The complete tables and experimental limits are in `benchmarks/comparative/RESULTS.md`.

## 2026-08-19 — Stabilization of the radial architecture

The brainstorming phase was consolidated into **paper.md**, a normative specification distinct from the 0.1 manual. The document defines the target database without presenting as implemented any features still under design.

The main decisions are:

- central server with exclusive directory ownership and embedded single-process mode;
- durable canonical record, with cache, indexes, projections, and VRAM always rebuildable;
- complete CPU reference and optional GPU selected by a cost model;
- single logical writer per shard and atomicity within each partition;
- at-least-once workflow with idempotency, leases, and fencing tokens;
- work surface separated from the read surface;
- radial cache with distinct budgets and a score based on freshness, access, urgency, readiness, cost, and size;
- initial proposal for segmented WAL, manifests, and native segments, later replaced by the backend contract documented in revision 1.2;
- per-block compression with Zstandard, versioned dictionaries, and Raw decision for data where compression is not practical;
- replica designed as a separate phase, not promised by the first server.

The paper includes reading and writing pipelines, CPU/RAM/storage cache, adaptation for NVMe/SSD/HDD, protocol, security, observability, recovery, backup, failure matrix, testing, benchmarking, and milestones 0–8.
Primary sources consulted were: Intel, NVIDIA, Apache Arrow, Zstandard, RocksDB, Redis, PostgreSQL, NVM Express, and the Raft paper.

**implementation-prompt.md** was created for the future clean session. The prompt instructs to read the specification in full, proceed by verifiable verticals, keep CPU-only as a gate, update this diary during work, and describe in the manual only truly completed capabilities.

No Rust files have been modified at this stage. The folder was not yet a Git repository; the prompt treats initialization as Milestone 0 activity and prohibits commits or unsolicited publication.

## 2026-08-19 — External review and specification 1.2

A review by Claude correctly highlighted the risk of reimplementing an entire storage engine before validating the distinctive functions of AProDB. Four corrections were accepted:

- backend contract instead of the obligation to write WAL, segments, and compaction immediately;
- Milestone 0.5 with a brief spike on Fjall and redb/RocksDB as fallback;
- single Durable mode with configurable group commit window;
- initial radial score reduced to freshness, workflow/urgency, and pin.

The GPU has not been postponed: Milestone 6 remains unchanged and its interfaces and representations must be provided by the foundations.

The first amendment to the paper delegated storage, but left §§17–18 as a mandatory physical format. Version 1.2 resolves the inconsistency by distinguishing:

1. WAL, memtable, segments, manifest, and private compaction of the built-in backend;
2. AProDB catalog and logical change log, written atomically with the record;
3. The WAL/segments format specific to AProDB is only applicable to a possible native backend.

The backend change is not presented as transparent: capabilities, transactions, snapshots, iterations, backup, and compaction may differ. Each change requires an ADR and verified export/import or migration.

**implementation-prompt.md** was updated: removed the obsolete reference to Strict/Group, added Milestone 0.5, clarified that the change log is not a second WAL, and prohibited physical reimplementation without supporting evidence. **manual.md** now summarizes the same target architecture.

No Rust files have been modified.

## 2026-08-19 — Minimal change log and coordinated compression, specification 1.3

Claude's second review approved the separation between the physical backend and AProDB logic and highlighted two costs to measure: write amplification of the change log and possible double compression.

The paper has moved to version 1.3 with these decisions:

- a change event contains key, version, sequence, metadata, and a minimal delta or payload reference;
- the full payload is not duplicated by default;
- a reference must indicate an immutable version retained up to the watermark of the required consumers;
- the spike measures event bytes/payload bytes, Durable latency, throughput, storage, compaction, and rebuild;
- AProDB logical compression and the backend's physical compression are coordinated per keyspace;
- the matrix compares only Zstandard in AProDB, only backend compression, both, and no compression;
- the ADR chooses separately for canonical payloads, catalog/change log, surfaces, blobs, and indexes.

**implementation-prompt.md** now contains the same requirements and prevents mistakenly reading the current version when the event refers to a previous version. **manual.md** keeps the available version 0.1 distinct from the target architecture 1.3.

No Rust files have been modified.

## 2026-08-19 — Event retention, specification 1.4

The third revision made explicit a choice that version 1.3 left implicit: an LSM backend can delete old versions during compaction, while MVCC snapshots are suitable for short reads and can retain storage if kept for a long time.

Each collection now declares an EventRetentionMode:

- **Delta:** self-sufficient event for projections;
- **VersionRef:** head and event refer to a single immutable copy identified by key/version or content hash;
- **SelfContained:** payload included only with explicit limits and policy.

Backend snapshots are not durable retention. The Milestone 0.5 spike must test slow consumers, multiple updates, compaction, restart, and garbage collection for all three modes. Invariants require the consumer to recover the exact version or delta even after compaction and restart.

The paper has moved to version 1.4; the prompt and manual have been aligned. No Rust file has been modified.

## 2026-08-19 — Preparation of the GitHub publication

The decision to host AProDB on GitHub was approved, while local preparation remained separate from remote creation. At that checkpoint, the read-only review verified:

- Local Git 2.54 installed, with name and email configured;
- AProDB directory not yet initialized as a repository;
- Integrated GitHub connection authenticated as `andreaprovenzali`, with admin and push permissions on visible repositories, but not intended for AProDB;
- Chrome authenticated on the correct `provenzali` account, with accessible repository creation page and selectable owner;
- GitHub CLI `gh` absent, but not necessary because authorized creation can occur from the browser;
- no matches for tokens, private keys, or strong credentials in the candidate files;
- no files of at least 10 MiB outside build directories.

At that checkpoint, no `git init`, commit, repository creation, or push had been performed. The approved owner was `provenzali`; the repository name, visibility, and the MIT license then stated in the manifest were still awaiting a decision. Later entries record the completed publication and the final AGPL-3.0/Apache-2.0 licensing boundary. The account email is not saved in documents intended for the repository.

**implementation-prompt.md** now includes a GitHub phase for Milestone 0: file audit, secret and size check, license, `main` branch, authorized baseline commit, confirmed creation of the repository only, and CPU-only CI. The `.gitignore` has been strengthened for nested crates, runtime data, local databases, sensitive configurations, and diagnostic outputs. Cargo.lock must remain version-controlled.

Final checks confirmed zero matches for strong credentials, `cargo fmt --all --check` passed, 7 CPU-only tests passed, and Clippy CPU-only without warnings. `git check-ignore` must be repeated after `git init`, because the command requires a Git worktree; the rules have still been inspected and cover the listed artifacts.

No Rust file has been modified.

## 2026-08-19 — Start of implementation and Milestone 0 baseline

The implementation session fully read the specification, manual, diary, manifests, sources, tests, and comparative benchmark documentation before modifying the project.
There are no additional `AGENTS.md` files in the workspace beyond the session instructions.

### Initial checks

- Git 2.54.0 is available and the local repository has been initialized on the `main` branch without commit, staging, or remote.
- Chrome was rechecked in read-only mode: the active account was `provenzali`, and the repository creation page was accessible with that owner. No resource had been created at that checkpoint.
- `gh` is not installed and the GitHub connector on the other account has not been used.
- Scanning the 28 candidate files outside `target` did not find strong credential patterns, sensitive files, or files of at least 10 MiB.
- `target/bench-lab`, the benchmark's nested target, and other large artifacts were detected and preserved.
- The ignore rules were tested on nested builds, data, WAL, snapshots, databases, `.env`, keys, and logs; `Cargo.lock` remains version-controlled.

The distribution will include specification, manual, diary, locally declared comparative results, sources,
tests, benchmarks, ADR, and requirements matrix.
The handoff prompt remains local because it contains paths and machine-specific state.
MIT text consistent with the then-current `Cargo.toml` was added; license confirmation, repository
name, and visibility still had to be resolved before remote creation. ADR-0005 and the later
publication entry supersede this provisional MIT state with the final AGPL-3.0/Apache-2.0 boundary.

### Baseline gate

Using Rust stable 1.97.1, formatting, Clippy with warnings denied, and tests all passed both CPU-only and with default features.
The GPU test ignored from the prototype was not forced and no external servers were started.
The requirements–milestone–test matrix is in `docs/requirements-matrix.md`; distribution decisions
are in `docs/repository-baseline.md`.

## 2026-08-19 — Vertical Milestones 0, 0.5, and 1

### Objective

Build the first verifiable 1.x path without replacing the prototype in a monolithic way: stable types and formats, backend chosen via spike, atomic record/change log/catalog mutation, recovery, and limits.

### Implementation

The workspace now contains `aprodb-types`, `aprodb-storage`, `aprodb-compute`, and `aprodb-engine`; the `aprodb` crate maintains 0.1 compatibility and exposes the new path under `aprodb::v1`. The graph does not introduce GPU dependencies in the CPU-only path. The Record, Head, Change, and Catalog frames have magic, version, length, and CRC32, with golden tests, property tests, and fuzz targets.

Fjall 3.1.8 has been isolated behind a contract with explicit capabilities. The directory has format marker and exclusive lock in-process/cross-process. Records, Versions, Events, Catalog, and Idempotency are distinct keyspaces; mutation uses a single `OwnedWriteBatch`. AProDB does not interpret journal, manifest, SST, or physical compaction. The 0.1 format is covered by golden tests and is rejected by the new engine; automatic import is not implemented.

The engine implements Put, Get, Delete, CAS, and AtomicBatch within a partition, a single logical writer per shard, a versioned catalog, receipts, Durable/Relaxed modes, and bounded group commit. VersionRef preserves and reads the exact immutable version; Delta must be self-sufficient; SelfContained has an explicit policy and limit. Required-consumer watermarks and GC control retention without long-lived MVCC snapshots. Verify, sync, paginated logical checkpoints, stats, and major compaction with a timeout are available.

A test showed that Fjall's `rotate_memtable_and_wait` may not wait for a flush already queued after auto-rotation. The wrapper therefore checks the flush queue and write buffer before compaction. The risk of an upstream error in a partial batch journal led to a fail-closed latch: any commit or persist error stops new writes until reopening.

### Spike and decision

The spike performed eight durable workloads with 4,000 mutations each, compaction and restart.
Of 4,096,000 compressible bytes, adaptive Zstandard produced 135,677 bytes; on random data it
retained Raw.
The minimum VersionRef event costs 92,000 bytes (2.246% of the logical payload), compared to 64,000
for the synthetic delta and up to 4,208,000 for SelfContained Raw.
Latency, throughput, space, process I/O, and measurement limits are detailed in `benchmarks/storage-spike`.

ADR-0001 accepts Fjall for the experimental vertical with exact pin, physical LZ4, logical checkpoint, and mandatory review before Milestone 7. Redb and RocksDB remain fallback options, not automatic parallel spikes.

### Checks and limits

Formatting, Clippy with warnings denied, and all workspace tests passed both CPU-only and with default features. The suite includes golden/proptest, lock subprocess, immediate kill after Durable ACK, reopen, compaction, checkpoint, fail-closed fault injection, and the 300-version slow consumer for each mode through compaction, restart, watermark, and GC. The GPU 0.1 test remains ignored with justification because it requires an adapter; CPU correctness passes.

The fuzz target shares the same exercise function: on non-Windows it uses LibFuzzer, while on Windows-GNU it compiles and runs a corpus smoke test because `libfuzzer-sys` requires an unavailable MSVC/Clang toolchain. Windows check and smoke tests pass; the CPU-only Linux CI will compile the LibFuzzer entrypoint after the authorized publication of the repository.

Narrowly scoped physical-corruption testing on disposable copies remains unresolved, linked to Fjall risk #311, as do import 0.1, server, idempotency keys, and all functions from Milestones 2–7. The logical checkpoint is not yet an operational online backup.

## 2026-08-19 — Milestone 2, multiprocess server

### Objective and decisions

The recommended approach for multiple processes was added without sharing the data directory. The daemon remains the only engine owner; protocol, client, and server are separate crates, and the protocol does not depend on the server. Protobuf uses Prost types maintained in the repository and a canonical `.proto` schema, avoiding a build dependency on `protoc` on the user machine.

The data and admin endpoints are also separated for handshake purposes and require distinct tokens. Comparison is constant-time and token debug output is redacted. TCP plaintext is limited to loopback unless explicitly overridden; TLS remains a Milestone 7 gate and is not simulated.

### Implementation and invariants

`aprodb-proto` defines magic, major/minor version, request id, UTC deadline, durability, record/versions/receipt, batch, and administration. Frames and batches have limits before decoding or execution. Goldens are available for handshake, Put, and Durable response, with property tests and fuzzing for the storage and protocol decoders.

`aprodb-server` offers TCP, Windows named pipes, and Unix domain sockets, multiple in-flight requests, out-of-order responses, bounded queues, and semaphores for connection and global limits. Expired deadlines are rejected before admission. Backpressure includes a configurable retry-after; no unbounded queue is created. Blocking engine operations remain outside the async runtime. Remote shutdown or Ctrl+C stops accepting new connections, completes admitted requests, and drains responses.

`aprodb-client` offers a multiplexed async API and blocking wrapper for Put, Get, Delete, CAS, AtomicBatch, Sync, and Administration. The monotonic deadline covers both client-side queue wait and response; the transmitted UTC deadline protects server admission. Automatic retries are postponed until Milestone 4 provides persistent idempotency keys.

The executables `aprodb-server` and `aprodb-cli` read tokens only from environment variables. The administrative CLI implements Health, Stats, Verify, Compact, and Shutdown. Frames, connections, in-flight requests, queues, timeouts, and retry hints are configurable; invalid values are rejected at startup.

### Verification and limits

Clippy with warnings denied and the targeted test suites of the four network crates pass.
The five end-to-end server tests cover invalid token, role separation, Put/Get/CAS/Delete/AtomicBatch, concurrent requests, expired deadline, frame limit, deterministic backpressure with retry-after, metrics, verification, named pipe, and shutdown.
A multiprocess smoke test started the server binary, queried Health through a second CLI process, and verified termination after Shutdown.

The following remain outside Milestone 2: retry/idempotency, tenant quotas, persistent audit, TLS, at-rest encryption, exported metrics, and client bindings for languages other than Rust.
Unix transport is covered by code and conditional tests, but in this Windows session only the named pipe test was run; Linux CI remains to be executed after publication is authorized.

After code, golden, and documentation alignment, `cargo fmt --all --check`, Clippy workspace/all-targets with warnings denied, and workspace tests both with `--no-default-features` and with default features all passed.
The GPU 0.1 test remains the only one ignored, with hardware reasons already documented. The updated fuzz target passes check and Windows corpus smoke. The multiprocess smoke test was repeated on the final binaries and passed.

## 2026-08-19 — Milestone 3, radial engine and storage capacity

### Objective and decisions

The vertical implementation makes the canonical dataset independent of the available RAM without reimplementing WAL, manifest, segments, Bloom, flush or Fjall's compaction.
Physical capabilities not exposed by the backend remain declared absent: in particular, storage classes are logical on a single device and any alternative path is rejected, rather than simulating non-existent tiering.

The server detects physical memory and cgroup limits via `sysinfo` 0.39.6 with only the `system` feature.
It uses half the detected ceiling unless overridden, and applies the minimum of configuration, physical memory, and container limits.
The minimum budget is 128 MiB; the storage cache, seven memtables, in-flight allocations, and AProDB caches are validated as a single limited reserve.

### Implementation and invariants

Separate metadata, object, and negative caches have been added, each with 16 shards and a byte budget. Object admission weighs frequency, radial score, size, and pin; scans and maintenance do not populate the point-lookup working set. Metrics distinguish hits, misses, admissions, rejections, and evictions. The `ExplainPlacement` query checks residency without changing frequencies or statistics.

Policies for collections, storage classes, and radial descriptors are versioned and persistent logical formats. Each canonical mutation atomically updates the descriptor and any TTL index in the same batch as the record, head, event, and catalog. The score uses freshness and urgency, with separate thresholds, minimum retention, and expiring pins. `ExplainPlacement` also reports version, layer, class, physical capacity, and reasons; protocol, client, server, and CLI expose the same operation.

Reads hide an expired record even before cleanup. The TTL sweep is bounded and uses identity/version as fencing: a stale index does not remove a subsequent Put. The admin can run `expire`; there is not yet a scheduled task. Expiry of a `Delta` collection explicitly fails until a self-sufficient delta is available, whereas the default `VersionRef` mode retains the reference to the exact version.

### Verification, costs and limits

Golden and fuzz tests include `RadialDescriptor`, radial state, and TTL index. The tests cover minimum budget, independent caches, eviction, negative invalidation, TTL/update/reopen, policy/pin/storage class, compaction, and `ExplainPlacement` without effects on the object cache. Server integration verifies placement, cache metrics, and expiration; multiprocess smoke testing on the final binaries ran Health, CacheStats, Expire, and Shutdown with a 128 MiB budget.

The dedicated capacity gate wrote 129 MiB of pseudorandom data with a 128 MiB engine budget, then performed sync, compaction, reopen, verification, and exact reads: it passed in about 81 seconds. It remains `ignored` in the ordinary suite because it is intentionally resource-intensive and is explicitly invoked for the M3 gate. It does not claim to saturate all physical RAM and is not a performance benchmark.

After the only Clippy correction to a constant assertion, formatting, workspace/all-targets Clippy with warnings denied, and workspace tests all passed both CPU-only and with default features. The capacity test is the only one ignored for CPU-only; with default features, the previously addressed GPU 0.1 test also remains ignored. The fuzz-target check and Windows corpus smoke test pass.

Physical tiering, storage-media detection, I/O priorities, automatic TTL sweep, tenant quotas, and a test that genuinely exceeds physical RAM remain open. These limitations do not prevent disk-backed data from exceeding the engine budget, but preclude declaring SLAs or unmeasured physical control.

## 2026-08-19 — Milestone 4, workflow and surfaces

### Objective and decisions

The vertical adds generic worker primitives and reconstructable projections without inserting business logic into the engine. Semantics are at-least-once with persistent idempotency and fencing; external effects are not declared exactly-once. Work surface and read surface use the same limited builder but remain distinct types in the catalog. The publication of a generation is forced Durable even when the caller requests Relaxed: advancing a non-persisted watermark could authorize the GC to lose the necessary source.

### Implementation and invariants

Put/Delete/AtomicBatch and workflow operations accept a 32-byte idempotency hash. Scope, fingerprint, receipt, and expiry are versioned logical formats; the idempotency record enters the same mutation batch. Identical replay returns the original receipt and lease even after reopen, while the same hash with different parameters fails. A time index supports bounded purging; there is no automatic sweep server yet.

Append creates `pending`; Claim indexes by scope/state/deadline and assigns a random lease, deadline, and increasing fencing under the shard writer. Heartbeat, Complete, and Fail check current lease and fencing. Fail returns to `pending` or transitions to `dead_letter`; Publish requires `completed`. Monotonic time is valid within the process, while restart uses persisted UTC and a safety margin. The number of claims, duration, and active leases have configurable limits.

`SubscribeChanges` exposes filtered pages with the global shard watermark, preserves AtomicBatch, and signals `ChangeLogGap`. The surfaces persist definition, pointer, generation, and payload in dedicated keyspaces. The incremental builder applies the exact VersionRef or SelfContained, rejects a generic Delta, publishes generation/pointer/catalog/watermark in the same batch, and retains a limited number of generations. Rebuild acquires the writers, snapshots the watermarks, and scans the canonical state without long-lived MVCC snapshots. Reading reports watermark, staleness in sequence, completeness, and errors.

The canonical protocol, async/blocking client, server, and CLI were extended with the same operations. Logical golden files cover workflow, idempotency, and surfaces; wire golden files add Claim and surface response. The fuzz target also decodes all new frames. Two examples in the same Cargo package were renamed to eliminate the output collision reported by the all-targets gate.

### Checks and limitations

Engine tests cover replay, restart, and idempotency expiry, the complete state machine, stale fencing, concurrent claims synchronized with barrier, incremental work/read surfaces, and rebuild after GC/gap.
The TCP test passes through Append, replay, change stream, Claim/Heartbeat/Fail/Complete/Publish, surfaces, and Verify with separation of data/admin.
The first run correctly showed that `server_time` changes between replay Claims: the contract and test retain the exact record, lease, deadline, and receipt, treating server time as response metadata.
Targeted test suites and Clippy with warnings denied passed; the final workspace gates are recorded after the documentation alignment.

The current surface definition supports a source, filtering by state, ordering by identity, and record/JSON output.
Time windows, generic indexes, transformations, Arrow/Protobuf, dependencies, a periodic scheduler, rollback, and aggregate metrics remain open and are not described as available.
Automatic client retries, TTL/idempotency sweeps, and tenant quotas also remain absent.

After documentation and final formats, `cargo fmt --all --check`, Clippy workspace/all-targets with warnings denied, and workspace tests both CPU-only and with default features passed.
The first combined default command reached the external timeout after Clippy and during doc-tests without showing errors; the hot cached workspace test passed completely.
Only the capacity gate M3 and GPU 0.1 test, both with rationale, remain ignored.
The updated fuzz target compiles and the corpus smoke Windows passes.

## 2026-08-19 — Milestone 5, logical compression and dictionaries

### Objective and decisions

The 1.x canonical payload has moved from the entirely raw `APRC` record frame to an `APRX` envelope that only compresses the serialized `Payload` and keeps identity, metadata, workflow, and version directly verifiable.
`APRC` remains readable for compatibility with already produced experimental directories.
The policy is Raw for Surface and adaptive Raw/Zstandard for hot, warm, cold, and archive; a list by content-type avoids work on formats known to be pre-compressed.

The Fjall default for the canonical keyspace has been set to no physical compression, while metadata, change logs, and surfaces retain LZ4.
Double compression remains configurable and measured, but is not the default.
ADR-0002 records the separation and updates the previous Fjall decision.

### Implementation and invariants

`StoredPayload` carries codec version, logical length, CRC32, bytes, and optional dictionary id. The decoder always checks length and checksum and loads the exact dictionary indicated by the version. Policies are versioned per collection in the new Compression keyspace and published Durable. Logical record size is checked before encoding; the SelfContained limit is recalculated after assigning the dictionary id to avoid underestimating the frame.

A power-of-two pool reuses Zstandard compressor/decompressor. The scratch area has a limited atomic reserve and produces backpressure before commit. The cache of compressed frames is independent from the decoded object cache and participates in budget allocation. Metrics distinguish bytes, codec, fallback, skip, timing, failure, channels, and scratch; they measure codec attempts even if a request fails later.

Dictionary training limits samples, total bytes, size, and number of dictionaries. A separate validation set is used, and publication is refused without a minimum gain. Updated dictionary and catalog are atomic and Durable; there is not yet any GC for dictionaries, which are retained conservatively to protect immutable versions. Protocol, async/blocking clients, server, and CLI expose metrics, policy reading/configuration, and training; dictionary bytes are not returned over the administrative wire.

### Formats, tests, and benchmarks

Golden files, property tests, and fuzz targets include `APRX`, catalog, and dictionary. Engine tests cover Zstandard/Raw selection, skip content-type, reopen and exact version, separate policy/cache, validated and missing dictionary, and backpressure on scratch without publication. The TCP test covers policy, metrics, compressed cache, and training through the central server. Formatting, workspace/all-targets Clippy with warnings denied, and workspace tests passed both CPU-only and with default features. Only the M3 capacity gate and the already justified GPU 0.1 test remain ignored. The updated fuzz target compiles and the corpus smoke test passes on Windows.

The new `benchmarks/compression` lab runs the four modes on compressible and pseudorandom
payloads with the same durability, then sync, compaction, verification and reopen.
In the local debug run, 1,049,600 bytes of compressible logical data became 6,655 bytes with Zstandard;
the 256 pseudorandom payloads remained Raw.
The recorded results include ratio, p50/p95/p99 for Durable batches, throughput, CPU, RSS, I/O, space, and
recovery.
Fjall preallocated 64 MiB, so the physical bytes from the small run should not be interpreted as
a production comparison; no competitive superiority is claimed.

### Open limits

Repeated release-mode tuning on large datasets, dictionary GC, an effective blob policy, and administrative rewriting of existing versions are still missing.
Experimental directories created with canonical LZ4 retain the previous physical option: the logical format remains compatible, but migration/tuning tooling is scheduled for Milestone 7.

## 2026-08-19 — Milestone 6, heterogeneous compute

### Objective and decisions

The 1.x vertical now exposes exact/top-k vector search without depending on the GPU.
The CPU path defines the reference semantics; wgpu is a server and facade feature, lazily initialized and removable with `--no-default-features`.
ADR-0003 records layout, cost model, VRAM cache, and fallback.
During the review, a stale-reuse risk was corrected: the maximum shard sequence is not a sufficient watermark.
The projection is now built under a barrier across all shards and uses the captured global generation, then releases the locks before compute.

### Implementation and invariants

`ColumnarF32Batch` retains contiguous f32 values, a u32 validity bitmap, and explicit layout.
`CpuPool` uses a dedicated Rayon pool.
The scheduler has a bounded channel and byte budget, compatible micro-batching, a timeout, a worker limit, a circuit breaker/cooldown, and CPU fallback.
`Auto` compares all cost components; the response includes estimate, actual backend, and fallback reason.

The WGSL backend calculates dot product/cosine, performs `map_async` readback, applies timeouts, and recreates device/pipeline after errors.
The LRU cache limits VRAM and indexes by projection id, source generation, and schema; hit, miss, eviction, upload, readback, transfer, kernel, and reset are metrics.
Storage, catalog, and server are independent of the device.
Protocol/client/server add VectorSearch data and ComputeStats admin; the CLI exposes `compute-stats`.

### Tests and measurements

The deterministic tests cover layout/null handling, non-finite values, ties, cost selection, byte budgets, micro-batches, faults, fallback, and cooldown.
Engine integration verifies mixed collection, limit, and reopen; the TCP test checks data path and roles.
Protobuf golden files fix the encoding of the request and vector response.
The wgpu test on the Intel Iris Xe compares top-20 results with the CPU within `1e-4`, then checks hit, invalidation, miss, and VRAM rebuild.

The release benchmark in `benchmarks/compute` measured four shapes with nine samples.
The warm GPU outperformed the CPU on 8,192×64 and 65,536×64, but not on 1,024×64 or 65,536×256; cold
initialization reached 590 ms.
The crossover is not monotonic, so no universal threshold is set and no superiority is claimed.
The first release build exceeded two 120-second wrapper timeouts but continued in child
processes; after checking and waiting for those PIDs, the final binary was run directly and completed successfully.

### Open issues

ExactFlat briefly blocks mutations during scanning and does not replace ANN.
Portable wgpu does not expose a host-pinned pool; the implementation uses internal staging and batch/queue budget, with serialized device access.
The model is not self-tuning, there is no CUDA/HIP or other GPU operators, and no GPU result yet publishes a mutation.

The final M6 gates passed: formatting; workspace/all-targets Clippy with warnings denied for CPU-only and default features; CPU-only and default workspace tests; nine real GPU compute tests, including equivalence and cache; wire golden files; fuzz-target check; and the Windows corpus smoke test.
The CPU-only suite ignores only the M3 capacity gate.
The default suite also ignores the prototype 0.1 GPU test, while the new wgpu 1.x test is run and passes.
A first default run exceeded the timeout wrapper during build; the process terminated, and rerunning with a hot cache returned zero exit code for the entire workspace.

## 2026-08-19 — Milestone 7, operability and single-node security

### Objective and decisions

The final single-node vertical adds recoverable procedures without in-place upgrade.
ADR-0004 establishes copy-and-verify for backup/restore, repair, rekey, and import 0.1.
The mature primitives selected are XChaCha20-Poly1305 for values at rest, Rustls/Tokio-Rustls for TCP, and BLAKE3 for inventories and audit targets.
Key IDs are public; key material remains only in the external keyring and is redacted from `Debug`.

The server records `Attempted` before the administrative mutations and the outcome after dispatch. If the initial event does not become Durable, the operation does not start. Tenant quotas are applied before global permits and limit bytes, rate, in-flight work, and vector work. The engine checks the data quota, free-space reserve, and estimated temporary space before writes, compaction, checkpoints, and restore cycles.

### Implementation and invariants

`EncryptedBackend` protects the values of all twelve keyspaces with a random nonce and AAD on version, keyspace, key id, and storage key. The marker prevents silent opening with a different configuration. Checkpoints also include compression and audit; backup reopens and verifies the checkpoint, inventories its files, and publishes the manifest only at the end. Restore recopies with `create_new`, recalculates the hash, and rechecks the catalog generation/watermark. Rekey creates a new copy and reopens it with the new key.

`verify` pages through the entire space instead of stopping at the first page limit and checks records/versions/events, radial state, TTL, workflow, surfaces, dictionaries, and audit. Repair reconstructs only derived indexes on a copy with exact confirmation and produces a serializable report; it does not attempt implicit canonical recovery.

Protocol, client, and CLI expose AuditList and online backup under a `backup_root`; the name cannot traverse directories. The server loads TLS, mTLS, keyring, and quotas from size-limited files. `aprodb-ops` provides verify-backup, restore, verify, repair, rekey, and offline import with JSON output.

The 1.x engine always refuses to open a 0.1 directory directly. The importer copies the snapshot and WAL into `raw`, verifies BLAKE3 before and after, creates a second reader copy because the historical reader can repair a truncated WAL tail, exports within limits, and maps the five types into Durable batches in one partition. The 1.x database is created in a work directory, passes `verify`, and is renamed; historical deletes do not reappear, and the source remains byte-identical.

### Tests, formats, and distribution

Logical golden files add `APAU`/`APAS`; Protobuf golden files add the request and audit response. The fuzz target decodes both audit frames. Targeted tests have passed for encryption/tamper/wrong key, backup/restore, repair, rekey, audit/restart, tenant/disk quotas, server backup, TLS/mTLS, redacted keyring, and snapshot+WAL 0.1 migration. The entire CPU-only `server_integration` test has passed, as have the engine library tests and the types/protocol golden tests.

`operability_long` is intentionally ignored in the fast suite because it performs 2,048 encrypted
Durable writes, four backup/restore cycles, and a rekey operation.
It was explicitly executed in this session and completed successfully in about 132 seconds.
The reason and command are in the manual and in the manual CI workflow.
`cargo package -p aprodb-types --allow-dirty` verifies the independently packageable base crate.
`cargo package --workspace --allow-dirty --no-verify` creates all workspace archives, but full
workspace verification currently fails because the unpublished interdependent crates are resolved
through crates.io. This package-verification limitation does not affect GitHub publication.

### Open limitations

Application encryption does not hide physical key names and does not replace volume encryption. KMS, online restore, collection-level RBAC, remote audit, a metrics exporter, and automatic canonical repair are missing. Rate quotas use fixed windows in memory. An interrupted offline operation keeps the partial copy for diagnosis and requires a new destination. The logical-format writer supports v1; future formats are rejected until a copy-only migration exists.

Milestone 8 replication remains deliberately out of scope. At the close of this milestone, remote creation, commit, push, and final licensing still required explicit user authorization; the later publication entries record their completion and the final license choices.

### Final gates for the single-node tranche

The final gate confirmed formatting, workspace/all-targets Clippy with warnings denied for both CPU-only and default features, workspace tests for both configurations, the explicit long operability suite, package verification for `aprodb-types`, creation of all workspace archives with `--no-verify`, and compilation of the fuzz crate. Full workspace package verification is blocked by unpublished internal crates. The final scan found no `unsafe` blocks, no publishable files of at least 10 MiB, no strong secret patterns, and no email addresses. `cargo` was not in the final shell's `PATH`; metadata checks were repeated using the explicit path to the stable toolchain and passed. The `cargo-fuzz` subcommand is not installed: the target was compiled, but no libFuzzer campaign was run as part of this gate.

## 2026-08-19 — License, attribution, and public beta preparation

The user formally approved the core under `AGPL-3.0-only` and the integration boundary (`aprodb-client`, `aprodb-proto`, `aprodb-types`) under `Apache-2.0`. No Apache alternative is offered for the core. The four shared compute types re-exported by the client were moved into `aprodb-types`, with compatible re-exports from the compute crate, eliminating the permissive client's dependency on the AGPL implementation.

Each source file bears a copyright notice and SPDX identifier; each crate contains the full text of its own license. `NOTICE`, `AUTHORS.md`, `CITATION.cff`, `LICENSING.md`, `TRADEMARKS.md`, `CONTRIBUTING.md`, `SECURITY.md`, and ADR-0005 document origin, boundary, DCO, and procedures for private disclosure. Andrea Provenzali is identified as the original creator and author of the specification, with ORCID `0009-0009-9677-9840`. No email address, tax ID, date of birth, or nationality is published.

README and the security policy declare AProDB in beta testing and not production-ready. At this checkpoint, `provenzali/aprodb` was still the candidate target; the later publication entry records the completed commit, remote creation, and push.

### EU AI Act verification and AI assistance

The source base has been reviewed with respect to Regulation (EU) 2024/1689 and the Commission guidelines applicable from 2 August 2026. AProDB does not incorporate models, chatbots, content generation, or runtime calls to AI services: exact vector search and GPU execution are deterministic computations. OpenAI Codex's assistance during development does not alter the product classification or the AGPL/Apache boundary; moreover, the Article 50 guidance excludes source code from the machine-readable marking of synthetic content.

For voluntary transparency, `AI_ASSISTANCE.md` and `docs/eu-ai-act-assessment.md` have been added. Andrea Provenzali retains direction, review, editorial responsibility, and attribution; Codex is not listed as author, copyright holder, or contributor. The evaluation should be reopened if models, inference, generative content, direct interaction with people, or regulated use cases are introduced. This is a technical project assessment, not legal advice.

The pre-publication gates following the modification of the licenses and the boundary of the types
passed `cargo fmt --all --check`, workspace/all-targets Clippy with warnings denied, and workspace
tests, both CPU-only and with default features.
The 129 MiB capacity gate, the long gate with 2,000 Durable writes, four backup/restore cycles,
and one rekey operation, and the GPU comparison on a real adapter remain separate from the ordinary
suite, with explicit justification.
Package verification passes for `aprodb-types`, and the full workspace packages with `--no-verify`.
Full workspace verification currently fails because unpublished internal path-and-version dependencies
are resolved through crates.io; this does not affect GitHub publication.

The final scan of the 129 candidate files did not detect any files of at least 10 MiB, strong patterns of secrets, or email addresses. `.gitignore` has been expanded with root data directory, database, WAL and snapshot, and the `.aprodb` extension; `Cargo.lock` remains version-controlled and `target/bench-lab` has not been removed.

## 2026-08-19 — Publication of the public beta

The target has been confirmed and the public repository has been created as `provenzali/aprodb`, with default branch `main` and `origin` HTTPS. The baseline commit `b0d5d1ebbda4aae1052028f7f8ed34f8e922cf7b` contains 135 files and has been published without force-push. GitHub recognizes both the AGPL-3.0 and Apache-2.0 licenses, and makes available the citation, contribution guide, and security policy.

The main page declares the beta test, links to the manual, English abstract, and paper, and provides two Mermaid diagrams: the architecture boundaries and the path from the canonical record to the incremental surface. The paper is described as a public, non-peer-reviewed technical specification, distinct from the usable functions documented in the manual. Internet publication may constitute prior art for future patent applications; copyright protects text and code, not ideas, algorithms, or abstract features.

Topics, dependency graph, Dependabot alerts, push protection, and private vulnerability reporting have been configured on the repository. The CPU-only GitHub Action for the baseline commit (`32301352066`) reported successful formatting, Clippy, tests, and fuzz compile-check. Current package behavior and its unpublished-internal-crate limitation are documented above.

AProDB was also added to the ORCID record `0009-0009-9677-9840` as a public Software work, dated 2026-08-19, URL `https://github.com/provenzali/aprodb`, BibTeX citation, and Andrea Provenzali's Software role. The work was included among those highlighted; no additional personal data was made public.

## 2026-08-20 — English publication and editorial QA

All public Markdown, source-facing messages, benchmark labels, and the two Mermaid diagrams were
reviewed for idiomatic technical English. The pass corrected literal translations, author-name
typos, number formatting, stale publication chronology, and package-verification claims while
preserving benchmark values, links, format versions, and technical semantics.

The remaining Italian greeting samples in tests and fixtures were changed to `hello`/`hello!`.
This intentionally changed six golden byte sequences: logical record, stored record, surface
payload, Protobuf Put request, and the legacy 0.1 Put-frame and snapshot fixtures. The final
workspace gate caught that the two legacy fixtures had initially retained the old payload; they
were regenerated and the complete suite was rerun. Sample lengths/content and checksums changed,
but frame kinds and format versions did not.

`cargo fmt --all --check` passed. Workspace/all-target Clippy with warnings denied and workspace
tests passed both CPU-only and with default features on the final fixture set. The logical golden
suite passed 3 tests, the wire golden suite passed 2 tests, the 0.1 database suite passed 5 tests,
and the one-shot migration test passed.
The final scan covered 32 Markdown files, found no missing local links or residual Italian prose,
and verified that both Mermaid blocks use English labels. `git diff --check` passed.
