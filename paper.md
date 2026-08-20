# AProDB — Adaptive radial architecture specification

**State:** stabilized normative baseline for implementation
**Document version:** 1.4
**Date:** August 19, 2026
**Reference language:** English
**Reference implementation:** Rust, edition 2024

> [!NOTE]
> This document is a public technical specification of the target product, not an academic peer-reviewed paper, nor a statement that every described function is already available. The implemented and testable state is documented in the [manual](manual.md); AProDB is in beta testing.

## Summary

AProDB is a specialized database for systems in which data move through a processing cycle, have non-uniform utility, and lose or gain value over time. Examples include editorial platforms, content collection and enrichment, observability, intelligence, continuously updating catalogs, processing queues, and application surfaces powered by events.

The distinctive property of AProDB is not simply the presence of a cache. The engine natively considers:

- freshness;
- probability of access;
- urgency of processing;
- state of preparation;
- cost of reconstruction;
- most convenient form of the data;
- hardware component best suited to serve or transform it.

The canonical record remains durable. Around it, AProDB builds reconstructible projections: indexes, decompressed blocks, columnar representations, work queues, and already serialized surfaces. CPU, hardware cache, RAM, NVMe, SSD, HDD, and GPU are treated as a heterogeneous hierarchy. No functionality depends on the presence of a GPU for correctness.

This specification defines the target product and explicitly separates:

- what must be included in the first single-node server version;
- what can be introduced later without changing the logical format;
- what AProDB deliberately chooses not to be.

## 1. Normative rules

In this document:

- **MUST** and **MUST NOT** indicate a mandatory requirement;
- **SHOULD** indicates the default choice, which may be overridden only with justification and careful consideration;
- **MAY** indicates an optional capability;
- **experimental** indicates a feature that cannot be required to read, retrieve, or administer canonical data.

The paper is the normative source for the architecture.
The manual describes only actually available functions.
The journal records decisions, implementations, tests, and deviations.
If the code requires a change to this specification, the change must be declared with an architectural decision; it cannot be made silently.

## 2. Problem and motivation

General-purpose relational databases offer SQL, arbitrary joins, complex constraints, multiple levels of isolation, and a mature ecosystem. These features are valuable, but do not always align with the typical needs of applications such as automated editorial systems:

1. acquire new elements;
2. prevent or detect duplicates;
3. assign work to concurrent processes;
4. progressively enrich the elements;
5. publish an orderly and ready view;
6. serve primarily recent elements;
7. cool and compress data that lose likelihood of access;
8. reactivate old elements when they become relevant.

The static analysis of the Commit server showed a concrete example: PostgreSQL correctly coordinates concurrent workers through transactions, advisory locks, SKIP LOCKED, unique constraints, and leases; the public surface, instead, is a large materialized view that is periodically refreshed in its entirety. AProDB is not intended to copy Commit or to replace PostgreSQL without equivalent correctness. It is designed to make the recurring operations of this class of systems both incremental and native.

## 3. Objectives

AProDB MUST:

1. run on a machine without a GPU;
2. offer a secure central service for multiple client processes;
3. ensure verifiable persistence and recovery;
4. make claims, leases, completion, and version comparison atomic;
5. give priority to point reads, time windows, ordered queues, and incremental surfaces;
6. keep memory and background work within explicit budgets;
7. adaptively compress all data that traverse the persistent path, keeping them uncompressed when compression would be disadvantageous;
8. use CPU cache-friendly layouts;
9. use the GPU only when the total expected cost is lower than the CPU path;
10. expose staleness, watermark, durability, and trade-offs instead of hiding them;
11. be able to reconstruct every cache or projection starting from the canonical state;
12. offer reproducible benchmarks, separating embedded and client/server modes.

## 4. Non-objectives of the first version

AProDB 1.x MUST NOT declare:

- general SQL compatibility;
- arbitrary joins;
- serializable transactions between shards;
- universal replacement of PostgreSQL, MySQL, or MariaDB;
- durability entrusted to VRAM;
- guaranteed acceleration solely by using a GPU;
- multi-leader replication;
- execution of untrusted application code in the database;
- unlimited memory or datasets necessarily fully resident in RAM;
- performance results extrapolated from in-process benchmarks to servers queried over the network.

A limited SQL gateway, RESP3 gateway, and Raft replica are successive extensions, not prerequisites of the single-node core.

## 5. Conceptual model: sphere, radius, and sectors

### 5.1 Core

The core is the control plane.
It contains:

- event ordering;
- routing to shards;
- durability adapter and change log;
- versions and fencing tokens;
- queues, claim, and lease;
- budgeting and backpressure;
- catalog of collections, indexes, and projections;
- CPU/GPU scheduler;
- metrics and operational state.

Domain processes, such as librarians or editorial teams, remain external clients.
The core coordinates their work, but does not automatically incorporate their logic.
Internal operators permitted are only deterministic and controlled operators, such as filters, sorting, top-k, hashing, compression, and vector search.

### 5.2 Radial layers

The layers are logical; they can share the same physical device:

| Layer   | Prevalent Content          | Form                                   | Durability       |
|--------|----------------------------|----------------------------------------|------------------|
| Surface| Ready responses and records| Uncompressed or already serialized     | Reconstructible  |
| Hot    | Records and columns with high probability of access | Raw or very fast compression             | Canonical or reconstructible |
| Warm   | Recent blocks and secondary indexes      | Fast Zstandard                          | Canonical        |
| Cold   | Infrequently accessed segments           | Denser Zstandard, dictionaries          | Canonical        |
| Archive| Historical data                          | Large segments, strong compression      | Canonical        |

The surface is not the source of truth. Its loss may temporarily degrade performance, but it will never cause the loss of a committed write according to the required durability.

### 5.3 Sectors

The radius expresses readiness and expected latency; the sector expresses purpose or phase. The same record can feed different sectors:

- acquisition;
- deduplication;
- classification;
- translation;
- moderation;
- publication;
- comments;
- analysis;
- archive.

This prevents the mistake of representing the lifecycle with only one temperature. A news item may be hot for a classifier but not yet present on the public surface.

### 5.4 Two surfaces

AProDB distinguishes:

- **work surface:** elements that a worker must claim immediately;
- **read surface:** elements ready to display to a user or consume from a service.

The two surfaces have independent policies, orderings, consistency, and budgets.

## 6. Radial heat model

Each record has a radial descriptor separated from the payload. The descriptor contains at least:

- creation time and last update;
- decaying estimate of access frequency;
- last sampled access;
- freshness half-life defined by the collection;
- optional urgency and expiration;
- processing state;
- readiness for each projection;
- estimated reconstruction cost;
- logical and physical size;
- storage class;
- administrative pin;
- canonical version.

The initial score is minimal:

    radial_score = wf * freshness + wu * workflow_urgency

with components in the range from zero to one; the administrative pin overrides the score. Freshness uses exponential decay relative to the collection's half-life. Urgency and processing state are explicit signals, not opaque inferences.

Additional signals — access heat with a decaying probability counter, readiness, reconstruction cost, and size pressure — are specified by the descriptor but are only included in the score when measurements show that the minimum version misplaces the data. Each additional signal specifies weight, rationale, and telemetry.

The score MUST NOT determine correctness, authorization, or canonical deletion. It determines admission, promotion, prefetch, and cache victim selection.

The following must exist:

- distinct thresholds for promotion and demotion;
- minimum residency in the layer;
- migration limit per interval;
- protection against one-off scans;
- pin with expiration;
- telemetry recording the reason for each decision.

Weights are configurable per collection. Autotuning may propose or vary them within limits, but it must be able to be disabled and must log every change.

## 7. Logical data model

### 7.1 Identity

The complete identity is:

    tenant / namespace / collection / partition / key

- tenant isolates quotas and authorizations;
- namespace groups applications;
- collection defines schema and policy;
- partition is the unit of atomicity and routing;
- key identifies the record.

Keys are byte strings with a configurable limit. Textual conventions should not be required by the engine.

### 7.2 Canonical record

The canonical record contains:

- identity;
- typed or opaque payload;
- content type;
- version;
- created_at and updated_at assigned by the server;
- optional TTL;
- user metadata with limits;
- workflow descriptor;
- optional idempotency key;
- checksum and reference to compression dictionary;
- tombstone in case of deletion.

The minimal types are:

- bytes;
- UTF-8 text;
- signed 64-bit integer;
- finite 64-bit float;
- boolean;
- timestamp;
- 32-bit float vector with declared size;
- structured document with versioned schema;
- reference to blob.

Large objects exceeding a configurable threshold are placed in segmented blobs. The canonical record, indexes, and change log retain references rather than repeated copies of the body.

### 7.3 Schema

A collection can be:

- schemaless, with opaque payload;
- typed, with declared fields and types;
- columnar, for batches and projections.

Allowed evolutions without rewriting are the addition of optional fields and new derived indexes. Incompatible changes require a new schema version and explicit migration. The file format must not depend on the in-memory layout of a Rust struct.

### 7.4 Versions

A version is an ordered tuple:

    epoch, shard_id, sequence

Epoch changes when a write authority is recreated or promoted. Sequence increases monotonically within the shard. A version is never reused.

## 8. Operation model

### 8.1 Fundamental operations

The first server version MUST offer:

- Put;
- Get;
- GetBatch;
- Delete;
- CompareAndSwap;
- AtomicBatch within a partition;
- Limited ScanPrefix;
- ScanTimeRange on index;
- QueryIndex on declared indices;
- Append;
- Claim;
- Heartbeat;
- Complete;
- Fail;
- Publish;
- GetSurface;
- SubscribeChanges;
- Sync;
- CreateCheckpoint;
- Stats and ExplainPlacement.

Each mutation command can carry an idempotency key. The server retains the outcome within a configured window and returns the same outcome on retry.

### 8.2 Operations not offered

There are no arbitrary queries on non-indexed fields nor implicit joins. An unsupported request must clearly fail, rather than degrade into an undeclared full scan.

### 8.3 Claim and lease

Claim atomically selects eligible items, changes their status, and returns:

- record and version;
- random lease_id;
- monotonic fencing_token;
- lease_deadline;
- server_time;
- retry metadata.

Heartbeat only extends a still-valid lease. Complete and Fail require lease_id and fencing_token. An obsolete worker cannot overwrite the result of a successor worker.

After a restart, persisted leases are re-evaluated using server time and the collection's policy. Monotonic time is used during processing; persisted expiration uses UTC and allows for a configured safety interval. The clock must not be used to order canonical writes: sequence numbers serve this purpose.

The semantics for external workers are at-least-once with idempotency. AProDB does not promise exactly-once for effects produced outside the database.

## 9. Process architecture

### 9.1 Server mode

The recommended mode is a daemon that exclusively owns the data directory.
Local clients use Unix domain sockets on Linux/macOS and named pipes on Windows when available; TCP is available for remote access.

At startup, the server acquires an exclusive process lock. A second instance on the same directory must refuse to start and must exit. The lock does not substitute for backend durability and recovery.

### 9.2 Embedded mode

The embedded library remains supported for single-process testing and applications. It uses the same storage engine and acquires the same lock. It does not allow the directory to be shared between processes.

### 9.3 Components

The process contains:

- protocol acceptor and decoder;
- authentication and quotas;
- partition router;
- write actor per shard;
- read snapshots;
- storage adapter and group commit;
- catalog and logical change log;
- working set and backend snapshot;
- indexes;
- radial cache manager;
- projection builder;
- compaction and tiering scheduler;
- CPU compute pool;
- optional GPU scheduler;
- metrics, tracing, and administration.

Network, write, compaction, and compute threads must not share a single, unlimited pool. The database must avoid oversubscription between the asynchronous runtime, Rayon, and GPU drivers.

### 9.4 Workspace Rust

Migration from a single crate to a workspace should produce acyclic boundaries:

| Crate           | Responsibility                                             |
|-----------------|-----------------------------------------------------------|
| aprodb-types    | identifiers, record envelope, versions, errors, and shared configuration |
| aprodb-storage  | storage contract, built-in backend adapter, checkpoint, blob, codec, and recovery |
| aprodb-engine   | shard actor, working set, cache, indexes, workflow, query, and projections |
| aprodb-compute  | optional CPU operators and GPU backends                    |
| aprodb-proto    | wire schema and protocol compatibility                     |
| aprodb-client   | asynchronous and blocking client                           |
| aprodb-server   | daemon, transports, auth, quotas, and administration       |
| aprodb-cli      | user and operational commands                              |
| aprodb          | compatible facade and embedded mode                        |

The desired graph is types at the base; storage and compute depend on types; engine depends on types, storage, and compute; protocol does not depend on server; client and server depend on protocol.

Rust unsafe is prohibited by default. When necessary for I/O, SIMD, or GPU interoperability, it must be confined to small modules, accompanied by safety invariants, Miri tests where applicable, and a safe fallback. GPU features, direct I/O, encryption, and replication must compile separately; the CPU-only build remains the main gate.

## 10. Concurrency and consistency

### 10.1 Single writer per shard

Each shard has a single logical sequencer for mutations. It is not necessary to permanently dedicate a thread to every shard: multiple actors can be run by a limited number of workers, but two mutations of the same shard cannot be applied out of order.

Reads use immutable structures or snapshots published atomically. The read path does not update a global LRU list on each hit; heat sampling is performed using local counters and periodic aggregation.

### 10.2 Guarantees

On a single-node leader:

- A Get after a confirmed Put sees at least that version;
- CompareAndSwap is linearizable within the key;
- AtomicBatch is atomic within a partition;
- Claim is atomic with respect to other Claims in the same partition;
- a snapshot of a single partition is consistent;
- surfaces and derived indices may lag behind, but they expose watermark and version.

AProDB 1.x does not offer a serializable transaction that spans different partitions. Operations requiring atomicity must use a common partition key or a workflow with outbox, compensation, and idempotency.

### 10.3 Declared ACID properties

Within a partition:

- **Atomicity:** the batch appears entirely or not at all;
- **Consistency:** versions, schema, local constraints, and transitions are verified;
- **Isolation:** mutations are serialized by the actor, and reads observe published snapshots;
- **Durability:** depends on the mode explicitly requested.

Documentation must not use the word ACID without specifying this scope.

### 10.4 Backpressure

Each queue is limited. When persistence, compaction, projections, memory, or GPU accumulate debt, the server slows down or refuses new operations with a retry-after and a retriable error. It must not continue allocating memory until the OOM killer intervenes.

## 11. Write pipeline

A mutation follows this logical order:

1. decode with size limit;
2. authentication, authorization, and quota;
3. key, schema, and idempotency validation;
4. partition and shard routing;
5. version or lease check;
6. sequence assignment;
7. atomic batch construction with record, catalog, essential indexes, and change log;
8. backend commit according to durability;
9. publication of the updated working set;
10. publication of the new read snapshot;
11. notification to change log consumers;
12. response with version and durability receipt.

Step 8 precedes confirmation in durable modes. Indexes and surfaces may be updated after confirmation; the canonical record must not.

### 11.1 Durability

The public modes are:

| Mode      | Confirmation                          | Guarantee                                                                 |
|-----------|----------------------------------------|---------------------------------------------------------------------------|
| Durable   | after the persistent point documented by the backend, with configurable group commit window | survives process crash and power loss within the limits declared by backend, OS, and device |
| Relaxed   | after delivery to the operating system | may lose the recent tail in a power loss                                   |
| Ephemeral | memory only                           | no persistence                                                            |

Durable is the recommended value for concurrent services: the group commit window amortizes the latency, and a window of zero is equivalent to a flush per request.
The receipt includes shard, sequence, applied mode, and known durable watermark.
The server must not promote a mode less strong than the one requested.

Real guarantees depend on proper handling of flush/barrier by the filesystem, virtualization, and device.
AProDB must document and test the platform; it cannot promise more than what the underlying layer guarantees.

### 11.2 Group commit

The storage adapter collects requests up to a maximum window and byte limit, choosing whichever limit is reached first.
A request can force the next commit to persist; a window of zero avoids intentional waiting.
The window is configurable and observable.
The adapter must not promise batching or flushes that the backend does not expose.

## 12. Read pipeline

Get performs:

1. routing;
2. fingerprint micro-cache check, if enabled;
3. search in the backend working set or snapshot;
4. consultation of available logical and physical indices;
5. cache check for decompressed objects or blocks;
6. reading the record or block via the adapter;
7. integrity check;
8. decompression with worker context;
9. decoding and version check;
10. asynchronous heat sampling;
11. possible cache admission.

Reading a surface uses a ready immutable generation. A generation is replaced by an atomic swap; in-progress readers can complete on the previous generation.

A derived response must include:

- projection_generation;
- source_watermark for the affected shard;
- generated_at;
- stale_by;
- complete or partial;
- any sector errors.

## 13. Memory and cache

### 13.1 CPU hardware cache

L1, L2, and L3 are managed by the processor. AProDB does not attempt to lock records there. The engine optimizes the path by:

- contiguous structures;
- separation between hot and cold fields;
- struct-of-arrays for scans;
- compact buckets and descriptors;
- batching;
- reduced allocations;
- queues and counters for workers;
- alignment to avoid false sharing;
- prefetching only when measured;
- sharding consistent with CPU and NUMA topology.

The engine detects cache and topology sizes, but does not encode any cache line or capacity as universal. The structures must remain correct on hardware that cannot be detected.

Indicatively:

- L1/L2 benefit loops, fingerprints, code, and small local buckets;
- L3 benefits directory segments, filters, and hot parts of the indexes;
- RAM contains payload, working set, surfaces, and blocks.

### 13.2 Global budget

The server determines an effective memory limit as the minimum between configuration, container/job limit, and available physical memory. If the user does not set a budget, the conservative initial value is 50% of the effective limit detected. The value is shown at startup and may be declined in environments where detection is uncertain.

Initial pools, reallocatable within minimum and maximum boundaries:

| Pool | Initial quota |
|---|---:|
| Working set and write buffers | 20% |
| Indexes and metadata | 20% |
| Hot object cache | 20% |
| Decompressed block cache | 15% |
| Surfaces | 15% |
| I/O, compression, and emergency reserve | 10% |

The total includes measurable overhead, not just payload. Iterators, batches, and in-flight responses must be accounted for. No pool may take from the emergency reserve.

### 13.3 Specialized caches

AProDB uses separate caches:

1. **metadata cache:** directory, footer, Bloom filter, and radial;
2. **object cache:** frequently decoded values;
3. **decompressed block cache:** verified and decompressed blocks;
4. **optional compressed block cache:** especially useful with direct I/O;
5. **surface cache:** generations already serialized;
6. **negative cache:** absences with a short TTL and catalog version;
7. **VRAM cache:** reconstructable GPU projections.

A scan must not automatically evict the current working set. Scans use a separate admission class or bypass the object cache.

### 13.4 Admission and eviction

The default policy is a radial variant of Window TinyLFU:

- a small window protects newly observed elements;
- a decaying TinyLFU estimate compares candidate and victim;
- the radial score incorporates freshness, urgency, readiness, and reconstruction cost;
- an SLRU separates probation and protected segments;
- size and maintenance cost weigh on the decision.

Reads do not acquire a global mutex to update order. Sampled events are periodically merged into policy structures. Pin, TTL, and tenant quotas take precedence over score.

### 13.5 Coherence

Each derived element carries a source version or watermark.
A mutation:

- updates the canonical source;
- invalidates or updates the affected projections;
- does not modify a visible generation in place;
- publishes the new generation only when complete according to policy.

A cache cannot confirm a canonical write.
Write-back is only allowed for data declared Ephemeral; for other cases, the cache is read-through or write-through after the backend commit.

### 13.6 Operating-system page cache

The default backend uses buffered I/O and benefits from the operating-system page cache, because it is portable and safe as a starting point.
A direct-I/O backend can be enabled on supported platforms when:

- AProDB has a complete budget;
- alignment and sizes are validated;
- benchmarks show that double caching is harmful;
- there is automatic fallback.

Direct I/O is not synonymous with higher speed and should not be enabled for its own sake.

## 14. Hardware adaptation

### 14.1 Hardware profile

At startup, AProDB builds a versioned profile:

- architecture and SIMD set;
- physical and logical cores;
- cache and NUMA;
- memory and container limits;
- type and capacity of filesystems;
- logical sector size and alignment requirements;
- rotational, SSD, or NVMe if detectable;
- GPU, VRAM, compute and transfer capacity;
- OS, driver, and backend version.

Uncertain information is marked as such. The profile must not contain sensitive identifiers in public logs.

### 14.2 Calibration

A short and limited calibration measures:

- memory bandwidth and latency;
- hash, compression, and decompression cost on representative sizes;
- I/O latency and throughput;
- fixed cost and throughput for the GPU;
- balancing batch size.

Results are saved with hardware/software fingerprinting.
Calibration does not perform destructive writes on the data and can be disabled.
Runtime decisions use moving measurements, not just the initial benchmark.

### 14.3 CPU and NUMA

The shard count is a power of two for fast routing, but does not necessarily match the number of threads.
For NUMA machines:

- Working sets and code are preferably allocated in the worker's node;
- Compaction and GPU staging respect NUMA affinity when possible;
- Cross-node accesses are measured;
- Thread pinning remains configurable and initially experimental.

The CPU path is the reference implementation for all operators.

## 15. GPU and heterogeneous computing

### 15.1 Fundamental rule

The GPU is optional, volatile, and rebuildable.
Persistence, catalog, authorization, lease, sorting, and recovery do not depend on the GPU.

Each accelerated operator implements the same semantics as on the CPU. For floats and vectors, numerical tolerances and rules for NaN, infinity, and tie-breaking are defined.

### 15.2 Candidate operations

The following are candidates:

- vector distance and top-k;
- columnar filters on large batches;
- aggregations;
- ordering and ranking;
- hashing or massive deduplication;
- numerical transformations;
- compression and decompression of large batches, as an extension;
- construction of some projections.

The following are not candidates in the first phase:

- single Get;
- small mutation;
- fsync;
- claim and lease;
- complex and heavily branched parsing;
- operations where data transfer outweighs the compute workload.

### 15.3 Scheduler

The scheduler selects GPU only if:

    transfer_in + queue_wait + launch + gpu_compute
      + transfer_out + synchronization + risk_margin
      < estimated_cpu_compute

The estimate includes the probability of data already in VRAM being reused.
The following are required:

- micro-batching with a maximum wait time;
- host pinned buffers only within a set budget;
- asynchronous transfers;
- multiple buffers or streams when supported;
- limit on the number of in-flight requests;
- timeout, circuit breaker, and CPU fallback;
- VRAM hit metrics and transfer time;
- isolation of driver errors.

A GPU error must not bring down the database. The device is put into cooldown and the work is retried on the CPU when semantically safe.

If a GPU result is to produce a mutation or projection, the engine compares the source version and watermark again before publishing. A result computed from obsolete input is discarded or recalculated; it cannot overwrite a more recent state.

### 15.4 Formats

The GPU representation is columnar, with contiguous buffers, validity bitmap, offset, and declared alignment. Apache Arrow is the conceptual reference for memory interoperability; AProDB does not necessarily need to depend on the entire Arrow runtime in the core.

VRAM retains only projection_id, source watermark, schema version, and derived buffers. A schema or generation change invalidates the projection.

### 15.5 Backend

The first portable backend can use wgpu. CUDA or HIP backends can be added behind the same interface for operators where portability and performance diverge. The file format and protocol must not encode a GPU vendor.

## 16. Physical storage

### 16.0 Backend contract

Chapters 16, 17, 18, and 20 define the guarantees that the storage backend MUST provide, not a requirement to reimplement an LSM. The first implementation SHOULD use an existing embedded engine selected in Milestone 0.5. Fjall is the first candidate to be evaluated; it is not an approved dependency before the spike and ADR. Redb and RocksDB remain as documented fallback options.

The minimum contract includes:

- atomic batch for records, essential indices, and log events;
- Durable and Relaxed persistence with a demonstrable confirmation point;
- snapshot or consistent reads;
- Get, range, and limited prefix iteration;
- recovery after crash;
- checkpoint or consistent backup;
- datasets larger than RAM;
- configurable or at least declared physical compression for keyspace, data, and indices;
- limits and telemetry sufficient to prevent uncontrolled growth;
- defined behavior for compaction, space exhaustion, and corruption.

Guarantees, record envelope, versions, log events, watermarks, and logical formats belong to AProDB. WAL, memtable, segments, manifest, and physical compaction belong to the embedded backend. AProDB does not duplicate the backend WAL.

Substitutability is not free: transactions, snapshots, iterators, backup, and compaction control may differ. The adapter exposes a capability matrix and does not simulate missing capabilities with weaker guarantees. Changing the backend requires verified export/import or explicit migration. A native engine is written only if concrete measurements demonstrate that the chosen backend prevents essential features of AProDB.

Fault injection, recovery, and durability tests apply to the contract regardless of the backend.

### 16.1 Principles

A possible native reference backend combines:

- append-only WAL;
- mutable and limited memtable;
- ordered immutable segments;
- transactional manifest;
- background compaction;
- separate blobs for large values.

Radial promotion does not continually rewrite the canonical record. It prefers to create or eliminate projections. Canonical segments migrate between physical classes with file or extent granularity, not on every read.

With an embedded backend, these elements are private to the backend implementation. AProDB manages logical records, sectors, its own necessary indices, surfaces, and reconstructible placement above them.

### 16.2 Storage media

**NVMe:** Asynchronous code, batching, and parallelism limited to effective queue depth; ideal for WAL, recent segments, and compaction.
**SATA/SAS SSD:** more moderate concurrency; warm and cold storage.
**HDD:** sequential access, large segments, and archiving; avoid random lookups and aggressive compaction during service.
**Single device:** layers remain logical and differ by format, cache, and I/O priority.

The user registers one or more storage classes with path, budget, and preference. The engine detects the medium but allows overrides, because virtualization and RAID can obscure it.

### 16.3 I/O priority

The default order is:

1. WAL and recovery;
2. Foreground reads;
3. Surface publication;
4. Memtable flush;
5. Necessary compaction to prevent stall;
6. Prefetch;
7. Migration and archiving.

Compaction and tiering consume bandwidth and IOPS tokens. They must not saturate the device to the point of unbounded foreground latency degradation.

If the built-in backend does not provide sufficient priority or tiering, the adapter declares this capability absent. The feature remains disabled or is implemented at the AProDB projection level; non-existent physical control is not declared.

## 17. Physical durability and event log

### 17.1 Embedded backend WAL

The physical WAL, its framing, and recovery belong to the built-in backend. The AProDB adapter MUST:

- Map Durable and Relaxed to documented backend primitives;
- confirm Durable only after the persistent point provided by the backend;
- keep AtomicBatch indivisible;
- verify reopening, incomplete tails, and crashes through fault injection tests;
- expose durable watermark and failure modes;
- prevent unsupported concurrent openings.

AProDB does not directly interpret or modify private backend WAL files.

### 17.2 AProDB logical event log

AProDB maintains an ordered and versioned change log within the same atomic batch as the canonical mutation and the necessary indices. The log shall contain at least:

- collection and partition;
- epoch, shard, and sequence;
- type of operation;
- key or reference;
- previous and new version when available;
- transaction or batch id;
- idempotency hash if present;
- metadata necessary for projections and workflow;
- reference to the payload and its version or a minimally sufficient delta;
- logical checksum or integrity provided by the record envelope.

The change log feeds SubscribeChanges, projections, surfaces, watermarks, and incremental rebuilds. It does not replace the physical WAL and is not used to guarantee durability exceeding that of the backend.

The event MUST NOT duplicate the full payload by default. Each collection declares an EventRetentionMode:

- **Delta:** the event contains the minimal, self-sufficient delta required by projections;
- **VersionRef:** the current record and event reference an immutable object identified by key/version or content hash;
- **SelfContained:** the event includes the payload only by explicit policy, with size and retention limits.

In VersionRef, the immutable payload is written only once; the current head and event retain references to it. The version remains readable until all required consumers have passed the watermark, and as long as backup or future replication requires it. Simply reading the current version is not correct if a subsequent update already exists.

Backend MVCC snapshots can be used for the consistency of a short-lived request, NOT as a durable retention mechanism. They do not survive as an application contract after a restart, and if retained for long periods, they can prevent garbage collection of obsolete versions. For retention and compaction, AProDB must use versioned keys, content-addressed objects, or self-sufficient deltas.

The cost of the change log is measured separately: event bytes/payload bytes, write amplification, durable latency, throughput, space after compaction, and rebuild cost.

An AtomicBatch produces a single logical commit or an indivisible group identified by the same batch id. No consumer can observe a prefix of the batch. Events are removed only when checkpoint, retention, projections, backup, and future replication no longer require them.

### 17.3 Format for a potential native backend

The WAL is a sequence of numbered and pre-allocatable segments. Each frame contains:

- magic number and format version;
- frame type;
- flags;
- shard and epoch;
- sequence or interval;
- transaction/batch ID;
- idempotency hash if present;
- header and payload length;
- payload;
- CRC32C checksum of stored bytes.

Large records are fragmented with first, middle, last, and a common identifier. Recovery:

1. reads manifest and valid checkpoint;
2. sorts the WAL segments;
3. verifies frame and sequence;
4. reapplies only events following the checkpoint;
5. ignores or truncates an incomplete tail;
6. treats corruption as an error in confirmed data;
7. produces a report and does not hide skipped records.

An AtomicBatch is represented by a single logical record or by a begin/part/commit sequence with overall checksum and count. Recovery applies the batch only if the commit is valid and all parts are present; it never makes a prefix of the batch visible.

The WAL is recycled only after a durable checkpoint, the manifest is published, and replication or backup needs have been met.

This subsection is normative only for a native AProDB backend. It does not impose the physical format on Fjall, redb, RocksDB, or other embedded backends.

## 18. Logical catalog, segments, and manifest

### 18.1 Embedded backend

Segments, Bloom filter, physical manifest, temporary files, and compaction belong to the built-in backend. The adapter verifies the required guarantees and translates metrics and checkpoints when available.

AProDB maintains the following in a dedicated and transactional logical space:

- schemas and their versions;
- dictionaries and references;
- index and projection definitions;
- generation and watermark;
- dynamic configuration;
- idempotency state and retention;
- capability and backend version.

This catalog is updated atomically together with the operations it belongs to, or through a versioned and recoverable transition.

### 18.2 Format for a possible native backend

Each immutable segment contains:

- header with magic, version, UUID, collection, shard, and schema;
- key and time range;
- record blocks;
- codec and dictionary id per block;
- checksum per block;
- sparse index of keys;
- time index;
- Bloom filter;
- min/max statistics for indexed fields;
- tombstone and version bounds;
- footer with offset and checksum.

All on-disk integers have explicit endianness. Limits and offsets are checked before memory allocation. This format is not enforced for built-in backends.

### 18.3 Manifest for a possible native backend

The manifest lists active segments, checkpoints, dictionaries, schemas, projections, and generations. Updating uses:

1. writing a new temporary manifest;
2. file flush;
3. a supported atomic rename;
4. directory flush where necessary;
5. controlled retention of the previous generation.

Orphan files are detected at startup and are quarantined or recovered according to testing, never automatically added to the canonical state.

## 19. Integrated compression

### 19.1 Interpretation of “compress every datum”

Every persistent value passes through the compression engine. The result may have a Raw codec when:

- the value is too small;
- it is already compressed or encrypted;
- the sample does not yield a minimum gain;
- the latency of the layer outweighs the savings;
- the surface requires a ready-to-use form.

Keeping Raw is determined by the compressor, not by bypassing. Compressing incompressible data increases space usage and CPU consumption.

### 19.2 Codec

Zstandard is the default general persistent codec for ratio, speed, decompression, and dictionaries. Each compressed logical payload records the codec and version, enabling future codecs without immediate migration. Any future native backend may also compress physical blocks.

Initial policy:

- Surface: Raw;
- Hot: Raw or Zstandard fast/low level;
- Warm: Low-level Zstandard;
- Cold: Medium-level Zstandard chosen by budget;
- Archive: Denser Zstandard, applied off the foreground path.

Numerical levels are not universally fixed: autotuning and benchmarks for each payload class choose within administrative ranges.

### 19.3 Channels

Compression and decompression use a limited pool of reusable contexts, typically one per active worker—not a global context, and not one thread per value. Channels:

- receive batches;
- have limited queues;
- expose timing, ratio, and fallback;
- respect foreground priorities;
- do not retain buffers beyond the budget limit.

### 19.4 Dictionaries

Dictionaries are per collection and schema.
They are trained on limited samples in the background, validated on a separate sample, and published only if they improve a cost function that includes space and latency.

Each dictionary has an ID, checksum, schema, status, and validity interval.
A dictionary cannot be deleted while there is a block that requires it.
Loading uses pre-digested and shareable forms when the codec allows.

### 19.5 Integrity, encryption, and ordering

The process is:

    encode -> compress decision -> optional authenticated encryption -> checksum/frame

Encryption uses a verified library and external keys; AProDB does not invent cryptographic primitives.
Sensitive metadata must be able to be included in the encryption.
Key rotation and backup are explicit administrative procedures.

### 19.6 Coordination with backend compression

AProDB logical compression and backend physical compression are independent layers. Both should not be blindly enabled on the same content.

Initial hypothesis to be verified in Milestone 0.5:

- Canonical payload: AProDB Raw/Zstandard; backend data block compression disabled;
- Catalog, change log, and small metadata: AProDB raw payload; fast backend compression enabled;
- Ready surfaces: raw, with backend compression only if it reduces total cost;
- Images, archives, and already compressed or encrypted payloads: no second compression;
- Physical indexes: backend policy separated from data blocks.

This is an experimental matrix, not an approved default. The spike compares at least:

1. only AProDB Zstandard;
2. only fast backend compression;
3. both levels;
4. no compression.

For each variant, measure ingest, p95/p99, decompression, CPU, space, compaction, and recovery on repetitive, random, and already compressed payloads. The ADR chooses by keyspace and data class. If the backend does not allow this distinction, the capability matrix declares it and the ADR evaluates whether the limit is acceptable.

## 20. Memtable, flush, and compaction

With an embedded backend, memtable, flush, and physical compaction are the responsibility of the backend and are not duplicated by AProDB. The adapter configures only supported options, collects metrics, and applies backpressure using real signals. Radial structures, caches, and AProDB surfaces remain derived and separate.

For a potential native backend, the memtable contains recent versions and minimum indices. When it reaches a byte or age threshold:

1. it is frozen;
2. a new memtable accepts writes;
3. the frozen one is sorted;
4. it produces immutable segments;
5. the manifest is published;
6. the covered WAL becomes recyclable.

Compaction is time-partitioned and shard-aware. It must:

- delete outdated versions beyond retention;
- apply tombstone only when no level or replica requires the record;
- merge segments without unnecessarily mixing time windows;
- recompress according to layer;
- preserve sequence and checksum;
- avoid uncontrolled write amplification;
- stop or slow down when it harms the foreground.

Compaction debt is measured in bytes, segments, and estimated time. If thresholds are exceeded, backpressure is applied before disk exhaustion.

If an embedded backend does not expose compaction debt, AProDB uses only documented observable indicators such as latency, space, stall, and backlog. It does not invent unavailable precise measurements.

## 21. Indexes and queries

### 21.1 Mandatory indexes

Each collection has:

- exact key index;
- version index;
- time index if freshness is declared;
- workflow index if Claim is used;
- TTL index if expirations are used.

The secondary indices allowed are declarative:

- hash equality;
- ordered range;
- prefix;
- time/priority/state composite;
- future full-text;
- exact vector or ANN.

An index is canonical only for locating the record; the others are derived and reconstructible from the log/segments. The catalog stores the source watermark and build state.

### 21.2 Lookup and segments

In RAM, the engine can combine a hash index for Get and an ordered structure for ranges. On disk, segments are ordered and use sparse indexes, Bloom filters, and block statistics to avoid reads.

The concrete choice among hash table, B-tree, ART, or skiplist remains internal and can change without modifying the protocol. It must be evaluated on AProDB layout and workload; it is not a trait of the public format.

### 21.3 Limited query planner

The planner:

- accepts only supported predicates;
- estimates segments, blocks, and rows;
- declares the chosen index and fallback;
- applies a cost limit;
- rejects a full scan unless the client explicitly authorizes it;
- can choose CPU or GPU for the batch phase.

Explain returns the plan, estimates, tier, possible staleness, and backend compute without executing. Analyze adds measurements and is subject to authorization.

### 21.4 Vectors

The engine offers:

- ExactFlat CPU required;
- ExactFlat GPU optional;
- HNSW CPU as a subsequent derived index;
- IVF or product quantization as future experimental search methods.

Size, metric, and normalization are properties of the schema. Approximate results must declare the index and parameters; they cannot be presented as exact. Updates and deletions must implement a rebuild and tombstone strategy.

## 22. Surfaces and projections

### 22.1 Definition

A named projection specifies:

- source collections;
- indexed filters;
- total ordering with tie-breaker;
- allowed fields or transformations;
- output format;
- record and byte limits;
- time window;
- maximum desired staleness;
- publication policy;
- dependencies.

External domain transformations write new fields or events; the engine does not perform arbitrary translations or modeling within the transaction.

### 22.2 Incremental update

Each canonical event is evaluated against dependent projections. The builder:

1. read from the sequence following the watermark;
2. apply insert, update, or remove to the candidate structure;
3. verify ordering, limits, and dependencies;
4. serialize the new generation or delta;
5. publish atomically;
6. advance the watermark.

A complete rebuild is performed on request, schema change, corruption, or changelog gap.
This is not the normal periodic procedure.

### 22.3 Surface formats

The following are allowed:

- AProDB binary records;
- pre-serialized JSON;
- MessagePack or Protobuf as defined by the projection;
- Arrow IPC for analytic batches.

Dynamic headers and user permissions must not be materialized within a shared surface.
The application response may combine the public surface and separate personal data.

### 22.4 Publication

A generation is immutable and addressed by ID.
The current pointer changes atomically.
The server retains a limited number of generations for active readers and operational rollback.
Generations that are no longer referenced are removed in the background.

## 23. Protocol and Client API

### 23.1 Data Plane

The protocol is binary, versioned, and language-neutral. The first implementation should use length-delimited Protobuf messages with:

- magic and protocol version in the handshake;
- request_id;
- operation;
- deadline;
- tenant and namespace;
- required consistency and durability;
- payload;
- response status;
- server version, record version, and watermark.

The transport supports multiple in-flight and batched requests. The server enforces a maximum frame size, in-flight limits per connection, and an idle timeout.

### 23.2 Transports

- Named pipe or Unix domain socket for local clients;
- TCP with TLS for remote connections;
- Plaintext TCP only on loopback or explicitly trusted networks;
- a separable administrative endpoint.

Protocol compression is negotiated only for payloads that are sufficiently large and not already compressed. It must not unnecessarily duplicate the compression of persistent blocks.

### 23.3 Administrative API

The Administrative API provides:

- health, readiness, and build information;
- catalog and effective configuration;
- metrics;
- checkpoint and backup;
- controlled compaction;
- index and projection status;
- hardware profile;
- placement explanation;
- integrity verification;
- orderly drain and shutdown.

Destructive operations require separate authorization and confirmation of the target.

### 23.4 Redis Compatibility

A RESP3 gateway may in the future map GET, SET, MGET, DEL, TTL, INCR, and a subset of stream/queue commands.
It serves as an adapter, not as the internal semantics.
Non-equivalent commands must fail; they must not result in surprising approximations.

## 24. Security

### 24.1 Boundaries

The data directory is accessible only to the service account.
A single process opens it for writing.
Clients do not receive file system paths.

### 24.2 Identity and authorization

The following are planned:

- local authentication via transport credentials when available;
- short-lived tokens or mTLS for remote access;
- roles for tenant, namespace, collection, and operation;
- data/admin separation;
- quotas for bytes, requests, connections, and GPUs;
- auditing of administrative mutations.

Tokens are never written to logs.
Secret comparisons use constant-time operations when applicable.

### 24.3 Input

Decoders and parsers:

- verify lengths before allocating;
- enforce maximum depth and cardinality;
- reject non-finite float values when prohibited by the schema;
- validate UTF-8 only for text types;
- do not trust on-disk offsets;
- are subject to fuzz testing.

### 24.4 Encryption

TLS protects data in transit. At-rest encryption is optional but designed per block using AEAD and key IDs. Keys come from a protected file, injected variable, or KMS; they are not stored in the manifest in plaintext.

## 25. Observability

Minimum metrics:

- throughput and p50/p95/p99 latency per operation;
- errors and retries;
- queue depth and backpressure;
- backend commit, group size, persist latency, and durable watermark;
- working set, write buffer, and flush as reported by the backend;
- segments, compaction debt, and write amplification;
- logical, physical, and temporary space;
- cache hit, miss, admission, and eviction;
- staleness and build time for surfaces;
- claim, lease expired, heartbeat, and stale completion;
- compression ratio and CPU usage per codec/tier;
- I/O bytes, latency, and outstanding operations;
- CPU pool saturation;
- GPU queue, transfer, kernel, fallback, and VRAM;
- recovery time and records replayed;
- quotas per tenant.

Logs are structured and include event id, component, shard, and sequence when relevant. Payloads and full keys are excluded by default. Tracing propagates request_id and connects write, backend commit, change log, projection, and response.

Health indicates a live process; readiness requires a servable catalog and shards. A delayed projection can degrade an endpoint without declaring the entire database dead.

## 26. Resource management

### 26.1 Disk

Configured as follows:

- data quota;
- minimum free reserve;
- log and backend temporary file quota;
- temporary quota for compaction;
- warning, throttle, and read-only emergency thresholds.

Before a compaction, the engine estimates temporary space. If it is insufficient, compaction does not start and backpressure is applied. In emergencies, it preserves reads and recovery; canonical data is not deleted to free space.

### 26.2 CPU

Separate pools have limits:

- foreground;
- commit storage;
- compression/decompression;
- compaction;
- projection;
- vector/compute.

Priority favors durability and foreground requests. The autotuner cannot consume all cores, maintaining an administrative margin.

### 26.3 GPU

VRAM is allocated per index/projection and reserved per batch. The server does not depend on the driver's overcommit. VRAM eviction occurs before memory exhaustion; a GPU out-of-memory error triggers fallback and cooldown.

### 26.4 Tenant

Each tenant may have limits on:

- canonical space;
- surfaces;
- cache;
- requests and bytes per second;
- in-flight operations;
- claims;
- GPU.

A scan or projection for one tenant must not starve others.

## 27. Recovery, backup and repair

### 27.1 Recovery

Recovery must be deterministic, idempotent, and observable.
A failed start does not irreversibly modify the sole valid copy.
The server can start in read-only mode for inspection.

The classes of damage are:

- incomplete backend commit: not visible after recovery;
- confirmed canonical state but not published in the working set: recovery or rereading makes it visible;
- change log inconsistent with the record: atomicity violation and controlled shutdown;
- corrupt physical backend file: recovery, error, or repair according to the guarantees documented by the backend;
- corrupt derived segment: reconstruction required;
- corrupt canonical state: restore from replica/backup or repair with declared loss;
- corrupt logical catalog: proof of previous version or restore;
- lost cache/surface/VRAM: requires rebuild.

### 27.2 Checkpoint

A checkpoint records:

- backend checkpoint ID and catalog generation;
- durable watermark per shard;
- inventory and checksum exposed by the backend;
- required schemas and dictionaries;
- catalog;
- encryption key IDs;
- software version, logical format, and backend.

### 27.3 Backup

Online backup:

1. Create or select a consistent checkpoint;
2. Use a snapshot, checkpoint, or backup procedure supported by the backend;
3. Include logical catalog, blobs, and dictionaries;
4. Include the subsequent change log if required;
5. Produce inventory and checksum;
6. Release the snapshot or pin.

A successful copy does not constitute a verified backup. Periodic restore tests must be performed on separate directories.

### 27.4 Repair

Repair is not performed automatically. It operates on a copy or with explicit confirmation, produces a machine-readable report, and distinguishes between recovered, lost, and questionable records.

## 28. Replication and high availability

The initial production version is single-node. The logical format prepares for replication through epoch, sequence, checkpoint, and an ordered change log.

The distributed phase uses leader-based consensus, preferably Raft for each shard group:

- a write is committed after quorum according to policy;
- followers apply the same log;
- lease and fencing depend on the term/epoch;
- follower reads declare staleness;
- installable snapshots reduce the log;
- membership changes are controlled.

An incomplete Raft-like consensus protocol is not implemented. Before replication, a formal state model, fault injection, and partition testing are required. Multi-leader and cross-group transactions remain outside the scope of the 1.x project.

## 29. Compatibility and migrations

### 29.1 Versions

The following components are versioned separately:

- Protocol;
- catalog;
- change log;
- backend adapter and format;
- user schema;
- projection;
- checkpoint.

A reader supports a declared window of previous formats.
A writer does not automatically upgrade to an irreversible format without a backup/checkpoint and rollback plan.

### 29.2 AProDB 0.1

The current prototype contains:

- Typed key-value storage;
- Sharded HashMap with RwLock synchronization;
- Single Write-Ahead Log (WAL);
- Full snapshot support;
- Zstandard compression for values and channels;
- Rayon-based parallelism;
- CPU/GPU vector search using wgpu;
- Command-line interface (CLI) and benchmarking utilities.

It does not contain the full file format, protocol, or guarantees of this specification. It is experimental material intended for reuse of tests and components, not a 1.0 format to be implicitly maintained.

The new implementation must:

1. Freeze reading tests for version 0.1;
2. Choose a one-shot import or declare incompatibility;
3. Do not open a 0.1 directory as 1.0 without explicit acknowledgment;
4. Keep a backup copy before conversion;
5. Document anything that cannot be migrated.

## 30. Configuration

The configuration includes:

- Versioned static file;
- Environment variables for referencing secrets and selected overrides;
- Dynamic values persisted in the catalog;
- Queryable effective configuration;
- Validation before startup.

Minimum groups:

- server and transport;
- data paths and storage classes;
- memory budget and pool;
- shard and partition;
- backend, change log, and durability;
- capability, checkpoint, and compaction;
- compression and dictionaries;
- cache and radial weights;
- projections;
- CPU/GPU;
- authentication/TLS;
- quotas;
- metrics and logs;
- backup and retention.

Units are explicit. Duration and byte values do not use unsuffixed numbers in files intended for operators.

## 31. Correctness tests

The mandatory hierarchy is:

1. unit tests for codec, versions, score, and transitions;
2. property tests for encode/decode and ordering;
3. golden tests of record envelopes, change log, catalog, and protocol;
4. fuzzing of parser, recovery, and formats;
5. concurrent tests of CAS, batch, claim, and lease;
6. model-based tests against a sequential model;
7. fault injection between preparation, backend commit, publication, and checkpoint;
8. repeated kill and restart;
9. full disk, permissions, short read/write, and corruption;
10. clock jump and expired lease;
11. GPU reset/out-of-memory and fallback;
12. compatibility between versions;
13. backup and restore.

Invariants to verify:

- no Durable ACK without a recoverable record;
- sequence is never reused;
- an obsolete fencing token does not complete a lease;
- budget is not exceeded beyond a measured tolerance;
- projection never declares a watermark that has not been applied;
- a slow consumer reads the exact version or delta even after compaction and restart;
- no application retention depends on the lifetime of a backend snapshot;
- lost cache does not result in data loss;
- CPU-only retains all functions;
- corruption is not silently transformed into absence.

## 32. Benchmark

### 32.1 Profiles

The official benchmark suite includes:

- exact Get/Put on small records;
- batch Get/Put;
- append, claim, heartbeat, and complete multi-client;
- temporal feed with incremental updates;
- surface read;
- cold miss and decompression;
- indexed scan;
- exact CPU and GPU vectors;
- mixed workload with compaction;
- recovery and rebuild;
- compressible, already compressed, and random payloads;
- RAM limited relative to the dataset.

### 32.2 Method

The following benchmark parameters must be declared:

- hardware, OS, filesystem, and power mode;
- version and configuration of each database;
- dataset and seed;
- warmup;
- client, transport, and pool;
- equivalent durability;
- concurrency;
- confidence intervals and discarded runs;
- logical, physical, and temporary space;
- p50, p95, p99, and sustained throughput;
- CPU, RAM, I/O, and GPU;
- recovery time.

Embedded and server comparisons are presented in separate tables.
Results do not constitute permanent product characteristics and do not replace benchmarks using the user's workload.

### 32.3 Criteria

No numerical guarantees are set prior to testing on a reference machine.
The architectural gates are:

- no regression of correctness in order to achieve throughput;
- functional CPU performance without relying on GPU;
- more efficient incremental surface area compared to a complete rebuild on the target profile;
- limited memory usage without OOM;
- sustained throughput without indefinite growth of compaction debt;
- positive GPU acceleration only above a measured threshold;
- comparative results must be reproducible.

## 33. Implementation roadmap

### Milestone 0 — Foundations

- initialize Git before substantial changes;
- preserve the prototype;
- create Rust workspace;
- define crate structure and boundaries;
- set up local CI, formatting, Clippy, and tests;
- ADR and feature matrix;
- identifiers and error model.

### Milestone 0.5 — Storage backend selection

- define the minimum storage contract: atomicity, durability, range scan, recovery, compaction, memory management, and support for datasets larger than RAM;
- conduct a brief spike on fjall against the contract;
- specifically verify atomic batch operations across record, catalog, and event log, Durable mapping, snapshots, temporal ranges, and reopening after crash;
- measure the overhead of the minimal change log without duplicating payload by default;
- compare Delta, VersionRef, and SelfContained with slow consumers, compaction, and restart; prohibit long-lived snapshots as retention;
- execute the AProDB/backend compression matrix defined in §19.6;
- apply limited exit criteria: correctness, essential capabilities, Windows/Linux build, and no architectural blocker;
- redb and RocksDB remain documented fallbacks if fjall fails fault tests;
- decision recorded as ADR.

### Milestone 1 — Single-node canonical storage

- directory lock;
- adapter for the backend chosen in Milestone 0.5, behind the storage contract;
- AProDB catalog and logical change log in the same transactional domain as records;
- Put, Get, Delete, CAS, and AtomicBatch;
- Durable and Relaxed durability;
- recovery and checkpoint;
- memory limits;
- fault injection tests on the contract.

### Milestone 2 — Multiprocess server

- versioned binary protocol;
- local transport and TCP;
- batching, deadlines, and backpressure;
- basic authentication and quotas;
- Rust client;
- metrics;
- administrative CLI.

### Milestone 3 — Radial engine and storage capacity

- validation and telemetry of segments, indices, flush, and compaction provided by the backend;
- no reimplementation of physical formats without an ADR and demonstrated necessity;
- separate caches;
- radial descriptor and policy;
- TTL;
- storage classes;
- ExplainPlacement;
- datasets larger than RAM.

### Milestone 4 — Workflow and surfaces

- Append, Claim, Heartbeat, Complete, and Fail;
- fencing and idempotency;
- change stream;
- temporal/workflow indices;
- incremental projection builder;
- surface generation and watermark;
- pre-serialized formats.

### Milestone 5 — Adaptive compression

- codec for logical and versioned payload;
- tier levels;
- adaptive Raw fallback;
- versioned dictionaries;
- coordination for keyspace with physical backend compression;
- compressed/decompressed cache;
- channel budgeting;
- telemetry and benchmark.

### Milestone 6 — Heterogeneous compute

- CPU reference trait;
- cost-based scheduler;
- columnar layout;
- optional wgpu;
- VRAM cache;
- exact vector and top-k;
- fault isolation and CPU fallback;
- parity benchmark.

### Milestone 7 — Operability

- backup/restore;
- controlled verify and repair;
- optional encryption at rest;
- TLS/mTLS;
- audit;
- upgrade and import 0.1;
- long tests and packages.

### Milestone 8 — Distribution, separate

- Raft specification;
- replicated logical log;
- follower read;
- snapshot install;
- failover;
- network and partition tests.

A milestone is complete only when code, tests, manual, and diary are in agreement. Partial functions remain experimental and are disabled by default.

## 34. Stabilized architectural decisions

| Decision                 | Outcome                                                                                 |
|-------------------------|----------------------------------------------------------------------------------------|
| Mandatory GPU           | No; CPU reference always                                                                 |
| GPU placement           | Milestone 6 unchanged; interfaces and layouts prepared by foundations                    |
| Deployment model        | Central server, embedded exclusive                                                      |
| General SQL             | Not in 1.x                                                                              |
| Source of truth         | Backend transactional state + catalog and AProDB logical change log                      |
| Storage engine          | Backend contract; embedded engine (Fjall candidate, redb/RocksDB fallback) before a native engine |
| Physical storage        | WAL, memtable, segments, manifests, and compaction belong to the embedded backend        |
| Backend change          | Not transparent; requires capability check and export/import or verified migration       |
| Durability              | Single durable mode with configurable group commit window                                |
| Change log              | Minimal envelope; full payload not duplicated by default                                 |
| Event retention         | Delta, VersionRef, or SelfContained per collection; never long-lived MVCC snapshot       |
| Compression             | AProDB logical and backend physical compression coordinated per keyspace via ADR and benchmark |
| Radial score            | Minimum (freshness + workflow + pin); other signals only if measured                    |
| Surface                 | Derived, incremental, generational                                                      |
| Radial displacement     | Projections before canonical rewrite                                                    |
| Write concurrency       | Single logical writer per shard                                                         |
| Atomicity               | Within partition                                                                        |
| Cross-shard serializable| Not in 1.x                                                                              |
| Worker semantics        | At-least-once + idempotency + fencing                                                   |
| Cache                   | Separate budgets, radial TinyLFU admission                                              |
| Default I/O             | Buffered; direct is experimental and measured                                           |
| GPU format              | Columnar and reconstructable                                                            |
| Replication             | Designed, not part of the first server                                                  |
| Business logic in DB    | No; only limited and deterministic internal operators                                   |
| Redis compatibility     | Future gateway                                                                          |

## 35. Main risks

### 35.1 Complexity

Integrating database, workflow, cache, and GPU can create a system that is too large.
Mitigation: milestones, feature flags, simple formats, CPU reference, and no replication before single-node maturity.

### 35.2 Write amplification

Compaction and migration can consume storage.
Mitigation: temporal segments, reconstructable projections, I/O tokens, and explicit metrics.

### 35.3 Cache pollution

Feeds and scans can evict useful lookups.
Mitigation: separate pools and admission classes, TinyLFU, and bypass.

### 35.4 Radial thrashing

Boundary data can oscillate between tiers.
Mitigation: hysteresis, minimum residency, migration cost, and rate limits.

### 35.5 Negative GPU payoff

Transfers and drivers can worsen latency or stability.
Mitigation: cost model, calibrated thresholds, circuit breaker, and CPU fallback.

### 35.6 Projection coherence

A fast but outdated surface can show incorrect data.
Mitigation: watermark, atomic generations, read-your-writes token, and verifiable rebuild.

### 35.7 Corruption and upgrade

A young format is risky.
Mitigation: versioning, golden file, fuzzing, checker, backup, and non-destructive upgrade.

### 35.8 Excessive specialization

Including commit logic would make AProDB less general.
Mitigation: generic workflow primitives and declarative projections; translations and editorial decisions remain external.

## 36. Product 1.0 completion criteria

AProDB 1.0 can be declared complete when:

- the single-node multiprocess server is the recommended path;
- recovery passes fault injection and kill loop tests;
- durable ACK survives the specified crash tests;
- datasets larger than RAM are supported;
- memory, queues, and disk have limits;
- claim/lease/fencing are verified under concurrency;
- surfaces are incremental and report watermarks;
- adaptive compression and dictionaries are recoverable;
- CPU-only passes the entire functional suite;
- GPU is optional, isolated, and measured;
- backup is automatically restored in test;
- protocol and file format are versioned;
- manual describes only what is implemented;
- fair benchmark servers are published with configuration;
- there are no known data loss defects classified as minor.

## 37. Technical sources

These sources support principles and comparisons; they do not automatically transfer their guarantees to AProDB.

1. Intel, **Intel 64 and IA-32 Architectures Optimization Reference Manual**: cache, memory, and layout optimization.
   https://www.intel.com/content/www/us/en/developer/articles/technical/intel64-and-ia32-architectures-optimization.html
2. NVIDIA, **CUDA C++ Best Practices Guide**: transfer costs, pinned memory, and asynchronous overlap.
   https://docs.nvidia.com/cuda/cuda-c-best-practices-guide/index.html
3. Apache Arrow, **Columnar Format**: interoperable columnar representations.
   https://arrow.apache.org/docs/format/Columnar.html
4. Meta, **Zstandard Manual**: levels, context reuse, and dictionaries.
   https://facebook.github.io/zstd/zstd_manual.html
5. Einziger et al., **TinyLFU: A Highly Efficient Cache Admission Policy**.
   https://arxiv.org/abs/1512.00727
6. Redis, **Key eviction**: approximate LRU/LFU and memory limits.
   https://redis.io/docs/latest/develop/reference/eviction/
7. RocksDB, **Block Cache**: compressed/uncompressed cache and sharding.
   https://github.com/facebook/rocksdb/wiki/Block-Cache
8. RocksDB, **Write Ahead Log File Format** and **RocksDB Overview**: WAL, memtable, segments, and compaction.
   https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format
   https://github.com/facebook/rocksdb/wiki/RocksDB-Overview
9. PostgreSQL, **Concurrency Control** and **Transaction Isolation**: scope of concurrency guarantees and meaning of serializability.
   https://www.postgresql.org/docs/current/mvcc.html
   https://www.postgresql.org/docs/current/transaction-iso.html
10. PostgreSQL, **REFRESH MATERIALIZED VIEW**: replacement of content and behavior of CONCURRENTLY, relevant for comparison with incremental surfaces.
    https://www.postgresql.org/docs/current/sql-refreshmaterializedview.html
11. NVM Express, **Base Specification**: submission/completion queue and ordering of operations.
    https://nvmexpress.org/wp-content/uploads/NVMe-NVM-Express-2.0a-2021.07.26-Ratified.pdf
12. Ongaro and Ousterhout, **In Search of an Understandable Consensus Algorithm**: foundation for future leader-based replication.
    https://web.stanford.edu/~ouster/cgi-bin/papers/raft-extended.pdf
13. Fjall, **KeyspaceCreateOptions**, **CompressionType**, **Snapshot**, and **SeqNo**: compression policy and retention limits via snapshots/MVCC versions in the candidate backend.
    https://docs.rs/fjall/latest/fjall/struct.KeyspaceCreateOptions.html
    https://docs.rs/fjall/latest/fjall/enum.CompressionType.html
    https://docs.rs/fjall/latest/fjall/struct.Snapshot.html
    https://docs.rs/fjall/latest/fjall/type.SeqNo.html

## Appendix A — Work state machine

Suggested minimum states:

    pending -> leased -> completed
       |         |           |
       |         +-> pending  +-> published
       |              expiry
       +-> dead_letter
       +-> cancelled

Transitions:

- pending to leased: atomic Claim;
- leased to pending: lease expired or retryable Fail;
- leased to completed: Complete with valid fencing;
- completed to published: idempotent Publish;
- pending/leased to dead_letter: attempt limit or permanent error;
- any non-final state to cancelled: authorized and versioned operation.

Collections may add states, but must declare permitted transitions. The engine does not interpret editorial meaning.

## Appendix B — Example of radial policy

A news collection could declare:

- Freshness half-life: 60 minutes;
- public area: last 24 hours, maximum 10,000 items;
- work surface: condition hanging, ordered by urgency and time;
- minimum holding: 15 minutes;
- warm: 7 days;
- Cold: 180 days;
- archive: over 180 days;
- translations as records/staff projections;
- comments with distinct half-life;
- pinning for breaking news;
- high cost rebuild for results of expensive models.

These values are examples, not universal defaults.

## Appendix C — Fault matrix

| Event | Required behaviour |
|---|---|
| Crash before backend commit | No ACK, no mutations |
| Crash after durable commit but before RAM publication | Recovery or rereading exposes the mutation |
| Crash during commit | The backend does not make a partial batch visible |
| Record updated without change log | Contract violation; controlled stop |
| Crash during checkpoint | Previous checkpoint remains valid |
| Temporary backend file | Managed by documented backend recovery |
| Full disk | Throttle/read-only, no implicit deletion |
| GPU lost | CPU fallback, VRAM rebuild |
| Projection builder stopped | Canonical records available, visible staleness |
| Worker beyond lease | Fencing rejects Complete |
| Clock goes backward | Sequence preserves order; lease uses safety policy |
| Missing dictionary | Integrity error, no incomprehensible bytes returned |
| Corrupt cache | Discard and rebuild |
| Canonical state of backend corrupted | Controlled stop or explicit restore/repair |

## Appendix D — Glossary

- **canonical:** necessary to reconstruct the confirmed state;
- **derived:** disposable and reconstructable;
- **surface:** projection ready for low-latency consumption;
- **sector:** stage or purpose independent of tier;
- **radial score:** placement signal, never a correctness rule;
- **watermark:** highest source sequence applied;
- **generation:** immutable published version of a projection;
- **lease:** temporary possession of a task;
- **fencing token:** number that prevents an obsolete owner from writing;
- **compaction debt:** accumulated work required to maintain the engine;
- **durability receipt:** application-level proof for a mutation's durability;
- **storage class:** physical destination with budget and characteristics;
- **CPU reference:** semantically authoritative implementation available everywhere.
