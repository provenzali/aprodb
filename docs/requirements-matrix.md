# AProDB 1.x Implementation Matrix

This matrix links the normative requirements of `paper.md` to the verified state, the responsible milestone, and the minimum acceptance test.
"Prototype" does not equate to a completed 1.x feature.

|Requirement|Paper|Verified current state|Milestone|Acceptance test|
|---|---|---|---|---|
|Baseline Git and audited distribution|§33 M0|local audit completed; `main` initialized; public repository published with an AGPL core and Apache-licensed integration boundary|0|staging audit, secret/large-file/license scan, CPU-only gate, and remote verification|
|Acyclic workspace and compatibility facade|§9.4|types, storage, compute, and engine crates plus the `aprodb::v1` facade; acyclic dependency graph|0|`cargo metadata`, workspace build, and dependency-graph check|
|Complete identity model and stable types|§7|tenant/namespace/collection/partition/key identity, versions, records, and receipts implemented|0|unit/property/golden tests for identity, versions, and records|
|Error model and shared limits|§7, §10.4, §26|typed errors and shared limits applied to configuration, payload, batch, and in-flight work|0|invalid-configuration tests and rejected allocations|
|Boundary compute CPU/GPU and columnar layout|§15|trait CPU/accelerator, batch f32 contiguous and validity bitmap; wgpu isolated from CPU build|0/6|build CPU-only and GPU; layout/alignment and null verified|
|Storage contract with explicit capabilities|§16.0|contract implemented with capabilities, stats, limits and fault injector|0.5|common contractual suite and capacity matrix|
|Limited Fjall spike|§16.0, §33 M0.5|Fjall 3.1.8 tested on atomic batch, sync, scan, reopen, compaction and logical checkpoint|0.5|atomic batch, sync, scan, reopen, checkpoint and >RAM|
|Change log cost and compression per keyspace|§17.2, §19.6|eight workloads measured and documented in `benchmarks/storage-spike`|0.5|four-mode benchmark covering bytes, latency, throughput, space, and compaction|
|Retention Delta/VersionRef/SelfContained|§17.2|implemented with immutable versions, watermark consumer and GC|0.5/1|slow consumer, multiple updates, restart, GC and exact version|
|Backend choice ADR|§16.0, §33 M0.5|ADR-0001 approves Fjall with pin, fail-closed and M7 review|0.5|documented and reproducible exit criteria|
|Exclusive directory lock|§9.1–9.2|implemented and tested in same process and subprocess|1|second open/process rejected, release after drop/crash|
|Format 0.1 recognition|§29.2|golden 0.1 present; 1.x engine always rejects legacy directories; import one-shot operates offline on verified copies|1/7|golden 0.1, rejection on open, source hash unchanged and imported type mapping|
|Versioned catalog|§18.1|atomic v1 logical catalog, with generation, policy and watermark|1|mutate/reopen/recovery and atomic transitions|
|Record and change event in the same batch|§11, §17.2|version/head/event/catalog in an `OwnedWriteBatch` cross-keyspace|1|fault injection before/after commit and invariant checking|
|Single logical writer per shard|§10.1|mutex writer per shard and separate catalog serialization|1|sequential model, concurrent CAS, sequence never reused|
|Put/Get/Delete/CAS/AtomicBatch|§8.1, §10|implemented; AtomicBatch rejects different partitions and duplicate identities|1|linearity per key and batch indivisible within partition|
|Durable/Relaxed and receipt|§11.1|implemented with SyncAll/Buffer, durable watermark and receipt|1|no Durable ACK lost; watermark verified|
|Limited group commit|§11.2|limited channel, window and byte cap; zero triggers SyncAll per request|1|zero window per request; window/byte cap and forced sync|
|Checkpoint and deterministic recovery|§27|reopen, logical checkpoint, kill after Durable ACK, and injected open failure verified; narrower physical-corruption cases remain open|1|reopen, tail fault, publication fault, and checkpoint fault|
|Key/record/batch/memory/queue/disk budgets|§10.4, §13, §26|limits applied; M7 adds a data quota, free-space reserve, and temporary compaction budget|1/7|threshold and backpressure tests before mutation, compaction, and copies|
|Golden/property/fuzz testing of logical formats|§31|golden/proptest pass also for `APRX`, catalog/compression dictionary; fuzz tests storage formats and protocol messages, with Windows smoke and LibFuzzer on non-Windows|1+|stable golden files, proptest and fuzz targets compile|
|Framed and versioned Protobuf protocol|§23|canonical schema and Prost types, handshake magic/major/role, bounded `u32` frames, and golden wire data|2|golden wire data, oversized frames, and version negotiation|
|Central server, local transport, and TCP|§9, §23.2|single daemon with TCP plus Windows named pipes or Unix sockets; end-to-end server/CLI smoke test|2|multi-client integration, local transport, and exclusive directory lock|
|Rust client, deadline, and request ID|§23.1|multiplexed asynchronous and blocking clients; total deadline, out-of-order correlation, and typed receipts|2|deadlines, multiple in-flight requests, and receipt correlation|
|Data/admin authentication, quotas, and shutdown|§23–26|separate tokens, constant-time comparison, endpoint roles, global limits, and tenant quotas by byte/rate/in-flight/compute work, retry-after, metrics, and drain|2/7|permissions, failed authentication, tenant quotas, backpressure, and deterministic shutdown|
|Observable backend segments/flush/compaction|§16, §18, §20|Fjall stats expose disk, write buffer, journal, tables, flush and compaction without interpreting private files|3|dataset beyond budget, compaction/reopen and capability-aware metrics|
|Cache with separate budgets|§13|metadata/object/compressed/negative caches sharded and limited; scans and maintenance do not populate the object cache|3/5|working set beyond cache, invalidation, eviction and total reserve respected|
|Radial descriptor and explainable placement|§5–6|persistent descriptor/policy, hysteresis, minimum permanence, pin and admin explanation without effect on the object cache|3|score, hysteresis, pin/TTL, restart and `ExplainPlacement`|
|TTL and time index|§7, §18.2|atomic TTL index with record/version; reads hide expired and admin sweep deletes with fencing|3|update TTL, expiration, stale entries, reopen and verification|
|Storage classes and datasets larger than memory|§16.2, §33 M3|persistent logical classes; Fjall physical-tiering capability declared absent; 129 MiB verified with a 128 MiB budget|3|dedicated workload, sync, compaction, reopen, and exact reads|
|Append and workflow lease/fencing|§8.3, App. A|persistent Append/Claim/Heartbeat/Complete/Fail/Publish; random lease, monotonically increasing fencing token, UTC+safety recovery, and active limits|4|concurrent claim, replay, heartbeat, stale completion, dead-letter, and restart|
|Mutation idempotency|§8.1, §10.3|32-byte hash, fingerprint, exact receipt/lease, expiry index, and bounded purge; stored in the mutation batch|4|retry/restart returns the same outcome, reuse with different parameters fails, and expiry permits a new outcome|
|SubscribeChanges and watermark|§17.2, §22|pull paged for shard/collection, global cursor, indivisible batch and `ChangeLogGap` explicit|4|indivisible batch, slow consumer, compaction/restart and gap detected|
|Incremental projections/surfaces|§22|separate generational work/read surfaces for one collection, status filters, record/JSON output, caps, watermark/staleness, and rebuild; generic filters/transformations remain open|4|immutable generations, atomic publication, restart, staleness, and rebuild after a gap|
|Raw/Zstd logical compression by tier|§19|`APRX` frame with adaptive selection for hot/warm/cold/archive, Raw surfaces, content-type skipping, and Durable per-collection policy; legacy `APRC` remains readable|5|tier policy, Zstd/Raw, checksum, ratio, exact-version retrieval, and recovery|
|Bounded pool and versioned dictionaries|§19.3–19.4|pool size 1–64 and bounded scratch space; bounded training with a validation gate, atomic publication, checksum, and conservative retention|5|backpressure, missing/corrupt dictionary, TCP server, and reopen|
|Compressed/decompressed cache|§13.3, §19.6|compressed frame cache and decoded record with separate budget/metric/validation|5|separate budget, hit/miss and invalidation by version|
|Coordinated physical compression and cost|§19.5–19.6|canonical data uses no physical LZ4 by default; metadata/events/surfaces use LZ4; measured four-mode matrix in `benchmarks/compression`|5|ratio, p50/p95/p99, throughput, CPU, RAM, I/O, space, compaction, and recovery|
|CPU reference batch computation|§15, §21.4|exact vector/top-k 1.x through the engine, protocol, and client; dot/cosine, limits, and deterministic tie-breaking|6|CPU-only, mixed collections, reopen, and TCP server|
|Total-cost scheduler|§15.3|explicit formula with transfer, queue, launch, compute, synchronization, and risk; queue/byte budgets, micro-batching, and override|6|cost, budget, batching, timeout, fallback, and metrics|
|Optional wgpu and rebuildable VRAM|§15|optional WGSL, LRU cache by projection/generation/schema, circuit breaker, and reset; relative tolerance 1e-4|6|CPU/GPU equivalence, hit/invalidate/rebuild, simulated fault/OOM, and crossover benchmark|
|Verified backup/restore|§27.2–27.3|online checkpoint reopened/verified, BLAKE3 inventory and manifest; restore rejects existing destinations and rechecks catalog/watermark|7|tamper detection, restore to a separate destination, online server backup, and repeated long-running gates|
|Explicit Verify and Repair|§27.4|`verify` pages through every logical keyspace; repair reconstructs indexes/surfaces only on a copy, with literal confirmation and JSON reports|7|derived corruption detected, source unchanged, and copy verified|
|TLS and mTLS|§23.2, §24|Rustls/Tokio-Rustls over TCP, server CA, and optional client certificate; non-loopback plaintext fails closed|7|valid mTLS, anonymous peer rejected, and local sockets unchanged|
|At-rest encryption and rotation|§19.5, §24.4|XChaCha20-Poly1305 on values in every keyspace, contextual AAD, external/redacted keyring, and copy-only rekey|7|plaintext absent; wrong key and tampering detected; reopen/checkpoint/rekey verified|
|Administrative audit|§24.2|Durable Attempted/outcome events with sequence, principal, and target hash; paginated admin endpoint included in backup/verify|7|TCP mutation, role denial, restart, and logical golden/fuzz tests|
|Tenant and disk quotas|§24.2, §26|admission by byte/rate/in-flight/vector work; data quota, free-space reserve, and temporary compaction/copy estimate|7|pre-dispatch/pre-mutation rejection and deterministic retry-after|
|Upgrade and import 0.1|§29|no in-place; offline import one-shot preserves raw, uses reader-copy, durable batch, verification and rename; future unknown formats rejected|7|snapshot+WAL with delete, all types, unaltered source hash and verified destination|
|Long-running tests and packaging|§31, §33 M7|explicit gate: 2,048 encrypted Durable writes, four backup/restore cycles, and one rekey; `aprodb-types` package verification passes, and workspace packaging passes with `--no-verify`; full workspace verification is blocked by unpublished internal crates|7|explicit ignored test run and documented package-verification limitation|
|Replication|§28, M8|outside the initial scope|separate Milestone 8|not declared available in Milestones 0–7|

The matrix is updated only when a gate is actually passed; a row does not become "complete" solely due to the presence of an API or scaffold.
