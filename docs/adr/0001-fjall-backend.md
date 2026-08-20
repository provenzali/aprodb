# ADR-0001 — Fjall as the backend for the single-node vertical

- State: accepted with experimental constraints
- Date: August 19, 2026
- Scope: Milestones 0.5 and 1; reviewed during Milestone 7

## Background

AProDB requires atomic batches across records, immutable versioning, change events, and catalog; Durable/Relaxed modes; ordered scans; datasets larger than RAM; keyspace-level compression; recovery and compaction. The AProDB change log must not duplicate or interpret the WAL, manifest, or physical segments of the backend.

Fjall documents a database with multiple LSM keyspaces, atomic batches, lexicographic ordering, and configurable persistence, and prohibits multiple processes from opening the same directory. AProDB adds its own exclusive lock because this is a server invariant, not a backend detail:
[README Fjall](https://github.com/fjall-rs/fjall/blob/main/README.md),
[OwnedWriteBatch](https://docs.rs/fjall/3.1.8/fjall/struct.OwnedWriteBatch.html),
[PersistMode](https://docs.rs/fjall/3.1.8/fjall/enum.PersistMode.html).

## Decision

Fjall 3.1.8, exact pin, is accepted behind `StorageBackend`. Acceptance is valid for the single-node experimental path and does not imply production suitability.

- `OwnedWriteBatch` provides atomicity across keyspaces.
- `SyncAll` implements Durable; `Buffer` implements Relaxed and the buffered phase of the group commit.
- Records, Versions, Events, Catalog, and Idempotency are separate keyspaces.
- From Milestone 5, the canonical keyspace uses logical Raw/Zstandard payloads and does not apply LZ4 physical compression by default; metadata, change log, and surfaces retain LZ4. The decision and four-mode matrix are in [ADR-0002](0002-logical-compression.md).
- Explicit compaction uses only Fjall APIs and waits for observable flushes with timeout; AProDB does not interpret SSTs, journals, or manifests.
- Fjall does not yet offer a stable native checkpoint. AProDB creates a logical checkpoint in a new directory with Durable watermark and verifies it at reopen.
- Long-lived MVCC snapshots are not used for retention. Immutable versions and consumer watermarks are AProDB application data.
- The 0.1 format is recognized and rejected: no automatic opening as format 1.x.

Redb and RocksDB remain as fallbacks. They are not subjected to parallel spikes as long as Fjall meets the criteria or until an open risk blocks a milestone.

## Evidence

The suite verifies in-process and cross-process locks, atomic batch after reopening, limited scans, flush/major compaction, recovery, logical checkpoint, limits, fault injection, and retention for all three modes. The quantitative spike and the limits of the I/O metric are in [`benchmarks/storage-spike`](../../benchmarks/storage-spike/RESULTS.md).

The local run measured 4,096,000 payload bytes and 92,000 bytes of minimal VersionRef event.
With compressible data, the Zstandard adaptive code produced 135,677 bytes; with random data it
retained raw.
All eight variants passed compaction, reopen, and payload verification.

## Risks and mitigations

- The upstream issue with failures during batch journal writes was explicitly reported against 3.1.8. The AProDB backend therefore enters a fail-closed state after any commit or persist error and requires reopening: [issue Fjall #308](https://github.com/fjall-rs/fjall/issues/308).
- At the time of the spike, strict recovery mode capable of distinguishing internal corruption from a truncated tail remained an upstream request. Milestone 7 therefore added kill tests and corruption tests on throwaway copies: [issue Fjall #311](https://github.com/fjall-rs/fjall/issues/311).
- A bug has been reported regarding sealed journals with inactive keyspaces. AProDB does not use `clear`, maintains limits and metrics on the journal, and requires explicit maintenance, but must add a soak test: [issue Fjall #288](https://github.com/fjall-rs/fjall/issues/288).
- At the time of the spike, a native checkpoint remained an upstream request. The AProDB logical checkpoint is more expensive and is distinct from the verified operational backup added in Milestone 7: [issue Fjall #52](https://github.com/fjall-rs/fjall/issues/52).
- The Fjall APIs used for counters and major compaction are experimental. The exact pin prevents silent upgrades; each version change requires gating and a review of this ADR.

## Criteria for reopening the decision

The decision must be reconsidered if a Durable kill-test fails, recovery accepts internal corruption without diagnosis, the journal exceeds its budget, compaction APIs are removed, the logical checkpoint does not scale, or a milestone requires a capability that cannot be emulated without violating the backend boundary.
