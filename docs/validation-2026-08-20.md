# Validation report — 2026-08-20

This report records observed results from the public-beta validation session. It does not represent a stability guarantee for experimental features. Commands are re-run after the final documentation and localization changes before publication.

## Build and test gates

| Gate | Result | Notes |
|---|---|---|
| `cargo fmt --all --check` | Passed | Workspace formatting gate. |
| Clippy, workspace, all targets, CPU-only, warnings denied | Passed | `--no-default-features`. |
| Tests, workspace, CPU-only | Passed | 26 suites; two deliberately ignored stress/optional cases. |
| Clippy, workspace, all targets, default features, warnings denied | Passed | Includes the optional compute boundary. |
| Tests, workspace, default features | Passed | 26 suites; three deliberately ignored stress/optional cases. |
| Dataset-larger-than-budget stress test | Passed | 129 MiB canonical dataset with a 128 MiB configured memory budget. |
| Long encrypted backup/restore/rekey test | Passed | 2,000 Durable writes and four verified backup/restore cycles. |
| Forced CPU/GPU equivalence test | Passed | Intel Iris Xe; the CPU path remains the reference. |

The ignored cases were invoked explicitly where hardware or runtime cost made them unsuitable for the ordinary suite. No ignored failure was relabeled as a pass.

## Server black-box smoke test

An isolated server instance was started with separate data, backup, TCP, and administrative endpoints. Readiness, health, statistics, online backup, orderly shutdown, offline data verification, backup verification, and process exit status all passed. The test used temporary local paths and did not expose a service publicly.

## PostgreSQL import validation

The source was accessed using strict-host-key SSH and a read-only, repeatable-read PostgreSQL transaction. Credentials remained inside the remote container and were neither printed nor copied into the repository.

| Case | Result | Acceptance evidence |
|---|---|---|
| Primary-key table | Passed | 82 exported/imported rows; all heads verified and found after reopen. |
| Table without a primary key | Passed | 588 rows mapped by snapshot `tableoid` + `ctid`; all heads verified and found after reopen. |
| `vector(384)` column | Passed | 100 lossless JSON rows; all heads verified and found after reopen. |
| Exact large JSON numeric | Passed | Unit test preserves a number beyond JavaScript's safe-integer range byte-for-byte. |
| Backslashes in source text | Passed after correction | The first export exposed COPY text escaping; CSV control delimiters now preserve valid JSON. |
| Truncated stream | Passed | The importer rejects a stream without its complete frame. |
| Inconsistent table count | Passed | The manifest, completion count, and imported count must match. |
| Atomic publication | Passed | Complete input is renamed into place; truncated input never appears at the destination. |
| Bounded input, batching, and disk | Passed | 17 MiB frame limit, 32 MiB total buffered batches, 64 GiB data cap, 16 GiB compaction cap, and 8 GiB free-space reserve by default. |

A full-table representative import and a bounded all-table schema/type sample are recorded here only after they complete. A full production-database clone is not started merely to improve a benchmark number: its source load, long MVCC snapshot, destination capacity, and expected runtime must be acceptable first.

## Known gaps in this session

- `cargo-fuzz` is not installed, so the LibFuzzer target was not executed. The repository's deterministic golden/property/fuzz-smoke coverage did run through the ordinary test gates.
- No NVIDIA device is present. The optional GPU equivalence test passed on the available Intel adapter.
- The PostgreSQL importer is one-way and not resumable. Interrupted work remains isolated in an unpublished staging directory.
- This report is a correctness and operability record, not a comparative performance claim. Embedded and networked database measurements remain clearly separated.