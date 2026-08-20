# AProDB Runpod validation handoff

Use this prompt in the next Codex session opened in the AProDB repository. Speak to the user in Italian, but keep repository content, commands, reports, diagrams, and benchmark labels in English.

## Mission

Validate the exact public-beta revision from `origin/main` on a disposable Runpod host. Run fair CPU and optional GPU tests, compare like-for-like durability and deployment modes, preserve reproducible evidence, publish only reviewed results, and remove paid resources after the agreed teardown gate.

Do not install AProDB or any test software on the source PostgreSQL server. That server may only be accessed through a read-only account/export path explicitly approved by the user.

## Mandatory startup

1. Load and follow the `clean-session-output` skill when available.
2. Read `AGENTS.md`, `paper.md`, `manual.md`, `diary.md`, `README.md`, `docs/validation-2026-08-20.md`, `docs/postgresql-import.md`, and the benchmark documentation.
3. Inspect Git status and preserve unrelated user changes.
4. Fetch `origin/main`, record the exact commit SHA, and verify that the GitHub CPU-only CI for that revision is green. Do not test an uncommitted or ambiguous revision.
5. Create and maintain a concise operational plan. Keep visible output suitable for screen recording: no raw patches, file bodies, secrets, or long logs.

## Paid-resource approval gate

Before creating any resources, display the current Runpod dashboard pricing and require the user to explicitly approve all the following items in a single decision:

- maximum total expenditure;
- GPU model and region/availability;
- size of the local ephemeral disk;
- whether a persistent network volume is required;
- automatic teardown policy after evidence has been copied.

Initial recommendation, to be revalidated against the live dashboard: one on-demand NVIDIA L4 Pod, approximately 12 vCPUs, 50 GB RAM, 24 GB VRAM, and 250–300 GB of local ephemeral disk. As of 2026-08-20, the indicative compute price was USD 0.39/hour; storage is billed separately. Prefer on-demand rather than interruptible capacity for durability and recovery tests. Official references:

- <https://docs.runpod.io/pods/pricing>
- <https://www.runpod.io/pricing>
- <https://docs.runpod.io/storage/network-volumes>

Do not create a Pod, volume, public endpoint, or any other billable resource without that explicit, up-to-date approval. Report actual costs and never silently exceed the specified cap.

## Storage and security

- Use the Pod's local ephemeral disk as the primary database test disk. Do not use a network volume for latency measurements; Runpod documents typical network volume throughput of roughly 200–400 MB/s, which may become the benchmark bottleneck.
- Use a network volume only for optional staging or preserved result artifacts, and clearly identify its use in every result.
- Do not expose database ports publicly. Bind benchmark servers to loopback or a private interface, and use SSH tunneling only when necessary.
- Pass credentials via the approved secrets mechanism or short-lived environment variables. Never print credentials, write them to the repository, embed them in an image, or copy the local key directory.
- Access the source PostgreSQL database read-only, and only through a repeatable-read export. Never install packages, create tables, change configuration, or run AProDB on that source host.
- Scan result artifacts for IP addresses, credentials, machine-specific paths, emails, and private source data before publication.

## Reproducible environment

Use a minimal supported Linux image with the GPU driver/CUDA layer required by wgpu, Rust stable edition 2024, and pinned versions of every comparator. Record:

- Runpod Pod/GPU/CPU/RAM/disk configuration and region;
- Linux kernel, filesystem, mount options, Rust/Cargo, compiler, driver, CUDA/Vulkan, and wgpu adapter information;
- exact AProDB commit and feature flags;
- exact comparator versions and durability settings;
- dataset generator/export command, seed, logical size, row count, and checksums;
- warm-up, repetitions, concurrency, run duration, and whether caches were cold or warm.

Keep AProDB server mode separate from embedded mode. Never compare an in-process AProDB result with a networked competitor when making claims about database superiority.

## Correctness gates before performance

Run the repository's documented commands, adapting paths for Linux:

1. `cargo fmt --all --check`
2. `cargo clippy --workspace --all-targets --no-default-features -- -D warnings`
3. `cargo test --workspace --no-default-features`
4. default-feature workspace clippy and tests
5. explicit GPU tests when the backend is available
6. deterministic server/client, reopen/recovery, resource-budget, backpressure, slow-consumer retention, backup/restore, and exact VersionRef tests

Test CPU-only first and treat it as the semantic reference. Then enable GPU and require declared numerical equivalence, bounded timeout, circuit-breaker behavior, and CPU fallback. A GPU failure must not damage storage or stop the server.

For the PostgreSQL importer, run a small heterogeneous sample first. Check row counts, logical bytes, events, reopen verification, atomic publication, no-primary-key identity handling, and rejection of truncated input before attempting a full export.

## Benchmark matrix

Run all systems on the same Pod, filesystem, and local disk. Do not run competing databases simultaneously. At minimum, distinguish:

- AProDB client/server over loopback TCP, CPU-only;
- AProDB client/server over loopback TCP with GPU enabled for eligible compute;
- AProDB embedded mode, reported in a separate table;
- PostgreSQL server mode;
- Valkey or Redis server mode only after recording the exact version and license;
- a raw embedded backend comparison, only in a separately labeled embedded table.

Use equivalent durability classes. The strict class must require acknowledgement only after durable persistence (for example, AProDB `Durable` with a zero group-commit window, PostgreSQL synchronous commit, and a comparator setting with an equivalent fsync contract). Report relaxed/asynchronous modes separately, and never mix them in one ranking.

Cover at least:

- Put/Get/Delete and mixed read/write workloads;
- 1 KiB records and larger JSON/document payloads;
- concurrency 1, 4, 16, and a bounded higher level appropriate to 12 vCPU;
- cold and warm cache runs;
- a sustained run long enough to expose compaction effects;
- a dataset larger than the configured cache/RAM budget when disk and spend permit;
- checkpoint, clean restart, abrupt process recovery, and recovery duration;
- exact vector search on CPU versus GPU, including host/device transfer, cold start, warm execution, and break-even size;
- compression modes as already defined by the repository: AProDB Zstandard only, backend compression only, both, and neither.

Measure p50/p95/p99 latency, sustained throughput, CPU, peak and steady RAM, GPU utilization/VRAM, bytes read/written, physical space, write amplification when observable, compaction time, recovery time, and error/backpressure counts. Use multiple repetitions and retain raw machine-readable samples. Do not estimate missing numbers or turn one run into a general superiority claim.

## Live-data validation

Use the existing bounded exporter/importer scripts with their documented frame limits. Export through the read-only path into local Pod storage; do not allow the AProDB importer to connect with administrative database privileges. Confirm that there is enough available space for source data, AProDB data, compaction headroom, and reserve before performing a full copy. Stop safely if the projected footprint or expenditure would exceed the approved limit.

Do not publish source rows. Results may include only aggregate counts, sizes, timing, resource metrics, schema-shape summaries, and non-sensitive diagnostics.

## Evidence, GitHub, and teardown

Write an English, environment-specific report under the existing benchmark/documentation structure. Include the commands or scripts required to reproduce the run, raw-result checksums, all failures, ignored tests with reasons, and limitations. Clearly label AProDB as public beta.

Before publishing, run formatting, clippy, tests, Markdown/link checks, residual-language checks, and a secret/large-file scan. Use the GitHub publication workflow only with the `provenzali/aprodb` repository and the correctly authenticated `provenzali` account. Never force-push. Do not use a connector authenticated as another owner.

After verified result artifacts are copied out and their checksums confirmed, follow the teardown policy approved at the paid-resource gate. Stop and delete the Pod and any disposable volume only within that explicit authorization. Verify that billing resources are removed and report the final cost. If deletion was not authorized, stop and ask rather than assume.

Finish with a concise separation of: passed correctness tests, performance results, GPU-specific results, experimental findings, failures, publication status, destroyed or retained resources, actual cost, and remaining risks.
