# AProDB Vast.ai validation handoff

Use this prompt in the next Codex session opened in the AProDB repository. Speak to the user in Italian, but keep repository content, commands, reports, diagrams, and benchmark labels in English.

## Objective

Validate the public-beta AProDB revision on disposable Vast.ai hosts. Run CPU correctness gates, comparative database benchmarks, optional GPU compute checks, and Supabase-derived sanitized workload tests. Preserve reproducible evidence, publish only reviewed results, and destroy paid resources immediately after each job.

## Cost and safety gates

- Do not start any Vast.ai instance until the benchmark archive, commands, and teardown command are ready.
- Keep each database test under the user-approved cap of EUR 5 per active GPU/instance test.
- Prefer one host per database when useful, but only when the job is ready to upload and run.
- Do not expose database ports publicly. Bind services to loopback or a private interface.
- Do not print credentials, connection strings, API tokens, raw dumps, or secrets.
- Supabase is read-only source data. Export into a sanitized JSONL/CSV workload before upload.
- Replace detected keys, tokens, and passwords with deterministic placeholders such as `key00001` and `pass00001`.
- Do not use the ex44 emeroteca server unless the user explicitly reauthorizes it.

## Repository revision

Use `origin/main` unless the current session has produced a newer commit specifically for the benchmark harness. Record:

- commit SHA;
- AGPL/Apache split from `LICENSING.md`;
- public beta status;
- Rust version;
- OS, CPU, RAM, disk type, GPU model if present.

## Local preparation before provisioning

Run locally:

```powershell
cargo fmt --all --check
cargo clippy --workspace --all-targets --no-default-features -- -D warnings
cargo test --workspace --no-default-features
cargo run --release --manifest-path benchmarks/comparative/Cargo.toml -- `
  --backends aprodb,sqlite --profiles compressible,random `
  --records 5000 --reads 5000 --payload-bytes 512 `
  --batch-size 500 --runs 1 --scan-repeats 5 --scan-limit 500 `
  --workdir target/bench-lab/smoke-local
```

If these fail, fix the repository first and do not start paid hosts.

## Comparative benchmark matrix

Target backends:

- AProDB;
- SQLite;
- PostgreSQL;
- MySQL;
- MariaDB;
- Redis or Valkey.

Use the existing runner:

```bash
cargo run --release --manifest-path benchmarks/comparative/Cargo.toml -- \
  --backends aprodb,sqlite,postgres,mysql,mariadb,redis \
  --profiles compressible,random \
  --records 50000 --reads 50000 --payload-bytes 512 \
  --batch-size 500 --runs 3 --scan-repeats 20 --scan-limit 1000 \
  --postgres-url postgresql://postgres@127.0.0.1:55432/aprodb_bench \
  --mysql-url mysql://root@127.0.0.1:53306/aprodb_bench \
  --mariadb-url mysql://root@127.0.0.1:53307/aprodb_bench \
  --redis-url redis://127.0.0.1:6379/0 \
  --workdir target/bench-lab/vast
```

Record server configuration, durability settings, versions, and whether Redis/Valkey uses AOF/fsync. Do not claim equality of durability unless it was configured and verified.

## Supabase-derived workload

Create a read-only export from Supabase into sanitized records:

- table and column names may be retained if not sensitive;
- credential-like values must be replaced before upload;
- retain enough distribution and payload size to represent the workload;
- record sanitization counts and checksums of the sanitized files only.

Run AProDB import/reopen/verify and, where suitable, load the same sanitized key/value representation into the comparison backends. Do not benchmark unsanitized data.

## GPU checks

If the selected Vast.ai host has a supported GPU:

```bash
cargo test --workspace --features gpu
cargo run --release -p aprodb-engine --example compute_benchmark
```

GPU failure must not invalidate CPU correctness. Report CPU/GPU equivalence, fallback behavior, and any driver/backend issue.

## Reports

Write an English report under `docs/` or `benchmarks/comparative/` with:

- commands;
- host specs;
- database versions and configs;
- raw result file paths and checksums;
- pass/fail list;
- p50/p95/p99 and throughput;
- memory/disk notes;
- all failures and limitations;
- teardown confirmation for every paid host.

Keep the visible conversation concise: do not print diffs, patches, full file bodies, credentials, or long logs.
