# AProDB comparative benchmark

This crate evaluates the same key-value API across AProDB, SQLite, PostgreSQL, MySQL, MariaDB, and Redis/Valkey. It is separated from the main crate to prevent SQL and Redis drivers from becoming dependencies of the engine.

## Workload

- 50,000 deterministic keys and a 512-byte binary payload;
- `compressible` profile, similar to repetitive log/document fields;
- `random` profile, pseudo-random deterministic high entropy;
- ingestion in batches of 500, with a durable commit per batch;
- 50,000 point lookups against the hot dataset;
- 20 ordered scans of the `042` group, up to 1,000 rows;
- three repetitions; the published comparison uses the median value.

## Execution

First, create an empty database called `aprodb_bench` on the desired SQL servers. The default laboratory ports are PostgreSQL `55432`, MySQL `53306`, MariaDB `53307`, and Redis/Valkey `6379`.

```powershell
cargo run --release --manifest-path benchmarks/comparative/Cargo.toml -- `
  --backends aprodb,sqlite,postgres,mysql,mariadb,redis `
  --profiles compressible,random `
  --records 50000 --reads 50000 --payload-bytes 512 `
  --batch-size 500 --runs 3 --scan-repeats 20 --scan-limit 1000 `
  --workdir target/bench-lab/results
```

The runner writes `report.json` after each individual trial. If a backend fails, it retains the valid results already collected, records the error, and exits with a nonzero code.

To run only the embedded backends, which do not require servers:

```powershell
cargo run --release --manifest-path benchmarks/comparative/Cargo.toml -- `
  --backends aprodb,sqlite --profiles compressible,random
```

Server URLs can be modified with `--postgres-url`, `--mysql-url`, `--mariadb-url`, and `--redis-url`. See `--help` for all parameters.

## Correct interpretation

AProDB and SQLite run in the same runner process. PostgreSQL, MySQL, MariaDB, and Redis/Valkey use a single TCP connection over loopback. The test therefore measures the APIs as they are today, including protocol and SQL parsing where applicable, and does not attempt to isolate only the internal index.

The reported space is the AProDB directory, the SQLite file after checkpoint, `pg_total_relation_size`, the allocated InnoDB tablespace, or Redis/Valkey `used_memory_dataset`. The global WAL/redo/AOF files of servers are not included. The local results published are in [RESULTS.md](RESULTS.md).

A disposable Vast.ai Linux/Tesla T4 validation run from 2026-08-20 is documented in [VAST_2026_08_20.md](VAST_2026_08_20.md). It remains public-beta evidence, not an SLA.
