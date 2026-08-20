# AProDB comparative benchmark

This crate evaluates the same key-value API across AProDB, SQLite, PostgreSQL, MySQL, and MariaDB. It is separated from the main crate to prevent SQL drivers from becoming dependencies of the engine.

## Workload

- 50,000 deterministic keys and a 512-byte binary payload;
- `compressible` profile, similar to repetitive log/document fields;
- `random` profile, pseudo-random deterministic high entropy;
- ingestion in batches of 500, with a durable commit per batch;
- 50,000 point lookups against the hot dataset;
- 20 ordered scans of the `042` group, up to 1,000 rows;
- three repetitions; the published comparison uses the median value.

## Execution

First, create an empty database called `aprodb_bench` on the desired servers. The default laboratory ports are PostgreSQL `55432`, MySQL `53306`, and MariaDB `53307`.

```powershell
cargo run --release --manifest-path benchmarks/comparative/Cargo.toml -- `
  --backends aprodb,sqlite,postgres,mysql,mariadb `
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

Server URLs can be modified with `--postgres-url`, `--mysql-url`, and `--mariadb-url`. See `--help` for all parameters.

## Correct interpretation

AProDB and SQLite run in the same runner process. PostgreSQL, MySQL, and MariaDB use a single TCP connection over loopback. The test therefore measures the APIs as they are today, including protocol and SQL parsing, and does not attempt to isolate only the internal index.

The reported space is the AProDB directory, the SQLite file after checkpoint, `pg_total_relation_size`, or the allocated InnoDB tablespace. The global WAL/redo files of SQL servers are not included. The local results published are in [RESULTS.md](RESULTS.md).
