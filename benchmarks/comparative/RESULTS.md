# Comparative results — 19 August 2026

Local results from a release build: median of three repetitions, 50,000 records, a 512-byte payload,
batches of 500, 50,000 warm lookups, and 20 scans.
All 30 trials passed the correctness checks.

Machine: Windows 11 Home build 26200, Intel Core i5-1340P (12 cores/16 threads), 15.7 GiB RAM, SSD
NVMe WDSN740.
Versions: AProDB 0.1.0, SQLite 3.53.2, PostgreSQL 18.6, MySQL 26.7.0, MariaDB 12.3.2.

## Compressible payload

|Engine|Ingest ops/s|Lookup ops/s|Lookup p99 μs|Scan ops/s|MiB space|
|---|---:|---:|---:|---:|---:|
|AProDB|43,215|161,677|10.8|400.4|6.76|
|SQLite|6,157|13,107|223.6|2,455.9|29.61|
|PostgreSQL|32,157|974|1,630.2|310.2|31.10|
|MySQL|15,915|1,870|969.2|608.9|44.00|
|MariaDB|55,200|2,290|838.1|634.6|44.00|

In the internal AProDB format, 24.46 MiB of logical data becomes 4.28 MiB (`compression_ratio` about 0.175);
with WAL keys and frames, the directory occupies 6.76 MiB.
All 50,000 values choose Zstd.

## Pseudorandom payload

|Engine|Ingest ops/s|Lookup ops/s|Lookup p99 μs|Scan ops/s|MiB space|
|---|---:|---:|---:|---:|---:|
|AProDB|28,091|395,376|3.1|387.1|27.32|
|SQLite|6,032|13,559|230.0|2,565.3|29.61|
|PostgreSQL|32,918|966|1,610.8|277.5|31.10|
|MySQL|13,431|1,791|984.1|551.8|44.00|
|MariaDB|36,106|2,146|896.5|677.3|44.00|

Zstd cannot reduce data with high entropy: the adaptive policy retains all 50,000 raw values.
Internal metadata brings 24.46 MiB of logical data to 24.84 MiB stored; the complete directory occupies
27.32 MiB.
Lookup is faster than on the compressible profile because decompression is not required.

## Reading the results

- AProDB has the fastest point lookup: on the compressible profile it is about 12.3×
  SQLite and 70.6× the best of SQL servers; on the random about 29.2× SQLite and 184× the best of
  SQL servers. This advantage is due to the embedded/in-memory architecture and the absence of the SQL protocol.
- AProDB does not lead on durable ingestion: MariaDB is about 1.28× faster on compressible data and
  1.29× on random data. PostgreSQL surpasses AProDB on random profile.
- AProDB does not yet have an ordered index. `scan_prefix` scans the shards, while the other engines
  use the primary key B-tree. SQLite is about 6.1–6.6× faster in the scans; MariaDB and MySQL
  surpass AProDB in both tests.
- Compression provides a significant capacity advantage: on repeated data, the AProDB directory uses about 77% less space than SQLite, 78% less than PostgreSQL, and 85% less than the InnoDB tablespace.
- On random data, the space advantage is small, as expected: the policy avoids Zstd expansion and only pays for header, keys, and WAL.

## Limitations

This is not TPC-C, full YCSB, nor a maximum capacity measurement. It uses a single process, one connection per server, hot data, and no client contention. AProDB keeps the active dataset in RAM; SQL servers have their own cache and protocol. Joins, multi-table transactions, replication, recovery under fault, multi-process, concurrency, or vector GPU queries are not measured. The data demonstrate performance on this specific workload and do not constitute an SLA.

The raw report of the session is `target/bench-lab/results/session-1787111407/report.json`.
