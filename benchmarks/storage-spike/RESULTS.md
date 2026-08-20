# Local storage-spike results

Date: August 19, 2026.
Fjall 3.1.8, Rust 1.97.1, Windows 11 build 26200, Intel Core i5-1340P (12 cores/16 threads), 15.7 GiB
RAM, NVMe WDSN740 512 GB.

Each row represents a run with 4,000 mutations and 4,096,000 bytes of logical payload.
After compaction and reopening, four SSTs and a journal fragment were found; the recovered write
buffer was operationally empty, even though the Fjall counter was reconstructed.

| Policy | Data | Encoded payload bytes | Process I/O bytes | I/O / submitted bytes | Durable p50/p95/p99 μs | Mutations/s | Reopened size bytes | Recovery ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|
|AProDB Zstd|compressible|135,677|1,545,849|3.328|826 / 1,080 / 1,114|29,000|1,089,046|68|
|Fjall LZ4|compressible|4,116,000|5,238,468|1.179|1,147 / 1,633 / 1,643|34,145|4,916,881|78|
|Both|compressible|135,677|997,490|2.147|894 / 1,275 / 1,809|28,891|818,793|76|
|None|compressible|4,116,000|13,708,634|3.084|2,003 / 8,432 / 13,741|15,487|9,114,367|91|
|AProDB Zstd|random|4,116,000|13,708,634|3.084|1,768 / 6,344 / 11,691|14,153|9,114,367|108|
|Fjall LZ4|random|4,116,000|13,396,090|3.014|1,816 / 7,142 / 8,930|17,097|8,998,031|97|
|Both|random|4,116,000|13,396,110|3.014|1,829 / 6,723 / 7,688|14,446|8,998,028|88|
|None|random|4,116,000|13,708,634|3.084|1,894 / 5,556 / 7,979|15,875|9,114,367|91|

## Change-log cost

- minimum VersionRef event: 92,000 bytes, equal to 2.246% of the logical payload;
- 16-byte synthetic delta: 64,000 bytes, equal to 1.563%;
- SelfContained: 227,677 bytes with a compressible Zstandard payload, or 4,208,000 bytes when the
  payload remains raw;
- bytes passed to the backend: 464,557 for the compressible Zstandard case and 4,444,880 in the other cases.

The payload is not duplicated in the VersionRef path: version and head/event share the logical identity of the same immutable copy. The engine test also verifies the exact version is recovered after 300 updates, compaction, restart, and GC for Delta, VersionRef, and SelfContained.

## Interpretation

On the compressible profile, adaptive Zstandard is decisive; physical LZ4 alone reduces the space but not the number of logical bytes delivered. On the random profile, Zstandard selects raw mode, and LZ4 offers only a marginal space gain. Double compression was measured, but the final payload decision was deferred to Milestone 5: for Milestone 1, the backend used LZ4 for keyspaces and did not yet enable an AProDB logical codec.

These numbers are exploratory and not comparable with the results of external servers in `benchmarks/comparative`.
