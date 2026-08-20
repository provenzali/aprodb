# Local results — 19 August 2026

Command: `cargo run -q -p aprodb-engine --example compression_benchmark` (debug profile, Windows, local filesystem). These are functional and diagnostic results, not numbers intended for product comparison.

## Compressible payload

| Mode | Encoded/logical ratio | p50/p95/p99 Durable μs | Records/s | CPU ms | I/O read/write bytes | Final RSS bytes | Post-compaction disk bytes | Recovery ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| AProDB Zstd | 0.006341 | 18.640 / 20.442 / 20.442 | 840.02 | 625 | 239,375 / 720,217 | 16,355,328 | 67,303,249 | 98 |
| Fjall LZ4 | 1.000000 | 31.044 / 48.878 / 48.878 | 459.54 | 1,032 | 145,127 / 537,139 | 15,732,736 | 67,212,166 | 147 |
| both | 0.006341 | 18.611 / 20.505 / 20.505 | 838.83 | 656 | 82,787 / 417,210 | 17,842,176 | 67,156,829 | 136 |
| none | 1.000000 | 27.860 / 33.481 / 33.481 | 546.61 | 1,000 | 1,353,158 / 3,957,286 | 16,240,640 | 68,376,895 | 197 |

Zstandard compressed all 256 payloads: 1,049,600 logical bytes were reduced to 6,655 bytes.
The cumulative codec time was 109,589 μs with Zstd alone and 110,432 μs with double
compression.

## Pseudorandom payload

| Mode | Encoded/logical ratio | p50/p95/p99 Durable μs | Records/s | CPU ms | I/O read/write bytes | Final RSS bytes | Post-compaction disk bytes | Recovery ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| AProDB Zstd adaptive | 1.000000 | 30.985 / 55.777 / 55.777 | 426.55 | 1,109 | 1,353,094 / 3,957,161 | 18,558,976 | 68,376,834 | 243 |
| Fjall LZ4 | 1.000000 | 37.438 / 53.667 / 53.667 | 372.12 | 1,343 | 1,193,850 / 3,681,624 | 16,306,176 | 68,260,638 | 179 |
| both | 1.000000 | 39.190 / 62.993 / 62.993 | 324.23 | 1,437 | 1,193,817 / 3,681,560 | 18,649,088 | 68,260,607 | 199 |
| none | 1.000000 | 26.437 / 41.130 / 41.130 | 520.80 | 875 | 1,353,158 / 3,957,286 | 16,252,928 | 68,376,895 | 108 |

The adaptive policy retained Raw for all 256 pseudorandom payloads and recorded 256 fallbacks. The Zstandard attempt thus incurs a measurable cost with no benefit; content-type prefixes and minimum thresholds are used to avoid this work when the format is already compressed or known to be incompressible.

## Interpretation


- The logical format allows determining whether and how each payload is compressed, verifying checksums and length, and retaining Raw when Zstandard is not advantageous.
- Double compression is not the default for the canonical keyspace: data from this run does not demonstrate a consistent benefit that would justify the added cost.
- The physical metrics at 64 MiB are dominated by Fjall preallocation. Repeated releases and larger datasets are required for production tuning decisions.
