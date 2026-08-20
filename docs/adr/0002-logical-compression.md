# ADR-0002 — Adaptive logical compression of canonical payloads

- Status: accepted for Milestone 5, with experimental tuning
- Date: August 19, 2026
- Scope: single-node canonical payloads; reviewed during Milestone 7

## Background

AProDB must apply Raw/Zstandard policies by tier, keep dictionaries versioned and recoverable, and
clearly distinguish logical format from physical backend compression.
A record must remain decodable after a restart, and one version must not depend on the current payload.
Pre-serialized surfaces, metadata/change log, blobs, and indexes each require separate policies.

## Decision

- New canonical records use the `APRX` v1 frame. The serialized payload is
  Raw or Zstandard and includes codec version, length, checksum, and optional dictionary id. The experimental legacy `APRC`
  frames remain readable.
- The choice is adaptive for hot/warm/cold/archive and Raw for already pre-serialized surfaces. Input
  threshold, minimum savings, level, and content-type prefixes are configurable per collection.
- Dictionaries are trained on limited samples, accepted only if a separate validation set
  improves the total, saved atomically with the catalog, and never removed while a version may reference
  them.
- The codec pool and scratch space have explicit limits; exhaustion produces
  backpressure before publication.
- Decompressed and compressed caches have separate budgets and metrics.
- The Fjall canonical keyspace uses `None` as the default physical compression; metadata, change log,
  and surfaces retain LZ4 physical compression. Double compression is only available as a measured configuration,
  not as the default.

## Evidence

Golden files, property tests, and fuzz targets cover records, catalogs, and dictionaries.
Tests verify Zstandard/Raw selection, content-type skip, reopening, exact version, missing dictionary,
separate caches, and backpressure on scratch.
The admin path is covered by the TCP client/server test.

The reproducible local matrix is in
[`benchmarks/compression`](../../benchmarks/compression/RESULTS.md).
On compressible payloads, the logical codec reduced 1,049,600 bytes to 6,655; on pseudorandom payloads, Raw
was preserved in 256 cases out of 256.
The run is small, uses a debug build, and is dominated by Fjall preallocation: it is not a proof of superiority.

## Consequences and risks

- Changing the physical default does not modify the Fjall logical format or require interpreting WAL/SST; experimental directories already created retain their physical options. Explicit migration/tuning remains open after Milestone 7.
- Codec metrics count encoding attempts, including those belonging to requests that may later fail; they are not a counter of bytes definitely committed.
- Dictionaries do not yet have garbage collection. This is intentional until there is a complete reachability proof for retained versions.
- External blobs are not compressed by the canonical frame; their policy remains separate and unimplemented until the blob store exists.
