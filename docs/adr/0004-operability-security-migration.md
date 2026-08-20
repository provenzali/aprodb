# ADR-0004 — Operability, security, and copy-only migration

- State: accepted for Milestone 7
- Date: August 19, 2026
- Scope: single-node AProDB 1.x

## Background

Backup, repair, key rotation, and import 0.1 cannot irreversibly modify the only valid copy.
TLS and at-rest encryption must use mature libraries; audit and quota checks must fail before the
protected operation, without logging secrets or payloads.

## Decision

- Online backup creates a coherent logical checkpoint, reopens it, runs `verify`, inventories each file using BLAKE3, and publishes the manifest only after verification. Restore rechecks manifest, checksum, key id, catalog generation, and watermark in a new directory.
- `repair_derived_to_copy` requires the literal confirmation `REBUILD_DERIVED_ON_SEPARATE_COPY`, reconstructs only indexes and derived surfaces, and does not modify the source. Canonical or catalog corruption requires restore.
- All Fjall keyspace values can be protected with XChaCha20-Poly1305. A random nonce, key id, and AAD bind the ciphertext, keyspace, and storage key. Keys come from a size-limited, access-protected JSON file; markers and manifests contain only identifiers. Key rotation rewrites and verifies a separate copy.
- Remote TCP uses Rustls 0.23/Tokio-Rustls with server authentication and optional mTLS. Plaintext non-loopback remains rejected except with explicit override; named pipes and Unix sockets remain local transports.
- Each supported admin mutation records an `Attempted` event and a `Succeeded`/`Failed` outcome in a Durable batch. The target identifier is a BLAKE3 hash, not a plaintext key or payload. The audit log is readable only from the admin endpoint.
- Tenant quotas are checked before dispatch and limit request bytes, frequency, in-flight requests, and vectorized work. The engine also applies a data quota, free-space reserve, and temporary compaction estimate; backup and restore verify available space before copying.
- AProDB 0.1 is imported only once and exclusively offline. Original files are copied and verified in `raw`; another copy may undergo repair of the 0.1 reader's WAL queue. The new database is created in a temporary directory, verified, then renamed. The source is not opened by the 1.x engine.
- There are no in-place upgrades. Backup/restore, rekey, and future format changes always use a copy-and-verify procedure with rollback via the original copy.

## Evidence

The tests cover ciphertext/tamper/wrong key, backup/restore and altered inventory, copy repair,
audit after restart, roles, quotas, TLS/mTLS, redacted keyring, rekey, and import 0.1 with unchanged
source hash.
One long gate runs 2,048 encrypted Durable writes, four backup/restore cycles, and one rekey.
`cargo package -p aprodb-types --allow-dirty` verifies the independently packageable base crate,
and `cargo package --workspace --allow-dirty --no-verify` creates all workspace archives. Full
workspace package verification is blocked until the interdependent internal crates are published.

## Consequences and limits

- The names of Fjall physical keys are not hidden; values, records, catalog, change log, audit, surfaces, and dictionaries are encrypted. This is not a volume-encryption format and does not replace BitLocker/LUKS.
- The keyring file is not a KMS, and on Windows, ACL protection remains the operator's responsibility. No secrets are printed or saved in the manifest.
- Requests-per-second quotas use fixed in-memory windows and reset at restart. They do not serve for billing or distributed isolation.
- Restore, repair, rekey, and import are offline operations; a partial result is kept for diagnosis and not automatically deleted.
- The logical format supported by the writer is v1. Any unknown future format is rejected until a verified copy-only migration exists.
