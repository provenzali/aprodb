# Repository baseline

Initial state verified on 19 August 2026 for Milestone 0, with publication status updated after authorization.

## Local scope

- Git repository initialized locally on the `main` branch;
- before publication authorization, there were no commits, staged files, or remote;
- Git 2.54.0 and Rust stable 1.97.1 available via local toolchain paths;
- `gh` not installed;
- Chrome verified in read-only mode as the `provenzali` account, with accessible repository creation page;
- GitHub connector associated with a different account is not authorized for AProDB.

The user formally approved public publication and the licensing structure on 19 August 2026:
core `AGPL-3.0-only`; client, protocol, and public types `Apache-2.0`.
The confirmed target, now configured as `origin`, is the public repository [`provenzali/aprodb`](https://github.com/provenzali/aprodb).
Andrea Provenzali is identified with ORCID `0009-0009-9677-9840`, without publishing email or other personal data.

## File audit

The initial scan and the one repeated after Milestone 7 on 103 candidate files outside `.git`/`target` did not find strong token patterns, private keys or credentials, email addresses, or files of at least 10 MiB.
This scanning is a preventive gate and does not replace a dedicated secret scanner in the CI.

The `cargo metadata` audit following the relicensing examined 282 third-party packages in the entire graph and 103 in the Apache boundary.
No package is missing a license field and none declares an incompatible copyleft license as the only option.
The inventory is in `THIRD_PARTY_LICENSES.md` and must be regenerated when `Cargo.lock` changes.

The `.gitignore` rules have been verified with representative cases and cover:

- every Rust `target` directory, including `target/bench-lab` and the comparative benchmark target;
- AProDB data directories, WAL, snapshot, and local databases;
- `.env`, keys, private certificates, and logs;
- the local handoff prompt `implementation-prompt.md`.

`Cargo.lock` is not ignored and must be versioned.

## Distribution decision

Included in the distribution:

- sources, tests, benchmarks, and related manifests/lockfiles;
- `README.md`, `manual.md`, `paper.md`, and `diary.md`;
- `benchmarks/comparative/README.md` and `RESULTS.md`, maintained as historical local results with explicit limits;
- ADR, requirements matrix, AGPL/Apache texts, attribution, citation, governance, and CPU-only CI workflow.

Not included in the distribution:

- build artifacts, large reproducible lab outputs, and raw reports under `target`;
- data, WAL, snapshot, database, logs, profiles, and local configurations;
- credentials or cryptographic material;
- `implementation-prompt.md`, stored locally because it contains machine-specific paths and session state.

Published notes and results must use relative paths or neutral examples. They must not contain account emails, secrets, or unnecessary identifiers. The embedded benchmarks and client/server benchmarks remain separate.

## Initial Gates

Before any modification to the engine, the following have passed:

- `cargo fmt --all --check`;
- `cargo clippy --workspace --all-targets --no-default-features -- -D warnings`;
- `cargo test --workspace --no-default-features`;
- Clippy and tests with default features.

The GPU test explicitly ignored by the prototype was not enforced in this baseline; the historical motivation is documented in the diary. No external servers or comparative laboratories have been started.
