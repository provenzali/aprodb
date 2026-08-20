# Contributing to AProDB

AProDB is in beta testing. Changes must preserve a CPU-only build, bounded resources, recoverable persistence, and the public compatibility guarantees documented in `paper.md` and `manual.md`.

Before proposing a change:

1. Open an issue for architecture or persistent-format changes.
2. Keep commits focused and do not include credentials, local data, or generated build artifacts.
3. Run code formatting, Clippy with warnings denied, and the relevant CPU-only tests.
4. Add tests and update `diary.md` and `manual.md` to reflect completed behavior.
5. Add `Signed-off-by: Your Name <address>` to each commit to certify the [Developer Certificate of Origin](https://developercertificate.org/).

Contributions to `aprodb-client`, `aprodb-proto`, and `aprodb-types` are provided under Apache-2.0. Contributions to the database core and all other original components are provided under AGPL-3.0-only. Submitting a contribution does not transfer the contributor's copyright.

Security vulnerabilities must follow `SECURITY.md`, not a public issue.