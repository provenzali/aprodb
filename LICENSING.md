# Licensing AProDB

AProDB is open-source software with a component-scoped licensing model. The SPDX identifier in each source file and the package metadata in each `Cargo.toml` identify the applicable license.

## Database core — AGPL-3.0-only

The root `aprodb` facade, `aprodb-storage`, `aprodb-engine`, `aprodb-compute`, `aprodb-server`, `aprodb-cli`, benchmarks, fuzz targets and other original project material not explicitly listed below are licensed under the GNU Affero General Public License, version 3 only. The complete text is in `LICENSE`.

The AGPL permits use, study, modification, redistribution and commercial use. Its conditions include preserving legal notices, licensing covered derivative works under the AGPL, and offering Corresponding Source to users interacting remotely with a modified network version.

## Integration boundary — Apache-2.0

The following crates are licensed under the Apache License 2.0:

- `aprodb-client`;
- `aprodb-proto`;
- `aprodb-types`.

Their complete license text is in `LICENSE-APACHE-2.0` and in each corresponding crate directory. This boundary allows applications to use the client and public protocol types without incorporating the AGPL database implementation. Whether a particular integration forms a derivative or combined work remains a question for the distributor's legal review.

Licensing the integration crates under Apache-2.0 is not an alternative license for the database core. In particular, the core is not offered as “AGPL OR Apache.”

## Attribution and trademarks

Copyright and author notices must be preserved as required by the applicable license. `NOTICE`, `AUTHORS.md` and `CITATION.cff` identify Andrea Provenzali as the original project creator and specification author. Contributors may retain copyright in their original contributions.

Licenses cover copyright and, where stated, patent permissions. They do not grant a right to misrepresent a fork as the official AProDB project or imply endorsement. See `TRADEMARKS.md`.

## Contributions and third-party software

By contributing, a contributor agrees to license the contribution under the license already applying to the target component and certifies provenance using the Developer Certificate of Origin process described in `CONTRIBUTING.md`. Third-party dependencies retain their own licenses; they are not relicensed by AProDB. `THIRD_PARTY_LICENSES.md` records the locked dependency graph.

This document is an implementation map, not legal advice. If a deployment or distribution model is legally material, obtain independent counsel.