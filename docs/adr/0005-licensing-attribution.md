# ADR-0005 — Licensing for components and attribution

## Status

Accepted on 19 August 2026 by Andrea Provenzali.

## Background

AProDB must be available for download and use by anyone, even for commercial purposes, without allowing modifications to the database offered as a service to become silently proprietary. At the same time, the client and protocol must remain easily integrable into applications with different licenses. The name of the original author must accompany redistributions without publishing unnecessary personal data.

## Decision

- Core, server, storage, engine, compute, CLI, and facade: `AGPL-3.0-only`.
- Rust client, protocol, and shared public types: `Apache-2.0`.
- The compute types used by the client are located in `aprodb-types`; the permissive client does not depend on the AGPL compute implementation.
- Each source file contains copyright and an SPDX identifier; packages include the full text of their license.
- `NOTICE`, `AUTHORS.md`, and `CITATION.cff` identify Andrea Provenzali as the original creator and author of the specification, with ORCID `0009-0009-9677-9840`.
- No tax codes, dates of birth, nationalities, or personal email addresses are published.
- Contributions follow the DCO and the license of the recipient component; no CLA is introduced at this stage.

## Consequences

The database also retains copyleft for modified versions exposed via the network, while external applications can use the Apache-licensed client and protocol. No “AGPL OR Apache” choice is offered for the core. A future dual commercial license would require compatible permissions from all copyright holders of the affected components and a new ADR.