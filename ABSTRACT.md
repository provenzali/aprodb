# AProDB — Abstract

AProDB (*Adaptive Parallel Object Database*) is a Rust single-node database for
data whose practical value evolves over time. It maintains a durable canonical
record and constructs change streams, lease-and-fencing workflows, incremental
projections, read surfaces, and rebuildable representations around it. CPU
execution defines the definitive semantics; an optional GPU path can accelerate
batch operators and exact vector search, with failure isolation from storage
correctness.

The architecture separates types, storage, engine, compute, protocol, client,
and server; constrains queues, caches, batches, and memory usage; and provides recovery,
checkpoints, verified backup and restore, and operational tooling. The project is in
**public beta testing** and is not yet production-ready.

The normative target architecture is described in [paper.md](paper.md), while
the implemented and usable behavior is documented in [manual.md](manual.md).
