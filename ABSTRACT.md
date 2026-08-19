# AProDB — Abstract

## Italiano

AProDB (*Adaptive Parallel Object Database*) è un database single-node scritto
in Rust per dati che cambiano valore operativo nel tempo. Conserva un record
canonico durevole e costruisce attorno a esso change stream, workflow con lease
e fencing, proiezioni incrementali, superfici di lettura e rappresentazioni
ricostruibili. Il motore usa CPU come riferimento semantico e può impiegare la
GPU in modo opzionale per operatori batch e ricerca vettoriale esatta, con
fallback che non coinvolge la correttezza dello storage.

L'architettura separa tipi, storage, engine, compute, protocollo, client e
server; impone limiti a code, cache, batch e memoria; supporta recovery,
checkpoint, backup/restore verificato e strumenti operativi. Il progetto è in
**beta test**: è destinato a valutazione e collaudo, non ancora a produzione.

## English

AProDB (*Adaptive Parallel Object Database*) is a Rust single-node database for
data whose operational value changes over time. It keeps a durable canonical
record and builds change streams, lease-and-fencing workflows, incremental
projections, read surfaces and rebuildable representations around it. CPU
execution defines the reference semantics; an optional GPU path can accelerate
batch operators and exact vector search, with failure isolation from storage
correctness.

The architecture separates types, storage, engine, compute, protocol, client
and server; bounds queues, caches, batches and memory; and provides recovery,
checkpoints, verified backup/restore and operational tooling. The project is in
**public beta testing** and is not yet production-ready.

The normative target architecture is described in [paper.md](paper.md), while
the implemented and usable behaviour is documented in [manual.md](manual.md).
