# Benchmark compressione Milestone 5

Questo laboratorio misura il percorso embedded reale di AProDB, non un server
via rete. Confronta quattro configurazioni per gli stessi keyspace e la stessa
durabilità: Zstandard logico AProDB, LZ4 fisico Fjall, entrambi e nessuno.

## Esecuzione

```powershell
cargo run --release -p aprodb-engine --example compression_benchmark
```

Ogni variante scrive 256 record da 4 KiB, in 16 batch atomici Durable, poi
esegue sync, compaction, verify, riapertura e una lettura. Il workload viene
ripetuto con payload comprimibili e pseudocasuali deterministici.

Le latenze p50/p95/p99 sono per batch, il throughput è in record/s. I contatori
I/O di processo su Windows includono tutto l'I/O del processo; la memoria è il
resident set alla fine del tratto misurato, non il picco. I file Fjall partono da
una preallocazione di 64 MiB: a questa scala `disk_bytes_before_compaction` non
è utile per confrontare il payload, mentre byte logici/codificati e I/O di
processo restano confrontabili. Un singolo run locale non autorizza affermazioni
competitive.

I risultati verificati sono in [RESULTS.md](RESULTS.md). La decisione è in
[ADR-0002](../../docs/adr/0002-logical-compression.md).
