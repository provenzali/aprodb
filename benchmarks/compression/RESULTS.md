# Risultati locali — 19 agosto 2026

Comando: `cargo run -q -p aprodb-engine --example compression_benchmark`
(profilo debug, Windows, filesystem locale). Sono risultati funzionali e
diagnostici, non numeri per confronti di prodotto.

## Payload comprimibile

| Modalità | ratio codificato/logico | p50/p95/p99 Durable µs | record/s | CPU ms | I/O read/write byte | RSS finale byte | disk post-compaction byte | recovery ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| AProDB Zstd | 0.006341 | 18,640 / 20,442 / 20,442 | 840.02 | 625 | 239,375 / 720,217 | 16,355,328 | 67,303,249 | 98 |
| Fjall LZ4 | 1.000000 | 31,044 / 48,878 / 48,878 | 459.54 | 1,032 | 145,127 / 537,139 | 15,732,736 | 67,212,166 | 147 |
| entrambi | 0.006341 | 18,611 / 20,505 / 20,505 | 838.83 | 656 | 82,787 / 417,210 | 17,842,176 | 67,156,829 | 136 |
| nessuno | 1.000000 | 27,860 / 33,481 / 33,481 | 546.61 | 1,000 | 1,353,158 / 3,957,286 | 16,240,640 | 68,376,895 | 197 |

Zstandard logico ha codificato tutti i 256 payload: 1,049,600 byte logici in
6,655 byte. Il tempo codec cumulato era 109,589 µs con solo Zstd e 110,432 µs
con doppia compressione.

## Payload pseudocasuale

| Modalità | ratio codificato/logico | p50/p95/p99 Durable µs | record/s | CPU ms | I/O read/write byte | RSS finale byte | disk post-compaction byte | recovery ms |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| AProDB Zstd adattivo | 1.000000 | 30,985 / 55,777 / 55,777 | 426.55 | 1,109 | 1,353,094 / 3,957,161 | 18,558,976 | 68,376,834 | 243 |
| Fjall LZ4 | 1.000000 | 37,438 / 53,667 / 53,667 | 372.12 | 1,343 | 1,193,850 / 3,681,624 | 16,306,176 | 68,260,638 | 179 |
| entrambi | 1.000000 | 39,190 / 62,993 / 62,993 | 324.23 | 1,437 | 1,193,817 / 3,681,560 | 18,649,088 | 68,260,607 | 199 |
| nessuno | 1.000000 | 26,437 / 41,130 / 41,130 | 520.80 | 875 | 1,353,158 / 3,957,286 | 16,252,928 | 68,376,895 | 108 |

La policy adattiva ha conservato Raw per tutti i 256 payload pseudocasuali e ha
registrato 256 fallback. Il tentativo Zstandard ha quindi un costo misurabile
senza beneficio; i prefissi content-type e le soglie minime servono a evitare
questo lavoro quando il formato è già compresso o noto come incomprimibile.

## Interpretazione

- Il formato logico permette di sapere se e come ogni payload è compresso,
  verificare checksum/lunghezza e conservare Raw quando Zstandard non conviene.
- La doppia compressione non è il default per il keyspace canonico: i dati di
  questo run non dimostrano un beneficio stabile che giustifichi il costo.
- Le metriche fisiche a 64 MiB sono dominate dalla preallocazione Fjall. Servono
  ripetizioni release e dataset più grandi per una scelta di tuning production.
