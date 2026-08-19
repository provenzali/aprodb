# Risultati comparativi — 19 agosto 2026

Risultati locali in build release: mediana di tre ripetizioni, 50.000 record, payload da 512 byte, batch da 500, 50.000 lookup caldi e 20 scansioni. Tutte le 30 prove hanno superato i controlli di correttezza.

Macchina: Windows 11 Home build 26200, Intel Core i5-1340P (12 core/16 thread), 15,7 GiB RAM, SSD NVMe WDSN740. Versioni: AProDB 0.1.0, SQLite 3.53.2, PostgreSQL 18.6, MySQL 26.7.0, MariaDB 12.3.2.

## Payload comprimibile

| Motore | Ingest ops/s | Lookup ops/s | Lookup p99 µs | Scan ops/s | Spazio MiB |
|---|---:|---:|---:|---:|---:|
| AProDB | 43.215 | 161.677 | 10,8 | 400,4 | 6,76 |
| SQLite | 6.157 | 13.107 | 223,6 | 2.455,9 | 29,61 |
| PostgreSQL | 32.157 | 974 | 1.630,2 | 310,2 | 31,10 |
| MySQL | 15.915 | 1.870 | 969,2 | 608,9 | 44,00 |
| MariaDB | 55.200 | 2.290 | 838,1 | 634,6 | 44,00 |

Nel formato interno AProDB, 24,46 MiB logici diventano 4,28 MiB (`compression_ratio` circa 0,175); con chiavi e frame WAL la directory occupa 6,76 MiB. Tutti i 50.000 valori scelgono Zstd.

## Payload pseudo-casuale

| Motore | Ingest ops/s | Lookup ops/s | Lookup p99 µs | Scan ops/s | Spazio MiB |
|---|---:|---:|---:|---:|---:|
| AProDB | 28.091 | 395.376 | 3,1 | 387,1 | 27,32 |
| SQLite | 6.032 | 13.559 | 230,0 | 2.565,3 | 29,61 |
| PostgreSQL | 32.918 | 966 | 1.610,8 | 277,5 | 31,10 |
| MySQL | 13.431 | 1.791 | 984,1 | 551,8 | 44,00 |
| MariaDB | 36.106 | 2.146 | 896,5 | 677,3 | 44,00 |

Zstd non può ridurre dati ad alta entropia: la policy adattiva conserva tutti i 50.000 valori raw. I metadati interni portano 24,46 MiB logici a 24,84 MiB memorizzati; la directory completa occupa 27,32 MiB. Il lookup è più rapido che sul profilo comprimibile perché non serve decomprimere.

## Lettura dei risultati

- AProDB ha il lookup puntuale più rapido: sul profilo comprimibile è circa 12,3× SQLite e 70,6× il migliore dei server SQL; sul casuale circa 29,2× SQLite e 184× il migliore dei server SQL. Il vantaggio include l'architettura embedded/in-memory e l'assenza di protocollo SQL.
- AProDB non vince l'ingest durevole: MariaDB è circa 1,28× più rapido sui dati comprimibili e 1,29× sui casuali. PostgreSQL supera AProDB sul profilo casuale.
- AProDB non ha ancora un indice ordinato. `scan_prefix` visita gli shard, mentre gli altri motori usano la primary key B-tree. SQLite è circa 6,1–6,6× più rapido nelle scansioni; MariaDB e MySQL superano AProDB in entrambe le prove.
- La compressione dà il risultato di capacità più netto: sui dati ripetitivi la directory AProDB usa circa il 77% di spazio in meno di SQLite, il 78% in meno di PostgreSQL e l'85% in meno dei tablespace InnoDB.
- Sul casuale il vantaggio di spazio è piccolo, come deve essere: la policy evita l'espansione Zstd e paga soltanto header, chiavi e WAL.

## Limiti

Questo non è TPC-C, YCSB completo né una misura di capacità massima. Usa un processo, una connessione per server, dati caldi e nessuna contesa tra client. AProDB mantiene il dataset attivo in RAM; i server SQL hanno cache e protocollo propri. Non sono misurati join, transazioni multi-tabella, replica, recovery sotto fault, multi-processo, concorrenza o query vettoriali GPU. I dati dimostrano prestazioni su questo workload preciso, non costituiscono SLA.

Il rapporto grezzo della sessione è `target/bench-lab/results/session-1787111407/report.json`.
