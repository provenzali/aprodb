# Risultati locali dello spike storage

Data: 19 agosto 2026. Fjall 3.1.8, Rust 1.97.1, Windows 11 build 26200,
Intel Core i5-1340P (12 core/16 thread), 15,7 GiB RAM, NVMe WDSN740 512 GB.

Ogni riga rappresenta un run con 4.000 mutazioni e 4.096.000 byte di payload
logico. Dopo la compattazione e la riapertura risultavano quattro SST e un
frammento journal; il write buffer recuperato era vuoto dal punto di vista
operativo, anche se il contatore Fjall riportava memoria ricostruita.

| Policy | Dati | Payload codificato B | I/O processo B | I/O / byte inviati | Durable p50/p95/p99 µs | Mutazioni/s | Spazio riaperto B | Recovery ms |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| AProDB Zstd | comprimibili | 135.677 | 1.545.849 | 3,328 | 826 / 1.080 / 1.114 | 29.000 | 1.089.046 | 68 |
| Fjall LZ4 | comprimibili | 4.116.000 | 5.238.468 | 1,179 | 1.147 / 1.633 / 1.643 | 34.145 | 4.916.881 | 78 |
| Entrambe | comprimibili | 135.677 | 997.490 | 2,147 | 894 / 1.275 / 1.809 | 28.891 | 818.793 | 76 |
| Nessuna | comprimibili | 4.116.000 | 13.708.634 | 3,084 | 2.003 / 8.432 / 13.741 | 15.487 | 9.114.367 | 91 |
| AProDB Zstd | casuali | 4.116.000 | 13.708.634 | 3,084 | 1.768 / 6.344 / 11.691 | 14.153 | 9.114.367 | 108 |
| Fjall LZ4 | casuali | 4.116.000 | 13.396.090 | 3,014 | 1.816 / 7.142 / 8.930 | 17.097 | 8.998.031 | 97 |
| Entrambe | casuali | 4.116.000 | 13.396.110 | 3,014 | 1.829 / 6.723 / 7.688 | 14.446 | 8.998.028 | 88 |
| Nessuna | casuali | 4.116.000 | 13.708.634 | 3,084 | 1.894 / 5.556 / 7.979 | 15.875 | 9.114.367 | 91 |

## Costo del change log

- evento minimale VersionRef: 92.000 byte, pari al 2,246% del payload logico;
- delta sintetico di 16 byte: 64.000 byte, pari all'1,563%;
- SelfContained: 227.677 byte con payload Zstandard comprimibile, oppure
  4.208.000 byte quando il payload resta raw;
- byte sottoposti al backend: 464.557 con Zstandard comprimibile e 4.444.880
  negli altri casi.

Il payload non viene duplicato nel percorso VersionRef: versione e head/evento
condividono l'identità logica della stessa copia immutabile. Il test del motore
verifica inoltre recupero della versione esatta dopo 300 aggiornamenti,
compaction, restart e GC per Delta, VersionRef e SelfContained.

## Interpretazione

Sul profilo comprimibile, Zstandard adattivo è determinante; LZ4 fisico da solo
riduce lo spazio ma non il numero di byte logici consegnati. Sul profilo casuale
Zstandard sceglie raw e LZ4 offre un guadagno di spazio marginale. La doppia
compressione è stata misurata, ma la decisione definitiva sui payload resta alla
Milestone 5: per la Milestone 1 il backend usa LZ4 per keyspace e non abilita
ancora un codec logico AProDB.

Questi numeri sono esplorativi e non confrontabili con i risultati server esterni
in `benchmarks/comparative`.
