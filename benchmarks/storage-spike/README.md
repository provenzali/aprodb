# Spike storage Fjall 3.1.8

Questo laboratorio copre i criteri quantitativi della Milestone 0.5. Non è un
benchmark competitivo e non misura il protocollo client/server.

## Esecuzione

```powershell
cargo run --release -p aprodb-storage --example fjall_spike
```

Il programma crea directory temporanee indipendenti per otto workload: quattro
policy di compressione (`aprodb_zstd_only`, `backend_lz4_only`, `both`, `none`)
su payload comprimibili e pseudo-casuali. Ogni workload scrive 2.000 record,
esegue due versioni per record in batch da 100 e usa `SyncAll` per ogni batch.

Ogni mutazione scrive una versione immutabile, un head, un evento minimale con
riferimento alla versione e il watermark di catalogo. Al termine forza flush e
major compaction tramite l'API Fjall, riapre il database e verifica un campione
di versioni esatte.

## Metriche e limiti

- Le latenze Durable sono p50/p95/p99 dei 40 batch; il throughput conta le
  mutazioni, non le singole operazioni fisiche.
- `process_io_written_bytes` proviene dai contatori del processo. Su Windows
  include tutto l'I/O del processo: è un proxy comparativo dell'amplificazione,
  non un contatore di byte fisici attribuito nativamente da Fjall.
- `submitted_storage_bytes` è la somma di chiavi e valori consegnati al backend;
  il rapporto I/O usa questo valore come denominatore.
- Il costo `minimal_event_bytes` è un limite inferiore sintetico. I frame logici
  completi AProDB includono anche identità, sequence, batch id e checksum.
- Un singolo run locale non è sufficiente per regressioni o affermazioni di
  superiorità. La Milestone 5 aggiungerà ripetizioni, warm-up e policy per tier.

I risultati verificati sono in [RESULTS.md](RESULTS.md). La decisione è
registrata in [ADR-0001](../../docs/adr/0001-fjall-backend.md).
