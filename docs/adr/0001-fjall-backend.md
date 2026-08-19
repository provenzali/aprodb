# ADR-0001 — Fjall come backend della verticale single-node

- Stato: accettato con vincoli sperimentali
- Data: 19 agosto 2026
- Ambito: Milestone 0.5 e 1; riesame obbligatorio prima della Milestone 7

## Contesto

AProDB richiede batch atomici fra record, versione immutabile, change event e
catalogo; modalità Durable/Relaxed; scansioni ordinate; dataset maggiori della
RAM; compressione per keyspace; recovery e compaction. Il change log AProDB non
deve duplicare o interpretare WAL, manifest o segmenti fisici del backend.

Fjall documenta database con più keyspace LSM, batch atomici, ordinamento
lessicografico, persistenza configurabile e divieto di apertura multiprocesso
della stessa directory. AProDB aggiunge comunque un lock esclusivo proprio,
perché questa è un'invariante del server e non un dettaglio del backend:
[README Fjall](https://github.com/fjall-rs/fjall/blob/main/README.md),
[OwnedWriteBatch](https://docs.rs/fjall/3.1.8/fjall/struct.OwnedWriteBatch.html),
[PersistMode](https://docs.rs/fjall/3.1.8/fjall/enum.PersistMode.html).

## Decisione

Si accetta Fjall 3.1.8, pin esatto, dietro `StorageBackend`. L'accettazione vale
per il percorso sperimentale single-node e non equivale a idoneità production.

- `OwnedWriteBatch` realizza l'atomicità cross-keyspace.
- `SyncAll` implementa Durable; `Buffer` implementa Relaxed e la fase buffered
  del group commit.
- Records, Versions, Events, Catalog e Idempotency sono keyspace separati.
- Da Milestone 5 il keyspace canonico usa payload logici Raw/Zstandard e non
  applica LZ4 fisico per default; metadata, change log e superfici mantengono
  LZ4. La decisione e la matrice a quattro modalità sono in
  [ADR-0002](0002-logical-compression.md).
- La compattazione esplicita usa soltanto API Fjall e attende flush osservabili
  con timeout; AProDB non interpreta SST, journal o manifest.
- Fjall non offre ancora un checkpoint nativo stabile. AProDB crea un checkpoint
  logico in una nuova directory con watermark Durable e lo verifica in riapertura.
- Snapshot MVCC longevi non sono usati per retention. Le versioni immutabili e i
  watermark dei consumer sono dati applicativi AProDB.
- Il formato 0.1 viene riconosciuto e rifiutato: nessuna apertura automatica come
  formato 1.x.

Redb e RocksDB restano fallback. Non vengono sottoposti a spike paralleli finché
Fjall soddisfa i criteri o finché un rischio aperto non blocca una milestone.

## Evidenze

La suite verifica lock in-process e cross-process, batch atomico dopo riapertura,
scansioni limitate, flush/major compaction, recovery, checkpoint logico, limiti,
fault injection e retention delle tre modalità. Lo spike quantitativo e i limiti
della metrica I/O sono in
[`benchmarks/storage-spike`](../../benchmarks/storage-spike/RESULTS.md).

Il run locale ha misurato 4.096.000 byte di payload e 92.000 byte di evento
VersionRef minimale. Con dati comprimibili, la codifica Zstandard adattiva ha
prodotto 135.677 byte; con dati casuali ha conservato raw. Tutte le otto varianti
hanno superato compaction, riapertura e verifica del payload.

## Rischi e mitigazioni

- Il problema upstream sui fallimenti durante la scrittura di un batch journal è
  stato segnalato esplicitamente contro 3.1.8. Il backend AProDB entra quindi in
  stato fail-closed dopo qualsiasi errore di commit o persist e richiede la
  riapertura: [issue Fjall #308](https://github.com/fjall-rs/fjall/issues/308).
- La modalità di recovery stretta, capace di distinguere corruzione interna da
  tail troncata, è ancora una richiesta upstream. Prima della Milestone 7 servono
  kill-test e corruzione su copie usa-e-getta:
  [issue Fjall #311](https://github.com/fjall-rs/fjall/issues/311).
- Esiste un bug segnalato sui journal sealed con keyspace inattivi. AProDB non usa
  `clear`, mantiene limiti e metriche sul journal e forza manutenzione esplicita,
  ma deve aggiungere un soak test:
  [issue Fjall #288](https://github.com/fjall-rs/fjall/issues/288).
- Il checkpoint nativo è ancora richiesto upstream; il checkpoint logico AProDB è
  più costoso e non sostituisce ancora il backup operativo della Milestone 7:
  [issue Fjall #52](https://github.com/fjall-rs/fjall/issues/52).
- Le API Fjall usate per contatori e major compaction sono sperimentali. Il pin
  esatto impedisce upgrade silenziosi; ogni cambio di versione richiede gate e
  revisione di questa ADR.

## Criteri di riapertura della decisione

La decisione va riesaminata se fallisce un kill-test Durable, la recovery accetta
corruzione interna senza diagnostica, il journal supera il budget, le API di
compaction scompaiono, il checkpoint logico non scala o una milestone richiede
una capability non emulabile senza violare il confine del backend.
