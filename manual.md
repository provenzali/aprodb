# Manuale software di AProDB

## 1. Panoramica

AProDB è un database embedded key-value scritto in Rust. Conserva il working set in memoria, registra le modifiche su disco e offre ricerca vettoriale parallela. È pensato come base sperimentale ad alte prestazioni con API tipizzata, non ancora come servizio distribuito production-ready.

Caratteristiche principali:

- chiavi UTF-8 e valori tipizzati;
- accesso concorrente mediante sharding;
- `put`/`get`/`delete`, batch lookup e prefix scan;
- write-ahead log con CRC32 e recupero della coda troncata;
- snapshot consistenti;
- compressione adattiva Zstandard in RAM, WAL e snapshot;
- dot product e cosine similarity su vettori `f32`;
- CPU parallela con Rayon;
- compute shader GPU tramite `wgpu`, con fallback CPU in modalità automatica;
- libreria Rust e interfaccia da riga di comando.

## 2. Stato e limiti della versione 0.1

Questa versione è un MVP single-process e single-node. Non implementa SQL, transazioni multi-chiave, replica, autenticazione, protocollo di rete o distributed consensus. Il dataset attivo deve entrare in RAM. Il WAL non viene ancora compattato automaticamente e può crescere; lo snapshot riduce il tempo di avvio e ripulisce i tombstone in memoria, ma conserva il WAL come storia completa di recupero.

## 3. Requisiti

- Rust stable con Cargo;
- un compilatore C compatibile, usato dalla libreria di riferimento Zstandard;
- una piattaforma supportata da `wgpu` per la feature GPU;
- driver grafici aggiornati per usare l'accelerazione;
- nessuna GPU obbligatoria: `--no-default-features` produce un binario CPU-only.

Su questa workstation Windows il progetto usa il target `x86_64-pc-windows-gnu`; Rustup e WinLibs UCRT sono già installati. Una nuova shell eredita automaticamente entrambi dal `PATH` utente.

## 4. Compilazione

Build completa:

```powershell
cargo build --release
```

Build solo CPU:

```powershell
cargo build --release --no-default-features
```

Test:

```powershell
cargo test --all-features
```

Controlli statici consigliati:

```powershell
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
```

## 5. Modello dati

Ogni elemento è identificato da una chiave UTF-8 non vuota. `Value` supporta:

| Variante | Rust | Uso tipico |
|---|---|---|
| `Bytes` | `Vec<u8>` | payload binari |
| `Text` | `String` | testo UTF-8 |
| `Integer` | `i64` | contatori e identificatori |
| `Float` | `f64` | valori numerici scalari |
| `Vector` | `Vec<f32>` | embedding e feature numeriche |

Float e componenti vettoriali devono essere finiti; `NaN` e infinito vengono rifiutati.

## 6. Uso come libreria Rust

Esempio minimo:

```rust
use aprodb::{Config, Database, Value};

let db = Database::open(Config::new("./data"))?;
db.put("utente:42", Value::Text("Ada".into()))?;
assert_eq!(db.get("utente:42")?, Some(Value::Text("Ada".into())));
db.sync()?;
# Ok::<(), aprodb::AproError>(())
```

Scrittura e lettura batch:

```rust
let writes = vec![
    ("a".to_string(), Value::Integer(1)),
    ("b".to_string(), Value::Integer(2)),
];
db.put_batch(writes)?;
let values = db.get_batch(&["a".into(), "b".into()])?;
```

Ricerca vettoriale:

```rust
use aprodb::{ComputeBackend, Metric};

db.put("doc:1", Value::Vector(vec![1.0, 0.0, 0.0]))?;
db.put("doc:2", Value::Vector(vec![0.0, 1.0, 0.0]))?;
let result = db.vector_search(
    &[0.9, 0.1, 0.0],
    10,
    Metric::Cosine,
    ComputeBackend::Auto,
)?;
```

`SearchResult.backend` dice se l'operazione ha usato CPU o GPU; `accelerator` contiene il nome dell'adapter quando disponibile.

## 7. Configurazione

`Config::new(path)` sceglie default sicuri. I campi modificabili sono:

- `path`: directory contenente WAL e snapshot;
- `shards`: potenza di due; più shard riducono la contesa ma aumentano l'overhead;
- `durability`: `SyncData` sincronizza ogni singola operazione o batch, `Relaxed` privilegia throughput e latenza;
- `gpu_min_work`: componenti minime (`vettori × dimensione`) perché `Auto` tenti la GPU; default 16.777.216.
- `compression_level`: livello Zstandard, default `1` per privilegiare la velocità;
- `compression_min_size`: soglia minima per tentare Zstd, default 32 byte;
- `compression_channels`: contesti indipendenti di compressione/decompressione, potenza di due, default limitato a 32 e massimo 64.

`SyncData` è il default. Per ingest massivo, usare `put_batch` è molto più efficiente di molti `put`, perché effettua una sola sincronizzazione del WAL per batch.

## 8. Compressione integrata

Ogni valore attraversa `CompressionChannel` in ingresso e in uscita. Il formato interno contiene versione, codec, tipo logico e lunghezza originale. Sopra `compression_min_size`, AProDB prova Zstandard; conserva però il risultato compresso solo se risparmia almeno otto byte. I payload piccoli, incomprimibili o già compressi restano raw e non subiscono espansione artificiale.

Il livello di default è Zstd 1, scelto per limitare la latenza. Livelli superiori possono ridurre maggiormente lo spazio, ma consumano più CPU. Non esiste un compressore migliore per ogni distribuzione di dati: l'adattività è parte del progetto, non un'eccezione.

I canali sono context pool indipendenti, selezionati mediante hash della chiave. Un solo contesto serializzerebbe tutte le operazioni; un contesto per ciascuno dei molti shard userebbe troppa memoria. Il pool configurabile offre parallelismo controllato. `put_batch` comprime più valori in parallelo prima dell'append seriale al WAL.

I byte compressi sono conservati direttamente nel working set in RAM e riutilizzati da WAL e snapshot. In lettura, errori Zstd, lunghezze diverse da quella dichiarata o tipi incoerenti causano un errore di corruzione.

`stats` espone:

- `compressed_values` e `raw_values`;
- `logical_value_bytes` e `stored_value_bytes`;
- `compression_ratio`, dove valori sotto 1 indicano risparmio;
- `compression_channels`.

## 9. Persistenza e recupero

La directory dati contiene:

- `aprodb.wal`: log append-only delle mutazioni;
- `aprodb.snapshot`: immagine consistente delle chiavi vive;
- `aprodb.snapshot.tmp`: possibile file temporaneo durante la creazione; non viene letto come database valido.

Procedura di scrittura:

1. validazione di chiave e valore;
2. assegnazione del numero di sequenza;
3. append del record al WAL con CRC32;
4. eventuale `sync_data` secondo la durabilità;
5. applicazione allo shard in memoria.

All'apertura, AProDB carica snapshot e WAL. Un frame WAL finale incompleto, tipico di uno spegnimento durante una scrittura, viene ignorato e il file viene troncato all'ultimo frame integro. Un checksum errato in un frame completo viene invece segnalato come corruzione.

Lo snapshot blocca temporaneamente nuove scritture, ma non le letture. Il WAL resta la fonte completa per il recupero.

## 10. Parallelismo

- Lo sharding limita i lock alle chiavi dello stesso shard.
- `get_batch`, `put_batch`, prefix scan, raccolta dei vettori, scoring CPU e ordinamento usano Rayon.
- compressione e decompressione usano un pool sharded di contesti Zstd senza lock globale.
- La sequenza per record garantisce *last assigned write wins* anche quando thread diversi terminano fuori ordine.
- Lo snapshot usa un write gate globale solo per ottenere un confine consistente.

## 11. Accelerazione GPU

La GPU è inizializzata in modo lazy al primo uso. Un compute shader WGSL assegna un thread logico a ciascun vettore e calcola:

- `Dot`: somma dei prodotti tra componenti;
- `Cosine`: dot product diviso per il prodotto delle norme.

Politiche:

- `Cpu`: usa sempre Rayon;
- `Gpu`: richiede la GPU e restituisce errore se non disponibile;
- `Auto`: prova la GPU oltre `gpu_min_work`; se inizializzazione o dispatch falliscono, torna alla CPU.

La GPU conviene solo quando il lavoro numerico ripaga upload, dispatch e readback. Il lookup key-value resta intenzionalmente sulla CPU/RAM.

## 12. CLI

Sintassi generale:

```text
aprodb [--path DIRECTORY] [--relaxed] <COMANDO>
```

Opzioni globali:

- `--path`: directory dati; default `.aprodb`;
- `--relaxed`: evita `sync_data` a ogni operazione e privilegia il throughput accettando un rischio maggiore in caso di crash.

La CLI scrive risultati JSON su standard output e diagnostica gli errori con exit code diverso da zero.

### `put`

```powershell
aprodb put saluto "ciao mondo"
aprodb put visite 42 --kind integer
aprodb put temperatura 21.5 --kind float
aprodb put embedding "0.1,0.2,0.3" --kind vector
aprodb put firma "00ff10" --kind bytes
```

Formati `--kind`: `text` (default), `bytes` esadecimali, `integer`, `float`, `vector` con componenti separate da virgola. Il comando restituisce chiave e sequenza assegnata.

### `get` e `delete`

```powershell
aprodb get saluto
aprodb delete saluto
```

`get` restituisce il valore tipizzato o `null`. `delete` restituisce `deleted: true` soltanto se la chiave era viva.

### `scan`

```powershell
aprodb scan "utente:" --limit 100
```

Esegue una prefix scan parallela, ordina le chiavi e limita il risultato. Una prefix vuota scansiona tutte le chiavi vive.

### `vector-search`

```powershell
aprodb vector-search "0.9,0.1,0" --limit 10 --metric cosine --backend auto
```

- `--metric`: `cosine` (default) o `dot`;
- `--backend`: `auto` (default), `cpu` o `gpu`;
- vengono considerate solo le chiavi con `Value::Vector` della stessa dimensione della query.

Il risultato riporta hit, score, numero di candidati, backend effettivo e nome dell'acceleratore.

### `stats`, `snapshot` e `gpu-info`

```powershell
aprodb stats
aprodb snapshot
aprodb gpu-info
```

`stats` espone chiavi, tombstone, sequenza, byte WAL, thread, compressione e disponibilità della feature GPU. `snapshot` salva le chiavi vive e ripulisce i tombstone in RAM. `gpu-info` inizializza il backend e restituisce l'adapter selezionato oppure un errore esplicito.

### `demo`

```powershell
aprodb --relaxed demo --items 10000 --dimension 128 --backend auto
```

Genera vettori deterministici con prefix `demo:vector:`, esegue un ingest batch e una ricerca top-5, quindi mostra tempi e throughput. Scrive nella directory scelta: usare un path dedicato se non si vogliono mescolare i dati demo con quelli applicativi.

### Help

```powershell
aprodb --help
aprodb vector-search --help
```

## 13. Manutenzione e sicurezza operativa

- Eseguire backup della directory dati solo dopo `sync()` o a processo fermo.
- Non modificare WAL o snapshot a mano.
- Non condividere la stessa directory tra più processi: il locking cross-process non è ancora implementato.
- Monitorare `wal_bytes` e pianificare una futura compattazione del WAL.
- Usare `Relaxed` solo accettando la possibile perdita delle ultime scritture in caso di crash o interruzione elettrica.
- Conservare copie esterne: WAL e snapshot non sostituiscono una strategia di backup.

## 14. Struttura del codice

- `src/engine.rs`: API pubblica, sharding, concorrenza e orchestrazione;
- `src/value.rs`: tipi e formato binario dei valori;
- `src/compression.rs`: `StoredValue`, scelta raw/Zstd e pool di contesti;
- `src/record.rs`: frame persistenti e checksum;
- `src/wal.rs`: append e recovery;
- `src/snapshot.rs`: lettura e scrittura snapshot;
- `src/compute/cpu.rs`: scoring parallelo CPU;
- `src/compute/gpu.rs`: inizializzazione `wgpu`, shader e readback;
- `src/main.rs`: CLI;
- `tests/`: comportamento end-to-end e persistenza.

## 15. Risoluzione problemi

**GPU non disponibile**: usare `Auto` o `Cpu`; verificare driver e supporto del backend `wgpu`.

**Errore di checksum**: lavorare su una copia della directory, conservare i file originali e ripristinare da backup; non cancellare automaticamente il frame segnalato.

**WAL molto grande**: lo snapshot migliora il caricamento dello stato, ma la versione 0.1 non riscrive ancora il WAL. Pianificare spazio adeguato.

**Prestazioni ingest scarse**: preferire `put_batch`; valutare `Relaxed` soltanto se il relativo rischio di durabilità è accettabile.

**Prima query GPU lenta**: l'inizializzazione lazy di adapter e pipeline è inclusa nella prima richiesta. Riutilizzare la stessa istanza `Database`; le richieste successive usano la pipeline già creata. In modalità `Auto`, regolare `gpu_min_work` con benchmark sul proprio hardware.

## 16. Benchmark riproducibile

```powershell
cargo bench --bench throughput
```

Il benchmark crea una directory temporanea, inserisce 50.000 vettori da 64 componenti e misura CPU, prima richiesta GPU e GPU calda, verificando anche che il ranking coincida. Sulla workstation Intel Iris Xe usata durante lo sviluppo: circa 61.120 insert/s in batch relaxed, CPU 71,77 ms, GPU fredda 534,40 ms e GPU calda 98,98 ms. Sono dati locali, non SLA; spiegano il default prudente di `gpu_min_work`.

## 17. Benchmark contro database esterni

Il crate `benchmarks/comparative` confronta AProDB con SQLite, PostgreSQL, MySQL e MariaDB usando chiavi e payload identici. È indipendente dal crate principale: i driver SQL non vengono inclusi nelle applicazioni che dipendono da AProDB.

### Protocollo

- due profili da 50.000 record: ripetitivo/comprimibile e pseudo-casuale;
- payload binario da 512 byte e batch da 500;
- un commit durevole per batch;
- 50.000 lookup puntuali a dataset caldo;
- 20 prefix/range scan ordinate, limite 1.000;
- tre ripetizioni, confronto sulla mediana;
- verifica automatica di lunghezze e numero di righe.

Esecuzione completa, dopo avere creato `aprodb_bench` sui server:

```powershell
cargo run --release --manifest-path benchmarks/comparative/Cargo.toml -- `
  --backends aprodb,sqlite,postgres,mysql,mariadb `
  --profiles compressible,random `
  --records 50000 --reads 50000 --payload-bytes 512 `
  --batch-size 500 --runs 3 --scan-repeats 20 --scan-limit 1000
```

Per una prova embedded senza server:

```powershell
cargo run --release --manifest-path benchmarks/comparative/Cargo.toml -- `
  --backends aprodb,sqlite --profiles compressible,random
```

Il runner accetta URL personalizzati con `--postgres-url`, `--mysql-url` e `--mariadb-url`. Scrive un rapporto JSON incrementale sotto il `--workdir`; un errore di backend non cancella le prove già concluse.

### Risultati locali del 19 agosto 2026

Su Intel Core i5-1340P, 16 thread e SSD NVMe, tutte le 30 prove hanno superato i controlli. Mediane principali:

| Profilo | Motore | Ingest ops/s | Lookup ops/s | Lookup p99 µs | Scan ops/s | Spazio MiB |
|---|---|---:|---:|---:|---:|---:|
| comprimibile | AProDB | 43.215 | 161.677 | 10,8 | 400,4 | 6,76 |
| comprimibile | SQLite | 6.157 | 13.107 | 223,6 | 2.455,9 | 29,61 |
| comprimibile | PostgreSQL | 32.157 | 974 | 1.630,2 | 310,2 | 31,10 |
| comprimibile | MySQL | 15.915 | 1.870 | 969,2 | 608,9 | 44,00 |
| comprimibile | MariaDB | 55.200 | 2.290 | 838,1 | 634,6 | 44,00 |
| casuale | AProDB | 28.091 | 395.376 | 3,1 | 387,1 | 27,32 |
| casuale | SQLite | 6.032 | 13.559 | 230,0 | 2.565,3 | 29,61 |
| casuale | PostgreSQL | 32.918 | 966 | 1.610,8 | 277,5 | 31,10 |
| casuale | MySQL | 13.431 | 1.791 | 984,1 | 551,8 | 44,00 |
| casuale | MariaDB | 36.106 | 2.146 | 896,5 | 677,3 | 44,00 |

AProDB è nettamente primo nei lookup puntuali e nello spazio sui dati comprimibili. Non è primo nell'ingest durevole: MariaDB vince entrambi i profili e PostgreSQL supera AProDB sui dati casuali. SQLite domina le scansioni perché usa la primary key ordinata, mentre `scan_prefix` di AProDB attraversa oggi tutti gli shard. Questo individua nell'indice ordinato una priorità concreta.

Il confronto embedded/client-server va interpretato correttamente. AProDB e SQLite non attraversano la rete; PostgreSQL, MySQL e MariaDB usano una connessione TCP locale e parsing SQL. AProDB tiene il dataset attivo in RAM. Lo spazio SQL non include redo/WAL globali. Il test non misura capacità massima, client concorrenti, join, replica o fault recovery e non è uno SLA. Metodologia e tabelle complete sono in `benchmarks/comparative/RESULTS.md`.

## 18. Capacità e scelta del motore

| Caratteristica | AProDB 0.1 | SQLite | PostgreSQL/MySQL/MariaDB |
|---|---|---|---|
| Modello | KV tipizzato e vettori | SQL relazionale embedded | SQL relazionale client/server |
| Lookup KV caldo | Molto rapido in-process | Rapido in-process | Include protocollo e SQL |
| Compressione per valore | Zstd adattiva integrata | Non equivalente nel core | Dipende dal motore/configurazione |
| Prefix/range index | Non ancora; scansione shard | B-tree | B-tree e indici avanzati |
| Transazioni ACID generali | Non ancora | Sì | Sì |
| Accesso multi-processo/rete | Non ancora | Multi-processo su file, niente server core | Sì |
| Query, join e vincoli | No | Sì | Sì |
| Ricerca vettoriale GPU | Integrata | No nel core | Tramite estensioni/prodotti specifici |
| Replica e alta disponibilità | No | No nel core | Sì, con capacità diverse |
| Maturità operativa | Prototipo/MVP | Molto alta | Molto alta |

Usare AProDB 0.1 quando il caso è embedded, single-process, key-value o vettoriale e si accettano i limiti espliciti dell'MVP. Usare SQLite quando servono SQL e transazioni locali in un singolo file. Usare PostgreSQL, MySQL o MariaDB quando servono utenti concorrenti, rete, transazioni complete, indici multipli, replica e strumenti operativi maturi.

## 19. Architettura obiettivo

La specifica normativa del futuro AProDB radiale è in [paper.md](paper.md). Non descrive capacità già disponibili nella versione 0.1: stabilisce il contratto da implementare e i criteri necessari per dichiarare completate le milestone.

La direzione approvata comprende:

- servizio centrale per accesso multiprocesso e modalità embedded esclusiva;
- CPU come implementazione completa di riferimento e GPU come acceleratore opzionale;
- contratto di storage con Fjall come primo candidato, redb/RocksDB come fallback e backend nativo soltanto se necessario;
- catalogo e change log logico AProDB atomici con il record, distinti dal WAL fisico del backend; gli eventi non duplicano il payload completo per default;
- retention degli eventi scelta per collection fra Delta, VersionRef e SelfContained, senza snapshot MVCC longevi;
- modalità Durable unica con finestra di group commit configurabile;
- atomicità entro una partizione, CAS, idempotenza, claim, lease e fencing;
- cache separate e radial score iniziale basato su freschezza, workflow e pin; altri segnali soltanto dopo misure;
- superficie di lavoro e superficie di lettura incrementali, ricostruibili e dotate di watermark;
- compressione adattiva del payload logico con Zstandard e fallback Raw, coordinata per keyspace con la compressione fisica del backend;
- supporto a dataset maggiori della RAM, storage class e compaction limitata;
- protocollo binario versionato, quote, backpressure, recovery, backup e osservabilità.

La parte GPU rimane nella roadmap originaria: non è stata rinviata. Interfacce compute e layout vengono preparati dalle fondazioni; la correttezza resta disponibile anche su CPU-only.

La [matrice di implementazione](docs/requirements-matrix.md) collega i requisiti normativi ai gate verificabili. Fino all'implementazione e al superamento dei relativi test, valgono i limiti dichiarati nella sezione 2 di questo manuale.

## 20. Repository e distribuzione del sorgente

AProDB è preparato per il repository canonico pubblico
`https://github.com/provenzali/aprodb`. Il 19 agosto 2026 la directory locale è
un repository Git sul branch `main`; creazione remota e push devono essere
registrati nel diario solo dopo la verifica effettiva. Il collegamento GitHub
integrato appartiene a un account diverso e non è autorizzato per AProDB.

Il progetto è esplicitamente in **beta test** e non è production-ready. La
distribuzione identifica Andrea Provenzali come creatore originario e autore
della specifica tramite nome, account `@provenzali` e ORCID
`0009-0009-9677-9840`; codice fiscale, data di nascita, nazionalità ed email non
appartengono ai file pubblici.

La prima sessione operativa deve completare la baseline della Milestone 0 prima della pubblicazione:

- owner `provenzali`, candidato `aprodb` e visibilità pubblica;
- audit di segreti, file grandi e artefatti locali;
- core `AGPL-3.0-only`; client, protocollo e tipi pubblici `Apache-2.0`;
- branch `main` e commit baseline verificato;
- test CPU-only locali e successiva CI GitHub CPU-only;
- nessuna dipendenza da GPU nei runner ospitati;
- nessun force-push o modifica di repository estranei ad AProDB.

Il `.gitignore` esclude build Rust, dati runtime, WAL, snapshot, database locali, configurazioni sensibili e log. Cargo.lock fa parte della distribuzione riproducibile e non deve essere ignorato.

La mappa vincolante dei componenti è in `LICENSING.md`. Le distribuzioni
conservano `NOTICE`; i contributi usano DCO e assumono la licenza del componente
modificato. Il client non dipende dall'implementazione compute AGPL: i tipi
condivisi necessari al wire sono in `aprodb-types`.

## 21. Motore canonico 1.x sperimentale — Milestone 1

Il facade conserva l'API 0.1 alla radice e rende disponibile la nuova verticale
tramite `aprodb::v1`. Questo percorso è utilizzabile come libreria embedded per
prove locali; per più processi usare il server della sezione successiva, che
rimane l'unico proprietario della directory dati.

Esempio minimo:

```rust
use aprodb::v1::{Engine, EngineConfig, Payload, PutRequest, RecordIdentity};

fn main() -> Result<(), aprodb::v1::AproError> {
    let config = EngineConfig::new("./data-v1");
    let engine = Engine::open(config)?;
    let id = RecordIdentity::new(
        "tenant", "namespace", "objects", "partition-a", "key-1"
    )?;

    let receipt = engine.put(PutRequest::new(
        id.clone(), Payload::Text("ciao".into())
    ))?;
    let record = engine.get(&id)?.expect("record presente");
    assert_eq!(record.version, receipt.version);
    Ok(())
}
```

### Configurazione e limiti

`EngineConfig` richiede una directory dati, un numero di shard potenza di due,
budget positivi e coerenti per chiavi, record, batch, memoria inflight, code e
storage. La directory viene marcata come formato logico 1.x e bloccata in modo
esclusivo anche fra processi. Una directory 0.1 contenente `aprodb.wal` o snapshot
legacy viene rifiutata con `IncompatibleFormat`; non esiste import automatico.
L'import one-shot esplicito e copy-only è descritto nella sezione 27.

La configurazione predefinita usa Fjall 3.1.8 con payload canonici senza
compressione fisica aggiuntiva e LZ4 per metadata, change log e superfici;
cache e memtable sono limitate e la manutenzione ha timeout. La compressione
logica canonica è descritta nella sezione 25. `group_commit_window = 0` impone un
`SyncAll` per ogni richiesta Durable. Con finestra positiva, le richieste vengono
accodate su un canale limitato e ricevono il receipt soltanto dopo il persist del
gruppo; `group_commit_max_bytes` chiude anticipatamente il gruppo.

### Operazioni e consistenza

Sono disponibili Put, Get, Delete, CompareAndSwap e AtomicBatch entro una sola
partizione. Ogni mutazione scrive nello stesso batch atomico:

- la versione immutabile del record;
- il puntatore head;
- il change event con sequence e batch id;
- il catalogo versionato e i watermark.

`Durable` riconosce la richiesta solo dopo `SyncAll` o dopo il persist del group
commit. `Relaxed` garantisce visibilità e ordine logico, non sopravvivenza a un
power loss; `Engine::sync()` porta catalogo e watermark allo stato Durable.

Ogni collection può usare Delta, VersionRef o SelfContained. Delta richiede dati
autosufficienti forniti dalla richiesta. VersionRef legge sempre la versione
immutabile indicata dall'evento, mai il valore corrente. SelfContained richiede
policy esplicita e rispetta il limite dimensionale configurato. Il GC elimina
eventi e versioni obsolete soltanto fino al minimo watermark dei consumer
obbligatori e conserva la versione corrente.

### Recovery, checkpoint e manutenzione

La riapertura ricostruisce lo stato da Fjall e valida formato, backend, shard e
catalogo. `verify()` controlla che ogni head risolva la versione esatta e che le
sequence degli eventi siano coerenti. `create_checkpoint(destination)` arresta
logicamente gli writer, rende Durable il catalogo e copia in modo paginato i
keyspace in una nuova directory; non sovrascrive una destinazione esistente.

`major_compact()` forza flush e compaction tramite API Fjall con timeout, senza
interpretare i file fisici. `stats()` espone spazio, write buffer, journal,
tabelle, flush e contatori di compaction disponibili. Dopo qualsiasi errore di
commit o persist il backend e il motore entrano in stato fail-closed: le nuove
operazioni vengono rifiutate fino alla chiusura e riapertura, perché l'esito
fisico potrebbe essere ambiguo.

### Limiti reali

Idempotency key, workflow e proiezioni sono disponibili attraverso la verticale
della sezione 24; GPU e operabilità sono descritte nelle sezioni 26 e 27. Lo
spike Fjall, l'ADR e i rischi upstream sono documentati in
`benchmarks/storage-spike` e `docs/adr/0001-fjall-backend.md`.

## 22. Server multiprocesso sperimentale — Milestone 2

`aprodb-server` è il processo centrale 1.x. Apre e blocca in esclusiva la
directory dati; i processi applicativi devono usare `aprodb-client` e non aprire
la stessa directory. TCP usa per default `127.0.0.1:7643` per le operazioni dati
e `127.0.0.1:7644` per l'amministrazione.

### Avvio e autenticazione

I token data e admin devono essere distinti e contenere fra 16 e 4096 byte.
Sono letti da `APRODB_DATA_TOKEN` e `APRODB_ADMIN_TOKEN`, non dagli argomenti e
non vengono stampati dai tipi di configurazione o dai log di avvio.

```powershell
$env:APRODB_DATA_TOKEN = "sostituire-token-data"
$env:APRODB_ADMIN_TOKEN = "sostituire-token-admin"
cargo run -p aprodb-server -- --data-dir .\aprodb-data
```

Gli endpoint si cambiano con `--data-listen` e `--admin-listen` o si disabilitano
con `--no-data-tcp` e `--no-admin-tcp`. `--data-local` e `--admin-local`
abilitano named pipe Windows (per esempio `\\.\pipe\aprodb-data`) o Unix domain
socket. Il server crea la prima named pipe prima di dichiararsi avviato.

TCP plaintext è rifiutato su indirizzi non loopback. L'opzione esplicita
`--allow-plaintext-non-loopback` rimuove soltanto questo blocco di sicurezza e
non è indicata per reti non fidate; TLS/mTLS è descritto nella sezione 27. Le
variabili d'ambiente restano visibili all'account del processo secondo le
regole del sistema operativo; usare un account di servizio e ACL adeguate.

### Protocollo e client Rust

Il wire format è Protobuf con frame length-delimited a prefisso `u32` big-endian.
L'handshake verifica magic `APRODB`, major protocol 1, ruolo, token e dimensione
massima. Il limite predefinito è 8 MiB e il client applica il minimo negoziato.
I messaggi e gli enum canonici sono anche descritti in
`crates/aprodb-proto/proto/aprodb.proto`; golden e property test proteggono il
formato.

`AsyncClient` multiplexa più request id su una connessione limitata e correla
risposte anche fuori ordine. `BlockingClient` offre lo stesso percorso per
programmi sincroni. Sono disponibili Put, Get, Delete, CompareAndSwap,
AtomicBatch entro partizione, workflow, change stream, superfici, Sync e i
comandi amministrativi. La receipt conserva
versione, shard, sequence, durabilità applicata e durable watermark del motore.
Durable e Relaxed hanno la stessa semantica descritta nella sezione 21.

La deadline client copre insieme attesa nella coda e risposta. Il server rifiuta
prima dell'ammissione deadline già scadute; un'operazione storage già ammessa non
viene interrotta a metà, per non lasciare un esito ambiguo. Le idempotency key
persistenti rendono sicuro il retry esplicito del chiamante; il client non
esegue ancora retry automatici.

### Limiti, backpressure e shutdown

Frame, connessioni, richieste in volo per connessione e globali, coda risposte,
idle timeout e drain timeout hanno limiti configurabili. Le opzioni principali
sono `--max-frame-bytes`, `--max-connections`,
`--max-inflight-per-connection`, `--max-inflight-global`,
`--response-queue-depth`, `--idle-timeout-ms`, `--drain-timeout-ms` e
`--backpressure-retry-ms`. Al superamento degli inflight il server restituisce
`Backpressure` con un `retry_after` positivo, senza creare una coda illimitata.

Il ruolo data non può eseguire Health, Stats, Verify, Compact o Shutdown; il
ruolo admin non può leggere o mutare record. La CLI amministrativa usa solo TCP:

```powershell
$env:APRODB_ADMIN_TOKEN = "sostituire-token-admin"
cargo run -p aprodb-cli -- health
cargo run -p aprodb-cli -- stats
cargo run -p aprodb-cli -- verify
cargo run -p aprodb-cli -- compact
cargo run -p aprodb-cli -- shutdown
```

`Stats` espone byte su disco/write buffer e contatori di connessioni, inflight,
richieste, rifiuti e autenticazioni fallite. Shutdown smette di accettare nuove
richieste, completa quelle già ammesse, chiude le risposte e attende il drain;
Ctrl+C usa lo stesso percorso. Quote tenant, audit e TLS sono aggiunti dalla
Milestone 7; l'esportazione verso un sistema di metriche esterno non è ancora
disponibile.

## 23. Motore radiale e capacità storage sperimentali — Milestone 3

La Milestone 3 mantiene Fjall come proprietario di WAL, manifest, segmenti,
Bloom, flush e compaction. AProDB non interpreta né duplica questi formati:
`stats()` riporta le misure offerte dal backend e `major_compact()` usa soltanto
la sua API pubblica. I record canonici restano su storage e non esiste una
`HashMap` contenente l'intero dataset 1.x.

### Budget memoria e cache

All'avvio il server rileva la memoria fisica e, quando disponibile, il limite
cgroup. Senza override usa metà del minore; `--memory-budget-bytes N` richiede un
valore di almeno 128 MiB e viene comunque limitato dal ceiling rilevato. Il log
di avvio riporta budget effettivo, fisico, container e configurato in byte.

`EngineConfig::apply_memory_budget` ripartisce il budget fra cache storage,
memtable, inflight, metadata cache, object cache, cache compressa, scratch codec
e negative cache, lasciando headroom non prenotata. La validazione rifiuta una
somma delle riserve superiore al budget. Le quattro cache AProDB sono sharded e
indipendenti; la object cache usa
ammissione pesata per frequenza, score, dimensione e pin, mentre le assenze hanno
TTL breve. Scansioni, checkpoint, verify e compaction bypassano la object cache.
`cache_stats()` e il comando admin seguente espongono budget, resident byte,
hit, miss, admission, rejection ed eviction:

```powershell
cargo run -p aprodb-cli -- cache-stats
```

La prova di capacità dedicata scrive 129 MiB di payload pseudo-casuali con un
budget motore di 128 MiB, esegue sync, compaction, reopen e letture esatte. È
marcata `ignored` nella suite ordinaria perché dura circa 80 secondi e scrive più
di 129 MiB; il gate esplicito è:

```powershell
cargo test -p aprodb-engine --no-default-features `
  canonical_dataset_can_exceed_the_configured_memory_budget -- --ignored
```

Questa prova dimostra il superamento del budget configurato, non costituisce un
benchmark fino a esaurire la RAM fisica né uno SLA di capacità.

### TTL

`PutRequest::expires_at_unix_ms` imposta una scadenza UTC assoluta. Versione,
head, change event, descrittore radiale e indice TTL sono aggiornati nello stesso
batch. `Get` non restituisce mai un record scaduto, anche prima della pulizia
fisica. `expire_due(limit, durability)` esamina un numero limitato di entry e
cancella soltanto se chiave e versione coincidono ancora, così un vecchio indice
non può eliminare un aggiornamento più recente. La CLI esegue uno sweep Durable
di massimo 1024 entry:

```powershell
cargo run -p aprodb-cli -- expire
```

Non esiste ancora un ciclo TTL automatico nel daemon. Per collection con
retention `Delta`, l'expiry viene rifiutato finché non è disponibile un delta
autosufficiente dichiarato; `VersionRef` e `SelfContained` conservano le normali
garanzie del change log. L'orario UTC non ordina le scritture: version e sequence
restano l'autorità.

### Radial descriptor, policy e storage class

Ogni Put crea o aggiorna in modo atomico un `RadialDescriptor` con versione canonica,
timestamp, deadline, dimensioni, stato workflow/proiezioni, costo ricostruzione,
classe, layer e motivazione. Policy per collection, storage class e generazione
sono versionate e recuperate alla riapertura. Lo score iniziale usa freschezza e
urgenza; soglie separate, permanenza minima e pin con scadenza limitano
oscillazioni. Lo score guida il placement ma non cambia la correttezza.

`explain_placement` restituisce versione, score, segnali, layer corrente e
raccomandato, classe, pin, residenza in cache, capacità di tiering fisico e
motivazioni. È read-only rispetto alla object cache. Dal client amministrativo:

```powershell
cargo run -p aprodb-cli -- explain tenant namespace collection partition key
```

Sul backend Fjall attuale `physical_storage_tiering` è `false`: più classi senza
path rimangono etichette logiche, mentre la registrazione di un path fisico
alternativo fallisce con `Unsupported`. Non vengono simulate migrazioni di file,
priorità I/O o controllo del medium che il backend non espone. Le superfici
derivate della Milestone 4 realizzano placement ricostruibile senza muovere il
record canonico.

## 24. Workflow, change stream e superfici sperimentali — Milestone 4

La verticale Milestone 4 è disponibile sia in modalità embedded tramite
`aprodb::v1::Engine` sia sul data plane mediante `AsyncClient` e
`BlockingClient`. La semantica worker è at-least-once: AProDB rende atomiche le
transizioni nel database, ma non promette exactly-once per effetti esterni.

### Idempotenza persistente

Put, Delete, AtomicBatch, Append, Claim, Heartbeat, Complete, Fail e Publish
accettano un hash di idempotenza opzionale di 32 byte. Il chiamante deve
calcolare l'hash da una chiave opaca senza inviare il segreto originale. Lo scope
è la partizione; il motore salva fingerprint della richiesta e receipt nello
stesso batch della mutazione canonica. Un retry identico entro
`EngineConfig::idempotency_retention` restituisce la stessa versione, receipt e
lease; riusare lo stesso hash per parametri diversi restituisce `Conflict`.

Il default di retention è 24 ore. `purge_expired_idempotency(now, limit)` rimuove
record e indice di scadenza con un batch limitato; non esiste ancora uno sweep
periodico nel daemon. Un record scaduto non viene comunque riutilizzato come
esito valido. La durabilità della registrazione coincide con quella della
mutazione; un esito Durable viene riconosciuto soltanto dopo il persist.

### Workflow e fencing

`Append` crea un record nuovo nello stato `pending`. `Claim` opera entro una
`WorkflowScope` tenant/namespace/collection/partition, sceglie un batch limitato
di record eleggibili e li porta atomicamente a `leased`. Ogni risultato include
record/versione, receipt, lease id casuale a 128 bit, fencing token monotono,
deadline UTC, tempo server e retry metadata. Il limite predefinito è 128 record,
la lease massima 15 minuti e il totale di lease attive nel processo è limitato.

`Heartbeat` richiede lease id e fencing token correnti e assegna una nuova
deadline a partire dal tempo server. `Complete` porta il record a `completed`;
`Fail(false)` lo riporta a `pending`, salvo raggiungimento di
`max_workflow_attempts`, mentre `Fail(true)` lo porta subito a `dead_letter`.
`Publish` accetta soltanto `completed` e produce `published`; ripeterlo su un
record già pubblicato è un no-op idempotente. Lease scadute o prove obsolete
restituiscono `Conflict` e non mutano il record.

Nel processo una `Instant` monotona protegge la validità della lease. Dopo un
riavvio valgono deadline UTC persistita e
`lease_recovery_safety_margin` configurabile; un record leased scaduto torna
eleggibile e il claim successivo incrementa il fencing token. Record, indice
workflow, change event, idempotenza e catalogo sono aggiornati nello stesso
batch storage. Le collection `Delta` vengono rifiutate dal workflow generico
finché non è dichiarato un delta autosufficiente per le transizioni.

Esempio client asincrono abbreviato:

```rust
use std::time::Duration;
use aprodb_client::{AsyncClient, PutOptions};
use aprodb_types::{Durability, Payload, WorkflowScope};

let receipt = client.append(
    identity.clone(),
    Payload::Text("job".into()),
    PutOptions {
        idempotency_key_hash: Some([1; 32]),
        ..PutOptions::default()
    },
    Durability::Durable,
).await?;

let claimed = client.claim(
    WorkflowScope::new("tenant", "namespace", "jobs", "partition-a")?,
    16,
    Duration::from_secs(60),
    Some([2; 32]),
    Durability::Durable,
).await?;
if let Some(job) = claimed.first() {
    client.complete(
        job.record.identity.clone(), job.lease, Some([3; 32]),
        Durability::Durable,
    ).await?;
}
# let _ = receipt;
# Ok::<(), Box<dyn std::error::Error>>(())
```

### Change stream

`subscribe_changes(tenant, namespace, collection, shard, after_sequence, limit)`
restituisce una pagina di `ChangeEvent` e il watermark globale dello shard. Il
watermark può avanzare anche se la pagina filtrata è vuota, perché altri
collection condividono lo shard: il consumer deve salvare il watermark della
risposta, non inferirlo dall'ultimo evento filtrato. Un AtomicBatch non viene
spezzato; un cursore nel mezzo di un batch viene rifiutato.

Se il GC ha eliminato la sequence richiesta, il server restituisce
`ChangeLogGap`. `VersionRef` punta sempre alla versione immutabile esatta;
`SelfContained` contiene il record soltanto secondo policy e limite; `Delta`
resta autosufficiente per il consumer dichiarato. Il protocollo usa frame
limitati: il consumer deve scegliere pagine proporzionate al frame negoziato.
Questa API è pull/paginata; notifiche push e binding non-Rust non sono ancora
presenti.

### Superfici work/read

Una `SurfaceDefinition` persistente contiene id, tipo `Work` o `Read`, una
collection sorgente, gli stati workflow ammessi, formato, limiti di record/byte
e numero di generazioni trattenute. L'ordine è totale e deterministico secondo
`RecordIdentity`. I formati implementati sono il frame binario AProDB di record
e JSON pre-serializzato. Il builder limita l'output prima per record e poi per
byte; non alloca una coda senza limite.

`create_surface` è amministrativa e idempotente soltanto per la stessa
definizione. Registra anche un consumer obbligatorio per shard, così GC non può
rimuovere le versioni richieste. `build_surface(id, max_events, durability)`
legge dal watermark successivo, applica insert/update/remove, serializza una
generazione immutabile e pubblica nello stesso batch generazione, puntatore,
watermark consumer e catalogo. La pubblicazione viene sempre resa Durable,
anche se il parametro richiesto è Relaxed, perché un watermark non durevole non
può autorizzare GC. Non esiste ancora un builder periodico: va invocato dal
controllo amministrativo.

`get_surface` usa il data plane e restituisce generazione, timestamp, watermark
per shard, staleness in sequence, flag `complete` ed errori. `complete` è vero
quando i watermark raggiungono le sequence correnti; una build con budget eventi
esaurito può pubblicare una generazione valida ma stale. `rebuild_surface` è
amministrativa, scansiona lo stato canonico sotto lock degli shard e va usata
esplicitamente dopo `ChangeLogGap`, schema change o danno derivato. Non usa
snapshot MVCC longevi.

Esempio amministrativo:

```powershell
cargo run -p aprodb-cli -- create-surface pending-work work tenant namespace jobs pending records 1000 8388608 2
cargo run -p aprodb-cli -- build-surface pending-work 4096
cargo run -p aprodb-cli -- rebuild-surface pending-work
```

Il client dati legge la superficie con
`get_surface(tenant, namespace, projection_id)`. Il server verifica che tenant e
namespace coincidano con la sorgente. Le generazioni più vecchie oltre la
retention vengono eliminate atomicamente durante la pubblicazione; non esiste
ancora rollback amministrativo a una generazione precedente.

### Recovery, verifica e limiti

Workflow index, idempotenza, definizioni, puntatori, generazioni e payload delle
superfici appartengono al checkpoint logico e vengono riaperti senza ricostruire
stato già Durable. `verify()` controlla indici workflow, riferimenti di versione,
generazioni trattenute e coerenza fra pointer e watermark del catalogo. Un gap
non viene nascosto: la build incrementale fallisce e richiede rebuild esplicito.

Le superfici attuali sono un incremento minimo dichiarativo: una collection,
filtro per stato workflow, ordine per identità e output record/JSON. Filtri su
indici generici, finestre temporali, selezione/trasformazione campi,
MessagePack/Protobuf/Arrow, dipendenze fra proiezioni, scheduler automatico e
rollback restano non implementati. Non sono ancora esportate metriche aggregate
di claim/lease/build; receipt, build report, watermark, staleness e Stats del
server sono le superfici osservabili disponibili. Quote per tenant, audit, TLS e
cifratura sono descritti nella sezione 27.

## 25. Compressione logica canonica — Milestone 5

I nuovi record 1.x usano il frame logico `APRX` v1. Il record conserva metadata
e workflow separati dal payload; il payload serializzato contiene versione del
codec, `Raw` o `Zstandard`, lunghezza logica, CRC32 e optional dictionary id.
Una lettura verifica frame, lunghezza, checksum, dizionario e decompressione
prima di ricostruire `Payload`. I frame sperimentali `APRC` prodotti prima della
Milestone 5 restano leggibili; i nuovi writer non li producono.

### Policy per collection e tier

`CompressionPolicy` ha una policy distinta per `Surface`, `Hot`, `Warm`, `Cold`
e `Archive`. Ogni tier canonico configura modalità, livello Zstandard, soglia
minima input, risparmio minimo e dictionary id. `Surface` è Raw in questa
milestone perché il suo payload è già serializzato e può usare la compressione
fisica separata del keyspace. I prefissi predefiniti saltano image, audio,
video, zip, gzip e zstd. Se il candidato Zstandard non supera il risparmio
minimo, il record conserva Raw senza espansione.

La policy è persistita Durable nel catalogo compressione e si applica alle
nuove versioni; non riscrive retroattivamente quelle esistenti. Il layer usato è
quello del descrittore radiale precedente, oppure Warm per una nuova chiave.
Esempi amministrativi:

```powershell
cargo run -p aprodb-cli -- compression-policy tenant namespace objects
cargo run -p aprodb-cli -- set-compression tenant namespace objects raw
cargo run -p aprodb-cli -- set-compression tenant namespace objects zstd
cargo run -p aprodb-cli -- compression-stats
```

`set-compression raw|zstd` applica il profilo uniforme CLI ai quattro tier
canonici. Per livelli, soglie, skip list e dictionary id specifici usare
`AsyncClient::configure_compression` o
`Engine::configure_compression_policy` con una `CompressionPolicy` completa.

### Pool, memoria e cache

`EngineConfig::compression_channels` deve essere una potenza di due fra 1 e 64;
il default è la potenza di due della parallelità disponibile, massimo 16. I
contesti compressor/decompressor vengono riusati. Lo scratch totale è limitato
da `compression_scratch_bytes`; una richiesta che non può prenotarlo riceve
`Backpressure` prima del commit. `apply_memory_budget` assegna per default il
12% allo scratch e l'8% alla cache compressa.

La object cache contiene record decodificati; la compressed cache conserva il
frame della versione corrente. Hanno budget, admission, hit/miss, eviction e
invalidazione per versione separati. `cache-stats` espone entrambe. Scansioni e
manutenzione continuano a bypassare la object cache; le versioni storiche non
sono trattenute indefinitamente in cache.

`compression-stats` espone byte logici/codificati, record Raw/Zstandard/con
dizionario, fallback incomprimibili, skip content-type, microsecondi codec,
errori, canali e scratch corrente/budget. Sono contatori dei tentativi del codec,
non soltanto dei commit definitivamente riusciti.

### Dizionari

`train_and_activate_dictionary` e `AsyncClient::train_dictionary` richiedono
almeno otto campioni di training, un set di validazione separato, schema,
dimensione massima e guadagno minimo. Numero campioni, byte totali, dimensione e
numero dizionari sono limitati da `EngineConfig`. Il dizionario viene pubblicato
solo se riduce il totale del validation set rispetto allo stesso livello senza
dizionario. Dizionario e catalogo aggiornato entrano nello stesso batch Durable.

Ogni versione registra l'id esatto; una lettura non usa mai il dizionario
corrente al suo posto. I byte del dizionario, checksum e statistiche di
validazione appartengono a checkpoint e recovery. Un dizionario mancante o
corrotto restituisce `Corrupt`, non un valore parziale. Non esiste ancora garbage
collection dei dizionari: vengono trattenuti conservativamente finché non sarà
disponibile una prova completa di reachability delle versioni.

### Compressione fisica e benchmark

Il default evita la doppia compressione sul keyspace canonico; Fjall conserva
LZ4 per metadata, change log e superfici. Le opzioni fisiche restano
configurabili per keyspace, ma abilitarle insieme a Zstandard richiede una misura
del workload reale. La matrice riproducibile a quattro modalità, con ratio,
latenza Durable, throughput, CPU, RAM, I/O, spazio, compaction e recovery, è in
`benchmarks/compression`. Il run locale è piccolo e non definisce uno SLA.

Blob esterni non vengono trasformati dal codec canonico: `BlobReference` resta
un riferimento. Compressione e storage dei byte blob richiederanno una policy
separata quando il blob store sarà implementato. TLS, cifratura at-rest, backup
e tooling copy-only sono descritti nella sezione 27.

## 26. Compute eterogeneo — Milestone 6

`Engine::vector_exact` e `AsyncClient::vector_exact` eseguono ricerca exact
top-k su tutti i `Payload::Vector` della collection che hanno la stessa
dimensione della query. Sono implementate dot product e cosine similarity. La
CPU è l'autorità semantica: input NaN/infinito sono rifiutati, la cosine di un
vettore nullo vale zero e i pareggi sono ordinati per riga, quindi per identità
nell'ordine della scansione canonica. CPU e GPU dichiarano tolleranza relativa
`1e-4` sui risultati float.

Esempio client asincrono:

```rust
use aprodb_client::{ComputePreference, VectorMetric};

let result = client.vector_exact(
    b"tenant".to_vec(), b"namespace".to_vec(), b"embeddings".to_vec(),
    vec![0.9, 0.1, 0.0], VectorMetric::Cosine,
    10, 100_000, ComputePreference::Auto,
).await?;
for hit in result.hits {
    println!("{:?} {}", hit.identity.key, hit.score);
}
# Ok::<(), aprodb_client::ClientError>(())
```

`max_scan_records` è obbligatorio e limita i record esaminati, non soltanto i
vettori compatibili. Se la collection lo supera, la richiesta fallisce con
`ResourceLimit` invece di restituire un risultato parziale. Il batch colonnare
deve inoltre rispettare `compute.max_batch_rows` e `max_batch_bytes`. Record non
vettoriali e vettori di altra dimensione vengono ignorati. ExactFlat è O(N):
non esiste ancora un indice ANN.

### Consistenza, scheduler e fallback

La scansione acquisisce brevemente tutti gli ordinatori shard, costruisce una
proiezione coerente e cattura la generazione globale; i lock vengono rilasciati
prima del calcolo. Il risultato rappresenta quella generazione e può essere
superato da una mutazione concorrente successiva, come una normale lettura. La
cache VRAM usa projection id, generazione e versione schema: non legge mai un
buffer di una generazione precedente.

`ComputePreference::Cpu` forza il pool CPU dedicato. `Auto` sceglie accelerator
soltanto quando la stima
`transfer_in + queue_wait + launch + gpu_compute + transfer_out + sync + risk`
è inferiore alla CPU. `Accelerator` salta il confronto di costo, ma conserva il
fallback CPU sicuro. Assenza GPU, coda/byte budget esauriti, timeout, errore
driver o circuit breaker non compromettono storage e producono
`CpuFallback` con una motivazione nella risposta.

La coda, i byte in volo, i worker, il batch, l'attesa micro-batch, il timeout e
la VRAM hanno tutti limiti in `EngineConfig::compute`. Il server espone override
con `--compute-cpu-threads`, `--compute-queue-depth`,
`--compute-queue-bytes`, `--compute-max-batch-rows`,
`--compute-max-batch-bytes`, `--compute-timeout-ms`,
`--compute-micro-batch-ms` e `--gpu-vram-bytes`. Valori incoerenti impediscono
l'avvio. Il budget memoria automatico riserva anche la coda compute.

### GPU, metriche e benchmark

La feature server predefinita `gpu` usa wgpu e inizializza adapter/device/pipeline
solo alla prima richiesta accelerata. `--no-default-features` elimina wgpu e
mantiene l'intera semantica via CPU. La VRAM conserva soltanto buffer derivati
con eviction LRU; schema/generazione diversa, invalidazione o reset del device
richiedono un nuovo upload. Readback asincrono, poll e attesa risposta sono
limitati dal timeout. Non esistono dati canonici in VRAM.

L'endpoint admin `compute_stats` e la CLI espongono richieste CPU/accelerator,
fallback, rifiuti, timeout, batch, byte in volo, nome adapter, uso/hit/miss/evict
VRAM, byte upload/readback, tempi transfer/kernel e reset:

```powershell
cargo run -p aprodb-cli -- compute-stats
```

Il benchmark riproducibile CPU/GPU, inclusi trasferimenti e top-k, è in
`benchmarks/compute`. Sul sistema locale la GPU calda è risultata più veloce
solo per alcune forme intermedie: non viene promessa accelerazione e il modello
va calibrato sull'hardware reale. ANN, filtri/aggregazioni GPU, CUDA/HIP,
autotaratura e pubblicazione di proiezioni mutate da GPU non sono disponibili.

## 27. Operabilità e sicurezza — Milestone 7

La Milestone 7 è disponibile sul percorso 1.x e resta sperimentale. Tutte le
procedure che possono cambiare formato o ricostruire dati lavorano in una nuova
directory: AProDB non esegue restore, repair, rekey, upgrade o import in-place.
Una directory destinazione già esistente viene sempre rifiutata.

### Cifratura at-rest e keyring

`EngineConfig::encryption` abilita XChaCha20-Poly1305 per i valori di tutti i
keyspace. L'AAD lega ciphertext, keyspace, key id e chiave storage; nonce e tag
sono verificati a ogni lettura. Chiave errata, frame spostato o alterato
restituiscono `Encryption`/`Corrupt` senza fallback in chiaro. Un database
cifrato non si apre senza tutte le chiavi richieste dal backup o dai record.

Il server accetta `--encryption-keyring FILE` oppure
`APRODB_ENCRYPTION_KEYRING_FILE`. Il file JSON è limitato a 64 KiB:

```json
{
  "active_key_id": "primary-2026",
  "keys": {
    "primary-2026": "<64 caratteri hex, 32 byte>"
  }
}
```

Sono ammesse al massimo 16 chiavi. Il materiale non appare in `Debug`, log,
manifest o audit. Su Unix il loader richiede permessi solo-owner; su Windows
l'operatore deve applicare una ACL equivalente. File PEM, keyring, `.env` e
chiavi sono esclusi dal repository. I nomi delle chiavi fisiche Fjall non sono
occultati: per cifrare anche nomi file e pattern di accesso serve cifratura del
volume.

La rotazione è esplicita e copy-only:

```powershell
cargo run -p aprodb-cli --bin aprodb-ops -- rekey .\data-old .\data-new `
  --source-keyring .\old-keyring.json --destination-keyring .\new-keyring.json
```

La copia viene riaperta e verificata con il nuovo keyring; la sorgente conserva
la chiave precedente e non viene modificata.

### TLS e mTLS

TCP può usare Rustls con catena e chiave PEM:

```powershell
cargo run -p aprodb-server -- --data-dir .\aprodb-data `
  --tls-cert .\server-cert.pem --tls-key .\server-key.pem
```

`--tls-client-ca .\client-ca.pem` rende obbligatorio un certificato client
valido. La CLI admin usa `--tls-ca`, `--tls-server-name` e, per mTLS,
`--tls-cert`/`--tls-key`. Timeout TLS e handshake applicativo sono limitati.
Named pipe e Unix socket restano locali e non applicano TLS. Token data/admin
continuano a essere verificati anche dentro il canale TLS.

### Audit e quote

Compact, shutdown, expiry, creazione/build/rebuild superficie, configurazione
compressione, training dizionario e backup producono un evento Durable
`Attempted` prima dell'azione e un evento `Succeeded` o `Failed` dopo l'esito.
Ogni evento contiene sequence, event id, timestamp, request id, principal,
operazione, outcome, hash BLAKE3 opzionale del target e classe errore; non
contiene token, chiave record o payload. La lettura è paginata e solo admin:

```powershell
cargo run -p aprodb-cli -- audit - 100
cargo run -p aprodb-cli -- audit 200 100
```

`--admin-principal` assegna l'identità registrata. L'audit è incluso in
checkpoint, backup, recovery e `verify`.

`--tenant-quotas FILE` carica un JSON limitato con questa forma:

```json
{
  "tenants": {
    "tenant-a": {
      "max_inflight": 8,
      "max_requests_per_second": 500,
      "max_request_bytes": 1048576,
      "max_vector_work_items": 10000000
    }
  }
}
```

Le quote vengono controllate prima del dispatch. Superare byte o lavoro
compute restituisce `ResourceLimit`; frequenza o inflight restituiscono
`Backpressure` con retry-after. La finestra richieste/secondo è fissa, in
memoria e non costituisce billing. `--max-data-bytes`,
`--min-free-disk-bytes` e `--max-compaction-temporary-bytes` proteggono il
disco. Scritture oltre quota falliscono prima della mutazione; compaction,
checkpoint e restore controllano la stima di spazio prima di iniziare.

### Backup, restore, verify e repair

Con `--backup-root PATH`, il server accetta soltanto nomi backup ASCII sicuri e
li risolve sotto quella root:

```powershell
cargo run -p aprodb-cli -- backup daily-001
```

Il backup crea un checkpoint coerente, lo riapre, esegue `verify`, inventaria
file e byte con BLAKE3 e pubblica `backup-manifest.json` con catalog generation,
watermark, backend, formato e key id. Una semplice copia non è dichiarata
backup riuscito. Verifica e restore offline:

```powershell
cargo run -p aprodb-cli --bin aprodb-ops -- verify-backup .\backups\daily-001
cargo run -p aprodb-cli --bin aprodb-ops -- restore .\backups\daily-001 .\restored `
  --keyring .\keyring.json
cargo run -p aprodb-cli --bin aprodb-ops -- verify .\restored --keyring .\keyring.json
```

`verify` pagina tutti i record, versioni/eventi, TTL, workflow, radial index,
superfici, dizionari e audit. Non ripara. La sola ricostruzione ammessa riguarda
stato derivato e richiede copia più conferma letterale:

```powershell
cargo run -p aprodb-cli --bin aprodb-ops -- repair .\source .\repaired `
  REBUILD_DERIVED_ON_SEPARATE_COPY --keyring .\keyring.json
```

Il report JSON distingue record persi/dubbi e indici ricostruiti. Corruzione del
record canonico o catalogo richiede restore; non viene nascosta né cancellata.
Interruzioni lasciano la copia parziale per diagnosi e retry verso una nuova
destinazione.

### Import AProDB 0.1 e upgrade

Il motore 1.x rifiuta sempre directory con `aprodb.wal` o
`aprodb.snapshot`. L'import è offline e richiede sorgente, copia preservata,
destinazione e mapping dell'identità:

```powershell
cargo run -p aprodb-cli --bin aprodb-ops -- import-0.1 `
  .\legacy .\legacy-preserved .\aprodb-1 legacy import default p0 `
  --max-records 1000000 --max-stored-bytes 4294967296 `
  --max-source-bytes 17179869184 --batch-operations 256
```

Il comando copia snapshot/WAL con checksum in `raw`, usa una seconda copia per
il reader 0.1 (che può troncare solo una coda WAL incompleta), esporta con limiti
di record/byte, scrive batch Durable in una directory di lavoro, verifica e
rinomina. Delete già applicate non vengono importate; bytes, text, i64, f64 e
vector f32 conservano il tipo. Sequence, timestamp, compressione e layout shard
0.1 non hanno equivalente e vengono rigenerati. La sorgente viene ricontrollata
durante la copia e deve essere offline.

Il writer supporta formato logico 1; formati futuri sconosciuti vengono
rifiutati. Finché non esiste una migrazione specifica, backup/restore e
copy-and-verify costituiscono il solo piano di upgrade/rollback.

### Gate operativi e limiti

Il test lungo `operability_long` è ignorato nella suite rapida perché esegue
2.048 scritture Durable cifrate, quattro restore e rekey. Va eseguito con:

```powershell
cargo test -p aprodb-engine --no-default-features --test operability_long -- `
  --ignored --exact repeated_encrypted_backup_restore_and_rekey_remain_consistent
cargo package --workspace
```

Non sono disponibili KMS, restore online, repair canonico automatico, RBAC
fine-grained per collection, audit remoto, metric exporter o replica. TLS,
cifratura e backup sono meccanismi applicativi sperimentali e richiedono
procedure periodiche di restore, gestione ACL e conservazione esterna delle
chiavi. La replica della Milestone 8 resta fuori dall'implementazione iniziale.
