# Diario tecnico di AProDB

Questo file registra le decisioni principali e il modo in cui le parti del motore cooperano. Non è un changelog di ogni riga: descrive le procedure che contano per capire, verificare e far evolvere il database.

## 2026-08-18 — Avvio del progetto

Il workspace era vuoto. È stato creato un nuovo crate Rust chiamato **AProDB**, abbreviazione di *Adaptive Parallel Object Database*. L'obiettivo della prima versione è un MVP realmente compilabile e persistente, non un sostituto immediato di PostgreSQL o Redis in produzione.

### Architettura scelta

- Il dataset attivo vive in RAM ed è diviso in un numero di shard pari a una potenza di due.
- `xxh3_64` associa ogni chiave a uno shard; ogni shard ha un proprio `RwLock<HashMap<...>>`.
- Le letture su shard diversi non si bloccano a vicenda. Le operazioni batch e le scansioni usano Rayon.
- Ogni modifica viene prima serializzata nel WAL e poi applicata allo shard. Il WAL usa frame con magic number, lunghezza e CRC32.
- Ogni record ha un numero di sequenza crescente. Lo shard accetta solo la versione più recente, così due write concorrenti sulla stessa chiave non possono riportare in vita una versione precedente.
- Le cancellazioni sono conservate temporaneamente come tombstone. Questo è necessario per ordinare correttamente put e delete concorrenti.
- Lo snapshot acquisisce un gate esclusivo sulle scritture, salva le sole chiavi vive e poi elimina i tombstone dalla RAM.

### Tipi e procedure centrali

- `Value`: rappresenta bytes, testo UTF-8, interi, float e vettori `f32`; possiede codifica binaria e validazione.
- `Record`: contiene sequenza, chiave e operazione `Put`/`Delete`.
- `Wal::open`: legge i frame validi, recupera i record e tronca in sicurezza solo una coda incompleta.
- `Wal::append_batch`: registra un intero batch e applica la policy di durabilità una sola volta.
- `Database::put_batch`: valida, assegna sequenze, persiste e applica in parallelo.
- `Database::vector_search`: raccoglie in parallelo i vettori della dimensione richiesta, sceglie CPU o GPU, calcola gli score e restituisce i migliori risultati.
- `GpuExecutor`: mantiene device, queue e compute pipeline; lo shader WGSL esegue dot product o cosine similarity con un work item per vettore.

### Regola importante sull'uso della GPU

Una GPU non accelera il lookup di una singola chiave: il costo di trasferire e sincronizzare dati sarebbe maggiore del calcolo. AProDB la usa per ricerche vettoriali batch e in modalità `auto` attende una soglia configurabile; per i carichi piccoli usa Rayon sulla CPU.

### Stato al termine della fase iniziale

In questa fase erano presenti formato dei valori, record con checksum, WAL, snapshot, motore sharded, API batch e percorso CPU/GPU. CLI, test di integrazione, benchmark e verifica completa sono stati aggiunti nelle fasi successive.

## 2026-08-18 — Compressione integrata per canale

È stato aggiunto un livello di memorizzazione compresso tra `Value` e `Record`. La scelta pratica è **Zstandard livello 1**: non esiste un algoritmo universalmente ottimo per ogni possibile input, ma Zstd combina un buon rapporto di compressione con encode/decode veloci e un formato stabile. Il livello basso privilegia la latenza del database.

### Procedura di ingresso

1. `Database::put_batch` valida tutti i valori.
2. Rayon distribuisce i valori sui worker.
3. L'hash della chiave seleziona uno dei canali `CompressionChannel`.
4. Il canale codifica il `Value` e tenta Zstd quando il payload supera la soglia configurata.
5. Se il risultato non risparmia almeno otto byte, viene conservato il payload raw. In questo modo dati piccoli, casuali o già compressi non si espandono inutilmente.
6. `StoredValue` registra versione del formato, codec, tipo logico, lunghezza originale e payload.
7. Lo stesso `StoredValue` viene scritto nel WAL e inserito in RAM; non viene compresso due volte.

### Procedura di uscita

`get`, batch get, prefix scan e ricerca vettoriale clonano il riferimento economico `Arc<[u8]>`, rilasciano il lock dello shard e usano il canale determinato dalla chiave. Il decoder verifica lunghezza e tipo dopo la decompressione prima di ricostruire `Value`. Gli errori di decompressione sono trattati come corruzione, non come valore assente.

### Perché più canali

Un singolo contesto Zstd protetto da mutex sarebbe un collo di bottiglia. AProDB crea un numero configurabile e limitato di coppie compressor/decompressor, normalmente una potenza di due vicina ai core e al massimo 32. Questo consente lavoro simultaneo senza moltiplicare i contesti per tutti gli shard, che potrebbe consumare troppa memoria. Chiavi diverse vengono distribuite deterministicamente sui canali.

### Osservabilità

`Database::stats` include valori compressi/raw, byte logici, byte effettivamente memorizzati, rapporto di compressione e numero di canali. Il rapporto misura il formato interno del valore e non include l'overhead dei frame WAL.

### Taratura CPU/GPU

Il benchmark release su Intel Iris Xe, 50.000 vettori × 64 componenti, ha misurato CPU 71,77 ms, GPU fredda 534,40 ms e GPU calda 98,98 ms. La vecchia soglia basata soltanto sul numero di vettori avrebbe scelto la GPU più lenta. `Auto` usa ora il lavoro totale `numero vettori × dimensione`, con soglia predefinita di 16.777.216 componenti. La GPU rimane forzabile; la soglia è configurabile e va tarata sull'hardware reale.

### Verifica toolchain

La prima build Windows GNU si è fermata prima della compilazione del progetto perché mancava `dlltool.exe`. È stato installato WinLibs UCRT tramite `winget`; in seguito formattazione, compilazione all-target, Clippy, suite CPU, suite GPU e build CPU-only sono terminate correttamente.

## 2026-08-18 — CLI, test e misure

La CLI è stata completata con `put`, `get`, `delete`, `scan`, `vector-search`, `stats`, `snapshot`, `gpu-info` e `demo`. Uno smoke test ha scritto un testo altamente comprimibile, riaperto il database tra processi CLI, verificato il round-trip, eseguito ricerca sulla Intel Iris Xe e osservato `compression_ratio = 0,0119` per quel payload sintetico.

La suite automatica copre codifica dei valori, compressione adattiva, persistenza, delete, batch, snapshot, riparazione di una coda WAL incompleta, concorrenza e ranking CPU. Il test GPU separato confronta top-20 CPU/GPU su 512 vettori ed è passato sulla Intel Iris Xe.

Clippy ha chiesto di rendere esplicito `.truncate(false)` all'apertura del WAL. La modifica documenta che l'apertura deve preservare la storia esistente; non cambia l'intenzione del codice.

### Esito finale della verifica

- `cargo fmt --check`: superato;
- `cargo clippy --all-targets --all-features -- -D warnings`: superato;
- suite all-features: 7 test superati, test GPU separato escluso come previsto;
- test GPU forzato su Intel Iris Xe: superato;
- suite `--no-default-features`: 7 test superati;
- smoke CLI multiprocesso: superato;
- benchmark release: circa 61.120 insert/s; CPU 71,77 ms; GPU fredda 534,40 ms; GPU calda 98,98 ms su 50.000 × 64.

Il database resta esplicitamente un MVP single-process. Le principali attività future sono locking cross-process, compattazione/rotazione WAL, crash testing con fault injection, fuzzing dei decoder, transazioni multi-chiave e un protocollo di rete opzionale.

## 2026-08-19 — Harness comparativo multi-database

È stato aggiunto il crate indipendente `benchmarks/comparative`. Rimane separato dal `Cargo.toml` del motore, così i driver SQL non diventano dipendenze di AProDB. Il runner usa la stessa sequenza deterministica di chiavi e payload per AProDB, SQLite, PostgreSQL, MySQL e MariaDB e salva un `report.json` incrementale dopo ogni prova.

I carichi hanno due profili: `compressible` simula campi ripetitivi di documenti e log, mentre `random` genera byte pseudo-casuali ad alta entropia. Questa distinzione serve a misurare sia il vantaggio sia il costo limite della compressione adattiva, senza presentare un solo dataset favorevole a Zstd.

Ogni prova comprende ingest in batch, lookup casuali a dataset caldo, range/prefix scan ordinata e spazio fisico. I commit avvengono una volta per batch con durabilità attiva: `SyncData` per AProDB, `synchronous=FULL` e WAL per SQLite, impostazioni durevoli del server per i database SQL. Le latenze p50/p95/p99 sono raccolte con un istogramma HDR. Il runner verifica lunghezza e numero di record durante le misure; un errore di correttezza invalida la singola prova e viene registrato nel rapporto.

Il confronto annota un limite architetturale essenziale: AProDB e SQLite sono embedded nello stesso processo del benchmark, mentre PostgreSQL, MySQL e MariaDB ricevono query tramite una connessione TCP sul loopback. Le misure descrivono quindi l'esperienza attuale delle rispettive API, non isolano il solo algoritmo di indicizzazione. Anche lo spazio SQL riguarda tabella e indici e non include WAL/redo globali del server.

### Esecuzione del laboratorio

Sono state usate distribuzioni portabili ufficiali: PostgreSQL 18.6, MySQL Community 26.7.0 e MariaDB 12.3.2. Gli hash pubblicati di MySQL e MariaDB sono risultati identici; per l'archivio PostgreSQL è stato registrato lo SHA-256 locale `fbe23da234ee31547bf8a36d29dfd81e82b849df2d2b78d2eecb43d360252f8c`. I cluster sono stati creati sotto `target/bench-lab`, vincolati a `127.0.0.1` sulle porte 55432, 53306 e 53307, senza servizi Windows permanenti.

PostgreSQL ha eseguito con `fsync=on`, `synchronous_commit=on` e `full_page_writes=on`; MySQL e MariaDB con `innodb_flush_log_at_trx_commit=1`. Il binlog era disabilitato perché il test non misura replica. AProDB ha usato `SyncData`; SQLite WAL e `synchronous=FULL`. A fine prova i tre server sono stati arrestati ordinatamente e le porte verificate chiuse.

Il primo tentativo di scaricare MariaDB tramite il redirect REST ha prodotto un file vuoto: è stato scartato prima dell'estrazione e sostituito dall'archivio ufficiale, poi verificato via SHA-256. Il primo readiness check PostgreSQL ometteva `-U postgres`, quindi il server rifiutava il ruolo Windows inesistente `andre`; i log hanno identificato il problema e il controllo corretto ha confermato che il server era già sano.

### Risultato

Le 30 prove finali in release sono terminate senza errori. Sui payload comprimibili, le mediane AProDB sono 43.215 ingest/s, 161.677 lookup/s, p99 10,8 µs, 400,4 scan/s e 6,76 MiB fisici. Sui payload casuali: 28.091 ingest/s, 395.376 lookup/s, p99 3,1 µs, 387,1 scan/s e 27,32 MiB.

La compressione interna riduce 24,46 MiB logici a 4,28 MiB sui dati ripetitivi; sui dati casuali riconosce che Zstd peggiorerebbe il risultato e conserva tutti i valori raw. Il risultato espone anche due priorità tecniche: un indice ordinato per le prefix scan e ottimizzazioni dell'ingest compresso. Le tabelle complete e i limiti sperimentali sono in `benchmarks/comparative/RESULTS.md`.

## 2026-08-19 — Stabilizzazione dell'architettura radiale

La fase di brainstorming è stata consolidata in **paper.md**, una specifica normativa distinta dal manuale della versione 0.1. Il documento definisce il database obiettivo senza presentare come implementate funzioni ancora progettuali.

Le decisioni principali sono:

- server centrale con possesso esclusivo della directory e modalità embedded single-process;
- record canonico durevole, con cache, indici, proiezioni e VRAM sempre ricostruibili;
- CPU reference completa e GPU opzionale selezionata da un modello di costo;
- single logical writer per shard e atomicità entro partizione;
- workflow at-least-once con idempotenza, lease e fencing token;
- superficie di lavoro separata dalla superficie di lettura;
- cache radiale con budget distinti e score basato su freschezza, accessi, urgenza, prontezza, costo e dimensione;
- proposta iniziale di WAL segmentato, manifest e segmenti nativi, successivamente sostituita dal contratto di backend documentato nella revisione 1.2;
- compressione per blocco con Zstandard, dizionari versionati e decisione Raw per dati non convenienti;
- replica progettata come fase separata, non promessa dal primo server.

Il paper include pipeline di lettura e scrittura, cache CPU/RAM/storage, adattamento a NVMe/SSD/HDD, protocollo, sicurezza, osservabilità, recovery, backup, failure matrix, test, benchmark e milestone 0–8. Sono state consultate fonti primarie Intel, NVIDIA, Apache Arrow, Zstandard, RocksDB, Redis, PostgreSQL, NVM Express e il paper Raft.

È stato creato **implementation-prompt.md** per la futura sessione pulita. Il prompt ordina di leggere integralmente la specifica, procedere per verticali verificabili, mantenere CPU-only come gate, aggiornare questo diario durante il lavoro e descrivere nel manuale soltanto capacità realmente completate.

Nessun file Rust è stato modificato in questa fase. La cartella non risultava ancora un repository Git; il prompt tratta l'inizializzazione come attività della Milestone 0 e vieta commit o pubblicazione non richiesti.

## 2026-08-19 — Revisione esterna e specifica 1.2

Una revisione di Claude ha evidenziato correttamente il rischio di reimplementare un intero storage engine prima di validare le funzioni distintive di AProDB. Sono state accettate quattro correzioni:

- contratto di backend invece dell'obbligo di scrivere subito WAL, segmenti e compaction;
- Milestone 0.5 con spike breve su Fjall e redb/RocksDB come fallback;
- unica modalità Durable con finestra di group commit configurabile;
- radial score iniziale ridotto a freshness, workflow/urgenza e pin.

La GPU non è stata rinviata: la Milestone 6 resta invariata e le relative interfacce e rappresentazioni devono essere predisposte dalle fondazioni.

La prima modifica del paper delegava lo storage, ma lasciava §17–18 come formato fisico obbligatorio. La versione 1.2 risolve l'incongruenza distinguendo:

1. WAL, memtable, segmenti, manifest e compaction privati del backend incorporato;
2. catalogo e change log logico AProDB, scritti atomicamente con il record;
3. formato WAL/segmenti AProDB applicabile soltanto a un eventuale backend nativo.

Il cambio di backend non è presentato come trasparente: capability, transazioni, snapshot, iterazioni, backup e compaction possono differire. Ogni cambio richiede ADR ed export/import o migrazione verificata.

È stato allineato **implementation-prompt.md**: rimosso il riferimento obsoleto a Strict/Group, aggiunta la Milestone 0.5, chiarito che il change log non è un secondo WAL e vietata una reimplementazione fisica senza prove. **manual.md** ora sintetizza la stessa architettura obiettivo.

Nessun file Rust è stato modificato.

## 2026-08-19 — Change log minimale e compressione coordinata, specifica 1.3

La seconda revisione di Claude ha approvato la separazione fra backend fisico e logica AProDB e ha evidenziato due costi da misurare: write amplification del change log e possibile doppia compressione.

Il paper è passato alla versione 1.3 con queste decisioni:

- un change event contiene key, versione, sequence, metadata e un delta minimo o payload reference;
- il payload completo non viene duplicato per default;
- un riferimento deve indicare una versione immutabile trattenuta fino al watermark dei consumer obbligatori;
- lo spike misura byte evento/payload, latenza Durable, throughput, spazio, compaction e rebuild;
- la compressione logica AProDB e quella fisica del backend sono coordinate per keyspace;
- la matrice confronta solo Zstandard AProDB, solo backend, entrambi e nessuna compressione;
- l'ADR sceglie separatamente per payload canonici, catalogo/change log, superfici, blob e indici.

**implementation-prompt.md** contiene ora gli stessi requisiti e impedisce di leggere per errore la versione corrente quando l'evento si riferisce a una versione precedente. **manual.md** mantiene distinta la versione 0.1 disponibile dall'architettura obiettivo 1.3.

Nessun file Rust è stato modificato.

## 2026-08-19 — Retention degli eventi, specifica 1.4

La terza revisione ha reso esplicita una scelta che la versione 1.3 lasciava implicita: un backend LSM può eliminare le vecchie versioni durante la compaction, mentre gli snapshot MVCC sono adatti a letture brevi e possono trattenere spazio se mantenuti a lungo.

Ogni collection dichiara ora una EventRetentionMode:

- **Delta:** evento autosufficiente per le proiezioni;
- **VersionRef:** head ed evento riferiscono una sola copia immutabile identificata da key/version o content hash;
- **SelfContained:** payload incluso soltanto con limiti e policy espliciti.

Gli snapshot del backend non sono una retention durevole. Lo spike della Milestone 0.5 deve provare consumer lento, aggiornamenti multipli, compaction, riavvio e garbage collection per tutte e tre le modalità. Gli invarianti impongono che il consumer recuperi la versione o il delta esatto anche dopo compaction e restart.

Il paper è passato alla versione 1.4; prompt e manuale sono stati allineati. Nessun file Rust è stato modificato.

## 2026-08-19 — Preparazione della pubblicazione GitHub

È stata approvata la direzione di ospitare AProDB su GitHub, mantenendo separata la preparazione locale dalla creazione della risorsa remota. Il controllo in sola lettura ha verificato:

- Git locale 2.54 installato, con nome ed email configurati;
- directory AProDB non ancora inizializzata come repository;
- collegamento GitHub integrato autenticato come `andreaprovenzali`, con permessi admin e push sui repository visibili, ma non destinato ad AProDB;
- Chrome autenticato sull'account corretto `provenzali`, con pagina di creazione repository accessibile e owner selezionabile;
- GitHub CLI `gh` assente, ma non necessaria perché la creazione autorizzata può avvenire dal browser;
- nessuna corrispondenza per token, chiavi private o credenziali forti nei file candidati;
- nessun file di almeno 10 MiB fuori dalle directory di build.

Non sono stati eseguiti `git init`, commit, creazione di repository o push. L'owner approvato è `provenzali`; restano decisioni dell'utente il nome definitivo, la visibilità pubblica/privata e la conferma della licenza MIT già dichiarata nel manifest. L'email dell'account non viene salvata nei documenti destinati al repository.

**implementation-prompt.md** ora include una fase GitHub della Milestone 0: audit dei file, controllo segreti e dimensioni, licenza, branch `main`, commit baseline autorizzato, creazione del solo repository confermato e CI CPU-only. Il `.gitignore` è stato rafforzato per crate annidati, dati runtime, database locali, configurazioni sensibili e output diagnostici. Cargo.lock deve rimanere versionato.

I controlli finali hanno confermato zero corrispondenze per credenziali forti, `cargo fmt --all --check` superato, 7 test CPU-only superati e Clippy CPU-only senza warning. `git check-ignore` va ripetuto dopo `git init`, perché il comando richiede un worktree Git; le regole sono state comunque ispezionate e coprono gli artefatti elencati.

Nessun file Rust è stato modificato.

## 2026-08-19 — Avvio dell'implementazione e baseline Milestone 0

La sessione di implementazione ha letto integralmente specifica, manuale, diario, manifest, sorgenti, test e documentazione del benchmark comparativo prima di modificare il progetto. Non esistono ulteriori `AGENTS.md` nel workspace oltre alle istruzioni della sessione.

### Verifiche iniziali

- Git 2.54.0 è disponibile e il repository locale è stato inizializzato sul branch `main` senza commit, staging o remoto.
- Chrome è stato ricontrollato in sola lettura: l'account attivo è `provenzali` e la pagina di creazione repository è accessibile con quell'owner. Nessuna risorsa è stata creata.
- `gh` non è installato e il connettore GitHub sull'account differente non è stato usato.
- La scansione dei 28 file candidati fuori da `target` non ha trovato pattern forti di credenziali, file sensibili o file di almeno 10 MiB.
- `target/bench-lab`, il target annidato del benchmark e gli altri artefatti voluminosi sono stati rilevati e conservati.
- Le regole di ignore sono state provate su build annidate, dati, WAL, snapshot, database, `.env`, chiavi e log; `Cargo.lock` resta versionabile.

La distribuzione includerà specifica, manuale, diario, risultati comparativi dichiarati locali, sorgenti, test, benchmark, ADR e matrice dei requisiti. Il prompt di handoff resta locale perché contiene path e stato macchina-specifici. È stato aggiunto il testo MIT coerente con `Cargo.toml`; la conferma della licenza, insieme a nome e visibilità, resta obbligatoria prima della creazione del repository remoto.

### Gate della baseline

Con Rust stable 1.97.1 sono passati format, Clippy con warning negati e test sia CPU-only sia con feature predefinite. Il test GPU ignorato del prototipo non è stato forzato e nessun server esterno è stato avviato. La matrice requisiti–milestone–test è in `docs/requirements-matrix.md`; le decisioni di distribuzione sono in `docs/repository-baseline.md`.

## 2026-08-19 — Verticale Milestone 0, 0.5 e 1

### Obiettivo

Costruire il primo percorso 1.x verificabile senza sostituire in modo monolitico
il prototipo: tipi e formati stabili, backend scelto mediante spike, mutazione
atomica record/change log/catalogo, recovery e limiti.

### Implementazione

Il workspace contiene ora `aprodb-types`, `aprodb-storage`, `aprodb-compute` e
`aprodb-engine`; il crate `aprodb` conserva la compatibilità 0.1 ed espone il
nuovo percorso sotto `aprodb::v1`. Il grafo non porta dipendenze GPU nel percorso
CPU-only. I frame Record, Head, Change e Catalog hanno magic, versione, lunghezza
e CRC32, con golden, property test e target fuzz.

Fjall 3.1.8 è stato isolato dietro un contratto con capability esplicite. La
directory possiede marker di formato e lock esclusivo in-process/cross-process.
Records, Versions, Events, Catalog e Idempotency sono keyspace distinti; la
mutazione usa un unico `OwnedWriteBatch`. AProDB non interpreta journal, manifest,
SST o compaction fisica. Il formato 0.1 è fotografato da golden e viene rifiutato
dal nuovo motore; l'import automatico non è implementato.

Il motore implementa Put, Get, Delete, CAS e AtomicBatch entro partizione,
writer logico per shard, catalogo versionato, receipt, Durable/Relaxed e group
commit limitato. VersionRef conserva e legge la versione immutabile esatta;
Delta deve essere autosufficiente; SelfContained ha policy e limite. Watermark
dei consumer obbligatori e GC controllano la retention senza snapshot MVCC
longevi. Sono disponibili verify, sync, checkpoint logico paginato, stats e major
compaction con timeout.

Un test ha mostrato che `rotate_memtable_and_wait` di Fjall può non attendere un
flush già accodato dopo auto-rotazione. Il wrapper verifica quindi coda flush e
write buffer prima della compattazione. Il rischio upstream di errore a metà
batch journal ha portato a un latch fail-closed: qualsiasi errore di commit o
persist arresta nuove scritture fino alla riapertura.

### Spike e decisione

Lo spike ha eseguito otto workload Durable con 4.000 mutazioni ciascuno,
compaction e reopen. Su 4.096.000 byte comprimibili, Zstandard adattivo ha
prodotto 135.677 byte; su dati casuali ha conservato raw. L'evento VersionRef
minimale costa 92.000 byte (2,246% del payload logico), contro 64.000 per il delta
sintetico e fino a 4.208.000 per SelfContained raw. Latenze, throughput, spazio,
I/O di processo e limiti della misura sono in `benchmarks/storage-spike`.

ADR-0001 accetta Fjall per la verticale sperimentale con pin esatto, LZ4 fisico,
checkpoint logico e riesame obbligatorio prima della Milestone 7. Redb e RocksDB
restano fallback, non spike paralleli automatici.

### Verifiche e limiti

Sono passati format, Clippy con warning negati e tutti i test workspace sia
CPU-only sia con feature predefinite. La suite comprende golden/proptest,
lock subprocess, kill immediato dopo ACK Durable, reopen, compaction, checkpoint,
fault fail-closed e il consumer lento da 300 versioni per ciascuna modalità
attraverso compaction, restart, watermark e GC. Il test GPU 0.1 resta ignorato
con motivazione perché richiede un adapter; la correttezza CPU passa.

Il target fuzz condivide la stessa funzione di esercizio: su non-Windows usa
LibFuzzer, mentre su Windows-GNU compila ed esegue un corpus smoke perché
`libfuzzer-sys` richiede un toolchain MSVC/Clang non disponibile. Il check e lo
smoke Windows passano; la CI CPU-only Linux compilerà l'entrypoint LibFuzzer dopo
la pubblicazione autorizzata del repository.

Resta aperta la corruzione fisica stretta su copie usa-e-getta, collegata al
rischio Fjall #311, oltre a import 0.1, server, idempotency key e tutte le
funzioni delle milestone 2–7. Il checkpoint logico non è ancora un backup
operativo.

## 2026-08-19 — Milestone 2, server multiprocesso

### Obiettivo e decisioni

È stato aggiunto il percorso raccomandato per più processi senza condividere la
directory dati. Il daemon resta l'unico proprietario del motore; protocollo,
client e server sono crate separati e il protocollo non dipende dal server.
Protobuf è usato con tipi Prost controllati nel repository e schema canonico
`.proto`, evitando una dipendenza di build da `protoc` sulla macchina utente.

Gli endpoint data e admin sono separati anche nel ruolo di handshake e richiedono
token distinti. Il confronto è constant-time e il debug dei token è redatto.
TCP plaintext è limitato al loopback salvo override esplicito; TLS rimane un gate
della Milestone 7 e non viene simulato.

### Implementazione e invarianti

`aprodb-proto` definisce magic, major/minor, request id, deadline UTC, durability,
record/versioni/receipt, batch e amministrazione. I frame e i batch hanno limiti
prima della decodifica o dell'esecuzione. Sono presenti golden per handshake,
Put e risposta Durable, property test e fuzz dei decoder storage e protocollo.

`aprodb-server` offre TCP, named pipe Windows e Unix domain socket, più richieste
in volo, risposta fuori ordine, code limitate e semafori per connessione e
globali. Deadline scadute vengono rifiutate prima dell'ammissione. Backpressure
include un retry-after configurabile; non viene creata una coda illimitata. Le
operazioni bloccanti del motore restano fuori dal runtime async. Shutdown remoto
o Ctrl+C interrompe l'accept, completa le richieste ammesse e drena le risposte.

`aprodb-client` offre API async multiplexata e wrapper bloccante per Put, Get,
Delete, CAS, AtomicBatch, Sync e amministrazione. La deadline monotona copre
insieme attesa nella coda client e risposta; la deadline UTC trasmessa protegge
l'ammissione server. I retry automatici sono rinviati finché la Milestone 4 non
fornisce idempotency key persistenti.

Gli eseguibili `aprodb-server` e `aprodb-cli` leggono i token soltanto da
variabili d'ambiente. La CLI amministrativa implementa Health, Stats, Verify,
Compact e Shutdown. Frame, connessioni, inflight, code, timeout e suggerimento
di retry sono configurabili; i valori non validi vengono rifiutati all'avvio.

### Verifiche e limiti

Clippy con warning negati e le suite mirate dei quattro crate di rete passano.
I cinque test end-to-end server coprono token errato, separazione dei ruoli,
Put/Get/CAS/Delete/AtomicBatch, richieste concorrenti, deadline scaduta, limite
frame, backpressure deterministica con retry-after, metriche, verify, named pipe
e shutdown. Uno smoke multiprocesso ha avviato il binario server, interrogato
Health tramite un secondo processo CLI e verificato la terminazione dopo
Shutdown.

Restano fuori dalla Milestone 2 retry/idempotenza, quote per tenant, audit
persistente, TLS, cifratura at-rest, metriche esportate e binding client per
linguaggi diversi da Rust. Il trasporto Unix è protetto da codice e test
condizionali, ma in questa sessione Windows è stato eseguito soltanto il test
named pipe; la CI Linux resta da eseguire dopo una pubblicazione autorizzata.

Dopo l'allineamento di codice, golden e documentazione sono passati
`cargo fmt --all --check`, Clippy workspace/all-targets con warning negati e test
workspace sia `--no-default-features` sia con feature predefinite. Il test GPU
0.1 rimane l'unico ignorato, con motivazione hardware già registrata. Il target
fuzz aggiornato passa check e corpus smoke Windows. Lo smoke multiprocesso è
stato ripetuto sui binari finali ed è passato.

## 2026-08-19 — Milestone 3, motore radiale e capacità storage

### Obiettivo e decisioni

La verticale rende il dataset canonico indipendente dalla RAM disponibile senza
reimplementare WAL, manifest, segmenti, Bloom, flush o compaction di Fjall. Le
capacità fisiche non esposte dal backend restano dichiarate assenti: in
particolare le storage class sono logiche su un singolo dispositivo e un path
alternativo viene rifiutato, invece di simulare un tiering inesistente.

Il server rileva memoria fisica e limite cgroup tramite `sysinfo` 0.39.6 con la
sola feature `system`. Usa metà del ceiling rilevato salvo override e applica il
minimo fra configurazione, memoria fisica e container. Il budget minimo è 128
MiB; cache storage, sette memtable, inflight e cache AProDB sono validate come
un'unica riserva limitata.

### Implementazione e invarianti

Sono state aggiunte metadata, object e negative cache separate, ciascuna in 16
shard e con byte budget. L'ammissione object pesa frequenza, radial score,
dimensione e pin; le scansioni/manutenzioni non popolano il working set puntuale.
Le metriche distinguono hit, miss, admission, rejection ed eviction. La query
`ExplainPlacement` controlla la residenza senza cambiare frequenze o statistiche.

Policy per collection, storage class e descrittori radiali sono formati logici
versionati e persistenti. Ogni mutazione canonica aggiorna atomicamente
descrittore ed eventuale indice TTL nello stesso batch di record, head, evento e
catalogo. Lo score usa freshness e urgenza, con soglie separate, permanenza
minima e pin a scadenza. `ExplainPlacement` riporta anche versione, layer,
classe, capacità fisica e motivazioni; protocollo, client, server e CLI espongono
la stessa operazione.

Le letture nascondono un record scaduto anche prima del cleanup. Lo sweep TTL è
limitato e usa identità/versione come fencing: un indice vecchio non cancella un
Put successivo. L'admin può eseguire `expire`; non esiste ancora un task
periodico. L'expiry di una collection `Delta` fallisce esplicitamente finché non
è disponibile un delta autosufficiente, mentre il default `VersionRef` conserva
il riferimento alla versione esatta.

### Verifiche, costo e limiti

Golden e fuzz includono `RadialDescriptor`, stato radiale e indice TTL. I test
coprono budget minimo, cache indipendenti, eviction, negative invalidation,
TTL/update/reopen, policy/pin/storage class, compaction e `ExplainPlacement`
senza effetti sulla object cache. L'integrazione server verifica placement,
metriche cache e scadenza; lo smoke multiprocesso sui binari finali ha eseguito
Health, CacheStats, Expire e Shutdown con budget 128 MiB.

Il capacity gate dedicato ha scritto 129 MiB pseudo-casuali con budget motore
128 MiB, quindi ha eseguito sync, compaction, reopen, verify e letture esatte:
è passato in circa 81 secondi. Rimane `ignored` nella suite ordinaria perché è
intenzionalmente voluminoso e viene invocato esplicitamente per il gate M3. Non
pretende di saturare tutta la RAM fisica e non è un benchmark prestazionale.

Dopo l'unica correzione Clippy a un assert costante sono passati format, Clippy
workspace/all-targets con warning negati e test workspace sia CPU-only sia con
feature predefinite. Il test capacità è l'unico ignorato CPU-only; con feature
predefinite resta ignorato anche il test GPU 0.1 già motivato. Il check del target
fuzz e il corpus smoke Windows passano.

Restano aperti il tiering fisico, il rilevamento del medium, le priorità I/O, lo
sweep TTL automatico, le quote tenant e una prova realmente superiore alla RAM
fisica. Questi limiti non impediscono il percorso disk-backed oltre il budget
motore, ma vietano di dichiarare SLA o controllo fisico non misurato.

## 2026-08-19 — Milestone 4, workflow e superfici

### Obiettivo e decisioni

La verticale aggiunge primitive worker generiche e proiezioni ricostruibili
senza inserire business logic nel motore. La semantica è at-least-once con
idempotenza persistente e fencing; gli effetti esterni non sono dichiarati
exactly-once. Work surface e read surface usano lo stesso builder limitato ma
restano tipi distinti nel catalogo. La pubblicazione di una generazione viene
forzata Durable anche quando il chiamante chiede Relaxed: avanzare un watermark
non persistito potrebbe autorizzare il GC a perdere la sorgente necessaria.

### Implementazione e invarianti

Put/Delete/AtomicBatch e le operazioni workflow accettano un hash idempotenza a
32 byte. Scope, fingerprint, receipt ed expiry sono formati logici versionati;
il record idempotenza entra nello stesso batch della mutazione. Replay identico
restituisce receipt e lease originali anche dopo reopen, mentre lo stesso hash
con parametri diversi fallisce. Un indice temporale supporta purge limitato;
non esiste ancora uno sweep server automatico.

Append crea `pending`; Claim indicizza per scope/stato/deadline e assegna lease
casuale, deadline e fencing crescente sotto il writer dello shard. Heartbeat,
Complete e Fail verificano lease e fencing correnti. Fail torna `pending` o
passa a `dead_letter`; Publish richiede `completed`. Il tempo monotono vale nel
processo, mentre restart usa UTC persistita e margine di sicurezza. Numero di
claim, durata e lease attive hanno limiti configurabili.

`SubscribeChanges` espone pagine filtrate con watermark globale dello shard,
preserva AtomicBatch e segnala `ChangeLogGap`. Le superfici persistono
definizione, pointer, generazione e payload in keyspace dedicati. Il builder
incrementale applica VersionRef esatta o SelfContained, rifiuta Delta generico,
pubblica generation/pointer/catalogo/watermark nello stesso batch e trattiene un
numero limitato di generazioni. Rebuild acquisisce gli writer, fotografa i
watermark e scansiona lo stato canonico senza snapshot MVCC longevo. La lettura
riporta watermark, staleness in sequence, complete ed errori.

Protocollo canonico, client async/bloccante, server e CLI sono stati estesi con
le stesse operazioni. I golden logici coprono workflow, idempotenza e superfici;
i golden wire aggiungono Claim e risposta superficie. Il fuzz target decodifica
anche tutti i nuovi frame. Due esempi Cargo omonimi sono stati rinominati per
eliminare la collisione di output segnalata dal gate all-targets.

### Verifiche e limiti

I test motore coprono replay/restart/expiry dell'idempotenza, state machine
completa, fencing obsoleto, claim concorrente sincronizzato con barrier,
superfici work/read incrementali e rebuild dopo GC/gap. Il test TCP attraversa
Append, replay, change stream, Claim/Heartbeat/Fail/Complete/Publish, superfici e
Verify con separazione data/admin. Il primo run ha correttamente mostrato che
`server_time` cambia fra replay Claim: il contratto e il test conservano record,
lease, deadline e receipt esatti, trattando il tempo server come metadata della
risposta. Suite mirate e Clippy con warning negati sono passati; i gate workspace
finali vengono registrati dopo l'allineamento documentale.

La definizione superficie attuale supporta una sorgente, filtro per stato,
ordine per identità e output record/JSON. Finestre, indici generici,
trasformazioni, Arrow/Protobuf, dipendenze, scheduler periodico, rollback e
metriche aggregate restano aperti e non sono descritti come disponibili. Anche
retry automatici client, sweep TTL/idempotenza e quote tenant restano assenti.

Dopo documentazione e formati finali sono passati `cargo fmt --all --check`,
Clippy workspace/all-targets con warning negati e test workspace sia CPU-only sia
con feature predefinite. Il primo comando combinato default ha raggiunto il
timeout esterno dopo Clippy e durante i doc-test senza mostrare errori; il test
workspace rilanciato a cache calda è passato integralmente. Restano ignorati
soltanto il capacity gate M3 e il test GPU 0.1, entrambi con motivazione. Il
target fuzz aggiornato compila e il corpus smoke Windows passa.

## 2026-08-19 — Milestone 5, compressione logica e dizionari

### Obiettivo e decisioni

Il payload canonico 1.x è passato dal frame record interamente Raw `APRC` a un
envelope `APRX` che comprime soltanto il `Payload` serializzato e mantiene
identità, metadata, workflow e versione direttamente verificabili. `APRC` resta
leggibile per compatibilità con le directory sperimentali già prodotte. La
policy è Raw per Surface e adattiva Raw/Zstandard per hot, warm, cold e archive;
una lista per content-type evita lavoro su formati notoriamente già compressi.

Il default Fjall del keyspace canonico è stato portato a nessuna compressione
fisica, mentre metadata, change log e superfici mantengono LZ4. La doppia
compressione rimane configurabile e misurata, ma non è default. ADR-0002
registra la separazione e aggiorna la decisione Fjall precedente.

### Implementazione e invarianti

`StoredPayload` porta codec version, lunghezza logica, CRC32, byte e optional
dictionary id. Il decoder verifica sempre lunghezza/checksum e carica il
dizionario esatto indicato dalla versione. Le policy sono versionate per
collection nel nuovo keyspace Compression e pubblicate Durable. La dimensione
record logica viene controllata prima della codifica; il limite SelfContained è
ricalcolato dopo l'assegnazione del dictionary id per non sottostimare il frame.

Un pool power-of-two riusa compressor/decompressor Zstandard. Lo scratch ha una
riserva atomica limitata e produce Backpressure prima del commit. La cache dei
frame compressi è indipendente dalla object cache decodificata e partecipa alla
ripartizione del budget. Le metriche distinguono byte, codec, fallback, skip,
tempi, failure, canali e scratch; misurano tentativi codec, anche se una richiesta
fallisce successivamente.

Il training dei dizionari limita campioni, byte totali, dimensione e numero di
dizionari. Usa un validation set separato e rifiuta la pubblicazione senza un
guadagno minimo. Dizionario e catalogo aggiornato sono atomici e Durable; non
esiste ancora GC dei dizionari, trattenuti conservativamente per proteggere le
versioni immutabili. Protocollo, client async/bloccante, server e CLI espongono
metriche, lettura/configurazione policy e training; i byte del dizionario non
vengono restituiti sul wire amministrativo.

### Formati, test e benchmark

Golden file, property test e fuzz target includono `APRX`, catalogo e dizionario.
I test motore coprono scelta Zstandard/Raw, skip content-type, reopen e exact
version, policy/cache separate, dizionario validato e mancante, e backpressure
scratch senza pubblicazione. Il test TCP copre policy, metriche, cache compressa
e training attraverso il server centrale. Sono passati format, Clippy
workspace/all-targets con warning negati e test workspace sia CPU-only sia con
feature predefinite. Restano ignorati soltanto il capacity gate M3 e il test GPU
0.1 già motivati. Il target fuzz aggiornato compila e il corpus smoke Windows
passa.

Il nuovo laboratorio `benchmarks/compression` esegue le quattro modalità su
payload comprimibili e pseudocasuali con stessa durabilità, poi sync, compaction,
verify e reopen. Nel run debug locale i 1.049.600 byte logici comprimibili sono
diventati 6.655 byte con Zstandard; i 256 payload pseudocasuali sono rimasti Raw.
Sono registrati ratio, p50/p95/p99 per batch Durable, throughput, CPU, RSS, I/O,
spazio e recovery. Fjall prealloca 64 MiB, quindi i byte fisici del piccolo run
non vanno interpretati come confronto production; non viene dichiarata alcuna
superiorità competitiva.

### Limiti aperti

Mancano tuning release ripetuto su dataset grandi, GC dei dizionari, policy blob
effettiva e riscrittura amministrativa delle versioni già esistenti. Directory
sperimentali create con LZ4 canonico conservano l'opzione fisica precedente: il
formato logico resta compatibile, ma un tooling di migrazione/tuning appartiene
alla Milestone 7.

## 2026-08-19 — Milestone 6, compute eterogeneo

### Obiettivo e decisioni

La verticale 1.x espone ora vector exact/top-k senza dipendere dalla GPU. Il
percorso CPU è la semantica di riferimento; wgpu è una feature del server e del
facade, inizializzata lazy e rimovibile con `--no-default-features`. ADR-0003
registra layout, modello di costo, cache VRAM e fallback. Durante la revisione è
stato corretto un rischio di riuso stale: il massimo delle sequence shard non è
un watermark sufficiente. La proiezione viene ora costruita sotto barriera di
tutti gli shard e usa la generazione globale catturata, poi rilascia i lock
prima del compute.

### Implementazione e invarianti

`ColumnarF32Batch` conserva valori f32 contigui, validity bitmap u32 e layout
esplicito. `CpuPool` usa un pool Rayon dedicato. Lo scheduler ha canale e budget
byte limitati, micro-batching compatibile, timeout, massimo worker, circuit
breaker/cooldown e fallback CPU. `Auto` confronta tutte le componenti del costo;
la risposta include stima, backend effettivo e motivo del fallback.

Il backend WGSL calcola dot/cosine, esegue readback `map_async`, applica timeout
e ricrea device/pipeline dopo errore. La cache LRU limita la VRAM e indicizza
projection id, source generation e schema; hit, miss, eviction, upload,
readback, transfer, kernel e reset sono metriche. Storage, catalogo e server non
dipendono dal device. Protocollo/client/server aggiungono VectorSearch data e
ComputeStats admin; la CLI espone `compute-stats`.

### Test e misure

I test deterministici coprono layout/null, non-finiti, tie, scelta costo, budget
byte, micro-batch, fault, fallback e cooldown. L'integrazione motore verifica
collection mista, limite e reopen; il test TCP verifica percorso dati e ruoli.
Golden Protobuf fissano richiesta e risposta vector. Il test wgpu sull'Intel
Iris Xe confronta top-20 con la CPU entro `1e-4`, poi verifica hit,
invalidazione, miss e rebuild VRAM.

Il benchmark release in `benchmarks/compute` ha misurato quattro forme con nove
campioni. La GPU calda ha superato la CPU su 8.192×64 e 65.536×64, ma non su
1.024×64 né 65.536×256; l'inizializzazione fredda ha raggiunto 590 ms. Il
crossover non è monotono, quindi non viene fissata una soglia universale né
dichiarata superiorità. Il primo build release ha superato due timeout esterni
da 120 secondi ma ha continuato nei processi figli; dopo aver verificato e
atteso quei PID, il binario finale è stato eseguito direttamente con successo.

### Limiti aperti

ExactFlat blocca brevemente le mutazioni durante la scansione e non sostituisce
ANN. wgpu portabile non espone un pool host pinned; l'implementazione usa staging
interno e budget batch/coda, con accesso device serializzato. Il modello non si
autotara, non esistono CUDA/HIP né altri operatori GPU e nessun risultato GPU
pubblica ancora una mutazione.

I gate finali M6 sono passati: format; Clippy workspace/all-targets con warning
negati CPU-only e default; test workspace CPU-only e default; nove test compute
GPU reali, inclusi equivalenza e cache; golden wire; check e corpus smoke fuzz
Windows. La suite CPU-only ignora soltanto il capacity gate M3. La suite default
ignora anche il test GPU del prototipo 0.1, mentre il nuovo test wgpu 1.x viene
eseguito e passa. Un primo run default ha oltrepassato il timeout wrapper durante
la build; il processo è terminato e il rilancio a cache calda ha restituito exit
code zero per l'intero workspace.

## 2026-08-19 — Milestone 7, operabilità e sicurezza single-node

### Obiettivo e decisioni

La verticale finale single-node aggiunge procedure recuperabili senza upgrade
in-place. ADR-0004 stabilisce copy-and-verify per backup/restore, repair, rekey e
import 0.1. Le primitive mature scelte sono XChaCha20-Poly1305 per i valori
at-rest, Rustls/Tokio-Rustls per TCP e BLAKE3 per inventari e target audit. I
key id sono pubblici; il materiale delle chiavi resta soltanto nel keyring
esterno e viene redatto da `Debug`.

Il server registra `Attempted` prima delle mutazioni amministrative e l'esito
dopo il dispatch. Se l'evento iniziale non diventa Durable, l'operazione non
parte. Le quote tenant sono applicate prima dei permit globali e limitano byte,
rate, inflight e lavoro vettoriale. Il motore controlla quota dati, riserva
libera e spazio temporaneo stimato prima di write, compaction, checkpoint e
restore.

### Implementazione e invarianti

`EncryptedBackend` protegge i valori di tutti i dodici keyspace con nonce
casuale e AAD su versione, keyspace, key id e chiave storage. Il marker impedisce
apertura silenziosa con configurazione diversa. Checkpoint include anche
compressione e audit; backup riapre/verifica, inventaria i file e pubblica il
manifest solo alla fine. Restore ricopia con `create_new`, ricalcola hash e
ricontrolla catalog generation/watermark. Rekey crea una nuova copia e la riapre
con la nuova chiave.

`verify` pagina l'intero spazio invece di fermarsi al primo limite di coda e
controlla record/versioni/eventi, radial, TTL, workflow, superfici, dizionari e
audit. Repair ricostruisce solo indici derivati su copia con conferma esatta e
produce un report serializzabile; non tenta recupero canonico implicito.

Protocollo, client e CLI espongono AuditList e backup online sotto una
`backup_root`; il nome non può attraversare directory. Il server carica TLS,
mTLS, keyring e quote da file limitati. `aprodb-ops` fornisce verify-backup,
restore, verify, repair, rekey e import offline con output JSON.

L'import 0.1 rifiuta sempre l'apertura diretta dal motore 1.x. Copia snapshot e
WAL in `raw`, verifica BLAKE3 prima/dopo, crea una seconda reader-copy perché il
reader storico può riparare una coda WAL troncata, esporta con limiti e mappa i
cinque tipi in batch Durable su una partizione. Il database 1.x nasce in una
directory di lavoro, passa `verify` e viene rinominato; delete storiche non
riappaiono e la sorgente resta byte-identica.

### Test, formati e distribuzione

Golden logici aggiungono `APAU`/`APAS`; golden Protobuf aggiungono richiesta e
risposta audit. Il fuzz target decodifica entrambi i frame audit. I test mirati
sono passati per cifratura/tamper/wrong key, backup/restore, repair, rekey,
audit/restart, quota tenant/disco, server backup, TLS/mTLS, keyring redatto e
migrazione snapshot+WAL 0.1. L'intero test `server_integration` CPU-only è
passato, così come i test lib del motore e i golden types/protocollo.

`operability_long` è intenzionalmente ignorato nella suite rapida perché esegue
2.048 scritture Durable cifrate, quattro backup/restore e rekey. È stato eseguito
esplicitamente in questa sessione ed è passato in circa 132 secondi. Il motivo e
il comando sono nel manuale e nel workflow manuale CI. `cargo package
--workspace --allow-dirty` ha creato e verificato localmente nove archivi in
circa 500 secondi; non è avvenuta alcuna pubblicazione.

### Limiti aperti

La cifratura applicativa non nasconde nomi delle chiavi fisiche e non sostituisce
la cifratura volume. Mancano KMS, restore online, RBAC per collection, audit
remoto, metric exporter e repair canonico automatico. Le quote rate sono
finestre fisse in memoria. Un'operazione offline interrotta conserva la copia
parziale per diagnosi e richiede una nuova destinazione. Il writer logico è v1;
formati futuri vengono rifiutati finché non esiste una migrazione copy-only.

La replica Milestone 8 resta deliberatamente fuori perimetro. Repository remoto,
commit e push non sono stati creati: nome, visibilità e conferma MIT richiedono
ancora autorizzazione esplicita dell'utente.

### Gate finali della tranche single-node

Il gate conclusivo ha confermato format, Clippy workspace/all-targets con warning
negati sia CPU-only sia default, test workspace CPU-only e default, suite lunga
operability esplicita, packaging verificato dei nove crate e compilazione del
crate fuzz. La scansione finale non ha trovato blocchi `unsafe`, file pubblicabili
da almeno 10 MiB, pattern forti di segreti o indirizzi email. `cargo` non era nel
`PATH` della shell finale; il controllo metadata è stato ripetuto con il percorso
esplicito del toolchain stabile ed è passato. Il sottocomando `cargo-fuzz` non è
installato: il target è stato compilato, ma in questo gate non è stata eseguita
una campagna libFuzzer.

## 2026-08-19 — Licenza, attribuzione e preparazione beta pubblica

L'utente ha approvato formalmente il core sotto `AGPL-3.0-only` e il boundary
d'integrazione (`aprodb-client`, `aprodb-proto`, `aprodb-types`) sotto
`Apache-2.0`. Non viene offerta un'alternativa Apache per il core. I quattro tipi
compute riesportati dal client sono stati trasferiti in `aprodb-types`, con
riesportazione compatibile dal crate compute, eliminando la dipendenza del
client permissivo dall'implementazione AGPL.

Ogni sorgente porta copyright e identificatore SPDX; ogni crate contiene il
testo della propria licenza. `NOTICE`, `AUTHORS.md`, `CITATION.cff`,
`LICENSING.md`, `TRADEMARKS.md`, `CONTRIBUTING.md`, `SECURITY.md` e ADR-0005
documentano origine, boundary, DCO e segnalazioni private. Andrea Provenzali è
identificato come creatore originario e autore della specifica con ORCID
`0009-0009-9677-9840`. Non vengono pubblicati email, codice fiscale, data di
nascita o nazionalità.

README e security policy dichiarano AProDB in beta test e non production-ready.
Il target candidato è il repository pubblico `provenzali/aprodb`; commit,
creazione remota e push saranno annotati soltanto dopo l'esito effettivo.

### Verifica EU AI Act e assistenza IA

È stata riesaminata la base sorgente rispetto al Regolamento (UE) 2024/1689 e
alle linee guida della Commissione applicabili dal 2 agosto 2026. AProDB non
incorpora modelli, chatbot, generazione di contenuti o chiamate runtime a servizi
IA: vector exact e GPU sono calcolo deterministico. L'assistenza di OpenAI Codex
nello sviluppo non modifica la classificazione del prodotto né il boundary
AGPL/Apache; inoltre la guida dell'articolo 50 esclude il codice sorgente dalla
marcatura machine-readable del contenuto sintetico.

Per trasparenza volontaria sono stati aggiunti `AI_ASSISTANCE.md` e
`docs/eu-ai-act-assessment.md`. Andrea Provenzali conserva direzione, revisione,
responsabilità editoriale e attribuzione; Codex non è indicato come autore,
titolare del copyright o contributore. La valutazione va riaperta se entreranno
modelli, inferenza, contenuti generativi, interazione diretta con persone o casi
d'uso regolati. È una valutazione tecnica di progetto, non consulenza legale.

I gate pre-pubblicazione successivi alla modifica delle licenze e del boundary
dei tipi hanno superato `cargo fmt --all --check`, Clippy workspace/all-targets
con warning negati e test workspace, sia CPU-only sia con feature predefinite.
La suite ordinaria mantiene separati e motivati il capacity gate da 129 MiB, il
gate lungo con 2.000 scritture Durable/quattro restore/rekey e il confronto GPU
su adapter reale. Il packaging dei nove crate è riuscito; i manifest avvisano
ancora che manca il repository URL, che potrà essere inserito solo dopo la
creazione effettiva del remoto.

La scansione finale sui 129 file candidati non ha rilevato file da almeno 10
MiB, pattern forti di segreti o indirizzi email. `.gitignore` è stato esteso con
directory root dati, database, WAL e snapshot e con l'estensione `.aprodb`;
`Cargo.lock` rimane versionabile e `target/bench-lab` non è stato rimosso.
