# AProDB — Specifica dell'architettura radiale adattiva

**Stato:** baseline normativa stabilizzata per l'implementazione
**Versione del documento:** 1.4
**Data:** 19 agosto 2026
**Lingua di riferimento:** italiano
**Implementazione di riferimento:** Rust, edition 2024

> [!NOTE]
> Questo documento è una specifica tecnica pubblica del prodotto obiettivo, non
> un articolo accademico sottoposto a peer review e non una dichiarazione che
> ogni funzione descritta sia già disponibile. Lo stato implementato e
> collaudabile è documentato nel [manuale](manual.md); AProDB è in beta test.

## Sommario

AProDB è un database specializzato per sistemi nei quali i dati attraversano un ciclo di lavorazione, hanno utilità non uniforme e perdono o acquistano valore con il tempo. Esempi sono piattaforme editoriali, raccolta e arricchimento di contenuti, osservabilità, intelligence, cataloghi in aggiornamento continuo, code di elaborazione e superfici applicative alimentate da eventi.

La proprietà distintiva di AProDB non è la semplice presenza di una cache. Il motore considera nativi:

- freschezza;
- probabilità di accesso;
- urgenza di lavorazione;
- stato di preparazione;
- costo di ricostruzione;
- forma più conveniente del dato;
- componente hardware più adatto a servirlo o trasformarlo.

Il record canonico rimane durevole. Attorno a esso AProDB costruisce proiezioni ricostruibili: indici, blocchi decompressi, rappresentazioni colonnari, code di lavoro e superfici già serializzate. CPU, cache hardware, RAM, NVMe, SSD, HDD e GPU sono trattati come una gerarchia eterogenea. Nessuna funzionalità corretta dipende dalla presenza di una GPU.

Questa specifica stabilisce il prodotto obiettivo e separa esplicitamente:

- ciò che deve appartenere alla prima versione server single-node;
- ciò che può essere introdotto dopo senza cambiare il formato logico;
- ciò che AProDB sceglie deliberatamente di non essere.

## 1. Regole normative

Nel documento:

- **DEVE** e **NON DEVE** indicano un requisito necessario;
- **DOVREBBE** indica la scelta predefinita, derogabile solo con misura e motivazione;
- **PUÒ** indica una capacità opzionale;
- **sperimentale** indica una funzione che non può essere necessaria per leggere, recuperare o amministrare i dati canonici.

Il paper è la fonte normativa dell'architettura. Il manuale descrive soltanto funzioni realmente disponibili. Il diario registra decisioni, implementazioni, test e deviazioni. Se il codice richiede una modifica a questa specifica, la modifica deve essere dichiarata con una decisione architetturale; non può avvenire silenziosamente.

## 2. Problema e motivazione

I database relazionali generalisti offrono SQL, join arbitrari, vincoli complessi, più livelli di isolamento e un ecosistema maturo. Queste proprietà sono preziose, ma non sempre coincidono con il percorso dominante di applicazioni come una redazione automatizzata:

1. acquisire nuovi elementi;
2. impedire o rilevare duplicati;
3. assegnare lavoro a processi concorrenti;
4. arricchire progressivamente gli elementi;
5. pubblicare una vista ordinata e pronta;
6. servire soprattutto elementi recenti;
7. raffreddare e comprimere ciò che perde probabilità di accesso;
8. riattivare elementi vecchi quando tornano rilevanti.

L'analisi statica del server Commit ha mostrato un esempio concreto: PostgreSQL coordina correttamente worker concorrenti mediante transazioni, advisory lock, SKIP LOCKED, vincoli unici e lease; la superficie pubblica è invece una vista materializzata ampia, rinfrescata periodicamente nella sua interezza. AProDB non nasce per copiare Commit né per sostituire PostgreSQL senza equivalenti di correttezza. Nasce per rendere incrementali e native le operazioni ricorrenti di questa classe di sistemi.

## 3. Obiettivi

AProDB DEVE:

1. funzionare su una macchina priva di GPU;
2. offrire un servizio centrale sicuro per più processi client;
3. garantire persistenza e recupero verificabili;
4. rendere atomici claim, lease, completamento e confronto di versione;
5. privilegiare letture puntuali, finestre temporali, code ordinate e superfici incrementali;
6. mantenere memoria e lavoro in background entro budget espliciti;
7. comprimere adattivamente tutti i dati che attraversano il percorso persistente, conservandoli grezzi quando comprimere sarebbe svantaggioso;
8. sfruttare layout favorevoli alle cache CPU;
9. usare la GPU soltanto quando il costo totale previsto è inferiore al percorso CPU;
10. esporre staleness, watermark, durabilità e compromessi invece di nasconderli;
11. poter ricostruire ogni cache o proiezione partendo dallo stato canonico;
12. offrire benchmark riproducibili, separando modalità embedded e client/server.

## 4. Non-obiettivi della prima versione

AProDB 1.x NON DEVE dichiarare:

- compatibilità SQL generale;
- join arbitrari;
- transazioni serializzabili fra shard;
- sostituzione universale di PostgreSQL, MySQL o MariaDB;
- durabilità affidata alla VRAM;
- accelerazione garantita per il solo fatto di usare una GPU;
- replica multi-leader;
- esecuzione nel database di codice applicativo non fidato;
- memoria illimitata o dataset necessariamente residente per intero in RAM;
- risultati prestazionali estrapolati da benchmark in-process verso server interrogati via rete.

Un gateway SQL limitato, un gateway RESP3 e la replica Raft sono estensioni successive, non prerequisiti del nucleo single-node.

## 5. Modello concettuale: sfera, raggio e settori

### 5.1 Nucleo

Il nucleo è il piano di controllo. Contiene:

- ordinamento degli eventi;
- routing agli shard;
- adattatore di durabilità e change log;
- versioni e fencing token;
- code, claim e lease;
- budgeting e backpressure;
- catalogo di collezioni, indici e proiezioni;
- scheduler CPU/GPU;
- metriche e stato operativo.

I processi di dominio, come bibliotecari o redazioni, rimangono client esterni. Il nucleo ne coordina il lavoro, ma non incorpora automaticamente la loro logica. Operatori interni ammessi sono soltanto operatori deterministici e controllati, come filtri, ordinamento, top-k, hashing, compressione e ricerca vettoriale.

### 5.2 Strati radiali

Gli strati sono logici; possono condividere lo stesso dispositivo fisico:

| Strato | Contenuto prevalente | Forma | Durabilità |
|---|---|---|---|
| Superficie | Risposte e record pronti | Non compressa o già serializzata | Ricostruibile |
| Hot | Record e colonne ad alta probabilità d'uso | Grezza o compressione velocissima | Canonica o ricostruibile |
| Warm | Blocchi recenti e indici secondari | Zstandard rapido | Canonica |
| Cold | Segmenti poco consultati | Zstandard più denso, dizionari | Canonica |
| Archive | Dati storici | Segmenti grandi, compressione forte | Canonica |

La superficie non è la fonte di verità. La sua perdita può peggiorare temporaneamente le prestazioni, mai perdere una scrittura confermata secondo la durabilità richiesta.

### 5.3 Settori

Il raggio esprime prontezza e latenza attesa; il settore esprime finalità o fase. Uno stesso record può alimentare settori diversi:

- acquisizione;
- deduplicazione;
- classificazione;
- traduzione;
- moderazione;
- pubblicazione;
- commenti;
- analisi;
- archivio.

Questo evita l'errore di rappresentare il ciclo di vita con una sola temperatura. Una notizia può essere calda per un classificatore ma non ancora presente sulla superficie pubblica.

### 5.4 Due superfici

AProDB distingue:

- **superficie di lavoro:** elementi che un worker deve reclamare immediatamente;
- **superficie di lettura:** elementi pronti da mostrare a un utente o consumare da un servizio.

Le due superfici hanno politiche, ordinamenti, consistenza e budget indipendenti.

## 6. Modello del calore radiale

Ogni record possiede un descrittore radiale separato dal payload. Il descrittore contiene almeno:

- tempo di creazione e ultimo aggiornamento;
- stima decadente della frequenza di accesso;
- ultimo accesso campionato;
- emivita di freschezza definita dalla collezione;
- urgenza e scadenza opzionali;
- stato di lavorazione;
- prontezza per ciascuna proiezione;
- costo stimato di ricostruzione;
- dimensione logica e fisica;
- classe di storage;
- pin amministrativo;
- versione canonica.

Il punteggio iniziale è minimo:

    radial_score = wf * freshness + wu * workflow_urgency

con componenti nell'intervallo da zero a uno; il pin amministrativo prevale sul punteggio. La freschezza usa decadimento esponenziale rispetto all'emivita della collezione. Urgenza e stato di lavorazione sono segnali espliciti, non deduzioni opache.

Segnali aggiuntivi — calore d'accesso con contatore probabilistico decadente, prontezza, costo di ricostruzione e pressione di dimensione — sono previsti dal descrittore ma entrano nel punteggio soltanto quando misure dimostrano che la versione minima colloca male i dati. Ogni segnale aggiunto dichiara peso, motivazione e telemetria.

Il punteggio NON DEVE decidere correttezza, autorizzazione o cancellazione canonica. Decide ammissione, promozione, prefetch e vittima di cache.

Devono esistere:

- soglie diverse per promozione e retrocessione;
- permanenza minima nello strato;
- limite di migrazioni per intervallo;
- protezione contro scansioni una tantum;
- pin con scadenza;
- telemetria della motivazione di ogni decisione.

I pesi sono configurabili per collezione. L'autotuning può proporli o variarli entro limiti, ma deve poter essere disattivato e deve registrare ogni cambiamento.

## 7. Modello logico dei dati

### 7.1 Identità

L'identità completa è:

    tenant / namespace / collection / partition / key

- tenant isola quote e autorizzazioni;
- namespace raggruppa applicazioni;
- collection definisce schema e policy;
- partition è l'unità di atomicità e routing;
- key identifica il record.

Le chiavi sono byte string con limite configurabile. Le convenzioni testuali non devono essere necessarie al motore.

### 7.2 Record canonico

Il record canonico contiene:

- identità;
- payload tipizzato oppure opaco;
- content type;
- versione;
- created_at e updated_at assegnati dal server;
- TTL opzionale;
- metadata utente con limiti;
- descrittore di workflow;
- idempotency key opzionale;
- checksum e riferimento al dizionario di compressione;
- tombstone in caso di cancellazione.

I tipi minimi sono:

- bytes;
- UTF-8 text;
- signed integer 64 bit;
- float 64 bit finito;
- boolean;
- timestamp;
- vettore float 32 bit con dimensione dichiarata;
- document strutturato con schema versionato;
- riferimento a blob.

Oggetti grandi superano una soglia configurabile e vengono collocati in blob segmentati. Record canonico, indici e change log conservano riferimenti, non copie ripetute del corpo.

### 7.3 Schema

Una collection può essere:

- schemaless, con payload opaco;
- typed, con campi e tipi dichiarati;
- columnar, destinata a batch e proiezioni.

Le evoluzioni ammesse senza riscrittura sono aggiunta di campi opzionali e nuovi indici derivati. Modifiche incompatibili richiedono nuova versione di schema e migrazione esplicita. Il file format non deve dipendere dalla disposizione in memoria di una struct Rust.

### 7.4 Versioni

La versione è una tupla ordinabile:

    epoch, shard_id, sequence

Epoch cambia quando un'autorità di scrittura viene ricreata o promossa. Sequence cresce monotonicamente nello shard. Una versione non viene riutilizzata.

## 8. Modello delle operazioni

### 8.1 Operazioni fondamentali

La prima versione server DEVE offrire:

- Put;
- Get;
- GetBatch;
- Delete;
- CompareAndSwap;
- AtomicBatch entro una partizione;
- ScanPrefix limitata;
- ScanTimeRange su indice;
- QueryIndex su indici dichiarati;
- Append;
- Claim;
- Heartbeat;
- Complete;
- Fail;
- Publish;
- GetSurface;
- SubscribeChanges;
- Sync;
- CreateCheckpoint;
- Stats e ExplainPlacement.

Ogni comando di mutazione può portare una idempotency key. Il server conserva l'esito entro una finestra configurata e restituisce lo stesso esito ai retry.

### 8.2 Operazioni non offerte

Non esistono query arbitrarie su campi non indicizzati né join impliciti. Una richiesta non supportata deve fallire chiaramente, non degradare in una scansione completa non dichiarata.

### 8.3 Claim e lease

Claim seleziona atomicamente elementi eleggibili, ne cambia lo stato e restituisce:

- record e versione;
- lease_id casuale;
- fencing_token monotono;
- lease_deadline;
- server_time;
- retry metadata.

Heartbeat estende soltanto un lease ancora valido. Complete e Fail richiedono lease_id e fencing_token. Un worker obsoleto non può sovrascrivere il risultato di un worker subentrato.

Dopo un riavvio, i lease persistiti vengono rivalutati usando tempo del server e policy della collection. Il tempo monotono è usato durante il processo; la scadenza persistita usa UTC e tollera un intervallo di sicurezza configurato. L'orologio non deve essere usato per ordinare scritture canoniche: a tale scopo serve sequence.

La semantica verso worker esterni è at-least-once con idempotenza. AProDB non promette exactly-once per effetti prodotti fuori dal database.

## 9. Architettura di processo

### 9.1 Modalità server

La modalità raccomandata è un daemon che possiede in esclusiva la directory dati. I client locali usano Unix domain socket su Linux/macOS e named pipe su Windows quando disponibile; TCP è disponibile per accesso remoto.

All'apertura il server acquisisce un lock esclusivo di processo. Una seconda istanza sulla stessa directory deve rifiutarsi di partire. Il lock non sostituisce durabilità e recovery del backend.

### 9.2 Modalità embedded

La libreria embedded rimane supportata per test e applicazioni a processo singolo. Usa lo stesso storage engine e acquisisce lo stesso lock. Non consente condivisione della directory fra processi.

### 9.3 Componenti

Il processo contiene:

- acceptor e decoder del protocollo;
- autenticazione e quote;
- router delle partizioni;
- actor di scrittura per shard;
- snapshot di lettura;
- adattatore storage e group commit;
- catalogo e change log logico;
- working set e snapshot del backend;
- indici;
- cache manager radiale;
- projection builder;
- compaction e tiering scheduler;
- CPU compute pool;
- GPU scheduler opzionale;
- metriche, tracing e amministrazione.

I thread di rete, scrittura, compaction e calcolo non devono condividere un unico pool non limitato. Il database deve evitare oversubscription fra runtime asincrono, Rayon e driver GPU.

### 9.4 Workspace Rust

La migrazione dal crate unico a un workspace dovrebbe produrre confini aciclici:

| Crate | Responsabilità |
|---|---|
| aprodb-types | identificatori, record envelope, versioni, errori e configurazione condivisa |
| aprodb-storage | contratto di storage, adattatore sul backend incorporato, checkpoint, blob, codec e recovery |
| aprodb-engine | shard actor, working set, cache, indici, workflow, query e proiezioni |
| aprodb-compute | operatori CPU e backend GPU opzionali |
| aprodb-proto | schema wire e compatibilità protocollo |
| aprodb-client | client asincrono e bloccante |
| aprodb-server | daemon, trasporti, auth, quote e amministrazione |
| aprodb-cli | comandi utente e operativi |
| aprodb | facade compatibile e modalità embedded |

Il grafo desiderato è types alla base; storage e compute dipendono da types; engine dipende da types, storage e compute; protocollo non dipende dal server; client e server dipendono dal protocollo.

Rust unsafe è vietato per default. Quando necessario per I/O, SIMD o interoperabilità GPU deve essere confinato in moduli piccoli, accompagnato da invarianti di sicurezza, test Miri dove applicabile e fallback safe. Le feature GPU, direct I/O, encryption e replica devono compilare separatamente; la build CPU-only resta il gate principale.

## 10. Concorrenza e consistenza

### 10.1 Single writer per shard

Ogni shard ha un solo ordinatore logico delle mutazioni. Non è necessario dedicare permanentemente un thread a ogni shard: più actor possono essere eseguiti da un numero limitato di worker, ma due mutazioni dello stesso shard non possono essere applicate fuori ordine.

Le letture usano strutture immutabili o snapshot pubblicati atomicamente. Il percorso di lettura non aggiorna una lista LRU globale a ogni hit; il campionamento del calore avviene tramite contatori locali e aggregazione periodica.

### 10.2 Garanzie

Sul leader single-node:

- Get dopo Put confermato vede almeno quella versione;
- CompareAndSwap è lineare entro la chiave;
- AtomicBatch è atomico entro una partizione;
- Claim è atomico rispetto ad altri Claim della stessa partizione;
- snapshot di una singola partizione è coerente;
- superfici e indici derivati possono essere in ritardo, ma espongono watermark e versione.

AProDB 1.x non offre una transazione serializzabile che coinvolga partizioni diverse. Operazioni che richiedono atomicità devono usare una chiave di partizione comune oppure un workflow con outbox, compensazione e idempotenza.

### 10.3 ACID dichiarato

Entro una partizione:

- **Atomicità:** il batch appare interamente o non appare;
- **Consistenza:** versioni, schema, vincoli locali e transizioni sono verificati;
- **Isolamento:** le mutazioni sono serializzate dall'actor e le letture vedono snapshot pubblicati;
- **Durabilità:** dipende dalla modalità esplicitamente richiesta.

La documentazione non deve usare la parola ACID senza indicare questo perimetro.

### 10.4 Backpressure

Ogni coda è limitata. Quando persistenza, compaction, proiezioni, memoria o GPU accumulano debito, il server rallenta o rifiuta nuove operazioni con errore ritentabile e retry-after. Non deve continuare ad allocare memoria fino all'intervento dell'OOM killer.

## 11. Pipeline di scrittura

Una mutazione segue questo ordine logico:

1. decodifica con limite di dimensione;
2. autenticazione, autorizzazione e quota;
3. validazione di chiave, schema e idempotenza;
4. routing a partizione e shard;
5. controllo di versione o lease;
6. assegnazione della sequence;
7. costruzione del batch atomico con record, catalogo, indici essenziali e change log;
8. commit del backend secondo durabilità;
9. pubblicazione del working set aggiornato;
10. pubblicazione del nuovo snapshot di lettura;
11. notifica dei consumer del change log;
12. risposta con versione e durability receipt.

Il punto 8 precede la conferma nelle modalità durevoli. Indici e superfici possono aggiornarsi dopo la conferma; il record canonico no.

### 11.1 Durabilità

Le modalità pubbliche sono:

| Modalità | Conferma | Garanzia |
|---|---|---|
| Durable | dopo il punto persistente documentato dal backend, con finestra di group commit configurabile | sopravvive a crash di processo e perdita di alimentazione nei limiti dichiarati da backend, OS e dispositivo |
| Relaxed | dopo consegna al sistema operativo | può perdere la coda recente in un power loss |
| Ephemeral | solo memoria | nessuna persistenza |

Durable è il valore raccomandato per servizi concorrenti: la finestra di group commit ammortizza la latenza e una finestra pari a zero equivale a un flush per richiesta. La receipt include shard, sequence, modalità applicata e durable watermark noto. Il server non deve promuovere una modalità meno forte di quella richiesta.

Le garanzie reali dipendono dal rispetto di flush/barrier da parte di filesystem, virtualizzazione e dispositivo. AProDB deve documentare e testare la piattaforma; non può promettere più di quanto il livello sottostante garantisca.

### 11.2 Group commit

L'adattatore storage raccoglie richieste per una finestra massima e per un limite di byte, scegliendo il primo limite raggiunto. Una richiesta può forzare il prossimo commit persistente; una finestra pari a zero evita attesa intenzionale. La finestra è configurabile e osservabile. L'adattatore non deve promettere batching o flush che il backend non espone.

## 12. Pipeline di lettura

Get esegue:

1. routing;
2. controllo della microcache di fingerprint, se abilitata;
3. ricerca nel working set o snapshot del backend;
4. consultazione degli indici logici e fisici disponibili;
5. controllo cache oggetti o blocchi decompressi;
6. lettura del record o blocco tramite l'adattatore;
7. verifica di integrità;
8. decompressione con contesto del worker;
9. decodifica e verifica versione;
10. campionamento asincrono del calore;
11. eventuale ammissione in cache.

La lettura di una superficie usa una generazione immutabile già pronta. Una generazione viene sostituita con uno swap atomico; i lettori in corso possono terminare sulla generazione precedente.

Una risposta derivata deve includere:

- projection_generation;
- source_watermark per shard interessato;
- generated_at;
- stale_by;
- complete o partial;
- eventuali errori di settore.

## 13. Memoria e cache

### 13.1 Cache hardware CPU

L1, L2 e L3 sono amministrate dal processore. AProDB non tenta di bloccarvi record. Il motore rende favorevole il percorso mediante:

- strutture contigue;
- separazione fra campi hot e cold;
- struct-of-arrays per scansioni;
- bucket e descrittori compatti;
- batching;
- allocazioni ridotte;
- code e contatori per worker;
- allineamento per evitare false sharing;
- prefetch soltanto quando misurato;
- sharding coerente con topologia CPU e NUMA.

Il motore rileva dimensioni cache e topologia, ma non codifica come universale una cache line o una capacità. Le strutture devono rimanere corrette su hardware non rilevabile.

Indicativamente:

- L1/L2 beneficiano loop, fingerprint, code e piccoli bucket locali;
- L3 beneficia directory dei segmenti, filtri e parti calde degli indici;
- RAM contiene payload, working set, superfici e blocchi.

### 13.2 Budget globale

Il server determina un effective memory limit dal minimo fra configurazione, limite container/job e memoria fisica disponibile. Se l'utente non imposta un budget, il valore iniziale prudente è il 50% del limite effettivo rilevato. Il valore viene mostrato all'avvio e può essere rifiutato in ambienti nei quali la rilevazione è incerta.

Pool iniziali, riallocabili entro minimi e massimi:

| Pool | Quota iniziale |
|---|---:|
| Working set e write buffers | 20% |
| Indici e metadata | 20% |
| Cache oggetti hot | 20% |
| Cache blocchi decompressi | 15% |
| Superfici | 15% |
| I/O, compressione e riserva di emergenza | 10% |

Il totale comprende overhead misurabile, non soltanto payload. Iterator, batch e risposte in volo devono essere contabilizzati. Nessun pool può prendere la riserva di emergenza.

### 13.3 Cache specializzate

AProDB usa cache separate:

1. **metadata cache:** directory, footer, Bloom filter e radiale;
2. **object cache:** valori decodificati frequentemente;
3. **decompressed block cache:** blocchi verificati e decompressi;
4. **compressed block cache opzionale:** utile soprattutto con direct I/O;
5. **surface cache:** generazioni già serializzate;
6. **negative cache:** assenze con TTL breve e versione del catalogo;
7. **VRAM cache:** proiezioni GPU ricostruibili.

Una scansione non deve espellere automaticamente il working set puntuale. Le scansioni usano una classe di ammissione separata o bypassano la object cache.

### 13.4 Ammissione ed espulsione

La policy predefinita è una variante radiale di Window TinyLFU:

- una piccola finestra protegge elementi appena osservati;
- una stima TinyLFU decadente confronta candidato e vittima;
- il radial score aggiunge freschezza, urgenza, prontezza e costo di ricostruzione;
- una SLRU separa probation e protected;
- dimensione e costo di mantenimento pesano sulla decisione.

Le letture non acquisiscono un mutex globale per aggiornare l'ordine. Eventi campionati confluiscono periodicamente nelle strutture di policy. Pin, TTL e quote per tenant prevalgono sul punteggio.

### 13.5 Coerenza

Ogni elemento derivato porta source version o watermark. Una mutazione:

- aggiorna la fonte canonica;
- invalida o aggiorna le proiezioni interessate;
- non modifica in-place una generazione visibile;
- pubblica la nuova generazione soltanto quando completa secondo la policy.

Una cache non può confermare una scrittura canonica. Write-back è ammesso soltanto per dati dichiarati Ephemeral; per gli altri percorsi la cache è read-through o write-through dopo il commit del backend.

### 13.6 Page cache del sistema operativo

Il backend predefinito usa buffered I/O e beneficia della page cache, perché è portabile e sicuro come punto di partenza. Un backend direct-I/O può essere attivato su piattaforme supportate quando:

- AProDB dispone di un budget completo;
- allineamento e dimensioni sono validati;
- benchmark mostrano che la doppia cache è dannosa;
- esiste fallback automatico.

Direct-I/O non è sinonimo di maggiore velocità e non deve essere abilitato per slogan.

## 14. Adattamento all'hardware

### 14.1 Hardware profile

All'avvio AProDB costruisce un profilo versionato:

- architettura e set SIMD;
- core fisici e logici;
- cache e NUMA;
- memoria e limiti del container;
- tipo e capacità dei filesystem;
- settore logico e requisiti di allineamento;
- rotazionale, SSD o NVMe se rilevabile;
- GPU, VRAM, capacità di calcolo e trasferimento;
- versione di OS, driver e backend.

Le informazioni incerte sono marcate come tali. Il profilo non deve contenere identificatori sensibili nei log pubblici.

### 14.2 Calibrazione

Una calibrazione breve e limitata misura:

- bandwidth e latenza memoria;
- costo di hash, compressione e decompressione su taglie rappresentative;
- latenza e throughput I/O;
- costo fisso e throughput GPU;
- dimensione di batch di pareggio.

I risultati sono salvati con fingerprint hardware/software. La calibrazione non esegue scritture distruttive sui dati e può essere disabilitata. Le decisioni runtime usano misure mobili e non soltanto il benchmark iniziale.

### 14.3 CPU e NUMA

Lo shard count è potenza di due per routing veloce, ma non coincide necessariamente con il numero di thread. Per macchine NUMA:

- working set e code sono allocati preferibilmente nel nodo del worker;
- compaction e GPU staging rispettano affinità quando possibile;
- accessi cross-node sono misurati;
- il pinning dei thread rimane configurabile e inizialmente sperimentale.

Il percorso CPU è l'implementazione di riferimento per tutti gli operatori.

## 15. GPU e calcolo eterogeneo

### 15.1 Regola fondamentale

La GPU è opzionale, volatile e ricostruibile. Persistenza, catalogo, autorizzazione, lease, ordinamento e recovery non dipendono dalla GPU.

Ogni operatore accelerato implementa la stessa semantica su CPU. Per float e vettori sono dichiarate tolleranze numeriche e regole per NaN, infinito e ordinamento dei pareggi.

### 15.2 Operazioni candidate

Sono candidate:

- distanza vettoriale e top-k;
- filtri colonnari su grandi batch;
- aggregazioni;
- ordinamenti e ranking;
- hashing o deduplicazione massiva;
- trasformazioni numeriche;
- compressione e decompressione di grandi batch, come estensione;
- costruzione di alcune proiezioni.

Non sono candidate nel primo percorso:

- singolo Get;
- piccola mutazione;
- fsync;
- claim e lease;
- parsing complesso e molto ramificato;
- operazioni con trasferimento superiore al lavoro.

### 15.3 Scheduler

Il scheduler seleziona GPU soltanto se:

    transfer_in + queue_wait + launch + gpu_compute
      + transfer_out + synchronization + risk_margin
      < estimated_cpu_compute

La stima include probabilità di riuso di dati già in VRAM. Sono necessari:

- micro-batching con attesa massima;
- buffer host pinned solo entro un budget;
- trasferimenti asincroni;
- più buffer o stream quando supportati;
- limite di richieste in volo;
- timeout, circuit breaker e fallback CPU;
- metriche di hit VRAM e tempo di trasferimento;
- isolamento degli errori del driver.

Un errore GPU non deve abbattere il database. Il dispositivo viene messo in cooldown e il lavoro ritentato su CPU quando semanticamente sicuro.

Se un risultato GPU deve produrre una mutazione o una proiezione, il motore confronta nuovamente source version e watermark prima della pubblicazione. Un risultato calcolato su input ormai superato viene scartato o ricalcolato; non può sovrascrivere uno stato più recente.

### 15.4 Formati

La rappresentazione GPU è colonnare, con buffer contigui, validity bitmap, offset e allineamento dichiarati. Apache Arrow è il riferimento concettuale per l'interoperabilità in memoria; AProDB non deve necessariamente dipendere dall'intero runtime Arrow nel nucleo.

La VRAM conserva soltanto projection_id, source watermark, schema version e buffer derivati. Il cambio di schema o generazione invalida la proiezione.

### 15.5 Backend

Il primo backend portabile può usare wgpu. Backend CUDA o HIP possono essere aggiunti dietro la stessa interfaccia per operatori nei quali portabilità e prestazioni divergono. Il file format e il protocollo non devono codificare un vendor GPU.

## 16. Storage fisico

### 16.0 Contratto di backend

I capitoli 16, 17, 18 e 20 definiscono le garanzie che il backend di storage DEVE fornire, non un obbligo di reimplementare un LSM. La prima implementazione DOVREBBE usare un motore incorporato esistente scelto nella Milestone 0.5. Fjall è il primo candidato da verificare; non è una dipendenza approvata prima dello spike e dell'ADR. Redb e RocksDB rimangono vie di uscita documentate.

Il contratto minimo comprende:

- batch atomico per record, indici essenziali e log eventi;
- persistenza Durable e Relaxed con punto di conferma dimostrabile;
- snapshot o letture coerenti;
- Get, range e prefix iteration limitabili;
- recovery dopo crash;
- checkpoint o backup consistente;
- dataset maggiori della RAM;
- compressione fisica configurabile o almeno dichiarata per keyspace, dati e indici;
- limiti e telemetria sufficienti a prevenire crescita incontrollata;
- comportamento definito per compaction, spazio esaurito e corruzione.

Garanzie, record envelope, versioni, log eventi, watermark e formati logici appartengono ad AProDB. WAL, memtable, segmenti, manifest e compaction fisici appartengono invece al backend incorporato. AProDB non duplica il WAL del backend.

La sostituibilità non è gratuita: transazioni, snapshot, iteratori, backup e controllo della compaction possono differire. L'adattatore espone una capability matrix e non simula capacità mancanti con garanzie più deboli. Un cambio di backend richiede export/import verificato o una migrazione esplicita. Un motore nativo viene scritto soltanto se misure concrete dimostrano che il backend scelto impedisce caratteristiche essenziali di AProDB.

I test di fault injection, recovery e durabilità si applicano al contratto qualunque sia il backend.

### 16.1 Principi

Un eventuale backend nativo di riferimento combina:

- WAL append-only;
- memtable mutabile e limitata;
- segmenti immutabili ordinati;
- manifest transazionale;
- compaction in background;
- blob separati per valori grandi.

La promozione radiale non riscrive continuamente il record canonico. Preferisce creare o eliminare proiezioni. I segmenti canonici migrano fra classi fisiche con granularità di file o extent, non a ogni lettura.

Con un backend incorporato, questi elementi sono implementazione privata del backend. AProDB gestisce sopra di essi record logici, settori, indici propri necessari, superfici e placement ricostruibile.

### 16.2 Supporti

**NVMe:** code asincrone, batch e parallelismo limitato alla queue depth utile; ideale per WAL, segmenti recenti e compaction.
**SSD SATA/SAS:** concorrenza più moderata; warm e cold.
**HDD:** accesso sequenziale, segmenti grandi e archivio; evita lookup casuali e compaction aggressiva durante il servizio.
**Un solo dispositivo:** gli strati rimangono logici e differiscono per formato, cache e priorità I/O.

L'utente registra una o più storage class con path, budget e preferenza. Il motore rileva il medium ma permette override, perché virtualizzazione e RAID possono nasconderlo.

### 16.3 Priorità I/O

L'ordine predefinito è:

1. WAL e recovery;
2. letture foreground;
3. surface publication;
4. flush memtable;
5. compaction necessaria contro stall;
6. prefetch;
7. migrazione e archivio.

Compaction e tiering consumano token di bandwidth e IOPS. Non devono saturare il dispositivo fino a degradare senza limite la latenza foreground.

Se il backend incorporato non espone priorità o tiering sufficienti, l'adattatore dichiara la capacità assente. La funzione rimane disabilitata o viene realizzata a livello di proiezioni AProDB; non si dichiara un controllo fisico inesistente.

## 17. Durabilità fisica e log eventi

### 17.1 WAL del backend incorporato

Il WAL fisico, il suo framing e il recovery appartengono al backend incorporato. L'adattatore AProDB DEVE:

- mappare Durable e Relaxed su primitive documentate;
- confermare Durable soltanto dopo il punto persistente offerto dal backend;
- mantenere AtomicBatch indivisibile;
- verificare riapertura, coda incompleta e crash mediante fault test;
- esporre durable watermark e failure mode;
- impedire l'apertura concorrente non supportata.

AProDB non interpreta né modifica direttamente i file WAL privati del backend.

### 17.2 Log eventi logico AProDB

AProDB conserva un change log ordinato e versionato nello stesso batch atomico della mutazione canonica e degli indici necessari. Il log contiene almeno:

- collection e partition;
- epoch, shard e sequence;
- tipo di operazione;
- key o riferimento;
- versione precedente e nuova quando disponibili;
- transaction o batch id;
- idempotency hash se presente;
- metadata necessari a proiezioni e workflow;
- riferimento al payload e alla sua versione oppure delta minimo sufficiente;
- checksum logico o integrità fornita dal record envelope.

Il change log alimenta SubscribeChanges, proiezioni, superfici, watermark e rebuild incrementali. Non sostituisce il WAL fisico e non viene usato per promettere una durabilità maggiore di quella del backend.

L'evento NON DEVE duplicare per default il payload completo. Ogni collection dichiara una EventRetentionMode:

- **Delta:** l'evento contiene il delta minimo e autosufficiente richiesto dalle proiezioni;
- **VersionRef:** record corrente ed evento riferiscono un oggetto immutabile identificato da key/version o content hash;
- **SelfContained:** l'evento include il payload soltanto per una policy esplicita, con limiti di dimensione e retention.

In VersionRef il payload immutabile viene scritto una sola volta; head corrente ed evento conservano riferimenti. La versione resta leggibile finché tutti i consumer obbligatori hanno superato il watermark e finché backup o futura replica la richiedono. Leggere semplicemente la versione corrente non è corretto se nel frattempo esiste un aggiornamento successivo.

Gli snapshot MVCC del backend sono utilizzabili per coerenza di una richiesta breve, NON come meccanismo di retention durevole. Non sopravvivono come contratto applicativo a un riavvio e, se trattenuti a lungo, possono impedire il garbage collection delle versioni obsolete. Retention e compaction AProDB devono usare chiavi versionate, oggetti content-addressed o delta autosufficienti.

Il costo del change log viene misurato separatamente: byte evento/payload, write amplification, latenza Durable, throughput, spazio dopo compaction e costo del rebuild.

Un AtomicBatch produce un unico commit logico oppure un gruppo indivisibile identificato dallo stesso batch id. Nessun consumer può osservare un prefisso del batch. Gli eventi vengono rimossi soltanto quando checkpoint, retention, proiezioni, backup e futura replica non li richiedono più.

### 17.3 Formato per un eventuale backend nativo

Il WAL è una sequenza di segmenti numerati e preallocabili. Ogni frame contiene:

- magic e format version;
- frame type;
- flags;
- shard e epoch;
- sequence o intervallo;
- transaction/batch id;
- idempotency hash se presente;
- lunghezza header e payload;
- payload;
- checksum CRC32C dei byte memorizzati.

Record grandi sono frammentati con first, middle, last e identificatore comune. Il recovery:

1. legge manifest e checkpoint valido;
2. ordina i segmenti WAL;
3. verifica frame e sequence;
4. riapplica soltanto eventi successivi al checkpoint;
5. ignora o tronca una coda incompleta;
6. considera corruzione un errore nel mezzo di dati già confermati;
7. produce un report e non nasconde record saltati.

Un AtomicBatch è rappresentato da un singolo record logico oppure da una sequenza begin/part/commit con checksum e conteggio complessivi. Recovery applica il batch soltanto se il commit è valido e tutte le parti sono presenti; non rende mai visibile un prefisso del batch.

Il WAL viene riciclato soltanto dopo checkpoint durevole, manifest pubblicato e rispetto delle necessità di replica o backup.

Questa sottosezione è normativa soltanto per un backend nativo AProDB. Non impone il formato fisico a Fjall, redb, RocksDB o altri backend incorporati.

## 18. Catalogo logico, segmenti e manifest

### 18.1 Backend incorporato

Segmenti, Bloom filter, manifest fisico, file temporanei e compaction appartengono al backend incorporato. L'adattatore verifica le garanzie richieste e traduce metriche e checkpoint quando disponibili.

AProDB mantiene in uno spazio logico dedicato e transazionale:

- schemi e relative versioni;
- dizionari e riferimenti;
- definizioni di indice e proiezione;
- generation e watermark;
- configurazione dinamica;
- idempotency state e retention;
- capability e versione del backend.

Questo catalogo viene aggiornato atomicamente con le operazioni alle quali appartiene oppure tramite una transizione versionata e recuperabile.

### 18.2 Formato per un eventuale backend nativo

Ogni segmento immutabile contiene:

- header con magic, version, UUID, collection, shard e schema;
- intervallo di chiavi e tempo;
- blocchi di record;
- codec e dictionary id per blocco;
- checksum per blocco;
- indice sparse delle chiavi;
- indice temporale;
- Bloom filter;
- statistiche min/max per campi indicizzati;
- tombstone e version bounds;
- footer con offset e checksum.

Tutti gli interi on-disk hanno endian esplicito. Limiti e offset sono verificati prima di allocare memoria. Questo formato non viene imposto ai backend incorporati.

### 18.3 Manifest per un eventuale backend nativo

Il manifest elenca segmenti attivi, checkpoint, dizionari, schemi, proiezioni e generazioni. L'aggiornamento usa:

1. scrittura di un nuovo manifest temporaneo;
2. flush del file;
3. rename atomico supportato;
4. flush della directory ove necessario;
5. conservazione controllata della generazione precedente.

I file orfani sono rilevati all'avvio e quarantinati o recuperati secondo prove, mai aggiunti automaticamente allo stato canonico.

## 19. Compressione integrata

### 19.1 Interpretazione di “comprimere ogni dato”

Ogni valore persistente attraversa il motore di compressione. Il risultato può avere codec Raw quando:

- il valore è troppo piccolo;
- è già compresso o cifrato;
- il campione non produce un guadagno minimo;
- la latenza dello strato prevale sul risparmio;
- la superficie richiede forma pronta.

Conservare Raw è una decisione del compressore, non un aggiramento. Comprimere dati incomprimibili aumenta spazio e CPU.

### 19.2 Codec

Zstandard è il codec persistente generale predefinito per rapporto, velocità, decompressione e dizionari. Ogni payload logico compresso registra codec e versione, consentendo codec futuri senza migrazione immediata. Un eventuale backend nativo può inoltre comprimere blocchi fisici.

Policy iniziale:

- Surface: Raw;
- Hot: Raw oppure Zstandard fast/level basso;
- Warm: Zstandard basso;
- Cold: Zstandard medio scelto dal budget;
- Archive: Zstandard più denso, eseguito fuori dal percorso foreground.

I livelli numerici non sono fissati universalmente: autotuning e benchmark per classi di payload scelgono entro intervalli amministrativi.

### 19.3 Canali

Compressione e decompressione usano un pool limitato di contesti riutilizzabili, normalmente uno per worker attivo, non un contesto globale e non un thread per valore. I canali:

- ricevono batch;
- hanno code limitate;
- espongono tempo, ratio e fallback;
- rispettano priorità foreground;
- non trattengono buffer oltre il budget.

### 19.4 Dizionari

I dizionari sono per collection e schema. Vengono addestrati su campioni limitati in background, validati su un campione distinto e pubblicati soltanto se migliorano una funzione di costo che include spazio e latenza.

Ogni dizionario ha ID, checksum, schema, stato e intervallo di validità. Un dizionario non può essere eliminato finché esiste un blocco che lo richiede. Il caricamento usa forme pre-digerite e condivisibili quando il codec lo consente.

### 19.5 Integrità, cifratura e ordine

Il percorso è:

    encode -> compress decision -> optional authenticated encryption -> checksum/frame

La cifratura usa una libreria verificata e chiavi esterne; AProDB non inventa primitive crittografiche. Metadata sensibili devono poter essere inclusi nella cifratura. Rotazione chiavi e backup sono procedure amministrative esplicite.

### 19.6 Coordinamento con la compressione del backend

La compressione logica AProDB e la compressione fisica del backend sono livelli indipendenti. Non devono essere abilitate entrambe alla cieca sullo stesso contenuto.

Ipotesi iniziale da verificare nella Milestone 0.5:

- payload canonici: Raw/Zstandard AProDB; compressione dei data block del backend disabilitata;
- catalogo, change log e piccoli metadata: payload AProDB raw; compressione veloce del backend abilitata;
- superfici pronte: raw, con compressione backend soltanto se riduce il costo totale;
- immagini, archivi e payload già compressi o cifrati: nessuna seconda compressione;
- indici fisici: policy del backend separata dai data block.

Questa è una matrice sperimentale, non un default approvato. Lo spike confronta almeno:

1. solo Zstandard AProDB;
2. solo compressione veloce del backend;
3. entrambi i livelli;
4. nessuna compressione.

Per ogni variante misura ingest, p95/p99, decompressione, CPU, spazio, compaction e recovery su payload ripetitivi, casuali e già compressi. L'ADR sceglie per keyspace e classe di dati. Se il backend non permette questa distinzione, la capability matrix lo dichiara e l'ADR valuta se il limite è accettabile.

## 20. Memtable, flush e compaction

Con un backend incorporato, memtable, flush e compaction fisici sono responsabilità del backend e non vengono duplicati da AProDB. L'adattatore configura soltanto opzioni supportate, raccoglie metriche e applica backpressure usando segnali reali. Le strutture radiali, le cache e le superfici AProDB restano derivate e separate.

Per un eventuale backend nativo, la memtable contiene versioni recenti e indici minimi. Quando raggiunge soglia di byte o età:

1. viene congelata;
2. una nuova memtable accetta scritture;
3. la congelata viene ordinata;
4. produce segmenti immutabili;
5. il manifest viene pubblicato;
6. il WAL coperto diventa riciclabile.

La compaction è time-partitioned e shard-aware. Deve:

- eliminare versioni superate oltre retention;
- applicare tombstone solo quando nessun livello o replica richiede il record;
- unire segmenti senza mescolare inutilmente finestre temporali;
- ricomprimere secondo strato;
- preservare sequence e checksum;
- evitare write amplification non controllata;
- fermarsi o rallentare quando danneggia il foreground.

Il compaction debt è misurato in byte, segmenti e tempo stimato. Se supera soglie, scatta backpressure prima dell'esaurimento del disco.

Se un backend incorporato non espone il debito di compaction, AProDB usa soltanto indicatori osservabili documentati, come latenza, spazio, stall e backlog. Non inventa una misura precisa non disponibile.

## 21. Indici e query

### 21.1 Indici obbligatori

Ogni collection ha:

- indice esatto della chiave;
- indice di versione;
- indice temporale se dichiara freshness;
- indice di workflow se usa Claim;
- TTL index se usa scadenze.

Gli indici secondari ammessi sono dichiarativi:

- hash equality;
- ordered range;
- prefix;
- composite time/priority/state;
- full-text futuro;
- vector exact o ANN.

Un indice è canonico soltanto per ciò che serve a localizzare il record; gli altri sono derivati e ricostruibili dal log/segmenti. Il catalogo conserva source watermark e stato di build.

### 21.2 Lookup e segmenti

In RAM, il motore può combinare hash index per Get e struttura ordinata per range. Su disco i segmenti sono ordinati e usano indice sparse, Bloom filter e statistiche di blocco per evitare letture.

La scelta concreta di hash table, B-tree, ART o skiplist rimane interna e può cambiare senza modificare il protocollo. Deve essere misurata su layout e workload AProDB; non è un tratto del formato pubblico.

### 21.3 Query planner limitato

Il planner:

- accetta soltanto predicati supportati;
- stima segmenti, blocchi e righe;
- dichiara indice scelto e fallback;
- applica un limite di costo;
- rifiuta una scansione completa se il client non la autorizza esplicitamente;
- può scegliere CPU o GPU per la fase batch.

Explain restituisce piano, stime, tier, possibile staleness e backend compute senza eseguire. Analyze aggiunge misure ed è soggetto ad autorizzazione.

### 21.4 Vettori

Il motore offre:

- ExactFlat CPU obbligatorio;
- ExactFlat GPU opzionale;
- HNSW CPU come indice derivato successivo;
- IVF o product quantization come ricerca sperimentale futura.

Dimensione, metrica e normalizzazione appartengono allo schema. I risultati approssimati devono dichiarare l'indice e i parametri; non possono essere presentati come exact. Aggiornamenti e cancellazioni devono avere una strategia di rebuild e tombstone.

## 22. Superfici e proiezioni

### 22.1 Definizione

Una proiezione nominata specifica:

- collezioni sorgente;
- filtri indicizzati;
- ordinamento totale con tie-break;
- campi o trasformazioni ammesse;
- formato di uscita;
- limite di record e byte;
- finestra temporale;
- staleness massima desiderata;
- policy di pubblicazione;
- dipendenze.

Le trasformazioni di dominio esterne scrivono nuovi campi o eventi; il motore non esegue traduzioni o modelli arbitrari all'interno della transazione.

### 22.2 Aggiornamento incrementale

Ogni evento canonico viene valutato contro le proiezioni dipendenti. Il builder:

1. legge dalla sequence successiva al watermark;
2. applica insert, update o remove alla struttura candidata;
3. verifica ordinamento, limiti e dipendenze;
4. serializza la nuova generazione o delta;
5. pubblica atomicamente;
6. avanza il watermark.

Un rebuild completo avviene su richiesta, schema change, corruzione o gap del change log. Non è la procedura normale periodica.

### 22.3 Formati di superficie

Sono ammessi:

- record AProDB binari;
- JSON pre-serializzato;
- MessagePack o Protobuf definito dalla proiezione;
- Arrow IPC per batch analitici.

Header dinamici e autorizzazioni per utente non devono essere materializzati dentro una superficie condivisa. La risposta applicativa può combinare superficie pubblica e dati personali separati.

### 22.4 Pubblicazione

Una generazione è immutabile e indirizzata da ID. Il puntatore current cambia atomicamente. Il server conserva un numero limitato di generazioni per lettori in corso e rollback operativo. Le generazioni non più referenziate sono rimosse in background.

## 23. Protocollo e API client

### 23.1 Data plane

Il protocollo è binario, versionato e language-neutral. La prima implementazione dovrebbe usare messaggi Protobuf in frame length-delimited con:

- magic e protocol version nel handshake;
- request_id;
- operation;
- deadline;
- tenant e namespace;
- consistency e durability richieste;
- payload;
- response status;
- server version, record version e watermark.

Il trasporto supporta più richieste in volo e batch. Il server impone dimensione massima di frame, inflight per connessione e tempo di inattività.

### 23.2 Trasporti

- named pipe o Unix domain socket per client locali;
- TCP con TLS per remoto;
- plaintext TCP soltanto su loopback o rete esplicitamente fidata;
- un endpoint amministrativo separabile.

Compressione del protocollo è negoziata soltanto per payload sufficientemente grandi e non già compressi. Non deve duplicare inutilmente la compressione dei blocchi persistenti.

### 23.3 API amministrativa

L'API amministrativa offre:

- health, readiness e build info;
- catalogo e configurazione effettiva;
- metriche;
- checkpoint e backup;
- compaction controllata;
- stato indici e proiezioni;
- hardware profile;
- explain placement;
- verifica integrità;
- drain e shutdown ordinato.

Le operazioni distruttive richiedono autorizzazione distinta e conferma del target.

### 23.4 Compatibilità Redis

Un gateway RESP3 può mappare in futuro GET, SET, MGET, DEL, TTL, INCR e subset di stream/queue. È un adattatore, non la semantica interna. Comandi non equivalenti devono fallire; non devono avere un'approssimazione sorprendente.

## 24. Sicurezza

### 24.1 Confini

La directory dati è accessibile soltanto all'account del servizio. Un solo processo la apre in scrittura. I client non ricevono percorsi filesystem.

### 24.2 Identità e autorizzazione

Sono previsti:

- autenticazione locale tramite credenziali del trasporto quando disponibile;
- token brevi o mTLS per remoto;
- ruoli per tenant, namespace, collection e operazione;
- separazione data/admin;
- quote di byte, richieste, connessioni e GPU;
- audit delle mutazioni amministrative.

I token non vengono scritti nei log. Confronti di segreti sono constant-time quando applicabile.

### 24.3 Input

Decoder e parser:

- verificano lunghezze prima di allocare;
- hanno profondità e cardinalità massime;
- rifiutano float non finiti quando lo schema li vieta;
- validano UTF-8 soltanto per tipi text;
- non fidano di offset on-disk;
- sono sottoposti a fuzzing.

### 24.4 Cifratura

TLS protegge il transito. La cifratura at-rest è opzionale ma progettata per blocco con AEAD e key id. Le chiavi provengono da file protetto, variabile iniettata o KMS; non sono inserite nel manifest in chiaro.

## 25. Osservabilità

Metriche minime:

- throughput e latenza p50/p95/p99 per operazione;
- errori e retry;
- queue depth e backpressure;
- backend commit, group size, persist latency e durable watermark;
- working set, write buffer e flush esposti dal backend;
- segmenti, compaction debt e write amplification;
- spazio logico, fisico e temporaneo;
- hit/miss/admission/eviction per cache;
- staleness e build time delle superfici;
- claim, lease expired, heartbeat e stale completion;
- compression ratio e CPU per codec/tier;
- I/O bytes, latency e outstanding;
- CPU pool saturation;
- GPU queue, transfer, kernel, fallback e VRAM;
- recovery time e record riprodotti;
- quote per tenant.

I log sono strutturati e includono event id, component, shard e sequence quando pertinenti. Payload e chiavi complete sono esclusi per default. Il tracing propaga request_id e collega scrittura, commit backend, change log, proiezione e risposta.

Health indica processo vivo; readiness richiede catalogo e shard servibili. Una proiezione in ritardo può rendere degradato un endpoint senza dichiarare morto l'intero database.

## 26. Gestione delle risorse

### 26.1 Disco

Sono configurati:

- quota dati;
- riserva minima libera;
- quota del log e dei file temporanei del backend;
- quota temporanei di compaction;
- soglie warning, throttle e read-only emergency.

Prima di una compaction il motore stima spazio temporaneo. Se non è sufficiente, non inizia e applica backpressure. In emergenza preserva letture e recovery; non cancella dati canonici per liberare spazio.

### 26.2 CPU

Pool separati hanno limiti:

- foreground;
- commit storage;
- compression/decompression;
- compaction;
- projection;
- vector/compute.

La priorità favorisce durabilità e richieste foreground. L'autotuner non può consumare tutti i core senza un margine amministrativo.

### 26.3 GPU

VRAM ha quote per indice/proiezione e riserva per batch. Il server non dipende dall'overcommit del driver. La rimozione da VRAM avviene prima di esaurire memoria; un out-of-memory GPU attiva fallback e cooldown.

### 26.4 Tenant

Ogni tenant può avere limiti su:

- spazio canonico;
- superfici;
- cache;
- richieste e byte al secondo;
- operazioni in volo;
- claim;
- GPU.

Una scansione o proiezione di un tenant non deve affamare gli altri.

## 27. Recovery, backup e riparazione

### 27.1 Recovery

Recovery deve essere deterministico, idempotente e osservabile. Una partenza fallita non modifica irreversibilmente l'unica copia valida. Il server può avviarsi read-only per ispezione.

Le classi di danno sono:

- commit backend incompleto: non visibile dopo recovery;
- stato canonico confermato ma non pubblicato nel working set: recovery o rilettura lo rende visibile;
- change log incoerente con il record: violazione atomica e stop controllato;
- file fisico backend corrotto: recovery, errore o repair secondo le garanzie documentate dal backend;
- segmento derivato corrotto: ricostruzione;
- stato canonico corrotto: ripristino da replica/backup o repair con perdita dichiarata;
- catalogo logico corrotto: prova della versione precedente o restore;
- cache/surface/VRAM persa: rebuild.

### 27.2 Checkpoint

Un checkpoint registra:

- backend checkpoint id e catalog generation;
- durable watermark per shard;
- inventario e checksum esposti dal backend;
- schemi e dizionari richiesti;
- catalogo;
- encryption key ids;
- versione software, formato logico e backend.

### 27.3 Backup

Il backup online:

1. crea o seleziona checkpoint consistente;
2. usa snapshot, checkpoint o procedura di backup supportata dal backend;
3. include catalogo logico, blob e dizionari;
4. include il change log successivo se richiesto;
5. produce inventario e checksum;
6. rilascia snapshot o pin.

Il successo di una copia non equivale a backup verificato. Devono esistere restore test periodici su directory separata.

### 27.4 Repair

Repair non è eseguito automaticamente. Lavora su copia o con conferma esplicita, produce un rapporto machine-readable e distingue record recuperati, persi e dubbi.

## 28. Replica e alta disponibilità

La prima versione di produzione è single-node. Il formato logico prepara la replica mediante epoch, sequence, checkpoint e change log ordinato.

La fase distribuita usa un consenso leader-based, preferibilmente Raft per gruppo di shard:

- una scrittura è committed dopo quorum secondo policy;
- i follower applicano lo stesso log;
- lease e fencing dipendono dal term/epoch;
- letture follower dichiarano staleness;
- snapshot installabili riducono il log;
- membership change è controllata.

Non si implementa un protocollo di consenso “simile a Raft” incompleto. Prima della replica servono modello formale degli stati, fault injection e test di partizione. Multi-leader e transazioni cross-group restano fuori dal progetto 1.x.

## 29. Compatibilità e migrazioni

### 29.1 Versioni

Sono versionati separatamente:

- protocollo;
- catalogo;
- change log;
- adattatore e formato del backend;
- schema utente;
- proiezione;
- checkpoint.

Un reader supporta una finestra dichiarata di formati precedenti. Un writer non aggiorna automaticamente un formato irreversibile senza backup/checkpoint e piano di rollback.

### 29.2 AProDB 0.1

Il prototipo corrente contiene:

- typed key-value;
- shard HashMap con RwLock;
- WAL singolo;
- snapshot completo;
- compressione Zstandard per valore e canali;
- parallelismo Rayon;
- ricerca vettoriale CPU/GPU con wgpu;
- CLI e benchmark.

Non contiene il file format, il protocollo o le garanzie complete di questa specifica. È materiale sperimentale da cui riusare test e componenti, non un formato 1.0 da mantenere implicitamente.

La nuova implementazione deve:

1. congelare test di lettura 0.1;
2. scegliere import one-shot oppure dichiarare incompatibilità;
3. non aprire una directory 0.1 come 1.0 senza riconoscimento esplicito;
4. conservare una copia prima della conversione;
5. documentare cosa non può essere migrato.

## 30. Configurazione

La configurazione ha:

- file statico versionato;
- variabili ambiente per riferimenti a segreti e override selezionati;
- valori dinamici persistiti nel catalogo;
- effective config interrogabile;
- validazione prima dell'avvio.

Gruppi minimi:

- server e trasporti;
- data paths e storage classes;
- memory budget e pool;
- shard e partition;
- backend, change log e durabilità;
- capability, checkpoint e compaction;
- compressione e dizionari;
- cache e radial weights;
- proiezioni;
- CPU/GPU;
- autenticazione/TLS;
- quote;
- metriche e log;
- backup e retention.

Le unità sono esplicite. Durate e byte non usano numeri privi di suffisso nei file destinati agli operatori.

## 31. Test di correttezza

La gerarchia obbligatoria è:

1. unit test di codec, versioni, score e transizioni;
2. property test di encode/decode e ordinamento;
3. golden test di record envelope, change log, catalogo e protocollo;
4. fuzzing di parser, recovery e formati;
5. test concorrenti di CAS, batch, claim e lease;
6. model-based test contro un modello sequenziale;
7. fault injection fra preparazione, commit backend, publication e checkpoint;
8. kill e riavvio ripetuti;
9. disco pieno, permessi, short read/write e corruzione;
10. clock jump e lease scaduti;
11. GPU reset/out-of-memory e fallback;
12. compatibilità fra versioni;
13. restore di backup.

Invarianti da verificare:

- nessun ACK Durable senza record recuperabile;
- sequence mai riutilizzata;
- un fencing token obsoleto non completa un lease;
- budget non superato oltre una tolleranza misurata;
- proiezione mai dichiara un watermark non applicato;
- un consumer lento legge la versione o il delta esatto anche dopo compaction e riavvio;
- nessuna retention applicativa dipende dalla vita di uno snapshot del backend;
- cache persa non perde dati;
- CPU-only conserva tutte le funzioni;
- corruzione non viene trasformata silenziosamente in assenza.

## 32. Benchmark

### 32.1 Profili

Il benchmark ufficiale comprende:

- exact Get/Put su piccoli record;
- batch Get/Put;
- append, claim, heartbeat e complete multi-client;
- feed temporale con aggiornamento incrementale;
- surface read;
- cold miss e decompressione;
- scansione indicizzata;
- vettori exact CPU e GPU;
- workload misto con compaction;
- recovery e rebuild;
- payload comprimibili, già compressi e casuali;
- RAM limitata rispetto al dataset.

### 32.2 Metodo

Devono essere dichiarati:

- hardware, OS, filesystem e power mode;
- versione e configurazione di ogni database;
- dataset e seed;
- warmup;
- client, trasporto e pool;
- durabilità equivalente;
- concorrenza;
- intervalli di confidenza e run scartati;
- spazio logico, fisico e temporaneo;
- p50, p95, p99 e throughput sostenuto;
- CPU, RAM, I/O e GPU;
- tempo di recovery.

Confronti embedded e server stanno in tabelle separate. I risultati non sono proprietà permanenti del prodotto e non sostituiscono benchmark sul workload dell'utente.

### 32.3 Criteri

Non vengono fissate promesse numeriche prima di una macchina di riferimento. I gate architetturali sono:

- nessuna regressione di correttezza per ottenere throughput;
- prestazioni CPU funzionali senza GPU;
- superficie incrementale più economica del rebuild completo sul profilo target;
- memoria limitata senza OOM;
- throughput sostenuto senza debito di compaction crescente indefinitamente;
- accelerazione GPU positiva soltanto oltre una soglia misurata;
- risultati comparativi riproducibili.

## 33. Roadmap di implementazione

### Milestone 0 — Fondazioni

- inizializzare Git prima di modifiche sostanziali;
- preservare il prototipo;
- creare workspace Rust;
- definire crate e confini;
- CI locale, format, clippy, test;
- ADR e feature matrix;
- identificatori e error model.

### Milestone 0.5 — Scelta del backend di storage

- definire il contratto minimo dello storage: atomicità, durabilità, range scan, recovery, compaction, memoria e dataset maggiori della RAM;
- spike breve su fjall contro il contratto;
- verificare in particolare batch atomico fra record, catalogo e log eventi, mapping di Durable, snapshot, range temporali e riapertura dopo crash;
- misurare l'overhead del change log minimale senza duplicare il payload per default;
- confrontare Delta, VersionRef e SelfContained con consumer lento, compaction e riavvio; vietare snapshot longevi come retention;
- eseguire la matrice di compressione AProDB/backend definita in §19.6;
- applicare criteri di uscita limitati: correttezza, capacità essenziali, build Windows/Linux e assenza di un blocco architetturale;
- redb e RocksDB restano fallback documentati se fjall fallisce i fault test;
- decisione registrata come ADR.

### Milestone 1 — Storage canonico single-node

- directory lock;
- adattatore sul backend scelto in Milestone 0.5, dietro il contratto di storage;
- catalogo e change log logico AProDB nello stesso dominio transazionale dei record;
- Put, Get, Delete, CAS e AtomicBatch;
- durabilità Durable e Relaxed;
- recovery e checkpoint;
- limiti memoria;
- test di fault injection sul contratto.

### Milestone 2 — Server multiprocesso

- protocollo binario versionato;
- local transport e TCP;
- batching, deadline e backpressure;
- auth di base e quote;
- client Rust;
- metriche;
- CLI amministrativa.

### Milestone 3 — Motore radiale e capacità storage

- validazione e telemetria di segmenti, indici, flush e compaction offerti dal backend;
- nessuna reimplementazione dei formati fisici senza ADR e prove di necessità;
- cache separate;
- radial descriptor e policy;
- TTL;
- storage classes;
- ExplainPlacement;
- dataset superiore alla RAM.

### Milestone 4 — Workflow e superfici

- Append, Claim, Heartbeat, Complete e Fail;
- fencing e idempotenza;
- change stream;
- indici temporali/workflow;
- projection builder incrementale;
- surface generation e watermark;
- formati pre-serializzati.

### Milestone 5 — Compressione adattiva

- codec per payload logico e formato versionato;
- livelli per tier;
- adaptive Raw fallback;
- dizionari versionati;
- coordinamento per keyspace con la compressione fisica del backend;
- cache compressa/decompressa;
- budgeting dei canali;
- telemetria e benchmark.

### Milestone 6 — Compute eterogeneo

- trait CPU reference;
- scheduler a costo;
- layout colonnare;
- wgpu opzionale;
- VRAM cache;
- vector exact e top-k;
- fault isolation e CPU fallback;
- benchmark di pareggio.

### Milestone 7 — Operabilità

- backup/restore;
- verify e repair controllato;
- encryption at rest opzionale;
- TLS/mTLS;
- audit;
- upgrade e import 0.1;
- test lunghi e pacchetti.

### Milestone 8 — Distribuzione, separata

- specifica Raft;
- replicated logical log;
- follower read;
- snapshot install;
- failover;
- test di rete e partizione.

Una milestone è completa soltanto quando codice, test, manuale e diario concordano. Funzioni parziali restano sperimentali e disabilitate per default.

## 34. Decisioni architetturali stabilizzate

| Decisione | Esito |
|---|---|
| GPU obbligatoria | No; CPU reference sempre |
| Collocazione GPU | Milestone 6 invariata; interfacce e layout preparati dalle fondazioni |
| Modello di deployment | Server centrale, embedded esclusivo |
| SQL generale | No in 1.x |
| Fonte di verità | Stato transazionale del backend + catalogo e change log logico AProDB |
| Storage engine | Contratto di backend; motore incorporato (fjall candidato, redb/RocksDB fallback) prima di un motore nativo |
| Storage fisico | WAL, memtable, segmenti, manifest e compaction appartengono al backend incorporato |
| Cambio backend | Non trasparente; richiede capability check ed export/import o migrazione verificata |
| Durabilità | Modalità Durable unica con finestra di group commit configurabile |
| Change log | Envelope minimale; payload completo non duplicato per default |
| Retention eventi | Delta, VersionRef o SelfContained per collection; mai snapshot MVCC longevo |
| Compressione | Logica AProDB e fisica backend coordinate per keyspace tramite ADR e benchmark |
| Radial score | Minimo (freshness + workflow + pin); altri segnali solo se misurati |
| Superficie | Derivata, incrementale, generazionale |
| Spostamento radiale | Proiezioni prima di riscrittura canonica |
| Concorrenza scritture | Single logical writer per shard |
| Atomicità | Entro partizione |
| Cross-shard serializable | Fuori 1.x |
| Semantica worker | At-least-once + idempotenza + fencing |
| Cache | Budget separate, ammissione radiale TinyLFU |
| I/O predefinito | Buffered; direct sperimentale e misurato |
| Formato GPU | Colonnare e ricostruibile |
| Replica | Progettata, non parte del primo server |
| Business logic nel DB | No; operatori interni limitati e deterministici |
| Compatibilità Redis | Gateway futuro |

## 35. Rischi principali

### 35.1 Complessità

Integrare database, workflow, cache e GPU può creare un sistema troppo ampio. Mitigazione: milestone, feature flag, formati semplici, CPU reference e nessuna replica prima della maturità single-node.

### 35.2 Write amplification

Compaction e migrazione possono consumare storage. Mitigazione: segmenti temporali, proiezioni ricostruibili, token I/O e metrica esplicita.

### 35.3 Cache pollution

Feed e scansioni possono espellere lookup utili. Mitigazione: pool e classi di ammissione separate, TinyLFU e bypass.

### 35.4 Thrashing radiale

Dati al confine possono oscillare. Mitigazione: isteresi, permanenza minima, costo di migrazione e rate limit.

### 35.5 GPU negativa

Trasferimenti e driver possono peggiorare latenza o stabilità. Mitigazione: modello di costo, soglie calibrate, circuit breaker e fallback CPU.

### 35.6 Coerenza delle proiezioni

Una superficie veloce ma obsoleta può mostrare dati errati. Mitigazione: watermark, generazioni atomiche, read-your-writes token e rebuild verificabile.

### 35.7 Corruzione e upgrade

Un formato giovane è rischioso. Mitigazione: versioning, golden file, fuzz, checker, backup e upgrade non distruttivo.

### 35.8 Specializzazione eccessiva

Inserire logica di Commit renderebbe AProDB poco generale. Mitigazione: primitive di workflow generiche e proiezioni dichiarative; traduzioni e decisioni editoriali restano esterne.

## 36. Criterio di completamento del prodotto 1.0

AProDB 1.0 è dichiarabile quando:

- il server multiprocesso single-node è il percorso raccomandato;
- recovery supera fault injection e kill loop;
- l'ACK Durable sopravvive ai test di crash previsti;
- dataset maggiori della RAM sono supportati;
- memoria, code e disco hanno limiti;
- claim/lease/fencing sono verificati concorrentemente;
- superfici sono incrementali e riportano watermark;
- compressione adattiva e dizionari sono recuperabili;
- CPU-only supera l'intera suite funzionale;
- GPU è opzionale, isolata e misurata;
- backup viene ripristinato automaticamente in test;
- protocollo e file format sono versionati;
- manuale descrive solo realtà;
- benchmark server equi sono pubblicati con configurazione;
- non esistono difetti noti di perdita dati classificati come minori.

## 37. Fonti tecniche

Queste fonti sostengono principi e confronti; non trasferiscono automaticamente le loro garanzie ad AProDB.

1. Intel, **Intel 64 and IA-32 Architectures Optimization Reference Manual**: cache, memoria e ottimizzazione del layout.
   https://www.intel.com/content/www/us/en/developer/articles/technical/intel64-and-ia32-architectures-optimization.html
2. NVIDIA, **CUDA C++ Best Practices Guide**: costo dei trasferimenti, pinned memory e sovrapposizione asincrona.
   https://docs.nvidia.com/cuda/cuda-c-best-practices-guide/index.html
3. Apache Arrow, **Columnar Format**: rappresentazioni colonnari interoperabili.
   https://arrow.apache.org/docs/format/Columnar.html
4. Meta, **Zstandard Manual**: livelli, riuso dei contesti e dizionari.
   https://facebook.github.io/zstd/zstd_manual.html
5. Einziger et al., **TinyLFU: A Highly Efficient Cache Admission Policy**.
   https://arxiv.org/abs/1512.00727
6. Redis, **Key eviction**: LRU/LFU approssimati e limiti di memoria.
   https://redis.io/docs/latest/develop/reference/eviction/
7. RocksDB, **Block Cache**: cache compresse/non compresse e sharding.
   https://github.com/facebook/rocksdb/wiki/Block-Cache
8. RocksDB, **Write Ahead Log File Format** e **RocksDB Overview**: WAL, memtable, segmenti e compaction.
   https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format
   https://github.com/facebook/rocksdb/wiki/RocksDB-Overview
9. PostgreSQL, **Concurrency Control** e **Transaction Isolation**: perimetro delle garanzie concorrenti e significato di serializzabilità.
   https://www.postgresql.org/docs/current/mvcc.html
   https://www.postgresql.org/docs/current/transaction-iso.html
10. PostgreSQL, **REFRESH MATERIALIZED VIEW**: sostituzione del contenuto e comportamento di CONCURRENTLY, rilevante per il confronto con superfici incrementali.
    https://www.postgresql.org/docs/current/sql-refreshmaterializedview.html
11. NVM Express, **Base Specification**: submission/completion queue e ordinamento delle operazioni.
    https://nvmexpress.org/wp-content/uploads/NVMe-NVM-Express-2.0a-2021.07.26-Ratified.pdf
12. Ongaro e Ousterhout, **In Search of an Understandable Consensus Algorithm**: base per la futura replica leader-based.
    https://web.stanford.edu/~ouster/cgi-bin/papers/raft-extended.pdf
13. Fjall, **KeyspaceCreateOptions**, **CompressionType**, **Snapshot** e **SeqNo**: policy di compressione e limiti della retention tramite snapshot/versioni MVCC nel backend candidato.
    https://docs.rs/fjall/latest/fjall/struct.KeyspaceCreateOptions.html
    https://docs.rs/fjall/latest/fjall/enum.CompressionType.html
    https://docs.rs/fjall/latest/fjall/struct.Snapshot.html
    https://docs.rs/fjall/latest/fjall/type.SeqNo.html

## Appendice A — Macchina a stati del lavoro

Stati minimi suggeriti:

    pending -> leased -> completed
       |         |           |
       |         +-> pending  +-> published
       |              expiry
       +-> dead_letter
       +-> cancelled

Transizioni:

- pending a leased: Claim atomico;
- leased a pending: lease scaduto o Fail ritentabile;
- leased a completed: Complete con fencing valido;
- completed a published: Publish idempotente;
- pending/leased a dead_letter: limite tentativi o errore permanente;
- qualsiasi stato non finale a cancelled: operazione autorizzata e versionata.

Le collection possono aggiungere stati, ma devono dichiarare transizioni consentite. Il motore non interpreta il significato editoriale.

## Appendice B — Esempio di policy radiale

Una collection di notizie potrebbe dichiarare:

- emivita freshness: 60 minuti;
- superficie pubblica: ultime 24 ore, massimo 10.000 elementi;
- superficie di lavoro: stato pending, ordinata per urgenza e tempo;
- hot retention minima: 15 minuti;
- warm: 7 giorni;
- cold: 180 giorni;
- archive: oltre 180 giorni;
- traduzioni come record/proiezioni dipendenti;
- commenti con emivita diversa;
- pin per breaking news;
- rebuild cost elevato per risultati di modelli costosi.

Questi valori sono esempio, non default universali.

## Appendice C — Matrice dei guasti

| Evento | Comportamento richiesto |
|---|---|
| Crash prima del commit backend | Nessun ACK, nessuna mutazione |
| Crash dopo commit Durable ma prima della pubblicazione RAM | Recovery o rilettura espone la mutazione |
| Crash durante commit | Il backend non rende visibile un batch parziale |
| Record aggiornato senza change log | Violazione del contratto; stop controllato |
| Crash durante checkpoint | Resta valido il checkpoint precedente |
| File temporaneo del backend | Gestito dal recovery documentato del backend |
| Disco pieno | Throttle/read-only, nessuna cancellazione implicita |
| GPU persa | Fallback CPU, VRAM rebuild |
| Projection builder fermo | Record canonici disponibili, staleness visibile |
| Worker oltre lease | Fencing rifiuta Complete |
| Orologio arretra | Sequence preserva ordine; lease usa policy di sicurezza |
| Dizionario mancante | Errore di integrità, non bytes incomprensibili restituiti |
| Cache corrotta | Scarto e ricostruzione |
| Stato canonico del backend corrotto | Stop controllato o restore/repair esplicito |

## Appendice D — Glossario

- **canonico:** necessario per ricostruire lo stato confermato;
- **derivato:** eliminabile e ricostruibile;
- **superficie:** proiezione pronta per consumo a bassa latenza;
- **settore:** fase o finalità indipendente dal tier;
- **radial score:** segnale di placement, mai regola di correttezza;
- **watermark:** massima sequence sorgente applicata;
- **generation:** versione immutabile pubblicata di una proiezione;
- **lease:** possesso temporaneo di un lavoro;
- **fencing token:** numero che impedisce a un proprietario obsoleto di scrivere;
- **compaction debt:** lavoro necessario accumulato per mantenere il motore;
- **durability receipt:** prova applicativa del livello applicato a una mutazione;
- **storage class:** destinazione fisica con budget e caratteristiche;
- **CPU reference:** implementazione semanticamente autorevole disponibile ovunque.
