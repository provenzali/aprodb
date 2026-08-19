# Matrice di implementazione AProDB 1.x

Questa matrice collega i requisiti normativi di `paper.md` allo stato verificato, alla milestone responsabile e al test minimo di accettazione. “Prototipo” non equivale a funzione 1.x completata.

| Requisito | Paper | Stato corrente verificato | Milestone | Test di accettazione |
|---|---|---|---|---|
| Baseline Git e distribuzione auditata | §33 M0 | audit locale pronto; `main` inizializzato; pubblicazione pubblica autorizzata con core AGPL e boundary Apache | 0 | audit staging, secret/large-file/license scan, gate CPU-only e verifica remota |
| Workspace aciclico e facade compatibile | §9.4 | crate types/storage/compute/engine e facade `aprodb::v1`; grafo aciclico | 0 | `cargo metadata`, build workspace e controllo dipendenze |
| Identità completa e tipi stabili | §7 | identità tenant/namespace/collection/partition/key, versioni, record e receipt implementati | 0 | unit/property/golden test di identità, versione e record |
| Error model e limiti condivisi | §7, §10.4, §26 | errori tipizzati e limiti condivisi applicati a config, payload, batch e inflight | 0 | test di configurazione invalida e allocazioni rifiutate |
| Boundary compute CPU/GPU e layout colonnare | §15 | trait CPU/accelerator, batch f32 contiguo e validity bitmap; wgpu isolato dalla build CPU | 0/6 | build CPU-only e GPU; layout/allineamento e null verificati |
| Contratto storage con capability esplicite | §16.0 | contratto implementato con capability, stats, limiti e fault injector | 0.5 | suite contrattuale comune e capability matrix |
| Spike Fjall limitato | §16.0, §33 M0.5 | Fjall 3.1.8 provato su batch atomico, sync, scan, reopen, compaction e checkpoint logico | 0.5 | atomic batch, sync, scan, reopen, checkpoint e >RAM |
| Costo change log e compressione per keyspace | §17.2, §19.6 | otto workload misurati e documentati in `benchmarks/storage-spike` | 0.5 | benchmark 4 modalità con byte, latenza, throughput, spazio e compaction |
| Retention Delta/VersionRef/SelfContained | §17.2 | implementata con versioni immutabili, watermark consumer e GC | 0.5/1 | consumer lento, update multipli, restart, GC e versione esatta |
| ADR di scelta backend | §16.0, §33 M0.5 | ADR-0001 accetta Fjall con pin, fail-closed e riesame M7 | 0.5 | criteri di uscita documentati e riproducibili |
| Lock esclusivo directory | §9.1–9.2 | implementato e provato nello stesso processo e in subprocess | 1 | seconda apertura/processo rifiutata, rilascio dopo drop/crash |
| Riconoscimento formato 0.1 | §29.2 | golden 0.1 presenti; il motore 1.x rifiuta sempre directory legacy; import one-shot opera offline su copie verificate | 1/7 | golden 0.1, rifiuto apertura, hash sorgente invariato e mapping tipi importato |
| Catalogo versionato | §18.1 | catalogo logico v1 atomico, con generazione, policy e watermark | 1 | mutate/reopen/recovery e transizioni atomiche |
| Record e change event nello stesso batch | §11, §17.2 | versione/head/evento/catalogo in un `OwnedWriteBatch` cross-keyspace | 1 | fault injection prima/dopo commit e invariant checker |
| Single logical writer per shard | §10.1 | mutex writer per shard e serializzazione separata del catalogo | 1 | modello sequenziale, CAS concorrente, sequence mai riusata |
| Put/Get/Delete/CAS/AtomicBatch | §8.1, §10 | implementati; AtomicBatch rifiuta partizioni diverse e identità duplicate | 1 | linearità per chiave e batch indivisibile entro partizione |
| Durable/Relaxed e receipt | §11.1 | implementati con SyncAll/Buffer, durable watermark e receipt | 1 | nessun ACK Durable non recuperabile; watermark verificato |
| Group commit limitato | §11.2 | canale limitato, finestra e byte cap; zero esegue SyncAll per richiesta | 1 | finestra zero per-request; finestra/byte cap e forced sync |
| Checkpoint e recovery deterministico | §27 | reopen, checkpoint logico, kill dopo ACK Durable e fault di apertura verificati; corruzione fisica stretta resta aperta | 1 | reopen, tail fault, publish fault, checkpoint fault |
| Budget chiavi/record/batch/memoria/code/disco | §10.4, §13, §26 | limiti applicati; M7 aggiunge quota dati, riserva libera e budget temporaneo compaction | 1/7 | test di soglia e backpressure prima della mutazione, compaction e copie |
| Golden/property/fuzz dei formati logici | §31 | golden/proptest passano anche per `APRX`, catalogo/dizionario compressione; fuzz esercita formati storage e messaggi protocollo, con smoke Windows e LibFuzzer non-Windows | 1+ | golden stabili, proptest e fuzz target compilabili |
| Protocollo Protobuf framed e versionato | §23 | schema canonico e tipi Prost, handshake magic/major/ruolo, frame `u32` limitati e golden wire | 2 | golden wire, frame oversize, version negotiation |
| Server centrale, local transport e TCP | §9, §23.2 | daemon unico con TCP e named pipe Windows/Unix socket; smoke reale server+CLI | 2 | integrazione multi-client, trasporto locale e lock unico della directory |
| Client Rust, deadline e request id | §23.1 | client async multiplexato e bloccante; deadline totale, correlazione fuori ordine e receipt tipizzate | 2 | deadline, più inflight e correlazione receipt |
| Auth data/admin, quote e shutdown | §23–26 | token separati, confronto constant-time, ruoli endpoint, limiti globali e quote tenant byte/frequenza/inflight/compute, retry-after, metriche e drain | 2/7 | permessi, auth fallita, quota tenant, backpressure e shutdown deterministici |
| Segmenti/flush/compaction backend osservabili | §16, §18, §20 | stats Fjall espongono disco, write buffer, journal, tabelle, flush e compaction senza interpretare file privati | 3 | dataset oltre budget, compaction/reopen e metriche capability-aware |
| Cache con budget separati | §13 | metadata/object/compressed/negative cache sharded e limitate; scansioni e manutenzione non popolano la object cache | 3/5 | working set oltre cache, invalidazione, eviction e riserva totale rispettati |
| Radial descriptor e placement spiegabile | §5–6 | descriptor/policy persistenti, isteresi, permanenza minima, pin e spiegazione admin senza effetti sulla object cache | 3 | score, isteresi, pin/TTL, restart e `ExplainPlacement` |
| TTL e indice temporale | §7, §18.2 | indice TTL atomico con record/versione; letture nascondono scaduti e sweep admin cancella con fencing | 3 | update TTL, scadenza, entry stale, reopen e verify |
| Storage class e dataset oltre memoria | §16.2, §33 M3 | classi logiche persistenti; capacità tiering fisico Fjall dichiarata assente; 129 MiB verificati con budget 128 MiB | 3 | workload dedicato, sync, compaction, reopen e lettura esatta |
| Append e workflow lease/fencing | §8.3, App. A | Append/Claim/Heartbeat/Complete/Fail/Publish persistenti; lease casuale, fencing monotono, recovery UTC+safety e limiti attivi | 4 | claim concorrente, replay, heartbeat, stale complete, dead-letter e restart |
| Idempotenza delle mutazioni | §8.1, §10.3 | hash a 32 byte, fingerprint, receipt/lease esatti, expiry index e purge limitato; stesso batch della mutazione | 4 | retry/restart restituisce stesso esito, riuso diverso fallisce, expiry consente nuovo esito |
| SubscribeChanges e watermark | §17.2, §22 | pull paginato per shard/collection, cursore globale, batch indivisibile e `ChangeLogGap` esplicito | 4 | batch indivisibile, slow consumer, compaction/restart e gap rilevato |
| Proiezioni/superfici incrementali | §22 | work/read generazionali su una collection, filtro stato, record/JSON, caps, watermark/staleness e rebuild; filtri/trasformazioni generici restano aperti | 4 | generazioni immutabili, publish atomico, restart, staleness e rebuild dopo gap |
| Compressione logica Raw/Zstd per tier | §19 | frame `APRX` con scelta adattiva per hot/warm/cold/archive, Surface Raw, skip content-type e policy Durable per collection; `APRC` legacy leggibile | 5 | policy per tier, Zstd/Raw, checksum, ratio, exact-version e recovery |
| Pool limitato e dizionari versionati | §19.3–19.4 | pool 1–64 e scratch limitato; training bounded con validation gate, pubblicazione atomica, checksum e retention conservativa | 5 | backpressure, dizionario mancante/corrotto, server TCP e reopen |
| Cache compressa/decompressa | §13.3, §19.6 | cache frame compresso e record decodificato con budget/metriche/invalidazione distinti | 5 | budget separati, hit/miss e invalidazione per versione |
| Compressione fisica coordinata e costo | §19.5–19.6 | canonico senza LZ4 fisico di default; metadata/eventi/superfici LZ4; matrice reale 4 modalità in `benchmarks/compression` | 5 | ratio, p50/p95/p99, throughput, CPU, RAM, I/O, spazio, compaction e recovery |
| Compute CPU reference batch | §15, §21.4 | vector exact/top-k 1.x via engine, protocollo e client; dot/cosine, limiti e tie deterministici | 6 | CPU-only, collection mista, reopen e server TCP |
| Scheduler costo totale | §15.3 | formula esplicita con transfer, queue, launch, compute, sync e rischio; queue/byte budget, micro-batch e override | 6 | scelta costo, budget, batching, timeout, fallback e metriche |
| wgpu opzionale e VRAM ricostruibile | §15 | WGSL opzionale, cache LRU per projection/generazione/schema, circuit breaker e reset; tolleranza relativa 1e-4 | 6 | equivalenza CPU/GPU, hit/invalidate/rebuild, fault/OOM simulato e benchmark crossover |
| Backup/restore verificato | §27.2–27.3 | checkpoint online riaperto/verificato, inventario BLAKE3 e manifest; restore rifiuta destinazioni esistenti e ricontrolla catalog/watermark | 7 | tamper rilevato, restore separato, backup online server e gate lungo ripetuto |
| Verify e repair esplicito | §27.4 | verify pagina tutti i keyspace logici; repair ricostruisce indici/superfici solo su copia con conferma letterale e report JSON | 7 | corruzione derivata rilevata, sorgente invariata e copia verificata |
| TLS e mTLS | §23.2, §24 | Rustls/Tokio-Rustls su TCP, CA server e certificato client opzionale; plaintext non-loopback fail-closed | 7 | mTLS valido, peer anonimo rifiutato, socket locali invariati |
| Cifratura at-rest e rotazione | §19.5, §24.4 | XChaCha20-Poly1305 su valori di ogni keyspace, AAD contestuale, keyring esterno/redatto e rekey copy-only | 7 | plaintext assente, wrong key/tamper, reopen/checkpoint/rekey verificati |
| Audit amministrativo | §24.2 | eventi Durable Attempted/esito con sequence, principal e target hash; endpoint admin paginato, incluso in backup/verify | 7 | mutazione TCP, role denial, restart e golden/fuzz logico |
| Quote tenant e disco | §24.2, §26 | admission per byte/rate/inflight/vector work; quota dati, riserva libera e stima temp compaction/copia | 7 | rifiuto pre-dispatch/pre-mutation e retry-after deterministico |
| Upgrade e import 0.1 | §29 | nessun in-place; import offline one-shot conserva raw, usa reader-copy, batch Durable, verify e rename; formati futuri sconosciuti rifiutati | 7 | snapshot+WAL con delete, tutti i tipi, hash sorgente invariato e destinazione verificata |
| Test lunghi e pacchetti | §31, §33 M7 | gate esplicito 2.048 write cifrate + 4 restore + rekey; nove crate confezionati e ricompilati localmente | 7 | test ignored eseguito esplicitamente e `cargo package --workspace` |
| Replica | §28, M8 | fuori perimetro iniziale | 8 separata | non dichiarata disponibile nelle milestone 0–7 |

La matrice viene aggiornata quando un gate passa realmente; una riga non diventa “completa” sulla sola presenza di API o scaffold.
