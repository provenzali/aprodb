# ADR-0003 — Compute eterogeneo con CPU reference e wgpu

- Stato: accettato per Milestone 6, calibrazione hardware sperimentale
- Data: 19 agosto 2026
- Ambito: vector exact/top-k single-node

## Contesto

Il percorso corretto deve funzionare senza GPU. L'acceleratore è derivato,
volatile e fallibile; una decisione basata sulla sola dimensione del batch non
include trasferimenti, coda, inizializzazione, readback o rischio. Il contenuto
in VRAM non può sopravvivere come autorità né essere riusato dopo una mutazione.

## Decisione

- `CpuPool` implementa la semantica di riferimento per dot product e cosine su
  batch colonnari contigui con validity bitmap. Input non finiti sono rifiutati,
  il vettore nullo ha cosine zero e i pareggi seguono l'indice riga.
- Lo scheduler usa una coda e un budget byte limitati, micro-batching con attesa
  massima, pool CPU separato, timeout, circuit breaker e fallback CPU. `Auto`
  confronta costo CPU e somma di transfer-in, queue wait, launch, compute GPU,
  transfer-out, sincronizzazione e margine di rischio.
- La feature `gpu` usa wgpu/WGSL. Il readback usa `map_async` e un timeout; un
  errore resetta il contesto derivato, apre il circuit breaker secondo soglia e
  non modifica storage o server.
- La cache VRAM è LRU e limitata. La chiave contiene projection id, generazione
  globale della sorgente e versione schema. VectorExact costruisce il batch
  sotto una barriera breve di tutti gli shard, cattura la generazione e rilascia
  i lock prima del calcolo; una mutazione successiva forza una nuova chiave.
- `VectorExact` è read-only. Non esiste ancora una pubblicazione derivata da GPU;
  quando verrà aggiunta dovrà ricontrollare versione e watermark prima del
  commit.

## Evidenze

Test CPU deterministici coprono layout, null, ranking, pareggi, limiti, budget
coda, micro-batch, timeout/fault e cooldown. Il test wgpu confronta top-20 con
la CPU entro tolleranza relativa `1e-4`, verifica hit, invalidazione e rebuild
della cache VRAM. Il test TCP attraversa Put vettoriale, VectorExact CPU,
metriche e separazione dei ruoli. Golden wire proteggono richiesta e risposta.

Il laboratorio in [`benchmarks/compute`](../../benchmarks/compute/RESULTS.md)
misura CPU, GPU fredda e GPU calda includendo trasferimenti e top-k. Sul sistema
locale il crossover non è monotono: il modello resta configurabile e i dati non
sono uno SLA.

## Conseguenze e limiti

- ExactFlat scansiona al massimo `max_scan_records`; non è un indice ANN e
  blocca brevemente le mutazioni mentre fotografa la generazione.
- wgpu non espone memoria host pinned portabile: questa implementazione non
  mantiene un pool pinned applicativo e usa gli staging interni limitati dal
  batch/queue budget. Il backend predefinito usa un worker GPU; configurazioni
  maggiori restano limitate ma l'accesso al device è serializzato.
- La stima iniziale è configurabile ma non si autotara ancora. L'operatore può
  forzare CPU o accelerator; anche la richiesta accelerator torna su CPU se il
  device fallisce, perché l'operazione è read-only e semanticamente sicura.
- Non sono implementati ANN, filtri/aggregazioni GPU, CUDA/HIP, persistenza VRAM
  o promesse di accelerazione. Storage, recovery e protocollo restano completi
  nel binario `--no-default-features`.
