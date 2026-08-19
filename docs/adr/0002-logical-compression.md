# ADR-0002 — Compressione logica adattiva dei payload canonici

- Stato: accettato per Milestone 5, con tuning sperimentale
- Data: 19 agosto 2026
- Ambito: payload canonici single-node; riesame operativo in Milestone 7

## Contesto

AProDB deve applicare policy Raw/Zstandard per tier, mantenere dizionari
versionati e recuperabili e distinguere il formato logico dalla compressione
fisica del backend. Un record deve restare decodificabile dopo riavvio e una
versione non può dipendere dal payload corrente. Superfici pre-serializzate,
metadata/change log, blob e indici richiedono policy separate.

## Decisione

- I nuovi record canonici usano il frame `APRX` v1. Il payload serializzato è
  Raw oppure Zstandard e porta versione codec, lunghezza, checksum e optional
  dictionary id. I frame legacy sperimentali `APRC` restano leggibili.
- La scelta è adattiva per hot/warm/cold/archive e Raw per le superfici già
  serializzate. Soglia input, risparmio minimo, livello e prefissi content-type
  sono configurabili per collection.
- I dizionari vengono addestrati su campioni limitati, accettati solo se un set
  di validazione separato migliora il totale, salvati atomicamente con il
  catalogo e mai rimossi mentre una versione può riferirli.
- Il pool codec e lo scratch hanno limiti espliciti; l'esaurimento produce
  backpressure prima della pubblicazione.
- Le cache decompresse e compresse hanno budget e metriche distinti.
- Il keyspace canonico Fjall usa `None` come compressione fisica predefinita;
  metadata, change log e superfici mantengono LZ4 fisico. La doppia compressione
  è disponibile solo come configurazione misurata, non come default.

## Evidenze

Golden file, property test e fuzz target coprono record, catalogo e dizionari.
I test verificano scelta Zstandard/Raw, content-type skip, riapertura, versione
esatta, dizionario mancante, cache separate e backpressure scratch. Il percorso
admin è coperto dal test client/server TCP.

La matrice locale riproducibile è in
[`benchmarks/compression`](../../benchmarks/compression/RESULTS.md). Sul payload
comprimibile il codec logico ha ridotto 1,049,600 byte a 6,655; sul payload
pseudocasuale ha conservato Raw in 256 casi su 256. Il run è piccolo, debug e
dominato dalla preallocazione Fjall: non è una prova di superiorità.

## Conseguenze e rischi

- Cambiare il default fisico non modifica il formato logico Fjall né richiede
  interpretare WAL/SST; directory sperimentali già create conservano le proprie
  opzioni fisiche. Una migrazione/tuning esplicita resta materia Milestone 7.
- Le metriche codec contano tentativi di codifica, inclusi quelli appartenenti a
  richieste che possono fallire in seguito; non sono un contatore di byte
  definitivamente committati.
- I dizionari non hanno ancora garbage collection. È intenzionale finché non
  esiste una prova di reachability completa sulle versioni trattenute.
- Blob esterni non vengono compressi dal frame canonico; la loro policy resta
  separata e non implementata finché il blob store non esiste.
