# Baseline del repository

Stato verificato il 19 agosto 2026 per la Milestone 0.

## Perimetro locale

- repository Git inizializzato localmente sul branch `main`;
- nessun commit, staging o remoto prima dell'autorizzazione di pubblicazione;
- Git 2.54.0 e Rust stable 1.97.1 disponibili tramite i percorsi toolchain locali;
- `gh` non installato;
- Chrome verificato in sola lettura come account `provenzali`, con pagina di creazione repository accessibile;
- il connettore GitHub associato a un account diverso non è autorizzato per AProDB.

L'utente ha approvato formalmente il 19 agosto 2026 la pubblicazione pubblica e
la struttura di licenza: core `AGPL-3.0-only`; client, protocollo e tipi pubblici
`Apache-2.0`. Il target candidato è `provenzali/aprodb`; la disponibilità del
nome è stata verificata in sola lettura. Andrea Provenzali è identificato con
ORCID `0009-0009-9677-9840`, senza pubblicare email o altri dati anagrafici.

## Audit dei file

La scansione iniziale e quella ripetuta dopo la Milestone 7 sui 103 file
candidati fuori da `.git`/`target` non hanno trovato pattern forti di token,
chiavi private o credenziali, indirizzi email o file di almeno 10 MiB. La
scansione è un gate preventivo e non sostituisce un secret scanner dedicato
nella CI.

L'audit `cargo metadata` successivo alla rilicenza ha esaminato 282 pacchetti
terzi nell'intero grafo e 103 nel boundary Apache. Nessun pacchetto è privo del
campo licenza e nessuno dichiara come unica scelta una licenza copyleft
incompatibile. L'inventario è in `THIRD_PARTY_LICENSES.md` e deve essere
rigenerato quando cambia `Cargo.lock`.

Le regole di `.gitignore` sono state verificate con casi rappresentativi e coprono:

- ogni directory Rust `target`, compreso `target/bench-lab` e il target del benchmark comparativo;
- directory dati AProDB, WAL, snapshot e database locali;
- `.env`, chiavi, certificati privati e log;
- il prompt di handoff locale `implementation-prompt.md`.

`Cargo.lock` non è ignorato e deve essere versionato.

## Decisione di distribuzione

Appartengono alla distribuzione:

- sorgenti, test, benchmark e relativi manifest/lockfile;
- `README.md`, `manual.md`, `paper.md` e `diary.md`;
- `benchmarks/comparative/README.md` e `RESULTS.md`, mantenuti come risultati locali storici con limiti espliciti;
- ADR, matrice dei requisiti, testi AGPL/Apache, attribuzione, citazione,
  governance e workflow CI CPU-only.

Non appartengono alla distribuzione:

- build, laboratorio riproducibile voluminoso e report grezzi sotto `target`;
- dati, WAL, snapshot, database, log, profili e configurazioni locali;
- credenziali o materiale crittografico;
- `implementation-prompt.md`, conservato localmente perché contiene path e stato di sessione macchina-specifici.

Note e risultati pubblicabili devono usare path relativi o esempi neutrali. Non devono contenere email dell'account, segreti o identificatori non necessari. I benchmark embedded e client/server restano separati.

## Gate iniziali

Prima di qualsiasi modifica al motore sono passati:

- `cargo fmt --all --check`;
- `cargo clippy --workspace --all-targets --no-default-features -- -D warnings`;
- `cargo test --workspace --no-default-features`;
- Clippy e test con feature predefinite.

Il test GPU esplicitamente ignorato dal prototipo non è stato forzato in questa baseline; la motivazione storica è documentata nel diario. Nessun server esterno o laboratorio comparativo è stato avviato.
