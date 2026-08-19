# ADR-0005 — Licenze per componenti e attribuzione

## Stato

Accettata il 19 agosto 2026 da Andrea Provenzali.

## Contesto

AProDB deve poter essere scaricato e usato da chiunque, anche commercialmente,
senza consentire che le modifiche al database offerto come servizio diventino
silenziosamente proprietarie. Allo stesso tempo, client e protocollo devono
restare facilmente integrabili in applicazioni con licenze diverse. Il nome
dell'autore originario deve viaggiare con le redistribuzioni senza pubblicare
dati anagrafici non necessari.

## Decisione

- Core, server, storage, engine, compute, CLI e facade: `AGPL-3.0-only`.
- Client Rust, protocollo e tipi pubblici condivisi: `Apache-2.0`.
- I tipi compute usati dal client sono collocati in `aprodb-types`; il client
  permissivo non dipende dall'implementazione compute AGPL.
- Ogni sorgente contiene copyright e identificatore SPDX; i pacchetti includono
  il testo della propria licenza.
- `NOTICE`, `AUTHORS.md` e `CITATION.cff` identificano Andrea Provenzali come
  creatore originario e autore della specifica, con ORCID
  `0009-0009-9677-9840`.
- Non vengono pubblicati codice fiscale, data di nascita, nazionalità o email
  personali.
- I contributi seguono DCO e la licenza del componente destinatario; non viene
  introdotta una CLA in questa fase.

## Conseguenze

Il database conserva copyleft anche per versioni modificate esposte via rete,
mentre applicazioni esterne possono usare client e protocollo Apache. Non viene
offerta una scelta “AGPL OR Apache” per il core. Una futura doppia licenza
commerciale richiederebbe autorizzazioni compatibili da tutti i titolari delle
parti coinvolte e una nuova ADR.
