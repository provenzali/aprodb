# ADR-0004 — Operabilità, sicurezza e migrazione copy-only

- Stato: accettato per Milestone 7
- Data: 19 agosto 2026
- Ambito: nodo singolo AProDB 1.x

## Contesto

Backup, repair, rotazione chiavi e import 0.1 non possono modificare
irreversibilmente l'unica copia valida. TLS e cifratura at-rest devono usare
librerie mature; audit e quote devono fallire prima dell'operazione protetta,
senza registrare segreti o payload.

## Decisione

- Il backup online crea un checkpoint logico coerente, lo riapre, esegue
  `verify`, inventaria ogni file con BLAKE3 e pubblica il manifest solo dopo la
  verifica. Restore ricontrolla manifest, checksum, key id, catalog generation e
  watermark in una directory nuova.
- `repair_derived_to_copy` richiede la conferma letterale
  `REBUILD_DERIVED_ON_SEPARATE_COPY`, ricostruisce soltanto indici e superfici
  derivati e non modifica la sorgente. Corruzione canonica o del catalogo
  richiede restore.
- Tutti i valori dei keyspace Fjall possono essere protetti con
  XChaCha20-Poly1305. Nonce casuale, key id e AAD legano ciphertext, keyspace e
  chiave storage. Le chiavi arrivano da un file JSON limitato e protetto; marker
  e manifest contengono soltanto identificatori. La rotazione riscrive e
  verifica una copia separata.
- TCP remoto usa Rustls 0.23/Tokio-Rustls con server authentication e mTLS
  opzionale. Plaintext non-loopback resta rifiutato salvo override esplicito;
  named pipe e Unix socket restano trasporti locali.
- Ogni mutazione admin supportata registra in batch Durable un evento
  `Attempted` e un esito `Succeeded`/`Failed`. Il target è BLAKE3, non una chiave
  o un payload in chiaro. L'audit è leggibile soltanto dall'endpoint admin.
- Le quote tenant vengono ammesse prima del dispatch e limitano byte richiesta,
  frequenza, inflight e lavoro vettoriale. Il motore applica inoltre quota dati,
  riserva libera e stima temporanea di compaction; backup e restore verificano
  lo spazio prima della copia.
- AProDB 0.1 viene importato una sola volta e soltanto offline. I file originali
  sono copiati e verificati in `raw`; un'altra copia può subire il repair della
  coda WAL del reader 0.1. Il nuovo database nasce in una directory temporanea,
  viene verificato e poi rinominato. La sorgente non viene aperta dal motore 1.x.
- Non esistono upgrade in-place. Backup/restore, rekey e futuri cambi di formato
  seguono sempre una procedura copy-and-verify con rollback tramite la copia
  originale.

## Evidenze

I test coprono ciphertext/tamper/wrong key, backup/restore e inventario alterato,
repair su copia, audit dopo riavvio, ruoli, quote, TLS/mTLS, keyring redatto,
rekey e import 0.1 con hash sorgente invariato. Un gate lungo esegue 2.048
scritture Durable cifrate, quattro cicli backup/restore e rekey; i pacchetti del
workspace vengono estratti e ricompilati da `cargo package --workspace`.

## Conseguenze e limiti

- I nomi delle chiavi fisiche Fjall non sono occultati; vengono cifrati valori,
  record, catalogo, change log, audit, superfici e dizionari. Non è un formato
  volume-encryption né sostituisce BitLocker/LUKS.
- Il keyring file non è un KMS e su Windows la protezione ACL resta compito
  dell'operatore. Nessun segreto viene stampato o salvato nel manifest.
- Le quote richieste/secondo usano finestre fisse in memoria e si azzerano al
  riavvio. Non sono billing né isolamento distribuito.
- Restore, repair, rekey e import sono operazioni offline; un risultato parziale
  viene conservato per diagnosi e non cancellato automaticamente.
- Il formato logico supportato dal writer è v1. Un formato futuro sconosciuto
  viene rifiutato finché non esiste una migrazione copy-only verificata.
