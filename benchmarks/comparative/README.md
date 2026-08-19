# Benchmark comparativo AProDB

Questo crate misura la stessa API key-value su AProDB, SQLite, PostgreSQL, MySQL e MariaDB. È separato dal crate principale per evitare che i driver SQL diventino dipendenze del motore.

## Carico

- 50.000 chiavi deterministiche e payload binari da 512 byte;
- profilo `compressible`, simile a campi ripetitivi di log/documenti;
- profilo `random`, pseudo-casuale deterministico ad alta entropia;
- ingest in batch da 500 con un commit durevole per batch;
- 50.000 lookup puntuali a dataset caldo;
- 20 scansioni ordinate del gruppo `042`, fino a 1.000 righe;
- tre ripetizioni; il confronto pubblicato usa la mediana.

## Esecuzione

Creare prima un database vuoto chiamato `aprodb_bench` sui server desiderati. Le porte predefinite del laboratorio sono PostgreSQL `55432`, MySQL `53306` e MariaDB `53307`.

```powershell
cargo run --release --manifest-path benchmarks/comparative/Cargo.toml -- `
  --backends aprodb,sqlite,postgres,mysql,mariadb `
  --profiles compressible,random `
  --records 50000 --reads 50000 --payload-bytes 512 `
  --batch-size 500 --runs 3 --scan-repeats 20 --scan-limit 1000 `
  --workdir target/bench-lab/results
```

Il runner scrive `report.json` dopo ogni singola prova. Se un backend fallisce, conserva le prove già valide, registra l'errore e termina con codice diverso da zero.

Per eseguire soltanto i backend embedded, che non richiedono server:

```powershell
cargo run --release --manifest-path benchmarks/comparative/Cargo.toml -- `
  --backends aprodb,sqlite --profiles compressible,random
```

Gli URL dei server sono modificabili con `--postgres-url`, `--mysql-url` e `--mariadb-url`. Consultare `--help` per tutti i parametri.

## Interpretazione corretta

AProDB e SQLite sono nello stesso processo del runner. PostgreSQL, MySQL e MariaDB usano una singola connessione TCP su loopback. Il test misura quindi le API così come sono oggi, compresi protocollo e parsing SQL, e non pretende di isolare il solo indice interno.

Lo spazio riportato è la directory AProDB, il file SQLite dopo checkpoint, `pg_total_relation_size` o il tablespace InnoDB allocato. I file WAL/redo globali dei server SQL non sono inclusi. I risultati locali pubblicati sono in [RESULTS.md](RESULTS.md).
