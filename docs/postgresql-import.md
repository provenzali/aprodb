# PostgreSQL import (public beta release)

The PostgreSQL importer is a bounded, one-way validation and migration tool. It copies base table rows into a new AProDB data directory without opening or modifying the PostgreSQL data directory. It does not reproduce SQL indexes, constraints, triggers, views, sequences, permissions, or query semantics.

## Safety model

- The exporter runs in a read-only, repeatable-read transaction.
- The PostgreSQL credentials remain inside the source container. The local process receives only JSONL data over SSH.
- The SSH wrapper requires strict host-key checking and non-interactive authentication.
- AProDB writes to a sibling staging directory, verifies the logical database, reopens and verifies it again, and publishes it with a same-volume directory rename only after a complete stream.
- An interrupted import never appears at the requested destination. Its staging directory is retained for diagnosis and must be removed explicitly.
- Input frames are limited to 17 MiB. Buffered mutations are limited to 32 MiB in total, and the database enforces its record, metadata, batch, and storage limits as usual.
- The default destination limits are 64 GiB of database data, 16 GiB of temporary compaction space, and an 8 GiB free-space reserve. The wrapper exposes all three limits explicitly; increase them only after checking the destination filesystem.

The operation still imposes read and serialization load on the source server. For large databases, use a verified backup, a quiet replica, or an agreed maintenance window. Long repeatable-read transactions can delay PostgreSQL vacuum cleanup.

## Build and run

First, build the importer:

```powershell
cargo build -p aprodb-cli --bin aprodb-pg-import
```

Import one table through an SSH target whose PostgreSQL container exposes the standard `POSTGRES_USER` and `POSTGRES_PASSWORD` environment variables:

```powershell
pwsh -NoProfile -File scripts/import_postgres_over_ssh.ps1 `
  -DataDir C:\data\aprodb-import `
  -SshTarget ssh-target `
  -Container postgres-container `
  -SourceDatabase source-db `
  -Schema public `
  -Table source_table
```

Use `-Schema '*' -Table '*'` to include all base tables, but only after testing with a smaller sample first. The `-RowLimit N` option limits the number of rows per selected table and is intended for testing purposes. The default durability setting is `durable`; `relaxed` is available only if the caller accepts its weaker crash guarantees.

The importer writes progress updates to standard error and outputs a single machine-readable JSON summary to standard output. A successful summary reports table and row counts, logical bytes, committed batches, elapsed time, and the head and event counts found by both verification passes.

## Mapping

- The PostgreSQL `schema.table` maps to the corresponding AProDB collection.
- Rows are stored using PostgreSQL's `to_jsonb` representation without any intermediate floating-point conversion. As a result, exact numeric values retain their JSON number text, but PostgreSQL-specific type identities are not preserved.
- The AProDB key is a BLAKE3 digest of the source schema, table, and ordered primary key values. The first byte of the digest selects one of 16 import partitions.
- For tables without a primary key, the exporter uses the row's `tableoid` and `ctid` from the same repeatable-read snapshot. `tableoid` prevents collisions between child partitions. This mapping is suitable for a one-time copy, not for synchronization: `ctid` can change after a row is rewritten in PostgreSQL.
- Source schema, table, primary key definition, snapshot `tableoid`, and `ctid` are retained as record metadata.

The current beta importer is not a change data capture system and cannot resume an interrupted stream. Run it again into a new destination, or use the retained staging directory only for diagnosis. Incremental synchronization and a stable mapping for tables without primary keys require a separate design and are not claimed as available.