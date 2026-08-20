-- Copyright 2026 Andrea Provenzali and AProDB contributors
-- SPDX-License-Identifier: AGPL-3.0-only
--
-- Run with psql -qAt -v ON_ERROR_STOP=1. Optional variables:
--   -v schema_name=public -v table_name=news
-- The defaults select every non-system base table. The transaction is
-- read-only and repeatable-read so all exported rows share one snapshot.

\if :{?schema_name}
\else
\set schema_name '*'
\endif
\if :{?table_name}
\else
\set table_name '*'
\endif
\if :{?row_limit}
\else
\set row_limit 0
\endif

BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY;

COPY (
    SELECT jsonb_build_object(
        'kind', 'manifest',
        'database', current_database(),
        'tables', count(*)
    )::text
    FROM information_schema.tables AS tables
    JOIN pg_namespace AS namespaces
      ON namespaces.nspname = tables.table_schema
    JOIN pg_class AS classes
      ON classes.relnamespace = namespaces.oid
     AND classes.relname = tables.table_name
     AND classes.relkind IN ('r', 'p')
     AND NOT classes.relispartition
    WHERE tables.table_type = 'BASE TABLE'
      AND tables.table_schema NOT IN ('pg_catalog', 'information_schema')
      AND (:'schema_name' = '*' OR tables.table_schema = :'schema_name')
      AND (:'table_name' = '*' OR tables.table_name = :'table_name')
) TO STDOUT WITH (FORMAT csv, DELIMITER E'\x1f', QUOTE E'\x1e', ESCAPE E'\x1d');

SELECT format(
    $command$
COPY (SELECT %L) TO STDOUT WITH (FORMAT csv, DELIMITER E'\x1f', QUOTE E'\x1e', ESCAPE E'\x1d');
COPY (
    SELECT jsonb_build_object(
        'kind', 'row',
        'ctid', source.ctid::text,
        'tableoid', source.tableoid::regclass::text,
        'row', to_jsonb(source)
    )::text
    FROM %I.%I AS source
    LIMIT NULLIF(%s, 0)
) TO STDOUT WITH (FORMAT csv, DELIMITER E'\x1f', QUOTE E'\x1e', ESCAPE E'\x1d');
COPY (SELECT %L) TO STDOUT WITH (FORMAT csv, DELIMITER E'\x1f', QUOTE E'\x1e', ESCAPE E'\x1d');
$command$,
    jsonb_build_object(
        'kind', 'table',
        'schema', tables.table_schema,
        'table', tables.table_name,
        'primary_key', coalesce(
            (
                SELECT jsonb_agg(keys.column_name ORDER BY keys.ordinal_position)
                FROM information_schema.table_constraints AS constraints
                JOIN information_schema.key_column_usage AS keys
                  USING (
                      constraint_catalog,
                      constraint_schema,
                      constraint_name,
                      table_catalog,
                      table_schema,
                      table_name
                  )
                WHERE constraints.constraint_type = 'PRIMARY KEY'
                  AND constraints.table_schema = tables.table_schema
                  AND constraints.table_name = tables.table_name
            ),
            '[]'::jsonb
        ),
        'estimated_rows', coalesce(stats.n_live_tup, 0)
    )::text,
    tables.table_schema,
    tables.table_name,
    greatest(:'row_limit'::bigint, 0),
    '{"kind":"end"}'
)
FROM information_schema.tables AS tables
JOIN pg_namespace AS namespaces
  ON namespaces.nspname = tables.table_schema
JOIN pg_class AS classes
  ON classes.relnamespace = namespaces.oid
 AND classes.relname = tables.table_name
 AND classes.relkind IN ('r', 'p')
 AND NOT classes.relispartition
LEFT JOIN pg_stat_user_tables AS stats
  ON stats.schemaname = tables.table_schema
 AND stats.relname = tables.table_name
WHERE tables.table_type = 'BASE TABLE'
  AND tables.table_schema NOT IN ('pg_catalog', 'information_schema')
  AND (:'schema_name' = '*' OR tables.table_schema = :'schema_name')
  AND (:'table_name' = '*' OR tables.table_name = :'table_name')
ORDER BY tables.table_schema, tables.table_name
\gexec

COPY (
    SELECT jsonb_build_object(
        'kind', 'complete',
        'tables', count(*)
    )::text
    FROM information_schema.tables AS tables
    JOIN pg_namespace AS namespaces
      ON namespaces.nspname = tables.table_schema
    JOIN pg_class AS classes
      ON classes.relnamespace = namespaces.oid
     AND classes.relname = tables.table_name
     AND classes.relkind IN ('r', 'p')
     AND NOT classes.relispartition
    WHERE tables.table_type = 'BASE TABLE'
      AND tables.table_schema NOT IN ('pg_catalog', 'information_schema')
      AND (:'schema_name' = '*' OR tables.table_schema = :'schema_name')
      AND (:'table_name' = '*' OR tables.table_name = :'table_name')
) TO STDOUT WITH (FORMAT csv, DELIMITER E'\x1f', QUOTE E'\x1e', ESCAPE E'\x1d');

COMMIT;
