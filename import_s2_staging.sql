\timing on
\set ON_ERROR_STOP on

\echo 'Starting S2 CSV import...'

\copy staging.s2_indices_raw FROM '/opt/project/kenya_s2_ops/zonal_stats_s2.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '', ENCODING 'UTF8');

\echo 'S2 CSV import finished.'

SELECT COUNT(*) AS imported_rows
FROM staging.s2_indices_raw;
