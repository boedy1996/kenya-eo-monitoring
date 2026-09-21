#!/usr/bin/env bash
set -Eeuo pipefail

DB_NAME="kenya_monitoring"
LOG_DIR="/opt/project/logs_s1_load"

mkdir -p "${LOG_DIR}"

for YEAR in $(seq 2016 2026); do
    NEXT_YEAR=$((YEAR + 1))
    LOG_FILE="${LOG_DIR}/load_s1_${YEAR}.log"

    echo "============================================================"
    echo "Loading S1 calendar year ${YEAR}"
    echo "Log: ${LOG_FILE}"
    echo "============================================================"

    sudo -u postgres psql \
        -v ON_ERROR_STOP=1 \
        -v year="${YEAR}" \
        -v next_year="${NEXT_YEAR}" \
        -d "${DB_NAME}" <<'SQL' 2>&1 | tee "${LOG_FILE}"

\timing on
\set ON_ERROR_STOP on

-- Pastikan partition tahun kalender tersedia.
SELECT format(
    'CREATE TABLE IF NOT EXISTS timeseries.s1_indices_%s
     PARTITION OF timeseries.s1_indices
     FOR VALUES FROM (%L) TO (%L)',
    :'year',
    make_date(:'year'::integer, 1, 1),
    make_date(:'next_year'::integer, 1, 1)
)
\gexec

BEGIN;

SET LOCAL synchronous_commit = off;

INSERT INTO timeseries.s1_indices (
    field_id,
    region,
    layer_name,
    raster_file,
    start_date,
    end_date,
    iso_year,
    week_no,

    sigma_vv_linear_mean,
    sigma_vv_linear_median,
    sigma_vv_linear_min,
    sigma_vv_linear_max,
    sigma_vv_linear_std,
    sigma_vv_linear_count,

    sigma_vh_linear_mean,
    sigma_vh_linear_median,
    sigma_vh_linear_min,
    sigma_vh_linear_max,
    sigma_vh_linear_std,
    sigma_vh_linear_count,

    p_ratio_mean,
    p_ratio_median,
    p_ratio_min,
    p_ratio_max,
    p_ratio_std,
    p_ratio_count,

    rvi_mean,
    rvi_median,
    rvi_min,
    rvi_max,
    rvi_std,
    rvi_count,

    rcspr_mean,
    rcspr_median,
    rcspr_min,
    rcspr_max,
    rcspr_std,
    rcspr_count
)
SELECT
    btrim(s.uid) AS field_id,
    lower(btrim(s.region)) AS region,
    s.layer_name,
    s.raster_file,
    s.start_date,
    s.end_date,
    s.iso_year,
    s.week_no,

    s.sigma_vv_linear_mean,
    s.sigma_vv_linear_median,
    s.sigma_vv_linear_min,
    s.sigma_vv_linear_max,
    s.sigma_vv_linear_std,
    s.sigma_vv_linear_count,

    s.sigma_vh_linear_mean,
    s.sigma_vh_linear_median,
    s.sigma_vh_linear_min,
    s.sigma_vh_linear_max,
    s.sigma_vh_linear_std,
    s.sigma_vh_linear_count,

    s.p_ratio_mean,
    s.p_ratio_median,
    s.p_ratio_min,
    s.p_ratio_max,
    s.p_ratio_std,
    s.p_ratio_count,

    s.rvi_mean,
    s.rvi_median,
    s.rvi_min,
    s.rvi_max,
    s.rvi_std,
    s.rvi_count,

    s.rcspr_mean,
    s.rcspr_median,
    s.rcspr_min,
    s.rcspr_max,
    s.rcspr_std,
    s.rcspr_count

FROM staging.s1_indices_raw AS s

WHERE s.start_date >= make_date(:'year'::integer, 1, 1)
  AND s.start_date <  make_date(:'next_year'::integer, 1, 1)
  AND s.uid IS NOT NULL
  AND btrim(s.uid) <> ''
  AND s.start_date IS NOT NULL

ON CONFLICT (field_id, start_date) DO NOTHING;

COMMIT;

-- ANALYZE partition yang baru diisi.
SELECT format(
    'ANALYZE timeseries.s1_indices_%s',
    :'year'
)
\gexec

-- Ringkasan tahun yang selesai.
SELECT format(
    'SELECT
         %s AS calendar_year,
         COUNT(*) AS rows,
         COUNT(DISTINCT field_id) AS fields,
         MIN(start_date) AS first_date,
         MAX(start_date) AS last_date
     FROM timeseries.s1_indices_%s',
    :'year',
    :'year'
)
\gexec

SQL

    echo "Completed S1 year ${YEAR}"
done

echo "============================================================"
echo "All S1 years completed"
echo "============================================================"
