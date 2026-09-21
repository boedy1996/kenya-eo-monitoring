#!/usr/bin/env bash
set -Eeuo pipefail

DB_NAME="kenya_monitoring"
LOG_DIR="/opt/project/logs_s2_load"

mkdir -p "${LOG_DIR}"

MIN_YEAR=$(
    sudo -u postgres psql -d "${DB_NAME}" -Atc "
        SELECT EXTRACT(YEAR FROM MIN(start_date))::integer
        FROM staging.s2_indices_raw
        WHERE start_date IS NOT NULL;
    "
)

MAX_YEAR=$(
    sudo -u postgres psql -d "${DB_NAME}" -Atc "
        SELECT EXTRACT(YEAR FROM MAX(start_date))::integer
        FROM staging.s2_indices_raw
        WHERE start_date IS NOT NULL;
    "
)

if [[ -z "${MIN_YEAR}" || -z "${MAX_YEAR}" ]]; then
    echo "Tidak ada start_date valid di staging.s2_indices_raw"
    exit 1
fi

echo "S2 calendar years: ${MIN_YEAR}-${MAX_YEAR}"

for YEAR in $(seq "${MIN_YEAR}" "${MAX_YEAR}"); do
    NEXT_YEAR=$((YEAR + 1))
    LOG_FILE="${LOG_DIR}/load_s2_${YEAR}.log"

    echo "============================================================"
    echo "Loading S2 calendar year ${YEAR}"
    echo "Log: ${LOG_FILE}"
    echo "============================================================"

    sudo -u postgres psql \
        -v ON_ERROR_STOP=1 \
        -v year="${YEAR}" \
        -v next_year="${NEXT_YEAR}" \
        -d "${DB_NAME}" <<'SQL' 2>&1 | tee "${LOG_FILE}"

\timing on
\set ON_ERROR_STOP on

SELECT format(
    'CREATE TABLE IF NOT EXISTS timeseries.s2_indices_%s
     PARTITION OF timeseries.s2_indices
     FOR VALUES FROM (%L) TO (%L)',
    :'year',
    make_date(:'year'::integer, 1, 1),
    make_date(:'next_year'::integer, 1, 1)
)
\gexec

BEGIN;

SET LOCAL synchronous_commit = off;

INSERT INTO timeseries.s2_indices (
    field_id,
    region,
    layer_name,
    raster_file,
    start_date,
    end_date,
    iso_year,
    week_no,

    ndvi_mean,
    ndvi_median,
    ndvi_min,
    ndvi_max,
    ndvi_std,
    ndvi_count,

    evi_mean,
    evi_median,
    evi_min,
    evi_max,
    evi_std,
    evi_count,

    ndmi_mean,
    ndmi_median,
    ndmi_min,
    ndmi_max,
    ndmi_std,
    ndmi_count,

    ndre_mean,
    ndre_median,
    ndre_min,
    ndre_max,
    ndre_std,
    ndre_count,

    msi_mean,
    msi_median,
    msi_min,
    msi_max,
    msi_std,
    msi_count,

    pvr_mean,
    pvr_median,
    pvr_min,
    pvr_max,
    pvr_std,
    pvr_count,

    lai_mean,
    lai_median,
    lai_min,
    lai_max,
    lai_std,
    lai_count
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

    s.ndvi_mean,
    s.ndvi_median,
    s.ndvi_min,
    s.ndvi_max,
    s.ndvi_std,
    s.ndvi_count,

    s.evi_mean,
    s.evi_median,
    s.evi_min,
    s.evi_max,
    s.evi_std,
    s.evi_count,

    s.ndmi_mean,
    s.ndmi_median,
    s.ndmi_min,
    s.ndmi_max,
    s.ndmi_std,
    s.ndmi_count,

    s.ndre_mean,
    s.ndre_median,
    s.ndre_min,
    s.ndre_max,
    s.ndre_std,
    s.ndre_count,

    s.msi_mean,
    s.msi_median,
    s.msi_min,
    s.msi_max,
    s.msi_std,
    s.msi_count,

    s.pvr_mean,
    s.pvr_median,
    s.pvr_min,
    s.pvr_max,
    s.pvr_std,
    s.pvr_count,

    s.lai_mean,
    s.lai_median,
    s.lai_min,
    s.lai_max,
    s.lai_std,
    s.lai_count

FROM staging.s2_indices_raw AS s

WHERE s.start_date >= make_date(:'year'::integer, 1, 1)
  AND s.start_date <  make_date(:'next_year'::integer, 1, 1)
  AND s.uid IS NOT NULL
  AND btrim(s.uid) <> ''
  AND s.start_date IS NOT NULL

ON CONFLICT (field_id, start_date) DO NOTHING;

COMMIT;

SELECT format(
    'ANALYZE timeseries.s2_indices_%s',
    :'year'
)
\gexec

SELECT format(
    'SELECT
         %s AS calendar_year,
         COUNT(*) AS rows,
         COUNT(DISTINCT field_id) AS fields,
         MIN(start_date) AS first_date,
         MAX(start_date) AS last_date
     FROM timeseries.s2_indices_%s',
    :'year',
    :'year'
)
\gexec

SQL

    echo "Completed S2 year ${YEAR}"
done

sudo -u postgres psql \
    -v ON_ERROR_STOP=1 \
    -d "${DB_NAME}" \
    -c "ANALYZE timeseries.s2_indices;"

echo "============================================================"
echo "All S2 years completed"
echo "============================================================"
