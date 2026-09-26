#!/usr/bin/env bash
set -Eeuo pipefail

MODE="${1:-}"
if [[ -z "${MODE}" ]]; then
    echo "Usage: $0 {pilot|prepare|extract-s1|extract-s2|validate|database-prepare|database-load|database-finalize|database-validate|status}"
    exit 2
fi

PROJECT_DIR="${PROJECT_DIR:-/opt/project}"
PYTHON_BIN="${PYTHON_BIN:-/root/.pyenv/versions/yield_estimate/bin/python}"
RUN_ID="${RUN_ID:-adaptive-center-20260926-v1}"
RUN_ROOT="${RUN_ROOT:-/opt/project/zonal_runs}"
RUN_DIR="${RUN_ROOT}/${RUN_ID}"
GPKG_PATH="${GPKG_PATH:-/opt/project/final2.gpkg}"
S1_INPUT_DIR="${S1_INPUT_DIR:-/opt/project/kenya_s1_ops/final_target/indices}"
S2_INPUT_DIR="${S2_INPUT_DIR:-/data/indices}"
WORKERS="${WORKERS:-2}"
DUCKDB_MEMORY_LIMIT="${DUCKDB_MEMORY_LIMIT:-6GB}"
DB_NAME="${DB_NAME:-kenya_monitoring}"
PSQL_COMMAND="${PSQL_COMMAND:-sudo -u postgres psql}"

export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"

cd "${PROJECT_DIR}"

prepare_run() {
    if [[ -f "${RUN_DIR}/run_manifest.json" ]]; then
        echo "Run already prepared: ${RUN_DIR}"
        return
    fi
    "${PYTHON_BIN}" zonal_center_pipeline.py prepare \
        --run-dir "${RUN_DIR}" \
        --gpkg-path "${GPKG_PATH}" \
        --uid-field field_id \
        --machakos-layer machakos_fin \
        --nakuru-layer nakuru_fin \
        --s1-input-dir "${S1_INPUT_DIR}" \
        --s2-input-dir "${S2_INPUT_DIR}" \
        --min-pixels 5 \
        --tile-size 1024
}

extract_sensor() {
    local sensor="$1"
    "${PYTHON_BIN}" zonal_center_pipeline.py extract \
        --run-dir "${RUN_DIR}" \
        --sensor "${sensor}" \
        --workers "${WORKERS}" \
        --batch-rows 500000 \
        --duckdb-memory-limit "${DUCKDB_MEMORY_LIMIT}"
}

case "${MODE}" in
    pilot)
        PILOT_RUN_DIR="${RUN_ROOT}/${RUN_ID}-pilot775"
        if [[ ! -f "${PILOT_RUN_DIR}/run_manifest.json" ]]; then
            "${PYTHON_BIN}" zonal_center_pipeline.py prepare \
                --run-dir "${PILOT_RUN_DIR}" \
                --gpkg-path "${GPKG_PATH}" \
                --uid-field field_id \
                --machakos-layer machakos_fin \
                --nakuru-layer nakuru_fin \
                --s1-input-dir "${S1_INPUT_DIR}" \
                --s2-input-dir "${S2_INPUT_DIR}" \
                --min-pixels 5 \
                --tile-size 1024 \
                --field-id-file /opt/project/field_id_list.txt \
                --raster-start-date 2023-06-12 \
                --raster-end-date 2023-06-12
        fi
        "${PYTHON_BIN}" zonal_center_pipeline.py extract \
            --run-dir "${PILOT_RUN_DIR}" \
            --sensor both \
            --workers 1 \
            --duckdb-memory-limit 3GB \
            --start-date 2023-06-12 \
            --end-date 2023-06-12
        "${PYTHON_BIN}" zonal_center_pipeline.py validate \
            --run-dir "${PILOT_RUN_DIR}" \
            --sensor both
        ;;
    prepare)
        prepare_run
        ;;
    extract-s1)
        extract_sensor s1
        ;;
    extract-s2)
        extract_sensor s2
        ;;
    validate)
        "${PYTHON_BIN}" zonal_center_pipeline.py validate \
            --run-dir "${RUN_DIR}" \
            --sensor both \
            --full
        ;;
    database-prepare)
        "${PYTHON_BIN}" load_zonal_v2.py prepare \
            --run-dir "${RUN_DIR}" \
            --database "${DB_NAME}" \
            --psql-command "${PSQL_COMMAND}"
        ;;
    database-load)
        "${PYTHON_BIN}" load_zonal_v2.py load \
            --run-dir "${RUN_DIR}" \
            --database "${DB_NAME}" \
            --psql-command "${PSQL_COMMAND}" \
            --sensor both
        ;;
    database-finalize)
        "${PYTHON_BIN}" load_zonal_v2.py finalize \
            --run-dir "${RUN_DIR}" \
            --database "${DB_NAME}" \
            --psql-command "${PSQL_COMMAND}"
        ;;
    database-validate)
        "${PYTHON_BIN}" load_zonal_v2.py validate \
            --run-dir "${RUN_DIR}" \
            --database "${DB_NAME}" \
            --psql-command "${PSQL_COMMAND}"
        ;;
    status)
        echo "Run directory: ${RUN_DIR}"
        if [[ -f "${RUN_DIR}/run_manifest.json" ]]; then
            "${PYTHON_BIN}" -c 'import json,sys; p=json.load(open(sys.argv[1])); print(json.dumps({k:p.get(k) for k in ("field_counts","raster_counts","expected_rows","buffer_distribution","below_minimum_fields")},indent=2))' "${RUN_DIR}/run_manifest.json"
        fi
        find "${RUN_DIR}/parts" -type f -name '*.parquet' 2>/dev/null | awk -F/ '{counts[$(NF-2)]++} END {for (key in counts) print key, counts[key]}' | sort || true
        [[ -f "${RUN_DIR}/validation_report.json" ]] && cat "${RUN_DIR}/validation_report.json"
        [[ -f "${RUN_DIR}/database_validation_report.json" ]] && cat "${RUN_DIR}/database_validation_report.json"
        ;;
    *)
        echo "Unknown mode: ${MODE}"
        exit 2
        ;;
esac
