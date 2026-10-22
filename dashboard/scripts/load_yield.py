#!/usr/bin/env python3
"""Load validated all-field seasonal yield outputs into the dashboard schema.

Each season is copied and checkpointed in one transaction. Re-running the
loader skips seasons whose source file and database row count still match.
The new yield run becomes active only after every expected row and summary
group has been validated.
"""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import psycopg
import pyarrow.parquet as pq
from psycopg.types.json import Jsonb


YIELD_COLUMNS = [
    "yield_run_id", "model_run_id", "field_id", "county", "season_code",
    "season_start", "season_end", "area_ha", "area_method", "crop_label",
    "crop_confidence", "crop_status", "intercrop_status", "survey_crop_text",
    "survey_crop_match", "sos", "peak_date", "eos", "phenology_method",
    "phenology_confidence", "phenology_qa_status", "optical_valid_weeks",
    "sar_supported_weeks", "seasonal_npp_gC_m2", "npp_valid_days",
    "npp_total_days", "npp_temporal_coverage", "npp_mean_spatial_coverage",
    "npp_mean_effective_pixels", "npp_periods_used", "tbp_tDM_ha",
    "yield_low_t_ha", "yield_central_t_ha", "yield_high_t_ha",
    "production_low_t", "production_central_t", "production_high_t",
    "phenology_narrow_central_yield_t_ha",
    "phenology_wide_central_yield_t_ha", "parameter_set_id", "method_version",
    "yield_status", "qa_flags",
]

# PostgreSQL folds unquoted identifiers to lower case. Keep the Parquet
# contract unchanged, but target the physical database column names when
# streaming CSV into seasonal_yield.
DATABASE_COLUMN_OVERRIDES = {
    "seasonal_npp_gC_m2": "seasonal_npp_gc_m2",
    "tbp_tDM_ha": "tbp_tdm_ha",
}
SMALLINT_COLUMNS = {
    "optical_valid_weeks",
    "sar_supported_weeks",
    "npp_valid_days",
    "npp_total_days",
    "npp_periods_used",
}


def log(message: str) -> None:
    stamp = datetime.now().astimezone().isoformat(timespec="seconds")
    print(f"[{stamp}] {message}", flush=True)


def database_url() -> str:
    return os.environ.get(
        "DATABASE_URL",
        "postgresql:///kenya_monitoring?host=/var/run/postgresql",
    ).replace("postgresql+psycopg://", "postgresql://", 1)


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument(
        "--scratch-dir", type=Path, default=Path("/opt/kenya-dashboard/load-tmp")
    )
    return parser.parse_args()


def read_run(args: argparse.Namespace) -> tuple[dict[str, Any], list[Path], int]:
    manifest_path = args.run_dir / "run_manifest.json"
    validation_path = args.run_dir / "validation_report.json"
    if not manifest_path.is_file() or not validation_path.is_file():
        raise RuntimeError("Run manifest and final validation report are required")
    manifest = json.loads(manifest_path.read_text())
    validation = json.loads(validation_path.read_text())
    if validation.get("status") != "passed":
        raise RuntimeError("Yield production validation has not passed")
    season_codes = [str(value) for value in manifest["season_codes"]]
    files = [args.run_dir / "outputs" / f"{code}.parquet" for code in season_codes]
    missing = [str(path) for path in files if not path.is_file()]
    if missing:
        raise RuntimeError(f"Missing {len(missing)} seasonal output files")
    expected_fields = int(manifest["field_count"])
    required = set(YIELD_COLUMNS)
    for code, path in zip(season_codes, files):
        parquet = pq.ParquetFile(path)
        absent = required - set(parquet.schema_arrow.names)
        if absent:
            raise RuntimeError(f"{path} is missing columns: {sorted(absent)}")
        if parquet.metadata.num_rows != expected_fields:
            raise RuntimeError(
                f"{code}: rows={parquet.metadata.num_rows:,}; expected={expected_fields:,}"
            )
    expected_rows = expected_fields * len(files)
    if int(validation.get("output_rows", expected_rows)) != expected_rows:
        raise RuntimeError("Validation report row count does not match the manifest")
    return manifest, files, expected_rows


def parquet_to_csv(path: Path, csv_path: Path) -> None:
    selections = ", ".join(
        (
            f'CAST("{column}" AS SMALLINT) AS "{column}"'
            if column in SMALLINT_COLUMNS
            else f'"{column}"'
        )
        for column in YIELD_COLUMNS
    )
    duck = duckdb.connect()
    try:
        duck.execute("SET threads = 4")
        duck.execute("SET memory_limit = '8GB'")
        duck.execute(
            f"""
            COPY (
                SELECT {selections}
                FROM read_parquet({sql_literal(str(path))})
            ) TO {sql_literal(str(csv_path))}
            (FORMAT CSV, HEADER TRUE, NULL '')
            """
        )
    finally:
        duck.close()


def register_loading_run(
    conn: psycopg.Connection,
    manifest: dict[str, Any],
    files: list[Path],
    expected_rows: int,
) -> None:
    metadata = {
        "source_run_dir": str(manifest.get("run_id", "")),
        "production_version": manifest.get("production_version"),
        "phenology_version": manifest.get("phenology_version"),
        "code_version": manifest.get("code_version"),
        "source_files": [path.name for path in files],
        "loader": "dashboard/scripts/load_yield.py",
    }
    with conn.transaction():
        conn.execute(
            """
            INSERT INTO dashboard.yield_runs(
                yield_run_id, model_run_id, method_version, parameter_set_id,
                season_count, field_count, result_rows, status, is_active, metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, 'loading', false, %s)
            ON CONFLICT (yield_run_id) DO UPDATE SET
                model_run_id = EXCLUDED.model_run_id,
                method_version = EXCLUDED.method_version,
                parameter_set_id = EXCLUDED.parameter_set_id,
                season_count = EXCLUDED.season_count,
                field_count = EXCLUDED.field_count,
                result_rows = EXCLUDED.result_rows,
                status = CASE
                    WHEN dashboard.yield_runs.is_active THEN dashboard.yield_runs.status
                    ELSE 'loading'
                END,
                metadata = EXCLUDED.metadata
            """,
            (
                manifest["run_id"], manifest["model_run_id"], manifest["method_version"],
                manifest["parameter_set_id"], len(files), int(manifest["field_count"]),
                expected_rows, Jsonb(metadata),
            ),
        )


def checkpoint_state(
    conn: psycopg.Connection, yield_run_id: str
) -> dict[str, tuple[str, int, int]]:
    rows = conn.execute(
        """
        SELECT season_code, source_file, source_size_bytes, row_count
        FROM dashboard.yield_load_checkpoints
        WHERE yield_run_id = %s
        """,
        (yield_run_id,),
    ).fetchall()
    return {str(row[0]): (str(row[1]), int(row[2]), int(row[3])) for row in rows}


def load_season(
    conn: psycopg.Connection,
    path: Path,
    yield_run_id: str,
    scratch_dir: Path,
) -> int:
    season_code = path.stem
    row_count = int(pq.ParquetFile(path).metadata.num_rows)
    with tempfile.NamedTemporaryFile(
        prefix=f"seasonal-yield-{season_code}-",
        suffix=".csv",
        dir=scratch_dir,
        delete=False,
    ) as handle:
        csv_path = Path(handle.name)
    try:
        parquet_to_csv(path, csv_path)
        columns = ", ".join(
            f'"{DATABASE_COLUMN_OVERRIDES.get(column, column)}"'
            for column in YIELD_COLUMNS
        )
        with conn.transaction():
            conn.execute(
                "DELETE FROM dashboard.seasonal_yield WHERE yield_run_id = %s AND season_code = %s",
                (yield_run_id, season_code),
            )
            conn.execute(
                "DELETE FROM dashboard.yield_load_checkpoints WHERE yield_run_id = %s AND season_code = %s",
                (yield_run_id, season_code),
            )
            with conn.cursor() as cursor, cursor.copy(
                f"COPY dashboard.seasonal_yield ({columns}) "
                "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')"
            ) as copy:
                with csv_path.open("rb") as source:
                    for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
                        copy.write(chunk)
            conn.execute(
                """
                INSERT INTO dashboard.yield_load_checkpoints(
                    yield_run_id, season_code, source_file, source_size_bytes, row_count
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                (yield_run_id, season_code, path.name, path.stat().st_size, row_count),
            )
    finally:
        csv_path.unlink(missing_ok=True)
    return row_count


def load_outputs(
    conn: psycopg.Connection,
    args: argparse.Namespace,
    manifest: dict[str, Any],
    files: list[Path],
) -> None:
    run_id = str(manifest["run_id"])
    checkpoints = checkpoint_state(conn, run_id)
    for position, path in enumerate(files, 1):
        season_code = path.stem
        expected_rows = int(pq.ParquetFile(path).metadata.num_rows)
        checkpoint = checkpoints.get(season_code)
        if checkpoint == (path.name, path.stat().st_size, expected_rows):
            database_rows = conn.execute(
                "SELECT count(*) FROM dashboard.seasonal_yield WHERE yield_run_id=%s AND season_code=%s",
                (run_id, season_code),
            ).fetchone()[0]
            if database_rows == expected_rows:
                log(f"[{position}/{len(files)}] {season_code} checkpoint valid; skipping")
                continue
        started = time.monotonic()
        loaded = load_season(conn, path, run_id, args.scratch_dir)
        log(
            f"[{position}/{len(files)}] loaded {season_code}: {loaded:,} rows "
            f"in {time.monotonic() - started:.1f}s"
        )


def build_summary(conn: psycopg.Connection, yield_run_id: str) -> None:
    log("Building seasonal yield summary")
    with conn.transaction():
        conn.execute(
            "DELETE FROM dashboard.seasonal_yield_summary WHERE yield_run_id = %s",
            (yield_run_id,),
        )
        conn.execute(
            """
            INSERT INTO dashboard.seasonal_yield_summary(
                yield_run_id, model_run_id, season_code, county, crop_label,
                yield_status, field_count, calculated_fields,
                yield_central_mean, yield_central_median,
                yield_central_min, yield_central_max, production_central_t
            )
            SELECT yield_run_id, model_run_id, season_code, county, crop_label,
                   yield_status, count(*)::integer,
                   count(yield_central_t_ha)::integer,
                   avg(yield_central_t_ha),
                   percentile_cont(0.5) WITHIN GROUP (ORDER BY yield_central_t_ha),
                   min(yield_central_t_ha), max(yield_central_t_ha),
                   sum(production_central_t)
            FROM dashboard.seasonal_yield
            WHERE yield_run_id = %s
            GROUP BY yield_run_id, model_run_id, season_code, county,
                     crop_label, yield_status
            """,
            (yield_run_id,),
        )


def validate_and_activate(
    conn: psycopg.Connection,
    manifest: dict[str, Any],
    files: list[Path],
    expected_rows: int,
    report_path: Path,
) -> None:
    run_id = str(manifest["run_id"])
    metrics = conn.execute(
        """
        SELECT count(*)::bigint, count(DISTINCT field_id)::bigint,
               count(DISTINCT season_code)::integer,
               count(*) FILTER (WHERE yield_central_t_ha IS NOT NULL)::bigint,
               count(*) FILTER (
                   WHERE yield_central_t_ha IS NOT NULL
                     AND NOT (yield_low_t_ha <= yield_central_t_ha
                              AND yield_central_t_ha <= yield_high_t_ha)
               )::bigint
        FROM dashboard.seasonal_yield
        WHERE yield_run_id = %s
        """,
        (run_id,),
    ).fetchone()
    checkpoints = conn.execute(
        """
        SELECT count(*)::integer, coalesce(sum(row_count), 0)::bigint
        FROM dashboard.yield_load_checkpoints WHERE yield_run_id = %s
        """,
        (run_id,),
    ).fetchone()
    summary_groups = conn.execute(
        "SELECT count(*) FROM dashboard.seasonal_yield_summary WHERE yield_run_id = %s",
        (run_id,),
    ).fetchone()[0]
    failures: list[str] = []
    expected_fields = int(manifest["field_count"])
    checks = {
        "rows": (int(metrics[0]), expected_rows),
        "fields": (int(metrics[1]), expected_fields),
        "seasons": (int(metrics[2]), len(files)),
        "checkpoint_files": (int(checkpoints[0]), len(files)),
        "checkpoint_rows": (int(checkpoints[1]), expected_rows),
    }
    for name, (actual, expected) in checks.items():
        if actual != expected:
            failures.append(f"{name}={actual:,}; expected={expected:,}")
    if int(metrics[4]):
        failures.append(f"yield scenario order failed for {int(metrics[4]):,} rows")
    if not summary_groups:
        failures.append("yield summary is empty")
    report = {
        "yield_run_id": run_id,
        "model_run_id": manifest["model_run_id"],
        "metrics": {
            "rows": int(metrics[0]), "fields": int(metrics[1]),
            "seasons": int(metrics[2]), "calculated_rows": int(metrics[3]),
            "summary_groups": int(summary_groups),
            "checkpoint_files": int(checkpoints[0]),
            "checkpoint_rows": int(checkpoints[1]),
        },
        "failures": failures,
        "status": "passed" if not failures else "failed",
    }
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    if failures:
        with conn.transaction():
            conn.execute(
                "UPDATE dashboard.yield_runs SET status='failed', is_active=false WHERE yield_run_id=%s",
                (run_id,),
            )
        raise RuntimeError(f"Database yield validation failed: {failures}")
    with conn.transaction():
        conn.execute(
            "UPDATE dashboard.yield_runs SET is_active=false WHERE model_run_id=%s",
            (manifest["model_run_id"],),
        )
        conn.execute(
            """
            UPDATE dashboard.yield_runs
            SET status='validated', is_active=true, loaded_at=now(),
                metadata = metadata || %s
            WHERE yield_run_id=%s
            """,
            (Jsonb({"validated_at": datetime.now(timezone.utc).isoformat()}), run_id),
        )
    print(json.dumps(report, indent=2))
def main() -> None:
    args = parse_args()
    args.run_dir = args.run_dir.resolve()
    args.scratch_dir.mkdir(parents=True, exist_ok=True)
    manifest, files, expected_rows = read_run(args)
    report_path = args.scratch_dir / f"{manifest['run_id']}-database-validation.json"
    with psycopg.connect(database_url(), autocommit=True) as conn:
        register_loading_run(conn, manifest, files, expected_rows)
        load_outputs(conn, args, manifest, files)
        build_summary(conn, str(manifest["run_id"]))
        validate_and_activate(conn, manifest, files, expected_rows, report_path)
    log("Seasonal yield dashboard load complete")


if __name__ == "__main__":
    main()
