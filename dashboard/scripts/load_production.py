#!/usr/bin/env python3
"""Load full model predictions and field geometry into the dashboard schema.

The loader commits one Parquet file at a time and records a checkpoint in the
same transaction. Re-running the command safely resumes after the last
completed file. Existing Sentinel-1 and Sentinel-2 production tables are read
by the API directly and are never copied or modified here.
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
import geopandas as gpd
import pandas as pd
import psycopg
import pyarrow.parquet as pq
from psycopg.types.json import Jsonb


COUNTIES = {"Machakos": "machakos_fin", "Nakuru": "nakuru_fin"}
PREDICTION_COLUMNS = [
    "model_run_id",
    "field_id",
    "week_start",
    "county",
    "season",
    "week_in_season",
    "scorable",
    "label",
    "reason",
    "confidence",
    "p_maize",
    "p_beans",
    "p_potatoes",
]


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
    parser.add_argument("--model-output-root", type=Path, required=True)
    parser.add_argument("--gpkg", type=Path, required=True)
    parser.add_argument("--model-run-id", required=True)
    parser.add_argument("--expected-fields", type=int, required=True)
    parser.add_argument("--expected-weeks", type=int, required=True)
    parser.add_argument("--expected-predictions", type=int, required=True)
    parser.add_argument(
        "--scratch-dir",
        type=Path,
        default=Path("/opt/kenya-dashboard/load-tmp"),
    )
    return parser.parse_args()


def ensure_sources(args: argparse.Namespace) -> list[Path]:
    if not args.gpkg.is_file():
        raise RuntimeError(f"GeoPackage not found: {args.gpkg}")
    files = sorted(args.model_output_root.glob("*/*.parquet"))
    if not files:
        raise RuntimeError(f"No Parquet files found under {args.model_output_root}")
    for path in files:
        schema = set(pq.ParquetFile(path).schema_arrow.names)
        missing = set(PREDICTION_COLUMNS[1:]) - schema
        if missing:
            raise RuntimeError(f"{path} is missing columns: {sorted(missing)}")
    return files


def source_statistics(files: list[Path]) -> dict[str, Any]:
    pattern = str(files[0].parents[1] / "*" / "*.parquet")
    duck = duckdb.connect()
    try:
        duck.execute("SET threads = 4")
        duck.execute("SET memory_limit = '8GB'")
        row = duck.execute(
            f"""
            SELECT count(*)::BIGINT,
                   count(DISTINCT field_id)::BIGINT,
                   count(DISTINCT CAST(week_start AS DATE))::BIGINT,
                   min(CAST(week_start AS DATE)),
                   max(CAST(week_start AS DATE))
            FROM read_parquet({sql_literal(pattern)}, union_by_name=true)
            """
        ).fetchone()
    finally:
        duck.close()
    return {
        "prediction_rows": int(row[0]),
        "field_count": int(row[1]),
        "week_count": int(row[2]),
        "date_from": row[3],
        "date_to": row[4],
        "file_count": len(files),
    }


def validate_expected(args: argparse.Namespace, stats: dict[str, Any]) -> None:
    expected = {
        "field_count": args.expected_fields,
        "week_count": args.expected_weeks,
        "prediction_rows": args.expected_predictions,
    }
    failures = [
        f"{key}: source={stats[key]:,}, expected={value:,}"
        for key, value in expected.items()
        if stats[key] != value
    ]
    if failures:
        raise RuntimeError("Source inventory mismatch: " + "; ".join(failures))


def load_fields(conn: psycopg.Connection, args: argparse.Namespace) -> None:
    existing = conn.execute("SELECT count(*) FROM dashboard.fields").fetchone()[0]
    if existing == args.expected_fields:
        log(f"Fields already loaded: {existing:,}; skipping geometry reload")
        return
    if existing:
        log(f"Replacing incomplete field inventory ({existing:,} rows)")

    frames: list[gpd.GeoDataFrame] = []
    for county, layer in COUNTIES.items():
        log(f"Reading geometry layer {layer}")
        frame = gpd.read_file(
            args.gpkg,
            layer=layer,
            columns=["field_id", "area_ha"],
            engine="pyogrio",
        )
        frame["field_id"] = frame["field_id"].astype(str).str.strip()
        frame["county"] = county
        frame["source_layer"] = layer
        frames.append(frame[["field_id", "county", "area_ha", "source_layer", "geometry"]])

    fields = gpd.GeoDataFrame(
        pd.concat(frames, ignore_index=True),
        geometry="geometry",
        crs=frames[0].crs,
    ).to_crs(3857)
    if len(fields) != args.expected_fields or fields["field_id"].nunique() != args.expected_fields:
        raise RuntimeError(
            f"Geometry inventory mismatch: rows={len(fields):,}, "
            f"unique={fields['field_id'].nunique():,}, expected={args.expected_fields:,}"
        )

    with conn.transaction():
        conn.execute("TRUNCATE dashboard.fields RESTART IDENTITY")
        conn.execute(
            """
            CREATE TEMP TABLE stage_dashboard_fields (
                field_id text,
                county text,
                area_ha double precision,
                source_layer text,
                geom_wkb bytea
            ) ON COMMIT DROP
            """
        )
        with conn.cursor() as cursor, cursor.copy(
            "COPY stage_dashboard_fields "
            "(field_id, county, area_ha, source_layer, geom_wkb) FROM STDIN"
        ) as copy:
            for row in fields.itertuples(index=False):
                copy.write_row(
                    (
                        str(row.field_id),
                        str(row.county),
                        None if pd.isna(row.area_ha) else float(row.area_ha),
                        str(row.source_layer),
                        bytes(row.geometry.wkb),
                    )
                )
        conn.execute(
            """
            WITH source AS (
                SELECT field_id, county, area_ha, source_layer,
                       ST_GeomFromWKB(geom_wkb, 3857) AS raw_geom
                FROM stage_dashboard_fields
            ), repaired AS (
                SELECT field_id, county, area_ha, source_layer,
                       ST_Multi(ST_CollectionExtract(ST_MakeValid(raw_geom), 3)) AS valid_geom
                FROM source
            )
            INSERT INTO dashboard.fields(
                field_id, county, area_ha, source_layer,
                geom, geom_simplified, centroid
            )
            SELECT field_id, county, area_ha, source_layer,
                   valid_geom,
                   ST_Multi(ST_SimplifyPreserveTopology(valid_geom, 5.0)),
                   ST_PointOnSurface(valid_geom)
            FROM repaired
            WHERE NOT ST_IsEmpty(valid_geom)
            """
        )
    loaded = conn.execute("SELECT count(*) FROM dashboard.fields").fetchone()[0]
    if loaded != args.expected_fields:
        raise RuntimeError(f"Loaded {loaded:,} fields; expected {args.expected_fields:,}")
    log(f"Loaded {loaded:,} field geometries")


def checkpoint_rows(conn: psycopg.Connection, model_run_id: str) -> dict[str, tuple[int, int]]:
    rows = conn.execute(
        """
        SELECT source_file, source_size_bytes, row_count
        FROM dashboard.load_checkpoints
        WHERE model_run_id = %s
        """,
        (model_run_id,),
    ).fetchall()
    return {str(row[0]): (int(row[1]), int(row[2])) for row in rows}


def parquet_to_csv(path: Path, csv_path: Path, model_run_id: str) -> None:
    duck = duckdb.connect()
    try:
        duck.execute("SET threads = 4")
        duck.execute("SET memory_limit = '8GB'")
        duck.execute(
            f"""
            COPY (
                SELECT {sql_literal(model_run_id)} AS "model_run_id",
                       CAST("field_id" AS VARCHAR) AS "field_id",
                       CAST("week_start" AS DATE) AS "week_start",
                       CAST("county" AS VARCHAR) AS "county",
                       CAST("season" AS VARCHAR) AS "season",
                       CAST("week_in_season" AS SMALLINT) AS "week_in_season",
                       CAST("scorable" AS BOOLEAN) AS "scorable",
                       CAST("label" AS VARCHAR) AS "label",
                       CAST("reason" AS VARCHAR) AS "reason",
                       "confidence", "p_maize", "p_beans", "p_potatoes"
                FROM read_parquet({sql_literal(str(path))})
            ) TO {sql_literal(str(csv_path))}
            (FORMAT CSV, HEADER TRUE, NULL '')
            """
        )
    finally:
        duck.close()


def copy_prediction_file(
    conn: psycopg.Connection,
    path: Path,
    relative_name: str,
    model_run_id: str,
    scratch_dir: Path,
) -> int:
    row_count = int(pq.ParquetFile(path).metadata.num_rows)
    size = path.stat().st_size
    with tempfile.NamedTemporaryFile(
        prefix="dashboard-predictions-",
        suffix=".csv",
        dir=scratch_dir,
        delete=False,
    ) as handle:
        csv_path = Path(handle.name)
    try:
        parquet_to_csv(path, csv_path, model_run_id)
        columns = ", ".join(f'"{column}"' for column in PREDICTION_COLUMNS)
        with conn.transaction():
            with conn.cursor() as cursor, cursor.copy(
                f"COPY dashboard.crop_predictions ({columns}) "
                "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')"
            ) as copy:
                with csv_path.open("rb") as source:
                    for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
                        copy.write(chunk)
            conn.execute(
                """
                INSERT INTO dashboard.load_checkpoints(
                    model_run_id, source_file, source_size_bytes, row_count
                ) VALUES (%s, %s, %s, %s)
                """,
                (model_run_id, relative_name, size, row_count),
            )
    finally:
        csv_path.unlink(missing_ok=True)
    return row_count


def load_predictions(
    conn: psycopg.Connection,
    args: argparse.Namespace,
    files: list[Path],
) -> None:
    completed = checkpoint_rows(conn, args.model_run_id)
    total_loaded = sum(value[1] for value in completed.values())
    log(
        f"Prediction resume state: {len(completed)}/{len(files)} files, "
        f"{total_loaded:,}/{args.expected_predictions:,} rows"
    )

    for number, path in enumerate(files, start=1):
        relative_name = str(path.relative_to(args.model_output_root))
        existing = completed.get(relative_name)
        if existing:
            if existing[0] != path.stat().st_size:
                raise RuntimeError(f"Source file changed after checkpoint: {relative_name}")
            log(f"[{number}/{len(files)}] checkpoint exists; skipping {relative_name}")
            continue
        started = time.monotonic()
        log(f"[{number}/{len(files)}] loading {relative_name}")
        rows = copy_prediction_file(
            conn,
            path,
            relative_name,
            args.model_run_id,
            args.scratch_dir,
        )
        total_loaded += rows
        log(
            f"[{number}/{len(files)}] loaded {rows:,} rows in "
            f"{time.monotonic() - started:.1f}s; cumulative={total_loaded:,}"
        )

    completed_total = conn.execute(
        "SELECT coalesce(sum(row_count), 0) FROM dashboard.load_checkpoints WHERE model_run_id = %s",
        (args.model_run_id,),
    ).fetchone()[0]
    if completed_total != args.expected_predictions:
        raise RuntimeError(
            f"Checkpoint total is {completed_total:,}; expected {args.expected_predictions:,}"
        )


def load_summary(
    conn: psycopg.Connection,
    args: argparse.Namespace,
    files: list[Path],
) -> None:
    pattern = str(files[0].parents[1] / "*" / "*.parquet")
    with tempfile.NamedTemporaryFile(
        prefix="dashboard-summary-",
        suffix=".csv",
        dir=args.scratch_dir,
        delete=False,
    ) as handle:
        csv_path = Path(handle.name)
    duck = duckdb.connect()
    try:
        duck.execute("SET threads = 4")
        duck.execute("SET memory_limit = '8GB'")
        duck.execute(
            f"""
            COPY (
                SELECT {sql_literal(args.model_run_id)} AS "model_run_id",
                       CAST("week_start" AS DATE) AS "week_start",
                       "county",
                       "season",
                       CAST("week_in_season" AS SMALLINT) AS "week_in_season",
                       "label",
                       "reason",
                       CAST(count(*) AS INTEGER) AS "field_count",
                       CAST(avg("confidence") AS DOUBLE) AS "mean_confidence"
                FROM read_parquet({sql_literal(pattern)}, union_by_name=true)
                GROUP BY "week_start", "county", "season", "week_in_season", "label", "reason"
                ORDER BY "week_start", "county", "label", "reason"
            ) TO {sql_literal(str(csv_path))}
            (FORMAT CSV, HEADER TRUE, NULL '')
            """
        )
    finally:
        duck.close()

    columns = (
        "model_run_id, week_start, county, season, week_in_season, "
        "label, reason, field_count, mean_confidence"
    )
    try:
        with conn.transaction():
            conn.execute(
                "DELETE FROM dashboard.prediction_week_summary WHERE model_run_id = %s",
                (args.model_run_id,),
            )
            with conn.cursor() as cursor, cursor.copy(
                f"COPY dashboard.prediction_week_summary ({columns}) "
                "FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')"
            ) as copy:
                with csv_path.open("rb") as source:
                    for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
                        copy.write(chunk)
    finally:
        csv_path.unlink(missing_ok=True)
    log("Loaded prediction week summary")


def activate_run(
    conn: psycopg.Connection,
    args: argparse.Namespace,
    stats: dict[str, Any],
    files: list[Path],
) -> None:
    metadata = {
        "loader": "dashboard/scripts/load_production.py",
        "loaded_at": datetime.now(timezone.utc).isoformat(),
        "source_file_count": len(files),
        "source_statistics": {key: str(value) for key, value in stats.items()},
    }
    with conn.transaction():
        conn.execute("UPDATE dashboard.model_runs SET is_active = false WHERE is_active")
        conn.execute(
            """
            INSERT INTO dashboard.model_runs(
                model_run_id, source_path, date_from, date_to,
                field_count, week_count, prediction_rows,
                is_active, metadata
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, true, %s)
            ON CONFLICT (model_run_id) DO UPDATE SET
                source_path = EXCLUDED.source_path,
                date_from = EXCLUDED.date_from,
                date_to = EXCLUDED.date_to,
                field_count = EXCLUDED.field_count,
                week_count = EXCLUDED.week_count,
                prediction_rows = EXCLUDED.prediction_rows,
                is_active = true,
                metadata = EXCLUDED.metadata,
                loaded_at = now()
            """,
            (
                args.model_run_id,
                str(args.model_output_root),
                stats["date_from"],
                stats["date_to"],
                stats["field_count"],
                stats["week_count"],
                stats["prediction_rows"],
                Jsonb(metadata),
            ),
        )
    log(f"Activated model run {args.model_run_id}")


def main() -> None:
    args = parse_args()
    args.scratch_dir.mkdir(parents=True, exist_ok=True)
    files = ensure_sources(args)
    log(f"Inventoried {len(files)} prediction Parquet files")
    stats = source_statistics(files)
    log("Source statistics: " + json.dumps(stats, default=str, sort_keys=True))
    validate_expected(args, stats)

    with psycopg.connect(database_url(), autocommit=True) as conn:
        completed_run = conn.execute(
            "SELECT is_active FROM dashboard.model_runs WHERE model_run_id = %s",
            (args.model_run_id,),
        ).fetchone()
        if completed_run and completed_run[0]:
            log(f"Model run {args.model_run_id} is already active; nothing to load")
            return
        load_fields(conn, args)
        load_predictions(conn, args, files)
        load_summary(conn, args, files)
        activate_run(conn, args, stats, files)
        conn.execute("ANALYZE dashboard.fields")
        conn.execute("ANALYZE dashboard.crop_predictions")
        conn.execute("ANALYZE dashboard.prediction_week_summary")
    log("Production dashboard load complete")


if __name__ == "__main__":
    main()
