#!/usr/bin/env python3
"""Load a validated dashboard development bundle into local PostGIS."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
from pathlib import Path
from typing import Any

import duckdb
import geopandas as gpd
import psycopg
from psycopg.types.json import Jsonb


def database_url() -> str:
    return os.environ.get(
        "DATABASE_URL",
        "postgresql://127.0.0.1:5432/kenya_dashboard_dev",
    ).replace("postgresql+psycopg://", "postgresql://", 1)


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def columns_for_table(conn: psycopg.Connection, schema: str, table: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
          AND is_identity = 'NO'
        ORDER BY ordinal_position
        """,
        (schema, table),
    ).fetchall()
    return [row[0] for row in rows]


def copy_parquet(
    conn: psycopg.Connection,
    parquet_path: Path,
    schema: str,
    table: str,
    columns: list[str],
    select_expressions: list[str] | None = None,
) -> int:
    expressions = select_expressions or [f'"{column}"' for column in columns]
    with tempfile.NamedTemporaryFile(prefix=f"{table}-", suffix=".csv", delete=False) as handle:
        csv_path = Path(handle.name)
    duck = duckdb.connect()
    try:
        duck.execute(
            f"""
            COPY (
                SELECT {', '.join(expressions)}
                FROM read_parquet({sql_literal(str(parquet_path))})
            ) TO {sql_literal(str(csv_path))}
            (FORMAT CSV, HEADER TRUE, NULL '')
            """
        )
        row_count = int(
            duck.execute(
                f"SELECT count(*) FROM read_parquet({sql_literal(str(parquet_path))})"
            ).fetchone()[0]
        )
        quoted = ", ".join(f'"{column}"' for column in columns)
        with conn.cursor() as cursor, cursor.copy(
            f'COPY "{schema}"."{table}" ({quoted}) FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL \'\')'
        ) as copy:
            with csv_path.open("rb") as source:
                for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
                    copy.write(chunk)
        return row_count
    finally:
        duck.close()
        csv_path.unlink(missing_ok=True)


def load_fields(conn: psycopg.Connection, path: Path) -> int:
    fields = gpd.read_parquet(path)
    if fields.crs is None:
        raise RuntimeError("fields.parquet has no CRS")
    fields = fields.to_crs(3857)
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
        "COPY stage_dashboard_fields (field_id, county, area_ha, source_layer, geom_wkb) FROM STDIN"
    ) as copy:
        for row in fields.itertuples(index=False):
            copy.write_row(
                (
                    str(row.field_id),
                    str(row.county),
                    None if row.area_ha is None else float(row.area_ha),
                    str(row.source_layer),
                    bytes(row.geometry.wkb),
                )
            )
    conn.execute(
        """
        WITH source AS (
            SELECT field_id,
                   county,
                   area_ha,
                   source_layer,
                   ST_GeomFromWKB(geom_wkb, 3857) AS raw_geom
            FROM stage_dashboard_fields
        ), repaired AS (
            SELECT field_id,
                   county,
                   area_ha,
                   source_layer,
                   ST_Multi(
                       ST_CollectionExtract(ST_MakeValid(raw_geom), 3)
                   ) AS valid_geom
            FROM source
        )
        INSERT INTO dashboard.fields(
            field_id, county, area_ha, source_layer,
            geom, geom_simplified, centroid
        )
        SELECT field_id,
               county,
               area_ha,
               source_layer,
               valid_geom,
               ST_Multi(ST_SimplifyPreserveTopology(valid_geom, 5.0)),
               ST_PointOnSurface(valid_geom)
        FROM repaired
        WHERE NOT ST_IsEmpty(valid_geom)
        """
    )
    return len(fields)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle-dir", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = json.loads((args.bundle_dir / "manifest.json").read_text())
    if manifest.get("status") != "passed":
        raise RuntimeError("Bundle manifest has not passed validation")

    model_run_id = str(manifest["model_run_id"])
    with psycopg.connect(database_url()) as conn:
        with conn.transaction():
            conn.execute("TRUNCATE dashboard.prediction_week_summary")
            conn.execute("TRUNCATE dashboard.crop_predictions")
            conn.execute("TRUNCATE timeseries.s1_indices")
            conn.execute("TRUNCATE timeseries.s2_indices")
            conn.execute("TRUNCATE dashboard.fields RESTART IDENTITY")
            conn.execute("UPDATE dashboard.model_runs SET is_active = false WHERE is_active")

            print("Loading fields...", flush=True)
            field_rows = load_fields(conn, args.bundle_dir / "fields.parquet")

            prediction_columns = columns_for_table(conn, "dashboard", "crop_predictions")
            print("Loading crop predictions...", flush=True)
            prediction_rows = copy_parquet(
                conn,
                args.bundle_dir / "crop_predictions.parquet",
                "dashboard",
                "crop_predictions",
                prediction_columns,
            )

            for sensor in ("s1", "s2"):
                table = f"{sensor}_indices"
                columns = columns_for_table(conn, "timeseries", table)
                print(f"Loading {sensor.upper()} observations...", flush=True)
                rows = copy_parquet(
                    conn,
                    args.bundle_dir / f"{sensor}_observations.parquet",
                    "timeseries",
                    table,
                    columns,
                )
                print(f"Loaded {rows:,} {sensor.upper()} rows", flush=True)

            conn.execute(
                """
                INSERT INTO dashboard.model_runs(
                    model_run_id, source_path, date_from, date_to,
                    field_count, week_count, prediction_rows, is_active, metadata
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
                    model_run_id,
                    manifest.get("source_path"),
                    manifest["date_from"],
                    manifest["date_to"],
                    manifest["field_count"],
                    manifest["week_count"],
                    manifest["prediction_rows"],
                    Jsonb(manifest),
                ),
            )
            conn.execute(
                """
                INSERT INTO dashboard.prediction_week_summary(
                    model_run_id, week_start, county, season, week_in_season,
                    label, reason, field_count, mean_confidence
                )
                SELECT model_run_id, week_start, county, season, week_in_season,
                       label, reason, count(*)::integer, avg(confidence)
                FROM dashboard.crop_predictions
                GROUP BY model_run_id, week_start, county, season,
                         week_in_season, label, reason
                """
            )

        for relation in (
            "dashboard.fields",
            "dashboard.crop_predictions",
            "dashboard.prediction_week_summary",
            "timeseries.s1_indices",
            "timeseries.s2_indices",
        ):
            conn.execute(f"ANALYZE {relation}")
        print(f"Loaded {field_rows:,} fields and {prediction_rows:,} predictions.")


if __name__ == "__main__":
    main()
