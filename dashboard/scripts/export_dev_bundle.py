#!/usr/bin/env python3
"""Build a deterministic, production-derived dashboard development bundle.

Run this script on the production server. It reads production sources without
modifying them and writes a self-contained bundle of Parquet files plus a
validation manifest.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import geopandas as gpd
import pandas as pd


FIELD_ID_RE = re.compile(r"^[A-Za-z0-9_.:-]+$")
COUNTIES = ("Machakos", "Nakuru")
LAYERS = {"Machakos": "machakos_fin", "Nakuru": "nakuru_fin"}


def log(message: str) -> None:
    print(f"[{datetime.now().astimezone().isoformat(timespec='seconds')}] {message}", flush=True)


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def psql_rows(args: argparse.Namespace, query: str) -> list[list[str]]:
    command = [
        *shlex.split(args.psql_command),
        "-d",
        args.database,
        "-X",
        "-A",
        "-t",
        "-F",
        "\t",
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        query,
    ]
    result = subprocess.run(command, check=True, text=True, capture_output=True)
    return [line.split("\t") for line in result.stdout.splitlines() if line.strip()]


def inventory_fields(args: argparse.Namespace) -> pd.DataFrame:
    rows = psql_rows(
        args,
        f"""
        SELECT field_id,
               CASE lower(region)
                   WHEN 'machakos' THEN 'Machakos'
                   WHEN 'nakuru' THEN 'Nakuru'
               END AS county
        FROM timeseries.s2_indices
        WHERE start_date = DATE {sql_literal(args.date_to)}
        ORDER BY field_id
        """,
    )
    frame = pd.DataFrame(rows, columns=["field_id", "county"])
    if frame.empty or frame["county"].isna().any():
        raise RuntimeError("Production field inventory is empty or contains an unknown county")
    if frame["field_id"].duplicated().any():
        raise RuntimeError("Production field inventory contains duplicate field IDs")
    invalid = frame.loc[~frame["field_id"].str.fullmatch(FIELD_ID_RE), "field_id"]
    if len(invalid):
        raise RuntimeError(f"Invalid production field IDs: {invalid.head(10).tolist()}")
    return frame


def read_required_ids(args: argparse.Namespace) -> tuple[set[str], set[str]]:
    giuseppe = {
        line.strip()
        for line in args.giuseppe_field_list.read_text().splitlines()
        if line.strip()
    }
    out_of_range: set[str] = set()
    with args.out_of_range_csv.open(newline="") as source:
        for row in csv.DictReader(source):
            field_id = (row.get("field_id") or "").strip()
            if field_id:
                out_of_range.add(field_id)
    return giuseppe, out_of_range


def select_fields(args: argparse.Namespace, inventory: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    giuseppe, out_of_range = read_required_ids(args)
    known = set(inventory["field_id"])
    missing = sorted((giuseppe | out_of_range) - known)
    if missing:
        raise RuntimeError(f"Required field IDs are absent from production: {missing[:10]}")

    required = giuseppe | out_of_range
    selected_parts: list[pd.DataFrame] = []
    for county in COUNTIES:
        county_fields = inventory.loc[inventory["county"] == county].copy()
        county_fields["required"] = county_fields["field_id"].isin(required)
        required_count = int(county_fields["required"].sum())
        if required_count > args.fields_per_county:
            raise RuntimeError(
                f"{county} has {required_count} required fields, exceeding "
                f"--fields-per-county={args.fields_per_county}"
            )
        county_fields["sample_order"] = county_fields["field_id"].map(
            lambda value: hashlib.sha256(f"{args.seed}:{value}".encode()).hexdigest()
        )
        chosen = pd.concat(
            [
                county_fields.loc[county_fields["required"]].sort_values("field_id"),
                county_fields.loc[~county_fields["required"]].sort_values("sample_order").head(
                    args.fields_per_county - required_count
                ),
            ],
            ignore_index=True,
        )
        selected_parts.append(chosen[["field_id", "county"]])

    selected = (
        pd.concat(selected_parts, ignore_index=True)
        .sort_values(["county", "field_id"])
        .reset_index(drop=True)
    )
    expected = args.fields_per_county * len(COUNTIES)
    if len(selected) != expected or selected["field_id"].nunique() != expected:
        raise RuntimeError("Deterministic field selection produced an unexpected count")
    audit = {
        "giuseppe_field_count": len(giuseppe),
        "out_of_range_field_count": len(out_of_range),
        "required_union_count": len(required),
        "selected_by_county": selected.groupby("county").size().astype(int).to_dict(),
    }
    return selected, audit


def export_fields(args: argparse.Namespace, selected: pd.DataFrame, destination: Path) -> None:
    selected_ids = set(selected["field_id"])
    frames: list[gpd.GeoDataFrame] = []
    for county, layer in LAYERS.items():
        log(f"Reading geometry layer {layer}")
        frame = gpd.read_file(
            args.gpkg,
            layer=layer,
            columns=["field_id", "area_ha", "geometry"],
            engine="pyogrio",
        )
        frame["field_id"] = frame["field_id"].astype(str).str.strip()
        frame = frame.loc[frame["field_id"].isin(selected_ids)].copy()
        frame["county"] = county
        frame["source_layer"] = layer
        frames.append(frame[["field_id", "county", "area_ha", "source_layer", "geometry"]])
    fields = gpd.GeoDataFrame(pd.concat(frames, ignore_index=True), crs=frames[0].crs)
    if len(fields) != len(selected) or set(fields["field_id"]) != selected_ids:
        raise RuntimeError("Geometry export does not match selected field inventory")
    if fields.geometry.isna().any() or fields.geometry.is_empty.any() or (~fields.geometry.is_valid).any():
        raise RuntimeError("Selected field geometry contains null, empty or invalid geometry")
    fields.sort_values(["county", "field_id"]).to_parquet(destination, index=False, compression="zstd")


def export_predictions(args: argparse.Namespace, selected: pd.DataFrame, destination: Path) -> None:
    pattern = str(args.model_output_root / "*" / "*.parquet")
    connection = duckdb.connect()
    try:
        connection.register("selected_fields", selected[["field_id"]])
        query = f"""
            SELECT p.field_id,
                   CAST(p.week_start AS DATE) AS week_start,
                   p.county,
                   p.season,
                   CAST(p.week_in_season AS SMALLINT) AS week_in_season,
                   p.scorable,
                   p.label,
                   p.reason,
                   p.confidence,
                   p.p_maize,
                   p.p_beans,
                   p.p_potatoes,
                   {sql_literal(args.model_run_id)} AS model_run_id
            FROM read_parquet({sql_literal(pattern)}, union_by_name=true) AS p
            JOIN selected_fields AS f USING (field_id)
            ORDER BY p.field_id, p.week_start
        """
        connection.execute(
            f"COPY ({query}) TO {sql_literal(str(destination))} "
            "(FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)"
        )
    finally:
        connection.close()


def export_observations(
    args: argparse.Namespace,
    selected: pd.DataFrame,
    sensor: str,
    destination: Path,
) -> None:
    values = ",".join(sql_literal(value) for value in selected["field_id"])
    query = f"""
        COPY (
            SELECT *
            FROM timeseries.{sensor}_indices
            WHERE field_id = ANY(ARRAY[{values}]::text[])
              AND start_date BETWEEN DATE {sql_literal(args.date_from)}
                                 AND DATE {sql_literal(args.date_to)}
            ORDER BY field_id, start_date
        ) TO STDOUT WITH (FORMAT CSV, HEADER TRUE, NULL '')
    """
    csv_path = destination.with_suffix(".csv.partial")
    command = [
        *shlex.split(args.psql_command),
        "-d",
        args.database,
        "-X",
        "-q",
        "-v",
        "ON_ERROR_STOP=1",
        "-c",
        query,
    ]
    try:
        with csv_path.open("wb") as target:
            subprocess.run(command, check=True, stdout=target)
        connection = duckdb.connect()
        try:
            connection.execute(
                f"""
                COPY (
                    SELECT * FROM read_csv_auto(
                        {sql_literal(str(csv_path))},
                        header=true,
                        nullstr='',
                        sample_size=-1
                    )
                ) TO {sql_literal(str(destination))}
                (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
                """
            )
        finally:
            connection.close()
    finally:
        csv_path.unlink(missing_ok=True)


def parquet_summary(path: Path, key_columns: tuple[str, str] | None = None) -> dict[str, Any]:
    connection = duckdb.connect()
    try:
        path_sql = sql_literal(str(path))
        rows = int(connection.execute(f"SELECT count(*) FROM read_parquet({path_sql})").fetchone()[0])
        result: dict[str, Any] = {"rows": rows, "bytes": path.stat().st_size, "sha256": sha256_file(path)}
        if key_columns:
            field_column, date_column = key_columns
            values = connection.execute(
                f"""
                SELECT count(DISTINCT {field_column}),
                       count(DISTINCT {date_column}),
                       count(*) - count(DISTINCT ({field_column}, {date_column}))
                FROM read_parquet({path_sql})
                """
            ).fetchone()
            result.update(fields=int(values[0]), weeks=int(values[1]), duplicate_keys=int(values[2]))
        return result
    finally:
        connection.close()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--database", default="kenya_monitoring")
    parser.add_argument("--psql-command", default="sudo -u postgres psql")
    parser.add_argument("--gpkg", type=Path, default=Path("/opt/project/final2.gpkg"))
    parser.add_argument(
        "--model-output-root",
        type=Path,
        default=Path("/opt/project/model_runs/kenya_v3_all_fields_20261014/outputs"),
    )
    parser.add_argument("--model-run-id", default="kenya_v3_all_fields_20261014")
    parser.add_argument("--giuseppe-field-list", type=Path, default=Path("/opt/project/field_id_list.txt"))
    parser.add_argument(
        "--out-of-range-csv",
        type=Path,
        default=Path(
            "/opt/project/model_runs/kenya_v3_all_fields_20261014/out_of_range_raw_rows.csv"
        ),
    )
    parser.add_argument("--date-from", default="2016-01-04")
    parser.add_argument("--date-to", default="2026-05-25")
    parser.add_argument("--expected-weeks", type=int, default=543)
    parser.add_argument("--fields-per-county", type=int, default=1000)
    parser.add_argument("--seed", default="kenya-dashboard-dev-v1")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    args.output_dir.mkdir(parents=True, exist_ok=True)
    if any(args.output_dir.iterdir()):
        raise RuntimeError(f"Output directory must be empty: {args.output_dir}")

    log("Inventorying production fields")
    inventory = inventory_fields(args)
    selected, selection_audit = select_fields(args, inventory)
    selected_path = args.output_dir / "selected_fields.csv"
    selected.to_csv(selected_path, index=False)
    log(f"Selected {len(selected):,} fields: {selection_audit['selected_by_county']}")

    paths = {
        "fields": args.output_dir / "fields.parquet",
        "predictions": args.output_dir / "crop_predictions.parquet",
        "s1": args.output_dir / "s1_observations.parquet",
        "s2": args.output_dir / "s2_observations.parquet",
    }
    export_fields(args, selected, paths["fields"])
    log("Exporting crop predictions")
    export_predictions(args, selected, paths["predictions"])
    log("Exporting Sentinel-1 observations")
    export_observations(args, selected, "s1", paths["s1"])
    log("Exporting Sentinel-2 observations")
    export_observations(args, selected, "s2", paths["s2"])

    expected_fields = args.fields_per_county * len(COUNTIES)
    expected_predictions = expected_fields * args.expected_weeks
    files = {
        "selected_fields.csv": {
            "rows": len(selected),
            "bytes": selected_path.stat().st_size,
            "sha256": sha256_file(selected_path),
        },
        "fields.parquet": parquet_summary(paths["fields"], ("field_id", "field_id")),
        "crop_predictions.parquet": parquet_summary(paths["predictions"], ("field_id", "week_start")),
        "s1_observations.parquet": parquet_summary(paths["s1"], ("field_id", "start_date")),
        "s2_observations.parquet": parquet_summary(paths["s2"], ("field_id", "start_date")),
    }
    prediction_info = files["crop_predictions.parquet"]
    failures = []
    if prediction_info["rows"] != expected_predictions:
        failures.append(
            f"prediction rows {prediction_info['rows']:,} != expected {expected_predictions:,}"
        )
    if prediction_info["fields"] != expected_fields:
        failures.append("prediction field count mismatch")
    if prediction_info["weeks"] != args.expected_weeks:
        failures.append("prediction week count mismatch")
    if prediction_info["duplicate_keys"]:
        failures.append("prediction duplicate keys detected")

    manifest = {
        "bundle_id": args.seed,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "model_run_id": args.model_run_id,
        "source_path": str(args.model_output_root),
        "date_from": args.date_from,
        "date_to": args.date_to,
        "field_count": expected_fields,
        "week_count": args.expected_weeks,
        "prediction_rows": expected_predictions,
        "selection": selection_audit,
        "files": files,
        "failures": failures,
        "status": "passed" if not failures else "failed",
    }
    manifest_path = args.output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    log(f"Bundle validation status: {manifest['status']}")
    log(f"Manifest: {manifest_path}")
    if failures:
        raise RuntimeError("; ".join(failures))


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr, flush=True)
        raise
