#!/usr/bin/env python3
"""Export canonical area-weighted county, crop and season yield statistics."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq
from psycopg.rows import dict_row


AGGREGATION_METHOD_VERSION = "county-yield-area-weighted-v1"


def database_url() -> str:
    return os.environ.get(
        "DATABASE_URL",
        "postgresql:///kenya_monitoring?host=/var/run/postgresql",
    ).replace("postgresql+psycopg://", "postgresql://", 1)


def repository_commit() -> str:
    completed = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=Path(__file__).resolve().parents[2],
        text=True,
        capture_output=True,
        check=False,
    )
    return completed.stdout.strip() or "unknown"


def aggregation_query() -> str:
    return """
        SELECT
            y.yield_run_id,
            y.model_run_id,
            y.season_code,
            d.season_name,
            d.season_start,
            d.season_end,
            y.county,
            y.crop_label,
            count(*)::integer AS calculated_fields,
            count(*) FILTER (
                WHERE y.yield_status = 'calculated_unvalidated'
            )::integer AS calculated_unvalidated_fields,
            count(*) FILTER (
                WHERE y.yield_status = 'calculated_unvalidated_intercrop_unknown'
            )::integer AS intercrop_unknown_fields,
            sum(y.area_ha) AS mapped_area_ha,
            sum(y.production_low_t) AS production_low_t,
            sum(y.production_central_t) AS production_central_t,
            sum(y.production_high_t) AS production_high_t,
            sum(y.production_low_t) / nullif(sum(y.area_ha), 0)
                AS yield_low_area_weighted_t_ha,
            sum(y.production_central_t) / nullif(sum(y.area_ha), 0)
                AS yield_central_area_weighted_t_ha,
            sum(y.production_high_t) / nullif(sum(y.area_ha), 0)
                AS yield_high_area_weighted_t_ha,
            avg(y.yield_central_t_ha) AS yield_plain_mean_t_ha,
            percentile_cont(0.5) WITHIN GROUP (ORDER BY y.yield_central_t_ha)
                AS yield_plain_median_t_ha,
            min(y.yield_central_t_ha) AS yield_min_t_ha,
            max(y.yield_central_t_ha) AS yield_max_t_ha,
            min(y.method_version) AS yield_method_version,
            min(y.parameter_set_id) AS parameter_set_id
        FROM dashboard.seasonal_yield AS y
        JOIN dashboard.season_definitions AS d
          ON d.model_run_id = y.model_run_id
         AND d.season_code = y.season_code
        WHERE y.yield_run_id = %(yield_run_id)s
          AND y.yield_central_t_ha IS NOT NULL
          AND y.area_ha > 0
        GROUP BY y.yield_run_id, y.model_run_id, y.season_code,
                 d.season_name, d.season_start, d.season_end,
                 y.county, y.crop_label
        ORDER BY d.season_start, y.county, y.crop_label
    """


def validate_export(rows: list[dict[str, object]], expected_seasons: int) -> dict[str, object]:
    failures: list[str] = []
    seasons = len({row["season_code"] for row in rows})
    if not rows:
        failures.append("aggregation export is empty")
    if seasons != expected_seasons:
        failures.append(f"seasons={seasons}; expected={expected_seasons}")
    keys = [
        (row["yield_run_id"], row["season_code"], row["county"], row["crop_label"])
        for row in rows
    ]
    duplicate_keys = len(keys) - len(set(keys))
    if duplicate_keys:
        failures.append(f"duplicate aggregation keys={duplicate_keys}")
    required_numeric = [
        "calculated_fields", "mapped_area_ha", "production_central_t",
        "yield_central_area_weighted_t_ha",
    ]
    null_required = sum(
        any(row.get(column) is None for column in required_numeric) for row in rows
    )
    if null_required:
        failures.append(f"rows with null required metrics={null_required}")
    differences = [
        abs(
            float(row["production_central_t"]) / float(row["mapped_area_ha"])
            - float(row["yield_central_area_weighted_t_ha"])
        )
        for row in rows
        if all(row.get(column) is not None for column in required_numeric)
    ]
    maximum_difference = max(differences, default=0.0)
    if maximum_difference > 1e-10:
        failures.append(f"area-weighted reconciliation difference={maximum_difference}")
    return {
        "rows": len(rows),
        "seasons": seasons,
        "counties": len({row["county"] for row in rows}),
        "crops": len({row["crop_label"] for row in rows}),
        "duplicate_keys": duplicate_keys,
        "maximum_reconciliation_difference": maximum_difference,
        "failures": failures,
        "status": "passed" if not failures else "failed",
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--yield-run-id", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--expected-seasons", type=int, default=16)
    parser.add_argument("--aggregation-code-commit")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    query = aggregation_query()
    with psycopg.connect(database_url(), row_factory=dict_row) as conn:
        records = [dict(row) for row in conn.execute(
            query, {"yield_run_id": args.yield_run_id}
        ).fetchall()]
    built_at = datetime.now(timezone.utc).isoformat()
    commit = args.aggregation_code_commit or repository_commit()
    query_hash = hashlib.sha256(query.encode()).hexdigest()
    for record in records:
        record["aggregation_method_version"] = AGGREGATION_METHOD_VERSION
        record["aggregation_code_commit"] = commit
        record["aggregation_query_sha256"] = query_hash
        record["aggregation_built_at"] = built_at
    report = validate_export(records, args.expected_seasons)
    if report["status"] != "passed":
        raise RuntimeError(f"Aggregation validation failed: {report['failures']}")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{args.yield_run_id}_county_crop_season_area_weighted"
    csv_path = args.output_dir / f"{stem}.csv"
    parquet_path = args.output_dir / f"{stem}.parquet"
    manifest_path = args.output_dir / f"{stem}.manifest.json"
    table = pa.Table.from_pylist(records)
    pacsv.write_csv(table, csv_path)
    pq.write_table(table, parquet_path, compression="zstd")
    manifest = {
        "yield_run_id": args.yield_run_id,
        "aggregation_method_version": AGGREGATION_METHOD_VERSION,
        "aggregation_code_commit": commit,
        "aggregation_query_sha256": query_hash,
        "aggregation_built_at": built_at,
        "scope": "Rows with non-null central yield and positive field area",
        "formula": "sum(production) / sum(mapped field area)",
        "validation": report,
        "files": [csv_path.name, parquet_path.name],
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
