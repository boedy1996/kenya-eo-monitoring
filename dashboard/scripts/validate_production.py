#!/usr/bin/env python3
"""Validate a completed production dashboard load without rescanning all facts."""

from __future__ import annotations

import json
import os
import sys

import psycopg
from psycopg.rows import dict_row


def database_url() -> str:
    return os.environ.get(
        "DATABASE_URL",
        "postgresql:///kenya_monitoring?host=/var/run/postgresql",
    ).replace("postgresql+psycopg://", "postgresql://", 1)


def main() -> None:
    with psycopg.connect(database_url(), row_factory=dict_row) as conn:
        run = conn.execute(
            "SELECT * FROM dashboard.model_runs WHERE is_active ORDER BY loaded_at DESC LIMIT 1"
        ).fetchone()
        if not run:
            raise RuntimeError("No active dashboard model run")
        metrics = conn.execute(
            """
            SELECT
                (SELECT count(*) FROM dashboard.fields) AS fields,
                (SELECT count(*) FROM dashboard.fields WHERE NOT ST_IsValid(geom)) AS invalid_geometries,
                (SELECT count(*) FROM dashboard.load_checkpoints
                 WHERE model_run_id = %(run)s) AS source_files,
                (SELECT coalesce(sum(row_count), 0) FROM dashboard.load_checkpoints
                 WHERE model_run_id = %(run)s) AS checkpoint_rows,
                (SELECT count(DISTINCT week_start) FROM dashboard.prediction_week_summary
                 WHERE model_run_id = %(run)s) AS summary_weeks,
                (SELECT coalesce(sum(field_count), 0) FROM dashboard.prediction_week_summary
                 WHERE model_run_id = %(run)s) AS summary_rows,
                (SELECT count(*) FROM dashboard.prediction_week_summary
                 WHERE model_run_id = %(run)s) AS summary_groups
            """,
            {"run": run["model_run_id"]},
        ).fetchone()
        sample = conn.execute(
            """
            SELECT count(*) AS sampled_fields,
                   count(*) FILTER (WHERE prediction_rows > 0) AS with_predictions,
                   count(*) FILTER (WHERE s1_rows > 0) AS with_s1,
                   count(*) FILTER (WHERE s2_rows > 0) AS with_s2
            FROM (
                SELECT f.field_id,
                       (SELECT count(*) FROM dashboard.crop_predictions p
                        WHERE p.model_run_id = %(run)s AND p.field_id = f.field_id) AS prediction_rows,
                       (SELECT count(*) FROM timeseries.s1_indices s
                        WHERE s.field_id = f.field_id) AS s1_rows,
                       (SELECT count(*) FROM timeseries.s2_indices s
                        WHERE s.field_id = f.field_id) AS s2_rows
                FROM dashboard.fields f
                ORDER BY md5(f.field_id)
                LIMIT 100
            ) sampled
            """,
            {"run": run["model_run_id"]},
        ).fetchone()

    failures: list[str] = []
    comparisons = {
        "fields": run["field_count"],
        "checkpoint_rows": run["prediction_rows"],
        "summary_rows": run["prediction_rows"],
        "summary_weeks": run["week_count"],
        "invalid_geometries": 0,
    }
    for key, expected in comparisons.items():
        if metrics[key] != expected:
            failures.append(f"{key}: {metrics[key]} != {expected}")
    for key in ("with_predictions", "with_s1", "with_s2"):
        if sample[key] != sample["sampled_fields"]:
            failures.append(f"sample {key}: {sample[key]} != {sample['sampled_fields']}")

    report = {
        "model_run_id": run["model_run_id"],
        "metrics": dict(metrics),
        "sample": dict(sample),
        "failures": failures,
        "status": "passed" if not failures else "failed",
    }
    print(json.dumps(report, indent=2, default=str))
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
