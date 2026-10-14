#!/usr/bin/env python3
from __future__ import annotations

import json
import os
import sys

import psycopg
from psycopg.rows import dict_row


def database_url() -> str:
    return os.environ.get(
        "DATABASE_URL",
        "postgresql://127.0.0.1:5432/kenya_dashboard_dev",
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
                (SELECT count(*) FROM dashboard.crop_predictions
                 WHERE model_run_id = %(run)s) AS prediction_rows,
                (SELECT count(DISTINCT week_start) FROM dashboard.crop_predictions
                 WHERE model_run_id = %(run)s) AS weeks,
                (SELECT count(*) - count(DISTINCT (field_id, week_start))
                 FROM dashboard.crop_predictions WHERE model_run_id = %(run)s) AS duplicate_keys,
                (SELECT count(*) FROM timeseries.s1_indices) AS s1_rows,
                (SELECT count(*) FROM timeseries.s2_indices) AS s2_rows,
                (SELECT count(*) FROM dashboard.prediction_week_summary) AS summary_rows,
                (SELECT count(*) FROM dashboard.fields WHERE NOT ST_IsValid(geom)) AS invalid_geometries
            """,
            {"run": run["model_run_id"]},
        ).fetchone()
        failures = []
        for key, expected in (
            ("fields", run["field_count"]),
            ("prediction_rows", run["prediction_rows"]),
            ("weeks", run["week_count"]),
            ("duplicate_keys", 0),
            ("invalid_geometries", 0),
        ):
            if metrics[key] != expected:
                failures.append(f"{key}: {metrics[key]} != {expected}")
        report = {
            "model_run_id": run["model_run_id"],
            "metrics": dict(metrics),
            "failures": failures,
            "status": "passed" if not failures else "failed",
        }
        print(json.dumps(report, indent=2, default=str))
        if failures:
            sys.exit(1)


if __name__ == "__main__":
    main()
