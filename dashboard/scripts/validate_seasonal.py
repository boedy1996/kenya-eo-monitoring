#!/usr/bin/env python3
"""Validate seasonal predictions for the active dashboard model run."""

from __future__ import annotations

import json
import os
import sys
from datetime import date, timedelta

import psycopg
from psycopg.rows import dict_row


def database_url() -> str:
    return os.environ.get(
        "DATABASE_URL",
        "postgresql:///kenya_monitoring?host=/var/run/postgresql",
    ).replace("postgresql+psycopg://", "postgresql://", 1)


def expected_season_count(date_from: date, date_to: date) -> int:
    count = 0
    for year in range(date_from.year, date_to.year + 1):
        for start, end in (
            (date(year, 3, 1), date(year, 5, 31)),
            (date(year, 10, 1), date(year, 12, 31)),
        ):
            count += int(end >= date_from and start <= date_to)
    return count


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
                (SELECT count(*) FROM dashboard.season_definitions
                 WHERE model_run_id = %(run)s) AS seasons,
                (SELECT count(*) FROM dashboard.seasonal_build_checkpoints
                 WHERE model_run_id = %(run)s) AS checkpoints,
                (SELECT count(*) FROM dashboard.seasonal_predictions
                 WHERE model_run_id = %(run)s) AS prediction_rows,
                (SELECT count(DISTINCT field_id) FROM dashboard.seasonal_predictions
                 WHERE model_run_id = %(run)s) AS fields,
                (SELECT coalesce(sum(field_count), 0)
                 FROM dashboard.seasonal_prediction_summary
                 WHERE model_run_id = %(run)s) AS summary_rows,
                (SELECT count(*) FROM dashboard.seasonal_predictions
                 WHERE model_run_id = %(run)s
                   AND (
                     status NOT IN ('final', 'provisional', 'ambiguous', 'insufficient_data')
                     OR (status IN ('final', 'provisional') AND seasonal_label NOT IN ('maize', 'beans', 'potatoes'))
                     OR (status IN ('ambiguous', 'insufficient_data') AND seasonal_label <> 'Unclassified')
                     OR (status IN ('final', 'provisional') AND seasonal_confidence IS NULL)
                     OR (status IN ('ambiguous', 'insufficient_data') AND seasonal_confidence IS NOT NULL)
                   )) AS invalid_results,
                (SELECT count(*) FROM dashboard.seasonal_predictions
                 WHERE model_run_id = %(run)s
                   AND classified_weeks < 3
                   AND status <> 'insufficient_data') AS invalid_minimum_weeks,
                (SELECT count(*)
                 FROM dashboard.season_definitions
                 WHERE model_run_id = %(run)s
                   AND is_complete <> (season_end <= %(coverage_end)s)) AS invalid_completion_flags
            """,
            {
                "run": run["model_run_id"],
                "coverage_end": run["date_to"] + timedelta(days=6),
            },
        ).fetchone()

        status_counts = conn.execute(
            """
            SELECT status, seasonal_label AS label, count(*) AS fields
            FROM dashboard.seasonal_predictions
            WHERE model_run_id = %s
            GROUP BY status, seasonal_label
            ORDER BY status, seasonal_label
            """,
            (run["model_run_id"],),
        ).fetchall()

    expected_seasons = expected_season_count(
        run["date_from"], run["date_to"] + timedelta(days=6)
    )
    expected_rows = run["field_count"] * expected_seasons
    failures: list[str] = []
    comparisons = {
        "seasons": expected_seasons,
        "checkpoints": expected_seasons,
        "prediction_rows": expected_rows,
        "fields": run["field_count"],
        "summary_rows": expected_rows,
        "invalid_results": 0,
        "invalid_minimum_weeks": 0,
        "invalid_completion_flags": 0,
    }
    for key, expected in comparisons.items():
        if metrics[key] != expected:
            failures.append(f"{key}: {metrics[key]} != {expected}")

    report = {
        "model_run_id": run["model_run_id"],
        "method": {
            "minimum_classified_weeks": 3,
            "primary_strength": "mean of top 3 weekly confidences per crop",
            "near_tie_threshold": 0.05,
            "tie_breakers": ["appearance frequency", "median confidence"],
        },
        "metrics": dict(metrics),
        "status_counts": [dict(row) for row in status_counts],
        "failures": failures,
        "status": "passed" if not failures else "failed",
    }
    print(json.dumps(report, indent=2, default=str))
    if failures:
        sys.exit(1)


if __name__ == "__main__":
    main()
