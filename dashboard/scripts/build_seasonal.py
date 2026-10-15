#!/usr/bin/env python3
"""Build resumable field-level Long Rains and Short Rains predictions."""

from __future__ import annotations

import argparse
import os
import time
from dataclasses import dataclass
from datetime import date, datetime

import psycopg
from psycopg.rows import dict_row


@dataclass(frozen=True)
class Season:
    code: str
    year: int
    kind: str
    name: str
    start: date
    end: date


def log(message: str) -> None:
    print(f"[{datetime.now().astimezone().isoformat(timespec='seconds')}] {message}", flush=True)


def database_url() -> str:
    return os.environ.get(
        "DATABASE_URL",
        "postgresql:///kenya_monitoring?host=/var/run/postgresql",
    ).replace("postgresql+psycopg://", "postgresql://", 1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model-run-id")
    parser.add_argument("--minimum-classified-weeks", type=int, default=3)
    parser.add_argument("--near-tie", type=float, default=0.05)
    parser.add_argument("--median-tie", type=float, default=0.01)
    parser.add_argument("--method-version", default="seasonal-top3-v1")
    parser.add_argument(
        "--work-mem",
        default="512MB",
        help="PostgreSQL work_mem for this builder session only (default: 512MB)",
    )
    parser.add_argument("--rebuild", action="store_true")
    return parser.parse_args()


def seasons_between(date_from: date, date_to: date) -> list[Season]:
    result: list[Season] = []
    for year in range(date_from.year, date_to.year + 1):
        for kind, name, start, end in (
            ("lr", "Long Rains", date(year, 3, 1), date(year, 5, 31)),
            ("sr", "Short Rains", date(year, 10, 1), date(year, 12, 31)),
        ):
            if end >= date_from and start <= date_to:
                result.append(Season(f"{year}_{kind}", year, kind, name, start, end))
    return result


INSERT_SEASON_SQL = """
WITH season_rows AS MATERIALIZED (
    SELECT field_id, county, week_start, label, confidence, scorable, reason
    FROM dashboard.crop_predictions
    WHERE model_run_id = %(run)s
      AND week_start BETWEEN %(start)s AND %(end)s
),
field_totals AS MATERIALIZED (
    SELECT field_id,
           max(county) AS county,
           count(*)::smallint AS total_weeks,
           count(*) FILTER (
               WHERE scorable
                 AND reason = 'classified'
                 AND label IN ('maize', 'beans', 'potatoes')
                 AND confidence IS NOT NULL
           )::smallint AS classified_weeks
    FROM season_rows
    GROUP BY field_id
),
eligible AS MATERIALIZED (
    SELECT field_id, county, label, confidence
    FROM season_rows
    WHERE scorable
      AND reason = 'classified'
      AND label IN ('maize', 'beans', 'potatoes')
      AND confidence IS NOT NULL
),
crop_values AS MATERIALIZED (
    SELECT field_id,
           max(county) AS county,
           label,
           count(*)::smallint AS appearance_count,
           array_agg(confidence ORDER BY confidence DESC) AS confidence_values
    FROM eligible
    GROUP BY field_id, label
),
crop_stats AS MATERIALIZED (
    SELECT field_id,
           county,
           label,
           appearance_count,
           (SELECT avg(value) FROM unnest(confidence_values[1:3]) AS value)
               AS top3_confidence,
           CASE
               WHEN appearance_count %% 2 = 1
                   THEN confidence_values[(appearance_count + 1) / 2]
               ELSE (
                   confidence_values[appearance_count / 2]
                   + confidence_values[appearance_count / 2 + 1]
               ) / 2.0
           END AS median_confidence,
           confidence_values[1] AS maximum_confidence
    FROM crop_values
),
strength AS MATERIALIZED (
    SELECT crop_stats.*,
           max(top3_confidence) OVER (PARTITION BY field_id) AS best_confidence
    FROM crop_stats
),
candidates AS MATERIALIZED (
    SELECT strength.*,
           row_number() OVER (
               PARTITION BY field_id
               ORDER BY appearance_count DESC,
                        median_confidence DESC,
                        top3_confidence DESC,
                        label
           ) AS choice_rank
    FROM strength
    WHERE top3_confidence >= best_confidence - %(near_tie)s
),
winner AS MATERIALIZED (
    SELECT first.field_id,
           first.label AS leading_label,
           first.appearance_count,
           first.top3_confidence,
           first.median_confidence,
           first.maximum_confidence,
           second.label AS runner_up_label,
           second.top3_confidence AS runner_up_confidence,
           CASE
               WHEN second.label IS NOT NULL
               THEN first.top3_confidence - second.top3_confidence
           END AS confidence_margin,
           CASE
               WHEN second.label IS NOT NULL
                AND first.appearance_count = second.appearance_count
                AND abs(first.median_confidence - second.median_confidence) <= %(median_tie)s
               THEN true ELSE false
           END AS is_ambiguous
    FROM candidates AS first
    LEFT JOIN candidates AS second
      ON second.field_id = first.field_id
     AND second.choice_rank = 2
    WHERE first.choice_rank = 1
)
INSERT INTO dashboard.seasonal_predictions (
    model_run_id, field_id, county, season_code, season_year, season_kind,
    season_start, season_end, seasonal_label, leading_label,
    seasonal_confidence, status, total_weeks, classified_weeks,
    appearance_count, vote_share, median_confidence, maximum_confidence,
    runner_up_label, runner_up_confidence, confidence_margin, method_version
)
SELECT %(run)s,
       totals.field_id,
       totals.county,
       %(season_code)s,
       %(season_year)s,
       %(season_kind)s,
       %(start)s,
       %(end)s,
       CASE
           WHEN totals.classified_weeks < %(minimum_weeks)s OR winner.field_id IS NULL
               THEN 'Unclassified'
           WHEN winner.is_ambiguous THEN 'Unclassified'
           ELSE winner.leading_label
       END,
       winner.leading_label,
       CASE
           WHEN totals.classified_weeks < %(minimum_weeks)s OR winner.is_ambiguous
               THEN NULL
           ELSE winner.top3_confidence
       END,
       CASE
           WHEN totals.classified_weeks < %(minimum_weeks)s OR winner.field_id IS NULL
               THEN 'insufficient_data'
           WHEN winner.is_ambiguous THEN 'ambiguous'
           WHEN %(end)s > %(model_date_to)s THEN 'provisional'
           ELSE 'final'
       END,
       totals.total_weeks,
       totals.classified_weeks,
       winner.appearance_count,
       winner.appearance_count::double precision / NULLIF(totals.classified_weeks, 0),
       winner.median_confidence,
       winner.maximum_confidence,
       winner.runner_up_label,
       winner.runner_up_confidence,
       winner.confidence_margin,
       %(method_version)s
FROM field_totals AS totals
LEFT JOIN winner USING (field_id)
"""


def main() -> None:
    args = parse_args()
    if args.minimum_classified_weeks < 1:
        raise ValueError("--minimum-classified-weeks must be positive")
    if not 0 <= args.near_tie <= 1 or not 0 <= args.median_tie <= 1:
        raise ValueError("tie thresholds must be between zero and one")

    with psycopg.connect(database_url(), autocommit=True, row_factory=dict_row) as conn:
        configured_work_mem = conn.execute(
            "SELECT set_config('work_mem', %s, false) AS work_mem",
            (args.work_mem,),
        ).fetchone()["work_mem"]
        log(f"PostgreSQL session work_mem: {configured_work_mem}")
        if args.model_run_id:
            run = conn.execute(
                "SELECT * FROM dashboard.model_runs WHERE model_run_id = %s",
                (args.model_run_id,),
            ).fetchone()
        else:
            run = conn.execute(
                "SELECT * FROM dashboard.model_runs WHERE is_active ORDER BY loaded_at DESC LIMIT 1"
            ).fetchone()
        if not run:
            raise RuntimeError("Dashboard model run not found")

        seasons = seasons_between(run["date_from"], run["date_to"])
        log(
            f"Building {len(seasons)} seasons for {run['model_run_id']} "
            f"({run['field_count']:,} fields)"
        )
        for number, season in enumerate(seasons, start=1):
            checkpoint = conn.execute(
                """
                SELECT method_version, row_count
                FROM dashboard.seasonal_build_checkpoints
                WHERE model_run_id = %s AND season_code = %s
                """,
                (run["model_run_id"], season.code),
            ).fetchone()
            if (
                checkpoint
                and not args.rebuild
                and checkpoint["method_version"] == args.method_version
                and checkpoint["row_count"] == run["field_count"]
            ):
                log(f"[{number}/{len(seasons)}] checkpoint exists; skipping {season.code}")
                continue

            started = time.monotonic()
            log(f"[{number}/{len(seasons)}] building {season.code}")
            params = {
                "run": run["model_run_id"],
                "season_code": season.code,
                "season_year": season.year,
                "season_kind": season.kind,
                "start": season.start,
                "end": season.end,
                "model_date_to": run["date_to"],
                "minimum_weeks": args.minimum_classified_weeks,
                "near_tie": args.near_tie,
                "median_tie": args.median_tie,
                "method_version": args.method_version,
            }
            with conn.transaction():
                conn.execute(
                    "DELETE FROM dashboard.seasonal_predictions WHERE model_run_id = %s AND season_code = %s",
                    (run["model_run_id"], season.code),
                )
                conn.execute(INSERT_SEASON_SQL, params)
                row_count = conn.execute(
                    """
                    SELECT count(*) FROM dashboard.seasonal_predictions
                    WHERE model_run_id = %s AND season_code = %s
                    """,
                    (run["model_run_id"], season.code),
                ).fetchone()["count"]
                if row_count != run["field_count"]:
                    raise RuntimeError(
                        f"{season.code} produced {row_count:,} rows; expected {run['field_count']:,}"
                    )
                conn.execute(
                    """
                    INSERT INTO dashboard.season_definitions (
                        model_run_id, season_code, season_year, season_kind,
                        season_name, season_start, season_end, is_complete,
                        method_version, built_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, now())
                    ON CONFLICT (model_run_id, season_code) DO UPDATE SET
                        season_year = EXCLUDED.season_year,
                        season_kind = EXCLUDED.season_kind,
                        season_name = EXCLUDED.season_name,
                        season_start = EXCLUDED.season_start,
                        season_end = EXCLUDED.season_end,
                        is_complete = EXCLUDED.is_complete,
                        method_version = EXCLUDED.method_version,
                        built_at = now()
                    """,
                    (
                        run["model_run_id"], season.code, season.year, season.kind,
                        season.name, season.start, season.end,
                        season.end <= run["date_to"], args.method_version,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO dashboard.seasonal_build_checkpoints (
                        model_run_id, season_code, method_version, row_count, built_at
                    ) VALUES (%s, %s, %s, %s, now())
                    ON CONFLICT (model_run_id, season_code) DO UPDATE SET
                        method_version = EXCLUDED.method_version,
                        row_count = EXCLUDED.row_count,
                        built_at = now()
                    """,
                    (run["model_run_id"], season.code, args.method_version, row_count),
                )
            log(
                f"[{number}/{len(seasons)}] built {row_count:,} rows for {season.code} "
                f"in {time.monotonic() - started:.1f}s"
            )

        log("Rebuilding seasonal summary")
        with conn.transaction():
            conn.execute(
                "DELETE FROM dashboard.seasonal_prediction_summary WHERE model_run_id = %s",
                (run["model_run_id"],),
            )
            conn.execute(
                """
                INSERT INTO dashboard.seasonal_prediction_summary (
                    model_run_id, season_code, county, label, status,
                    field_count, mean_confidence
                )
                SELECT model_run_id, season_code, county, seasonal_label, status,
                       count(*)::integer, avg(seasonal_confidence)
                FROM dashboard.seasonal_predictions
                WHERE model_run_id = %s
                GROUP BY model_run_id, season_code, county, seasonal_label, status
                """,
                (run["model_run_id"],),
            )
        conn.execute("ANALYZE dashboard.seasonal_predictions")
        conn.execute("ANALYZE dashboard.seasonal_prediction_summary")
        log("Seasonal prediction build complete")


if __name__ == "__main__":
    main()
