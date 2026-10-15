from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Query, Request

from ..sql import active_model_run_id

router = APIRouter(tags=["summary"])


@router.get("/summary")
async def summary(
    request: Request,
    week: date,
    model_run_id: str | None = None,
    county: str | None = None,
    season: str | None = None,
    week_in_season: int | None = Query(default=None, ge=1, le=53),
    label: str | None = None,
    reason: str | None = None,
    min_confidence: float | None = Query(default=None, ge=0, le=1),
) -> dict[str, object]:
    async with request.app.state.db_pool.connection() as conn:
        run_id = await active_model_run_id(conn, model_run_id)
        clauses = ["model_run_id = %s", "week_start = %s"]
        params: list[object] = [run_id, week]
        for column, value in (
            ("county", county),
            ("season", season),
            ("week_in_season", week_in_season),
            ("label", label),
            ("reason", reason),
        ):
            if value is not None:
                clauses.append(f"{column} = %s")
                params.append(value)
        if min_confidence is None or min_confidence <= 0:
            query = f"""
                SELECT county, season, week_in_season, label, reason,
                       field_count, mean_confidence
                FROM dashboard.prediction_week_summary
                WHERE {' AND '.join(clauses)}
                ORDER BY county, label, reason
            """
        else:
            clauses.append("confidence >= %s")
            params.append(min_confidence)
            query = f"""
                SELECT county, season, week_in_season, label, reason,
                       count(*)::integer AS field_count,
                       avg(confidence) AS mean_confidence
                FROM dashboard.crop_predictions
                WHERE {' AND '.join(clauses)}
                GROUP BY county, season, week_in_season, label, reason
                ORDER BY county, label, reason
            """
        rows = await (await conn.execute(query, params)).fetchall()
    return {"model_run_id": run_id, "week": week, "groups": rows}


@router.get("/summary/seasonal")
async def seasonal_summary(
    request: Request,
    season: str,
    model_run_id: str | None = None,
    county: str | None = None,
    label: str | None = None,
    status: str | None = None,
    min_confidence: float | None = Query(default=None, ge=0, le=1),
) -> dict[str, object]:
    async with request.app.state.db_pool.connection() as conn:
        run_id = await active_model_run_id(conn, model_run_id)
        clauses = ["model_run_id = %s", "season_code = %s"]
        params: list[object] = [run_id, season]
        for column, value in (("county", county), ("seasonal_label", label), ("status", status)):
            if value:
                clauses.append(f"{column} = %s")
                params.append(value)
        if min_confidence is None or min_confidence <= 0:
            summary_clauses = [clause.replace("seasonal_label", "label") for clause in clauses]
            rows = await (
                await conn.execute(
                    f"""
                    SELECT county, label, status, field_count, mean_confidence
                    FROM dashboard.seasonal_prediction_summary
                    WHERE {' AND '.join(summary_clauses)}
                    ORDER BY county, label, status
                    """,
                    params,
                )
            ).fetchall()
        else:
            clauses.append("seasonal_confidence >= %s")
            params.append(min_confidence)
            rows = await (
                await conn.execute(
                    f"""
                    SELECT county, seasonal_label AS label, status,
                           count(*)::integer AS field_count,
                           avg(seasonal_confidence) AS mean_confidence
                    FROM dashboard.seasonal_predictions
                    WHERE {' AND '.join(clauses)}
                    GROUP BY county, seasonal_label, status
                    ORDER BY county, seasonal_label, status
                    """,
                    params,
                )
            ).fetchall()
    return {"model_run_id": run_id, "season": season, "groups": rows}
