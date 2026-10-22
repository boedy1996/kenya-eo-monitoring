from __future__ import annotations

from fastapi import APIRouter, Query, Request

from ..sql import active_model_run_id

router = APIRouter(tags=["catalog"])


@router.get("/catalog")
async def catalog(request: Request, model_run_id: str | None = None) -> dict[str, object]:
    async with request.app.state.db_pool.connection() as conn:
        run_id = await active_model_run_id(conn, model_run_id)
        run = await (
            await conn.execute(
                """
                SELECT model_run_id, date_from, date_to, field_count, week_count,
                       prediction_rows, metadata, loaded_at
                FROM dashboard.model_runs
                WHERE model_run_id = %s
                """,
                (run_id,),
            )
        ).fetchone()
        dimensions = await (
            await conn.execute(
                """
                SELECT
                    array_agg(DISTINCT county ORDER BY county) AS counties,
                    array_agg(DISTINCT label ORDER BY label) AS labels,
                    array_agg(DISTINCT reason ORDER BY reason) AS reasons,
                    array_agg(DISTINCT season ORDER BY season)
                        FILTER (WHERE season IS NOT NULL) AS seasons
                FROM dashboard.prediction_week_summary
                WHERE model_run_id = %s
                """,
                (run_id,),
            )
        ).fetchone()
    return {"model_run": run, **dimensions}


@router.get("/weeks")
async def weeks(
    request: Request,
    model_run_id: str | None = None,
    county: str | None = Query(default=None),
) -> dict[str, object]:
    async with request.app.state.db_pool.connection() as conn:
        run_id = await active_model_run_id(conn, model_run_id)
        params: list[object] = [run_id]
        county_clause = ""
        if county:
            county_clause = " AND county = %s"
            params.append(county)
        rows = await (
            await conn.execute(
                f"""
                SELECT week_start,
                       max(season) FILTER (WHERE season IS NOT NULL) AS season,
                       max(week_in_season) FILTER (WHERE week_in_season IS NOT NULL) AS week_in_season
                FROM dashboard.prediction_week_summary
                WHERE model_run_id = %s {county_clause}
                GROUP BY week_start
                ORDER BY week_start
                """,
                params,
            )
        ).fetchall()
    return {"model_run_id": run_id, "weeks": rows}


@router.get("/seasons")
async def seasons(
    request: Request,
    model_run_id: str | None = None,
) -> dict[str, object]:
    async with request.app.state.db_pool.connection() as conn:
        run_id = await active_model_run_id(conn, model_run_id)
        rows = await (
            await conn.execute(
                """
                SELECT d.season_code, d.season_year, d.season_kind, d.season_name,
                       d.season_start, d.season_end, d.is_complete, d.method_version,
                       (ys.season_code IS NOT NULL) AS yield_available,
                       yr.yield_run_id, yr.method_version AS yield_method_version
                FROM dashboard.season_definitions AS d
                LEFT JOIN dashboard.yield_runs AS yr
                  ON yr.model_run_id = d.model_run_id AND yr.is_active
                LEFT JOIN (
                    SELECT DISTINCT yield_run_id, season_code
                    FROM dashboard.seasonal_yield_summary
                ) AS ys
                  ON ys.yield_run_id = yr.yield_run_id
                 AND ys.season_code = d.season_code
                WHERE d.model_run_id = %s
                ORDER BY season_start
                """,
                (run_id,),
            )
        ).fetchall()
    return {"model_run_id": run_id, "seasons": rows}
