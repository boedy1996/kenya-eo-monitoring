from __future__ import annotations

from datetime import date

from fastapi import APIRouter, HTTPException, Query, Request

from ..sql import active_model_run_id, date_range_clause

router = APIRouter(tags=["fields"])


@router.get("/search/fields")
async def search_fields(
    request: Request,
    q: str = Query(min_length=1, max_length=30),
    limit: int = Query(default=20, ge=1, le=50),
) -> dict[str, object]:
    async with request.app.state.db_pool.connection() as conn:
        rows = await (
            await conn.execute(
                """
                SELECT field_id, county, area_ha
                FROM dashboard.fields
                WHERE field_id ILIKE %s
                ORDER BY CASE WHEN lower(field_id) = lower(%s) THEN 0 ELSE 1 END,
                         field_id
                LIMIT %s
                """,
                (f"{q}%", q, limit),
            )
        ).fetchall()
    return {"query": q, "fields": rows}


@router.get("/fields/{field_id}")
async def field_detail(
    field_id: str,
    request: Request,
    week: date | None = None,
    model_run_id: str | None = None,
) -> dict[str, object]:
    async with request.app.state.db_pool.connection() as conn:
        run_id = await active_model_run_id(conn, model_run_id)
        row = await (
            await conn.execute(
                """
                SELECT f.field_id, f.county, f.area_ha, f.source_layer,
                       ST_X(ST_Transform(f.centroid, 4326)) AS longitude,
                       ST_Y(ST_Transform(f.centroid, 4326)) AS latitude,
                       p.week_start, p.season, p.week_in_season, p.scorable,
                       p.label, p.reason, p.confidence,
                       p.p_maize, p.p_beans, p.p_potatoes
                FROM dashboard.fields AS f
                LEFT JOIN LATERAL (
                    SELECT *
                    FROM dashboard.crop_predictions
                    WHERE model_run_id = %s
                      AND field_id = f.field_id
                      AND (%s::date IS NULL OR week_start = %s::date)
                    ORDER BY week_start DESC
                    LIMIT 1
                ) AS p ON true
                WHERE f.field_id = %s
                """,
                (run_id, week, week, field_id),
            )
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Field not found")
    return {"model_run_id": run_id, "field": row}


@router.get("/fields/{field_id}/predictions")
async def field_predictions(
    field_id: str,
    request: Request,
    model_run_id: str | None = None,
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, object]:
    async with request.app.state.db_pool.connection() as conn:
        run_id = await active_model_run_id(conn, model_run_id)
        clauses = ["model_run_id = %s", "field_id = %s"]
        params: list[object] = [run_id, field_id]
        range_sql, range_params = date_range_clause(date_from, date_to)
        if range_sql:
            clauses.append(range_sql)
            params.extend(range_params)
        rows = await (
            await conn.execute(
                f"""
                SELECT week_start, county, season, week_in_season, scorable,
                       label, reason, confidence, p_maize, p_beans, p_potatoes
                FROM dashboard.crop_predictions
                WHERE {' AND '.join(clauses)}
                ORDER BY week_start
                LIMIT 600
                """,
                params,
            )
        ).fetchall()
    return {"model_run_id": run_id, "field_id": field_id, "predictions": rows}


@router.get("/fields/{field_id}/signals")
async def field_signals(
    field_id: str,
    request: Request,
    date_from: date | None = None,
    date_to: date | None = None,
) -> dict[str, object]:
    range_sql, range_params = date_range_clause(date_from, date_to, column="start_date")
    clauses = ["field_id = %s"]
    if range_sql:
        clauses.append(range_sql)
    params: list[object] = [field_id, *range_params]
    async with request.app.state.db_pool.connection() as conn:
        s1 = await (
            await conn.execute(
                f"""
                SELECT start_date, buffer_width_m, pixel_count_before,
                       pixel_count_after, below_min_pixel_threshold,
                       sigma_vv_linear_mean, sigma_vh_linear_mean,
                       p_ratio_mean, rvi_mean, rcspr_mean
                FROM timeseries.s1_indices
                WHERE {' AND '.join(clauses)}
                ORDER BY start_date
                LIMIT 600
                """,
                params,
            )
        ).fetchall()
        s2 = await (
            await conn.execute(
                f"""
                SELECT start_date, buffer_width_m, pixel_count_before,
                       pixel_count_after, below_min_pixel_threshold,
                       ndvi_mean, evi_mean, ndmi_mean, ndre_mean,
                       msi_mean, pvr_mean, lai_mean
                FROM timeseries.s2_indices
                WHERE {' AND '.join(clauses)}
                ORDER BY start_date
                LIMIT 600
                """,
                params,
            )
        ).fetchall()
    return {"field_id": field_id, "s1": s1, "s2": s2}
