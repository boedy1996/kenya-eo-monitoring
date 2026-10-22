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


@router.get("/fields/{field_id}/seasonal")
async def field_seasonal_detail(
    field_id: str,
    request: Request,
    season: str,
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
                       p.season_code, p.season_year, p.season_kind,
                       p.season_start, p.season_end,
                       p.seasonal_label, p.leading_label,
                       p.seasonal_confidence, p.status,
                       p.total_weeks, p.classified_weeks,
                       p.appearance_count, p.vote_share,
                       p.median_confidence, p.maximum_confidence,
                       p.runner_up_label, p.runner_up_confidence,
                       p.confidence_margin, p.method_version,
                       yr.yield_run_id,
                       y.yield_status, y.qa_flags AS yield_qa_flags,
                       y.area_ha AS yield_area_ha, y.area_method,
                       y.intercrop_status, y.survey_crop_text, y.survey_crop_match,
                       y.sos, y.peak_date, y.eos,
                       y.phenology_method, y.phenology_confidence,
                       y.phenology_qa_status,
                       y.optical_valid_weeks, y.sar_supported_weeks,
                       y.seasonal_npp_gc_m2 AS "seasonal_npp_gC_m2",
                       y.npp_temporal_coverage,
                       y.npp_mean_spatial_coverage, y.npp_mean_effective_pixels,
                       y.tbp_tdm_ha AS "tbp_tDM_ha",
                       y.yield_low_t_ha, y.yield_central_t_ha, y.yield_high_t_ha,
                       y.production_low_t, y.production_central_t, y.production_high_t,
                       y.phenology_narrow_central_yield_t_ha,
                       y.phenology_wide_central_yield_t_ha,
                       y.parameter_set_id AS yield_parameter_set_id,
                       y.method_version AS yield_method_version
                FROM dashboard.fields AS f
                LEFT JOIN dashboard.seasonal_predictions AS p
                  ON p.field_id = f.field_id
                 AND p.model_run_id = %s
                 AND p.season_code = %s
                LEFT JOIN dashboard.yield_runs AS yr
                  ON yr.model_run_id = %s AND yr.is_active
                LEFT JOIN dashboard.seasonal_yield AS y
                  ON y.yield_run_id = yr.yield_run_id
                 AND y.field_id = f.field_id
                 AND y.season_code = %s
                WHERE f.field_id = %s
                """,
                (run_id, season, run_id, season, field_id),
            )
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="Field not found")
    return {"model_run_id": run_id, "field": row}


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
