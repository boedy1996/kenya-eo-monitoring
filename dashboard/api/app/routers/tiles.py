from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Query, Request, Response

from ..sql import active_model_run_id, validate_tile_coordinates

router = APIRouter(tags=["tiles"])


@router.get("/tiles/fields/{z}/{x}/{y}.mvt")
async def field_tile(
    z: int,
    x: int,
    y: int,
    request: Request,
    week: date,
    model_run_id: str | None = None,
    county: str | None = None,
    label: str | None = None,
    reason: str | None = None,
    min_confidence: float | None = Query(default=None, ge=0, le=1),
) -> Response:
    validate_tile_coordinates(z, x, y)
    geometry_column = "f.geom_simplified" if z <= 13 else "f.geom"
    spatial_column = "f.centroid" if z <= 10 else geometry_column
    clauses = [
        "p.model_run_id = %s",
        "p.week_start = %s",
        f"{spatial_column} && b.geom",
    ]
    async with request.app.state.db_pool.connection() as conn:
        run_id = await active_model_run_id(conn, model_run_id)
        filter_params: list[object] = [run_id, week]
        for column, value in (("p.county", county), ("p.label", label), ("p.reason", reason)):
            if value:
                clauses.append(f"{column} = %s")
                filter_params.append(value)
        if min_confidence is not None:
            clauses.append("p.confidence >= %s")
            filter_params.append(min_confidence)
        if z <= 10:
            grid_size = 40_000.0 / (2 ** max(z - 6, 0))
            params: list[object] = [z, x, y, *filter_params, grid_size]
            query = f"""
                WITH bounds AS (
                    SELECT ST_TileEnvelope(%s, %s, %s) AS geom
                ), clusters AS (
                    SELECT
                        row_number() OVER () AS feature_id,
                        p.county,
                        p.label,
                        p.reason,
                        count(*)::integer AS field_count,
                        round(avg(p.confidence)::numeric, 4)::double precision AS confidence,
                        round(avg(p.p_maize)::numeric, 4)::double precision AS p_maize,
                        round(avg(p.p_beans)::numeric, 4)::double precision AS p_beans,
                        round(avg(p.p_potatoes)::numeric, 4)::double precision AS p_potatoes,
                        ST_Centroid(ST_Collect(f.centroid)) AS cluster_geom
                    FROM dashboard.fields AS f
                    JOIN dashboard.crop_predictions AS p USING (field_id)
                    CROSS JOIN bounds AS b
                    WHERE {' AND '.join(clauses)}
                    GROUP BY p.county, p.label, p.reason,
                             ST_SnapToGrid(f.centroid, %s)
                ), tile_rows AS (
                    SELECT feature_id, county, label, reason, field_count,
                           confidence, p_maize, p_beans, p_potatoes,
                           ST_AsMVTGeom(cluster_geom, b.geom, 4096, 64, true) AS geom
                    FROM clusters
                    CROSS JOIN bounds AS b
                )
                SELECT COALESCE(
                    ST_AsMVT(tile_rows, 'fields', 4096, 'geom', 'feature_id'),
                    ''::bytea
                ) AS tile
                FROM tile_rows
            """
        else:
            params = [z, x, y, *filter_params]
            query = f"""
                WITH bounds AS (
                    SELECT ST_TileEnvelope(%s, %s, %s) AS geom
                ), tile_rows AS (
                    SELECT
                        f.feature_id,
                        f.field_id,
                        p.county,
                        p.label,
                        p.reason,
                        1::integer AS field_count,
                        round(p.confidence::numeric, 4)::double precision AS confidence,
                        round(p.p_maize::numeric, 4)::double precision AS p_maize,
                        round(p.p_beans::numeric, 4)::double precision AS p_beans,
                        round(p.p_potatoes::numeric, 4)::double precision AS p_potatoes,
                        ST_AsMVTGeom({geometry_column}, b.geom, 4096, 64, true) AS geom
                    FROM dashboard.fields AS f
                    JOIN dashboard.crop_predictions AS p USING (field_id)
                    CROSS JOIN bounds AS b
                    WHERE {' AND '.join(clauses)}
                )
                SELECT COALESCE(
                    ST_AsMVT(tile_rows, 'fields', 4096, 'geom', 'feature_id'),
                    ''::bytea
                ) AS tile
                FROM tile_rows
            """
        row = await (await conn.execute(query, params)).fetchone()
    return Response(
        content=bytes(row["tile"]),
        media_type="application/vnd.mapbox-vector-tile",
        headers={"Cache-Control": "public, max-age=300"},
    )


@router.get("/tiles/seasonal/{z}/{x}/{y}.mvt")
async def seasonal_field_tile(
    z: int,
    x: int,
    y: int,
    request: Request,
    season: str,
    model_run_id: str | None = None,
    county: str | None = None,
    label: str | None = None,
    status: str | None = None,
    min_confidence: float | None = Query(default=None, ge=0, le=1),
) -> Response:
    validate_tile_coordinates(z, x, y)
    geometry_column = "f.geom_simplified" if z <= 13 else "f.geom"
    spatial_column = "f.centroid" if z <= 10 else geometry_column
    clauses = [
        "p.model_run_id = %s",
        "p.season_code = %s",
        f"{spatial_column} && b.geom",
    ]
    async with request.app.state.db_pool.connection() as conn:
        run_id = await active_model_run_id(conn, model_run_id)
        filter_params: list[object] = [run_id, season]
        for column, value in (
            ("p.county", county),
            ("p.seasonal_label", label),
            ("p.status", status),
        ):
            if value:
                clauses.append(f"{column} = %s")
                filter_params.append(value)
        if min_confidence is not None:
            clauses.append("p.seasonal_confidence >= %s")
            filter_params.append(min_confidence)
        if z <= 10:
            grid_size = 40_000.0 / (2 ** max(z - 6, 0))
            params: list[object] = [z, x, y, *filter_params, grid_size]
            query = f"""
                WITH bounds AS (
                    SELECT ST_TileEnvelope(%s, %s, %s) AS geom
                ), clusters AS (
                    SELECT row_number() OVER () AS feature_id,
                           p.county,
                           p.seasonal_label AS label,
                           p.status AS reason,
                           count(*)::integer AS field_count,
                           round(avg(p.seasonal_confidence)::numeric, 4)::double precision AS confidence,
                           ST_Centroid(ST_Collect(f.centroid)) AS cluster_geom
                    FROM dashboard.fields AS f
                    JOIN dashboard.seasonal_predictions AS p USING (field_id)
                    CROSS JOIN bounds AS b
                    WHERE {' AND '.join(clauses)}
                    GROUP BY p.county, p.seasonal_label, p.status,
                             ST_SnapToGrid(f.centroid, %s)
                ), tile_rows AS (
                    SELECT feature_id, county, label, reason, field_count,
                           confidence,
                           ST_AsMVTGeom(cluster_geom, b.geom, 4096, 64, true) AS geom
                    FROM clusters
                    CROSS JOIN bounds AS b
                )
                SELECT COALESCE(
                    ST_AsMVT(tile_rows, 'fields', 4096, 'geom', 'feature_id'),
                    ''::bytea
                ) AS tile
                FROM tile_rows
            """
        else:
            params = [z, x, y, *filter_params]
            query = f"""
                WITH bounds AS (
                    SELECT ST_TileEnvelope(%s, %s, %s) AS geom
                ), tile_rows AS (
                    SELECT f.feature_id, f.field_id, p.county,
                           p.seasonal_label AS label,
                           p.status AS reason,
                           1::integer AS field_count,
                           round(p.seasonal_confidence::numeric, 4)::double precision AS confidence,
                           ST_AsMVTGeom({geometry_column}, b.geom, 4096, 64, true) AS geom
                    FROM dashboard.fields AS f
                    JOIN dashboard.seasonal_predictions AS p USING (field_id)
                    CROSS JOIN bounds AS b
                    WHERE {' AND '.join(clauses)}
                )
                SELECT COALESCE(
                    ST_AsMVT(tile_rows, 'fields', 4096, 'geom', 'feature_id'),
                    ''::bytea
                ) AS tile
                FROM tile_rows
            """
        row = await (await conn.execute(query, params)).fetchone()
    return Response(
        content=bytes(row["tile"]),
        media_type="application/vnd.mapbox-vector-tile",
        headers={"Cache-Control": "public, max-age=300"},
    )
