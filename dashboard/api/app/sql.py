from __future__ import annotations

from datetime import date

from fastapi import HTTPException
from psycopg import AsyncConnection


async def active_model_run_id(conn: AsyncConnection, requested: str | None) -> str:
    if requested:
        row = await (
            await conn.execute(
                "SELECT model_run_id FROM dashboard.model_runs WHERE model_run_id = %s",
                (requested,),
            )
        ).fetchone()
    else:
        row = await (
            await conn.execute(
                """
                SELECT model_run_id
                FROM dashboard.model_runs
                WHERE is_active
                ORDER BY loaded_at DESC
                LIMIT 1
                """
            )
        ).fetchone()
    if not row:
        raise HTTPException(status_code=404, detail="No matching model run is loaded")
    return str(row["model_run_id"])


def validate_tile_coordinates(z: int, x: int, y: int) -> None:
    maximum = (1 << z) - 1
    if not 0 <= x <= maximum or not 0 <= y <= maximum:
        raise HTTPException(status_code=400, detail="Tile coordinates are outside the zoom grid")


def date_range_clause(
    date_from: date | None,
    date_to: date | None,
    *,
    column: str = "week_start",
) -> tuple[str, list[object]]:
    clauses: list[str] = []
    params: list[object] = []
    if date_from:
        clauses.append(f"{column} >= %s")
        params.append(date_from)
    if date_to:
        clauses.append(f"{column} <= %s")
        params.append(date_to)
    return (" AND ".join(clauses), params)
