from __future__ import annotations

from fastapi import APIRouter, Request

from ..config import get_settings

router = APIRouter(tags=["system"])


@router.get("/health")
async def health(request: Request) -> dict[str, object]:
    async with request.app.state.db_pool.connection() as conn:
        row = await (
            await conn.execute(
                "SELECT current_database() AS database, postgis_lib_version() AS postgis"
            )
        ).fetchone()
    settings = get_settings()
    return {
        "status": "ok",
        "service": settings.app_name,
        "version": settings.app_version,
        "database": row["database"],
        "postgis": row["postgis"],
    }
