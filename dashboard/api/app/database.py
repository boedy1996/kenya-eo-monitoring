from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI, Request
from psycopg import AsyncConnection
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from .config import get_settings


def build_pool() -> AsyncConnectionPool:
    settings = get_settings()
    return AsyncConnectionPool(
        conninfo=settings.database_url,
        min_size=settings.db_pool_min_size,
        max_size=settings.db_pool_max_size,
        open=False,
        kwargs={
            "autocommit": True,
            "row_factory": dict_row,
            "options": f"-c statement_timeout={settings.db_statement_timeout_ms}",
        },
    )


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    pool = build_pool()
    await pool.open(wait=True, timeout=10)
    app.state.db_pool = pool
    try:
        yield
    finally:
        await pool.close()


@asynccontextmanager
async def connection(request: Request) -> AsyncIterator[AsyncConnection]:
    async with request.app.state.db_pool.connection() as conn:
        yield conn
