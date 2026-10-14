from __future__ import annotations

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .config import get_settings
from .database import lifespan
from .routers import catalog, fields, summary, system, tiles

settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version=settings.app_version,
    lifespan=lifespan,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=False,
    allow_methods=["GET"],
    allow_headers=["*"],
)

prefix = "/api/v1"
app.include_router(system.router, prefix=prefix)
app.include_router(catalog.router, prefix=prefix)
app.include_router(summary.router, prefix=prefix)
app.include_router(fields.router, prefix=prefix)
app.include_router(tiles.router, prefix=prefix)


@app.get("/", include_in_schema=False)
async def root() -> dict[str, str]:
    return {"service": settings.app_name, "docs": "/docs"}
