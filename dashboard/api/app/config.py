from __future__ import annotations

from functools import lru_cache

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file="dashboard/api/.env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "Kenya Crop Monitoring API"
    app_version: str = "0.1.0"
    database_url: str = "postgresql://127.0.0.1:5432/kenya_dashboard_dev"
    db_pool_min_size: int = Field(default=1, ge=1, le=20)
    db_pool_max_size: int = Field(default=10, ge=1, le=50)
    db_statement_timeout_ms: int = Field(default=15_000, ge=1_000, le=120_000)
    cors_origins: list[str] = ["http://127.0.0.1:3000", "http://localhost:3000"]

    @field_validator("database_url")
    @classmethod
    def normalize_database_url(cls, value: str) -> str:
        return value.replace("postgresql+psycopg://", "postgresql://", 1)

    @field_validator("cors_origins", mode="before")
    @classmethod
    def split_cors_origins(cls, value: object) -> object:
        if isinstance(value, str):
            return [part.strip() for part in value.split(",") if part.strip()]
        return value


@lru_cache
def get_settings() -> Settings:
    return Settings()
