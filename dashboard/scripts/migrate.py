#!/usr/bin/env python3
from __future__ import annotations

import os
from pathlib import Path

import psycopg


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_DIR = ROOT / "db" / "migrations"


def database_url() -> str:
    value = os.environ.get(
        "DATABASE_URL",
        "postgresql://127.0.0.1:5432/kenya_dashboard_dev",
    )
    return value.replace("postgresql+psycopg://", "postgresql://", 1)


def main() -> None:
    migrations = sorted(MIGRATION_DIR.glob("*.sql"))
    if not migrations:
        raise RuntimeError(f"No migrations found in {MIGRATION_DIR}")

    with psycopg.connect(database_url(), autocommit=True) as conn:
        with conn.cursor() as cursor:
            cursor.execute("CREATE SCHEMA IF NOT EXISTS dashboard")
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS dashboard.schema_migrations (
                    version text PRIMARY KEY,
                    applied_at timestamptz NOT NULL DEFAULT now()
                )
                """
            )
            cursor.execute("SELECT version FROM dashboard.schema_migrations")
            applied = {row[0] for row in cursor.fetchall()}

            for path in migrations:
                if path.name in applied:
                    print(f"Already applied: {path.name}")
                    continue
                print(f"Applying: {path.name}")
                with conn.transaction():
                    cursor.execute(path.read_text())
                    cursor.execute(
                        "INSERT INTO dashboard.schema_migrations(version) VALUES (%s)",
                        (path.name,),
                    )
    print("Database migrations are current.")


if __name__ == "__main__":
    main()
