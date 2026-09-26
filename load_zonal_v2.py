#!/usr/bin/env python3
"""Load validated zonal Parquet parts into versioned PostgreSQL tables.

This server uses PostgreSQL peer authentication. The loader streams SQL through
``sudo -u postgres psql`` instead of requiring a database password. Weekly
Parquet parts are converted to one temporary CSV at a time; CSV COPY and its
checkpoint are committed in the same transaction.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shlex
import subprocess
import tempfile
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import pyarrow.parquet as pq

from zonal_center_pipeline import (
    BANDS,
    PIPELINE_VERSION,
    SENSORS,
    RasterRecord,
    atomic_write_json,
    expected_output_columns,
    part_path,
)


LOADER_VERSION = "1.0.0"
IDENTIFIER_RE = re.compile(r"^[a-z_][a-z0-9_]*$")


def log(message: str) -> None:
    stamp = datetime.now().astimezone().isoformat(timespec="seconds")
    print(f"[{stamp}] {message}", flush=True)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def checked_identifier(value: str) -> str:
    if not IDENTIFIER_RE.fullmatch(value):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


def run_psql(args: argparse.Namespace, statement: str, tuples_only: bool = False) -> str:
    command = shlex.split(args.psql_command) + [
        "-X", "-v", "ON_ERROR_STOP=1", "-d", args.database,
    ]
    if tuples_only:
        command.extend(["-A", "-t", "-F", "\t"])
    result = subprocess.run(command, input=statement, text=True, capture_output=True)
    if result.returncode:
        raise RuntimeError(
            f"psql failed ({result.returncode})\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return result.stdout.strip()


def load_run(run_dir: Path) -> tuple[dict[str, Any], pd.DataFrame]:
    manifest_path = run_dir / "run_manifest.json"
    validation_path = run_dir / "validation_report.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"Missing run manifest: {manifest_path}")
    if not validation_path.exists():
        raise FileNotFoundError(f"Run has not been validated: {validation_path}")
    manifest = json.loads(manifest_path.read_text())
    validation = json.loads(validation_path.read_text())
    if validation.get("status") != "passed":
        raise ValueError(f"Zonal validation has not passed: {validation_path}")
    inventory = pd.read_parquet(run_dir / "manifest" / "rasters.parquet")
    return manifest, inventory


def version_token(run_dir: Path) -> str:
    clean = re.sub(r"[^a-z0-9]+", "_", run_dir.name.lower()).strip("_")[:24]
    digest = hashlib.sha256(str(run_dir.resolve()).encode()).hexdigest()[:8]
    return f"z_{clean}_{digest}" if clean else f"z_{digest}"


def versioned_tables(run_dir: Path) -> dict[str, str]:
    token = version_token(run_dir)
    return {sensor: checked_identifier(f"{sensor}_indices__{token}") for sensor in SENSORS}


def table_columns_sql(sensor: str) -> str:
    definitions = [
        "field_id TEXT NOT NULL", "region TEXT NOT NULL", "layer_name TEXT NOT NULL",
        "source TEXT NOT NULL", "raster_file TEXT NOT NULL", "start_date DATE NOT NULL",
        "end_date DATE NOT NULL", "iso_year SMALLINT NOT NULL", "week_no SMALLINT NOT NULL",
        "buffer_width_m SMALLINT NOT NULL", "pixel_count_before INTEGER NOT NULL",
        "pixel_count_after INTEGER NOT NULL", "below_min_pixel_threshold BOOLEAN NOT NULL",
    ]
    for band in BANDS[sensor]:
        definitions.extend(f"{band}_{suffix} REAL" for suffix in ("mean", "median", "min", "max", "std"))
        definitions.append(f"{band}_count INTEGER")
    definitions.extend([
        "PRIMARY KEY (field_id, start_date)", "CHECK (week_no BETWEEN 1 AND 53)",
        "CHECK (end_date >= start_date)", "CHECK (buffer_width_m IN (0, 5, 10))",
        "CHECK (pixel_count_before >= 0)",
        "CHECK (pixel_count_after >= 0 AND pixel_count_after <= pixel_count_before)",
    ])
    return ",\n    ".join(definitions)


def metadata_sql() -> str:
    return """
CREATE SCHEMA IF NOT EXISTS metadata;
CREATE SCHEMA IF NOT EXISTS timeseries;
CREATE TABLE IF NOT EXISTS metadata.zonal_runs (
    run_id TEXT PRIMARY KEY,
    run_dir TEXT NOT NULL,
    pipeline_version TEXT NOT NULL,
    loader_version TEXT NOT NULL,
    s1_table TEXT NOT NULL,
    s2_table TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    swapped_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS metadata.zonal_load_parts (
    run_id TEXT NOT NULL REFERENCES metadata.zonal_runs(run_id) ON DELETE CASCADE,
    sensor TEXT NOT NULL,
    part_path TEXT NOT NULL,
    raster_file TEXT NOT NULL,
    row_count BIGINT NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, sensor, part_path)
);
"""


def create_table_sql(sensor: str, table_name: str, years: Iterable[int]) -> str:
    checked_identifier(table_name)
    statements = [
        f"CREATE TABLE timeseries.{table_name} (\n    {table_columns_sql(sensor)}\n) PARTITION BY RANGE (start_date);"
    ]
    for year in sorted(set(years)):
        partition = checked_identifier(f"{table_name}_{year}")
        statements.append(
            f"CREATE TABLE timeseries.{partition} PARTITION OF timeseries.{table_name} "
            f"FOR VALUES FROM ('{year}-01-01') TO ('{year + 1}-01-01');"
        )
    source = checked_identifier(f"{sensor}_indices")
    statements.append(f"""
DO $grant_copy$
DECLARE item record;
BEGIN
    FOR item IN
        SELECT grantee, privilege_type
        FROM information_schema.role_table_grants
        WHERE table_schema = 'timeseries' AND table_name = '{source}'
    LOOP
        EXECUTE format(
            'GRANT %s ON TABLE timeseries.%I TO %s',
            item.privilege_type, '{table_name}',
            CASE WHEN item.grantee = 'PUBLIC' THEN 'PUBLIC' ELSE quote_ident(item.grantee) END
        );
    END LOOP;
END
$grant_copy$;
""")
    return "\n".join(statements)


def prepare_database(args: argparse.Namespace) -> None:
    run_dir = Path(args.run_dir).resolve()
    manifest, inventory = load_run(run_dir)
    tables = versioned_tables(run_dir)
    run_id = version_token(run_dir)
    years = sorted({int(str(value)[:4]) for value in inventory["start_date"]})
    existence = run_psql(
        args,
        "\n".join(f"SELECT to_regclass('timeseries.{table}') IS NOT NULL;" for table in tables.values()),
        tuples_only=True,
    ).splitlines()
    if any(value.strip() == "t" for value in existence) and not args.recreate:
        raise FileExistsError(f"Shadow tables already exist: {tables}. Use --recreate only to restart this run.")
    drop_sql = ""
    if args.recreate:
        drop_sql = "\n".join(f"DROP TABLE IF EXISTS timeseries.{table} CASCADE;" for table in tables.values())
    statement = fr"""
\set ON_ERROR_STOP on
BEGIN;
{metadata_sql()}
{drop_sql}
DELETE FROM metadata.zonal_runs WHERE run_id = {sql_string(run_id)};
{create_table_sql('s1', tables['s1'], years)}
{create_table_sql('s2', tables['s2'], years)}
INSERT INTO metadata.zonal_runs
    (run_id, run_dir, pipeline_version, loader_version, s1_table, s2_table, status, updated_at)
VALUES (
    {sql_string(run_id)}, {sql_string(run_dir)},
    {sql_string(manifest.get('pipeline_version', PIPELINE_VERSION))}, {sql_string(LOADER_VERSION)},
    {sql_string(tables['s1'])}, {sql_string(tables['s2'])}, 'prepared', now()
);
COMMIT;
"""
    run_psql(args, statement)
    atomic_write_json(run_dir / "database_state.json", {
        "run_id": run_id, "run_dir": str(run_dir), "tables": tables,
        "years": years, "prepared_at_utc": utc_now(),
    })
    log(f"Prepared shadow tables: {tables}")


def read_database_state(run_dir: Path) -> dict[str, Any]:
    path = run_dir / "database_state.json"
    if not path.exists():
        raise FileNotFoundError(f"Run database prepare first: {path}")
    state = json.loads(path.read_text())
    if state.get("tables") != versioned_tables(run_dir):
        raise ValueError(f"Database state does not match this run: {path}")
    return state


def parquet_to_csv(part: Path, csv_path: Path, sensor: str, batch_rows: int) -> int:
    parquet = pq.ParquetFile(part)
    columns = expected_output_columns(sensor)
    if parquet.schema_arrow.names != columns:
        raise ValueError(f"Unexpected Parquet schema: {part}")
    copied = 0
    first = True
    for batch in parquet.iter_batches(batch_size=batch_rows, columns=columns):
        frame = batch.to_pandas(date_as_object=True)
        frame.to_csv(
            csv_path, mode="w" if first else "a", index=False, header=False,
            na_rep="\\N", lineterminator="\n",
        )
        first = False
        copied += len(frame)
    if first:
        csv_path.write_text("")
    os.chmod(csv_path, 0o644)
    if copied != parquet.metadata.num_rows:
        raise RuntimeError(f"CSV row mismatch for {part}: {copied} != {parquet.metadata.num_rows}")
    return copied


def loaded_checkpoints(args: argparse.Namespace, run_id: str) -> set[tuple[str, str]]:
    output = run_psql(
        args,
        f"SELECT sensor, part_path FROM metadata.zonal_load_parts WHERE run_id = {sql_string(run_id)};",
        tuples_only=True,
    )
    checkpoints: set[tuple[str, str]] = set()
    for line in output.splitlines():
        if line.strip():
            sensor, path = line.split("\t", 1)
            checkpoints.add((sensor, path))
    return checkpoints


def copy_part_sql(
    table_name: str,
    columns: list[str],
    csv_path: Path,
    run_id: str,
    sensor: str,
    relative_part: str,
    raster_file: str,
    row_count: int,
) -> str:
    checked_identifier(table_name)
    column_sql = ", ".join(checked_identifier(column) for column in columns)
    return fr"""
\set ON_ERROR_STOP on
BEGIN;
\copy timeseries.{table_name} ({column_sql}) FROM {sql_string(csv_path)} WITH (FORMAT CSV, NULL '\N')
INSERT INTO metadata.zonal_load_parts
    (run_id, sensor, part_path, raster_file, row_count)
VALUES (
    {sql_string(run_id)}, {sql_string(sensor)}, {sql_string(relative_part)},
    {sql_string(raster_file)}, {row_count}
);
UPDATE metadata.zonal_runs SET status = 'loading', updated_at = now()
WHERE run_id = {sql_string(run_id)};
COMMIT;
"""


def load_parts(args: argparse.Namespace) -> None:
    run_dir = Path(args.run_dir).resolve()
    _, inventory = load_run(run_dir)
    state = read_database_state(run_dir)
    sensors = list(SENSORS) if args.sensor == "both" else [args.sensor]
    checkpoints = loaded_checkpoints(args, state["run_id"])
    failures: list[dict[str, str]] = []
    temp_root = Path(args.temp_dir)
    temp_root.mkdir(parents=True, exist_ok=True)
    os.chmod(temp_root, 0o755)
    for sensor in sensors:
        rows = inventory[inventory.sensor == sensor].sort_values(["start_date", "region"])
        table_name = state["tables"][sensor]
        columns = expected_output_columns(sensor)
        for position, raw in enumerate(rows.to_dict("records"), start=1):
            record = RasterRecord(**{key: raw[key] for key in RasterRecord.__dataclass_fields__})
            part = part_path(run_dir, record)
            relative_part = str(part.relative_to(run_dir))
            if (sensor, relative_part) in checkpoints:
                log(f"{sensor.upper()} {position}/{len(rows)} skipped: {part.name}")
                continue
            descriptor, temporary_name = tempfile.mkstemp(
                prefix=f"zonal-{sensor}-{record.start_date}-", suffix=".csv", dir=temp_root
            )
            os.close(descriptor)
            csv_path = Path(temporary_name)
            started = time.monotonic()
            try:
                copied = parquet_to_csv(part, csv_path, sensor, args.batch_rows)
                run_psql(args, copy_part_sql(
                    table_name, columns, csv_path, state["run_id"], sensor,
                    relative_part, record.raster_file, copied,
                ))
                checkpoints.add((sensor, relative_part))
                log(
                    f"{sensor.upper()} {position}/{len(rows)} loaded {copied:,} rows: "
                    f"{part.name} ({time.monotonic() - started:.1f}s)"
                )
            except Exception as exc:
                failures.append({"sensor": sensor, "part": str(part), "error": str(exc)})
                log(f"FAILED loading {part}: {exc}")
                if not args.continue_on_error:
                    raise
            finally:
                csv_path.unlink(missing_ok=True)
    if failures:
        atomic_write_json(run_dir / "database_load_failures.json", failures)
        raise RuntimeError(f"Database load completed with {len(failures)} failures")


def finalize_indexes(args: argparse.Namespace) -> None:
    run_dir = Path(args.run_dir).resolve()
    state = read_database_state(run_dir)
    statements = ["\\set ON_ERROR_STOP on"]
    token = checked_identifier("i_" + state["run_id"][-8:])
    for sensor in SENSORS:
        table = checked_identifier(state["tables"][sensor])
        statements.extend([
            f"CREATE INDEX IF NOT EXISTS {sensor}_{token}_date_field_idx ON timeseries.{table} (start_date, field_id);",
            f"CREATE INDEX IF NOT EXISTS {sensor}_{token}_region_date_idx ON timeseries.{table} (region, start_date);",
            f"ANALYZE timeseries.{table};",
        ])
    run_psql(args, "\n".join(statements))
    log("Secondary indexes and ANALYZE completed")


def validate_database(args: argparse.Namespace) -> None:
    run_dir = Path(args.run_dir).resolve()
    manifest, inventory = load_run(run_dir)
    state = read_database_state(run_dir)
    report: dict[str, Any] = {
        "run_id": state["run_id"], "checked_at_utc": utc_now(), "sensors": {}, "failures": [],
    }
    for sensor in SENSORS:
        table = checked_identifier(state["tables"][sensor])
        count_condition = " OR ".join(
            f"{band}_count < 0 OR {band}_count > pixel_count_after" for band in BANDS[sensor]
        )
        output = run_psql(args, f"""
SELECT
    count(*),
    count(*) FILTER (WHERE buffer_width_m NOT IN (0,5,10)),
    count(*) FILTER (WHERE pixel_count_after < 0 OR pixel_count_after > pixel_count_before),
    count(*) FILTER (WHERE {count_condition})
FROM timeseries.{table};
SELECT count(*), coalesce(sum(row_count), 0)
FROM metadata.zonal_load_parts
WHERE run_id = {sql_string(state['run_id'])} AND sensor = {sql_string(sensor)};
""", tuples_only=True).splitlines()
        table_values = [int(value) for value in output[0].split("\t")]
        load_values = [int(value) for value in output[1].split("\t")]
        expected_rows = int(manifest["expected_rows"][sensor])
        expected_parts = int((inventory.sensor == sensor).sum())
        values = {
            "table": f"timeseries.{table}", "rows": table_values[0],
            "expected_rows": expected_rows, "loaded_parts": load_values[0],
            "expected_parts": expected_parts, "checkpoint_rows": load_values[1],
            "bad_buffer": table_values[1], "bad_geometry_count": table_values[2],
            "bad_valid_count": table_values[3],
        }
        report["sensors"][sensor] = values
        for key, actual, expected in (
            ("rows", values["rows"], expected_rows),
            ("loaded_parts", values["loaded_parts"], expected_parts),
            ("checkpoint_rows", values["checkpoint_rows"], expected_rows),
            ("bad_buffer", values["bad_buffer"], 0),
            ("bad_geometry_count", values["bad_geometry_count"], 0),
            ("bad_valid_count", values["bad_valid_count"], 0),
        ):
            if actual != expected:
                report["failures"].append(
                    f"{sensor.upper()} {key}: actual={actual:,}, expected={expected:,}"
                )
    report["status"] = "passed" if not report["failures"] else "failed"
    database_status = "validated" if report["status"] == "passed" else "validation_failed"
    run_psql(
        args,
        f"UPDATE metadata.zonal_runs SET status = {sql_string(database_status)}, updated_at = now() "
        f"WHERE run_id = {sql_string(state['run_id'])};",
    )
    report_path = run_dir / "database_validation_report.json"
    atomic_write_json(report_path, report)
    log(json.dumps(report, indent=2))
    if report["failures"]:
        raise RuntimeError(f"Database validation failed; see {report_path}")


def swap_tables(args: argparse.Namespace) -> None:
    run_dir = Path(args.run_dir).resolve()
    state = read_database_state(run_dir)
    report_path = run_dir / "database_validation_report.json"
    if not report_path.exists() or json.loads(report_path.read_text()).get("status") != "passed":
        raise ValueError("Database validation must pass before swap")
    suffix = (args.legacy_suffix or datetime.now().strftime("%Y%m%d_%H%M%S")).lower()
    if not re.fullmatch(r"[a-z0-9_]+", suffix):
        raise ValueError("--legacy-suffix may contain only letters, numbers, and underscores")
    statements = [
        "\\set ON_ERROR_STOP on", "BEGIN;",
        "SELECT pg_advisory_xact_lock(hashtext('kenya_zonal_table_swap'));",
    ]
    for sensor in SENSORS:
        current = checked_identifier(f"{sensor}_indices")
        legacy = checked_identifier(f"{current}_legacy_{suffix}")
        next_table = checked_identifier(state["tables"][sensor])
        statements.extend([
            f"DO $check$ BEGIN IF to_regclass('timeseries.{legacy}') IS NOT NULL THEN RAISE EXCEPTION 'Legacy table exists: timeseries.{legacy}'; END IF; END $check$;",
            f"ALTER TABLE timeseries.{current} RENAME TO {legacy};",
            f"ALTER TABLE timeseries.{next_table} RENAME TO {current};",
        ])
    statements.extend([
        "UPDATE metadata.zonal_runs SET status = 'swapped', updated_at = now(), swapped_at = now() "
        f"WHERE run_id = {sql_string(state['run_id'])};",
        "COMMIT;",
    ])
    run_psql(args, "\n".join(statements))
    log(f"Atomic swap complete. Previous tables use suffix: legacy_{suffix}")


def add_common_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--run-dir", required=True)
    parser.add_argument("--database", default="kenya_monitoring")
    parser.add_argument("--psql-command", default="sudo -u postgres psql")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", action="version", version=LOADER_VERSION)
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare_parser = subparsers.add_parser("prepare", help="Create versioned shadow tables")
    add_common_arguments(prepare_parser)
    prepare_parser.add_argument("--recreate", action="store_true")
    prepare_parser.set_defaults(func=prepare_database)

    load_parser = subparsers.add_parser("load", help="Load Parquet parts with resumable checkpoints")
    add_common_arguments(load_parser)
    load_parser.add_argument("--sensor", choices=["s1", "s2", "both"], default="both")
    load_parser.add_argument("--batch-rows", type=int, default=25_000)
    load_parser.add_argument("--temp-dir", default="/tmp/zonal-db-load")
    load_parser.add_argument("--continue-on-error", action="store_true")
    load_parser.set_defaults(func=load_parts)

    finalize_parser = subparsers.add_parser("finalize", help="Create indexes and ANALYZE shadow tables")
    add_common_arguments(finalize_parser)
    finalize_parser.set_defaults(func=finalize_indexes)

    validate_parser = subparsers.add_parser("validate", help="Validate shadow tables")
    add_common_arguments(validate_parser)
    validate_parser.set_defaults(func=validate_database)

    swap_parser = subparsers.add_parser("swap", help="Atomically replace both production tables")
    add_common_arguments(swap_parser)
    swap_parser.add_argument("--legacy-suffix")
    swap_parser.set_defaults(func=swap_tables)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    try:
        args.func(args)
        return 0
    except KeyboardInterrupt:
        log("Interrupted")
        return 130
    except Exception as exc:
        log(f"FATAL: {exc}")
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
