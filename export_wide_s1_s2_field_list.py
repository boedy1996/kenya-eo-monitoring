#!/usr/bin/env python3
"""Export a complete field-by-week S1/S2 production dataset to wide Parquet."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq


FIELD_ID_RE = re.compile(r"^(?:MA|NA)[0-9]+$")
POSTGRES_TO_ARROW = {
    "text": pa.string(),
    "date": pa.date32(),
    "smallint": pa.int16(),
    "integer": pa.int32(),
    "boolean": pa.bool_(),
    "real": pa.float32(),
}


def log(message: str) -> None:
    stamp = datetime.now().astimezone().isoformat(timespec="seconds")
    print(f"[{stamp}] {message}", flush=True)


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def read_field_ids(path: Path, expected_count: int) -> list[str]:
    field_ids = [line.strip() for line in path.read_text().splitlines() if line.strip()]
    invalid = [field_id for field_id in field_ids if not FIELD_ID_RE.fullmatch(field_id)]
    if invalid:
        raise ValueError(f"Invalid field IDs in {path}: {invalid[:10]}")
    duplicates = sorted({field_id for field_id in field_ids if field_ids.count(field_id) > 1})
    if duplicates:
        raise ValueError(f"Duplicate field IDs in {path}: {duplicates[:10]}")
    if len(field_ids) != expected_count:
        raise ValueError(
            f"Field count mismatch: found {len(field_ids):,}, expected {expected_count:,}"
        )
    return field_ids


def psql_command(base_command: str, database: str) -> list[str]:
    return shlex.split(base_command) + ["-X", "-v", "ON_ERROR_STOP=1", "-d", database]


def database_columns(base_command: str, database: str) -> dict[str, list[tuple[str, str]]]:
    query = """
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'timeseries'
          AND table_name IN ('s1_indices', 's2_indices')
        ORDER BY table_name, ordinal_position
    """
    result = subprocess.run(
        psql_command(base_command, database) + ["-A", "-t", "-F", "\t", "-c", query],
        text=True,
        capture_output=True,
    )
    if result.returncode:
        raise RuntimeError(f"Could not read database schema:\n{result.stderr}")
    rows = [line.split("\t") for line in result.stdout.splitlines() if line.strip()]
    result: dict[str, list[tuple[str, str]]] = {"s1_indices": [], "s2_indices": []}
    for table_name, column_name, data_type in rows:
        if data_type not in POSTGRES_TO_ARROW:
            raise ValueError(
                f"Unsupported PostgreSQL type {data_type!r} for {table_name}.{column_name}"
            )
        result[table_name].append((column_name, data_type))
    if not result["s1_indices"] or not result["s2_indices"]:
        raise ValueError("Could not read the production S1/S2 table schemas")
    return result


def output_schema(columns: dict[str, list[tuple[str, str]]], metadata: dict[str, str]) -> pa.Schema:
    fields = [
        pa.field("field_id", pa.string()),
        pa.field("week_start", pa.date32()),
        pa.field("week_end", pa.date32()),
        pa.field("iso_year", pa.int16()),
        pa.field("week_no", pa.int16()),
        pa.field("buffer_width_m", pa.int16()),
    ]
    for prefix, table_name in (("s1", "s1_indices"), ("s2", "s2_indices")):
        fields.extend(
            pa.field(f"{prefix}_{column_name}", POSTGRES_TO_ARROW[data_type])
            for column_name, data_type in columns[table_name]
        )
    encoded_metadata = {key.encode(): value.encode() for key, value in metadata.items()}
    return pa.schema(fields, metadata=encoded_metadata)


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def select_sql(
    columns: dict[str, list[tuple[str, str]]],
    field_ids: list[str],
    date_from: date,
    date_to: date,
) -> str:
    sensor_columns: list[str] = []
    for alias, table_name in (("s1", "s1_indices"), ("s2", "s2_indices")):
        sensor_columns.extend(
            f'{alias}."{column_name}" AS "{alias}_{column_name}"'
            for column_name, _ in columns[table_name]
        )
    prefixed_columns = ",\n    ".join(sensor_columns)
    field_values = ",\n        ".join(f"({sql_string(field_id)})" for field_id in field_ids)
    return f"""
WITH export_fields(field_id) AS (
    VALUES
        {field_values}
),
weeks AS (
    SELECT generate_series(
        DATE {sql_string(date_from.isoformat())},
        DATE {sql_string(date_to.isoformat())},
        interval '7 days'
    )::date AS week_start
),
field_buffers AS (
    SELECT
        fields.field_id,
        coalesce(s1_buffer.buffer_width_m, s2_buffer.buffer_width_m) AS buffer_width_m
    FROM export_fields AS fields
    LEFT JOIN LATERAL (
        SELECT buffer_width_m
        FROM timeseries.s1_indices
        WHERE field_id = fields.field_id
        ORDER BY start_date
        LIMIT 1
    ) AS s1_buffer ON true
    LEFT JOIN LATERAL (
        SELECT buffer_width_m
        FROM timeseries.s2_indices
        WHERE field_id = fields.field_id
        ORDER BY start_date
        LIMIT 1
    ) AS s2_buffer ON true
)
SELECT
    fields.field_id,
    weeks.week_start,
    (weeks.week_start + 6)::date AS week_end,
    extract(isoyear FROM weeks.week_start)::smallint AS iso_year,
    extract(week FROM weeks.week_start)::smallint AS week_no,
    fields.buffer_width_m,
    {prefixed_columns}
FROM field_buffers AS fields
CROSS JOIN weeks
LEFT JOIN timeseries.s1_indices AS s1
  ON s1.field_id = fields.field_id
 AND s1.start_date = weeks.week_start
LEFT JOIN timeseries.s2_indices AS s2
  ON s2.field_id = fields.field_id
 AND s2.start_date = weeks.week_start
ORDER BY fields.field_id, weeks.week_start
"""


def write_parquet(
    base_command: str,
    database: str,
    query: str,
    schema: pa.Schema,
    partial_path: Path,
    batch_rows: int,
) -> int:
    copy_query = (
        "COPY (" + query.strip().rstrip(";") + ") "
        "TO STDOUT WITH (FORMAT CSV, NULL '__PARQUET_NULL__')"
    )
    process = subprocess.Popen(
        psql_command(base_command, database)
        + [
            "-q",
            "-c",
            "SET enable_seqscan=off; SET enable_mergejoin=off; SET enable_hashjoin=off;",
            "-c",
            copy_query,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if process.stdout is None or process.stderr is None:
        process.kill()
        raise RuntimeError("Could not open psql pipes")
    writer = pq.ParquetWriter(
        partial_path,
        schema,
        compression="zstd",
        use_dictionary=True,
        write_statistics=True,
        version="2.6",
    )
    total_rows = 0
    try:
        reader = pacsv.open_csv(
            process.stdout,
            read_options=pacsv.ReadOptions(
                column_names=schema.names,
                block_size=16 * 1024 * 1024,
                use_threads=True,
            ),
            parse_options=pacsv.ParseOptions(newlines_in_values=False),
            convert_options=pacsv.ConvertOptions(
                column_types={field.name: field.type for field in schema},
                null_values=["__PARQUET_NULL__"],
                strings_can_be_null=True,
                true_values=["t"],
                false_values=["f"],
            ),
        )
        for batch in reader:
            table = pa.Table.from_batches([batch], schema=schema)
            writer.write_table(table, row_group_size=batch_rows)
            total_rows += batch.num_rows
            log(f"Exported {total_rows:,} rows")
        process.stdout.close()
        return_code = process.wait()
        stderr = process.stderr.read().decode(errors="replace")
        if return_code:
            raise RuntimeError(f"psql export failed ({return_code}):\n{stderr}")
    except Exception:
        process.terminate()
        process.wait(timeout=30)
        writer.close()
        partial_path.unlink(missing_ok=True)
        raise
    writer.close()
    return total_rows


def validate_parquet(
    output_path: Path,
    field_ids: list[str],
    date_from: date,
    date_to: date,
    expected_columns: int,
) -> dict[str, Any]:
    expected_weeks = (date_to - date_from).days // 7 + 1
    expected_rows = len(field_ids) * expected_weeks
    parquet = pq.ParquetFile(output_path)
    connection = duckdb.connect()
    try:
        path_literal = str(output_path).replace("'", "''")
        values = connection.execute(
            f"""
            SELECT
                count(*) AS rows,
                count(DISTINCT field_id) AS fields,
                count(DISTINCT week_start) AS weeks,
                count(*) - count(DISTINCT (field_id, week_start)) AS duplicate_keys,
                min(week_start) AS min_week,
                max(week_start) AS max_week,
                count(*) FILTER (WHERE buffer_width_m NOT IN (0, 5, 10) OR buffer_width_m IS NULL) AS bad_buffer,
                count(*) FILTER (
                    WHERE s1_buffer_width_m IS NOT NULL
                      AND s1_buffer_width_m <> buffer_width_m
                ) AS s1_buffer_mismatch,
                count(*) FILTER (
                    WHERE s2_buffer_width_m IS NOT NULL
                      AND s2_buffer_width_m <> buffer_width_m
                ) AS s2_buffer_mismatch,
                count(*) FILTER (
                    WHERE s1_pixel_count_before < 0
                       OR s1_pixel_count_after < 0
                       OR s1_pixel_count_after > s1_pixel_count_before
                ) AS bad_s1_pixel_count,
                count(*) FILTER (
                    WHERE s2_pixel_count_before < 0
                       OR s2_pixel_count_after < 0
                       OR s2_pixel_count_after > s2_pixel_count_before
                ) AS bad_s2_pixel_count,
                count(*) FILTER (WHERE s1_field_id IS NULL) AS s1_null_rows,
                count(*) FILTER (WHERE s2_field_id IS NULL) AS s2_null_rows
            FROM read_parquet('{path_literal}')
            """
        ).fetchone()
        exported_field_ids = {
            row[0]
            for row in connection.execute(
                f"SELECT DISTINCT field_id FROM read_parquet('{path_literal}')"
            ).fetchall()
        }
        bad_field_week_counts = connection.execute(
            f"""
            SELECT count(*)
            FROM (
                SELECT field_id, count(*) AS week_count
                FROM read_parquet('{path_literal}')
                GROUP BY field_id
                HAVING count(*) <> {expected_weeks}
            )
            """
        ).fetchone()[0]
    finally:
        connection.close()

    keys = (
        "rows",
        "fields",
        "weeks",
        "duplicate_keys",
        "min_week",
        "max_week",
        "bad_buffer",
        "s1_buffer_mismatch",
        "s2_buffer_mismatch",
        "bad_s1_pixel_count",
        "bad_s2_pixel_count",
        "s1_null_rows",
        "s2_null_rows",
    )
    report: dict[str, Any] = dict(zip(keys, values, strict=True))
    report.update(
        {
            "file": str(output_path),
            "file_size_bytes": output_path.stat().st_size,
            "columns": len(parquet.schema_arrow),
            "row_groups": parquet.metadata.num_row_groups,
            "expected_rows": expected_rows,
            "expected_fields": len(field_ids),
            "expected_weeks": expected_weeks,
            "expected_columns": expected_columns,
            "bad_field_week_counts": bad_field_week_counts,
            "missing_field_ids": sorted(set(field_ids) - exported_field_ids),
            "unexpected_field_ids": sorted(exported_field_ids - set(field_ids)),
        }
    )
    failures: list[str] = []
    exact_checks = {
        "rows": expected_rows,
        "fields": len(field_ids),
        "weeks": expected_weeks,
        "duplicate_keys": 0,
        "min_week": date_from,
        "max_week": date_to,
        "bad_buffer": 0,
        "s1_buffer_mismatch": 0,
        "s2_buffer_mismatch": 0,
        "bad_s1_pixel_count": 0,
        "bad_s2_pixel_count": 0,
        "columns": expected_columns,
        "bad_field_week_counts": 0,
    }
    for key, expected in exact_checks.items():
        if report[key] != expected:
            failures.append(f"{key}: actual={report[key]!r}, expected={expected!r}")
    if report["missing_field_ids"]:
        failures.append(f"missing_field_ids: {report['missing_field_ids'][:10]}")
    if report["unexpected_field_ids"]:
        failures.append(f"unexpected_field_ids: {report['unexpected_field_ids'][:10]}")
    report["failures"] = failures
    report["status"] = "passed" if not failures else "failed"
    return report


def json_ready(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, dict):
        return {key: json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [json_ready(item) for item in value]
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--field-id-file", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--date-from", type=parse_date, required=True)
    parser.add_argument("--date-to", type=parse_date, required=True)
    parser.add_argument("--expected-fields", type=int, default=775)
    parser.add_argument("--batch-rows", type=int, default=10_000)
    parser.add_argument("--database", default="kenya_monitoring")
    parser.add_argument("--psql-command", default="sudo -u postgres psql")
    parser.add_argument("--overwrite", action="store_true")
    args = parser.parse_args()

    if args.date_to < args.date_from:
        raise ValueError("--date-to must be on or after --date-from")
    if (args.date_to - args.date_from).days % 7:
        raise ValueError("The date range must align to a seven-day weekly grid")
    args.output = args.output.resolve()
    partial_path = args.output.with_suffix(args.output.suffix + ".partial")
    validation_path = args.output.with_suffix(args.output.suffix + ".validation.json")
    if args.output.exists() and not args.overwrite:
        raise FileExistsError(f"Output already exists: {args.output}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    partial_path.unlink(missing_ok=True)

    field_ids = read_field_ids(args.field_id_file, args.expected_fields)
    expected_weeks = (args.date_to - args.date_from).days // 7 + 1
    log(
        f"Preparing {len(field_ids):,} fields x {expected_weeks:,} weeks "
        f"= {len(field_ids) * expected_weeks:,} rows"
    )

    columns = database_columns(args.psql_command, args.database)
    metadata = {
        "created_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source_s1": "timeseries.s1_indices",
        "source_s2": "timeseries.s2_indices",
        "field_id_source": args.field_id_file.name,
        "date_from": args.date_from.isoformat(),
        "date_to": args.date_to.isoformat(),
        "layout": "one row per field_id and week; S1/S2 columns are prefixed",
        "missing_values": "native Parquet null; no value imputation",
        "buffer_width_m": "shared adaptive field buffer; sensor pixel counts remain prefixed",
    }
    schema = output_schema(columns, metadata)
    rows = write_parquet(
        args.psql_command,
        args.database,
        select_sql(columns, field_ids, args.date_from, args.date_to),
        schema,
        partial_path,
        args.batch_rows,
    )
    expected_rows = len(field_ids) * expected_weeks
    if rows != expected_rows:
        partial_path.unlink(missing_ok=True)
        raise RuntimeError(f"Exported {rows:,} rows; expected {expected_rows:,}")
    os.replace(partial_path, args.output)
    log(f"Parquet written: {args.output}")

    report = validate_parquet(
        args.output,
        field_ids,
        args.date_from,
        args.date_to,
        len(schema),
    )
    validation_path.write_text(json.dumps(json_ready(report), indent=2) + "\n")
    log(json.dumps(json_ready(report), indent=2))
    if report["status"] != "passed":
        raise RuntimeError(f"Validation failed; see {validation_path}")


if __name__ == "__main__":
    main()
