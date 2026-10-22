#!/usr/bin/env python3
"""Run the delivered Kenya v3 crop model for every production field.

The production zonal tables are much larger than the 775-field delivery file.
This runner therefore partitions fields by county, keeps every weekly row for a
field in the same batch, exports only the eleven columns needed by the model,
scores one batch per process, and writes a partitioned Parquet result.

Commands:

    prepare    inventory fields, freeze source-table identities, verify models
    run-batch  export and score one batch
    run-all    score all pending batches sequentially and resumably
    status     report completed, failed and pending batches
    validate   aggregate batch checks into one production validation report
"""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import os
import re
import shlex
import subprocess
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.csv as pacsv
import pyarrow.parquet as pq


RUNNER_VERSION = "1.0.0"
COUNTIES = ("Machakos", "Nakuru")
ALLOWED_REASONS = {
    "classified",
    "below_threshold",
    "inter_season",
    "insufficient_observations",
    "incomplete_history",
    "out_of_training_range",
    "out_of_range",
    "unknown_county",
}
INPUT_COLUMNS = [
    "field_id",
    "week_start",
    "s2_ndvi_mean",
    "s2_evi_mean",
    "s2_lai_mean",
    "s2_ndvi_count",
    "s2_evi_count",
    "s2_lai_count",
    "s1_sigma_vv_linear_mean",
    "s1_sigma_vh_linear_mean",
    "county",
]
INPUT_SCHEMA = pa.schema(
    [
        pa.field("field_id", pa.string()),
        pa.field("week_start", pa.date32()),
        pa.field("s2_ndvi_mean", pa.float32()),
        pa.field("s2_evi_mean", pa.float32()),
        pa.field("s2_lai_mean", pa.float32()),
        pa.field("s2_ndvi_count", pa.int32()),
        pa.field("s2_evi_count", pa.int32()),
        pa.field("s2_lai_count", pa.int32()),
        pa.field("s1_sigma_vv_linear_mean", pa.float32()),
        pa.field("s1_sigma_vh_linear_mean", pa.float32()),
        pa.field("county", pa.string()),
    ]
)
OUTPUT_COLUMNS = [
    "field_id",
    "week_start",
    "county",
    "season",
    "week_in_season",
    "scorable",
    "label",
    "reason",
    "confidence",
    "p_maize",
    "p_beans",
    "p_potatoes",
]
FIELD_ID_RE = re.compile(r"^(?:MA|NA)[0-9]+$")


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def log(message: str) -> None:
    print(f"[{datetime.now().astimezone().isoformat(timespec='seconds')}] {message}", flush=True)


def json_ready(value: Any) -> Any:
    if isinstance(value, (date, datetime, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, dict):
        return {str(k): json_ready(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_ready(v) for v in value]
    return value


def atomic_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    partial = path.with_suffix(path.suffix + ".partial")
    partial.write_text(json.dumps(json_ready(data), indent=2, sort_keys=True) + "\n")
    os.replace(partial, path)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(8 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sql_string(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def psql_command(args: argparse.Namespace) -> list[str]:
    return shlex.split(args.psql_command) + [
        "-X",
        "-v",
        "ON_ERROR_STOP=1",
        "-d",
        args.database,
    ]


def psql_text(args: argparse.Namespace, query: str) -> str:
    result = subprocess.run(
        psql_command(args) + ["-A", "-t", "-F", "\t", "-c", query],
        text=True,
        capture_output=True,
    )
    if result.returncode:
        raise RuntimeError(f"psql failed ({result.returncode}):\n{result.stderr}")
    return result.stdout.strip()


def source_facts(args: argparse.Namespace) -> dict[str, Any]:
    query = """
SELECT
    's1' AS sensor,
    'timeseries.s1_indices'::regclass::oid::text AS relation_oid,
    (SELECT start_date FROM timeseries.s1_indices ORDER BY start_date LIMIT 1)::text AS min_date,
    (SELECT start_date FROM timeseries.s1_indices ORDER BY start_date DESC LIMIT 1)::text AS max_date
UNION ALL
SELECT
    's2',
    'timeseries.s2_indices'::regclass::oid::text,
    (SELECT start_date FROM timeseries.s2_indices ORDER BY start_date LIMIT 1)::text,
    (SELECT start_date FROM timeseries.s2_indices ORDER BY start_date DESC LIMIT 1)::text
ORDER BY sensor;
"""
    rows = [line.split("\t") for line in psql_text(args, query).splitlines() if line]
    if len(rows) != 2:
        raise RuntimeError(f"Expected S1 and S2 source facts, received: {rows}")
    facts = {
        sensor: {"relation_oid": int(oid), "min_date": min_date, "max_date": max_date}
        for sensor, oid, min_date, max_date in rows
    }
    date_from = min(date.fromisoformat(v["min_date"]) for v in facts.values())
    date_to = max(date.fromisoformat(v["max_date"]) for v in facts.values())
    if date_from.weekday() != 0 or date_to.weekday() != 0:
        raise ValueError(f"Weekly range is not Monday-aligned: {date_from} to {date_to}")
    if (date_to - date_from).days % 7:
        raise ValueError(f"Weekly range is not divisible by seven days: {date_from} to {date_to}")
    facts["date_from"] = date_from.isoformat()
    facts["date_to"] = date_to.isoformat()
    facts["weeks"] = (date_to - date_from).days // 7 + 1
    return facts


def ensure_source_unchanged(args: argparse.Namespace, manifest: dict[str, Any]) -> None:
    current = source_facts(args)
    expected = manifest["source"]
    for sensor in ("s1", "s2"):
        if current[sensor]["relation_oid"] != expected[sensor]["relation_oid"]:
            raise RuntimeError(
                f"Source table changed for {sensor}: current OID "
                f"{current[sensor]['relation_oid']}, prepared OID {expected[sensor]['relation_oid']}"
            )
    for key in ("date_from", "date_to", "weeks"):
        if current[key] != expected[key]:
            raise RuntimeError(f"Source {key} changed: current={current[key]}, prepared={expected[key]}")


def verify_package(package_dir: Path) -> dict[str, Any]:
    required = [
        package_dir / "Models" / "model_config.json",
        package_dir / "Models" / "classifier_Machakos.json",
        package_dir / "Models" / "classifier_Nakuru.json",
        package_dir / "Models" / "golden_fixture.json",
        package_dir / "SHA256SUMS.txt",
    ]
    missing = [str(path) for path in required if not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing package files: {missing}")
    check = subprocess.run(
        ["sha256sum", "-c", "SHA256SUMS.txt"],
        cwd=package_dir,
        text=True,
        capture_output=True,
    )
    if check.returncode:
        raise RuntimeError(f"Package checksum validation failed:\n{check.stdout}\n{check.stderr}")
    config_path = package_dir / "Models" / "model_config.json"
    config = json.loads(config_path.read_text())
    return {
        "package_dir": str(package_dir.resolve()),
        "model_version": config["version"],
        "model_config_sha256": sha256_file(config_path),
        "xgboost_required": config.get("versions", {}).get("xgboost"),
        "checksums": "passed",
    }


def model_imports(package_dir: Path):
    package = str(package_dir.resolve())
    if package not in sys.path:
        sys.path.insert(0, package)
    from kenya_pipeline.scoring import Scorer  # type: ignore

    return Scorer


def verify_golden_fixture(package_dir: Path) -> dict[str, Any]:
    Scorer = model_imports(package_dir)
    fixture = json.loads((package_dir / "Models" / "golden_fixture.json").read_text())
    raw = pd.DataFrame(fixture["raw_rows"])
    expected = pd.DataFrame(fixture["expected"])
    raw["week_start"] = pd.to_datetime(raw["week_start"])
    expected["week_start"] = pd.to_datetime(expected["week_start"])
    result = Scorer(package_dir / "Models" / "model_config.json").score(raw)
    compared = result.merge(
        expected[["field_id", "week_start", "label", "reason"]],
        on=["field_id", "week_start"],
        suffixes=("", "_expected"),
    )
    passed = bool(
        len(compared) == len(expected)
        and (compared["label"] == compared["label_expected"]).all()
        and (compared["reason"] == compared["reason_expected"]).all()
    )
    if not passed:
        raise RuntimeError("Golden fixture labels or reasons differ from the saved reference")
    return {"rows": len(compared), "status": "passed"}


def inventory_fields(args: argparse.Namespace, latest_s2_date: str) -> pd.DataFrame:
    query = f"""
SELECT
    field_id,
    CASE lower(region)
        WHEN 'machakos' THEN 'Machakos'
        WHEN 'nakuru' THEN 'Nakuru'
        ELSE initcap(region)
    END AS county
FROM timeseries.s2_indices
WHERE start_date = DATE {sql_string(latest_s2_date)}
ORDER BY county, field_id;
"""
    text = psql_text(args, query)
    rows = [line.split("\t") for line in text.splitlines() if line]
    fields = pd.DataFrame(rows, columns=["field_id", "county"])
    if fields.empty:
        raise RuntimeError("Field inventory is empty")
    invalid_ids = fields.loc[~fields["field_id"].str.fullmatch(FIELD_ID_RE), "field_id"]
    if len(invalid_ids):
        raise ValueError(f"Invalid field IDs: {invalid_ids.head(10).tolist()}")
    duplicates = fields.loc[fields.duplicated("field_id", keep=False), "field_id"].unique()
    if len(duplicates):
        raise ValueError(f"Duplicate field IDs: {duplicates[:10].tolist()}")
    unexpected_counties = sorted(set(fields["county"]) - set(COUNTIES))
    if unexpected_counties:
        raise ValueError(f"Unexpected counties: {unexpected_counties}")
    return fields


def build_batch_plan(fields: pd.DataFrame, batch_size: int) -> tuple[pd.DataFrame, list[dict[str, Any]]]:
    fields = fields.sort_values(["county", "field_id"]).reset_index(drop=True).copy()
    fields["county_row"] = fields.groupby("county").cumcount()
    fields["batch_number"] = (fields["county_row"] // batch_size).astype("int32")
    fields["batch_id"] = (
        fields["county"].str.lower()
        + "_"
        + fields["batch_number"].map(lambda value: f"{value:05d}")
    )
    batches = []
    for batch_index, (batch_id, group) in enumerate(fields.groupby("batch_id", sort=True)):
        batches.append(
            {
                "batch_index": batch_index,
                "batch_id": batch_id,
                "county": group["county"].iloc[0],
                "batch_number": int(group["batch_number"].iloc[0]),
                "fields": len(group),
                "first_field_id": group["field_id"].iloc[0],
                "last_field_id": group["field_id"].iloc[-1],
            }
        )
    return fields.drop(columns=["county_row"]), batches


def run_paths(run_dir: Path, batch: dict[str, Any]) -> dict[str, Path]:
    county_slug = batch["county"].lower()
    batch_id = batch["batch_id"]
    return {
        "input": run_dir / "inputs" / county_slug / f"{batch_id}.parquet",
        "output": run_dir / "outputs" / county_slug / f"{batch_id}.parquet",
        "status": run_dir / "status" / f"{batch_id}.json",
        "log": run_dir / "logs" / f"{batch_id}.log",
    }


def load_manifest(run_dir: Path) -> dict[str, Any]:
    path = run_dir / "run_manifest.json"
    if not path.exists():
        raise FileNotFoundError(f"Run prepare first: {path}")
    manifest = json.loads(path.read_text())
    if manifest.get("runner_version") != RUNNER_VERSION:
        raise ValueError(
            f"Runner version mismatch: manifest={manifest.get('runner_version')}, code={RUNNER_VERSION}"
        )
    return manifest


def batch_by_id(manifest: dict[str, Any], batch_id: str) -> dict[str, Any]:
    matches = [batch for batch in manifest["batches"] if batch["batch_id"] == batch_id]
    if len(matches) != 1:
        raise ValueError(f"Unknown batch ID: {batch_id}")
    return matches[0]


def prepare(args: argparse.Namespace) -> None:
    run_dir = args.run_dir.resolve()
    manifest_path = run_dir / "run_manifest.json"
    if manifest_path.exists() and not args.force:
        raise FileExistsError(f"Run already prepared: {manifest_path}")
    run_dir.mkdir(parents=True, exist_ok=True)
    for directory in ("inputs", "outputs", "status", "logs"):
        (run_dir / directory).mkdir(exist_ok=True)

    package = verify_package(args.package_dir.resolve())
    golden = verify_golden_fixture(args.package_dir.resolve())
    source = source_facts(args)
    fields = inventory_fields(args, source["s2"]["max_date"])
    if args.expected_fields and len(fields) != args.expected_fields:
        raise ValueError(f"Found {len(fields):,} fields; expected {args.expected_fields:,}")
    by_county = fields.groupby("county").size().to_dict()
    fields, batches = build_batch_plan(fields, args.batch_size)
    fields_path = run_dir / "fields.parquet"
    fields.to_parquet(fields_path, index=False, compression="zstd")
    expected_rows = len(fields) * source["weeks"]
    manifest = {
        "runner_version": RUNNER_VERSION,
        "created_at_utc": now_utc(),
        "run_dir": str(run_dir),
        "database": args.database,
        "psql_command": args.psql_command,
        "package": package,
        "golden_fixture": golden,
        "source": source,
        "batch_size": args.batch_size,
        "fields": len(fields),
        "fields_by_county": by_county,
        "weeks": source["weeks"],
        "expected_rows": expected_rows,
        "batch_count": len(batches),
        "batches": batches,
        "fields_file": str(fields_path),
        "fields_sha256": sha256_file(fields_path),
        "layout": "county-specific batches; complete weekly history stays in one batch",
        "output_format": "partitioned Parquet",
    }
    atomic_json(manifest_path, manifest)
    log(
        f"Prepared {len(fields):,} fields x {source['weeks']:,} weeks = "
        f"{expected_rows:,} rows in {len(batches)} batches"
    )
    log(f"Fields by county: {by_county}")
    log(f"Golden fixture: {golden}")
    log(f"Manifest: {manifest_path}")


def field_values_sql(fields: pd.DataFrame) -> str:
    return ",\n        ".join(
        f"({sql_string(field_id)}, {sql_string(county)})"
        for field_id, county in fields[["field_id", "county"]].itertuples(index=False)
    )


def batch_select_sql(fields: pd.DataFrame, date_from: str, date_to: str) -> str:
    return f"""
WITH export_fields(field_id, county) AS (
    VALUES
        {field_values_sql(fields)}
),
weeks AS (
    SELECT generate_series(
        DATE {sql_string(date_from)},
        DATE {sql_string(date_to)},
        interval '7 days'
    )::date AS week_start
)
SELECT
    fields.field_id,
    weeks.week_start,
    s2.ndvi_mean AS s2_ndvi_mean,
    s2.evi_mean AS s2_evi_mean,
    s2.lai_mean AS s2_lai_mean,
    s2.ndvi_count AS s2_ndvi_count,
    s2.evi_count AS s2_evi_count,
    s2.lai_count AS s2_lai_count,
    s1.sigma_vv_linear_mean AS s1_sigma_vv_linear_mean,
    s1.sigma_vh_linear_mean AS s1_sigma_vh_linear_mean,
    fields.county
FROM export_fields AS fields
CROSS JOIN weeks
LEFT JOIN timeseries.s1_indices AS s1
  ON s1.field_id = fields.field_id
 AND s1.start_date = weeks.week_start
LEFT JOIN timeseries.s2_indices AS s2
  ON s2.field_id = fields.field_id
 AND s2.start_date = weeks.week_start
ORDER BY fields.field_id, weeks.week_start
"""


def export_batch_input(
    args: argparse.Namespace,
    manifest: dict[str, Any],
    batch: dict[str, Any],
    fields: pd.DataFrame,
    output_path: Path,
) -> dict[str, Any]:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    partial = output_path.with_suffix(output_path.suffix + ".partial")
    partial.unlink(missing_ok=True)
    query = batch_select_sql(
        fields,
        manifest["source"]["date_from"],
        manifest["source"]["date_to"],
    )
    copy_query = (
        "COPY (" + query.strip().rstrip(";") + ") "
        "TO STDOUT WITH (FORMAT CSV, NULL '__PARQUET_NULL__')"
    )
    process = subprocess.Popen(
        psql_command(args) + ["-q"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    if process.stdin is None or process.stdout is None or process.stderr is None:
        process.kill()
        raise RuntimeError("Could not open psql export pipes")
    sql_script = (
        "SET enable_seqscan=off; "
        "SET enable_mergejoin=off; "
        "SET enable_hashjoin=off;\n"
        + copy_query
        + ";\n"
    )
    process.stdin.write(sql_script.encode())
    process.stdin.close()
    metadata = {
        b"runner_version": RUNNER_VERSION.encode(),
        b"batch_id": batch["batch_id"].encode(),
        b"county": batch["county"].encode(),
        b"date_from": manifest["source"]["date_from"].encode(),
        b"date_to": manifest["source"]["date_to"].encode(),
        b"missing_values": b"native Parquet null; no imputation",
    }
    schema = INPUT_SCHEMA.with_metadata(metadata)
    writer = pq.ParquetWriter(
        partial,
        schema,
        compression="zstd",
        use_dictionary=True,
        write_statistics=True,
        version="2.6",
    )
    rows = 0
    try:
        reader = pacsv.open_csv(
            process.stdout,
            read_options=pacsv.ReadOptions(
                column_names=INPUT_COLUMNS,
                block_size=16 * 1024 * 1024,
                use_threads=True,
            ),
            parse_options=pacsv.ParseOptions(newlines_in_values=False),
            convert_options=pacsv.ConvertOptions(
                column_types={field.name: field.type for field in INPUT_SCHEMA},
                null_values=["__PARQUET_NULL__"],
                strings_can_be_null=True,
            ),
        )
        for record_batch in reader:
            table = pa.Table.from_batches([record_batch], schema=schema)
            writer.write_table(table, row_group_size=100_000)
            rows += record_batch.num_rows
        process.stdout.close()
        return_code = process.wait()
        stderr = process.stderr.read().decode(errors="replace")
        if return_code:
            raise RuntimeError(f"psql batch export failed ({return_code}):\n{stderr}")
    except Exception:
        process.terminate()
        process.wait(timeout=30)
        writer.close()
        partial.unlink(missing_ok=True)
        raise
    writer.close()
    expected_rows = len(fields) * manifest["weeks"]
    if rows != expected_rows:
        partial.unlink(missing_ok=True)
        raise RuntimeError(f"Exported {rows:,} rows; expected {expected_rows:,}")
    os.replace(partial, output_path)
    return {
        "rows": rows,
        "fields": len(fields),
        "weeks": manifest["weeks"],
        "bytes": output_path.stat().st_size,
        "sha256": sha256_file(output_path),
    }


def score_batch(
    package_dir: Path,
    input_path: Path,
    output_path: Path,
    expected_fields: list[str],
    expected_weeks: int,
) -> dict[str, Any]:
    Scorer = model_imports(package_dir)
    raw = pd.read_parquet(input_path, columns=INPUT_COLUMNS)
    expected_rows = len(expected_fields) * expected_weeks
    if len(raw) != expected_rows:
        raise RuntimeError(f"Input has {len(raw):,} rows; expected {expected_rows:,}")
    if raw.duplicated(["field_id", "week_start"]).any():
        raise RuntimeError("Input contains duplicate field-week keys")
    actual_fields = set(raw["field_id"].unique())
    if actual_fields != set(expected_fields):
        raise RuntimeError("Input field IDs differ from the batch manifest")
    week_counts = raw.groupby("field_id", sort=False).size()
    if not (week_counts == expected_weeks).all():
        raise RuntimeError("At least one input field does not have the complete weekly history")

    result = Scorer(package_dir / "Models" / "model_config.json").score(raw)
    del raw
    gc.collect()
    for column in OUTPUT_COLUMNS:
        if column not in result.columns:
            result[column] = np.nan
    result = result[OUTPUT_COLUMNS].sort_values(["field_id", "week_start"]).reset_index(drop=True)
    if len(result) != expected_rows:
        raise RuntimeError(f"Model returned {len(result):,} rows; expected {expected_rows:,}")
    duplicate_keys = int(result.duplicated(["field_id", "week_start"]).sum())
    if duplicate_keys:
        raise RuntimeError(f"Model output has {duplicate_keys} duplicate field-week keys")
    reasons = set(result["reason"].dropna().unique())
    unexpected_reasons = sorted(reasons - ALLOWED_REASONS)
    if unexpected_reasons:
        raise RuntimeError(f"Unexpected model reasons: {unexpected_reasons}")
    probability_columns = ["p_maize", "p_beans", "p_potatoes"]
    has_probability = result[probability_columns].notna().any(axis=1)
    probability_sums = result.loc[has_probability, probability_columns].sum(axis=1)
    bad_probability_sums = int(((probability_sums - 1.0).abs() > 1e-5).sum())
    if bad_probability_sums:
        raise RuntimeError(f"Found {bad_probability_sums} probability rows that do not sum to one")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    partial = output_path.with_suffix(output_path.suffix + ".partial")
    partial.unlink(missing_ok=True)
    result.to_parquet(partial, index=False, compression="zstd", engine="pyarrow")
    os.replace(partial, output_path)

    label_counts = {str(k): int(v) for k, v in result["label"].value_counts(dropna=False).items()}
    reason_counts = {str(k): int(v) for k, v in result["reason"].value_counts(dropna=False).items()}
    county_counts = {str(k): int(v) for k, v in result["county"].value_counts(dropna=False).items()}
    supported = result[result["reason"].isin(["classified", "below_threshold"])]
    horizon = {}
    for county, group in supported.groupby("county"):
        horizon[str(county)] = {
            "min": float(group["week_in_season"].min()),
            "max": float(group["week_in_season"].max()),
            "nunique": int(group["week_in_season"].nunique()),
        }
    report = {
        "rows": len(result),
        "fields": int(result["field_id"].nunique()),
        "weeks": int(result["week_start"].nunique()),
        "duplicate_keys": duplicate_keys,
        "unknown_county": int((result["reason"] == "unknown_county").sum()),
        "out_of_range": int((result["reason"] == "out_of_range").sum()),
        "bad_probability_sums": bad_probability_sums,
        "label_counts": label_counts,
        "reason_counts": reason_counts,
        "county_counts": county_counts,
        "supported_horizons": horizon,
        "bytes": output_path.stat().st_size,
        "sha256": sha256_file(output_path),
    }
    del result
    gc.collect()
    return report


def run_batch(args: argparse.Namespace) -> None:
    run_dir = args.run_dir.resolve()
    manifest = load_manifest(run_dir)
    ensure_source_unchanged(args, manifest)
    batch = batch_by_id(manifest, args.batch_id)
    paths = run_paths(run_dir, batch)
    if paths["status"].exists() and not args.force:
        status = json.loads(paths["status"].read_text())
        if status.get("status") == "passed" and paths["output"].exists():
            log(f"SKIP {args.batch_id}: validated output already exists")
            return
        log(
            f"RETRY {args.batch_id}: previous state is {status.get('status', 'unknown')}; "
            "the validated input will be reused when available"
        )
        paths["status"].unlink(missing_ok=True)
        paths["output"].unlink(missing_ok=True)
    if args.force:
        for key in ("input", "output", "status"):
            paths[key].unlink(missing_ok=True)

    fields = pd.read_parquet(run_dir / "fields.parquet")
    fields = fields[fields["batch_id"] == args.batch_id].copy()
    if len(fields) != batch["fields"]:
        raise RuntimeError(f"Batch field count mismatch for {args.batch_id}")
    started = time.monotonic()
    status: dict[str, Any] = {
        "runner_version": RUNNER_VERSION,
        "batch_id": args.batch_id,
        "batch_index": batch["batch_index"],
        "county": batch["county"],
        "started_at_utc": now_utc(),
        "status": "running",
    }
    try:
        log(f"{args.batch_id}: exporting {len(fields):,} fields x {manifest['weeks']:,} weeks")
        if paths["input"].exists():
            parquet = pq.ParquetFile(paths["input"])
            expected_rows = len(fields) * manifest["weeks"]
            if parquet.metadata.num_rows != expected_rows or parquet.schema_arrow.names != INPUT_COLUMNS:
                raise RuntimeError(f"Existing batch input failed structural validation: {paths['input']}")
            input_report = {
                "rows": parquet.metadata.num_rows,
                "fields": len(fields),
                "weeks": manifest["weeks"],
                "bytes": paths["input"].stat().st_size,
                "sha256": sha256_file(paths["input"]),
                "reused": True,
            }
        else:
            input_report = export_batch_input(args, manifest, batch, fields, paths["input"])
            input_report["reused"] = False
        log(
            f"{args.batch_id}: exported {input_report['rows']:,} rows, "
            f"{input_report['bytes'] / 1024**2:.1f} MiB"
        )
        log(f"{args.batch_id}: scoring with model {manifest['package']['model_version']}")
        output_report = score_batch(
            Path(manifest["package"]["package_dir"]),
            paths["input"],
            paths["output"],
            fields["field_id"].tolist(),
            manifest["weeks"],
        )
        status.update(
            {
                "status": "passed",
                "completed_at_utc": now_utc(),
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "input": input_report,
                "output": output_report,
                "input_file": str(paths["input"]),
                "output_file": str(paths["output"]),
            }
        )
        atomic_json(paths["status"], status)
        log(
            f"{args.batch_id}: PASSED {output_report['rows']:,} rows in "
            f"{status['elapsed_seconds']:.1f}s"
        )
    except Exception as exc:
        status.update(
            {
                "status": "failed",
                "failed_at_utc": now_utc(),
                "elapsed_seconds": round(time.monotonic() - started, 3),
                "error": f"{type(exc).__name__}: {exc}",
            }
        )
        atomic_json(paths["status"], status)
        raise


def selected_batches(manifest: dict[str, Any], from_index: int, to_index: int | None):
    batches = manifest["batches"]
    end = len(batches) - 1 if to_index is None else to_index
    if from_index < 0 or end >= len(batches) or from_index > end:
        raise ValueError(f"Invalid batch index range {from_index}..{end}; available 0..{len(batches)-1}")
    return [b for b in batches if from_index <= b["batch_index"] <= end]


def run_all(args: argparse.Namespace) -> None:
    run_dir = args.run_dir.resolve()
    manifest = load_manifest(run_dir)
    ensure_source_unchanged(args, manifest)
    batches = selected_batches(manifest, args.from_index, args.to_index)
    log(f"Running {len(batches)} batches sequentially")
    for position, batch in enumerate(batches, start=1):
        paths = run_paths(run_dir, batch)
        if paths["status"].exists() and paths["output"].exists() and not args.force:
            status = json.loads(paths["status"].read_text())
            if status.get("status") == "passed":
                log(f"[{position}/{len(batches)}] SKIP {batch['batch_id']}: passed")
                continue
        log(f"[{position}/{len(batches)}] START {batch['batch_id']}")
        paths["log"].parent.mkdir(parents=True, exist_ok=True)
        command = [
            sys.executable,
            str(Path(__file__).resolve()),
            "run-batch",
            "--run-dir",
            str(run_dir),
            "--batch-id",
            batch["batch_id"],
            "--database",
            args.database,
            "--psql-command",
            args.psql_command,
        ]
        if args.force:
            command.append("--force")
        with paths["log"].open("w") as handle:
            result = subprocess.run(command, stdout=handle, stderr=subprocess.STDOUT)
        if result.returncode:
            tail = paths["log"].read_text(errors="replace").splitlines()[-20:]
            raise RuntimeError(
                f"Batch {batch['batch_id']} failed; see {paths['log']}\n" + "\n".join(tail)
            )
        log(f"[{position}/{len(batches)}] DONE {batch['batch_id']}")
    log("Selected batches completed")


def status_summary(run_dir: Path, manifest: dict[str, Any]) -> dict[str, Any]:
    result = {
        "run_dir": str(run_dir),
        "batch_count": manifest["batch_count"],
        "passed": 0,
        "failed": 0,
        "running_or_incomplete": 0,
        "pending": 0,
        "passed_rows": 0,
        "failed_batches": [],
        "pending_batches": [],
    }
    for batch in manifest["batches"]:
        paths = run_paths(run_dir, batch)
        if not paths["status"].exists():
            result["pending"] += 1
            result["pending_batches"].append(batch["batch_id"])
            continue
        item = json.loads(paths["status"].read_text())
        state = item.get("status")
        if state == "passed" and paths["output"].exists():
            result["passed"] += 1
            result["passed_rows"] += int(item["output"]["rows"])
        elif state == "failed":
            result["failed"] += 1
            result["failed_batches"].append(
                {"batch_id": batch["batch_id"], "error": item.get("error")}
            )
        else:
            result["running_or_incomplete"] += 1
    return result


def show_status(args: argparse.Namespace) -> None:
    run_dir = args.run_dir.resolve()
    manifest = load_manifest(run_dir)
    print(json.dumps(json_ready(status_summary(run_dir, manifest)), indent=2))


def merge_counts(target: Counter[str], values: dict[str, int]) -> None:
    for key, value in values.items():
        target[str(key)] += int(value)


def validate(args: argparse.Namespace) -> None:
    run_dir = args.run_dir.resolve()
    manifest = load_manifest(run_dir)
    ensure_source_unchanged(args, manifest)
    progress = status_summary(run_dir, manifest)
    failures = []
    warnings = []
    if progress["passed"] != manifest["batch_count"]:
        failures.append(
            f"passed batches {progress['passed']} != expected {manifest['batch_count']}"
        )
    if progress["failed"]:
        failures.append(f"failed batches: {progress['failed_batches']}")
    if progress["pending"] or progress["running_or_incomplete"]:
        failures.append(
            f"pending={progress['pending']}, incomplete={progress['running_or_incomplete']}"
        )

    labels: Counter[str] = Counter()
    reasons: Counter[str] = Counter()
    county_rows: Counter[str] = Counter()
    supported_min: dict[str, float] = {}
    supported_max: dict[str, float] = {}
    supported_values: dict[str, set[int]] = defaultdict(set)
    output_files = []
    total_output_bytes = 0
    total_input_bytes = 0
    unknown_county = 0
    out_of_range = 0
    bad_probability_sums = 0
    duplicate_keys = 0
    for batch in manifest["batches"]:
        paths = run_paths(run_dir, batch)
        if not paths["status"].exists():
            continue
        item = json.loads(paths["status"].read_text())
        if item.get("status") != "passed":
            continue
        output = item["output"]
        expected_rows = batch["fields"] * manifest["weeks"]
        if output["rows"] != expected_rows:
            failures.append(
                f"{batch['batch_id']} rows {output['rows']} != expected {expected_rows}"
            )
        if output["fields"] != batch["fields"]:
            failures.append(
                f"{batch['batch_id']} fields {output['fields']} != expected {batch['fields']}"
            )
        if output["weeks"] != manifest["weeks"]:
            failures.append(
                f"{batch['batch_id']} weeks {output['weeks']} != expected {manifest['weeks']}"
            )
        if not paths["output"].exists():
            failures.append(f"Missing output: {paths['output']}")
            continue
        parquet = pq.ParquetFile(paths["output"])
        if parquet.metadata.num_rows != output["rows"]:
            failures.append(f"Parquet metadata row mismatch: {paths['output']}")
        output_files.append(str(paths["output"]))
        total_output_bytes += paths["output"].stat().st_size
        total_input_bytes += paths["input"].stat().st_size
        duplicate_keys += int(output["duplicate_keys"])
        unknown_county += int(output["unknown_county"])
        out_of_range += int(output["out_of_range"])
        bad_probability_sums += int(output["bad_probability_sums"])
        merge_counts(labels, output["label_counts"])
        merge_counts(reasons, output["reason_counts"])
        merge_counts(county_rows, output["county_counts"])
        for county, horizon in output["supported_horizons"].items():
            supported_min[county] = min(supported_min.get(county, horizon["min"]), horizon["min"])
            supported_max[county] = max(supported_max.get(county, horizon["max"]), horizon["max"])
            supported_values[county].update(
                range(int(horizon["min"]), int(horizon["max"]) + 1)
            )

    total_rows = sum(county_rows.values())
    if total_rows != manifest["expected_rows"]:
        failures.append(f"total rows {total_rows} != expected {manifest['expected_rows']}")
    if progress["passed_rows"] != manifest["expected_rows"]:
        failures.append(
            f"checkpoint rows {progress['passed_rows']} != expected {manifest['expected_rows']}"
        )
    if duplicate_keys:
        failures.append(f"duplicate keys: {duplicate_keys}")
    if unknown_county:
        failures.append(f"unknown county rows: {unknown_county}")
    if out_of_range:
        warnings.append(
            f"model safely abstained on {out_of_range} rows with reason=out_of_range; "
            "the scorer replaced offending raw values with missing before feature construction"
        )
    if bad_probability_sums:
        failures.append(f"bad probability sums: {bad_probability_sums}")
    expected_county_rows = {
        county: manifest["fields_by_county"][county] * manifest["weeks"] for county in COUNTIES
    }
    if dict(county_rows) != expected_county_rows:
        failures.append(
            f"county rows {dict(county_rows)} != expected {expected_county_rows}"
        )

    config = json.loads(
        (Path(manifest["package"]["package_dir"]) / "Models" / "model_config.json").read_text()
    )
    expected_horizons = {
        county: {
            "min": int(spec["feature_ranges"]["week_in_season"][0]),
            "max": int(spec["feature_ranges"]["week_in_season"][1]),
        }
        for county, spec in config["classifiers"].items()
    }
    supported_horizons = {
        county: {
            "min": supported_min.get(county),
            "max": supported_max.get(county),
            "nunique": len(supported_values.get(county, set())),
        }
        for county in COUNTIES
    }
    for county, expected in expected_horizons.items():
        actual = supported_horizons[county]
        expected_nunique = expected["max"] - expected["min"] + 1
        if (
            actual["min"] != expected["min"]
            or actual["max"] != expected["max"]
            or actual["nunique"] != expected_nunique
        ):
            failures.append(
                f"{county} supported horizons {actual} != expected "
                f"{expected | {'nunique': expected_nunique}}"
            )

    report = {
        "runner_version": RUNNER_VERSION,
        "validated_at_utc": now_utc(),
        "run_dir": str(run_dir),
        "status": "passed" if not failures else "failed",
        "failures": failures,
        "warnings": warnings,
        "fields": manifest["fields"],
        "fields_by_county": manifest["fields_by_county"],
        "weeks": manifest["weeks"],
        "rows": total_rows,
        "expected_rows": manifest["expected_rows"],
        "batches": progress,
        "duplicate_keys": duplicate_keys,
        "unknown_county": unknown_county,
        "out_of_range": out_of_range,
        "bad_probability_sums": bad_probability_sums,
        "label_counts": dict(labels),
        "reason_counts": dict(reasons),
        "county_rows": dict(county_rows),
        "supported_horizons": supported_horizons,
        "input_bytes": total_input_bytes,
        "output_bytes": total_output_bytes,
        "output_files": output_files,
    }
    report_path = run_dir / "validation_report.json"
    atomic_json(report_path, report)
    log(json.dumps(json_ready(report), indent=2))
    if failures:
        raise RuntimeError(f"Production validation failed; see {report_path}")
    log(f"Production validation PASSED: {report_path}")


def add_database_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--database", default="kenya_monitoring")
    parser.add_argument("--psql-command", default="sudo -u postgres psql")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_prepare = sub.add_parser("prepare", help="inventory fields and create a resumable batch plan")
    p_prepare.add_argument("--run-dir", type=Path, required=True)
    p_prepare.add_argument("--package-dir", type=Path, required=True)
    p_prepare.add_argument("--batch-size", type=int, default=5_000)
    p_prepare.add_argument("--expected-fields", type=int, default=269_453)
    p_prepare.add_argument("--force", action="store_true")
    add_database_args(p_prepare)

    p_batch = sub.add_parser("run-batch", help="export and score one field batch")
    p_batch.add_argument("--run-dir", type=Path, required=True)
    p_batch.add_argument("--batch-id", required=True)
    p_batch.add_argument("--force", action="store_true")
    add_database_args(p_batch)

    p_all = sub.add_parser("run-all", help="run all pending batches sequentially")
    p_all.add_argument("--run-dir", type=Path, required=True)
    p_all.add_argument("--from-index", type=int, default=0)
    p_all.add_argument("--to-index", type=int)
    p_all.add_argument("--force", action="store_true")
    add_database_args(p_all)

    p_status = sub.add_parser("status", help="show resumable production progress")
    p_status.add_argument("--run-dir", type=Path, required=True)

    p_validate = sub.add_parser("validate", help="validate all batch outputs")
    p_validate.add_argument("--run-dir", type=Path, required=True)
    add_database_args(p_validate)

    args = parser.parse_args()
    if getattr(args, "batch_size", 1) <= 0:
        parser.error("--batch-size must be positive")
    return args


def main() -> None:
    args = parse_args()
    if args.command == "prepare":
        prepare(args)
    elif args.command == "run-batch":
        run_batch(args)
    elif args.command == "run-all":
        run_all(args)
    elif args.command == "status":
        show_status(args)
    elif args.command == "validate":
        validate(args)
    else:
        raise AssertionError(args.command)


if __name__ == "__main__":
    main()
