#!/usr/bin/env python3
from __future__ import annotations

import argparse
import io
import json
import os
import shlex
import shutil
import subprocess
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
import psycopg2
import rasterio
import requests
from shapely import from_wkt
from exactextract import exact_extract
from rasterio.transform import rowcol
from affine import Affine

from .core import (
    CROP_PARAMETERS,
    METHOD_VERSION,
    NPP_TO_TBP,
    PHENOLOGY_VERSION,
    calculate_yield,
    dekads_between,
    derive_phenology,
    integrate_field_npp,
    season_bounds,
)


WAPOR_COLLECTION = "projects/UNFAO/wapor/v3/L2-NPP-D"
WAPOR_URL = (
    "https://storage.googleapis.com/fao-gismgr-wapor-3-data/"
    "DATA/WAPOR-3/MAPSET/L2-NPP-D/{code}.tif"
)
PARAMETER_SET_ID = "kenya-yield-starting-v1"


def log(message: str) -> None:
    print(f"[{datetime.now().astimezone().isoformat(timespec='seconds')}] {message}", flush=True)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, default=str) + "\n")


def normalize_dsn(value: str) -> str:
    return value.replace("postgresql+psycopg://", "postgresql://", 1)


def load_field_ids(path: Path) -> list[str]:
    values = [line.strip() for line in path.read_text().splitlines() if line.strip()]
    values = list(dict.fromkeys(values))
    if not values:
        raise ValueError(f"No field IDs in {path}")
    return values


def read_sql(conn, query: str, params: tuple[Any, ...]) -> pd.DataFrame:
    return pd.read_sql_query(query, conn, params=params)


def psql_copy_frame(psql_command: str, query: str) -> pd.DataFrame:
    command = [*shlex.split(psql_command), "-X", "-q", "-v", "ON_ERROR_STOP=1"]
    script = f"COPY ({query}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);\n"
    completed = subprocess.run(
        command,
        input=script,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode:
        raise RuntimeError(f"psql export failed: {completed.stderr.strip()}")
    return pd.read_csv(io.StringIO(completed.stdout))


def sql_field_ids(field_ids: list[str]) -> str:
    for value in field_ids:
        if not value or any(char not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-" for char in value):
            raise ValueError(f"Unsafe field ID: {value!r}")
    return ",".join("'" + value + "'" for value in field_ids)


def prepare_with_psql(
    psql_command: str,
    field_ids: list[str],
    season_code: str,
    start: date,
    end: date,
    model_run_id: str | None,
) -> tuple[str, gpd.GeoDataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    ids = sql_field_ids(field_ids)
    if model_run_id:
        safe_run = model_run_id.replace("'", "''")
        run_expression = f"'{safe_run}'"
    else:
        run_expression = "(SELECT model_run_id FROM dashboard.model_runs WHERE is_active ORDER BY loaded_at DESC LIMIT 1)"
    fields_raw = psql_copy_frame(
        psql_command,
        f"""
        SELECT field_id, county, area_ha,
               ST_AsText(ST_Transform(geom, 4326)) AS geometry_wkt
        FROM dashboard.fields
        WHERE field_id IN ({ids})
        ORDER BY field_id
        """,
    )
    fields = gpd.GeoDataFrame(
        fields_raw.drop(columns="geometry_wkt"),
        geometry=from_wkt(fields_raw["geometry_wkt"].to_numpy()),
        crs=4326,
    )
    seasonal = psql_copy_frame(
        psql_command,
        f"""
        SELECT * FROM dashboard.seasonal_predictions
        WHERE model_run_id = {run_expression}
          AND season_code = '{season_code}' AND field_id IN ({ids})
        ORDER BY field_id
        """,
    )
    s2 = psql_copy_frame(
        psql_command,
        f"""
        SELECT field_id, start_date AS week_start,
               ndvi_median, ndvi_count, evi_median, evi_count,
               lai_median, lai_count, pixel_count_after, buffer_width_m
        FROM timeseries.s2_indices
        WHERE field_id IN ({ids}) AND start_date BETWEEN '{start}' AND '{end}'
        ORDER BY field_id, start_date
        """,
    )
    s1 = psql_copy_frame(
        psql_command,
        f"""
        SELECT field_id, start_date AS week_start,
               rvi_median, rvi_count, pixel_count_after, buffer_width_m
        FROM timeseries.s1_indices
        WHERE field_id IN ({ids}) AND start_date BETWEEN '{start}' AND '{end}'
        ORDER BY field_id, start_date
        """,
    )
    if seasonal.empty:
        resolved_run = model_run_id or "unknown"
    else:
        resolved_run = str(seasonal["model_run_id"].iloc[0])
    return resolved_run, fields, seasonal, s2, s1


def survey_values(row: pd.Series, season_kind: str) -> tuple[str, str, str]:
    def text(name: str) -> str:
        value = row.get(name)
        if value is None or pd.isna(value):
            return ""
        return str(value).strip()

    suffix = "_2" if season_kind == "lr" else ""
    patterns = " ".join(
        filter(None, [text(f"cropping_pattern{suffix}"), text(f"lngsht_cropping_pattern{suffix}")])
    ).lower()
    mixed = " ".join(
        filter(None, [text(f"croptype_mixed{suffix}"), text(f"lngsht_croptype_mixed{suffix}")])
    ).strip()
    mono = " ".join(
        filter(None, [text(f"croptype_mono{suffix}"), text(f"lngsht_croptype_mono{suffix}")])
    ).strip()
    return patterns, mixed, mono


def classify_intercrop(row: pd.Series, season_kind: str = "lr") -> str:
    patterns, mixed, mono = survey_values(row, season_kind)
    if mixed or "mix" in patterns or "intercrop" in patterns:
        return "yes"
    if mono or "mono" in patterns or "single" in patterns:
        return "no"
    return "unknown"


def survey_crop_matches(predicted_crop: str, survey_crop_text: Any) -> bool | None:
    if survey_crop_text is None or pd.isna(survey_crop_text) or not str(survey_crop_text).strip():
        return None
    normalized = str(survey_crop_text).lower().replace("_", " ").replace("-", " ")
    aliases = {
        "maize": ("maize", "corn"),
        "beans": ("bean", "beans"),
        "potatoes": ("potato", "potatoes"),
    }
    return any(alias in normalized.split() for alias in aliases.get(predicted_crop, (predicted_crop,)))


def read_survey_intercrop(gpkg: Path, field_ids: list[str], season_kind: str) -> pd.DataFrame:
    if not gpkg.exists():
        return pd.DataFrame(columns=["field_id", "intercrop_status"])
    quoted = ",".join("'" + value.replace("'", "''") + "'" for value in field_ids)
    frames: list[pd.DataFrame] = []
    for layer in ("machakos_fin", "nakuru_fin"):
        try:
            frame = gpd.read_file(gpkg, layer=layer, where=f"field_id IN ({quoted})")
        except Exception as exc:
            log(f"Survey attributes unavailable for {layer}: {exc}")
            continue
        if frame.empty:
            continue
        values = frame.apply(lambda row: survey_values(row, season_kind), axis=1)
        frame["survey_cropping_pattern"] = [value[0] or None for value in values]
        frame["survey_crop_text"] = [value[1] or value[2] or None for value in values]
        frame["intercrop_status"] = frame.apply(
            lambda row: classify_intercrop(row, season_kind), axis=1
        )
        frame["survey_season_kind"] = season_kind
        frames.append(
            frame[
                ["field_id", "intercrop_status", "survey_cropping_pattern", "survey_crop_text", "survey_season_kind"]
            ]
        )
    if not frames:
        return pd.DataFrame(columns=["field_id", "intercrop_status"])
    return pd.concat(frames, ignore_index=True).drop_duplicates("field_id", keep="last")


def prepare(args: argparse.Namespace) -> None:
    run_dir = args.run_dir
    inputs = run_dir / "inputs"
    inputs.mkdir(parents=True, exist_ok=True)
    field_ids = load_field_ids(args.field_list)
    start, end = season_bounds(args.season_code)
    log(f"Reading {len(field_ids):,} requested fields for {args.season_code}")
    if args.psql_command:
        run_id, fields, seasonal, s2, s1 = prepare_with_psql(
            args.psql_command, field_ids, args.season_code, start, end, args.model_run_id
        )
    else:
        dsn = normalize_dsn(args.database_url)
        with psycopg2.connect(dsn) as conn:
            run_id = args.model_run_id
            if not run_id:
                row = read_sql(
                    conn,
                    "SELECT model_run_id FROM dashboard.model_runs WHERE is_active ORDER BY loaded_at DESC LIMIT 1",
                    (),
                )
                if row.empty:
                    raise RuntimeError("No active dashboard model run")
                run_id = str(row.iloc[0]["model_run_id"])
            fields = gpd.read_postgis(
                "SELECT field_id, county, area_ha, geom FROM dashboard.fields WHERE field_id = ANY(%s) ORDER BY field_id",
                conn,
                geom_col="geom",
                params=(field_ids,),
            )
            seasonal = read_sql(
                conn,
                "SELECT * FROM dashboard.seasonal_predictions WHERE model_run_id = %s AND season_code = %s AND field_id = ANY(%s) ORDER BY field_id",
                (run_id, args.season_code, field_ids),
            )
            s2 = read_sql(
                conn,
                "SELECT field_id, start_date AS week_start, ndvi_median, ndvi_count, evi_median, evi_count, lai_median, lai_count, pixel_count_after, buffer_width_m FROM timeseries.s2_indices WHERE field_id = ANY(%s) AND start_date BETWEEN %s AND %s ORDER BY field_id, start_date",
                (field_ids, start, end),
            )
            s1 = read_sql(
                conn,
                "SELECT field_id, start_date AS week_start, rvi_median, rvi_count, pixel_count_after, buffer_width_m FROM timeseries.s1_indices WHERE field_id = ANY(%s) AND start_date BETWEEN %s AND %s ORDER BY field_id, start_date",
                (field_ids, start, end),
            )
    missing = sorted(set(field_ids) - set(fields["field_id"]))
    if missing:
        raise RuntimeError(f"{len(missing)} requested field IDs are absent; first: {missing[:10]}")
    season_kind = args.season_code.split("_", 1)[1].lower()
    survey = read_survey_intercrop(args.gpkg, field_ids, season_kind) if args.gpkg else pd.DataFrame()
    fields["intercrop_status"] = "unknown"
    if not survey.empty:
        fields = fields.merge(survey, on="field_id", how="left", suffixes=("", "_survey"))
        fields["intercrop_status"] = fields["intercrop_status_survey"].fillna("unknown")
        fields = fields.drop(columns="intercrop_status_survey")
    for column in ("survey_cropping_pattern", "survey_crop_text", "survey_season_kind"):
        if column not in fields:
            fields[column] = None
    fields.to_parquet(inputs / "fields.parquet", index=False)
    seasonal.to_parquet(inputs / "seasonal_predictions.parquet", index=False)
    s2.to_parquet(inputs / "weekly_s2.parquet", index=False)
    s1.to_parquet(inputs / "weekly_s1.parquet", index=False)

    bounds: dict[str, list[float]] = {}
    wgs84 = fields.to_crs(4326)
    for county, group in wgs84.groupby("county"):
        xmin, ymin, xmax, ymax = map(float, group.total_bounds)
        bounds[str(county).lower()] = [xmin - 0.02, ymin - 0.02, xmax + 0.02, ymax + 0.02]
    write_json(run_dir / "aoi_bounds.json", bounds)
    write_json(
        run_dir / "parameter_set.json",
        {
            "parameter_set_id": PARAMETER_SET_ID,
            "status": "experimental-unvalidated",
            "method_version": METHOD_VERSION,
            "npp_to_tbp": NPP_TO_TBP,
            "crops": {name: value.to_dict() for name, value in CROP_PARAMETERS.items()},
        },
    )
    manifest = {
        "run_id": run_dir.name,
        "created_at": datetime.now().astimezone().isoformat(),
        "code_version": subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=Path(__file__).resolve().parents[1],
            text=True,
            capture_output=True,
            check=False,
        ).stdout.strip() or "unknown",
        "season_code": args.season_code,
        "season_start": start,
        "season_end": end,
        "model_run_id": run_id,
        "field_count": len(fields),
        "county_counts": fields.groupby("county")["field_id"].count().to_dict(),
        "seasonal_rows": len(seasonal),
        "s2_rows": len(s2),
        "s1_rows": len(s1),
        "wapor_collection": WAPOR_COLLECTION,
        "method_version": METHOD_VERSION,
        "phenology_version": PHENOLOGY_VERSION,
        "parameter_set_id": PARAMETER_SET_ID,
        "source_files": {
            "field_list": str(args.field_list),
            "gpkg": str(args.gpkg) if args.gpkg else None,
        },
    }
    write_json(run_dir / "run_manifest.json", manifest)
    log(f"Prepared inputs: {len(fields):,} fields, {len(s2):,} S2 rows, {len(s1):,} S1 rows")


def inventory(args: argparse.Namespace) -> None:
    start, end = season_bounds(args.season_code)
    rows = dekads_between(start, end)
    session = requests.Session()
    for i, row in enumerate(rows, 1):
        url = WAPOR_URL.format(code=row["code"])
        try:
            response = session.head(url, timeout=args.timeout, allow_redirects=True)
            response.raise_for_status()
            row.update(
                {
                    "available": True,
                    "url": url,
                    "content_length": int(response.headers.get("content-length", 0)),
                    "etag": response.headers.get("etag", "").strip('"'),
                    "last_modified": response.headers.get("last-modified"),
                }
            )
        except requests.RequestException as exc:
            row.update({"available": False, "url": url, "error": str(exc)})
        log(f"Inventoried WaPOR NPP {i}/{len(rows)}: {row['code']} available={row['available']}")
    frame = pd.DataFrame(rows)
    out = args.run_dir / "inventory" / "wapor_l2_npp_d.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(out, index=False)
    summary = {
        "collection": WAPOR_COLLECTION,
        "unit": "gC/m²/day",
        "scale_factor": 0.001,
        "offset": 0.0,
        "nodata": -9999,
        "crs": "EPSG:4326",
        "pixel_size_degrees": 0.0009765625,
        "requested_periods": len(frame),
        "available_periods": int(frame["available"].sum()),
        "missing_codes": frame.loc[~frame["available"], "code"].tolist(),
        "observed_at": datetime.now().astimezone().isoformat(),
    }
    write_json(args.run_dir / "inventory" / "summary.json", summary)
    if summary["missing_codes"]:
        raise RuntimeError(f"Missing {len(summary['missing_codes'])} requested WaPOR periods")


def validate_raster(path: Path) -> dict[str, Any]:
    with rasterio.open(path) as src:
        if src.crs.to_epsg() != 4326:
            raise RuntimeError(f"Unexpected CRS for {path}: {src.crs}")
        if src.count != 1 or src.dtypes[0] != "int16":
            raise RuntimeError(f"Unexpected raster layout for {path}")
        if src.nodata != -9999:
            raise RuntimeError(f"Unexpected nodata for {path}: {src.nodata}")
        if abs(src.scales[0] - 0.001) > 1e-12 or abs(src.offsets[0]) > 1e-12:
            raise RuntimeError(f"Unexpected scale/offset for {path}: {src.scales}/{src.offsets}")
        return {
            "width": src.width,
            "height": src.height,
            "bounds": list(src.bounds),
            "scale": src.scales[0],
            "offset": src.offsets[0],
            "nodata": src.nodata,
            "size_bytes": path.stat().st_size,
        }


def clip(args: argparse.Namespace) -> None:
    inventory_path = args.run_dir / "inventory" / "wapor_l2_npp_d.csv"
    bounds_path = args.run_dir / "aoi_bounds.json"
    if not inventory_path.exists() or not bounds_path.exists():
        raise RuntimeError("Run prepare and inventory before clip")
    inventory_frame = pd.read_csv(inventory_path)
    bounds = json.loads(bounds_path.read_text())
    if args.county:
        bounds = {args.county.lower(): bounds[args.county.lower()]}
    records: list[dict[str, Any]] = []
    total = len(inventory_frame) * len(bounds)
    number = 0
    for county, (xmin, ymin, xmax, ymax) in bounds.items():
        county_dir = args.run_dir / "rasters" / county
        county_dir.mkdir(parents=True, exist_ok=True)
        for row in inventory_frame.itertuples(index=False):
            number += 1
            output = county_dir / f"{row.code}.tif"
            if output.exists():
                try:
                    info = validate_raster(output)
                    records.append({"county": county, "code": row.code, "path": str(output), **info})
                    log(f"Clip {number}/{total} exists and valid: {output.name} ({county})")
                    continue
                except Exception:
                    output.unlink()
            temp = output.with_suffix(".tif.partial")
            temp.unlink(missing_ok=True)
            source = "/vsicurl/" + row.url
            command = [
                args.gdal_translate,
                "-q",
                "-projwin", str(xmin), str(ymax), str(xmax), str(ymin),
                "-of", "COG",
                "-co", "COMPRESS=ZSTD",
                source,
                str(temp),
            ]
            env = os.environ.copy()
            env.update(
                {
                    "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
                    "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".tif",
                }
            )
            log(f"Clip {number}/{total}: {row.code} ({county})")
            subprocess.run(command, check=True, env=env)
            temp.replace(output)
            info = validate_raster(output)
            records.append({"county": county, "code": row.code, "path": str(output), **info})
    pd.DataFrame(records).to_csv(args.run_dir / "inventory" / "clipped_rasters.csv", index=False)
    log(f"Clipped and validated {len(records)} county-period rasters")


def source_pixel_count(values, coverage):
    return int(np.count_nonzero(~np.ma.getmaskarray(values)))


def effective_pixel_count(values, coverage):
    mask = np.ma.getmaskarray(values)
    return float(np.asarray(coverage)[~mask].sum())


def valid_coverage_fraction(values, coverage):
    mask = np.ma.getmaskarray(values)
    coverage = np.asarray(coverage)
    denominator = coverage.sum()
    return float(coverage[~mask].sum() / denominator) if denominator else np.nan


def extract_npp(args: argparse.Namespace) -> None:
    fields_path = args.run_dir / "inputs" / "fields.parquet"
    inventory_path = args.run_dir / "inventory" / "wapor_l2_npp_d.csv"
    if not fields_path.exists() or not inventory_path.exists():
        raise RuntimeError("Run prepare and inventory before extract-npp")
    fields = gpd.read_parquet(fields_path).to_crs(4326)
    inventory_frame = pd.read_csv(inventory_path)
    inventory_frame["period_start"] = pd.to_datetime(inventory_frame["period_start"])
    inventory_frame["period_end"] = pd.to_datetime(inventory_frame["period_end"])
    parts: list[pd.DataFrame] = []
    total = len(inventory_frame) * fields["county"].nunique()
    number = 0
    for county, county_fields in fields.groupby("county"):
        county_key = str(county).lower()
        vector = county_fields[["field_id", "geometry"]].copy()
        representative = vector.geometry.representative_point()
        global_transform = Affine(0.0009765625, 0.0, -180.0, 0.0, -0.0009765625, 60.0)
        global_cells = {}
        for field_id, point in zip(vector["field_id"], representative):
            grid_row, grid_col = rowcol(global_transform, point.x, point.y)
            global_cells[field_id] = int(grid_row) * 368640 + int(grid_col)
        for meta in inventory_frame.itertuples(index=False):
            number += 1
            raster_path = args.run_dir / "rasters" / county_key / f"{meta.code}.tif"
            if not raster_path.exists():
                raise FileNotFoundError(raster_path)
            with rasterio.open(raster_path) as src:
                scale, offset = src.scales[0], src.offsets[0]
            result = exact_extract(
                str(raster_path),
                vector,
                ["mean", source_pixel_count, effective_pixel_count, valid_coverage_fraction],
                include_cols=["field_id"],
                output="pandas",
                strategy="feature-sequential",
            )
            result = result.rename(
                columns={
                    "mean": "npp_raw_mean",
                    "valid_coverage_fraction": "coverage_fraction",
                }
            )
            result["npp_rate_gC_m2_day"] = result["npp_raw_mean"] * scale + offset
            result.loc[result["npp_raw_mean"].isna(), "npp_rate_gC_m2_day"] = np.nan
            result["period_start"] = meta.period_start
            result["period_end"] = meta.period_end
            result["days_in_period"] = int(meta.days_in_period)
            result["county"] = county
            result["representative_source_cell_id"] = result["field_id"].map(global_cells)
            result["wapor_code"] = meta.code
            result["source_raster"] = str(raster_path)
            result["scale_factor"] = scale
            parts.append(result)
            log(f"Extracted NPP {number}/{total}: {meta.code} ({county}, {len(result):,} fields)")
    output = pd.concat(parts, ignore_index=True)
    output = output.sort_values(["field_id", "period_start"]).reset_index(drop=True)
    path = args.run_dir / "features" / "field_dekad_npp.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    output.to_parquet(path, index=False)
    log(f"Wrote {len(output):,} field-dekad NPP rows to {path}")


def build_weekly_input(s2: pd.DataFrame, s1: pd.DataFrame) -> pd.DataFrame:
    keep_s2 = [
        c for c in ("field_id", "week_start", "ndvi_median", "evi_median", "lai_median")
        if c in s2
    ]
    keep_s1 = [c for c in ("field_id", "week_start", "rvi_median") if c in s1]
    return s2[keep_s2].merge(s1[keep_s1], on=["field_id", "week_start"], how="outer")


def calculate(args: argparse.Namespace) -> None:
    start, end = season_bounds(args.season_code)
    fields = gpd.read_parquet(args.run_dir / "inputs" / "fields.parquet").drop(columns="geometry")
    seasonal = pd.read_parquet(args.run_dir / "inputs" / "seasonal_predictions.parquet")
    s2 = pd.read_parquet(args.run_dir / "inputs" / "weekly_s2.parquet")
    s1 = pd.read_parquet(args.run_dir / "inputs" / "weekly_s1.parquet")
    npp = pd.read_parquet(args.run_dir / "features" / "field_dekad_npp.parquet")
    weekly = build_weekly_input(s2, s1)
    representative_cells = npp[["field_id", "representative_source_cell_id"]].drop_duplicates("field_id")
    sharing = representative_cells.groupby("representative_source_cell_id")["field_id"].transform("count")
    representative_cells["representative_source_cell_pilot_field_count"] = sharing.astype(int)

    phenology_rows: list[dict[str, Any]] = []
    for field_id in fields["field_id"]:
        result = derive_phenology(weekly[weekly["field_id"] == field_id], start, end)
        phenology_rows.append({"field_id": field_id, "season_code": args.season_code, **result})
    phenology = pd.DataFrame(phenology_rows)
    phenology_path = args.run_dir / "features" / "field_season_phenology.parquet"
    phenology.to_parquet(phenology_path, index=False)

    base = (
        fields.merge(seasonal, on=["field_id", "county"], how="left")
        .merge(phenology, on="field_id", how="left")
        .merge(representative_cells, on="field_id", how="left")
    )
    output_rows: list[dict[str, Any]] = []
    for row in base.itertuples(index=False):
        crop = str(row.seasonal_label).lower() if pd.notna(row.seasonal_label) else ""
        field_npp = npp[npp["field_id"] == row.field_id]
        metrics = integrate_field_npp(field_npp, row.sos, row.eos)
        narrow_sos = min(row.eos, row.sos + timedelta(days=10))
        narrow_eos = max(narrow_sos, row.eos - timedelta(days=10))
        wide_sos = max(start, row.sos - timedelta(days=10))
        wide_eos = min(end, row.eos + timedelta(days=10))
        narrow = integrate_field_npp(field_npp, narrow_sos, narrow_eos)
        wide = integrate_field_npp(field_npp, wide_sos, wide_eos)
        reasons: list[str] = []
        eligible = True
        if crop not in CROP_PARAMETERS:
            reasons.append("unsupported_or_unclassified_crop")
            eligible = False
        if getattr(row, "status", None) != "final":
            reasons.append(f"seasonal_status_{getattr(row, 'status', 'missing')}")
            eligible = False
        confidence = getattr(row, "seasonal_confidence", np.nan)
        if pd.isna(confidence) or float(confidence) < args.minimum_crop_confidence:
            reasons.append("crop_confidence_below_threshold")
            eligible = False
        if row.intercrop_status == "yes":
            reasons.append("mixed_crop_biomass_only")
            eligible = False
        survey_match = survey_crop_matches(crop, getattr(row, "survey_crop_text", None))
        if row.intercrop_status == "no" and survey_match is False:
            reasons.append("survey_crop_mismatch")
            eligible = False
        if metrics["npp_temporal_coverage"] < args.minimum_npp_coverage:
            reasons.append("low_npp_temporal_coverage")
            eligible = False
        if (
            pd.isna(metrics["npp_mean_spatial_coverage"])
            or metrics["npp_mean_spatial_coverage"] < args.minimum_npp_spatial_coverage
        ):
            reasons.append("low_npp_spatial_coverage")
            eligible = False
        if pd.isna(metrics["seasonal_npp_gC_m2"]):
            reasons.append("missing_npp")
            eligible = False
        tbp = (
            float(metrics["seasonal_npp_gC_m2"]) * NPP_TO_TBP
            if not pd.isna(metrics["seasonal_npp_gC_m2"])
            else np.nan
        )
        record: dict[str, Any] = {
            "run_id": args.run_dir.name,
            "model_run_id": getattr(row, "model_run_id", None),
            "field_id": row.field_id,
            "county": row.county,
            "area_ha": row.area_ha,
            "season_code": args.season_code,
            "season_start": start,
            "season_end": end,
            "crop_label": crop or "unclassified",
            "crop_confidence": confidence,
            "crop_status": getattr(row, "status", None),
            "intercrop_status": row.intercrop_status,
            "survey_crop_text": getattr(row, "survey_crop_text", None),
            "survey_crop_match": survey_match,
            "representative_source_cell_id": row.representative_source_cell_id,
            "representative_source_cell_pilot_field_count": row.representative_source_cell_pilot_field_count,
            "sos": row.sos,
            "peak_date": row.peak_date,
            "eos": row.eos,
            "phenology_method": row.method,
            "phenology_confidence": row.confidence,
            "phenology_qa_status": row.qa_status,
            "optical_valid_weeks": row.optical_valid_weeks,
            "sar_supported_weeks": row.sar_supported_weeks,
            **metrics,
            "tbp_tDM_ha": tbp,
            "parameter_set_id": PARAMETER_SET_ID,
            "method_version": METHOD_VERSION,
            "yield_status": "not_calculated" if not eligible else "calculated_unvalidated",
            "qa_flags": ";".join(reasons),
        }
        if eligible:
            values = calculate_yield(tbp, crop)
            for scenario, value in values.items():
                record[f"yield_{scenario}_t_ha"] = value
                record[f"production_{scenario}_t"] = value * float(row.area_ha)
            if row.intercrop_status == "unknown":
                record["yield_status"] = "calculated_unvalidated_intercrop_unknown"
                record["qa_flags"] = ";".join(filter(None, [record["qa_flags"], "intercrop_unknown"]))
            for name, sensitivity in (("narrow", narrow), ("wide", wide)):
                sens_npp = sensitivity["seasonal_npp_gC_m2"]
                record[f"phenology_{name}_central_yield_t_ha"] = (
                    calculate_yield(float(sens_npp) * NPP_TO_TBP, crop)["central"]
                    if not pd.isna(sens_npp) else np.nan
                )
        else:
            for scenario in ("low", "central", "high"):
                record[f"yield_{scenario}_t_ha"] = np.nan
                record[f"production_{scenario}_t"] = np.nan
            record["phenology_narrow_central_yield_t_ha"] = np.nan
            record["phenology_wide_central_yield_t_ha"] = np.nan
        output_rows.append(record)
    output = pd.DataFrame(output_rows).sort_values("field_id").reset_index(drop=True)
    out_path = args.run_dir / "outputs" / "field_season_calculated_yield.parquet"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    output.to_parquet(out_path, index=False)
    summary = (
        output.groupby(["county", "crop_label", "yield_status"], dropna=False)
        .agg(
            fields=("field_id", "count"),
            yield_central_mean=("yield_central_t_ha", "mean"),
            yield_central_median=("yield_central_t_ha", "median"),
            yield_central_min=("yield_central_t_ha", "min"),
            yield_central_max=("yield_central_t_ha", "max"),
            production_central_t=("production_central_t", "sum"),
            mean_npp_coverage=("npp_temporal_coverage", "mean"),
        )
        .reset_index()
    )
    summary.to_csv(args.run_dir / "outputs" / "yield_summary.csv", index=False)
    log(f"Calculated {len(output):,} field-season rows; eligible={output['yield_central_t_ha'].notna().sum():,}")


def validate(args: argparse.Namespace) -> None:
    manifest = json.loads((args.run_dir / "run_manifest.json").read_text())
    npp = pd.read_parquet(args.run_dir / "features" / "field_dekad_npp.parquet")
    output = pd.read_parquet(args.run_dir / "outputs" / "field_season_calculated_yield.parquet")
    inventory_frame = pd.read_csv(args.run_dir / "inventory" / "wapor_l2_npp_d.csv")
    expected_npp = int(manifest["field_count"]) * len(inventory_frame)
    failures: list[str] = []
    if len(output) != int(manifest["field_count"]):
        failures.append(f"output rows {len(output)} != fields {manifest['field_count']}")
    if output["field_id"].duplicated().any():
        failures.append("duplicate field IDs in yield output")
    if len(npp) != expected_npp:
        failures.append(f"NPP rows {len(npp)} != expected {expected_npp}")
    if npp.duplicated(["field_id", "period_start"]).any():
        failures.append("duplicate field-period NPP rows")
    if (npp["npp_rate_gC_m2_day"].dropna() < 0).any():
        failures.append("negative scaled NPP rate")
    calculated = output[output["yield_central_t_ha"].notna()]
    if not calculated.empty:
        order_bad = ~(
            (calculated["yield_low_t_ha"] <= calculated["yield_central_t_ha"])
            & (calculated["yield_central_t_ha"] <= calculated["yield_high_t_ha"])
        )
        if order_bad.any():
            failures.append("low/central/high yield ordering failure")
        expected_production = calculated["yield_central_t_ha"] * calculated["area_ha"]
        if not np.allclose(expected_production, calculated["production_central_t"], rtol=1e-7):
            failures.append("production does not equal yield times area")
    report = {
        "run_id": args.run_dir.name,
        "metrics": {
            "fields": len(output),
            "field_dekad_rows": len(npp),
            "expected_field_dekad_rows": expected_npp,
            "calculated_fields": int(output["yield_central_t_ha"].notna().sum()),
            "not_calculated_fields": int(output["yield_central_t_ha"].isna().sum()),
            "missing_npp_rows": int(npp["npp_rate_gC_m2_day"].isna().sum()),
            "intercrop_unknown_fields": int((output["intercrop_status"] == "unknown").sum()),
            "phenology_fallback_fields": int(output["phenology_method"].str.contains("fallback").sum()),
            "fields_sharing_representative_pixel": int(
                (output["representative_source_cell_pilot_field_count"] > 1).sum()
            ),
        },
        "warnings": {
            "npp_rate_above_14_4": int((npp["npp_rate_gC_m2_day"] > 14.4).sum()),
            "central_yield_above_20_t_ha": int((output["yield_central_t_ha"] > 20).sum()),
        },
        "failures": failures,
        "status": "passed" if not failures else "failed",
        "interpretation": "Formula and data-integrity validation only; no observed-yield accuracy validation.",
    }
    write_json(args.run_dir / "validation_report.json", report)
    print(json.dumps(report, indent=2))
    if failures:
        raise RuntimeError("Yield pilot validation failed")


def status(args: argparse.Namespace) -> None:
    paths = {
        "prepared": args.run_dir / "run_manifest.json",
        "inventoried": args.run_dir / "inventory" / "summary.json",
        "clipped": args.run_dir / "inventory" / "clipped_rasters.csv",
        "npp_extracted": args.run_dir / "features" / "field_dekad_npp.parquet",
        "calculated": args.run_dir / "outputs" / "field_season_calculated_yield.parquet",
        "validated": args.run_dir / "validation_report.json",
    }
    print(json.dumps({name: path.exists() for name, path in paths.items()}, indent=2))


def parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--run-dir", type=Path, required=True)
    common.add_argument("--season-code", default="2023_lr")
    root = argparse.ArgumentParser(description="WaPOR NPP calculated-yield pilot")
    commands = root.add_subparsers(dest="command", required=True)

    p = commands.add_parser("prepare", parents=[common])
    p.add_argument("--field-list", type=Path, required=True)
    p.add_argument("--gpkg", type=Path)
    p.add_argument("--database-url", default=os.environ.get("DATABASE_URL", "postgresql:///kenya_monitoring?host=/var/run/postgresql"))
    p.add_argument("--psql-command", help="Read-only psql command, for example: sudo -u postgres psql -d kenya_monitoring")
    p.add_argument("--model-run-id")
    p.set_defaults(func=prepare)

    p = commands.add_parser("inventory", parents=[common])
    p.add_argument("--timeout", type=int, default=30)
    p.set_defaults(func=inventory)

    p = commands.add_parser("clip", parents=[common])
    p.add_argument("--county", choices=["machakos", "nakuru"])
    p.add_argument("--gdal-translate", default=shutil.which("gdal_translate") or "gdal_translate")
    p.set_defaults(func=clip)

    p = commands.add_parser("extract-npp", parents=[common])
    p.set_defaults(func=extract_npp)

    p = commands.add_parser("calculate", parents=[common])
    p.add_argument("--minimum-crop-confidence", type=float, default=0.0)
    p.add_argument("--minimum-npp-coverage", type=float, default=0.8)
    p.add_argument("--minimum-npp-spatial-coverage", type=float, default=0.8)
    p.set_defaults(func=calculate)

    p = commands.add_parser("validate", parents=[common])
    p.set_defaults(func=validate)

    p = commands.add_parser("status", parents=[common])
    p.set_defaults(func=status)
    return root


def main() -> None:
    args = parser().parse_args()
    args.run_dir = args.run_dir.resolve()
    args.func(args)


if __name__ == "__main__":
    main()
