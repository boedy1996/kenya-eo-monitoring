#!/usr/bin/env python3
from __future__ import annotations

import argparse
import concurrent.futures
import io
import json
import os
import shlex
import shutil
import subprocess
import tempfile
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import requests
from affine import Affine
from exactextract import exact_extract
from rasterio.transform import rowcol

from .core import (
    CROP_PARAMETERS,
    METHOD_VERSION,
    NPP_TO_TBP,
    PHENOLOGY_VERSION,
    calculate_yield,
    dekads_between,
    derive_phenology_matrix,
)
from .pipeline import (
    PARAMETER_SET_ID,
    WAPOR_COLLECTION,
    WAPOR_URL,
    survey_crop_matches,
    validate_raster,
)


PRODUCTION_VERSION = "all-fields-all-complete-seasons-v1"
COUNTY_LAYERS = {"Machakos": "machakos_fin", "Nakuru": "nakuru_fin"}
SURVEY_COLUMNS = (
    "cropping_pattern", "croptype_mono", "croptype_mixed",
    "lngsht_cropping_pattern", "lngsht_croptype_mono", "lngsht_croptype_mixed",
    "cropping_pattern_2", "croptype_mono_2", "croptype_mixed_2",
    "lngsht_cropping_pattern_2", "lngsht_croptype_mono_2", "lngsht_croptype_mixed_2",
)


def log(message: str) -> None:
    print(f"[{datetime.now().astimezone().isoformat(timespec='seconds')}] {message}", flush=True)


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".partial")
    temp.write_text(json.dumps(value, indent=2, default=str) + "\n")
    temp.replace(path)


def psql_copy_frame(psql_command: str, query: str) -> pd.DataFrame:
    command = [*shlex.split(psql_command), "-X", "-q", "-v", "ON_ERROR_STOP=1"]
    completed = subprocess.run(
        command,
        input=f"COPY ({query}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);\n",
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode:
        raise RuntimeError(f"psql export failed: {completed.stderr.strip()}")
    return pd.read_csv(io.StringIO(completed.stdout))


def psql_copy_path(psql_command: str, query: str, path: Path) -> None:
    command = [*shlex.split(psql_command), "-X", "-q", "-v", "ON_ERROR_STOP=1"]
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + ".partial")
    with temp.open("w") as stream:
        completed = subprocess.run(
            command,
            input=f"COPY ({query}) TO STDOUT WITH (FORMAT CSV, HEADER TRUE);\n",
            text=True,
            stdout=stream,
            stderr=subprocess.PIPE,
            check=False,
        )
    if completed.returncode:
        temp.unlink(missing_ok=True)
        raise RuntimeError(f"psql export failed: {completed.stderr.strip()}")
    temp.replace(path)


def _clean_text(frame: pd.DataFrame, names: Iterable[str]) -> pd.Series:
    values = pd.Series("", index=frame.index, dtype="string")
    for name in names:
        if name in frame:
            current = frame[name].astype("string").fillna("").str.strip()
            values = (values + " " + current).str.strip()
    return values


def survey_attributes(frame: pd.DataFrame, suffix: str) -> tuple[pd.Series, pd.Series, pd.Series]:
    patterns = _clean_text(
        frame, (f"cropping_pattern{suffix}", f"lngsht_cropping_pattern{suffix}")
    ).str.lower()
    mixed = _clean_text(frame, (f"croptype_mixed{suffix}", f"lngsht_croptype_mixed{suffix}"))
    mono = _clean_text(frame, (f"croptype_mono{suffix}", f"lngsht_croptype_mono{suffix}"))
    status = pd.Series("unknown", index=frame.index, dtype="string")
    mixed_mask = mixed.ne("") | patterns.str.contains("mix|intercrop", regex=True, na=False)
    mono_mask = mono.ne("") | patterns.str.contains("mono|single", regex=True, na=False)
    status.loc[mixed_mask] = "yes"
    status.loc[~mixed_mask & mono_mask] = "no"
    crop_text = mixed.where(mixed.ne(""), mono).replace("", pd.NA)
    return status, crop_text, patterns.replace("", pd.NA)


def prepare(args: argparse.Namespace) -> None:
    run_dir = args.run_dir
    run_dir.mkdir(parents=True, exist_ok=True)
    active = psql_copy_frame(
        args.psql_command,
        """
        SELECT model_run_id, date_from, date_to
        FROM dashboard.model_runs
        WHERE is_active
        ORDER BY loaded_at DESC
        LIMIT 1
        """,
    )
    if active.empty:
        raise RuntimeError("No active dashboard model run")
    model_run_id = args.model_run_id or str(active.iloc[0].model_run_id)
    safe_run = model_run_id.replace("'", "''")
    seasons = psql_copy_frame(
        args.psql_command,
        f"""
        SELECT season_code, season_year, season_kind, season_name,
               season_start, season_end, is_complete
        FROM dashboard.season_definitions
        WHERE model_run_id = '{safe_run}'
        ORDER BY season_start
        """,
    )
    model_date_from = pd.Timestamp(active.iloc[0].date_from)
    model_date_to = pd.Timestamp(active.iloc[0].date_to)
    seasons["season_start"] = pd.to_datetime(seasons.season_start)
    seasons["season_end"] = pd.to_datetime(seasons.season_end)
    seasons = seasons[
        seasons["is_complete"].astype(bool)
        & seasons.season_start.ge(model_date_from)
        & seasons.season_end.le(model_date_to)
    ].copy()
    if args.season_code:
        requested = set(args.season_code)
        seasons = seasons[seasons.season_code.isin(requested)].copy()
        missing = requested - set(seasons.season_code)
        if missing:
            raise ValueError(f"Requested seasons are absent or incomplete: {sorted(missing)}")
    if seasons.empty:
        raise RuntimeError("No complete seasons selected")
    areas = psql_copy_frame(
        args.psql_command,
        "SELECT field_id, area_ha AS database_area_ha FROM dashboard.fields ORDER BY field_id",
    )

    geometry_dir = run_dir / "fields"
    geometry_dir.mkdir(parents=True, exist_ok=True)
    attributes: list[pd.DataFrame] = []
    bounds: dict[str, list[float]] = {}
    for county, layer in COUNTY_LAYERS.items():
        available = set(gpd.list_layers(args.gpkg).name)
        if layer not in available:
            raise RuntimeError(f"Missing layer {layer} in {args.gpkg}")
        columns = ["field_id", *SURVEY_COLUMNS]
        fields = gpd.read_file(args.gpkg, layer=layer, columns=columns)
        fields = fields[fields.field_id.notna()].copy()
        fields["field_id"] = fields.field_id.astype(str)
        fields["county"] = county
        fields = fields.merge(areas, on="field_id", how="left")
        geometry_area = fields.to_crs(6933).geometry.area / 10_000.0
        database_area = pd.to_numeric(fields.database_area_ha, errors="coerce")
        use_database = database_area.notna() & np.isfinite(database_area) & (database_area > 0)
        fields["source_area_ha"] = database_area
        fields["geometry_area_ha"] = geometry_area
        fields["area_ha"] = database_area.where(use_database, geometry_area)
        fields["area_method"] = np.where(use_database, "database", "geometry_epsg6933")
        for kind, suffix in (("lr", "_2"), ("sr", "")):
            status, crop_text, pattern = survey_attributes(fields, suffix)
            fields[f"intercrop_{kind}"] = status
            fields[f"survey_crop_{kind}"] = crop_text
            fields[f"survey_pattern_{kind}"] = pattern
        if fields.area_ha.isna().any() or (fields.area_ha <= 0).any():
            raise RuntimeError(f"Invalid field area in {county}")
        wgs84 = fields.to_crs(4326)
        xmin, ymin, xmax, ymax = map(float, wgs84.total_bounds)
        bounds[county.lower()] = [xmin - 0.02, ymin - 0.02, xmax + 0.02, ymax + 0.02]
        geometry = wgs84[["field_id", "geometry"]].sort_values("field_id")
        geometry.to_parquet(geometry_dir / f"{county.lower()}.parquet", index=False)
        keep = [
            "field_id", "county", "area_ha", "source_area_ha", "geometry_area_ha", "area_method",
            "intercrop_lr", "survey_crop_lr", "survey_pattern_lr",
            "intercrop_sr", "survey_crop_sr", "survey_pattern_sr",
        ]
        attributes.append(pd.DataFrame(fields[keep]).sort_values("field_id"))
        log(f"Prepared {len(fields):,} {county} fields")

    field_attributes = pd.concat(attributes, ignore_index=True).sort_values("field_id")
    if field_attributes.field_id.duplicated().any():
        raise RuntimeError("Duplicate field IDs across county layers")
    field_attributes.to_parquet(run_dir / "fields" / "attributes.parquet", index=False)
    seasons.to_parquet(run_dir / "seasons.parquet", index=False)
    write_json(run_dir / "aoi_bounds.json", bounds)
    manifest = {
        "run_id": run_dir.name,
        "production_version": PRODUCTION_VERSION,
        "created_at": datetime.now().astimezone().isoformat(),
        "code_version": subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=Path(__file__).resolve().parents[1],
            text=True, capture_output=True, check=False,
        ).stdout.strip() or "unknown",
        "model_run_id": model_run_id,
        "model_date_from": model_date_from.date().isoformat(),
        "model_date_to": model_date_to.date().isoformat(),
        "field_count": len(field_attributes),
        "county_counts": field_attributes.groupby("county").size().to_dict(),
        "season_count": len(seasons),
        "season_codes": seasons.season_code.tolist(),
        "method_version": METHOD_VERSION,
        "phenology_version": f"{PHENOLOGY_VERSION}-matrix",
        "parameter_set_id": PARAMETER_SET_ID,
        "gpkg": str(args.gpkg),
    }
    write_json(run_dir / "run_manifest.json", manifest)
    log(f"Prepared production run: {len(field_attributes):,} fields, {len(seasons)} complete seasons")


def inventory(args: argparse.Namespace) -> None:
    seasons = pd.read_parquet(args.run_dir / "seasons.parquet")
    rows: list[dict[str, Any]] = []
    for season in seasons.itertuples(index=False):
        for period in dekads_between(pd.Timestamp(season.season_start).date(), pd.Timestamp(season.season_end).date()):
            rows.append({"season_code": season.season_code, **period})
    frame = pd.DataFrame(rows).drop_duplicates("code").sort_values("period_start")
    session = requests.Session()
    records: list[dict[str, Any]] = []
    for position, row in enumerate(frame.to_dict("records"), 1):
        url = WAPOR_URL.format(code=row["code"])
        try:
            response = session.head(url, timeout=args.timeout, allow_redirects=True)
            response.raise_for_status()
            row.update(
                available=True, url=url,
                content_length=int(response.headers.get("content-length", 0)),
                etag=response.headers.get("etag", "").strip('"'),
                last_modified=response.headers.get("last-modified"),
            )
        except requests.RequestException as exc:
            row.update(available=False, url=url, error=str(exc))
        records.append(row)
        log(f"Inventory {position}/{len(frame)} {row['code']} available={row['available']}")
    output = pd.DataFrame(records)
    missing = output.loc[~output.available, "code"].tolist()
    coverage = (
        output.groupby("season_code", sort=False)
        .agg(requested_periods=("code", "count"), available_periods=("available", "sum"))
        .reset_index()
    )
    coverage["complete"] = coverage.available_periods.eq(coverage.requested_periods)
    supported = coverage.loc[coverage.complete, "season_code"].astype(str).tolist()
    unsupported = coverage.loc[~coverage.complete].to_dict("records")
    if not supported:
        raise RuntimeError("No season has complete WaPOR NPP coverage")
    selected_periods = output[output.season_code.isin(supported)].copy()
    path = args.run_dir / "inventory" / "periods.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    selected_periods.to_parquet(path, index=False)
    coverage.to_csv(args.run_dir / "inventory" / "season_coverage.csv", index=False)

    seasons_path = args.run_dir / "seasons.parquet"
    selected_seasons = seasons[seasons.season_code.isin(supported)].copy()
    selected_seasons.to_parquet(seasons_path, index=False)
    manifest_path = args.run_dir / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["model_complete_season_count"] = int(len(seasons))
    manifest["model_complete_season_codes"] = seasons.season_code.astype(str).tolist()
    manifest["season_count"] = int(len(selected_seasons))
    manifest["season_codes"] = selected_seasons.season_code.astype(str).tolist()
    manifest["wapor_excluded_seasons"] = unsupported
    write_json(manifest_path, manifest)
    write_json(
        args.run_dir / "inventory" / "summary.json",
        {
            "requested_periods": len(output),
            "available_periods": int(output.available.sum()),
            "selected_periods": len(selected_periods),
            "selected_seasons": supported,
            "excluded_seasons": unsupported,
            "missing_codes": missing,
        },
    )
    log(
        f"Selected {len(supported)} seasons with complete WaPOR coverage; "
        f"excluded {len(unsupported)} incomplete seasons"
    )


def _clip_one(task: tuple[str, str, list[float], str, str]) -> dict[str, Any]:
    county, code, bounds, url, output_text = task
    output = Path(output_text)
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        try:
            return {"county": county, "code": code, "path": str(output), **validate_raster(output), "status": "existing"}
        except Exception:
            output.unlink()
    temp = output.with_suffix(".tif.partial")
    xmin, ymin, xmax, ymax = bounds
    command = [
        "gdal_translate", "-q", "-projwin", str(xmin), str(ymax), str(xmax), str(ymin),
        "-of", "COG", "-co", "COMPRESS=ZSTD", "/vsicurl/" + url, str(temp),
    ]
    env = os.environ.copy()
    env.update(
        {
            "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
            "CPL_VSIL_CURL_ALLOWED_EXTENSIONS": ".tif",
            "GDAL_HTTP_MAX_RETRY": "4",
            "GDAL_HTTP_RETRY_DELAY": "2",
            "GDAL_HTTP_CONNECTTIMEOUT": "30",
        }
    )
    last_error: subprocess.CalledProcessError | None = None
    attempts = 4
    for attempt in range(1, attempts + 1):
        temp.unlink(missing_ok=True)
        try:
            subprocess.run(command, check=True, env=env)
            last_error = None
            break
        except subprocess.CalledProcessError as exc:
            last_error = exc
            if attempt < attempts:
                time.sleep(2**attempt)
    if last_error is not None:
        temp.unlink(missing_ok=True)
        raise RuntimeError(f"Clip failed after {attempts} attempts: {county} {code}") from last_error
    temp.replace(output)
    return {
        "county": county, "code": code, "path": str(output),
        **validate_raster(output), "status": "created", "attempts": attempt,
    }


def clip(args: argparse.Namespace) -> None:
    periods = pd.read_parquet(args.run_dir / "inventory" / "periods.parquet")
    bounds = json.loads((args.run_dir / "aoi_bounds.json").read_text())
    tasks = []
    for county, county_bounds in bounds.items():
        for row in periods.itertuples(index=False):
            output = args.run_dir / "rasters" / county / f"{row.code}.tif"
            tasks.append((county, row.code, county_bounds, row.url, str(output)))
    records = []
    failures = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(_clip_one, task): task for task in tasks}
        for number, future in enumerate(concurrent.futures.as_completed(futures), 1):
            try:
                result = future.result()
                records.append(result)
                log(
                    f"Clip {number}/{len(tasks)} {result['county']} "
                    f"{result['code']} {result['status']} attempts={result.get('attempts', 0)}"
                )
            except Exception as exc:
                task = futures[future]
                failures.append({"county": task[0], "code": task[1], "error": str(exc)})
                log(f"Clip {number}/{len(tasks)} {task[0]} {task[1]} failed: {exc}")
    frame = pd.DataFrame(records).sort_values(["county", "code"])
    frame.to_parquet(args.run_dir / "inventory" / "clipped_rasters.parquet", index=False)
    write_json(args.run_dir / "inventory" / "clip_failures.json", failures)
    if failures:
        raise RuntimeError(f"{len(failures)} raster clips failed; rerun the resumable clip stage")
    log(f"Clipped and validated {len(frame):,} county-period rasters")


def _weight_chunk(reference: Path, geometry: gpd.GeoDataFrame, start: int, output: Path) -> None:
    if output.exists():
        return
    result = exact_extract(
        str(reference), geometry, ["cell_id", "coverage"], include_cols=["field_id"],
        output="pandas", strategy="feature-sequential",
    )
    lengths = np.fromiter((len(value) for value in result.cell_id), dtype=np.int32, count=len(result))
    if lengths.sum() == 0:
        raise RuntimeError(f"No WaPOR cells intersect fields {start}:{start + len(geometry)}")
    field_index = np.repeat(np.arange(start, start + len(result), dtype=np.int32), lengths)
    cell_id = np.concatenate(result.cell_id.to_numpy()).astype(np.int64, copy=False)
    coverage = np.concatenate(result.coverage.to_numpy()).astype(np.float32, copy=False)
    temp = output.with_suffix(".npz.partial")
    with temp.open("wb") as stream:
        np.savez_compressed(stream, field_index=field_index, cell_id=cell_id, coverage=coverage)
    temp.replace(output)


def build_weights(args: argparse.Namespace) -> None:
    periods = pd.read_parquet(args.run_dir / "inventory" / "periods.parquet")
    reference_code = str(periods.iloc[0].code)
    for county in ("machakos", "nakuru"):
        geometry = gpd.read_parquet(args.run_dir / "fields" / f"{county}.parquet").to_crs(4326)
        geometry = geometry.sort_values("field_id").reset_index(drop=True)
        reference = args.run_dir / "rasters" / county / f"{reference_code}.tif"
        if not reference.exists():
            raise FileNotFoundError(reference)
        chunk_dir = args.run_dir / "weights" / f"{county}_chunks"
        chunk_dir.mkdir(parents=True, exist_ok=True)
        for start in range(0, len(geometry), args.chunk_size):
            stop = min(len(geometry), start + args.chunk_size)
            path = chunk_dir / f"{start:09d}-{stop:09d}.npz"
            before = time.monotonic()
            _weight_chunk(reference, geometry.iloc[start:stop], start, path)
            log(f"Weights {county} fields {start:,}:{stop:,} ({time.monotonic() - before:.1f}s)")
        arrays = [np.load(path) for path in sorted(chunk_dir.glob("*.npz"))]
        field_index = np.concatenate([item["field_index"] for item in arrays])
        cell_id = np.concatenate([item["cell_id"] for item in arrays])
        coverage = np.concatenate([item["coverage"] for item in arrays])
        total_coverage = np.bincount(field_index, weights=coverage, minlength=len(geometry)).astype(np.float32)
        if (total_coverage <= 0).any():
            raise RuntimeError(f"{county}: fields without WaPOR coverage")
        representative = geometry.geometry.representative_point()
        global_transform = Affine(0.0009765625, 0.0, -180.0, 0.0, -0.0009765625, 60.0)
        global_ids = []
        for point in representative:
            grid_row, grid_col = rowcol(global_transform, point.x, point.y)
            global_ids.append(int(grid_row) * 368640 + int(grid_col))
        with rasterio.open(reference) as src:
            metadata = {
                "crs": src.crs.to_string(), "width": src.width, "height": src.height,
                "transform": list(src.transform), "reference": str(reference),
            }
        output = args.run_dir / "weights" / f"{county}.npz"
        temp = output.with_suffix(".npz.partial")
        with temp.open("wb") as stream:
            np.savez_compressed(
                stream, field_ids=geometry.field_id.to_numpy(dtype=str), field_index=field_index,
                cell_id=cell_id, coverage=coverage, total_coverage=total_coverage,
                representative_source_cell_id=np.asarray(global_ids, dtype=np.int64),
            )
        temp.replace(output)
        write_json(args.run_dir / "weights" / f"{county}.json", metadata)
        log(f"Completed {county} weights: {len(geometry):,} fields, {len(cell_id):,} intersections")


def _grid_matches(src: rasterio.io.DatasetReader, metadata: dict[str, Any]) -> bool:
    return (
        src.crs.to_string() == metadata["crs"] and src.width == metadata["width"]
        and src.height == metadata["height"]
        and np.allclose(np.asarray(src.transform), np.asarray(metadata["transform"]), atol=1e-12)
    )


def weighted_raster_statistics(
    raw: np.ndarray,
    nodata: float | int | None,
    scale: float,
    offset: float,
    field_index: np.ndarray,
    cell_id: np.ndarray,
    coverage: np.ndarray,
    total_coverage: np.ndarray,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray]:
    """Apply cached polygon-cell weights to one WaPOR raster band."""
    sampled = np.asarray(raw).reshape(-1)[cell_id].astype(np.float32)
    valid = np.isfinite(sampled)
    if nodata is not None and np.isfinite(nodata):
        valid &= sampled != nodata
    physical = sampled * float(scale) + float(offset)
    size = len(total_coverage)
    valid_weight = coverage * valid
    denominator = np.bincount(field_index, weights=valid_weight, minlength=size)
    numerator = np.bincount(
        field_index, weights=np.where(valid, physical * coverage, 0.0), minlength=size
    )
    pixel_count = np.bincount(field_index[valid], minlength=size).astype(np.int32)
    mean = np.divide(numerator, denominator, out=np.full(size, np.nan), where=denominator > 0)
    coverage_fraction = np.divide(
        denominator, total_coverage, out=np.full(size, np.nan), where=total_coverage > 0
    )
    return mean, pixel_count, denominator, coverage_fraction


def _extract_county(args_tuple: tuple[str, str]) -> dict[str, Any]:
    run_dir_text, county = args_tuple
    run_dir = Path(run_dir_text)
    periods = pd.read_parquet(run_dir / "inventory" / "periods.parquet")
    weights = np.load(run_dir / "weights" / f"{county}.npz")
    field_ids = weights["field_ids"].astype(str)
    field_index = weights["field_index"]
    cell_id = weights["cell_id"]
    coverage = weights["coverage"]
    total_coverage = weights["total_coverage"]
    representative = weights["representative_source_cell_id"]
    metadata = json.loads((run_dir / "weights" / f"{county}.json").read_text())
    completed = 0
    for row in periods.itertuples(index=False):
        output = run_dir / "npp" / str(row.season_code) / county / f"{row.code}.parquet"
        if output.exists():
            try:
                existing = pd.read_parquet(output, columns=["field_id"])
                if len(existing) == len(field_ids):
                    completed += 1
                    continue
            except Exception:
                pass
            output.unlink(missing_ok=True)
        raster_path = run_dir / "rasters" / county / f"{row.code}.tif"
        with rasterio.open(raster_path) as src:
            if not _grid_matches(src, metadata):
                raise RuntimeError(f"Grid mismatch: {raster_path}")
            mean, pixel_count, denominator, coverage_fraction = weighted_raster_statistics(
                src.read(1), src.nodata, src.scales[0], src.offsets[0],
                field_index, cell_id, coverage, total_coverage,
            )
        frame = pd.DataFrame(
            {
                "field_id": field_ids,
                "county": county.capitalize(),
                "season_code": row.season_code,
                "period_start": pd.Timestamp(row.period_start),
                "period_end": pd.Timestamp(row.period_end),
                "days_in_period": int(row.days_in_period),
                "wapor_code": row.code,
                "npp_rate_gC_m2_day": mean.astype(np.float32),
                "source_pixel_count": pixel_count,
                "effective_pixel_count": denominator.astype(np.float32),
                "coverage_fraction": coverage_fraction.astype(np.float32),
                "representative_source_cell_id": representative,
            }
        )
        output.parent.mkdir(parents=True, exist_ok=True)
        temp = output.with_suffix(".parquet.partial")
        frame.to_parquet(temp, index=False)
        temp.replace(output)
        completed += 1
        log(f"NPP {county} {completed}/{len(periods)} {row.code}")
    return {"county": county, "periods": completed, "fields": len(field_ids)}


def extract_npp(args: argparse.Namespace) -> None:
    counties = [args.county] if args.county else ["machakos", "nakuru"]
    with concurrent.futures.ProcessPoolExecutor(max_workers=min(args.workers, len(counties))) as executor:
        results = list(executor.map(_extract_county, [(str(args.run_dir), county) for county in counties]))
    write_json(args.run_dir / "npp" / "extraction_summary.json", results)
    log(f"NPP extraction complete: {results}")


def _load_signal_matrix(
    frame: pd.DataFrame,
    field_ids: np.ndarray,
    week_dates: np.ndarray,
    columns: list[str],
) -> list[np.ndarray]:
    matrices = [np.full((len(field_ids), len(week_dates)), np.nan, dtype=np.float32) for _ in columns]
    if frame.empty:
        return matrices
    field_codes = pd.Categorical(frame.field_id.astype(str), categories=field_ids).codes
    dates = pd.to_datetime(frame.week_start).to_numpy(dtype="datetime64[D]")
    week_codes = np.searchsorted(week_dates, dates)
    valid = (field_codes >= 0) & (week_codes >= 0) & (week_codes < len(week_dates))
    for matrix, column in zip(matrices, columns):
        values = pd.to_numeric(frame[column], errors="coerce").to_numpy(dtype=np.float32)
        matrix[field_codes[valid], week_codes[valid]] = values[valid]
    return matrices


def _integrate_npp_table(
    npp: pd.DataFrame,
    phenology: pd.DataFrame,
    season_start: date,
    season_end: date,
) -> pd.DataFrame:
    frame = npp.merge(phenology[["field_id", "sos", "eos"]], on="field_id", how="inner")
    for column in ("period_start", "period_end", "sos", "eos"):
        frame[column] = pd.to_datetime(frame[column])

    def aggregate(prefix: str, sos: pd.Series, eos: pd.Series) -> pd.DataFrame:
        left = pd.concat([frame.period_start, sos], axis=1).max(axis=1)
        right = pd.concat([frame.period_end, eos], axis=1).min(axis=1)
        days = ((right - left).dt.days + 1).clip(lower=0).astype(np.int16)
        valid = (days > 0) & frame.npp_rate_gC_m2_day.notna()
        work = pd.DataFrame(
            {
                "field_id": frame.field_id,
                "contribution": np.where(valid, frame.npp_rate_gC_m2_day * days, 0.0),
                "valid_days": np.where(valid, days, 0),
                "coverage_days": np.where(valid, frame.coverage_fraction * days, 0.0),
                "pixel_days": np.where(valid, frame.effective_pixel_count * days, 0.0),
                "period_used": valid.astype(np.int8),
            }
        )
        grouped = work.groupby("field_id", sort=False).sum()
        total_days = (eos - sos).dt.days + 1
        totals = pd.DataFrame({"field_id": frame.field_id, "total_days": total_days}).groupby("field_id").first()
        grouped = grouped.join(totals)
        grouped[f"{prefix}seasonal_npp_gC_m2"] = grouped.contribution.where(grouped.valid_days > 0)
        grouped[f"{prefix}npp_valid_days"] = grouped.valid_days
        grouped[f"{prefix}npp_total_days"] = grouped.total_days
        grouped[f"{prefix}npp_temporal_coverage"] = grouped.valid_days / grouped.total_days
        grouped[f"{prefix}npp_mean_spatial_coverage"] = grouped.coverage_days / grouped.valid_days.replace(0, np.nan)
        grouped[f"{prefix}npp_mean_effective_pixels"] = grouped.pixel_days / grouped.valid_days.replace(0, np.nan)
        grouped[f"{prefix}npp_periods_used"] = grouped.period_used
        return grouped[[column for column in grouped if column.startswith(prefix)]]

    central = aggregate("", frame.sos, frame.eos)
    ten_days = np.timedelta64(10, "D")
    narrow_sos = pd.concat([frame.eos, frame.sos + ten_days], axis=1).min(axis=1)
    narrow_eos = pd.concat([narrow_sos, frame.eos - ten_days], axis=1).max(axis=1)
    wide_sos = pd.concat(
        [pd.Series(pd.Timestamp(season_start), index=frame.index), frame.sos - ten_days], axis=1
    ).max(axis=1)
    wide_eos = pd.concat(
        [pd.Series(pd.Timestamp(season_end), index=frame.index), frame.eos + ten_days], axis=1
    ).min(axis=1)
    narrow = aggregate("narrow_", narrow_sos, narrow_eos)
    wide = aggregate("wide_", wide_sos, wide_eos)
    return central.join(narrow).join(wide).reset_index()


def calculate_season(args: argparse.Namespace, season_code: str | None = None) -> Path:
    season_code = season_code or args.season_code
    seasons = pd.read_parquet(args.run_dir / "seasons.parquet")
    selected = seasons[seasons.season_code == season_code]
    if selected.empty:
        raise ValueError(f"Unknown complete season: {season_code}")
    season = selected.iloc[0]
    start = pd.Timestamp(season.season_start).date()
    end = pd.Timestamp(season.season_end).date()
    manifest = json.loads((args.run_dir / "run_manifest.json").read_text())
    model_run = str(manifest["model_run_id"]).replace("'", "''")
    safe_season = season_code.replace("'", "''")
    work_dir = args.run_dir / "work" / season_code
    work_dir.mkdir(parents=True, exist_ok=True)
    seasonal_csv = work_dir / "seasonal.csv"
    psql_copy_path(
        args.psql_command,
        f"""
        SELECT field_id, county, seasonal_label, seasonal_confidence, status
        FROM dashboard.seasonal_predictions
        WHERE model_run_id = '{model_run}' AND season_code = '{safe_season}'
        ORDER BY field_id
        """,
        seasonal_csv,
    )
    seasonal = pd.read_csv(seasonal_csv)
    eligible_crop = seasonal.status.eq("final") & seasonal.seasonal_label.str.lower().isin(CROP_PARAMETERS)
    eligible_ids = seasonal.loc[eligible_crop, "field_id"].astype(str).sort_values().to_numpy()
    log(f"{season_code}: {len(eligible_ids):,} final supported crop calls")
    s2_csv = work_dir / "s2.csv"
    s1_csv = work_dir / "s1.csv"
    psql_copy_path(
        args.psql_command,
        f"""
        SELECT s.field_id, s.start_date AS week_start, s.ndvi_median, s.evi_median, s.lai_median
        FROM timeseries.s2_indices s
        JOIN dashboard.seasonal_predictions p ON p.field_id=s.field_id
         AND p.model_run_id='{model_run}' AND p.season_code='{safe_season}'
         AND p.status='final' AND lower(p.seasonal_label) IN ('maize','beans','potatoes')
        WHERE s.start_date BETWEEN '{start}' AND '{end}'
        ORDER BY s.field_id, s.start_date
        """,
        s2_csv,
    )
    psql_copy_path(
        args.psql_command,
        f"""
        SELECT s.field_id, s.start_date AS week_start, s.rvi_median
        FROM timeseries.s1_indices s
        JOIN dashboard.seasonal_predictions p ON p.field_id=s.field_id
         AND p.model_run_id='{model_run}' AND p.season_code='{safe_season}'
         AND p.status='final' AND lower(p.seasonal_label) IN ('maize','beans','potatoes')
        WHERE s.start_date BETWEEN '{start}' AND '{end}'
        ORDER BY s.field_id, s.start_date
        """,
        s1_csv,
    )
    s2 = pd.read_csv(s2_csv)
    s1 = pd.read_csv(s1_csv)
    weeks = np.unique(
        np.concatenate(
            [pd.to_datetime(s2.week_start).to_numpy(dtype="datetime64[D]"), pd.to_datetime(s1.week_start).to_numpy(dtype="datetime64[D]")]
        )
    )
    ndvi, evi, lai = _load_signal_matrix(s2, eligible_ids, weeks, ["ndvi_median", "evi_median", "lai_median"])
    (rvi,) = _load_signal_matrix(s1, eligible_ids, weeks, ["rvi_median"])
    phenology = derive_phenology_matrix(eligible_ids, weeks, ndvi, evi, lai, rvi, start, end)
    phenology["season_code"] = season_code
    feature_dir = args.run_dir / "features" / season_code
    feature_dir.mkdir(parents=True, exist_ok=True)
    phenology.to_parquet(feature_dir / "phenology.parquet", index=False)

    npp_paths = sorted((args.run_dir / "npp" / season_code).glob("*/*.parquet"))
    if not npp_paths:
        raise RuntimeError(f"No extracted NPP for {season_code}")
    npp = pd.concat([pd.read_parquet(path) for path in npp_paths], ignore_index=True)
    npp = npp[npp.field_id.isin(set(eligible_ids))].copy()
    metrics = _integrate_npp_table(npp, phenology, start, end)

    fields = pd.read_parquet(args.run_dir / "fields" / "attributes.parquet")
    kind = str(season.season_kind)
    base = fields.merge(seasonal, on=["field_id", "county"], how="left")
    base = base.merge(phenology, on="field_id", how="left").merge(metrics, on="field_id", how="left")
    base["crop_label"] = base.seasonal_label.fillna("Unclassified").str.lower()
    base.loc[base.crop_label == "unclassified", "crop_label"] = "Unclassified"
    base["intercrop_status"] = base[f"intercrop_{kind}"].fillna("unknown")
    base["survey_crop_text"] = base[f"survey_crop_{kind}"]
    base["survey_crop_match"] = [
        survey_crop_matches(crop.lower(), survey)
        for crop, survey in zip(base.crop_label.astype(str), base.survey_crop_text)
    ]
    reasons = pd.Series("", index=base.index, dtype="string")

    def flag(mask: pd.Series, text: str) -> None:
        nonlocal reasons
        reasons.loc[mask] = reasons.loc[mask].where(reasons.loc[mask].eq(""), reasons.loc[mask] + ";") + text

    supported = base.crop_label.str.lower().isin(CROP_PARAMETERS)
    final = base.status.eq("final")
    confidence_ok = base.seasonal_confidence.notna() & (base.seasonal_confidence >= args.minimum_crop_confidence)
    mixed = base.intercrop_status.eq("yes")
    mismatch = base.intercrop_status.eq("no") & base.survey_crop_match.eq(False)
    temporal_ok = base.npp_temporal_coverage.fillna(0) >= args.minimum_npp_coverage
    spatial_ok = base.npp_mean_spatial_coverage.fillna(0) >= args.minimum_npp_spatial_coverage
    npp_ok = base.seasonal_npp_gC_m2.notna()
    flag(~supported, "unsupported_or_unclassified_crop")
    flag(~final, "seasonal_status_not_final")
    flag(~confidence_ok, "crop_confidence_below_threshold")
    flag(mixed, "mixed_crop_biomass_only")
    flag(mismatch, "survey_crop_mismatch")
    flag(~temporal_ok & supported & final, "low_npp_temporal_coverage")
    flag(~spatial_ok & supported & final, "low_npp_spatial_coverage")
    flag(~npp_ok & supported & final, "missing_npp")
    eligible = supported & final & confidence_ok & ~mixed & ~mismatch & temporal_ok & spatial_ok & npp_ok
    base["tbp_tDM_ha"] = base.seasonal_npp_gC_m2 * NPP_TO_TBP
    for scenario in ("low", "central", "high"):
        base[f"yield_{scenario}_t_ha"] = np.nan
        base[f"production_{scenario}_t"] = np.nan
    base["phenology_narrow_central_yield_t_ha"] = np.nan
    base["phenology_wide_central_yield_t_ha"] = np.nan
    for crop in CROP_PARAMETERS:
        crop_mask = eligible & base.crop_label.str.lower().eq(crop)
        if not crop_mask.any():
            continue
        params = CROP_PARAMETERS[crop]
        for index, scenario in enumerate(("low", "central", "high")):
            value = (
                base.loc[crop_mask, "tbp_tDM_ha"] * params.fc[index] * params.aot[index]
                * params.hi[index] / (1.0 - params.moisture[index])
            )
            base.loc[crop_mask, f"yield_{scenario}_t_ha"] = value
            base.loc[crop_mask, f"production_{scenario}_t"] = value * base.loc[crop_mask, "area_ha"]
        central_index = 1
        for name in ("narrow", "wide"):
            tbp = base.loc[crop_mask, f"{name}_seasonal_npp_gC_m2"] * NPP_TO_TBP
            base.loc[crop_mask, f"phenology_{name}_central_yield_t_ha"] = (
                tbp * params.fc[central_index] * params.aot[central_index] * params.hi[central_index]
                / (1.0 - params.moisture[central_index])
            )
    base["yield_status"] = np.where(eligible, "calculated_unvalidated", "not_calculated")
    unknown = eligible & base.intercrop_status.eq("unknown")
    base.loc[unknown, "yield_status"] = "calculated_unvalidated_intercrop_unknown"
    flag(unknown, "intercrop_unknown")
    base["qa_flags"] = reasons
    base["yield_run_id"] = args.run_dir.name
    base["model_run_id"] = manifest["model_run_id"]
    base["season_code"] = season_code
    base["season_start"] = start
    base["season_end"] = end
    base["parameter_set_id"] = PARAMETER_SET_ID
    base["method_version"] = METHOD_VERSION
    output_columns = [
        "yield_run_id", "model_run_id", "field_id", "county", "season_code", "season_start", "season_end",
        "area_ha", "area_method", "crop_label", "seasonal_confidence", "status", "intercrop_status",
        "survey_crop_text", "survey_crop_match", "sos", "peak_date", "eos", "method", "confidence",
        "qa_status", "optical_valid_weeks", "sar_supported_weeks", "seasonal_npp_gC_m2",
        "npp_valid_days", "npp_total_days", "npp_temporal_coverage", "npp_mean_spatial_coverage",
        "npp_mean_effective_pixels", "npp_periods_used", "tbp_tDM_ha", "yield_low_t_ha",
        "yield_central_t_ha", "yield_high_t_ha", "production_low_t", "production_central_t",
        "production_high_t", "phenology_narrow_central_yield_t_ha",
        "phenology_wide_central_yield_t_ha", "parameter_set_id", "method_version", "yield_status", "qa_flags",
    ]
    rename = {
        "seasonal_confidence": "crop_confidence", "status": "crop_status",
        "method": "phenology_method", "confidence": "phenology_confidence",
        "qa_status": "phenology_qa_status",
    }
    output = base[output_columns].rename(columns=rename).sort_values("field_id")
    output_dir = args.run_dir / "outputs"
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / f"{season_code}.parquet"
    temp = output_path.with_suffix(".parquet.partial")
    output.to_parquet(temp, index=False)
    temp.replace(output_path)
    summary = (
        output.groupby(["county", "crop_label", "yield_status"], dropna=False)
        .agg(
            fields=("field_id", "count"), calculated_fields=("yield_central_t_ha", "count"),
            yield_central_mean=("yield_central_t_ha", "mean"),
            yield_central_median=("yield_central_t_ha", "median"),
            yield_central_min=("yield_central_t_ha", "min"),
            yield_central_max=("yield_central_t_ha", "max"),
            production_central_t=("production_central_t", lambda values: values.sum(min_count=1)),
        ).reset_index()
    )
    summary["season_code"] = season_code
    summary.to_csv(output_dir / f"{season_code}_summary.csv", index=False)
    validate_season_output(args, season_code, output)
    log(f"{season_code}: wrote {len(output):,} rows, calculated={output.yield_central_t_ha.notna().sum():,}")
    if not args.keep_work:
        shutil.rmtree(work_dir, ignore_errors=True)
    return output_path


def validate_season_output(args: argparse.Namespace, season_code: str, output: pd.DataFrame | None = None) -> dict[str, Any]:
    manifest = json.loads((args.run_dir / "run_manifest.json").read_text())
    if output is None:
        output = pd.read_parquet(args.run_dir / "outputs" / f"{season_code}.parquet")
    failures = []
    if len(output) != int(manifest["field_count"]):
        failures.append(f"rows={len(output)} expected={manifest['field_count']}")
    if output.field_id.duplicated().any():
        failures.append("duplicate field IDs")
    calculated = output[output.yield_central_t_ha.notna()]
    if not calculated.empty:
        if not ((calculated.yield_low_t_ha <= calculated.yield_central_t_ha) & (calculated.yield_central_t_ha <= calculated.yield_high_t_ha)).all():
            failures.append("yield scenario order")
        expected = calculated.yield_central_t_ha * calculated.area_ha
        if not np.allclose(expected, calculated.production_central_t, rtol=1e-7):
            failures.append("production identity")
    report = {
        "season_code": season_code, "rows": len(output), "calculated_fields": len(calculated),
        "not_calculated_fields": int(output.yield_central_t_ha.isna().sum()),
        "phenology_fallback_fields": int(output.phenology_method.fillna("").str.contains("fallback").sum()),
        "failures": failures, "status": "passed" if not failures else "failed",
    }
    write_json(args.run_dir / "validation" / f"{season_code}.json", report)
    if failures:
        raise RuntimeError(f"{season_code} validation failed: {failures}")
    return report


def calculate_all(args: argparse.Namespace) -> None:
    seasons = pd.read_parquet(args.run_dir / "seasons.parquet")
    reports = []
    for position, season_code in enumerate(seasons.season_code, 1):
        validation_path = args.run_dir / "validation" / f"{season_code}.json"
        if validation_path.exists() and not args.force:
            report = json.loads(validation_path.read_text())
            if report.get("status") == "passed":
                log(f"Calculate {position}/{len(seasons)} {season_code} skipped (validated)")
                reports.append(report)
                continue
        calculate_season(args, str(season_code))
        reports.append(json.loads(validation_path.read_text()))
    write_json(
        args.run_dir / "validation_report.json",
        {
            "run_id": args.run_dir.name, "season_count": len(reports),
            "calculated_fields": sum(item["calculated_fields"] for item in reports),
            "failures": [item for item in reports if item["status"] != "passed"],
            "status": "passed" if all(item["status"] == "passed" for item in reports) else "failed",
        },
    )


def validate_all(args: argparse.Namespace) -> None:
    seasons = pd.read_parquet(args.run_dir / "seasons.parquet")
    reports = [validate_season_output(args, str(code)) for code in seasons.season_code]
    report = {
        "run_id": args.run_dir.name, "season_count": len(reports),
        "output_rows": sum(item["rows"] for item in reports),
        "calculated_fields": sum(item["calculated_fields"] for item in reports),
        "failures": [item for item in reports if item["status"] != "passed"],
        "status": "passed" if all(item["status"] == "passed" for item in reports) else "failed",
    }
    write_json(args.run_dir / "validation_report.json", report)
    print(json.dumps(report, indent=2))


def status(args: argparse.Namespace) -> None:
    seasons_path = args.run_dir / "seasons.parquet"
    seasons = pd.read_parquet(seasons_path).season_code.tolist() if seasons_path.exists() else []
    outputs = {code: (args.run_dir / "outputs" / f"{code}.parquet").exists() for code in seasons}
    validations = {code: (args.run_dir / "validation" / f"{code}.json").exists() for code in seasons}
    print(
        json.dumps(
            {
                "prepared": (args.run_dir / "run_manifest.json").exists(),
                "inventoried": (args.run_dir / "inventory" / "summary.json").exists(),
                "clipped": (args.run_dir / "inventory" / "clipped_rasters.parquet").exists(),
                "weights": {county: (args.run_dir / "weights" / f"{county}.npz").exists() for county in ("machakos", "nakuru")},
                "npp_extracted": (args.run_dir / "npp" / "extraction_summary.json").exists(),
                "outputs": outputs, "validations": validations,
            }, indent=2,
        )
    )


def build_parser() -> argparse.ArgumentParser:
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--run-dir", type=Path, required=True)
    root = argparse.ArgumentParser(description="All-field all-complete-season WaPOR yield production")
    commands = root.add_subparsers(dest="command", required=True)
    p = commands.add_parser("prepare", parents=[common])
    p.add_argument("--gpkg", type=Path, required=True)
    p.add_argument("--psql-command", default="sudo -u postgres psql -d kenya_monitoring")
    p.add_argument("--model-run-id")
    p.add_argument("--season-code", action="append")
    p.set_defaults(func=prepare)
    p = commands.add_parser("inventory", parents=[common])
    p.add_argument("--timeout", type=int, default=30)
    p.set_defaults(func=inventory)
    p = commands.add_parser("clip", parents=[common])
    p.add_argument("--workers", type=int, default=2)
    p.set_defaults(func=clip)
    p = commands.add_parser("build-weights", parents=[common])
    p.add_argument("--chunk-size", type=int, default=20_000)
    p.set_defaults(func=build_weights)
    p = commands.add_parser("extract-npp", parents=[common])
    p.add_argument("--county", choices=["machakos", "nakuru"])
    p.add_argument("--workers", type=int, default=2)
    p.set_defaults(func=extract_npp)
    for name, function in (("calculate-season", calculate_season), ("calculate-all", calculate_all)):
        p = commands.add_parser(name, parents=[common])
        if name == "calculate-season":
            p.add_argument("--season-code", required=True)
        p.add_argument("--psql-command", default="sudo -u postgres psql -d kenya_monitoring")
        p.add_argument("--minimum-crop-confidence", type=float, default=0.0)
        p.add_argument("--minimum-npp-coverage", type=float, default=0.8)
        p.add_argument("--minimum-npp-spatial-coverage", type=float, default=0.8)
        p.add_argument("--keep-work", action="store_true")
        p.add_argument("--force", action="store_true")
        p.set_defaults(func=function)
    p = commands.add_parser("validate", parents=[common])
    p.set_defaults(func=validate_all)
    p = commands.add_parser("status", parents=[common])
    p.set_defaults(func=status)
    return root


def main() -> None:
    args = build_parser().parse_args()
    args.run_dir = args.run_dir.resolve()
    args.func(args)


if __name__ == "__main__":
    main()
