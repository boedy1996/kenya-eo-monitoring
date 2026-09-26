#!/usr/bin/env python3
"""Production zonal statistics using pixel-centre inclusion and adaptive buffers.

The pipeline has three stages:

1. ``prepare`` inventories every raster and builds one common adaptive buffer for
   S1 and S2.  It also stores the exact pixel membership for each sensor grid.
2. ``extract`` reads only raster tiles containing selected pixels and writes one
   atomic Parquet part per weekly raster.
3. ``validate`` checks source coverage, Parquet schemas, row counts, pixel-count
   invariants, and optionally scans every value column with DuckDB.

No Earth Engine call is made by this program.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import math
import os
import re
import shutil
import tempfile
import time
import traceback
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Sequence

import duckdb
import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import rasterio
from affine import Affine
from rasterio.features import geometry_mask
from rasterio.windows import Window, from_bounds
from shapely.geometry.base import BaseGeometry


PIPELINE_VERSION = "1.0.0"
DATE_RE = re.compile(r"(?P<start>\d{4}-\d{2}-\d{2})_(?P<end>\d{4}-\d{2}-\d{2})")
REGIONS = ("machakos", "nakuru")
SENSORS = ("s1", "s2")

BANDS: dict[str, list[str]] = {
    "s1": ["sigma_vv_linear", "sigma_vh_linear", "p_ratio", "rvi", "rcspr"],
    "s2": ["ndvi", "evi", "ndmi", "ndre", "msi", "pvr", "lai"],
}

BAND_ALIASES: dict[str, list[set[str]]] = {
    "s1": [
        {"sigmavvlinear", "vv", "band1"},
        {"sigmavhlinear", "vh", "band2"},
        {"pratio", "band3"},
        {"rvi", "band4"},
        {"rcspr", "band5"},
    ],
    "s2": [
        {"ndvi", "band1"},
        {"evi", "band2"},
        {"ndmi", "band3"},
        {"ndre", "band4"},
        {"msi", "band5"},
        {"pvr", "band6"},
        {"lai", "band7"},
    ],
}

def log(message: str) -> None:
    stamp = datetime.now().astimezone().isoformat(timespec="seconds")
    print(f"[{stamp}] {message}", flush=True)


def normalize_name(value: str | None) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def sql_literal(value: str | Path) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def atomic_write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".tmp-{os.getpid()}")
    temporary.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


@dataclass(frozen=True)
class GridSignature:
    crs_wkt: str
    transform: tuple[float, ...]
    width: int
    height: int
    band_count: int
    dtypes: tuple[str, ...]
    nodata: tuple[float | None, ...]

    @classmethod
    def from_dataset(cls, dataset: rasterio.io.DatasetReader) -> "GridSignature":
        if dataset.crs is None:
            raise ValueError(f"Raster has no CRS: {dataset.name}")
        nodata = tuple(None if value is None else float(value) for value in dataset.nodatavals)
        return cls(
            crs_wkt=dataset.crs.to_wkt(),
            transform=tuple(float(value) for value in tuple(dataset.transform)),
            width=int(dataset.width),
            height=int(dataset.height),
            band_count=int(dataset.count),
            dtypes=tuple(str(value) for value in dataset.dtypes),
            nodata=nodata,
        )

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "GridSignature":
        return cls(
            crs_wkt=str(payload["crs_wkt"]),
            transform=tuple(float(value) for value in payload["transform"]),
            width=int(payload["width"]),
            height=int(payload["height"]),
            band_count=int(payload["band_count"]),
            dtypes=tuple(str(value) for value in payload["dtypes"]),
            nodata=tuple(payload["nodata"]),
        )

    @property
    def affine(self) -> Affine:
        return Affine(*self.transform[:6])

    def grid_equals(self, other: "GridSignature") -> bool:
        return (
            self.crs_wkt == other.crs_wkt
            and self.transform == other.transform
            and self.width == other.width
            and self.height == other.height
            and self.band_count == other.band_count
            and self.dtypes == other.dtypes
        )


@dataclass(frozen=True)
class RasterRecord:
    sensor: str
    region: str
    path: str
    raster_file: str
    start_date: str
    end_date: str
    iso_year: int
    week_no: int
    size_bytes: int
    grid_key: str


def parse_raster_name(path: Path, sensor: str) -> tuple[str, date, date]:
    lower = path.name.lower()
    region = next((item for item in REGIONS if lower.startswith(item + "_")), None)
    if region is None:
        raise ValueError(f"Cannot infer region from raster filename: {path.name}")
    match = DATE_RE.search(path.name)
    if not match:
        raise ValueError(f"Cannot parse weekly dates from raster filename: {path.name}")
    start = date.fromisoformat(match.group("start"))
    end = date.fromisoformat(match.group("end"))
    if end < start:
        raise ValueError(f"End date precedes start date: {path.name}")
    expected_bands = len(BANDS[sensor])
    return region, start, end


def validate_band_descriptions(dataset: rasterio.io.DatasetReader, sensor: str) -> None:
    expected = BANDS[sensor]
    if dataset.count != len(expected):
        raise ValueError(
            f"{dataset.name}: expected {len(expected)} {sensor.upper()} bands, found {dataset.count}"
        )
    if any(not np.issubdtype(np.dtype(dtype), np.floating) for dtype in dataset.dtypes):
        raise ValueError(f"{dataset.name}: index bands must use a floating-point dtype")
    descriptions = dataset.descriptions or tuple(None for _ in expected)
    for index, canonical in enumerate(expected):
        description = descriptions[index] if index < len(descriptions) else None
        if description is None or not str(description).strip():
            continue
        if normalize_name(description) not in BAND_ALIASES[sensor][index]:
            raise ValueError(
                f"{dataset.name}: band {index + 1} description {description!r} "
                f"does not match expected {canonical!r}"
            )


def weekly_gaps(records: Sequence[RasterRecord]) -> list[str]:
    if not records:
        return []
    starts = sorted(date.fromisoformat(record.start_date) for record in records)
    present = set(starts)
    cursor = starts[0]
    missing: list[str] = []
    while cursor <= starts[-1]:
        if cursor not in present:
            missing.append(cursor.isoformat())
        cursor += timedelta(days=7)
    return missing


def inventory_rasters(sensor: str, input_dir: Path) -> tuple[list[RasterRecord], dict[str, GridSignature]]:
    paths = sorted(input_dir.glob("*.tif"))
    if not paths:
        raise FileNotFoundError(f"No GeoTIFF files found for {sensor.upper()}: {input_dir}")

    records: list[RasterRecord] = []
    reference_grids: dict[str, GridSignature] = {}
    seen_keys: set[tuple[str, str]] = set()

    for number, path in enumerate(paths, start=1):
        region, start, end = parse_raster_name(path, sensor)
        key = (region, start.isoformat())
        if key in seen_keys:
            raise ValueError(f"Duplicate {sensor.upper()} raster for {region} {start}: {path}")
        seen_keys.add(key)

        with rasterio.open(path) as dataset:
            validate_band_descriptions(dataset, sensor)
            signature = GridSignature.from_dataset(dataset)

        if region in reference_grids and not reference_grids[region].grid_equals(signature):
            raise ValueError(
                f"Grid mismatch in {sensor.upper()} {region}: {path}. "
                "All weekly rasters must share CRS, transform, dimensions, and band count."
            )
        reference_grids.setdefault(region, signature)
        iso = start.isocalendar()
        records.append(
            RasterRecord(
                sensor=sensor,
                region=region,
                path=str(path.resolve()),
                raster_file=path.name,
                start_date=start.isoformat(),
                end_date=end.isoformat(),
                iso_year=int(iso.year),
                week_no=int(iso.week),
                size_bytes=path.stat().st_size,
                grid_key=f"{sensor}_{region}",
            )
        )
        if number % 100 == 0 or number == len(paths):
            log(f"Inventoried {sensor.upper()} rasters: {number}/{len(paths)}")

    missing_regions = set(REGIONS) - set(reference_grids)
    if missing_regions:
        raise ValueError(f"Missing {sensor.upper()} rasters for regions: {sorted(missing_regions)}")
    return records, reference_grids


def clipped_candidate_window(grid: GridSignature, geometry: BaseGeometry) -> Window | None:
    if geometry is None or geometry.is_empty:
        return None
    raw = from_bounds(*geometry.bounds, transform=grid.affine)
    col_start = max(0, math.floor(raw.col_off))
    row_start = max(0, math.floor(raw.row_off))
    col_stop = min(grid.width, math.ceil(raw.col_off + raw.width))
    row_stop = min(grid.height, math.ceil(raw.row_off + raw.height))
    if col_stop <= col_start or row_stop <= row_start:
        return None
    return Window(col_start, row_start, col_stop - col_start, row_stop - row_start)


def center_pixel_indices(grid: GridSignature, geometry: BaseGeometry) -> np.ndarray:
    """Return flattened raster indexes whose pixel centres are inside geometry."""
    window = clipped_candidate_window(grid, geometry)
    if window is None:
        return np.empty(0, dtype=np.int64)
    height = int(window.height)
    width = int(window.width)
    mask = geometry_mask(
        [geometry.__geo_interface__],
        out_shape=(height, width),
        transform=rasterio.windows.transform(window, grid.affine),
        all_touched=False,
        invert=True,
    )
    local_rows, local_cols = np.nonzero(mask)
    if len(local_rows) == 0:
        return np.empty(0, dtype=np.int64)
    rows = local_rows.astype(np.int64, copy=False) + int(window.row_off)
    cols = local_cols.astype(np.int64, copy=False) + int(window.col_off)
    return rows * grid.width + cols


def load_fields(
    gpkg_path: Path,
    layer_name: str,
    uid_field: str,
    limit: int | None,
    selected_ids: set[str] | None,
) -> gpd.GeoDataFrame:
    frame = gpd.read_file(gpkg_path, layer=layer_name, columns=[uid_field, "geometry"], engine="pyogrio")
    frame = frame.rename(columns={uid_field: "field_id"})
    frame["field_id"] = frame["field_id"].astype(str).str.strip()
    if selected_ids is not None:
        frame = frame[frame["field_id"].isin(selected_ids)].copy()
    if limit is not None:
        frame = frame.iloc[:limit].copy()
    if frame["field_id"].eq("").any() or frame["field_id"].isna().any():
        raise ValueError(f"Layer {layer_name} contains empty field IDs")
    duplicates = frame.loc[frame["field_id"].duplicated(), "field_id"].head(10).tolist()
    if duplicates:
        raise ValueError(f"Layer {layer_name} contains duplicate field IDs: {duplicates}")
    if frame.geometry.isna().any() or frame.geometry.is_empty.any():
        raise ValueError(f"Layer {layer_name} contains null or empty geometry")
    invalid = ~frame.geometry.is_valid
    if invalid.any():
        examples = frame.loc[invalid, "field_id"].head(10).tolist()
        raise ValueError(f"Layer {layer_name} contains invalid geometry: {examples}")
    return frame.reset_index(drop=True)


def sort_membership_by_tile(
    field_indexes: list[np.ndarray],
    flat_pixels: list[np.ndarray],
    grid: GridSignature,
    tile_size: int,
) -> dict[str, np.ndarray]:
    if flat_pixels:
        all_pixels = np.concatenate(flat_pixels).astype(np.int64, copy=False)
        all_fields = np.concatenate(field_indexes).astype(np.int32, copy=False)
    else:
        all_pixels = np.empty(0, dtype=np.int64)
        all_fields = np.empty(0, dtype=np.int32)

    rows = all_pixels // grid.width
    cols = all_pixels % grid.width
    tile_columns = math.ceil(grid.width / tile_size)
    tile_ids_all = (rows // tile_size) * tile_columns + (cols // tile_size)
    order = np.argsort(tile_ids_all, kind="stable")
    all_pixels = all_pixels[order]
    all_fields = all_fields[order]
    tile_ids_all = tile_ids_all[order]

    if len(tile_ids_all):
        tile_ids, starts = np.unique(tile_ids_all, return_index=True)
        offsets = np.concatenate([starts.astype(np.int64), np.array([len(tile_ids_all)], dtype=np.int64)])
    else:
        tile_ids = np.empty(0, dtype=np.int64)
        offsets = np.array([0], dtype=np.int64)

    return {
        "field_index": all_fields,
        "flat_pixel": all_pixels,
        "tile_ids": tile_ids.astype(np.int64, copy=False),
        "tile_offsets": offsets,
        "tile_size": np.array([tile_size], dtype=np.int32),
        "raster_width": np.array([grid.width], dtype=np.int32),
        "raster_height": np.array([grid.height], dtype=np.int32),
    }


def write_npz_atomic(path: Path, arrays: dict[str, np.ndarray]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + f".tmp-{os.getpid()}.npz")
    np.savez(temporary, **arrays)
    os.replace(temporary, path)


def build_region_manifest(
    run_dir: Path,
    region: str,
    layer_name: str,
    fields: gpd.GeoDataFrame,
    grids: dict[str, GridSignature],
    min_pixels: int,
    tile_size: int,
) -> pd.DataFrame:
    target_crs = rasterio.crs.CRS.from_wkt(grids["s1"].crs_wkt)
    if fields.crs is None:
        raise ValueError(f"Layer {layer_name} has no CRS")
    if fields.crs != target_crs:
        fields = fields.to_crs(target_crs)

    sensor_pixels: dict[str, list[np.ndarray]] = {sensor: [] for sensor in SENSORS}
    sensor_field_indexes: dict[str, list[np.ndarray]] = {sensor: [] for sensor in SENSORS}
    rows: list[dict[str, Any]] = []

    for field_index, item in enumerate(fields.itertuples(index=False)):
        geometries = {
            0: item.geometry,
            5: item.geometry.buffer(-5),
            10: item.geometry.buffer(-10),
        }
        candidates: dict[str, dict[int, np.ndarray]] = {sensor: {} for sensor in SENSORS}
        for sensor in SENSORS:
            for width in (0, 5, 10):
                candidates[sensor][width] = center_pixel_indices(grids[sensor], geometries[width])

        selected = next(
            (
                width
                for width in (10, 5, 0)
                if len(candidates["s1"][width]) >= min_pixels
                and len(candidates["s2"][width]) >= min_pixels
            ),
            0,
        )
        below_min = (
            len(candidates["s1"][selected]) < min_pixels
            or len(candidates["s2"][selected]) < min_pixels
        )
        record: dict[str, Any] = {
            "field_id": str(item.field_id),
            "region": region,
            "layer_name": layer_name,
            "buffer_width_m": selected,
            "below_min_pixel_threshold": below_min,
        }
        for sensor in SENSORS:
            selected_pixels = candidates[sensor][selected]
            record[f"{sensor}_pixel_count_before"] = len(candidates[sensor][0])
            record[f"{sensor}_pixel_count_after"] = len(selected_pixels)
            if len(selected_pixels):
                sensor_pixels[sensor].append(selected_pixels)
                sensor_field_indexes[sensor].append(
                    np.full(len(selected_pixels), field_index, dtype=np.int32)
                )
        rows.append(record)
        if (field_index + 1) % 5000 == 0 or field_index + 1 == len(fields):
            log(f"Prepared adaptive buffers for {region}: {field_index + 1:,}/{len(fields):,}")

    manifest = pd.DataFrame(rows)
    for sensor in SENSORS:
        arrays = sort_membership_by_tile(
            sensor_field_indexes[sensor], sensor_pixels[sensor], grids[sensor], tile_size
        )
        membership_path = run_dir / "membership" / f"{sensor}_{region}.npz"
        write_npz_atomic(membership_path, arrays)

        counts = np.bincount(arrays["field_index"], minlength=len(manifest))
        expected = manifest[f"{sensor}_pixel_count_after"].to_numpy(dtype=np.int64)
        if not np.array_equal(counts, expected):
            raise RuntimeError(f"Membership count mismatch for {sensor.upper()} {region}")

        sensor_manifest = pd.DataFrame(
            {
                "field_index": np.arange(len(manifest), dtype=np.int32),
                "field_id": manifest["field_id"].astype("string"),
                "region": manifest["region"].astype("string"),
                "layer_name": manifest["layer_name"].astype("string"),
                "buffer_width_m": manifest["buffer_width_m"].astype("int8"),
                "pixel_count_before": manifest[f"{sensor}_pixel_count_before"].astype("int32"),
                "pixel_count_after": manifest[f"{sensor}_pixel_count_after"].astype("int32"),
                "below_min_pixel_threshold": manifest["below_min_pixel_threshold"].astype(bool),
            }
        )
        sensor_manifest_path = run_dir / "manifest" / f"fields_{sensor}_{region}.parquet"
        sensor_manifest_path.parent.mkdir(parents=True, exist_ok=True)
        sensor_manifest.to_parquet(sensor_manifest_path, index=False, compression="zstd")
        log(
            f"Wrote {sensor.upper()} {region} membership: "
            f"{len(arrays['field_index']):,} field-pixel links"
        )

    return manifest


def prepare(args: argparse.Namespace) -> None:
    run_dir = Path(args.run_dir).resolve()
    if (run_dir / "run_manifest.json").exists() and not args.overwrite:
        raise FileExistsError(
            f"Run already prepared: {run_dir}. Use a new run ID or pass --overwrite."
        )
    if args.overwrite and run_dir.exists():
        shutil.rmtree(run_dir)
    run_dir.mkdir(parents=True, exist_ok=True)

    gpkg_path = Path(args.gpkg_path).resolve()
    inputs = {"s1": Path(args.s1_input_dir).resolve(), "s2": Path(args.s2_input_dir).resolve()}
    layer_names = {"machakos": args.machakos_layer, "nakuru": args.nakuru_layer}
    selected_ids: set[str] | None = None
    if args.field_id_file:
        selected_ids = {
            line.strip()
            for line in Path(args.field_id_file).read_text().splitlines()
            if line.strip()
        }
        if not selected_ids:
            raise ValueError(f"Field ID file is empty: {args.field_id_file}")

    inventories: dict[str, list[RasterRecord]] = {}
    grids_by_sensor: dict[str, dict[str, GridSignature]] = {}
    for sensor in SENSORS:
        inventories[sensor], grids_by_sensor[sensor] = inventory_rasters(sensor, inputs[sensor])
        if args.raster_start_date:
            inventories[sensor] = [
                record for record in inventories[sensor]
                if record.start_date >= args.raster_start_date
            ]
        if args.raster_end_date:
            inventories[sensor] = [
                record for record in inventories[sensor]
                if record.start_date <= args.raster_end_date
            ]
        available_regions = {record.region for record in inventories[sensor]}
        if available_regions != set(REGIONS):
            raise ValueError(
                f"Raster date filter leaves incomplete {sensor.upper()} regions: "
                f"found={sorted(available_regions)}, required={list(REGIONS)}"
            )

    all_field_ids: set[str] = set()
    region_manifests: list[pd.DataFrame] = []
    field_counts: dict[str, int] = {}
    for region in REGIONS:
        fields = load_fields(
            gpkg_path, layer_names[region], args.uid_field, args.limit_fields, selected_ids
        )
        duplicate_across_regions = all_field_ids.intersection(fields["field_id"])
        if duplicate_across_regions:
            raise ValueError(
                "field_id must be globally unique because the database key does not include region. "
                f"Examples: {sorted(duplicate_across_regions)[:10]}"
            )
        all_field_ids.update(fields["field_id"])
        field_counts[region] = len(fields)
        region_manifest = build_region_manifest(
            run_dir=run_dir,
            region=region,
            layer_name=layer_names[region],
            fields=fields,
            grids={sensor: grids_by_sensor[sensor][region] for sensor in SENSORS},
            min_pixels=args.min_pixels,
            tile_size=args.tile_size,
        )
        region_manifests.append(region_manifest)

    if selected_ids is not None:
        missing_ids = selected_ids - all_field_ids
        if missing_ids:
            raise ValueError(
                f"Field ID file contains {len(missing_ids)} IDs absent from both layers; "
                f"examples: {sorted(missing_ids)[:10]}"
            )

    common_manifest = pd.concat(region_manifests, ignore_index=True)
    common_manifest_path = run_dir / "manifest" / "fields_common.parquet"
    common_manifest.to_parquet(common_manifest_path, index=False, compression="zstd")

    inventory_records = [asdict(record) for sensor in SENSORS for record in inventories[sensor]]
    inventory_frame = pd.DataFrame(inventory_records).sort_values(["sensor", "region", "start_date"])
    inventory_path = run_dir / "manifest" / "rasters.parquet"
    inventory_frame.to_parquet(inventory_path, index=False, compression="zstd")

    grid_payload = {
        f"{sensor}_{region}": asdict(grids_by_sensor[sensor][region])
        for sensor in SENSORS
        for region in REGIONS
    }
    raster_counts = {
        sensor: {
            region: sum(record.region == region for record in inventories[sensor])
            for region in REGIONS
        }
        for sensor in SENSORS
    }
    expected_rows = {
        sensor: sum(field_counts[region] * raster_counts[sensor][region] for region in REGIONS)
        for sensor in SENSORS
    }
    gaps = {
        sensor: {
            region: weekly_gaps([r for r in inventories[sensor] if r.region == region])
            for region in REGIONS
        }
        for sensor in SENSORS
    }
    summary = {
        "pipeline_version": PIPELINE_VERSION,
        "created_at_utc": utc_now(),
        "run_dir": str(run_dir),
        "gpkg_path": str(gpkg_path),
        "gpkg_sha256": sha256_file(gpkg_path),
        "uid_field": args.uid_field,
        "layers": layer_names,
        "input_dirs": {sensor: str(path) for sensor, path in inputs.items()},
        "field_counts": field_counts,
        "total_fields": sum(field_counts.values()),
        "raster_counts": raster_counts,
        "expected_rows": expected_rows,
        "minimum_pixels": args.min_pixels,
        "tile_size": args.tile_size,
        "limit_fields": args.limit_fields,
        "field_id_file": str(Path(args.field_id_file).resolve()) if args.field_id_file else None,
        "raster_start_date": args.raster_start_date,
        "raster_end_date": args.raster_end_date,
        "grid_signatures": grid_payload,
        "weekly_gaps": gaps,
        "below_minimum_fields": int(common_manifest["below_min_pixel_threshold"].sum()),
        "buffer_distribution": {
            str(width): int((common_manifest["buffer_width_m"] == width).sum())
            for width in (10, 5, 0)
        },
        "status": "prepared",
    }
    atomic_write_json(run_dir / "run_manifest.json", summary)
    log(f"Preparation complete: {run_dir}")
    log(json.dumps({key: summary[key] for key in ("field_counts", "raster_counts", "expected_rows", "buffer_distribution", "below_minimum_fields")}, indent=2))


_WORKER_MEMBERSHIP: dict[str, np.ndarray] | None = None
_WORKER_STATIC_MANIFEST: Path | None = None
_WORKER_GRID: GridSignature | None = None
_WORKER_SENSOR: str | None = None
_WORKER_REGION: str | None = None
_WORKER_TEMP_ROOT: Path | None = None
_WORKER_MEMORY_LIMIT: str | None = None


def init_extract_worker(
    membership_path: str,
    static_manifest_path: str,
    grid_payload: dict[str, Any],
    sensor: str,
    region: str,
    temp_root: str,
    memory_limit: str,
) -> None:
    global _WORKER_MEMBERSHIP, _WORKER_STATIC_MANIFEST, _WORKER_GRID
    global _WORKER_SENSOR, _WORKER_REGION, _WORKER_TEMP_ROOT, _WORKER_MEMORY_LIMIT
    with np.load(membership_path, allow_pickle=False) as archive:
        _WORKER_MEMBERSHIP = {name: archive[name] for name in archive.files}
    _WORKER_STATIC_MANIFEST = Path(static_manifest_path)
    _WORKER_GRID = GridSignature.from_dict(grid_payload)
    _WORKER_SENSOR = sensor
    _WORKER_REGION = region
    _WORKER_TEMP_ROOT = Path(temp_root)
    _WORKER_TEMP_ROOT.mkdir(parents=True, exist_ok=True)
    _WORKER_MEMORY_LIMIT = memory_limit


def gathered_schema(sensor: str) -> pa.Schema:
    return pa.schema(
        [pa.field("field_index", pa.int32())]
        + [pa.field(name, pa.float32()) for name in BANDS[sensor]]
    )


def write_gathered_values(
    raster_path: Path,
    gathered_path: Path,
    batch_rows: int,
) -> int:
    assert _WORKER_MEMBERSHIP is not None
    assert _WORKER_GRID is not None
    assert _WORKER_SENSOR is not None
    sensor = _WORKER_SENSOR
    membership = _WORKER_MEMBERSHIP
    tile_size = int(membership["tile_size"][0])
    tile_columns = math.ceil(_WORKER_GRID.width / tile_size)

    writer = pq.ParquetWriter(gathered_path, gathered_schema(sensor), compression="zstd")
    pending_fields: list[np.ndarray] = []
    pending_values: list[np.ndarray] = []
    pending_count = 0
    total = 0

    def flush() -> None:
        nonlocal pending_count, total
        if pending_count == 0:
            return
        field_values = np.concatenate(pending_fields).astype(np.int32, copy=False)
        values = np.concatenate(pending_values, axis=0).astype(np.float32, copy=False)
        arrays: list[pa.Array] = [pa.array(field_values, type=pa.int32())]
        for band_index in range(values.shape[1]):
            column = values[:, band_index]
            invalid = ~np.isfinite(column)
            arrays.append(pa.array(column, mask=invalid, type=pa.float32()))
        writer.write_table(pa.Table.from_arrays(arrays, schema=gathered_schema(sensor)))
        total += pending_count
        pending_fields.clear()
        pending_values.clear()
        pending_count = 0

    try:
        with rasterio.open(raster_path) as dataset:
            signature = GridSignature.from_dataset(dataset)
            if not _WORKER_GRID.grid_equals(signature):
                raise ValueError(f"Grid changed since prepare: {raster_path}")
            validate_band_descriptions(dataset, sensor)

            for position, tile_id_value in enumerate(membership["tile_ids"]):
                start = int(membership["tile_offsets"][position])
                stop = int(membership["tile_offsets"][position + 1])
                tile_id = int(tile_id_value)
                tile_row = tile_id // tile_columns
                tile_col = tile_id % tile_columns
                row_off = tile_row * tile_size
                col_off = tile_col * tile_size
                height = min(tile_size, dataset.height - row_off)
                width = min(tile_size, dataset.width - col_off)
                window = Window(col_off, row_off, width, height)

                pixels = membership["flat_pixel"][start:stop]
                local_rows = pixels // dataset.width - row_off
                local_cols = pixels % dataset.width - col_off
                tile = dataset.read(window=window, masked=True)
                selected = tile[:, local_rows, local_cols].T
                values = np.asarray(selected.filled(np.nan), dtype=np.float32)
                selected_mask = np.ma.getmaskarray(selected)
                if selected_mask.any():
                    values[selected_mask] = np.nan
                values[~np.isfinite(values)] = np.nan

                pending_fields.append(membership["field_index"][start:stop])
                pending_values.append(values)
                pending_count += stop - start
                if pending_count >= batch_rows:
                    flush()
        flush()
    finally:
        writer.close()
    return total


def stat_select_sql(sensor: str) -> str:
    expressions: list[str] = []
    for band in BANDS[sensor]:
        quoted = '"' + band.replace('"', '""') + '"'
        expressions.extend(
            [
                f"CAST(avg({quoted}) AS REAL) AS {band}_mean",
                f"CAST(median({quoted}) AS REAL) AS {band}_median",
                f"CAST(min({quoted}) AS REAL) AS {band}_min",
                f"CAST(max({quoted}) AS REAL) AS {band}_max",
                f"CAST(stddev_pop({quoted}) AS REAL) AS {band}_std",
                f"CAST(count({quoted}) AS INTEGER) AS {band}_count",
            ]
        )
    return ",\n            ".join(expressions)


def output_select_sql(sensor: str) -> str:
    expressions: list[str] = []
    for band in BANDS[sensor]:
        for suffix in ("mean", "median", "min", "max", "std"):
            expressions.append(f"a.{band}_{suffix}")
        expressions.append(f"CAST(coalesce(a.{band}_count, 0) AS INTEGER) AS {band}_count")
    return ",\n        ".join(expressions)


def expected_output_columns(sensor: str) -> list[str]:
    base = [
        "field_id",
        "region",
        "layer_name",
        "source",
        "raster_file",
        "start_date",
        "end_date",
        "iso_year",
        "week_no",
        "buffer_width_m",
        "pixel_count_before",
        "pixel_count_after",
        "below_min_pixel_threshold",
    ]
    stats = [f"{band}_{suffix}" for band in BANDS[sensor] for suffix in ("mean", "median", "min", "max", "std", "count")]
    return base + stats


def aggregate_gathered(
    gathered_path: Path,
    output_path: Path,
    record: RasterRecord,
    temp_dir: Path,
) -> None:
    assert _WORKER_STATIC_MANIFEST is not None
    assert _WORKER_SENSOR is not None
    assert _WORKER_MEMORY_LIMIT is not None
    sensor = _WORKER_SENSOR
    temp_dir.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        connection.execute("SET threads = 1")
        connection.execute(f"SET memory_limit = {sql_literal(_WORKER_MEMORY_LIMIT)}")
        connection.execute(f"SET temp_directory = {sql_literal(temp_dir)}")
        query = f"""
        COPY (
            WITH aggregated AS (
                SELECT
                    field_index,
                    {stat_select_sql(sensor)}
                FROM read_parquet({sql_literal(gathered_path)})
                GROUP BY field_index
            )
            SELECT
                m.field_id,
                m.region,
                m.layer_name,
                {sql_literal(sensor)}::VARCHAR AS source,
                {sql_literal(record.raster_file)}::VARCHAR AS raster_file,
                DATE {sql_literal(record.start_date)} AS start_date,
                DATE {sql_literal(record.end_date)} AS end_date,
                {record.iso_year}::SMALLINT AS iso_year,
                {record.week_no}::SMALLINT AS week_no,
                m.buffer_width_m::TINYINT AS buffer_width_m,
                m.pixel_count_before::INTEGER AS pixel_count_before,
                m.pixel_count_after::INTEGER AS pixel_count_after,
                m.below_min_pixel_threshold,
                {output_select_sql(sensor)}
            FROM read_parquet({sql_literal(_WORKER_STATIC_MANIFEST)}) AS m
            LEFT JOIN aggregated AS a USING (field_index)
            ORDER BY m.field_index
        ) TO {sql_literal(output_path)}
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 100000)
        """
        connection.execute(query)
    finally:
        connection.close()


def validate_part(path: Path, sensor: str, expected_rows: int) -> None:
    parquet = pq.ParquetFile(path)
    if parquet.metadata.num_rows != expected_rows:
        raise ValueError(
            f"{path}: expected {expected_rows:,} rows, found {parquet.metadata.num_rows:,}"
        )
    names = parquet.schema_arrow.names
    expected = expected_output_columns(sensor)
    if names != expected:
        raise ValueError(f"{path}: output schema mismatch\nexpected={expected}\nactual={names}")


def extract_one_raster(task: dict[str, Any]) -> dict[str, Any]:
    record = RasterRecord(**task["record"])
    output_path = Path(task["output_path"])
    expected_rows = int(task["expected_rows"])
    overwrite = bool(task["overwrite"])
    batch_rows = int(task["batch_rows"])
    sensor = record.sensor

    if output_path.exists() and not overwrite:
        try:
            validate_part(output_path, sensor, expected_rows)
            return {"status": "skipped", "path": str(output_path), "rows": expected_rows}
        except Exception:
            output_path.rename(output_path.with_suffix(output_path.suffix + ".invalid"))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    assert _WORKER_TEMP_ROOT is not None
    work_dir = Path(tempfile.mkdtemp(prefix=f"{sensor}-{record.region}-{record.start_date}-", dir=_WORKER_TEMP_ROOT))
    gathered_path = work_dir / "gathered.parquet"
    temporary_output = work_dir / "output.parquet"
    started = time.monotonic()
    try:
        memberships = write_gathered_values(Path(record.path), gathered_path, batch_rows)
        aggregate_gathered(gathered_path, temporary_output, record, work_dir / "duckdb")
        validate_part(temporary_output, sensor, expected_rows)
        os.replace(temporary_output, output_path)
        return {
            "status": "written",
            "path": str(output_path),
            "rows": expected_rows,
            "memberships": memberships,
            "elapsed_seconds": round(time.monotonic() - started, 3),
        }
    except Exception as exc:
        return {
            "status": "failed",
            "path": str(output_path),
            "error": str(exc),
            "traceback": traceback.format_exc(),
        }
    finally:
        shutil.rmtree(work_dir, ignore_errors=True)


def part_path(run_dir: Path, record: RasterRecord) -> Path:
    stem = Path(record.raster_file).stem
    return (
        run_dir
        / "parts"
        / record.sensor
        / f"region={record.region}"
        / f"year={int(record.start_date[:4]):04d}"
        / f"{stem}.parquet"
    )


def extract(args: argparse.Namespace) -> None:
    run_dir = Path(args.run_dir).resolve()
    run_manifest = json.loads((run_dir / "run_manifest.json").read_text())
    inventory = pd.read_parquet(run_dir / "manifest" / "rasters.parquet")
    sensors = list(SENSORS) if args.sensor == "both" else [args.sensor]
    failures: list[dict[str, Any]] = []

    for sensor in sensors:
        for region in REGIONS:
            selected = inventory[(inventory.sensor == sensor) & (inventory.region == region)].copy()
            if args.start_date:
                selected = selected[selected.start_date >= args.start_date]
            if args.end_date:
                selected = selected[selected.start_date <= args.end_date]
            selected = selected.sort_values("start_date")
            if selected.empty:
                log(f"No selected rasters for {sensor.upper()} {region}")
                continue

            membership_path = run_dir / "membership" / f"{sensor}_{region}.npz"
            static_manifest_path = run_dir / "manifest" / f"fields_{sensor}_{region}.parquet"
            expected_rows = int(run_manifest["field_counts"][region])
            grid_payload = run_manifest["grid_signatures"][f"{sensor}_{region}"]
            temp_root = run_dir / "tmp" / sensor / region
            tasks = []
            for row in selected.to_dict("records"):
                record = RasterRecord(**{key: row[key] for key in RasterRecord.__dataclass_fields__})
                tasks.append(
                    {
                        "record": asdict(record),
                        "output_path": str(part_path(run_dir, record)),
                        "expected_rows": expected_rows,
                        "overwrite": args.overwrite,
                        "batch_rows": args.batch_rows,
                    }
                )

            log(
                f"Extracting {sensor.upper()} {region}: {len(tasks)} rasters, "
                f"{expected_rows:,} fields each, workers={args.workers}"
            )
            completed = 0
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=args.workers,
                initializer=init_extract_worker,
                initargs=(
                    str(membership_path),
                    str(static_manifest_path),
                    grid_payload,
                    sensor,
                    region,
                    str(temp_root),
                    args.duckdb_memory_limit,
                ),
            ) as executor:
                futures = {executor.submit(extract_one_raster, task): task for task in tasks}
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    completed += 1
                    if result["status"] == "failed":
                        failures.append(result)
                        log(f"FAILED {result['path']}: {result['error']}")
                    else:
                        log(
                            f"{sensor.upper()} {region} {completed}/{len(tasks)} "
                            f"{result['status']}: {Path(result['path']).name}"
                        )

    if failures:
        failure_path = run_dir / "logs" / f"extract_failures_{datetime.now().strftime('%Y%m%dT%H%M%S')}.json"
        atomic_write_json(failure_path, failures)
        raise RuntimeError(f"Extraction completed with {len(failures)} failures; see {failure_path}")
    log("Extraction completed without failures")


def validate_basic(run_dir: Path, sensors: Sequence[str]) -> dict[str, Any]:
    manifest = json.loads((run_dir / "run_manifest.json").read_text())
    inventory = pd.read_parquet(run_dir / "manifest" / "rasters.parquet")
    report: dict[str, Any] = {"checked_at_utc": utc_now(), "sensors": {}, "failures": []}

    for sensor in sensors:
        sensor_report = {"expected_parts": 0, "found_parts": 0, "rows": 0}
        for row in inventory[inventory.sensor == sensor].to_dict("records"):
            record = RasterRecord(**{key: row[key] for key in RasterRecord.__dataclass_fields__})
            path = part_path(run_dir, record)
            sensor_report["expected_parts"] += 1
            if not path.exists():
                report["failures"].append(f"Missing part: {path}")
                continue
            sensor_report["found_parts"] += 1
            expected_rows = int(manifest["field_counts"][record.region])
            try:
                validate_part(path, sensor, expected_rows)
                sensor_report["rows"] += expected_rows
            except Exception as exc:
                report["failures"].append(str(exc))
        report["sensors"][sensor] = sensor_report
    return report


def full_validation_sql(run_dir: Path, sensor: str) -> dict[str, Any]:
    glob_path = run_dir / "parts" / sensor / "region=*" / "year=*" / "*.parquet"
    count_checks = " OR ".join(
        f"{band}_count < 0 OR {band}_count > pixel_count_after OR "
        f"{band}_count <> trunc({band}_count)"
        for band in BANDS[sensor]
    )
    connection = duckdb.connect()
    try:
        connection.execute("SET threads = 4")
        result = connection.execute(
            f"""
            SELECT
                count(*) AS rows,
                count(DISTINCT (field_id, start_date)) AS unique_keys,
                count(*) FILTER (WHERE buffer_width_m NOT IN (0, 5, 10)) AS bad_buffer,
                count(*) FILTER (WHERE pixel_count_after > pixel_count_before OR pixel_count_after < 0) AS bad_geometry_count,
                count(*) FILTER (WHERE {count_checks}) AS bad_valid_count
            FROM read_parquet({sql_literal(glob_path)}, hive_partitioning=true)
            """
        ).fetchone()
        return {
            "rows": int(result[0]),
            "unique_keys": int(result[1]),
            "bad_buffer": int(result[2]),
            "bad_geometry_count": int(result[3]),
            "bad_valid_count": int(result[4]),
        }
    finally:
        connection.close()


def validate(args: argparse.Namespace) -> None:
    run_dir = Path(args.run_dir).resolve()
    sensors = list(SENSORS) if args.sensor == "both" else [args.sensor]
    report = validate_basic(run_dir, sensors)
    manifest = json.loads((run_dir / "run_manifest.json").read_text())
    if args.full and not report["failures"]:
        report["full_checks"] = {}
        for sensor in sensors:
            result = full_validation_sql(run_dir, sensor)
            report["full_checks"][sensor] = result
            expected = int(manifest["expected_rows"][sensor])
            if result["rows"] != expected:
                report["failures"].append(
                    f"{sensor.upper()} full row count {result['rows']:,} != expected {expected:,}"
                )
            if result["unique_keys"] != result["rows"]:
                report["failures"].append(f"{sensor.upper()} has duplicate (field_id, start_date) keys")
            for key in ("bad_buffer", "bad_geometry_count", "bad_valid_count"):
                if result[key] != 0:
                    report["failures"].append(f"{sensor.upper()} {key}={result[key]:,}")

    report["status"] = "passed" if not report["failures"] else "failed"
    report_path = run_dir / "validation_report.json"
    atomic_write_json(report_path, report)
    log(json.dumps(report, indent=2))
    if report["failures"]:
        raise RuntimeError(f"Validation failed; see {report_path}")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", action="version", version=PIPELINE_VERSION)
    subparsers = parser.add_subparsers(dest="command", required=True)

    prepare_parser = subparsers.add_parser("prepare", help="Inventory rasters and build adaptive pixel membership")
    prepare_parser.add_argument("--run-dir", required=True)
    prepare_parser.add_argument("--gpkg-path", required=True)
    prepare_parser.add_argument("--uid-field", default="field_id")
    prepare_parser.add_argument("--machakos-layer", default="machakos_fin")
    prepare_parser.add_argument("--nakuru-layer", default="nakuru_fin")
    prepare_parser.add_argument("--s1-input-dir", required=True)
    prepare_parser.add_argument("--s2-input-dir", required=True)
    prepare_parser.add_argument("--min-pixels", type=int, default=5)
    prepare_parser.add_argument("--tile-size", type=int, default=1024)
    prepare_parser.add_argument("--limit-fields", type=int, help="Pilot only: limit fields per region")
    prepare_parser.add_argument("--field-id-file", help="Pilot only: one field_id per line")
    prepare_parser.add_argument("--raster-start-date", help="Pilot only: inclusive YYYY-MM-DD inventory filter")
    prepare_parser.add_argument("--raster-end-date", help="Pilot only: inclusive YYYY-MM-DD inventory filter")
    prepare_parser.add_argument("--overwrite", action="store_true")
    prepare_parser.set_defaults(func=prepare)

    extract_parser = subparsers.add_parser("extract", help="Extract weekly zonal statistics")
    extract_parser.add_argument("--run-dir", required=True)
    extract_parser.add_argument("--sensor", choices=["s1", "s2", "both"], default="both")
    extract_parser.add_argument("--workers", type=int, default=2)
    extract_parser.add_argument("--batch-rows", type=int, default=500_000)
    extract_parser.add_argument("--duckdb-memory-limit", default="6GB")
    extract_parser.add_argument("--start-date", help="Inclusive YYYY-MM-DD pilot filter")
    extract_parser.add_argument("--end-date", help="Inclusive YYYY-MM-DD pilot filter")
    extract_parser.add_argument("--overwrite", action="store_true")
    extract_parser.set_defaults(func=extract)

    validate_parser = subparsers.add_parser("validate", help="Validate all generated Parquet parts")
    validate_parser.add_argument("--run-dir", required=True)
    validate_parser.add_argument("--sensor", choices=["s1", "s2", "both"], default="both")
    validate_parser.add_argument("--full", action="store_true", help="Scan all values with DuckDB")
    validate_parser.set_defaults(func=validate)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if getattr(args, "workers", 1) < 1:
        parser.error("--workers must be at least 1")
    if getattr(args, "min_pixels", 1) < 1:
        parser.error("--min-pixels must be at least 1")
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
