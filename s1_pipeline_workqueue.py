#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Operational Sentinel-1 weekly pipeline with AOI-subset SNAP preprocessing.

Tujuan versi ini:
1) Tetap 10 m
2) Lebih cepat daripada full-scene TC biasa
3) Memilih scene harian dengan overlap AOI terbesar, bukan sekadar scene pertama
4) Menghindari banyak scene kecil/tidak relevan yang hanya menyentuh ujung AOI

Workflow:
1. Query & download SAFE dari CDSE
2. Hitung overlap footprint vs AOI
3. Pilih N scene per hari dengan overlap terbesar
4. SNAP subset -> orbit -> border noise -> thermal noise -> calibration -> terrain correction
5. Clip ke grid regional
6. Daily mosaic
7. Weekly composite
8. Optional indices
"""

import argparse
import json
import os
import random
import shutil
import subprocess
import threading
import time
import warnings
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date
from pathlib import Path
from urllib.parse import quote

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import requests
from rasterio.enums import Resampling
from rasterio.features import geometry_mask
from rasterio.vrt import WarpedVRT
from rasterio.windows import Window
from shapely import wkt as shapely_wkt
from shapely.geometry import shape as shapely_shape


CATALOG_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
DOWNLOAD_PRODUCTS_URL = "https://download.dataspace.copernicus.eu/odata/v1/Products"

DEFAULT_REGIONS = ["Nakuru", "Machakos"]
TARGET_BANDS = ["VV", "VH"]
BAND_NAMES = TARGET_BANDS.copy()

DEFAULT_USERNAMES = [
    "Sentinel1@geo-infinity.com",
    "Sentinel2@geo-infinity.com",
    "Sentinel3@geo-infinity.com",
    "Sentinel4@geo-infinity.com",
]

DEFAULT_PRODUCT_TYPES = ["IW_GRDH_1S"]

GLOBAL_DOWNLOAD_SEMAPHORE = None
GLOBAL_WEEK_DOWNLOAD_SEMAPHORE = None
GLOBAL_ACCOUNT_POOL = None

GLOBAL_AUTH_COOLDOWN_UNTIL = 0.0
GLOBAL_AUTH_COOLDOWN_LOCK = threading.Lock()


def parse_args():
    p = argparse.ArgumentParser(description="Sentinel-1 weekly pipeline with overlap-aware scene selection")

    p.add_argument("--date-from", default=None, help="YYYY-MM-DD")
    p.add_argument("--date-to", default=None, help="YYYY-MM-DD")

    p.add_argument("--run-mode", choices=["single_week", "backfill_weeknum"], default="single_week")
    p.add_argument("--start-year", type=int, default=None)
    p.add_argument("--start-week", type=int, default=None)
    p.add_argument("--end-year", type=int, default=None)
    p.add_argument("--end-week", type=int, default=None)
    p.add_argument("--max-week-workers", type=int, default=1)
    p.add_argument("--week-order", choices=["asc", "desc"], default="desc")

    p.add_argument("--shp-path", required=True)
    p.add_argument("--regions", nargs="+", default=DEFAULT_REGIONS)

    p.add_argument("--workdir", required=True)
    p.add_argument("--target-final-root", default=None)
    p.add_argument("--existing-final-root", default=None)
    p.add_argument("--delete-intermediate-on-success", action="store_true", dest="delete_intermediate_on_success")

    p.add_argument("--usernames", nargs="+", default=DEFAULT_USERNAMES)
    p.add_argument("--password", default=os.getenv("CDSE_PASSWORD"))

    p.add_argument(
        "--product-types",
        nargs="+",
        default=DEFAULT_PRODUCT_TYPES,
        help="Daftar full CDSE S1 productType. Contoh: IW_GRDH_1S IW_GRDM_1S",
    )
    p.add_argument("--sensor-mode", default="IW")
    p.add_argument("--polarisation", default=None)

    p.add_argument("--top-n", type=int, default=100)
    p.add_argument("--target-res", type=float, default=10.0)

    p.add_argument("--download-workers", type=int, default=4)
    p.add_argument("--global-download-slots", type=int, default=4)
    p.add_argument("--max-weeks-in-download", type=int, default=1)
    p.add_argument("--download-max-retries-per-file", type=int, default=8)
    p.add_argument("--download-max-rounds", type=int, default=30)
    p.add_argument("--download-round-sleep", type=int, default=90)
    p.add_argument("--download-backoff-base", type=float, default=12.0)
    p.add_argument("--download-backoff-max", type=float, default=240.0)

    p.add_argument("--snap-workers", type=int, default=2)
    p.add_argument("--temp-workers", type=int, default=1)
    p.add_argument("--daily-workers", type=int, default=1)
    p.add_argument("--weekly-reducer", choices=["nanmean", "nanmedian", "lastvalid", "firstvalid"], default="nanmean")
    p.add_argument("--weekly-chunk-size", type=int, default=10000)

    p.add_argument("--overwrite", action="store_true")
    p.add_argument("--skip-query-download", action="store_true")
    p.add_argument("--skip-unzip", action="store_true")
    p.add_argument("--compress-final", action="store_true")
    p.add_argument("--cleanup-zips", action="store_true")
    p.add_argument("--cleanup-safe", action="store_true")
    p.add_argument("--save-manifest-json", action="store_true")
    p.add_argument("--allow-partial-regions", action="store_true")

    p.add_argument("--compute-indices", action="store_true")

    p.add_argument("--use-snap-preprocessing", action="store_true")
    p.add_argument("--gpt-path", default="gpt")
    p.add_argument("--snap-dem-name", default="Copernicus 30m Global DEM")
    p.add_argument("--snap-pixel-spacing", type=float, default=10.0)

    p.add_argument(
        "--scene-selection-mode",
        choices=["all_overlap", "top_n_daily", "adaptive_daily_cover", "adaptive_weekly_cover"],
        default="adaptive_weekly_cover",
    )
    p.add_argument("--max-scenes-per-day", type=int, default=1)
    p.add_argument("--adaptive-max-scenes", type=int, default=0)
    p.add_argument("--coverage-target-ratio", type=float, default=0.995)
    p.add_argument("--min-incremental-overlap-area-deg2", type=float, default=0.0)
    p.add_argument("--min-weekly-valid-coverage-ratio", type=float, default=0.0)
    p.add_argument("--overlap-buffer-deg", type=float, default=0.05)
    p.add_argument("--min-overlap-area-deg2", type=float, default=0.0)
    p.add_argument("--snap-subset-buffer-deg", type=float, default=0.05)

    return p.parse_args()


def ensure_dir(path):
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def iso_start(d):
    return f"{d}T00:00:00.000Z"


def iso_end(d):
    return f"{d}T23:59:59.999Z"


def safe_zip_name(product_name):
    return product_name if product_name.endswith(".zip") else f"{product_name}.zip"


def save_csv(df, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"[SAVE] {path}")


def cleanup_dir(path):
    path = Path(path)
    if path.exists():
        shutil.rmtree(path, ignore_errors=True)
        print(f"[CLEANUP] removed: {path}")


def now_label():
    return time.strftime("%Y-%m-%d %H:%M:%S")


def format_duration(seconds):
    seconds = float(seconds or 0.0)
    if seconds < 60:
        return f"{seconds:.1f}s"

    minutes, rem = divmod(seconds, 60)
    if minutes < 60:
        return f"{int(minutes)}m {rem:.1f}s"

    hours, minutes = divmod(minutes, 60)
    return f"{int(hours)}h {int(minutes)}m {rem:.1f}s"


def record_stage_time(stage_times, stage_name, started_at):
    elapsed = time.time() - started_at
    stage_times[stage_name] = elapsed
    print(f"[TIMING] {stage_name}: {format_duration(elapsed)}")
    return elapsed


def snap_bounds_to_resolution(bounds, res):
    minx, miny, maxx, maxy = bounds
    res = float(res)
    return (
        float(np.floor(minx / res) * res),
        float(np.floor(miny / res) * res),
        float(np.ceil(maxx / res) * res),
        float(np.ceil(maxy / res) * res),
    )


def finite_float(value, default=0.0):
    try:
        out = float(value)
    except Exception:
        return default
    return out if np.isfinite(out) else default


def clean_s1_linear_array(arr, nodata=None):
    arr = arr.astype(np.float32, copy=False)
    if nodata is not None and not np.isnan(nodata):
        arr[arr == nodata] = np.nan
    arr[~np.isfinite(arr)] = np.nan
    arr[arr <= 0] = np.nan
    return arr


def calc_backoff(attempt, base=8.0, max_wait=180.0, jitter=0.35):
    wait = min(max_wait, base * (2 ** max(0, attempt - 1)))
    low = wait * (1 - jitter)
    high = wait * (1 + jitter)
    return max(1.0, random.uniform(low, high))


def wait_for_global_auth_cooldown():
    global GLOBAL_AUTH_COOLDOWN_UNTIL
    while True:
        with GLOBAL_AUTH_COOLDOWN_LOCK:
            now = time.time()
            wait_s = GLOBAL_AUTH_COOLDOWN_UNTIL - now
        if wait_s <= 0:
            return
        sleep_s = min(wait_s, 5.0)
        print(f"[AUTH-COOLDOWN] waiting {sleep_s:.1f}s...")
        time.sleep(sleep_s)


def set_global_auth_cooldown(seconds):
    global GLOBAL_AUTH_COOLDOWN_UNTIL
    until = time.time() + max(1.0, seconds)
    with GLOBAL_AUTH_COOLDOWN_LOCK:
        GLOBAL_AUTH_COOLDOWN_UNTIL = max(GLOBAL_AUTH_COOLDOWN_UNTIL, until)


def iso_week_to_date_range(year, week):
    week_start = date.fromisocalendar(year, week, 1)
    week_end = date.fromisocalendar(year, week, 7)
    return week_start, week_end


def list_iso_weeks(start_year, start_week, end_year, end_week):
    out = []
    y = start_year
    w = start_week
    while True:
        out.append((y, w))
        if y == end_year and w == end_week:
            break
        try:
            date.fromisocalendar(y, w + 1, 1)
            w += 1
        except ValueError:
            y += 1
            w = 1
    return out


def current_iso_year_week():
    y, w, _ = date.today().isocalendar()
    return y, w


def final_outputs_exist(final_root, regions, date_from, date_to, require_indices=False):
    final_root = Path(final_root)
    weekly_root = final_root / "weekly"
    indices_root = final_root / "indices"

    for region_name in regions:
        weekly_path = weekly_root / f"{region_name.lower()}_{date_from}_{date_to}_weekly_clean_10m.tif"
        if not weekly_path.exists():
            return False

        if require_indices:
            indices_path = indices_root / f"{region_name.lower()}_{date_from}_{date_to}_weekly_indices_10m.tif"
            if not indices_path.exists():
                return False

    return True


def parse_safe_name(safe_name):
    safe_name = Path(safe_name).name.replace(".zip", "").replace(".SAFE", "").replace(".dim", "")
    parts = safe_name.split("_")
    if len(parts) < 8:
        raise ValueError(f"SAFE name tidak valid: {safe_name}")

    platform = parts[0]
    sensor_mode = parts[1] if len(parts) > 1 else None
    product_type = parts[2] if len(parts) > 2 else None
    resolution_class = parts[3] if len(parts) > 3 else None
    acq_start = parts[4] if len(parts) > 4 else None
    acq_date = acq_start[:8] if acq_start else None
    polarisation_code = parts[3][-2:] if len(parts) > 3 and parts[3] and len(parts[3]) >= 2 else None

    return {
        "platform": platform,
        "sensor_mode": sensor_mode,
        "product_type": product_type,
        "resolution_class": resolution_class,
        "acq_date": acq_date,
        "tile_code": "S1",
        "polarisation_code": polarisation_code,
    }


def load_regions(shp_path, target_regions):
    gdf = gpd.read_file(shp_path).to_crs(4326)
    aoi_gdf = gdf[gdf["ADM1_EN"].isin(target_regions)].copy()
    found = sorted(aoi_gdf["ADM1_EN"].unique().tolist())
    expected = sorted(target_regions)
    if found != expected:
        raise ValueError(f"Region tidak lengkap. Ketemu: {found}, expected: {expected}")
    return aoi_gdf


def build_bbox_wkt_from_bounds(minx, miny, maxx, maxy):
    return (
        f"POLYGON(({minx} {miny}, "
        f"{maxx} {miny}, "
        f"{maxx} {maxy}, "
        f"{minx} {maxy}, "
        f"{minx} {miny}))"
    )


def split_bounds_into_four(bounds):
    minx, miny, maxx, maxy = bounds
    midx = (minx + maxx) / 2.0
    midy = (miny + maxy) / 2.0
    return [
        (minx, miny, midx, midy),
        (midx, miny, maxx, midy),
        (minx, midy, midx, maxy),
        (midx, midy, maxx, maxy),
    ]


def build_odata_url_s1(bbox_wkt, date_from, date_to, top_n=100, product_type="IW_GRDH_1S"):
    clauses = [
        "Collection/Name eq 'SENTINEL-1'",
        "Attributes/OData.CSC.StringAttribute/any(att:att/Name eq 'productType' and att/OData.CSC.StringAttribute/Value eq '{}')".format(product_type),
        f"OData.CSC.Intersects(area=geography'SRID=4326;{bbox_wkt}')",
        f"ContentDate/Start ge {date_from}",
        f"ContentDate/Start le {date_to}",
    ]

    filter_expr = " and ".join(clauses)
    encoded_filter = quote(filter_expr, safe="()=;,:/' ")
    return f"{CATALOG_URL}?$filter={encoded_filter}&$top={top_n}"


def filter_s1_results_locally(df, sensor_mode=None, polarisation=None):
    if df.empty:
        return df

    out = df.copy()

    if sensor_mode:
        sensor_mode_u = sensor_mode.upper()
        out = out[
            out["sensor_mode"].fillna("").str.upper().eq(sensor_mode_u)
            | out["name"].fillna("").str.contains(f"_{sensor_mode_u}_", case=False, regex=False)
        ].copy()

    if polarisation:
        pol_u = polarisation.upper()
        out = out[
            out["polarisation_code"].fillna("").str.upper().eq(pol_u)
            | out["name"].fillna("").str.contains(pol_u, case=False, regex=False)
        ].copy()

    return out.reset_index(drop=True)


def safe_parse_footprint_to_geom(footprint_value):
    if not footprint_value:
        return None
    if isinstance(footprint_value, dict):
        try:
            return shapely_shape(footprint_value)
        except Exception:
            return None
    try:
        return shapely_wkt.loads(footprint_value)
    except Exception:
        try:
            return shapely_shape(footprint_value)
        except Exception:
            return None


def add_overlap_metrics(scenes_df, aoi_gdf, buffer_deg=0.05):
    if scenes_df.empty:
        return scenes_df

    out = scenes_df.copy()
    out["footprint_geom"] = out["footprint"].apply(safe_parse_footprint_to_geom)

    region_geom_map = {}
    region_bbox_map = {}
    for region_name in sorted(aoi_gdf["ADM1_EN"].unique()):
        reg = aoi_gdf[aoi_gdf["ADM1_EN"] == region_name].copy()
        geom = reg.union_all()
        region_geom_map[region_name] = geom
        region_bbox_map[region_name] = geom.buffer(buffer_deg).envelope

    overlap_area = []
    overlap_ratio = []
    bbox_hit = []

    for _, row in out.iterrows():
        reg = row["region"]
        fp = row["footprint_geom"]
        region_geom = region_geom_map.get(reg)
        region_bbox = region_bbox_map.get(reg)

        if fp is None or region_geom is None:
            overlap_area.append(0.0)
            overlap_ratio.append(0.0)
            bbox_hit.append(False)
            continue

        try:
            hit = fp.intersects(region_bbox)
            bbox_hit.append(bool(hit))
            if not hit:
                overlap_area.append(0.0)
                overlap_ratio.append(0.0)
                continue

            inter = fp.intersection(region_geom)
            ia = float(inter.area) if (inter is not None and not inter.is_empty) else 0.0
            fa = float(fp.area) if fp.area else 0.0
            rr = (ia / fa) if fa > 0 else 0.0
            overlap_area.append(ia)
            overlap_ratio.append(rr)
        except Exception:
            overlap_area.append(0.0)
            overlap_ratio.append(0.0)
            bbox_hit.append(False)

    out["overlap_area_deg2"] = overlap_area
    out["overlap_ratio"] = overlap_ratio
    out["bbox_hit"] = bbox_hit
    return out


def limit_scenes_per_day(scenes, max_scenes_per_day=1):
    if scenes is None or scenes.empty:
        return scenes
    if max_scenes_per_day is None or max_scenes_per_day <= 0:
        return scenes

    df = scenes.copy()
    before = len(df)

    sort_cols = [
        "region",
        "acq_date",
        "overlap_area_deg2",
        "overlap_ratio",
        "content_start",
        "name",
    ]
    ascending = [True, True, False, False, True, True]

    existing_sort_cols = [c for c in sort_cols if c in df.columns]
    existing_ascending = [ascending[sort_cols.index(c)] for c in existing_sort_cols]
    df = df.sort_values(existing_sort_cols, ascending=existing_ascending).reset_index(drop=True)

    df = (
        df.groupby(["region", "acq_date"], as_index=False, group_keys=False)
        .head(max_scenes_per_day)
        .reset_index(drop=True)
    )

    print(
        f"[SCENE LIMIT] before={before} | after={len(df)} | max_scenes_per_day={max_scenes_per_day}"
    )
    if "overlap_area_deg2" in df.columns:
        print(df[[c for c in ["region", "acq_date", "name", "overlap_area_deg2", "overlap_ratio"] if c in df.columns]])
    return df


def select_scenes_for_coverage(
    scenes,
    aoi_gdf,
    group_cols,
    coverage_target_ratio=0.995,
    max_scenes_per_group=0,
    min_incremental_overlap_area_deg2=0.0,
):
    if scenes is None or scenes.empty:
        return scenes

    df = scenes.copy()
    if "footprint_geom" not in df.columns:
        df["footprint_geom"] = df["footprint"].apply(safe_parse_footprint_to_geom)

    coverage_target_ratio = max(0.0, min(1.0, float(coverage_target_ratio)))
    max_scenes_per_group = int(max_scenes_per_group or 0)
    min_incremental_overlap_area_deg2 = float(min_incremental_overlap_area_deg2 or 0.0)

    region_geom_map = {}
    region_area_map = {}
    for region_name in sorted(aoi_gdf["ADM1_EN"].unique()):
        reg = aoi_gdf[aoi_gdf["ADM1_EN"] == region_name].copy()
        geom = reg.union_all().buffer(0)
        region_geom_map[region_name] = geom
        region_area_map[region_name] = float(geom.area) if geom and not geom.is_empty else 0.0

    df["selection_order"] = np.nan
    df["selection_incremental_area_deg2"] = np.nan
    df["selection_coverage_ratio"] = np.nan

    selected_indices = []
    before = len(df)

    sort_cols = [
        "overlap_area_deg2",
        "overlap_ratio",
        "content_start",
        "name",
    ]
    ascending = [False, False, True, True]
    existing_sort_cols = [c for c in sort_cols if c in df.columns]
    existing_ascending = [ascending[sort_cols.index(c)] for c in existing_sort_cols]

    for group_key, group in df.groupby(group_cols, sort=True):
        group_sorted = group.sort_values(existing_sort_cols, ascending=existing_ascending)
        region_name = group_sorted.iloc[0]["region"]
        region_geom = region_geom_map.get(region_name)
        region_area = region_area_map.get(region_name, 0.0)

        if region_geom is None or region_area <= 0:
            fallback_idx = group_sorted.index[0]
            selected_indices.append(fallback_idx)
            df.loc[fallback_idx, "selection_order"] = 1
            df.loc[fallback_idx, "selection_incremental_area_deg2"] = finite_float(
                df.loc[fallback_idx].get("overlap_area_deg2", 0.0)
            )
            df.loc[fallback_idx, "selection_coverage_ratio"] = np.nan
            continue

        candidates = list(group_sorted.index)
        covered_geom = None
        covered_area = 0.0
        group_selected = []
        order = 1

        while candidates:
            if max_scenes_per_group > 0 and len(group_selected) >= max_scenes_per_group:
                break

            best_idx = None
            best_intersection = None
            best_increment = 0.0
            best_overlap_area = -1.0

            for idx in candidates:
                fp = df.at[idx, "footprint_geom"]
                if fp is None:
                    continue

                try:
                    inter = fp.intersection(region_geom)
                    if inter is None or inter.is_empty:
                        continue
                    if covered_geom is None:
                        increment_geom = inter
                    else:
                        increment_geom = inter.difference(covered_geom)
                    increment = float(increment_geom.area) if increment_geom and not increment_geom.is_empty else 0.0
                    overlap_area = finite_float(df.at[idx, "overlap_area_deg2"])
                except Exception:
                    continue

                if (
                    best_idx is None
                    or increment > best_increment
                    or (increment == best_increment and overlap_area > best_overlap_area)
                ):
                    best_idx = idx
                    best_intersection = inter
                    best_increment = increment
                    best_overlap_area = overlap_area

            if best_idx is None or best_increment <= min_incremental_overlap_area_deg2:
                break

            group_selected.append(best_idx)
            selected_indices.append(best_idx)

            if covered_geom is None:
                covered_geom = best_intersection
            else:
                covered_geom = covered_geom.union(best_intersection).buffer(0)
            covered_area = float(covered_geom.area) if covered_geom and not covered_geom.is_empty else covered_area
            coverage_ratio = covered_area / region_area if region_area > 0 else np.nan

            df.loc[best_idx, "selection_order"] = order
            df.loc[best_idx, "selection_incremental_area_deg2"] = best_increment
            df.loc[best_idx, "selection_coverage_ratio"] = coverage_ratio

            candidates.remove(best_idx)
            order += 1

            if coverage_ratio >= coverage_target_ratio:
                break

        if not group_selected and not group_sorted.empty:
            fallback_idx = group_sorted.index[0]
            selected_indices.append(fallback_idx)
            df.loc[fallback_idx, "selection_order"] = 1
            fallback_overlap_area = finite_float(
                df.loc[fallback_idx].get("overlap_area_deg2", 0.0)
            )
            df.loc[fallback_idx, "selection_incremental_area_deg2"] = fallback_overlap_area
            df.loc[fallback_idx, "selection_coverage_ratio"] = fallback_overlap_area / region_area
            group_selected = [fallback_idx]

        group_label_values = group_key if isinstance(group_key, tuple) else (group_key,)
        group_label = " | ".join(
            f"{col}={val}" for col, val in zip(group_cols, group_label_values)
        )
        final_coverage = df.loc[group_selected, "selection_coverage_ratio"].max()
        final_coverage_label = "nan" if pd.isna(final_coverage) else f"{float(final_coverage):.4f}"
        print(
            f"[SCENE COVER] {group_label} | candidates={len(group_sorted)} | "
            f"selected={len(group_selected)} | coverage={final_coverage_label} | "
            f"target={coverage_target_ratio:.4f}"
        )

    out = df.loc[selected_indices].copy()
    out = out.sort_values(
        [c for c in ["region", "acq_date", "selection_order", "content_start", "name"] if c in out.columns]
    ).reset_index(drop=True)

    print(
        f"[SCENE COVER] before={before} | after={len(out)} | "
        f"group_cols={group_cols} | adaptive_max_scenes={max_scenes_per_group or 'unlimited'}"
    )
    print(out[[c for c in [
        "region",
        "acq_date",
        "selection_order",
        "name",
        "overlap_area_deg2",
        "selection_incremental_area_deg2",
        "selection_coverage_ratio",
    ] if c in out.columns]])
    return out


def fetch_products_for_region(region_name, region_gdf, date_from, date_to, top_n=100, product_types=None, sensor_mode=None, polarisation=None):
    if not product_types:
        product_types = DEFAULT_PRODUCT_TYPES

    bounds = tuple(region_gdf.total_bounds)
    sub_bounds_list = split_bounds_into_four(bounds)
    dfs = []

    print(f"\n=== QUERY REGION: {region_name} ===")
    print("Full bounds:", bounds)
    print("Subqueries:", len(sub_bounds_list))
    print("Product types:", product_types)

    for product_type in product_types:
        for idx, sb in enumerate(sub_bounds_list, start=1):
            bbox_wkt = build_bbox_wkt_from_bounds(*sb)
            url = build_odata_url_s1(
                bbox_wkt=bbox_wkt,
                date_from=date_from,
                date_to=date_to,
                top_n=top_n,
                product_type=product_type,
            )
            print(f"[QUERY-{region_name}-{product_type}-{idx}/4] bounds={sb}")
            print("URL length:", len(url))

            r = requests.get(url, headers={"Accept": "application/json"}, timeout=180)
            print("status:", r.status_code)
            r.raise_for_status()

            data = r.json()
            rows = []
            for it in data.get("value", []):
                name = it.get("Name")
                if not name:
                    continue
                try:
                    meta = parse_safe_name(name)
                except Exception:
                    continue
                rows.append({
                    "region": region_name,
                    "id": it.get("Id"),
                    "name": name,
                    "platform": meta["platform"],
                    "acq_date": meta["acq_date"],
                    "tile_code": meta["tile_code"],
                    "sensor_mode": meta["sensor_mode"],
                    "product_type": meta["product_type"],
                    "resolution_class": meta["resolution_class"],
                    "polarisation_code": meta["polarisation_code"],
                    "content_start": (it.get("ContentDate") or {}).get("Start"),
                    "content_end": (it.get("ContentDate") or {}).get("End"),
                    "s3path": it.get("S3Path"),
                    "online": it.get("Online"),
                    "origin_date": it.get("OriginDate"),
                    "publication_date": it.get("PublicationDate"),
                    "footprint": it.get("GeoFootprint") or it.get("Footprint"),
                    "query_part": idx,
                    "query_product_type": product_type,
                })
            dfs.append(pd.DataFrame(rows))
            print(f"[QUERY-{region_name}-{product_type}-{idx}/4] products={len(rows)}")

    out = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    if not out.empty:
        out = out.drop_duplicates(subset=["id"]).reset_index(drop=True)
        out = filter_s1_results_locally(out, sensor_mode=sensor_mode, polarisation=polarisation)
    return out


def scan_existing_safe(raw_root, scenes=None):
    rows = []
    for safe_dir in sorted(Path(raw_root).rglob("*.SAFE")):
        safe_name = safe_dir.name
        try:
            meta = parse_safe_name(safe_name)
        except Exception:
            continue

        scene_hits = []
        if scenes is not None and not scenes.empty:
            hit = scenes[scenes["name"] == safe_name]
            scene_hits = hit.to_dict("records") if not hit.empty else []

        if not scene_hits:
            scene_hits = [{"region": None, "id": None}]

        for hit in scene_hits:
            rows.append({
                "region": hit.get("region"),
                "id": hit.get("id"),
                "name": safe_name,
                "platform": meta["platform"],
                "acq_date": meta["acq_date"],
                "tile_code": meta["tile_code"],
                "sensor_mode": meta["sensor_mode"],
                "product_type": meta["product_type"],
                "resolution_class": meta["resolution_class"],
                "polarisation_code": meta["polarisation_code"],
                "safe_dir": str(safe_dir),
            })
    return pd.DataFrame(rows)


def scan_existing_zips(raw_root, scenes=None):
    rows = []
    for z in sorted(Path(raw_root).rglob("*.zip")):
        safe_name = z.name.replace(".zip", "")
        try:
            meta = parse_safe_name(safe_name)
        except Exception:
            continue

        scene_hits = []
        if scenes is not None and not scenes.empty:
            hit = scenes[scenes["name"] == safe_name]
            scene_hits = hit.to_dict("records") if not hit.empty else []

        if not scene_hits:
            scene_hits = [{"region": None, "id": None}]

        for hit in scene_hits:
            rows.append({
                "region": hit.get("region"),
                "id": hit.get("id"),
                "name": safe_name,
                "platform": meta["platform"],
                "acq_date": meta["acq_date"],
                "tile_code": meta["tile_code"],
                "sensor_mode": meta["sensor_mode"],
                "product_type": meta["product_type"],
                "resolution_class": meta["resolution_class"],
                "polarisation_code": meta["polarisation_code"],
                "zip_path": str(z),
            })
    return pd.DataFrame(rows)


def get_cdse_access_token(username, password):
    payload = {
        "client_id": "cdse-public",
        "username": username,
        "password": password,
        "grant_type": "password",
    }
    r = requests.post(TOKEN_URL, data=payload, timeout=120)
    print(f"token status [{username}]:", r.status_code)
    r.raise_for_status()
    return r.json()["access_token"]


def is_token_expired_response(resp):
    if resp is None or resp.status_code != 401:
        return False
    try:
        data = resp.json()
        msg = str(data.get("message", "")).lower()
        code = str(data.get("code", "")).upper()
        return ("expired" in msg) or (code == "DAT-ZIP-109")
    except Exception:
        txt = (resp.text or "").lower()
        return "token is expired" in txt


def is_max_session_response(resp):
    if resp is None or resp.status_code != 429:
        return False
    try:
        data = resp.json()
        msg = str(data.get("message", "")).lower()
        code = str(data.get("code", "")).upper()
        return ("max session number exceeded" in msg) or (code == "DAT-ZIP-100")
    except Exception:
        txt = (resp.text or "").lower()
        return "max session number exceeded" in txt


def is_auth_rate_limited_response(resp):
    if resp is None:
        return False
    return resp.status_code in (401, 403, 429)


class TokenManager:
    def __init__(self, username, password, base_backoff=8.0, max_backoff=180.0):
        self.username = username
        self.password = password
        self.base_backoff = base_backoff
        self.max_backoff = max_backoff
        self._lock = threading.Lock()
        self._token = None
        self._last_refresh_ts = 0.0
        self._min_refresh_interval = 20.0

    def get_token(self, force_refresh=False):
        with self._lock:
            now = time.time()

            if self._token is not None and not force_refresh:
                return self._token

            if force_refresh and (now - self._last_refresh_ts) < self._min_refresh_interval:
                if self._token is not None:
                    return self._token

            for attempt in range(1, 8):
                try:
                    token = get_cdse_access_token(self.username, self.password)
                    self._token = token
                    self._last_refresh_ts = time.time()
                    return token
                except requests.RequestException as e:
                    wait_s = calc_backoff(attempt, self.base_backoff, self.max_backoff)
                    print(f"[AUTH:{self.username}] token request failed attempt={attempt} | wait={wait_s:.1f}s | {e}")
                    if attempt >= 7:
                        raise
                    time.sleep(wait_s)

            raise RuntimeError(f"Gagal mendapatkan token CDSE untuk {self.username}")

    def invalidate(self):
        with self._lock:
            self._token = None

    def refresh(self):
        return self.get_token(force_refresh=True)


class AccountPool:
    def __init__(self, usernames, password, base_backoff=8.0, max_backoff=180.0):
        if not usernames:
            raise ValueError("Minimal 1 username diperlukan")
        self._accounts = [
            TokenManager(username=u, password=password, base_backoff=base_backoff, max_backoff=max_backoff)
            for u in usernames
        ]
        self._idx = 0
        self._lock = threading.Lock()

    def acquire(self):
        with self._lock:
            acc = self._accounts[self._idx]
            self._idx = (self._idx + 1) % len(self._accounts)
            return acc

    @property
    def size(self):
        return len(self._accounts)


def build_product_download_url(product_id):
    return f"{DOWNLOAD_PRODUCTS_URL}({product_id})/$value"


def download_one_product(
    product_id,
    product_name,
    region,
    acq_date,
    out_dir,
    overwrite=False,
    max_retries=8,
    backoff_base=12.0,
    backoff_max=240.0,
):
    global GLOBAL_DOWNLOAD_SEMAPHORE
    global GLOBAL_ACCOUNT_POOL

    if GLOBAL_DOWNLOAD_SEMAPHORE is None:
        raise RuntimeError("GLOBAL_DOWNLOAD_SEMAPHORE belum diinisialisasi")
    if GLOBAL_ACCOUNT_POOL is None:
        raise RuntimeError("GLOBAL_ACCOUNT_POOL belum diinisialisasi")

    region_dir = ensure_dir(Path(out_dir) / region.lower() / acq_date)
    out_path = region_dir / safe_zip_name(product_name)
    tmp_path = out_path.with_suffix(out_path.suffix + ".part")

    if out_path.exists() and out_path.stat().st_size > 0 and not overwrite:
        print(f"[SKIP] already exists: {out_path.name}")
        return out_path

    if not overwrite:
        for existing_path in sorted(Path(out_dir).rglob(safe_zip_name(product_name))):
            if existing_path.exists() and existing_path.stat().st_size > 0:
                print(f"[SKIP] already exists elsewhere: {existing_path}")
                return existing_path

    url = build_product_download_url(product_id)
    account = GLOBAL_ACCOUNT_POOL.acquire()

    for attempt in range(1, max_retries + 1):
        wait_for_global_auth_cooldown()

        token = account.get_token(force_refresh=False)
        headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/octet-stream",
        }

        print(f"[DOWNLOAD:{account.username}] {product_name} | attempt {attempt}/{max_retries}")

        try:
            with GLOBAL_DOWNLOAD_SEMAPHORE:
                with requests.Session() as session:
                    session.headers.update(headers)
                    with session.get(url, stream=True, timeout=(30, 300), allow_redirects=True) as r:
                        print("final url:", r.url)
                        print("download status:", r.status_code)

                        if r.status_code != 200:
                            try:
                                print("response preview:", r.text[:500])
                            except Exception:
                                pass

                        if is_token_expired_response(r):
                            wait_s = calc_backoff(attempt, backoff_base, backoff_max)
                            print(f"[AUTH:{account.username}] token expired. cooldown {wait_s:.1f}s then refresh...")
                            account.invalidate()
                            set_global_auth_cooldown(wait_s)
                            time.sleep(wait_s)
                            account.refresh()
                            continue

                        if is_max_session_response(r):
                            wait_s = calc_backoff(attempt, backoff_base * 2.0, backoff_max)
                            print(f"[RATE LIMIT:{account.username}] max session exceeded. cooldown {wait_s:.1f}s")
                            set_global_auth_cooldown(wait_s)
                            time.sleep(wait_s)
                            continue

                        if is_auth_rate_limited_response(r):
                            wait_s = calc_backoff(attempt, backoff_base * 1.5, backoff_max)
                            print(f"[AUTH/RATE:{account.username}] auth limited. cooldown {wait_s:.1f}s")
                            account.invalidate()
                            set_global_auth_cooldown(wait_s)
                            time.sleep(wait_s)
                            account.refresh()
                            continue

                        r.raise_for_status()

                        if tmp_path.exists():
                            try:
                                tmp_path.unlink()
                            except Exception:
                                pass

                        with open(tmp_path, "wb") as f:
                            for chunk in r.iter_content(chunk_size=1024 * 1024):
                                if chunk:
                                    f.write(chunk)

                        if not tmp_path.exists() or tmp_path.stat().st_size == 0:
                            raise RuntimeError(f"Downloaded file kosong: {product_name}")

                        tmp_path.replace(out_path)
                        print(f"[OK:{account.username}] saved: {out_path}")
                        return out_path

        except requests.HTTPError as e:
            if attempt >= max_retries:
                if tmp_path.exists():
                    try:
                        tmp_path.unlink()
                    except Exception:
                        pass
                raise
            wait_s = calc_backoff(attempt, backoff_base, backoff_max)
            print(f"[WARN:{account.username}] HTTPError retry in {wait_s:.1f}s | {e}")
            time.sleep(wait_s)

        except requests.RequestException as e:
            if attempt >= max_retries:
                if tmp_path.exists():
                    try:
                        tmp_path.unlink()
                    except Exception:
                        pass
                raise
            wait_s = calc_backoff(attempt, backoff_base, backoff_max)
            print(f"[WARN:{account.username}] request failed retry in {wait_s:.1f}s | {e}")
            time.sleep(wait_s)

        except Exception:
            if tmp_path.exists():
                try:
                    tmp_path.unlink()
                except Exception:
                    pass
            raise

    raise RuntimeError(f"Download gagal setelah {max_retries} percobaan: {product_name}")


def download_all_products_parallel(
    scenes,
    raw_root,
    overwrite=False,
    download_workers=4,
    max_rounds=30,
    sleep_between_rounds=90,
    max_retries_per_file=8,
    backoff_base=12.0,
    backoff_max=240.0,
):
    if scenes.empty:
        return pd.DataFrame()

    scenes_df = scenes.copy().reset_index(drop=True)
    scenes_df["_download_key"] = scenes_df["id"].fillna(scenes_df["name"]).astype(str)

    product_df = scenes_df.drop_duplicates(subset=["_download_key"], keep="first").reset_index(drop=True)
    expected_keys = set(product_df["_download_key"].tolist())
    remaining_df = product_df.copy().reset_index(drop=True)
    success_map = {}

    if len(product_df) != len(scenes_df):
        print(
            f"[DOWNLOAD] unique products={len(product_df)} | "
            f"region-scene rows={len(scenes_df)}"
        )

    for round_no in range(1, max_rounds + 1):
        if remaining_df.empty:
            break

        print(f"\n{'='*80}")
        print(f"[DOWNLOAD ROUND {round_no}] remaining={len(remaining_df)}")
        print(f"{'='*80}")

        rows = remaining_df.to_dict("records")
        round_results = []

        def _download_row(row_dict):
            out_path = download_one_product(
                product_id=row_dict["id"],
                product_name=row_dict["name"],
                region="_shared",
                acq_date=row_dict["acq_date"],
                out_dir=raw_root,
                overwrite=overwrite,
                max_retries=max_retries_per_file,
                backoff_base=backoff_base,
                backoff_max=backoff_max,
            )
            return {**row_dict, "zip_path": str(out_path), "status": "ok"}

        with ThreadPoolExecutor(max_workers=max(1, download_workers)) as ex:
            futures = {ex.submit(_download_row, row): row for row in rows}
            for i, fut in enumerate(as_completed(futures), start=1):
                row = futures[fut]
                print(f"\n=== DOWNLOAD DONE {i}/{len(rows)} | ROUND {round_no} ===")
                try:
                    result = fut.result()
                    round_results.append(result)
                except Exception as e:
                    print(f"[FAIL] {row['name']} | {e}")
                    round_results.append({
                        **row,
                        "zip_path": None,
                        "status": f"fail: {e}",
                    })

        round_df = pd.DataFrame(round_results)
        if round_df.empty:
            raise RuntimeError(f"Round {round_no} tidak menghasilkan hasil download.")

        round_df["zip_exists"] = round_df["zip_path"].apply(
            lambda p: bool(p is not None and Path(p).exists() and Path(p).stat().st_size > 0)
        )

        success_df = round_df[(round_df["status"] == "ok") & (round_df["zip_exists"])].copy()
        fail_df = round_df[(round_df["status"] != "ok") | (~round_df["zip_exists"])].copy()

        for _, row in success_df.iterrows():
            success_map[row["_download_key"]] = row.to_dict()

        print(
            f"[ROUND {round_no}] success_valid={len(success_df)} | "
            f"failed={len(fail_df)} | total_success_accum={len(success_map)}"
        )

        if len(success_map) == len(expected_keys):
            print(f"[DOWNLOAD] strict success completed in round {round_no}")
            break

        failed_keys = sorted(expected_keys - set(success_map.keys()))
        remaining_df = product_df[product_df["_download_key"].isin(failed_keys)].copy().reset_index(drop=True)

        if round_no < max_rounds and not remaining_df.empty:
            print(f"[DOWNLOAD] sleep {sleep_between_rounds}s before next round...")
            time.sleep(sleep_between_rounds)

    product_results_df = pd.DataFrame(list(success_map.values()))
    if product_results_df.empty:
        raise RuntimeError("Tidak ada file yang berhasil di-download.")

    product_results_df = product_results_df.drop_duplicates(subset=["_download_key"], keep="last").reset_index(drop=True)
    got_keys = set(product_results_df["_download_key"].tolist())
    missing = sorted(expected_keys - got_keys)

    if missing:
        raise RuntimeError(
            f"STRICT download gagal. Masih ada {len(missing)} produk belum terdownload. "
            f"Contoh: {missing[:10]}"
        )

    key_to_zip = dict(zip(product_results_df["_download_key"], product_results_df["zip_path"]))
    final_rows = []
    for _, scene_row in scenes_df.iterrows():
        row_dict = scene_row.drop(labels=["_download_key"]).to_dict()
        row_dict["zip_path"] = key_to_zip[scene_row["_download_key"]]
        row_dict["status"] = "ok"
        final_rows.append(row_dict)

    final_df = pd.DataFrame(final_rows)
    final_df = final_df.sort_values(["region", "acq_date", "name"]).reset_index(drop=True)
    print(
        f"\n[DOWNLOAD] STRICT SUCCESS | products={len(product_results_df)} | "
        f"region-scene rows={len(final_df)}"
    )
    return final_df


def unzip_archives(downloaded_df, overwrite=False, cleanup_zips=False):
    rows = []
    extracted_cache = {}
    cleaned_zips = set()
    for _, row in downloaded_df.iterrows():
        zip_path = Path(row["zip_path"])
        safe_dir = zip_path.parent / row["name"]

        cache_key = str(zip_path)
        if cache_key in extracted_cache:
            safe_dir = Path(extracted_cache[cache_key])
            print(f"[SKIP] reused extracted: {safe_dir.name}")
        elif safe_dir.exists() and not overwrite:
            print(f"[SKIP] extracted: {safe_dir.name}")
            extracted_cache[cache_key] = str(safe_dir)
        else:
            print(f"[UNZIP] {zip_path.name}")
            try:
                with zipfile.ZipFile(zip_path, "r") as zf:
                    zf.extractall(zip_path.parent)
                extracted_cache[cache_key] = str(safe_dir)
            except zipfile.BadZipFile:
                print(f"[ERROR] bad zip: {zip_path}")
                continue
        if safe_dir.exists():
            rows.append({**row.to_dict(), "safe_dir": str(safe_dir)})
            if cleanup_zips and cache_key not in cleaned_zips:
                try:
                    if zip_path.exists():
                        zip_path.unlink()
                        print(f"[CLEANUP] deleted zip: {zip_path.name}")
                    cleaned_zips.add(cache_key)
                except Exception as e:
                    print(f"[WARN] cleanup zip failed {zip_path}: {e}")
        else:
            print(f"[ERROR] SAFE not found after unzip: {safe_dir}")
    return pd.DataFrame(rows)


def build_region_aoi_wkt(region_gdf, buffer_deg=0.05):
    region_ll = region_gdf.to_crs(4326)
    geom = region_ll.union_all().buffer(0)
    subset_geom = geom.buffer(float(buffer_deg)).envelope
    return subset_geom.wkt


def write_snap_graph_xml(graph_path):
    graph_path = Path(graph_path)
    graph_path.parent.mkdir(parents=True, exist_ok=True)

    graph_xml = """<graph id="S1_GRD_SUBSET_TC">
  <version>1.0</version>

  <node id="Read">
    <operator>Read</operator>
    <sources/>
    <parameters>
      <file>${SOURCE}</file>
    </parameters>
  </node>

  <node id="Apply-Orbit-File">
    <operator>Apply-Orbit-File</operator>
    <sources>
      <sourceProduct refid="Read"/>
    </sources>
    <parameters>
      <orbitType>Sentinel Precise (Auto Download)</orbitType>
      <polyDegree>3</polyDegree>
      <continueOnFail>true</continueOnFail>
    </parameters>
  </node>

  <node id="Remove-GRD-Border-Noise">
    <operator>Remove-GRD-Border-Noise</operator>
    <sources>
      <sourceProduct refid="Apply-Orbit-File"/>
    </sources>
    <parameters/>
  </node>

  <node id="ThermalNoiseRemoval">
    <operator>ThermalNoiseRemoval</operator>
    <sources>
      <sourceProduct refid="Remove-GRD-Border-Noise"/>
    </sources>
    <parameters/>
  </node>

  <node id="Calibration">
    <operator>Calibration</operator>
    <sources>
      <sourceProduct refid="ThermalNoiseRemoval"/>
    </sources>
    <parameters>
      <outputSigmaBand>true</outputSigmaBand>
      <outputGammaBand>false</outputGammaBand>
      <outputBetaBand>false</outputBetaBand>
      <selectedPolarisations>VV,VH</selectedPolarisations>
      <outputImageScaleInDb>false</outputImageScaleInDb>
      <createGammaBand>false</createGammaBand>
      <createBetaBand>false</createBetaBand>
    </parameters>
  </node>

  <node id="Subset">
    <operator>Subset</operator>
    <sources>
      <sourceProduct refid="Calibration"/>
    </sources>
    <parameters>
      <copyMetadata>true</copyMetadata>
      <geoRegion>${AOI_WKT}</geoRegion>
    </parameters>
  </node>

  <node id="Terrain-Correction">
    <operator>Terrain-Correction</operator>
    <sources>
      <sourceProduct refid="Subset"/>
    </sources>
    <parameters>
      <demName>${DEM_NAME}</demName>
      <pixelSpacingInMeter>${PIXEL_SPACING}</pixelSpacingInMeter>
      <mapProjection>AUTO:42001</mapProjection>
      <saveSelectedSourceBand>true</saveSelectedSourceBand>
      <nodataValueAtSea>true</nodataValueAtSea>
      <saveDEM>false</saveDEM>
      <saveLatLon>false</saveLatLon>
      <saveLocalIncidenceAngle>false</saveLocalIncidenceAngle>
      <saveProjectedLocalIncidenceAngle>false</saveProjectedLocalIncidenceAngle>
      <saveIncidenceAngleFromEllipsoid>false</saveIncidenceAngleFromEllipsoid>
      <saveSigmaNought>false</saveSigmaNought>
      <saveGammaNought>false</saveGammaNought>
      <saveBetaNought>false</saveBetaNought>
      <imgResamplingMethod>BILINEAR_INTERPOLATION</imgResamplingMethod>
      <demResamplingMethod>BILINEAR_INTERPOLATION</demResamplingMethod>
    </parameters>
  </node>

  <node id="Write">
    <operator>Write</operator>
    <sources>
      <sourceProduct refid="Terrain-Correction"/>
    </sources>
    <parameters>
      <file>${OUTPUT}</file>
      <formatName>GeoTIFF-BigTIFF</formatName>
    </parameters>
  </node>
</graph>
"""
    graph_path.write_text(graph_xml)
    return graph_path


def run_snap_gpt_safe(
    safe_dir,
    output_tif,
    graph_xml,
    aoi_wkt,
    gpt_path="gpt",
    dem_name="Copernicus 30m Global DEM",
    pixel_spacing=10.0,
):
    output_tif = Path(output_tif)
    output_tif.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        gpt_path,
        str(graph_xml),
        f"-PSOURCE={safe_dir}",
        f"-PDEM_NAME={dem_name}",
        f"-PPIXEL_SPACING={pixel_spacing}",
        f"-PAOI_WKT={aoi_wkt}",
        f"-POUTPUT={output_tif}",
    ]

    print(
        f"[SNAP] gpt={gpt_path} source={Path(safe_dir).name} "
        f"output={output_tif.name} aoi_wkt_chars={len(aoi_wkt)}"
    )
    subprocess.run(cmd, check=True)
    return output_tif


def preprocess_one_safe_with_snap(
    safe_dir,
    region_name,
    out_dir,
    graph_xml,
    aoi_wkt,
    gpt_path="gpt",
    dem_name="Copernicus 30m Global DEM",
    pixel_spacing=10.0,
    overwrite=False,
):
    safe_dir = Path(safe_dir)
    safe_name = safe_dir.name
    meta = parse_safe_name(safe_name)
    acq_date = meta["acq_date"]

    out_dir = Path(out_dir)
    if acq_date:
        out_dir = out_dir / acq_date
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / f"{safe_name.replace('.SAFE', '')}_tc.tif"

    if out_path.exists() and out_path.stat().st_size > 0 and not overwrite:
        print(f"[SKIP] SNAP exists: {out_path.name}")
        return out_path

    result = run_snap_gpt_safe(
        safe_dir=safe_dir,
        output_tif=out_path,
        graph_xml=graph_xml,
        aoi_wkt=aoi_wkt,
        gpt_path=gpt_path,
        dem_name=dem_name,
        pixel_spacing=pixel_spacing,
    )
    print(f"[OK] SNAP: {region_name} | {acq_date} -> {result.name}")
    return result


def preprocess_safe_stage_with_snap(
    safe_df,
    aoi_gdf,
    target_regions,
    snap_root,
    graph_xml,
    gpt_path="gpt",
    dem_name="Copernicus 30m Global DEM",
    pixel_spacing=10.0,
    overwrite=False,
    snap_workers=1,
    snap_subset_buffer_deg=0.05,
):
    rows = []

    region_wkt_map = {
        region_name: build_region_aoi_wkt(
            aoi_gdf[aoi_gdf["ADM1_EN"] == region_name].copy(),
            buffer_deg=snap_subset_buffer_deg,
        )
        for region_name in target_regions
    }

    def _run_row(row_dict, region_name, out_dir, aoi_wkt):
        out_path = preprocess_one_safe_with_snap(
            safe_dir=row_dict["safe_dir"],
            region_name=region_name,
            out_dir=out_dir,
            graph_xml=graph_xml,
            aoi_wkt=aoi_wkt,
            gpt_path=gpt_path,
            dem_name=dem_name,
            pixel_spacing=pixel_spacing,
            overwrite=overwrite,
        )
        return {
            "region": region_name,
            "safe_name": row_dict["name"],
            "acq_date": row_dict["acq_date"],
            "tile_code": row_dict["tile_code"],
            "snap_tif": str(out_path) if out_path else None,
        }

    for region_name in target_regions:
        region_safe = safe_df[safe_df["region"] == region_name].copy()
        if region_safe.empty:
            print(f"\n=== SNAP PREPROCESS {region_name} ===\nJumlah SAFE: 0")
            continue

        region_safe = region_safe.sort_values(["acq_date", "name"]).reset_index(drop=True)
        print(f"\n=== SNAP PREPROCESS {region_name} ===")
        print("Jumlah SAFE:", len(region_safe))

        out_dir = Path(snap_root) / region_name.lower()
        aoi_wkt = region_wkt_map[region_name]
        records = region_safe.to_dict("records")

        if snap_workers <= 1:
            for row in records:
                try:
                    rows.append(_run_row(row, region_name, out_dir, aoi_wkt))
                except Exception as e:
                    print(f"[FAIL SNAP] {row['name']}: {e}")
                    rows.append({
                        "region": region_name,
                        "safe_name": row["name"],
                        "acq_date": row["acq_date"],
                        "tile_code": row["tile_code"],
                        "snap_tif": None,
                    })
        else:
            futures = {}
            with ThreadPoolExecutor(max_workers=max(1, snap_workers)) as ex:
                for row in records:
                    fut = ex.submit(_run_row, row, region_name, out_dir, aoi_wkt)
                    futures[fut] = row

                for fut in as_completed(futures):
                    row = futures[fut]
                    try:
                        rows.append(fut.result())
                    except Exception as e:
                        print(f"[FAIL SNAP] {row['name']}: {e}")
                        rows.append({
                            "region": region_name,
                            "safe_name": row["name"],
                            "acq_date": row["acq_date"],
                            "tile_code": row["tile_code"],
                            "snap_tif": None,
                        })

    out_df = pd.DataFrame(rows)
    if not out_df.empty:
        out_df = out_df[out_df["snap_tif"].notna()].reset_index(drop=True)
        out_df = out_df.sort_values(["region", "acq_date", "safe_name"]).reset_index(drop=True)
    return out_df


def process_one_safe_to_temp_tile(
    snap_tif,
    region_name,
    region_gdf,
    out_dir,
    target_res=10.0,
    overwrite=False,
    compress=False,
    grid_info=None,
    region_mask=None,
):
    snap_tif = Path(snap_tif)
    safe_name = snap_tif.stem.replace("_tc", "")
    meta = parse_safe_name(safe_name)
    acq_date = meta["acq_date"]
    tile_code = meta["tile_code"]

    out_dir = ensure_dir(out_dir)
    out_path = out_dir / f"{safe_name}_clean_10m.tif"

    if out_path.exists() and out_path.stat().st_size > 0 and not overwrite:
        print(f"[SKIP] exists: {out_path.name}")
        return out_path

    if grid_info is None:
        target_crs = region_gdf.estimate_utm_crs()
        region_proj = region_gdf.to_crs(target_crs)
        geoms_proj = [
            geom.__geo_interface__
            for geom in region_proj.geometry
            if geom is not None and not geom.is_empty
        ]
        res = float(target_res)
        minx, miny, maxx, maxy = snap_bounds_to_resolution(region_proj.total_bounds, res)
        width = max(1, int(round((maxx - minx) / res)))
        height = max(1, int(round((maxy - miny) / res)))
        transform = rasterio.transform.from_origin(minx, maxy, res, res)
    else:
        target_crs = grid_info["target_crs"]
        geoms_proj = grid_info["geoms_proj"]
        width = grid_info["width"]
        height = grid_info["height"]
        transform = grid_info["transform"]

    if not geoms_proj:
        print(f"[SKIP] empty geometry: {safe_name}")
        return None

    if region_mask is None:
        region_mask = geometry_mask(
            geoms_proj,
            transform=transform,
            invert=True,
            out_shape=(height, width),
        )

    if not np.any(region_mask):
        print(f"[SKIP] no valid pixels in AOI mask: {safe_name}")
        return None

    with rasterio.open(snap_tif) as src:
        out_stack = np.full((len(TARGET_BANDS), height, width), np.nan, dtype=np.float32)

        band_map = {}
        for idx, desc in enumerate(src.descriptions, start=1):
            if desc:
                band_map[desc.upper()] = idx

        vv_idx = band_map.get("SIGMA0_VV") or band_map.get("VV") or 1
        vh_idx = band_map.get("SIGMA0_VH") or band_map.get("VH") or (2 if src.count >= 2 else None)
        src_indices = {"VV": vv_idx, "VH": vh_idx}

        with WarpedVRT(
            src,
            crs=target_crs,
            transform=transform,
            width=width,
            height=height,
            resampling=Resampling.bilinear,
            nodata=np.nan,
        ) as vrt:
            for i, band in enumerate(TARGET_BANDS):
                src_idx = src_indices.get(band)
                if src_idx is None:
                    print(f"[INFO] SNAP band not available {band}: {snap_tif.name}")
                    continue

                arr = clean_s1_linear_array(vrt.read(src_idx), nodata=src.nodata)
                arr[~region_mask] = np.nan
                out_stack[i] = arr

                valid = np.isfinite(arr)
                print(
                    f"[DEBUG SNAP] {safe_name} | {band} | "
                    f"valid={int(valid.sum())} | "
                    f"min={np.nanmin(arr) if valid.any() else 'nan'} | "
                    f"max={np.nanmax(arr) if valid.any() else 'nan'} | "
                    f"mean={np.nanmean(arr) if valid.any() else 'nan'}"
                )

    if np.all(np.isnan(out_stack)):
        print(f"[SKIP] all output bands are NaN after SNAP clip: {safe_name}")
        return None

    meta_out = {
        "driver": "GTiff",
        "height": height,
        "width": width,
        "count": len(TARGET_BANDS),
        "dtype": "float32",
        "crs": target_crs,
        "transform": transform,
        "tiled": True,
        "nodata": np.nan,
        "BIGTIFF": "IF_SAFER",
    }
    if compress:
        meta_out.update({"compress": "DEFLATE", "predictor": 2})

    with rasterio.open(out_path, "w", **meta_out) as dst:
        dst.write(out_stack)
        for idx, name in enumerate(BAND_NAMES, start=1):
            dst.set_band_description(idx, name)

    print(f"[OK] temp tile: {region_name} | {acq_date} | {tile_code} -> {out_path.name}")
    return out_path


def process_safe_stage(
    preprocessed_df,
    aoi_gdf,
    target_regions,
    temp_root,
    target_res=10.0,
    overwrite=False,
    temp_workers=1,
    region_grid_info=None,
):
    temp_rows = []
    region_to_gdf = {
        region_name: aoi_gdf[aoi_gdf["ADM1_EN"] == region_name].copy()
        for region_name in target_regions
    }
    if region_grid_info is None:
        region_grid_info = build_region_grid_info(aoi_gdf, target_regions, target_res=target_res)

    for region_name in target_regions:
        region = region_to_gdf[region_name]
        region_df = preprocessed_df[preprocessed_df["region"] == region_name].copy()
        if region_df.empty:
            print(f"\n=== REGION {region_name} ===\nJumlah SNAP TIFF: 0")
            continue

        grid_info = region_grid_info[region_name]
        region_mask = geometry_mask(
            grid_info["geoms_proj"],
            transform=grid_info["transform"],
            invert=True,
            out_shape=(grid_info["height"], grid_info["width"]),
        )

        region_df = region_df.sort_values(["acq_date", "safe_name"]).reset_index(drop=True)
        print(f"\n=== REGION {region_name} ===")
        print("Jumlah SNAP TIFF:", len(region_df))
        out_dir = ensure_dir(Path(temp_root) / region_name.lower())

        if temp_workers <= 1:
            for _, row in region_df.iterrows():
                try:
                    out_path = process_one_safe_to_temp_tile(
                        snap_tif=row["snap_tif"],
                        region_name=region_name,
                        region_gdf=region,
                        out_dir=out_dir,
                        target_res=target_res,
                        overwrite=overwrite,
                        compress=False,
                        grid_info=grid_info,
                        region_mask=region_mask,
                    )
                except Exception as e:
                    print(f"[FAIL] {row['safe_name']}: {e}")
                    out_path = None

                temp_rows.append({
                    "region": region_name,
                    "safe_name": row["safe_name"],
                    "acq_date": row["acq_date"],
                    "tile_code": row["tile_code"],
                    "temp_tile": str(out_path) if out_path else None,
                })
        else:
            futures = {}
            with ThreadPoolExecutor(max_workers=temp_workers) as ex:
                for _, row in region_df.iterrows():
                    fut = ex.submit(
                        process_one_safe_to_temp_tile,
                        row["snap_tif"],
                        region_name,
                        region,
                        out_dir,
                        target_res,
                        overwrite,
                        False,
                        grid_info,
                        region_mask,
                    )
                    futures[fut] = row

                for fut in as_completed(futures):
                    row = futures[fut]
                    try:
                        out_path = fut.result()
                    except Exception as e:
                        print(f"[FAIL] {row['safe_name']}: {e}")
                        out_path = None

                    temp_rows.append({
                        "region": region_name,
                        "safe_name": row["safe_name"],
                        "acq_date": row["acq_date"],
                        "tile_code": row["tile_code"],
                        "temp_tile": str(out_path) if out_path else None,
                    })

    temp_tiles_df = pd.DataFrame(temp_rows)
    if not temp_tiles_df.empty:
        temp_tiles_df = temp_tiles_df[temp_tiles_df["temp_tile"].notna()].reset_index(drop=True)
        temp_tiles_df = temp_tiles_df.sort_values(["region", "acq_date", "safe_name"]).reset_index(drop=True)
    return temp_tiles_df


def build_region_grid_info(aoi_gdf, target_regions, target_res=10.0):
    info = {}
    res = float(target_res)
    for region_name in target_regions:
        region = aoi_gdf[aoi_gdf["ADM1_EN"] == region_name].copy()
        target_crs = region.estimate_utm_crs()
        region_proj = region.to_crs(target_crs)
        minx, miny, maxx, maxy = snap_bounds_to_resolution(region_proj.total_bounds, res)
        width = max(1, int(round((maxx - minx) / res)))
        height = max(1, int(round((maxy - miny) / res)))
        transform = rasterio.transform.from_origin(minx, maxy, res, res)
        geoms_proj = [
            geom.__geo_interface__
            for geom in region_proj.geometry
            if geom is not None and not geom.is_empty
        ]
        info[region_name] = {
            "target_crs": target_crs,
            "bounds": (minx, miny, maxx, maxy),
            "width": width,
            "height": height,
            "transform": transform,
            "geoms_proj": geoms_proj,
            "target_res": res,
        }
    return info


def mosaic_daily_precomputed(region_name, acq_date, tile_paths, out_path, target_crs, bounds,
                             res=10, overwrite=False, use_compression=False):
    out_path = Path(out_path)
    if out_path.exists() and not overwrite:
        return str(out_path)

    srcs = []
    try:
        for p in tile_paths:
            p = Path(p)
            if p.exists():
                srcs.append(rasterio.open(p))

        if not srcs:
            print(f"[SKIP] no tiles for {region_name} {acq_date}")
            return None

        ref = srcs[0]
        for s in srcs[1:]:
            if s.crs != ref.crs:
                raise ValueError(f"CRS mismatch: {s.name}")
            if s.transform != ref.transform:
                raise ValueError(f"Transform mismatch: {s.name}")
            if s.width != ref.width or s.height != ref.height:
                raise ValueError(f"Shape mismatch: {s.name}")
            if s.count != ref.count:
                raise ValueError(f"Band count mismatch: {s.name}")

        stack = np.stack(
            [clean_s1_linear_array(s.read(), nodata=s.nodata) for s in srcs],
            axis=0,
        )
        out_arr = np.full(stack.shape[1:], np.nan, dtype=np.float32)

        for k in range(stack.shape[0]):
            cur = stack[k]
            fill_mask = ~np.isfinite(out_arr) & np.isfinite(cur)
            out_arr[fill_mask] = cur[fill_mask]

        meta = ref.meta.copy()
        meta.update({
            "driver": "GTiff",
            "height": ref.height,
            "width": ref.width,
            "transform": ref.transform,
            "crs": ref.crs,
            "count": ref.count,
            "dtype": "float32",
            "tiled": True,
            "nodata": np.nan,
            "BIGTIFF": "IF_SAFER",
        })
        if use_compression:
            meta.update({"compress": "DEFLATE", "predictor": 2})

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(out_path, "w", **meta) as dst:
            dst.write(out_arr)
            for i, name in enumerate(BAND_NAMES, start=1):
                dst.set_band_description(i, name)

        return str(out_path)
    finally:
        for s in srcs:
            s.close()


def run_one_daily_task(task):
    region_name = task["region_name"]
    acq_date = task["acq_date"]
    tile_paths = task["tile_paths"]
    out_path = task["out_path"]
    target_crs = task["target_crs"]
    bounds = task["bounds"]
    res = task["res"]
    overwrite = task["overwrite"]
    use_compression = task["use_compression"]
    t0 = time.time()
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] START {region_name} {acq_date} | n_tiles={len(tile_paths)}")
    result = mosaic_daily_precomputed(region_name, acq_date, tile_paths, out_path, target_crs, bounds, res,
                                      overwrite=overwrite, use_compression=use_compression)
    dt = time.time() - t0
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] DONE  {region_name} {acq_date} in {dt:.1f}s")
    return {
        "region": region_name,
        "acq_date": acq_date,
        "daily_path": str(result) if result else None,
        "n_tiles": len(tile_paths),
    }


def build_daily_mosaics(temp_tiles_df, target_regions, region_grid_info, daily_root,
                        target_res=10, overwrite=False, daily_workers=1):
    tasks = []
    for region_name in target_regions:
        df_reg = temp_tiles_df[temp_tiles_df["region"] == region_name].copy()
        if df_reg.empty:
            print(f"\n=== DAILY MOSAIC: {region_name} ===\nTidak ada temp tile.")
            continue

        grouped = (
            df_reg.groupby("acq_date", sort=True)["temp_tile"]
            .apply(list)
            .reset_index()
            .rename(columns={"temp_tile": "tile_paths"})
        )
        grouped["n_tiles"] = grouped["tile_paths"].apply(len)
        grouped = grouped.sort_values(["n_tiles", "acq_date"]).reset_index(drop=True)

        print(f"\n=== DAILY MOSAIC: {region_name} ===")
        print(grouped[["acq_date", "n_tiles"]])

        for _, row in grouped.iterrows():
            acq_date = row["acq_date"]
            tile_paths = row["tile_paths"]
            out_path = Path(daily_root) / region_name.lower() / f"{region_name.lower()}_{acq_date}_daily_clean_10m.tif"
            tasks.append({
                "region_name": region_name,
                "acq_date": acq_date,
                "tile_paths": tile_paths,
                "out_path": str(out_path),
                "target_crs": region_grid_info[region_name]["target_crs"],
                "bounds": region_grid_info[region_name]["bounds"],
                "res": target_res,
                "overwrite": overwrite,
                "use_compression": False,
            })

    print(f"\nTOTAL DAILY TASKS = {len(tasks)}")
    if not tasks:
        return pd.DataFrame()

    daily_rows = []
    with ThreadPoolExecutor(max_workers=max(1, daily_workers)) as ex:
        futures = [ex.submit(run_one_daily_task, task) for task in tasks]
        for fut in as_completed(futures):
            try:
                row = fut.result()
                if row is not None:
                    daily_rows.append(row)
            except Exception as e:
                print("[ERROR]", repr(e))

    daily_df = pd.DataFrame(daily_rows)
    if not daily_df.empty:
        daily_df = daily_df[daily_df["daily_path"].notna()].reset_index(drop=True)
        daily_df = daily_df.sort_values(["region", "acq_date"]).reset_index(drop=True)
    return daily_df


def create_weekly_composite_from_daily_per_band(daily_paths, out_path, reducer="nanmean", chunk_size=10000,
                                                overwrite=False, compress=False, keep_temp=False, temp_dir=None):
    out_path = Path(out_path)
    if out_path.exists() and not overwrite:
        print(f"[SKIP] exists -> {out_path}")
        return out_path

    daily_paths = [Path(p) for p in daily_paths if p is not None and Path(p).exists()]
    if not daily_paths:
        print("No daily files.")
        return None

    srcs = [rasterio.open(p) for p in daily_paths]
    try:
        ref = srcs[0]
        for src in srcs[1:]:
            if src.crs != ref.crs:
                raise ValueError(f"CRS mismatch: {src.name}")
            if src.transform != ref.transform:
                raise ValueError(f"Transform mismatch: {src.name}")
            if src.width != ref.width or src.height != ref.height:
                raise ValueError(f"Shape mismatch: {src.name}")
            if src.count != ref.count:
                raise ValueError(f"Band count mismatch: {src.name}")

        n_bands = ref.count
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if temp_dir is None:
            temp_dir = out_path.parent / f"{out_path.stem}_tempbands"
        temp_dir = Path(temp_dir)
        if temp_dir.exists() and overwrite:
            shutil.rmtree(temp_dir)
        temp_dir.mkdir(parents=True, exist_ok=True)

        print(f"[INFO] weekly per-band | reducer={reducer} | n_daily={len(srcs)} | n_bands={n_bands} | shape=({ref.height}, {ref.width})")

        band_meta = ref.meta.copy()
        band_meta.update({
            "driver": "GTiff",
            "count": 1,
            "dtype": "float32",
            "nodata": np.nan,
            "tiled": True,
            "BIGTIFF": "IF_SAFER",
        })
        if compress:
            band_meta.update({"compress": "DEFLATE", "predictor": 2})

        def reduce_stack(stack, mode):
            if mode == "nanmean":
                with np.errstate(all="ignore"):
                    return np.nanmean(stack, axis=0).astype(np.float32)
            elif mode == "nanmedian":
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=RuntimeWarning)
                    return np.nanmedian(stack, axis=0).astype(np.float32)
            elif mode == "lastvalid":
                out = np.full(stack.shape[1:], np.nan, dtype=np.float32)
                for i in range(stack.shape[0]):
                    a = stack[i]
                    m = np.isfinite(a)
                    out[m] = a[m]
                return out
            elif mode == "firstvalid":
                out = np.full(stack.shape[1:], np.nan, dtype=np.float32)
                for i in range(stack.shape[0]):
                    a = stack[i]
                    m = (~np.isfinite(out)) & np.isfinite(a)
                    out[m] = a[m]
                return out
            else:
                raise ValueError(f"Unknown reducer: {mode}")

        temp_band_paths = []
        n_rows = (ref.height + chunk_size - 1) // chunk_size
        n_cols = (ref.width + chunk_size - 1) // chunk_size
        total_chunks = n_rows * n_cols

        for band_idx in range(1, n_bands + 1):
            band_name = ref.descriptions[band_idx - 1] or f"band_{band_idx}"
            temp_band_path = temp_dir / f"band_{band_idx:02d}_{band_name}.tif"

            if temp_band_path.exists() and not overwrite:
                print(f"[SKIP] temp band exists -> {temp_band_path.name}")
                temp_band_paths.append(temp_band_path)
                continue

            print(f"\n[PROCESS] band {band_idx}/{n_bands} -> {band_name}")
            with rasterio.open(temp_band_path, "w", **band_meta) as dst_band:
                done = 0
                for row_off in range(0, ref.height, chunk_size):
                    h = min(chunk_size, ref.height - row_off)
                    for col_off in range(0, ref.width, chunk_size):
                        w = min(chunk_size, ref.width - col_off)
                        window = Window(col_off, row_off, w, h)
                        arrs = []
                        for src in srcs:
                            a = clean_s1_linear_array(
                                src.read(band_idx, window=window),
                                nodata=src.nodata,
                            )
                            arrs.append(a)
                        stack = np.stack(arrs, axis=0)
                        comp = reduce_stack(stack, reducer)
                        dst_band.write(comp, 1, window=window)
                        done += 1
                        if done % 10 == 0 or done == total_chunks:
                            print(f"  band {band_idx} chunk {done}/{total_chunks}")
                dst_band.set_band_description(1, band_name)
            temp_band_paths.append(temp_band_path)
            print(f"[OK] temp band saved -> {temp_band_path}")

        final_meta = ref.meta.copy()
        final_meta.update({
            "driver": "GTiff",
            "count": n_bands,
            "dtype": "float32",
            "nodata": np.nan,
            "tiled": True,
            "BIGTIFF": "IF_SAFER",
        })
        if compress:
            final_meta.update({"compress": "DEFLATE", "predictor": 2})

        print(f"\n[MERGE] temp bands -> {out_path}")
        with rasterio.open(out_path, "w", **final_meta) as dst:
            for out_band_idx, temp_band_path in enumerate(temp_band_paths, start=1):
                with rasterio.open(temp_band_path) as src_band:
                    for _, window in src_band.block_windows(1):
                        arr = src_band.read(1, window=window).astype(np.float32)
                        dst.write(arr, out_band_idx, window=window)
                    band_name = src_band.descriptions[0] or f"band_{out_band_idx}"
                    dst.set_band_description(out_band_idx, band_name)

        print(f"[OK] weekly composite -> {out_path}")
        if not keep_temp:
            shutil.rmtree(temp_dir, ignore_errors=True)
            print(f"[CLEANUP] removed temp dir -> {temp_dir}")
        return out_path
    finally:
        for s in srcs:
            s.close()


def build_weekly(daily_df, target_regions, weekly_root, date_from, date_to, reducer="nanmean",
                 chunk_size=10000, overwrite=False, compress=False):
    weekly_rows = []
    for region_name in target_regions:
        df_reg = daily_df[daily_df["region"] == region_name].copy()
        if df_reg.empty:
            print(f"[SKIP] no daily files for {region_name}")
            continue
        daily_paths = df_reg["daily_path"].dropna().tolist()
        out_path = Path(weekly_root) / f"{region_name.lower()}_{date_from}_{date_to}_weekly_clean_10m.tif"
        print(f"\n=== WEEKLY: {region_name} ===")
        print("n_daily:", len(daily_paths))
        result = create_weekly_composite_from_daily_per_band(
            daily_paths=daily_paths,
            out_path=out_path,
            reducer=reducer,
            chunk_size=chunk_size,
            overwrite=overwrite,
            compress=compress,
            keep_temp=False,
        )
        weekly_rows.append({
            "region": region_name,
            "weekly_path": str(result) if result else None,
            "n_daily": len(daily_paths),
        })

    weekly_df = pd.DataFrame(weekly_rows)
    if not weekly_df.empty:
        weekly_df = weekly_df[weekly_df["weekly_path"].notna()].reset_index(drop=True)
    return weekly_df


def calc_aoi_valid_coverage_from_raster(raster_path, region_gdf):
    raster_path = Path(raster_path)
    if not raster_path.exists():
        return 0.0

    with rasterio.open(raster_path) as src:
        region_proj = region_gdf.to_crs(src.crs)
        geoms_proj = [
            geom.__geo_interface__
            for geom in region_proj.geometry
            if geom is not None and not geom.is_empty
        ]
        if not geoms_proj:
            return 0.0

        aoi_mask = geometry_mask(
            geoms_proj,
            transform=src.transform,
            invert=True,
            out_shape=(src.height, src.width),
        )
        total = int(aoi_mask.sum())
        if total <= 0:
            return 0.0

        valid = aoi_mask.copy()
        for band_idx in range(1, src.count + 1):
            arr = clean_s1_linear_array(src.read(band_idx), nodata=src.nodata)
            valid &= np.isfinite(arr)

        return float(valid.sum()) / float(total)


def clip_array(arr, vmin=None, vmax=None):
    arr = arr.astype(np.float32, copy=False)
    if vmin is not None or vmax is not None:
        arr = np.clip(arr, vmin if vmin is not None else -np.inf, vmax if vmax is not None else np.inf)
    return arr


def safe_divide(a, b):
    out = np.full_like(a, np.nan, dtype=np.float32)
    m = np.isfinite(a) & np.isfinite(b) & (b != 0)
    out[m] = a[m] / b[m]
    return out


def compute_indices_from_weekly(weekly_path, out_path, overwrite=False, compress=True):
    weekly_path = Path(weekly_path)
    out_path = Path(out_path)
    if out_path.exists() and not overwrite:
        print(f"[SKIP] exists -> {out_path}")
        return out_path
    if not weekly_path.exists():
        print(f"Missing: {weekly_path}")
        return None

    with rasterio.open(weekly_path) as src:
        VV = clean_s1_linear_array(src.read(1), nodata=src.nodata)
        VH = clean_s1_linear_array(src.read(2), nodata=src.nodata)

        vv_minus_vh = clip_array((VV - VH).astype(np.float32), -100, 100)
        vv_div_vh = clip_array(safe_divide(VV, VH), -100, 100)

        stack = np.stack([vv_minus_vh, vv_div_vh], axis=0).astype(np.float32)

        meta = src.meta.copy()
        meta.update({"count": 2, "dtype": "float32", "nodata": np.nan, "BIGTIFF": "IF_SAFER", "tiled": True})
        if compress:
            meta.update({"compress": "DEFLATE", "predictor": 2})

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(out_path, "w", **meta) as dst:
            dst.write(stack)
            dst.set_band_description(1, "VV_minus_VH")
            dst.set_band_description(2, "VV_div_VH")

    print(f"[OK] indices -> {out_path}")
    return out_path


def run_one_week(args):
    global GLOBAL_WEEK_DOWNLOAD_SEMAPHORE

    if not args.date_from or not args.date_to:
        raise ValueError("date-from dan date-to wajib untuk single_week")

    week_started_at = now_label()
    week_t0 = time.time()
    stage_times = {}

    date_from_iso = iso_start(args.date_from)
    date_to_iso = iso_end(args.date_to)

    base_workdir = ensure_dir(args.workdir)
    target_final_root = ensure_dir(args.target_final_root) if args.target_final_root else ensure_dir(base_workdir / "final_target")
    existing_final_root = Path(args.existing_final_root) if args.existing_final_root else target_final_root
    target_final_weekly_root = ensure_dir(target_final_root / "weekly")
    target_final_indices_root = ensure_dir(target_final_root / "indices")

    raw_root = ensure_dir(base_workdir / "downloads")
    temp_root = ensure_dir(base_workdir / "temp_tiles")
    daily_root = ensure_dir(base_workdir / "daily")
    logs_root = ensure_dir(base_workdir / "logs")
    snap_root = ensure_dir(base_workdir / "snap_tc")
    graph_root = ensure_dir(base_workdir / "snap_graphs")
    snap_graph_path = graph_root / "s1_grd_subset_tc.xml"

    print(f"[TIMING] week_start: {week_started_at} | {args.date_from} -> {args.date_to}")

    stage_t0 = time.time()
    print("=== LOAD SHAPEFILE ===")
    aoi_gdf = load_regions(args.shp_path, args.regions)
    print(aoi_gdf[["ADM1_EN", "ADM1_PCODE"]])
    record_stage_time(stage_times, "load_shapefile", stage_t0)

    if final_outputs_exist(existing_final_root, args.regions, args.date_from, args.date_to, require_indices=False) and not args.overwrite:
        print(f"[SKIP] final outputs already exist for {args.date_from} -> {args.date_to} at {existing_final_root}")
        total_elapsed = time.time() - week_t0
        print(f"[TIMING] week_total: {format_duration(total_elapsed)}")
        return {
            "date_from": args.date_from,
            "date_to": args.date_to,
            "status": "skipped_exists",
            "started_at": week_started_at,
            "finished_at": now_label(),
            "duration_sec": round(total_elapsed, 1),
            "duration_min": round(total_elapsed / 60.0, 2),
            "duration_hms": format_duration(total_elapsed),
        }

    stage_t0 = time.time()
    if args.skip_query_download:
        scenes = pd.DataFrame()
        print("[SKIP] query/download stage")
    else:
        dfs = []
        for region_name in args.regions:
            region = aoi_gdf[aoi_gdf["ADM1_EN"] == region_name].copy()
            df_region = fetch_products_for_region(
                region_name=region_name,
                region_gdf=region,
                date_from=date_from_iso,
                date_to=date_to_iso,
                top_n=args.top_n,
                product_types=args.product_types,
                sensor_mode=args.sensor_mode,
                polarisation=args.polarisation,
            )
            dfs.append(df_region)
        scenes = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        print(f"[SCENES] raw_query={len(scenes)}")
        if not scenes.empty:
            scenes = scenes.drop_duplicates(subset=["region", "id"]).reset_index(drop=True)
            print(f"[SCENES] unique={len(scenes)}")
            scenes = add_overlap_metrics(scenes, aoi_gdf, buffer_deg=args.overlap_buffer_deg)
            n_with_geom = int(scenes["footprint_geom"].notna().sum()) if "footprint_geom" in scenes.columns else 0
            n_bbox_hit = int(scenes["bbox_hit"].sum()) if "bbox_hit" in scenes.columns else 0
            n_overlap_hit = int((scenes["overlap_area_deg2"] > args.min_overlap_area_deg2).sum())
            print(
                f"[SCENES] footprint_geom={n_with_geom} | bbox_hit={n_bbox_hit} | "
                f"overlap_hit={n_overlap_hit} | min_overlap_area_deg2={args.min_overlap_area_deg2}"
            )
            before_overlap_filter = len(scenes)
            scenes = scenes[
                scenes["bbox_hit"] & (scenes["overlap_area_deg2"] > args.min_overlap_area_deg2)
            ].copy().reset_index(drop=True)
            print(
                f"[SCENES] after_overlap_filter={len(scenes)} | "
                f"dropped={before_overlap_filter - len(scenes)}"
            )
            if args.scene_selection_mode == "all_overlap":
                print(f"[SCENE SELECT] all_overlap active | scenes_kept={len(scenes)}")
            elif args.scene_selection_mode == "top_n_daily":
                scenes = limit_scenes_per_day(scenes, max_scenes_per_day=args.max_scenes_per_day)
            elif args.scene_selection_mode == "adaptive_daily_cover":
                scenes = select_scenes_for_coverage(
                    scenes=scenes,
                    aoi_gdf=aoi_gdf,
                    group_cols=["region", "acq_date"],
                    coverage_target_ratio=args.coverage_target_ratio,
                    max_scenes_per_group=args.adaptive_max_scenes,
                    min_incremental_overlap_area_deg2=args.min_incremental_overlap_area_deg2,
                )
            elif args.scene_selection_mode == "adaptive_weekly_cover":
                scenes = select_scenes_for_coverage(
                    scenes=scenes,
                    aoi_gdf=aoi_gdf,
                    group_cols=["region"],
                    coverage_target_ratio=args.coverage_target_ratio,
                    max_scenes_per_group=args.adaptive_max_scenes,
                    min_incremental_overlap_area_deg2=args.min_incremental_overlap_area_deg2,
                )
            else:
                raise ValueError(f"Unknown scene_selection_mode: {args.scene_selection_mode}")
            scenes = scenes.drop(columns=["footprint_geom"], errors="ignore")
            scenes = scenes.sort_values(["content_start", "region", "name"]).reset_index(drop=True)
            save_csv(scenes, logs_root / f"scenes_{args.date_from}_{args.date_to}.csv")

        if scenes.empty:
            raise RuntimeError(
                "Tidak ada scene S1 yang lolos filter AOI/tanggal/productType. "
                "Cek log [SCENES] raw_query/footprint_geom/bbox_hit dan file scenes CSV."
            )
    record_stage_time(stage_times, "query_select_scenes", stage_t0)

    stage_t0 = time.time()
    if args.skip_query_download:
        downloaded_df = scan_existing_zips(raw_root, scenes=scenes)
        if downloaded_df.empty:
            raise RuntimeError("Skip query/download aktif tetapi tidak ada ZIP existing.")
        downloaded_df["zip_exists"] = downloaded_df["zip_path"].apply(
            lambda p: bool(p is not None and Path(p).exists() and Path(p).stat().st_size > 0)
        )
        downloaded_df = downloaded_df[downloaded_df["zip_exists"]].copy().reset_index(drop=True)

        if scenes is not None and not scenes.empty:
            expected = set(zip(scenes["region"], scenes["name"]))
            got = set(zip(downloaded_df["region"], downloaded_df["name"]))
            missing = sorted(expected - got, key=str)
            if missing:
                raise RuntimeError(
                    f"Skip query/download dipakai, tetapi ZIP belum lengkap. "
                    f"Missing={len(missing)} | contoh={missing[:10]}"
                )
    else:
        if GLOBAL_WEEK_DOWNLOAD_SEMAPHORE is None:
            raise RuntimeError("GLOBAL_WEEK_DOWNLOAD_SEMAPHORE belum diinisialisasi")

        print(f"[WEEK-DOWNLOAD] waiting slot for week {args.date_from} -> {args.date_to} ...")
        with GLOBAL_WEEK_DOWNLOAD_SEMAPHORE:
            print(f"[WEEK-DOWNLOAD] acquired slot for week {args.date_from} -> {args.date_to}")

            downloaded_df = download_all_products_parallel(
                scenes=scenes,
                raw_root=raw_root,
                overwrite=args.overwrite,
                download_workers=args.download_workers,
                max_rounds=args.download_max_rounds,
                sleep_between_rounds=args.download_round_sleep,
                max_retries_per_file=args.download_max_retries_per_file,
                backoff_base=args.download_backoff_base,
                backoff_max=args.download_backoff_max,
            )
            save_csv(downloaded_df, logs_root / f"download_results_{args.date_from}_{args.date_to}.csv")

        print(f"[WEEK-DOWNLOAD] released slot for week {args.date_from} -> {args.date_to}")
    record_stage_time(stage_times, "download", stage_t0)

    if downloaded_df.empty and not args.skip_unzip:
        raise RuntimeError("Tidak ada zip untuk diproses")

    stage_t0 = time.time()
    if args.skip_unzip:
        safe_df = scan_existing_safe(raw_root, scenes=scenes)
    else:
        safe_df = unzip_archives(downloaded_df, overwrite=args.overwrite, cleanup_zips=args.cleanup_zips)

    if safe_df.empty:
        raise RuntimeError("Tidak ada SAFE ditemukan")

    expected_safe = set(zip(downloaded_df["region"], downloaded_df["name"]))
    got_safe = set(zip(safe_df["region"], safe_df["name"]))
    missing_safe = sorted(expected_safe - got_safe, key=str)
    if missing_safe:
        raise RuntimeError(
            f"Tidak semua ZIP berhasil diekstrak menjadi SAFE. "
            f"Missing SAFE={len(missing_safe)} | contoh={missing_safe[:10]}"
        )

    if "region" in safe_df.columns and safe_df["region"].isna().any() and not scenes.empty:
        missing_region_mask = safe_df["region"].isna()
        name_to_region = dict(zip(scenes["name"], scenes["region"]))
        safe_df.loc[missing_region_mask, "region"] = safe_df.loc[missing_region_mask, "name"].map(name_to_region)
    save_csv(safe_df, logs_root / f"safe_df_{args.date_from}_{args.date_to}.csv")

    if not scenes.empty:
        safe_selected = safe_df[safe_df["name"].isin(set(scenes["name"].tolist()))].copy()
    else:
        safe_selected = safe_df.copy()
    save_csv(safe_selected, logs_root / f"safe_selected_{args.date_from}_{args.date_to}.csv")
    record_stage_time(stage_times, "unzip_prepare_safe", stage_t0)

    if args.use_snap_preprocessing:
        write_snap_graph_xml(snap_graph_path)

        t0 = time.time()
        preprocessed_df = preprocess_safe_stage_with_snap(
            safe_df=safe_selected,
            aoi_gdf=aoi_gdf,
            target_regions=args.regions,
            snap_root=snap_root,
            graph_xml=snap_graph_path,
            gpt_path=args.gpt_path,
            dem_name=args.snap_dem_name,
            pixel_spacing=args.snap_pixel_spacing,
            overwrite=args.overwrite,
            snap_workers=args.snap_workers,
            snap_subset_buffer_deg=args.snap_subset_buffer_deg,
        )
        record_stage_time(stage_times, "snap_preprocess", t0)

        if preprocessed_df.empty:
            raise RuntimeError("Tidak ada hasil SNAP preprocessing dihasilkan")

        save_csv(preprocessed_df, logs_root / f"snap_preprocessed_{args.date_from}_{args.date_to}.csv")
    else:
        raise RuntimeError("Aktifkan --use-snap-preprocessing.")

    region_grid_info = build_region_grid_info(aoi_gdf, args.regions, target_res=args.target_res)

    t0 = time.time()
    temp_tiles_df = process_safe_stage(
        preprocessed_df,
        aoi_gdf,
        args.regions,
        temp_root=temp_root,
        target_res=args.target_res,
        overwrite=args.overwrite,
        temp_workers=args.temp_workers,
        region_grid_info=region_grid_info,
    )
    record_stage_time(stage_times, "temp_tiles", t0)
    if temp_tiles_df.empty:
        raise RuntimeError("Tidak ada temp tile dihasilkan")
    save_csv(temp_tiles_df, logs_root / f"temp_tiles_{args.date_from}_{args.date_to}.csv")

    t0 = time.time()
    daily_df = build_daily_mosaics(
        temp_tiles_df,
        args.regions,
        region_grid_info,
        daily_root=daily_root,
        target_res=args.target_res,
        overwrite=args.overwrite,
        daily_workers=args.daily_workers,
    )
    record_stage_time(stage_times, "daily_mosaic", t0)
    if daily_df.empty:
        raise RuntimeError("Tidak ada daily mosaic dihasilkan")
    save_csv(daily_df, logs_root / f"daily_{args.date_from}_{args.date_to}.csv")

    t0 = time.time()
    weekly_df = build_weekly(
        daily_df,
        args.regions,
        weekly_root=target_final_weekly_root,
        date_from=args.date_from,
        date_to=args.date_to,
        reducer=args.weekly_reducer,
        chunk_size=args.weekly_chunk_size,
        overwrite=args.overwrite,
        compress=args.compress_final,
    )
    record_stage_time(stage_times, "weekly_composite", t0)
    if weekly_df.empty:
        raise RuntimeError("Tidak ada weekly composite dihasilkan")

    weekly_df["valid_coverage_ratio"] = np.nan
    for idx, row in weekly_df.iterrows():
        region_name = row["region"]
        region_gdf = aoi_gdf[aoi_gdf["ADM1_EN"] == region_name].copy()
        coverage_ratio = calc_aoi_valid_coverage_from_raster(row["weekly_path"], region_gdf)
        weekly_df.at[idx, "valid_coverage_ratio"] = coverage_ratio
        print(f"[VALID COVERAGE] {region_name}: {coverage_ratio:.4f}")

    if args.min_weekly_valid_coverage_ratio > 0:
        low_coverage = weekly_df[
            weekly_df["valid_coverage_ratio"].fillna(0.0) < args.min_weekly_valid_coverage_ratio
        ].copy()
        if not low_coverage.empty:
            details = low_coverage[["region", "valid_coverage_ratio", "weekly_path"]].to_dict("records")
            for bad_path in low_coverage["weekly_path"].dropna().tolist():
                try:
                    bad_path = Path(bad_path)
                    if bad_path.exists():
                        bad_path.unlink()
                        print(f"[CLEANUP] removed low-coverage weekly output: {bad_path}")
                except Exception as e:
                    print(f"[WARN] failed to remove low-coverage weekly output {bad_path}: {e}")
            raise RuntimeError(
                f"Weekly valid coverage terlalu rendah. "
                f"threshold={args.min_weekly_valid_coverage_ratio:.4f} | details={details}. "
                "Gunakan --scene-selection-mode all_overlap untuk minggu bermasalah, "
                "atau turunkan threshold jika gap tersebut memang tidak bisa diisi oleh scene minggu itu."
            )

    save_csv(weekly_df, logs_root / f"weekly_{args.date_from}_{args.date_to}.csv")

    produced_weekly_regions = set(weekly_df["region"].tolist()) if not weekly_df.empty else set()
    missing_weekly_regions = sorted(set(args.regions) - produced_weekly_regions)
    if missing_weekly_regions and not args.allow_partial_regions:
        raise RuntimeError(
            f"Weekly output tidak lengkap. Missing regions={missing_weekly_regions}. "
            "Gunakan --allow-partial-regions jika memang ingin menerima hasil sebagian."
        )
    if missing_weekly_regions:
        print(f"[WARN] partial weekly outputs. missing={missing_weekly_regions}")

    t0 = time.time()
    indices_df = pd.DataFrame()
    if args.compute_indices:
        indices_rows = []
        for _, row in weekly_df.iterrows():
            region_name = row["region"]
            weekly_path = row["weekly_path"]
            if not weekly_path:
                continue
            out_path = Path(target_final_indices_root) / f"{region_name.lower()}_{args.date_from}_{args.date_to}_weekly_indices_10m.tif"
            result = compute_indices_from_weekly(
                weekly_path=weekly_path,
                out_path=out_path,
                overwrite=args.overwrite,
                compress=args.compress_final,
            )
            indices_rows.append({"region": region_name, "indices_path": str(result) if result else None})

        indices_df = pd.DataFrame(indices_rows)
        if not indices_df.empty:
            indices_df = indices_df[indices_df["indices_path"].notna()].reset_index(drop=True)
        save_csv(indices_df, logs_root / f"indices_{args.date_from}_{args.date_to}.csv")
    else:
        print("[SKIP] compute indices disabled")
    record_stage_time(stage_times, "indices", t0)

    total_elapsed = time.time() - week_t0
    stage_times_rounded = {k: round(v, 1) for k, v in stage_times.items()}
    print("\n=== TIMING ===")
    for stage_name, elapsed in stage_times.items():
        print(f"{stage_name}: {format_duration(elapsed)}")
    print(f"week_total: {format_duration(total_elapsed)}")

    summary = {
        "date_from": args.date_from,
        "date_to": args.date_to,
        "started_at": week_started_at,
        "finished_at": now_label(),
        "duration_sec": round(total_elapsed, 1),
        "duration_min": round(total_elapsed / 60.0, 2),
        "duration_hms": format_duration(total_elapsed),
        "stage_times_sec": stage_times_rounded,
        "missing_weekly_regions": missing_weekly_regions,
        "regions": args.regions,
        "workdir": str(base_workdir),
        "existing_final_root": str(existing_final_root),
        "target_final_root": str(target_final_root),
        "n_scenes": int(len(scenes)) if scenes is not None else 0,
        "n_zip": int(len(downloaded_df)),
        "n_safe": int(len(safe_df)),
        "n_temp_tiles": int(len(temp_tiles_df)),
        "n_daily": int(len(daily_df)),
        "n_weekly": int(len(weekly_df)),
        "n_indices": int(len(indices_df)) if not indices_df.empty else 0,
        "weekly_reducer": args.weekly_reducer,
        "n_accounts": GLOBAL_ACCOUNT_POOL.size if GLOBAL_ACCOUNT_POOL else 0,
        "product_types": args.product_types,
        "sensor_mode": args.sensor_mode,
        "polarisation": args.polarisation,
        "compute_indices": args.compute_indices,
        "use_snap_preprocessing": args.use_snap_preprocessing,
        "gpt_path": args.gpt_path,
        "snap_dem_name": args.snap_dem_name,
        "snap_pixel_spacing": args.snap_pixel_spacing,
        "snap_workers": args.snap_workers,
        "scene_selection_mode": args.scene_selection_mode,
        "max_scenes_per_day": args.max_scenes_per_day,
        "adaptive_max_scenes": args.adaptive_max_scenes,
        "coverage_target_ratio": args.coverage_target_ratio,
        "min_incremental_overlap_area_deg2": args.min_incremental_overlap_area_deg2,
        "min_weekly_valid_coverage_ratio": args.min_weekly_valid_coverage_ratio,
        "overlap_buffer_deg": args.overlap_buffer_deg,
        "min_overlap_area_deg2": args.min_overlap_area_deg2,
        "snap_subset_buffer_deg": args.snap_subset_buffer_deg,
        "allow_partial_regions": args.allow_partial_regions,
    }
    print("\n=== SUMMARY ===")
    print(json.dumps(summary, indent=2))

    if args.save_manifest_json:
        manifest_path = logs_root / f"manifest_{args.date_from}_{args.date_to}.json"
        manifest_path.write_text(json.dumps(summary, indent=2))
        print(f"[SAVE] {manifest_path}")

    if args.delete_intermediate_on_success and final_outputs_exist(
        target_final_root,
        args.regions,
        args.date_from,
        args.date_to,
        require_indices=args.compute_indices,
    ):
        cleanup_dir(base_workdir)

    print("\nDONE.")
    return summary


def run_backfill_parallel_by_weeknum(args):
    if args.start_year is None or args.start_week is None:
        raise ValueError("--start-year dan --start-week wajib untuk run-mode backfill_weeknum")

    if args.end_year is None or args.end_week is None:
        args.end_year, args.end_week = current_iso_year_week()

    week_items = list_iso_weeks(args.start_year, args.start_week, args.end_year, args.end_week)

    if args.week_order == "desc":
        week_items = list(reversed(week_items))

    queue_items = [
        {"queue_idx": i, "year": y, "week": w}
        for i, (y, w) in enumerate(week_items, start=1)
    ]

    base_workdir = Path(args.workdir)
    existing_final_root = Path(args.existing_final_root) if args.existing_final_root else (
        Path(args.target_final_root) if args.target_final_root else (base_workdir / "final_target")
    )

    print(f"TOTAL QUEUE ITEMS = {len(queue_items)}")
    print(f"MAX_WEEK_WORKERS = {args.max_week_workers}")
    print(f"MAX_WEEKS_IN_DOWNLOAD = {args.max_weeks_in_download}")
    print(f"WEEK ORDER = {args.week_order}")
    if queue_items:
        print(f"FIRST QUEUE ITEM = {queue_items[0]['year']} W{queue_items[0]['week']:02d}")
        print(f"LAST  QUEUE ITEM = {queue_items[-1]['year']} W{queue_items[-1]['week']:02d}")

    def _run_week_item(item):
        queue_idx = item["queue_idx"]
        year = item["year"]
        week = item["week"]
        item_started_at = now_label()
        item_t0 = time.time()

        week_start, week_end = iso_week_to_date_range(year, week)
        date_from = week_start.strftime("%Y-%m-%d")
        date_to = week_end.strftime("%Y-%m-%d")
        run_dir = base_workdir / "runs" / f"{year}_W{week:02d}_{date_from}_{date_to}"

        if final_outputs_exist(existing_final_root, args.regions, date_from, date_to, require_indices=False) and not args.overwrite:
            duration_sec = time.time() - item_t0
            print(f"[SKIP] final exists | queue={queue_idx} | {year} W{week:02d} | {date_from} -> {date_to}")
            return {
                "queue_idx": queue_idx,
                "year": year,
                "week": week,
                "date_from": date_from,
                "date_to": date_to,
                "started_at": item_started_at,
                "finished_at": now_label(),
                "duration_sec": round(duration_sec, 1),
                "duration_min": round(duration_sec / 60.0, 2),
                "duration_hms": format_duration(duration_sec),
                "status": "skipped_exists",
            }

        week_args = argparse.Namespace(**vars(args))
        week_args.date_from = date_from
        week_args.date_to = date_to
        week_args.workdir = str(run_dir)

        try:
            print(f"[QUEUE] START | queue={queue_idx} | {year} W{week:02d} | {date_from} -> {date_to}")
            week_summary = run_one_week(week_args)
            duration_sec = time.time() - item_t0
            return {
                "queue_idx": queue_idx,
                "year": year,
                "week": week,
                "date_from": date_from,
                "date_to": date_to,
                "started_at": item_started_at,
                "finished_at": now_label(),
                "duration_sec": round(duration_sec, 1),
                "duration_min": round(duration_sec / 60.0, 2),
                "duration_hms": format_duration(duration_sec),
                "n_scenes": week_summary.get("n_scenes") if isinstance(week_summary, dict) else None,
                "n_zip": week_summary.get("n_zip") if isinstance(week_summary, dict) else None,
                "n_safe": week_summary.get("n_safe") if isinstance(week_summary, dict) else None,
                "n_daily": week_summary.get("n_daily") if isinstance(week_summary, dict) else None,
                "n_weekly": week_summary.get("n_weekly") if isinstance(week_summary, dict) else None,
                "status": "ok",
            }
        except Exception as e:
            duration_sec = time.time() - item_t0
            print(f"[FAIL] queue={queue_idx} | {year} W{week:02d} | {date_from} -> {date_to} | {e}")
            return {
                "queue_idx": queue_idx,
                "year": year,
                "week": week,
                "date_from": date_from,
                "date_to": date_to,
                "started_at": item_started_at,
                "finished_at": now_label(),
                "duration_sec": round(duration_sec, 1),
                "duration_min": round(duration_sec / 60.0, 2),
                "duration_hms": format_duration(duration_sec),
                "status": f"fail: {e}",
            }

    results = []
    with ThreadPoolExecutor(max_workers=max(1, args.max_week_workers)) as ex:
        futures = [ex.submit(_run_week_item, item) for item in queue_items]
        for fut in as_completed(futures):
            res = fut.result()
            results.append(res)
            print(
                f"[QUEUE] DONE | queue={res['queue_idx']} | {res['year']} W{res['week']:02d} | "
                f"{res['status']} | duration={res.get('duration_hms', 'n/a')}"
            )

    results_df = pd.DataFrame(results)
    if not results_df.empty:
        results_df = results_df.sort_values(["queue_idx"]).reset_index(drop=True)

    print("\n=== BACKFILL SUMMARY ===")
    print(results_df)
    summary_path = base_workdir / "backfill_weeknum_summary.csv"
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    results_df.to_csv(summary_path, index=False)
    print(f"[SAVE] {summary_path}")
    return results_df


def main():
    args = parse_args()

    global GLOBAL_DOWNLOAD_SEMAPHORE
    global GLOBAL_WEEK_DOWNLOAD_SEMAPHORE
    global GLOBAL_ACCOUNT_POOL

    if not args.skip_query_download:
        if not args.password:
            raise ValueError("CDSE password belum diisi")
        if not args.usernames:
            raise ValueError("Daftar username kosong")

        GLOBAL_ACCOUNT_POOL = AccountPool(
            usernames=args.usernames,
            password=args.password,
            base_backoff=args.download_backoff_base,
            max_backoff=args.download_backoff_max,
        )

        GLOBAL_DOWNLOAD_SEMAPHORE = threading.BoundedSemaphore(max(1, args.global_download_slots))
        GLOBAL_WEEK_DOWNLOAD_SEMAPHORE = threading.BoundedSemaphore(max(1, args.max_weeks_in_download))

        print(f"[INIT] usernames = {args.usernames}")
        print(f"[INIT] account pool size = {GLOBAL_ACCOUNT_POOL.size}")
        print(f"[INIT] global download slots = {args.global_download_slots}")
        print(f"[INIT] max weeks in download phase = {args.max_weeks_in_download}")

    if args.run_mode == "single_week":
        run_one_week(args)
    else:
        run_backfill_parallel_by_weeknum(args)


if __name__ == "__main__":
    main()
