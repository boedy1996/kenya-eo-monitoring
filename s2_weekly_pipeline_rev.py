#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sentinel-2 weekly pipeline adapted from the notebook workflow.

Pipeline:
1) Query CDSE OData per region (bbox strategy)
2) Download selected products
3) Extract SAFE archives
4) Build clean temp tiles per SAFE:
   - resample all target bands to 10 m B02 grid
   - apply SCL cloud/shadow mask
   - clip as early as possible to region overlap window
   - mask outside AOI polygon
5) Build daily mosaics per region/date
6) Build weekly composite from daily outputs
7) Compute vegetation indices from weekly composite

Recommended usage:
- Keep temp/daily outputs uncompressed for speed
- Compress only final outputs if needed
- Resume-friendly: existing outputs are skipped unless --overwrite
- Credentials are read from env vars or CLI args
"""
import argparse
import json
import math
import os
import re
import shutil
import sys
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from pathlib import Path
from urllib.parse import quote

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
import requests
from rasterio.enums import Resampling
from rasterio.features import geometry_mask
from rasterio.merge import merge
from rasterio.vrt import WarpedVRT
from rasterio.windows import Window
from rasterio.features import geometry_window

import warnings

# =========================
# DEFAULTS / CONFIG
# =========================
CATALOG_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
DOWNLOAD_PRODUCTS_URL = "https://download.dataspace.copernicus.eu/odata/v1/Products"

DEFAULT_REGIONS = ["Nakuru", "Machakos"]

# Follow notebook order expected by compute_indices_from_weekly:
# B02 B03 B04 B05 B06 B07 B08 B8A B11 B12
TARGET_BANDS = ["B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12"]
BAND_NAMES = TARGET_BANDS.copy()

# Valid SCL classes:
# 4 vegetation, 5 not vegetated, 6 water, 7 unclassified
VALID_SCL = {4, 5, 6, 7}


def parse_args():
    p = argparse.ArgumentParser(description="Sentinel-2 weekly clean mosaic pipeline adapted from notebook")
    p.add_argument("--date-from", required=True, help="YYYY-MM-DD")
    p.add_argument("--date-to", required=True, help="YYYY-MM-DD")
    p.add_argument("--shp-path", required=True, help="Path to Kenya ADM1 shapefile")
    p.add_argument("--regions", nargs="+", default=DEFAULT_REGIONS, help="ADM1_EN region names")
    p.add_argument("--workdir", required=True, help="Working directory")
    p.add_argument("--username", default=os.getenv("CDSE_USERNAME"), help="CDSE username")
    p.add_argument("--password", default=os.getenv("CDSE_PASSWORD"), help="CDSE password")
    p.add_argument("--top-n", type=int, default=100, help="Max OData rows per region query")

    p.add_argument("--download-workers", type=int, default=2, help="Parallel download workers")
    p.add_argument("--download-max-rounds", type=int, default=20, help="Max round retry strict download")
    p.add_argument("--download-round-sleep", type=int, default=30, help="Sleep antar round retry download (detik)")

    p.add_argument("--target-res", type=float, default=10.0, help="Target resolution in meters")
    p.add_argument("--temp-workers", type=int, default=1, help="Workers for SAFE -> temp tile stage. For stability keep 1-2.")
    p.add_argument("--daily-workers", type=int, default=2, help="Workers for daily mosaics (threaded)")
    p.add_argument("--weekly-mode", choices=["full","chunked"], default="full", help="Compatibility arg; weekly build still uses per-band reducer")
    p.add_argument("--weekly-chunk-size", type=int, default=10000, help="Chunk size for weekly composite")
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs")
    p.add_argument("--skip-query-download", action="store_true", help="Skip query+download and reuse existing raw zips / SAFE")
    p.add_argument("--skip-unzip", action="store_true", help="Skip unzip and reuse existing SAFE")
    p.add_argument("--compress-final", action="store_true", help="Compress weekly / indices outputs")
    p.add_argument("--cleanup-zips", action="store_true", help="Delete zip after successful extract")
    p.add_argument("--cleanup-safe", action="store_true", help="Delete SAFE folders after successful temp tile creation")
    p.add_argument("--save-manifest-json", action="store_true", help="Save final manifest json")
    return p.parse_args()

def is_token_expired_response(resp):
    if resp is None:
        return False
    if resp.status_code != 401:
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
    if resp is None:
        return False
    if resp.status_code != 429:
        return False

    try:
        data = resp.json()
        msg = str(data.get("message", "")).lower()
        code = str(data.get("code", "")).upper()
        return ("max session number exceeded" in msg) or (code == "DAT-ZIP-100")
    except Exception:
        txt = (resp.text or "").lower()
        return "max session number exceeded" in txt

def refresh_cdse_token(username, password):
    print("[AUTH] refreshing CDSE token...")
    return get_cdse_access_token(username, password)

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


def parse_safe_name(safe_name):
    safe_name = Path(safe_name).name.replace(".zip", "").replace(".SAFE", "")
    parts = safe_name.split("_")
    if len(parts) < 6:
        raise ValueError(f"SAFE name tidak valid: {safe_name}")
    return {
        "platform": parts[0],
        "acq_date": parts[2][:8],
        "tile_code": parts[5],
    }


def load_regions(shp_path, target_regions):
    gdf = gpd.read_file(shp_path).to_crs(4326)
    aoi_gdf = gdf[gdf["ADM1_EN"].isin(target_regions)].copy()
    found = sorted(aoi_gdf["ADM1_EN"].unique().tolist())
    expected = sorted(target_regions)
    if found != expected:
        raise ValueError(f"Region tidak lengkap. Ketemu: {found}, expected: {expected}")
    return aoi_gdf


def build_bbox_wkt_from_region(region_gdf):
    minx, miny, maxx, maxy = region_gdf.total_bounds
    bbox_wkt = (
        f"POLYGON(({minx} {miny}, "
        f"{maxx} {miny}, "
        f"{maxx} {maxy}, "
        f"{minx} {maxy}, "
        f"{minx} {miny}))"
    )
    return bbox_wkt, (minx, miny, maxx, maxy)


def build_odata_url(bbox_wkt, date_from, date_to, top_n=100):
    filter_expr = (
        "Collection/Name eq 'SENTINEL-2' "
        "and Attributes/OData.CSC.StringAttribute/any(att:"
        "att/Name eq 'productType' and "
        "att/OData.CSC.StringAttribute/Value eq 'S2MSI2A') "
        f"and OData.CSC.Intersects(area=geography'SRID=4326;{bbox_wkt}') "
        f"and ContentDate/Start ge {date_from} "
        f"and ContentDate/Start le {date_to}"
    )
    encoded_filter = quote(filter_expr, safe="()=;,:/' ")
    return f"{CATALOG_URL}?$filter={encoded_filter}&$top={top_n}"


def fetch_products_for_region(region_name, region_gdf, date_from, date_to, top_n=100):
    bbox_wkt, bounds = build_bbox_wkt_from_region(region_gdf)
    url = build_odata_url(bbox_wkt, date_from, date_to, top_n=top_n)
    print(f"\n=== QUERY REGION: {region_name} ===")
    print("Bounds:", bounds)
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
            "content_start": (it.get("ContentDate") or {}).get("Start"),
            "content_end": (it.get("ContentDate") or {}).get("End"),
            "s3path": it.get("S3Path"),
            "online": it.get("Online"),
            "origin_date": it.get("OriginDate"),
            "publication_date": it.get("PublicationDate"),
            "footprint": it.get("Footprint"),
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
    print("token status:", r.status_code)
    r.raise_for_status()
    return r.json()["access_token"]


def build_product_download_url(product_id):
    return f"{DOWNLOAD_PRODUCTS_URL}({product_id})/$value"


def download_one_product(
    product_id,
    product_name,
    region,
    acq_date,
    token,
    out_dir,
    overwrite=False,
    username=None,
    password=None,
    max_retries=5,
):
    region_dir = ensure_dir(Path(out_dir) / region.lower() / acq_date)
    out_path = region_dir / safe_zip_name(product_name)

    if out_path.exists() and out_path.stat().st_size > 0 and not overwrite:
        print(f"[SKIP] already exists: {out_path.name}")
        return out_path, token

    url = build_product_download_url(product_id)
    current_token = token

    for attempt in range(1, max_retries + 1):
        headers = {
            "Authorization": f"Bearer {current_token}",
            "Accept": "application/octet-stream",
        }

        print(f"[DOWNLOAD] {product_name} | attempt {attempt}/{max_retries}")

        try:
            with requests.Session() as session:
                session.headers.update(headers)

                with session.get(url, stream=True, timeout=300, allow_redirects=True) as r:
                    print("final url:", r.url)
                    print("download status:", r.status_code)

                    if r.status_code != 200:
                        try:
                            print("response preview:", r.text[:500])
                        except Exception:
                            pass

                    # token expired -> refresh
                    if is_token_expired_response(r):
                        if not username or not password:
                            r.raise_for_status()

                        current_token = refresh_cdse_token(username, password)
                        print("[AUTH] token refreshed, retrying download...")
                        time.sleep(2)
                        continue

                    # too many concurrent sessions -> backoff
                    if is_max_session_response(r):
                        wait_s = min(120, 15 * attempt)
                        print(f"[RATE LIMIT] Max session exceeded. Sleep {wait_s}s lalu retry...")
                        time.sleep(wait_s)
                        continue

                    r.raise_for_status()

                    tmp_path = out_path.with_suffix(out_path.suffix + ".part")
                    with open(tmp_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                f.write(chunk)

                    tmp_path.replace(out_path)
                    print(f"[OK] saved: {out_path}")
                    return out_path, current_token

        except requests.HTTPError as e:
            if attempt >= max_retries:
                raise
            wait_s = min(60, 5 * attempt)
            print(f"[WARN] HTTPError, retrying in {wait_s}s... ({attempt}/{max_retries}) | {e}")
            time.sleep(wait_s)

        except requests.RequestException as e:
            if attempt >= max_retries:
                raise
            wait_s = min(60, 5 * attempt)
            print(f"[WARN] request failed: {e} | retrying in {wait_s}s... ({attempt}/{max_retries})")
            time.sleep(wait_s)

    raise RuntimeError(f"Download gagal setelah {max_retries} percobaan: {product_name}")


def download_all_products_parallel(
    scenes,
    raw_root,
    username,
    password,
    overwrite=False,
    download_workers=2,
    max_rounds=20,
    sleep_between_rounds=30,
):
    """
    STRICT MODE:
    - semua file harus terdownload
    - kalau masih ada yang gagal, ulang hanya file yang gagal
    - baru return kalau semua file sudah valid
    """
    if scenes.empty:
        return pd.DataFrame()

    remaining_df = scenes.copy().reset_index(drop=True)
    all_success_rows = []

    round_no = 0

    while not remaining_df.empty:
        round_no += 1
        print(f"\n{'='*70}")
        print(f"[DOWNLOAD ROUND {round_no}] remaining files: {len(remaining_df)}")
        print(f"{'='*70}")

        if round_no > max_rounds:
            fail_names = remaining_df["name"].tolist()
            raise RuntimeError(
                f"Masih ada file gagal setelah {max_rounds} round. "
                f"Jumlah fail={len(fail_names)} | contoh={fail_names[:10]}"
            )

        # 1 shared token per round
        shared_token = {"value": get_cdse_access_token(username, password)}
        token_lock = threading.Lock()

        def _download_row(row_dict):
            with token_lock:
                token = shared_token["value"]

            out_path, new_token = download_one_product(
                product_id=row_dict["id"],
                product_name=row_dict["name"],
                region=row_dict["region"],
                acq_date=row_dict["acq_date"],
                token=token,
                out_dir=raw_root,
                overwrite=overwrite,
                username=username,
                password=password,
                max_retries=5,
            )

            with token_lock:
                shared_token["value"] = new_token

            return {
                **row_dict,
                "zip_path": str(out_path),
                "status": "ok",
            }

        rows = remaining_df.to_dict("records")
        round_results = []
        ok = fail = 0

        with ThreadPoolExecutor(max_workers=max(1, download_workers)) as ex:
            futures = {ex.submit(_download_row, row): row for row in rows}

            for i, fut in enumerate(as_completed(futures), start=1):
                row = futures[fut]
                print(f"\n=== DOWNLOAD DONE {i}/{len(rows)} | ROUND {round_no} ===")
                try:
                    result = fut.result()
                    round_results.append(result)
                    ok += 1
                except Exception as e:
                    print(f"[FAIL] {row['name']} | {e}")
                    round_results.append({
                        **row,
                        "zip_path": None,
                        "status": f"fail: {e}",
                    })
                    fail += 1

        print(f"\n[ROUND {round_no}] summary: ok={ok} fail={fail}")

        round_df = pd.DataFrame(round_results)
        if round_df.empty:
            raise RuntimeError(f"Round {round_no} tidak menghasilkan output download.")

        round_df["zip_exists"] = round_df["zip_path"].apply(
            lambda p: bool(p is not None and Path(p).exists() and Path(p).stat().st_size > 0)
        )

        success_df = round_df[(round_df["status"] == "ok") & (round_df["zip_exists"])].copy()
        fail_df = round_df[(round_df["status"] != "ok") | (~round_df["zip_exists"])].copy()

        print(
            f"[ROUND {round_no}] valid_success={len(success_df)} | "
            f"still_failed={len(fail_df)}"
        )

        if not success_df.empty:
            all_success_rows.extend(success_df.to_dict("records"))

        if fail_df.empty:
            print(f"[DOWNLOAD] semua file berhasil pada round {round_no}")
            break

        fail_names = fail_df["name"].tolist()
        print(f"[DOWNLOAD] retry lagi {len(fail_names)} file gagal")
        print(f"[DOWNLOAD] contoh fail: {fail_names[:10]}")

        remaining_df = remaining_df[remaining_df["name"].isin(fail_names)].copy().reset_index(drop=True)

        print(f"[DOWNLOAD] sleep {sleep_between_rounds}s sebelum round berikutnya...")
        time.sleep(sleep_between_rounds)

    final_df = pd.DataFrame(all_success_rows)
    if final_df.empty:
        raise RuntimeError("Download selesai tetapi tidak ada file sukses tercatat.")

    # dedup by name
    final_df = final_df.drop_duplicates(subset=["name"], keep="last").reset_index(drop=True)

    expected_names = set(scenes["name"].tolist())
    got_names = set(final_df["name"].tolist())
    missing = sorted(expected_names - got_names)

    if missing:
        raise RuntimeError(
            f"Strict download gagal: masih ada file yang belum lengkap. "
            f"Jumlah missing={len(missing)} | contoh={missing[:10]}"
        )

    final_df = final_df.sort_values(["region", "acq_date", "name"]).reset_index(drop=True)

    print(
        f"\n[DOWNLOAD] STRICT SUCCESS | expected={len(expected_names)} | "
        f"success={len(final_df)}"
    )
    return final_df

def unzip_archives(downloaded_df, overwrite=False, cleanup_zips=False):
    rows = []
    for _, row in downloaded_df.iterrows():
        zip_path = Path(row["zip_path"])
        safe_dir = zip_path.parent / row["name"]
        if safe_dir.exists() and not overwrite:
            print(f"[SKIP] extracted: {safe_dir.name}")
        else:
            print(f"[UNZIP] {zip_path.name}")
            try:
                with zipfile.ZipFile(zip_path, "r") as zf:
                    zf.extractall(zip_path.parent)
            except zipfile.BadZipFile:
                print(f"[ERROR] bad zip: {zip_path}")
                continue

        if safe_dir.exists():
            rows.append({**row.to_dict(), "safe_dir": str(safe_dir)})
            if cleanup_zips:
                try:
                    zip_path.unlink()
                    print(f"[CLEANUP] deleted zip: {zip_path.name}")
                except Exception as e:
                    print(f"[WARN] cleanup zip failed {zip_path}: {e}")
        else:
            print(f"[ERROR] SAFE not found after unzip: {safe_dir}")
    return pd.DataFrame(rows)


def scan_existing_safe(raw_root, scenes=None):
    rows = []
    for safe_dir in sorted(Path(raw_root).rglob("*.SAFE")):
        safe_name = safe_dir.name
        try:
            meta = parse_safe_name(safe_name)
        except Exception:
            continue
        region = None
        scene_id = None
        if scenes is not None and not scenes.empty:
            hit = scenes[scenes["name"] == safe_name]
            if not hit.empty:
                region = hit.iloc[0]["region"]
                scene_id = hit.iloc[0]["id"]
        rows.append({
            "region": region,
            "id": scene_id,
            "name": safe_name,
            "platform": meta["platform"],
            "acq_date": meta["acq_date"],
            "tile_code": meta["tile_code"],
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
        region = None
        scene_id = None
        if scenes is not None and not scenes.empty:
            hit = scenes[scenes["name"] == safe_name]
            if not hit.empty:
                region = hit.iloc[0]["region"]
                scene_id = hit.iloc[0]["id"]
        rows.append({
            "region": region,
            "id": scene_id,
            "name": safe_name,
            "platform": meta["platform"],
            "acq_date": meta["acq_date"],
            "tile_code": meta["tile_code"],
            "zip_path": str(z),
        })
    return pd.DataFrame(rows)


def find_band_file(safe_dir, band):
    safe_dir = Path(safe_dir)
    patterns = [
        f"**/*_{band}_10m.jp2",
        f"**/*_{band}_20m.jp2",
        f"**/*_{band}_60m.jp2",
        f"**/*_{band}.jp2",
    ]
    for pat in patterns:
        hits = list(safe_dir.glob(pat))
        if hits:
            return hits[0]
    return None


def build_valid_mask_from_scl(scl_arr, valid_scl=VALID_SCL):
    return np.isin(scl_arr, list(valid_scl))


def process_one_safe_to_temp_tile(safe_dir, region_name, region_gdf, out_dir, overwrite=False, compress=False):
    safe_dir = Path(safe_dir)
    safe_name = safe_dir.name
    meta = parse_safe_name(safe_name)
    acq_date = meta["acq_date"]
    tile_code = meta["tile_code"]

    ref_path = find_band_file(safe_dir, "B02")
    scl_path = find_band_file(safe_dir, "SCL")
    if ref_path is None or scl_path is None:
        print(f"[SKIP] missing B02 or SCL: {safe_name}")
        return None

    out_dir = ensure_dir(out_dir)
    out_path = out_dir / f"{safe_name.replace('.SAFE', '')}_clean_10m.tif"
    if out_path.exists() and out_path.stat().st_size > 0 and not overwrite:
        print(f"[SKIP] exists: {out_path.name}")
        return out_path

    with rasterio.open(ref_path) as ref_ds:
        region_src = region_gdf.to_crs(ref_ds.crs)
        geoms = [geom.__geo_interface__ for geom in region_src.geometry if geom is not None and not geom.is_empty]
        if not geoms:
            print(f"[SKIP] empty geometry: {safe_name}")
            return None

        try:
            win = geometry_window(ref_ds, geoms, pad_x=0, pad_y=0)
        except Exception:
            print(f"[SKIP] no overlap: {safe_name}")
            return None

        if win.width <= 0 or win.height <= 0:
            print(f"[SKIP] invalid window: {safe_name}")
            return None

        ref_transform = ref_ds.window_transform(win)
        height = int(win.height)
        width = int(win.width)
        crs = ref_ds.crs

        with rasterio.open(scl_path) as scl_ds:
            with WarpedVRT(
                scl_ds,
                crs=crs,
                transform=ref_transform,
                width=width,
                height=height,
                resampling=Resampling.nearest,
            ) as scl_vrt:
                scl_arr = scl_vrt.read(1)

        scl_valid_mask = build_valid_mask_from_scl(scl_arr)
        region_mask = geometry_mask(
            geoms,
            transform=ref_transform,
            invert=True,
            out_shape=(height, width),
        )
        valid_mask = scl_valid_mask & region_mask

        if not np.any(valid_mask):
            print(f"[SKIP] no valid pixels after AOI+SCL mask: {safe_name}")
            return None

        out_stack = np.full((len(TARGET_BANDS), height, width), np.nan, dtype=np.float32)

        for i, band in enumerate(TARGET_BANDS):
            band_path = find_band_file(safe_dir, band)
            if band_path is None:
                print(f"[WARN] band missing {band}: {safe_name}")
                continue

            with rasterio.open(band_path) as bds:
                with WarpedVRT(
                    bds,
                    crs=crs,
                    transform=ref_transform,
                    width=width,
                    height=height,
                    resampling=Resampling.bilinear,
                ) as vrt:
                    arr = vrt.read(1).astype(np.float32)

            arr = arr / 10000.0
            arr[~valid_mask] = np.nan
            out_stack[i] = arr

        meta_out = {
            "driver": "GTiff",
            "height": height,
            "width": width,
            "count": len(TARGET_BANDS),
            "dtype": "float32",
            "crs": crs,
            "transform": ref_transform,
            "tiled": True,
            "nodata": np.nan,
            "BIGTIFF": "IF_SAFER",
        }
        if compress:
            meta_out["compress"] = "DEFLATE"
            meta_out["predictor"] = 2

        with rasterio.open(out_path, "w", **meta_out) as dst:
            dst.write(out_stack)
            for idx, name in enumerate(BAND_NAMES, start=1):
                dst.set_band_description(idx, name)

    print(f"[OK] temp tile: {region_name} | {acq_date} | {tile_code} -> {out_path.name}")
    return out_path


def process_safe_stage(safe_df, aoi_gdf, target_regions, temp_root, overwrite=False, temp_workers=1, cleanup_safe=False):
    temp_rows = []
    region_to_names = {
        region_name: set(safe_df.loc[safe_df["region"] == region_name, "name"].dropna().tolist())
        for region_name in target_regions
    }
    region_to_gdf = {
        region_name: aoi_gdf[aoi_gdf["ADM1_EN"] == region_name].copy()
        for region_name in target_regions
    }

    # ProcessPool is faster but more fragile. Use only when explicitly desired.
    # For script stability default workers=1.
    for region_name in target_regions:
        region = region_to_gdf[region_name]
        region_names = region_to_names[region_name]
        region_safe = safe_df[safe_df["name"].isin(region_names)].copy()
        if region_safe.empty:
            print(f"\n=== REGION {region_name} ===\nJumlah SAFE: 0")
            continue

        region_safe = region_safe.sort_values(["acq_date", "tile_code", "name"]).reset_index(drop=True)
        print(f"\n=== REGION {region_name} ===")
        print("Jumlah SAFE:", len(region_safe))
        out_dir = ensure_dir(Path(temp_root) / region_name.lower())

        # conservative serial mode
        if temp_workers <= 1:
            for _, row in region_safe.iterrows():
                try:
                    out_path = process_one_safe_to_temp_tile(
                        safe_dir=row["safe_dir"],
                        region_name=region_name,
                        region_gdf=region,
                        out_dir=out_dir,
                        overwrite=overwrite,
                        compress=False,
                    )
                except Exception as e:
                    print(f"[FAIL] {row['name']}: {e}")
                    out_path = None

                temp_rows.append({
                    "region": region_name,
                    "safe_name": row["name"],
                    "acq_date": row["acq_date"],
                    "tile_code": row["tile_code"],
                    "temp_tile": str(out_path) if out_path else None,
                })

                if cleanup_safe and out_path:
                    try:
                        shutil.rmtree(row["safe_dir"])
                        print(f"[CLEANUP] deleted SAFE: {row['safe_dir']}")
                    except Exception as e:
                        print(f"[WARN] cleanup SAFE gagal {row['safe_dir']}: {e}")
        else:
            # Threaded version avoids process-pool issues, but GDAL scaling is limited
            futures = {}
            with ThreadPoolExecutor(max_workers=temp_workers) as ex:
                for _, row in region_safe.iterrows():
                    fut = ex.submit(
                        process_one_safe_to_temp_tile,
                        row["safe_dir"], region_name, region, out_dir, overwrite, False
                    )
                    futures[fut] = row
                for fut in as_completed(futures):
                    row = futures[fut]
                    try:
                        out_path = fut.result()
                    except Exception as e:
                        print(f"[FAIL] {row['name']}: {e}")
                        out_path = None
                    temp_rows.append({
                        "region": region_name,
                        "safe_name": row["name"],
                        "acq_date": row["acq_date"],
                        "tile_code": row["tile_code"],
                        "temp_tile": str(out_path) if out_path else None,
                    })

    temp_tiles_df = pd.DataFrame(temp_rows)
    if not temp_tiles_df.empty:
        temp_tiles_df = temp_tiles_df[temp_tiles_df["temp_tile"].notna()].reset_index(drop=True)
        temp_tiles_df = temp_tiles_df.sort_values(["region", "acq_date", "tile_code", "safe_name"]).reset_index(drop=True)
    return temp_tiles_df


def build_region_grid_info(aoi_gdf, target_regions):
    info = {}
    for region_name in target_regions:
        region = aoi_gdf[aoi_gdf["ADM1_EN"] == region_name].copy()
        target_crs = region.estimate_utm_crs()
        region_proj = region.to_crs(target_crs)
        minx, miny, maxx, maxy = region_proj.total_bounds
        info[region_name] = {"target_crs": target_crs, "bounds": (minx, miny, maxx, maxy)}
    return info


def mosaic_daily_precomputed(region_name, acq_date, tile_paths, out_path, target_crs, bounds, res=10, overwrite=False, use_compression=False):
    out_path = Path(out_path)
    if out_path.exists() and not overwrite:
        return str(out_path)

    tile_paths = [Path(p) for p in tile_paths if p is not None and Path(p).exists()]
    if not tile_paths:
        print(f"[SKIP] no tiles for {region_name} {acq_date}")
        return None

    minx, miny, maxx, maxy = bounds
    vrt_list = []
    srcs = []

    try:
        for p in tile_paths:
            src = rasterio.open(p)
            srcs.append(src)
            vrt = WarpedVRT(
                src,
                crs=target_crs,
                resampling=Resampling.nearest,
                nodata=np.nan,
            )
            vrt_list.append(vrt)

        out_path.parent.mkdir(parents=True, exist_ok=True)
        mosaic_arr, mosaic_transform = merge(
            vrt_list,
            bounds=(minx, miny, maxx, maxy),
            res=(res, res),
            nodata=np.nan,
            method="first",
        )

        meta = vrt_list[0].meta.copy()
        meta.update({
            "driver": "GTiff",
            "height": mosaic_arr.shape[1],
            "width": mosaic_arr.shape[2],
            "transform": mosaic_transform,
            "crs": target_crs,
            "count": mosaic_arr.shape[0],
            "dtype": "float32",
            "tiled": True,
            "nodata": np.nan,
            "BIGTIFF": "IF_SAFER",
        })
        if use_compression:
            meta.update({"compress": "DEFLATE", "predictor": 2})

        with rasterio.open(out_path, "w", **meta) as dst:
            dst.write(mosaic_arr.astype(np.float32))
            for i, name in enumerate(BAND_NAMES, start=1):
                dst.set_band_description(i, name)

        return str(out_path)
    finally:
        for v in vrt_list:
            v.close()
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
    result = mosaic_daily_precomputed(
        region_name=region_name,
        acq_date=acq_date,
        tile_paths=tile_paths,
        out_path=out_path,
        target_crs=target_crs,
        bounds=bounds,
        res=res,
        overwrite=overwrite,
        use_compression=use_compression,
    )
    dt = time.time() - t0
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] DONE  {region_name} {acq_date} in {dt:.1f}s")
    return {
        "region": region_name,
        "acq_date": acq_date,
        "daily_path": str(result) if result else None,
        "n_tiles": len(tile_paths),
    }


def build_daily_mosaics(temp_tiles_df, target_regions, region_grid_info, daily_root, target_res=10, overwrite=False, daily_workers=2):
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
                "use_compression": False,  # keep intermediates fast
            })

    print(f"\nTOTAL DAILY TASKS = {len(tasks)}")
    daily_rows = []
    if not tasks:
        return pd.DataFrame()

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

def create_weekly_composite_from_daily_per_band(
    daily_paths,
    out_path,
    reducer="nanmean",          # "nanmean", "nanmedian", "lastvalid", "firstvalid"
    chunk_size=2048,
    overwrite=False,
    compress=False,
    keep_temp=False,
    temp_dir=None,
):
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

        # -----------------------------
        # validate all daily files
        # -----------------------------
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

        # -----------------------------
        # temp folder
        # -----------------------------
        if temp_dir is None:
            temp_dir = out_path.parent / f"{out_path.stem}_tempbands"
        temp_dir = Path(temp_dir)

        if temp_dir.exists() and overwrite:
            shutil.rmtree(temp_dir)

        temp_dir.mkdir(parents=True, exist_ok=True)

        print(
            f"[INFO] weekly per-band | reducer={reducer} | "
            f"n_daily={len(srcs)} | n_bands={n_bands} | shape=({ref.height}, {ref.width})"
        )

        # -----------------------------
        # metadata untuk single-band temp
        # -----------------------------
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
            band_meta.update({
                "compress": "DEFLATE",
                "predictor": 2,
            })

        # -----------------------------
        # helper reducer
        # -----------------------------
        def reduce_stack(stack, mode):
            # stack shape = (n_daily, h, w)
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

        # -----------------------------
        # STEP 1: build temp tif per band
        # -----------------------------
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
                            a = src.read(band_idx, window=window).astype(np.float32)

                            # amankan kalau nodata numeric, walau file Anda tampaknya sudah NaN
                            if src.nodata is not None and not np.isnan(src.nodata):
                                a[a == src.nodata] = np.nan

                            arrs.append(a)

                        stack = np.stack(arrs, axis=0)   # (n_daily, h, w)
                        comp = reduce_stack(stack, reducer)

                        dst_band.write(comp, 1, window=window)

                        done += 1
                        if done % 10 == 0 or done == total_chunks:
                            print(f"  band {band_idx} chunk {done}/{total_chunks}")

                dst_band.set_band_description(1, band_name)

            temp_band_paths.append(temp_band_path)
            print(f"[OK] temp band saved -> {temp_band_path}")

        # -----------------------------
        # STEP 2: merge temp single-band to multiband
        # -----------------------------
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
            final_meta.update({
                "compress": "DEFLATE",
                "predictor": 2,
            })

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

def create_weekly_composite_from_daily_full_memory(daily_paths, out_path, overwrite=False, compress=False):
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

        print(f"[INFO] weekly full-memory | n_daily={len(srcs)} | shape=({ref.count}, {ref.height}, {ref.width})")
        stack = np.stack([src.read().astype(np.float32) for src in srcs], axis=0)
        with np.errstate(all="ignore"):
            comp = np.nanmean(stack, axis=0).astype(np.float32)

        meta = ref.meta.copy()
        meta.update({
            "driver": "GTiff",
            "dtype": "float32",
            "tiled": True,
            "nodata": np.nan,
            "BIGTIFF": "IF_SAFER",
        })
        if compress:
            meta.update({"compress": "DEFLATE", "predictor": 2})

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(out_path, "w", **meta) as dst:
            dst.write(comp)
            for i, name in enumerate(BAND_NAMES, start=1):
                dst.set_band_description(i, name)

        print(f"[OK] weekly composite -> {out_path}")
        return out_path
    finally:
        for s in srcs:
            s.close()

def create_weekly_composite_from_daily_chunked(daily_paths, out_path, chunk_size=2048, overwrite=False, compress=False):
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

        meta = ref.meta.copy()
        meta.update({
            "driver": "GTiff",
            "dtype": "float32",
            "tiled": True,
            "nodata": np.nan,
            "BIGTIFF": "IF_SAFER",
        })
        if compress:
            meta.update({"compress": "DEFLATE", "predictor": 2})

        out_path.parent.mkdir(parents=True, exist_ok=True)
        n_rows = (ref.height + chunk_size - 1) // chunk_size
        n_cols = (ref.width + chunk_size - 1) // chunk_size
        total_chunks = n_rows * n_cols
        done = 0

        with rasterio.open(out_path, "w", **meta) as dst:
            for row_off in range(0, ref.height, chunk_size):
                h = min(chunk_size, ref.height - row_off)
                for col_off in range(0, ref.width, chunk_size):
                    w = min(chunk_size, ref.width - col_off)
                    window = Window(col_off, row_off, w, h)
                    stack = np.stack([src.read(window=window).astype(np.float32) for src in srcs], axis=0)
                    with np.errstate(all="ignore"):
                        comp = np.nanmean(stack, axis=0).astype(np.float32)
                    dst.write(comp, window=window)
                    done += 1
                    if done % 10 == 0 or done == total_chunks:
                        print(f"  chunk {done}/{total_chunks}")
            for i, name in enumerate(BAND_NAMES, start=1):
                dst.set_band_description(i, name)

        print(f"[OK] weekly composite -> {out_path}")
        return out_path
    finally:
        for s in srcs:
            s.close()


def build_weekly(daily_df, target_regions, weekly_root, date_from, date_to, weekly_mode="full", chunk_size=10000, overwrite=False, compress=False):
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
            reducer="nanmean",     # bisa ganti: nanmedian / lastvalid / firstvalid
            chunk_size=10000,
            overwrite=False,
            compress=False,
            keep_temp=False,
        )

        weekly_rows.append({
            "region": region_name,
            "weekly_path": str(result) if result else None,
            "n_daily": len(daily_paths),
        })

    weekly_df = pd.DataFrame(weekly_rows)
    weekly_df = weekly_df[weekly_df["weekly_path"].notna()].reset_index(drop=True)

    print("\n=== WEEKLY DF ===")
    print(weekly_df)

    if not weekly_df.empty:
        weekly_df = weekly_df[weekly_df["weekly_path"].notna()].reset_index(drop=True)
        
    return weekly_df



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
        # Expected weekly order:
        # B02 B03 B04 B05 B06 B07 B08 B8A B11 B12
        B02 = src.read(1).astype(np.float32)  # BLUE
        B03 = src.read(2).astype(np.float32)  # GREEN
        B04 = src.read(3).astype(np.float32)  # RED
        B05 = src.read(4).astype(np.float32)  # RE1
        B08 = src.read(7).astype(np.float32)  # NIR
        B8A = src.read(8).astype(np.float32)  # NIR narrow
        B11 = src.read(9).astype(np.float32)  # SWIR1

        ndvi = clip_array(safe_divide((B08 - B04), (B08 + B04)), -1, 1)
        evi  = clip_array(safe_divide(2.5 * (B08 - B04), (B08 + 6 * B04 - 7.5 * B02 + 1.0)), -1, 1)
        ndmi = clip_array(safe_divide((B08 - B11), (B08 + B11)), -1, 1)
        ndre = clip_array(safe_divide((B8A - B05), (B8A + B05)), -1, 1)
        msi  = clip_array(safe_divide(B11, B08), 0, 10)
        pvr  = clip_array(safe_divide((B03 - B02), (B03 + B02)), -1, 1)
        lai  = clip_array((3.618 * ndvi - 0.118).astype(np.float32), 0, 10)

        stack = np.stack([ndvi, evi, ndmi, ndre, msi, pvr, lai], axis=0).astype(np.float32)

        meta = src.meta.copy()
        meta.update({"count": 7, "dtype": "float32", "nodata": np.nan, "BIGTIFF": "IF_SAFER", "tiled": True})
        if compress:
            meta.update({"compress": "DEFLATE", "predictor": 2})

        out_path.parent.mkdir(parents=True, exist_ok=True)
        with rasterio.open(out_path, "w", **meta) as dst:
            dst.write(stack)
            dst.set_band_description(1, "NDVI")
            dst.set_band_description(2, "EVI")
            dst.set_band_description(3, "NDMI")
            dst.set_band_description(4, "NDRE")
            dst.set_band_description(5, "MSI")
            dst.set_band_description(6, "PVR")
            dst.set_band_description(7, "LAI")

    print(f"[OK] indices -> {out_path}")
    return out_path


def save_csv(df, path):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)
    print(f"[SAVE] {path}")


def main():
    args = parse_args()

    date_from_iso = iso_start(args.date_from)
    date_to_iso = iso_end(args.date_to)

    workdir = ensure_dir(args.workdir)
    raw_root = ensure_dir(workdir / "downloads")
    temp_root = ensure_dir(workdir / "temp_tiles")
    daily_root = ensure_dir(workdir / "daily")
    weekly_root = ensure_dir(workdir / "weekly")
    indices_root = ensure_dir(workdir / "indices")
    logs_root = ensure_dir(workdir / "logs")

    print("=== LOAD SHAPEFILE ===")
    aoi_gdf = load_regions(args.shp_path, args.regions)
    print(aoi_gdf[["ADM1_EN", "ADM1_PCODE"]])

    # STEP 1: query scenes
    if args.skip_query_download:
        scenes = pd.DataFrame()
        print("[SKIP] query/download stage")
    else:
        dfs = []
        for region_name in args.regions:
            region = aoi_gdf[aoi_gdf["ADM1_EN"] == region_name].copy()
            df_region = fetch_products_for_region(region_name, region, date_from_iso, date_to_iso, top_n=args.top_n)
            dfs.append(df_region)

        scenes = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
        if not scenes.empty:
            scenes = scenes.drop_duplicates(subset=["id"]).reset_index(drop=True)
            scenes = scenes.sort_values(["content_start", "region", "name"]).reset_index(drop=True)
            save_csv(scenes, logs_root / f"scenes_{args.date_from}_{args.date_to}.csv")

        # STEP 2: download
        if args.skip_query_download:
            downloaded_df = scan_existing_zips(raw_root, scenes=scenes)

            if downloaded_df.empty:
                raise RuntimeError("--skip-query-download dipakai tetapi tidak ada zip existing yang ditemukan.")

            downloaded_df["zip_exists"] = downloaded_df["zip_path"].apply(
                lambda p: bool(p is not None and Path(p).exists() and Path(p).stat().st_size > 0)
            )
            downloaded_df = downloaded_df[downloaded_df["zip_exists"]].reset_index(drop=True)

            expected_names = set(scenes["name"].tolist()) if scenes is not None and not scenes.empty else set()
            got_names = set(downloaded_df["name"].tolist())
            missing = sorted(expected_names - got_names)

            if missing:
                raise RuntimeError(
                    f"--skip-query-download dipakai, tetapi zip belum lengkap. "
                    f"Jumlah missing={len(missing)} | contoh={missing[:10]}"
                )

        else:
            if not args.username or not args.password:
                raise ValueError(
                    "CDSE username/password belum diisi. "
                    "Pakai --username --password atau env vars CDSE_USERNAME/CDSE_PASSWORD"
                )

            downloaded_df = download_all_products_parallel(
                scenes=scenes,
                raw_root=raw_root,
                username=args.username,
                password=args.password,
                overwrite=args.overwrite,
                download_workers=args.download_workers,
                max_rounds=args.download_max_rounds,
                sleep_between_rounds=args.download_round_sleep,
            )

            save_csv(downloaded_df, logs_root / f"download_results_{args.date_from}_{args.date_to}.csv")

        if downloaded_df.empty:
            raise RuntimeError("Tidak ada zip valid untuk diproses")

    # STEP 3: unzip or scan SAFE
    if args.skip_unzip:
        safe_df = scan_existing_safe(raw_root, scenes=scenes)
    else:
        safe_df = unzip_archives(downloaded_df, overwrite=args.overwrite, cleanup_zips=args.cleanup_zips)

    if safe_df.empty:
        raise RuntimeError("Tidak ada SAFE ditemukan")

    # strict check: semua zip valid harus menghasilkan SAFE
    expected_safe_names = set(downloaded_df["name"].tolist())
    got_safe_names = set(safe_df["name"].tolist())
    missing_safe = sorted(expected_safe_names - got_safe_names)

    if missing_safe:
        raise RuntimeError(
            f"Tidak semua zip berhasil diekstrak menjadi SAFE. "
            f"Jumlah missing SAFE={len(missing_safe)} | contoh={missing_safe[:10]}"
        )

    if "region" in safe_df.columns and safe_df["region"].isna().any() and not scenes.empty:
        name_to_region = dict(zip(scenes["name"], scenes["region"]))
        safe_df["region"] = safe_df["name"].map(name_to_region)

    save_csv(safe_df, logs_root / f"safe_df_{args.date_from}_{args.date_to}.csv")

    # STEP 4: filter SAFE based on selected products per region
    if not scenes.empty:
        safe_selected = safe_df[safe_df["name"].isin(set(scenes["name"].tolist()))].copy()
    else:
        safe_selected = safe_df.copy()

    save_csv(safe_selected, logs_root / f"safe_selected_{args.date_from}_{args.date_to}.csv")

    # STEP 5: SAFE -> temp tiles
    t0 = time.time()
    temp_tiles_df = process_safe_stage(
        safe_selected,
        aoi_gdf,
        args.regions,
        temp_root=temp_root,
        overwrite=args.overwrite,
        temp_workers=args.temp_workers,
        cleanup_safe=args.cleanup_safe,
    )
    print(f"TEMP TILE TIME: {time.time() - t0:.1f}s")
    if temp_tiles_df.empty:
        raise RuntimeError("Tidak ada temp tile dihasilkan")
    save_csv(temp_tiles_df, logs_root / f"temp_tiles_{args.date_from}_{args.date_to}.csv")

    # STEP 6: daily mosaics
    region_grid_info = build_region_grid_info(aoi_gdf, args.regions)
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
    print(f"DAILY TIME: {time.time() - t0:.1f}s")
    if daily_df.empty:
        raise RuntimeError("Tidak ada daily mosaic dihasilkan")
    save_csv(daily_df, logs_root / f"daily_{args.date_from}_{args.date_to}.csv")

    # STEP 7: weekly composites
    t0 = time.time()
    weekly_df = build_weekly(
        daily_df,
        args.regions,
        weekly_root=weekly_root,
        date_from=args.date_from,
        date_to=args.date_to,
        chunk_size=args.weekly_chunk_size,
        overwrite=args.overwrite,
        compress=args.compress_final,
    )
    print(f"WEEKLY TIME: {time.time() - t0:.1f}s")
    if weekly_df.empty:
        raise RuntimeError("Tidak ada weekly composite dihasilkan")
    save_csv(weekly_df, logs_root / f"weekly_{args.date_from}_{args.date_to}.csv")

    # STEP 8: indices
    indices_rows = []
    for _, row in weekly_df.iterrows():
        region_name = row["region"]
        weekly_path = row["weekly_path"]
        if not weekly_path:
            continue
        out_path = Path(indices_root) / f"{region_name.lower()}_{args.date_from}_{args.date_to}_weekly_indices_10m.tif"
        result = compute_indices_from_weekly(
            weekly_path=weekly_path,
            out_path=out_path,
            overwrite=args.overwrite,
            compress=args.compress_final,
        )
        indices_rows.append({
            "region": region_name,
            "indices_path": str(result) if result else None,
        })

    indices_df = pd.DataFrame(indices_rows)
    if not indices_df.empty:
        indices_df = indices_df[indices_df["indices_path"].notna()].reset_index(drop=True)
    save_csv(indices_df, logs_root / f"indices_{args.date_from}_{args.date_to}.csv")

    # Summary / manifest
    summary = {
        "date_from": args.date_from,
        "date_to": args.date_to,
        "regions": args.regions,
        "workdir": str(workdir),
        "n_scenes": int(len(scenes)) if scenes is not None else 0,
        "n_safe": int(len(safe_df)),
        "n_temp_tiles": int(len(temp_tiles_df)),
        "n_daily": int(len(daily_df)),
        "n_weekly": int(len(weekly_df)),
        "n_indices": int(len(indices_df)),
        "weekly_mode": args.weekly_mode,
    }
    print("\n=== SUMMARY ===")
    print(json.dumps(summary, indent=2))

    if args.save_manifest_json:
        manifest_path = logs_root / f"manifest_{args.date_from}_{args.date_to}.json"
        manifest_path.write_text(json.dumps(summary, indent=2))
        print(f"[SAVE] {manifest_path}")

    print("\nDONE.")


if __name__ == "__main__":
    main()

