#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Operational Sentinel-2 weekly pipeline.

Perubahan utama versi ini:
1) Parallel week queue berbasis ISO week
2) Final outputs dipisahkan dari scratch/intermediate
3) Scratch per-week di runs/<year>_W<week>_<date_from>_<date_to>
4) Optional cleanup scratch setelah weekly + indices sukses
5) STRICT download per week
6) Global cross-week download session limiter
7) Shared token manager per account dengan refresh ter-serialisasi
8) Resume-friendly: skip week jika final outputs sudah ada
9) Week-level download queue: hanya N week boleh masuk fase download bersamaan
10) Query dibagi menjadi 4 bagian (quadrants) per region untuk memperkecil hasil per query
11) Multi-account download: 4 akun dipakai bergiliran/parallel-safe untuk multi session
12) Output final dicek dari existing-final-root, sementara hasil final baru ditulis ke target-final-root

Catatan desain:
- existing-final-root: lokasi final lama / sumber pengecekan skip
- target-final-root: lokasi final baru hasil pipeline ini
- jika existing-final-root tidak diisi, fallback ke target-final-root
- untuk mengurangi bentrok sesi CDSE, default global-download-slots dan download-workers sebaiknya tetap konservatif
"""

import argparse
import json
import os
import random
import shutil
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
from rasterio.features import geometry_mask, geometry_window
from rasterio.merge import merge
from rasterio.vrt import WarpedVRT
from rasterio.windows import Window

# =========================
# DEFAULTS / CONFIG
# =========================
CATALOG_URL = "https://catalogue.dataspace.copernicus.eu/odata/v1/Products"
TOKEN_URL = "https://identity.dataspace.copernicus.eu/auth/realms/CDSE/protocol/openid-connect/token"
DOWNLOAD_PRODUCTS_URL = "https://download.dataspace.copernicus.eu/odata/v1/Products"

DEFAULT_REGIONS = ["Nakuru", "Machakos"]
TARGET_BANDS = ["B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12"]
BAND_NAMES = TARGET_BANDS.copy()
VALID_SCL = {4, 5, 6, 7}
DEFAULT_USERNAMES = [
    "Sentinel1@geo-infinity.com",
    "Sentinel2@geo-infinity.com",
    "Sentinel3@geo-infinity.com",
    "Sentinel4@geo-infinity.com",
]

# global objects initialized in main()
GLOBAL_DOWNLOAD_SEMAPHORE = None
GLOBAL_WEEK_DOWNLOAD_SEMAPHORE = None
GLOBAL_ACCOUNT_POOL = None

# cooldown shared across all threads when auth/rate-limit happens
GLOBAL_AUTH_COOLDOWN_UNTIL = 0.0
GLOBAL_AUTH_COOLDOWN_LOCK = threading.Lock()


# =========================
# ARGUMENTS
# =========================
def parse_args():
    p = argparse.ArgumentParser(description="Sentinel-2 weekly operational pipeline")

    # single-week mode
    p.add_argument("--date-from", default=None, help="YYYY-MM-DD")
    p.add_argument("--date-to", default=None, help="YYYY-MM-DD")

    # backfill queue by ISO week number
    p.add_argument("--run-mode", choices=["single_week", "backfill_weeknum"], default="single_week")
    p.add_argument("--start-year", type=int, default=None, help="ISO start year for backfill queue")
    p.add_argument("--start-week", type=int, default=None, help="ISO start week for backfill queue")
    p.add_argument("--end-year", type=int, default=None, help="ISO end year for backfill queue")
    p.add_argument("--end-week", type=int, default=None, help="ISO end week for backfill queue")
    p.add_argument("--max-week-workers", type=int, default=3, help="How many weeks run in parallel")
    p.add_argument("--week-order", choices=["asc", "desc"], default="desc", help="desc = newest week first")

    p.add_argument("--shp-path", required=True, help="Path to Kenya ADM1 shapefile")
    p.add_argument("--regions", nargs="+", default=DEFAULT_REGIONS, help="ADM1_EN region names")

    # scratch and final storage
    p.add_argument("--workdir", required=True, help="Base working directory")
    p.add_argument(
        "--target-final-root",
        default=None,
        help="Lokasi output final baru. Default: <workdir>/final_target",
    )
    p.add_argument(
        "--existing-final-root",
        default=None,
        help="Lokasi final lama untuk pengecekan skip. Default: pakai target-final-root",
    )
    p.add_argument(
        "--delete-intermediate-on-success",
        action="store_true",
        help="Delete scratch run dir after weekly+indices succeed",
    )

    # auth
    p.add_argument(
        "--usernames",
        nargs="+",
        default=DEFAULT_USERNAMES,
        help="Daftar username CDSE multi account",
    )
    p.add_argument("--password", default=os.getenv("CDSE_PASSWORD"), help="CDSE password (semua akun sama)")

    # pipeline tuning
    p.add_argument("--top-n", type=int, default=100, help="Max OData rows per region-subquery")
    p.add_argument("--target-res", type=float, default=10.0, help="Target resolution in meters")

    p.add_argument("--download-workers", type=int, default=4, help="Parallel download workers per week")
    p.add_argument("--global-download-slots", type=int, default=4, help="Global active CDSE download sessions across ALL weeks")
    p.add_argument(
        "--max-weeks-in-download",
        type=int,
        default=1,
        help="How many weeks may be in download phase at the same time.",
    )
    p.add_argument("--download-max-retries-per-file", type=int, default=8, help="Retries per file attempt")
    p.add_argument("--download-max-rounds", type=int, default=30, help="Strict download rounds per week")
    p.add_argument("--download-round-sleep", type=int, default=90, help="Sleep between strict retry rounds (sec)")
    p.add_argument("--download-backoff-base", type=float, default=12.0, help="Base seconds for download backoff")
    p.add_argument("--download-backoff-max", type=float, default=240.0, help="Max seconds for download backoff")

    p.add_argument("--temp-workers", type=int, default=1, help="SAFE -> temp tile workers per week")
    p.add_argument("--daily-workers", type=int, default=1, help="Daily mosaic workers per week")
    p.add_argument("--weekly-reducer", choices=["nanmean", "nanmedian", "lastvalid", "firstvalid"], default="nanmean")
    p.add_argument("--weekly-chunk-size", type=int, default=10000, help="Chunk size in pixels for weekly per-band composite")

    # behavior
    p.add_argument("--overwrite", action="store_true", help="Overwrite existing outputs")
    p.add_argument("--skip-query-download", action="store_true", help="Skip query+download and reuse existing raw zips / SAFE")
    p.add_argument("--skip-unzip", action="store_true", help="Skip unzip and reuse existing SAFE")
    p.add_argument("--compress-final", action="store_true", help="Compress weekly / indices outputs")
    p.add_argument("--cleanup-zips", action="store_true", help="Delete zip after successful extract")
    p.add_argument("--cleanup-safe", action="store_true", help="Delete SAFE folders after successful temp tile creation")
    p.add_argument("--save-manifest-json", action="store_true", help="Save final manifest json")
    return p.parse_args()


# =========================
# GENERIC HELPERS
# =========================
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


# =========================
# ISO WEEK QUEUE HELPERS
# =========================
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


# =========================
# FINAL OUTPUT HELPERS
# =========================
def final_outputs_exist(final_root, regions, date_from, date_to):
    final_root = Path(final_root)
    weekly_root = final_root / "weekly"
    indices_root = final_root / "indices"
    for region_name in regions:
        weekly_path = weekly_root / f"{region_name.lower()}_{date_from}_{date_to}_weekly_clean_10m.tif"
        indices_path = indices_root / f"{region_name.lower()}_{date_from}_{date_to}_weekly_indices_10m.tif"
        if not weekly_path.exists() or not indices_path.exists():
            return False
    return True


# =========================
# QUERY / SCENE HELPERS
# =========================
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
    bounds = tuple(region_gdf.total_bounds)
    sub_bounds_list = split_bounds_into_four(bounds)
    dfs = []

    print(f"\n=== QUERY REGION: {region_name} ===")
    print("Full bounds:", bounds)
    print("Subqueries:", len(sub_bounds_list))

    for idx, sb in enumerate(sub_bounds_list, start=1):
        bbox_wkt = build_bbox_wkt_from_bounds(*sb)
        url = build_odata_url(bbox_wkt, date_from, date_to, top_n=top_n)
        print(f"[QUERY-{region_name}-{idx}/4] bounds={sb}")
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
                "query_part": idx,
            })
        dfs.append(pd.DataFrame(rows))

    if not dfs:
        return pd.DataFrame()
    out = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    if not out.empty:
        out = out.drop_duplicates(subset=["id"]).reset_index(drop=True)
    return out


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


# =========================
# AUTH / DOWNLOAD
# =========================
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

    expected_names = set(scenes["name"].tolist())
    remaining_df = scenes.copy().reset_index(drop=True)
    success_map = {}

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
                region=row_dict["region"],
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
            success_map[row["name"]] = row.to_dict()

        print(
            f"[ROUND {round_no}] success_valid={len(success_df)} | "
            f"failed={len(fail_df)} | total_success_accum={len(success_map)}"
        )

        if len(success_map) == len(expected_names):
            print(f"[DOWNLOAD] strict success completed in round {round_no}")
            break

        failed_names = sorted(expected_names - set(success_map.keys()))
        remaining_df = scenes[scenes["name"].isin(failed_names)].copy().reset_index(drop=True)

        if round_no < max_rounds and not remaining_df.empty:
            print(f"[DOWNLOAD] sleep {sleep_between_rounds}s before next round...")
            time.sleep(sleep_between_rounds)

    final_df = pd.DataFrame(list(success_map.values()))
    if final_df.empty:
        raise RuntimeError("Tidak ada file yang berhasil di-download.")

    final_df = final_df.drop_duplicates(subset=["name"], keep="last").reset_index(drop=True)
    got_names = set(final_df["name"].tolist())
    missing = sorted(expected_names - got_names)

    if missing:
        raise RuntimeError(
            f"STRICT download gagal. Masih ada {len(missing)} file belum terdownload. "
            f"Contoh: {missing[:10]}"
        )

    final_df = final_df.sort_values(["region", "acq_date", "name"]).reset_index(drop=True)
    print(f"\n[DOWNLOAD] STRICT SUCCESS | expected={len(expected_names)} | got={len(final_df)}")
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


# =========================
# TEMP TILE STAGE
# =========================
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
        region_mask = geometry_mask(geoms, transform=ref_transform, invert=True, out_shape=(height, width))
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
            meta_out.update({"compress": "DEFLATE", "predictor": 2})

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
            futures = {}
            with ThreadPoolExecutor(max_workers=temp_workers) as ex:
                for _, row in region_safe.iterrows():
                    fut = ex.submit(process_one_safe_to_temp_tile, row["safe_dir"], region_name, region, out_dir, overwrite, False)
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


# =========================
# DAILY MOSAICS
# =========================
def build_region_grid_info(aoi_gdf, target_regions):
    info = {}
    for region_name in target_regions:
        region = aoi_gdf[aoi_gdf["ADM1_EN"] == region_name].copy()
        target_crs = region.estimate_utm_crs()
        region_proj = region.to_crs(target_crs)
        minx, miny, maxx, maxy = region_proj.total_bounds
        info[region_name] = {"target_crs": target_crs, "bounds": (minx, miny, maxx, maxy)}
    return info


def mosaic_daily_precomputed(region_name, acq_date, tile_paths, out_path, target_crs, bounds,
                             res=10, overwrite=False, use_compression=False):
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
            vrt = WarpedVRT(src, crs=target_crs, resampling=Resampling.nearest, nodata=np.nan)
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


# =========================
# WEEKLY COMPOSITE
# =========================
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
                            a = src.read(band_idx, window=window).astype(np.float32)
                            if src.nodata is not None and not np.isnan(src.nodata):
                                a[a == src.nodata] = np.nan
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


# =========================
# INDICES
# =========================
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
        B02 = src.read(1).astype(np.float32)
        B03 = src.read(2).astype(np.float32)
        B04 = src.read(3).astype(np.float32)
        B05 = src.read(4).astype(np.float32)
        B08 = src.read(7).astype(np.float32)
        B8A = src.read(8).astype(np.float32)
        B11 = src.read(9).astype(np.float32)

        ndvi = clip_array(safe_divide((B08 - B04), (B08 + B04)), -1, 1)
        evi = clip_array(safe_divide(2.5 * (B08 - B04), (B08 + 6 * B04 - 7.5 * B02 + 1.0)), -1, 1)
        ndmi = clip_array(safe_divide((B08 - B11), (B08 + B11)), -1, 1)
        ndre = clip_array(safe_divide((B8A - B05), (B8A + B05)), -1, 1)
        msi = clip_array(safe_divide(B11, B08), 0, 10)
        pvr = clip_array(safe_divide((B03 - B02), (B03 + B02)), -1, 1)
        lai = clip_array((3.618 * ndvi - 0.118).astype(np.float32), 0, 10)

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


# =========================
# SINGLE WEEK EXECUTION
# =========================
def run_one_week(args):
    global GLOBAL_WEEK_DOWNLOAD_SEMAPHORE

    if not args.date_from or not args.date_to:
        raise ValueError("date-from dan date-to wajib untuk single_week")

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

    print("=== LOAD SHAPEFILE ===")
    aoi_gdf = load_regions(args.shp_path, args.regions)
    print(aoi_gdf[["ADM1_EN", "ADM1_PCODE"]])

    if final_outputs_exist(existing_final_root, args.regions, args.date_from, args.date_to) and not args.overwrite:
        print(f"[SKIP] final outputs already exist for {args.date_from} -> {args.date_to} at {existing_final_root}")
        return {
            "date_from": args.date_from,
            "date_to": args.date_to,
            "status": "skipped_exists",
        }

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

    # STEP 2: download STRICT
    if args.skip_query_download:
        downloaded_df = scan_existing_zips(raw_root, scenes=scenes)
        if downloaded_df.empty:
            raise RuntimeError("Skip query/download aktif tetapi tidak ada ZIP existing.")
        downloaded_df["zip_exists"] = downloaded_df["zip_path"].apply(
            lambda p: bool(p is not None and Path(p).exists() and Path(p).stat().st_size > 0)
        )
        downloaded_df = downloaded_df[downloaded_df["zip_exists"]].copy().reset_index(drop=True)

        if scenes is not None and not scenes.empty:
            expected = set(scenes["name"].tolist())
            got = set(downloaded_df["name"].tolist())
            missing = sorted(expected - got)
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

    if downloaded_df.empty and not args.skip_unzip:
        raise RuntimeError("Tidak ada zip untuk diproses")

    # STEP 3: unzip or scan SAFE
    if args.skip_unzip:
        safe_df = scan_existing_safe(raw_root, scenes=scenes)
    else:
        safe_df = unzip_archives(downloaded_df, overwrite=args.overwrite, cleanup_zips=args.cleanup_zips)

    if safe_df.empty:
        raise RuntimeError("Tidak ada SAFE ditemukan")

    expected_safe = set(downloaded_df["name"].tolist())
    got_safe = set(safe_df["name"].tolist())
    missing_safe = sorted(expected_safe - got_safe)
    if missing_safe:
        raise RuntimeError(
            f"Tidak semua ZIP berhasil diekstrak menjadi SAFE. "
            f"Missing SAFE={len(missing_safe)} | contoh={missing_safe[:10]}"
        )

    if "region" in safe_df.columns and safe_df["region"].isna().any() and not scenes.empty:
        name_to_region = dict(zip(scenes["name"], scenes["region"]))
        safe_df["region"] = safe_df["name"].map(name_to_region)
    save_csv(safe_df, logs_root / f"safe_df_{args.date_from}_{args.date_to}.csv")

    # STEP 4: filter SAFE
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

    # STEP 7: weekly -> TARGET FINAL ROOT
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
    print(f"WEEKLY TIME: {time.time() - t0:.1f}s")
    if weekly_df.empty:
        raise RuntimeError("Tidak ada weekly composite dihasilkan")
    save_csv(weekly_df, logs_root / f"weekly_{args.date_from}_{args.date_to}.csv")

    # STEP 8: indices -> TARGET FINAL ROOT
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

    summary = {
        "date_from": args.date_from,
        "date_to": args.date_to,
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
        "n_indices": int(len(indices_df)),
        "weekly_reducer": args.weekly_reducer,
        "n_accounts": GLOBAL_ACCOUNT_POOL.size if GLOBAL_ACCOUNT_POOL else 0,
    }
    print("\n=== SUMMARY ===")
    print(json.dumps(summary, indent=2))

    if args.save_manifest_json:
        manifest_path = logs_root / f"manifest_{args.date_from}_{args.date_to}.json"
        manifest_path.write_text(json.dumps(summary, indent=2))
        print(f"[SAVE] {manifest_path}")

    if args.delete_intermediate_on_success and final_outputs_exist(target_final_root, args.regions, args.date_from, args.date_to):
        cleanup_dir(base_workdir)

    print("\nDONE.")
    return summary


# =========================
# BACKFILL QUEUE BY WEEK NUMBER
# =========================
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
    existing_final_root = Path(args.existing_final_root) if args.existing_final_root else (Path(args.target_final_root) if args.target_final_root else (base_workdir / "final_target"))

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

        week_start, week_end = iso_week_to_date_range(year, week)
        date_from = week_start.strftime("%Y-%m-%d")
        date_to = week_end.strftime("%Y-%m-%d")
        run_dir = base_workdir / "runs" / f"{year}_W{week:02d}_{date_from}_{date_to}"

        if final_outputs_exist(existing_final_root, args.regions, date_from, date_to) and not args.overwrite:
            print(f"[SKIP] final exists | queue={queue_idx} | {year} W{week:02d} | {date_from} -> {date_to}")
            return {
                "queue_idx": queue_idx,
                "year": year,
                "week": week,
                "date_from": date_from,
                "date_to": date_to,
                "status": "skipped_exists",
            }

        week_args = argparse.Namespace(**vars(args))
        week_args.date_from = date_from
        week_args.date_to = date_to
        week_args.workdir = str(run_dir)

        try:
            print(f"[QUEUE] START | queue={queue_idx} | {year} W{week:02d} | {date_from} -> {date_to}")
            run_one_week(week_args)
            return {
                "queue_idx": queue_idx,
                "year": year,
                "week": week,
                "date_from": date_from,
                "date_to": date_to,
                "status": "ok",
            }
        except Exception as e:
            print(f"[FAIL] queue={queue_idx} | {year} W{week:02d} | {date_from} -> {date_to} | {e}")
            return {
                "queue_idx": queue_idx,
                "year": year,
                "week": week,
                "date_from": date_from,
                "date_to": date_to,
                "status": f"fail: {e}",
            }

    results = []
    with ThreadPoolExecutor(max_workers=max(1, args.max_week_workers)) as ex:
        futures = [ex.submit(_run_week_item, item) for item in queue_items]
        for fut in as_completed(futures):
            res = fut.result()
            results.append(res)
            print(f"[QUEUE] DONE | queue={res['queue_idx']} | {res['year']} W{res['week']:02d} | {res['status']}")

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


# =========================
# ENTRYPOINT
# =========================
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

        GLOBAL_DOWNLOAD_SEMAPHORE = threading.BoundedSemaphore(
            max(1, args.global_download_slots)
        )

        GLOBAL_WEEK_DOWNLOAD_SEMAPHORE = threading.BoundedSemaphore(
            max(1, args.max_weeks_in_download)
        )

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
