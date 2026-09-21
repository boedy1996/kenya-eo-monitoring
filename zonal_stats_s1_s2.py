#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import glob
import argparse
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pandas as pd
import geopandas as gpd
import rasterio
from shapely.geometry import box
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
import fiona

try:
    from exactextract import exact_extract
except ImportError:
    print("ERROR: package 'exactextract' belum terinstall. Install dulu: pip install exactextract")
    sys.exit(1)


S1_BAND_CANDIDATES = {
    1: ["sigma_vv_linear", "vv", "band1"],
    2: ["sigma_vh_linear", "vh", "band2"],
    3: ["p_ratio", "pratio", "band3"],
    4: ["rvi", "band4"],
    5: ["rcspr", "band5"],
}

S2_BAND_CANDIDATES = {
    1: ["ndvi", "band1"],
    2: ["evi", "band2"],
    3: ["ndmi", "band3"],
    4: ["ndre", "band4"],
    5: ["msi", "band5"],
    6: ["pvr", "band6"],
    7: ["lai", "band7"],
}

S1_CANONICAL = {
    1: "sigma_vv_linear",
    2: "sigma_vh_linear",
    3: "p_ratio",
    4: "RVI",
    5: "RCSPR",
}

S2_CANONICAL = {
    1: "NDVI",
    2: "EVI",
    3: "NDMI",
    4: "NDRE",
    5: "MSI",
    6: "PVR",
    7: "LAI",
}

DEFAULT_STATS = ["mean", "median", "min", "max", "stdev", "count"]


def log(msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


def safe_mkdir(path: str) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def parse_args():
    p = argparse.ArgumentParser(
        description="Hitung zonal statistics wide-format untuk raster S1/S2 dari GeoPackage menggunakan exactextract."
    )

    p.add_argument("--gpkg-path", required=True, help="Path GeoPackage")
    p.add_argument("--uid-field", required=True, help="Nama field unique id pada layer polygon")

    p.add_argument("--s1-input-dir", help="Folder raster S1")
    p.add_argument("--s1-output-csv", help="Output CSV final S1")
    p.add_argument("--s1-output-parquet", help="Output Parquet final S1")
    p.add_argument("--s1-parts-dir", help="Folder parquet part S1")

    p.add_argument("--s2-input-dir", help="Folder raster S2")
    p.add_argument("--s2-output-csv", help="Output CSV final S2")
    p.add_argument("--s2-output-parquet", help="Output Parquet final S2")
    p.add_argument("--s2-parts-dir", help="Folder parquet part S2")

    p.add_argument("--layers", nargs="*", default=None,
                   help="Nama layer yang dipakai. Default: autodetect semua layer di gpkg")
    p.add_argument("--overwrite", action="store_true",
                   help="Overwrite final outputs dan overwrite parquet parts")
    p.add_argument("--resume", action="store_true",
                   help="Skip parquet parts yang sudah ada")
    p.add_argument("--raster-glob", default="*.tif",
                   help="Pattern raster, default *.tif")
    p.add_argument("--workers", type=int, default=1,
                   help="Jumlah worker paralel per raster. Default 1")
    p.add_argument("--skip-csv", action="store_true",
                   help="Skip export CSV final, hanya simpan parquet")
    p.add_argument("--check-crs", action="store_true",
                   help="Cek CRS vector vs raster secara ketat")

    return p.parse_args()


def list_gpkg_layers(gpkg_path: str) -> List[str]:
    return list(fiona.listlayers(gpkg_path))


def load_vector_layer(gpkg_path: str, layer_name: str, uid_field: str) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(gpkg_path, layer=layer_name)

    if uid_field not in gdf.columns:
        raise ValueError(f"Field UID '{uid_field}' tidak ditemukan di layer '{layer_name}'")

    gdf = gdf[[uid_field, "geometry"]].copy()
    gdf = gdf[~gdf.geometry.isna()].copy()
    gdf = gdf[~gdf.geometry.is_empty].copy()
    gdf = gdf[gdf.geometry.is_valid].copy()
    gdf = gdf.reset_index(drop=True)

    return gdf


def infer_region_from_filename(filename: str) -> Optional[str]:
    name = os.path.basename(filename).lower()
    if name.startswith("nakuru_"):
        return "nakuru"
    if name.startswith("machakos_"):
        return "machakos"
    return None


def choose_layer_for_region(region: str, available_layers: List[str]) -> Optional[str]:
    region = region.lower()
    candidates = [lyr for lyr in available_layers if region in lyr.lower()]
    if not candidates:
        return None

    for preferred in [f"{region}_fin", f"final2_{region}_fin", f"{region}_joined_final"]:
        for lyr in candidates:
            if lyr.lower() == preferred.lower():
                return lyr

    return candidates[0]


def parse_dates_from_filename(filename: str) -> Tuple[Optional[str], Optional[str], Optional[int], Optional[int]]:
    base = os.path.basename(filename)
    m = re.search(r'(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})', base)
    if not m:
        return None, None, None, None

    start_date = m.group(1)
    end_date = m.group(2)

    try:
        dt = datetime.strptime(start_date, "%Y-%m-%d")
        iso = dt.isocalendar()
        year = int(iso.year)
        week_no = int(iso.week)
    except Exception:
        year = None
        week_no = None

    return start_date, end_date, year, week_no


def build_band_name_map(src: rasterio.io.DatasetReader) -> Dict[int, str]:
    band_map = {}
    descriptions = src.descriptions

    for i in range(1, src.count + 1):
        desc = descriptions[i - 1] if descriptions and len(descriptions) >= i else None
        if desc is None or str(desc).strip() == "":
            band_map[i] = f"band{i}"
        else:
            band_map[i] = str(desc).strip()
    return band_map


def normalize_name(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(s).lower())


def resolve_index_names(band_map: Dict[int, str], source_type: str) -> Dict[int, str]:
    result = {}

    if source_type == "s1":
        candidates = S1_BAND_CANDIDATES
        canonical = S1_CANONICAL
    elif source_type == "s2":
        candidates = S2_BAND_CANDIDATES
        canonical = S2_CANONICAL
    else:
        raise ValueError(f"source_type tidak dikenal: {source_type}")

    for band_idx, raw_name in band_map.items():
        raw_norm = normalize_name(raw_name)
        chosen = None

        for expected_band_idx, aliases in candidates.items():
            for alias in aliases:
                if raw_norm == normalize_name(alias):
                    chosen = canonical[expected_band_idx]
                    break
            if chosen:
                break

        if chosen is None:
            if band_idx in canonical:
                chosen = canonical[band_idx]
            else:
                chosen = raw_name

        result[band_idx] = chosen

    return result


def sanitize_part_filename(filename: str) -> str:
    name = os.path.splitext(os.path.basename(filename))[0]
    name = re.sub(r"[^A-Za-z0-9_\-]+", "_", name)
    return name + ".parquet"


def raster_candidates(input_dir: str, raster_glob: str) -> List[str]:
    return sorted(glob.glob(os.path.join(input_dir, raster_glob)))


def write_part_parquet(df: pd.DataFrame, out_path: str) -> None:
    safe_mkdir(os.path.dirname(out_path))
    df.to_parquet(out_path, index=False)


def subset_gdf_by_raster_extent(gdf: gpd.GeoDataFrame, raster_path: str) -> gpd.GeoDataFrame:
    with rasterio.open(raster_path) as src:
        if src.crs is None:
            raise ValueError(f"CRS raster kosong: {raster_path}")
        if gdf.crs is None:
            raise ValueError("CRS vector kosong")

        if gdf.crs != src.crs:
            gdf = gdf.to_crs(src.crs)

        bounds = src.bounds
        raster_bbox = box(bounds.left, bounds.bottom, bounds.right, bounds.top)

    hits = gdf.geometry.intersects(raster_bbox)
    gdf2 = gdf.loc[hits].copy().reset_index(drop=True)
    return gdf2


def build_expected_output_columns(index_name_map: Dict[int, str]) -> List[str]:
    cols = []
    for band_idx in sorted(index_name_map.keys()):
        idx_name = index_name_map[band_idx]
        for stat in ["mean", "median", "min", "max", "std", "count"]:
            cols.append(f"{idx_name}_{stat}")
    return cols


def rename_exactextract_columns(stats_df: pd.DataFrame, index_name_map: Dict[int, str]) -> pd.DataFrame:
    """
    exactextract output untuk multiband biasanya akan menghasilkan kolom seperti:
    band_1_mean, band_1_median, ..., band_2_mean, dst
    atau kadang b1_mean, b2_mean, dst

    Fungsi ini me-rename ke:
    sigma_vv_linear_mean, sigma_vv_linear_median, ...
    """
    rename_map = {}

    for band_idx, idx_name in index_name_map.items():
        possible_prefixes = [
            f"band_{band_idx}_",
            f"band{band_idx}_",
            f"b{band_idx}_",
        ]

        for stat in DEFAULT_STATS:
            stat_out = "std" if stat == "stdev" else stat
            for pref in possible_prefixes:
                src_col = f"{pref}{stat}"
                if src_col in stats_df.columns:
                    rename_map[src_col] = f"{idx_name}_{stat_out}"

    stats_df = stats_df.rename(columns=rename_map)

    # pastikan semua kolom expected ada
    expected_cols = build_expected_output_columns(index_name_map)
    for col in expected_cols:
        if col not in stats_df.columns:
            stats_df[col] = 0 if col.endswith("_count") else pd.NA

    return stats_df[expected_cols]


def exactextract_stats_all_bands(
    raster_path: str,
    gdf: gpd.GeoDataFrame,
    index_name_map: Dict[int, str]
) -> pd.DataFrame:
    stats_df = exact_extract(
        rast=raster_path,
        vec=gdf,
        ops=DEFAULT_STATS,
        include_cols=[],
        output="pandas"
    )

    stats_df = rename_exactextract_columns(stats_df, index_name_map)
    return stats_df


def merge_parquet_parts(parts_dir: str,
                        final_parquet: Optional[str],
                        final_csv: Optional[str],
                        overwrite: bool,
                        skip_csv: bool) -> None:
    part_files = sorted(glob.glob(os.path.join(parts_dir, "*.parquet")))
    if not part_files:
        log(f"Tidak ada parquet parts di: {parts_dir}")
        return

    if final_parquet and os.path.exists(final_parquet) and not overwrite:
        raise FileExistsError(f"Final parquet sudah ada: {final_parquet}")

    if final_csv and (not skip_csv) and os.path.exists(final_csv) and not overwrite:
        raise FileExistsError(f"Final CSV sudah ada: {final_csv}")

    log(f"Gabung parquet parts: {len(part_files)} file")

    dfs = []
    for fp in tqdm(part_files, desc="Merging parquet parts", unit="file"):
        dfs.append(pd.read_parquet(fp))

    df_all = pd.concat(dfs, ignore_index=True)

    if final_parquet:
        safe_mkdir(os.path.dirname(final_parquet))
        df_all.to_parquet(final_parquet, index=False)
        log(f"Tulis final parquet: {final_parquet}")

    if final_csv and not skip_csv:
        safe_mkdir(os.path.dirname(final_csv))
        df_all.to_csv(final_csv, index=False)
        log(f"Tulis final CSV: {final_csv}")

    log(f"Merge selesai. Total row: {len(df_all):,}")


def process_single_raster_worker(task: dict) -> Tuple[str, bool, str]:
    raster_path = task["raster_path"]
    gpkg_path = task["gpkg_path"]
    uid_field = task["uid_field"]
    layer_name = task["layer_name"]
    source_type = task["source_type"]
    part_out_path = task["part_out_path"]
    overwrite = task["overwrite"]
    check_crs = task["check_crs"]

    raster_name = os.path.basename(raster_path)

    try:
        if os.path.exists(part_out_path) and not overwrite:
            return raster_name, True, f"Skip existing part: {part_out_path}"

        start_date, end_date, year, week_no = parse_dates_from_filename(raster_path)
        region = infer_region_from_filename(raster_path)

        gdf = load_vector_layer(gpkg_path, layer_name, uid_field)
        gdf = subset_gdf_by_raster_extent(gdf, raster_path)

        if len(gdf) == 0:
            df_empty = pd.DataFrame(columns=[
                "uid", "region", "layer_name", "source", "raster_file",
                "start_date", "end_date", "year", "weekNo"
            ])
            write_part_parquet(df_empty, part_out_path)
            return raster_name, True, "Tidak ada feature overlap, part kosong ditulis."

        with rasterio.open(raster_path) as src:
            if src.crs is None:
                raise ValueError(f"CRS raster kosong: {raster_path}")

            if gdf.crs is None:
                raise ValueError(f"CRS vector layer '{layer_name}' kosong")

            if check_crs and gdf.crs != src.crs:
                raise ValueError(f"CRS mismatch setelah subset: vector={gdf.crs}, raster={src.crs}")

            band_map = build_band_name_map(src)
            index_name_map = resolve_index_names(band_map, source_type=source_type)

            meta_df = pd.DataFrame({
                "uid": gdf[uid_field].values,
                "region": region,
                "layer_name": layer_name,
                "source": source_type,
                "raster_file": raster_name,
                "start_date": start_date,
                "end_date": end_date,
                "year": year,
                "weekNo": week_no,
            })

            stats_df = exactextract_stats_all_bands(
                raster_path=raster_path,
                gdf=gdf,
                index_name_map=index_name_map
            )

            df = pd.concat([meta_df, stats_df], axis=1)
            write_part_parquet(df, part_out_path)

            return raster_name, True, f"OK | overlap={len(gdf):,} | rows={len(df):,}"

    except Exception as e:
        return raster_name, False, f"{e}\n{traceback.format_exc()}"


def process_source(source_type: str,
                   input_dir: Optional[str],
                   gpkg_path: str,
                   uid_field: str,
                   parts_dir: Optional[str],
                   output_parquet: Optional[str],
                   output_csv: Optional[str],
                   layers: Optional[List[str]],
                   overwrite: bool,
                   resume: bool,
                   raster_glob: str,
                   workers: int,
                   skip_csv: bool,
                   check_crs: bool) -> None:
    if not input_dir:
        log(f"Skip source={source_type}, karena input dir tidak diberikan.")
        return

    if not os.path.isdir(input_dir):
        raise NotADirectoryError(f"Input dir tidak ditemukan: {input_dir}")

    if not parts_dir:
        raise ValueError(f"--{source_type}-parts-dir wajib diisi")

    safe_mkdir(parts_dir)
    log(f"Mulai proses source={source_type}")

    available_layers = list_gpkg_layers(gpkg_path)
    log(f"Layer tersedia di GPKG: {available_layers}")

    if layers:
        working_layers = [lyr for lyr in layers if lyr in available_layers]
        if not working_layers:
            raise ValueError("Tidak ada layer valid yang cocok dengan --layers")
    else:
        working_layers = available_layers

    all_rasters = raster_candidates(input_dir, raster_glob)
    log(f"Total raster ditemukan: {len(all_rasters)}")

    valid_rasters = []
    for rp in all_rasters:
        base = os.path.basename(rp).lower()
        if "indices" not in base:
            continue

        region = infer_region_from_filename(rp)
        if region is None:
            continue

        layer_name = choose_layer_for_region(region, working_layers)
        if layer_name is None:
            continue

        part_name = sanitize_part_filename(os.path.basename(rp))
        part_out_path = os.path.join(parts_dir, part_name)

        if resume and os.path.exists(part_out_path) and not overwrite:
            continue

        valid_rasters.append((rp, region, layer_name, part_out_path))

    log(f"Raster valid untuk {source_type}: {len(valid_rasters)}")

    if not valid_rasters:
        log(f"Tidak ada raster untuk diproses pada source={source_type}")
        merge_parquet_parts(
            parts_dir=parts_dir,
            final_parquet=output_parquet,
            final_csv=output_csv,
            overwrite=overwrite,
            skip_csv=skip_csv,
        )
        return

    tasks = []
    for rp, region, layer_name, part_out_path in valid_rasters:
        tasks.append({
            "raster_path": rp,
            "gpkg_path": gpkg_path,
            "uid_field": uid_field,
            "layer_name": layer_name,
            "source_type": source_type,
            "part_out_path": part_out_path,
            "overwrite": overwrite,
            "check_crs": check_crs,
        })

    total = len(tasks)

    if workers <= 1:
        with tqdm(tasks, total=total, desc=f"{source_type.upper()} rasters", unit="raster") as pbar:
            for task in pbar:
                raster_name = os.path.basename(task["raster_path"])
                pbar.set_postfix_str(raster_name[:60])

                name, ok, msg = process_single_raster_worker(task)

                if ok:
                    log(f"{name} -> {msg}")
                else:
                    log(f"ERROR {name} -> {msg}")
    else:
        log(f"Jalankan paralel workers={workers}")
        with ProcessPoolExecutor(max_workers=workers) as ex:
            future_map = {ex.submit(process_single_raster_worker, task): task for task in tasks}

            with tqdm(total=total, desc=f"{source_type.upper()} rasters", unit="raster") as pbar:
                for fut in as_completed(future_map):
                    task = future_map[fut]
                    raster_name = os.path.basename(task["raster_path"])

                    try:
                        name, ok, msg = fut.result()
                        if ok:
                            log(f"{name} -> {msg}")
                        else:
                            log(f"ERROR {name} -> {msg}")
                    except Exception as e:
                        log(f"ERROR {raster_name} -> {e}")

                    pbar.update(1)
                    pbar.set_postfix_str(raster_name[:60])

    log(f"Selesai hitung part untuk source={source_type}")
    merge_parquet_parts(
        parts_dir=parts_dir,
        final_parquet=output_parquet,
        final_csv=output_csv,
        overwrite=overwrite,
        skip_csv=skip_csv,
    )


def main():
    args = parse_args()

    try:
        process_source(
            source_type="s1",
            input_dir=args.s1_input_dir,
            gpkg_path=args.gpkg_path,
            uid_field=args.uid_field,
            parts_dir=args.s1_parts_dir,
            output_parquet=args.s1_output_parquet,
            output_csv=args.s1_output_csv,
            layers=args.layers,
            overwrite=args.overwrite,
            resume=args.resume,
            raster_glob=args.raster_glob,
            workers=args.workers,
            skip_csv=args.skip_csv,
            check_crs=args.check_crs,
        )

        process_source(
            source_type="s2",
            input_dir=args.s2_input_dir,
            gpkg_path=args.gpkg_path,
            uid_field=args.uid_field,
            parts_dir=args.s2_parts_dir,
            output_parquet=args.s2_output_parquet,
            output_csv=args.s2_output_csv,
            layers=args.layers,
            overwrite=args.overwrite,
            resume=args.resume,
            raster_glob=args.raster_glob,
            workers=args.workers,
            skip_csv=args.skip_csv,
            check_crs=args.check_crs,
        )

        log("Semua proses selesai.")
    except Exception as e:
        log(f"FATAL ERROR: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()