#!/usr/bin/env python3
import os
import glob
import time
import argparse
import logging
import numpy as np
import rasterio

def setup_logger(log_file=None, verbose=True):
    logger = logging.getLogger("s1_indices")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    if verbose:
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        logger.addHandler(ch)

    if log_file:
        fh = logging.FileHandler(log_file, mode="a", encoding="utf-8")
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        logger.addHandler(fh)

    return logger

def safe_divide(numerator, denominator):
    out = np.full(numerator.shape, np.nan, dtype=np.float32)
    valid = np.isfinite(numerator) & np.isfinite(denominator) & (denominator != 0)
    out[valid] = numerator[valid] / denominator[valid]
    return out

def compute_indices(vv, vh):
    # Input diasumsikan SUDAH linear
    sigma_vv_linear = vv.astype(np.float32)
    sigma_vh_linear = vh.astype(np.float32)

    p_ratio = safe_divide(sigma_vh_linear, sigma_vv_linear)
    rvi = safe_divide(4.0 * sigma_vh_linear, sigma_vv_linear + sigma_vh_linear)
    rcspr = safe_divide(
        sigma_vv_linear - sigma_vh_linear,
        sigma_vv_linear + sigma_vh_linear
    )

    return sigma_vv_linear, sigma_vh_linear, p_ratio, rvi, rcspr

def format_eta(seconds):
    if seconds is None or not np.isfinite(seconds):
        return "unknown"
    seconds = int(max(0, seconds))
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f"{h}h {m}m {s}s"
    if m > 0:
        return f"{m}m {s}s"
    return f"{s}s"

def process_tif(input_path, output_dir, logger, overwrite=False):
    base = os.path.splitext(os.path.basename(input_path))[0]
    output_path = os.path.join(output_dir, f"{base}_indices.tif")

    if os.path.exists(output_path) and not overwrite:
        logger.info(f"SKIP | output already exists | {output_path}")
        return "skip"

    with rasterio.open(input_path) as src:
        if src.count < 2:
            logger.warning(f"SKIP | band kurang dari 2 | {input_path}")
            return "skip"

        vv = src.read(1).astype(np.float32)  # Band 1 = VV
        vh = src.read(2).astype(np.float32)  # Band 2 = VH

        nodata = src.nodata
        if nodata is not None:
            vv[vv == nodata] = np.nan
            vh[vh == nodata] = np.nan

        sigma_vv_linear, sigma_vh_linear, p_ratio, rvi, rcspr = compute_indices(vv, vh)

        profile = src.profile.copy()
        profile.update(
            dtype=rasterio.float32,
            count=5,
            compress="deflate",
            nodata=np.nan
        )

        with rasterio.open(output_path, "w", **profile) as dst:
            dst.write(sigma_vv_linear, 1)
            dst.write(sigma_vh_linear, 2)
            dst.write(p_ratio, 3)
            dst.write(rvi, 4)
            dst.write(rcspr, 5)

            dst.set_band_description(1, "sigma_vv_linear")
            dst.set_band_description(2, "sigma_vh_linear")
            dst.set_band_description(3, "p_ratio")
            dst.set_band_description(4, "RVI")
            dst.set_band_description(5, "RCSPR")

    logger.info(f"OK   | saved | {output_path}")
    return "ok"

def main():
    parser = argparse.ArgumentParser(
        description="Hitung indeks SAR dari GeoTIFF mingguan Sentinel-1 (input linear)."
    )
    parser.add_argument(
        "--input-dir",
        required=True,
        help="Folder berisi file .tif input mingguan"
    )
    parser.add_argument(
        "--output-dir",
        default=None,
        help="Folder output. Default: <input-dir>/indices"
    )
    parser.add_argument(
        "--pattern",
        default="*.tif",
        help="Pattern file input. Default: *.tif"
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Path file log. Default: <output-dir>/process_indices.log"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Timpa output jika sudah ada"
    )

    args = parser.parse_args()

    input_dir = args.input_dir
    output_dir = args.output_dir or os.path.join(input_dir, "indices")
    os.makedirs(output_dir, exist_ok=True)

    log_file = args.log_file or os.path.join(output_dir, "process_indices.log")
    logger = setup_logger(log_file=log_file, verbose=True)

    tif_files = sorted(glob.glob(os.path.join(input_dir, args.pattern)))

    if not tif_files:
        logger.warning(f"Tidak ada file yang cocok di: {os.path.join(input_dir, args.pattern)}")
        return

    total = len(tif_files)
    ok_count = 0
    skip_count = 0
    err_count = 0

    logger.info("=" * 80)
    logger.info("START processing S1 indices")
    logger.info(f"Input dir  : {input_dir}")
    logger.info(f"Output dir : {output_dir}")
    logger.info(f"Pattern    : {args.pattern}")
    logger.info(f"Total files: {total}")
    logger.info(f"Log file   : {log_file}")
    logger.info("=" * 80)

    t0_all = time.time()

    for i, tif in enumerate(tif_files, start=1):
        t0 = time.time()
        percent = (i / total) * 100.0

        elapsed_total = time.time() - t0_all
        avg_time = elapsed_total / (i - 1) if i > 1 else None
        remaining = total - (i - 1)
        eta = avg_time * remaining if avg_time is not None else None

        logger.info(
            f"[{i}/{total}] {percent:6.2f}% | ETA {format_eta(eta)} | processing: {os.path.basename(tif)}"
        )

        try:
            result = process_tif(
                input_path=tif,
                output_dir=output_dir,
                logger=logger,
                overwrite=args.overwrite
            )
            if result == "ok":
                ok_count += 1
            else:
                skip_count += 1

        except Exception as e:
            err_count += 1
            logger.exception(f"ERROR | failed processing | {tif} | {e}")

        dt = time.time() - t0
        logger.info(f"[{i}/{total}] done in {dt:.2f}s")

    total_time = time.time() - t0_all

    logger.info("=" * 80)
    logger.info("FINISH processing S1 indices")
    logger.info(f"Total processed loop : {total}")
    logger.info(f"Success              : {ok_count}")
    logger.info(f"Skipped              : {skip_count}")
    logger.info(f"Errors               : {err_count}")
    logger.info(f"Total duration       : {format_eta(total_time)}")
    logger.info("=" * 80)

if __name__ == "__main__":
    main()