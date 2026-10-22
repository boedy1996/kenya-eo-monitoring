#!/usr/bin/env python3
"""Audit the numeric polarization order of weekly Sentinel-1 GeoTIFFs.

The archive's band descriptions are recorded, but classification is based on
paired positive samples. For Sentinel-1 dual-pol GRD over land, VV normally has
the larger regional distribution and VH the smaller one. Ambiguous and
single-polarization files are deliberately left unresolved.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import random
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

import numpy as np
import rasterio


def finite_positive(values: np.ndarray, nodata: float | None) -> np.ndarray:
    valid = np.isfinite(values) & (values > 0)
    if nodata is not None and math.isfinite(nodata):
        valid &= values != nodata
    return valid


def classify(valid_1: int, valid_2: int, paired: int, ratio: float, fraction_2_gt_1: float) -> str:
    if valid_1 == 0 and valid_2 == 0:
        return "both_empty"
    if valid_1 > 0 and valid_2 == 0:
        return "band1_only"
    if valid_2 > 0 and valid_1 == 0:
        return "band2_only"
    if paired < 1_000 or not np.isfinite(ratio):
        return "insufficient_paired_samples"
    if ratio >= 1.25 and fraction_2_gt_1 >= 0.70:
        return "band1_likely_vh_band2_likely_vv"
    if ratio <= 0.80 and fraction_2_gt_1 <= 0.30:
        return "band1_likely_vv_band2_likely_vh"
    return "ambiguous"


def audit_one(path_text: str, min_blocks: int, max_blocks: int, target_pairs: int) -> dict:
    path = Path(path_text)
    record: dict[str, object] = {"path": str(path), "file": path.name, "error": ""}
    try:
        with rasterio.open(path) as src:
            record.update(
                band_count=src.count,
                description_1=src.descriptions[0] if src.count >= 1 else None,
                description_2=src.descriptions[1] if src.count >= 2 else None,
                width=src.width,
                height=src.height,
                crs=str(src.crs),
                nodata=src.nodata,
            )
            if src.count < 2:
                record.update(classification="fewer_than_two_bands")
                return record

            windows = [window for _, window in src.block_windows(1)]
            seed = int.from_bytes(hashlib.sha256(path.name.encode()).digest()[:8], "big")
            random.Random(seed).shuffle(windows)

            paired_1: list[np.ndarray] = []
            paired_2: list[np.ndarray] = []
            valid_1_total = 0
            valid_2_total = 0
            paired_total = 0
            blocks_read = 0

            for window in windows[:max_blocks]:
                values = src.read((1, 2), window=window, masked=False)
                mask_1 = finite_positive(values[0], src.nodata)
                mask_2 = finite_positive(values[1], src.nodata)
                paired_mask = mask_1 & mask_2
                valid_1_total += int(mask_1.sum())
                valid_2_total += int(mask_2.sum())
                paired_count = int(paired_mask.sum())
                paired_total += paired_count
                blocks_read += 1
                if paired_count:
                    paired_1.append(values[0][paired_mask].astype(np.float32, copy=False))
                    paired_2.append(values[1][paired_mask].astype(np.float32, copy=False))
                if blocks_read >= min_blocks and paired_total >= target_pairs:
                    break

            if paired_total:
                values_1 = np.concatenate(paired_1)
                values_2 = np.concatenate(paired_2)
                median_1 = float(np.median(values_1))
                median_2 = float(np.median(values_2))
                ratio = median_2 / median_1 if median_1 > 0 else float("nan")
                fraction_2_gt_1 = float(np.mean(values_2 > values_1))
            else:
                median_1 = median_2 = ratio = fraction_2_gt_1 = float("nan")

            record.update(
                blocks_read=blocks_read,
                valid_band1_samples=valid_1_total,
                valid_band2_samples=valid_2_total,
                paired_samples=paired_total,
                band1_median=median_1,
                band2_median=median_2,
                median_band2_over_band1=ratio,
                fraction_band2_gt_band1=fraction_2_gt_1,
                classification=classify(
                    valid_1_total, valid_2_total, paired_total, ratio, fraction_2_gt_1
                ),
            )
    except Exception as exc:  # preserve a complete inventory
        record.update(classification="error", error=f"{type(exc).__name__}: {exc}")
    return record


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--pattern", default="*.tif")
    parser.add_argument("--workers", type=int, default=2)
    parser.add_argument("--min-blocks", type=int, default=12)
    parser.add_argument("--max-blocks", type=int, default=64)
    parser.add_argument("--target-pairs", type=int, default=100_000)
    args = parser.parse_args()

    paths = sorted(args.input_dir.glob(args.pattern))
    if not paths:
        raise SystemExit(f"No files matched {args.input_dir / args.pattern}")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    records: list[dict] = []
    with ProcessPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(
                audit_one, str(path), args.min_blocks, args.max_blocks, args.target_pairs
            ): path
            for path in paths
        }
        for completed, future in enumerate(as_completed(futures), start=1):
            record = future.result()
            records.append(record)
            if completed % 25 == 0 or completed == len(paths):
                print(f"Audited {completed}/{len(paths)} TIFFs", flush=True)

    records.sort(key=lambda item: str(item["file"]))
    columns = sorted({key for record in records for key in record})
    csv_path = args.output_dir / "weekly_tiff_band_audit.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(records)

    counts: dict[str, int] = {}
    for record in records:
        key = str(record.get("classification", "missing"))
        counts[key] = counts.get(key, 0) + 1
    summary = {
        "input_dir": str(args.input_dir),
        "file_count": len(paths),
        "classification_counts": dict(sorted(counts.items())),
        "parameters": {
            "min_blocks": args.min_blocks,
            "max_blocks": args.max_blocks,
            "target_pairs": args.target_pairs,
            "workers": args.workers,
        },
    }
    (args.output_dir / "weekly_tiff_band_audit_summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, indent=2), flush=True)


if __name__ == "__main__":
    main()
