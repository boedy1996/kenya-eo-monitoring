#!/usr/bin/env python3
"""Recompute seven Sentinel-2 indices from existing weekly L2A TIFFs.

Assumption: the source SAFE products used BOA_ADD_OFFSET=-1000, while the
weekly TIFFs contain DN/10000. The original SAFE IDs are not retained in
all weekly TIFFs, so this assumption is recorded in every output.
"""

from __future__ import annotations

import argparse
import fcntl
import fnmatch
import hashlib
import json
import os
import shutil
import sys
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import rasterio


ALGORITHM_VERSION = "weekly_boa_minus_1000_v1"
BAND_INPUTS = (1, 2, 3, 4, 7, 8, 9)
SOURCE_DESCRIPTIONS = ("B02", "B03", "B04", "B05", "B06", "B07", "B08", "B8A", "B11", "B12")
INDEX_DESCRIPTIONS = ("NDVI", "EVI", "NDMI", "NDRE", "MSI", "PVR", "LAI")
OFFSET = np.float32(0.1)
GIB = 1024**3


class InsufficientSpace(RuntimeError):
    pass


def log(event: str, **fields: object) -> None:
    print(json.dumps({"time_utc": datetime.now(timezone.utc).isoformat(), "event": event, **fields}), flush=True)


def safe_divide(numerator: np.ndarray, denominator: np.ndarray) -> np.ndarray:
    result = np.full(numerator.shape, np.nan, dtype=np.float32)
    valid = np.isfinite(numerator) & np.isfinite(denominator) & (denominator != 0)
    np.divide(numerator, denominator, out=result, where=valid)
    return result


def compute_indices(raw: np.ndarray) -> tuple[np.ndarray, ...]:
    """Use only the seven required weekly bands, in BAND_INPUTS order."""
    raw[~np.isfinite(raw) | (raw == 0)] = np.nan
    raw -= OFFSET
    b02, b03, b04, b05, b08, b8a, b11 = raw
    with np.errstate(divide="ignore", invalid="ignore", over="ignore"):
        ndvi = np.clip(safe_divide(b08 - b04, b08 + b04), -1, 1)
        evi = np.clip(safe_divide(np.float32(2.5) * (b08 - b04), b08 + 6 * b04 - 7.5 * b02 + 1), -1, 1)
        ndmi = np.clip(safe_divide(b08 - b11, b08 + b11), -1, 1)
        ndre = np.clip(safe_divide(b8a - b05, b8a + b05), -1, 1)
        msi = np.clip(safe_divide(b11, b08), 0, 10)
        pvr = np.clip(safe_divide(b03 - b02, b03 + b02), -1, 1)
        lai = np.clip((np.float32(3.618) * ndvi - np.float32(0.118)).astype(np.float32), 0, 10)
    return ndvi, evi, ndmi, ndre, msi, pvr, lai


def output_name(source: Path) -> str:
    suffix = "_weekly_clean_10m.tif"
    if not source.name.endswith(suffix):
        raise ValueError(f"Unexpected weekly TIFF filename: {source.name}")
    return source.name[: -len(suffix)] + "_weekly_indices_10m.tif"


def validate_source(src: rasterio.DatasetReader, path: Path) -> None:
    if src.count != 10 or any(dtype != "float32" for dtype in src.dtypes):
        raise ValueError(f"Expected 10 float32 source bands: {path}")
    if src.descriptions != SOURCE_DESCRIPTIONS:
        raise ValueError(f"Unexpected source band order: {path}: {src.descriptions}")
    if src.crs is None or src.transform is None:
        raise ValueError(f"Missing source grid: {path}")


def check_free_space(directory: Path, reserve_bytes: int, required_extra: int = 0) -> int:
    free = shutil.disk_usage(directory).free
    if free <= reserve_bytes + required_extra:
        raise InsufficientSpace(
            f"Free {free / GIB:.1f} GiB, reserve {reserve_bytes / GIB:.1f} GiB, "
            f"additional requirement {required_extra / GIB:.1f} GiB"
        )
    return free


def expected_tags(source: Path, source_stat: os.stat_result) -> dict[str, str]:
    return {
        "ALGORITHM_VERSION": ALGORITHM_VERSION,
        "SOURCE_WEEKLY_TIFF": source.name,
        "SOURCE_SIZE_BYTES": str(source_stat.st_size),
        "SOURCE_MTIME_NS": str(source_stat.st_mtime_ns),
        "SOURCE_BASELINE": "UNVERIFIED_ASSUMED_BOA_ADD_OFFSET_MINUS_1000",
        "WEEKLY_REFLECTANCE_CORRECTION": "subtract_0.1_from_each_valid_used_band",
    }


def validate_output(path: Path, src: rasterio.DatasetReader, tags: dict[str, str],
                    sample: tuple[rasterio.windows.Window, tuple[np.ndarray, ...]] | None = None) -> None:
    with rasterio.open(path) as dst:
        if dst.count != 7 or any(dtype != "float32" for dtype in dst.dtypes):
            raise ValueError(f"Invalid output band count/dtype: {path}")
        if dst.shape != src.shape or dst.crs != src.crs or dst.transform != src.transform:
            raise ValueError(f"Output grid differs from source: {path}")
        if dst.descriptions != INDEX_DESCRIPTIONS or not np.isnan(dst.nodata):
            raise ValueError(f"Invalid output band descriptions/nodata: {path}")
        actual_tags = dst.tags()
        if any(actual_tags.get(key) != value for key, value in tags.items()):
            raise ValueError(f"Output provenance differs from source or algorithm: {path}")
        if sample is not None:
            window, expected = sample
            actual = dst.read(window=window)
            for number, band in enumerate(expected):
                if not np.array_equal(actual[number], band, equal_nan=True):
                    raise ValueError(f"Output sample mismatch in {INDEX_DESCRIPTIONS[number]}: {path}")


def append_manifest(path: Path, record: dict[str, object]) -> None:
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(record, sort_keys=True) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def audit_sources(sources: list[Path]) -> tuple[list[Path], list[dict[str, str]]]:
    valid = []
    issues = []
    for source in sources:
        try:
            with rasterio.open(source) as src:
                validate_source(src, source)
        except (OSError, ValueError, rasterio.errors.RasterioIOError) as exc:
            issue = {"source": str(source), "reason": str(exc)}
            issues.append(issue)
            log("source_invalid", **issue)
        else:
            valid.append(source)
    return valid, issues


def write_issue_report(path: Path, issues: list[dict[str, str]]) -> None:
    partial = path.with_suffix(path.suffix + ".partial")
    payload = {"checked_utc": datetime.now(timezone.utc).isoformat(),
               "issue_count": len(issues), "issues": issues}
    partial.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    os.replace(partial, path)


@contextmanager
def single_runner_lock(directory: Path):
    lock_path = directory / ".recompute_weekly_indices.lock"
    with lock_path.open("a+") as stream:
        try:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError(f"Another index rebuild holds {lock_path}") from exc
        stream.seek(0)
        stream.truncate()
        stream.write(f"pid={os.getpid()} started_utc={datetime.now(timezone.utc).isoformat()}\n")
        stream.flush()
        try:
            yield
        finally:
            fcntl.flock(stream.fileno(), fcntl.LOCK_UN)


def process_file(source: Path, output_dir: Path, reserve_bytes: int,
                 script_sha256: str, manifest: Path) -> str:
    target = output_dir / output_name(source)
    partial = output_dir / (target.name + ".partial")
    source_stat = source.stat()
    tags = expected_tags(source, source_stat)
    started = time.monotonic()

    with rasterio.open(source) as src:
        validate_source(src, source)
        if target.exists():
            validate_output(target, src, tags)
            log("skip_valid_existing", source=source.name, output=str(target))
            return "skipped"

        if partial.exists():
            log("remove_stale_partial", path=str(partial), bytes=partial.stat().st_size)
            partial.unlink()

        free_before = check_free_space(output_dir, reserve_bytes, 2 * GIB)
        log("start_file", source=source.name, source_bytes=source_stat.st_size,
            free_bytes=free_before, output=str(target))
        profile = src.profile.copy()
        profile.update(count=7, dtype="float32", nodata=np.nan, tiled=True,
                       blockxsize=256, blockysize=256, compress="DEFLATE",
                       predictor=2, BIGTIFF="IF_SAFER")
        source_hash = hashlib.sha256()
        output_hash = hashlib.sha256()
        source_zero_band_pixels = 0
        blocks = 0
        sample = None

        try:
            with rasterio.open(partial, "w", **profile) as dst:
                for number, description in enumerate(INDEX_DESCRIPTIONS, 1):
                    dst.set_band_description(number, description)
                for _, window in src.block_windows(1):
                    raw = src.read(BAND_INPUTS, window=window, out_dtype="float32")
                    source_hash.update(raw.tobytes(order="C"))
                    source_zero_band_pixels += int(np.count_nonzero((raw == 0) & np.isfinite(raw)))
                    bands = compute_indices(raw)
                    for number, band in enumerate(bands, 1):
                        dst.write(band, number, window=window)
                        output_hash.update(band.tobytes(order="C"))
                    if sample is None and np.isfinite(bands[0]).any():
                        sample = (window, tuple(band.copy() for band in bands))
                    blocks += 1
                    if blocks % 32 == 0:
                        check_free_space(output_dir, reserve_bytes)
                check_free_space(output_dir, reserve_bytes)
                dst.update_tags(**tags, SOURCE_USED_BANDS_SHA256=source_hash.hexdigest(),
                                OUTPUT_PIXEL_SHA256=output_hash.hexdigest(),
                                SCRIPT_SHA256=script_sha256)

            if source.stat().st_size != source_stat.st_size or source.stat().st_mtime_ns != source_stat.st_mtime_ns:
                raise RuntimeError(f"Source changed during processing: {source}")
            validate_output(partial, src, tags, sample)
            if target.exists():
                raise FileExistsError(f"Output appeared while processing: {target}")
            os.replace(partial, target)
            record = {
                "event": "completed", "time_utc": datetime.now(timezone.utc).isoformat(),
                "source": str(source), "output": str(target),
                "source_bytes": source_stat.st_size, "source_mtime_ns": source_stat.st_mtime_ns,
                "output_bytes": target.stat().st_size, "blocks": blocks,
                "source_zero_band_pixels": source_zero_band_pixels,
                "source_used_bands_sha256": source_hash.hexdigest(),
                "output_pixel_sha256": output_hash.hexdigest(),
                "script_sha256": script_sha256, "algorithm_version": ALGORITHM_VERSION,
                "source_baseline": tags["SOURCE_BASELINE"],
                "elapsed_seconds": round(time.monotonic() - started, 1),
                "free_bytes_after": shutil.disk_usage(output_dir).free,
            }
            append_manifest(manifest, record)
            log("completed", **{key: value for key, value in record.items() if key != "event"})
            return "completed"
        except BaseException:
            if partial.exists():
                partial.unlink()
            raise


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, default=Path("/data/weekly"))
    parser.add_argument("--output-dir", type=Path, default=Path("/data/indices"))
    parser.add_argument("--reserve-gb", type=float, default=100,
                        help="Stop if output filesystem free space falls below this many GiB (default: 100)")
    parser.add_argument("--only", help="Optional filename glob, e.g. 'machakos_2023-05-29_*'")
    parser.add_argument("--limit", type=int, help="Maximum source files to consider, useful for a smoke run")
    parser.add_argument("--dry-run", action="store_true", help="List the matching source count without writing")
    args = parser.parse_args()
    if args.reserve_gb < 0 or (args.limit is not None and args.limit < 1):
        parser.error("--reserve-gb must be nonnegative and --limit must be at least 1")
    return args


def main() -> int:
    args = parse_args()
    source_dir = args.source_dir.resolve()
    output_dir = args.output_dir.resolve()
    if not source_dir.is_dir():
        raise FileNotFoundError(source_dir)
    sources = sorted(path for path in source_dir.glob("*_weekly_clean_10m.tif")
                     if path.name.startswith(("machakos_", "nakuru_"))
                     and (args.only is None or fnmatch.fnmatch(path.name, args.only)))
    if args.limit is not None:
        sources = sources[:args.limit]
    if not sources:
        raise ValueError("No weekly TIFFs match the selection")
    log("preflight", source_dir=str(source_dir), output_dir=str(output_dir),
        source_count=len(sources), reserve_gib=args.reserve_gb, dry_run=args.dry_run,
        first=sources[0].name, last=sources[-1].name,
        source_total_bytes=sum(path.stat().st_size for path in sources))
    valid_sources, issues = audit_sources(sources)
    log("source_audit", checked=len(sources), valid=len(valid_sources), invalid=len(issues))
    if args.dry_run:
        return 3 if issues else 0

    output_dir.mkdir(parents=True, exist_ok=True)
    reserve_bytes = int(args.reserve_gb * GIB)
    script_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    manifest = output_dir / "recompute_weekly_indices_manifest.jsonl"
    with single_runner_lock(output_dir):
        issue_report = output_dir / "recompute_weekly_indices_source_issues.json"
        write_issue_report(issue_report, issues)
        counts = {"completed": 0, "skipped": 0}
        for number, source in enumerate(valid_sources, 1):
            log("progress", number=number, total=len(valid_sources), source=source.name)
            status = process_file(source, output_dir, reserve_bytes, script_sha256, manifest)
            counts[status] += 1
        if issues:
            log("incomplete_source_issues", source_count=len(sources), invalid=len(issues),
                issue_report=str(issue_report), **counts)
        else:
            log("all_done", total=len(sources), **counts)
    return 3 if issues else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:
        log("stopped_with_error", error_type=type(exc).__name__, error=str(exc))
        sys.exit(2)
