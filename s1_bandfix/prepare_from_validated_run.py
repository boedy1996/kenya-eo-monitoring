#!/usr/bin/env python3
"""Prepare an S1 band-fix run by reusing validated pixel membership."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from dataclasses import asdict
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from zonal_center_pipeline import (
    PIPELINE_VERSION,
    REGIONS,
    GridSignature,
    RasterRecord,
    atomic_write_json,
    inventory_rasters,
    sha256_file,
    utc_now,
    weekly_gaps,
)


def spatial_grid_equals(left: GridSignature, right: GridSignature) -> bool:
    return (
        left.crs_wkt == right.crs_wkt
        and left.transform == right.transform
        and left.width == right.width
        and left.height == right.height
    )


def hardlink_or_copy(source: Path, target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    try:
        os.link(source, target)
    except OSError:
        shutil.copy2(source, target)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-run", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--s1-weekly-dir", type=Path, required=True)
    args = parser.parse_args()

    base_run = args.base_run.resolve()
    run_dir = args.run_dir.resolve()
    weekly_dir = args.s1_weekly_dir.resolve()
    if run_dir.exists():
        raise FileExistsError(f"Target run already exists: {run_dir}")

    base_manifest = json.loads((base_run / "run_manifest.json").read_text())
    base_validation = json.loads((base_run / "validation_report.json").read_text())
    if base_validation.get("status") != "passed":
        raise ValueError(f"Base run validation did not pass: {base_run}")
    base_inventory = pd.read_parquet(base_run / "manifest" / "rasters.parquet")

    records, raw_grids = inventory_rasters("s1", weekly_dir, input_kind="weekly-linear")
    old_s1 = base_inventory[base_inventory.sensor == "s1"].copy()
    old_keys = {
        (str(row.region), str(row.start_date), str(row.end_date))
        for row in old_s1.itertuples(index=False)
    }
    new_keys = {(item.region, item.start_date, item.end_date) for item in records}
    if old_keys != new_keys:
        missing = sorted(old_keys - new_keys)[:10]
        added = sorted(new_keys - old_keys)[:10]
        raise ValueError(f"Weekly S1 inventory differs from base run; missing={missing}, added={added}")

    for region in REGIONS:
        old_grid = GridSignature.from_dict(base_manifest["grid_signatures"][f"s1_{region}"])
        if not spatial_grid_equals(old_grid, raw_grids[region]):
            raise ValueError(f"S1 spatial grid differs from validated membership for {region}")

        membership = base_run / "membership" / f"s1_{region}.npz"
        fields = base_run / "manifest" / f"fields_s1_{region}.parquet"
        field_rows = len(pd.read_parquet(fields, columns=["field_id"]))
        expected_fields = int(base_manifest["field_counts"][region])
        if field_rows != expected_fields:
            raise ValueError(f"Field manifest count mismatch for {region}: {field_rows} != {expected_fields}")
        with np.load(membership, allow_pickle=False) as archive:
            if int(archive["raster_width"][0]) != raw_grids[region].width:
                raise ValueError(f"Membership width mismatch for {region}")
            if int(archive["raster_height"][0]) != raw_grids[region].height:
                raise ValueError(f"Membership height mismatch for {region}")

    run_dir.mkdir(parents=True)
    for region in REGIONS:
        hardlink_or_copy(
            base_run / "membership" / f"s1_{region}.npz",
            run_dir / "membership" / f"s1_{region}.npz",
        )
        hardlink_or_copy(
            base_run / "manifest" / f"fields_s1_{region}.parquet",
            run_dir / "manifest" / f"fields_s1_{region}.parquet",
        )
    hardlink_or_copy(
        base_run / "manifest" / "fields_common.parquet",
        run_dir / "manifest" / "fields_common.parquet",
    )

    new_s1 = pd.DataFrame([asdict(record) for record in records])
    retained_s2 = base_inventory[base_inventory.sensor == "s2"].copy()
    inventory = pd.concat([new_s1, retained_s2], ignore_index=True)
    inventory = inventory.sort_values(["sensor", "region", "start_date"])
    inventory.to_parquet(run_dir / "manifest" / "rasters.parquet", index=False, compression="zstd")

    manifest = dict(base_manifest)
    manifest.update(
        pipeline_version=PIPELINE_VERSION,
        created_at_utc=utc_now(),
        run_dir=str(run_dir),
        base_validated_run=str(base_run),
        base_validation_sha256=sha256_file(base_run / "validation_report.json"),
        membership_reused=True,
        input_dirs={**base_manifest["input_dirs"], "s1": str(weekly_dir)},
        input_kinds={"s1": "weekly-linear", "s2": "indices"},
        status="prepared",
    )
    manifest["grid_signatures"] = dict(base_manifest["grid_signatures"])
    for region in REGIONS:
        manifest["grid_signatures"][f"s1_{region}"] = asdict(raw_grids[region])
    manifest["raster_counts"] = dict(base_manifest["raster_counts"])
    manifest["raster_counts"]["s1"] = {
        region: sum(record.region == region for record in records)
        for region in REGIONS
    }
    manifest["expected_rows"] = dict(base_manifest["expected_rows"])
    manifest["expected_rows"]["s1"] = sum(
        int(manifest["field_counts"][region]) * int(manifest["raster_counts"]["s1"][region])
        for region in REGIONS
    )
    manifest["weekly_gaps"] = dict(base_manifest.get("weekly_gaps", {}))
    manifest["weekly_gaps"]["s1"] = {
        region: weekly_gaps([record for record in records if record.region == region])
        for region in REGIONS
    }
    atomic_write_json(run_dir / "run_manifest.json", manifest)
    print(
        json.dumps(
            {
                "run_dir": str(run_dir),
                "base_run": str(base_run),
                "field_counts": manifest["field_counts"],
                "s1_raster_counts": manifest["raster_counts"]["s1"],
                "s1_expected_rows": manifest["expected_rows"]["s1"],
                "membership_reused": True,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
