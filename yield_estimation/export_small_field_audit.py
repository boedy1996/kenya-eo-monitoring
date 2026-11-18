#!/usr/bin/env python3
"""Export a cell-level WaPOR audit for one field-season result."""

from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path
from typing import Any

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from affine import Affine
from pyproj import Geod
from rasterio.transform import rowcol
from shapely.geometry import box, mapping


GLOBAL_WAPOR_TRANSFORM = Affine(0.0009765625, 0.0, -180.0, 0.0, -0.0009765625, 60.0)
GLOBAL_WAPOR_WIDTH = 368_640


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--field-id", required=True)
    parser.add_argument("--season-code", required=True)
    parser.add_argument("--county", choices=("machakos", "nakuru"), required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    return parser.parse_args()


def json_value(value: Any) -> Any:
    if isinstance(value, (pd.Timestamp, date)):
        return value.isoformat()
    if isinstance(value, np.generic):
        return value.item()
    if pd.isna(value):
        return None
    return value


def row_dict(row: pd.Series) -> dict[str, Any]:
    return {str(key): json_value(value) for key, value in row.items()}


def overlap_days(start: pd.Timestamp, end: pd.Timestamp, sos: pd.Timestamp, eos: pd.Timestamp) -> int:
    overlap_start = max(start, sos)
    overlap_end = min(end, eos)
    return max(0, int((overlap_end - overlap_start).days) + 1)


def geodesic_area_ha(bounds: tuple[float, float, float, float]) -> float:
    left, bottom, right, top = bounds
    geod = Geod(ellps="WGS84")
    area, _ = geod.polygon_area_perimeter(
        [left, right, right, left, left],
        [bottom, bottom, top, top, bottom],
    )
    return abs(float(area)) / 10_000.0


def main() -> None:
    args = parse_args()
    run_dir = args.run_dir.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = run_dir / "outputs" / f"{args.season_code}.parquet"
    field_rows = pd.read_parquet(
        output_path,
        filters=[("field_id", "=", args.field_id)],
    )
    if len(field_rows) != 1:
        raise RuntimeError(f"Expected one field result, found {len(field_rows)}")
    field = field_rows.iloc[0]
    if str(field.county).lower() != args.county:
        raise RuntimeError(f"Field county is {field.county}, not {args.county}")
    if float(field.area_ha) >= 1.0:
        raise RuntimeError(f"Field is not under 1 ha: {float(field.area_ha):.6f} ha")
    if pd.isna(field.yield_central_t_ha):
        raise RuntimeError("Field has no calculated central yield")

    geometry_frame = gpd.read_parquet(run_dir / "fields" / f"{args.county}.parquet")
    geometry_frame = geometry_frame[geometry_frame.field_id.astype(str) == args.field_id]
    if len(geometry_frame) != 1:
        raise RuntimeError(f"Expected one field geometry, found {len(geometry_frame)}")
    field_geometry = geometry_frame.to_crs(4326).geometry.iloc[0]

    weights = np.load(run_dir / "weights" / f"{args.county}.npz")
    field_ids = weights["field_ids"].astype(str)
    positions = np.flatnonzero(field_ids == args.field_id)
    if len(positions) != 1:
        raise RuntimeError(f"Field ID occurs {len(positions)} times in cached weights")
    field_position = int(positions[0])
    mask = weights["field_index"] == field_position
    cell_ids = weights["cell_id"][mask].astype(np.int64)
    coverage = weights["coverage"][mask].astype(np.float64)
    total_coverage = float(weights["total_coverage"][field_position])
    if not len(cell_ids) or total_coverage <= 0:
        raise RuntimeError("Field has no cached WaPOR cell intersections")

    npp_dir = run_dir / "npp" / args.season_code / args.county
    npp_files = sorted(npp_dir.glob("*.parquet"))
    if not npp_files:
        raise RuntimeError(f"No dekadal NPP files in {npp_dir}")

    npp_rows: list[dict[str, Any]] = []
    for path in npp_files:
        frame = pd.read_parquet(path, filters=[("field_id", "=", args.field_id)])
        if len(frame) != 1:
            raise RuntimeError(f"{path.name}: expected one field row, found {len(frame)}")
        npp_rows.append(row_dict(frame.iloc[0]))
    npp = pd.DataFrame(npp_rows).sort_values("period_start").reset_index(drop=True)
    npp["period_start"] = pd.to_datetime(npp.period_start)
    npp["period_end"] = pd.to_datetime(npp.period_end)
    sos = pd.Timestamp(field.sos)
    eos = pd.Timestamp(field.eos)
    npp["overlap_days"] = [
        overlap_days(start, end, sos, eos)
        for start, end in zip(npp.period_start, npp.period_end)
    ]
    npp["used"] = npp.overlap_days > 0
    npp["npp_contribution_gC_m2"] = npp.npp_rate_gC_m2_day * npp.overlap_days
    npp["cumulative_npp_gC_m2"] = npp.npp_contribution_gC_m2.cumsum()

    pixel_rows: list[dict[str, Any]] = []
    pixel_features: dict[int, dict[str, Any]] = {}
    for period in npp.itertuples(index=False):
        raster_path = run_dir / "rasters" / args.county / f"{period.wapor_code}.tif"
        with rasterio.open(raster_path) as src:
            raw = src.read(1).reshape(-1)[cell_ids].astype(np.float64)
            valid = np.isfinite(raw)
            if src.nodata is not None and np.isfinite(src.nodata):
                valid &= raw != src.nodata
            physical = raw * float(src.scales[0]) + float(src.offsets[0])
            valid_coverage = np.where(valid, coverage, 0.0)
            denominator = float(valid_coverage.sum())
            normalized_weight = np.divide(
                valid_coverage,
                denominator,
                out=np.zeros_like(valid_coverage),
                where=denominator > 0,
            )
            extracted = float(np.sum(np.where(valid, physical * normalized_weight, 0.0)))
            if not np.isclose(extracted, float(period.npp_rate_gC_m2_day), atol=1e-5):
                raise RuntimeError(
                    f"{period.wapor_code}: pixel reconstruction {extracted} "
                    f"!= extracted {period.npp_rate_gC_m2_day}"
                )
            for local_cell_id, fraction, is_valid, stored, rate, weight in zip(
                cell_ids, coverage, valid, raw, physical, normalized_weight
            ):
                grid_row = int(local_cell_id) // src.width
                grid_col = int(local_cell_id) % src.width
                left, bottom, right, top = rasterio.transform.array_bounds(
                    1, 1, src.window_transform(rasterio.windows.Window(grid_col, grid_row, 1, 1))
                )
                center_x = (left + right) / 2.0
                center_y = (bottom + top) / 2.0
                global_row, global_col = rowcol(GLOBAL_WAPOR_TRANSFORM, center_x, center_y)
                global_cell_id = int(global_row) * GLOBAL_WAPOR_WIDTH + int(global_col)
                cell_area_ha = geodesic_area_ha((left, bottom, right, top))
                pixel_features.setdefault(
                    int(local_cell_id),
                    {
                        "local_cell_id": int(local_cell_id),
                        "global_cell_id": global_cell_id,
                        "grid_row": grid_row,
                        "grid_col": grid_col,
                        "coverage_fraction_of_cell": float(fraction),
                        "normalized_polygon_weight": float(fraction / total_coverage),
                        "cell_area_ha": cell_area_ha,
                        "field_intersection_area_ha": float(fraction) * cell_area_ha,
                        "bounds": [left, bottom, right, top],
                    },
                )
                weighted_rate = float(rate * weight) if is_valid else 0.0
                pixel_rows.append(
                    {
                        "field_id": args.field_id,
                        "season_code": args.season_code,
                        "wapor_code": period.wapor_code,
                        "period_start": period.period_start,
                        "period_end": period.period_end,
                        "overlap_days": int(period.overlap_days),
                        "used": bool(period.used),
                        "local_cell_id": int(local_cell_id),
                        "global_cell_id": global_cell_id,
                        "grid_row": grid_row,
                        "grid_col": grid_col,
                        "cell_left": left,
                        "cell_bottom": bottom,
                        "cell_right": right,
                        "cell_top": top,
                        "cell_area_ha": cell_area_ha,
                        "coverage_fraction_of_cell": float(fraction),
                        "normalized_polygon_weight": float(weight),
                        "valid": bool(is_valid),
                        "raw_dn": float(stored) if is_valid else None,
                        "scale": float(src.scales[0]),
                        "offset": float(src.offsets[0]),
                        "npp_rate_gC_m2_day": float(rate) if is_valid else None,
                        "weighted_rate_contribution": weighted_rate,
                        "seasonal_npp_contribution_gC_m2": (
                            weighted_rate * int(period.overlap_days)
                        ),
                    }
                )

    pixels = pd.DataFrame(pixel_rows)
    spatial_pixels = pd.DataFrame(pixel_features.values()).sort_values("local_cell_id")
    support_area_ha = float(spatial_pixels.cell_area_ha.sum())
    geometry_field_area_ha = float(spatial_pixels.field_intersection_area_ha.sum())
    spatial_support = {
        "field_id": args.field_id,
        "database_area_ha": float(field.area_ha),
        "geometry_area_from_pixel_intersections_ha": geometry_field_area_ha,
        "intersecting_source_pixels": int(len(spatial_pixels)),
        "total_fractional_pixels": total_coverage,
        "full_pixel_support_area_ha": support_area_ha,
        "field_to_full_pixel_support_ratio": (
            geometry_field_area_ha / support_area_ha if support_area_ha else None
        ),
        "maximum_field_fraction_of_one_pixel": float(
            spatial_pixels.coverage_fraction_of_cell.max()
        ),
        "dominant_polygon_weight": float(spatial_pixels.normalized_polygon_weight.max()),
        "interpretation": (
            "Fractional coverage weights the intersecting pixels correctly, but each WaPOR "
            "pixel value remains a mixed signal from its full footprint. The fractions measure "
            "geometric support and do not unmix the field from neighbouring land."
        ),
    }

    seasonal_from_dekads = float(npp.npp_contribution_gC_m2.sum())
    seasonal_from_pixels = float(pixels.seasonal_npp_contribution_gC_m2.sum())
    checks = {
        "dekad_rows": int(len(npp)),
        "used_dekads": int(npp.used.sum()),
        "pixel_trace_rows": int(len(pixels)),
        "seasonal_npp_pipeline_gC_m2": float(field.seasonal_npp_gC_m2),
        "seasonal_npp_from_dekads_gC_m2": seasonal_from_dekads,
        "seasonal_npp_from_pixels_gC_m2": seasonal_from_pixels,
        "dekad_difference_gC_m2": seasonal_from_dekads - float(field.seasonal_npp_gC_m2),
        "pixel_difference_gC_m2": seasonal_from_pixels - float(field.seasonal_npp_gC_m2),
        "dekad_reconciliation": bool(
            np.isclose(seasonal_from_dekads, float(field.seasonal_npp_gC_m2), atol=1e-3)
        ),
        "pixel_reconciliation": bool(
            np.isclose(seasonal_from_pixels, float(field.seasonal_npp_gC_m2), atol=1e-3)
        ),
    }
    if not checks["dekad_reconciliation"] or not checks["pixel_reconciliation"]:
        raise RuntimeError(f"NPP reconciliation failed: {checks}")

    field_summary = row_dict(field)
    field_summary["audit_field_selection"] = (
        "Nakuru maize 2020 LR; area below 1 ha; detected phenology; "
        "complete temporal and spatial NPP coverage; high crop confidence"
    )
    run_metadata = json.loads((run_dir / "run_manifest.json").read_text())
    (output_dir / "field_summary.json").write_text(json.dumps(field_summary, indent=2) + "\n")
    (output_dir / "spatial_support.json").write_text(json.dumps(spatial_support, indent=2) + "\n")
    (output_dir / "reconciliation_checks.json").write_text(json.dumps(checks, indent=2) + "\n")
    (output_dir / "run_metadata.json").write_text(json.dumps(run_metadata, indent=2) + "\n")
    pd.DataFrame([field_summary]).to_csv(output_dir / "field_summary.csv", index=False)
    npp.to_csv(output_dir / "dekadal_npp.csv", index=False)
    pixels.to_csv(output_dir / "pixel_trace.csv", index=False)
    pixels.to_parquet(output_dir / "pixel_trace.parquet", index=False)
    spatial_pixels.to_csv(output_dir / "spatial_pixels.csv", index=False)

    features = [
        {
            "type": "Feature",
            "geometry": mapping(field_geometry),
            "properties": {
                "feature_type": "field",
                "field_id": args.field_id,
                "database_area_ha": float(field.area_ha),
            },
        }
    ]
    for pixel in spatial_pixels.to_dict("records"):
        left, bottom, right, top = pixel.pop("bounds")
        features.append(
            {
                "type": "Feature",
                "geometry": mapping(box(left, bottom, right, top)),
                "properties": {"feature_type": "wapor_pixel", **pixel},
            }
        )
    geojson = {"type": "FeatureCollection", "features": features}
    (output_dir / "spatial_support.geojson").write_text(json.dumps(geojson) + "\n")
    print(json.dumps({"field": field_summary, "spatial_support": spatial_support, "checks": checks}, indent=2))


if __name__ == "__main__":
    main()
