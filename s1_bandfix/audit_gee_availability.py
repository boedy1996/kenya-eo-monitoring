#!/usr/bin/env python3
"""Check Earth Engine S1 polarization availability for selected weekly TIFF rows."""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import ee
import pandas as pd


DATE_RE = re.compile(r"^(?P<region>machakos|nakuru)_(?P<start>\d{4}-\d{2}-\d{2})_(?P<end>\d{4}-\d{2}-\d{2})_")
REGION_POINTS = {
    "machakos": (37.2634, -1.5177),
    "nakuru": (36.0800, -0.3031),
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--audit-csv", type=Path, required=True)
    parser.add_argument("--output-csv", type=Path, required=True)
    parser.add_argument("--project", default="ee-boedy1996")
    parser.add_argument(
        "--classifications",
        nargs="+",
        default=["band1_only", "ambiguous", "insufficient_paired_samples"],
    )
    args = parser.parse_args()

    ee.Initialize(project=args.project)
    source = pd.read_csv(args.audit_csv)
    source = source[source["classification"].isin(args.classifications)].copy()

    features = []
    for row in source.itertuples(index=False):
        match = DATE_RE.match(row.file)
        if not match:
            raise ValueError(f"Cannot parse weekly filename: {row.file}")
        region = match.group("region")
        lon, lat = REGION_POINTS[region]
        features.append(
            ee.Feature(
                ee.Geometry.Point([lon, lat]),
                {
                    "file": row.file,
                    "region": region,
                    "start": match.group("start"),
                    "end": match.group("end"),
                    "local_classification": row.classification,
                },
            )
        )

    base = (
        ee.ImageCollection("COPERNICUS/S1_GRD_FLOAT")
        .filter(ee.Filter.eq("instrumentMode", "IW"))
        .filter(ee.Filter.eq("resolution_meters", 10))
    )

    def summarize(feature: ee.Feature) -> ee.Feature:
        feature = ee.Feature(feature)
        start = ee.Date(feature.get("start"))
        end_exclusive = ee.Date(feature.get("end")).advance(1, "day")
        collection = base.filterBounds(feature.geometry()).filterDate(start, end_exclusive)
        vv = collection.filter(ee.Filter.listContains("transmitterReceiverPolarisation", "VV"))
        dual = vv.filter(ee.Filter.listContains("transmitterReceiverPolarisation", "VH"))
        vv_only = vv.filter(ee.Filter.Not(ee.Filter.listContains("transmitterReceiverPolarisation", "VH")))
        return feature.set(
            {
                "gee_total_images": collection.size(),
                "gee_vv_images": vv.size(),
                "gee_dual_vv_vh_images": dual.size(),
                "gee_vv_only_images": vv_only.size(),
                "gee_product_ids": collection.aggregate_array("system:index"),
            }
        )

    result = ee.FeatureCollection(features).map(summarize).getInfo()
    rows = []
    for feature in result["features"]:
        props = feature["properties"]
        props["gee_product_ids"] = "|".join(props.get("gee_product_ids", []))
        rows.append(props)
    frame = pd.DataFrame(rows).sort_values(["start", "region"])
    args.output_csv.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(args.output_csv, index=False)
    print(frame.groupby("local_classification")[["gee_total_images", "gee_dual_vv_vh_images", "gee_vv_only_images"]].sum())
    print(f"Wrote {len(frame)} rows to {args.output_csv}")


if __name__ == "__main__":
    main()
