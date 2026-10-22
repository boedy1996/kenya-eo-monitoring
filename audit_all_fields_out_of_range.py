#!/usr/bin/env python3
"""Audit raw model inputs that violate the delivered scorer's physical ranges."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds


RANGES = {
    "s2_ndvi_mean": (-1.0, 1.0),
    "s2_evi_mean": (-1.0, 1.0),
    "s2_lai_mean": (0.0, 10.0),
    "s1_sigma_vv_linear_mean": (0.0, 10.0),
    "s1_sigma_vh_linear_mean": (0.0, 10.0),
}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--run-dir", type=Path, required=True)
    args = parser.parse_args()
    run_dir = args.run_dir.resolve()
    dataset = ds.dataset(run_dir / "inputs", format="parquet")

    expression = None
    for column, (lower, upper) in RANGES.items():
        condition = (ds.field(column) < lower) | (ds.field(column) > upper)
        expression = condition if expression is None else expression | condition
    columns = ["field_id", "week_start", "county", *RANGES]
    frame = dataset.to_table(columns=columns, filter=expression).to_pandas()
    frame = frame.sort_values(["county", "field_id", "week_start"]).reset_index(drop=True)

    output_dataset = ds.dataset(run_dir / "outputs", format="parquet")
    affected_fields = frame["field_id"].drop_duplicates().tolist()
    output = output_dataset.to_table(
        columns=["field_id", "week_start", "reason", "label"],
        filter=ds.field("field_id").isin(affected_fields),
    ).to_pandas()
    frame["week_start"] = pd.to_datetime(frame["week_start"])
    output["week_start"] = pd.to_datetime(output["week_start"])
    frame = frame.merge(output, on=["field_id", "week_start"], how="left", validate="one_to_one")

    causes = []
    summary = {}
    for column, (lower, upper) in RANGES.items():
        bad = frame[column].notna() & ((frame[column] < lower) | (frame[column] > upper))
        frame[f"bad_{column}"] = bad
        causes.append(frame.loc[bad, ["field_id", "week_start", "county", column]].assign(column=column))
        values = frame.loc[bad, column]
        summary[column] = {
            "rows": int(bad.sum()),
            "min": None if values.empty else float(values.min()),
            "max": None if values.empty else float(values.max()),
            "lower_bound": lower,
            "upper_bound": upper,
        }

    output_csv = run_dir / "out_of_range_raw_rows.csv"
    frame.to_csv(output_csv, index=False)
    report = {
        "rows": len(frame),
        "fields": int(frame["field_id"].nunique()),
        "min_week": None if frame.empty else str(frame["week_start"].min()),
        "max_week": None if frame.empty else str(frame["week_start"].max()),
        "by_county": {str(k): int(v) for k, v in frame["county"].value_counts().items()},
        "by_output_reason": {str(k): int(v) for k, v in frame["reason"].value_counts().items()},
        "by_output_label": {str(k): int(v) for k, v in frame["label"].value_counts().items()},
        "by_column": summary,
        "csv": str(output_csv),
    }
    report_path = run_dir / "out_of_range_audit.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
