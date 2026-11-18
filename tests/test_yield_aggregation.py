from __future__ import annotations

from datetime import date

import pandas as pd

from dashboard.scripts.export_yield_aggregation import (
    aggregation_method_version,
    aggregation_query,
    area_threshold_token,
    validate_export,
)
from yield_estimation.export_small_field_audit import overlap_days


def test_area_weighted_export_validation_reconciles() -> None:
    rows = [
        {
            "yield_run_id": "run-1",
            "season_code": "2020_lr",
            "county": "Nakuru",
            "crop_label": "maize",
            "calculated_fields": 2,
            "mapped_area_ha": 4.0,
            "production_central_t": 24.0,
            "yield_central_area_weighted_t_ha": 6.0,
        },
        {
            "yield_run_id": "run-1",
            "season_code": "2020_sr",
            "county": "Nakuru",
            "crop_label": "maize",
            "calculated_fields": 2,
            "mapped_area_ha": 5.0,
            "production_central_t": 35.0,
            "yield_central_area_weighted_t_ha": 7.0,
        },
    ]
    report = validate_export(rows, expected_seasons=2)
    assert report["status"] == "passed"
    assert report["maximum_reconciliation_difference"] == 0


def test_minimum_area_export_has_distinct_guard_and_query() -> None:
    assert area_threshold_token(1.2) == "1p2"
    assert (
        aggregation_method_version(1.2)
        == "county-yield-area-weighted-min-1p2ha-v1"
    )
    assert aggregation_method_version(None) == "county-yield-area-weighted-v1"
    assert "y.area_ha >= %(minimum_field_area_ha)s" in aggregation_query(1.2)
    assert "minimum_field_area_ha" not in aggregation_query()


def test_partial_dekad_overlap_is_inclusive() -> None:
    assert overlap_days(
        pd.Timestamp(date(2020, 4, 11)),
        pd.Timestamp(date(2020, 4, 20)),
        pd.Timestamp(date(2020, 4, 20)),
        pd.Timestamp(date(2020, 7, 27)),
    ) == 1
    assert overlap_days(
        pd.Timestamp(date(2020, 7, 21)),
        pd.Timestamp(date(2020, 7, 31)),
        pd.Timestamp(date(2020, 4, 20)),
        pd.Timestamp(date(2020, 7, 27)),
    ) == 7
