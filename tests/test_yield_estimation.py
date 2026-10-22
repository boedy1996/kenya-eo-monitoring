from __future__ import annotations

import unittest
from datetime import date

import numpy as np
import pandas as pd

from yield_estimation.core import (
    calculate_yield,
    dekads_between,
    derive_phenology,
    derive_phenology_matrix,
    integrate_field_npp,
    season_bounds,
)
from yield_estimation.pipeline import (
    classify_intercrop,
    decode_exactextract_mean,
    survey_crop_matches,
)
from yield_estimation.production import _integrate_npp_table, weighted_raster_statistics


class YieldCoreTests(unittest.TestCase):
    def test_season_and_dekad_calendar(self):
        self.assertEqual(season_bounds("2023_lr"), (date(2023, 3, 1), date(2023, 8, 31)))
        self.assertEqual(season_bounds("2023_sr"), (date(2023, 10, 1), date(2024, 2, 29)))
        periods = dekads_between(date(2023, 3, 1), date(2023, 3, 31))
        self.assertEqual([p["days_in_period"] for p in periods], [10, 10, 11])

    def test_workbook_maize_example(self):
        tbp = 300 * 0.022222
        result = calculate_yield(tbp, "maize")
        self.assertAlmostEqual(result["low"], 4.1599584, places=6)
        self.assertAlmostEqual(result["central"], 4.457098285714285, places=6)
        self.assertAlmostEqual(result["high"], 4.771716988235294, places=6)

    def test_npp_integration_uses_partial_dekads(self):
        npp = pd.DataFrame(
            {
                "period_start": pd.to_datetime(["2023-03-01", "2023-03-11", "2023-03-21"]),
                "period_end": pd.to_datetime(["2023-03-10", "2023-03-20", "2023-03-31"]),
                "npp_rate_gC_m2_day": [1.0, 2.0, 3.0],
                "coverage_fraction": [1.0, 1.0, 1.0],
                "effective_pixel_count": [0.4, 0.4, 0.4],
            }
        )
        result = integrate_field_npp(npp, date(2023, 3, 5), date(2023, 3, 25))
        self.assertAlmostEqual(result["seasonal_npp_gC_m2"], 6 + 20 + 15)
        self.assertEqual(result["npp_valid_days"], 21)
        self.assertAlmostEqual(result["npp_temporal_coverage"], 1.0)

    def test_phenology_detects_clear_cycle(self):
        dates = pd.date_range("2023-03-06", periods=25, freq="7D")
        curve = np.array(
            [0.10, 0.11, 0.13, 0.18, 0.30, 0.48, 0.62, 0.72, 0.78, 0.82,
             0.84, 0.82, 0.79, 0.72, 0.62, 0.48, 0.34, 0.22, 0.15, 0.12,
             0.10, 0.09, 0.09, 0.08, 0.08]
        )
        weekly = pd.DataFrame(
            {
                "week_start": dates,
                "ndvi_median": curve,
                "evi_median": curve * 0.8,
                "lai_median": curve * 4,
                "rvi_median": curve * 0.5,
            }
        )
        result = derive_phenology(weekly, date(2023, 3, 1), date(2023, 8, 31))
        self.assertEqual(result["qa_status"], "detected")
        self.assertLess(result["sos"], result["peak_date"])
        self.assertLess(result["peak_date"], result["eos"])

    def test_missing_survey_values_do_not_become_mixed(self):
        row = pd.Series({"cropping_pattern": np.nan, "croptype_mixed": np.nan})
        self.assertEqual(classify_intercrop(row), "unknown")
        self.assertEqual(
            classify_intercrop(pd.Series({"croptype_mixed_2": "maize + beans"}), "lr"),
            "yes",
        )
        row = pd.Series(
            {
                "cropping_pattern": "mixed_cropping",
                "croptype_mixed": "maize beans",
                "cropping_pattern_2": "mono_culture",
                "croptype_mono_2": "maize",
            }
        )
        self.assertEqual(classify_intercrop(row, "lr"), "no")
        self.assertEqual(classify_intercrop(row, "sr"), "yes")
        self.assertTrue(survey_crop_matches("potatoes", "irish_potato"))
        self.assertFalse(survey_crop_matches("beans", "maize"))
        self.assertIsNone(survey_crop_matches("maize", np.nan))

    def test_exactextract_mean_is_already_scaled(self):
        raw, physical = decode_exactextract_mean(pd.Series([2.5, np.nan]), 0.001, 0.0)
        self.assertAlmostEqual(physical.iloc[0], 2.5)
        self.assertAlmostEqual(raw.iloc[0], 2500.0)
        self.assertTrue(pd.isna(physical.iloc[1]))

    def test_matrix_phenology_detects_clear_cycle(self):
        dates = pd.date_range("2023-03-06", periods=25, freq="7D")
        curve = np.array(
            [0.10, 0.11, 0.13, 0.18, 0.30, 0.48, 0.62, 0.72, 0.78, 0.82,
             0.84, 0.82, 0.79, 0.72, 0.62, 0.48, 0.34, 0.22, 0.15, 0.12,
             0.10, 0.09, 0.09, 0.08, 0.08], dtype=np.float32
        )[None, :]
        result = derive_phenology_matrix(
            np.array(["MA1"]), dates.to_numpy(), curve, curve * 0.8,
            curve * 4, curve * 0.5, date(2023, 3, 1), date(2023, 8, 31),
        ).iloc[0]
        self.assertEqual(result.qa_status, "detected")
        self.assertLess(result.sos, result.peak_date)
        self.assertLess(result.peak_date, result.eos)

    def test_cached_coverage_weights_match_weighted_mean(self):
        raw = np.array([[1000, 3000], [-9999, 5000]], dtype=np.int16)
        field_index = np.array([0, 0, 1, 1], dtype=np.int32)
        cell_id = np.array([0, 1, 2, 3], dtype=np.int64)
        coverage = np.array([0.25, 0.75, 0.5, 0.5], dtype=np.float32)
        total = np.array([1.0, 1.0], dtype=np.float32)
        mean, pixels, effective, fraction = weighted_raster_statistics(
            raw, -9999, 0.001, 0.0, field_index, cell_id, coverage, total
        )
        self.assertAlmostEqual(mean[0], 2.5, places=6)
        self.assertAlmostEqual(mean[1], 5.0, places=6)
        self.assertEqual(pixels.tolist(), [2, 1])
        self.assertAlmostEqual(effective[1], 0.5)
        self.assertAlmostEqual(fraction[1], 0.5)

    def test_vectorized_npp_integration_respects_partial_dekads(self):
        npp = pd.DataFrame(
            {
                "field_id": ["field-a", "field-a"],
                "period_start": ["2023-03-01", "2023-03-11"],
                "period_end": ["2023-03-10", "2023-03-20"],
                "npp_rate_gC_m2_day": [2.0, 3.0],
                "coverage_fraction": [1.0, 0.5],
                "effective_pixel_count": [4.0, 2.0],
            }
        )
        phenology = pd.DataFrame(
            {
                "field_id": ["field-a"],
                "sos": [pd.Timestamp("2023-03-06")],
                "eos": [pd.Timestamp("2023-03-15")],
            }
        )
        result = _integrate_npp_table(
            npp, phenology, date(2023, 3, 1), date(2023, 8, 31)
        ).iloc[0]
        self.assertAlmostEqual(result["seasonal_npp_gC_m2"], 25.0)
        self.assertEqual(result["npp_valid_days"], 10)
        self.assertAlmostEqual(result["npp_temporal_coverage"], 1.0)
        self.assertAlmostEqual(result["npp_mean_spatial_coverage"], 0.75)


if __name__ == "__main__":
    unittest.main()
