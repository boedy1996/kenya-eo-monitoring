import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import rasterio
from rasterio.transform import from_origin
from shapely.geometry import box

from load_zonal_v2 import copy_part_sql, parquet_to_csv


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPT = PROJECT_ROOT / "zonal_center_pipeline.py"


class ZonalCenterPipelineIntegrationTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.gpkg = self.root / "fields.gpkg"
        self.s1 = self.root / "s1"
        self.s2 = self.root / "s2"
        self.run_dir = self.root / "run"
        self.s1.mkdir()
        self.s2.mkdir()

        for region, field_id in (("machakos", "MA1"), ("nakuru", "NA1")):
            frame = gpd.GeoDataFrame(
                {"field_id": [field_id]},
                geometry=[box(0, 20, 20, 40)],
                crs="EPSG:32737",
            )
            frame.to_file(self.gpkg, layer=f"{region}_fin", driver="GPKG", engine="pyogrio")
            self._write_raster(self.s1 / f"{region}_2023-01-02_2023-01-08_weekly_clean_10m_indices.tif", 5)
            self._write_raster(self.s2 / f"{region}_2023-01-02_2023-01-08_weekly_indices_10m.tif", 7)

    def tearDown(self):
        self.temporary.cleanup()

    @staticmethod
    def _write_raster(path: Path, band_count: int):
        values = np.stack(
            [np.arange(16, dtype=np.float32).reshape(4, 4) + band * 100 for band in range(band_count)]
        )
        values[0, 0, 0] = -9999
        descriptions = (
            ["sigma_vv_linear", "sigma_vh_linear", "p_ratio", "RVI", "RCSPR"]
            if band_count == 5
            else ["NDVI", "EVI", "NDMI", "NDRE", "MSI", "PVR", "LAI"]
        )
        with rasterio.open(
            path,
            "w",
            driver="GTiff",
            width=4,
            height=4,
            count=band_count,
            dtype="float32",
            crs="EPSG:32737",
            transform=from_origin(0, 40, 10, 10),
            nodata=-9999,
        ) as dataset:
            dataset.write(values)
            dataset.descriptions = tuple(descriptions)

    def run_command(self, *arguments: str):
        result = subprocess.run(
            [sys.executable, str(SCRIPT), *arguments],
            cwd=PROJECT_ROOT,
            text=True,
            capture_output=True,
        )
        if result.returncode:
            self.fail(f"Command failed ({result.returncode}):\n{result.stdout}\n{result.stderr}")

    def test_prepare_extract_and_validate(self):
        self.run_command(
            "prepare",
            "--run-dir", str(self.run_dir),
            "--gpkg-path", str(self.gpkg),
            "--s1-input-dir", str(self.s1),
            "--s2-input-dir", str(self.s2),
            "--min-pixels", "4",
            "--tile-size", "2",
        )
        common = pd.read_parquet(self.run_dir / "manifest" / "fields_common.parquet")
        self.assertEqual(len(common), 2)
        self.assertTrue((common["buffer_width_m"] == 0).all())
        self.assertTrue((common["s1_pixel_count_after"] == 4).all())
        self.assertTrue((common["s2_pixel_count_after"] == 4).all())

        self.run_command(
            "extract",
            "--run-dir", str(self.run_dir),
            "--sensor", "both",
            "--workers", "1",
            "--batch-rows", "2",
            "--duckdb-memory-limit", "1GB",
        )
        self.run_command("validate", "--run-dir", str(self.run_dir), "--sensor", "both", "--full")

        output = next((self.run_dir / "parts" / "s2" / "region=machakos").rglob("*.parquet"))
        frame = pd.read_parquet(output)
        self.assertEqual(frame.loc[0, "ndvi_count"], 3)
        self.assertAlmostEqual(frame.loc[0, "ndvi_mean"], (1 + 4 + 5) / 3, places=5)
        self.assertEqual(frame.loc[0, "pixel_count_before"], 4)
        self.assertEqual(frame.loc[0, "pixel_count_after"], 4)

        csv_path = self.root / "database-copy.csv"
        copied = parquet_to_csv(output, csv_path, "s2", batch_rows=1)
        self.assertEqual(copied, 1)
        self.assertEqual(len(csv_path.read_text().splitlines()), 1)
        copy_sql = copy_part_sql(
            "s2_indices__z_test", frame.columns.tolist(), csv_path,
            "z_test", "s2", "part.parquet", "raster.tif", copied,
        )
        self.assertIn("NULL '\\N'", copy_sql)
        self.assertIn("BEGIN;", copy_sql)
        self.assertIn("COMMIT;", copy_sql)

        report = json.loads((self.run_dir / "validation_report.json").read_text())
        self.assertEqual(report["status"], "passed")


if __name__ == "__main__":
    unittest.main()
