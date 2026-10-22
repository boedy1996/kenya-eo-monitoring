#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import shutil
import tempfile
import zipfile
from pathlib import Path


BASE_FILES = (
    "README.md",
    "s2_four_cited_ndvi_comparison.csv",
    "s2_four_cited_observations.csv",
    "four_fields_buffer_audit.csv",
    "production_buffer_summary.csv",
    "production_vs_gee_residual_summary.csv",
    "production_validation_summary.csv",
)

AUDIT_COPIES = {
    "gee_availability_nonstandard_weeks.csv": "s1_gee_availability_nonstandard_weeks.csv",
    "s1_corrected_vs_gee_pilot.csv": "s1_corrected_vs_gee_pilot.csv",
    "s1_corrected_vs_gee_summary.json": "s1_corrected_vs_gee_summary.json",
    "postload_semantic_audit.csv": "s1_postload_semantic_audit.csv",
    "postload_field_comparison.csv": "s1_previous_vs_corrected_fields.csv",
    "post_swap_checks.txt": "s1_post_swap_checks.txt",
    "s1_extraction_validation_report.json": "s1_extraction_validation_report.json",
    "s1_database_validation_report.json": "s1_database_validation_report.json",
}


def sanitize_weekly_csv(source: Path, destination: Path) -> None:
    with source.open(newline="", encoding="utf-8") as source_file:
        reader = csv.DictReader(source_file)
        fieldnames = [name for name in (reader.fieldnames or []) if name != "path"]
        with destination.open("w", newline="", encoding="utf-8") as destination_file:
            writer = csv.DictWriter(destination_file, fieldnames=fieldnames)
            writer.writeheader()
            for row in reader:
                writer.writerow({name: row.get(name, "") for name in fieldnames})


def sanitize_weekly_summary(source: Path, destination: Path) -> None:
    payload = json.loads(source.read_text(encoding="utf-8"))
    payload.pop("input_dir", None)
    destination.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def build(args: argparse.Namespace) -> None:
    audit_dir = args.audit_dir.resolve()
    workbook = args.workbook.resolve()
    delivery_root = args.delivery_root.resolve()
    package_name = "giuseppe_audit_package_2026-09-28"
    package_dir = delivery_root / package_name
    zip_path = delivery_root / f"{package_name}.zip"
    delivery_root.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory(prefix="giuseppe-package-", dir=delivery_root) as temp_name:
        temp_dir = Path(temp_name)
        for name in BASE_FILES:
            shutil.copy2(delivery_root / name, temp_dir / name)
        shutil.copy2(workbook, temp_dir / "giuseppe_audit_package.xlsx")
        for source_name, destination_name in AUDIT_COPIES.items():
            shutil.copy2(audit_dir / source_name, temp_dir / destination_name)
        sanitize_weekly_csv(
            audit_dir / "weekly_tiff_band_audit.csv",
            temp_dir / "s1_weekly_band_audit.csv",
        )
        sanitize_weekly_summary(
            audit_dir / "weekly_tiff_band_audit_summary.json",
            temp_dir / "s1_weekly_band_audit_summary.json",
        )

        if package_dir.exists():
            shutil.rmtree(package_dir)
        temp_dir.rename(package_dir)

    shutil.copy2(workbook, delivery_root / "giuseppe_audit_package.xlsx")
    for item in package_dir.iterdir():
        if item.name != "giuseppe_audit_package.xlsx":
            shutil.copy2(item, delivery_root / item.name)

    temporary_zip = zip_path.with_suffix(".zip.tmp")
    with zipfile.ZipFile(temporary_zip, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for item in sorted(package_dir.iterdir()):
            archive.write(item, arcname=f"{package_name}/{item.name}")
    temporary_zip.replace(zip_path)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--audit-dir", type=Path, required=True)
    parser.add_argument("--workbook", type=Path, required=True)
    parser.add_argument("--delivery-root", type=Path, required=True)
    build(parser.parse_args())


if __name__ == "__main__":
    main()
