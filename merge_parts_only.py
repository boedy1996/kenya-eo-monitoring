#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import glob
import argparse
from datetime import datetime
from typing import Optional, List

import pandas as pd

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
except ImportError:
    print("ERROR: pyarrow belum terinstall. Install dulu: pip install pyarrow")
    sys.exit(1)

try:
    from tqdm import tqdm
except ImportError:
    print("ERROR: tqdm belum terinstall. Install dulu: pip install tqdm")
    sys.exit(1)


BASE_STRING_COLS = [
    "uid",
    "region",
    "layer_name",
    "source",
    "raster_file",
    "start_date",
    "end_date",
]

BASE_INT_COLS = [
    "year",
    "weekNo",
]

STAT_SUFFIXES = (
    "_mean",
    "_median",
    "_min",
    "_max",
    "_std",
    "_count",
)


def log(msg: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {msg}", flush=True)


def safe_mkdir(path: Optional[str]) -> None:
    if path:
        os.makedirs(path, exist_ok=True)


def parse_args():
    p = argparse.ArgumentParser(
        description="Merge parquet parts menjadi final parquet/csv dengan schema yang diseragamkan."
    )
    p.add_argument("--parts-dir", required=True, help="Folder berisi parquet parts")
    p.add_argument("--final-parquet", required=True, help="Path output parquet final")
    p.add_argument("--final-csv", help="Path output csv final")
    p.add_argument("--skip-csv", action="store_true", help="Skip export CSV")
    p.add_argument("--overwrite", action="store_true", help="Overwrite output jika sudah ada")
    return p.parse_args()


def list_part_files(parts_dir: str) -> List[str]:
    return sorted(glob.glob(os.path.join(parts_dir, "*.parquet")))


def standardize_dataframe_schema(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in BASE_STRING_COLS:
        if col in df.columns:
            df[col] = df[col].astype("string")

    for col in BASE_INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    stat_cols = [c for c in df.columns if c.endswith(STAT_SUFFIXES)]
    for col in stat_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    return df


def infer_target_column_order(part_files: List[str]) -> List[str]:
    """
    Ambil union semua kolom dari seluruh parquet parts, lalu susun urutan kolom:
    - base string
    - base int
    - sisanya urut alfabet
    """
    all_cols = set()

    log("Membaca union schema kolom dari seluruh parquet parts...")
    for fp in tqdm(part_files, desc="Scanning schemas", unit="file"):
        df = pd.read_parquet(fp)
        all_cols.update(df.columns.tolist())
        del df

    ordered = []
    for c in BASE_STRING_COLS:
        if c in all_cols:
            ordered.append(c)
    for c in BASE_INT_COLS:
        if c in all_cols:
            ordered.append(c)

    other_cols = sorted([c for c in all_cols if c not in ordered])
    ordered.extend(other_cols)
    return ordered


def build_target_schema(columns: List[str]) -> pa.Schema:
    fields = []
    for col in columns:
        if col in BASE_STRING_COLS:
            fields.append(pa.field(col, pa.string()))
        elif col in BASE_INT_COLS:
            fields.append(pa.field(col, pa.int64()))
        else:
            fields.append(pa.field(col, pa.float64()))
    return pa.schema(fields)


def align_dataframe_to_columns(df: pd.DataFrame, target_columns: List[str]) -> pd.DataFrame:
    df = df.copy()

    for col in target_columns:
        if col not in df.columns:
            if col in BASE_STRING_COLS:
                df[col] = pd.Series([pd.NA] * len(df), dtype="string")
            elif col in BASE_INT_COLS:
                df[col] = pd.Series([pd.NA] * len(df), dtype="Int64")
            else:
                df[col] = pd.Series([float("nan")] * len(df), dtype="float64")

    df = df[target_columns]
    return df


def finalize_dtypes_for_arrow(df: pd.DataFrame, target_columns: List[str]) -> pd.DataFrame:
    """
    Pastikan dtype konsisten persis dengan target schema sebelum diubah ke pyarrow table.
    """
    df = df.copy()

    for col in target_columns:
        if col in BASE_STRING_COLS:
            df[col] = df[col].astype("string")
        elif col in BASE_INT_COLS:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
        else:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")

    return df


def merge_parquet_parts(
    parts_dir: str,
    final_parquet: str,
    final_csv: Optional[str],
    overwrite: bool,
    skip_csv: bool,
) -> None:
    part_files = list_part_files(parts_dir)
    if not part_files:
        raise FileNotFoundError(f"Tidak ada parquet parts di: {parts_dir}")

    if os.path.exists(final_parquet) and not overwrite:
        raise FileExistsError(f"Final parquet sudah ada: {final_parquet}")

    if final_csv and (not skip_csv) and os.path.exists(final_csv) and not overwrite:
        raise FileExistsError(f"Final CSV sudah ada: {final_csv}")

    safe_mkdir(os.path.dirname(final_parquet))
    if final_csv and not skip_csv:
        safe_mkdir(os.path.dirname(final_csv))

    target_columns = infer_target_column_order(part_files)
    target_schema = build_target_schema(target_columns)

    log(f"Total part files: {len(part_files)}")
    log(f"Total target columns: {len(target_columns)}")

    writer = None
    first_csv = True
    total_rows = 0
    success_files = 0
    failed_files = 0

    try:
        for fp in tqdm(part_files, desc="Merging parquet parts", unit="file"):
            try:
                df = pd.read_parquet(fp)

                if len(df) == 0:
                    success_files += 1
                    continue

                df = standardize_dataframe_schema(df)
                df = align_dataframe_to_columns(df, target_columns)
                df = finalize_dtypes_for_arrow(df, target_columns)

                table = pa.Table.from_pandas(
                    df,
                    preserve_index=False,
                    schema=target_schema
                )

                if writer is None:
                    writer = pq.ParquetWriter(final_parquet, target_schema, compression="snappy")

                writer.write_table(table)

                if final_csv and not skip_csv:
                    df.to_csv(
                        final_csv,
                        mode="a",
                        index=False,
                        header=first_csv
                    )
                    first_csv = False

                total_rows += len(df)
                success_files += 1

                del df
                del table

            except Exception as e:
                failed_files += 1
                log(f"ERROR merge file {os.path.basename(fp)} -> {e}")

    finally:
        if writer is not None:
            writer.close()

    log(f"Merge selesai.")
    log(f"Berhasil : {success_files} file")
    log(f"Gagal    : {failed_files} file")
    log(f"Total row: {total_rows:,}")

    if failed_files > 0:
        log("Ada file yang gagal di-merge. Periksa log error di atas.")


def main():
    args = parse_args()

    try:
        merge_parquet_parts(
            parts_dir=args.parts_dir,
            final_parquet=args.final_parquet,
            final_csv=args.final_csv,
            overwrite=args.overwrite,
            skip_csv=args.skip_csv,
        )
    except Exception as e:
        log(f"FATAL ERROR: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()