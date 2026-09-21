import pandas as pd
import psycopg
from pathlib import Path

DB_DSN = "dbname=kenya_monitoring user=dev"

OUTPUT_PARQUET = Path(
    "/opt/project/eo_775_fields_2021_2023_all_columns.parquet"
)

DATE_FROM = "2021-01-01"
DATE_TO = "2024-01-01"

EXPECTED_FIELDS = 775
EXPECTED_WEEKS = 156
EXPECTED_ROWS = EXPECTED_FIELDS * EXPECTED_WEEKS

print("Connecting to PostgreSQL...")

conn = psycopg.connect(DB_DSN)

# ============================================================
# 1. Ambil semua nama kolom S1 dan S2
# ============================================================

column_sql = """
SELECT
    table_name,
    column_name,
    ordinal_position
FROM information_schema.columns
WHERE table_schema = 'timeseries'
  AND table_name IN ('s1_indices', 's2_indices')
ORDER BY table_name, ordinal_position;
"""

cols = pd.read_sql_query(column_sql, conn)

s1_cols = (
    cols[cols["table_name"] == "s1_indices"]
    .sort_values("ordinal_position")["column_name"]
    .tolist()
)

s2_cols = (
    cols[cols["table_name"] == "s2_indices"]
    .sort_values("ordinal_position")["column_name"]
    .tolist()
)

print(f"S1 columns: {len(s1_cols)}")
print(f"S2 columns: {len(s2_cols)}")

print("\nS1:")
for c in s1_cols:
    print("  ", c)

print("\nS2:")
for c in s2_cols:
    print("  ", c)

# ============================================================
# 2. Prefix seluruh kolom agar tidak bentrok
# ============================================================

s1_select = [
    f's1."{c}" AS "s1_{c}"'
    for c in s1_cols
]

s2_select = [
    f's2."{c}" AS "s2_{c}"'
    for c in s2_cols
]

all_columns = ",\n    ".join(
    s1_select + s2_select
)

# ============================================================
# 3. Query complete field × week grid
# ============================================================

query = f"""
SELECT
    g.field_id,
    g.week_start,
    g.week_end,
    g.iso_year,
    g.week_no,

    {all_columns}

FROM staging.extract_grid_2021_2023 AS g

LEFT JOIN timeseries.s1_indices AS s1
  ON s1.field_id = g.field_id
 AND s1.start_date = g.week_start
 AND s1.start_date >= DATE '{DATE_FROM}'
 AND s1.start_date <  DATE '{DATE_TO}'

LEFT JOIN timeseries.s2_indices AS s2
  ON s2.field_id = g.field_id
 AND s2.start_date = g.week_start
 AND s2.start_date >= DATE '{DATE_FROM}'
 AND s2.start_date <  DATE '{DATE_TO}'

ORDER BY
    g.field_id,
    g.week_start;
"""

print("\nRunning extraction query...")

df = pd.read_sql_query(query, conn)

conn.close()

# ============================================================
# 4. QA dasar
# ============================================================

print("\n================ QA ================")

print("Rows:", len(df))
print("Columns:", len(df.columns))
print("Fields:", df["field_id"].nunique())
print("Weeks:", df["week_start"].nunique())

assert len(df) == EXPECTED_ROWS, (
    f"ERROR rows: {len(df)} != {EXPECTED_ROWS}"
)

assert df["field_id"].nunique() == EXPECTED_FIELDS, (
    f"ERROR fields: {df['field_id'].nunique()} != {EXPECTED_FIELDS}"
)

assert df["week_start"].nunique() == EXPECTED_WEEKS, (
    f"ERROR weeks: {df['week_start'].nunique()} != {EXPECTED_WEEKS}"
)

duplicates = df.duplicated(
    subset=["field_id", "week_start"]
).sum()

print("Duplicate field-week:", duplicates)

assert duplicates == 0

weeks_per_field = (
    df.groupby("field_id")
      .size()
)

bad_fields = weeks_per_field[
    weeks_per_field != EXPECTED_WEEKS
]

print("Fields with wrong week count:", len(bad_fields))

assert len(bad_fields) == 0

# ============================================================
# 5. Tambahkan pixel_count
#
# HANYA pixel_count yang boleh di-fill menjadi 0.
# Feature lain tetap NULL.
# ============================================================

if "s2_ndvi_count" in df.columns:
    df["pixel_count"] = (
        df["s2_ndvi_count"]
        .fillna(0)
    )
else:
    raise RuntimeError(
        "s2_ndvi_count tidak ditemukan"
    )

# ============================================================
# 6. QA NULL
# ============================================================

print("\n============= NULL QA =============")

print(
    "s2_ndvi_mean NULL:",
    df["s2_ndvi_mean"].isna().sum()
)

print(
    "pixel_count == 0:",
    (df["pixel_count"] == 0).sum()
)

if "s1_rvi_mean" in df.columns:
    print(
        "s1_rvi_mean NULL:",
        df["s1_rvi_mean"].isna().sum()
    )

print("\nTop 30 columns by NULL count:")

print(
    df.isna()
      .sum()
      .sort_values(ascending=False)
      .head(30)
)

# ============================================================
# 7. Contoh minggu optical kosong
# ============================================================

empty_optical = df[
    df["s2_ndvi_mean"].isna()
][
    [
        "field_id",
        "week_start",
        "s2_ndvi_mean",
        "s2_evi_mean",
        "pixel_count",
    ]
].head(20)

print("\nExample optical-null weeks:")
print(empty_optical.to_string(index=False))

# ============================================================
# 8. Export Parquet
# ============================================================

print("\nWriting Parquet...")

df.to_parquet(
    OUTPUT_PARQUET,
    index=False,
    compression="zstd"
)

print("\nDONE")
print("Output:", OUTPUT_PARQUET)
print("Rows:", len(df))
print("Columns:", len(df.columns))
