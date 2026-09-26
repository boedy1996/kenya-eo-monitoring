# Recompute zonal statistics with adaptive centre pixels

This run replaces fractional polygon coverage with the agreed rule:

1. Use one common inward buffer for each field across S1 and S2.
2. Try 10 m, then 5 m, then 0 m.
3. A candidate buffer passes only when both sensor grids contain at least five
   pixel centres.
4. Keep fields that remain below five pixels at 0 m and set
   `below_min_pixel_threshold=true`.
5. Compute unweighted statistics from pixels whose centres fall inside the
   selected geometry.

The pipeline does not call Earth Engine.

## Files

- `zonal_center_pipeline.py`: source inventory, common buffer manifest, pixel
  membership, weekly extraction, and validation.
- `load_zonal_v2.py`: resumable PostgreSQL shadow-table loader and atomic swap.
- `run_zonal_production.sh`: server commands with the current production paths.
- `tests/test_zonal_center_pipeline.py`: synthetic end-to-end integration test.

## Local verification

```bash
cd /Users/boedy1996/kenya-eo-monitoring
pyenv local kenya-eo-monitoring
python -m unittest -v tests/test_zonal_center_pipeline.py
bash -n run_zonal_production.sh
```

## Deploy to the server

GitHub is recommended because it leaves an auditable version. Add only the new
pipeline files so unrelated working changes are not included:

```bash
cd /Users/boedy1996/kenya-eo-monitoring
git add zonal_center_pipeline.py load_zonal_v2.py run_zonal_production.sh \
  tests/test_zonal_center_pipeline.py ZONAL_RECOMPUTE_PRODUCTION.md
git commit -m "Add adaptive centre-pixel zonal pipeline"
git push origin main
```

Then update the server:

```bash
ssh -i ~/.ssh/server_victor.pem root@yield-forecast.geo-infinity.com
cd /opt/project
git status --short
git pull --ff-only origin main
chmod +x zonal_center_pipeline.py load_zonal_v2.py run_zonal_production.sh
```

The local workstation uses the `kenya-eo-monitoring` pyenv environment. The
server script uses the existing server environment at
`/root/.pyenv/versions/yield_estimate/bin/python`.

## Set the production run ID

Use the same value in every command:

```bash
cd /opt/project
export RUN_ID=adaptive-center-20260926-v1
export WORKERS=2
export DUCKDB_MEMORY_LIMIT=6GB
mkdir -p /opt/project/zonal_runs/logs
```

Start with two workers. Increase workers only after checking RAM and temporary
disk use during the pilot.

## 1. Run the pilot

The pilot uses the 775 audited fields from `/opt/project/field_id_list.txt` and
the week starting 2023-06-12, which is present for both sensors and regions.

```bash
nohup env RUN_ID="$RUN_ID" WORKERS=1 \
  ./run_zonal_production.sh pilot \
  > "/opt/project/zonal_runs/logs/${RUN_ID}-pilot.log" 2>&1 &
echo $!
```

Monitor it:

```bash
tail -f "/opt/project/zonal_runs/logs/${RUN_ID}-pilot.log"
```

Success ends with `Extraction completed without failures` and a validation
report whose status is `passed`.

The real-server verification on these 775 fields passed with 775 unique rows
per sensor. The common buffer distribution was 449 fields at 10 m, 258 at 5 m,
and 68 at 0 m; no field remained below five geometric pixels. This supersedes
the earlier diagnostic that reported one failing field: that diagnostic rounded
the candidate raster window length and omitted a valid row of pixel centres.

## 2. Prepare all fields

This inventories all rasters, verifies their grids, builds the adaptive buffer
for all 269,453 fields, and writes the reusable pixel membership.

```bash
nohup env RUN_ID="$RUN_ID" \
  ./run_zonal_production.sh prepare \
  > "/opt/project/zonal_runs/logs/${RUN_ID}-prepare.log" 2>&1 &
echo $!
```

Monitor it:

```bash
tail -f "/opt/project/zonal_runs/logs/${RUN_ID}-prepare.log"
```

Inspect the summary after it finishes:

```bash
env RUN_ID="$RUN_ID" ./run_zonal_production.sh status
```

Also inspect the recorded weekly gaps:

```bash
/root/.pyenv/versions/yield_estimate/bin/python -c \
  'import json,os; p=json.load(open("/opt/project/zonal_runs/"+os.environ["RUN_ID"]+"/run_manifest.json")); print(json.dumps(p["weekly_gaps"],indent=2))'
```

The current S2 inventory is missing three known Nakuru weeks starting
2025-05-26, 2025-06-02, and 2025-06-16. Restore those corrected index TIFFs
before `prepare`, or document that the final dataset intentionally omits them.
Do not copy the corresponding old zonal parts into this run.

## 3. Extract S1

```bash
nohup env RUN_ID="$RUN_ID" WORKERS="$WORKERS" DUCKDB_MEMORY_LIMIT="$DUCKDB_MEMORY_LIMIT" \
  ./run_zonal_production.sh extract-s1 \
  > "/opt/project/zonal_runs/logs/${RUN_ID}-extract-s1.log" 2>&1 &
echo $!
```

```bash
tail -f "/opt/project/zonal_runs/logs/${RUN_ID}-extract-s1.log"
```

Rerunning the same command resumes the run. A part is skipped only after its
Parquet row count and schema have been verified.

## 4. Extract S2

Run S2 after S1 to avoid competing for memory and disk bandwidth:

```bash
nohup env RUN_ID="$RUN_ID" WORKERS="$WORKERS" DUCKDB_MEMORY_LIMIT="$DUCKDB_MEMORY_LIMIT" \
  ./run_zonal_production.sh extract-s2 \
  > "/opt/project/zonal_runs/logs/${RUN_ID}-extract-s2.log" 2>&1 &
echo $!
```

```bash
tail -f "/opt/project/zonal_runs/logs/${RUN_ID}-extract-s2.log"
```

## 5. Validate every Parquet record

```bash
nohup env RUN_ID="$RUN_ID" \
  ./run_zonal_production.sh validate \
  > "/opt/project/zonal_runs/logs/${RUN_ID}-validate.log" 2>&1 &
echo $!
```

The full validation checks expected row counts, unique `(field_id,
start_date)` keys, buffer values, geometric counts, and every band count. Do not
start the database load unless `validation_report.json` says `passed`.

## 6. Prepare versioned database tables

This creates new shadow tables. It does not modify the current S1 or S2 tables.

```bash
nohup env RUN_ID="$RUN_ID" \
  ./run_zonal_production.sh database-prepare \
  > "/opt/project/zonal_runs/logs/${RUN_ID}-db-prepare.log" 2>&1 &
echo $!
```

## 7. Load the database

```bash
nohup env RUN_ID="$RUN_ID" \
  ./run_zonal_production.sh database-load \
  > "/opt/project/zonal_runs/logs/${RUN_ID}-db-load.log" 2>&1 &
echo $!
```

Each weekly part is committed with its checkpoint. Rerunning the command safely
resumes from the next unloaded part. Temporary CSV files are deleted after each
commit.

## 8. Build indexes and validate shadow tables

```bash
nohup env RUN_ID="$RUN_ID" \
  ./run_zonal_production.sh database-finalize \
  > "/opt/project/zonal_runs/logs/${RUN_ID}-db-finalize.log" 2>&1 &
echo $!
```

```bash
nohup env RUN_ID="$RUN_ID" \
  ./run_zonal_production.sh database-validate \
  > "/opt/project/zonal_runs/logs/${RUN_ID}-db-validate.log" 2>&1 &
echo $!
```

Review:

```bash
cat "/opt/project/zonal_runs/${RUN_ID}/database_validation_report.json"
```

Both sensors must have status `passed`, expected row counts, zero bad buffers,
zero bad geometry counts, and zero bad valid counts.

## 9. Atomic production swap

Run this only after reviewing both validation reports:

```bash
/root/.pyenv/versions/yield_estimate/bin/python load_zonal_v2.py swap \
  --run-dir "/opt/project/zonal_runs/${RUN_ID}" \
  --database kenya_monitoring \
  --psql-command "sudo -u postgres psql" \
  --legacy-suffix pre_adaptive_20260926
```

S1 and S2 are renamed in one PostgreSQL transaction. The previous tables remain
available with the `legacy_pre_adaptive_20260926` suffix for rollback and audit.
Do not delete old tables, old Parquet outputs, or old CSV files until the
application and model checks have passed.

## Output columns

Every weekly S1 and S2 row contains:

- `buffer_width_m`: common physical buffer used for both sensors;
- `pixel_count_before`: geometric centre-pixel count for that sensor grid;
- `pixel_count_after`: geometric count after the common buffer;
- `below_min_pixel_threshold`: true when either sensor remains below five
  geometric pixels at 0 m;
- one integer `*_count` per band: pixels with valid weekly data.

`pixel_count_after` is static by field and sensor. A weekly `*_count` can be
smaller because of nodata or missing observations.
