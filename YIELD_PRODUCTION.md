# All-field seasonal yield production

This workflow calculates remote sensing yield estimates for every field in the
active crop model and every complete model season. Long Rains runs from 1 March
through 31 August. Short Rains runs from 1 October through the end of February.
Calendar-complete seasons are also required to fit fully within the active
model run's date range and have every required WaPOR NPP dekad. Boundary
seasons with partial model history and seasons with incomplete WaPOR coverage
are kept in the crop dashboard but do not receive a yield estimate.

The output remains labelled **calculated, unvalidated** because observed crop
cuts are not available. Fields classified as unsupported, non-final, mixed, or
with inadequate WaPOR coverage remain in the output with a null yield and a QA
reason.

## Data flow

1. Inventory fields and complete seasons from PostGIS.
2. Inventory FAO WaPOR dekadal NPP, retain only seasons with complete source
   coverage, and clip the rasters for Machakos and Nakuru.
3. Cache exact field-to-pixel fractional coverage weights once per county.
4. Extract NPP for all fields and dekads with the cached weights.
5. Derive field phenology from weekly S2 NDVI, EVI and LAI plus S1 RVI.
6. Integrate NPP between field SOS and EOS and calculate low, central and high
   crop yield scenarios.
7. Validate every seasonal Parquet and load all results into PostGIS with a
   resumable checkpoint per season.
8. Activate the yield run only after database validation succeeds.

## Server commands

Run from `/opt/project` with the production environment:

```bash
export RUN_ID=wapor-yield-all-fields-v1
export WORKERS=2
export CHUNK_SIZE=20000
mkdir -p /opt/project/yield_runs/logs
```

Each long stage can run under `nohup`. Complete the stages in this order:

```bash
nohup env RUN_ID="$RUN_ID" ./run_yield_production.sh prepare \
  > "/opt/project/yield_runs/logs/${RUN_ID}-prepare.log" 2>&1 &

nohup env RUN_ID="$RUN_ID" ./run_yield_production.sh inventory \
  > "/opt/project/yield_runs/logs/${RUN_ID}-inventory.log" 2>&1 &

nohup env RUN_ID="$RUN_ID" WORKERS="$WORKERS" ./run_yield_production.sh clip \
  > "/opt/project/yield_runs/logs/${RUN_ID}-clip.log" 2>&1 &

nohup env RUN_ID="$RUN_ID" CHUNK_SIZE="$CHUNK_SIZE" ./run_yield_production.sh build-weights \
  > "/opt/project/yield_runs/logs/${RUN_ID}-weights.log" 2>&1 &

nohup env RUN_ID="$RUN_ID" WORKERS="$WORKERS" ./run_yield_production.sh extract-npp \
  > "/opt/project/yield_runs/logs/${RUN_ID}-npp.log" 2>&1 &
```

Calculate one gate season before the full calculation:

```bash
nohup env RUN_ID="$RUN_ID" ./run_yield_production.sh calculate-season 2023_lr \
  > "/opt/project/yield_runs/logs/${RUN_ID}-calculate-2023_lr.log" 2>&1 &

cat "/opt/project/yield_runs/${RUN_ID}/validation/2023_lr.json"
```

Continue only when the gate report says `passed`:

```bash
nohup env RUN_ID="$RUN_ID" ./run_yield_production.sh calculate-all \
  > "/opt/project/yield_runs/logs/${RUN_ID}-calculate-all.log" 2>&1 &

env RUN_ID="$RUN_ID" ./run_yield_production.sh validate
```

Apply dashboard migrations, then load and atomically activate the yield run:

```bash
sudo -u postgres env \
  DATABASE_URL='postgresql:///kenya_monitoring?host=/var/run/postgresql' \
  /opt/kenya-dashboard/venv/bin/python \
  /opt/kenya-dashboard/current/dashboard/scripts/migrate.py

nohup env RUN_ID="$RUN_ID" ./run_yield_production.sh load-dashboard \
  > "/opt/project/yield_runs/logs/${RUN_ID}-load-dashboard.log" 2>&1 &
```

The loader writes its database validation report under
`/opt/kenya-dashboard/load-tmp/`. Restart the API after the new application
release is active, then select **Seasonal** in the dashboard. Complete seasons
show the yield availability indicator, season summary and field-level result.

## Monitoring and resume

```bash
env RUN_ID="$RUN_ID" ./run_yield_production.sh status
tail -n 50 "/opt/project/yield_runs/logs/${RUN_ID}-calculate-all.log"
tail -n 50 "/opt/project/yield_runs/logs/${RUN_ID}-load-dashboard.log"
```

Every raster, weight chunk, seasonal result and database load has a completion
artifact. Re-running the same stage resumes from valid artifacts.
