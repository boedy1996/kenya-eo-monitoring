# WaPOR calculated-yield pilot

This pilot calculates auditable, unvalidated field-season yield estimates from
WaPOR v3 Level-2 dekadal NPP. It does not use observed yield, FAPAR, solar
radiation, rainfall, temperature, AETI, or Earth Engine during production.

## Scope

- Season: `2023_lr` (1 March through 31 August 2023)
- Fields: the 775 IDs in `/opt/project/field_id_list.txt`
- Crop calls: the active dashboard seasonal model run
- Phenology: existing weekly S2 NDVI/EVI/LAI with S1 RVI support
- Biomass: WaPOR v3 `L2-NPP-D`, read as HTTP range requests and clipped to the
  selected field extents
- Output status: `calculated_unvalidated`

The database is read only. All outputs are written below
`/opt/project/yield_runs/<RUN_ID>`.

## Files

- `yield_estimation/core.py`: season calendar, phenology, NPP integration, and
  the workbook-equivalent yield formula.
- `yield_estimation/pipeline.py`: resumable prepare, inventory, clip, extract,
  calculate, validate, and status commands.
- `run_yield_pilot.sh`: server wrapper.
- `tests/test_yield_estimation.py`: formula, calendar, partial-dekad, and
  phenology checks.

## Local checks

```bash
cd /Users/boedy1996/kenya-eo-monitoring
pyenv local kenya-eo-monitoring
python -m unittest -v tests/test_yield_estimation.py
bash -n run_yield_pilot.sh
```

## Server run

After committing, pushing, and pulling these files to `/opt/project`:

```bash
cd /opt/project
chmod +x run_yield_pilot.sh
export RUN_ID=wapor-yield-2023lr-pilot-v1
export SEASON_CODE=2023_lr
mkdir -p /opt/project/yield_runs/logs
```

Run each stage separately so every result can be inspected before continuing:

```bash
nohup env RUN_ID="$RUN_ID" SEASON_CODE="$SEASON_CODE" \
  ./run_yield_pilot.sh prepare \
  > "/opt/project/yield_runs/logs/${RUN_ID}-prepare.log" 2>&1 &
```

```bash
nohup env RUN_ID="$RUN_ID" SEASON_CODE="$SEASON_CODE" \
  ./run_yield_pilot.sh inventory \
  > "/opt/project/yield_runs/logs/${RUN_ID}-inventory.log" 2>&1 &
```

```bash
nohup env RUN_ID="$RUN_ID" SEASON_CODE="$SEASON_CODE" \
  ./run_yield_pilot.sh clip \
  > "/opt/project/yield_runs/logs/${RUN_ID}-clip.log" 2>&1 &
```

The clip stage reads only the Nakuru and Machakos windows from each global COG.
It may run for some time, but the local files are only county-sized clips. It
is resumable: already valid clips are skipped.

```bash
nohup env RUN_ID="$RUN_ID" SEASON_CODE="$SEASON_CODE" \
  ./run_yield_pilot.sh extract-npp \
  > "/opt/project/yield_runs/logs/${RUN_ID}-extract-npp.log" 2>&1 &
```

```bash
nohup env RUN_ID="$RUN_ID" SEASON_CODE="$SEASON_CODE" \
  ./run_yield_pilot.sh calculate \
  > "/opt/project/yield_runs/logs/${RUN_ID}-calculate.log" 2>&1 &
```

```bash
nohup env RUN_ID="$RUN_ID" SEASON_CODE="$SEASON_CODE" \
  ./run_yield_pilot.sh validate \
  > "/opt/project/yield_runs/logs/${RUN_ID}-validate.log" 2>&1 &
```

Monitor a stage with:

```bash
tail -f "/opt/project/yield_runs/logs/${RUN_ID}-clip.log"
./run_yield_pilot.sh status
```

Do not load the pilot into dashboard production tables. Review these first:

```text
outputs/field_season_calculated_yield.parquet
outputs/yield_summary.csv
validation_report.json
```

The validation report checks formula and data integrity. It cannot report yield
accuracy because no observed-yield dataset is available.
