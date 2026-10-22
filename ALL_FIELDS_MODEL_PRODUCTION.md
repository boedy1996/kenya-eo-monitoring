# Kenya v3 model: all production fields

This run applies the delivered Kenya v3 county models to every production
field while keeping each field's complete weekly history in one batch.

## Fixed paths

```bash
export PY=/root/.pyenv/versions/kenya_v3_19predictors_20261014/bin/python
export RUNNER=/opt/project/run_all_fields_model_production.py
export RUN_DIR=/opt/project/model_runs/kenya_v3_all_fields_20261014
```

The prepared manifest contains 269,453 fields, 543 weeks, 146,312,979 expected
rows and 55 county-specific batches. Batch `machakos_00000` has already passed.

## Start or resume

```bash
mkdir -p "$RUN_DIR/logs"
nohup env PYTHONUNBUFFERED=1 \
  "$PY" "$RUNNER" run-all --run-dir "$RUN_DIR" \
  > "$RUN_DIR/logs/run-all.log" 2>&1 &
echo $! > "$RUN_DIR/run-all.pid"
```

Passed batches are skipped. A failed or interrupted batch is retried, and an
existing validated batch input is reused.

## Monitor

```bash
tail -f "$RUN_DIR/logs/run-all.log"
```

In another shell:

```bash
"$PY" "$RUNNER" status --run-dir "$RUN_DIR"
ps -fp "$(cat "$RUN_DIR/run-all.pid")"
```

Each batch has its own log under `logs/<batch_id>.log`.

## Final validation

Run only after status reports 55 passed batches and zero pending/failed batches.

```bash
"$PY" "$RUNNER" validate --run-dir "$RUN_DIR" \
  > "$RUN_DIR/logs/validate.log" 2>&1
cat "$RUN_DIR/logs/validate.log"
```

The final report is `validation_report.json`. A valid run has 269,453 fields,
543 weeks, 146,312,979 rows, 55 passed batches, no duplicate keys, no unknown
county, no physical-range errors and no invalid probability sums.

## Output dataset

The model calls are a partitioned Parquet dataset:

```text
/opt/project/model_runs/kenya_v3_all_fields_20261014/outputs/machakos/*.parquet
/opt/project/model_runs/kenya_v3_all_fields_20261014/outputs/nakuru/*.parquet
```

The lean model inputs are retained under `inputs/`. Do not combine the output
into one CSV; use the Parquet directory as one dataset.

The model remains a pilot baseline. It has no crop gate, so fields outside the
model classes can still receive maize, beans or potatoes calls.
