# Recompute weekly indices on the server

The production code is `recompute_weekly_indices.py` and its `nohup` launcher is `start_recompute_weekly_indices.sh`. Both are installed at `/opt/project/` on `yield-forecast.geo-infinity.com`. The script reads `/data/weekly/` and writes seven-band TIFFs into `/data/indices/` with the established `*_weekly_indices_10m.tif` names.

## Start

From your computer:

```bash
ssh -i ~/.ssh/server_victor.pem root@yield-forecast.geo-infinity.com
```

Then on the server:

```bash
bash /opt/project/start_recompute_weekly_indices.sh
```

The launcher prints the PID and log path, starts the Python program with `nohup`, and returns to the shell. It reserves 100 GiB of free space on the output filesystem. If space falls below that reserve, the current partial file is removed and the run stops with an error in the log.

## Monitor

Use the log path printed by the launcher:

```bash
tail -f /data/indices/recompute_weekly_indices_YYYYMMDDTHHMMSSZ.log
ps -p PID -o pid,etime,stat,command
df -h /data
wc -l /data/indices/recompute_weekly_indices_manifest.jsonl
```

Each completed file produces a `completed` log event and a JSON record in `recompute_weekly_indices_manifest.jsonl`. If the process stops, run the launcher again: validated completed outputs are skipped, and an incomplete `.partial` file is removed before that week is retried. A lock prevents two copies of this script from writing at the same time. An existing output with incompatible source or algorithm metadata causes a stop for inspection.

The source audit found **three incomplete Nakuru weekly TIFFs**: 2025-05-26 (B02–B04 missing), 2025-06-02 (B11 missing), and 2025-06-16 (B04–B12 missing). The program skips these sources, writes their details to `/data/indices/recompute_weekly_indices_source_issues.json`, and can produce **1,042 valid outputs** from the current source set. It ends with `incomplete_source_issues` and exit code 3 to make the missing three explicit. After the source TIFFs are repaired, rerun the same launcher; it will skip completed outputs and create the remaining three. An `all_done` event with 1,045 files then indicates full completion.

## Data policy

The code assumes every source weekly TIFF contains L2A DN divided by 10,000 and that the contributing products carry `BOA_ADD_OFFSET=-1000`. It subtracts 0.1 from each valid source band used for indices. Source value zero remains nodata. The exact SAFE IDs are absent from some weekly TIFFs, so the assumption is embedded in every output tag and manifest record. The acquisition date alone is not used to select an offset policy.

Outputs use the same seven index formulas and band order as the existing pipeline: NDVI, EVI, NDMI, NDRE, MSI, PVR, LAI. They are written block by block to a temporary file on the output filesystem, sampled for pixel correctness, then moved atomically to the final name. The original weekly TIFFs remain unchanged.

The deployed `/opt/project/s2_pipeline_workqueue.py` has also been updated to use the same corrected formula for future weekly products. Its previous version is backed up at `/opt/project/s2_pipeline_workqueue.py.bak-20260922`.
