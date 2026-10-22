#!/usr/bin/env bash
set -euo pipefail

python_bin=/root/.pyenv/versions/yield_estimate/bin/python
script_path=/opt/project/recompute_weekly_indices.py
source_dir=/data/weekly
output_dir=/data/indices
reserve_gib=100

if [[ ! -x "$python_bin" ]]; then
  echo "Python interpreter not found: $python_bin" >&2
  exit 1
fi
if [[ ! -f "$script_path" ]]; then
  echo "Production script not found: $script_path" >&2
  exit 1
fi
if [[ ! -d "$source_dir" ]]; then
  echo "Source directory not found: $source_dir" >&2
  exit 1
fi

mkdir -p "$output_dir"
stamp=$(date -u +%Y%m%dT%H%M%SZ)
log_path="$output_dir/recompute_weekly_indices_${stamp}.log"

nohup "$python_bin" -u "$script_path" \
  --source-dir "$source_dir" \
  --output-dir "$output_dir" \
  --reserve-gb "$reserve_gib" \
  > "$log_path" 2>&1 < /dev/null &
pid=$!

echo "PID: $pid"
echo "Log: $log_path"
echo "Results: $output_dir"
echo "Monitor: tail -f $log_path"
