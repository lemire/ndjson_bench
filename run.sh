#!/usr/bin/env bash
# Run bench.py for every system, dataset and thread count.
#
#   run.sh --data ~/jsonbench/ndjson [--out results/<host>.txt]
#          [--threads "1 2 4 8 16 32 64 128"] [--reps 10]
#          [--systems "duckdb polars arrow"] [--python .venv/bin/python]
#
# The corpora are the JSON-lines files of jsonstreambench (<dataset>.ndjson).
# They are copied to /dev/shm first so that DuckDB, which reads from a path,
# reads from memory; the copies are removed at the end.
set -euo pipefail

data=""
out=""
threads="1 2 4 8 16 32 64 128"
reps=10
systems="duckdb polars arrow"
datasets="twitter bestbuy google_map nspl walmart wiki"
python=".venv/bin/python"
while [ $# -gt 0 ]; do
  case "$1" in
    --data) data="$2"; shift 2 ;;
    --out) out="$2"; shift 2 ;;
    --threads) threads="$2"; shift 2 ;;
    --reps) reps="$2"; shift 2 ;;
    --systems) systems="$2"; shift 2 ;;
    --datasets) datasets="$2"; shift 2 ;;
    --python) python="$2"; shift 2 ;;
    *) echo "unknown option $1" >&2; exit 2 ;;
  esac
done
[ -n "$data" ] || { echo "usage: run.sh --data <dir>" >&2; exit 2; }
here="$(cd "$(dirname "$0")" && pwd)"
[ -n "$out" ] || out="$here/results/$(hostname -s).txt"
mkdir -p "$(dirname "$out")"

shm=/dev/shm/ndjson_bench.$$
mkdir -p "$shm"
trap 'rm -rf "$shm"' EXIT
for d in $datasets; do cp "$data/$d.ndjson" "$shm/"; done

{
  echo "# host=$(hostname -s) cpu=$(grep -m1 'model name' /proc/cpuinfo | cut -d: -f2 | xargs)" \
       "cpus=$(nproc) date=$(date -u +%Y-%m-%dT%H:%M:%SZ) reps=$reps unpinned"
  echo "# $("$python" -c 'import duckdb, polars, pyarrow, sys; print("python=%s duckdb=%s polars=%s pyarrow=%s" % (sys.version.split()[0], duckdb.__version__, polars.__version__, pyarrow.__version__))')"
} >> "$out"

for s in $systems; do
  for d in $datasets; do
    for t in $threads; do
      "$python" "$here/bench.py" --system "$s" --dataset "$d" --file "$shm/$d.ndjson" \
        --threads "$t" --reps "$reps" | tee -a "$out"
    done
  done
done
