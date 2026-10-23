#!/usr/bin/env bash
# Downloads NYC TLC yellow-taxi trips for a month range plus the taxi zone lookup (public data).
#   scripts/download_data.sh 2024-01 2024-06
set -euo pipefail
START="${1:-2024-01}"
END="${2:-$START}"
BASE="https://d37ci6vzurychx.cloudfront.net"
mkdir -p data/raw
for MONTH in $(python3 -c "from pipeline.run import months_between; print(' '.join(months_between('$START', '$END')))"); do
  curl -fSL -o "data/raw/yellow_tripdata_${MONTH}.parquet" "${BASE}/trip-data/yellow_tripdata_${MONTH}.parquet"
done
curl -fSL -o "data/raw/taxi_zone_lookup.csv" "${BASE}/misc/taxi_zone_lookup.csv"
ls -lh data/raw
