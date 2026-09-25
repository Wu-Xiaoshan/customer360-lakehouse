#!/usr/bin/env bash
# Runs the full pipeline: Bronze -> Silver -> Gold.
#
#   baseline:   LAKEHOUSE_MODE=local   LOCAL_DATA=./data ./run_pipeline.sh
#   lakehouse:  LAKEHOUSE_MODE=iceberg LOCAL_DATA=/home/iceberg/data ./run_pipeline.sh
#
# Both modes execute the same code, so row counts and DQ percentages must match.
set -e
echo "===== mode=${LAKEHOUSE_MODE:-local}  data=${LOCAL_DATA:-./data}  run=${RUN_ID:-run_2025_01} ====="
echo "===== [1/3] Bronze ingestion ====="
python3 src/bronze_ingest.py
echo "===== [2/3] Silver cleaning and validation ====="
python3 src/silver_clean.py
echo "===== [3/3] Gold metrics ====="
python3 src/gold_metrics.py
echo "===== Pipeline complete ====="
