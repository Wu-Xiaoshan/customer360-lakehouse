#!/usr/bin/env bash
# Runs the full pipeline: Bronze -> Silver -> Gold -> churn model.
#
#   baseline:   LAKEHOUSE_MODE=local   LOCAL_DATA=./data ./run_pipeline.sh
#   lakehouse:  LAKEHOUSE_MODE=iceberg LOCAL_DATA=/home/iceberg/data ./run_pipeline.sh
#
# Both modes execute the same code, so row counts and DQ percentages must match.
set -e
export RUN_ID="${RUN_ID:-run_$(date -u +%Y%m%dT%H%M%SZ)}"
echo "===== mode=${LAKEHOUSE_MODE:-local}  data=${LOCAL_DATA:-./data}  run=${RUN_ID}  as_of=${AS_OF_DATE:-2025-12-01} ====="
echo "===== [1/4] Bronze ingestion ====="
python3 src/bronze_ingest.py
echo "===== [2/4] Silver cleaning and validation ====="
python3 src/silver_clean.py
echo "===== [3/4] Gold metrics ====="
python3 src/gold_metrics.py
echo "===== [4/4] Churn model ====="
python3 src/ml/churn_model.py
echo "===== Pipeline complete ====="
