#!/usr/bin/env bash
# =============================================================================
# FinSight - Phase 5: build the per-customer behavioural baseline (S1/S2 history).
#
#   spark/streaming/run_bootstrap.sh                       # from HDFS Parquet
#   spark/streaming/run_bootstrap.sh --from csv --csv /opt/finsight/data/sample/history.csv
# =============================================================================
set -euo pipefail
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'

CONTAINER="${SPARK_CONTAINER:-finsight-spark-master}"
JOB="/opt/finsight/spark/streaming/bootstrap_customer_history.py"
MASTER="${SPARK_MASTER:-local[2]}"

echo "[finsight] spark-submit bootstrap_customer_history.py  (master=${MASTER})"
exec docker exec -i "$CONTAINER" /opt/spark/bin/spark-submit \
  --master "$MASTER" \
  --name finsight-bootstrap-customer-history \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  "$JOB" --master "" "$@"
