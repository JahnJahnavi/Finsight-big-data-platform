#!/usr/bin/env bash
# =============================================================================
# FinSight - Phase 6: run the Spark Core batch risk-scoring job.
#
# Runs spark-submit INSIDE finsight-spark-master. Independent Spark application
# (appName finsight-batch-risk). Pure batch - no Kafka package needed.
#
#   spark/batch/run_risk_scoring.sh                       # from HDFS Parquet
#   spark/batch/run_risk_scoring.sh --show
#   spark/batch/run_risk_scoring.sh --from csv --csv /opt/finsight/data/sample/txns.csv
#   SPARK_MASTER=local[2] spark/batch/run_risk_scoring.sh
# =============================================================================
set -euo pipefail
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'

CONTAINER="${SPARK_CONTAINER:-finsight-spark-master}"
JOB="/opt/finsight/spark/batch/risk_scoring.py"
MASTER="${SPARK_MASTER:-spark://spark-master:7077}"

echo "[finsight] spark-submit risk_scoring.py  (master=${MASTER})"
exec docker exec -i "$CONTAINER" /opt/spark/bin/spark-submit \
  --master "$MASTER" \
  --name "${SPARK_APPNAME_RISK:-finsight-batch-risk}" \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY:-640m}" \
  --conf spark.driver.memory="${SPARK_DRIVER_MEMORY:-640m}" \
  --conf spark.cores.max="${SPARK_CORES_MAX:-2}" \
  --conf spark.sql.shuffle.partitions=8 \
  "$JOB" --master "" "$@"
