#!/usr/bin/env bash
# =============================================================================
# FinSight - Phase 4: run the streaming fraud-detection job.
#
# Runs spark-submit INSIDE the finsight-spark-master container (which has
# Python 3.8 + Spark 3.5.3; the host's Python 3.13 cannot run PySpark 3.5).
#
#   spark/streaming/run_fraud_detection.sh                       # continuous
#   spark/streaming/run_fraud_detection.sh --once --starting-offsets earliest
#   spark/streaming/run_fraud_detection.sh --reset-checkpoint
#   SPARK_MASTER=local[2] spark/streaming/run_fraud_detection.sh --once
#
# Any extra args are forwarded to fraud_detection.py.
# =============================================================================
set -euo pipefail
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'

CONTAINER="${SPARK_CONTAINER:-finsight-spark-master}"
JOB="/opt/finsight/spark/streaming/fraud_detection.py"
MASTER="${SPARK_MASTER:-spark://spark-master:7077}"
KAFKA_PKG_VERSION="${SPARK_KAFKA_PKG_VERSION:-3.5.3}"
PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:${KAFKA_PKG_VERSION}"

echo "[finsight] spark-submit fraud_detection.py  (master=${MASTER})"
exec docker exec -i "$CONTAINER" /opt/spark/bin/spark-submit \
  --master "$MASTER" \
  --name "${SPARK_APPNAME_FRAUD:-finsight-streaming-fraud}" \
  --packages "$PKG" \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  --conf spark.sql.streaming.schemaInference=false \
  --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY:-640m}" \
  --conf spark.driver.memory="${SPARK_DRIVER_MEMORY:-640m}" \
  --conf spark.cores.max="${SPARK_CORES_MAX:-2}" \
  "$JOB" --master "" "$@"
