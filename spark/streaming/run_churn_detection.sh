#!/usr/bin/env bash
# =============================================================================
# FinSight - Phase 5: run the streaming churn-detection job.
#
# Runs spark-submit INSIDE finsight-spark-master. INDEPENDENT of the fraud job -
# both can run at the same time on the same txn-raw topic.
#
# Run bootstrap_customer_history.py first (spark/streaming/run_bootstrap.sh) so
# the S1/S2 historical baseline exists.
#
#   spark/streaming/run_churn_detection.sh
#   spark/streaming/run_churn_detection.sh --once --starting-offsets earliest
#   spark/streaming/run_churn_detection.sh --reset-checkpoint
#   SPARK_MASTER=local[2] spark/streaming/run_churn_detection.sh --once
# =============================================================================
set -euo pipefail
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'

CONTAINER="${SPARK_CONTAINER:-finsight-spark-master}"
JOB="/opt/finsight/spark/streaming/churn_detection.py"
MASTER="${SPARK_MASTER:-spark://spark-master:7077}"
KAFKA_PKG_VERSION="${SPARK_KAFKA_PKG_VERSION:-3.5.3}"
PKG="org.apache.spark:spark-sql-kafka-0-10_2.12:${KAFKA_PKG_VERSION}"

echo "[finsight] spark-submit churn_detection.py  (master=${MASTER})"
exec docker exec -i "$CONTAINER" /opt/spark/bin/spark-submit \
  --master "$MASTER" \
  --name "${SPARK_APPNAME_CHURN:-finsight-streaming-churn}" \
  --packages "$PKG" \
  --py-files /opt/finsight/spark/streaming/churn_rule.py \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  --conf spark.sql.streaming.schemaInference=false \
  --conf spark.sql.execution.arrow.pyspark.enabled=true \
  --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY:-512m}" \
  --conf spark.driver.memory="${SPARK_DRIVER_MEMORY:-512m}" \
  --conf spark.cores.max="${SPARK_CORES_MAX:-2}" \
  "$JOB" --master "" "$@"
