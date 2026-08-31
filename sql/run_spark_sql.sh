#!/usr/bin/env bash
# =============================================================================
# FinSight - Phase 9: run the Spark SQL analytics job.
#
# Runs spark-submit INSIDE finsight-spark-master against the Hive warehouse.
#
#   sql/run_spark_sql.sh --mode compliance
#   sql/run_spark_sql.sh --mode customer_summary
#   sql/run_spark_sql.sh --mode dormancy
#   sql/run_spark_sql.sh --mode compliance --from csv --csv /opt/finsight/data/sample/txns.csv
#   SPARK_MASTER=local[2] sql/run_spark_sql.sh --mode dormancy
# =============================================================================
set -euo pipefail
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'

CONTAINER="${SPARK_CONTAINER:-finsight-spark-master}"
JOB="/opt/finsight/sql/spark_sql_jobs.py"
MASTER="${SPARK_MASTER:-spark://spark-master:7077}"

echo "[finsight] spark-submit spark_sql_jobs.py  (master=${MASTER})"
exec docker exec -i "$CONTAINER" /opt/spark/bin/spark-submit \
  --master "$MASTER" \
  --name "${SPARK_APPNAME_SQL:-finsight-spark-sql}" \
  --conf spark.jars.ivy=/opt/spark/.ivy2 \
  --conf spark.executor.memory="${SPARK_EXECUTOR_MEMORY:-640m}" \
  --conf spark.driver.memory="${SPARK_DRIVER_MEMORY:-640m}" \
  --conf spark.cores.max="${SPARK_CORES_MAX:-2}" \
  --conf spark.sql.shuffle.partitions=8 \
  "$JOB" --master "" "$@"
