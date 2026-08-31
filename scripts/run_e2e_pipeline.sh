#!/usr/bin/env bash
# ===========================================================================
# FinSight - Phase 14: end-to-end pipeline driver.
#
# Runs the FinSight pipeline stage by stage so scripts/validate_e2e.py can
# verify every checkpoint. Each stage is idempotent (re-run safe) and OPT-OUT
# via a flag - nothing here rewrites code, it only executes existing jobs.
#
#   scripts/run_e2e_pipeline.sh                 # full pipeline
#   scripts/run_e2e_pipeline.sh --from 6        # resume at stage 6
#   scripts/run_e2e_pipeline.sh --only 6,10,15  # just these stages
#   SAMPLE_LIMIT=2000 scripts/run_e2e_pipeline.sh
#
# Stages:
#   1  produce txn-raw            kafka/producer.py
#   2  wait for HDFS sink flush   (Kafka Connect -> Parquet)
#   3  Hive warehouse             hive/run_warehouse.sh
#   4  fraud streaming (--once)   spark/streaming/run_fraud_detection.sh
#   5  churn streaming (--once)   spark/streaming/run_churn_detection.sh
#   6  risk scoring               spark/batch/run_risk_scoring.sh
#   7  CLV scoring                spark/batch/run_clv_scoring.sh
#   8  Spark SQL (3 modes)        sql/run_spark_sql.sh
#   9  export customer_fraud_summary Hive table  alteryx/prereq/*.hql
#   10 MongoDB import             mongodb/import_customers.sh
#   11 Neo4j load                 neo4j/loader.py
#   12 Alteryx fallbacks          alteryx/fallback/*.py
#   13 Power BI bridge + exports  powerbi/*
# ===========================================================================
set -uo pipefail
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'
cd "$(dirname "${BASH_SOURCE[0]}")/.."

FROM=1 ; ONLY="" ; FAILED=()
while [ $# -gt 0 ]; do
  case "$1" in
    --from) FROM="$2"; shift 2 ;;
    --only) ONLY=",$2,"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

run_stage() {  # <n> <label> <command...>
  local n="$1" label="$2"; shift 2
  [ -n "$ONLY" ] && { [[ "$ONLY" == *",$n,"* ]] || return 0; }
  [ -z "$ONLY" ] && [ "$n" -lt "$FROM" ] && return 0
  echo ; echo "=== stage $n: $label ==="
  if "$@"; then echo "--- stage $n OK"; else
    echo "--- stage $n FAILED (exit $?)"; FAILED+=("$n:$label"); fi
}

produce_txn_raw() {
  local src=""
  for c in "${PRODUCER_SOURCE_CSV:-}" \
           "data/raw/NovaCrest_Transactions.csv" \
           "data/raw/Transactions.csv" \
           "Bigdata Data set file/src-data/src-data/Transactions.csv" \
           "Bigdata Data set file/src-data/Transactions.csv"; do
    [ -n "$c" ] && [ -f "$c" ] && { src="$c"; break; }
  done
  [ -z "$src" ] && { echo "ERROR: Transactions.csv not found (set PRODUCER_SOURCE_CSV)"; return 2; }
  echo "producing from: $src  (limit ${SAMPLE_LIMIT:-4000})"
  python kafka/producer.py --file "$src" --limit "${SAMPLE_LIMIT:-4000}" --rate 2000
}

wait_for_hdfs_flush() {
  echo "waiting up to 180s for Kafka Connect to flush txn-raw -> HDFS ..."
  for _ in $(seq 1 18); do
    n=$(docker exec finsight-namenode hdfs dfs -ls /finsight/raw/txn-raw 2>/dev/null | grep -c "step=" || true)
    echo "  step partitions: ${n:-0}"; [ "${n:-0}" -ge 1 ] && return 0; sleep 10
  done; return 1
}

run_stage 1  "produce txn-raw"          produce_txn_raw
run_stage 2  "HDFS sink flush"          wait_for_hdfs_flush
run_stage 3  "Hive warehouse"           bash hive/run_warehouse.sh
run_stage 4  "fraud streaming (once)"   bash spark/streaming/run_fraud_detection.sh --once --starting-offsets earliest
run_stage 5  "churn streaming (once)"   bash spark/streaming/run_churn_detection.sh --once --starting-offsets earliest
run_stage 6  "risk scoring"             bash spark/batch/run_risk_scoring.sh
run_stage 7  "CLV scoring"              bash spark/batch/run_clv_scoring.sh
run_stage 8  "Spark SQL compliance"     bash sql/run_spark_sql.sh --mode compliance
run_stage 8  "Spark SQL customer_summary" bash sql/run_spark_sql.sh --mode customer_summary
run_stage 8  "Spark SQL dormancy"       bash sql/run_spark_sql.sh --mode dormancy
run_stage 9  "customer_fraud_summary Hive table" bash -c 'docker exec -i finsight-hiveserver2 beeline -u jdbc:hive2://localhost:10000/ < alteryx/prereq/customer_fraud_summary_external.hql'
run_stage 10 "MongoDB import"           bash mongodb/import_customers.sh
run_stage 11 "Neo4j load"               python neo4j/loader.py --wipe
run_stage 12 "Alteryx WF1 fallback"     python alteryx/fallback/customer_risk_blend.py
run_stage 12 "Alteryx WF2 fallback"     python alteryx/fallback/transaction_summary.py
run_stage 13 "Power BI bridge"          python powerbi/kafka_bridge/txn_flagged_bridge.py --once --from-beginning --idle-timeout 8
run_stage 13 "Power BI exports"         python powerbi/export_datasets.py

echo ; echo "======================================================"
if [ ${#FAILED[@]} -eq 0 ]; then
  echo "pipeline complete - all requested stages OK"
else
  echo "pipeline finished with ${#FAILED[@]} failed stage(s):"
  printf '  - %s\n' "${FAILED[@]}"
fi
echo "now run:  python scripts/validate_e2e.py"
[ ${#FAILED[@]} -eq 0 ]
