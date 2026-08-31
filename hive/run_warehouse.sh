#!/usr/bin/env bash
# =============================================================================
# FinSight - Phase 8: build the Hive data warehouse.
#
# Runs the hive/ SQL files IN ORDER via beeline inside the finsight-hiveserver2
# container. The hive/ dir is mounted read-only at /finsight/hive.
#
#   hive/run_warehouse.sh            # create db + tables + view + mart + stats
#   hive/run_warehouse.sh --ddl-only # skip the mart INSERT OVERWRITE and stats
# =============================================================================
set -euo pipefail
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'

CONTAINER="${HIVESERVER2_CONTAINER:-finsight-hiveserver2}"
JDBC="jdbc:hive2://localhost:10000/"
BEELINE=(beeline -u "$JDBC" --silent=true --showWarnings=false)

DDL_ONLY=0
[[ "${1:-}" == "--ddl-only" ]] && DDL_ONLY=1

FILES=(
  /finsight/hive/ddl/00_create_database.sql
  /finsight/hive/ddl/01_transactions_external.sql
  /finsight/hive/ddl/02_vw_fraud_transactions.sql
  /finsight/hive/ddl/04_customer_clv_external.sql
)
if [[ $DDL_ONLY -eq 0 ]]; then
  FILES+=(
    /finsight/hive/ddl/03_txn_summary_mart.sql   # CREATE + INSERT OVERWRITE
    /finsight/hive/analyze/compute_statistics.sql
  )
fi

for f in "${FILES[@]}"; do
  echo
  echo "==================================================================="
  echo "[finsight]  $f"
  echo "==================================================================="
  docker exec -i "$CONTAINER" "${BEELINE[@]}" -f "$f"
done

echo
echo "[finsight] warehouse built. Verify with:  scripts/validate_phase8.py"
