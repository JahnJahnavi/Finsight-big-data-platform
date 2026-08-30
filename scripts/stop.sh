#!/usr/bin/env bash
# =============================================================================
# FinSight - stop the local infrastructure stack.
#
#   scripts/stop.sh            # stop + remove containers (volumes/data kept)
#   scripts/stop.sh --wipe     # ALSO delete all named volumes (destroys data)
# =============================================================================
set -euo pipefail

cd "$(dirname "$0")/.."

WIPE=0
for arg in "$@"; do
  case "$arg" in
    --wipe) WIPE=1 ;;
    *) echo "[finsight] unknown option: $arg" ; exit 2 ;;
  esac
done

# --profile '*' ensures containers from every profile are torn down
if [[ $WIPE -eq 1 ]]; then
  echo "[finsight] stopping stack and DELETING ALL VOLUMES (HDFS, Mongo, Neo4j, Kafka, Hive metastore)..."
  read -r -p "Type 'wipe' to confirm: " confirm
  [[ "$confirm" == "wipe" ]] || { echo "aborted."; exit 1; }
  docker compose --profile tools --profile connect --profile hive --profile spark down -v --remove-orphans
else
  echo "[finsight] stopping stack (data volumes preserved)..."
  docker compose --profile tools --profile connect --profile hive --profile spark down --remove-orphans
fi

echo "[finsight] done."
