#!/usr/bin/env bash
# =============================================================================
# FinSight - create the canonical HDFS directory layout.
#
# Safe to run repeatedly. Runs `hdfs dfs` inside the running namenode container.
#   ./scripts/init_hdfs.sh
# =============================================================================
set -euo pipefail

# Git Bash / MSYS on Windows rewrites POSIX-looking args ("/finsight" -> "C:/...").
# Disable that so the HDFS paths pass through to the container untouched.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL='*'

NN="${NAMENODE_CONTAINER:-finsight-namenode}"

DIRS=(
  /finsight
  /finsight/raw                     # Kafka Connect HDFS sink writes <topic>/step=<N>/ under here
  /finsight/logs                    # Connect sink write-ahead logs
  /finsight/checkpoints             # Spark Structured Streaming checkpoints (Phase 4)
  /finsight/processed               # Spark batch / SQL outputs (Phase 4-5)
  /finsight/exports                 # CSV exports for Alteryx (Phase 7)
  /user/hive/warehouse              # Hive managed tables
)

echo "[finsight] creating HDFS layout via container '${NN}'..."
for d in "${DIRS[@]}"; do
  docker exec "$NN" hdfs dfs -mkdir -p "$d"
  docker exec "$NN" hdfs dfs -chmod 775 "$d"
  echo "  ok  $d"
done

echo
echo "[finsight] HDFS layout:"
docker exec "$NN" hdfs dfs -ls -R /finsight
