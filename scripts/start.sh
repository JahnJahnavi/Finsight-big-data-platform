#!/usr/bin/env bash
# =============================================================================
# FinSight - start the local infrastructure stack.
#
#   scripts/start.sh              # start everything in COMPOSE_PROFILES (.env)
#   scripts/start.sh --min        # core only: kafka, HDFS, mongodb, neo4j
#   scripts/start.sh --build      # (re)build the custom images first
#   scripts/start.sh hive spark   # start only the named profiles (+ core)
# =============================================================================
set -euo pipefail

cd "$(dirname "$0")/.."

if [[ ! -f .env ]]; then
  echo "[finsight] .env not found - creating it from .env.example."
  echo "[finsight] >>> Edit .env and replace every CHANGE_ME value before continuing. <<<"
  cp .env.example .env
  exit 1
fi

BUILD=""
PROFILES_OVERRIDE=""
MIN=0

for arg in "$@"; do
  case "$arg" in
    --build) BUILD="--build" ;;
    --min)   MIN=1 ;;
    --*)     echo "[finsight] unknown option: $arg" ; exit 2 ;;
    *)       PROFILES_OVERRIDE="${PROFILES_OVERRIDE} --profile ${arg}" ;;
  esac
done

COMPOSE=(docker compose)

if [[ $MIN -eq 1 ]]; then
  echo "[finsight] starting CORE services only (no profiles)."
  COMPOSE_PROFILES="" "${COMPOSE[@]}" up -d --remove-orphans $BUILD \
    kafka namenode datanode mongodb neo4j
elif [[ -n "$PROFILES_OVERRIDE" ]]; then
  echo "[finsight] starting core + profiles:${PROFILES_OVERRIDE}"
  # shellcheck disable=SC2086
  COMPOSE_PROFILES="" "${COMPOSE[@]}" $PROFILES_OVERRIDE up -d --remove-orphans $BUILD
else
  echo "[finsight] starting all services in COMPOSE_PROFILES from .env"
  "${COMPOSE[@]}" up -d --remove-orphans $BUILD
fi

echo
echo "[finsight] containers:"
"${COMPOSE[@]}" ps
echo
echo "[finsight] services need 1-3 minutes to become healthy on first run."
echo "[finsight] verify with:  python scripts/healthcheck.py"
