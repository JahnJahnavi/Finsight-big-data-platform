#!/usr/bin/env bash
# ===========================================================================
# FinSight - Phase 10: (re-)import NovaCrest customers into finsight.customers
#
#   mongodb/import_customers.sh [path/to/noveacrest_customers.json]
#
# Repeatable: --mode upsert --upsertFields customerId, so re-running re-syncs
# without creating duplicates. The dataset is NOT committed (see .gitignore);
# point this at wherever you unpacked it, or set CUSTOMERS_JSON in .env.
#
# Runs mongoimport INSIDE finsight-mongodb (no host mongo tools needed) by
# streaming the file over stdin, then applies indexes.js + validation.js.
# ===========================================================================
set -euo pipefail
export MSYS_NO_PATHCONV=1 MSYS2_ARG_CONV_EXCL='*'

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"

# --- config from .env (never hard-code credentials) ---
# only pull the keys this script needs - .env has values with spaces elsewhere
if [ -f "$REPO/.env" ]; then
  while IFS='=' read -r k v; do
    case "$k" in
      MONGO_CONTAINER|MONGO_DB|MONGO_COLLECTION|MONGO_INITDB_ROOT_USERNAME|MONGO_INITDB_ROOT_PASSWORD|CUSTOMERS_JSON)
        export "$k=$v" ;;
    esac
  done < <(grep -E '^[A-Z_]+=' "$REPO/.env")
fi

CONTAINER="${MONGO_CONTAINER:-finsight-mongodb}"
DB="${MONGO_DB:-finsight}"
COLL="${MONGO_COLLECTION:-customers}"
USER="${MONGO_INITDB_ROOT_USERNAME:?set MONGO_INITDB_ROOT_USERNAME in .env}"
PASS="${MONGO_INITDB_ROOT_PASSWORD:?set MONGO_INITDB_ROOT_PASSWORD in .env}"
URI="mongodb://${USER}:${PASS}@localhost:27017/${DB}?authSource=admin"

# --- locate the dataset ---
CANDIDATES=(
  "${1:-}"
  "${CUSTOMERS_JSON:-}"
  "$REPO/data/raw/noveacrest_customers.json"
  "$REPO/Bigdata Data set file/src-data/src-data/noveacrest_customers.json"
  "$REPO/Bigdata Data set file/src-data/noveacrest_customers.json"
)
SRC=""
for c in "${CANDIDATES[@]}"; do
  [ -n "$c" ] && [ -f "$c" ] && { SRC="$c"; break; }
done
if [ -z "$SRC" ]; then
  echo "ERROR: noveacrest_customers.json not found. Pass it as an argument or set CUSTOMERS_JSON in .env." >&2
  exit 2
fi

# JSON array vs newline-delimited (mongoimport needs --jsonArray for the former)
JSON_ARRAY_FLAG=""
[ "$(head -c 1 "$SRC")" = "[" ] && JSON_ARRAY_FLAG="--jsonArray"

echo "[import] source : $SRC ($(wc -c < "$SRC" | tr -d ' ') bytes)"
echo "[import] target : ${DB}.${COLL}  (upsert on customerId)"

docker exec -i "$CONTAINER" mongoimport \
  --uri "$URI" \
  --collection "$COLL" \
  $JSON_ARRAY_FLAG \
  --mode upsert --upsertFields customerId \
  < "$SRC"

echo "[import] applying indexes.js"
docker exec -i "$CONTAINER" mongosh "$URI" --quiet --file /opt/finsight/mongodb/indexes.js

echo "[import] running validation.js"
docker exec -i "$CONTAINER" mongosh "$URI" --quiet --file /opt/finsight/mongodb/validation.js
