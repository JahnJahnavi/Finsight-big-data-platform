# FinSight — Phase 10: MongoDB Customer Data

The customer master (`noveacrest_customers.json`, 10 000 records) loaded into
MongoDB as the **join dimension** for every downstream layer (Hive, Spark,
Power BI). `customerId` is the join key and is preserved exactly.

| | |
|---|---|
| Database | `finsight` |
| Collection | `customers` |
| Documents | 10 000 (one per `customerId`) |
| Source | `noveacrest_customers.json` — **not committed** (git-ignored, ~5.5 MB) |

**No Spark job changes** in this phase.

## Files (`mongodb/`)

```
mongodb/
├── import_customers.sh   repeatable mongoimport (upsert on customerId) + indexes + validation
├── indexes.js            unique customerId, compound {customerId,segment}, segment
├── validation.js         segment-count + join-key checks; quit(1) on failure
└── segments.py           EXPECTED_SEGMENTS + validate_segment_counts() (unit-tested)
```

## Import (repeatable)

```bash
mongodb/import_customers.sh                       # finds the JSON automatically
mongodb/import_customers.sh /path/to/noveacrest_customers.json
CUSTOMERS_JSON=./data/raw/noveacrest_customers.json mongodb/import_customers.sh
```

The script:

1. reads `MONGO_*` from `.env` (no hard-coded credentials),
2. locates the dataset (`$1` → `CUSTOMERS_JSON` → `data/raw/` → `Bigdata Data set file/…`),
3. streams it over **stdin** into `mongoimport` inside `finsight-mongodb` — no host
   MongoDB tools required — with:

   ```
   mongoimport --uri "<from .env>" --collection customers \
     --jsonArray --mode upsert --upsertFields customerId
   ```

   `--mode upsert` makes re-runs idempotent: matching `customerId`s are updated
   in place, new ones inserted, nothing duplicated.
4. applies `indexes.js`, then runs `validation.js`.

Raw equivalent (JSON array input needs `--jsonArray`):

```bash
docker exec -i finsight-mongodb mongoimport \
  --uri "mongodb://<user>:<pw>@localhost:27017/finsight?authSource=admin" \
  --collection customers --jsonArray --mode upsert --upsertFields customerId \
  < noveacrest_customers.json
```

## Indexes (`indexes.js`)

| Index | Keys | Purpose |
|-------|------|---------|
| `ux_customerId` | `{ customerId: 1 }`, **unique** | the join key — exact, no duplicates |
| `ix_customerId_segment` | `{ customerId: 1, segment: 1 }` | **required compound index** (spec 10) — segment-scoped customer joins |
| `ix_segment` | `{ segment: 1 }` | segment dashboards / count validation |

`customerId` is kept as the **source string field** and guarded by the unique
index — it is *not* promoted to Mongo's `_id` (`ASSUMPTIONS.md` I39). `createIndex`
is idempotent, so `indexes.js` is safe to re-run.

## Validation (`validation.js` / `segments.py`)

The spec names the five segments but gives no target counts, so validation is
**structural** (`ASSUMPTIONS.md` I41):

- collection non-empty; `customerId` present on every doc, a string, unique
- the required compound index `{ customerId: 1, segment: 1 }` exists (+ unique `customerId`)
- customer counts by segment cover **exactly**
  `Premium, Standard, Basic, Private Banking, Student`, each ≥ 1
- no segment value outside that set
- per-segment counts sum to the collection total (no NULL / missing segment)

`validation.js` prints the table and `quit(1)` on any failure so it can gate a
pipeline. `segments.py` holds the same rules in pure Python for unit testing.

Actual load:

```
  finsight.customers - 10000 customers
  ----------------------------------------
  Premium              1528
  Standard             4497
  Basic                2464
  Private Banking       528
  Student               983
  ----------------------------------------
  [validation] PASS: 10000 customers, 5 segments, customerId unique + indexed
```

## Run the checks

```bash
docker exec finsight-mongodb mongosh "<uri>" --quiet --file /opt/finsight/mongodb/validation.js
python -m pytest tests/unit/test_customer_segments.py -q     # 7 rule tests
python scripts/validate_phase10.py                           # 11 end-to-end checks
python scripts/validate_phase10.py --import                  # re-import, then check
```

`mongodb/` is mounted read-only at `/opt/finsight/mongodb` in the `mongodb`
service (see `docker-compose.yml`) so `mongosh --file` can reach the scripts.

## Inspect

```bash
docker exec finsight-mongodb mongosh "mongodb://<user>:<pw>@localhost:27017/finsight?authSource=admin" --quiet --eval '
  db.customers.countDocuments({});
  db.customers.aggregate([{ $group: { _id: "$segment", n: { $sum: 1 } } }, { $sort: { n: -1 } }]).toArray();
  db.customers.findOne();
  db.customers.getIndexes().map(i => i.name);
'
```
