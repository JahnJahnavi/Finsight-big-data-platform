# FinSight — Phase 8: Hive Data Warehouse

Database `finsight` with four objects over the HDFS data lake (spec section 8):

| Object | Type | Source | Spec |
|--------|------|--------|------|
| `finsight.transactions` | **EXTERNAL** | `/finsight/raw/txn-raw/` (Kafka Connect Parquet) | 8.1 |
| `finsight.vw_fraud_transactions` | **VIEW** | `finsight.transactions WHERE isFraud = 1` | 8.1 R1 |
| `finsight.txn_summary_mart` | **MANAGED** | `INSERT OVERWRITE` from `finsight.transactions` | 8.2 |
| `finsight.customer_clv` | **EXTERNAL** | `/finsight/processed/clv_scores/` (Phase 7 output) | 7.4 R1 |

**No Spark SQL** (`spark_sql_jobs.py`) in this phase.

## SQL files (`hive/`)

```
hive/
├── ddl/
│   ├── 00_create_database.sql          CREATE DATABASE finsight
│   ├── 01_transactions_external.sql    EXTERNAL finsight.transactions (13 cols)
│   ├── 02_vw_fraud_transactions.sql    VIEW finsight.vw_fraud_transactions
│   ├── 03_txn_summary_mart.sql         MANAGED finsight.txn_summary_mart + INSERT OVERWRITE
│   └── 04_customer_clv_external.sql    EXTERNAL finsight.customer_clv
├── analyze/
│   └── compute_statistics.sql          ANALYZE ... COMPUTE STATISTICS [FOR COLUMNS]
└── run_warehouse.sh                     runs them in order via beeline
```

## `finsight.transactions`

13 columns (`step, type, amount, nameOrig, oldbalanceOrg, newbalanceOrig,
nameDest, oldbalanceDest, newbalanceDest, isFraud, isFlaggedFraud, txnId,
ingest_ts`), `STORED AS PARQUET`, `LOCATION hdfs://namenode:8020/finsight/raw/txn-raw`.

- **Path**: the spec names `/finsight/raw/transactions/`; the Kafka Connect HDFS
  sink names the output directory after the source topic, so the data is at
  `/finsight/raw/txn-raw/` (`ASSUMPTIONS.md` I5b). The table points there.
- **Not `PARTITIONED BY (step)`**: the connector's `FieldPartitioner` keeps
  `step` as a data column in every Parquet file *and* the directory name, and
  Hive 3.1.3 NPEs on a partition column that also exists in the file schema. So
  `step` is a normal column; the `step=<N>/` sub-directories are read
  recursively (`hive.mapred.supports.subdirectories`,
  `mapreduce.input.fileinputformat.input.dir.recursive` — both in
  `hive-site.xml`, so every downstream query works).

## `finsight.txn_summary_mart` (spec 8.2)

One row **per customer per step**, 9 columns: `customerId` (= `nameOrig`),
`step`, `txn_count`, `total_amount`, `avg_amount`, `max_amount`, `fraud_count`,
`txn_types` (comma-separated distinct types), `last_balance`. Populated with
`INSERT OVERWRITE` from `finsight.transactions`, refreshed after each Spark Core
batch run.

`last_balance` = `newbalanceOrig` of the latest transaction in the
`(customer, step)` group, ordered by `ingest_ts DESC, txnId DESC` (no finer
intra-step ordering exists in the data).

## Statistics (spec 8.1 R2 / 8.2 R1)

```sql
ANALYZE TABLE finsight.transactions      COMPUTE STATISTICS;
ANALYZE TABLE finsight.txn_summary_mart   COMPUTE STATISTICS;
ANALYZE TABLE finsight.txn_summary_mart   COMPUTE STATISTICS FOR COLUMNS;
ANALYZE TABLE finsight.customer_clv       COMPUTE STATISTICS;
```

## Making HiveServer2 usable (dev-stack fixes)

Hive 3.1.3 in this no-YARN stack needed six settings to run `SELECT` /
`INSERT OVERWRITE` / `ANALYZE` (`docker/conf/hive/`, `docker-compose.yml`):

| Setting | Why |
|---|---|
| `docker/conf/hive/tez-site.xml` (`tez.local.mode=true`, …) | no YARN — Tez runs in-process; Tez does not read `hive-site.xml` |
| `hive.exec.scratchdir = file:///tmp/hive/scratch` | Tez `LocalClient` needs `_tez_session_dir` on the local FS, not `hdfs://` |
| `hive.query.results.cache.enabled = false` | Hive 3.1.3 `checkResultsCache()` NPEs on every `SELECT` (HIVE-22099) |
| `hive.compute.query.using.stats = false` | `COUNT(*)` via absent Parquet stats NPEs |
| recursive-subdirectory reads | external table over `step=<N>/` dirs |
| `IS_RESUME=true` on `hive-metastore` | Hive 3.1.3's entrypoint re-runs a bare `schematool -initSchema` on every start, which fails once the Postgres schema exists |

`docker/conf/spark/spark-defaults.conf` also gets
`spark.sql.hive.convertMetastoreParquet=false` + recursive flags so Spark can
read `finsight.transactions` (the sub-directory layout) in later phases.

## Run it

```bash
docker compose up -d                       # infra (rebuilds nothing; config-only)
# populate the lake first: Phase 2 producer -> Phase 3 sink -> Phase 7 CLV

hive/run_warehouse.sh                       # db + 4 objects + INSERT OVERWRITE + stats
hive/run_warehouse.sh --ddl-only            # skip the mart load and stats
```

## Validation

```bash
python scripts/validate_phase8.py          # 10 checks
```

Spec validation queries (all via beeline / HiveServer2):

```sql
SHOW TABLES IN finsight;
-- customer_clv, transactions, txn_summary_mart, vw_fraud_transactions

DESCRIBE finsight.transactions;            -- 13 columns

SELECT COUNT(*) FROM finsight.transactions;             -- 4000  (sample run)
SELECT COUNT(*) FROM finsight.vw_fraud_transactions;    -- 7     (= WHERE isFraud = 1)
```

`validate_phase8.py` additionally checks: `transactions` is `EXTERNAL`;
`txn_summary_mart` is `MANAGED` with one row per `(customer, step)` and the 9
spec fields; mart `txn_count` / `fraud_count` totals reconcile with the base
table; `customer_clv` is external over `clv_scores`; `numRows` /
`COLUMN_STATS_ACCURATE` present after `ANALYZE`.

## Inspect

```bash
docker exec finsight-hiveserver2 beeline -u jdbc:hive2://localhost:10000/ -e "
  USE finsight; SHOW TABLES;
  DESCRIBE FORMATTED finsight.transactions;
  SELECT * FROM finsight.txn_summary_mart ORDER BY total_amount DESC LIMIT 10;
  SELECT clv_classification, COUNT(*) FROM finsight.customer_clv GROUP BY clv_classification;
"
# HiveServer2 web UI: http://localhost:10002
```
