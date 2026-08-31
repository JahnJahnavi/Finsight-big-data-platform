# FinSight — Phase 12: Alteryx Data Blending

Two Alteryx Designer workflows that blend the FinSight serving layer into
Power BI-ready extracts (spec section 12 / 9.2).

| Workflow | Inputs | Output | Doc |
|---|---|---|---|
| **1 — Customer Risk Blend** | Hive `customer_fraud_summary` · MongoDB `customers` · Hive `customer_clv` · HDFS `churn_alerts` | `alteryx/outputs/customer_risk_blend.xlsx` | [workflow-1-customer-risk-blend.md](workflow-1-customer-risk-blend.md) |
| **2 — Transaction Summary** | Hive `txn_summary_mart` → see I47 | `alteryx/outputs/transaction_summary.csv` | [workflow-2-transaction-summary.md](workflow-2-transaction-summary.md) |

## Why there are no `.yxmd` files

Alteryx Designer is a licensed, Windows-only **desktop** application. This
environment has no Designer install, so a `.yxmd` cannot be authored *and
verified to open and run*. Rather than commit an unverifiable XML file that
might not load — or fabricate a "successful run" screenshot / output — Phase 12
ships:

1. **Complete manual build instructions** — every tool (Input, Join, Select,
   Formula, Filter, Summarize, Output), its exact configuration, field
   mappings, formulas, join keys, and the expected output schema. A Designer
   user can rebuild each workflow tool-for-tool from these.
2. **A headless reference implementation** per workflow under
   [`alteryx/fallback/`](../../alteryx/fallback/) — pandas, reads the same live
   sources via `docker exec`, applies the identical logic, writes the same
   output. This is explicitly **not** an Alteryx artifact; it exists so the
   blend is reproducible on CI and so a real Designer run can be diffed against
   a known-good baseline (`ASSUMPTIONS.md` I10).

No output file in this repo is presented as the product of an executed Alteryx
workflow. `alteryx/outputs/` is git-ignored.

## Data-source map

| Alteryx input | Physical source | Connection | Produced by |
|---|---|---|---|
| Hive `finsight.customer_fraud_summary` | HDFS Parquet `/finsight/processed/customer_fraud_summary/` | HiveServer2 ODBC (`localhost:10000`) | `sql/spark_sql_jobs.py --mode customer_summary` + [`alteryx/prereq/customer_fraud_summary_external.hql`](../../alteryx/prereq/customer_fraud_summary_external.hql) |
| MongoDB `finsight.customers` | MongoDB `finsight-mongodb:27017` | MongoDB ODBC / connector | `mongodb/import_customers.sh` (Phase 10) |
| Hive `finsight.customer_clv` | HDFS Parquet `/finsight/processed/clv_scores/` | HiveServer2 ODBC | `spark/batch/clv_scoring.py` (Phase 7) + Phase 8 DDL |
| HDFS `/finsight/processed/churn_alerts/` | HDFS Parquet | Parquet / ODBC (optional) | `spark/streaming/churn_detection.py` (Phase 5) |
| Hive `finsight.txn_summary_mart` | Managed Hive table | HiveServer2 ODBC | Phase 8 `hive/run_warehouse.sh` |
| Hive `finsight.transactions` | HDFS Parquet `/finsight/raw/txn-raw/` | HiveServer2 ODBC | Phases 2–3 + Phase 8 DDL |

## Execution order

```
# serving layer must be populated first
scripts/start.sh hive spark
python kafka/producer.py ...                 # -> txn-raw           (Phase 2)
scripts/register_hdfs_sink.py                # -> HDFS Parquet      (Phase 3)
spark/batch/clv_scoring.py                   # -> clv_scores        (Phase 7)
hive/run_warehouse.sh                        # -> Hive warehouse    (Phase 8)
sql/run_spark_sql.sh --mode customer_summary # -> customer_fraud_summary (Phase 9)

# Phase 12 prerequisite: expose customer_fraud_summary to Hive
docker exec -i finsight-hiveserver2 beeline -u jdbc:hive2://localhost:10000/ \
  < alteryx/prereq/customer_fraud_summary_external.hql

# then, in Alteryx Designer, open and run each workflow (manual build per the docs)
# or, headless:
python alteryx/fallback/customer_risk_blend.py
python alteryx/fallback/transaction_summary.py
```

## Validation

Each workflow doc has a **Validation** section (row-count and totals checks,
Hive/Mongo cross-queries, and a diff against the fallback output).
`tests/unit/test_alteryx_blend.py` unit-tests the formulas in
[`alteryx/fallback/blend_rules.py`](../../alteryx/fallback/blend_rules.py).
