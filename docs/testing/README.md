# FinSight — End-to-End Testing & Validation (Phase 14)

The whole pipeline, verified from one command. Two scripts:

| Script | Role |
|---|---|
| [`scripts/run_e2e_pipeline.sh`](../../scripts/run_e2e_pipeline.sh) | **driver** — runs every pipeline stage in order (producer → streaming → batch → Spark SQL → Mongo/Neo4j → Alteryx/Power BI prep). Idempotent; runs *existing* jobs only, never edits code. |
| [`scripts/validate_e2e.py`](../../scripts/validate_e2e.py) | **validator** — read-only. Walks all 21 checkpoints + the cross-cutting checks and writes [`E2E_REPORT.md`](E2E_REPORT.md). |

```bash
pip install -r requirements.txt
scripts/start.sh                       # bring the stack up, wait healthy

scripts/run_e2e_pipeline.sh            # populate every output  (~20-40 min)
python scripts/validate_e2e.py         # verify -> docs/testing/E2E_REPORT.md
```

`validate_e2e.py` also runs standalone against whatever state exists — missing
outputs report **BLOCKED** (with the job to run), not FAIL.

## The 21 checkpoints

| # | Checkpoint | What is verified |
|---|---|---|
| 1 | Kafka | broker running; `txn-raw` / `txn-flagged` / `txn-churn` exist |
| 2 | txn-raw | 3 partitions; ≥ 100 messages produced |
| 3 | Kafka Connect | REST up; `finsight-hdfs-sink-txn-raw` + all tasks `RUNNING` |
| 4 | HDFS | NameNode UI; `/finsight/{raw,processed,checkpoints,exports}` exist |
| 5 | Parquet | `raw/txn-raw/step=*/` partitions; parquet schema has step/type/amount/nameOrig/isFraud |
| 6 | Fraud Streaming | `streaming_metrics` JSON rows; schema; `0 ≤ fraud_rate ≤ 100` |
| 7 | txn-flagged | topic present; **every flagged row obeys the fraud rule** (type∈{TRANSFER,CASH_OUT}, amount>200000, newbalanceDest=0) |
| 8 | Churn Streaming | `churn_alerts` parquet rows; schema (customerId, signals, signal_count) |
| 9 | txn-churn | topic present; alert messages ≥ 0 |
| 10 | Risk Scoring | `risk_scores` parquet; schema (customerId, risk_score, risk_tier) |
| 11 | CLV Scoring | `clv_scores` parquet; schema; 100 ≤ rows ≤ 20000 |
| 12 | Hive transactions | `finsight.transactions` exists; 13-col schema; count |
| 13 | Hive fraud view | `vw_fraud_transactions` == `transactions WHERE isFraud=1` |
| 14 | Hive summary mart | `txn_summary_mart` exists; 9 spec fields |
| 15 | Spark SQL compliance | `compliance_summary` ≤ 5 per-type rows |
| 16 | Customer fraud summary | Hive table + HDFS parquet; schema |
| 17 | Dormancy | `dormancy_report` parquet (+ CSV export); schema |
| 18 | MongoDB | `customers` = 10 000; compound index `{customerId, segment}`; the 5 segments |
| 19 | Neo4j | Account/Transaction nodes; SENT/RECEIVED_BY; fraud-ring query runs |
| 20 | Alteryx outputs | `customer_risk_blend.*` + `transaction_summary.csv`; schemas |
| 21 | Power BI datasets | `dim_*`, `customer_products`, `transaction_summary` built; optional exports |

### Cross-cutting

- **Secrets** — no tracked `.env`, no `*.pem/.key`, no real `CHANGE_ME_*` values or private-key blocks in tracked source.
- **Large files** — no tracked file > 5 MB; no tracked `.parquet/.csv/.avro` datasets (test fixtures excepted).
- **Services** — all 11 containers `running`.
- **Schemas / record counts / business rules** — checked inline per checkpoint (see table).

## Pipeline flows validated

```
Transactions.csv → kafka/producer.py → Kafka txn-raw → fraud_detection.py → Kafka txn-flagged   (+ streaming_metrics)
Kafka txn-raw → Kafka Connect → HDFS Parquet → Hive → Spark SQL (compliance / customer_summary / dormancy)
HDFS raw → risk_scoring.py → /finsight/processed/risk_scores
HDFS raw → clv_scoring.py  → /finsight/processed/clv_scores
noveacrest_customers.json → mongoimport → MongoDB finsight.customers
neo4j_*.csv → neo4j/loader.py → (Account)-[:SENT]->(Transaction)-[:RECEIVED_BY]->(Account) → fraud_ring.cypher
Hive + MongoDB + Spark outputs → alteryx/fallback/*.py → powerbi/export_datasets.py + kafka_bridge → Power BI import files
```

## Status meanings

| Status | Meaning | Action |
|---|---|---|
| **PASS** | checkpoint verified | — |
| **FAIL** | a real defect | investigate; **do not** auto-fix — report first |
| **BLOCKED** | upstream output missing | run the named job / `run_e2e_pipeline.sh`, re-validate |
| **WARN** | works but degraded (synthetic-data or dev-stack quirk) | see [known-issues.md](known-issues.md) |

Latest run: [`E2E_REPORT.md`](E2E_REPORT.md).
