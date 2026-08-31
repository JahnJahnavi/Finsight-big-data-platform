# Power BI — Validation

Run after building the `.pbix`. Each check has a source-of-truth query so the
report can be reconciled against the pipeline.

## 0. Prerequisites present

```bash
python powerbi/export_datasets.py --list
```

Every row should read `OK`. `SKIP` rows list the exact command that produces
the missing dataset — run those, then re-run without `--list`.

## 1. Model integrity (Power BI)

| Check | How |
|---|---|
| No inactive/ambiguous relationships flagged | Model view — all relationships solid, single-direction except `CustomerProducts` |
| `DimDate` marked as Date table | Model view → DimDate → *Mark as date table* (`event_ts` / `Date`) |
| No blank-key rows in dimensions | `DimCustomer`, `DimDate`, `DimTransactionType` — filter each key `is not blank` == full row count |
| Fact rows not orphaned | add a temp card `COUNTROWS(FILTER(FlaggedTransactions, ISBLANK(RELATED(DimTransactionType[transaction_type]))))` → **0** |

## 2. Numbers reconcile with the pipeline

| Report figure | Source-of-truth |
|---|---|
| `[Flagged Transactions]` | `wc -l powerbi/exports/flagged_transactions.csv` (− 1 header); `SUM(StreamingMetrics[flagged_count])` |
| `[Fraud Rate %]` | `docker exec finsight-namenode hdfs dfs -cat /finsight/processed/streaming_metrics/*` → `sum(flagged)/sum(total)*100` |
| `[Transaction Count]` (Page 3) | `SELECT COUNT(*) FROM finsight.transactions WHERE step BETWEEN 1 AND 168` (via Spark — see the Hive/Tez note) → **4000** |
| `[Total Transaction Volume]` | `SUM(total_volume)` in `transaction_summary.csv` → **53,684,404.31** |
| `[Customers]` | `db.customers.countDocuments({})` → **10 000** |
| segment donut | `mongodb/validation.js` counts |
| `[Product Holdings]` | `wc -l powerbi/exports/customer_products.csv` (− 1) → **26 095** |
| compliance table | `sql/run_spark_sql.sh --mode compliance` |
| dormancy cards | `sql/run_spark_sql.sh --mode dormancy` row count by `dormancy_severity` |
| `[False Positive Rate %]` | `awk -F, 'NR>1{n++; fp+=$14} END{print 100*fp/n}' powerbi/exports/flagged_transactions.csv` (col 14 = `false_positive`) |

## 3. Business-rule spot checks

| Rule | Check |
|---|---|
| Fraud rule (Phase 4) | every `FlaggedTransactions` row: `type ∈ {TRANSFER, CASH_OUT}`, `amount > 200000`, `newbalanceDest = 0` |
| Compliance risk tiers (I34) | `risk_classification` = High ⇔ `fraud_rate_pct ≥ 5`; Medium ⇔ `≥ 1`; else Low |
| Dormancy severity (I36) | Dormant ⇔ `72 < steps_inactive ≤ 120`; Severely Dormant ⇔ `> 120` |
| Date mapping (I11) | `DimDate` step 1 → 2023-01-01 00:00 UTC; step 168 → 2023-01-07 23:00 |

## 4. Page requirements (spec section 13)

- **Exactly three pages**, named *Fraud Alert Board*, *Customer 360*,
  *Risk & Compliance Report*.
- Page 1 contains: total flagged, fraud rate, flagged value, false-positive
  rate, daily fraud trend, fraud count by type, recent flagged table. ✔
- Page 2 contains: risk vs churn, segment distribution, churn by
  segment/channel, CLV tiers, product holdings. ✔
- Page 3 contains: weekly fraud-rate trend, volume by type, top-20 flagged
  accounts, compliance summary, dormant + severely-dormant, transaction-type
  slicer. ✔

## 5. Unit tests

```bash
pytest tests/unit/test_powerbi_model.py -q     # DimDate / step-mapping logic
```
