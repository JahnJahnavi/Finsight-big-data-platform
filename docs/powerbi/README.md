# FinSight — Phase 13: Power BI Dashboard

A single Power BI report, **exactly three pages** (spec section 13):

| # | Page | Audience |
|---|---|---|
| 1 | [Fraud Alert Board](page-1-fraud-alert-board.md) | fraud ops — real-time |
| 2 | [Customer 360](page-2-customer-360.md) | relationship managers / analytics |
| 3 | [Risk & Compliance Report](page-3-risk-compliance-report.md) | risk & compliance |

Supporting docs: **[data-model.md](data-model.md)** (tables, relationships,
calculated columns) · **[dax-measures.md](dax-measures.md)** (every measure) ·
**[validation.md](validation.md)**.

## No `.pbix` is committed

Power BI Desktop is a licensed Windows desktop app; this toolchain has no
install, so a `.pbix` cannot be authored *and verified to open/refresh*. No
`.pbix`, and no screenshot or "it refreshed" claim, is fabricated. Delivered
instead:

1. **These docs** — data sources, imports, relationships, model, DAX,
   calculated columns, filters, slicers, visuals, layout, validation. An analyst
   with Power BI Desktop rebuilds the report from them.
2. **`powerbi/export_datasets.py`** — materialises every source table as a flat
   file in `powerbi/exports/` (git-ignored) for Import mode.
3. **`powerbi/kafka_bridge/txn_flagged_bridge.py`** — tails `txn-flagged` into a
   rolling CSV (Power BI can't read Kafka — `ASSUMPTIONS.md` G14).
4. **`powerbi/measures/measures.dax`** — all measures as copy-paste text.

### Where the `.pbix` lives once built

- Build it locally per these docs; save as **`powerbi/FinSight.pbix`**.
- `*.pbix` is **git-ignored** (binary, can't diff/verify) — do **not** commit it.
- **Delivery:** publish to the Power BI Service workspace **`FinSight`**
  (`app.powerbi.com` → *Publish*), or attach the `.pbix` to the project release.
  Record the workspace/report URL in this file when it exists.
- Keep the report's queries pointed at `powerbi/exports/` (relative) or a shared
  path agreed with the team; set **scheduled refresh** in the Service against
  the same files on a gateway, or re-run `export_datasets.py` + the bridge
  before each manual refresh.

## Data sources

| Model table | File (`powerbi/exports/`) | Origin | Produced by |
|---|---|---|---|
| `FlaggedTransactions` | `flagged_transactions.csv` | Kafka `txn-flagged` | `powerbi/kafka_bridge/txn_flagged_bridge.py` (Phase 4) |
| `StreamingMetrics` | `streaming_metrics.csv` | HDFS `/finsight/processed/streaming_metrics/` | `spark/streaming/run_fraud_detection.sh` |
| `TransactionSummary` | `transaction_summary.csv` | Alteryx WF2 | `alteryx/fallback/transaction_summary.py` (Phase 12) |
| `DailySummary` | `daily_summary.csv` | HDFS `/finsight/exports/daily_summary/` | `spark/batch/run_risk_scoring.sh` (Phase 6) |
| `ComplianceSummary` | `compliance_summary.csv` | HDFS `/finsight/exports/compliance_summary.csv` | `sql/run_spark_sql.sh --mode compliance` (Phase 9) |
| `DormancyReport` | `dormancy_report.csv` | HDFS `/finsight/exports/dormancy_report.csv` | `sql/run_spark_sql.sh --mode dormancy` (Phase 9) |
| `DimCustomer` | `dim_customer.csv` | MongoDB `customers` ⋈ Hive `customer_clv` (+ `risk_scores`) | `mongodb/import_customers.sh`, `clv_scoring.py`, `risk_scoring.py` |
| `CustomerProducts` | `customer_products.csv` | MongoDB `customers.products[]` | `export_datasets.py` |
| `DimDate` | `dim_date.csv` | generated — `step → SIM_EPOCH + (step-1)h` (I11) | `powerbi/model_helpers.py` |
| `DimTransactionType` | `dim_transaction_type.csv` | the 5 spec types | `powerbi/model_helpers.py` |

> **Alternative — DirectQuery to Hive:** for `TransactionSummary` /
> `ComplianceSummary` etc. you can connect Power BI's Hive ODBC to HiveServer2
> (`localhost:10000`, DB `finsight`) instead of importing CSVs. Aggregations on
> `finsight.transactions` may return 0 rows in this dev stack (Tez not recursing
> `step=<N>/` sub-dirs) — Import mode from `export_datasets.py` is the reliable
> path.

## Build order

```bash
pip install -r requirements.txt
scripts/start.sh hive spark

# populate upstream (see each phase doc)
#   Phases 2-3 ingest · 6 risk_scoring · 7 clv_scoring · 8 warehouse
#   9 compliance + dormancy · 10 mongo import · 12 Alteryx WF2
spark/streaming/run_fraud_detection.sh --once --starting-offsets earliest   # fills txn-flagged + streaming_metrics

python powerbi/kafka_bridge/txn_flagged_bridge.py --once --from-beginning   # -> flagged_transactions.csv
python powerbi/export_datasets.py                                           # -> all other exports

# then, in Power BI Desktop:
#   Get Data -> Text/CSV -> powerbi/exports/*.csv  (see data-model.md)
#   build relationships, paste measures.dax, build the 3 pages per the page docs
```
