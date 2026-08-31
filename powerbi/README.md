# FinSight — `powerbi/`

Phase 13: Power BI dashboard. One report, **exactly three pages** — Fraud Alert
Board · Customer 360 · Risk & Compliance Report.

```
powerbi/
├── export_datasets.py        builds powerbi/exports/*.csv from the live stack
├── model_helpers.py          step -> timestamp / DimDate / DimTransactionType (unit-tested)
├── kafka_bridge/
│   └── txn_flagged_bridge.py  tails txn-flagged -> exports/flagged_transactions.csv (G14)
├── measures/
│   └── measures.dax           every DAX measure, copy-paste ready
└── exports/                   (git-ignored) generated import files + FinSight.pbix
```

Full documentation: **[`docs/powerbi/`](../docs/powerbi/)** —
[README](../docs/powerbi/README.md) ·
[data model](../docs/powerbi/data-model.md) ·
[DAX](../docs/powerbi/dax-measures.md) ·
[page 1](../docs/powerbi/page-1-fraud-alert-board.md) ·
[page 2](../docs/powerbi/page-2-customer-360.md) ·
[page 3](../docs/powerbi/page-3-risk-compliance-report.md) ·
[validation](../docs/powerbi/validation.md).

## No `.pbix` is committed — and how to deliver one

Power BI Desktop is a licensed Windows desktop app with no install in this
toolchain, so a `.pbix` cannot be built *and verified*. Nothing here is a
fabricated report, screenshot, or "it refreshed" claim.

**When you build it** (following `docs/powerbi/`):

1. Save as `powerbi/FinSight.pbix`. `*.pbix` and `powerbi/exports/` are
   **git-ignored** — do not commit the binary.
2. **Deliver** by publishing to the Power BI Service workspace **`FinSight`**
   (`app.powerbi.com`), or attach the `.pbix` to the project release / hand-off
   drive. Add the report URL to `docs/powerbi/README.md`.
3. **Refresh:** either re-run `export_datasets.py` + the bridge before a manual
   Desktop refresh, or configure scheduled refresh in the Service via an
   on-premises data gateway pointed at the same `powerbi/exports/` folder (or a
   shared network path the queries use).

## Quick start (headless prep)

```bash
pip install -r requirements.txt
scripts/start.sh hive spark

# populate upstream first (see docs/powerbi/README.md "Build order")
spark/streaming/run_fraud_detection.sh --once --starting-offsets earliest

python powerbi/kafka_bridge/txn_flagged_bridge.py --once --from-beginning
python powerbi/export_datasets.py
python powerbi/export_datasets.py --list        # readiness report

pytest tests/unit/test_powerbi_model.py -q
```
