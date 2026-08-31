# `alteryx/fallback/` — headless reference implementations

**These are not Alteryx workflows and they do not produce Alteryx output.**

They are plain Python re-implementations of the two Phase 12 workflows, so that:

- the blend logic can run on a headless box / CI (no Designer),
- the formulas are unit-tested (`tests/unit/test_alteryx_blend.py`),
- a real Alteryx Designer run can be **diffed** against a known-good baseline.

This mirrors `ASSUMPTIONS.md` I10 (a PySpark/pandas fallback for each Alteryx
workflow).

| File | Mirrors | Reads | Writes |
|---|---|---|---|
| `blend_rules.py` | both — the Formula-tool expressions | — | — |
| `customer_risk_blend.py` | Workflow 1 | Hive `customer_fraud_summary` + `customer_clv` (beeline), MongoDB `customers` (mongosh), HDFS `churn_alerts` (Spark, optional) | `../outputs/customer_risk_blend.xlsx` + `.csv` |
| `transaction_summary.py` | Workflow 2 | Hive `finsight.transactions` via Spark inside `finsight-spark-master` | `../outputs/transaction_summary.csv` |

All I/O goes through `docker exec` — no Hive/Mongo Python driver needed; only
`pandas` + `openpyxl` (in `requirements.txt`).

```bash
python alteryx/fallback/customer_risk_blend.py [--normalize-fraud-pct] [--no-churn-alerts]
python alteryx/fallback/transaction_summary.py
```

Outputs land in `alteryx/outputs/` (git-ignored). Nothing here is presented as
the result of an executed Alteryx workflow.
