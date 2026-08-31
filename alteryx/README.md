# FinSight — `alteryx/`

Phase 12: Alteryx Data Blending. Two Designer workflows that turn the FinSight
serving layer into Power BI-ready extracts.

```
alteryx/
├── workflows/          Designer build guides (tool-by-tool). Build .yxmd from these (git-ignored).
│   ├── 01_customer_risk_blend.md
│   └── 02_transaction_summary.md
├── prereq/
│   └── customer_fraud_summary_external.hql   Hive table over the Phase 9 output (WF1 input 1)
├── fallback/           Headless pandas/Spark reference impls - NOT Alteryx artifacts
│   ├── README.md
│   ├── blend_rules.py          formulas (unit-tested: tests/unit/test_alteryx_blend.py)
│   ├── customer_risk_blend.py
│   └── transaction_summary.py
├── inputs/             (git-ignored) staged input extracts, if you export any
└── outputs/            (git-ignored) workflow outputs land here
```

## No `.yxmd` files are committed

Alteryx Designer is a licensed Windows desktop app; this repo's toolchain has no
Designer install, so a `.yxmd` cannot be authored **and verified to open/run**.
Committing an unverifiable XML file — or a fake "it ran" output — is explicitly
out. Instead:

- **`alteryx/workflows/*.md`** — every tool (Input, Join, Select, Formula,
  Filter, Summarize, Output) with its exact config. Rebuild each workflow
  tool-for-tool.
- **`docs/alteryx/`** — the analytical spec: field mappings, formulas, joins,
  expected output columns, execution steps, validation.
- **`alteryx/fallback/*.py`** — a headless implementation of each workflow that
  reads the same live sources and writes the same output, so the blend is
  reproducible and a real Designer run can be diffed against it.

`*.yxmd` / `*.yxmc` are git-ignored — build them locally from the guides.

## Quick start (headless)

```bash
pip install -r requirements.txt                        # pandas + openpyxl
scripts/start.sh hive spark

# upstream (once): Phases 2-3 ingest, 7 CLV, 8 warehouse, 9 customer_summary
docker exec -i finsight-hiveserver2 beeline -u jdbc:hive2://localhost:10000/ \
  < alteryx/prereq/customer_fraud_summary_external.hql

python alteryx/fallback/customer_risk_blend.py     # -> alteryx/outputs/customer_risk_blend.xlsx
python alteryx/fallback/transaction_summary.py     # -> alteryx/outputs/transaction_summary.csv
```

See [`docs/alteryx/README.md`](../docs/alteryx/README.md) for the full data-source
map and execution order.
