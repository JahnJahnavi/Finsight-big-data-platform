# Designer build — Workflow 1: Customer Risk Blend

Rebuild this workflow tool-for-tool in Alteryx Designer. Analytical spec (field
mappings, formulas, validation): [`docs/alteryx/workflow-1-customer-risk-blend.md`](../../docs/alteryx/workflow-1-customer-risk-blend.md).

Save as: `alteryx/workflows/01_customer_risk_blend.yxmd`. `*.yxmd` is
**git-ignored** (Phase 12 does not commit unverifiable Designer artifacts) —
rebuild from these steps.

## Connections (one-time)

- **ODBC DSN `FinSight Hive`** — Hive ODBC driver → Host `localhost`, Port
  `10000`, Database `finsight`, Auth `User Name`, user `hive` (no password, dev),
  Thrift Transport `SASL`. Test.
- **MongoDB** — Alteryx *Input Data* → *Data sources* → MongoDB → Host
  `localhost:27017`, Database `finsight`, Username / Password from `.env`
  (`MONGO_INITDB_ROOT_*`), **Authentication database `admin`**.

## Canvas

```
[1 Input: customer_fraud_summary]──┐
                                   ├──[5 Join (customerId, inner)]──┐
[2 Input: MongoDB customers]───────┘        └─J──────────────────┐  │
                                                                 │  │
[3 Input: customer_clv]──────────────[6 Join (customerId)]───────┘  │
                                          L∪J                       │
[4 Input: churn_alerts]──[8 Summarize]──[7 Join (customerId)]───────┘
                                          L∪J
                                            │
                          [9 Formula]──[10 Select]──[11 Sort]──[12 Output .xlsx]
```

## Tools

### 1 — Input Data · `customer_fraud_summary`
- Connection: `FinSight Hive` DSN.
- **Pre-SQL / query:**
  `SELECT customerId, total_transactions, total_amount, confirmed_fraud_count, fraud_rate_pct FROM customer_fraud_summary`
- Prereq: run `sql/run_spark_sql.sh --mode customer_summary`, then apply
  `alteryx/prereq/customer_fraud_summary_external.hql`.

### 2 — Input Data · MongoDB `customers`
- MongoDB connection above. Collection `customers`.
- Fields to keep (Select inside the tool, or a Select tool after):
  `customerId, segment, churn_probability, risk_score, composite_risk_score, kyc_status, is_active`

### 3 — Input Data · `customer_clv`
- `FinSight Hive` DSN.
  `SELECT customerId, clv_score, clv_classification FROM customer_clv`

### 4 — Input Data · `churn_alerts` *(optional)*
- Parquet folder `/finsight/processed/churn_alerts/` via ODBC, **or** a CSV you
  exported. If unavailable, delete tools 4 and 8 and wire a Formula default of
  `churn_signal_count = 0` after tool 7.

### 5 — Join · fraud summary × profile
- Join by **Specific Fields**: Left `customerId` = Right `customerId`.
- Take the **J** (inner) output — customers with both a history and a profile.
- In the Join config, deselect the duplicate right-side `customerId` (`Right_customerId`).

### 6 — Join · + CLV  (keep all left rows)
- Left = tool 5 **J** output; Right = tool 3.
- Key `customerId`. Wire **both** the **L** and **J** outputs into a **Union**
  (tool 6b) so unmatched customers keep null CLV columns.

### 7 — Join · + churn  (keep all left rows)
- Left = tool 6 union; Right = tool 8. Key `customerId`. Union **L** + **J** again.

### 8 — Summarize · churn_alerts → one row per customer
- Group By `customerId`; `signal_count` → **Max** → output `churn_signal_count`.

### 9 — Formula
| Output field | Type | Expression |
|---|---|---|
| `churn_signal_count` | Int64 | `IF IsNull([churn_signal_count]) THEN 0 ELSE [churn_signal_count] ENDIF` |
| `is_churn_flagged` | Int16 | `IF [churn_signal_count] > 0 THEN 1 ELSE 0 ENDIF` |
| `profile_composite_risk` | Double | `[composite_risk_score]`  *(rename via Select in tool 10; see note)* |
| `composite_risk_score` | Double | `([fraud_rate_pct] * 0.6) + ([churn_probability] * 0.4)` |

> The Mongo field `composite_risk_score` and the computed one share a name.
> Simplest: in tool 2's Select, **rename** the incoming Mongo field to
> `profile_composite_risk` up front, then tool 9 only creates the new
> `composite_risk_score`. (ASSUMPTIONS G16.)

### 10 — Select
- Rename Hive's lower-case `customerid` → `customerId` if needed.
- Column order:
  `customerId, segment, total_transactions, total_amount, confirmed_fraud_count, fraud_rate_pct, churn_probability, is_churn_flagged, churn_signal_count, clv_score, clv_classification, risk_score, profile_composite_risk, composite_risk_score`
- Deselect everything else (`kyc_status`, `is_active`, join artifacts).

### 11 — Sort
- `composite_risk_score` — **Descending**.

### 12 — Output Data
- File type **Excel `.xlsx`**, path `alteryx/outputs/customer_risk_blend.xlsx`,
  sheet `customer_risk_blend`, **Overwrite Sheet (Drop)**.

## Run & check

- Ctrl+R. Results window: record count ≈ **287** (synthetic) — equals distinct
  `customerId`.
- Open the `.xlsx`: 14 columns, header row, one row per customer, sorted by
  `composite_risk_score` desc.
- Cross-check against `python alteryx/fallback/customer_risk_blend.py`.
