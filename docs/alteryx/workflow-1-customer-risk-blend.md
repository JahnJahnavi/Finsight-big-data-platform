# Alteryx Workflow 1 — Customer Risk Blend

Blend the per-customer fraud summary, MongoDB profile, CLV score and churn
signal into one customer-grain table with a composite risk score, for the
Power BI customer risk page (spec section 12).

- **Designer build:** [`alteryx/workflows/01_customer_risk_blend.md`](../../alteryx/workflows/01_customer_risk_blend.md) (tool-by-tool)
- **Headless equivalent:** [`alteryx/fallback/customer_risk_blend.py`](../../alteryx/fallback/customer_risk_blend.py)
- **Output:** `alteryx/outputs/customer_risk_blend.xlsx` (+ `.csv`)

```
 Input: Hive customer_fraud_summary ─┐
 Input: MongoDB customers ───────────┤─► Join (customerId) ─► Formula ─► Select ─► Output .xlsx
 Input: Hive customer_clv ───────────┤       (+ Join x2)      composite   column
 Input: HDFS churn_alerts (optional)─┘                        _risk_score  order
```

## 1. Inputs

| # | Alteryx **Input Data** tool | Connection | Query / file |
|---|---|---|---|
| 1 | Hive `finsight.customer_fraud_summary` | ODBC DSN `FinSight Hive` → HiveServer2 `localhost:10000`, DB `finsight` | `SELECT customerId, total_transactions, total_amount, confirmed_fraud_count, fraud_rate_pct FROM customer_fraud_summary` |
| 2 | MongoDB `finsight.customers` | MongoDB ODBC / Data connector → `finsight-mongodb:27017`, auth DB `admin` | collection `customers`, fields `customerId, segment, churn_probability, risk_score, composite_risk_score, kyc_status, is_active` |
| 3 | Hive `finsight.customer_clv` | same Hive DSN | `SELECT customerId, clv_score, clv_classification FROM customer_clv` |
| 4 | HDFS `/finsight/processed/churn_alerts/` *(optional)* | Parquet via ODBC or a pre-exported CSV | aggregate to one row per `customerId`: `MAX(signal_count) AS churn_signal_count` |

> **Prerequisite:** input 1 needs the Hive table created first —
> `sql/run_spark_sql.sh --mode customer_summary` then apply
> [`alteryx/prereq/customer_fraud_summary_external.hql`](../../alteryx/prereq/customer_fraud_summary_external.hql).
> Input 4 is optional; if `churn_alerts` is empty the workflow proceeds with
> `churn_signal_count = 0`.

## 2. Field mappings

| Source field | Source | Workflow field | Type | Notes |
|---|---|---|---|---|
| `customerId` | all | `customerId` | String | **join key**; preserved exactly (Mongo camelCase; Hive returns lower-case `customerid` — rename in a Select tool) |
| `total_transactions` | fraud summary | `total_transactions` | Int64 | |
| `total_amount` | fraud summary | `total_amount` | Double | |
| `confirmed_fraud_count` | fraud summary | `confirmed_fraud_count` | Int64 | |
| `fraud_rate_pct` | fraud summary | `fraud_rate_pct` | Double | **0–100** percentage (ASSUMPTIONS I48) |
| `segment` | Mongo profile | `segment` | String | Premium / Standard / Basic / Private Banking / Student |
| `churn_probability` | Mongo profile | `churn_probability` | Double | **0–1** predicted probability |
| `risk_score` | Mongo profile | `risk_score` | Double | profile's own model score, carried through |
| `composite_risk_score` | Mongo profile | `profile_composite_risk` | Double | **renamed** to avoid colliding with the field this workflow computes (ASSUMPTIONS G16) |
| `clv_score` | customer_clv | `clv_score` | Double | 0–1 |
| `clv_classification` | customer_clv | `clv_classification` | String | High Value / Growth Potential / At Risk |
| `signal_count` (max) | churn_alerts | `churn_signal_count` | Int64 | 0 when not flagged / absent |
| *(derived)* | Formula | `is_churn_flagged` | Int64 | `IF [churn_signal_count] > 0 THEN 1 ELSE 0 ENDIF` |
| *(derived)* | Formula | `composite_risk_score` | Double | see §4 |

## 3. Joins

All joins on **`customerId`**. Use the **Join** tool (and take the **J** = inner
or **L** = left output as noted); a Join-Multiple tool works too.

| Join | Left | Right | Key | Keep | Cardinality |
|---|---|---|---|---|---|
| A | Input 1 (fraud summary) | Input 2 (Mongo profile) | `customerId` | **inner (J)** — customers that have *both* a transaction history and a profile | 1:1 |
| B | A result | Input 3 (customer_clv) | `customerId` | **left (L + J unioned)** — keep all rows from A | 1:1 |
| C | B result | Input 4 (churn_alerts agg) | `customerId` | **left** — keep all rows from B | 1:1 |

For B and C, `Union` the **L** (unmatched-left) and **J** outputs so every
customer from Join A survives; null CLV / churn columns are expected for
customers without those records.

> On the bundled synthetic data the fraud-summary customer IDs and the profile
> customer IDs are **different populations** — Join A returns ~287 rows. With
> real NovaCrest data the two populations coincide.

## 4. Formulas (Formula tool)

```
// composite risk score — spec section 12 (verbatim, weights frozen)
composite_risk_score = ([fraud_rate_pct] * 0.6) + ([churn_probability] * 0.4)

// churn flag from the optional streaming input
is_churn_flagged = IF IsNull([churn_signal_count]) OR [churn_signal_count] = 0
                   THEN 0 ELSE 1 ENDIF

// null-safe the optional column first (Formula tool, above the line above)
churn_signal_count = IF IsNull([churn_signal_count]) THEN 0 ELSE [churn_signal_count] ENDIF
```

> **Scale note (ASSUMPTIONS I48):** `fraud_rate_pct` is 0–100 while
> `churn_probability` is 0–1, so with the spec weights a fraudulent customer's
> score is dominated by the fraud term (e.g. `100*0.6 + 0.30*0.4 = 60.12`). The
> formula is implemented **exactly as the spec states**. If the owner intends
> both inputs on a 0–1 scale, add `fraud_rate_frac = [fraud_rate_pct] / 100`
> before the composite formula (the fallback's `--normalize-fraud-pct` flag) —
> **pending sign-off**, not applied by default.

## 5. Select tool — output column order

`customerId, segment, total_transactions, total_amount, confirmed_fraud_count,
fraud_rate_pct, churn_probability, is_churn_flagged, churn_signal_count,
clv_score, clv_classification, risk_score, profile_composite_risk,
composite_risk_score`

Sort (Sort tool) by `composite_risk_score` descending.

## 6. Output tool

- **Output Data** → `.xlsx`, sheet `customer_risk_blend`, write to
  `alteryx/outputs/customer_risk_blend.xlsx` (overwrite sheet).
- "Suitable for Power BI": single flat sheet, header row, no merged cells, one
  row per `customerId`.

## 7. Execution steps

1. `scripts/start.sh hive spark` and confirm `finsight-hiveserver2`,
   `finsight-mongodb` healthy.
2. Populate upstream: Phases 2–3 (ingest) → Phase 7 (`clv_scoring.py`) →
   Phase 8 (`hive/run_warehouse.sh`) → Phase 9
   (`sql/run_spark_sql.sh --mode customer_summary`).
3. `docker exec -i finsight-hiveserver2 beeline -u jdbc:hive2://localhost:10000/ < alteryx/prereq/customer_fraud_summary_external.hql`
4. *(optional)* Phase 5 `churn_detection.py` to populate `churn_alerts`.
5. Open the workflow in Alteryx Designer, set the two DSNs, **Run** (Ctrl+R).
6. Confirm the Output tool wrote `alteryx/outputs/customer_risk_blend.xlsx`.

Headless: `python alteryx/fallback/customer_risk_blend.py`

## 8. Expected output columns

| Column | Type | Example |
|---|---|---|
| `customerId` | String | `C2965266687` |
| `segment` | String | `Standard` |
| `total_transactions` | Int64 | `1` |
| `total_amount` | Double | `658485.60` |
| `confirmed_fraud_count` | Int64 | `1` |
| `fraud_rate_pct` | Double | `100.0` |
| `churn_probability` | Double | `0.3064` |
| `is_churn_flagged` | Int64 | `0` |
| `churn_signal_count` | Int64 | `0` |
| `clv_score` | Double | `0.698254` |
| `clv_classification` | String | `Growth Potential` |
| `risk_score` | Double | `0.2194` |
| `profile_composite_risk` | Double | `0.2542` |
| `composite_risk_score` | Double | `60.12256` |

Row count = Join A matches (synthetic: ~287; real: ≈ number of active customers
with a transaction history).

## 9. Validation

| Check | How |
|---|---|
| Row count = distinct `customerId` | `Summarize` CountDistinct `customerId` == record count; no fan-out from joins |
| `customerId` preserved | spot-check 5 IDs against Mongo `db.customers.findOne({customerId:...})` and Hive `customer_fraud_summary` |
| `composite_risk_score` maths | for 3 rows, hand-compute `fraud_rate_pct*0.6 + churn_probability*0.4` |
| No unexpected `segment` | values ⊆ {Premium, Standard, Basic, Private Banking, Student} |
| Diff vs fallback | `python alteryx/fallback/customer_risk_blend.py` then compare `alteryx/outputs/customer_risk_blend.csv` row-for-row (join on `customerId`, assert `composite_risk_score` within 1e-6) |
| Formula unit tests | `pytest tests/unit/test_alteryx_blend.py` |
