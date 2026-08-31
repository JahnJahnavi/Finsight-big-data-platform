# Power BI — Data Model

Star schema. Facts are dated by `step` → `DimDate`; typed by transaction type →
`DimTransactionType`; customer-keyed facts join `DimCustomer`.

```
                    ┌───────────────┐
                    │    DimDate     │  (step grain, 1 row/hour)
                    │  step (key)    │
                    └──────┬────────┘
        ┌──────────────────┼───────────────────┬───────────────────┐
        │ step             │ step              │ step              │
 ┌──────┴───────┐  ┌───────┴────────┐  ┌───────┴───────┐   ┌───────┴────────┐
 │FlaggedTxns   │  │TransactionSum. │  │ DailySummary  │   │ StreamingMetrics│
 │ nameOrig ────┼┐ │ transaction_ ──┼┐ │ type ─────────┼┐  │ (batch grain,   │
 │ type ────────┼┼┐│  type          ││ │ step          ││  │  standalone)    │
 └──────────────┘│││└────────────────┘│ └───────────────┘│  └────────────────┘
                 │││                  │                  │
   ┌─────────────┴┴┴──┐    ┌──────────┴──────────────────┴─┐
   │  DimCustomer      │    │      DimTransactionType        │
   │  customerId (key) │    │      transaction_type (key)    │
   └───┬────────┬─────┘    └──────────┬─────────────────────┘
       │        │                     │
 ┌─────┴────┐ ┌─┴──────────────┐ ┌────┴──────────────┐
 │Dormancy  │ │CustomerProducts│ │ ComplianceSummary │
 │Report    │ │ (1:*)          │ │ (transaction_type)│
 │customerId│ └────────────────┘ └───────────────────┘
 └──────────┘
```

## Tables

### Dimensions

| Table | File | Key | Columns |
|---|---|---|---|
| **DimDate** | `dim_date.csv` | `step` | `step, event_ts (datetime), date, hour_of_day, day_name, iso_week, week_start` |
| **DimTransactionType** | `dim_transaction_type.csv` | `transaction_type` | `transaction_type, type_group` (inflow/outflow) |
| **DimCustomer** | `dim_customer.csv` | `customerId` | `customerId, age, country, segment, annualIncome, productCount, kyc_status, risk_score, churn_probability, profile_composite_risk, account_opened, last_transaction_date, preferred_channel, email_verified, is_active, clv_score, clv_classification` (+ `model_risk_score, risk_tier` when `risk_scores` is loaded) |
| **CustomerProducts** | `customer_products.csv` | — (composite `customerId`+`product`) | `customerId, product` — exploded `products[]` |

### Facts

| Table | File | Grain | Key columns → dimension |
|---|---|---|---|
| **FlaggedTransactions** | `flagged_transactions.csv` | one flagged transaction | `step`→DimDate, `type`→DimTransactionType, `nameOrig`→DimCustomer |
| **StreamingMetrics** | `streaming_metrics.csv` | one Spark micro-batch | `batch_ts` (own datetime; standalone) |
| **TransactionSummary** | `transaction_summary.csv` | `transaction_type × step` | `step`→DimDate, `transaction_type`→DimTransactionType |
| **DailySummary** | `daily_summary.csv` | `type × step` | `step`→DimDate, `type`→DimTransactionType |
| **ComplianceSummary** | `compliance_summary.csv` | `transaction_type` | `transaction_type`→DimTransactionType |
| **DormancyReport** | `dormancy_report.csv` | one dormant customer | `customerId`→DimCustomer |

Fact column detail:

- `FlaggedTransactions`: `txnId, step, event_ts, type, amount, nameOrig,
  oldbalanceOrg, newbalanceOrig, nameDest, oldbalanceDest, newbalanceDest,
  isFraud, isFlaggedFraud, false_positive, fraud_rule, detected_at, bridged_at`
- `StreamingMetrics`: `batch_id, batch_ts, total_count, flagged_count,
  fraud_rate_pct, app_name, fraud_rule`
- `TransactionSummary`: `transaction_type, step, total_volume,
  transaction_count, avg_transaction_amount, fraud_count`
- `DailySummary`: `type, step, transaction_volume, total_amount, fraud_count`
- `ComplianceSummary`: `transaction_type, transaction_count, transaction_volume,
  fraud_count, fraud_rate_pct, risk_classification`
- `DormancyReport`: `customerId, last_active_step, max_step, steps_inactive,
  txn_history_count, dormancy_severity`

## Imports (Power BI — Get Data)

1. **Get Data → Text/CSV** for each file in `powerbi/exports/`. Set the folder
   as a parameter (`ExportsFolder`) so it is repointable.
2. In **Power Query**, per table:
   - promote headers, set types (see column lists above — `event_ts`/`batch_ts`
     = Date/Time, `step`/counts = Whole Number, amounts/rates = Decimal).
   - `FlaggedTransactions`: set `isFraud, isFlaggedFraud, false_positive` to
     Whole Number; `detected_at, bridged_at` to Date/Time.
   - `DimCustomer`: `email_verified, is_active` → True/False.
   - `CustomerProducts`: trim `product`.
3. **Close & Apply.**

Refresh: re-run `powerbi/export_datasets.py` and the bridge, then **Refresh** in
Desktop (or scheduled refresh in the Service over a gateway pointing at the same
folder).

## Relationships

All **single-direction**, **one-to-many** (dimension → fact), unless noted.

| From (1) | To (\*) | Active | Cross-filter |
|---|---|---|---|
| `DimDate[step]` | `FlaggedTransactions[step]` | ✔ | single |
| `DimDate[step]` | `TransactionSummary[step]` | ✔ | single |
| `DimDate[step]` | `DailySummary[step]` | ✔ | single |
| `DimTransactionType[transaction_type]` | `FlaggedTransactions[type]` | ✔ | single |
| `DimTransactionType[transaction_type]` | `TransactionSummary[transaction_type]` | ✔ | single |
| `DimTransactionType[transaction_type]` | `DailySummary[type]` | ✔ | single |
| `DimTransactionType[transaction_type]` | `ComplianceSummary[transaction_type]` | ✔ | single |
| `DimCustomer[customerId]` | `FlaggedTransactions[nameOrig]` | ✔ | single |
| `DimCustomer[customerId]` | `DormancyReport[customerId]` | ✔ | single |
| `DimCustomer[customerId]` | `CustomerProducts[customerId]` | ✔ | **both** (product slicer filters customers) |

- `StreamingMetrics` is **standalone** (KPI cards + micro-batch trend use its
  own `batch_ts`). Optionally add an inactive relationship
  `DimDate[date] → StreamingMetrics[batch_date]` if you add a `batch_date`
  column.
- `DimTransactionType` is the source of the **Page 3 transaction-type slicer**;
  because it filters `TransactionSummary`, `DailySummary`, `ComplianceSummary`
  and `FlaggedTransactions`, one slicer drives every type-aware visual.
- Mark **DimDate** as the report's **Date table** (`event_ts`), or add a
  contiguous `Date` column and mark that.

## Calculated columns

| Table | Column | DAX |
|---|---|---|
| `DimCustomer` | `churn_band` | `SWITCH(TRUE(), [churn_probability]>=0.66,"High", [churn_probability]>=0.33,"Medium","Low")` |
| `DimCustomer` | `risk_band` | `SWITCH(TRUE(), [risk_score]>=0.60,"High", [risk_score]>=0.25,"Medium","Low")` (spec 7.3 tiers; use `risk_tier` directly if `risk_scores` loaded) |
| `DimCustomer` | `clv_tier` | `IF(ISBLANK([clv_classification]),"Unscored",[clv_classification])` |
| `DimCustomer` | `income_band` | `SWITCH(TRUE(), [annualIncome]>=250000,"250k+", [annualIncome]>=100000,"100-250k", [annualIncome]>=50000,"50-100k","<50k")` |
| `FlaggedTransactions` | `amount_band` | `SWITCH(TRUE(), [amount]>=1000000,"1M+", [amount]>=500000,"500k-1M", [amount]>=200000,"200-500k","<200k")` |
| `DormancyReport` | `is_severe` | `IF([dormancy_severity]="Severely Dormant",1,0)` |
| `DimDate` | `Date` | `DATE(YEAR([event_ts]),MONTH([event_ts]),DAY([event_ts]))` (mark as Date table) |

## Model settings

- Hide every key column used only for relationships (`step` on facts, `nameOrig`,
  `type` on facts) from the report view.
- Hide raw component columns not used in visuals (`norm_*`, `oldbalance*`).
- Set default summarization to **Don't summarize** on `step`, `age`, all `*_id`
  and rate columns; the report uses explicit measures (see
  [dax-measures.md](dax-measures.md)).
