# Page 3 — Risk & Compliance Report

Governance view: fraud-rate trend, volume mix, worst accounts, per-type
compliance classification, and dormancy — all filterable by transaction type.

**Primary sources:** `TransactionSummary`, `ComplianceSummary`, `DormancyReport`,
`FlaggedTransactions` (+ `DimCustomer`), `DimDate`, `DimTransactionType`.

## Layout (16 : 9)

```
┌──────────────────────────────────────────────────────────────────────┐
│  FinSight — Risk & Compliance     [SLICER: transaction type ▸ ▸ ▸]    │
├───────────────────────┬──────────────────────────┬───────────────────┤
│ Dormant │ Severely    │  Weekly fraud rate trend  │ Transaction       │
│ Accts   │ Dormant     │  (line: week_start ×      │ volume by type    │
│ [card]  │ [card]      │   Summary Fraud Rate %)   │ (bar)             │
├─────────┴─────────────┼──────────────────────────┴───────────────────┤
│  Top 20 flagged accounts (table)   │  Compliance summary (table)      │
│  customerId · flagged count ·      │  transaction_type · count ·      │
│  flagged value  (Top N = 20)       │  volume · fraud_count ·          │
│                                    │  fraud_rate_pct · risk_class     │
├────────────────────────────────────┴──────────────────────────────────┤
│  Dormant accounts (table)  customerId · steps_inactive · txn_history  │
│  · dormancy_severity          (grouped / conditional-formatted)       │
└──────────────────────────────────────────────────────────────────────┘
```

## Visuals

| # | Visual | Type | Fields / measures |
|---|---|---|---|
| 1 | **Dormant accounts** | Card | `[Dormant Accounts]` |
| 2 | **Severely dormant accounts** | Card | `[Severely Dormant Accounts]` |
| 3 | **Weekly fraud rate trend** | Line | Axis `DimDate[week_start]` (or `iso_week`); Value `[Summary Fraud Rate %]`; markers on |
| 4 | **Transaction volume by type** | Clustered bar | Axis `DimTransactionType[transaction_type]`; Value `[Total Transaction Volume]`; sort desc; data labels (currency) |
| 5 | **Top 20 flagged accounts** | Table | `DimCustomer[customerId]`, `[Account Flagged Count]`, `[Account Flagged Value]`, `DimCustomer[segment]`, `DimCustomer[risk_tier]`; visual filter **Top N = 20 by `[Account Flagged Value]`**; sort desc |
| 6 | **Compliance summary** | Table | `ComplianceSummary[transaction_type, transaction_count, transaction_volume, fraud_count, fraud_rate_pct, risk_classification]`; conditional-format `risk_classification` (High=red, Medium=amber, Low=green) and a data-bar on `fraud_rate_pct` |
| 7 | **Dormant accounts** | Table (or Matrix by `dormancy_severity`) | `DormancyReport[customerId, last_active_step, steps_inactive, txn_history_count, dormancy_severity]`; sort `steps_inactive` desc; conditional-format `dormancy_severity` |

## Slicers

| Slicer | Field | Style | Scope |
|---|---|---|---|
| **Transaction type filtering** | `DimTransactionType[transaction_type]` | horizontal tiles, multi-select, "Select all" | filters visuals 3, 4, 6 and (via `FlaggedTransactions[type]`) visual 5. **Sync** with Page 1's type slicer (View → Sync slicers). |
| Week | `DimDate[week_start]` | dropdown | optional, visual 3 |
| Severity | `DormancyReport[dormancy_severity]` | tile | visual 7 only (set *Edit interactions*) |

## Filters

- **Page-level:** none. (Dormancy cards/tables are unaffected by the type
  slicer — set visual 1, 2, 7 *Edit interactions* with the type slicer to
  **None**, since `DormancyReport` has no `type`.)
- **Visual 3 / 4 / 6:** honour the type slicer through `DimTransactionType`.
- **Visual 5 Top-20:** the `ALLEXCEPT` measure keeps the per-account grain; the
  Top N filter does the ranking. If the type slicer is active, the count/value
  reflect only flagged txns of the selected type(s).

## Interactions

- Type slicer → visuals 3, 4, 5, 6.
- Bar (4) type → cross-filters trend (3) and compliance table (6).
- Compliance row (6) → cross-filters trend (3) to that type.
- Dormancy severity matrix/slicer → visual 7 only.

## Validation

| Check | Expected |
|---|---|
| `[Total Transaction Volume]` (no slicer) | = `SUM(transaction_summary.total_volume)` (synthetic: **53,684,404.31**) |
| `[Transaction Count]` (no slicer) | = 4000 (synthetic) = `COUNT(*)` of `finsight.transactions` steps 1–168 |
| Bar (4) sum | = `[Total Transaction Volume]` |
| `[Summary Fraud Rate %]` | `DIVIDE(SUM(fraud_count), SUM(transaction_count))*100`, per week on the axis |
| Compliance table `fraud_rate_pct` | matches `sql/run_spark_sql.sh --mode compliance` output |
| `risk_classification` | ∈ {High, Medium, Low}; High = fraud_rate_pct ≥ 5, Medium ≥ 1 (ASSUMPTIONS I34) |
| `[Dormant Accounts]` + `[Severely Dormant Accounts]` | = `[Total Dormant Accounts]` = rows in `dormancy_report.csv` |
| Dormancy severity | Dormant = `72 < steps_inactive ≤ 120`; Severely Dormant = `steps_inactive > 120` (ASSUMPTIONS I36) |
| Top-20 table | exactly 20 rows; `[Account Flagged Value]` descending; every account has ≥ 1 flagged txn |
| Type slicer | selecting one type filters visuals 3/4/5/6 and leaves 1/2/7 unchanged |
