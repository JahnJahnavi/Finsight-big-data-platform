# Page 1 — Fraud Alert Board

Real-time view for fraud ops: how much is being flagged, how fast, how much
value, and how noisy the rule is.

**Primary sources:** `FlaggedTransactions` (Kafka bridge), `StreamingMetrics`
(HDFS). **Dimensions:** `DimDate`, `DimTransactionType`, `DimCustomer`.

## Layout (16 : 9, 1280 × 720)

```
┌──────────────────────────────────────────────────────────────────────┐
│  FinSight — Fraud Alert Board          [slicer: date range][type]     │  header band
├───────────┬───────────┬───────────┬───────────┬──────────────────────┤
│ Total     │ Fraud     │ Flagged   │ False     │                      │
│ Flagged   │ Rate %    │ Value     │ Positive  │   (KPI cards row)     │
│ [card]    │ [card]    │ [card]    │ Rate %    │                      │
├───────────┴───────────┴───────────┴───────────┴──────────────────────┤
│  Daily fraud trend (line)                     │ Fraud count by       │
│  x: DimDate[date]  y: Flagged Transactions    │ transaction type     │
│  + line: Fraud Rate %                         │ (bar, horizontal)    │
├──────────────────────────────────────────────┴──────────────────────┤
│  Recent flagged transactions (table, newest first, top 100)          │
└──────────────────────────────────────────────────────────────────────┘
```

## Visuals

| # | Visual | Type | Fields / measures |
|---|---|---|---|
| 1 | **Total flagged transactions** | Card | `[Flagged Transactions]` |
| 2 | **Fraud rate** | Card | `[Fraud Rate %]` (fallback measure if `StreamingMetrics` absent) |
| 3 | **Total flagged transaction value** | Card | `[Flagged Transaction Value]` (currency) |
| 4 | **False positive rate** | Card | `[False Positive Rate %]` — subtitle "flagged but isFraud = 0" |
| 5 | **Daily fraud trend** | Line chart | Axis `DimDate[date]`; Values `[Flagged Transactions]`; secondary line `[Fraud Rate %]` (or `[Summary Fraud Rate %]`) |
| 6 | **Fraud count by transaction type** | Clustered bar | Axis `DimTransactionType[transaction_type]`; Value `[Flagged Transactions]`; data labels on |
| 7 | **Recent flagged transactions** | Table | `detected_at`, `txnId`, `type`, `amount`, `nameOrig`, `nameDest`, `newbalanceDest`, `isFraud`, `false_positive`; sort `detected_at` desc; visual-level Top N = 100 by `detected_at` |

## Slicers

| Slicer | Field | Style | Notes |
|---|---|---|---|
| Date range | `DimDate[date]` | Between (slider) | affects visuals 5–7 (and 1–4 if you remove metrics-based cards) |
| Transaction type | `DimTransactionType[transaction_type]` | Tile / dropdown | syncs to Page 3 (Sync Slicers) |

## Filters

- **Page-level:** none (the page *is* the flagged set — `FlaggedTransactions`
  only holds rule-matched rows).
- **Visual 4 (FP rate):** none — measure already restricts to `false_positive=1`.
- **Cards 1–4:** if you want them to honour the date slicer, base
  `Fraud Rate %` on `TransactionSummary` (has `step` → `DimDate`); the
  `StreamingMetrics` version is *stream-to-date* and ignores the date slicer by
  design (live totals).

## Interactions

- Bar (6) → cross-filters trend (5) and table (7) by type.
- Trend point (5) → cross-filters table (7) to that day.
- Cards do not cross-filter (set *Edit interactions* → None from cards).

## Validation

| Check | Expected |
|---|---|
| `[Flagged Transactions]` == rows in `flagged_transactions.csv` | equal |
| `[Flagged Transactions]` == `SUM(StreamingMetrics[flagged_count])` | equal (± in-flight batch) |
| `[Fraud Rate %]` == `SUM(flagged_count)/SUM(total_count)*100` | equal |
| Bar (6) totals == card 1 | equal |
| `[False Positive Rate %]` | 0–100; = `COUNT(false_positive=1) / COUNT(flagged)` |
| Trend (5) x-axis | within the loaded step range (synthetic: 2023-01-01 … 2023-01-07) |
| Every flagged row | `type ∈ {TRANSFER, CASH_OUT}` and `amount > 200000` and `newbalanceDest = 0` (Phase 4 rule) |

Cross-check with Spark:
`spark/streaming/run_fraud_detection.sh --once --starting-offsets earliest`
then compare the metrics rows.
