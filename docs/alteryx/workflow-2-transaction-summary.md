# Alteryx Workflow 2 — Transaction Summary

Aggregate the transaction history to `transaction type × step` with volume,
average amount and fraud count, for the Power BI transaction-trend page
(spec section 12).

- **Designer build:** [`alteryx/workflows/02_transaction_summary.md`](../../alteryx/workflows/02_transaction_summary.md) (tool-by-tool)
- **Headless equivalent:** [`alteryx/fallback/transaction_summary.py`](../../alteryx/fallback/transaction_summary.py)
- **Output:** `alteryx/outputs/transaction_summary.csv`

```
 Input: Hive transactions ─► Filter (step 1..168) ─► Summarize ─► Formula ─► Select ─► Output .csv
                                                    (group by      avg_txn    column
                                                     type, step)   amount     order
```

## 1. Input — source note (ASSUMPTIONS I47)

The workflow brief names **`finsight.txn_summary_mart`** as the input. That
table is pre-aggregated to **(customerId, step)** and stores the transaction
types as a comma-joined string `txn_types` (`PAYMENT,CASH_OUT`), with a single
`total_amount` / `txn_count` / `fraud_count` covering *all* of a customer-step's
types. It therefore **cannot** produce a correct `volume` / `average amount` /
`fraud count` split **per transaction type** — exploding `txn_types` would
attribute the whole customer-step amount to every type it contains.

**Adopted:** the Alteryx **Input Data** tool reads **`finsight.transactions`**
(the base fact, same Hive DSN), which has one row per transaction with a clean
`type` and `amount`. `txn_summary_mart` stays the input for *customer-grain*
questions; this type×step rollup needs the fact table.

| Alteryx Input Data | Connection | Query |
|---|---|---|
| Hive `finsight.transactions` | ODBC DSN `FinSight Hive` → HiveServer2 `localhost:10000`, DB `finsight` | `SELECT type, step, amount, isFraud FROM transactions` |

> In this dev stack, HiveServer2 (Tez local mode) does not recurse the
> `step=<N>/` Parquet sub-directories for aggregation jobs, so the fallback runs
> the same aggregation through **Spark** (`spark.table("finsight.transactions")`,
> which does recurse). Alteryx connecting via ODBC should push the aggregation
> down — if it returns 0 rows, point the Input at the Spark-produced
> `/finsight/exports/` CSV instead, or `SET
> hive.mapred.supports.subdirectories=true` on the DSN.

## 2. Field mappings

| Source field | Workflow field | Type | Notes |
|---|---|---|---|
| `type` | `transaction_type` | String | PAYMENT / TRANSFER / CASH_IN / CASH_OUT / DEBIT |
| `step` | `step` | Int32 | 1-hour simulation tick |
| `amount` | *(aggregated)* → `total_volume` | Double | `Sum([amount])` |
| `amount` | *(aggregated)* → `transaction_count` | Int64 | `Count()` |
| `amount` | *(derived)* → `avg_transaction_amount` | Double | `total_volume / transaction_count` |
| `isFraud` | *(aggregated)* → `fraud_count` | Int64 | `Sum([isFraud])` (values are 0/1) |

## 3. Filter tool

```
[step] >= 1 AND [step] <= 168
```

Inclusive both ends (spec: "step 1 through 168"). The bundled data is exactly
168 steps, so this passes everything; keep it explicit for a larger feed.

## 4. Summarize tool

| Field | Action | Output name |
|---|---|---|
| `transaction_type` | **Group By** | `transaction_type` |
| `step` | **Group By** | `step` |
| `amount` | **Sum** | `total_volume` |
| `amount` | **Count** | `transaction_count` |
| `isFraud` | **Sum** | `fraud_count` |

## 5. Formula tool

```
avg_transaction_amount = IF [transaction_count] = 0 THEN 0
                         ELSE [total_volume] / [transaction_count] ENDIF
total_volume           = Round([total_volume], 2)
avg_transaction_amount = Round([avg_transaction_amount], 2)
```

(Computing the average *after* the Summarize — `Sum/Count` — is exact; an
`Average` action inside Summarize would also work.)

## 6. Select tool — output column order

`transaction_type, step, total_volume, transaction_count,
avg_transaction_amount, fraud_count`

Sort by `step` then `transaction_type`.

## 7. Output tool

- **Output Data** → `.csv`, comma delimiter, header row, to
  `alteryx/outputs/transaction_summary.csv`.
- "Suitable for Power BI": flat CSV, one row per `transaction_type × step`.

## 8. Execution steps

1. `scripts/start.sh hive spark`; confirm `finsight-hiveserver2` healthy.
2. Populate upstream: Phases 2–3 (ingest) → Phase 8 (`hive/run_warehouse.sh`)
   so `finsight.transactions` resolves.
3. Open the workflow in Designer, set the Hive DSN, **Run**.
4. Confirm `alteryx/outputs/transaction_summary.csv` was written.

Headless: `python alteryx/fallback/transaction_summary.py`

## 9. Expected output columns

| Column | Type | Example |
|---|---|---|
| `transaction_type` | String | `CASH_OUT` |
| `step` | Int32 | `1` |
| `total_volume` | Double | `189038.00` |
| `transaction_count` | Int64 | `7` |
| `avg_transaction_amount` | Double | `27005.43` |
| `fraud_count` | Int64 | `0` |

Row count = number of `(type, step)` combinations that occur (≤ 5 × 168 = 840;
synthetic data: **720**). `Sum(transaction_count)` == total transactions in
`finsight.transactions` for steps 1–168 (synthetic: **4000**).

## 10. Validation

| Check | How |
|---|---|
| Steps in range | `MIN(step) >= 1 AND MAX(step) <= 168` |
| All types present | distinct `transaction_type` ⊆ {PAYMENT, TRANSFER, CASH_IN, CASH_OUT, DEBIT} |
| Count reconciliation | `SUM(transaction_count)` == `SELECT COUNT(*) FROM finsight.transactions WHERE step BETWEEN 1 AND 168` |
| Volume reconciliation | `SUM(total_volume)` == `SELECT ROUND(SUM(amount),2) FROM finsight.transactions WHERE step BETWEEN 1 AND 168` |
| Fraud reconciliation | `SUM(fraud_count)` == `SELECT SUM(isFraud) FROM finsight.transactions WHERE step BETWEEN 1 AND 168` (synthetic: **7**) |
| `avg` sanity | `avg_transaction_amount` == `total_volume / transaction_count` per row |
| Diff vs fallback | `python alteryx/fallback/transaction_summary.py`, compare `alteryx/outputs/transaction_summary.csv` |
