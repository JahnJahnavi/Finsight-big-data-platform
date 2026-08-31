# Designer build — Workflow 2: Transaction Summary

Rebuild tool-for-tool in Alteryx Designer. Analytical spec:
[`docs/alteryx/workflow-2-transaction-summary.md`](../../docs/alteryx/workflow-2-transaction-summary.md).

Save as: `alteryx/workflows/02_transaction_summary.yxmd` — `*.yxmd` is
**git-ignored** (Phase 12 does not commit unverifiable Designer artifacts).

## Canvas

```
[1 Input: finsight.transactions]──[2 Filter: step 1..168]──[3 Summarize]──[4 Formula]──[5 Select]──[6 Sort]──[7 Output .csv]
```

## Tools

### 1 — Input Data · `finsight.transactions`
- Connection: `FinSight Hive` DSN (see `01_customer_risk_blend.md`).
- Query: `SELECT type, step, amount, isFraud FROM transactions`
- **Source note (ASSUMPTIONS I47):** the brief says `txn_summary_mart`, but that
  table is (customerId, step)-grain with a comma-joined `txn_types` and cannot
  split volume/amount per type. Use the base fact `transactions`.
- If the DSN returns 0 rows for a `GROUP BY` (Tez not recursing `step=<N>/`
  sub-dirs in this dev stack): add `hive.mapred.supports.subdirectories=true;`
  and `mapreduce.input.fileinputformat.input.dir.recursive=true;` to the DSN's
  **Pre-SQL**, or point the Input at the Spark-exported CSV
  (`alteryx/fallback/transaction_summary.py` writes it).

### 2 — Filter
- Basic filter: `[step] >= 1 AND [step] <= 168`
- Keep the **True** output only.

### 3 — Summarize
| Field | Action | Output |
|---|---|---|
| `type` | Group By | `transaction_type` |
| `step` | Group By | `step` |
| `amount` | Sum | `total_volume` |
| `amount` | Count | `transaction_count` |
| `isFraud` | Sum | `fraud_count` |

### 4 — Formula
| Output field | Type | Expression |
|---|---|---|
| `avg_transaction_amount` | Double | `IF [transaction_count] = 0 THEN 0 ELSE [total_volume] / [transaction_count] ENDIF` |
| `total_volume` | Double | `Round([total_volume], 2)` |
| `avg_transaction_amount` | Double | `Round([avg_transaction_amount], 2)` |

### 5 — Select
- Column order:
  `transaction_type, step, total_volume, transaction_count, avg_transaction_amount, fraud_count`

### 6 — Sort
- `step` Ascending, then `transaction_type` Ascending.

### 7 — Output Data
- File type **CSV**, path `alteryx/outputs/transaction_summary.csv`, comma
  delimiter, **first row contains field names**.

## Run & check

- Ctrl+R. Record count = `(type, step)` groups present — synthetic: **720**.
- `Sum([transaction_count])` == `SELECT COUNT(*) FROM finsight.transactions WHERE step BETWEEN 1 AND 168` (synthetic **4000**).
- `Sum([fraud_count])` == `SELECT SUM(isFraud) ...` (synthetic **7**).
- Cross-check `python alteryx/fallback/transaction_summary.py`.
