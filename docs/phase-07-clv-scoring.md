# FinSight — Phase 7: Customer Lifetime Value Scoring (Spark Core batch)

```
/finsight/raw/txn-raw/ (Parquet, full history)
        │
        └─► per customer:  volume · frequency · diversity · recency
                             └─► CLV = 0.30·vol + 0.25·freq + 0.25·div + 0.20·rec   (∈ [0,1])
                             └─► /finsight/processed/clv_scores/
                                  (customerId, clv_score, clv_classification, …)
```

**Independent** Spark application — own `SparkSession`, distinct appName
`finsight-batch-clv`, output path separate from `risk_scores` (spec 7.4 R2).
`risk_scoring.py` is **not modified**. **No Hive** in this phase (spec 7.4 R1's
`finsight.customer_clv` external table is deferred).

## The four components (spec 7.4 — weights are given, do not change)

| Component | Weight | Score |
|---|---:|---|
| **Transaction Volume** | 30% | `sum(amount)` for the customer ÷ the highest-spending account |
| **Transaction Frequency** | 25% | the customer's transaction count ÷ the most-active account |
| **Product Diversity** | 25% | distinct transaction types used ÷ 5 (`PAYMENT, TRANSFER, CASH_IN, DEBIT, CASH_OUT`) |
| **Recency** | 20% | `clamp(1 − steps_since_last_txn / 48, 0, 1)` — 0 once inactivity ≥ 48 steps; `steps_since_last_txn = max_step_overall − customer_last_step` (`ASSUMPTIONS.md` G9 / I28) |

```
clv_score = 0.30·volume + 0.25·frequency + 0.25·diversity + 0.20·recency   (clamped to [0, 1])
```

Each component ∈ [0, 1] and the weights sum to 1, so the score is naturally in
range. The maths lives in one place —
[`spark/batch/clv_rules.py`](../spark/batch/clv_rules.py) (`volume_score`,
`frequency_score`, `diversity_score`, `recency_score`, `clv_score`,
`clv_classification`), unit-tested; `clv_scoring.py` re-expresses it as Spark
Column arithmetic.

### Classification (spec 7.4)

| `clv_score` | class |
|---|---|
| `> 0.70` | High Value |
| `0.40 – 0.70` (inclusive) | Growth Potential |
| `< 0.40` | At Risk |

## Output

**`/finsight/processed/clv_scores/`** (Parquet, overwrite): `customerId`,
`clv_score`, `clv_classification` (**required**), plus `total_amount`,
`txn_count`, `distinct_txn_types`, `last_step`, `steps_since_last_txn`, the four
`*_score` components, and `scored_at`.

## Config (environment-based)

| `.env` | default |
|---|---|
| `CLV_W_VOLUME` / `_FREQUENCY` / `_DIVERSITY` / `_RECENCY` | `0.30` / `0.25` / `0.25` / `0.20` |
| `CLV_RECENCY_ZERO_AFTER_STEPS` | `48` |
| `CLV_N_TXN_TYPES` | `5` |
| `CLV_TIER_HIGH_MIN` / `CLV_TIER_GROWTH_MIN` | `0.70` / `0.40` |
| `HDFS_CLV_SCORES` | `/finsight/processed/clv_scores` |
| `SPARK_APPNAME_CLV` | `finsight-batch-clv` |

Per-run CLI: `--from {hdfs,csv} --input --csv --out --namenode --show`.

## Run it

```bash
docker compose up -d

# production: full HDFS transaction history
spark/batch/run_clv_scoring.sh --show

# small dataset first
spark/batch/run_clv_scoring.sh --from csv --csv /opt/finsight/data/sample/txns.csv

# runs independently of, and alongside, risk_scoring.py:
spark/batch/run_risk_scoring.sh &
spark/batch/run_clv_scoring.sh
```

## Validation

```bash
pytest tests/unit/test_clv_scoring.py -v     # 18 tests: 4 components, weighting, clamp, recency cut-off, 3 classes + boundaries
python scripts/validate_phase7.py            # 9-check end-to-end
```

`validate_phase7.py` generates a 51-transaction CSV with customers engineered so
`C-HIGH` maxes every component, `C-GROWTH` sits mid-range, and `C-ATRISK` is at
the bottom and inactive:

| customer | clv_score | class |
|---|---|---|
| `C-HIGH` | 1.000 | High Value |
| `C-GROWTH` | 0.512 | Growth Potential |
| `C-ATRISK` | 0.070 | At Risk |

plus: all scores in `[0, 1]`; ordering `At Risk < Growth < High Value`; the
weighting is exactly 30/25/25/20; recency = 0 for `C-ATRISK` (158 steps since
last txn ≥ 48); diversity = `distinct_types / 5`.

## Inspect

```bash
MSYS_NO_PATHCONV=1 docker exec finsight-spark-master /opt/spark/bin/spark-submit --master local[1] - <<'PY'
from pyspark.sql import SparkSession
s = SparkSession.builder.getOrCreate()
s.read.parquet("hdfs://namenode:8020/finsight/processed/clv_scores") \
  .select("customerId","clv_score","clv_classification",
          "volume_score","frequency_score","diversity_score","recency_score") \
  .orderBy("clv_score", ascending=False).show(40, truncate=False)
PY
```

## Notes

- `avg_transfer_amount` (risk scoring) vs `total_amount` (CLV volume): CLV uses
  the customer's **cumulative** amount across all types, not just TRANSFER.
- Job re-runs are idempotent (`mode("overwrite")`).
- Spec 7.4 R1 (register as Hive external table `finsight.customer_clv`) is
  intentionally out of scope here — it happens in the Hive phase.
