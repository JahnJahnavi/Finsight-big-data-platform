# FinSight — Phase 6: Customer Risk Scoring (Spark Core batch)

```
/finsight/raw/txn-raw/ (Parquet, full history)
        │
        ├─► per-customer risk factors ─► min-max normalise ─► weighted sum ─► risk_score, risk_tier
        │      └─► /finsight/processed/risk_scores/      (customerId, risk_score, risk_tier, …)
        │
        └─► by (type, step) ─► volume / total amount / fraud count
               ├─► /finsight/processed/daily_summary/    (Parquet)
               └─► /finsight/exports/daily_summary/      (CSV, for the Alteryx Transaction Summary — spec 9.2)
```

**Independent** Spark application — own `SparkSession`, distinct appName
`finsight-batch-risk`. Pure batch: no Kafka, no streaming. **No CLV** in this phase.

## The four risk factors (spec 7.3)

Per customer (`nameOrig`, prefix `C`):

| Factor | Definition |
|---|---|
| 1 · `frequency` | count of the customer's transactions |
| 2 · `avg_transfer_amount` | mean `amount` of the customer's `TRANSFER` transactions (0 if none) — `ASSUMPTIONS.md` I23 |
| 3 · `cashout_proportion` | `CASH_OUT` count ÷ total count |
| 4 · `unique_dest_accounts` | distinct `nameDest` reached |

Each raw factor is **min-max normalised** `(v − min) / (max − min)` across all
customers → `[0, 1]`, then combined:

```
risk_score = w1·norm_frequency + w2·norm_avg_transfer + w3·norm_cashout_prop + w4·norm_unique_dest   (clamped to [0,1])
```

**Weights** — spec 7.3 says "four weighted factors" but gives **no weights**
(`ASSUMPTIONS.md` G6). Default **0.25 each** via `.env` `RISK_W_*`.
**Needs owner sign-off.** The rule maths lives in one place —
[`spark/batch/risk_rules.py`](../spark/batch/risk_rules.py) (`min_max`,
`weighted_risk_score`, `risk_tier`), unit-tested; `risk_scoring.py` re-expresses
it as Spark Column arithmetic.

### Risk tiers (spec 7.3 R1)

| `risk_score` | tier |
|---|---|
| `< 0.25` | Low |
| `0.25 – 0.60` (inclusive) | Medium |
| `> 0.60` | High |

## Output

**`/finsight/processed/risk_scores/`** (Parquet, overwrite): `customerId`,
`risk_score`, `risk_tier` (**required**), plus `frequency`,
`avg_transfer_amount`, `cashout_proportion`, `unique_dest_accounts`, their four
`norm_*` values, and `scored_at`.

**`/finsight/processed/daily_summary/`** (Parquet) and
**`/finsight/exports/daily_summary/`** (CSV) — spec 7.3 R2: `type`, `step`,
`transaction_volume`, `total_amount`, `fraud_count`, grouped by transaction type
**and** step.

## Config (environment-based)

| `.env` | default | |
|---|---|---|
| `RISK_W_FREQUENCY` / `_AVG_TRANSFER` / `_CASHOUT_PROP` / `_UNIQUE_DEST` | `0.25` | factor weights |
| `RISK_TIER_LOW_MAX` / `RISK_TIER_MEDIUM_MAX` | `0.25` / `0.60` | tier cut-offs |
| `HDFS_RAW_TXN` | `/finsight/raw/txn-raw` | input (I25) |
| `HDFS_RISK_SCORES` / `HDFS_DAILY_SUMMARY` / `HDFS_EXPORTS` | `/finsight/processed/…` | outputs |
| `SPARK_APPNAME_RISK` | `finsight-batch-risk` | |

All also overridable per-run: `--input --csv --risk-out --summary-out
--csv-export-dir --namenode --from {hdfs,csv} --show`.

## Run it

```bash
docker compose up -d                          # infra (spark profile)

# production: full HDFS transaction history
spark/batch/run_risk_scoring.sh
spark/batch/run_risk_scoring.sh --show

# small dataset first
spark/batch/run_risk_scoring.sh --from csv --csv /opt/finsight/data/sample/txns.csv

# schedule after market close (spec 7.3): cron / Airflow calling run_risk_scoring.sh
```

## Validation

```bash
pytest tests/unit/test_risk_scoring.py -v     # 12 tests: min-max, weighting, clamp, tiers + boundaries
python scripts/validate_phase6.py             # 9-check end-to-end
```

`validate_phase6.py` generates a 45-transaction CSV with six customers whose
factors are engineered so `C-LOW` sits at every factor's minimum, `C-HIGH` at
every maximum, and `C-MID` in the middle, runs the job, and asserts:

| customer | risk_score | tier |
|---|---|---|
| `C-LOW` | 0.000 | Low |
| `C-MID` | 0.367 | Medium |
| `C-HIGH` | 1.000 | High |

plus: all scores in `[0, 1]`; ordering `LOW < MID < HIGH`; `daily_summary`
grouped by type+step with `transaction_volume`/`total_amount`/`fraud_count`
totalling the input (45 txns, 2 fraud); CSV export present.

## Inspect

```bash
MSYS_NO_PATHCONV=1 docker exec finsight-spark-master /opt/spark/bin/spark-submit --master local[1] - <<'PY'
from pyspark.sql import SparkSession
s = SparkSession.builder.getOrCreate()
s.read.parquet("hdfs://namenode:8020/finsight/processed/risk_scores") \
  .select("customerId","risk_score","risk_tier").orderBy("risk_score",ascending=False).show(30)
s.read.parquet("hdfs://namenode:8020/finsight/processed/daily_summary").show(30)
PY

MSYS_NO_PATHCONV=1 docker exec finsight-namenode hdfs dfs -ls /finsight/processed/risk_scores /finsight/exports/daily_summary
```

## Notes

- Spec 7.3's "rolling 7-day" window == the full history (the dataset is 7 days /
  168 steps) — `ASSUMPTIONS.md` I24.
- `daily_summary` `total_amount` is the sum of `amount` for that `(type, step)`
  (not restricted to any type).
- Job re-runs are idempotent (`mode("overwrite")`).
