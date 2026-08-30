# FinSight — Phase 5: Streaming Churn Detection

```
history (HDFS Parquet / CSV) ─► bootstrap_customer_history.py ─► /finsight/processed/customer_baseline

Kafka txn-raw ─► Structured Streaming (per-customer state) ─► Kafka txn-churn
   (3 parts)     churn_detection.py                        └► HDFS /finsight/processed/churn_alerts/ (Parquet)

checkpoint: HDFS /finsight/checkpoints/churn
```

**Independent** of `fraud_detection.py` — own `SparkSession`, `appName`
(`finsight-streaming-churn`), checkpoint and output. Both jobs run concurrently
on `txn-raw` (spec 7.2 R1). Fraud detection was **not modified**.

## Churn signals — spec 7.2 (frozen)

A customer is flagged when **≥ 2** signals fire within a **24-step sliding
window** (`window` 24 steps, `slide` 12 steps by default, `.env`-configurable):

| Signal | Condition |
|---|---|
| **S1** `LOW_FREQUENCY` | window frequency < 1 txn per 12 steps **AND** the customer's historical average > 3 per 12 steps |
| **S2** `AMOUNT_DROP` | window average amount < 20% of the customer's all-time average amount |
| **S3** `EXCLUSIVE_CASHOUT` | every txn in the window is `CASH_OUT` — no `PAYMENT` or `DEBIT` |
| **S4** `CONSECUTIVE_LOW_BALANCE` | `newbalanceOrig` < 500 for ≥ 2 **consecutive** transactions |

Rule in one place — [`spark/streaming/churn_rule.py`](../spark/streaming/churn_rule.py) —
pure Python (`signal_1..4`, `evaluate_churn`), unit-tested and reused as-is by the
streaming job. Constants overridable via `.env` (`CHURN_*`); spec values are the
default.

> S1 and S4 are mutually exclusive in one window (S1 needs ≤ 1 txn, S4 needs
> ≥ 2). Maximum concurrent signals is 3.

## The historical baseline (S1 / S2)

S1 and S2 compare the window to the customer's **history**, which a streaming job
has no knowledge of at cold start (`ASSUMPTIONS.md` G8). `bootstrap_customer_history.py`
pre-computes it once:

```
per customerId (= nameOrig):
  all_time_txn_count, all_time_avg_amount, first_step, last_step
  hist_freq_per_12 = all_time_txn_count / ((max_step_overall - first_step + 1) / 12)
→ /finsight/processed/customer_baseline/   (Parquet)
```

Source: HDFS Parquet (`/finsight/raw/txn-raw`, Phase 3) by default, or `--from
csv --csv <path>`. The streaming job loads it as a **broadcast** static
dimension and left-joins on `customerId`; customers with no baseline can only
trigger S3 / S4.

## Streaming state model

`event_ts = SIM_EPOCH + (step-1) hours` (`ASSUMPTIONS.md` I11) →
`withWatermark("event_ts", 48h)` →
**`groupBy("customerId").applyInPandasWithState(...)`** (`outputMode="update"`,
`EventTimeTimeout`).

Per customer, the state holds the transactions inside the sliding window
(`steps, types, amounts, balances, max_step`) plus the joined baseline. On each
micro-batch: append new txns, **trim** to `step > max_step - 24`, evaluate the 4
signals via `evaluate_churn`, and if `is_churn` **yield** an alert
`{customerId, window_start_step, window_end_step, window_start, window_end,
signals[], signal_count, window_txn_count, window_avg_amount, detected_at}`.
State is evicted ~2 windows after a customer's last activity.

`update` mode emits an alert as soon as the threshold is crossed (real-time); an
alert for the same customer may be re-emitted as the window accrues more
evidence — downstream dedupes on `(customerId, window_end_step)`.

## Outputs

- **Kafka `txn-churn`** (1 partition): value = JSON with `customerId`,
  `window_start` / `window_end` (timestamps) + `window_start_step` /
  `window_end_step`, and `signals` (the triggering signal names) — spec 7.2 R2.
- **HDFS `/finsight/processed/churn_alerts/`**: Parquet, partitioned by
  `alert_date`, one row per alert with all the fields above.

Requires **pandas + pyarrow** on the Spark workers → `docker/spark/Dockerfile`
(`finsight/spark:3.5.3` = `apache/spark:3.5.3` + pandas 2.0.3 + pyarrow 14.0.2);
`docker-compose.yml` builds it for `spark-master` / `spark-worker`.

## Run it

```bash
docker compose up -d                            # infra (rebuilds finsight/spark)
docker compose build spark-master spark-worker  # first time / after Dockerfile change

# 1. build the baseline (from the HDFS transaction history, or a CSV)
spark/streaming/run_bootstrap.sh
spark/streaming/run_bootstrap.sh --from csv --csv /opt/finsight/data/sample/history.csv

# 2. run the churn stream (independent of the fraud stream; run both together)
spark/streaming/run_churn_detection.sh
spark/streaming/run_churn_detection.sh --once --starting-offsets earliest
spark/streaming/run_churn_detection.sh --reset-checkpoint
```

CLI / `.env`: `--bootstrap-servers --input-topic --output-topic --checkpoint
--alerts-path --baseline-path --namenode --starting-offsets --trigger
--watermark --once --reset-checkpoint --log-level`.
Graceful shutdown: `SIGINT` / `SIGTERM` / `SIGBREAK` → `query.stop()` → exit 0.

## Validation

```bash
pytest tests/unit/test_churn_rule.py -v      # 18 tests: each signal, boundaries, combos
python scripts/validate_phase5.py            # 9-check end-to-end
```

`validate_phase5.py` builds a baseline for six test customers and streams
transactions crafted to trigger each signal / combination:

| Customer | streamed behaviour | expected |
|---|---|---|
| `CHURN-S12` | 1 small PAYMENT, high history | flag `{S1, S2}` |
| `CHURN-S34` | 3 CASH_OUT, balances 400/100/50 | flag `{S3, S4}` |
| `CHURN-S123` | 1 tiny CASH_OUT, high history | flag `{S1, S2, S3}` |
| `NOFLAG-S1` | 1 normal TRANSFER, high history | not flagged (S1 only) |
| `NOFLAG-S3` | 2 CASH_OUT, normal balances | not flagged (S3 only) |
| `NOFLAG-NONE` | 3 normal PAYMENTs | not flagged |

Asserts: exactly `{CHURN-S12, CHURN-S34, CHURN-S123}` on `txn-churn` with the
expected signal sets; `NOFLAG-*` absent; `churn_alerts` Parquet has ≥ 3 rows;
checkpoint present.

## Inspect

```bash
docker exec finsight-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic txn-churn --from-beginning --timeout-ms 5000

MSYS_NO_PATHCONV=1 docker exec finsight-spark-master /opt/spark/bin/spark-submit --master local[1] - <<'PY'
from pyspark.sql import SparkSession
s = SparkSession.builder.getOrCreate()
s.read.parquet("hdfs://namenode:8020/finsight/processed/churn_alerts").show(50, False)
PY

MSYS_NO_PATHCONV=1 docker exec finsight-namenode hdfs dfs -ls -R /finsight/checkpoints/churn
```

## Notes / assumptions (see `docs/ASSUMPTIONS.md`)

- **I17** window/slide: spec says "24-step sliding window", slide unspecified →
  default 12 steps.
- **I18** S3 "exclusively CASH_OUT" → *every* txn in the window is CASH_OUT
  (and no PAYMENT/DEBIT).
- **I19** S4 "reaches zero or below 500" → `newbalanceOrig < 500` (zero is
  included); "consecutive" evaluated in `(step, kafka_offset)` order.
- **I20** `hist_freq_per_12` denominator = from the customer's first txn to the
  end of the observed history.
- `txn-churn` / `churn_alerts` may contain repeated alerts for one customer as a
  window fills; dedupe on `(customerId, window_end_step)`.
