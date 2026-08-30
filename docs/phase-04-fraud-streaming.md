# FinSight — Phase 4: Streaming Fraud Detection

```
Kafka txn-raw ──► Spark Structured Streaming ──► Kafka txn-flagged
   (3 parts)     fraud_detection.py (foreachBatch)  (1 part)
                          │
                          └──► HDFS /finsight/processed/streaming_metrics/
                               one JSON row per micro-batch: fraud rate

checkpoint: HDFS /finsight/checkpoints/fraud   (exactly-once recovery, spec 7.1 R1)
```

Real-time fraud only. **No churn** in this phase.

## Fraud rule — spec 7.1 (frozen)

A transaction is written to `txn-flagged` **only when all three are true**:

| # | Condition |
|---|-----------|
| 1 | `type` is `TRANSFER` or `CASH_OUT` |
| 2 | `amount` **> 200000** (strict) |
| 3 | `newbalanceDest` **== 0** |

The rule lives in **one place** — [`spark/streaming/fraud_rule.py`](../spark/streaming/fraud_rule.py) — as
`is_fraud(txn)` (pure Python, unit-tested) and `fraud_condition_sql()` (the Spark
SQL predicate the job runs). Constants are overridable via `.env`
(`FRAUD_TYPES`, `FRAUD_AMOUNT_THRESHOLD`) but the spec values are the default and
must not change.

## Files

| Path | |
|------|--|
| `spark/streaming/fraud_detection.py` | the job |
| `spark/streaming/fraud_rule.py` | the rule (Python + SQL forms) |
| `spark/streaming/run_fraud_detection.sh` | `spark-submit` wrapper (runs inside the container) |
| `spark/common/config.py` | env-driven settings (Kafka, paths, rule, stream) |
| `spark/common/schemas.py` | explicit `TXN_SCHEMA` + envelope parser |
| `spark/common/spark_session.py` | `SparkSession` builder |
| `tests/unit/test_fraud_rule.py` | 10 unit tests incl. the 5 spec scenarios |
| `scripts/validate_phase4.py` | 9-check end-to-end validation |
| `docker-compose.yml` | +`spark-ivy` volume (package cache), spark-master `mem_limit` 900m→1200m |

## Explicit schema

`txn-raw` values are the producer's Kafka Connect JSON **schema envelope**
(`{"schema":…,"payload":…}`, Phase 3) or the bare payload (`--raw`).
`spark/common/schemas.py` defines the strict `StructType` for the 13 payload
fields (`step int`, `amount double`, `isFraud int`, …) — **no schema inference**
(`spark.sql.streaming.schemaInference=false`). `parse_txn_value()` extracts the
payload from either form; rows that fail to parse become a struct of nulls and
are filtered out (logged per batch).

## Micro-batch logic (`foreachBatch`)

One streaming query, one checkpoint. Per micro-batch:

1. `total` = row count
2. drop unparseable rows (`type IS NULL`)
3. `flagged` = rows matching `fraud_condition_sql()`
4. write flagged rows → Kafka `txn-flagged` (key = `nameOrig`, value = the 13
   fields + `fraud_rule` + `detected_at`)
5. write one metrics row → HDFS: `{batch_id, batch_ts, total_count,
   flagged_count, fraud_rate_pct, app_name, fraud_rule}` — **fraud_rate_pct =
   flagged_count / total_count × 100** (spec 7.1 R2)
6. log `batch N | total=… flagged=… fraud_rate=…%`

A batch that throws is logged with context and re-raised — the stream stops and a
restart resumes from the checkpoint (no silent data loss). Writing to Kafka from
`foreachBatch` is at-least-once on retry; acceptable for the alert topic.

## Run it

```bash
docker compose up -d                       # infra (incl. spark profile)
# first run downloads the Kafka package into the spark-ivy volume (~1 min)

# continuous (Ctrl-C = graceful stop; resumes from checkpoint)
spark/streaming/run_fraud_detection.sh

# process what's on the topic now, then stop  (used by the test)
spark/streaming/run_fraud_detection.sh --once --starting-offsets earliest

# clean run
spark/streaming/run_fraud_detection.sh --reset-checkpoint

# local mode instead of the standalone cluster
SPARK_MASTER=local[2] spark/streaming/run_fraud_detection.sh --once
```

CLI flags (all have `.env` defaults): `--bootstrap-servers --input-topic
--output-topic --checkpoint --metrics-path --namenode --starting-offsets
--max-offsets-per-trigger --trigger --once --reset-checkpoint --log-level`.

Graceful shutdown: `SIGINT` / `SIGTERM` / `SIGBREAK` → `query.stop()` →
`awaitTermination()` returns → `spark.stop()`; exit 0.

## Validation

```bash
python scripts/validate_phase4.py
```

Publishes the five spec test transactions to `txn-raw`, runs the job `--once`,
and asserts:

| Test case | txn | flagged? |
|-----------|-----|----------|
| 1 | TRANSFER, amount 250 000, newbalanceDest 0 | **yes** |
| 2 | CASH_OUT, amount 500 000, newbalanceDest 0 | **yes** |
| 3 | TRANSFER, amount 150 000, newbalanceDest 0 | no (amount) |
| 4 | PAYMENT, amount 300 000, newbalanceDest 0 | no (type) |
| 5 | CASH_OUT, amount 400 000, newbalanceDest 1 234.56 | no (dest balance) |

Then: only `{TEST-1, TEST-2}` appear on `txn-flagged`; the HDFS metrics row has
`total_count=5, flagged_count=2, fraud_rate_pct=40.0`; the checkpoint exists at
`/finsight/checkpoints/fraud`.

Unit tests (host, no Spark — PySpark 3.5 needs Python ≤3.11, host is 3.13):

```bash
pytest tests/unit/test_fraud_rule.py -v
```

## Inspect

```bash
# the streaming query in the Spark UI:  http://localhost:8080  (or :4040 while running)

# flagged transactions
docker exec finsight-kafka kafka-console-consumer --bootstrap-server localhost:9092 \
  --topic txn-flagged --from-beginning --timeout-ms 5000

# per-batch fraud-rate metrics
MSYS_NO_PATHCONV=1 docker exec finsight-namenode hdfs dfs -cat \
  '/finsight/processed/streaming_metrics/*.json'

# checkpoint
MSYS_NO_PATHCONV=1 docker exec finsight-namenode hdfs dfs -ls -R /finsight/checkpoints/fraud
```

## Notes / assumptions

- **`newbalanceDest == 0`** is literal float equality on `0.0` — the connector
  writes the value as an exact `0.0` for account-emptying transactions, and the
  spec says `== 0`. (`ASSUMPTIONS.md` I14.)
- **Metrics format**: JSON append, one file per micro-batch (spec says "write
  this summary … for monitoring" without a format). Readable with
  `hdfs dfs -cat` and by Spark/Hive.
- **`txn-flagged` format**: bare JSON (the 13 fields + `fraud_rule` +
  `detected_at`). Power BI's Fraud Alert Board (Phase 9) reads this topic via a
  bridge.
- Runs in the `finsight-spark-master` container (Python 3.8); the host's Python
  3.13 cannot run PySpark 3.5.
