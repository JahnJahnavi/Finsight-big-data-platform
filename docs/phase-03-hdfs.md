# FinSight — Phase 3: Kafka `txn-raw` → HDFS (Parquet) via Kafka Connect

```
Kafka  txn-raw   ──►  Kafka Connect  ──►  HDFS 3 Sink  ──►  HDFS Parquet
 (3 partitions)        (finsight-kafka-connect)              /finsight/raw/txn-raw/step=<N>/
                                                            snappy-compressed, one dir per step
```

Ingestion + landing only. **No Spark processing** in this phase.

## Deliverables

| Path | What |
|------|------|
| `docker/kafka-connect/connectors/hdfs-sink-txn-raw.json` | the HDFS 3 Sink connector definition |
| `scripts/register_hdfs_sink.py` | register / update / delete / status the connector via the Connect REST API |
| `scripts/init_hdfs.sh` | create the canonical HDFS directory layout |
| `scripts/validate_phase3.py` | 8-step end-to-end validation |
| `kafka/producer.py` (updated) | now wraps each record in a Kafka Connect JSON **schema envelope** by default (`--raw` for the old bare form) |
| `kafka/transaction_schema.py` (updated) | adds `CONNECT_VALUE_SCHEMA`, `to_envelope()`, `unwrap()` |
| `docker-compose.yml` (updated) | connect heap 512m→900m, `mem_limit` 900m→1400m, mounts the connectors dir |

## Why the producer changed (schema envelope)

The HDFS sink's `ParquetFormat` **and** `FieldPartitioner` both require a typed
Kafka Connect `Struct`. Phase 2 produced schemaless JSON (`Map`), which fails
with *"Value is not Struct type"* / *"Parquet doesn't support schema-less
records"*. There is no Schema Registry in the stack.

The fix with the least moving parts: the producer emits the standard Connect
JSON envelope and the connector uses `JsonConverter` with `schemas.enable=true`:

```json
{"schema":{"type":"struct","name":"finsight.transaction","fields":[
   {"field":"step","type":"int32","optional":false}, ...]},
 "payload":{"step":1,"type":"CASH_OUT","amount":441.46, ... ,"txnId":"TXN000000001"}}
```

`kafka/consumer_test.py` and `validate_record()` transparently `unwrap()` the
envelope, so Phase 2's validation still passes. `python kafka/producer.py
... --raw` restores the bare-JSON output.

## Output layout

```
/finsight/raw/                         <- topics.dir
        +tmp/                          <- connector staging (in-progress, DO NOT read)
        txn-raw/                       <- source topic name (appended by the connector)
                step=1/
                        txn-raw+0+0000000000+0000000042.parquet
                        txn-raw+1+0000000000+0000000039.parquet
                        txn-raw+2+0000000000+0000000041.parquet
                step=2/  ...
                ...
                step=168/
/finsight/logs/txn-raw/{0,1,2}/log     <- write-ahead log (exactly-once recovery)
```

File name = `<topic>+<kafkaPartition>+<startOffset>+<endOffset>.parquet`.
Column schema (13): the 11 CSV columns typed (`step int`, `amount double`,
`isFraud int`, …) plus `txnId string`, `ingest_ts string`. `step` is also the
Hive-style partition column.

### Path note (deviation from the spec's `/finsight/raw/transactions/`)

The Kafka Connect HDFS sink **always** appends the source topic name as the
final path segment (`<topics.dir>/<topic>/…`). It cannot be pointed at an
arbitrary directory: a `RegexRouter` SMT that renames the topic crashes this
connector (`NullPointerException` in `DataWriter.write`, verified on v1.1.26).
So the raw Parquet lands at **`/finsight/raw/txn-raw/`** — this *is* the
transactions landing zone. Phase 4/6 (Spark, Hive external table) target
`/finsight/raw/txn-raw/`. Tracked in `docs/ASSUMPTIONS.md` (I5b).

## Run it

```bash
# 0. infra up (Phase 1) + deps
docker compose up -d
pip install -r kafka/requirements.txt

# 1. HDFS layout + Kafka topics
./scripts/init_hdfs.sh
python kafka/create_topics.py

# 2. register the connector
python scripts/register_hdfs_sink.py
#    for a quick test (commit files every 15s instead of every 5 min):
python scripts/register_hdfs_sink.py --flush-size 2000 --rotate-schedule-ms 15000

# 3. produce some transactions
python kafka/generate_sample_data.py --rows 1000 --out data/sample/transactions_sample.csv
python kafka/producer.py --file data/sample/transactions_sample.csv --limit 1000

# 4. validate end to end
python scripts/validate_phase3.py --records 1000
```

Connector management:

```bash
python scripts/register_hdfs_sink.py --status
python scripts/register_hdfs_sink.py --restart
python scripts/register_hdfs_sink.py --delete
```

## Inspecting HDFS — exact commands

```bash
# connector health
curl -s http://localhost:8083/connectors
curl -s http://localhost:8083/connectors/finsight-hdfs-sink-txn-raw/status | python -m json.tool

# NOTE: on Git Bash / Windows prefix docker exec with  MSYS_NO_PATHCONV=1
# so that "/finsight/..." is not rewritten to "C:/...".

# the step partitions (one directory per hour-step)
MSYS_NO_PATHCONV=1 docker exec finsight-namenode hdfs dfs -ls /finsight/raw/txn-raw

# files inside one partition
MSYS_NO_PATHCONV=1 docker exec finsight-namenode hdfs dfs -ls /finsight/raw/txn-raw/step=1

# whole tree
MSYS_NO_PATHCONV=1 docker exec finsight-namenode hdfs dfs -ls -R /finsight/raw/txn-raw

# count parquet files + total size
MSYS_NO_PATHCONV=1 docker exec finsight-namenode bash -c \
  "hdfs dfs -ls -R /finsight/raw/txn-raw | grep -c '\.parquet'; hdfs dfs -du -s -h /finsight/raw/txn-raw"

# how many step partitions
MSYS_NO_PATHCONV=1 docker exec finsight-namenode bash -c \
  "hdfs dfs -ls /finsight/raw/txn-raw | grep -oE 'step=[0-9]+' | wc -l"

# pull one parquet file to the host and inspect it
MSYS_NO_PATHCONV=1 docker exec finsight-namenode hdfs dfs -get \
  /finsight/raw/txn-raw/step=1/$(MSYS_NO_PATHCONV=1 docker exec finsight-namenode bash -c \
  "hdfs dfs -ls /finsight/raw/txn-raw/step=1 | grep parquet | head -1 | awk '{print \$8}' | xargs basename") /tmp/s1.parquet

# read it back with Spark (proves it is Spark/Hive-ready)
MSYS_NO_PATHCONV=1 docker exec finsight-spark-master /opt/spark/bin/spark-submit --master local[1] - <<'PY'
from pyspark.sql import SparkSession
s = SparkSession.builder.getOrCreate()
df = s.read.parquet("hdfs://namenode:8020/finsight/raw/txn-raw")
df.printSchema(); print("rows:", df.count())
df.groupBy("step").count().orderBy("step").show(5)
PY

# NameNode web UI
#   http://localhost:9870  ->  Utilities  ->  Browse the file system  ->  /finsight/raw/txn-raw
```

## Persistence

| State | Where | Survives |
|-------|-------|----------|
| Connector config | Kafka topic `_connect-configs` | container recreate, `compose down`/`up` |
| Consumed offsets | Kafka topic `_connect-offsets` | ditto |
| Sink write-ahead log | HDFS `/finsight/logs/` (volume `hdfs-data`) | ditto |
| Parquet data | HDFS `/finsight/raw/` (volume `hdfs-data`) | ditto |

Verified: `docker compose restart kafka-connect` → connector + 3 tasks resume
`RUNNING` and continue from the committed offset. Only `./scripts/stop.sh --wipe`
(deletes volumes) loses the data.

## Tuning / known behaviour

- **`flush.size` = 2000, `rotate.schedule.interval.ms` = 300000 (5 min)** by
  default. `FieldPartitioner` on `step` keeps up to 168 partitions open per
  task; keep `flush.size` moderate so the JVM heap holds
  `168 × flush.size × record` buffered rows.
- **Small files at test scale**: a few hundred rows spread over 168 steps × 3
  Kafka partitions ⇒ hundreds of ~3 KB parquet files. At full scale (6.3 M rows,
  ~37 K rows/step) files are ~0.5–1 MB — fine for Spark/Hive. A compaction step
  can be added later if needed.
- **`errors.tolerance=all`** + dead-letter queue `txn-raw-dlq`: a malformed
  record is routed there instead of failing the task. `validate_phase3.py`
  asserts the DLQ is empty.
