#!/usr/bin/env python3
"""
FinSight - Phase 4: Spark Structured Streaming real-time fraud detection.

    Kafka txn-raw  ──►  Structured Streaming  ──►  Kafka txn-flagged
                              │
                              └──►  HDFS /finsight/processed/streaming_metrics/
                                    (fraud rate per micro-batch)

Fraud rule (spec 7.1 - DO NOT CHANGE, see spark/streaming/fraud_rule.py):
    flag when ALL are true:
      1. type is TRANSFER or CASH_OUT
      2. amount > 200000
      3. newbalanceDest == 0

Checkpoint: /finsight/checkpoints/fraud  (exactly-once recovery, spec 7.1 R1)

Run (inside the Spark container - needs the Kafka package):
    spark/streaming/run_fraud_detection.sh
    spark/streaming/run_fraud_detection.sh --once --starting-offsets earliest
    spark/streaming/run_fraud_detection.sh --reset-checkpoint

Exit codes: 0 ok / stopped cleanly, 1 stream failure, 2 startup/config error.
"""
from __future__ import annotations

import argparse
import os
import signal
import sys
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.utils import AnalysisException, StreamingQueryException

# --- make spark/common and spark/streaming importable under spark-submit ---
_HERE = os.path.dirname(os.path.abspath(__file__))
_SPARK_DIR = os.path.dirname(_HERE)
for _p in (_SPARK_DIR, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from common.config import KAFKA, PATHS, STREAM  # noqa: E402
from common.schemas import TXN_COLUMNS, parse_txn_value  # noqa: E402
from common.spark_session import build_spark, set_log_level  # noqa: E402
from fraud_rule import (  # noqa: E402
    FRAUD_RULE_ID,
    fraud_condition_sql,
)

log = None  # set in main()

_METRICS_SCHEMA = StructType(
    [
        StructField("batch_id", LongType(), False),
        StructField("batch_ts", StringType(), False),
        StructField("total_count", LongType(), False),
        StructField("flagged_count", LongType(), False),
        StructField("fraud_rate_pct", StringType(), False),
        StructField("app_name", StringType(), False),
        StructField("fraud_rule", StringType(), False),
    ]
)


# --------------------------------------------------------------------------- #
# Micro-batch processing
# --------------------------------------------------------------------------- #
def make_batch_processor(spark: SparkSession, args: argparse.Namespace):
    metrics_path = f"{args.namenode}{args.metrics_path}"
    fraud_sql = fraud_condition_sql()
    log.info("fraud predicate: %s", fraud_sql)

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        batch_df = batch_df.persist()
        try:
            total = batch_df.count()

            # rows where the JSON parsed (type is non-null) are valid transactions
            valid_df = batch_df.filter(F.col("type").isNotNull())
            unparsed = total - valid_df.count()
            if unparsed:
                log.warning("batch %s: %d record(s) could not be parsed - skipped",
                            batch_id, unparsed)

            flagged_df = valid_df.filter(F.expr(fraud_sql))
            flagged = flagged_df.count()

            if flagged:
                _write_flagged(flagged_df, args)

            rate = (flagged / total * 100.0) if total else 0.0
            _write_metrics(spark, batch_id, total, flagged, rate, args)

            log.info(
                "batch %s | total=%d flagged=%d fraud_rate=%.4f%%",
                batch_id, total, flagged, rate,
            )
        except Exception:  # noqa: BLE001 - log context, then fail the batch
            log.exception("batch %s failed - stream will stop; "
                          "restart resumes from the checkpoint", batch_id)
            raise
        finally:
            batch_df.unpersist()

    return process_batch


def _write_flagged(flagged_df: DataFrame, args: argparse.Namespace) -> None:
    value_struct = F.struct(
        *[F.col(c) for c in TXN_COLUMNS],
        F.lit(FRAUD_RULE_ID).alias("fraud_rule"),
        F.date_format(
            F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"
        ).alias("detected_at"),
    )
    out = flagged_df.select(
        F.col("nameOrig").cast("string").alias("key"),
        F.to_json(value_struct).alias("value"),
    )
    (
        out.write.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("topic", args.output_topic)
        .save()
    )


def _write_metrics(spark, batch_id, total, flagged, rate, args) -> None:
    row = (
        int(batch_id),
        datetime.now(timezone.utc).isoformat(),
        int(total),
        int(flagged),
        f"{rate:.6f}",
        STREAM.app_name_fraud,
        FRAUD_RULE_ID,
    )
    df = spark.createDataFrame([row], schema=_METRICS_SCHEMA)
    (
        df.coalesce(1)
        .write.mode("append")
        .json(f"{args.namenode}{args.metrics_path}")
    )


# --------------------------------------------------------------------------- #
# Stream wiring
# --------------------------------------------------------------------------- #
def build_stream(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", args.starting_offsets)
        .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("timestamp").alias("kafka_ts"),
            parse_txn_value(F.col("value").cast("string")).alias("txn"),
        )
        .select("kafka_key", "kafka_ts", "txn.*")
    )


def run(args: argparse.Namespace) -> int:
    spark = build_spark(STREAM.app_name_fraud, master=args.master or None)
    global log
    log = set_log_level(spark, args.log_level)

    checkpoint = f"{args.namenode}{args.checkpoint}"
    if args.reset_checkpoint:
        _delete_hdfs(spark, checkpoint)
        log.warning("checkpoint reset: %s", checkpoint)

    log.info(
        "starting %s | %s -> %s | master=%s | checkpoint=%s | trigger=%s | once=%s",
        STREAM.app_name_fraud, args.input_topic, args.output_topic,
        spark.sparkContext.master, checkpoint, args.trigger, args.once,
    )

    try:
        stream_df = build_stream(spark, args)
    except AnalysisException as exc:
        log.error("failed to create the Kafka source - is the broker reachable "
                  "at %s? %s", args.bootstrap_servers, exc)
        return 2

    writer = (
        stream_df.writeStream
        .foreachBatch(make_batch_processor(spark, args))
        .option("checkpointLocation", checkpoint)
        .queryName(STREAM.app_name_fraud)
    )
    writer = writer.trigger(availableNow=True) if args.once \
        else writer.trigger(processingTime=args.trigger)

    query = writer.start()

    def _handler(signum, _frame):  # noqa: ANN001
        name = signal.Signals(signum).name
        log.warning("received %s - stopping stream gracefully...", name)
        query.stop()

    for sig in ("SIGINT", "SIGTERM", "SIGBREAK"):
        s = getattr(signal, sig, None)
        if s is not None:
            try:
                signal.signal(s, _handler)
            except (ValueError, OSError):
                pass

    try:
        query.awaitTermination()
    except StreamingQueryException as exc:
        log.error("stream terminated with an error: %s", exc)
        return 1
    finally:
        if not query.isActive:
            log.info("stream stopped. last progress: %s",
                     (query.lastProgress or {}).get("batchId"))
        spark.stop()

    return 0


def _delete_hdfs(spark: SparkSession, uri: str) -> None:
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    path = jvm.org.apache.hadoop.fs.Path(uri)
    fs = path.getFileSystem(hconf)
    if fs.exists(path):
        fs.delete(path, True)


# --------------------------------------------------------------------------- #
# CLI
# --------------------------------------------------------------------------- #
def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    ap.add_argument("--master", default="",
                    help="Spark master (default: from spark-submit / cluster)")
    ap.add_argument("--bootstrap-servers", default=KAFKA.bootstrap_servers)
    ap.add_argument("--input-topic", default=KAFKA.topic_raw)
    ap.add_argument("--output-topic", default=KAFKA.topic_flagged)
    ap.add_argument("--checkpoint", default=PATHS.checkpoint_fraud,
                    help="HDFS checkpoint path (spec 7.1 R1)")
    ap.add_argument("--metrics-path", default=PATHS.streaming_metrics,
                    help="HDFS path for per-batch fraud-rate metrics (spec 7.1 R2)")
    ap.add_argument("--namenode", default=PATHS.hdfs_namenode)
    ap.add_argument("--starting-offsets", default=KAFKA.starting_offsets,
                    choices=["latest", "earliest"])
    ap.add_argument("--max-offsets-per-trigger", default=KAFKA.max_offsets_per_trigger)
    ap.add_argument("--trigger", default=STREAM.trigger_interval,
                    help='processing-time trigger, e.g. "10 seconds"')
    ap.add_argument("--once", action="store_true",
                    help="process all currently-available data then stop "
                         "(Trigger.AvailableNow) - used by the validation test")
    ap.add_argument("--reset-checkpoint", action="store_true",
                    help="delete the checkpoint dir before starting (clean run)")
    ap.add_argument("--log-level", default=STREAM.log_level)
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return run(args)
    except KeyboardInterrupt:
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"FATAL: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
