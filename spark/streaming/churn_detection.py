#!/usr/bin/env python3
"""
FinSight - Phase 5: Spark Structured Streaming real-time customer churn detection.

    Kafka txn-raw ──► Structured Streaming (24-step sliding window) ──► Kafka txn-churn
                              │
                              └──► HDFS /finsight/processed/churn_alerts/  (Parquet)

INDEPENDENT of fraud_detection.py - own SparkSession / appName / checkpoint, and
runs concurrently on the same topic (spec 7.2 R1).

Signals (spec 7.2 - see spark/streaming/churn_rule.py, DO NOT change):
  S1  txn frequency < 1 per 12 steps AND historical avg > 3 per 12 steps
  S2  window avg amount < 20% of the customer's all-time avg
  S3  window activity is exclusively CASH_OUT (no PAYMENT / DEBIT)
  S4  newbalanceOrig < 500 for >= 2 consecutive transactions
Flag a customer when >= 2 signals fire within a 24-step sliding window.

Needs the per-customer baseline first:  spark/streaming/run_bootstrap.sh

Run:
    spark/streaming/run_churn_detection.sh
    spark/streaming/run_churn_detection.sh --once --starting-offsets earliest

Exit codes: 0 ok, 1 stream failure, 2 startup/config error.
"""
from __future__ import annotations

import argparse
import os
import signal
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType
from pyspark.sql.utils import AnalysisException, StreamingQueryException

_HERE = os.path.dirname(os.path.abspath(__file__))
_SPARK_DIR = os.path.dirname(_HERE)
for _p in (_SPARK_DIR, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from common.config import CHURN, KAFKA, PATHS, STREAM  # noqa: E402
from common.event_time import event_ts_expr  # noqa: E402
from common.schemas import parse_txn_value  # noqa: E402
from common.spark_session import build_spark, set_log_level  # noqa: E402
from churn_rule import (  # noqa: E402
    SIGNAL_NAMES,
    Thresholds,
    signal_4_consecutive_low_balance,
)

log = None

_T = Thresholds(
    freq_low_per_12=CHURN.freq_low_per_12,
    freq_hist_per_12=CHURN.freq_hist_per_12,
    amount_drop_fraction=CHURN.amount_drop_fraction,
    balance_low_threshold=CHURN.balance_low_threshold,
    balance_low_consecutive=CHURN.balance_low_consecutive,
    min_signals=CHURN.min_signals,
    window_steps=CHURN.window_steps,
)


# --- S4 needs transaction order -> a plain Python UDF over the collected events
@F.udf(BooleanType())
def _s4_consecutive_low_balance(events):  # noqa: ANN001
    if not events:
        return False
    ordered = sorted(events, key=lambda r: (r["step"], r["kafka_offset"]))
    return signal_4_consecutive_low_balance(
        [r["newbalanceOrig"] for r in ordered], _T)


def _signal_columns():
    """S1, S2, S3 as Spark Column expressions over the window aggregates."""
    win_per_12 = F.col("w_count") / F.lit(CHURN.window_steps / 12.0)
    s1 = (
        F.col("hist_freq_per_12").isNotNull()
        & (F.col("hist_freq_per_12") > F.lit(_T.freq_hist_per_12))
        & (win_per_12 < F.lit(_T.freq_low_per_12))
    )
    s2 = (
        F.col("all_time_avg_amount").isNotNull()
        & (F.col("all_time_avg_amount") > 0)
        & (F.col("w_avg_amount")
           < F.lit(_T.amount_drop_fraction) * F.col("all_time_avg_amount"))
    )
    s3 = (
        (F.col("w_count") > 0)
        & (F.col("cashout_count") == F.col("w_count"))
        & (F.col("payment_debit_count") == 0)
    )
    return s1, s2, s3


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
    txns = (
        raw.select(
            F.col("offset").alias("kafka_offset"),
            parse_txn_value(F.col("value").cast("string")).alias("txn"),
        )
        .select("kafka_offset", "txn.*")
        .filter(F.col("type").isNotNull() & F.col("nameOrig").isNotNull())
        .withColumn("event_ts", event_ts_expr(CHURN.sim_epoch, "step"))
        .withColumnRenamed("nameOrig", "customerId")
        .withColumn("amount", F.col("amount").cast("double"))
        .withColumn("newbalanceOrig", F.col("newbalanceOrig").cast("double"))
        .withWatermark("event_ts", args.watermark)
    )

    win = f"{CHURN.window_steps} hours"
    slide = f"{CHURN.slide_steps} hours"
    agg = (
        txns.groupBy(F.window("event_ts", win, slide), "customerId")
        .agg(
            F.count("*").alias("w_count"),
            F.avg("amount").alias("w_avg_amount"),
            F.sum(F.when(F.col("type") == "CASH_OUT", 1).otherwise(0)).alias("cashout_count"),
            F.sum(F.when(F.col("type").isin("PAYMENT", "DEBIT"), 1).otherwise(0))
                .alias("payment_debit_count"),
            F.collect_list(F.struct("step", "kafka_offset", "newbalanceOrig"))
                .alias("balance_events"),
        )
    )
    return agg


def load_baseline(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    src = f"{args.namenode}{args.baseline_path}"
    try:
        b = spark.read.parquet(src).select(
            "customerId", "hist_freq_per_12", "all_time_avg_amount")
        log.info("loaded customer baseline: %d row(s) from %s", b.count(), src)
        return b
    except AnalysisException:
        log.warning("no baseline at %s - S1/S2 cannot fire until "
                    "bootstrap_customer_history.py has run", src)
        return spark.createDataFrame(
            [], "customerId string, hist_freq_per_12 double, all_time_avg_amount double")


def make_batch_processor(spark: SparkSession, args: argparse.Namespace):
    baseline = load_baseline(spark, args).cache()
    epoch = CHURN.sim_epoch.replace("Z", "+00:00")
    s1, s2, s3 = _signal_columns()

    def process(agg_df: DataFrame, batch_id: int) -> None:
        try:
            scored = (
                agg_df.join(F.broadcast(baseline), "customerId", "left")
                .withColumn("S1_LOW_FREQUENCY", s1)
                .withColumn("S2_AMOUNT_DROP", s2)
                .withColumn("S3_EXCLUSIVE_CASHOUT", s3)
                .withColumn("S4_CONSECUTIVE_LOW_BALANCE",
                            _s4_consecutive_low_balance(F.col("balance_events")))
            )
            signal_arr = F.array_compact(F.array(*[
                F.when(F.col(name), F.lit(name)) for name in SIGNAL_NAMES
            ]))
            scored = (
                scored.withColumn("signals", signal_arr)
                .withColumn("signal_count", F.size("signals"))
            )
            alerts = scored.filter(F.col("signal_count") >= F.lit(_T.min_signals))

            step_of = (
                "cast((unix_timestamp({c}) - unix_timestamp(to_timestamp('"
                + epoch + "'))) / 3600 + 1 as int)")
            alerts = alerts.select(
                "customerId",
                F.col("window.start").alias("window_start"),
                F.col("window.end").alias("window_end"),
                F.expr(step_of.format(c="window.start")).alias("window_start_step"),
                F.expr(step_of.format(c="window.end") + " - 1").alias("window_end_step"),
                "signals", "signal_count",
                F.col("w_count").alias("window_txn_count"),
                F.round("w_avg_amount", 2).alias("window_avg_amount"),
                F.date_format(F.current_timestamp(),
                              "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("detected_at"),
            ).persist()

            n = alerts.count()
            if n == 0:
                log.info("batch %s | 0 churn alerts", batch_id)
                alerts.unpersist()
                return

            (alerts.withColumn("alert_date", F.to_date("window_end"))
             .write.mode("append").partitionBy("alert_date")
             .parquet(f"{args.namenode}{args.alerts_path}"))

            value = F.to_json(F.struct(
                "customerId", "window_start", "window_end",
                "window_start_step", "window_end_step",
                "signals", "signal_count", "window_txn_count",
                "window_avg_amount", "detected_at"))
            (alerts.select(F.col("customerId").alias("key"), value.alias("value"))
             .write.format("kafka")
             .option("kafka.bootstrap.servers", args.bootstrap_servers)
             .option("topic", args.output_topic).save())

            sample = alerts.select("customerId", "signals", "window_start_step",
                                   "window_end_step").limit(8).collect()
            log.info("batch %s | %d churn alert(s): %s", batch_id, n,
                     [(r.customerId, r.signals) for r in sample])
            alerts.unpersist()
        except Exception:  # noqa: BLE001
            log.exception("batch %s failed - stream will stop; restart resumes "
                          "from the checkpoint", batch_id)
            raise

    return process


def run(args: argparse.Namespace) -> int:
    spark = build_spark(STREAM.app_name_churn, master=args.master or None)
    global log
    log = set_log_level(spark, args.log_level)
    spark.sparkContext.addPyFile(os.path.join(_HERE, "churn_rule.py"))

    checkpoint = f"{args.namenode}{args.checkpoint}"
    if args.reset_checkpoint:
        _delete_hdfs(spark, checkpoint)
        log.warning("checkpoint reset: %s", checkpoint)

    log.info(
        "starting %s | %s -> %s + %s | window=%d slide=%d steps | checkpoint=%s | once=%s",
        STREAM.app_name_churn, args.input_topic, args.output_topic,
        args.alerts_path, CHURN.window_steps, CHURN.slide_steps, checkpoint, args.once,
    )

    try:
        agg = build_stream(spark, args)
    except AnalysisException as exc:
        log.error("failed to build the stream (broker at %s reachable?): %s",
                  args.bootstrap_servers, exc)
        return 2

    writer = (
        agg.writeStream
        .outputMode("update")
        .foreachBatch(make_batch_processor(spark, args))
        .option("checkpointLocation", checkpoint)
        .queryName(STREAM.app_name_churn)
    )
    writer = writer.trigger(availableNow=True) if args.once \
        else writer.trigger(processingTime=args.trigger)
    query = writer.start()

    def _handler(signum, _frame):  # noqa: ANN001
        log.warning("received %s - stopping stream gracefully...",
                    signal.Signals(signum).name)
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
            log.info("stream stopped. last batch: %s",
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


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--master", default="")
    ap.add_argument("--bootstrap-servers", default=KAFKA.bootstrap_servers)
    ap.add_argument("--input-topic", default=KAFKA.topic_raw)
    ap.add_argument("--output-topic", default=KAFKA.topic_churn)
    ap.add_argument("--checkpoint", default=PATHS.checkpoint_churn)
    ap.add_argument("--alerts-path", default=PATHS.churn_alerts,
                    help="HDFS Parquet output for churn alerts (spec 7.2)")
    ap.add_argument("--baseline-path", default=PATHS.customer_baseline)
    ap.add_argument("--namenode", default=PATHS.hdfs_namenode)
    ap.add_argument("--starting-offsets", default=KAFKA.starting_offsets,
                    choices=["latest", "earliest"])
    ap.add_argument("--max-offsets-per-trigger", default=KAFKA.max_offsets_per_trigger)
    ap.add_argument("--trigger", default=STREAM.trigger_interval)
    ap.add_argument("--watermark", default=os.environ.get("CHURN_WATERMARK", "48 hours"))
    ap.add_argument("--once", action="store_true")
    ap.add_argument("--reset-checkpoint", action="store_true")
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
