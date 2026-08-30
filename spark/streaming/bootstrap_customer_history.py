#!/usr/bin/env python3
"""
FinSight - Phase 5: build the per-customer behavioural baseline.

Churn signals S1 (frequency) and S2 (amount) compare the sliding window against
the customer's *history*, which a streaming job does not have at cold start
(docs/ASSUMPTIONS.md G8). This batch job pre-computes that history once; the
streaming job reads it as a static, broadcast dimension.

    read transaction history (HDFS Parquet or CSV)
        -> per customerId (= nameOrig):
             all_time_txn_count
             all_time_avg_amount
             first_step, last_step
             hist_freq_per_12  = txn_count / ((last_step_overall - first_step + 1) / 12)
        -> write Parquet to /finsight/processed/customer_baseline/

Usage (inside the spark container):
    /opt/spark/bin/spark-submit .../bootstrap_customer_history.py
    ... --from csv --csv /opt/finsight/data/sample/history.csv
    ... --path /finsight/raw/txn-raw --out /finsight/processed/customer_baseline
"""
from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import functions as F

_HERE = os.path.dirname(os.path.abspath(__file__))
_SPARK_DIR = os.path.dirname(_HERE)
for _p in (_SPARK_DIR, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from common.config import PATHS  # noqa: E402
from common.schemas import TXN_SCHEMA  # noqa: E402
from common.spark_session import build_spark, set_log_level  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--from", dest="source", choices=["hdfs", "csv"], default="hdfs")
    ap.add_argument("--path", default=PATHS.raw_txn,
                    help="HDFS Parquet root (when --from hdfs)")
    ap.add_argument("--csv", help="CSV path (when --from csv)")
    ap.add_argument("--out", default=PATHS.customer_baseline)
    ap.add_argument("--namenode", default=PATHS.hdfs_namenode)
    ap.add_argument("--master", default="")
    ap.add_argument("--log-level", default="WARN")
    args = ap.parse_args(argv)

    spark = build_spark("finsight-bootstrap-customer-history",
                        master=args.master or None)
    log = set_log_level(spark, args.log_level)

    if args.source == "csv":
        if not args.csv:
            log.error("--from csv requires --csv <path>")
            return 2
        # the default filesystem is HDFS - qualify a bare local path as file://
        csv_path = args.csv
        if "://" not in csv_path and csv_path.startswith("/"):
            csv_path = "file://" + csv_path
        df = (spark.read.option("header", True).schema(TXN_SCHEMA).csv(csv_path))
        log.info("reading history from CSV %s", csv_path)
    else:
        src = f"{args.namenode}{args.path}"
        try:
            df = spark.read.parquet(src)
        except Exception as exc:  # noqa: BLE001
            log.error("could not read %s (has Phase 3 landed any data?): %s", src, exc)
            return 2
        log.info("reading history from HDFS %s", src)

    df = df.select("nameOrig", "type", "amount", "step").filter(F.col("nameOrig").isNotNull())
    if df.rdd.isEmpty():
        log.error("no transaction history found - cannot build a baseline")
        return 2

    last_step_overall = df.agg(F.max("step")).first()[0] or 0

    baseline = (
        df.groupBy("nameOrig")
        .agg(
            F.count("*").alias("all_time_txn_count"),
            F.avg("amount").alias("all_time_avg_amount"),
            F.min("step").alias("first_step"),
            F.max("step").alias("last_step"),
        )
        .withColumn(
            "hist_freq_per_12",
            F.col("all_time_txn_count")
            / ((F.lit(last_step_overall) - F.col("first_step") + F.lit(1)) / F.lit(12.0)),
        )
        .withColumnRenamed("nameOrig", "customerId")
        .withColumn("baseline_built_at", F.current_timestamp())
    )

    out = f"{args.namenode}{args.out}"
    baseline.coalesce(1).write.mode("overwrite").parquet(out)
    n = baseline.count()
    log.info("wrote baseline for %d customer(s) -> %s (last_step_overall=%s)",
             n, out, last_step_overall)
    baseline.orderBy(F.desc("all_time_txn_count")).show(10, truncate=False)
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
