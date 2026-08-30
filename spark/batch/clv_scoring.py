#!/usr/bin/env python3
"""
FinSight - Phase 7: Spark Core batch Customer Lifetime Value scoring (spec 7.4).

    /finsight/raw/txn-raw/ (Parquet, full history)
        -> per customerId (nameOrig, prefix C):
             volume    = cumulative amount / highest-spending account
             frequency = txn count / most-active account
             diversity = distinct transaction types / 5
             recency   = 1 - steps_since_last_txn / 48  (0 once inactivity >= 48)
        -> clv_score = 0.30*volume + 0.25*frequency + 0.25*diversity + 0.20*recency
        -> clv_classification:  > 0.70 High Value | 0.40-0.70 Growth Potential | < 0.40 At Risk
        -> /finsight/processed/clv_scores/   (customerId, clv_score, clv_classification, ...)

INDEPENDENT Spark application - own SparkSession + distinct appName
(finsight-batch-clv), no shared state with risk_scoring.py (spec 7.4 R2).
risk_scoring.py is NOT modified. No Hive registration in this phase.

Run (inside the spark container):
    spark/batch/run_clv_scoring.sh
    spark/batch/run_clv_scoring.sh --show
    spark/batch/run_clv_scoring.sh --from csv --csv /opt/finsight/data/sample/txns.csv

Exit codes: 0 ok, 2 startup / no input data.
"""
from __future__ import annotations

import argparse
import os
import sys

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException

_HERE = os.path.dirname(os.path.abspath(__file__))
_SPARK_DIR = os.path.dirname(_HERE)
for _p in (_SPARK_DIR, _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from common.config import CLV, PATHS  # noqa: E402
from common.schemas import TXN_SCHEMA  # noqa: E402
from common.spark_session import build_spark, set_log_level  # noqa: E402

log = None


# --------------------------------------------------------------------------- #
def load_transactions(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    if args.source == "csv":
        if not args.csv:
            raise SystemExit("--from csv requires --csv <path>")
        path = args.csv
        if "://" not in path and path.startswith("/"):
            path = "file://" + path
        df = spark.read.option("header", True).schema(TXN_SCHEMA).csv(path)
        log.info("reading transactions from CSV %s", path)
    else:
        src = f"{args.namenode}{args.input}"
        try:
            df = spark.read.parquet(src)
        except AnalysisException as exc:
            raise SystemExit(
                f"could not read {src} - has Phase 3 landed any data? ({exc})")
        log.info("reading transactions from HDFS %s", src)

    return df.select("step", "type", "amount", "nameOrig").filter(
        F.col("nameOrig").isNotNull() & F.col("type").isNotNull())


def customer_components(txns: DataFrame) -> DataFrame:
    """One row per customer with the four CLV component scores (spec 7.4)."""
    max_step_overall = txns.agg(F.max("step")).first()[0] or 0

    per_cust = (
        txns.filter(F.col("nameOrig").startswith("C"))
        .groupBy(F.col("nameOrig").alias("customerId"))
        .agg(
            F.sum("amount").alias("total_amount"),
            F.count("*").alias("txn_count"),
            F.countDistinct("type").alias("distinct_txn_types"),
            F.max("step").alias("last_step"),
        )
        .withColumn("steps_since_last_txn",
                    F.lit(max_step_overall) - F.col("last_step"))
    )

    agg = per_cust.agg(
        F.max("total_amount").alias("max_total_amount"),
        F.max("txn_count").alias("max_txn_count"),
    ).first()
    max_amount = agg["max_total_amount"] or 0.0
    max_count = agg["max_txn_count"] or 0

    zero_after = CLV.recency_zero_after_steps
    n_types = CLV.n_txn_types

    vol = (F.col("total_amount") / F.lit(max_amount)) if max_amount > 0 else F.lit(0.0)
    freq = (F.col("txn_count") / F.lit(max_count)) if max_count > 0 else F.lit(0.0)
    div = F.col("distinct_txn_types") / F.lit(float(n_types))
    rec = F.when(
        F.col("steps_since_last_txn") >= F.lit(zero_after), F.lit(0.0)
    ).otherwise(F.lit(1.0) - F.col("steps_since_last_txn") / F.lit(float(zero_after)))

    def _c01(c):
        return F.greatest(F.lit(0.0), F.least(F.lit(1.0), c))

    return (
        per_cust
        .withColumn("volume_score", _c01(vol))
        .withColumn("frequency_score", _c01(freq))
        .withColumn("diversity_score", _c01(div))
        .withColumn("recency_score", _c01(rec))
    )


def score(components: DataFrame) -> DataFrame:
    w_vol, w_freq, w_div, w_rec = CLV.weights()
    return (
        components
        .withColumn(
            "clv_score",
            F.round(F.greatest(F.lit(0.0), F.least(F.lit(1.0),
                F.lit(w_vol) * F.col("volume_score")
                + F.lit(w_freq) * F.col("frequency_score")
                + F.lit(w_div) * F.col("diversity_score")
                + F.lit(w_rec) * F.col("recency_score"))), 6),
        )
        .withColumn(
            "clv_classification",
            F.when(F.col("clv_score") > F.lit(CLV.tier_high_min), "High Value")
            .when(F.col("clv_score") >= F.lit(CLV.tier_growth_min), "Growth Potential")
            .otherwise("At Risk"),
        )
        .withColumn("scored_at", F.current_timestamp())
    )


# --------------------------------------------------------------------------- #
def run(args: argparse.Namespace) -> int:
    spark = build_spark(CLV.app_name, master=args.master or None)
    global log
    log = set_log_level(spark, args.log_level)

    txns = load_transactions(spark, args).persist()
    total = txns.count()
    if total == 0:
        log.error("no transactions to score")
        return 2
    log.info("scoring CLV over %d transaction(s)", total)

    scored = score(customer_components(txns)).select(
        "customerId", "clv_score", "clv_classification",
        "total_amount", "txn_count", "distinct_txn_types",
        "last_step", "steps_since_last_txn",
        "volume_score", "frequency_score", "diversity_score", "recency_score",
        "scored_at",
    ).persist()

    n = scored.count()
    out = f"{args.namenode}{args.out}"
    scored.coalesce(1).write.mode("overwrite").parquet(out)
    log.info("wrote %d customer CLV score(s) -> %s", n, out)

    tiers = {r["clv_classification"]: r["n"] for r in
             scored.groupBy("clv_classification").agg(F.count("*").alias("n")).collect()}
    log.info("CLV tiers: High Value=%d  Growth Potential=%d  At Risk=%d",
             tiers.get("High Value", 0), tiers.get("Growth Potential", 0),
             tiers.get("At Risk", 0))
    if args.show:
        scored.orderBy(F.desc("clv_score")).show(20, truncate=False)

    txns.unpersist()
    scored.unpersist()
    spark.stop()
    return 0


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--master", default="")
    ap.add_argument("--from", dest="source", choices=["hdfs", "csv"], default="hdfs")
    ap.add_argument("--input", default=PATHS.raw_txn,
                    help="HDFS Parquet root of the transaction history")
    ap.add_argument("--csv", help="CSV path (with --from csv)")
    ap.add_argument("--out", default=PATHS.clv_scores)
    ap.add_argument("--namenode", default=PATHS.hdfs_namenode)
    ap.add_argument("--show", action="store_true")
    ap.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"))
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return run(args)
    except SystemExit as exc:
        print(f"FATAL: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001
        print(f"FATAL: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
