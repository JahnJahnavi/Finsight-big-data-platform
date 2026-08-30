#!/usr/bin/env python3
"""
FinSight - Phase 6: Spark Core batch customer risk scoring (spec section 7.3).

    /finsight/raw/txn-raw/ (Parquet, full history)
        -> per customerId (nameOrig):
             frequency, avg TRANSFER amount, CASH_OUT proportion, # unique dests
        -> min-max normalise each factor, weighted sum -> risk_score in [0, 1]
        -> risk_tier:  < 0.25 Low | 0.25-0.60 Medium | > 0.60 High
        -> /finsight/processed/risk_scores/     (customerId, risk_score, risk_tier, ...)

    plus (spec 7.3 R2):
        -> /finsight/processed/daily_summary/   txn volume, total amount, fraud
                                                count  BY transaction type AND step
        -> /finsight/exports/daily_summary/     the same, as CSV, for Alteryx (9.2)

INDEPENDENT Spark application - own SparkSession + distinct appName
(finsight-batch-risk). Pure batch: no Kafka, no streaming, no CLV.

Run (inside the spark container):
    spark/batch/run_risk_scoring.sh
    spark/batch/run_risk_scoring.sh --from csv --csv /opt/finsight/data/sample/txns.csv
    spark/batch/run_risk_scoring.sh --show

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

from common.config import PATHS, RISK  # noqa: E402
from common.schemas import TXN_SCHEMA  # noqa: E402
from common.spark_session import build_spark, set_log_level  # noqa: E402

log = None

# raw factor -> column produced by the per-customer aggregation
_FACTOR_COLS = {
    "frequency": "frequency",
    "avg_transfer_amount": "avg_transfer_amount",
    "cashout_proportion": "cashout_proportion",
    "unique_dest_accounts": "unique_dest_accounts",
}


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

    return df.select(
        "step", "type", "amount", "nameOrig", "newbalanceOrig",
        "nameDest", "isFraud",
    ).filter(F.col("nameOrig").isNotNull() & F.col("type").isNotNull())


# --------------------------------------------------------------------------- #
def customer_factors(txns: DataFrame) -> DataFrame:
    """One row per customer with the four RAW risk factors (spec 7.3)."""
    customers = txns.filter(F.col("nameOrig").startswith("C"))
    return (
        customers.groupBy(F.col("nameOrig").alias("customerId"))
        .agg(
            F.count("*").alias("frequency"),
            F.coalesce(
                F.avg(F.when(F.col("type") == "TRANSFER", F.col("amount"))),
                F.lit(0.0),
            ).alias("avg_transfer_amount"),
            (F.sum(F.when(F.col("type") == "CASH_OUT", 1).otherwise(0))
             / F.count("*")).alias("cashout_proportion"),
            F.countDistinct("nameDest").alias("unique_dest_accounts"),
        )
    )


def score_customers(factors: DataFrame) -> DataFrame:
    """Min-max normalise each factor, weighted sum -> risk_score + risk_tier."""
    stats = factors.agg(*[
        F.min(c).alias(f"{c}_min") for c in _FACTOR_COLS.values()
    ] + [
        F.max(c).alias(f"{c}_max") for c in _FACTOR_COLS.values()
    ]).first().asDict()

    def _norm(col: str):
        lo, hi = stats[f"{col}_min"], stats[f"{col}_max"]
        if hi is None or lo is None or hi <= lo:
            return F.lit(0.0)
        return F.greatest(F.lit(0.0), F.least(
            F.lit(1.0), (F.col(col) - F.lit(lo)) / (F.lit(hi) - F.lit(lo))))

    w_freq, w_avg, w_cash, w_dest = RISK.weights()
    scored = (
        factors
        .withColumn("norm_frequency", _norm("frequency"))
        .withColumn("norm_avg_transfer_amount", _norm("avg_transfer_amount"))
        .withColumn("norm_cashout_proportion", _norm("cashout_proportion"))
        .withColumn("norm_unique_dest_accounts", _norm("unique_dest_accounts"))
        .withColumn(
            "risk_score",
            F.round(F.greatest(F.lit(0.0), F.least(F.lit(1.0),
                F.lit(w_freq) * F.col("norm_frequency")
                + F.lit(w_avg) * F.col("norm_avg_transfer_amount")
                + F.lit(w_cash) * F.col("norm_cashout_proportion")
                + F.lit(w_dest) * F.col("norm_unique_dest_accounts"))), 6),
        )
        .withColumn(
            "risk_tier",
            F.when(F.col("risk_score") < F.lit(RISK.tier_low_max), "Low")
            .when(F.col("risk_score") <= F.lit(RISK.tier_medium_max), "Medium")
            .otherwise("High"),
        )
        .withColumn("scored_at", F.current_timestamp())
    )
    return scored


def daily_summary(txns: DataFrame) -> DataFrame:
    """Spec 7.3 R2 - volume / total amount / fraud count by type AND step."""
    return (
        txns.groupBy("type", "step")
        .agg(
            F.count("*").alias("transaction_volume"),
            F.round(F.sum("amount"), 2).alias("total_amount"),
            F.sum(F.coalesce(F.col("isFraud"), F.lit(0))).alias("fraud_count"),
        )
        .orderBy("type", "step")
    )


# --------------------------------------------------------------------------- #
def run(args: argparse.Namespace) -> int:
    spark = build_spark(RISK.app_name, master=args.master or None)
    global log
    log = set_log_level(spark, args.log_level)
    nn = args.namenode

    txns = load_transactions(spark, args).persist()
    total = txns.count()
    if total == 0:
        log.error("no transactions to score")
        return 2
    log.info("scoring %d transaction(s)", total)

    # --- risk scores ---
    factors = customer_factors(txns)
    scored = score_customers(factors)
    risk_out = (
        scored.select(
            "customerId", "risk_score", "risk_tier",
            "frequency", "avg_transfer_amount", "cashout_proportion",
            "unique_dest_accounts",
            "norm_frequency", "norm_avg_transfer_amount",
            "norm_cashout_proportion", "norm_unique_dest_accounts",
            "scored_at",
        )
    ).persist()

    n_cust = risk_out.count()
    risk_path = f"{nn}{args.risk_out}"
    risk_out.coalesce(1).write.mode("overwrite").parquet(risk_path)
    log.info("wrote %d customer risk score(s) -> %s", n_cust, risk_path)

    tiers = {r["risk_tier"]: r["n"] for r in
             risk_out.groupBy("risk_tier").agg(F.count("*").alias("n")).collect()}
    log.info("risk tiers: Low=%d Medium=%d High=%d",
             tiers.get("Low", 0), tiers.get("Medium", 0), tiers.get("High", 0))
    if args.show:
        risk_out.orderBy(F.desc("risk_score")).show(20, truncate=False)

    # --- daily summary ---
    summary = daily_summary(txns).persist()
    n_rows = summary.count()
    summary_path = f"{nn}{args.summary_out}"
    summary.coalesce(1).write.mode("overwrite").parquet(summary_path)
    csv_path = f"{nn}{args.csv_export_dir.rstrip('/')}/daily_summary"
    summary.coalesce(1).write.mode("overwrite").option("header", True).csv(csv_path)
    log.info("wrote daily summary (%d type/step rows) -> %s  (+ CSV %s)",
             n_rows, summary_path, csv_path)
    if args.show:
        summary.show(20, truncate=False)

    txns.unpersist()
    risk_out.unpersist()
    summary.unpersist()
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
    ap.add_argument("--risk-out", default=PATHS.risk_scores)
    ap.add_argument("--summary-out", default=PATHS.daily_summary)
    ap.add_argument("--csv-export-dir", default=PATHS.exports)
    ap.add_argument("--namenode", default=PATHS.hdfs_namenode)
    ap.add_argument("--show", action="store_true", help="print sample output")
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
