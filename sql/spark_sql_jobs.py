#!/usr/bin/env python3
"""
FinSight - Phase 9: Spark SQL analytics over the Hive warehouse (spec 7.5 / 7.6).

One entry point, three modes:

    python sql/spark_sql_jobs.py --mode compliance
    python sql/spark_sql_jobs.py --mode customer_summary
    python sql/spark_sql_jobs.py --mode dormancy

  compliance        by transaction type over a 168-step window: count, volume,
                    fraud count, fraud rate, risk classification
                    -> /finsight/processed/compliance_summary/   (+ CSV export)

  customer_summary  per customer: total transactions, total amount, confirmed
                    fraud count, fraud rate
                    -> /finsight/processed/customer_fraud_summary/

  dormancy          accounts inactive > 72 steps, >= 5 prior txns, prefix C
                    (merchants excluded); severity Dormant (72-120) /
                    Severely Dormant (>120)
                    -> /finsight/processed/dormancy_report/
                    -> /finsight/exports/dormancy_report.csv

Runs against Hive table `finsight.transactions` by default; --from parquet|csv
for isolated testing. No Alteryx / no Spark Structured Streaming here.

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
_REPO = os.path.dirname(_HERE)
for _p in (os.path.join(_REPO, "spark"), _HERE):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from common.config import PATHS, SQLJOBS  # noqa: E402
from common.schemas import TXN_SCHEMA  # noqa: E402
from common.spark_session import build_spark, set_log_level  # noqa: E402
from sql_rules import (  # noqa: E402
    RISK_HIGH_FRAUD_PCT,
    RISK_MEDIUM_FRAUD_PCT,
)

log = None
TXN = "txn_source"   # temp-view name the SQL queries read from


# --------------------------------------------------------------------------- #
def load_source(spark: SparkSession, args: argparse.Namespace) -> None:
    """Register the transaction source as the `txn_source` temp view."""
    if args.source == "hive":
        try:
            df = spark.table(args.table)
        except AnalysisException as exc:
            raise SystemExit(f"cannot read Hive table {args.table} "
                             f"(has Phase 8 run?): {exc}")
        log.info("source: Hive table %s", args.table)
    elif args.source == "parquet":
        src = args.input if "://" in args.input else f"{args.namenode}{args.input}"
        try:
            df = spark.read.parquet(src)
        except AnalysisException as exc:
            raise SystemExit(f"cannot read {src}: {exc}")
        log.info("source: Parquet %s", src)
    else:  # csv
        if not args.csv:
            raise SystemExit("--from csv requires --csv <path>")
        path = args.csv if "://" in args.csv else (
            "file://" + args.csv if args.csv.startswith("/") else args.csv)
        df = spark.read.option("header", True).schema(TXN_SCHEMA).csv(path)
        log.info("source: CSV %s", path)

    df.select("step", "type", "amount", "nameOrig", "newbalanceOrig", "isFraud") \
      .filter(F.col("nameOrig").isNotNull() & F.col("type").isNotNull()) \
      .createOrReplaceTempView(TXN)
    n = spark.table(TXN).count()
    if n == 0:
        raise SystemExit("no transactions in the source")
    log.info("loaded %d transaction row(s)", n)


def write_parquet(df: DataFrame, hdfs_path: str) -> int:
    df.coalesce(1).write.mode("overwrite").parquet(hdfs_path)
    return df.count()


def write_single_csv(spark: SparkSession, df: DataFrame, hdfs_file: str) -> None:
    """Write `df` as ONE CSV file at exactly `hdfs_file` (spec 7.6)."""
    tmp = hdfs_file + "__tmp"
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp)
    jvm = spark._jvm
    hconf = spark._jsc.hadoopConfiguration()
    fs = jvm.org.apache.hadoop.fs.FileSystem.get(hconf)
    Path = jvm.org.apache.hadoop.fs.Path
    part = None
    for st in fs.listStatus(Path(tmp)):
        name = st.getPath().getName()
        if name.startswith("part-") and name.endswith(".csv"):
            part = st.getPath()
    target = Path(hdfs_file)
    if fs.exists(target):
        fs.delete(target, True)
    if part is not None:
        fs.rename(part, target)
    fs.delete(Path(tmp), True)


# --------------------------------------------------------------------------- #
def run_compliance(spark: SparkSession, args: argparse.Namespace) -> int:
    w = SQLJOBS.compliance_window_steps
    hi, med = SQLJOBS.risk_high_fraud_pct, SQLJOBS.risk_medium_fraud_pct
    sql = f"""
        WITH agg AS (
          SELECT
            type                                              AS transaction_type,
            COUNT(*)                                           AS transaction_count,
            ROUND(SUM(amount), 2)                              AS transaction_volume,
            SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END)       AS fraud_count
          FROM {TXN}
          WHERE step BETWEEN 1 AND {w}
          GROUP BY type
        )
        SELECT
          transaction_type,
          transaction_count,
          transaction_volume,
          fraud_count,
          ROUND(fraud_count * 100.0 / transaction_count, 4)    AS fraud_rate_pct,
          CASE
            WHEN fraud_count * 100.0 / transaction_count >= {hi}  THEN 'High'
            WHEN fraud_count * 100.0 / transaction_count >= {med} THEN 'Medium'
            ELSE 'Low'
          END                                                 AS risk_classification
        FROM agg
        ORDER BY transaction_type
    """
    df = spark.sql(sql)
    df.show(truncate=False)
    n = write_parquet(df, f"{args.namenode}{args.compliance_out}")
    write_single_csv(spark, df, f"{args.namenode}{args.exports}/compliance_summary.csv")
    log.info("compliance: %d transaction-type row(s) over steps 1..%d -> %s (+ CSV)",
             n, w, args.compliance_out)
    return 0


def run_customer_summary(spark: SparkSession, args: argparse.Namespace) -> int:
    sql = f"""
        SELECT
          nameOrig                                             AS customerId,
          COUNT(*)                                             AS total_transactions,
          ROUND(SUM(amount), 2)                                AS total_amount,
          SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END)         AS confirmed_fraud_count,
          ROUND(SUM(CASE WHEN isFraud = 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 6)
                                                               AS fraud_rate_pct
        FROM {TXN}
        WHERE nameOrig LIKE 'C%'
        GROUP BY nameOrig
        ORDER BY confirmed_fraud_count DESC, total_amount DESC
    """
    df = spark.sql(sql)
    df.show(10, truncate=False)
    n = write_parquet(df, f"{args.namenode}{args.customer_summary_out}")
    log.info("customer_summary: %d customer row(s) -> %s", n, args.customer_summary_out)
    return 0


def run_dormancy(spark: SparkSession, args: argparse.Namespace) -> int:
    inactive = SQLJOBS.dormancy_inactive_steps
    severe = SQLJOBS.dormancy_severe_steps
    min_hist = SQLJOBS.dormancy_min_history
    sql = f"""
        WITH last_active AS (
          SELECT
            nameOrig,
            MAX(step)  AS last_active_step,
            COUNT(*)   AS txn_history_count
          FROM {TXN}
          GROUP BY nameOrig
        ),
        mx AS (SELECT MAX(step) AS max_step FROM {TXN})
        SELECT
          la.nameOrig                                          AS customerId,
          la.last_active_step,
          mx.max_step,
          (mx.max_step - la.last_active_step)                   AS steps_inactive,
          la.txn_history_count,
          CASE
            WHEN (mx.max_step - la.last_active_step) > {severe} THEN 'Severely Dormant'
            ELSE 'Dormant'
          END                                                  AS dormancy_severity
        FROM last_active la CROSS JOIN mx
        WHERE la.nameOrig LIKE 'C%'
          AND la.txn_history_count >= {min_hist}
          AND (mx.max_step - la.last_active_step) > {inactive}
        ORDER BY steps_inactive DESC
    """
    df = spark.sql(sql)
    df.show(20, truncate=False)
    n = write_parquet(df, f"{args.namenode}{args.dormancy_out}")
    write_single_csv(spark, df, f"{args.namenode}{args.exports}/dormancy_report.csv")
    by_sev = {r["dormancy_severity"]: r["n"] for r in
              df.groupBy("dormancy_severity").agg(F.count("*").alias("n")).collect()}
    log.info("dormancy: %d dormant account(s) [Dormant=%d, Severely Dormant=%d] -> %s (+ CSV)",
             n, by_sev.get("Dormant", 0), by_sev.get("Severely Dormant", 0),
             args.dormancy_out)
    return 0


_MODES = {
    "compliance": run_compliance,
    "customer_summary": run_customer_summary,
    "dormancy": run_dormancy,
}


# --------------------------------------------------------------------------- #
def run(args: argparse.Namespace) -> int:
    spark = build_spark(f"{SQLJOBS.app_name}-{args.mode}", master=args.master or None)
    global log
    log = set_log_level(spark, args.log_level)
    log.info("mode=%s  risk thresholds: High>=%.1f%%  Medium>=%.1f%%",
             args.mode, RISK_HIGH_FRAUD_PCT, RISK_MEDIUM_FRAUD_PCT)

    load_source(spark, args)
    rc = _MODES[args.mode](spark, args)

    spark.stop()
    return rc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--mode", required=True, choices=sorted(_MODES),
                    help="compliance | customer_summary | dormancy")
    ap.add_argument("--master", default="")
    ap.add_argument("--from", dest="source", choices=["hive", "parquet", "csv"],
                    default="hive")
    ap.add_argument("--table", default=SQLJOBS.txn_table,
                    help="Hive table (with --from hive)")
    ap.add_argument("--input", default=PATHS.raw_txn,
                    help="Parquet path (with --from parquet)")
    ap.add_argument("--csv", help="CSV path (with --from csv)")
    ap.add_argument("--namenode", default=PATHS.hdfs_namenode)
    ap.add_argument("--compliance-out", default=PATHS.compliance_summary)
    ap.add_argument("--customer-summary-out", default=PATHS.customer_fraud_summary)
    ap.add_argument("--dormancy-out", default=PATHS.dormancy_report)
    ap.add_argument("--exports", default=PATHS.exports)
    ap.add_argument("--log-level", default=os.environ.get("LOG_LEVEL", "INFO"))
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        return run(args)
    except SystemExit as exc:
        if isinstance(exc.code, int):
            return exc.code
        print(f"FATAL: {exc}", file=sys.stderr)
        return 2
    except Exception as exc:  # noqa: BLE001
        print(f"FATAL: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
