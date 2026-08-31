#!/usr/bin/env python3
"""
FinSight - Phase 12 / WORKFLOW 2: Transaction Summary  (HEADLESS FALLBACK)

NOT an Alteryx artifact. Reference implementation of
docs/alteryx/workflow-2-transaction-summary.md (ASSUMPTIONS I10).

  Filter : step 1..168 (inclusive)
  Group  : transaction type, step
  Calc   : total volume, average transaction amount, fraud count
  Output : alteryx/outputs/transaction_summary.csv  - suitable for Power BI.

Source (ASSUMPTIONS I47): the Alteryx workflow's stated input,
`finsight.txn_summary_mart`, is pre-aggregated to (customerId, step) and stores
`txn_types` as a comma-joined string, so it cannot split volume/amount per
transaction type. The accurate per-(type, step) grain is `finsight.transactions`
- which this fallback reads via **Spark** inside `finsight-spark-master` (Spark's
Hive reader recurses the `step=<N>/` Parquet layout; HiveServer2/Tez in this
dev stack does not - see Phase 8 notes). Aggregation is small (<= 5 types x 168
steps), so the result is collected and written straight to CSV.

    python alteryx/fallback/transaction_summary.py
"""
from __future__ import annotations

import io
import os
import subprocess
import sys
from pathlib import Path

try:
    import pandas as pd
except ModuleNotFoundError:
    sys.exit("pandas missing - `pip install -r requirements.txt`")

REPO = Path(__file__).resolve().parents[2]
SPARK = os.environ.get("SPARK_CONTAINER", "finsight-spark-master")
OUT = REPO / "alteryx" / "outputs" / "transaction_summary.csv"

SPARK_JOB = r'''
from pyspark.sql import SparkSession, functions as F
s = (SparkSession.builder.appName("finsight-alteryx-wf2-fallback")
     .enableHiveSupport().getOrCreate())
df = s.table("finsight.transactions").filter((F.col("step") >= 1) & (F.col("step") <= 168))
g = (df.groupBy(F.col("type").alias("transaction_type"), "step")
       .agg(F.round(F.sum("amount"), 2).alias("total_volume"),
            F.count(F.lit(1)).alias("transaction_count"),
            F.round(F.avg("amount"), 2).alias("avg_transaction_amount"),
            F.sum(F.when(F.col("isFraud") == 1, 1).otherwise(0)).alias("fraud_count"))
       .orderBy("step", "transaction_type"))
print("W2_START")
print("transaction_type,step,total_volume,transaction_count,avg_transaction_amount,fraud_count")
for r in g.collect():
    print(f"{r['transaction_type']},{r['step']},{r['total_volume']},"
          f"{r['transaction_count']},{r['avg_transaction_amount']},{r['fraud_count']}")
print("W2_END")
'''


def main() -> int:
    print(f"[txn-summary] Spark over finsight.transactions inside {SPARK} ...")
    helper = REPO / "scripts" / "_p12_wf2.py"
    helper.write_text(SPARK_JOB)
    subprocess.run(["docker", "cp", str(helper), f"{SPARK}:/tmp/_p12_wf2.py"],
                   capture_output=True, text=True)
    r = subprocess.run(
        ["docker", "exec", SPARK, "/opt/spark/bin/spark-submit", "--master",
         "spark://spark-master:7077", "--conf", "spark.jars.ivy=/opt/spark/.ivy2",
         "/tmp/_p12_wf2.py"],
        capture_output=True, text=True, timeout=600)
    helper.unlink(missing_ok=True)

    lines, cap = [], False
    for ln in (r.stdout + r.stderr).splitlines():
        if ln.strip() == "W2_START":
            cap = True
        elif ln.strip() == "W2_END":
            break
        elif cap:
            lines.append(ln)
    if len(lines) < 2:
        print((r.stdout + r.stderr)[-2000:])
        sys.exit("Spark job produced no rows - is finsight.transactions populated? "
                 "(Phases 2-3 + Phase 8 warehouse)")

    df = pd.read_csv(io.StringIO("\n".join(lines)))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(OUT, index=False)
    print(f"[txn-summary] wrote {OUT}  ({len(df)} rows = type x step groups)")
    print(f"[txn-summary] steps {int(df['step'].min())}..{int(df['step'].max())}, "
          f"types {sorted(df['transaction_type'].unique())}")
    print(f"[txn-summary] totals: volume={df['total_volume'].sum():,.2f}  "
          f"txns={int(df['transaction_count'].sum())}  fraud={int(df['fraud_count'].sum())}")
    print("\n[txn-summary] head:")
    print(df.head(8).to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
