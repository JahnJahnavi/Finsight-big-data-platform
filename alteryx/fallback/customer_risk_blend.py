#!/usr/bin/env python3
"""
FinSight - Phase 12 / WORKFLOW 1: Customer Risk Blend  (HEADLESS FALLBACK)

This is NOT an Alteryx artifact and produces NO Alteryx output. It is the
pandas reference implementation of the workflow documented in
docs/alteryx/workflow-1-customer-risk-blend.md, so the blend is reproducible on
a headless box and its numbers can be diffed against a real Designer run
(ASSUMPTIONS I10).

Inputs (read live via `docker exec`, no Hive/Mongo Python driver needed):
  * Hive   finsight.customer_fraud_summary   (alteryx/prereq/*.hql)
  * Mongo  finsight.customers                (profile: segment, churn_probability, ...)
  * Hive   finsight.customer_clv             (Phase 8)
  * HDFS   /finsight/processed/churn_alerts/ (Phase 5 streaming - OPTIONAL)

Join key: customerId.  Formula:
  composite_risk_score = (fraud_rate_pct * 0.6) + (churn_probability * 0.4)

Output: alteryx/outputs/customer_risk_blend.xlsx  (+ .csv)  - suitable for Power BI.

    python alteryx/fallback/customer_risk_blend.py
    python alteryx/fallback/customer_risk_blend.py --normalize-fraud-pct
"""
from __future__ import annotations

import argparse
import io
import json
import subprocess
import sys
from pathlib import Path

try:
    import pandas as pd
except ModuleNotFoundError:
    sys.exit("pandas missing - `pip install -r requirements.txt`")

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "alteryx" / "fallback"))
from blend_rules import CHURN_WEIGHT, FRAUD_WEIGHT, composite_risk_score  # noqa: E402

try:
    from dotenv import load_dotenv

    load_dotenv(REPO / ".env", override=False)
except ModuleNotFoundError:
    pass

import os  # noqa: E402

HS2 = os.environ.get("HIVE_CONTAINER", "finsight-hiveserver2")
MONGO = os.environ.get("MONGO_CONTAINER", "finsight-mongodb")
SPARK = os.environ.get("SPARK_CONTAINER", "finsight-spark-master")
MONGO_URI = (f"mongodb://{os.environ.get('MONGO_INITDB_ROOT_USERNAME','finsight_admin')}:"
             f"{os.environ.get('MONGO_INITDB_ROOT_PASSWORD','')}@localhost:27017/"
             f"{os.environ.get('MONGO_DB','finsight')}?authSource=admin")
OUT_DIR = REPO / "alteryx" / "outputs"
_NOISE = ("SLF4J", "log4j", "WARNING", "Picked up", "Connecting to", "Connected to",
          "Closing:", "Beeline", "Transaction isolation")


def sh(*cmd, stdin=None, timeout=300):
    return subprocess.run(cmd, capture_output=True, text=True, input=stdin,
                          timeout=timeout, cwd=REPO)


def hive_df(sql: str) -> pd.DataFrame:
    r = sh("docker", "exec", HS2, "beeline", "-u", "jdbc:hive2://localhost:10000/finsight",
           "--silent=true", "--showHeader=true", "--outputformat=csv2", "-e", sql)
    lines = [l for l in (r.stdout).splitlines() if l.strip()
             and not any(n in l for n in _NOISE)]
    if not lines:
        raise SystemExit(f"no rows from Hive:\n{sql}\n{r.stderr[-500:]}")
    df = pd.read_csv(io.StringIO("\n".join(lines)))
    # Hive lowercases result-set column names - restore the join key's camelCase
    return df.rename(columns={"customerid": "customerId"})


def mongo_profiles() -> pd.DataFrame:
    js = ('JSON.stringify(db.customers.find({},'
          '{_id:0,customerId:1,segment:1,churn_probability:1,risk_score:1,'
          'composite_risk_score:1,is_active:1,kyc_status:1}).toArray())')
    r = sh("docker", "exec", MONGO, "mongosh", MONGO_URI, "--quiet", "--eval", js)
    out = next((l for l in r.stdout.splitlines() if l.strip().startswith("[")), None)
    if not out:
        raise SystemExit(f"no docs from Mongo: {r.stdout[-300:]} {r.stderr[-300:]}")
    df = pd.DataFrame(json.loads(out))
    return df.rename(columns={"composite_risk_score": "profile_composite_risk"})


def churn_alerts_df() -> pd.DataFrame:
    """OPTIONAL - Phase 5 streaming output. Empty frame if the path has no data."""
    script = (
        "from pyspark.sql import SparkSession, functions as F\n"
        "s=SparkSession.builder.getOrCreate()\n"
        "try:\n"
        "    df=s.read.parquet('hdfs://namenode:8020/finsight/processed/churn_alerts')\n"
        "    g=df.groupBy('customerId').agg(F.max('signal_count').alias('churn_signal_count'))\n"
        "    print('C_START')\n"
        "    [print(r['customerId']+','+str(r['churn_signal_count'])) for r in g.collect()]\n"
        "    print('C_END')\n"
        "except Exception as e:\n"
        "    print('C_START'); print('C_END')\n"
    )
    (REPO / "scripts" / "_p12.py").write_text(script)
    sh("docker", "cp", str(REPO / "scripts" / "_p12.py"), f"{SPARK}:/tmp/_p12.py")
    r = sh("docker", "exec", SPARK, "/opt/spark/bin/spark-submit", "--master", "local[1]",
           "/tmp/_p12.py", timeout=240)
    (REPO / "scripts" / "_p12.py").unlink(missing_ok=True)
    rows, cap = [], False
    for ln in (r.stdout + r.stderr).splitlines():
        if ln.strip() == "C_START":
            cap = True
        elif ln.strip() == "C_END":
            break
        elif cap and "," in ln:
            cid, n = ln.rsplit(",", 1)
            rows.append({"customerId": cid, "churn_signal_count": int(n)})
    return pd.DataFrame(rows, columns=["customerId", "churn_signal_count"])


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--normalize-fraud-pct", action="store_true",
                    help="divide fraud_rate_pct by 100 before weighting (ASSUMPTIONS I48)")
    ap.add_argument("--no-churn-alerts", action="store_true",
                    help="skip the optional HDFS churn_alerts enrichment")
    args = ap.parse_args()

    print("[blend] reading Hive finsight.customer_fraud_summary ...")
    fraud = hive_df("SELECT customerId, total_transactions, total_amount, "
                    "confirmed_fraud_count, fraud_rate_pct FROM customer_fraud_summary")
    print(f"        {len(fraud)} rows")

    print("[blend] reading MongoDB finsight.customers ...")
    prof = mongo_profiles()
    print(f"        {len(prof)} rows")

    print("[blend] reading Hive finsight.customer_clv ...")
    clv = hive_df("SELECT customerId, clv_score, clv_classification FROM customer_clv")
    print(f"        {len(clv)} rows")

    churn = pd.DataFrame(columns=["customerId", "churn_signal_count"])
    if not args.no_churn_alerts:
        print("[blend] reading HDFS /finsight/processed/churn_alerts/ (optional) ...")
        churn = churn_alerts_df()
        print(f"        {len(churn)} flagged customers")

    # --- Join tool: inner (fraud x profile), left for CLV + churn ---
    df = fraud.merge(prof, on="customerId", how="inner")
    df = df.merge(clv, on="customerId", how="left")
    df = df.merge(churn, on="customerId", how="left")
    df["churn_signal_count"] = (pd.to_numeric(df["churn_signal_count"], errors="coerce")
                                .fillna(0).astype(int))
    df["is_churn_flagged"] = (df["churn_signal_count"] > 0).astype(int)

    # --- Formula tool ---
    df["composite_risk_score"] = [
        round(composite_risk_score(fr, cp, FRAUD_WEIGHT, CHURN_WEIGHT,
                                   normalize_fraud_pct=args.normalize_fraud_pct), 6)
        for fr, cp in zip(df["fraud_rate_pct"], df["churn_probability"])
    ]
    df = df.sort_values("composite_risk_score", ascending=False)

    # --- Select tool: final column order for Power BI ---
    cols = ["customerId", "segment", "total_transactions", "total_amount",
            "confirmed_fraud_count", "fraud_rate_pct", "churn_probability",
            "is_churn_flagged", "churn_signal_count", "clv_score", "clv_classification",
            "risk_score", "profile_composite_risk", "composite_risk_score"]
    df = df[[c for c in cols if c in df.columns]]

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = OUT_DIR / "customer_risk_blend.csv"
    df.to_csv(csv_path, index=False)
    print(f"[blend] wrote {csv_path}  ({len(df)} rows, {len(df.columns)} cols)")
    try:
        xlsx_path = OUT_DIR / "customer_risk_blend.xlsx"
        df.to_excel(xlsx_path, index=False, sheet_name="customer_risk_blend")
        print(f"[blend] wrote {xlsx_path}")
    except ModuleNotFoundError:
        print("[blend] openpyxl not installed - XLSX skipped (CSV is Power BI-ready). "
              "`pip install -r requirements.txt` for the .xlsx.")

    print("\n[blend] top 5 by composite_risk_score:")
    print(df.head(5).to_string(index=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
