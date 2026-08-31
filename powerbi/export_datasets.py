#!/usr/bin/env python3
"""
FinSight - Phase 13: materialise the Power BI import files into powerbi/exports/.

Power BI Desktop imports flat files (Import mode - the recommended path here;
DirectQuery to Hive is documented as an alternative in docs/powerbi/). This
script builds every dataset the report's data model needs, pulling from the live
stack via `docker exec` (no Hive/Mongo Python driver needed).

    python powerbi/export_datasets.py            # build everything it can
    python powerbi/export_datasets.py --list     # just show source readiness

Always built (pure Python):  dim_date, dim_transaction_type
Built from Mongo + Hive:     dim_customer, customer_products
Copied if upstream present:  compliance_summary, dormancy_report, daily_summary,
                             transaction_summary, streaming_metrics
Live-updated by the bridge:  flagged_transactions  (powerbi/kafka_bridge/)

Missing sources are reported with the command that produces them - nothing is
faked. Not a Power BI artifact.
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import subprocess
import sys
from pathlib import Path

try:
    import pandas as pd
except ModuleNotFoundError:
    sys.exit("pandas missing - `pip install -r requirements.txt`")

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "powerbi"))
from model_helpers import TXN_TYPES, build_date_dim  # noqa: E402

try:
    from dotenv import load_dotenv

    load_dotenv(REPO / ".env", override=False)
except ModuleNotFoundError:
    pass

OUT = REPO / "powerbi" / "exports"
HS2 = os.environ.get("HIVE_CONTAINER", "finsight-hiveserver2")
MONGO = os.environ.get("MONGO_CONTAINER", "finsight-mongodb")
NN = os.environ.get("NAMENODE_CONTAINER", "finsight-namenode")
MONGO_URI = (f"mongodb://{os.environ.get('MONGO_INITDB_ROOT_USERNAME', 'finsight_admin')}:"
             f"{os.environ.get('MONGO_INITDB_ROOT_PASSWORD', '')}@localhost:27017/"
             f"{os.environ.get('MONGO_DB', 'finsight')}?authSource=admin")
MAX_STEP = int(os.environ.get("POWERBI_MAX_STEP", "168"))
_NOISE = ("SLF4J", "log4j", "WARNING", "Picked up", "Connecting to", "Connected to",
          "Closing:", "Beeline", "Transaction isolation", "No such file")


def sh(*cmd, timeout=300):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=REPO)


def hive_df(sql: str) -> "pd.DataFrame | None":
    r = sh("docker", "exec", HS2, "beeline", "-u", "jdbc:hive2://localhost:10000/finsight",
           "--silent=true", "--showHeader=true", "--outputformat=csv2", "-e", sql)
    lines = [l for l in r.stdout.splitlines()
             if l.strip() and not any(n in l for n in _NOISE)]
    if len(lines) < 2:
        return None
    return pd.read_csv(io.StringIO("\n".join(lines))).rename(columns={"customerid": "customerId"})


def mongo_json(js: str):
    r = sh("docker", "exec", MONGO, "mongosh", MONGO_URI, "--quiet", "--eval", js)
    out = next((l for l in r.stdout.splitlines() if l.strip().startswith(("[", "{"))), None)
    return json.loads(out) if out else None


def hdfs_getmerge(hdfs_path: str) -> "str | None":
    r = sh("docker", "exec", NN, "bash", "-lc",
           f"hdfs dfs -test -e {hdfs_path} && hdfs dfs -getmerge -nl {hdfs_path} /tmp/_pbi_m 2>/dev/null "
           f"&& cat /tmp/_pbi_m && rm -f /tmp/_pbi_m")
    return r.stdout if r.returncode == 0 and r.stdout.strip() else None


class Report:
    def __init__(self): self.rows = []
    def ok(self, name, detail): self.rows.append(("OK  ", name, detail)); print(f"  [OK  ] {name} - {detail}")
    def skip(self, name, how): self.rows.append(("SKIP", name, how)); print(f"  [SKIP] {name} - run: {how}")


def build_dim_date(rep: Report):
    dim = build_date_dim(MAX_STEP)
    pd.DataFrame(dim).to_csv(OUT / "dim_date.csv", index=False)
    rep.ok("dim_date.csv", f"{len(dim)} rows (steps 1..{MAX_STEP}, "
           f"{dim[0]['date']}..{dim[-1]['date']})")


def build_dim_txn_type(rep: Report):
    rows = [{"transaction_type": t, "type_group":
             "outflow" if t in ("TRANSFER", "CASH_OUT", "PAYMENT", "DEBIT") else "inflow"}
            for t in TXN_TYPES]
    pd.DataFrame(rows).to_csv(OUT / "dim_transaction_type.csv", index=False)
    rep.ok("dim_transaction_type.csv", f"{len(rows)} types")


def build_dim_customer(rep: Report):
    docs = mongo_json(
        'JSON.stringify(db.customers.find({},{_id:0,customerId:1,segment:1,country:1,'
        'age:1,annualIncome:1,productCount:1,kyc_status:1,preferred_channel:1,'
        'email_verified:1,is_active:1,risk_score:1,churn_probability:1,'
        'composite_risk_score:1,account_opened:1,last_transaction_date:1}).toArray())')
    if not docs:
        rep.skip("dim_customer.csv", "mongodb/import_customers.sh (Phase 10)")
        return
    prof = pd.DataFrame(docs).rename(columns={"composite_risk_score": "profile_composite_risk"})

    clv = hive_df("SELECT customerId, clv_score, clv_classification FROM customer_clv")
    if clv is not None:
        prof = prof.merge(clv, on="customerId", how="left")
    else:
        rep.skip("(dim_customer) customer_clv", "spark/batch/clv_scoring.py + Phase 8 DDL")

    risk = hive_df("SELECT customerId, risk_score AS model_risk_score, risk_tier "
                   "FROM risk_scores") if _hive_table_exists("risk_scores") else None
    if risk is not None:
        prof = prof.merge(risk, on="customerId", how="left")
    else:
        rep.skip("(dim_customer) risk_scores", "spark/batch/risk_scoring.py + external DDL")

    prof.to_csv(OUT / "dim_customer.csv", index=False)
    rep.ok("dim_customer.csv", f"{len(prof)} customers, {len(prof.columns)} cols")

    # exploded product holdings
    pdocs = mongo_json('JSON.stringify(db.customers.find({},{_id:0,customerId:1,products:1}).toArray())')
    pairs = [{"customerId": d["customerId"], "product": p}
             for d in (pdocs or []) for p in (d.get("products") or [])]
    pd.DataFrame(pairs, columns=["customerId", "product"]).to_csv(
        OUT / "customer_products.csv", index=False)
    rep.ok("customer_products.csv", f"{len(pairs)} customer-product rows")


def _hive_table_exists(name: str) -> bool:
    r = sh("docker", "exec", HS2, "beeline", "-u", "jdbc:hive2://localhost:10000/finsight",
           "--silent=true", "--outputformat=tsv2", "-e", f"SHOW TABLES LIKE '{name}';")
    return name in r.stdout


def copy_export(rep: Report, name: str, hdfs_candidates: list[str],
                local_candidates: list[Path], produced_by: str):
    for lc in local_candidates:
        if lc.is_file() and lc.stat().st_size > 0:
            pd.read_csv(lc).to_csv(OUT / f"{name}.csv", index=False)
            rep.ok(f"{name}.csv", f"from {lc.relative_to(REPO)}")
            return
    for hc in hdfs_candidates:
        blob = hdfs_getmerge(hc)
        if blob:
            (OUT / f"{name}.csv").write_text(blob, encoding="utf-8")
            rep.ok(f"{name}.csv", f"from hdfs://{hc}")
            return
    rep.skip(f"{name}.csv", produced_by)


def streaming_metrics(rep: Report):
    blob = hdfs_getmerge("/finsight/processed/streaming_metrics")
    if not blob:
        rep.skip("streaming_metrics.csv", "spark/streaming/run_fraud_detection.sh")
        return
    recs = [json.loads(l) for l in blob.splitlines() if l.strip().startswith("{")]
    pd.DataFrame(recs).to_csv(OUT / "streaming_metrics.csv", index=False)
    rep.ok("streaming_metrics.csv", f"{len(recs)} micro-batch rows")


def _probe(rep: Report) -> None:
    rep.ok("dim_date.csv", "generated (always)")
    rep.ok("dim_transaction_type.csv", "generated (always)")
    docs = mongo_json('JSON.stringify({n: db.customers.estimatedDocumentCount()})')
    n = (docs or {}).get("n", 0)
    (rep.ok if n else rep.skip)("dim_customer.csv / customer_products.csv",
        f"MongoDB customers = {n}" if n else "mongodb/import_customers.sh")
    for name, probe, how in [
        ("compliance_summary.csv", "/finsight/exports/compliance_summary.csv",
         "sql/run_spark_sql.sh --mode compliance"),
        ("dormancy_report.csv", "/finsight/exports/dormancy_report.csv",
         "sql/run_spark_sql.sh --mode dormancy"),
        ("daily_summary.csv", "/finsight/exports/daily_summary",
         "spark/batch/run_risk_scoring.sh"),
        ("streaming_metrics.csv", "/finsight/processed/streaming_metrics",
         "spark/streaming/run_fraud_detection.sh"),
    ]:
        ex = sh("docker", "exec", NN, "hdfs", "dfs", "-test", "-e", probe).returncode == 0
        (rep.ok if ex else rep.skip)(name, f"hdfs://{probe}" if ex else how)
    ts = (REPO / "alteryx" / "outputs" / "transaction_summary.csv")
    (rep.ok if ts.is_file() else rep.skip)("transaction_summary.csv",
        "alteryx/outputs/" if ts.is_file() else "alteryx/fallback/transaction_summary.py")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--list", action="store_true",
                    help="probe source readiness only - write nothing")
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    rep = Report()
    mode = "readiness probe" if args.list else "export"
    print(f"\n  FinSight Phase 13 - Power BI dataset {mode} -> {OUT.relative_to(REPO)}/\n")

    if args.list:
        _probe(rep)
    else:
        build_dim_date(rep)
        build_dim_txn_type(rep)
        build_dim_customer(rep)
        copy_export(rep, "compliance_summary",
                    ["/finsight/exports/compliance_summary.csv"], [],
                    "sql/run_spark_sql.sh --mode compliance")
        copy_export(rep, "dormancy_report",
                    ["/finsight/exports/dormancy_report.csv"], [],
                    "sql/run_spark_sql.sh --mode dormancy")
        copy_export(rep, "daily_summary",
                    ["/finsight/exports/daily_summary"], [],
                    "spark/batch/run_risk_scoring.sh")
        copy_export(rep, "transaction_summary", [],
                    [REPO / "alteryx" / "outputs" / "transaction_summary.csv"],
                    "python alteryx/fallback/transaction_summary.py (Phase 12)")
        streaming_metrics(rep)

    fl = OUT / "flagged_transactions.csv"
    (rep.ok if fl.exists() else rep.skip)(
        "flagged_transactions.csv",
        f"{sum(1 for _ in fl.open()) - 1} rows" if fl.exists()
        else "python powerbi/kafka_bridge/txn_flagged_bridge.py")

    built = sum(1 for s, *_ in rep.rows if s == "OK  ")
    print(f"\n  {built}/{len(rep.rows)} datasets ready in {OUT.relative_to(REPO)}/")
    print("  see docs/powerbi/data-model.md for how they wire together\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
