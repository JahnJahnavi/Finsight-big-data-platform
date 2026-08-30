#!/usr/bin/env python3
"""
FinSight - Phase 6 validation: Spark Core customer risk scoring.

Generates a small transactions CSV with customers whose risk factors are known,
runs risk_scoring.py against it, and asserts the risk tiers, the [0, 1] score
range, and the daily-summary aggregates (spec 7.3 + 7.3 R2).

    python scripts/validate_phase6.py

Exit 0 only if every check passes.
"""
from __future__ import annotations

import argparse
import csv
import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SPARK = "finsight-spark-master"
NN = "finsight-namenode"
RISK_OUT = "/finsight/processed/risk_scores"
SUMMARY_OUT = "/finsight/processed/daily_summary"
CSV_EXPORT = "/finsight/exports/daily_summary"
SAMPLE_CSV = REPO / "data" / "sample" / "risk_txns.csv"
SAMPLE_IN_CONTAINER = "/opt/finsight/data/sample/risk_txns.csv"

CSV_COLS = ["step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
            "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud"]

# customer -> list of (step, type, amount, dest_id, isFraud)
# customerId must start with 'C' (spec: prefix C = customer account).
CUSTOMERS: dict[str, list[tuple]] = {
    # C-HIGH: max frequency / transfer amount / cash-out prop / unique dests
    "C-HIGH": (
        [(s, "CASH_OUT", 40_000.0, f"D{s:03d}", 1 if s in (5, 6) else 0) for s in range(1, 16)]
        + [(s, "TRANSFER", 800_000.0, f"D{s:03d}", 0) for s in range(16, 21)]
    ),
    # C-MID: middle of every factor
    "C-MID": (
        [(s, "CASH_OUT", 9_000.0, f"M{s % 5:02d}", 0) for s in range(1, 5)]
        + [(s, "PAYMENT", 3_000.0, f"M{s % 5:02d}", 0) for s in range(5, 9)]
        + [(s, "TRANSFER", 200_000.0, f"M{s % 5:02d}", 0) for s in range(9, 11)]
    ),
    # C-LOW: bottom of every factor
    "C-LOW": [(3, "PAYMENT", 500.0, "L01", 0)],
    # fillers so the min/max are well-defined and not all at the extremes
    "C-F1": [(s, "CASH_OUT", 12_000.0, f"F1{s}", 0) for s in range(1, 4)]
            + [(s, "TRANSFER", 300_000.0, f"F1{s}", 0) for s in range(4, 7)],
    "C-F2": [(s, "PAYMENT", 2_000.0, f"F2{s}", 0) for s in range(1, 5)]
            + [(5, "TRANSFER", 50_000.0, "F25", 0)],
    "C-F3": [(s, "CASH_IN", 5_000.0, f"F3{s}", 0) for s in range(1, 4)],
}
EXPECTED_TIER = {"C-HIGH": "High", "C-MID": "Medium", "C-LOW": "Low"}


def sh(*cmd, timeout=420):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=REPO)


class Check:
    def __init__(self): self.rows = []
    def add(self, name, ok, detail=""):
        self.rows.append((name, ok, detail))
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  - {detail}" if detail else ""))
        return ok
    def report(self):
        ok = sum(1 for _, o, _ in self.rows if o)
        print("\n  " + "=" * 60)
        print(f"  Phase 6 validation: {ok}/{len(self.rows)} checks passed")
        print("  " + "=" * 60)
        return 0 if ok == len(self.rows) else 1


def write_sample_csv() -> tuple[int, int]:
    SAMPLE_CSV.parent.mkdir(parents=True, exist_ok=True)
    rows, fraud = 0, 0
    with SAMPLE_CSV.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_COLS)
        w.writeheader()
        for cid, txns in CUSTOMERS.items():
            for step, ttype, amount, dest, is_fraud in txns:
                fraud += is_fraud
                rows += 1
                w.writerow({
                    "step": step, "type": ttype, "amount": f"{amount:.2f}",
                    "nameOrig": cid,
                    "oldbalanceOrg": "100000.0", "newbalanceOrig": "50000.0",
                    "nameDest": dest, "oldbalanceDest": "0.0", "newbalanceDest": "0.0",
                    "isFraud": is_fraud, "isFlaggedFraud": "0"})
    return rows, fraud


def run_job() -> subprocess.CompletedProcess:
    return sh("docker", "exec", SPARK, "/opt/spark/bin/spark-submit",
             "--master", "local[2]", "--conf", "spark.jars.ivy=/opt/spark/.ivy2",
             "/opt/finsight/spark/batch/risk_scoring.py", "--master", "",
             "--from", "csv", "--csv", SAMPLE_IN_CONTAINER)


def spark_read_json(path_expr: str) -> list[dict]:
    script = (
        "import json\n"
        "from pyspark.sql import SparkSession\n"
        "s=SparkSession.builder.getOrCreate()\n"
        f"df=s.read.parquet('hdfs://namenode:8020{path_expr}')\n"
        "print('ROWS_JSON_START')\n"
        "[print(json.dumps(r.asDict(), default=str)) for r in df.collect()]\n"
        "print('ROWS_JSON_END')\n"
    )
    (REPO / "scripts" / "_p6_read.py").write_text(script)
    sh("docker", "cp", str(REPO / "scripts" / "_p6_read.py"), f"{SPARK}:/tmp/_p6_read.py")
    r = sh("docker", "exec", SPARK, "/opt/spark/bin/spark-submit", "--master",
           "local[1]", "/tmp/_p6_read.py", timeout=180)
    (REPO / "scripts" / "_p6_read.py").unlink(missing_ok=True)
    out, capture, rows = r.stdout + r.stderr, False, []
    for ln in out.splitlines():
        if ln.strip() == "ROWS_JSON_START":
            capture = True
            continue
        if ln.strip() == "ROWS_JSON_END":
            break
        if capture and ln.strip().startswith("{"):
            try:
                rows.append(json.loads(ln))
            except json.JSONDecodeError:
                pass
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.parse_args()
    c = Check()
    print("\n  FinSight Phase 6 - customer risk scoring\n  " + "-" * 56)

    n_rows, n_fraud = write_sample_csv()
    c.add("1. Sample transactions CSV generated", SAMPLE_CSV.exists(),
          f"{n_rows} txns, {n_fraud} fraud, {len(CUSTOMERS)} customers")

    sh("docker", "exec", NN, "hdfs", "dfs", "-rm", "-r", "-f", "-skipTrash",
       RISK_OUT, SUMMARY_OUT, CSV_EXPORT)
    job = run_job()
    tail = "\n".join((job.stdout + job.stderr).strip().splitlines()[-5:])
    if not c.add("2. risk_scoring.py completed", job.returncode == 0, f"exit={job.returncode}"):
        print("      ---- job tail ----\n      " + tail.replace("\n", "\n      "))
        return c.report()

    risk = spark_read_json(RISK_OUT)
    by_id = {r["customerId"]: r for r in risk}
    cols_ok = risk and all(k in risk[0] for k in ("customerId", "risk_score", "risk_tier"))
    c.add("3. risk_scores has customerId / risk_score / risk_tier", bool(cols_ok),
          f"{len(risk)} customer rows, columns={sorted(risk[0]) if risk else []}")

    in_range = all(0.0 <= float(r["risk_score"]) <= 1.0 for r in risk)
    c.add("4. every risk_score in [0, 1]", in_range,
          f"range [{min(float(r['risk_score']) for r in risk):.3f}, "
          f"{max(float(r['risk_score']) for r in risk):.3f}]")

    tiers_ok = all(by_id.get(cid, {}).get("risk_tier") == exp
                   for cid, exp in EXPECTED_TIER.items())
    c.add("5. risk tiers correct (Low / Medium / High)", tiers_ok,
          "; ".join(f"{cid}={by_id.get(cid, {}).get('risk_tier')}"
                    f"({float(by_id.get(cid, {}).get('risk_score', -1)):.3f})"
                    for cid in EXPECTED_TIER))

    ordered = (float(by_id["C-LOW"]["risk_score"])
               < float(by_id["C-MID"]["risk_score"])
               < float(by_id["C-HIGH"]["risk_score"]))
    c.add("6. score ordering LOW < MID < HIGH", ordered)

    summary = spark_read_json(SUMMARY_OUT)
    sum_cols = summary and all(
        k in summary[0] for k in ("type", "step", "transaction_volume",
                                  "total_amount", "fraud_count"))
    c.add("7. daily_summary grouped by type AND step", bool(sum_cols),
          f"{len(summary)} type/step rows")

    total_vol = sum(int(r["transaction_volume"]) for r in summary)
    total_fraud = sum(int(r["fraud_count"]) for r in summary)
    c.add("8. daily_summary totals match input", total_vol == n_rows and total_fraud == n_fraud,
          f"volume {total_vol}/{n_rows}, fraud {total_fraud}/{n_fraud}")

    csv_ok = sh("docker", "exec", NN, "hdfs", "dfs", "-test", "-d", CSV_EXPORT).returncode == 0
    c.add("9. daily_summary CSV export written (for Alteryx)", csv_ok, CSV_EXPORT)

    return c.report()


if __name__ == "__main__":
    sys.exit(main())
