#!/usr/bin/env python3
"""
FinSight - Phase 9 validation: Spark SQL analytics (spec 7.5 / 7.6).

Generates a transactions CSV engineered for all three modes, runs each from the
single entry point (sql/spark_sql_jobs.py --mode ...), and checks the outputs.

    python scripts/validate_phase9.py

Exit 0 only if every check passes.
"""
from __future__ import annotations

import csv
import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
SPARK = "finsight-spark-master"
NN = "finsight-namenode"
SAMPLE_CSV = REPO / "data" / "sample" / "sql_txns.csv"
SAMPLE_IN_CONTAINER = "/opt/finsight/data/sample/sql_txns.csv"

COMPLIANCE_OUT = "/finsight/processed/compliance_summary"
CUSTSUM_OUT = "/finsight/processed/customer_fraud_summary"
DORMANCY_OUT = "/finsight/processed/dormancy_report"
DORMANCY_CSV = "/finsight/exports/dormancy_report.csv"
COMPLIANCE_CSV = "/finsight/exports/compliance_summary.csv"

CSV_COLS = ["step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
            "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud"]

# (nameOrig, [(step, type, amount, isFraud), ...])
ROWS: list[tuple] = []


def _cust(cid, txns):
    for step, ttype, amount, fr in txns:
        ROWS.append((step, ttype, amount, cid, fr))


def _spread(cid, ttype, amount, count, n_fraud, step_lo=1, step_hi=168):
    """`count` txns of one type spread evenly across [step_lo, step_hi] so the
    customer stays *active* (last step == step_hi); first `n_fraud` are fraud."""
    for i in range(count):
        step = step_hi if count == 1 else step_lo + round(i * (step_hi - step_lo) / (count - 1))
        ROWS.append((step, ttype, amount, cid, 1 if i < n_fraud else 0))


# --- compliance: fraud rate per type -> risk classification --------------
# spread across all 168 steps so these do NOT also register as dormant
_spread("C-FILL1", "CASH_IN", 5000.0, 20, 0)     # 0%  -> Low
_spread("C-FILL2", "PAYMENT", 3000.0, 50, 1)     # 2%  -> Medium
_spread("C-FILL3", "CASH_OUT", 9000.0, 20, 2)    # 10% -> High
_spread("C-FILL4", "TRANSFER", 250000.0, 10, 3)  # 30% -> High
_spread("C-FILL5", "DEBIT", 1500.0, 4, 3)        # 75% -> High

# --- dormancy fixtures (max step in the data = 168, from C-LATE) ---------
_cust("C-LATE",    [(168, "PAYMENT", 100.0, 0)])                                  # sets max step
_cust("C-ACTIVE",  [(s, "PAYMENT", 1000.0, 0) for s in (150, 155, 160, 163, 165, 168)])  # active
_cust("C-DORMANT", [(s, "PAYMENT", 1000.0, 0) for s in (20, 25, 30, 40, 50, 60)])  # inactive 108 -> Dormant
_cust("C-SEVERE",  [(s, "PAYMENT", 1000.0, 0) for s in (5, 8, 12, 15, 18, 20)])    # inactive 148 -> Severely Dormant
_cust("C-FEW",     [(s, "PAYMENT", 1000.0, 0) for s in (2, 5, 10)])                # only 3 txns -> not dormant
_cust("M-MERCH",   [(s, "PAYMENT", 1000.0, 0) for s in (2, 5, 8, 11, 14, 17)])     # merchant -> excluded

EXPECTED_DORMANCY = {"C-DORMANT": "Dormant", "C-SEVERE": "Severely Dormant"}
EXPECTED_NOT_DORMANT = {"C-ACTIVE", "C-FEW", "M-MERCH", "C-LATE"}
EXPECTED_RISK = {"CASH_IN": "Low", "PAYMENT": "Medium", "CASH_OUT": "High",
                 "TRANSFER": "High", "DEBIT": "High"}


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
        print("\n  " + "=" * 62)
        print(f"  Phase 9 validation: {ok}/{len(self.rows)} checks passed")
        print("  " + "=" * 62)
        return 0 if ok == len(self.rows) else 1


def write_csv() -> int:
    SAMPLE_CSV.parent.mkdir(parents=True, exist_ok=True)
    with SAMPLE_CSV.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_COLS)
        w.writeheader()
        for step, ttype, amount, cid, fr in ROWS:
            w.writerow({"step": step, "type": ttype, "amount": f"{amount:.2f}",
                        "nameOrig": cid, "oldbalanceOrg": "100000.0",
                        "newbalanceOrig": "90000.0", "nameDest": "M0000000001",
                        "oldbalanceDest": "0.0", "newbalanceDest": "0.0",
                        "isFraud": fr, "isFlaggedFraud": "0"})
    return len(ROWS)


def run_mode(mode: str) -> subprocess.CompletedProcess:
    return sh("docker", "exec", SPARK, "/opt/spark/bin/spark-submit",
             "--master", "local[2]", "--conf", "spark.jars.ivy=/opt/spark/.ivy2",
             "/opt/finsight/sql/spark_sql_jobs.py", "--master", "",
             "--mode", mode, "--from", "csv", "--csv", SAMPLE_IN_CONTAINER)


def read_parquet(path: str) -> list[dict]:
    script = (
        "import json\n"
        "from pyspark.sql import SparkSession\n"
        "s=SparkSession.builder.getOrCreate()\n"
        f"df=s.read.parquet('hdfs://namenode:8020{path}')\n"
        "print('R_START')\n"
        "[print(json.dumps(r.asDict(), default=str)) for r in df.collect()]\n"
        "print('R_END')\n"
    )
    (REPO / "scripts" / "_p9.py").write_text(script)
    sh("docker", "cp", str(REPO / "scripts" / "_p9.py"), f"{SPARK}:/tmp/_p9.py")
    r = sh("docker", "exec", SPARK, "/opt/spark/bin/spark-submit", "--master",
           "local[1]", "/tmp/_p9.py", timeout=180)
    (REPO / "scripts" / "_p9.py").unlink(missing_ok=True)
    rows, cap = [], False
    for ln in (r.stdout + r.stderr).splitlines():
        if ln.strip() == "R_START":
            cap = True
        elif ln.strip() == "R_END":
            break
        elif cap and ln.strip().startswith("{"):
            try:
                rows.append(json.loads(ln))
            except json.JSONDecodeError:
                pass
    return rows


def hdfs_test(path: str, flag: str = "-e") -> bool:
    return sh("docker", "exec", NN, "hdfs", "dfs", "-test", flag, path).returncode == 0


def main() -> int:
    c = Check()
    print("\n  FinSight Phase 9 - Spark SQL analytics\n  " + "-" * 58)
    n = write_csv()
    c.add("1. Sample transactions CSV generated", SAMPLE_CSV.exists(), f"{n} txns")

    sh("docker", "exec", NN, "hdfs", "dfs", "-rm", "-r", "-f", "-skipTrash",
       COMPLIANCE_OUT, CUSTSUM_OUT, DORMANCY_OUT, DORMANCY_CSV, COMPLIANCE_CSV)

    # --- one entry point, three modes ---
    results = {}
    for mode in ("compliance", "customer_summary", "dormancy"):
        r = run_mode(mode)
        results[mode] = r
        c.add(f"2.{mode[:4]} spark_sql_jobs.py --mode {mode} completed",
              r.returncode == 0, f"exit={r.returncode}")
        if r.returncode != 0:
            print("      " + "\n      ".join((r.stdout + r.stderr).strip().splitlines()[-4:]))

    # --- compliance ---
    comp = {row["transaction_type"]: row for row in read_parquet(COMPLIANCE_OUT)}
    c.add("3. compliance: one row per transaction type, spec columns",
          set(comp) == set(EXPECTED_RISK) and comp and
          all(k in next(iter(comp.values())) for k in
              ("transaction_count", "transaction_volume", "fraud_count",
               "fraud_rate_pct", "risk_classification")),
          f"types={sorted(comp)}")
    risk_ok = all(comp.get(t, {}).get("risk_classification") == cls
                  for t, cls in EXPECTED_RISK.items())
    c.add("4. compliance: risk classification by fraud rate", risk_ok,
          "; ".join(f"{t}={comp.get(t, {}).get('risk_classification')}"
                    f"({comp.get(t, {}).get('fraud_rate_pct')}%)" for t in EXPECTED_RISK))
    c.add("5. compliance: CSV export written", hdfs_test(COMPLIANCE_CSV), COMPLIANCE_CSV)

    # --- customer_summary ---
    cs = read_parquet(CUSTSUM_OUT)
    cs_by = {r["customerId"]: r for r in cs}
    cols_ok = cs and all(k in cs[0] for k in
                         ("customerId", "total_transactions", "total_amount",
                          "confirmed_fraud_count", "fraud_rate_pct"))
    c.add("6. customer_summary -> /finsight/processed/customer_fraud_summary/",
          bool(cols_ok) and hdfs_test(CUSTSUM_OUT, "-d"),
          f"{len(cs)} customer rows")
    # C-FILL5 = DEBIT, 4 txns, 3 fraud -> 75%
    f5 = cs_by.get("C-FILL5", {})
    c.add("7. customer_summary: per-customer fraud counts correct",
          int(f5.get("total_transactions", 0)) == 4
          and int(f5.get("confirmed_fraud_count", 0)) == 3
          and abs(float(f5.get("fraud_rate_pct", 0)) - 75.0) < 1e-6,
          f"C-FILL5 txns={f5.get('total_transactions')} fraud={f5.get('confirmed_fraud_count')} rate={f5.get('fraud_rate_pct')}")
    c.add("8. customer_summary: merchant (M) accounts excluded",
          "M-MERCH" not in cs_by, f"M-MERCH present: {'M-MERCH' in cs_by}")

    # --- dormancy ---
    dorm = {r["customerId"]: r for r in read_parquet(DORMANCY_OUT)}
    c.add("9. dormancy: exactly the dormant customers with correct severity",
          set(dorm) == set(EXPECTED_DORMANCY)
          and all(dorm[k]["dormancy_severity"] == v for k, v in EXPECTED_DORMANCY.items()),
          "; ".join(f"{k}={v.get('dormancy_severity')}(inactive={v.get('steps_inactive')})"
                    for k, v in sorted(dorm.items())))
    c.add("10. dormancy: non-dormant / merchant / <5-txn accounts excluded",
          all(x not in dorm for x in EXPECTED_NOT_DORMANT),
          f"unexpected: {[x for x in EXPECTED_NOT_DORMANT if x in dorm]}")
    c.add("11. dormancy: single CSV file at /finsight/exports/dormancy_report.csv",
          hdfs_test(DORMANCY_CSV) and not hdfs_test(DORMANCY_CSV, "-d"),
          DORMANCY_CSV)

    return c.report()


if __name__ == "__main__":
    sys.exit(main())
