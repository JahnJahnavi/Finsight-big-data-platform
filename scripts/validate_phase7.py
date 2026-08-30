#!/usr/bin/env python3
"""
FinSight - Phase 7 validation: Spark Core Customer Lifetime Value scoring.

Generates a transactions CSV with customers whose CLV components are engineered,
runs clv_scoring.py, and asserts the classifications, the [0, 1] score range,
and the recency 48-step cut-off (spec 7.4).

    python scripts/validate_phase7.py

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
CLV_OUT = "/finsight/processed/clv_scores"
SAMPLE_CSV = REPO / "data" / "sample" / "clv_txns.csv"
SAMPLE_IN_CONTAINER = "/opt/finsight/data/sample/clv_txns.csv"

CSV_COLS = ["step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
            "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud"]
TYPES5 = ["PAYMENT", "TRANSFER", "CASH_IN", "DEBIT", "CASH_OUT"]

# customer -> list of (step, type, amount)
CUSTOMERS: dict[str, list[tuple]] = {
    # C-HIGH: top spender, most active, all 5 types, active at the last step -> ~1.0
    "C-HIGH": [(1 + i, TYPES5[i % 5], 50_000.0) for i in range(24)] + [(168, "CASH_OUT", 50_000.0)],
    # C-GROWTH: mid on every component -> 0.40-0.70
    "C-GROWTH": [(1 + i * 9, ["PAYMENT", "TRANSFER", "CASH_IN"][i % 3], 30_000.0) for i in range(14)]
                + [(145, "PAYMENT", 30_000.0)],
    # C-ATRISK: tiny spend, 2 txns, 1 type, inactive > 48 steps -> < 0.40
    "C-ATRISK": [(3, "PAYMENT", 1_000.0), (10, "PAYMENT", 1_000.0)],
    # fillers
    "C-LATE": [(168, "PAYMENT", 500.0)],
    "C-F1": [(2 + i * 3, ["PAYMENT", "DEBIT"][i % 2], 15_000.0) for i in range(8)],
}
EXPECTED = {
    "C-HIGH": "High Value",
    "C-GROWTH": "Growth Potential",
    "C-ATRISK": "At Risk",
}


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
        print(f"  Phase 7 validation: {ok}/{len(self.rows)} checks passed")
        print("  " + "=" * 60)
        return 0 if ok == len(self.rows) else 1


def write_csv() -> int:
    SAMPLE_CSV.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with SAMPLE_CSV.open("w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=CSV_COLS)
        w.writeheader()
        for cid, txns in CUSTOMERS.items():
            for step, ttype, amount in txns:
                n += 1
                w.writerow({"step": step, "type": ttype, "amount": f"{amount:.2f}",
                            "nameOrig": cid, "oldbalanceOrg": "100000.0",
                            "newbalanceOrig": "50000.0", "nameDest": "M0000000001",
                            "oldbalanceDest": "0.0", "newbalanceDest": "0.0",
                            "isFraud": "0", "isFlaggedFraud": "0"})
    return n


def run_job() -> subprocess.CompletedProcess:
    return sh("docker", "exec", SPARK, "/opt/spark/bin/spark-submit",
             "--master", "local[2]", "--conf", "spark.jars.ivy=/opt/spark/.ivy2",
             "/opt/finsight/spark/batch/clv_scoring.py", "--master", "",
             "--from", "csv", "--csv", SAMPLE_IN_CONTAINER)


def read_clv() -> list[dict]:
    script = (
        "import json\n"
        "from pyspark.sql import SparkSession\n"
        "s=SparkSession.builder.getOrCreate()\n"
        f"df=s.read.parquet('hdfs://namenode:8020{CLV_OUT}')\n"
        "print('ROWS_START')\n"
        "[print(json.dumps(r.asDict(), default=str)) for r in df.collect()]\n"
        "print('ROWS_END')\n"
    )
    (REPO / "scripts" / "_p7_read.py").write_text(script)
    sh("docker", "cp", str(REPO / "scripts" / "_p7_read.py"), f"{SPARK}:/tmp/_p7_read.py")
    r = sh("docker", "exec", SPARK, "/opt/spark/bin/spark-submit", "--master",
           "local[1]", "/tmp/_p7_read.py", timeout=180)
    (REPO / "scripts" / "_p7_read.py").unlink(missing_ok=True)
    rows, cap = [], False
    for ln in (r.stdout + r.stderr).splitlines():
        if ln.strip() == "ROWS_START":
            cap = True
            continue
        if ln.strip() == "ROWS_END":
            break
        if cap and ln.strip().startswith("{"):
            try:
                rows.append(json.loads(ln))
            except json.JSONDecodeError:
                pass
    return rows


def main() -> int:
    c = Check()
    print("\n  FinSight Phase 7 - Customer Lifetime Value scoring\n  " + "-" * 56)

    n = write_csv()
    c.add("1. Sample transactions CSV generated", SAMPLE_CSV.exists(),
          f"{n} txns, {len(CUSTOMERS)} customers")

    sh("docker", "exec", NN, "hdfs", "dfs", "-rm", "-r", "-f", "-skipTrash", CLV_OUT)
    job = run_job()
    tail = "\n".join((job.stdout + job.stderr).strip().splitlines()[-5:])
    if not c.add("2. clv_scoring.py completed", job.returncode == 0, f"exit={job.returncode}"):
        print("      ---- job tail ----\n      " + tail.replace("\n", "\n      "))
        return c.report()

    rows = read_clv()
    by_id = {r["customerId"]: r for r in rows}
    cols_ok = rows and all(k in rows[0] for k in
                           ("customerId", "clv_score", "clv_classification"))
    c.add("3. clv_scores has customerId / clv_score / clv_classification", bool(cols_ok),
          f"{len(rows)} rows, cols={sorted(rows[0]) if rows else []}")

    in_range = all(0.0 <= float(r["clv_score"]) <= 1.0 for r in rows)
    c.add("4. every clv_score in [0, 1]", in_range,
          f"[{min(float(r['clv_score']) for r in rows):.3f}, "
          f"{max(float(r['clv_score']) for r in rows):.3f}]")

    cls_ok = all(by_id.get(cid, {}).get("clv_classification") == exp
                 for cid, exp in EXPECTED.items())
    c.add("5. classifications correct (High Value / Growth Potential / At Risk)", cls_ok,
          "; ".join(f"{cid}={by_id.get(cid, {}).get('clv_classification')}"
                    f"({float(by_id.get(cid, {}).get('clv_score', -1)):.3f})"
                    for cid in EXPECTED))

    ordered = (float(by_id["C-ATRISK"]["clv_score"])
               < float(by_id["C-GROWTH"]["clv_score"])
               < float(by_id["C-HIGH"]["clv_score"]))
    c.add("6. score ordering At Risk < Growth < High Value", ordered)

    hv = by_id["C-HIGH"]
    weights_ok = abs(
        0.30 * float(hv["volume_score"]) + 0.25 * float(hv["frequency_score"])
        + 0.25 * float(hv["diversity_score"]) + 0.20 * float(hv["recency_score"])
        - float(hv["clv_score"])) < 1e-4
    c.add("7. weighting is 30/25/25/20", weights_ok,
          f"vol={hv['volume_score']} freq={hv['frequency_score']} "
          f"div={hv['diversity_score']} rec={hv['recency_score']} -> {hv['clv_score']}")

    ar = by_id["C-ATRISK"]
    c.add("8. recency = 0 for inactivity > 48 steps",
          float(ar["recency_score"]) == 0.0 and int(ar["steps_since_last_txn"]) >= 48,
          f"C-ATRISK steps_since_last={ar['steps_since_last_txn']} recency={ar['recency_score']}")

    c.add("9. Product Diversity = distinct types / 5",
          abs(float(hv["diversity_score"]) - int(hv["distinct_txn_types"]) / 5) < 1e-9,
          f"C-HIGH types={hv['distinct_txn_types']} -> {hv['diversity_score']}")

    return c.report()


if __name__ == "__main__":
    sys.exit(main())
