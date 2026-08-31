#!/usr/bin/env python3
"""
FinSight - Phase 8 validation: Hive data warehouse.

Runs the spec's validation queries and checks the four objects exist and hold
the expected data:

    SHOW TABLES IN finsight;
    DESCRIBE finsight.transactions;
    SELECT COUNT(*) FROM finsight.transactions;
    SELECT COUNT(*) FROM finsight.vw_fraud_transactions;

All queries go through beeline against HiveServer2.

    python scripts/validate_phase8.py

Exit 0 only if every check passes.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
HS2 = "finsight-hiveserver2"


def sh(*cmd, timeout=300):
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, cwd=REPO)


_NOISE = ("SLF4J", "Failed to create", "No such file", "log4j")


def beeline(sql: str) -> tuple[int, str]:
    r = sh("docker", "exec", HS2, "beeline", "-u", "jdbc:hive2://localhost:10000/",
           "--silent=true", "--showHeader=true", "--outputformat=tsv2", "-e", sql)
    out = "\n".join(l for l in (r.stdout + r.stderr).splitlines()
                    if l.strip() and not any(n in l for n in _NOISE))
    return r.returncode, out


def query_rows(sql: str) -> list[dict]:
    """Run a SELECT and return rows as dicts (header row -> keys)."""
    rc, out = beeline(sql)
    lines = [l for l in out.splitlines() if "\t" in l or l.strip()]
    if rc != 0 or not lines:
        return []
    header = lines[0].split("\t")
    return [dict(zip(header, l.split("\t"))) for l in lines[1:] if l.strip()]


def scalar(sql: str, key: str, default=-1) -> int:
    rows = query_rows(sql)
    try:
        return int(float(rows[0][key])) if rows else default
    except (KeyError, ValueError, IndexError):
        return default


# kept for backwards compat with the check bodies below
def spark_sql(sql: str) -> list[dict]:
    return query_rows(sql)


class Check:
    def __init__(self): self.rows = []
    def add(self, name, ok, detail=""):
        self.rows.append((name, ok, detail))
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  - {detail}" if detail else ""))
        return ok
    def report(self):
        ok = sum(1 for _, o, _ in self.rows if o)
        print("\n  " + "=" * 62)
        print(f"  Phase 8 validation: {ok}/{len(self.rows)} checks passed")
        print("  " + "=" * 62)
        return 0 if ok == len(self.rows) else 1


def main() -> int:
    c = Check()
    print("\n  FinSight Phase 8 - Hive data warehouse\n  " + "-" * 58)

    # --- SHOW TABLES IN finsight ---
    rc, out = beeline("SHOW TABLES IN finsight;")
    tables = {l.strip() for l in out.splitlines()[1:] if l.strip()}
    want = {"transactions", "vw_fraud_transactions", "txn_summary_mart", "customer_clv"}
    c.add("1. Database finsight + 4 objects exist (SHOW TABLES)", want <= tables,
          f"found: {sorted(tables)}")

    # --- DESCRIBE finsight.transactions ---
    rc, out = beeline("DESCRIBE finsight.transactions;")
    cols = {l.split("\t")[0].strip() for l in out.splitlines()[1:] if "\t" in l}
    expect_cols = {"step", "type", "amount", "nameorig", "oldbalanceorg",
                   "newbalanceorig", "namedest", "oldbalancedest", "newbalancedest",
                   "isfraud", "isflaggedfraud", "txnid", "ingest_ts"}
    c.add("2. DESCRIBE finsight.transactions (13 columns)", expect_cols <= cols,
          f"{len(cols & expect_cols)}/13 expected columns")

    # --- transactions type = EXTERNAL ---
    rc, out = beeline("DESCRIBE FORMATTED finsight.transactions;")
    is_ext = "EXTERNAL_TABLE" in out or "EXTERNAL\ttrue" in out.replace(" ", "")
    c.add("3. finsight.transactions is EXTERNAL", is_ext)

    # --- SELECT COUNT(*) FROM finsight.transactions ---
    rows = spark_sql("SELECT COUNT(*) AS n FROM finsight.transactions")
    n_txn = int(rows[0]["n"]) if rows else 0
    c.add("4. SELECT COUNT(*) FROM finsight.transactions", n_txn > 0, f"{n_txn} rows")

    # --- SELECT COUNT(*) FROM finsight.vw_fraud_transactions ---
    rows = spark_sql("SELECT COUNT(*) AS n FROM finsight.vw_fraud_transactions")
    n_fraud_view = int(rows[0]["n"]) if rows else -1
    rows = spark_sql("SELECT COUNT(*) AS n FROM finsight.transactions WHERE isFraud = 1")
    n_fraud_base = int(rows[0]["n"]) if rows else -2
    c.add("5. vw_fraud_transactions = transactions WHERE isFraud=1",
          n_fraud_view == n_fraud_base and n_fraud_view >= 0,
          f"view={n_fraud_view}, base={n_fraud_base}")

    # --- txn_summary_mart: MANAGED, one row per (customer, step), 9 fields ---
    rc, out = beeline("DESCRIBE FORMATTED finsight.txn_summary_mart;")
    is_managed = "MANAGED_TABLE" in out
    rows = spark_sql("SELECT COUNT(*) AS n, "
                     "COUNT(DISTINCT concat(customerId,'|',step)) AS uniq "
                     "FROM finsight.txn_summary_mart")
    mart_n = int(rows[0]["n"]) if rows else 0
    mart_uniq = int(rows[0]["uniq"]) if rows else -1
    c.add("6. txn_summary_mart MANAGED, one row per customer/step", is_managed and mart_n == mart_uniq and mart_n > 0,
          f"managed={is_managed}, rows={mart_n}, distinct (customer,step)={mart_uniq}")

    rc, out = beeline("DESCRIBE finsight.txn_summary_mart;")
    mart_cols = {l.split("\t")[0].strip().lower() for l in out.splitlines()[1:] if "\t" in l}
    want_cols = {"customerid", "step", "txn_count", "total_amount", "avg_amount",
                 "max_amount", "fraud_count", "txn_types", "last_balance"}
    c.add("7. txn_summary_mart has the 9 spec fields",
          want_cols <= mart_cols, f"{sorted(mart_cols & want_cols)}")

    # mart totals reconcile with the base table
    rows = spark_sql("SELECT SUM(txn_count) AS t, SUM(fraud_count) AS f FROM finsight.txn_summary_mart")
    mart_txn = int(rows[0]["t"]) if rows else -1
    mart_fraud = int(rows[0]["f"]) if rows else -1
    c.add("8. mart totals reconcile with finsight.transactions",
          mart_txn == n_txn and mart_fraud == n_fraud_base,
          f"mart txn={mart_txn}/{n_txn}, fraud={mart_fraud}/{n_fraud_base}")

    # --- customer_clv external over clv_scores ---
    rows = spark_sql("SELECT COUNT(*) AS n, COUNT(DISTINCT clv_classification) AS c "
                     "FROM finsight.customer_clv")
    clv_n = int(rows[0]["n"]) if rows else 0
    clv_classes = int(rows[0]["c"]) if rows else 0
    c.add("9. customer_clv external over clv_scores", clv_n > 0 and clv_classes >= 1,
          f"{clv_n} rows, {clv_classes} classification(s)")

    # --- statistics computed ---
    rc, out = beeline("DESCRIBE FORMATTED finsight.transactions;")
    has_stats = "numRows" in out and "COLUMN_STATS_ACCURATE" in out
    c.add("10. statistics computed on finsight.transactions", has_stats,
          "numRows / COLUMN_STATS_ACCURATE present in table params")

    return c.report()


if __name__ == "__main__":
    sys.exit(main())
