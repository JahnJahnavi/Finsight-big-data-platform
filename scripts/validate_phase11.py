#!/usr/bin/env python3
"""
FinSight - Phase 11 validation: Neo4j fraud graph (spec section 11).

Checks the loaded graph against the source CSVs:
  * Account nodes          - count matches, accountId + accountType populated
  * Transaction nodes       - count matches, first-class, step/amount/isFraud set
  * SENT relationships      - count matches, shape (Account)-[:SENT]->(Transaction)
  * RECEIVED_BY relationships - count matches, (Transaction)-[:RECEIVED_BY]->(Account)
  * the model               - (Account)-[:SENT]->(Transaction)-[:RECEIVED_BY]->(Account)
  * fraud_ring.cypher       - returns accounts with > 3 distinct inbound senders,
                              cross-checked against an in-memory recomputation

    python scripts/validate_phase11.py          # validate current graph
    python scripts/validate_phase11.py --load   # run neo4j/loader.py --wipe first

Exit 0 only if every check passes.
"""
from __future__ import annotations

import csv
import os
import subprocess
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "neo4j"))

try:
    from neo4j import GraphDatabase
except ModuleNotFoundError:
    sys.exit("neo4j driver missing - `pip install -r requirements.txt`")
from graph_rules import fraud_ring_accounts  # noqa: E402
from loader import FILES, find_csv_dir  # noqa: E402

try:
    from dotenv import load_dotenv

    load_dotenv(REPO / ".env", override=False)
except ModuleNotFoundError:
    pass

URI = os.environ.get("NEO4J_URI", "bolt://localhost:7687")
USER = os.environ.get("NEO4J_USER", "neo4j")
PW = os.environ.get("NEO4J_PASSWORD", "")
DB = os.environ.get("NEO4J_DATABASE", "neo4j")
MIN_SENDERS = int(os.environ.get("NEO4J_FRAUD_RING_MIN_SENDERS", "3"))
FRAUD_RING_CYPHER = (REPO / "neo4j" / "fraud_ring.cypher").read_text(encoding="utf-8")


class Check:
    def __init__(self): self.rows = []
    def add(self, name, ok, detail=""):
        self.rows.append(bool(ok))
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}" + (f"  - {detail}" if detail else ""))
        return ok
    def report(self):
        ok = sum(self.rows)
        print("\n  " + "=" * 62)
        print(f"  Phase 11 validation: {ok}/{len(self.rows)} checks passed")
        print("  " + "=" * 62)
        return 0 if ok == len(self.rows) else 1


def read_csv(p: Path) -> list[dict]:
    with p.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def main() -> int:
    c = Check()
    print("\n  FinSight Phase 11 - Neo4j fraud graph\n  " + "-" * 58)

    csv_dir = find_csv_dir(None)
    src = {k: read_csv(csv_dir / v) for k, v in FILES.items()}
    exp_acc, exp_txn = len(src["accounts"]), len(src["transactions"])
    exp_sent, exp_recv = len(src["sent"]), len(src["received"])
    print(f"  source CSVs: {exp_acc} accounts, {exp_txn} txns, {exp_sent} SENT, {exp_recv} RECEIVED_BY")

    if "--load" in sys.argv:
        print("  running neo4j/loader.py --wipe ...")
        r = subprocess.run([sys.executable, str(REPO / "neo4j" / "loader.py"), "--wipe"],
                           capture_output=True, text=True, cwd=REPO, timeout=600)
        print("   ", (r.stdout + r.stderr).strip().splitlines()[-1] if (r.stdout + r.stderr).strip() else "")

    if not PW:
        sys.exit("no NEO4J_PASSWORD - set it in .env")
    driver = GraphDatabase.driver(URI, auth=(USER, PW))
    try:
        driver.verify_connectivity()
    except Exception as exc:  # noqa: BLE001
        driver.close()
        sys.exit(f"cannot reach Neo4j at {URI}: {exc}")

    with driver.session(database=DB) as s:
        def one(q, **p):
            return s.run(q, **p).single()

        # --- 1. Account nodes ---
        a_total = one("MATCH (a:Account) RETURN count(a) AS n")["n"]
        a_bad = one("MATCH (a:Account) WHERE a.accountId IS NULL OR a.accountType IS NULL "
                    "RETURN count(a) AS n")["n"]
        c.add("1. Account nodes loaded", a_total == exp_acc and a_bad == 0,
              f"{a_total}/{exp_acc}, {a_bad} missing accountId/accountType")

        # --- 2. Transaction nodes (first-class, required props) ---
        t_total = one("MATCH (t:Transaction) RETURN count(t) AS n")["n"]
        t_bad = one("MATCH (t:Transaction) "
                    "WHERE t.txnId IS NULL OR t.amount IS NULL OR t.isFraud IS NULL "
                    "OR t.step IS NULL RETURN count(t) AS n")["n"]
        c.add("2. Transaction nodes loaded (amount, isFraud, step populated)",
              t_total == exp_txn and t_bad == 0,
              f"{t_total}/{exp_txn}, {t_bad} missing a required property")

        # --- 3. SENT relationships ---
        sent_total = one("MATCH ()-[r:SENT]->() RETURN count(r) AS n")["n"]
        sent_shape = one("MATCH (x)-[r:SENT]->(y) "
                         "WHERE NOT x:Account OR NOT y:Transaction RETURN count(r) AS n")["n"]
        c.add("3. SENT relationships: (Account)-[:SENT]->(Transaction)",
              sent_total == exp_sent and sent_shape == 0,
              f"{sent_total}/{exp_sent}, {sent_shape} wrong-shape")

        # --- 4. RECEIVED_BY relationships ---
        recv_total = one("MATCH ()-[r:RECEIVED_BY]->() RETURN count(r) AS n")["n"]
        recv_shape = one("MATCH (x)-[r:RECEIVED_BY]->(y) "
                         "WHERE NOT x:Transaction OR NOT y:Account RETURN count(r) AS n")["n"]
        c.add("4. RECEIVED_BY relationships: (Transaction)-[:RECEIVED_BY]->(Account)",
              recv_total == exp_recv and recv_shape == 0,
              f"{recv_total}/{exp_recv}, {recv_shape} wrong-shape")

        # --- 5. the required path model resolves end to end ---
        path_txns = one(
            "MATCH (:Account)-[:SENT]->(t:Transaction)-[:RECEIVED_BY]->(:Account) "
            "RETURN count(DISTINCT t) AS n")["n"]
        c.add("5. model (Account)-[:SENT]->(Transaction)-[:RECEIVED_BY]->(Account)",
              path_txns == exp_txn, f"{path_txns}/{exp_txn} transactions on a full path")

        # --- 6. uniqueness constraints present ---
        cons = {r["name"] for r in s.run("SHOW CONSTRAINTS YIELD name")}
        c.add("6. uniqueness constraints on accountId + txnId",
              {"account_id", "transaction_id"} <= cons, f"{sorted(cons)}")

        # --- 7. fraud_ring.cypher ---
        rows = list(s.run(FRAUD_RING_CYPHER, minSenders=MIN_SENDERS))
        ring_graph = {r["account"]: r["distinct_senders"] for r in rows}
        all_over = all(v > MIN_SENDERS for v in ring_graph.values())

        sent_edges = [(r[":START_ID"], r[":END_ID"]) for r in src["sent"]]
        recv_edges = [(r[":START_ID"], r[":END_ID"]) for r in src["received"]]
        ring_expected = fraud_ring_accounts(sent_edges, recv_edges, MIN_SENDERS)

        c.add(f"7. fraud_ring.cypher: accounts with > {MIN_SENDERS} distinct inbound senders",
              len(rows) > 0 and all_over and ring_graph == ring_expected,
              f"{len(rows)} accounts (expected {len(ring_expected)}); "
              f"top: {rows[0]['account']}={rows[0]['distinct_senders']}" if rows else "none")

    driver.close()
    return c.report()


if __name__ == "__main__":
    sys.exit(main())
