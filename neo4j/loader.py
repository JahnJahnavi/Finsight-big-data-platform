#!/usr/bin/env python3
"""
FinSight - Phase 11: load the NovaCrest fraud graph into Neo4j (spec section 11).

Model:  (Account)-[:SENT]->(Transaction)-[:RECEIVED_BY]->(Account)
        Transaction is a first-class node - properties: txnId, step, type,
        amount, isFraud.

Reads the four provided CSVs (neo4j_accounts_nodes / neo4j_transaction_nodes /
neo4j_sent_rels / neo4j_received_rels), applies neo4j/schema.cypher, then loads
nodes and relationships with batched UNWIND ... MERGE (idempotent - safe to
re-run). The datasets are NOT committed to the repo (see .gitignore); point
--csv-dir at wherever they were unpacked.

    # 1. start Neo4j
    docker compose up -d neo4j

    # 2. load  (credentials come from .env - NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD)
    python neo4j/loader.py
    python neo4j/loader.py --csv-dir "Bigdata Data set file/src-data/src-data" --wipe

Exit codes: 0 ok, 2 startup / missing data / referential-integrity failure.
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path

try:
    from neo4j import GraphDatabase
except ModuleNotFoundError:
    sys.exit("neo4j driver missing - `pip install -r requirements.txt` (neo4j==5.24.0)")

REPO = Path(__file__).resolve().parents[1]
_HERE = Path(__file__).resolve().parent

try:
    from dotenv import load_dotenv

    load_dotenv(REPO / ".env", override=False)
except ModuleNotFoundError:
    pass

FILES = {
    "accounts": "neo4j_accounts_nodes.csv",
    "transactions": "neo4j_transaction_nodes.csv",
    "sent": "neo4j_sent_rels.csv",
    "received": "neo4j_received_rels.csv",
}

CSV_DIR_CANDIDATES = [
    str(REPO / "data" / "raw" / "neo4j"),
    str(REPO / "data" / "raw"),
    str(REPO / "Bigdata Data set file" / "src-data" / "src-data"),
    str(REPO / "Bigdata Data set file" / "src-data"),
]


def log(msg: str) -> None:
    print(f"[loader] {msg}", flush=True)


def find_csv_dir(explicit: str | None) -> Path:
    cands = []
    if explicit:
        cands.append(explicit)
    if os.environ.get("NEO4J_CSV_DIR"):
        cands.append(os.environ["NEO4J_CSV_DIR"])
    cands += CSV_DIR_CANDIDATES
    for c in cands:
        p = Path(c)
        if p.is_dir() and all((p / f).is_file() for f in FILES.values()):
            return p
    sys.exit(
        "could not find the Neo4j CSVs (neo4j_accounts_nodes.csv, "
        "neo4j_transaction_nodes.csv, neo4j_sent_rels.csv, neo4j_received_rels.csv).\n"
        "Pass --csv-dir <path> or set NEO4J_CSV_DIR."
    )


def read_csv(path: Path) -> list[dict]:
    with path.open(newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def batches(rows: list, size: int):
    for i in range(0, len(rows), size):
        yield rows[i:i + size]


# --------------------------------------------------------------------------- #
def apply_schema(session) -> int:
    text = (_HERE / "schema.cypher").read_text(encoding="utf-8")
    # drop // comment lines first, THEN split into statements on ';'
    code = "\n".join(l for l in text.splitlines() if not l.strip().startswith("//"))
    stmts = [s.strip() for s in code.split(";") if s.strip()]
    for stmt in stmts:
        session.run(stmt)
    return len(stmts)


def load_accounts(session, rows: list[dict], size: int) -> int:
    q = """
    UNWIND $rows AS row
    MERGE (a:Account {accountId: row.accountId})
      SET a.accountType = row.accountType
    """
    n = 0
    for b in batches(rows, size):
        session.run(q, rows=[{"accountId": r["accountId:ID"],
                              "accountType": r.get("accountType")} for r in b])
        n += len(b)
    return n


def load_transactions(session, rows: list[dict], size: int) -> int:
    q = """
    UNWIND $rows AS row
    MERGE (t:Transaction {txnId: row.txnId})
      SET t.step    = row.step,
          t.type    = row.type,
          t.amount  = row.amount,
          t.isFraud = row.isFraud
    """
    n = 0
    for b in batches(rows, size):
        session.run(q, rows=[{
            "txnId": r["txnId:ID"],
            "step": int(r["step:int"]),
            "type": r.get("type"),
            "amount": float(r["amount:float"]),
            "isFraud": int(r["isFraud:int"]),
        } for r in b])
        n += len(b)
    return n


def load_sent(session, rows: list[dict], size: int) -> tuple[int, int]:
    q = """
    UNWIND $rows AS row
    MATCH (a:Account {accountId: row.start})
    MATCH (t:Transaction {txnId: row.end})
    MERGE (a)-[r:SENT]->(t)
      SET r.amount = row.amount, r.step = row.step, r.transactionType = row.ttype
    RETURN count(r) AS made
    """
    made = 0
    for b in batches(rows, size):
        payload = [{"start": r[":START_ID"], "end": r[":END_ID"],
                    "amount": float(r["amount:float"]), "step": int(r["step:int"]),
                    "ttype": r.get("transactionType")} for r in b]
        made += session.run(q, rows=payload).single()["made"]
    return made, len(rows)


def load_received(session, rows: list[dict], size: int) -> tuple[int, int]:
    q = """
    UNWIND $rows AS row
    MATCH (t:Transaction {txnId: row.start})
    MATCH (a:Account {accountId: row.end})
    MERGE (t)-[r:RECEIVED_BY]->(a)
      SET r.newbalanceDest = row.newbalanceDest, r.isFraud = row.isFraud
    RETURN count(r) AS made
    """
    made = 0
    for b in batches(rows, size):
        payload = [{"start": r[":START_ID"], "end": r[":END_ID"],
                    "newbalanceDest": float(r["newbalanceDest:float"]),
                    "isFraud": int(r["isFraud:int"])} for r in b]
        made += session.run(q, rows=payload).single()["made"]
    return made, len(rows)


# --------------------------------------------------------------------------- #
def run(args: argparse.Namespace) -> int:
    csv_dir = find_csv_dir(args.csv_dir)
    log(f"CSV dir: {csv_dir}")
    data = {k: read_csv(csv_dir / v) for k, v in FILES.items()}
    log(f"rows: accounts={len(data['accounts'])} transactions={len(data['transactions'])} "
        f"sent={len(data['sent'])} received={len(data['received'])}")

    uri = args.uri or os.environ.get("NEO4J_URI", "bolt://localhost:7687")
    user = args.user or os.environ.get("NEO4J_USER", "neo4j")
    pw = args.password or os.environ.get("NEO4J_PASSWORD")
    if not pw:
        sys.exit("no Neo4j password - set NEO4J_PASSWORD in .env or pass --password")

    driver = GraphDatabase.driver(uri, auth=(user, pw))
    try:
        driver.verify_connectivity()
    except Exception as exc:  # noqa: BLE001
        driver.close()
        sys.exit(f"cannot reach Neo4j at {uri}: {exc}")

    db = args.database or os.environ.get("NEO4J_DATABASE", "neo4j")
    with driver.session(database=db) as s:
        if args.wipe:
            log("wiping existing graph (MATCH (n) DETACH DELETE n)")
            s.run("MATCH (n) DETACH DELETE n")

        if not args.no_schema:
            log(f"schema.cypher: {apply_schema(s)} statement(s)")

        n_acc = load_accounts(s, data["accounts"], args.batch_size)
        n_txn = load_transactions(s, data["transactions"], args.batch_size)
        log(f"nodes loaded: {n_acc} Account, {n_txn} Transaction")

        made_sent, want_sent = load_sent(s, data["sent"], args.batch_size)
        made_recv, want_recv = load_received(s, data["received"], args.batch_size)
        log(f"relationships: {made_sent} SENT, {made_recv} RECEIVED_BY")

        # --- referential integrity: every rel row must have matched endpoints ---
        rc = 0
        if made_sent != want_sent:
            log(f"WARNING: {want_sent - made_sent} SENT row(s) had a missing endpoint")
            rc = 2
        if made_recv != want_recv:
            log(f"WARNING: {want_recv - made_recv} RECEIVED_BY row(s) had a missing endpoint")
            rc = 2

        counts = s.run(
            "MATCH (a:Account) WITH count(a) AS accounts "
            "MATCH (t:Transaction) WITH accounts, count(t) AS transactions "
            "OPTIONAL MATCH ()-[se:SENT]->() WITH accounts, transactions, count(se) AS sent "
            "OPTIONAL MATCH ()-[rb:RECEIVED_BY]->() "
            "RETURN accounts, transactions, sent, count(rb) AS received"
        ).single()
        log(f"graph now: {counts['accounts']} Account, {counts['transactions']} Transaction, "
            f"{counts['sent']} SENT, {counts['received']} RECEIVED_BY")

        min_senders = int(os.environ.get("NEO4J_FRAUD_RING_MIN_SENDERS", "3"))
        ring_n = s.run(
            "MATCH (sender:Account)-[:SENT]->(:Transaction)-[:RECEIVED_BY]->(r:Account) "
            "WITH r, count(DISTINCT sender) AS senders "
            "WHERE senders > $m RETURN count(r) AS n", m=min_senders
        ).single()["n"]
        log(f"fraud-ring accounts (> {min_senders} distinct inbound senders): {ring_n}")
    driver.close()
    return rc


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--csv-dir", help="directory holding the four neo4j_*.csv files")
    ap.add_argument("--uri", help="bolt URI (default: NEO4J_URI or bolt://localhost:7687)")
    ap.add_argument("--user", help="default: NEO4J_USER or neo4j")
    ap.add_argument("--password", help="default: NEO4J_PASSWORD")
    ap.add_argument("--database", help="default: NEO4J_DATABASE or neo4j")
    ap.add_argument("--batch-size", type=int, default=1000)
    ap.add_argument("--wipe", action="store_true",
                    help="DETACH DELETE the whole graph before loading")
    ap.add_argument("--no-schema", action="store_true",
                    help="skip applying neo4j/schema.cypher")
    return ap.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    try:
        return run(parse_args(argv))
    except SystemExit as exc:
        if isinstance(exc.code, int):
            return exc.code
        print(f"FATAL: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
