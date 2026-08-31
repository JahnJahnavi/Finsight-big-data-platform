# FinSight — Phase 11: Neo4j Fraud Graph

The NovaCrest payment network as a graph, for fraud-ring detection (spec
section 11).

```
(Account)-[:SENT]->(Transaction)-[:RECEIVED_BY]->(Account)
```

`Transaction` is a **first-class node** — properties `txnId`, `step`, `type`,
`amount`, `isFraud`.

| Node / rel | Count (provided CSVs) | Source file |
|---|---|---|
| `(:Account)` | 499 (399 CUSTOMER, 100 MERCHANT) | `neo4j_accounts_nodes.csv` |
| `(:Transaction)` | 1 554 (158 fraud) | `neo4j_transaction_nodes.csv` |
| `[:SENT]` | 1 554 | `neo4j_sent_rels.csv` |
| `[:RECEIVED_BY]` | 1 554 | `neo4j_received_rels.csv` |

The CSVs are **not committed** (git-ignored). **No MongoDB changes** in this phase.

## Files (`neo4j/`)

```
neo4j/
├── loader.py         reads the 4 CSVs, applies schema, batched UNWIND MERGE (idempotent)
├── schema.cypher     uniqueness constraints (accountId, txnId) + secondary indexes
├── fraud_ring.cypher accounts receiving from > 3 distinct inbound senders
└── graph_rules.py    fraud-ring threshold in pure Python (unit-tested; mirrors the Cypher)
```

## Startup + load

```bash
# 1. Neo4j is in the always-on set - start it (or `docker compose up -d`)
docker compose up -d neo4j

# 2. install deps (once) - neo4j==5.24.0 is in requirements.txt
pip install -r requirements.txt

# 3. load the graph  (credentials from .env: NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD)
python neo4j/loader.py
python neo4j/loader.py --csv-dir "Bigdata Data set file/src-data/src-data" --wipe
python neo4j/loader.py --wipe --no-schema        # reload data only
```

`loader.py`:

1. locates the CSVs (`--csv-dir` → `NEO4J_CSV_DIR` → `data/raw/neo4j/` →
   `data/raw/` → `Bigdata Data set file/src-data/…`),
2. `--wipe` → `MATCH (n) DETACH DELETE n`,
3. applies `schema.cypher` (skip with `--no-schema`),
4. loads `Account` then `Transaction` nodes, then `SENT` and `RECEIVED_BY`
   relationships, in `--batch-size` (default 1000) chunks with
   `UNWIND $rows … MERGE` — **idempotent**, safe to re-run,
5. `MATCH`es both endpoints for every relationship row and fails (exit 2) if any
   row has a missing endpoint (referential-integrity guard),
6. prints final node/relationship counts and the fraud-ring account count.

## Schema (`schema.cypher`)

| Object | Definition | Why |
|---|---|---|
| constraint `account_id` | `Account.accountId IS UNIQUE` | node key; fast `MERGE`/`MATCH` |
| constraint `transaction_id` | `Transaction.txnId IS UNIQUE` | node key |
| index `transaction_isFraud` | `Transaction(isFraud)` | fraud filters |
| index `transaction_step` | `Transaction(step)` | time-window queries |
| index `account_type` | `Account(accountType)` | CUSTOMER vs MERCHANT |

All statements use `IF NOT EXISTS` — idempotent.

## Fraud-ring query (`fraud_ring.cypher`)

> "Identify accounts receiving transactions from **more than three** distinct
> inbound senders."

```cypher
MATCH (sender:Account)-[:SENT]->(t:Transaction)-[:RECEIVED_BY]->(receiver:Account)
WITH receiver, count(DISTINCT sender) AS distinct_senders,
     count(t) AS inbound_txns, sum(t.amount) AS inbound_amount,
     sum(t.isFraud) AS confirmed_fraud_txns
WHERE distinct_senders > coalesce($minSenders, 3)
RETURN receiver.accountId AS account, receiver.accountType AS account_type,
       distinct_senders, inbound_txns, round(inbound_amount, 2) AS inbound_amount,
       confirmed_fraud_txns
ORDER BY distinct_senders DESC, inbound_amount DESC;
```

`$minSenders` defaults to 3 (`NEO4J_FRAUD_RING_MIN_SENDERS`); "more than" is a
strict `>` (an account with exactly 3 senders is **not** flagged). On the
provided data: **157 accounts**, top `C2812140441` with 13 distinct senders.

```bash
docker exec finsight-neo4j cypher-shell -u neo4j -p "$NEO4J_PASSWORD" \
  -P "minSenders => 3" -f /finsight/neo4j/fraud_ring.cypher
```

(`./neo4j` is mounted read-only at `/finsight/neo4j` in the `neo4j` service.)

## Validation

```bash
python -m pytest tests/unit/test_fraud_ring.py -q     # 6 rule tests
python scripts/validate_phase11.py                    # 7 graph checks
python scripts/validate_phase11.py --load             # loader.py --wipe, then check
```

`validate_phase11.py` reads the source CSVs and asserts, against the live graph:
Account / Transaction node counts and required properties; `SENT` /
`RECEIVED_BY` counts **and endpoint shapes**; every transaction sits on a full
`(Account)-[:SENT]->(Transaction)-[:RECEIVED_BY]->(Account)` path; the
uniqueness constraints exist; and `fraud_ring.cypher` returns exactly the ring
membership recomputed in memory by `graph_rules.fraud_ring_accounts`.

## Inspect

```bash
docker exec finsight-neo4j cypher-shell -u neo4j -p "$NEO4J_PASSWORD" "
  MATCH (n) RETURN labels(n)[0] AS label, count(*) ORDER BY label;
  MATCH ()-[r]->() RETURN type(r) AS rel, count(*) ORDER BY rel;
  MATCH (s:Account)-[:SENT]->(t:Transaction)-[:RECEIVED_BY]->(r:Account)
  WHERE t.isFraud = 1 RETURN s.accountId, t.amount, r.accountId LIMIT 10;
"
# Neo4j Browser: http://localhost:7474  (bolt://localhost:7687)
```
