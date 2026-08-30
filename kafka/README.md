# FinSight — Phase 2: Kafka Ingestion

Replays the NovaCrest transaction dataset into Kafka as JSON events. This phase
covers **ingestion into `txn-raw` only**. HDFS landing, Spark, and fraud/churn
are later phases.

| File | Purpose |
|------|---------|
| `config.py` | Loads Kafka settings from the repo-root `.env`; shared logging |
| `transaction_schema.py` | The 11-column transaction schema + row→JSON conversion + validation |
| `create_topics.py` | Creates `txn-raw` (3p), `txn-flagged` (1p), `txn-churn` (1p) — idempotent |
| `producer.py` | CSV → JSON → `txn-raw`, rate-limited, graceful shutdown |
| `consumer_test.py` | Reads `txn-raw` back and verifies receipt / JSON / fields |
| `generate_sample_data.py` | Synthetic schema-accurate CSV for testing (real 6.3M-row file is git-ignored) |
| `validate_phase2.py` | Runs the full 7-step acceptance checklist |
| `requirements.txt` | `confluent-kafka`, `python-dotenv` (phase-scoped) |

## Prerequisites

1. Phase 1 infrastructure running:
   ```bash
   docker compose up -d          # or: ./scripts/start.sh
   python scripts/healthcheck.py # kafka must be healthy
   ```
2. Python deps (Python 3.10–3.13):
   ```bash
   pip install -r kafka/requirements.txt
   ```
   > The repo-root `requirements.txt` pins `confluent-kafka==2.5.3`, which has no
   > wheel for Python 3.12/3.13. `kafka/requirements.txt` uses `>=2.5,<3` — on
   > 3.13 that resolves to 2.15.x. (Tracked as a follow-up for `requirements.txt`.)

## Configuration

All settings come from `.env` (see `.env.example`). Relevant keys:

| Key | Default | Meaning |
|-----|---------|---------|
| `KAFKA_BOOTSTRAP_SERVERS_HOST` | `localhost:9092` | broker address from the host |
| `KAFKA_TOPIC_RAW` / `_FLAGGED` / `_CHURN` | `txn-raw` / `txn-flagged` / `txn-churn` | topic names |
| `KAFKA_TOPIC_RAW_PARTITIONS` | `3` | partitions for `txn-raw` |
| `KAFKA_TOPIC_FLAGGED_PARTITIONS` / `_CHURN_PARTITIONS` | `1` / `1` | |
| `KAFKA_TOPIC_REPLICATION` | `1` | single-broker dev cluster |
| `PRODUCER_TARGET_RATE` | `1000` | default messages/sec (spec 6.2) |
| `LOG_LEVEL` | `INFO` | |

Every script also takes CLI flags that override the env defaults
(`--bootstrap-servers`, `--topic`, `--rate`, …).

## Usage

```bash
# 1. create the topics
python kafka/create_topics.py
python kafka/create_topics.py --describe        # check partition counts

# 2. make a small test dataset (200 rows)
python kafka/generate_sample_data.py --rows 200 --out data/sample/transactions_sample.csv

# 3. produce 100 records
python kafka/producer.py --file data/sample/transactions_sample.csv --limit 100

# 4. consume them back and validate
python kafka/consumer_test.py --expect 100

# full replay of the real dataset at ~1000 msg/s
python kafka/producer.py --file data/raw/NovaCrest_Transactions.csv
```

### Producer

```
python kafka/producer.py --file <csv> [options]

  --file PATH              transactions CSV (required)
  --topic NAME             target topic (default: txn-raw)
  --bootstrap-servers S    default: localhost:9092
  --rate N                 target msg/sec; 0 = as fast as possible (default: 1000)
  --limit N                stop after N rows (testing)
  --report-every N         progress log cadence (default: 1000)
  --log-level LEVEL        DEBUG/INFO/WARNING/ERROR
```

- **Message key** = `nameOrig`, so all of one account's transactions land on the
  same partition (needed by the per-customer churn job in Phase 3).
- **Delivery**: `acks=all` + idempotent producer; `lz4` compression.
- **Graceful shutdown**: `SIGINT` / `SIGTERM` / `SIGBREAK` → stop reading, flush
  in-flight messages, print a summary, exit.
- **Exit codes**: `0` all delivered · `1` completed with skipped/failed rows ·
  `2` bad input file / unreachable broker.

### Message format

Each `txn-raw` value is a JSON object: the 11 original CSV columns (typed) plus:

| Field | Source |
|-------|--------|
| `txnId` | derived — `TXN` + 9-digit zero-padded sequence (metadata lists txnId as a derived PK) |
| `ingest_ts` | UTC ISO-8601, set when the producer reads the row |

```json
{"type":"CASH_OUT","nameOrig":"C1958682846","nameDest":"C9963334018","step":1,
 "isFraud":0,"isFlaggedFraud":0,"amount":441.46,"oldbalanceOrg":147294.24,
 "newbalanceOrig":146852.78,"oldbalanceDest":21096.09,"newbalanceDest":21537.55,
 "txnId":"TXN000000001","ingest_ts":"2026-08-30T15:24:48.536685+00:00"}
```

## Validation

```bash
python kafka/validate_phase2.py --records 100
```

Runs and reports: (1) Kafka reachable, (2) topics exist, (3) partition counts
3/1/1, (4) produce 100, (5) consume, (6) validate JSON/fields, (7) confirm
`txn-raw` message count increased by 100.

## Notes / assumptions

- The real `NovaCrest_Transactions.csv` is **not** in the repo (git-ignored). Use
  `generate_sample_data.py` for testing; drop the real file at
  `data/raw/NovaCrest_Transactions.csv` for a full run.
- `generate_sample_data.py` output is **synthetic and only approximately
  realistic** — for pipeline testing, not analytics.
- `txn-flagged` and `txn-churn` are created here but written to by the Spark
  Structured Streaming jobs in Phase 3, not by this producer.
- `SIM_EPOCH`-based `event_ts` is **not** added yet — deferred to Phase 3 where
  the streaming windows need it (see `docs/ASSUMPTIONS.md` I11).
