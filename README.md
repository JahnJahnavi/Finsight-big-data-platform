# FinSight — NovaCrest Bank Big-Data Analytics Platform

End-to-end big-data platform for **NovaCrest Bank**: real-time fraud & churn
detection, batch risk / CLV scoring, a Hive warehouse, a Neo4j fraud graph, and
executive Power BI dashboards.

> **Source of truth:** `FinSight_Full_Specification_Complete.pdf`.
> Everything not defined by the spec is tracked in [`docs/ASSUMPTIONS.md`](docs/ASSUMPTIONS.md).

---

## Project status

| Phase | Scope | Status |
|------:|-------|--------|
| **1** | Development environment & infrastructure (Docker Compose stack) | ✅ implemented |
| **2** | Kafka topics + Python producer (`txn-raw` ingestion) — see [`kafka/README.md`](kafka/README.md) | ✅ implemented |
| **3** | Kafka `txn-raw` → Kafka Connect HDFS sink → HDFS Parquet — see [`docs/phase-03-hdfs.md`](docs/phase-03-hdfs.md) | ✅ implemented |
| **4** | Spark Structured Streaming **fraud detection** (`txn-raw` → `txn-flagged` + HDFS metrics) — see [`docs/phase-04-fraud-streaming.md`](docs/phase-04-fraud-streaming.md) | ✅ implemented |
| **5** | Spark Structured Streaming **churn detection** (`txn-raw` → `txn-churn` + HDFS Parquet alerts) — see [`docs/phase-05-churn-streaming.md`](docs/phase-05-churn-streaming.md) | ✅ implemented |
| **6** | Spark Core **customer risk scoring** (`/finsight/raw/txn-raw` → `risk_scores` + `daily_summary`) — see [`docs/phase-06-risk-scoring.md`](docs/phase-06-risk-scoring.md) | ✅ implemented |
| **7** | Spark Core **CLV scoring** (`/finsight/raw/txn-raw` → `clv_scores`) — see [`docs/phase-07-clv-scoring.md`](docs/phase-07-clv-scoring.md) | ✅ implemented |
| **8** | **Hive data warehouse** — `finsight` db: `transactions` (external), `vw_fraud_transactions`, `txn_summary_mart` (managed), `customer_clv` — see [`docs/phase-08-hive.md`](docs/phase-08-hive.md) | ✅ implemented |
| 9 | Spark SQL jobs (compliance / customer summary / dormancy) | ⏳ not started |
| 10 | MongoDB import, Neo4j loader | ⏳ |
| 11 | Alteryx workflows | ⏳ |
| 12 | Power BI dashboards | ⏳ |

Spark SQL, MongoDB, Neo4j, Alteryx and Power BI artifacts are not implemented yet.

---

## What Phase 1 gives you

A one-command local stack via Docker Compose:

| Service | Image | Purpose |
|---------|-------|---------|
| Kafka (KRaft) | `confluentinc/cp-kafka:7.5.0` | Event ingestion backbone |
| Kafka Connect | `finsight/kafka-connect` (cp-kafka-connect + HDFS 3 sink) | `txn-raw` → HDFS (Phase 2) |
| Kafka UI | `provectuslabs/kafka-ui:v0.7.2` | Browser topic inspection |
| HDFS NameNode | `apache/hadoop:3.3.6` | Data-lake metadata |
| HDFS DataNode | `apache/hadoop:3.3.6` | Data-lake storage |
| Hive Metastore | `finsight/hive` (apache/hive:3.1.3 + PG driver) | Warehouse metadata (Thrift 9083) |
| HiveServer2 | `finsight/hive` | SQL / JDBC / ODBC endpoint (10000) |
| PostgreSQL | `postgres:15-alpine` | Hive Metastore backend |
| Spark master | `apache/spark:3.5.3` | Standalone cluster master |
| Spark worker | `apache/spark:3.5.3` | Standalone cluster worker |
| MongoDB | `mongo:7.0` | Customer document store |
| Neo4j | `neo4j:5.20-community` | Fraud knowledge graph |

Not containerised — installed on the Windows host in later phases:
**Alteryx Designer** and **Power BI Desktop** (Windows-only, licensed). See
[`docs/ASSUMPTIONS.md`](docs/ASSUMPTIONS.md).

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| **Docker Desktop** ≥ 4.30 (Compose v2) | Windows 11 + WSL2 backend |
| **Docker memory allocation** | Idle full stack ≈ **4–5 GB**. **8 GB** is enough to run the infrastructure; **12 GB** recommended once Spark jobs run (Phases 3–5). Docker Desktop → Settings → Resources → Memory. Validated on a machine with 7.4 GB allocated. |
| Docker disk | ~15 GB for images + volumes |
| **Python 3.10 or 3.11** | for `scripts/healthcheck.py` (stdlib only) and later phases. PySpark 3.5 does **not** support 3.12+. |
| Internet (first run only) | to pull images and build the Hive / Connect images |
| Git Bash *or* PowerShell | both script variants are provided |

Check your setup:

```bash
docker --version
docker compose version
docker info | grep -i "total memory"
python --version
```

> **Low on memory?** The stack is split into Compose **profiles**
> (`tools`, `connect`, `hive`, `spark`). Start a subset — see
> [Startup](#startup).

---

## Installation

```bash
git clone <this-repo> finsight
cd finsight

# 1. Create your environment file and set the CHANGE_ME secrets
cp .env.example .env          # PowerShell: Copy-Item .env.example .env
#   edit .env  ->  HIVE_DB_PASSWORD, MONGO_INITDB_ROOT_PASSWORD,
#                  MONGO_URI, NEO4J_PASSWORD

# 2. (optional) Python virtual-env for the health check / later phases
python -m venv .venv
# Windows:  .venv\Scripts\activate         Git Bash/Linux:  source .venv/bin/activate
pip install -r requirements.txt            # or requirements-dev.txt

# 3. Build the two custom images (needs internet)
docker compose build hive-metastore hiveserver2 kafka-connect
```

`.env` is git-ignored. **Never commit real credentials or datasets.**

---

## Startup

### Everything (idle ≈ 4–5 GB; 8 GB Docker memory is comfortable)

```bash
# Git Bash / Linux / macOS
./scripts/start.sh

# PowerShell
.\scripts\start.ps1
```

This respects `COMPOSE_PROFILES=tools,connect,hive,spark` in `.env`, so a plain
`docker compose up -d` is equivalent.

### Core only (Kafka + HDFS + MongoDB + Neo4j, ~5 GB)

```bash
./scripts/start.sh --min
.\scripts\start.ps1 -Min
```

### Selected groups

```bash
./scripts/start.sh hive spark          # core + Hive + Spark
.\scripts\start.ps1 -Profiles hive,spark
```

### Rebuild custom images while starting

```bash
./scripts/start.sh --build
.\scripts\start.ps1 -Build
```

**First start takes 2–4 minutes** (NameNode format, Hive schema init, Kafka
KRaft format). Then verify:

```bash
python scripts/healthcheck.py --wait 240
```

---

## Shutdown

```bash
# stop + remove containers, KEEP all data volumes
./scripts/stop.sh
.\scripts\stop.ps1

# stop AND delete every volume (HDFS, Mongo, Neo4j, Kafka, Hive metastore) — asks to confirm
./scripts/stop.sh --wipe
.\scripts\stop.ps1 -Wipe
```

---

## Service URLs

| Service | URL | Auth |
|---------|-----|------|
| Kafka UI | http://localhost:8085 | none |
| Kafka Connect REST | http://localhost:8083 | none |
| HDFS NameNode UI | http://localhost:9870 | none |
| HDFS DataNode UI | http://localhost:9864 | none |
| Spark Master UI | http://localhost:8080 | none |
| Spark Worker UI | http://localhost:8081 | none |
| HiveServer2 Web UI | http://localhost:10002 | none |
| HiveServer2 JDBC | `jdbc:hive2://localhost:10000/finsight` | none (dev) |
| Hive Metastore (Thrift) | `thrift://localhost:9083` | none |
| MongoDB | `mongodb://localhost:27017` | `MONGO_INITDB_ROOT_USERNAME` / `_PASSWORD` |
| Neo4j Browser | http://localhost:7474 | `neo4j` / `NEO4J_PASSWORD` |
| Neo4j Bolt | `bolt://localhost:7687` | same |

**Inside the Compose network** use service names, not `localhost`:
`kafka:29092`, `hdfs://namenode:8020`, `thrift://hive-metastore:9083`,
`spark://spark-master:7077`, `mongodb:27017`, `bolt://neo4j:7687`.

---

## Ports

| Host port | Container | Service | Configurable via `.env` |
|----------:|-----------|---------|--------------------------|
| 9092 | kafka | Kafka broker (host listener) | `KAFKA_HOST_PORT` |
| 8085 | kafka-ui | Kafka UI | `KAFKA_UI_HOST_PORT` |
| 8083 | kafka-connect | Kafka Connect REST | `KAFKA_CONNECT_HOST_PORT` |
| 9870 | namenode | HDFS NameNode UI | `HDFS_NAMENODE_UI_PORT` |
| 8020 | namenode | HDFS NameNode RPC | `HDFS_NAMENODE_RPC_PORT` |
| 9864 | datanode | HDFS DataNode UI | `HDFS_DATANODE_UI_PORT` |
| 9083 | hive-metastore | Metastore Thrift | `HIVE_METASTORE_PORT` |
| 10000 | hiveserver2 | HiveServer2 JDBC | `HIVESERVER2_PORT` |
| 10002 | hiveserver2 | HiveServer2 Web UI | `HIVESERVER2_UI_PORT` |
| 7077 | spark-master | Spark master RPC | `SPARK_MASTER_RPC_PORT` |
| 8080 | spark-master | Spark master UI | `SPARK_MASTER_UI_PORT` |
| 8081 | spark-worker | Spark worker UI | `SPARK_WORKER_UI_PORT` |
| 27017 | mongodb | MongoDB | `MONGO_PORT` |
| 7474 | neo4j | Neo4j HTTP / Browser | `NEO4J_HTTP_PORT` |
| 7687 | neo4j | Neo4j Bolt | `NEO4J_BOLT_PORT` |
| 5432 | hive-postgres | *(not published)* internal only | — |

Internal-only ports: `kafka:29092` / `29093`, `datanode:9866`.
Full reference: [`docs/PORTS_AND_SERVICES.md`](docs/PORTS_AND_SERVICES.md).

---

## Repository layout

```
finsight/
├── data/            raw/ (gitignored)  sample/  schemas/
├── kafka/           topics/  producer/  connect/          (Phase 2)
├── spark/           streaming/  batch/  common/           (Phases 3-4)
├── sql/             spark_sql_jobs.py                      (Phase 5)
├── hive/            ddl/  analyze/                         (Phase 6)
├── mongodb/         import/  validation/                   (Phase 6)
├── neo4j/           neo4j_loader.py  cypher/               (Phase 6)
├── alteryx/         workflows/  fallback/  inputs/  outputs/  (Phase 7)
├── powerbi/         datamodel/  kafka_bridge/              (Phase 8)
├── docker/          hadoop/  hive/  kafka-connect/  conf/  ← infra build context
├── scripts/         start.*  stop.*  healthcheck.py
├── tests/           unit/  integration/  data_quality/
├── docs/            ARCHITECTURE.md  ASSUMPTIONS.md  PORTS_AND_SERVICES.md
├── docker-compose.yml
├── .env.example  .gitignore  requirements.txt  requirements-dev.txt
└── README.md
```

---

## Troubleshooting

### `healthcheck.py` shows a service as `SKIP`
Its Compose profile is not active. Start it with
`./scripts/start.sh <profile>` or add the profile to `COMPOSE_PROFILES` in `.env`.

### Containers get OOM-killed / restart loop (`docker compose ps` shows `Restarting`)
Docker does not have enough memory. Either raise Docker Desktop memory to 12–16 GB
or run a subset: `./scripts/start.sh --min`, then bring up `hive` and `spark`
separately when needed.

### NameNode exits with `NameNode is not formatted`
The `hdfs-name` volume was created but never formatted (e.g. interrupted first
run). Reset it:
```bash
docker compose down
docker volume rm finsight_hdfs-name finsight_hdfs-data
./scripts/start.sh
```

### DataNode not showing under "Live datanodes"
Give it 30–60 s after the NameNode is up. Check logs:
`docker compose logs datanode`. A stale `hdfs-data` volume from a previous
cluster ID also causes this — wipe both HDFS volumes (above).

### Hive Metastore keeps restarting / `schematool` errors
First start initialises the Postgres schema and can take ~60 s. If it fails:
```bash
docker compose logs hive-metastore
# usually the Postgres container was not ready — just restart:
docker compose restart hive-metastore hiveserver2
```
If the schema is half-created, wipe the backend: `docker volume rm finsight_hive-pg`.

### HiveServer2 UI (10002) up but JDBC (10000) refused
HiveServer2 opens the web port before the Thrift port. Wait ~30 s more, or
`docker compose logs hiveserver2` and look for `Starting HiveServer2`.

### `docker compose build` fails downloading the PostgreSQL / HDFS-sink artifact
Network/proxy issue reaching Maven Central or Confluent Hub. Retry
`docker compose build --no-cache hive-metastore kafka-connect`. Behind a
corporate proxy, set `HTTP_PROXY` / `HTTPS_PROXY` in your shell first.

### Port already in use
Another process owns the host port. Change the matching `*_PORT` in `.env`
and restart.

### Kafka UI can't reach the broker
It uses the internal listener `kafka:29092`. If you changed `KAFKA_HOST_PORT`
that does not affect it. Check `docker compose logs kafka` for a completed
KRaft format (`Formatting ... metadata.version`).

### Neo4j: "password too short"
`.env` `NEO4J_PASSWORD` must be ≥ 4 characters (the compose file lowers the
default minimum for dev). Use something real for anything shared.

### Everything is slow on first query
Cold JVMs. The first Spark job / Hive query per container is always slow;
subsequent ones are fine.

---

## Next step

Phase 1 stops here. Do **not** proceed to Phase 2 automatically — see the
project status table and wait for the go-ahead.
