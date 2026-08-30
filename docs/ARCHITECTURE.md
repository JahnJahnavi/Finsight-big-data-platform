# FinSight — Architecture

## Target platform (all phases)

```
NovaCrest_Transactions.csv
        │  Python Kafka producer  (~1,000 msg/s)          [Phase 2]
        ▼
   ┌─────────┐   txn-raw (3 partitions)
   │  Kafka  │───────────────┬───────────────────────────────┐
   │ (KRaft) │               │                               │
   └─────────┘               ▼                               ▼
        │        Spark Structured Streaming        Kafka Connect HDFS 3 Sink
        │        ├─ fraud  → txn-flagged  [P3]      → HDFS Parquet, part. by step [P2]
        │        └─ churn  → txn-churn    +          │
        │                    /finsight/processed/churn_alerts/
        ▼                                            ▼
   txn-flagged / txn-churn                     ┌──────────┐
                                               │   HDFS   │  /finsight/raw/transactions/
                                               └──────────┘
                                                    │
                    ┌───────────────────────────────┼───────────────────────────┐
                    ▼                               ▼                           ▼
             Spark Core batch [P4]          Spark SQL [P5]                 Hive [P6]
             ├─ risk_scores/                ├─ compliance                  external: finsight.transactions
             ├─ daily_summary/              ├─ customer_fraud_summary/     view:     finsight.vw_fraud_transactions
             └─ clv_scores/                 └─ dormancy_report/            managed:  finsight.txn_summary_mart
                                                                          external: finsight.customer_clv

   MongoDB  ← noveacrest_customers.json (mongoimport)         [P6]   customer profiles, idx (customerId, segment)
   Neo4j    ← 4 graph CSVs (neo4j_loader.py)                  [P6]   (Account)-[:SENT]->(Transaction)-[:RECEIVED_BY]->(Account)

   Hive + MongoDB + Spark outputs ─► Alteryx [P7] ─► Power BI [P8]
                                     ├─ Customer Risk Blend      ├─ Fraud Alert Board
                                     └─ Transaction Summary      ├─ Customer 360
                                                                 └─ Risk & Compliance Report
```

## Phase 1 scope (this deliverable)

Only the **infrastructure** boxes above — the running services, networking,
volumes, and the scripts to operate them. No data flows are implemented.

```
                       docker network: finsight-net
 ┌──────────────────────────────────────────────────────────────────────┐
 │                                                                      │
 │  kafka ─ kafka-ui ─ kafka-connect        namenode ─ datanode         │
 │    (KRaft 9092/29092)                      (HDFS 8020 / 9870 / 9864)  │
 │                                                                      │
 │  hive-postgres ─ hive-metastore ─ hiveserver2    spark-master ─       │
 │        (5432)        (9083)         (10000/10002)  spark-worker       │
 │                                                   (7077/8080/8081)   │
 │                                                                      │
 │  mongodb (27017)                 neo4j (7474 / 7687)                  │
 │                                                                      │
 └──────────────────────────────────────────────────────────────────────┘
        persistent named volumes: hdfs-name, hdfs-data, hive-pg,
        kafka-data, connect-data, mongo-data, neo4j-data/-logs/-import
```

## Configuration flow

| Service | How it is configured |
|---------|----------------------|
| HDFS (namenode/datanode) | `docker/hadoop/hadoop.env` → image `envtoconf.py` writes `core-site.xml` / `hdfs-site.xml` |
| Hive (metastore/hiveserver2) | `docker/conf/hive/hive-site.xml` + `docker/conf/hadoop/*.xml` mounted into `/opt/hive/conf` and `/opt/hadoop/etc/hadoop`; DB credentials via `SERVICE_OPTS` |
| Spark (master/worker) | `docker/conf/spark/spark-defaults.conf` + hive-site + hadoop `*.xml` mounted into `/opt/spark/conf` |
| Kafka | `environment:` in `docker-compose.yml` (KRaft single-node) |
| Kafka Connect | `docker/kafka-connect/Dockerfile` (HDFS 3 sink plugin) + `CONNECT_*` env; hadoop conf mounted at `/etc/hadoop/conf` |
| MongoDB / Neo4j | `environment:` (root credentials / `NEO4J_AUTH` from `.env`) |

All secrets come from `.env` (git-ignored). `.env.example` is the template.

## Deliberately deferred

- HDFS directory layout (`/finsight/...`) — created in Phase 2.
- Kafka topics — created in Phase 2 with spec partition counts.
- Hive databases/tables — Phase 6.
- Spark History Server, Airflow, schema registry — not required by the spec; may be added later if a phase needs them.
