# FinSight — Ports & Services Reference

All host ports are overridable in `.env`. Inside the `finsight-net` Compose
network, services address each other by **service name** on the *internal* port.

## Host-published ports

| Host | Service (container) | Internal addr | Protocol | Purpose |
|-----:|---------------------|---------------|----------|---------|
| 9092 | `finsight-kafka` | `kafka:9092` (EXTERNAL) | Kafka | Broker — host clients |
| 8085 | `finsight-kafka-ui` | `kafka-ui:8080` | HTTP | Kafka UI |
| 8083 | `finsight-kafka-connect` | `kafka-connect:8083` | HTTP | Connect REST API |
| 9870 | `finsight-namenode` | `namenode:9870` | HTTP | NameNode Web UI |
| 8020 | `finsight-namenode` | `namenode:8020` | RPC | HDFS client RPC (`hdfs://`) |
| 9864 | `finsight-datanode` | `datanode:9864` | HTTP | DataNode Web UI |
| 9083 | `finsight-hive-metastore` | `hive-metastore:9083` | Thrift | Hive Metastore |
| 10000 | `finsight-hiveserver2` | `hiveserver2:10000` | Thrift/JDBC | HiveServer2 SQL (Alteryx/Power BI ODBC) |
| 10002 | `finsight-hiveserver2` | `hiveserver2:10002` | HTTP | HiveServer2 Web UI |
| 7077 | `finsight-spark-master` | `spark-master:7077` | RPC | Spark submit / cluster |
| 8080 | `finsight-spark-master` | `spark-master:8080` | HTTP | Spark Master UI |
| 8081 | `finsight-spark-worker` | `spark-worker:8081` | HTTP | Spark Worker UI |
| 27017 | `finsight-mongodb` | `mongodb:27017` | Mongo wire | MongoDB |
| 7474 | `finsight-neo4j` | `neo4j:7474` | HTTP | Neo4j Browser / REST |
| 7687 | `finsight-neo4j` | `neo4j:7687` | Bolt | Neo4j driver protocol |

## Internal-only ports (not published to host)

| Internal addr | Service | Purpose |
|---------------|---------|---------|
| `kafka:29092` | Kafka | INTERNAL listener — used by every in-network client (Connect, Spark, Kafka UI) |
| `kafka:29093` | Kafka | KRaft CONTROLLER listener |
| `datanode:9866` | HDFS | DataNode data-transfer port |
| `hive-postgres:5432` | PostgreSQL | Hive Metastore backend DB |

## Compose profiles → services

| Profile | Services | Approx. memory |
|---------|----------|---------------:|
| *(none — always on)* | `kafka`, `namenode`, `datanode`, `mongodb`, `neo4j` | ~5.2 GB |
| `tools` | `kafka-ui` | ~0.3 GB |
| `connect` | `kafka-connect` | ~0.7 GB |
| `hive` | `hive-postgres`, `hive-metastore`, `hiveserver2` | ~2.3 GB |
| `spark` | `spark-master`, `spark-worker` | ~2.0 GB |

`.env` default: `COMPOSE_PROFILES=tools,connect,hive,spark` (everything).

## Connection strings (later phases)

| From | Target | String |
|------|--------|--------|
| host Python | Kafka | `localhost:9092` |
| in-network | Kafka | `kafka:29092` |
| host Spark / tools | HDFS | `hdfs://localhost:8020` |
| in-network | HDFS | `hdfs://namenode:8020` |
| host | HiveServer2 | `jdbc:hive2://localhost:10000/finsight` |
| in-network Spark | Metastore | `thrift://hive-metastore:9083` |
| host Python | MongoDB | `mongodb://<user>:<pw>@localhost:27017/finsight?authSource=admin` |
| host Python | Neo4j | `bolt://localhost:7687` |
| in-network | Spark master | `spark://spark-master:7077` |

## Named volumes

| Volume | Mounted by | Holds |
|--------|-----------|-------|
| `finsight_hdfs-name` | namenode | HDFS namespace / fsimage |
| `finsight_hdfs-data` | datanode | HDFS blocks |
| `finsight_hive-pg` | hive-postgres | Metastore schema |
| `finsight_kafka-data` | kafka | Topic logs + KRaft metadata |
| `finsight_connect-data` | kafka-connect | Connector offsets (file) |
| `finsight_mongo-data` / `finsight_mongo-config` | mongodb | Documents |
| `finsight_neo4j-data` / `-logs` / `-import` | neo4j | Graph store, logs, import staging |
