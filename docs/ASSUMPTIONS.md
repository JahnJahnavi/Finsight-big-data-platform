# FinSight — Assumptions Register

The specification (`FinSight_Full_Specification_Complete.pdf`) is the source of
truth. This file records **every decision made where the spec is silent,
ambiguous, or self-contradictory**. Each entry has an ID so later phases and
code comments can reference it (`# see ASSUMPTIONS.md A3`).

Status: `OPEN` = needs owner confirmation · `ADOPTED` = provisionally in effect.

---

## Phase 1 — infrastructure

| ID | Area | Spec says | Assumption adopted | Status |
|----|------|-----------|--------------------|--------|
| **I1** | Container images | Not specified | Official images pinned: Kafka `cp-kafka:7.5.0` (KRaft, no ZooKeeper), `apache/hadoop:3.3.6`, `apache/hive:3.1.3`, `apache/spark:3.5.3`, `mongo:7.0`, `neo4j:5.20-community`, `postgres:15-alpine`, `provectuslabs/kafka-ui:v0.7.2`. | ADOPTED |
| **I2** | Hive version | Not specified | **Hive 3.1.3**, not 4.x. Spark 3.5's built-in Hive metastore client supports metastore versions only up to 3.1.x — a Hive 4.0.1 metastore was verified to fail Spark SQL with `Invalid method name: 'get_table'`. Hive 3.1.3 metastore + Spark 3.5 built-in (2.3.9) client is the supported, no-download combination. Hive 3.1.3 runs Tez in local mode (no YARN). | ADOPTED |
| **I2b** | Hive metastore backend | Not specified | PostgreSQL (not embedded Derby) so Spark, HiveServer2 and the metastore can share it concurrently. JDBC driver added via `docker/hive/Dockerfile`. | ADOPTED |
| **I3** | Spark deployment | "same Spark cluster" (7.4 R2) | Spark **standalone** master+worker (not YARN, not k8s) — lowest footprint that still gives a real cluster + Spark UI per app. | ADOPTED |
| **I4** | Kafka Connect distribution | "Kafka Connect HDFS Sink Connector" (6.3) | Confluent Community `kafka-connect-hdfs3` v1.1.x, installed at image build. Licensed under the Confluent Community License (allowed for this use). | ADOPTED |
| **I5** | HDFS partitioning | 5.1 "Parquet / day" vs 6.3 "partitioned by the step field" | Follow 6.3 (more specific & downstream jobs key on `step`): partition by `step`. Revisit in Phase 2. | ADOPTED |
| **I6** | HDFS replication | Not specified | `dfs.replication=1` (single DataNode dev cluster). | ADOPTED |
| **I7** | Security | Not specified | Dev mode: no Kerberos, HDFS permission checks disabled, plaintext Kafka, no TLS, HiveServer2 `doAs=false`. **Not production-safe** — documented for the capstone demo only. | ADOPTED |
| **I8** | Ports | Not specified | Host ports per `README.md`. Chosen to avoid common clashes; Spark master UI keeps 8080, Kafka UI moved to 8085. All overridable in `.env`. | ADOPTED |
| **I9** | Memory | Not specified | Per-service `mem_limit` + small JVM heaps tuned for a 12 GB Docker allocation. Compose **profiles** (`tools/connect/hive/spark`) allow partial startup below that. | ADOPTED |
| **I10** | Alteryx / Power BI | Required technologies | Cannot be containerised (Windows-only, licensed desktop). Run on the host in Phases 7–8. A PySpark/pandas fallback for each Alteryx workflow will live in `alteryx/fallback/` so the pipeline is runnable headless. | ADOPTED |
| **I11** | `.env` `SIM_EPOCH` | Data has only relative `step` (1 = 1 hour), no timestamp | Assume `step 1 == 2023-01-01T00:00:00Z`; `event_ts = SIM_EPOCH + (step-1) hours`. Needed for churn window bounds and Power BI date axes. | OPEN |
| **I12** | Kafka topic auto-create | Not specified | Disabled (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`). Topics are created explicitly in Phase 2 with the partition counts from spec 6.1 / 7.2. | ADOPTED |
| **I13** | `txn-raw` message format | Spec 6.2 "serialized JSON message" | Producer wraps the payload in a Kafka Connect JSON **schema envelope** (`{"schema":...,"payload":...}`) by default — the HDFS sink's `ParquetFormat` + `FieldPartitioner` need a typed Connect `Struct` and there is no Schema Registry. `--raw` emits the bare payload; consumer/validator `unwrap()` both forms. Also adds derived `txnId` (`TXN`+9-digit) and `ingest_ts`. | ADOPTED |
| **I5b** | HDFS raw path | Spec 7.3 / user Phase 3: `/finsight/raw/transactions/` | Kafka Connect's HDFS sink always appends `<topic>` as the last path segment and a topic-renaming `RegexRouter` crashes `Hdfs3SinkConnector` 1.1.26 (verified: NPE in `DataWriter.write`). Raw Parquet therefore lands at **`/finsight/raw/txn-raw/`** (`topics.dir=finsight/raw`). Downstream Hive/Spark target that path. Partitioning by `step` (I5) is unchanged. | ADOPTED |
| **I14** | Fraud rule condition 3 | Spec 7.1: "destination post-transaction balance is zero" / `newbalanceDest == 0` | Literal float equality against `0.0` (Spark SQL `newbalanceDest = 0.0`). The account-emptying pattern produces an exact `0.0`; no epsilon tolerance. Rule kept in one place: `spark/streaming/fraud_rule.py`. | ADOPTED |
| **I15** | `streaming_metrics` format | Spec 7.1 R2: "write this summary … for monitoring" — no format given | JSON, appended one file per micro-batch: `{batch_id, batch_ts, total_count, flagged_count, fraud_rate_pct, app_name, fraud_rule}`. Readable via `hdfs dfs -cat` and by Spark/Hive. | ADOPTED |
| **I16** | `txn-flagged` message format | Spec 7.1: "written to the `txn-flagged` topic" — no format given | Bare JSON: the 13 transaction fields + `fraud_rule` + `detected_at` (detection timestamp). Key = `nameOrig`. | ADOPTED |
| **I17** | Churn window slide | Spec 7.2: "24-step sliding window" — slide not given | 24-step window, **12-step slide** (`CHURN_SLIDE_STEPS`). Each txn contributes to 2 overlapping windows; a churning customer gets one alert per window. | ADOPTED |
| **I18** | Churn S3 "exclusively CASH_OUT" | Spec 7.2 signal 3 | Every transaction in the window is `CASH_OUT` (`cashout_count == w_count`) **and** no `PAYMENT`/`DEBIT`. | ADOPTED |
| **I19** | Churn S4 balance threshold / "consecutive" | Spec 7.2 signal 4: "reaches zero or below 500 for two or more consecutive transactions" | `newbalanceOrig < 500` (zero included); "consecutive" evaluated in `(step, kafka_offset)` order within the window; run length >= 2. | ADOPTED |
| **I20** | `hist_freq_per_12` (S1 baseline) | Spec 7.2: "historical average is above 3 per 12 steps" — not defined how | `all_time_txn_count / ((max_step_in_history - customer_first_step + 1) / 12)`, from `bootstrap_customer_history.py`. Customers absent from the baseline cannot trigger S1/S2. | ADOPTED |
| **I21** | Churn output-mode / dedup | Spec 7.2 R2 | `outputMode("update")` on the windowed aggregation — alerts emit in real time and a customer+window may be re-emitted as evidence accrues. `txn-churn` and `churn_alerts` consumers dedupe on `(customerId, window_end_step)`. | ADOPTED |
| **I22** | Spark image | Not specified | `finsight/spark:3.5.3` = `apache/spark:3.5.3` + `pandas==1.5.3` + `pyarrow==12.0.1` (base ships neither; needed for Arrow APIs from Phase 6). pandas pinned < 2 because PySpark 3.5's `applyInPandasWithState` breaks on pandas 2.x. | ADOPTED |
| **I23** | Risk factor: "average transfer amount" | Spec 7.3 factor 2 | Mean `amount` of the customer's **`type = 'TRANSFER'`** transactions (`0` if the customer makes none). | ADOPTED |
| **I24** | Risk "transaction frequency" / "rolling 7-day" | Spec 7.3: "rolling 7-day composite risk score" | Computed over the **full** transaction history — which is a 7-day (168-step) dataset — so the "rolling 7-day" window == the whole history here. Frequency = the customer's transaction count. | ADOPTED |
| **I25** | Risk input path | Spec 7.3: `/finsight/raw/transactions/` | Default `--input` is `/finsight/raw/txn-raw` (the actual Kafka Connect landing path, I5b); overridable. | ADOPTED |
| **I26** | `daily_summary` format | Spec 7.3 R2 + 9.2 (Alteryx reads a "CSV export") | Parquet to `/finsight/processed/daily_summary/` **and** CSV to `/finsight/exports/daily_summary/`. Grouped by `type` and `step`; columns `transaction_volume`, `total_amount`, `fraud_count`. | ADOPTED |
| **I27** | `risk_scores` columns | Spec 7.3: "one row per customerId with a normalised risk_score" + R1 tier column | Required `customerId`, `risk_score`, `risk_tier` **plus** the 4 raw + 4 normalised factor columns and `scored_at` (for Power BI drill-down). | ADOPTED |

---

## Carried forward from the specification analysis (to resolve in later phases)

| ID | Area | Gap | Planned resolution | Phase |
|----|------|-----|--------------------|-------|
| G1 | Hive | DDL for `finsight.transactions` referenced ("below") but absent from the PDF | Derive from the 11-column CSV schema + derived `txnId`, `event_ts`; external table partitioned by `step` | 6 |
| G2 | HDFS | Landing directory structure referenced ("below") but absent | RESOLVED in Phase 3: `/finsight/raw/txn-raw/step=<N>/*.parquet` (snappy). See I5b. Full layout in `scripts/init_hdfs.sh`. | 3 ✓ |
| G13 | Kafka Connect | HDFS sink + Parquet needs a schema; Confluent connector licensing | RESOLVED in Phase 3: `kafka-connect-hdfs3` 1.1.26 (Confluent Community licence), `ParquetFormat` + `JsonConverter schemas.enable=true` fed by the producer's schema envelope (I13). No Schema Registry. | 3 ✓ |
| G3 | MongoDB | `mongoimport` command referenced but absent | `mongoimport --db finsight --collection customers --file … (--jsonArray if needed)` | 6 |
| G4 | Neo4j | Cypher fraud-ring query referenced but absent | Implement from description: accounts with `> 3` distinct inbound senders | 6 |
| G5 | Neo4j | `neo4j_loader.py` described as "provided" but not in the dataset | We implement it (neo4j Python driver, batched UNWIND) | 6 |
| G6 | Spark Core | Risk scoring: "four weighted factors" — **weights not given** (unlike CLV) | RESOLVED Phase 6: `.env` `RISK_W_*`, default **0.25 each**. **Needs owner sign-off.** | 6 ⚠️ |
| G7 | Spark Core | Risk score normalisation method unspecified | RESOLVED Phase 6: **min-max per factor** across all customers, then weighted sum, clamped to [0,1]. | 6 ✓ |
| G8 | Streaming | Churn "historical / all-time average" — cold start in a streaming job | Seed from a one-off batch over HDFS history (`bootstrap_customer_history.py`) | 3 |
| G9 | Spark Core | CLV Recency: "inverse of steps since last txn, normalised" — formula ambiguous | `recency = clamp(1 - steps_since_last/48, 0, 1)`, 0 beyond 48 steps | 4 |
| G14 | Power BI | Page 1 source = "txn-flagged Kafka topic" — Power BI cannot consume Kafka natively | Bridge consumer writes `txn-flagged` to a rolling file / push dataset | 8 |
| G15 | MongoDB | "expected distribution" for segment validation not numerically given | Validate: 5 segments present, counts sum to 10,000 | 6 |
| G16 | Alteryx | Composite-risk formula differs from `composite_risk_score` already in the customer JSON | Keep as separate fields: `alteryx_composite_risk` vs `profile_composite_risk` | 7 |

---

## Source data

The primary datasets (`NovaCrest_Transactions.csv`, `noveacrest_customers.json`,
the 4 Neo4j CSVs, `neo4j_loader.py`) are **not yet in the repo** — only the
metadata reference (`Bigdata Data set file/NOVACR_1.TXT`) and the spec PDF.
Confirmed schemas are recorded in the Phase 0 analysis. Data acquisition
(real files vs. synthetic generator) is an open decision for Phase 2.
