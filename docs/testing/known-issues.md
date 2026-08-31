# FinSight — Known Issues (Phase 14 validation)

Issues surfaced by end-to-end validation that are **not code defects** in the
FinSight components — they are dev-stack limitations or synthetic-data
characteristics. Each has a workaround; none blocks the pipeline.

## KI-1 — HiveServer2 aggregation returns 0 rows on `finsight.transactions`

**Symptom.** `SELECT COUNT(*) FROM finsight.transactions` (and any `GROUP BY`)
returns **0** via beeline/HiveServer2, though the table has ~4000 rows.
`SELECT * … LIMIT n` (fetch task) returns real data; Spark reads the table
correctly. `vw_fraud_transactions` inherits the same 0.

**Cause.** The raw Parquet lands under `step=<N>/` sub-directories (Kafka
Connect `FieldPartitioner`). Tez in local mode (no YARN) does not recurse those
for split generation despite `hive.mapred.supports.subdirectories` /
`mapreduce.input.fileinputformat.input.dir.recursive` in `hive-site.xml`. Stale
table stats (`hive.compute.query.using.stats`) then report the phantom 0.
See `ASSUMPTIONS.md` I31 / I33 and `docs/phase-08-hive.md`.

**Impact.** Checkpoints **12, 13** report `WARN` (table/view/schema verified;
count unreliable). Checkpoint **15** (Spark SQL compliance) is unaffected — the
Spark SQL job reads through Spark, which recurses correctly.

**Workaround.** Read `finsight.transactions` via Spark
(`spark.table("finsight.transactions")`, `spark.sql.hive.convertMetastoreParquet=false`
+ recursive flags in `spark-defaults.conf`). All downstream Spark jobs
(Phases 6, 7, 9) already do this. For Power BI, use Import mode from
`powerbi/export_datasets.py`, not DirectQuery-to-Hive for transaction-level
aggregates.

## KI-2 — Synthetic dataset is 168 steps / 1 transaction per customer

The bundled `Transactions.csv` is a 7-day (168-step) PaySim sample with roughly
one transaction per originating customer. Consequences the validator flags as
`WARN` or expects with relaxed bounds:

| Where | Effect |
|---|---|
| `customer_fraud_summary` | `total_transactions` ≈ 1, `fraud_rate_pct` is 0 or 100 (not a smooth distribution) |
| Dormancy (checkpoint 17) | needs ≥ 5 historical txns per customer → **0 dormant accounts** on the real Hive data; `validate_phase9.py` uses a crafted fixture to exercise the rule |
| Alteryx WF1 (checkpoint 20) | transaction-set customer IDs vs profile customer IDs are different populations → inner join ≈ **287 rows**; many `clv_classification` = "Unscored" |
| Power BI daily/weekly trend | 7 days = 7 daily points ≈ 1–2 ISO weeks |
| `composite_risk_score` | fraud term (0–100) dominates churn term (0–1) — `ASSUMPTIONS.md` I48, **needs owner sign-off** |

All scale correctly with a larger feed; the rules and schemas are correct.

## KI-3 — Streaming checkpoints validated in `--once` mode

`run_e2e_pipeline.sh` runs `fraud_detection.py` / `churn_detection.py` with
`--once` (process the current backlog, then stop). Continuous streaming is the
production path; the e2e run verifies one micro-batch produces the right
`txn-flagged` / `txn-churn` / `streaming_metrics` / `churn_alerts` output.

## KI-4 — `spark-submit` latency on the dev box

Each Spark job is a fresh JVM on a single-worker standalone cluster with small
heaps; cold start + shuffle puts most jobs at 2–5 min and the full
`run_e2e_pipeline.sh` at ~20–40 min. Not a defect — budget accordingly. The
validator itself is fast (~2–4 min) because it only inspects.

## KI-5 — `alteryx/` and `powerbi/` outputs are prep artifacts

Phases 12–13 have no Alteryx Designer / Power BI Desktop in the toolchain.
Checkpoints 20–21 validate the **headless-prep** outputs
(`alteryx/fallback/*.py`, `powerbi/export_datasets.py`,
`powerbi/kafka_bridge/txn_flagged_bridge.py`) — the reproducible inputs a
Designer / Desktop build consumes. No `.yxmd` / `.pbix` is committed
(`ASSUMPTIONS.md` I49 / I50).

---

_Open a new entry here for any `FAIL` the validator reports — with the failing
checkpoint, the `expected`/`actual`, and the investigation — before changing any
component._
