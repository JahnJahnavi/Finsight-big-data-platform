# FinSight — Phase 9: Spark SQL Analytics

One entry point, three analytical modes over the Hive warehouse (spec 7.5 / 7.6):

```bash
python sql/spark_sql_jobs.py --mode compliance
python sql/spark_sql_jobs.py --mode customer_summary
python sql/spark_sql_jobs.py --mode dormancy
```

| Mode | Grain | Output (Parquet) | CSV export |
|------|-------|------------------|------------|
| `compliance` | one row per transaction `type`, 168-step window | `/finsight/processed/compliance_summary/` | `/finsight/exports/compliance_summary.csv` |
| `customer_summary` | one row per customer (`nameOrig LIKE 'C%'`) | `/finsight/processed/customer_fraud_summary/` | — |
| `dormancy` | one row per dormant account | `/finsight/processed/dormancy_report/` | `/finsight/exports/dormancy_report.csv` |

**No Alteryx** in this phase.

## Files

```
sql/
├── spark_sql_jobs.py     single entry point, --mode {compliance,customer_summary,dormancy}
├── sql_rules.py          pure-Python classification rules (unit-tested)
└── run_spark_sql.sh      spark-submit wrapper (runs inside finsight-spark-master)
tests/unit/test_sql_rules.py   11 tests for the classification thresholds
scripts/validate_phase9.py     13-check end-to-end validation
```

`spark_sql_jobs.py` registers the source as the `txn_source` temp view, then each
mode is a single `spark.sql(...)` query. Source is the Hive table
`finsight.transactions` by default; `--from parquet --input <path>` or
`--from csv --csv <path>` for isolated testing (the validation script uses CSV).

## `compliance` (spec 7.5)

`WHERE step BETWEEN 1 AND 168` (`COMPLIANCE_WINDOW_STEPS`), `GROUP BY type`:

| Column | Meaning |
|--------|---------|
| `transaction_type` | `type` |
| `transaction_count` | `COUNT(*)` |
| `transaction_volume` | `ROUND(SUM(amount), 2)` |
| `fraud_count` | `SUM(isFraud = 1)` |
| `fraud_rate_pct` | `fraud_count * 100 / transaction_count` |
| `risk_classification` | `>= 5%` → **High**, `>= 1%` → **Medium**, else **Low** |

The risk thresholds are **not given by the spec** — `ASSUMPTIONS.md` I34,
overridable via `COMPLIANCE_RISK_HIGH_PCT` / `COMPLIANCE_RISK_MEDIUM_PCT`, needs
owner sign-off. The same tiering lives in `sql/sql_rules.py:risk_classification`
so it can be unit-tested without Spark.

## `customer_summary` (spec 7.6)

`WHERE nameOrig LIKE 'C%'`, `GROUP BY nameOrig`: `customerId`,
`total_transactions`, `total_amount`, `confirmed_fraud_count` (`isFraud = 1`),
`fraud_rate_pct`. Parquet only — the spec names no CSV for this one.

## `dormancy` (spec 7.6)

An account is **dormant** when *all* hold:

1. `steps_inactive > 72` where `steps_inactive = MAX(step) over all data − account's last step`
2. `>= 5` historical transactions
3. `nameOrig LIKE 'C%'` — merchant accounts (`M…`) are excluded

Severity: `73–120` → **Dormant**, `> 120` → **Severely Dormant**. `72` itself is
not dormant (base condition is strict `> 72`) — `ASSUMPTIONS.md` I36.

Output columns: `customerId`, `last_active_step`, `max_step`, `steps_inactive`,
`txn_history_count`, `dormancy_severity`. Written as Parquet **and** as exactly
one CSV file at `/finsight/exports/dormancy_report.csv` (`coalesce(1)` then the
`part-*.csv` is renamed to the target path via the Hadoop `FileSystem` API).

> On the synthetic 168-step dataset every customer has ~1 transaction, so
> `dormancy` against the real Hive table returns 0 rows (condition 2 fails). The
> validation script feeds a crafted CSV with ≥5-txn customers to exercise it.

## Run it

```bash
# against the Hive warehouse (needs Phases 2/3/8)
sql/run_spark_sql.sh --mode compliance
sql/run_spark_sql.sh --mode customer_summary
sql/run_spark_sql.sh --mode dormancy
SPARK_MASTER=local[2] sql/run_spark_sql.sh --mode dormancy      # no cluster

# against an ad-hoc CSV
sql/run_spark_sql.sh --mode compliance --from csv --csv /opt/finsight/data/sample/sql_txns.csv
```

## Validation

```bash
python -m pytest tests/unit/test_sql_rules.py -q     # 11 rule tests
python scripts/validate_phase9.py                    # 13 end-to-end checks
```

`validate_phase9.py` generates `data/sample/sql_txns.csv` (engineered so the
five transaction types land in the five risk tiers Low→High, and a mix of
active / dormant / severely-dormant / <5-txn / merchant accounts), runs all
three modes from the single entry point, then reads the Parquet/CSV outputs back
and asserts: spec columns present; `risk_classification` correct per type;
per-customer fraud counts correct; `M…` accounts excluded from both
customer-facing modes; exactly the two dormant customers with the right
severity; non-dormant / merchant / short-history accounts excluded; a single
CSV file at the export path.
