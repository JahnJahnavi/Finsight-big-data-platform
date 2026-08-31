-- =============================================================================
-- FinSight - Phase 12 prerequisite: finsight.customer_fraud_summary  (EXTERNAL)
--
-- Alteryx WORKFLOW 1 lists "Hive customer fraud summary" as an input, but the
-- Phase 9 Spark SQL job (`sql/spark_sql_jobs.py --mode customer_summary`) writes
-- only Parquet to /finsight/processed/customer_fraud_summary/ - it is not
-- registered in the metastore. This DDL exposes that output as a Hive table so
-- Alteryx can read it over the same HiveServer2 ODBC DSN it uses for
-- txn_summary_mart / customer_clv.
--
-- Run AFTER:  sql/run_spark_sql.sh --mode customer_summary
--   docker exec -i finsight-hiveserver2 beeline -u jdbc:hive2://localhost:10000/ \
--     -f /finsight/alteryx/prereq/customer_fraud_summary_external.hql
-- (mount ./alteryx into the hiveserver2 container, or pipe the file over stdin)
--
-- Columns follow ASSUMPTIONS I37.
-- =============================================================================
USE finsight;

DROP TABLE IF EXISTS finsight.customer_fraud_summary;

-- Column types MUST match the Parquet the Phase 9 job writes (verified with
-- pyarrow): fraud_rate_pct is decimal128(31,6) because the query rounds a
-- `* 100.0` decimal literal - declaring it DOUBLE makes Hive throw
-- "HiveDecimalWritable cannot be cast to DoubleWritable" on SELECT.
CREATE EXTERNAL TABLE finsight.customer_fraud_summary (
  customerId            STRING         COMMENT 'nameOrig, LIKE C% - join key to MongoDB / customer_clv',
  total_transactions    BIGINT,
  total_amount          DOUBLE,
  confirmed_fraud_count BIGINT         COMMENT 'transactions with isFraud = 1',
  fraud_rate_pct        DECIMAL(31,6)  COMMENT '0-100, confirmed_fraud_count * 100 / total_transactions'
)
COMMENT 'Per-customer fraud summary - external over Phase 9 Spark SQL customer_summary output'
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/finsight/processed/customer_fraud_summary'
TBLPROPERTIES ('external.table.purge'='false');

-- sanity
-- SELECT COUNT(*) AS customers, ROUND(AVG(fraud_rate_pct), 4) AS avg_fraud_rate_pct
-- FROM finsight.customer_fraud_summary;
