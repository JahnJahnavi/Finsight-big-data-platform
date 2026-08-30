-- =============================================================================
-- FinSight - Phase 8: finsight.transactions  (EXTERNAL, spec section 8.1)
--
-- Over the Kafka Connect HDFS-sink Parquet output. The connector names the
-- output directory after the source topic, so the spec's
-- "/finsight/raw/transactions/" is physically "/finsight/raw/txn-raw/"
-- (docs/ASSUMPTIONS.md I5b). Files sit in step=<N>/ sub-directories; the
-- recursive-read settings are in hive-site.xml so every downstream query works
-- without SET.
--
-- NOT declared PARTITIONED BY (step): the connector's FieldPartitioner keeps
-- `step` as a data column in every Parquet file as well as the directory name,
-- and Hive 3.1.3 NPEs on a partition column that also exists in the file schema.
-- `step` is therefore a normal column read from the Parquet data.
-- =============================================================================
USE finsight;

DROP TABLE IF EXISTS finsight.transactions;

CREATE EXTERNAL TABLE finsight.transactions (
  step            INT      COMMENT 'time step, 1 = 1 hour, 1..168',
  type            STRING   COMMENT 'CASH_IN, CASH_OUT, DEBIT, PAYMENT, TRANSFER',
  amount          DOUBLE   COMMENT 'transaction amount, USD',
  nameOrig        STRING   COMMENT 'originating (customer) account, prefix C',
  oldbalanceOrg   DOUBLE,
  newbalanceOrig  DOUBLE,
  nameDest        STRING   COMMENT 'destination account, C customer / M merchant',
  oldbalanceDest  DOUBLE,
  newbalanceDest  DOUBLE,
  isFraud         INT      COMMENT 'ground-truth fraud label 0/1',
  isFlaggedFraud  INT      COMMENT 'legacy system flag 0/1',
  txnId           STRING   COMMENT 'derived id, TXN + 9 digits',
  ingest_ts       STRING   COMMENT 'producer ingest timestamp, ISO-8601 UTC'
)
COMMENT 'Raw transactions data lake - external over Kafka Connect Parquet output'
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/finsight/raw/txn-raw'
TBLPROPERTIES ('external.table.purge'='false');

DESCRIBE finsight.transactions;
