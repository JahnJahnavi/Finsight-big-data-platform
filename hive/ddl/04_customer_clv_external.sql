-- =============================================================================
-- FinSight - Phase 8: finsight.customer_clv  (EXTERNAL, spec section 7.4 R1)
--
-- Over the Spark Core CLV job output (/finsight/processed/clv_scores/), so it is
-- queryable alongside finsight.transactions without a separate file read.
-- Joinable to MongoDB / txn_summary_mart via customerId.
-- =============================================================================
USE finsight;

DROP TABLE IF EXISTS finsight.customer_clv;

CREATE EXTERNAL TABLE finsight.customer_clv (
  customerId            STRING,
  clv_score             DOUBLE  COMMENT '0-1, weighted (30/25/25/20)',
  clv_classification    STRING  COMMENT 'High Value | Growth Potential | At Risk',
  total_amount          DOUBLE,
  txn_count             BIGINT,
  distinct_txn_types    BIGINT,
  last_step             INT,
  steps_since_last_txn  INT,
  volume_score          DOUBLE,
  frequency_score       DOUBLE,
  diversity_score       DOUBLE,
  recency_score         DOUBLE,
  scored_at             TIMESTAMP
)
COMMENT 'Customer Lifetime Value scores - external over Spark Core clv_scores'
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/finsight/processed/clv_scores'
TBLPROPERTIES ('external.table.purge'='false');

DESCRIBE finsight.customer_clv;
SELECT clv_classification, COUNT(*) AS n FROM finsight.customer_clv GROUP BY clv_classification;
