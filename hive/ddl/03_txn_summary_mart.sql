-- =============================================================================
-- FinSight - Phase 8: finsight.txn_summary_mart  (MANAGED, spec section 8.2)
--
-- Pre-aggregated mart: ONE ROW PER CUSTOMER PER STEP. Managed internal table,
-- populated by INSERT OVERWRITE from finsight.transactions, refreshed each time
-- the Spark Core batch job completes.
-- =============================================================================
USE finsight;

DROP TABLE IF EXISTS finsight.txn_summary_mart;

CREATE TABLE finsight.txn_summary_mart (
  customerId    STRING  COMMENT 'nameOrig - join key to MongoDB / customer_clv',
  step          INT,
  txn_count     BIGINT  COMMENT 'transactions initiated by the customer in the step',
  total_amount  DOUBLE,
  avg_amount    DOUBLE,
  max_amount    DOUBLE  COMMENT 'largest single transaction in the step',
  fraud_count   BIGINT  COMMENT 'transactions with isFraud = 1',
  txn_types     STRING  COMMENT 'comma-separated distinct transaction types',
  last_balance  DOUBLE  COMMENT 'newbalanceOrig of the most recent txn in the step'
)
COMMENT 'Per-customer per-step transaction summary mart'
STORED AS PARQUET;

-- INSERT OVERWRITE from the raw table (spec 8.2).
-- last_balance: newbalanceOrig of the latest transaction in the (customer, step)
-- group, ordered by ingest_ts then txnId (no finer intra-step ordering exists).
INSERT OVERWRITE TABLE finsight.txn_summary_mart
SELECT
  a.nameOrig                                             AS customerId,
  a.step                                                 AS step,
  COUNT(*)                                               AS txn_count,
  SUM(a.amount)                                          AS total_amount,
  AVG(a.amount)                                          AS avg_amount,
  MAX(a.amount)                                          AS max_amount,
  SUM(CASE WHEN a.isFraud = 1 THEN 1 ELSE 0 END)         AS fraud_count,
  CONCAT_WS(',', SORT_ARRAY(COLLECT_SET(a.type)))        AS txn_types,
  MAX(lb.last_balance)                                   AS last_balance
FROM finsight.transactions a
LEFT JOIN (
  SELECT nameOrig, step, newbalanceOrig AS last_balance
  FROM (
    SELECT
      nameOrig, step, newbalanceOrig,
      ROW_NUMBER() OVER (
        PARTITION BY nameOrig, step
        ORDER BY ingest_ts DESC, txnId DESC
      ) AS rn
    FROM finsight.transactions
  ) r
  WHERE r.rn = 1
) lb
  ON a.nameOrig = lb.nameOrig AND a.step = lb.step
GROUP BY a.nameOrig, a.step;

SELECT COUNT(*) AS mart_rows FROM finsight.txn_summary_mart;
SELECT * FROM finsight.txn_summary_mart ORDER BY total_amount DESC LIMIT 5;
