-- =============================================================================
-- FinSight - Phase 8: finsight.vw_fraud_transactions  (VIEW, spec 8.1 R1)
--
-- Single consistent definition of a confirmed fraud record (isFraud = 1),
-- shared by the Neo4j graph load and the Power BI Fraud Alert Board.
-- =============================================================================
USE finsight;

DROP VIEW IF EXISTS finsight.vw_fraud_transactions;

CREATE VIEW finsight.vw_fraud_transactions
COMMENT 'Confirmed fraud transactions (isFraud = 1)'
AS
SELECT
  step, type, amount,
  nameOrig, oldbalanceOrg, newbalanceOrig,
  nameDest, oldbalanceDest, newbalanceDest,
  isFraud, isFlaggedFraud, txnId, ingest_ts
FROM finsight.transactions
WHERE isFraud = 1;

DESCRIBE finsight.vw_fraud_transactions;
