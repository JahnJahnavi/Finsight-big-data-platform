-- =============================================================================
-- FinSight - Phase 8: Hive table statistics
--
-- Spec 8.1 R2: ANALYZE finsight.transactions COMPUTE STATISTICS
--              (required before any Spark SQL query against the warehouse).
-- Spec 8.2 R1: ANALYZE finsight.txn_summary_mart COMPUTE STATISTICS FOR COLUMNS
--              (column-level stats for the compliance / dormancy range scans).
-- =============================================================================
USE finsight;

ANALYZE TABLE finsight.transactions COMPUTE STATISTICS;

ANALYZE TABLE finsight.txn_summary_mart COMPUTE STATISTICS;
ANALYZE TABLE finsight.txn_summary_mart COMPUTE STATISTICS FOR COLUMNS;

ANALYZE TABLE finsight.customer_clv COMPUTE STATISTICS;

DESCRIBE FORMATTED finsight.transactions;
DESCRIBE FORMATTED finsight.txn_summary_mart;
