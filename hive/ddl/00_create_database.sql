-- =============================================================================
-- FinSight - Phase 8: Hive warehouse database
-- Spec section 8.1
-- =============================================================================

CREATE DATABASE IF NOT EXISTS finsight
  COMMENT 'FinSight - NovaCrest Bank data warehouse'
  LOCATION 'hdfs://namenode:8020/user/hive/warehouse/finsight.db';

USE finsight;
SHOW TABLES;
