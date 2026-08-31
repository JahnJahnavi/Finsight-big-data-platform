// ===========================================================================
// FinSight - Phase 11: Neo4j fraud graph schema  (spec section 11)
//
//   cypher-shell -a bolt://localhost:7687 -u neo4j -p <pw> -f neo4j/schema.cypher
//   (neo4j/loader.py applies this automatically before loading)
//
// Graph model:  (Account)-[:SENT]->(Transaction)-[:RECEIVED_BY]->(Account)
// Transaction is a first-class node (properties: txnId, step, type, amount, isFraud).
//
// All statements are idempotent (IF NOT EXISTS).
// ===========================================================================

// --- node keys: uniqueness + MATCH performance for the loader's MERGE ---
CREATE CONSTRAINT account_id IF NOT EXISTS
  FOR (a:Account) REQUIRE a.accountId IS UNIQUE;

CREATE CONSTRAINT transaction_id IF NOT EXISTS
  FOR (t:Transaction) REQUIRE t.txnId IS UNIQUE;

// --- secondary indexes for the fraud-ring / analytics queries ---
CREATE INDEX transaction_isFraud IF NOT EXISTS
  FOR (t:Transaction) ON (t.isFraud);

CREATE INDEX transaction_step IF NOT EXISTS
  FOR (t:Transaction) ON (t.step);

CREATE INDEX account_type IF NOT EXISTS
  FOR (a:Account) ON (a.accountType);
