// ===========================================================================
// FinSight - Phase 11: fraud-ring detection  (spec section 11)
//
// "Identify accounts receiving transactions from more than three distinct
//  inbound senders."
//
//   cypher-shell -a bolt://localhost:7687 -u neo4j -p <pw> \
//     -P "minSenders => 3" -f neo4j/fraud_ring.cypher
//
// $minSenders defaults to 3 (NEO4J_FRAUD_RING_MIN_SENDERS). "more than" -> the
// receiver must have STRICTLY MORE than that many distinct sender accounts.
// ===========================================================================
MATCH (sender:Account)-[:SENT]->(t:Transaction)-[:RECEIVED_BY]->(receiver:Account)
WITH receiver,
     count(DISTINCT sender)   AS distinct_senders,
     count(t)                 AS inbound_txns,
     sum(t.amount)            AS inbound_amount,
     sum(t.isFraud)           AS confirmed_fraud_txns
WHERE distinct_senders > coalesce($minSenders, 3)
RETURN receiver.accountId    AS account,
       receiver.accountType  AS account_type,
       distinct_senders,
       inbound_txns,
       round(inbound_amount, 2) AS inbound_amount,
       confirmed_fraud_txns
ORDER BY distinct_senders DESC, inbound_amount DESC;
