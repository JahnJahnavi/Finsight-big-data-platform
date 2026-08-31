#!/usr/bin/env python3
"""
FinSight - Phase 11: fraud-graph rules (pure Python, unit-tested).

The fraud-ring threshold lives here so it can be tested without a live Neo4j;
`neo4j/fraud_ring.cypher` encodes the same rule in Cypher (keep the two in sync).
"""
from __future__ import annotations

from collections import defaultdict
from typing import Iterable

# spec section 11: "more than three distinct inbound senders"
FRAUD_RING_MIN_SENDERS = 3


def is_fraud_ring_account(distinct_sender_count: int | None,
                          min_senders: int = FRAUD_RING_MIN_SENDERS) -> bool:
    """True when a receiver has STRICTLY MORE than `min_senders` distinct senders."""
    return (distinct_sender_count or 0) > min_senders


def fraud_ring_accounts(sent_edges: Iterable[tuple[str, str]],
                        received_edges: Iterable[tuple[str, str]],
                        min_senders: int = FRAUD_RING_MIN_SENDERS) -> dict[str, int]:
    """Replicate neo4j/fraud_ring.cypher in memory.

    sent_edges     : (sender_account_id, txn_id)   -- Account-[:SENT]->Transaction
    received_edges : (txn_id, receiver_account_id) -- Transaction-[:RECEIVED_BY]->Account

    Returns {receiver_account_id: distinct_inbound_sender_count} for receivers
    over the threshold, so callers can assert on the exact ring membership.
    """
    txn_sender = {txn: sender for sender, txn in sent_edges}
    senders_by_receiver: dict[str, set] = defaultdict(set)
    for txn, receiver in received_edges:
        sender = txn_sender.get(txn)
        if sender is not None:
            senders_by_receiver[receiver].add(sender)
    return {r: len(s) for r, s in senders_by_receiver.items()
            if is_fraud_ring_account(len(s), min_senders)}
