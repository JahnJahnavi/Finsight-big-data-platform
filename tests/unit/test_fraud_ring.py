"""
FinSight - Phase 11: fraud-ring rule unit tests (pure Python).

    pytest tests/unit/test_fraud_ring.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "neo4j"))
from graph_rules import (  # noqa: E402
    FRAUD_RING_MIN_SENDERS,
    fraud_ring_accounts,
    is_fraud_ring_account,
)


# --- threshold: "more than three" -> strictly > 3 --------------------------- #
def test_default_threshold_is_three():
    assert FRAUD_RING_MIN_SENDERS == 3


def test_is_fraud_ring_account_boundary():
    assert is_fraud_ring_account(3) is False      # exactly three is NOT a ring
    assert is_fraud_ring_account(4) is True       # more than three
    assert is_fraud_ring_account(0) is False
    assert is_fraud_ring_account(None) is False


def test_is_fraud_ring_account_custom_threshold():
    assert is_fraud_ring_account(6, min_senders=5) is True
    assert is_fraud_ring_account(5, min_senders=5) is False


# --- fraud_ring_accounts mirrors neo4j/fraud_ring.cypher ------------------- #
def _edges():
    # R1 gets txns from S1..S4 (4 distinct senders) -> ring
    # R2 gets txns from S1..S3 (3 distinct senders) -> NOT a ring
    # R3 gets 5 txns but all from S1              -> NOT a ring
    sent = [
        ("S1", "T1"), ("S2", "T2"), ("S3", "T3"), ("S4", "T4"),
        ("S1", "T5"), ("S2", "T6"), ("S3", "T7"),
        ("S1", "T8"), ("S1", "T9"), ("S1", "T10"), ("S1", "T11"), ("S1", "T12"),
    ]
    received = [
        ("T1", "R1"), ("T2", "R1"), ("T3", "R1"), ("T4", "R1"),
        ("T5", "R2"), ("T6", "R2"), ("T7", "R2"),
        ("T8", "R3"), ("T9", "R3"), ("T10", "R3"), ("T11", "R3"), ("T12", "R3"),
    ]
    return sent, received


def test_fraud_ring_accounts_identifies_only_the_ring():
    sent, received = _edges()
    ring = fraud_ring_accounts(sent, received)
    assert ring == {"R1": 4}


def test_fraud_ring_accounts_counts_distinct_senders_not_txns():
    sent, received = _edges()
    ring = fraud_ring_accounts(sent, received, min_senders=0)
    assert ring == {"R1": 4, "R2": 3, "R3": 1}   # R3 has 5 txns but 1 sender


def test_fraud_ring_accounts_empty_graph():
    assert fraud_ring_accounts([], []) == {}
