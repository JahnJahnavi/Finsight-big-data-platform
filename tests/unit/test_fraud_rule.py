"""
FinSight - Phase 4: fraud rule unit tests (pure Python, no Spark).

Covers the five scenarios from the phase spec plus the > vs >= boundary.
The same rule constants are used by the Spark job via
``fraud_rule.fraud_condition_sql()``.

    pytest tests/unit/test_fraud_rule.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "spark" / "streaming"))
from fraud_rule import AMOUNT_THRESHOLD, fraud_condition_sql, is_fraud  # noqa: E402


def _txn(**over):
    base = dict(
        step=1, type="TRANSFER", amount=250_000.0,
        nameOrig="C1", oldbalanceOrg=250_000.0, newbalanceOrig=0.0,
        nameDest="C2", oldbalanceDest=0.0, newbalanceDest=0.0,
        isFraud=0, isFlaggedFraud=0, txnId="TXN000000001",
    )
    base.update(over)
    return base


# --- the five required scenarios ------------------------------------------- #
CASES = [
    ("1 qualifying TRANSFER",
     _txn(type="TRANSFER", amount=250_000.0, newbalanceDest=0.0), True),
    ("2 qualifying CASH_OUT",
     _txn(type="CASH_OUT", amount=500_000.0, newbalanceDest=0.0), True),
    ("3 TRANSFER below threshold",
     _txn(type="TRANSFER", amount=150_000.0, newbalanceDest=0.0), False),
    ("4 PAYMENT above threshold",
     _txn(type="PAYMENT", amount=300_000.0, newbalanceDest=0.0), False),
    ("5 CASH_OUT non-zero destination balance",
     _txn(type="CASH_OUT", amount=400_000.0, newbalanceDest=1_234.56), False),
]


@pytest.mark.parametrize("name,txn,expected", CASES, ids=[c[0] for c in CASES])
def test_required_scenarios(name, txn, expected):
    assert is_fraud(txn) is expected


# --- boundary / extra guards --------------------------------------------- #
def test_amount_strictly_greater_than_threshold():
    assert is_fraud(_txn(amount=AMOUNT_THRESHOLD)) is False          # 200000  -> no
    assert is_fraud(_txn(amount=AMOUNT_THRESHOLD + 0.01)) is True     # 200000.01 -> yes


def test_debit_and_cash_in_never_flag():
    assert is_fraud(_txn(type="DEBIT", amount=999_999.0, newbalanceDest=0.0)) is False
    assert is_fraud(_txn(type="CASH_IN", amount=999_999.0, newbalanceDest=0.0)) is False


def test_all_three_conditions_required():
    # each condition broken in isolation -> not fraud
    assert is_fraud(_txn(type="PAYMENT")) is False
    assert is_fraud(_txn(amount=1.0)) is False
    assert is_fraud(_txn(newbalanceDest=0.01)) is False


def test_malformed_input_is_not_fraud():
    assert is_fraud({}) is False
    assert is_fraud({"type": "TRANSFER", "amount": "oops", "newbalanceDest": 0}) is False


def test_sql_predicate_matches_the_rule():
    sql = fraud_condition_sql()
    assert "type IN ('TRANSFER', 'CASH_OUT')" in sql
    assert "amount > 200000" in sql
    assert "newbalanceDest = 0" in sql
