#!/usr/bin/env python3
"""
FinSight - Phase 12: Alteryx blend formulas in pure Python (unit-tested).

These mirror the Formula-tool expressions documented in
docs/alteryx/workflow-1-customer-risk-blend.md so the composite-risk maths can
be tested without Alteryx Designer and reused by alteryx/fallback/*.py.
"""
from __future__ import annotations

# WORKFLOW 1 - composite_risk_score weights (spec section 12, frozen)
FRAUD_WEIGHT = 0.6
CHURN_WEIGHT = 0.4


def composite_risk_score(fraud_rate_pct: float | None,
                         churn_probability: float | None,
                         fraud_weight: float = FRAUD_WEIGHT,
                         churn_weight: float = CHURN_WEIGHT,
                         normalize_fraud_pct: bool = False) -> float:
    """(fraud_rate_pct * 0.6) + (churn_probability * 0.4)

    Spec formula verbatim. `fraud_rate_pct` from Hive customer_fraud_summary is a
    0-100 percentage; `churn_probability` from the MongoDB profile is 0-1 - see
    ASSUMPTIONS I48. `normalize_fraud_pct=True` divides the percentage by 100
    first so both inputs share the 0-1 scale (documented alternative, off by
    default).
    """
    fr = fraud_rate_pct or 0.0
    if normalize_fraud_pct:
        fr = fr / 100.0
    cp = churn_probability or 0.0
    return fr * fraud_weight + cp * churn_weight


def avg_transaction_amount(total_volume: float | None, txn_count: float | None) -> float:
    """WORKFLOW 2 - average transaction amount = total volume / transaction count."""
    n = txn_count or 0
    if not n:
        return 0.0
    return (total_volume or 0.0) / n


# WORKFLOW 2 - inclusive step window (spec section 12)
STEP_MIN = 1
STEP_MAX = 168


def in_step_window(step: int | None, lo: int = STEP_MIN, hi: int = STEP_MAX) -> bool:
    return step is not None and lo <= step <= hi
