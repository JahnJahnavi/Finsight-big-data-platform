"""
FinSight - Phase 12: Alteryx blend-formula unit tests (pure Python).

    pytest tests/unit/test_alteryx_blend.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "alteryx" / "fallback"))
from blend_rules import (  # noqa: E402
    CHURN_WEIGHT,
    FRAUD_WEIGHT,
    avg_transaction_amount,
    composite_risk_score,
    in_step_window,
)


# --- WORKFLOW 1: composite_risk_score ------------------------------------- #
def test_weights_are_frozen():
    assert (FRAUD_WEIGHT, CHURN_WEIGHT) == (0.6, 0.4)


def test_composite_risk_score_spec_formula():
    # (12.5 * 0.6) + (0.30 * 0.4) = 7.5 + 0.12
    assert composite_risk_score(12.5, 0.30) == 7.62


def test_composite_risk_score_handles_nulls():
    assert composite_risk_score(None, None) == 0.0
    assert composite_risk_score(None, 0.5) == 0.2
    assert composite_risk_score(10.0, None) == 6.0


def test_composite_risk_score_normalized_option():
    # fraud_rate_pct 50% -> 0.5 ; (0.5*0.6) + (0.4*0.4) = 0.3 + 0.16
    assert composite_risk_score(50.0, 0.4, normalize_fraud_pct=True) == 0.46


def test_composite_risk_score_custom_weights():
    assert composite_risk_score(1.0, 1.0, fraud_weight=0.7, churn_weight=0.3) == 1.0


# --- WORKFLOW 2: helpers ------------------------------------------------- #
def test_avg_transaction_amount():
    assert avg_transaction_amount(1000.0, 4) == 250.0
    assert avg_transaction_amount(0.0, 0) == 0.0
    assert avg_transaction_amount(None, None) == 0.0


def test_in_step_window_inclusive_1_to_168():
    assert in_step_window(1) is True
    assert in_step_window(168) is True
    assert in_step_window(0) is False
    assert in_step_window(169) is False
    assert in_step_window(None) is False
