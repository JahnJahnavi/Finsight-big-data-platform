"""
FinSight - Phase 9: Spark SQL classification rule unit tests (pure Python).

    pytest tests/unit/test_sql_rules.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sql"))
from sql_rules import (  # noqa: E402
    dormancy_severity,
    is_dormant,
    risk_classification,
)


# --- compliance: risk classification by fraud rate % ---------------------- #
def test_risk_classification_tiers():
    assert risk_classification(0.0) == "Low"
    assert risk_classification(0.99) == "Low"
    assert risk_classification(1.0) == "Medium"
    assert risk_classification(4.99) == "Medium"
    assert risk_classification(5.0) == "High"
    assert risk_classification(42.0) == "High"


def test_risk_classification_none():
    assert risk_classification(None) == "Low"


def test_risk_classification_custom_thresholds():
    assert risk_classification(3.0, high_pct=10, medium_pct=2) == "Medium"
    assert risk_classification(1.0, high_pct=10, medium_pct=2) == "Low"


# --- dormancy severity (spec 7.6 R1) ---------------------------------- #
def test_severity_not_dormant_at_or_below_72():
    assert dormancy_severity(0) is None
    assert dormancy_severity(72) is None            # base condition is "> 72"


def test_severity_dormant_73_to_120():
    assert dormancy_severity(73) == "Dormant"
    assert dormancy_severity(120) == "Dormant"


def test_severity_severely_dormant_over_120():
    assert dormancy_severity(121) == "Severely Dormant"
    assert dormancy_severity(200) == "Severely Dormant"


def test_severity_none_input():
    assert dormancy_severity(None) is None


# --- is_dormant: all three spec 7.6 conditions ---------------------- #
def test_is_dormant_all_conditions_met():
    assert is_dormant(steps_inactive=100, txn_history_count=5, name_orig="C123") is True


def test_is_dormant_inactivity_must_exceed_72():
    assert is_dormant(72, 10, "C1") is False
    assert is_dormant(73, 10, "C1") is True


def test_is_dormant_needs_5_prior_transactions():
    assert is_dormant(100, 4, "C1") is False
    assert is_dormant(100, 5, "C1") is True


def test_is_dormant_merchant_accounts_excluded():
    assert is_dormant(100, 10, "M999") is False
    assert is_dormant(100, 10, "C999") is True
