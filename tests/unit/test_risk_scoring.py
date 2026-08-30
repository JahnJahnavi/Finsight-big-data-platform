"""
FinSight - Phase 6: risk scoring rule unit tests (pure Python, no Spark).

Covers min-max normalisation, the weighted-sum score, clamping to [0, 1] and
the three tiers with their boundaries (spec 7.3 R1).

    pytest tests/unit/test_risk_scoring.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "spark" / "batch"))
from risk_rules import min_max, risk_tier, weighted_risk_score  # noqa: E402


# --- min-max normalisation --------------------------------------------------
def test_min_max_basic():
    assert min_max(0, 0, 10) == 0.0
    assert min_max(10, 0, 10) == 1.0
    assert min_max(5, 0, 10) == 0.5


def test_min_max_clamps_out_of_range():
    assert min_max(-5, 0, 10) == 0.0
    assert min_max(15, 0, 10) == 1.0


def test_min_max_degenerate_range_is_zero():
    assert min_max(7, 7, 7) == 0.0      # every customer identical -> factor 0
    assert min_max(1, 10, 5) == 0.0     # hi <= lo


# --- weighted score --------------------------------------------------------
def test_weighted_score_equal_weights():
    # all factors 1.0, equal 0.25 weights -> 1.0
    assert weighted_risk_score(1, 1, 1, 1) == 1.0
    # all factors 0 -> 0
    assert weighted_risk_score(0, 0, 0, 0) == 0.0
    # one factor 1, rest 0 -> 0.25
    assert weighted_risk_score(1, 0, 0, 0) == 0.25


def test_weighted_score_custom_weights():
    s = weighted_risk_score(1.0, 0.0, 0.0, 0.0, weights=(0.6, 0.2, 0.1, 0.1))
    assert s == pytest.approx(0.6)


def test_weighted_score_clamped():
    assert weighted_risk_score(2, 2, 2, 2, weights=(1, 1, 1, 1)) == 1.0
    assert weighted_risk_score(-1, -1, -1, -1) == 0.0


# --- tiers (spec 7.3 R1) --------------------------------------------------
def test_tier_low():
    assert risk_tier(0.0) == "Low"
    assert risk_tier(0.24999) == "Low"


def test_tier_boundary_025_is_medium():
    assert risk_tier(0.25) == "Medium"        # "0.25-0.60 = Medium"


def test_tier_medium():
    assert risk_tier(0.4) == "Medium"
    assert risk_tier(0.60) == "Medium"        # inclusive upper bound


def test_tier_high():
    assert risk_tier(0.6001) == "High"
    assert risk_tier(1.0) == "High"


def test_tier_thresholds_configurable():
    assert risk_tier(0.5, tier_low_max=0.5, tier_medium_max=0.9) == "Medium"
    assert risk_tier(0.49, tier_low_max=0.5, tier_medium_max=0.9) == "Low"


def test_end_to_end_example():
    # a customer at the top of every factor -> score 1.0 -> High
    score = weighted_risk_score(
        min_max(100, 1, 100), min_max(9e5, 0, 9e5),
        min_max(1.0, 0.0, 1.0), min_max(40, 1, 40))
    assert score == 1.0
    assert risk_tier(score) == "High"
