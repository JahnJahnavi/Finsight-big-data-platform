"""
FinSight - Phase 7: CLV scoring rule unit tests (pure Python, no Spark).

Each of the four components, the weighted sum (spec weights 30/25/25/20), the
[0, 1] clamp, the recency 48-step cut-off, and the three classes with their
0.40 / 0.70 boundaries (spec 7.4).

    pytest tests/unit/test_clv_scoring.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "spark" / "batch"))
from clv_rules import (  # noqa: E402
    DEFAULT_WEIGHTS,
    Weights,
    clv_classification,
    clv_score,
    diversity_score,
    frequency_score,
    recency_score,
    volume_score,
)


# --- Transaction Volume (30%) --------------------------------------------- #
def test_volume_score():
    assert volume_score(1_000_000, 1_000_000) == 1.0     # the top spender
    assert volume_score(250_000, 1_000_000) == 0.25
    assert volume_score(0, 1_000_000) == 0.0


def test_volume_no_max():
    assert volume_score(100, 0) == 0.0


# --- Transaction Frequency (25%) ---------------------------------------- #
def test_frequency_score():
    assert frequency_score(200, 200) == 1.0              # the most active account
    assert frequency_score(50, 200) == 0.25
    assert frequency_score(0, 200) == 0.0


# --- Product Diversity (25%) ------------------------------------------ #
def test_diversity_score_is_distinct_types_over_5():
    assert diversity_score(5) == 1.0
    assert diversity_score(1) == 0.2
    assert diversity_score(3) == 0.6
    assert diversity_score(0) == 0.0


# --- Recency (20%) ---------------------------------------------------- #
def test_recency_most_recent_is_one():
    assert recency_score(0) == 1.0                        # transacted at the latest step


def test_recency_linear_decay():
    assert recency_score(24) == pytest.approx(0.5)        # halfway through the window
    assert recency_score(12) == pytest.approx(0.75)


def test_recency_zero_after_48_steps():
    assert recency_score(48) == 0.0
    assert recency_score(60) == 0.0
    assert recency_score(47) == pytest.approx(1 - 47 / 48)


def test_recency_none_is_zero():
    assert recency_score(None) == 0.0


# --- weighted CLV score ---------------------------------------------- #
def test_weights_are_the_spec_values():
    assert (DEFAULT_WEIGHTS.volume, DEFAULT_WEIGHTS.frequency,
            DEFAULT_WEIGHTS.diversity, DEFAULT_WEIGHTS.recency) == (0.30, 0.25, 0.25, 0.20)


def test_clv_score_all_max_is_one():
    assert clv_score(1, 1, 1, 1) == 1.0


def test_clv_score_all_zero_is_zero():
    assert clv_score(0, 0, 0, 0) == 0.0


def test_clv_score_weighting():
    # only volume maxed -> 0.30 ; only recency maxed -> 0.20
    assert clv_score(1, 0, 0, 0) == pytest.approx(0.30)
    assert clv_score(0, 0, 0, 1) == pytest.approx(0.20)
    # a realistic mix
    assert clv_score(0.8, 0.6, 0.4, 0.5) == pytest.approx(
        0.30 * 0.8 + 0.25 * 0.6 + 0.25 * 0.4 + 0.20 * 0.5)


def test_clv_score_clamped():
    assert clv_score(2, 2, 2, 2, w=Weights(1, 1, 1, 1)) == 1.0


# --- classification (spec 7.4) ------------------------------------- #
def test_class_high_value():
    assert clv_classification(0.71) == "High Value"
    assert clv_classification(1.0) == "High Value"


def test_class_boundary_070_is_growth_potential():
    assert clv_classification(0.70) == "Growth Potential"     # "> 0.70" is High


def test_class_growth_potential():
    assert clv_classification(0.40) == "Growth Potential"     # "0.40-0.70"
    assert clv_classification(0.55) == "Growth Potential"


def test_class_boundary_below_040_is_at_risk():
    assert clv_classification(0.3999) == "At Risk"
    assert clv_classification(0.0) == "At Risk"


def test_end_to_end_high_value_customer():
    s = clv_score(volume_score(9e5, 1e6), frequency_score(180, 200),
                  diversity_score(5), recency_score(2))
    assert s > 0.70
    assert clv_classification(s) == "High Value"
