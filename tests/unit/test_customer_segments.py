"""
FinSight - Phase 10: customer segment validation unit tests (pure Python).

    pytest tests/unit/test_customer_segments.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "mongodb"))
from segments import EXPECTED_SEGMENTS, validate_segment_counts  # noqa: E402


# real NovaCrest distribution (noveacrest_customers.json, 10 000 customers)
GOOD = {"Premium": 1528, "Standard": 4497, "Basic": 2464,
        "Private Banking": 528, "Student": 983}


def test_expected_segments_are_the_five_from_the_spec():
    assert set(EXPECTED_SEGMENTS) == {
        "Premium", "Standard", "Basic", "Private Banking", "Student"}


def test_valid_distribution_has_no_problems():
    assert validate_segment_counts(GOOD, 10000) == []


def test_missing_segment_flagged():
    counts = {k: v for k, v in GOOD.items() if k != "Student"}
    problems = validate_segment_counts(counts, sum(counts.values()))
    assert any("Student" in p for p in problems)


def test_empty_segment_flagged():
    counts = {**GOOD, "Student": 0}
    problems = validate_segment_counts(counts, 10000 - GOOD["Student"])
    assert any("Student" in p and "missing" in p for p in problems)


def test_unexpected_segment_flagged():
    counts = {**GOOD, "Platinum": 12}
    problems = validate_segment_counts(counts, 10012)
    assert any("Platinum" in p and "unexpected" in p for p in problems)


def test_counts_must_sum_to_total():
    # 5 customers carry a NULL / missing segment -> counts short of the total
    problems = validate_segment_counts(GOOD, 10005)
    assert any("sum" in p for p in problems)


def test_multiple_problems_all_reported():
    counts = {"Premium": 10, "Standard": 20, "Basic": 30, "Rogue": 5}
    problems = validate_segment_counts(counts, 100)
    assert len(problems) >= 2  # missing segments, unexpected value, bad sum
