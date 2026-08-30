"""
FinSight - Phase 5: churn signal unit tests (pure Python, no Spark).

Each of the four spec signals in isolation, the >= 2 flag threshold, signal
combinations, and boundary conditions. The same functions are used by
churn_detection.py's per-customer state function.

    pytest tests/unit/test_churn_rule.py -v
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "spark" / "streaming"))
from churn_rule import (  # noqa: E402
    DEFAULTS,
    evaluate_churn,
    signal_1_low_frequency,
    signal_2_amount_drop,
    signal_3_exclusive_cashout,
    signal_4_consecutive_low_balance,
)

WIN = 24


# --------------------------------------------------------------------------- #
# S1 - transaction frequency
# --------------------------------------------------------------------------- #
def test_s1_fires_low_freq_high_history():
    # 1 txn in a 24-step window -> 0.5 per 12 steps < 1 ; history 5 > 3
    assert signal_1_low_frequency(1, WIN, hist_freq_per_12=5.0) is True


def test_s1_no_when_history_not_above_3():
    assert signal_1_low_frequency(1, WIN, hist_freq_per_12=3.0) is False
    assert signal_1_low_frequency(1, WIN, hist_freq_per_12=None) is False


def test_s1_no_when_window_frequency_not_low():
    # 2 txns / 24 steps == exactly 1 per 12  -> NOT below 1
    assert signal_1_low_frequency(2, WIN, hist_freq_per_12=5.0) is False


# --------------------------------------------------------------------------- #
# S2 - average amount drop
# --------------------------------------------------------------------------- #
def test_s2_fires_below_20pct():
    assert signal_2_amount_drop(window_avg_amount=9_999.0, all_time_avg_amount=50_000.0) is True


def test_s2_boundary_exactly_20pct_does_not_fire():
    assert signal_2_amount_drop(10_000.0, 50_000.0) is False   # 20% exactly -> not "below"


def test_s2_no_baseline_no_signal():
    assert signal_2_amount_drop(1.0, None) is False
    assert signal_2_amount_drop(1.0, 0.0) is False


# --------------------------------------------------------------------------- #
# S3 - exclusively CASH_OUT
# --------------------------------------------------------------------------- #
def test_s3_fires_all_cashout():
    assert signal_3_exclusive_cashout(["CASH_OUT", "CASH_OUT", "CASH_OUT"]) is True


def test_s3_no_when_payment_or_debit_present():
    assert signal_3_exclusive_cashout(["CASH_OUT", "PAYMENT"]) is False
    assert signal_3_exclusive_cashout(["CASH_OUT", "DEBIT"]) is False


def test_s3_no_when_other_types_present():
    assert signal_3_exclusive_cashout(["CASH_OUT", "TRANSFER"]) is False
    assert signal_3_exclusive_cashout([]) is False


# --------------------------------------------------------------------------- #
# S4 - consecutive low balance
# --------------------------------------------------------------------------- #
def test_s4_fires_two_consecutive_below_500():
    assert signal_4_consecutive_low_balance([9000.0, 100.0, 50.0, 8000.0]) is True
    assert signal_4_consecutive_low_balance([0.0, 0.0]) is True


def test_s4_no_when_only_one_low():
    assert signal_4_consecutive_low_balance([100.0, 9000.0, 200.0]) is False


def test_s4_boundary_exactly_500_not_low():
    assert signal_4_consecutive_low_balance([500.0, 500.0]) is False       # "below 500"
    assert signal_4_consecutive_low_balance([499.99, 499.99]) is True


# --------------------------------------------------------------------------- #
# evaluate_churn - the >= 2 flag threshold and combinations
# --------------------------------------------------------------------------- #
def _profile(**over):
    base = dict(
        window_txn_count=3, window_steps=WIN, window_avg_amount=10_000.0,
        types_in_window=["PAYMENT", "TRANSFER", "CASH_IN"],
        balances_in_order=[9_000.0, 8_000.0, 7_000.0],
        hist_freq_per_12=1.0, all_time_avg_amount=10_000.0,
    )
    base.update(over)
    return base


def test_single_signal_does_not_flag():
    r = evaluate_churn(**_profile(window_txn_count=1, hist_freq_per_12=5.0))
    assert r["signals"] == ["S1_LOW_FREQUENCY"]
    assert r["is_churn"] is False


def test_s1_plus_s2_flags():
    r = evaluate_churn(**_profile(
        window_txn_count=1, hist_freq_per_12=5.0,
        window_avg_amount=1_000.0, all_time_avg_amount=50_000.0,
        types_in_window=["PAYMENT"], balances_in_order=[9_000.0]))
    assert set(r["signals"]) == {"S1_LOW_FREQUENCY", "S2_AMOUNT_DROP"}
    assert r["is_churn"] is True


def test_s3_plus_s4_flags():
    r = evaluate_churn(**_profile(
        window_txn_count=3, hist_freq_per_12=1.0,
        window_avg_amount=9_000.0, all_time_avg_amount=10_000.0,
        types_in_window=["CASH_OUT", "CASH_OUT", "CASH_OUT"],
        balances_in_order=[400.0, 100.0, 9_000.0]))
    assert set(r["signals"]) == {"S3_EXCLUSIVE_CASHOUT", "S4_CONSECUTIVE_LOW_BALANCE"}
    assert r["is_churn"] is True


def test_three_signals_s1_s2_s3():
    r = evaluate_churn(**_profile(
        window_txn_count=1, hist_freq_per_12=6.0,
        window_avg_amount=500.0, all_time_avg_amount=100_000.0,
        types_in_window=["CASH_OUT"], balances_in_order=[9_000.0]))
    assert set(r["signals"]) == {"S1_LOW_FREQUENCY", "S2_AMOUNT_DROP", "S3_EXCLUSIVE_CASHOUT"}
    assert r["signal_count"] == 3 and r["is_churn"] is True


def test_no_signals():
    r = evaluate_churn(**_profile())
    assert r["signals"] == [] and r["is_churn"] is False


def test_min_signals_default_is_two():
    assert DEFAULTS.min_signals == 2
