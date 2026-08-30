"""
FinSight - the churn detection signals (spec section 7.2).

A customer is flagged as a churn risk when **>= 2** of these signals are
observed within a **24-step (24-hour) sliding window**:

  S1  Transaction frequency drops below 1 txn per 12 steps for a customer whose
      historical average is above 3 per 12 steps.
  S2  The window's average transaction amount falls below 20% of the customer's
      all-time average amount.
  S3  Transaction behaviour shifts exclusively to CASH_OUT - no PAYMENT or DEBIT
      activity in the window.
  S4  newbalanceOrig reaches zero or below 500 for two or more CONSECUTIVE
      transactions.

DO NOT change these rules (see docs/ASSUMPTIONS.md). This module is pure Python
(no Spark) so it is unit-tested directly and reused as a UDF by
``churn_detection.py`` - one implementation, two callers.
"""
from __future__ import annotations

from dataclasses import dataclass

SIGNAL_NAMES = (
    "S1_LOW_FREQUENCY",
    "S2_AMOUNT_DROP",
    "S3_EXCLUSIVE_CASHOUT",
    "S4_CONSECUTIVE_LOW_BALANCE",
)

# Defaults (overridable via .env: CHURN_*). Frozen values from the spec.
FREQ_LOW_PER_12 = 1.0
FREQ_HIST_PER_12 = 3.0
AMOUNT_DROP_FRACTION = 0.20
BALANCE_LOW_THRESHOLD = 500.0
BALANCE_LOW_CONSECUTIVE = 2
MIN_SIGNALS = 2
WINDOW_STEPS = 24


@dataclass
class Thresholds:
    freq_low_per_12: float = FREQ_LOW_PER_12
    freq_hist_per_12: float = FREQ_HIST_PER_12
    amount_drop_fraction: float = AMOUNT_DROP_FRACTION
    balance_low_threshold: float = BALANCE_LOW_THRESHOLD
    balance_low_consecutive: int = BALANCE_LOW_CONSECUTIVE
    min_signals: int = MIN_SIGNALS
    window_steps: int = WINDOW_STEPS


DEFAULTS = Thresholds()


def _per_12(count: int, window_steps: int) -> float:
    if window_steps <= 0:
        return 0.0
    return count / (window_steps / 12.0)


def signal_1_low_frequency(window_txn_count: int, window_steps: int,
                           hist_freq_per_12: float | None,
                           t: Thresholds = DEFAULTS) -> bool:
    if hist_freq_per_12 is None or hist_freq_per_12 <= t.freq_hist_per_12:
        return False
    return _per_12(window_txn_count, window_steps) < t.freq_low_per_12


def signal_2_amount_drop(window_avg_amount: float | None,
                         all_time_avg_amount: float | None,
                         t: Thresholds = DEFAULTS) -> bool:
    if not all_time_avg_amount or all_time_avg_amount <= 0:
        return False
    if window_avg_amount is None:
        return False
    return window_avg_amount < t.amount_drop_fraction * all_time_avg_amount


def signal_3_exclusive_cashout(types_in_window: list[str],
                               t: Thresholds = DEFAULTS) -> bool:
    types = [str(x).upper() for x in types_in_window if x is not None]
    if not types:
        return False
    if "PAYMENT" in types or "DEBIT" in types:
        return False
    return all(x == "CASH_OUT" for x in types)


def signal_4_consecutive_low_balance(balances_in_order: list[float],
                                     t: Thresholds = DEFAULTS) -> bool:
    run = 0
    for b in balances_in_order:
        try:
            low = b is not None and float(b) < t.balance_low_threshold
        except (TypeError, ValueError):
            low = False
        run = run + 1 if low else 0
        if run >= t.balance_low_consecutive:
            return True
    return False


def evaluate_churn(
    *,
    window_txn_count: int,
    window_steps: int,
    window_avg_amount: float | None,
    types_in_window: list[str],
    balances_in_order: list[float],
    hist_freq_per_12: float | None,
    all_time_avg_amount: float | None,
    t: Thresholds = DEFAULTS,
) -> dict:
    """Evaluate all four signals for one (customer, window). Returns
    ``{"signals": [...], "signal_count": n, "is_churn": bool}``."""
    results = {
        "S1_LOW_FREQUENCY": signal_1_low_frequency(
            window_txn_count, window_steps, hist_freq_per_12, t),
        "S2_AMOUNT_DROP": signal_2_amount_drop(
            window_avg_amount, all_time_avg_amount, t),
        "S3_EXCLUSIVE_CASHOUT": signal_3_exclusive_cashout(types_in_window, t),
        "S4_CONSECUTIVE_LOW_BALANCE": signal_4_consecutive_low_balance(
            balances_in_order, t),
    }
    triggered = [name for name in SIGNAL_NAMES if results[name]]
    return {
        "signals": triggered,
        "signal_count": len(triggered),
        "is_churn": len(triggered) >= t.min_signals,
    }
