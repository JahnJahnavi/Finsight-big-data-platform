"""
FinSight - Customer Lifetime Value (CLV) scoring rules (spec section 7.4).

Pure Python (no Spark) so the component maths and the classification are
unit-tested directly. ``clv_scoring.py`` re-expresses the same maths as Spark
Column arithmetic.

CLV = weighted sum of four components, each in [0, 1]:

  Transaction Volume    (30%)  cumulative amount / highest-spending account
  Transaction Frequency (25%)  txn count / most-active account
  Product Diversity     (25%)  distinct transaction types used / 5
  Recency               (20%)  more recent = higher; 0 if no activity in the
                               last 48 steps

Weights ARE given by the spec - do NOT change them.

Classification (spec 7.4):
  > 0.70          High Value
  0.40 - 0.70     Growth Potential
  < 0.40          At Risk
"""
from __future__ import annotations

from dataclasses import dataclass

COMPONENT_NAMES = ("volume", "frequency", "diversity", "recency")
TXN_TYPES = ("PAYMENT", "TRANSFER", "CASH_IN", "DEBIT", "CASH_OUT")

W_VOLUME = 0.30
W_FREQUENCY = 0.25
W_DIVERSITY = 0.25
W_RECENCY = 0.20
RECENCY_ZERO_AFTER_STEPS = 48
N_TXN_TYPES = 5
TIER_HIGH_MIN = 0.70
TIER_GROWTH_MIN = 0.40


@dataclass
class Weights:
    volume: float = W_VOLUME
    frequency: float = W_FREQUENCY
    diversity: float = W_DIVERSITY
    recency: float = W_RECENCY


DEFAULT_WEIGHTS = Weights()


def _clamp01(x: float) -> float:
    return max(0.0, min(1.0, x))


def volume_score(customer_total_amount: float, max_total_amount: float) -> float:
    """Cumulative transaction amount normalised against the highest spender."""
    if not max_total_amount or max_total_amount <= 0:
        return 0.0
    return _clamp01(customer_total_amount / max_total_amount)


def frequency_score(customer_txn_count: int, max_txn_count: int) -> float:
    """Transaction count normalised against the most active account."""
    if not max_txn_count or max_txn_count <= 0:
        return 0.0
    return _clamp01(customer_txn_count / max_txn_count)


def diversity_score(distinct_txn_types: int, n_types: int = N_TXN_TYPES) -> float:
    """Distinct transaction types used / 5 (spec 7.4)."""
    if n_types <= 0:
        return 0.0
    return _clamp01(distinct_txn_types / n_types)


def recency_score(steps_since_last_txn: int,
                  zero_after_steps: int = RECENCY_ZERO_AFTER_STEPS) -> float:
    """More recent activity scores higher; 0 once inactivity exceeds the window.

    Linear decay:  1 - steps_since_last / zero_after_steps  (spec 7.4 /
    docs/ASSUMPTIONS.md G9). steps_since_last == 0 -> 1.0 ; >= 48 -> 0.0.
    """
    if steps_since_last_txn is None or steps_since_last_txn >= zero_after_steps:
        return 0.0
    return _clamp01(1.0 - steps_since_last_txn / zero_after_steps)


def clv_score(volume: float, frequency: float, diversity: float, recency: float,
              w: Weights = DEFAULT_WEIGHTS) -> float:
    """Weighted sum of the four component scores, clamped to [0, 1]."""
    return _clamp01(
        w.volume * volume
        + w.frequency * frequency
        + w.diversity * diversity
        + w.recency * recency
    )


def clv_classification(score: float,
                       tier_high_min: float = TIER_HIGH_MIN,
                       tier_growth_min: float = TIER_GROWTH_MIN) -> str:
    """Spec 7.4:  > 0.70 High Value | 0.40-0.70 Growth Potential | < 0.40 At Risk."""
    if score > tier_high_min:
        return "High Value"
    if score >= tier_growth_min:
        return "Growth Potential"
    return "At Risk"
