"""
FinSight - customer risk scoring rules (spec section 7.3).

Pure Python (no Spark) so the tier logic and the weighting/normalisation maths
are unit-tested directly. ``risk_scoring.py`` re-expresses the same maths as
Spark Column arithmetic.

Composite risk score = weighted sum of four min-max-normalised factors:

  1. transaction frequency        - # transactions the customer initiated
  2. average transfer amount      - mean amount of the customer's TRANSFER txns
  3. CASH_OUT proportion          - CASH_OUT txns / all the customer's txns
  4. unique destination accounts  - distinct nameDest reached

Each raw factor is normalised ``(v - min) / (max - min)`` across all customers,
then combined with the (config-driven) weights. Result clamped to [0, 1].

Weights default to 0.25 each - the spec names the four factors but gives no
weights (docs/ASSUMPTIONS.md G6). Override via RISK_W_* env vars.
"""
from __future__ import annotations

FACTOR_NAMES = (
    "frequency",
    "avg_transfer_amount",
    "cashout_proportion",
    "unique_dest_accounts",
)

# tier thresholds (spec 7.3 R1) - overridable via RISK_TIER_* env vars
TIER_LOW_MAX = 0.25
TIER_MEDIUM_MAX = 0.60


def min_max(value: float, lo: float, hi: float) -> float:
    """Normalise ``value`` to [0, 1] given the observed [lo, hi]. Degenerate
    range (all customers identical) -> 0.0."""
    if hi is None or lo is None or hi <= lo:
        return 0.0
    return max(0.0, min(1.0, (value - lo) / (hi - lo)))


def weighted_risk_score(
    norm_frequency: float,
    norm_avg_transfer: float,
    norm_cashout_prop: float,
    norm_unique_dest: float,
    weights: tuple[float, float, float, float] = (0.25, 0.25, 0.25, 0.25),
) -> float:
    """Weighted sum of the four normalised factors, clamped to [0, 1]."""
    w1, w2, w3, w4 = weights
    score = (
        w1 * norm_frequency
        + w2 * norm_avg_transfer
        + w3 * norm_cashout_prop
        + w4 * norm_unique_dest
    )
    return max(0.0, min(1.0, score))


def risk_tier(score: float,
              tier_low_max: float = TIER_LOW_MAX,
              tier_medium_max: float = TIER_MEDIUM_MAX) -> str:
    """Spec 7.3 R1:  < 0.25 = Low, 0.25-0.60 = Medium, > 0.60 = High."""
    if score < tier_low_max:
        return "Low"
    if score <= tier_medium_max:
        return "Medium"
    return "High"
