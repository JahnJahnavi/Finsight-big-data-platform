"""
FinSight - classification rules for the Spark SQL jobs (spec 7.5 / 7.6).

Pure Python (no Spark) so the small pieces of business logic are unit-tested.
``spark_sql_jobs.py`` re-expresses them as SQL CASE expressions.
"""
from __future__ import annotations

# --- compliance: risk classification of a transaction type by its fraud rate ---
# Spec 7.5 lists a "risk classification" column but no thresholds
# (docs/ASSUMPTIONS.md). Default: fraud rate % -> High / Medium / Low.
RISK_HIGH_FRAUD_PCT = 5.0
RISK_MEDIUM_FRAUD_PCT = 1.0


def risk_classification(fraud_rate_pct: float,
                        high_pct: float = RISK_HIGH_FRAUD_PCT,
                        medium_pct: float = RISK_MEDIUM_FRAUD_PCT) -> str:
    """fraud_rate_pct is a percentage (0-100)."""
    if fraud_rate_pct is None:
        return "Low"
    if fraud_rate_pct >= high_pct:
        return "High"
    if fraud_rate_pct >= medium_pct:
        return "Medium"
    return "Low"


# --- dormancy severity (spec 7.6 R1) ---
DORMANCY_INACTIVE_STEPS = 72
DORMANCY_SEVERE_STEPS = 120


def dormancy_severity(steps_inactive: int,
                      inactive_steps: int = DORMANCY_INACTIVE_STEPS,
                      severe_steps: int = DORMANCY_SEVERE_STEPS) -> str | None:
    """steps_inactive = max_step_overall - customer_last_active_step.

    Returns 'Dormant' for inactive_steps..severe_steps, 'Severely Dormant' for
    more than severe_steps, and None when the account is not dormant.
    """
    if steps_inactive is None or steps_inactive <= inactive_steps:
        return None
    if steps_inactive <= severe_steps:
        return "Dormant"
    return "Severely Dormant"


def is_dormant(steps_inactive: int, txn_history_count: int, name_orig: str,
               inactive_steps: int = DORMANCY_INACTIVE_STEPS,
               min_history: int = 5) -> bool:
    """All three spec 7.6 conditions:
      - inactivity strictly greater than `inactive_steps`
      - at least `min_history` prior transactions
      - a CUSTOMER account (nameOrig starts with 'C'); merchants ('M') excluded
    """
    return (
        steps_inactive is not None
        and steps_inactive > inactive_steps
        and (txn_history_count or 0) >= min_history
        and str(name_orig).upper().startswith("C")
    )
